//! Zero-Copy Binary Protocol Network Server & Client (`TASK-029`, `F-OPS-07`, `RULE-SAFE-01`).
//!
//! Wire Protocol (Little-Endian):
//! - Request Header (29 bytes):
//!   - `op: u8` (`0x01 = GetRawFrame`, `0x02 = PutRawFrame`, `0x03 = Delete`, `0x04 = Stats`)
//!   - `id: [u8; 24]` (`InlineId` raw bytes)
//!   - `body_len: u32`
//!   - `body: [u8; body_len]`
//! - Response Header (13 bytes):
//!   - `status: u8` (`0x00 = Ok`, `0x01 = NotFound`, `0x02 = Error`)
//!   - `version: u64`
//!   - `body_len: u32`
//!   - `body: [u8; body_len]`
//!
//! On `GetRawFrame`, the server streams the 64-byte-aligned `[RecordHeader | Payload]` bytes
//! directly from `RecordRef::raw_frame()` to the socket without deserializing `T` on the server!

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

use std::io::{self, BufReader, BufWriter, IoSlice, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use storage_codec::{AlignedBytes, RecordCodec, RecordView, ZeroCopyRecord};
use storage_core::{AgentObjectRecord, RecordRef, StorageEngine};
use storage_index::Indexable;
use storage_types::{
    InlineId, RecordFlags, Result, StorageError, Version, MAX_PAYLOAD_SIZE,
};

const OP_GET_RAW_FRAME: u8 = 0x01;
const OP_PUT_RAW_FRAME: u8 = 0x02;
const OP_DELETE: u8 = 0x03;
const OP_STATS: u8 = 0x04;

const STATUS_OK: u8 = 0x00;
const STATUS_NOT_FOUND: u8 = 0x01;
const STATUS_ERROR: u8 = 0x02;
const STATUS_OVERLOADED: u8 = 0x03;

/// Default hard ceiling on simultaneous active TCP connections before load-shedding kicks in.
pub const DEFAULT_MAX_CONNECTIONS: usize = 100_000;

struct ReactorShard {
    incoming: Mutex<Vec<TcpStream>>,
    wake_cv: Condvar,
}

impl ReactorShard {
    fn new() -> Self {
        Self {
            incoming: Mutex::new(Vec::new()),
            wake_cv: Condvar::new(),
        }
    }

    #[inline]
    fn push_stream(&self, stream: TcpStream) {
        let mut guard = self
            .incoming
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.push(stream);
        self.wake_cv.notify_one();
    }
}

/// Per-connection non-blocking state machine (~88 bytes when idle between frames).
/// Requires **zero OS thread stack** and **zero persistent heap buffers** when idle,
/// enabling a single server node to multiplex massive numbers of concurrent connections.
struct NonBlockingConn {
    stream: TcpStream,
    req_hdr: [u8; 29],
    hdr_read: u8,
    body_buf: Vec<u8>,
    body_read: usize,
    out_buf: Vec<u8>,
    out_pos: usize,
}

impl NonBlockingConn {
    #[inline]
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            req_hdr: [0u8; 29],
            hdr_read: 0,
            body_buf: Vec::new(),
            body_read: 0,
            out_buf: Vec::new(),
            out_pos: 0,
        }
    }

    /// Writes `[13B ResponseHeader | body]` directly to the non-blocking socket with **zero heap
    /// allocations** in the common case when the kernel send buffer accepts the frame immediately.
    /// Only spills unwritten trailing bytes into `self.out_buf` if the socket returns `WouldBlock`.
    #[inline]
    fn send_or_queue_response(&mut self, status: u8, version: u64, body: &[u8]) -> io::Result<()> {
        let mut hdr = [0u8; 13];
        hdr[0] = status;
        hdr[1..9].copy_from_slice(&version.to_le_bytes());
        hdr[9..13].copy_from_slice(&(body.len() as u32).to_le_bytes());

        let total = 13 + body.len();
        let written = if body.is_empty() {
            match self.stream.write(&hdr) {
                Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "socket closed")),
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => 0,
                Err(e) => return Err(e),
            }
        } else {
            let slices = [IoSlice::new(&hdr), IoSlice::new(body)];
            match self.stream.write_vectored(&slices) {
                Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "socket closed")),
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => 0,
                Err(e) => return Err(e),
            }
        };

        if written < total {
            let rem = total - written;
            self.out_buf.clear();
            if self.out_buf.try_reserve_exact(rem).is_err() {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "out of memory queuing socket response",
                ));
            }
            if written < 13 {
                self.out_buf.extend_from_slice(&hdr[written..]);
                self.out_buf.extend_from_slice(body);
            } else {
                let body_off = written - 13;
                self.out_buf.extend_from_slice(&body[body_off..]);
            }
            self.out_pos = 0;
        }
        Ok(())
    }
}

/// Embedded TCP Server streaming zero-copy record frames directly from `StorageEngine<T>`
/// using a fixed-size **Sharded Non-Blocking Event Reactor** (`ReactorShard` worker pool).
pub struct StorageServer<T: ZeroCopyRecord + Indexable> {
    local_addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    active_connections: Arc<AtomicUsize>,
    rejected_connections: Arc<AtomicUsize>,
    max_connections: Arc<AtomicUsize>,
    shards: Vec<Arc<ReactorShard>>,
    accept_thread: Option<JoinHandle<()>>,
    worker_threads: Vec<JoinHandle<()>>,
    _engine: Arc<StorageEngine<T>>,
}

impl<T: ZeroCopyRecord + Indexable> core::fmt::Debug for StorageServer<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StorageServer")
            .field("local_addr", &self.local_addr)
            .field(
                "active_connections",
                &self.active_connections.load(Ordering::Relaxed),
            )
            .field(
                "rejected_connections",
                &self.rejected_connections.load(Ordering::Relaxed),
            )
            .field(
                "max_connections",
                &self.max_connections.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl<T: ZeroCopyRecord + Indexable> StorageServer<T> {
    pub fn start(bind_addr: &str, engine: Arc<StorageEngine<T>>) -> Result<Self> {
        Self::start_with_max_connections(bind_addr, engine, DEFAULT_MAX_CONNECTIONS)
    }

    pub fn start_with_max_connections(
        bind_addr: &str,
        engine: Arc<StorageEngine<T>>,
        max_connections: usize,
    ) -> Result<Self> {
        let max_conns = Arc::new(AtomicUsize::new(max_connections.max(1)));
        let listener = TcpListener::bind(bind_addr)
            .map_err(|e| StorageError::io("bind StorageServer TCP listener", e))?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| StorageError::io("query local_addr", e))?;

        let num_workers = thread::available_parallelism()
            .map_or(4, |n| n.get().clamp(2, 16));
        let shutdown = Arc::new(AtomicBool::new(false));
        let active_connections = Arc::new(AtomicUsize::new(0));
        let rejected_connections = Arc::new(AtomicUsize::new(0));

        let mut shards = Vec::with_capacity(num_workers);
        let mut worker_threads = Vec::with_capacity(num_workers);

        for _ in 0..num_workers {
            let shard = Arc::new(ReactorShard::new());
            let shard_clone = Arc::clone(&shard);
            let eng_clone = Arc::clone(&engine);
            let sd_clone = Arc::clone(&shutdown);
            let conn_count = Arc::clone(&active_connections);
            shards.push(shard);
            worker_threads.push(thread::spawn(move || {
                reactor_worker_loop(shard_clone, eng_clone, sd_clone, conn_count);
            }));
        }

        let shutdown_accept = Arc::clone(&shutdown);
        let shards_accept = shards.clone();
        let conn_count_accept = Arc::clone(&active_connections);
        let rejected_accept = Arc::clone(&rejected_connections);
        let max_conns_accept = Arc::clone(&max_conns);

        let accept_thread = thread::spawn(move || {
            let mut rr = 0usize;
            for stream_res in listener.incoming() {
                if shutdown_accept.load(Ordering::Relaxed) {
                    break;
                }
                match stream_res {
                    Ok(mut stream) => {
                        let _ = stream.set_nodelay(true);
                        // Connection admission control: shed excess connections gracefully
                        // if active_connections >= max_conns.
                        let limit = max_conns_accept.load(Ordering::Acquire);
                        let prev = conn_count_accept.fetch_add(1, Ordering::AcqRel);
                        if prev >= limit {
                            conn_count_accept.fetch_sub(1, Ordering::Release);
                            rejected_accept.fetch_add(1, Ordering::Relaxed);
                            let msg = b"server busy: max_connections limit reached";
                            let mut hdr = [0u8; 13];
                            hdr[0] = STATUS_OVERLOADED;
                            hdr[9..13].copy_from_slice(&(msg.len() as u32).to_le_bytes());
                            let _ = stream.set_write_timeout(Some(Duration::from_millis(5)));
                            let _ = stream.write_vectored(&[IoSlice::new(&hdr), IoSlice::new(msg)]);
                            continue;
                        }
                        let _ = stream.set_nonblocking(true);
                        let target = &shards_accept[rr % shards_accept.len()];
                        rr = rr.wrapping_add(1);
                        target.push_stream(stream);
                    }
                    Err(_) => {
                        // Transient OS socket/file-descriptor exhaustion (`EMFILE` / `ENFILE`):
                        // yield briefly so the accept loop never busy-spins at 100% CPU.
                        thread::sleep(Duration::from_millis(1));
                    }
                }
            }
        });

        Ok(Self {
            local_addr,
            shutdown,
            active_connections,
            rejected_connections,
            max_connections: max_conns,
            shards,
            accept_thread: Some(accept_thread),
            worker_threads,
            _engine: engine,
        })
    }

    #[inline]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    #[inline]
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn rejected_connections(&self) -> usize {
        self.rejected_connections.load(Ordering::Relaxed)
    }

    /// Returns the current maximum simultaneous connection limit.
    #[inline]
    pub fn max_connections(&self) -> usize {
        self.max_connections.load(Ordering::Acquire)
    }

    /// Dynamically updates the maximum simultaneous connection limit at runtime without restarting.
    #[inline]
    pub fn set_max_connections(&self, max_connections: usize) {
        self.max_connections
            .store(max_connections.max(1), Ordering::Release);
    }

    pub fn stop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.local_addr);
        for shard in &self.shards {
            shard.wake_cv.notify_all();
        }
        if let Some(handle) = self.accept_thread.take() {
            let _ = handle.join();
        }
        for handle in self.worker_threads.drain(..) {
            let _ = handle.join();
        }
    }
}

impl<T: ZeroCopyRecord + Indexable> Drop for StorageServer<T> {
    fn drop(&mut self) {
        self.stop();
    }
}

fn reactor_worker_loop<T: ZeroCopyRecord + Indexable>(
    shard: Arc<ReactorShard>,
    engine: Arc<StorageEngine<T>>,
    shutdown: Arc<AtomicBool>,
    active_connections: Arc<AtomicUsize>,
) {
    let mut conns: Vec<NonBlockingConn> = Vec::with_capacity(64);
    let mut idle_iters = 0u32;

    while !shutdown.load(Ordering::Relaxed) {
        {
            let mut inc = shard
                .incoming
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if conns.is_empty() && inc.is_empty() {
                let (mut inc2, _) = shard
                    .wake_cv
                    .wait_timeout(inc, Duration::from_millis(25))
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                for stream in inc2.drain(..) {
                    conns.push(NonBlockingConn::new(stream));
                }
            } else {
                for stream in inc.drain(..) {
                    conns.push(NonBlockingConn::new(stream));
                }
            }
        }

        if conns.is_empty() {
            continue;
        }

        let mut made_progress = false;
        let mut idx = 0usize;
        while idx < conns.len() {
            match drive_connection(&mut conns[idx], &engine) {
                ConnStep::Progress => {
                    made_progress = true;
                    idx += 1;
                }
                ConnStep::WouldBlock => {
                    idx += 1;
                }
                ConnStep::Closed => {
                    conns.swap_remove(idx);
                    active_connections.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }

        if made_progress {
            idle_iters = 0;
        } else {
            idle_iters = idle_iters.saturating_add(1);
            if idle_iters <= 256 {
                std::hint::spin_loop();
            } else if idle_iters <= 512 {
                thread::yield_now();
            } else {
                let inc = shard
                    .incoming
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if inc.is_empty() {
                    let _ = shard
                        .wake_cv
                        .wait_timeout(inc, Duration::from_micros(250))
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
            }
        }
    }
}

enum ConnStep {
    Progress,
    WouldBlock,
    Closed,
}

fn drive_connection<T: ZeroCopyRecord + Indexable>(
    conn: &mut NonBlockingConn,
    engine: &StorageEngine<T>,
) -> ConnStep {
    // 1. Flush any pending response tail first
    if conn.out_pos < conn.out_buf.len() {
        match conn.stream.write(&conn.out_buf[conn.out_pos..]) {
            Ok(0) => return ConnStep::Closed,
            Ok(n) => {
                conn.out_pos += n;
                if conn.out_pos >= conn.out_buf.len() {
                    conn.out_buf = Vec::new();
                    conn.out_pos = 0;
                } else {
                    return ConnStep::Progress;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return ConnStep::WouldBlock,
            Err(_) => return ConnStep::Closed,
        }
    }

    // 2. Read 29-byte request header
    let hdr_pos = conn.hdr_read as usize;
    if hdr_pos < 29 {
        match conn.stream.read(&mut conn.req_hdr[hdr_pos..]) {
            Ok(0) => return ConnStep::Closed,
            Ok(n) => {
                let new_pos = hdr_pos + n;
                conn.hdr_read = new_pos as u8;
                if new_pos < 29 {
                    return ConnStep::Progress;
                }
                // Header complete; validate body_len before allocating
                let op = conn.req_hdr[0];
                let body_len =
                    u32::from_le_bytes(conn.req_hdr[25..29].try_into().unwrap()) as usize;
                if body_len > MAX_PAYLOAD_SIZE as usize {
                    let _ = conn.send_or_queue_response(
                        STATUS_ERROR,
                        0,
                        b"payload exceeds limit",
                    );
                    return ConnStep::Closed;
                }
                if op != OP_PUT_RAW_FRAME && body_len > 0 {
                    let _ = conn.send_or_queue_response(
                        STATUS_ERROR,
                        0,
                        b"unexpected request body for opcode",
                    );
                    return ConnStep::Closed;
                }
                if body_len > 0 {
                    conn.body_buf.clear();
                    if conn.body_buf.try_reserve_exact(body_len).is_err() {
                        let _ = conn.send_or_queue_response(
                            STATUS_OVERLOADED,
                            0,
                            b"server out of memory for request body",
                        );
                        return ConnStep::Closed;
                    }
                    conn.body_buf.resize(body_len, 0);
                    conn.body_read = 0;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return ConnStep::WouldBlock,
            Err(_) => return ConnStep::Closed,
        }
    }

    // 3. Read request body (if any)
    if conn.body_read < conn.body_buf.len() {
        match conn.stream.read(&mut conn.body_buf[conn.body_read..]) {
            Ok(0) => return ConnStep::Closed,
            Ok(n) => {
                conn.body_read += n;
                if conn.body_read < conn.body_buf.len() {
                    return ConnStep::Progress;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => return ConnStep::WouldBlock,
            Err(_) => return ConnStep::Closed,
        }
    }

    // 4. Execute request and write response directly
    let op = conn.req_hdr[0];
    let raw_id: [u8; 24] = conn.req_hdr[1..25].try_into().unwrap();
    conn.hdr_read = 0;
    conn.body_read = 0;

    let write_res = match op {
        OP_GET_RAW_FRAME => {
            let Ok(id) = InlineId::from_raw_bytes(raw_id) else {
                let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"invalid InlineId");
                return ConnStep::Progress;
            };
            match engine.get_ref(id) {
                Ok(rec_ref) => {
                    let raw_frame = rec_ref.raw_frame();
                    let ver = rec_ref.version().0;
                    conn.send_or_queue_response(STATUS_OK, ver, raw_frame)
                }
                Err(StorageError::RecordNotFound { .. }) => {
                    conn.send_or_queue_response(STATUS_NOT_FOUND, 0, &[])
                }
                Err(StorageError::InvalidArchiveLayout { .. }) => {
                    match engine.get_owned(id) {
                        Ok((ver, owned)) => {
                            let frame =
                                RecordCodec::encode_aligned(&owned, ver, 0, RecordFlags::NONE, 0);
                            conn.send_or_queue_response(STATUS_OK, ver.0, frame.as_slice())
                        }
                        Err(StorageError::RecordNotFound { .. }) => {
                            conn.send_or_queue_response(STATUS_NOT_FOUND, 0, &[])
                        }
                        Err(e) => {
                            conn.send_or_queue_response(STATUS_ERROR, 0, e.to_string().as_bytes())
                        }
                    }
                }
                Err(e) => conn.send_or_queue_response(STATUS_ERROR, 0, e.to_string().as_bytes()),
            }
        }
        OP_PUT_RAW_FRAME => {
            let body = core::mem::take(&mut conn.body_buf);
            let Ok(id) = InlineId::from_raw_bytes(raw_id) else {
                let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"invalid InlineId");
                return ConnStep::Progress;
            };
            let aligned = AlignedBytes::copy_from_slice(&body);
            match RecordCodec::decode_owned::<T>(aligned.as_slice()) {
                Ok((_, owned)) => match engine.put(id, &owned) {
                    Ok(ver) => conn.send_or_queue_response(STATUS_OK, ver.0, &[]),
                    Err(e) => {
                        conn.send_or_queue_response(STATUS_ERROR, 0, e.to_string().as_bytes())
                    }
                },
                Err(e) => conn.send_or_queue_response(STATUS_ERROR, 0, e.to_string().as_bytes()),
            }
        }
        OP_DELETE => {
            let Ok(id) = InlineId::from_raw_bytes(raw_id) else {
                let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"invalid InlineId");
                return ConnStep::Progress;
            };
            match engine.delete(id) {
                Ok(true) => conn.send_or_queue_response(STATUS_OK, 1, &[]),
                Ok(false) => conn.send_or_queue_response(STATUS_NOT_FOUND, 0, &[]),
                Err(e) => conn.send_or_queue_response(STATUS_ERROR, 0, e.to_string().as_bytes()),
            }
        }
        OP_STATS => {
            let st = engine.stats();
            let mut payload = [0u8; 24];
            payload[0..8].copy_from_slice(&(st.live_records as u64).to_le_bytes());
            payload[8..16].copy_from_slice(&st.total_reads.to_le_bytes());
            payload[16..24].copy_from_slice(&st.total_writes.to_le_bytes());
            conn.send_or_queue_response(STATUS_OK, st.live_records as u64, &payload)
        }
        _ => conn.send_or_queue_response(STATUS_ERROR, 0, b"unknown opcode"),
    };

    if write_res.is_err() {
        ConnStep::Closed
    } else {
        ConnStep::Progress
    }
}

/// High-speed TCP Client for `StorageServer<T>`.
#[derive(Debug)]
pub struct StorageClient {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl StorageClient {
    pub fn connect(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .map_err(|e| StorageError::io("connect StorageClient", e))?;
        let _ = stream.set_nodelay(true);
        let reader_stream = stream
            .try_clone()
            .map_err(|e| StorageError::io("clone client TcpStream", e))?;
        Ok(Self {
            reader: BufReader::with_capacity(64 * 1024, reader_stream),
            writer: BufWriter::with_capacity(64 * 1024, stream),
        })
    }

    /// Sends a record to the server encoded in zero-copy archive format.
    pub fn put<T: ZeroCopyRecord>(&mut self, id: InlineId, record: &T) -> Result<Version> {
        let frame = RecordCodec::encode_aligned(record, Version::INITIAL, 0, RecordFlags::NONE, 0);
        let (status, ver, body) = self.rpc(OP_PUT_RAW_FRAME, id, frame.as_slice())?;
        match status {
            STATUS_OK => Ok(Version(ver)),
            STATUS_OVERLOADED => Err(StorageError::io(
                "server overloaded during PUT",
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    String::from_utf8_lossy(&body).into_owned(),
                ),
            )),
            _ => Err(StorageError::corrupted_header("server rejected PUT request")),
        }
    }

    /// Fetches a 64-byte-aligned `AlignedBytes` frame from the server that can be inspected
    /// zero-copy via `RecordCodec::decode_verified::<T>(&bytes)`.
    pub fn get_frame(&mut self, id: InlineId) -> Result<AlignedBytes> {
        let (status, _, body) = self.rpc(OP_GET_RAW_FRAME, id, &[])?;
        match status {
            STATUS_OK => Ok(AlignedBytes::copy_from_slice(&body)),
            STATUS_NOT_FOUND => Err(StorageError::not_found(id)),
            STATUS_OVERLOADED => Err(StorageError::io(
                "server overloaded during GET",
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    String::from_utf8_lossy(&body).into_owned(),
                ),
            )),
            _ => Err(StorageError::corrupted_header("server GET returned error")),
        }
    }

    /// Deletes `id` on the remote server.
    pub fn delete(&mut self, id: InlineId) -> Result<bool> {
        let (status, _, body) = self.rpc(OP_DELETE, id, &[])?;
        match status {
            STATUS_OK => Ok(true),
            STATUS_NOT_FOUND => Ok(false),
            STATUS_OVERLOADED => Err(StorageError::io(
                "server overloaded during DELETE",
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    String::from_utf8_lossy(&body).into_owned(),
                ),
            )),
            _ => Err(StorageError::corrupted_header("server DELETE returned error")),
        }
    }

    fn rpc(&mut self, op: u8, id: InlineId, body: &[u8]) -> Result<(u8, u64, Vec<u8>)> {
        let mut req_hdr = [0u8; 29];
        req_hdr[0] = op;
        req_hdr[1..25].copy_from_slice(&id.to_raw_bytes());
        req_hdr[25..29].copy_from_slice(&(body.len() as u32).to_le_bytes());

        self.writer
            .write_all(&req_hdr)
            .map_err(|e| StorageError::io("write RPC request header", e))?;
        if !body.is_empty() {
            self.writer
                .write_all(body)
                .map_err(|e| StorageError::io("write RPC request body", e))?;
        }
        self.writer
            .flush()
            .map_err(|e| StorageError::io("flush RPC request", e))?;

        let mut resp_hdr = [0u8; 13];
        self.reader
            .read_exact(&mut resp_hdr)
            .map_err(|e| StorageError::io("read RPC response header", e))?;
        let status = resp_hdr[0];
        let version = u64::from_le_bytes(resp_hdr[1..9].try_into().unwrap());
        let resp_len = u32::from_le_bytes(resp_hdr[9..13].try_into().unwrap()) as usize;
        if resp_len > MAX_PAYLOAD_SIZE as usize {
            return Err(StorageError::corrupted_header(
                "RPC response body exceeds MAX_PAYLOAD_SIZE",
            ));
        }

        let mut resp_body = vec![0u8; resp_len];
        if resp_len > 0 {
            self.reader
                .read_exact(&mut resp_body)
                .map_err(|e| StorageError::io("read RPC response body", e))?;
        }
        Ok((status, version, resp_body))
    }
}

/// Convenience helper for decoding a `RecordView<'_, AgentObjectRecord>` from a fetched frame.
#[inline]
pub fn decode_agent_frame(frame: &AlignedBytes) -> Result<RecordView<'_, AgentObjectRecord>> {
    RecordCodec::decode_verified::<AgentObjectRecord>(frame.as_slice())
}

/// Suppresses unused import warning when `RecordRef` is only referenced in doc comments.
pub type DefaultServerRecordRef = RecordRef<AgentObjectRecord>;

#[cfg(test)]
mod tests {
    use super::*;
    use storage_core::EngineConfig;

    #[test]
    fn test_tcp_server_zero_copy_frame_streaming_roundtrip() {
        let dir = std::env::temp_dir().join("stge_test_tcp_server_e2e");
        let _ = std::fs::remove_dir_all(&dir);

        let engine = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(&dir, EngineConfig::default()).unwrap(),
        );
        let mut server = StorageServer::start("127.0.0.1:0", Arc::clone(&engine)).unwrap();
        let addr = server.local_addr();

        let mut client = StorageClient::connect(addr).unwrap();
        let id = InlineId::from_static("net_agent_01");
        let rec = AgentObjectRecord::new(id, "Network ZeroCopy Agent", 1_700_000_000, 777);

        let ver = client.put(id, &rec).unwrap();
        assert_eq!(ver, Version(1));

        let frame = client.get_frame(id).unwrap();
        let view = decode_agent_frame(&frame).unwrap();
        assert_eq!(view.get_str(&view.title), "Network ZeroCopy Agent");
        assert_eq!(view.score, 777);

        assert!(client.delete(id).unwrap());
        assert!(client.get_frame(id).is_err());

        server.stop();
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_tcp_server_connection_admission_control_and_overload_shedding() {
        let dir = std::env::temp_dir().join("stge_test_tcp_server_overload");
        let _ = std::fs::remove_dir_all(&dir);

        let engine = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(&dir, EngineConfig::default()).unwrap(),
        );
        // Configure server with max_connections = 2
        let mut server =
            StorageServer::start_with_max_connections("127.0.0.1:0", Arc::clone(&engine), 2)
                .unwrap();
        let addr = server.local_addr();

        let mut c1 = StorageClient::connect(addr).unwrap();
        let mut c2 = StorageClient::connect(addr).unwrap();
        let id1 = InlineId::from_static("overload_test_1");
        let rec1 = AgentObjectRecord::new(id1, "Conn 1 Record", 100, 10);
        assert_eq!(c1.put(id1, &rec1).unwrap(), Version(1));
        assert_eq!(c2.get_frame(id1).unwrap().as_slice().len(), 192);

        // 3rd simultaneous connection exceeds max_connections (2) and is immediately shed
        let mut c3 = StorageClient::connect(addr).unwrap();
        let res3 = c3.get_frame(id1);
        assert!(res3.is_err(), "3rd connection must be shed when max_connections=2 is full");
        assert!(server.rejected_connections() >= 1);

        // Existing connections c1 and c2 remain healthy and unaffected
        assert_eq!(c1.get_frame(id1).unwrap().as_slice().len(), 192);

        // Drop c2 so active_connections drops from 2 -> 1
        drop(c2);
        for _ in 0..50 {
            if server.active_connections() < 2 {
                break;
            }
            thread::sleep(Duration::from_millis(2));
        }

        // Now a new connection c4 succeeds!
        let mut c4 = StorageClient::connect(addr).unwrap();
        assert_eq!(c4.get_frame(id1).unwrap().as_slice().len(), 192);

        server.stop();
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
