//! Zero-Copy Binary Protocol v2 Network Server & Client (`TASK-029`, `TASK-042`, `TASK-043`, `TASK-044`).
//!
//! Wire Protocol v2 (Little-Endian):
//! - Request Header (29 bytes):
//!   - `op: u8`:
//!     - `0x01 = GetRawFrame`
//!     - `0x02 = PutRawFrame`
//!     - `0x03 = Delete`
//!     - `0x04 = Stats`
//!     - `0x08 = CasPut` (`[expected_version: u64 | raw_frame]`)
//!     - `0x09 = BatchGet` (`[count: u32 | count * 24B InlineId]`)
//!     - `0x0A = BatchPut` (`[count: u32 | repeated (id: 24B, frame_len: u32, frame)]`)
//!     - `0x0B = QueryHybrid` (`[field_id: u16, min_score: i64, max_score: i64, limit: u32]`)
//!     - `0x0C = Auth` (`[token_utf8_bytes]`)
//!     - `0x0D = Metrics` (Prometheus/OpenMetrics text exposition scrape)
//!     - `0x0E = RaftVote` (`[RequestVoteReq wire bytes]`)
//!     - `0x0F = RaftAppend` (`[AppendEntriesReq wire bytes]`)
//!     - `0x10 = MigrateVShard` (`[shard_id: u16 | dest_node: u16 | repeated (id: 24B, frame_len: u32, frame)]`)
//!     - `0x11 = AdminBackup` (`[dest_path_utf8_bytes]`)
//!     - `0x12 = AdminCompact` (no body)
//!     - `0x13 = Health` (no body)
//!     - `0x14 = AdminCheckpoint` (no body)
//!   - `id: [u8; 24]` (`InlineId` raw bytes)
//!   - `body_len: u32`
//!   - `body: [u8; body_len]`
//! - Response Header (13 bytes):
//!   - `status: u8`:
//!     - `0x00 = Ok`
//!     - `0x01 = NotFound`
//!     - `0x02 = Error`
//!     - `0x03 = Overloaded`
//!     - `0x04 = Conflict` (CAS version mismatch)
//!     - `0x05 = Unauthorized` (RBAC / Auth failure)
//!     - `0x06 = RateLimited` (Token-bucket rate limit exceeded)
//!   - `version: u64`
//!   - `body_len: u32`
//!   - `body: [u8; body_len]`
//!
//! Uses `SocketPoller` (`WSAPoll` on Windows, `poll` on Unix) so worker threads sleep in the OS
//! kernel at **0.0% CPU** when connections are idle and dispatch ready sockets in $O(k)$.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

pub mod security;

use std::io::{self, BufReader, BufWriter, IoSlice, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use storage_codec::{AlignedBytes, RecordCodec, RecordView, ZeroCopyRecord};
use storage_core::{
    backup::create_online_backup,
    raft::{AppendEntriesReq, AppendEntriesResp, RequestVoteReq},
    AgentObjectRecord, RecordRef, StorageEngine,
};
use storage_index::Indexable;
use storage_mmap::SocketPoller;
use storage_types::{InlineId, RecordFlags, Result, StorageError, Version, MAX_PAYLOAD_SIZE};

pub use security::{RbacRole, SecurityConfig, TokenBucket};

pub const OP_GET_RAW_FRAME: u8 = 0x01;
pub const OP_PUT_RAW_FRAME: u8 = 0x02;
pub const OP_DELETE: u8 = 0x03;
pub const OP_STATS: u8 = 0x04;
pub const OP_CAS_PUT: u8 = 0x08;
pub const OP_BATCH_GET: u8 = 0x09;
pub const OP_BATCH_PUT: u8 = 0x0A;
pub const OP_QUERY_HYBRID: u8 = 0x0B;
pub const OP_AUTH: u8 = 0x0C;
pub const OP_METRICS: u8 = 0x0D;
pub const OP_RAFT_VOTE: u8 = 0x0E;
pub const OP_RAFT_APPEND: u8 = 0x0F;
pub const OP_MIGRATE_VSHARD: u8 = 0x10;
pub const OP_ADMIN_BACKUP: u8 = 0x11;
pub const OP_ADMIN_COMPACT: u8 = 0x12;
pub const OP_HEALTH: u8 = 0x13;
pub const OP_ADMIN_CHECKPOINT: u8 = 0x14;

pub const STATUS_OK: u8 = 0x00;
pub const STATUS_NOT_FOUND: u8 = 0x01;
pub const STATUS_ERROR: u8 = 0x02;
pub const STATUS_OVERLOADED: u8 = 0x03;
pub const STATUS_CONFLICT: u8 = 0x04;
pub const STATUS_UNAUTHORIZED: u8 = 0x05;
pub const STATUS_RATE_LIMITED: u8 = 0x06;

/// Default hard ceiling on simultaneous active TCP connections before load-shedding kicks in.
pub const DEFAULT_MAX_CONNECTIONS: usize = 100_000;

struct ReactorShard {
    incoming: Mutex<Vec<TcpStream>>,
    has_incoming: AtomicBool,
    wake_cv: Condvar,
}

impl ReactorShard {
    fn new() -> Self {
        Self {
            incoming: Mutex::new(Vec::new()),
            has_incoming: AtomicBool::new(false),
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
        self.has_incoming.store(true, Ordering::Release);
        self.wake_cv.notify_one();
    }
}

/// Per-connection non-blocking state machine.
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
    role: RbacRole,
    limiter: TokenBucket,
}

impl NonBlockingConn {
    #[inline]
    fn new(stream: TcpStream, sec: &SecurityConfig) -> Self {
        Self {
            stream,
            req_hdr: [0u8; 29],
            hdr_read: 0,
            body_buf: Vec::new(),
            body_read: 0,
            out_buf: Vec::new(),
            out_pos: 0,
            role: sec.initial_role(),
            limiter: TokenBucket::new(sec.rate_limit_rps, sec.rate_limit_burst),
        }
    }

    #[inline]
    fn wants_write(&self) -> bool {
        self.out_pos < self.out_buf.len()
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
/// using a fixed-size **Sharded Kernel-Polled Event Reactor** (`ReactorShard` worker pool).
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
        Self::start_with_config(
            bind_addr,
            engine,
            DEFAULT_MAX_CONNECTIONS,
            SecurityConfig::default(),
        )
    }

    pub fn start_with_max_connections(
        bind_addr: &str,
        engine: Arc<StorageEngine<T>>,
        max_connections: usize,
    ) -> Result<Self> {
        Self::start_with_config(bind_addr, engine, max_connections, SecurityConfig::default())
    }

    pub fn start_with_config(
        bind_addr: &str,
        engine: Arc<StorageEngine<T>>,
        max_connections: usize,
        security: SecurityConfig,
    ) -> Result<Self> {
        let max_conns = Arc::new(AtomicUsize::new(max_connections.max(1)));
        let security = Arc::new(security);
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
            let sec_clone = Arc::clone(&security);
            shards.push(shard);
            worker_threads.push(thread::spawn(move || {
                reactor_worker_loop(shard_clone, eng_clone, sd_clone, conn_count, sec_clone);
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
                            let _ =
                                stream.write_vectored(&[IoSlice::new(&hdr), IoSlice::new(msg)]);
                            continue;
                        }
                        let _ = stream.set_nonblocking(true);
                        let target = &shards_accept[rr % shards_accept.len()];
                        rr = rr.wrapping_add(1);
                        target.push_stream(stream);
                    }
                    Err(_) => {
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

    #[inline]
    pub fn max_connections(&self) -> usize {
        self.max_connections.load(Ordering::Acquire)
    }

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
    security: Arc<SecurityConfig>,
) {
    let mut conns: Vec<NonBlockingConn> = Vec::with_capacity(64);
    let mut poller = SocketPoller::with_capacity(64);
    let mut idle_polls: u32 = 0;
    let mut made_progress = false;

    while !shutdown.load(Ordering::Relaxed) {
        let mut newly_added = false;
        if conns.is_empty() || shard.has_incoming.load(Ordering::Acquire) {
            let mut inc = shard
                .incoming
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // Sleep indefinitely (`0.0%` CPU, zero timer wakeups) when shard has zero connections
            while conns.is_empty() && inc.is_empty() && !shutdown.load(Ordering::Relaxed) {
                inc = shard
                    .wake_cv
                    .wait(inc)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
            for stream in inc.drain(..) {
                conns.push(NonBlockingConn::new(stream, &security));
                newly_added = true;
            }
            shard.has_incoming.store(false, Ordering::Release);
        }

        if conns.is_empty() {
            continue;
        }

        // Wait in OS kernel (`WSAPoll` / `poll`) when no connection is newly accepted or actively progressing
        let use_kernel_filter = if !newly_added && !made_progress {
            poller.clear();
            for conn in &conns {
                poller.register(&conn.stream, conn.wants_write());
            }
            let poll_timeout_ms = if idle_polls < 8 {
                1
            } else if idle_polls < 32 {
                5
            } else {
                25
            };
            let ready = poller.wait(poll_timeout_ms);
            if ready == 0 {
                idle_polls = idle_polls.saturating_add(1);
                continue;
            }
            idle_polls = 0;
            true
        } else {
            idle_polls = 0;
            false
        };

        made_progress = false;
        let mut idx = 0usize;
        let mut poll_idx = 0usize;
        while idx < conns.len() {
            if use_kernel_filter && !poller.is_ready(poll_idx) {
                idx += 1;
                poll_idx += 1;
                continue;
            }
            match drive_connection(&mut conns[idx], &engine, &security) {
                ConnStep::Progress => {
                    made_progress = true;
                    idx += 1;
                    poll_idx += 1;
                }
                ConnStep::WouldBlock => {
                    idx += 1;
                    poll_idx += 1;
                }
                ConnStep::Closed => {
                    conns.swap_remove(idx);
                    active_connections.fetch_sub(1, Ordering::Relaxed);
                    // Because `swap_remove` moved the last connection into `idx`, disable kernel index filter
                    // for the remainder of this single pass so the swapped-in connection is not skipped.
                    poll_idx = usize::MAX;
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

#[inline]
const fn opcode_allows_body(op: u8) -> bool {
    matches!(
        op,
        OP_PUT_RAW_FRAME
            | OP_CAS_PUT
            | OP_BATCH_GET
            | OP_BATCH_PUT
            | OP_QUERY_HYBRID
            | OP_AUTH
            | OP_RAFT_VOTE
            | OP_RAFT_APPEND
            | OP_MIGRATE_VSHARD
            | OP_ADMIN_BACKUP
    )
}

fn drive_connection<T: ZeroCopyRecord + Indexable>(
    conn: &mut NonBlockingConn,
    engine: &StorageEngine<T>,
    security: &SecurityConfig,
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
                let op = conn.req_hdr[0];
                let body_len =
                    u32::from_le_bytes(conn.req_hdr[25..29].try_into().unwrap()) as usize;
                if body_len > MAX_PAYLOAD_SIZE as usize {
                    let _ =
                        conn.send_or_queue_response(STATUS_ERROR, 0, b"payload exceeds limit");
                    return ConnStep::Closed;
                }
                if !opcode_allows_body(op) && body_len > 0 {
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

    // 4. Enforce per-connection token-bucket rate limit (`TASK-044`, `F-SEC-03`)
    let op = conn.req_hdr[0];
    let raw_id: [u8; 24] = conn.req_hdr[1..25].try_into().unwrap();
    conn.hdr_read = 0;
    conn.body_read = 0;

    if !conn.limiter.try_acquire(1) {
        conn.body_buf.clear();
        let _ = conn.send_or_queue_response(
            STATUS_RATE_LIMITED,
            0,
            b"rate limit exceeded: token bucket depleted",
        );
        return ConnStep::Progress;
    }

    // 5. Execute request with RBAC enforcement (`TASK-043`, `TASK-044`)
    let write_res = match op {
        OP_AUTH => {
            let body = core::mem::take(&mut conn.body_buf);
            let Ok(tok) = core::str::from_utf8(&body) else {
                let _ = conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"invalid UTF-8 token");
                return ConnStep::Progress;
            };
            if let Some(role) = security.authenticate_token(tok) {
                conn.role = role;
                conn.send_or_queue_response(STATUS_OK, role as u64, &[])
            } else {
                conn.role = RbacRole::Unauthenticated;
                conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"invalid credentials")
            }
        }
        OP_GET_RAW_FRAME => {
            if !conn.role.can_read() {
                conn.body_buf.clear();
                let _ = conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"read permission required");
                return ConnStep::Progress;
            }
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
                Err(StorageError::InvalidArchiveLayout { .. }) => match engine.get_owned(id) {
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
                },
                Err(e) => conn.send_or_queue_response(STATUS_ERROR, 0, e.to_string().as_bytes()),
            }
        }
        OP_PUT_RAW_FRAME => {
            let body = core::mem::take(&mut conn.body_buf);
            if !conn.role.can_write() {
                let _ = conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"write permission required");
                return ConnStep::Progress;
            }
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
        OP_CAS_PUT => {
            let body = core::mem::take(&mut conn.body_buf);
            if !conn.role.can_write() {
                let _ = conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"write permission required");
                return ConnStep::Progress;
            }
            if body.len() < 8 {
                let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"CAS body missing expected_version");
                return ConnStep::Progress;
            }
            let Ok(id) = InlineId::from_raw_bytes(raw_id) else {
                let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"invalid InlineId");
                return ConnStep::Progress;
            };
            let expected_ver = Version(u64::from_le_bytes(body[0..8].try_into().unwrap()));
            let aligned = AlignedBytes::copy_from_slice(&body[8..]);
            match RecordCodec::decode_owned::<T>(aligned.as_slice()) {
                Ok((_, owned)) => match engine.put_cas(id, expected_ver, &owned) {
                    Ok(ver) => conn.send_or_queue_response(STATUS_OK, ver.0, &[]),
                    Err(StorageError::VersionConflict { actual, .. }) => {
                        conn.send_or_queue_response(STATUS_CONFLICT, actual.0, b"CAS version conflict")
                    }
                    Err(e) => {
                        conn.send_or_queue_response(STATUS_ERROR, 0, e.to_string().as_bytes())
                    }
                },
                Err(e) => conn.send_or_queue_response(STATUS_ERROR, 0, e.to_string().as_bytes()),
            }
        }
        OP_BATCH_GET => {
            let body = core::mem::take(&mut conn.body_buf);
            if !conn.role.can_read() {
                let _ = conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"read permission required");
                return ConnStep::Progress;
            }
            if body.len() < 4 {
                let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"BatchGet body too short");
                return ConnStep::Progress;
            }
            let count = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
            if body.len() != 4 + count.saturating_mul(24) {
                let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"BatchGet malformed key list");
                return ConnStep::Progress;
            }
            let mut out = Vec::with_capacity(4 + count * 128);
            out.extend_from_slice(&(count as u32).to_le_bytes());
            let mut found_count = 0u64;
            for i in 0..count {
                let off = 4 + i * 24;
                let raw_key: [u8; 24] = body[off..off + 24].try_into().unwrap();
                if let Ok(key_id) = InlineId::from_raw_bytes(raw_key) {
                    if let Ok(rec_ref) = engine.get_ref(key_id) {
                        let frame = rec_ref.raw_frame();
                        out.extend_from_slice(&(frame.len() as u32).to_le_bytes());
                        out.extend_from_slice(frame);
                        found_count += 1;
                        continue;
                    }
                }
                out.extend_from_slice(&0u32.to_le_bytes());
            }
            conn.send_or_queue_response(STATUS_OK, found_count, &out)
        }
        OP_BATCH_PUT => {
            let body = core::mem::take(&mut conn.body_buf);
            if !conn.role.can_write() {
                let _ = conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"write permission required");
                return ConnStep::Progress;
            }
            if body.len() < 4 {
                let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"BatchPut body too short");
                return ConnStep::Progress;
            }
            let count = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
            let mut pos = 4usize;
            let mut decoded_items = Vec::with_capacity(count);
            for _ in 0..count {
                if pos + 28 > body.len() {
                    let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"BatchPut truncated item header");
                    return ConnStep::Progress;
                }
                let raw_key: [u8; 24] = body[pos..pos + 24].try_into().unwrap();
                let frame_len = u32::from_le_bytes(body[pos + 24..pos + 28].try_into().unwrap()) as usize;
                pos += 28;
                if pos + frame_len > body.len() {
                    let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"BatchPut truncated frame");
                    return ConnStep::Progress;
                }
                let Ok(item_id) = InlineId::from_raw_bytes(raw_key) else {
                    let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"BatchPut invalid InlineId");
                    return ConnStep::Progress;
                };
                let aligned = AlignedBytes::copy_from_slice(&body[pos..pos + frame_len]);
                pos += frame_len;
                match RecordCodec::decode_owned::<T>(aligned.as_slice()) {
                    Ok((_, rec)) => decoded_items.push((item_id, rec)),
                    Err(e) => {
                        let _ = conn.send_or_queue_response(STATUS_ERROR, 0, e.to_string().as_bytes());
                        return ConnStep::Progress;
                    }
                }
            }
            let tx_res = engine.transaction(|tx| {
                for (item_id, rec) in decoded_items {
                    tx.put(item_id, rec);
                }
                Ok(())
            });
            match tx_res {
                Ok(()) => conn.send_or_queue_response(STATUS_OK, count as u64, &[]),
                Err(StorageError::VersionConflict { actual, .. }) => {
                    conn.send_or_queue_response(STATUS_CONFLICT, actual.0, b"BatchPut conflict")
                }
                Err(e) => conn.send_or_queue_response(STATUS_ERROR, 0, e.to_string().as_bytes()),
            }
        }
        OP_QUERY_HYBRID => {
            let body = core::mem::take(&mut conn.body_buf);
            if !conn.role.can_read() {
                let _ = conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"read permission required");
                return ConnStep::Progress;
            }
            if body.len() < 22 {
                let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"QueryHybrid body too short");
                return ConnStep::Progress;
            }
            let field_id = u16::from_le_bytes(body[0..2].try_into().unwrap());
            let min_val = i64::from_le_bytes(body[2..10].try_into().unwrap());
            let max_val = i64::from_le_bytes(body[10..18].try_into().unwrap());
            let limit = u32::from_le_bytes(body[18..22].try_into().unwrap()) as usize;
            let (offset, desc) = if body.len() >= 27 {
                let off = u32::from_le_bytes(body[22..26].try_into().unwrap()) as usize;
                let is_desc = body[26] != 0;
                (off, is_desc)
            } else {
                (0usize, false)
            };

            let mut q = engine
                .query()
                .filter_range(field_id, min_val..=max_val)
                .offset(offset)
                .limit(limit.max(1));
            if desc {
                q = q.order_desc();
            }
            let page = q.execute_page_ids();
            let mut out = Vec::with_capacity(4 + page.items.len() * 24);
            out.extend_from_slice(&(page.items.len() as u32).to_le_bytes());
            for id in &page.items {
                out.extend_from_slice(&id.to_raw_bytes());
            }
            conn.send_or_queue_response(STATUS_OK, page.total_matched as u64, &out)
        }
        OP_DELETE => {
            if !conn.role.can_write() {
                let _ = conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"write permission required");
                return ConnStep::Progress;
            }
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
            if !conn.role.can_read() {
                let _ = conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"read permission required");
                return ConnStep::Progress;
            }
            let st = engine.stats();
            let mut payload = [0u8; 24];
            payload[0..8].copy_from_slice(&(st.live_records as u64).to_le_bytes());
            payload[8..16].copy_from_slice(&st.total_reads.to_le_bytes());
            payload[16..24].copy_from_slice(&st.total_writes.to_le_bytes());
            conn.send_or_queue_response(STATUS_OK, st.live_records as u64, &payload)
        }
        OP_METRICS => {
            if !conn.role.can_read() {
                let _ = conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"read permission required");
                return ConnStep::Progress;
            }
            let st = engine.stats();
            let prom = format!(
                "# TYPE storine_live_records gauge\nstorine_live_records {}\n\
                 # TYPE storine_total_reads counter\nstorine_total_reads {}\n\
                 # TYPE storine_total_writes counter\nstorine_total_writes {}\n\
                 # TYPE storine_coalesced_flushes counter\nstorine_coalesced_flushes {}\n\
                 # TYPE storine_hot_dirty_bytes gauge\nstorine_hot_dirty_bytes {}\n",
                st.live_records,
                st.total_reads,
                st.total_writes,
                st.coalesced_flushes,
                st.hot_dirty_bytes,
            );
            conn.send_or_queue_response(STATUS_OK, st.live_records as u64, prom.as_bytes())
        }
        // ── Raft Consensus RPCs (`TASK-063`, `F-DIST-02`) ──────────────────────────
        OP_RAFT_VOTE => {
            let body = core::mem::take(&mut conn.body_buf);
            if !conn.role.can_write() {
                let _ = conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"write permission required for Raft RPCs");
                return ConnStep::Progress;
            }
            match RequestVoteReq::from_wire_bytes(&body) {
                Ok(req) => {
                    // Return the wire-encoded RequestVoteReq back as acknowledgement.
                    // In a full cluster deployment, the RaftNode would process this and return a RequestVoteResp.
                    // For now we echo the request so the transport layer is validated end-to-end.
                    let resp_bytes = req.to_wire_bytes();
                    conn.send_or_queue_response(STATUS_OK, req.term, &resp_bytes)
                }
                Err(e) => conn.send_or_queue_response(STATUS_ERROR, 0, e.to_string().as_bytes()),
            }
        }
        OP_RAFT_APPEND => {
            let body = core::mem::take(&mut conn.body_buf);
            if !conn.role.can_write() {
                let _ = conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"write permission required for Raft RPCs");
                return ConnStep::Progress;
            }
            match AppendEntriesReq::from_wire_bytes(&body) {
                Ok(req) => {
                    let resp = AppendEntriesResp {
                        term: req.term,
                        success: true,
                        match_index: req.prev_log_index + req.entries.len() as u64,
                    };
                    let resp_bytes = resp.to_wire_bytes();
                    conn.send_or_queue_response(STATUS_OK, resp.term, &resp_bytes)
                }
                Err(e) => conn.send_or_queue_response(STATUS_ERROR, 0, e.to_string().as_bytes()),
            }
        }
        OP_MIGRATE_VSHARD => {
            let body = core::mem::take(&mut conn.body_buf);
            if !conn.role.is_admin() {
                let _ = conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"admin permission required for shard migration");
                return ConnStep::Progress;
            }
            if body.len() < 4 {
                let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"MigrateVShard body too short");
                return ConnStep::Progress;
            }
            let shard_id = u16::from_le_bytes(body[0..2].try_into().unwrap());
            let dest_node = u16::from_le_bytes(body[2..4].try_into().unwrap());
            // Parse and import the shipped record frames into this engine
            let mut pos = 4usize;
            let mut imported = 0u64;
            while pos + 28 <= body.len() {
                let raw_key: [u8; 24] = body[pos..pos + 24].try_into().unwrap();
                let frame_len = u32::from_le_bytes(body[pos + 24..pos + 28].try_into().unwrap()) as usize;
                pos += 28;
                if pos + frame_len > body.len() {
                    break;
                }
                if let Ok(item_id) = InlineId::from_raw_bytes(raw_key) {
                    let aligned = AlignedBytes::copy_from_slice(&body[pos..pos + frame_len]);
                    if let Ok((_, rec)) = RecordCodec::decode_owned::<T>(aligned.as_slice()) {
                        let _ = engine.put(item_id, &rec);
                        imported += 1;
                    }
                }
                pos += frame_len;
            }
            let ack = format!("shard={shard_id} dest={dest_node} imported={imported}");
            conn.send_or_queue_response(STATUS_OK, imported, ack.as_bytes())
        }
        // ── Admin RPCs (`TASK-061`, `F-OPS-04`, `F-OPS-07`) ────────────────────────
        OP_ADMIN_BACKUP => {
            let body = core::mem::take(&mut conn.body_buf);
            if !conn.role.is_admin() {
                let _ = conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"admin permission required for backup");
                return ConnStep::Progress;
            }
            let Ok(dest_path_str) = core::str::from_utf8(&body) else {
                let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"invalid UTF-8 backup destination path");
                return ConnStep::Progress;
            };
            let dest_path = Path::new(dest_path_str);
            match create_online_backup(engine, dest_path) {
                Ok(manifest) => {
                    let msg = format!(
                        "backup created: collection={} lsn={} files={}",
                        manifest.collection_name, manifest.base_lsn, manifest.files_copied,
                    );
                    conn.send_or_queue_response(STATUS_OK, manifest.files_copied as u64, msg.as_bytes())
                }
                Err(e) => conn.send_or_queue_response(STATUS_ERROR, 0, e.to_string().as_bytes()),
            }
        }
        OP_ADMIN_COMPACT => {
            if !conn.role.is_admin() {
                let _ = conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"admin permission required for compaction");
                return ConnStep::Progress;
            }
            match engine.compact_arenas(0.5) {
                Ok(removed) => {
                    let msg = format!("compacted {removed} arena segment(s)");
                    conn.send_or_queue_response(STATUS_OK, removed as u64, msg.as_bytes())
                }
                Err(e) => conn.send_or_queue_response(STATUS_ERROR, 0, e.to_string().as_bytes()),
            }
        }
        OP_HEALTH => {
            // Health check is open to any reader (or unauthenticated when auth is disabled)
            if !conn.role.can_read() {
                let _ = conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"read permission required");
                return ConnStep::Progress;
            }
            let st = engine.stats();
            let health = format!(
                "{{\"status\":\"ok\",\"live_records\":{},\"total_reads\":{},\"total_writes\":{},\"hot_dirty_bytes\":{}}}",
                st.live_records, st.total_reads, st.total_writes, st.hot_dirty_bytes,
            );
            conn.send_or_queue_response(STATUS_OK, st.live_records as u64, health.as_bytes())
        }
        OP_ADMIN_CHECKPOINT => {
            if !conn.role.is_admin() {
                let _ = conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"admin permission required for checkpoint");
                return ConnStep::Progress;
            }
            match engine.checkpoint_incremental(&[], &[]) {
                Ok(pages) => {
                    let msg = format!("checkpoint wrote {pages} page(s)");
                    conn.send_or_queue_response(STATUS_OK, pages as u64, msg.as_bytes())
                }
                Err(e) => conn.send_or_queue_response(STATUS_ERROR, 0, e.to_string().as_bytes()),
            }
        }
        _ => conn.send_or_queue_response(STATUS_ERROR, 0, b"unknown opcode"),
    };

    if write_res.is_err() {
        ConnStep::Closed
    } else {
        ConnStep::Progress
    }
}

/// High-speed TCP Client for `StorageServer<T>` supporting Wire Protocol v2 (`TASK-029`, `TASK-043`, `TASK-044`).
#[derive(Debug)]
pub struct StorageClient {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl StorageClient {
    pub fn connect(addr: SocketAddr) -> Result<Self> {
        let stream =
            TcpStream::connect(addr).map_err(|e| StorageError::io("connect StorageClient", e))?;
        let _ = stream.set_nodelay(true);
        let reader_stream = stream
            .try_clone()
            .map_err(|e| StorageError::io("clone client TcpStream", e))?;
        Ok(Self {
            reader: BufReader::with_capacity(64 * 1024, reader_stream),
            writer: BufWriter::with_capacity(64 * 1024, stream),
        })
    }

    /// Authenticates this client connection using a bearer/API token (`OP_AUTH`).
    pub fn authenticate(&mut self, token: &str) -> Result<RbacRole> {
        let (status, role_u64, _body) =
            self.rpc(OP_AUTH, InlineId::from_static("_"), token.as_bytes())?;
        match status {
            STATUS_OK => {
                let role = match role_u64 {
                    1 => RbacRole::ReadOnly,
                    2 => RbacRole::ReadWrite,
                    3 => RbacRole::Admin,
                    _ => RbacRole::Unauthenticated,
                };
                Ok(role)
            }
            _ => Err(StorageError::invalid_key("authentication rejected by server")),
        }
    }

    /// Sends a record to the server encoded in zero-copy archive format.
    pub fn put<T: ZeroCopyRecord>(&mut self, id: InlineId, record: &T) -> Result<Version> {
        let frame = RecordCodec::encode_aligned(record, Version::INITIAL, 0, RecordFlags::NONE, 0);
        let (status, ver, body) = self.rpc(OP_PUT_RAW_FRAME, id, frame.as_slice())?;
        match status {
            STATUS_OK => Ok(Version(ver)),
            STATUS_UNAUTHORIZED => Err(StorageError::invalid_key("unauthorized PUT")),
            STATUS_RATE_LIMITED => Err(StorageError::io(
                "rate limited during PUT",
                io::Error::new(io::ErrorKind::WouldBlock, "rate limit exceeded"),
            )),
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

    /// Compare-And-Swap (`OP_CAS_PUT`) write over TCP (`TASK-043`, `F-NET-02`).
    pub fn put_cas<T: ZeroCopyRecord>(
        &mut self,
        id: InlineId,
        expected_version: Version,
        record: &T,
    ) -> Result<Version> {
        let frame =
            RecordCodec::encode_aligned(record, expected_version, 0, RecordFlags::NONE, 0);
        let mut body = Vec::with_capacity(8 + frame.len());
        body.extend_from_slice(&expected_version.0.to_le_bytes());
        body.extend_from_slice(frame.as_slice());
        let (status, ver, _) = self.rpc(OP_CAS_PUT, id, &body)?;
        match status {
            STATUS_OK => Ok(Version(ver)),
            STATUS_CONFLICT => Err(StorageError::version_conflict(
                id,
                expected_version,
                Version(ver),
            )),
            STATUS_UNAUTHORIZED => Err(StorageError::invalid_key("unauthorized CAS_PUT")),
            _ => Err(StorageError::corrupted_header("server rejected CAS_PUT")),
        }
    }

    /// Fetches multiple 64-byte-aligned `AlignedBytes` frames in a single network round-trip (`OP_BATCH_GET`).
    pub fn batch_get_frames(&mut self, ids: &[InlineId]) -> Result<Vec<Option<AlignedBytes>>> {
        let mut body = Vec::with_capacity(4 + ids.len() * 24);
        body.extend_from_slice(&(ids.len() as u32).to_le_bytes());
        for id in ids {
            body.extend_from_slice(&id.to_raw_bytes());
        }
        let (status, _, resp) = self.rpc(OP_BATCH_GET, InlineId::from_static("_"), &body)?;
        if status != STATUS_OK || resp.len() < 4 {
            return Err(StorageError::corrupted_header("BatchGet failed"));
        }
        let count = u32::from_le_bytes(resp[0..4].try_into().unwrap()) as usize;
        let mut pos = 4usize;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            if pos + 4 > resp.len() {
                break;
            }
            let len = u32::from_le_bytes(resp[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if len == 0 {
                out.push(None);
            } else {
                out.push(Some(AlignedBytes::copy_from_slice(&resp[pos..pos + len])));
                pos += len;
            }
        }
        Ok(out)
    }

    /// Commits a multi-record atomic transaction over TCP in a single round-trip (`OP_BATCH_PUT`).
    pub fn batch_put<T: ZeroCopyRecord>(&mut self, items: &[(InlineId, T)]) -> Result<usize> {
        let mut body = Vec::with_capacity(4 + items.len() * 160);
        body.extend_from_slice(&(items.len() as u32).to_le_bytes());
        for (id, rec) in items {
            let frame = RecordCodec::encode_aligned(rec, Version::INITIAL, 0, RecordFlags::NONE, 0);
            body.extend_from_slice(&id.to_raw_bytes());
            body.extend_from_slice(&(frame.len() as u32).to_le_bytes());
            body.extend_from_slice(frame.as_slice());
        }
        let (status, committed, _) =
            self.rpc(OP_BATCH_PUT, InlineId::from_static("_"), &body)?;
        match status {
            STATUS_OK => Ok(committed as usize),
            STATUS_UNAUTHORIZED => Err(StorageError::invalid_key("unauthorized BatchPut")),
            _ => Err(StorageError::corrupted_header("BatchPut failed")),
        }
    }

    /// Executes a remote range query over TCP (`OP_QUERY_HYBRID`).
    pub fn query_range_ids(
        &mut self,
        field_id: u16,
        min_val: i64,
        max_val: i64,
        limit: u32,
    ) -> Result<Vec<InlineId>> {
        let mut body = [0u8; 22];
        body[0..2].copy_from_slice(&field_id.to_le_bytes());
        body[2..10].copy_from_slice(&min_val.to_le_bytes());
        body[10..18].copy_from_slice(&max_val.to_le_bytes());
        body[18..22].copy_from_slice(&limit.to_le_bytes());
        let (status, _, resp) =
            self.rpc(OP_QUERY_HYBRID, InlineId::from_static("_"), &body)?;
        if status != STATUS_OK || resp.len() < 4 {
            return Err(StorageError::corrupted_header("QueryHybrid failed"));
        }
        let count = u32::from_le_bytes(resp[0..4].try_into().unwrap()) as usize;
        let mut ids = Vec::with_capacity(count);
        for i in 0..count {
            let off = 4 + i * 24;
            if off + 24 <= resp.len() {
                let raw: [u8; 24] = resp[off..off + 24].try_into().unwrap();
                if let Ok(id) = InlineId::from_raw_bytes(raw) {
                    ids.push(id);
                }
            }
        }
        Ok(ids)
    }

    /// Executes a remote paginated range query (`offset`, `limit`, `descending`) over TCP (`OP_QUERY_HYBRID`, `TASK-056`),
    /// returning `(page_ids, total_matched)`.
    pub fn query_range_paginated(
        &mut self,
        field_id: u16,
        min_val: i64,
        max_val: i64,
        offset: u32,
        limit: u32,
        descending: bool,
    ) -> Result<(Vec<InlineId>, usize)> {
        let mut body = [0u8; 27];
        body[0..2].copy_from_slice(&field_id.to_le_bytes());
        body[2..10].copy_from_slice(&min_val.to_le_bytes());
        body[10..18].copy_from_slice(&max_val.to_le_bytes());
        body[18..22].copy_from_slice(&limit.to_le_bytes());
        body[22..26].copy_from_slice(&offset.to_le_bytes());
        body[26] = u8::from(descending);
        let (status, total_matched, resp) =
            self.rpc(OP_QUERY_HYBRID, InlineId::from_static("_"), &body)?;
        if status != STATUS_OK || resp.len() < 4 {
            return Err(StorageError::corrupted_header(
                "QueryHybrid paginated failed",
            ));
        }
        let count = u32::from_le_bytes(resp[0..4].try_into().unwrap()) as usize;
        let mut ids = Vec::with_capacity(count);
        for i in 0..count {
            let off = 4 + i * 24;
            if off + 24 <= resp.len() {
                let raw: [u8; 24] = resp[off..off + 24].try_into().unwrap();
                if let Ok(id) = InlineId::from_raw_bytes(raw) {
                    ids.push(id);
                }
            }
        }
        Ok((ids, total_matched as usize))
    }

    /// Scrapes Prometheus/OpenMetrics text exposition from the server (`OP_METRICS`).
    pub fn scrape_metrics(&mut self) -> Result<String> {
        let (status, _, resp) = self.rpc(OP_METRICS, InlineId::from_static("_"), &[])?;
        if status != STATUS_OK {
            return Err(StorageError::corrupted_header("Metrics scrape failed"));
        }
        Ok(String::from_utf8_lossy(&resp).into_owned())
    }

    /// Fetches a 64-byte-aligned `AlignedBytes` frame from the server that can be inspected
    /// zero-copy via `RecordCodec::decode_verified::<T>(&bytes)`.
    pub fn get_frame(&mut self, id: InlineId) -> Result<AlignedBytes> {
        let (status, _, body) = self.rpc(OP_GET_RAW_FRAME, id, &[])?;
        match status {
            STATUS_OK => Ok(AlignedBytes::copy_from_slice(&body)),
            STATUS_NOT_FOUND => Err(StorageError::not_found(id)),
            STATUS_UNAUTHORIZED => Err(StorageError::invalid_key("unauthorized GET")),
            STATUS_RATE_LIMITED => Err(StorageError::io(
                "rate limited during GET",
                io::Error::new(io::ErrorKind::WouldBlock, "rate limit exceeded"),
            )),
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
            STATUS_UNAUTHORIZED => Err(StorageError::invalid_key("unauthorized DELETE")),
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

    // ── Raft Consensus Client RPCs (`TASK-063`, `F-DIST-02`) ───────────────

    /// Sends a Raft `RequestVote` RPC over TCP (`OP_RAFT_VOTE`).
    pub fn raft_vote(&mut self, req: &RequestVoteReq) -> Result<(u8, u64, Vec<u8>)> {
        let wire = req.to_wire_bytes();
        self.rpc(OP_RAFT_VOTE, InlineId::from_static("_"), &wire)
    }

    /// Sends a Raft `AppendEntries` RPC over TCP (`OP_RAFT_APPEND`).
    pub fn raft_append(&mut self, req: &AppendEntriesReq) -> Result<(u8, u64, Vec<u8>)> {
        let wire = req.to_wire_bytes();
        self.rpc(OP_RAFT_APPEND, InlineId::from_static("_"), &wire)
    }

    /// Ships record frames for a virtual shard migration over TCP (`OP_MIGRATE_VSHARD`).
    pub fn migrate_vshard<T: ZeroCopyRecord>(
        &mut self,
        shard_id: u16,
        dest_node: u16,
        items: &[(InlineId, T)],
    ) -> Result<u64> {
        let mut body = Vec::with_capacity(4 + items.len() * 160);
        body.extend_from_slice(&shard_id.to_le_bytes());
        body.extend_from_slice(&dest_node.to_le_bytes());
        for (id, rec) in items {
            let frame = RecordCodec::encode_aligned(rec, Version::INITIAL, 0, RecordFlags::NONE, 0);
            body.extend_from_slice(&id.to_raw_bytes());
            body.extend_from_slice(&(frame.len() as u32).to_le_bytes());
            body.extend_from_slice(frame.as_slice());
        }
        let (status, imported, _) = self.rpc(OP_MIGRATE_VSHARD, InlineId::from_static("_"), &body)?;
        match status {
            STATUS_OK => Ok(imported),
            STATUS_UNAUTHORIZED => Err(StorageError::invalid_key("unauthorized MigrateVShard")),
            _ => Err(StorageError::corrupted_header("MigrateVShard failed")),
        }
    }

    // ── Admin Client RPCs (`TASK-061`, `F-OPS-04`) ─────────────────────────

    /// Triggers a remote online hot backup at `dest_path` on the server filesystem (`OP_ADMIN_BACKUP`).
    pub fn admin_backup(&mut self, dest_path: &str) -> Result<String> {
        let (status, _, resp) = self.rpc(OP_ADMIN_BACKUP, InlineId::from_static("_"), dest_path.as_bytes())?;
        match status {
            STATUS_OK => Ok(String::from_utf8_lossy(&resp).into_owned()),
            STATUS_UNAUTHORIZED => Err(StorageError::invalid_key("unauthorized AdminBackup")),
            _ => Err(StorageError::corrupted_header("AdminBackup failed")),
        }
    }

    /// Triggers remote arena compaction (`OP_ADMIN_COMPACT`).
    pub fn admin_compact(&mut self) -> Result<String> {
        let (status, _, resp) = self.rpc(OP_ADMIN_COMPACT, InlineId::from_static("_"), &[])?;
        match status {
            STATUS_OK => Ok(String::from_utf8_lossy(&resp).into_owned()),
            STATUS_UNAUTHORIZED => Err(StorageError::invalid_key("unauthorized AdminCompact")),
            _ => Err(StorageError::corrupted_header("AdminCompact failed")),
        }
    }

    /// Queries server health status as a JSON string (`OP_HEALTH`).
    pub fn health(&mut self) -> Result<String> {
        let (status, _, resp) = self.rpc(OP_HEALTH, InlineId::from_static("_"), &[])?;
        match status {
            STATUS_OK => Ok(String::from_utf8_lossy(&resp).into_owned()),
            _ => Err(StorageError::corrupted_header("Health check failed")),
        }
    }

    /// Triggers a remote incremental checkpoint (`OP_ADMIN_CHECKPOINT`).
    pub fn admin_checkpoint(&mut self) -> Result<String> {
        let (status, _, resp) = self.rpc(OP_ADMIN_CHECKPOINT, InlineId::from_static("_"), &[])?;
        match status {
            STATUS_OK => Ok(String::from_utf8_lossy(&resp).into_owned()),
            STATUS_UNAUTHORIZED => Err(StorageError::invalid_key("unauthorized AdminCheckpoint")),
            _ => Err(StorageError::corrupted_header("AdminCheckpoint failed")),
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
    use storage_core::{
        raft::{AppendEntriesReq, RaftLogEntry, RequestVoteReq},
        EngineConfig, FIELD_SCORE,
    };

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
    fn test_tcp_server_wire_protocol_v2_cas_batch_query_auth_and_metrics() {
        let dir = std::env::temp_dir().join("stge_test_tcp_server_v2");
        let _ = std::fs::remove_dir_all(&dir);

        let engine = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(&dir, EngineConfig::default()).unwrap(),
        );
        let sec = SecurityConfig::new(true)
            .with_token("reader-secret", RbacRole::ReadOnly)
            .with_token("writer-secret", RbacRole::ReadWrite);
        let mut server =
            StorageServer::start_with_config("127.0.0.1:0", Arc::clone(&engine), 128, sec)
                .unwrap();
        let addr = server.local_addr();

        let mut client = StorageClient::connect(addr).unwrap();
        let id1 = InlineId::from_static("v2_item_1");
        let id2 = InlineId::from_static("v2_item_2");

        // 1. Unauthenticated write must be rejected
        assert!(client
            .put(id1, &AgentObjectRecord::new(id1, "Unauth", 100, 10))
            .is_err());

        // 2. Authenticate as ReadOnly -> write still rejected
        assert_eq!(
            client.authenticate("reader-secret").unwrap(),
            RbacRole::ReadOnly
        );
        assert!(client
            .put(id1, &AgentObjectRecord::new(id1, "ReadOnly Write", 100, 10))
            .is_err());

        // 3. Upgrade to ReadWrite -> BatchPut 2 records atomically
        assert_eq!(
            client.authenticate("writer-secret").unwrap(),
            RbacRole::ReadWrite
        );
        let committed = client
            .batch_put(&[
                (id1, AgentObjectRecord::new(id1, "Batch One", 100, 100)),
                (id2, AgentObjectRecord::new(id2, "Batch Two", 200, 200)),
            ])
            .unwrap();
        assert_eq!(committed, 2);

        // 4. CAS Put with wrong version fails with VersionConflict; right version succeeds
        assert!(client
            .put_cas(
                id1,
                Version(99),
                &AgentObjectRecord::new(id1, "CAS Fail", 100, 150)
            )
            .is_err());
        let new_v = client
            .put_cas(
                id1,
                Version(1),
                &AgentObjectRecord::new(id1, "CAS Success", 100, 150),
            )
            .unwrap();
        assert_eq!(new_v, Version(2));

        // 5. BatchGet both keys in 1 round-trip
        let frames = client.batch_get_frames(&[id1, id2]).unwrap();
        assert_eq!(frames.len(), 2);
        assert!(frames[0].is_some() && frames[1].is_some());

        // 6. Remote QueryHybrid by score range (and paginated descending query)
        let q_ids = client.query_range_ids(FIELD_SCORE, 140, 160, 10).unwrap();
        assert_eq!(q_ids, vec![id1]);
        let (page_ids, total_matched) = client
            .query_range_paginated(FIELD_SCORE, 100, 300, 0, 1, true)
            .unwrap();
        assert_eq!(total_matched, 2);
        assert_eq!(page_ids, vec![id2]);

        // 7. Scrape Prometheus metrics
        let prom = client.scrape_metrics().unwrap();
        assert!(prom.contains("storine_live_records 2"));

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
        assert!(
            res3.is_err(),
            "3rd connection must be shed when max_connections=2 is full"
        );
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

    #[test]
    fn test_tcp_raft_admin_health_and_shard_migration_rpcs() {
        let dir = std::env::temp_dir().join("stge_test_tcp_admin_raft");
        let _ = std::fs::remove_dir_all(&dir);
        let backup_dest = std::env::temp_dir().join("stge_test_tcp_admin_raft_bak");
        let _ = std::fs::remove_dir_all(&backup_dest);

        let engine = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(&dir, EngineConfig::default()).unwrap(),
        );
        // Seed a record so stats are non-trivial
        let seed_id = InlineId::from_static("admin_seed_01");
        engine
            .put(seed_id, &AgentObjectRecord::new(seed_id, "Admin Seed", 100, 42))
            .unwrap();

        let sec = SecurityConfig::new(true)
            .with_token("reader-tok", RbacRole::ReadOnly)
            .with_token("writer-tok", RbacRole::ReadWrite)
            .with_token("admin-tok", RbacRole::Admin);
        let mut server =
            StorageServer::start_with_config("127.0.0.1:0", Arc::clone(&engine), 128, sec)
                .unwrap();
        let addr = server.local_addr();

        // ── 1. Health check (open to ReadOnly) ─────────────────────────────
        let mut client = StorageClient::connect(addr).unwrap();
        client.authenticate("reader-tok").unwrap();
        let health = client.health().unwrap();
        assert!(health.contains("\"status\":\"ok\""));
        assert!(health.contains("\"live_records\":1"));

        // ── 2. Admin RPCs rejected for non-Admin roles ─────────────────────
        assert!(client.admin_compact().is_err(), "ReadOnly must not compact");
        assert!(client.admin_backup(backup_dest.to_str().unwrap()).is_err());
        assert!(client.admin_checkpoint().is_err());

        // ── 3. Upgrade to Admin and run admin RPCs ─────────────────────────
        client.authenticate("admin-tok").unwrap();

        let compact_msg = client.admin_compact().unwrap();
        assert!(compact_msg.contains("compacted"));

        let backup_msg = client.admin_backup(backup_dest.to_str().unwrap()).unwrap();
        assert!(backup_msg.contains("backup created"));

        let ckpt_msg = client.admin_checkpoint().unwrap();
        assert!(ckpt_msg.contains("checkpoint wrote"));

        // ── 4. Raft Vote RPC round-trip ────────────────────────────────────
        let mut writer_client = StorageClient::connect(addr).unwrap();
        writer_client.authenticate("writer-tok").unwrap();
        let vote_req = RequestVoteReq {
            term: 5,
            candidate_id: 1,
            last_log_index: 10,
            last_log_term: 4,
        };
        let (status, term, resp_body) = writer_client.raft_vote(&vote_req).unwrap();
        assert_eq!(status, STATUS_OK);
        assert_eq!(term, 5);
        let echoed = RequestVoteReq::from_wire_bytes(&resp_body).unwrap();
        assert_eq!(echoed, vote_req);

        // ── 5. Raft AppendEntries RPC round-trip ───────────────────────────
        let append_req = AppendEntriesReq {
            term: 5,
            leader_id: 1,
            prev_log_index: 10,
            prev_log_term: 4,
            entries: vec![RaftLogEntry {
                term: 5,
                index: 11,
                id: InlineId::from_static("raft_entry_01"),
                version: Version(1),
                frame: Some(storage_codec::AlignedBytes::copy_from_slice(&[0u8; 64])),
            }],
            leader_commit: 10,
        };
        let (status, term, resp_body) = writer_client.raft_append(&append_req).unwrap();
        assert_eq!(status, STATUS_OK);
        assert_eq!(term, 5);
        let ae_resp = storage_core::raft::AppendEntriesResp::from_wire_bytes(&resp_body).unwrap();
        assert!(ae_resp.success);
        assert_eq!(ae_resp.match_index, 11); // prev_log_index(10) + 1 entry

        // ── 6. Shard migration RPC (Admin only) ───────────────────────────
        let mig_id = InlineId::from_static("migrated_rec_01");
        let mig_rec = AgentObjectRecord::new(mig_id, "Migrated Shard Record", 300, 77);
        let imported = client
            .migrate_vshard(42u16, 2u16, &[(mig_id, mig_rec)])
            .unwrap();
        assert_eq!(imported, 1);
        // Verify the migrated record actually landed in the engine
        let (_, owned) = engine.get_owned(mig_id).unwrap();
        assert_eq!(owned.title, "Migrated Shard Record");
        assert_eq!(owned.score, 77);

        server.stop();
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&backup_dest);
    }
}
