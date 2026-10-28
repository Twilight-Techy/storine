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
//!     - `0x0B = QueryHybrid` (`[field_id: u16, min_score: i64, max_score: i64, limit: u32 (, offset: u32, desc: u8)]`)
//!     - `0x0C = Auth` (`[token_utf8_bytes]`, at most 4 KiB)
//!     - `0x0D = Metrics` (Prometheus/OpenMetrics text exposition scrape)
//!     - `0x0E = RaftVote` (`[RequestVoteReq wire bytes]` -> `RequestVoteResp`)
//!     - `0x0F = RaftAppend` (`[AppendEntriesReq wire bytes]` -> `AppendEntriesResp`)
//!     - `0x10 = MigrateVShard` (`[shard_id: u16 | dest_node: u16 | repeated (id: 24B, frame_len: u32, frame)]`)
//!     - `0x11 = AdminBackup` (`[backup_name_utf8]`, created under `SecurityConfig::backup_root`)
//!     - `0x12 = AdminCompact` (no body)
//!     - `0x13 = Health` (no body)
//!     - `0x14 = AdminCheckpoint` (no body)
//!     - `0x15 = RaftInstallSnapshot` (`[InstallSnapshotReq wire bytes]` -> `InstallSnapshotResp`)
//!     - `0x20 = DocGet`, `0x21 = DocWrite`, `0x22 = DocQuery`, `0x23 = DocAdmin`, `0x25 = DocInfo`
//!       (document API, `protocol/PROTOCOL.md`)
//!     - `0x24 = Txn` (atomic typed-frame batch with per-item preconditions)
//!     - `0x26 = SetOptions` (`[flags u8]`: bit 0 = allow possibly stale follower reads)
//!     - `0x27 = ClusterInfo` (node id, leader, term, commit / applied indexes, member addresses)
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
//!     - `0x07 = NotLeader` (Raft follower; `version` = leader node id, `0` if unknown)
//!     - `0x08 = Invalid` (malformed request, bad value, type mismatch, bound violated)
//!     - `0x09 = CommitUnknown` (replicated write not confirmed; retry with the same request id)
//!   - `version: u64`
//!   - `body_len: u32`
//!   - `body: [u8; body_len]`
//!
//! Admission control happens on the 29-byte header, before any body memory is reserved: the
//! rate limit, the opcode's required role, and a per-opcode body ceiling are enforced first, so
//! unauthenticated clients can never make the server allocate large buffers. Requests that are
//! rejected at the header stage close the connection (their body is never read).
//!
//! Uses `SocketPoller` (`WSAPoll` on Windows, `poll` on Unix): worker threads sleep in the OS
//! kernel when connections are idle and only drive the sockets the kernel reports ready.
//! Long-running admin operations run on a bounded background worker, never on a reactor thread.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

pub mod cluster;
pub mod documents;
pub mod security;

use std::io::{self, BufReader, BufWriter, IoSlice, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use storage_codec::{AlignedBytes, RecordCodec, RecordView, ZeroCopyRecord};
use storage_core::{
    backup::create_online_backup,
    raft::{
        AppendEntriesReq, AppendEntriesResp, InstallSnapshotReq, RaftNode, RequestVoteReq,
        RequestVoteResp, RAFT_SNAPSHOT_CHUNK,
    },
    AgentObjectRecord, DocumentRecord, DocumentStore, LatencyHistogram, Precondition, RecordRef,
    SlowOpEntry, SlowOperationLog, StorageEngine, WriteKind, WriteOp,
};
use storage_index::Indexable;
use storage_mmap::SocketPoller;
use storage_types::wire::{read_array_24, read_i64_le, read_u16_le, read_u32_le, read_u64_le};
use storage_types::{InlineId, RecordFlags, Result, StorageError, Version, MAX_PAYLOAD_SIZE};

pub use security::{RbacRole, SecurityConfig, TokenBucket};

/// Placeholder key for RPCs that do not address a single record (validated at compile time).
const NO_KEY: InlineId = InlineId::from_static("_");

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
pub const OP_RAFT_SNAPSHOT: u8 = 0x15;
pub const OP_DOC_GET: u8 = 0x20;
pub const OP_DOC_WRITE: u8 = 0x21;
pub const OP_DOC_QUERY: u8 = 0x22;
pub const OP_DOC_ADMIN: u8 = 0x23;
pub const OP_TXN: u8 = 0x24;
pub const OP_DOC_INFO: u8 = 0x25;
pub const OP_SET_OPTIONS: u8 = 0x26;
pub const OP_CLUSTER_INFO: u8 = 0x27;

pub const STATUS_OK: u8 = 0x00;
pub const STATUS_NOT_FOUND: u8 = 0x01;
pub const STATUS_ERROR: u8 = 0x02;
pub const STATUS_OVERLOADED: u8 = 0x03;
pub const STATUS_CONFLICT: u8 = 0x04;
pub const STATUS_UNAUTHORIZED: u8 = 0x05;
pub const STATUS_RATE_LIMITED: u8 = 0x06;
pub const STATUS_NOT_LEADER: u8 = 0x07;
pub const STATUS_INVALID: u8 = 0x08;
pub const STATUS_COMMIT_UNKNOWN: u8 = 0x09;

/// Default hard ceiling on simultaneous active TCP connections before load-shedding kicks in.
pub const DEFAULT_MAX_CONNECTIONS: usize = 100_000;
/// Maximum `OP_AUTH` token body (`4 KiB`), the only body an unauthenticated client may send.
pub const MAX_AUTH_BODY: usize = 4096;
/// Maximum concurrently running admin jobs (backup / compaction / checkpoint / shard import).
pub const MAX_CONCURRENT_ADMIN_JOBS: usize = 2;

/// Wire size of one `BatchPut` / `MigrateVShard` item header (`24B id + 4B frame_len`).
const BATCH_ITEM_HEADER: usize = 28;

/// Minimum role required to execute `op`, or `None` for unknown opcodes.
#[inline]
const fn required_role(op: u8) -> Option<RbacRole> {
    match op {
        OP_AUTH => Some(RbacRole::Unauthenticated),
        OP_GET_RAW_FRAME | OP_STATS | OP_BATCH_GET | OP_QUERY_HYBRID | OP_METRICS | OP_HEALTH
        | OP_DOC_GET | OP_DOC_QUERY | OP_DOC_INFO | OP_SET_OPTIONS | OP_CLUSTER_INFO => {
            Some(RbacRole::ReadOnly)
        }
        OP_PUT_RAW_FRAME | OP_DELETE | OP_CAS_PUT | OP_BATCH_PUT | OP_TXN | OP_DOC_WRITE
        | OP_DOC_ADMIN => Some(RbacRole::ReadWrite),
        OP_RAFT_VOTE | OP_RAFT_APPEND | OP_RAFT_SNAPSHOT | OP_MIGRATE_VSHARD | OP_ADMIN_BACKUP
        | OP_ADMIN_COMPACT | OP_ADMIN_CHECKPOINT => Some(RbacRole::Admin),
        _ => None,
    }
}

/// Largest request body accepted for `op` (`0` = no body allowed).
#[inline]
const fn max_body_len(op: u8) -> usize {
    match op {
        OP_AUTH => MAX_AUTH_BODY,
        OP_ADMIN_BACKUP => 4096,
        OP_QUERY_HYBRID => 64,
        OP_RAFT_VOTE => 64,
        OP_PUT_RAW_FRAME | OP_CAS_PUT | OP_BATCH_GET | OP_BATCH_PUT | OP_RAFT_APPEND
        | OP_MIGRATE_VSHARD | OP_TXN | OP_DOC_WRITE => MAX_PAYLOAD_SIZE as usize,
        // Collection + key, or a query / collection command (filters are small).
        OP_DOC_GET => 2 * 1024 + 16,
        OP_SET_OPTIONS => 16,
        OP_DOC_QUERY | OP_DOC_ADMIN | OP_DOC_INFO => 1024 * 1024,
        // One snapshot chunk plus its fixed header.
        OP_RAFT_SNAPSHOT => RAFT_SNAPSHOT_CHUNK + 64,
        _ => 0,
    }
}

/// Self-wake socket pair for one reactor shard (`RULE-CPU-03`).
///
/// The read end sits in the shard's poll set, so a worker can block in the kernel with an
/// **infinite** timeout and still be woken for new connections, finished admin jobs and shutdown,
/// instead of re-polling every connection on a timer.
struct ShardWaker {
    tx: TcpStream,
    rx: TcpStream,
}

impl ShardWaker {
    fn new() -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let tx = TcpStream::connect(listener.local_addr()?)?;
        let tx_addr = tx.local_addr()?;
        // Another local process could race to connect first: accept until our own peer arrives.
        let rx = loop {
            let (rx, peer) = listener.accept()?;
            if peer == tx_addr {
                break rx;
            }
        };
        tx.set_nodelay(true)?;
        tx.set_nonblocking(true)?;
        rx.set_nonblocking(true)?;
        Ok(Self { tx, rx })
    }

    /// Makes the shard's poll set readable. A full socket buffer means a wake is already pending.
    #[inline]
    fn wake(&self) {
        let _ = (&self.tx).write(&[1]);
    }

    /// Consumes pending wake bytes so the next poll can block again.
    fn drain(&self) {
        let mut buf = [0u8; 64];
        while let Ok(n) = (&self.rx).read(&mut buf) {
            if n < buf.len() {
                break;
            }
        }
    }
}

struct ReactorShard {
    /// Request latency of read RPCs served by this shard's worker. Only that worker records into
    /// it, so recording never contends across cores (`RULE-SCALE-02`); exports merge all shards.
    read_latency: LatencyHistogram,
    /// Request latency of write RPCs served by this shard's worker.
    write_latency: LatencyHistogram,
    incoming: Mutex<Vec<TcpStream>>,
    has_incoming: AtomicBool,
    wake_cv: Condvar,
    /// `None` only if a loopback socket pair could not be created; the worker then falls back to
    /// bounded timed polling.
    waker: Option<ShardWaker>,
}

impl ReactorShard {
    fn new() -> Self {
        Self {
            read_latency: LatencyHistogram::new(),
            write_latency: LatencyHistogram::new(),
            incoming: Mutex::new(Vec::new()),
            has_incoming: AtomicBool::new(false),
            wake_cv: Condvar::new(),
            waker: ShardWaker::new().ok(),
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
        drop(guard);
        self.wake();
    }

    /// Wakes the worker whether it is parked on the condvar (no connections) or blocked in poll.
    #[inline]
    fn wake(&self) {
        if let Some(ref waker) = self.waker {
            waker.wake();
        }
    }

    /// Wakes the worker for shutdown. Holding `incoming` while notifying closes the lost-wakeup
    /// window between the worker's shutdown check and its condvar wait.
    fn wake_for_shutdown(&self) {
        let _guard = self
            .incoming
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.wake_cv.notify_all();
        drop(_guard);
        self.wake();
    }
}

/// Response produced asynchronously by an admin job.
type JobResponse = (u8, u64, Vec<u8>);

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
    /// Admin job running in the background; no new request is read until it answers.
    pending_job: Option<Receiver<JobResponse>>,
    /// Read request waiting for a Raft read barrier (executed when `pending_job` answers
    /// [`STATUS_BARRIER_PASSED`]).
    deferred: Option<Box<DeferredRequest>>,
    /// The client accepts possibly stale reads from a follower (`OP_SET_OPTIONS`).
    follower_reads: bool,
}

/// `(opcode, key, body)` of a request postponed behind a read barrier.
type DeferredRequest = (u8, [u8; 24], Vec<u8>);

/// Internal status: a read barrier passed, run the deferred request (never sent on the wire).
const STATUS_BARRIER_PASSED: u8 = 0xFE;

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
            pending_job: None,
            deferred: None,
            follower_reads: false,
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

impl StorageServer<DocumentRecord> {
    /// Starts a server hosting the document API (`OP_DOC_*`, used by every SDK and the SQL
    /// adapter) over `store`, plus the typed-frame opcodes over its engine.
    pub fn start_documents(
        bind_addr: &str,
        store: Arc<DocumentStore>,
        max_connections: usize,
        security: SecurityConfig,
        raft: Option<Arc<RaftNode<DocumentRecord>>>,
    ) -> Result<Self> {
        let engine = Arc::clone(store.engine());
        Self::start_inner(bind_addr, engine, max_connections, security, raft, Some(store), None)
    }

    /// Starts a clustered document server: the store's writes go through `options.runtime`
    /// (its node must wrap `store.engine()`).
    pub fn start_documents_cluster(
        bind_addr: &str,
        store: Arc<DocumentStore>,
        max_connections: usize,
        security: SecurityConfig,
        options: cluster::ClusterOptions<DocumentRecord>,
    ) -> Result<Self> {
        store.set_commit_sink(Arc::clone(&options.runtime) as Arc<dyn storage_core::CommitSink>);
        let engine = Arc::clone(store.engine());
        let raft = Some(Arc::clone(options.runtime.node()));
        Self::start_inner(bind_addr, engine, max_connections, security, raft, Some(store), Some(options))
    }
}

/// State shared by every reactor worker.
struct ServerShared<T: ZeroCopyRecord + Indexable> {
    engine: Arc<StorageEngine<T>>,
    security: SecurityConfig,
    raft: Option<Arc<RaftNode<T>>>,
    /// Document API (`OP_DOC_*`); `None` when the server hosts only typed records.
    documents: Option<Arc<DocumentStore>>,
    /// Raft cluster membership (writes through consensus, leader-only reads by default).
    cluster: Option<cluster::ClusterContext<T>>,
    admin_jobs: AtomicUsize,
    /// Every reactor shard, so `OP_METRICS` can merge their latency histograms.
    shards: Vec<Arc<ReactorShard>>,
    /// Requests slower than [`DEFAULT_SLOW_RPC_NS`] (bounded ring, `F-OBS-03`).
    slow_log: SlowOperationLog,
}

/// Default threshold above which a request is recorded in the slow-operation log (10 ms).
pub const DEFAULT_SLOW_RPC_NS: u64 = 10_000_000;
/// Slow-operation log capacity (oldest entries are evicted).
const SLOW_LOG_CAPACITY: usize = 256;

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
    shared: Arc<ServerShared<T>>,
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
        Self::start_with_raft(
            bind_addr,
            engine,
            DEFAULT_MAX_CONNECTIONS,
            SecurityConfig::default(),
            None,
        )
    }

    pub fn start_with_max_connections(
        bind_addr: &str,
        engine: Arc<StorageEngine<T>>,
        max_connections: usize,
    ) -> Result<Self> {
        Self::start_with_raft(
            bind_addr,
            engine,
            max_connections,
            SecurityConfig::default(),
            None,
        )
    }

    pub fn start_with_config(
        bind_addr: &str,
        engine: Arc<StorageEngine<T>>,
        max_connections: usize,
        security: SecurityConfig,
    ) -> Result<Self> {
        Self::start_with_raft(bind_addr, engine, max_connections, security, None)
    }

    /// Starts the server; `raft` (when set) serves `OP_RAFT_VOTE` / `OP_RAFT_APPEND`.
    ///
    /// Refuses to start without authentication (`SecurityConfig::require_auth == false`) on any
    /// non-loopback address, and refuses `require_auth` without any registered token.
    pub fn start_with_raft(
        bind_addr: &str,
        engine: Arc<StorageEngine<T>>,
        max_connections: usize,
        security: SecurityConfig,
        raft: Option<Arc<RaftNode<T>>>,
    ) -> Result<Self> {
        Self::start_inner(bind_addr, engine, max_connections, security, raft, None, None)
    }

    /// Starts a clustered server: every write goes through Raft consensus (`options.runtime`),
    /// reads are served by the leader (or by followers for clients that opt in), and the node
    /// answers its peers' Raft RPCs. The runtime's node must wrap `engine`.
    pub fn start_cluster(
        bind_addr: &str,
        engine: Arc<StorageEngine<T>>,
        max_connections: usize,
        security: SecurityConfig,
        options: cluster::ClusterOptions<T>,
    ) -> Result<Self> {
        let raft = Some(Arc::clone(options.runtime.node()));
        Self::start_inner(bind_addr, engine, max_connections, security, raft, None, Some(options))
    }

    #[allow(clippy::too_many_arguments)]
    fn start_inner(
        bind_addr: &str,
        engine: Arc<StorageEngine<T>>,
        max_connections: usize,
        security: SecurityConfig,
        raft: Option<Arc<RaftNode<T>>>,
        documents: Option<Arc<DocumentStore>>,
        cluster_opts: Option<cluster::ClusterOptions<T>>,
    ) -> Result<Self> {
        if security.require_auth && security.token_count() == 0 {
            return Err(StorageError::invalid_key(
                "require_auth is set but no authentication token is registered",
            ));
        }
        if !security.require_auth {
            let addrs = bind_addr
                .to_socket_addrs()
                .map_err(|e| StorageError::io("resolve StorageServer bind address", e))?;
            for addr in addrs {
                if !addr.ip().is_loopback() {
                    return Err(StorageError::invalid_key(
                        "refusing to serve without authentication on a non-loopback address \
                         (register tokens and set SecurityConfig::require_auth)",
                    ));
                }
            }
        }

        let max_conns = Arc::new(AtomicUsize::new(max_connections.max(1)));
        let listener = TcpListener::bind(bind_addr)
            .map_err(|e| StorageError::io("bind StorageServer TCP listener", e))?;
        let local_addr = listener
            .local_addr()
            .map_err(|e| StorageError::io("query local_addr", e))?;

        let num_workers = thread::available_parallelism()
            .map_or(4, |n| n.get().clamp(2, 16));
        let shards: Vec<Arc<ReactorShard>> =
            (0..num_workers).map(|_| Arc::new(ReactorShard::new())).collect();
        let shared = Arc::new(ServerShared {
            engine,
            security,
            raft,
            documents,
            cluster: cluster_opts.map(|o| cluster::ClusterContext {
                runtime: o.runtime,
                client_addrs: o.client_addrs,
                pool: cluster::WorkerPool::new((num_workers * 4).clamp(4, 64), 4096),
            }),
            admin_jobs: AtomicUsize::new(0),
            shards: shards.clone(),
            slow_log: SlowOperationLog::new(DEFAULT_SLOW_RPC_NS, SLOW_LOG_CAPACITY),
        });

        let shutdown = Arc::new(AtomicBool::new(false));
        let active_connections = Arc::new(AtomicUsize::new(0));
        let rejected_connections = Arc::new(AtomicUsize::new(0));

        let mut worker_threads = Vec::with_capacity(num_workers);
        for shard in &shards {
            let shard_clone = Arc::clone(shard);
            let shared_clone = Arc::clone(&shared);
            let sd_clone = Arc::clone(&shutdown);
            let conn_count = Arc::clone(&active_connections);
            worker_threads.push(thread::spawn(move || {
                reactor_worker_loop(shard_clone, shared_clone, sd_clone, conn_count);
            }));
        }

        let shutdown_accept = Arc::clone(&shutdown);
        let shards_accept = shards.clone();
        let conn_count_accept = Arc::clone(&active_connections);
        let rejected_accept = Arc::clone(&rejected_connections);
        let max_conns_accept = Arc::clone(&max_conns);

        let accept_thread = thread::spawn(move || {
            let mut rr = 0usize;
            let mut backoff_ms = 1u64;
            for stream_res in listener.incoming() {
                if shutdown_accept.load(Ordering::Relaxed) {
                    break;
                }
                match stream_res {
                    Ok(mut stream) => {
                        backoff_ms = 1;
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
                            // One non-blocking attempt: a flood of over-limit clients must never
                            // stall the accept thread (a blocking write would cap accepts for
                            // everyone at one per write timeout).
                            if stream.set_nonblocking(true).is_ok() {
                                let _ = stream
                                    .write_vectored(&[IoSlice::new(&hdr), IoSlice::new(msg)]);
                            }
                            continue;
                        }
                        if stream.set_nonblocking(true).is_err() {
                            conn_count_accept.fetch_sub(1, Ordering::Release);
                            continue;
                        }
                        let target = &shards_accept[rr % shards_accept.len()];
                        rr = rr.wrapping_add(1);
                        target.push_stream(stream);
                    }
                    Err(_) => {
                        // Transient descriptor exhaustion (`EMFILE` / `ENFILE`) or similar:
                        // back off exponentially instead of spinning (`RULE-CPU-03`).
                        thread::sleep(Duration::from_millis(backoff_ms));
                        backoff_ms = (backoff_ms * 2).min(100);
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
            shared,
        })
    }

    /// Requests slower than the slow-operation threshold, oldest first (`F-OBS-03`).
    pub fn slow_operations(&self) -> Vec<SlowOpEntry> {
        self.shared.slow_log.snapshot()
    }

    /// Changes the slow-operation threshold (default [`DEFAULT_SLOW_RPC_NS`]).
    pub fn set_slow_operation_threshold_ns(&self, threshold_ns: u64) {
        self.shared.slow_log.set_threshold_ns(threshold_ns);
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
            shard.wake_for_shutdown();
        }
        if let Some(handle) = self.accept_thread.take() {
            let _ = handle.join();
        }
        for handle in self.worker_threads.drain(..) {
            let _ = handle.join();
        }
        if let Some(ref c) = self.shared.cluster {
            c.pool.stop();
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
    shared: Arc<ServerShared<T>>,
    shutdown: Arc<AtomicBool>,
    active_connections: Arc<AtomicUsize>,
) {
    #[cfg(target_os = "linux")]
    if std::env::var("STORINE_REACTOR").map_or(true, |v| v != "poll") {
        if let Ok(ep) = storage_mmap::EpollPoller::new(1024) {
            return reactor_worker_loop_epoll(shard, shared, shutdown, active_connections, ep);
        }
    }
    // Windows: AFD poll over an I/O completion port (the epoll equivalent, `TASK-102`).
    #[cfg(windows)]
    if std::env::var("STORINE_REACTOR").map_or(true, |v| v != "poll") {
        if let Ok(ep) = storage_mmap::AfdPoller::new(1024) {
            return reactor_worker_loop_epoll(shard, shared, shutdown, active_connections, ep);
        }
    }
    let mut conns: Vec<NonBlockingConn> = Vec::with_capacity(64);
    let mut poller = SocketPoller::with_capacity(64);
    let mut idle_polls: u32 = 0;
    let mut made_progress = false;

    while !shutdown.load(Ordering::Relaxed) {
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
                conns.push(NonBlockingConn::new(stream, &shared.security));
            }
            shard.has_incoming.store(false, Ordering::Release);
        }

        if conns.is_empty() {
            continue;
        }

        // Ask the kernel which sockets are ready. After progress, poll without sleeping; while
        // recently active, use short timed waits (1 ms, then 5 ms), which resume measurably faster
        // than an untimed wait under request/response traffic. Once idle for ~128 ms, block
        // indefinitely (`0%` CPU, zero timer wake-ups): new connections, finished admin jobs and
        // shutdown all write to the waker. Without a waker, back off to 25 ms polls.
        let jobs_pending = conns.iter().any(|c| c.pending_job.is_some());
        poller.clear();
        let base = match shard.waker {
            Some(ref waker) => {
                poller.register(&waker.rx, false);
                1
            }
            None => 0,
        };
        for conn in &conns {
            poller.register(&conn.stream, conn.wants_write());
        }
        let poll_timeout_ms = if made_progress {
            0
        } else if idle_polls < 8 {
            1
        } else if idle_polls < 32 {
            5
        } else if shard.waker.is_some() {
            -1
        } else if jobs_pending {
            1
        } else {
            25
        };
        let ready = poller.wait(poll_timeout_ms);
        if base == 1 && poller.is_ready(0) {
            if let Some(ref waker) = shard.waker {
                waker.drain();
            }
        }
        if ready == 0 && !jobs_pending {
            made_progress = false;
            idle_polls = idle_polls.saturating_add(1);
            continue;
        }
        idle_polls = 0;

        made_progress = false;
        // Iterate in reverse so `swap_remove` never moves an unvisited connection into a slot
        // whose poll result has already been consumed.
        let mut idx = conns.len();
        while idx > 0 {
            idx -= 1;
            let conn_ready = poller.is_ready(base + idx) || conns[idx].pending_job.is_some();
            if !conn_ready {
                continue;
            }
            match drive_connection(&mut conns[idx], &shared, &shard) {
                ConnStep::Progress => made_progress = true,
                ConnStep::WouldBlock => {}
                ConnStep::Closed => {
                    conns.swap_remove(idx);
                    active_connections.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
    }
}

/// Readiness reactor (`TASK-090` Linux `epoll`, `TASK-102` Windows AFD poll): sockets are
/// registered once (token = descriptor / socket handle) and each wake-up drives only the ready
/// connections plus those awaiting a job, so its cost is `O(active)` rather than
/// `O(connections)`. Same timeout policy as the `poll` reactor; set `STORINE_REACTOR=poll` to use
/// that one instead.
#[cfg(any(target_os = "linux", windows))]
fn reactor_worker_loop_epoll<T: ZeroCopyRecord + Indexable, P: storage_mmap::ReadyPoller>(
    shard: Arc<ReactorShard>,
    shared: Arc<ServerShared<T>>,
    shutdown: Arc<AtomicBool>,
    active_connections: Arc<AtomicUsize>,
    mut ep: P,
) {
    const WAKER_TOKEN: u64 = u64::MAX;

    let mut conns: Vec<NonBlockingConn> = Vec::with_capacity(64);
    // token -> (index in `conns`, registered write interest)
    let mut slots: storage_types::FastHashMap<u64, (usize, bool)> = storage_types::FastHashMap::default();
    let mut pending_jobs: Vec<u64> = Vec::new();
    let mut work: Vec<u64> = Vec::new();
    let mut idle_polls: u32 = 0;
    let mut made_progress = false;
    if let Some(ref waker) = shard.waker {
        let _ = ep.add(&waker.rx, WAKER_TOKEN, false);
    }

    while !shutdown.load(Ordering::Relaxed) {
        if conns.is_empty() || shard.has_incoming.load(Ordering::Acquire) {
            let mut inc = shard
                .incoming
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            while conns.is_empty() && inc.is_empty() && !shutdown.load(Ordering::Relaxed) {
                inc = shard
                    .wake_cv
                    .wait(inc)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
            for stream in inc.drain(..) {
                let token = storage_mmap::socket_token(&stream);
                let conn = NonBlockingConn::new(stream, &shared.security);
                if ep.add(&conn.stream, token, false).is_err() {
                    // Not watchable: close it rather than leave it unserved.
                    active_connections.fetch_sub(1, Ordering::Relaxed);
                    continue;
                }
                slots.insert(token, (conns.len(), false));
                conns.push(conn);
            }
            shard.has_incoming.store(false, Ordering::Release);
        }
        if conns.is_empty() {
            continue;
        }

        // Same policy as the `poll` reactor (finished admin jobs write to the waker).
        let poll_timeout_ms = if made_progress {
            0
        } else if idle_polls < 8 {
            1
        } else if idle_polls < 32 {
            5
        } else if shard.waker.is_some() {
            -1
        } else if !pending_jobs.is_empty() {
            1
        } else {
            25
        };
        work.clear();
        for &token in ep.wait(poll_timeout_ms) {
            if token == WAKER_TOKEN {
                if let Some(ref waker) = shard.waker {
                    waker.drain();
                }
            } else {
                work.push(token);
            }
        }
        work.extend_from_slice(&pending_jobs);
        work.sort_unstable();
        work.dedup();
        if work.is_empty() {
            made_progress = false;
            idle_polls = idle_polls.saturating_add(1);
            continue;
        }
        idle_polls = 0;
        made_progress = false;
        pending_jobs.clear();

        for &token in &work {
            let Some(&(idx, registered_write)) = slots.get(&token) else {
                continue;
            };
            match drive_connection(&mut conns[idx], &shared, &shard) {
                ConnStep::Closed => {
                    let _ = ep.delete(&conns[idx].stream);
                    slots.remove(&token);
                    conns.swap_remove(idx);
                    if let Some(moved) = conns.get(idx) {
                        let moved_token = storage_mmap::socket_token(&moved.stream);
                        if let Some(slot) = slots.get_mut(&moved_token) {
                            slot.0 = idx;
                        }
                    }
                    active_connections.fetch_sub(1, Ordering::Relaxed);
                    continue;
                }
                ConnStep::Progress => made_progress = true,
                ConnStep::WouldBlock => {}
            }
            let conn = &conns[idx];
            if conn.pending_job.is_some() {
                pending_jobs.push(token);
            }
            let want_write = conn.wants_write();
            if want_write != registered_write && ep.modify(&conn.stream, token, want_write).is_ok() {
                if let Some(slot) = slots.get_mut(&token) {
                    slot.1 = want_write;
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

/// Sends a final error response and closes the connection (used for header-stage rejections,
/// where the request body is deliberately never read).
#[inline]
fn reject_and_close(conn: &mut NonBlockingConn, status: u8, msg: &[u8]) -> ConnStep {
    let _ = conn.send_or_queue_response(status, 0, msg);
    ConnStep::Closed
}

fn drive_connection<T: ZeroCopyRecord + Indexable>(
    conn: &mut NonBlockingConn,
    shared: &Arc<ServerShared<T>>,
    shard: &Arc<ReactorShard>,
) -> ConnStep {
    // 0. Deliver the result of a background admin job before reading anything else.
    if let Some(ref rx) = conn.pending_job {
        match rx.try_recv() {
            Ok((STATUS_BARRIER_PASSED, _, _)) => {
                conn.pending_job = None;
                if let Some(req) = conn.deferred.take() {
                    let (op, raw_id, body) = *req;
                    return execute_local(conn, shared, shard, op, raw_id, body);
                }
                return ConnStep::Progress;
            }
            Ok((status, ver, body)) => {
                conn.pending_job = None;
                conn.deferred = None;
                return match conn.send_or_queue_response(status, ver, &body) {
                    Ok(()) => ConnStep::Progress,
                    Err(_) => ConnStep::Closed,
                };
            }
            Err(TryRecvError::Empty) => return ConnStep::WouldBlock,
            Err(TryRecvError::Disconnected) => {
                conn.pending_job = None;
                return match conn.send_or_queue_response(STATUS_ERROR, 0, b"admin job failed") {
                    Ok(()) => ConnStep::Progress,
                    Err(_) => ConnStep::Closed,
                };
            }
        }
    }

    // 1. Flush any pending response tail first (per-connection backpressure, `RULE-SCALE-01`)
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

    // 2. Read 29-byte request header and admit the request BEFORE reserving body memory
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
                let body_len = read_u32_le(&conn.req_hdr, 25) as usize;

                let Some(needed) = required_role(op) else {
                    return reject_and_close(conn, STATUS_ERROR, b"unknown opcode");
                };
                if conn.role < needed {
                    return reject_and_close(
                        conn,
                        STATUS_UNAUTHORIZED,
                        b"insufficient role for opcode (authenticate with OP_AUTH)",
                    );
                }
                if body_len > max_body_len(op) {
                    return reject_and_close(
                        conn,
                        STATUS_ERROR,
                        b"request body exceeds the limit for this opcode",
                    );
                }
                if !conn.limiter.try_acquire(1) {
                    return reject_and_close(
                        conn,
                        STATUS_RATE_LIMITED,
                        b"rate limit exceeded: token bucket depleted",
                    );
                }
                if body_len > 0 {
                    conn.body_buf.clear();
                    if conn.body_buf.try_reserve_exact(body_len).is_err() {
                        return reject_and_close(
                            conn,
                            STATUS_OVERLOADED,
                            b"server out of memory for request body",
                        );
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

    let op = conn.req_hdr[0];
    let raw_id: [u8; 24] = read_array_24(&conn.req_hdr, 1);
    conn.hdr_read = 0;
    conn.body_read = 0;
    // Take the body so the connection never retains a large buffer after the request (`RULE-MEM-04`).
    let body = core::mem::take(&mut conn.body_buf);

    // 4. Execute the request, recording its latency in this worker's histogram (`F-OBS-01`) and
    //    in the slow-operation log when over the threshold (`F-OBS-03`).
    let started = Instant::now();
    let step = execute_request(conn, shared, shard, op, raw_id, body);
    let elapsed_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    match op_class(op) {
        OpClass::Read => shard.read_latency.record_ns(elapsed_ns),
        OpClass::Write => shard.write_latency.record_ns(elapsed_ns),
        OpClass::Other => {}
    }
    shared
        .slow_log
        .record_if_slow(op_name(op), elapsed_ns, || format!("opcode=0x{op:02X}"));
    step
}

enum OpClass {
    Read,
    Write,
    Other,
}

#[inline]
const fn op_class(op: u8) -> OpClass {
    match op {
        OP_GET_RAW_FRAME | OP_BATCH_GET | OP_QUERY_HYBRID | OP_STATS | OP_METRICS | OP_HEALTH
        | OP_DOC_GET | OP_DOC_QUERY | OP_DOC_INFO => OpClass::Read,
        OP_PUT_RAW_FRAME | OP_CAS_PUT | OP_BATCH_PUT | OP_DELETE | OP_TXN | OP_DOC_WRITE => {
            OpClass::Write
        }
        _ => OpClass::Other,
    }
}

const fn op_name(op: u8) -> &'static str {
    match op {
        OP_GET_RAW_FRAME => "get",
        OP_PUT_RAW_FRAME => "put",
        OP_DELETE => "delete",
        OP_STATS => "stats",
        OP_CAS_PUT => "cas_put",
        OP_BATCH_GET => "batch_get",
        OP_BATCH_PUT => "batch_put",
        OP_QUERY_HYBRID => "query",
        OP_AUTH => "auth",
        OP_METRICS => "metrics",
        OP_RAFT_VOTE => "raft_vote",
        OP_RAFT_APPEND => "raft_append",
        OP_RAFT_SNAPSHOT => "raft_snapshot",
        OP_MIGRATE_VSHARD => "migrate_vshard",
        OP_ADMIN_BACKUP => "admin_backup",
        OP_ADMIN_COMPACT => "admin_compact",
        OP_HEALTH => "health",
        OP_ADMIN_CHECKPOINT => "admin_checkpoint",
        OP_DOC_GET => "doc_get",
        OP_DOC_WRITE => "doc_write",
        OP_DOC_QUERY => "doc_query",
        OP_DOC_ADMIN => "doc_admin",
        OP_DOC_INFO => "doc_info",
        OP_TXN => "txn",
        OP_SET_OPTIONS => "set_options",
        OP_CLUSTER_INFO => "cluster_info",
        _ => "unknown",
    }
}

/// Executes one fully read request (role and size limits were enforced at the header stage).
fn execute_request<T: ZeroCopyRecord + Indexable>(
    conn: &mut NonBlockingConn,
    shared: &Arc<ServerShared<T>>,
    shard: &Arc<ReactorShard>,
    op: u8,
    raw_id: [u8; 24],
    body: Vec<u8>,
) -> ConnStep {
    match route_cluster(conn, shared, shard, op, raw_id, body) {
        Ok(step) => step,
        Err(body) => execute_local(conn, shared, shard, op, raw_id, body),
    }
}

/// Cluster routing (`TASK-101`): writes go through consensus on the leader's worker pool and are
/// redirected by followers; reads are served by the leader under its lease (or after a read
/// barrier), or by any replica if the client opted into follower reads. Returns `Err(body)` when
/// the request should simply run locally.
fn route_cluster<T: ZeroCopyRecord + Indexable>(
    conn: &mut NonBlockingConn,
    shared: &Arc<ServerShared<T>>,
    shard: &Arc<ReactorShard>,
    op: u8,
    raw_id: [u8; 24],
    body: Vec<u8>,
) -> core::result::Result<ConnStep, Vec<u8>> {
    let Some(ref cl) = shared.cluster else {
        return Err(body);
    };
    let reply = |conn: &mut NonBlockingConn, (st, v, b): (u8, u64, Vec<u8>)| {
        if conn.send_or_queue_response(st, v, &b).is_err() {
            ConnStep::Closed
        } else {
            ConnStep::Progress
        }
    };
    let is_leader = cl.runtime.node().is_leader();
    let wake: Arc<dyn Fn() + Send + Sync> = {
        let sh = Arc::clone(shard);
        Arc::new(move || sh.wake())
    };
    match op {
        OP_PUT_RAW_FRAME | OP_CAS_PUT | OP_DELETE | OP_BATCH_PUT | OP_TXN | OP_DOC_WRITE => {
            if !is_leader {
                return Ok(reply(conn, cl.not_leader()));
            }
            let job_shared = Arc::clone(shared);
            let job = Box::new(move || write_reply(&job_shared, op, raw_id, &body));
            match cl.pool.submit(job, wake) {
                Ok(rx) => {
                    conn.pending_job = Some(rx);
                    Ok(ConnStep::Progress)
                }
                Err(()) => Ok(reply(conn, (STATUS_OVERLOADED, 0, b"write queue is full".to_vec()))),
            }
        }
        OP_DOC_ADMIN | OP_MIGRATE_VSHARD => {
            if is_leader {
                Err(body)
            } else {
                Ok(reply(conn, cl.not_leader()))
            }
        }
        OP_GET_RAW_FRAME | OP_BATCH_GET | OP_QUERY_HYBRID | OP_DOC_GET | OP_DOC_QUERY | OP_DOC_INFO => {
            if conn.follower_reads || (is_leader && cl.runtime.lease_valid()) {
                return Err(body);
            }
            if !is_leader {
                return Ok(reply(conn, cl.not_leader()));
            }
            // Lease expired (e.g. right after an election): confirm leadership first.
            let rt = Arc::clone(&cl.runtime);
            let job = Box::new(move || match rt.read_barrier() {
                Ok(()) => (STATUS_BARRIER_PASSED, 0, Vec::new()),
                Err(e) => documents::error_reply(&e),
            });
            match cl.pool.submit(job, wake) {
                Ok(rx) => {
                    conn.pending_job = Some(rx);
                    conn.deferred = Some(Box::new((op, raw_id, body)));
                    Ok(ConnStep::Progress)
                }
                Err(()) => Ok(reply(conn, (STATUS_OVERLOADED, 0, b"read queue is full".to_vec()))),
            }
        }
        _ => Err(body),
    }
}

/// Executes one request on this node (after any cluster routing).
fn execute_local<T: ZeroCopyRecord + Indexable>(
    conn: &mut NonBlockingConn,
    shared: &Arc<ServerShared<T>>,
    shard: &Arc<ReactorShard>,
    op: u8,
    raw_id: [u8; 24],
    body: Vec<u8>,
) -> ConnStep {
    let engine = &shared.engine;

    // 4. Execute request (role already enforced at the header stage)
    let write_res = match op {
        OP_AUTH => {
            let Ok(tok) = core::str::from_utf8(&body) else {
                let _ = conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"invalid UTF-8 token");
                return ConnStep::Progress;
            };
            if let Some(role) = shared.security.authenticate_token(tok) {
                conn.role = role;
                conn.send_or_queue_response(STATUS_OK, role as u64, &[])
            } else {
                conn.role = shared.security.initial_role();
                conn.send_or_queue_response(STATUS_UNAUTHORIZED, 0, b"invalid credentials")
            }
        }
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
        OP_BATCH_GET => {
            if body.len() < 4 {
                let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"BatchGet body too short");
                return ConnStep::Progress;
            }
            let count = read_u32_le(&body, 0) as usize;
            if body.len() != 4 + count.saturating_mul(24) {
                let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"BatchGet malformed key list");
                return ConnStep::Progress;
            }
            match build_batch_get_response(engine, &body, count) {
                Ok((found, out)) => conn.send_or_queue_response(STATUS_OK, found, &out),
                Err(msg) => conn.send_or_queue_response(STATUS_OVERLOADED, 0, msg),
            }
        }
        OP_QUERY_HYBRID => {
            if body.len() < 22 {
                let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"QueryHybrid body too short");
                return ConnStep::Progress;
            }
            let field_id = read_u16_le(&body, 0);
            let min_val = read_i64_le(&body, 2);
            let max_val = read_i64_le(&body, 10);
            // Bound the response so it always fits a single frame.
            let max_ids = (MAX_PAYLOAD_SIZE as usize - 4) / 24;
            let limit = (read_u32_le(&body, 18) as usize).clamp(1, max_ids);
            let (offset, desc) = if body.len() >= 27 {
                (read_u32_le(&body, 22) as usize, body[26] != 0)
            } else {
                (0usize, false)
            };

            let mut q = engine
                .query()
                .filter_range(field_id, min_val..=max_val)
                .offset(offset)
                .limit(limit);
            if desc {
                q = q.order_desc();
            }
            let page = q.execute_page_ids();
            let mut out = Vec::new();
            if out.try_reserve_exact(4 + page.items.len() * 24).is_err() {
                let _ = conn.send_or_queue_response(STATUS_OVERLOADED, 0, b"out of memory");
                return ConnStep::Progress;
            }
            out.extend_from_slice(&(page.items.len() as u32).to_le_bytes());
            for id in &page.items {
                out.extend_from_slice(&id.to_raw_bytes());
            }
            conn.send_or_queue_response(STATUS_OK, page.total_matched as u64, &out)
        }
        OP_STATS => {
            let st = engine.stats();
            let mut payload = [0u8; 24];
            payload[0..8].copy_from_slice(&(st.live_records as u64).to_le_bytes());
            payload[8..16].copy_from_slice(&st.total_reads.to_le_bytes());
            payload[16..24].copy_from_slice(&st.total_writes.to_le_bytes());
            conn.send_or_queue_response(STATUS_OK, st.live_records as u64, &payload)
        }
        OP_METRICS => {
            let st = engine.stats();
            let reads = LatencyHistogram::new();
            let writes = LatencyHistogram::new();
            for s in &shared.shards {
                reads.merge_from(&s.read_latency);
                writes.merge_from(&s.write_latency);
            }
            let mut prom = st.to_prometheus_text(Some(&reads), Some(&writes));
            prom.push_str(&format!(
                "# HELP storine_slow_rpcs Slow requests currently retained in the slow-operation log\n\
                 # TYPE storine_slow_rpcs gauge\n\
                 storine_slow_rpcs {}\n",
                shared.slow_log.len()
            ));
            conn.send_or_queue_response(STATUS_OK, st.live_records as u64, prom.as_bytes())
        }
        // ── Raft Consensus RPCs (`TASK-063`, `F-DIST-02`) ──────────────────────────
        OP_RAFT_VOTE => {
            let Some(ref raft) = shared.raft else {
                let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"raft is not enabled on this server");
                return ConnStep::Progress;
            };
            match RequestVoteReq::from_wire_bytes(&body) {
                Ok(req) => {
                    let resp: RequestVoteResp = raft.handle_request_vote(req);
                    conn.send_or_queue_response(STATUS_OK, resp.term, &resp.to_wire_bytes())
                }
                Err(e) => conn.send_or_queue_response(STATUS_ERROR, 0, e.to_string().as_bytes()),
            }
        }
        OP_RAFT_APPEND => {
            let Some(ref raft) = shared.raft else {
                let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"raft is not enabled on this server");
                return ConnStep::Progress;
            };
            match AppendEntriesReq::from_wire_bytes(&body).and_then(|req| raft.handle_append_entries(req)) {
                Ok(resp) => {
                    let resp: AppendEntriesResp = resp;
                    conn.send_or_queue_response(STATUS_OK, resp.term, &resp.to_wire_bytes())
                }
                Err(e) => conn.send_or_queue_response(STATUS_ERROR, 0, e.to_string().as_bytes()),
            }
        }
        OP_RAFT_SNAPSHOT => {
            let Some(ref raft) = shared.raft else {
                let _ = conn.send_or_queue_response(STATUS_ERROR, 0, b"raft is not enabled on this server");
                return ConnStep::Progress;
            };
            match InstallSnapshotReq::from_wire_bytes(&body).and_then(|req| raft.handle_install_snapshot(req)) {
                Ok(resp) => conn.send_or_queue_response(STATUS_OK, resp.term, &resp.to_wire_bytes()),
                Err(e) => conn.send_or_queue_response(STATUS_ERROR, 0, e.to_string().as_bytes()),
            }
        }
        // ── Long-running admin RPCs run on a bounded background worker ──────────────
        OP_MIGRATE_VSHARD | OP_ADMIN_BACKUP | OP_ADMIN_COMPACT | OP_ADMIN_CHECKPOINT => {
            match spawn_admin_job(op, body, shared, shard) {
                Ok(rx) => {
                    conn.pending_job = Some(rx);
                    return ConnStep::Progress;
                }
                Err((status, msg)) => conn.send_or_queue_response(status, 0, msg),
            }
        }
        // ── Writes: one path for every write opcode (local commit or Raft consensus) ──
        OP_PUT_RAW_FRAME | OP_CAS_PUT | OP_DELETE | OP_BATCH_PUT | OP_TXN | OP_DOC_WRITE => {
            let (status, version, out) = write_reply(shared, op, raw_id, &body);
            conn.send_or_queue_response(status, version, &out)
        }
        // ── Document API (`TASK-098`) ───────────────────────────────────────────────
        OP_DOC_GET | OP_DOC_QUERY | OP_DOC_INFO | OP_DOC_ADMIN => {
            let Some(ref store) = shared.documents else {
                let _ = conn.send_or_queue_response(
                    STATUS_INVALID,
                    0,
                    b"this server does not host the document API",
                );
                return ConnStep::Progress;
            };
            if op == OP_DOC_ADMIN && documents::admin_is_slow(&body) {
                match spawn_admin_job(op, body, shared, shard) {
                    Ok(rx) => {
                        conn.pending_job = Some(rx);
                        return ConnStep::Progress;
                    }
                    Err((status, msg)) => conn.send_or_queue_response(status, 0, msg),
                }
            } else {
                let (status, version, out) = match op {
                    OP_DOC_GET => documents::doc_get(store, &body),
                    OP_DOC_QUERY => documents::doc_query(store, &body),
                    OP_DOC_INFO => documents::doc_admin(store, &body, false),
                    _ => documents::doc_admin(store, &body, true),
                };
                conn.send_or_queue_response(status, version, &out)
            }
        }
        OP_SET_OPTIONS => {
            conn.follower_reads = body.first().is_some_and(|f| f & 1 != 0);
            conn.send_or_queue_response(STATUS_OK, 0, &[])
        }
        OP_CLUSTER_INFO => {
            let out = cluster_info_body(shared);
            conn.send_or_queue_response(STATUS_OK, 0, &out)
        }
        OP_HEALTH => {
            let st = engine.stats();
            let health = format!(
                "{{\"status\":\"ok\",\"live_records\":{},\"total_reads\":{},\"total_writes\":{},\"hot_dirty_bytes\":{}}}",
                st.live_records, st.total_reads, st.total_writes, st.hot_dirty_bytes,
            );
            conn.send_or_queue_response(STATUS_OK, st.live_records as u64, health.as_bytes())
        }
        _ => conn.send_or_queue_response(STATUS_ERROR, 0, b"unknown opcode"),
    };

    if write_res.is_err() {
        ConnStep::Closed
    } else {
        ConnStep::Progress
    }
}

/// `OP_CLUSTER_INFO` reply: `u64 node_id | u8 is_leader | u64 leader_id | str leader_addr |
/// u64 term | u64 commit_index | u64 last_applied | u32 n | (u64 node_id, str addr)*`. A standalone
/// server reports node `0` as its own leader.
fn cluster_info_body<T: ZeroCopyRecord + Indexable>(shared: &ServerShared<T>) -> Vec<u8> {
    use storage_core::document_wire::put_str;
    let mut out = Vec::with_capacity(128);
    match shared.cluster {
        Some(ref cl) => {
            let st = cl.runtime.status();
            out.extend_from_slice(&u64::from(st.node_id).to_le_bytes());
            out.push(u8::from(st.is_leader && cl.runtime.lease_valid()));
            out.extend_from_slice(&st.leader_id.map_or(0, u64::from).to_le_bytes());
            let addr = st.leader_id.and_then(|l| cl.client_addrs.get(&l)).cloned().unwrap_or_default();
            put_str(&mut out, &addr);
            out.extend_from_slice(&st.term.to_le_bytes());
            out.extend_from_slice(&st.commit_index.to_le_bytes());
            out.extend_from_slice(&st.last_applied.to_le_bytes());
            let mut members: Vec<(&u16, &String)> = cl.client_addrs.iter().collect();
            members.sort();
            out.extend_from_slice(&(members.len() as u32).to_le_bytes());
            for (id, a) in members {
                out.extend_from_slice(&u64::from(*id).to_le_bytes());
                put_str(&mut out, a);
            }
        }
        None => {
            out.extend_from_slice(&0u64.to_le_bytes());
            out.push(1);
            out.extend_from_slice(&0u64.to_le_bytes());
            put_str(&mut out, "");
            for _ in 0..3 {
                out.extend_from_slice(&0u64.to_le_bytes());
            }
            out.extend_from_slice(&0u32.to_le_bytes());
        }
    }
    out
}

impl<T: ZeroCopyRecord + Indexable> ServerShared<T> {
    /// Commits `ops` atomically: through Raft consensus on a clustered server, else locally.
    fn commit(&self, ops: Vec<WriteOp<T>>) -> Result<Vec<storage_core::WriteResult>> {
        match self.cluster {
            Some(ref c) => c.runtime.commit(ops),
            None => self.engine.commit(ops),
        }
    }
}

/// Executes any write opcode and returns its reply (runs inline, or on the worker pool when the
/// server is clustered and must wait for consensus).
fn write_reply<T: ZeroCopyRecord + Indexable>(
    shared: &ServerShared<T>,
    op: u8,
    raw_id: [u8; 24],
    body: &[u8],
) -> (u8, u64, Vec<u8>) {
    let err = |e: &StorageError| documents::error_reply(e);
    let decode = |bytes: &[u8]| -> Result<T> {
        let aligned = AlignedBytes::try_copy_from_slice(bytes)?;
        Ok(RecordCodec::decode_owned::<T>(aligned.as_slice())?.1)
    };
    match op {
        OP_PUT_RAW_FRAME => {
            let res = InlineId::from_raw_bytes(raw_id)
                .and_then(|id| Ok((id, decode(body)?)))
                .and_then(|(id, rec)| shared.commit(vec![WriteOp::put(id, rec)]));
            match res {
                Ok(r) => (STATUS_OK, r.first().and_then(|x| x.version).map_or(0, |v| v.0), Vec::new()),
                Err(e) => err(&e),
            }
        }
        OP_CAS_PUT => {
            if body.len() < 8 {
                return (STATUS_INVALID, 0, b"CAS body missing expected_version".to_vec());
            }
            let expected = Version(read_u64_le(body, 0));
            // `0` means "must not exist" (create-only).
            let pre = if expected.0 == 0 { Precondition::Absent } else { Precondition::Version(expected) };
            let res = InlineId::from_raw_bytes(raw_id)
                .and_then(|id| Ok((id, decode(&body[8..])?)))
                .and_then(|(id, rec)| shared.commit(vec![WriteOp::put(id, rec).when(pre)]));
            match res {
                Ok(r) => (STATUS_OK, r.first().and_then(|x| x.version).map_or(0, |v| v.0), Vec::new()),
                Err(StorageError::VersionConflict { actual, .. }) => {
                    (STATUS_CONFLICT, actual.0, b"CAS version conflict".to_vec())
                }
                Err(StorageError::ConditionFailed { actual, .. }) => {
                    (STATUS_CONFLICT, actual.map_or(0, |v| v.0), b"CAS version conflict".to_vec())
                }
                Err(e) => err(&e),
            }
        }
        OP_DELETE => {
            let res = InlineId::from_raw_bytes(raw_id).and_then(|id| shared.commit(vec![WriteOp::delete(id)]));
            match res {
                Ok(r) if r.first().is_some_and(|x| x.existed) => (STATUS_OK, 1, Vec::new()),
                Ok(_) => (STATUS_NOT_FOUND, 0, Vec::new()),
                Err(e) => err(&e),
            }
        }
        OP_BATCH_PUT => match parse_batch_items::<T>(body, true) {
            Ok(items) => {
                let n = items.len() as u64;
                let ops = items.into_iter().map(|(id, rec)| WriteOp::put(id, rec)).collect();
                match shared.commit(ops) {
                    Ok(_) => (STATUS_OK, n, Vec::new()),
                    Err(e) => err(&e),
                }
            }
            Err(msg) => (STATUS_INVALID, 0, msg.into_bytes()),
        },
        OP_TXN => execute_txn(shared, body),
        OP_DOC_WRITE => match shared.documents {
            Some(ref store) => documents::doc_write(store, body),
            None => (STATUS_INVALID, 0, b"this server does not host the document API".to_vec()),
        },
        _ => (STATUS_INVALID, 0, b"not a write opcode".to_vec()),
    }
}

/// `OP_TXN` precondition / kind codes (`protocol/PROTOCOL.md` §9).
const TXN_PUT: u8 = 1;
const TXN_DELETE: u8 = 2;
const TXN_CHECK: u8 = 3;

/// Executes an atomic typed-frame batch: `u32 count | (24B id | u8 precondition | u64 version |
/// u8 kind | u32 frame_len | frame)*`, where precondition is `0` none, `1` absent, `2` exists,
/// `3` version. Replies `u32 count | (u8 exists | u64 version)*`, or `Conflict` with
/// `u32 index | u8 exists | u64 actual | message`.
fn execute_txn<T: ZeroCopyRecord + Indexable>(shared: &ServerShared<T>, body: &[u8]) -> (u8, u64, Vec<u8>) {
    use storage_codec::value_wire::SveReader;
    let parse = || -> Result<Vec<WriteOp<T>>> {
        let mut r = SveReader::new(body);
        let n = r.count(41)?;
        let mut ops = Vec::with_capacity(n);
        for _ in 0..n {
            let id = InlineId::from_raw_bytes(read_array_24(r.take(24)?, 0))?;
            let pre_kind = r.u8()?;
            let ver = Version(r.u64()?);
            let precondition = match pre_kind {
                0 => Precondition::None,
                1 => Precondition::Absent,
                2 => Precondition::Exists,
                3 => Precondition::Version(ver),
                _ => return Err(StorageError::invalid_value("unknown precondition")),
            };
            let kind = r.u8()?;
            let frame_len = r.u32()? as usize;
            let frame = r.take(frame_len)?;
            let kind = match kind {
                TXN_PUT => {
                    let aligned = AlignedBytes::try_copy_from_slice(frame)?;
                    let (_, rec) = RecordCodec::decode_owned::<T>(aligned.as_slice())?;
                    WriteKind::Put(rec)
                }
                TXN_DELETE => WriteKind::Delete,
                TXN_CHECK => WriteKind::Check,
                _ => return Err(StorageError::invalid_value("unknown txn item kind")),
            };
            ops.push(WriteOp { id, kind, precondition });
        }
        if r.remaining() != 0 {
            return Err(StorageError::invalid_value("trailing bytes after txn"));
        }
        Ok(ops)
    };
    let ops = match parse() {
        Ok(o) => o,
        Err(e) => return documents::error_reply(&e),
    };
    let ids: Vec<InlineId> = ops.iter().map(|o| o.id).collect();
    match shared.commit(ops) {
        Ok(results) => {
            let mut out = Vec::with_capacity(4 + results.len() * 9);
            out.extend_from_slice(&(results.len() as u32).to_le_bytes());
            for r in &results {
                out.push(u8::from(r.version.is_some()));
                out.extend_from_slice(&r.version.map_or(0, |v| v.0).to_le_bytes());
            }
            (STATUS_OK, results.len() as u64, out)
        }
        Err(e) => {
            let (id, actual) = match e {
                StorageError::VersionConflict { id, actual, .. } => (Some(id), (actual.0 != 0).then_some(actual)),
                StorageError::ConditionFailed { id, actual, .. } => (Some(id), actual),
                _ => (None, None),
            };
            match id {
                Some(id) => {
                    let idx = ids.iter().position(|&x| x == id).unwrap_or(u32::MAX as usize);
                    let mut out = Vec::new();
                    out.extend_from_slice(&(idx as u32).to_le_bytes());
                    out.push(u8::from(actual.is_some()));
                    out.extend_from_slice(&actual.map_or(0, |v| v.0).to_le_bytes());
                    out.extend_from_slice(e.to_string().as_bytes());
                    (STATUS_CONFLICT, 0, out)
                }
                None => documents::error_reply(&e),
            }
        }
    }
}

/// Builds the `BatchGet` response with fallible, size-bounded allocation.
fn build_batch_get_response<T: ZeroCopyRecord + Indexable>(
    engine: &StorageEngine<T>,
    body: &[u8],
    count: usize,
) -> core::result::Result<(u64, Vec<u8>), &'static [u8]> {
    const TOO_LARGE: &[u8] = b"BatchGet response exceeds the maximum frame size";
    let mut out = Vec::new();
    out.try_reserve_exact(4 + count * 4)
        .map_err(|_| &b"out of memory"[..])?;
    out.extend_from_slice(&(count as u32).to_le_bytes());
    let mut found = 0u64;
    for i in 0..count {
        let raw_key = read_array_24(body, 4 + i * 24);
        let frame_ref = InlineId::from_raw_bytes(raw_key)
            .ok()
            .and_then(|key| engine.get_ref(key).ok());
        match frame_ref {
            Some(rec_ref) => {
                let frame = rec_ref.raw_frame();
                if out.len() + 4 + frame.len() > MAX_PAYLOAD_SIZE as usize {
                    return Err(TOO_LARGE);
                }
                out.try_reserve(4 + frame.len())
                    .map_err(|_| &b"out of memory"[..])?;
                out.extend_from_slice(&(frame.len() as u32).to_le_bytes());
                out.extend_from_slice(frame);
                found += 1;
            }
            None => {
                if out.len() + 4 > MAX_PAYLOAD_SIZE as usize {
                    return Err(TOO_LARGE);
                }
                out.extend_from_slice(&0u32.to_le_bytes());
            }
        }
    }
    Ok((found, out))
}

/// Parses `[count u32 | (id 24B, len u32, frame)*]` (`BatchPut`) or the `MigrateVShard` item list
/// (no count prefix) with bounded, fallible allocation.
fn parse_batch_items<T: ZeroCopyRecord>(
    body: &[u8],
    with_count: bool,
) -> core::result::Result<Vec<(InlineId, T)>, String> {
    let (mut pos, declared) = if with_count {
        if body.len() < 4 {
            return Err("BatchPut body too short".into());
        }
        (4usize, Some(read_u32_le(body, 0) as usize))
    } else {
        (0usize, None)
    };
    // Every item needs at least its 28-byte header, which bounds the item count by body size.
    let max_items = (body.len() - pos) / BATCH_ITEM_HEADER;
    let expected = declared.unwrap_or(max_items);
    if expected > max_items {
        return Err("batch item count exceeds body size".into());
    }
    let mut items = Vec::new();
    items
        .try_reserve_exact(expected)
        .map_err(|_| "out of memory for batch".to_string())?;
    while items.len() < expected && pos + BATCH_ITEM_HEADER <= body.len() {
        let raw_key = read_array_24(body, pos);
        let frame_len = read_u32_le(body, pos + 24) as usize;
        pos += BATCH_ITEM_HEADER;
        let Some(frame) = pos.checked_add(frame_len).and_then(|end| body.get(pos..end)) else {
            return Err("batch item frame truncated".into());
        };
        pos += frame_len;
        let item_id = InlineId::from_raw_bytes(raw_key).map_err(|e| e.to_string())?;
        let aligned = AlignedBytes::try_copy_from_slice(frame).map_err(|e| e.to_string())?;
        let (_, rec) = RecordCodec::decode_owned::<T>(aligned.as_slice()).map_err(|e| e.to_string())?;
        items.push((item_id, rec));
    }
    if declared.is_some() && (items.len() != expected || pos != body.len()) {
        return Err("BatchPut malformed item list".into());
    }
    Ok(items)
}

/// Runs a long admin operation on a background thread (at most `MAX_CONCURRENT_ADMIN_JOBS`),
/// so reactor threads keep serving other connections.
fn spawn_admin_job<T: ZeroCopyRecord + Indexable>(
    op: u8,
    body: Vec<u8>,
    shared: &Arc<ServerShared<T>>,
    shard: &Arc<ReactorShard>,
) -> core::result::Result<Receiver<JobResponse>, (u8, &'static [u8])> {
    // Validate cheap inputs up front so bad requests are answered immediately.
    let backup_path = if op == OP_ADMIN_BACKUP {
        let Ok(name) = core::str::from_utf8(&body) else {
            return Err((STATUS_ERROR, b"invalid UTF-8 backup name"));
        };
        if shared.security.backup_root.is_none() {
            return Err((
                STATUS_UNAUTHORIZED,
                b"admin backups are disabled (no SecurityConfig::backup_root configured)",
            ));
        }
        let Some(path) = shared.security.resolve_backup_path(name) else {
            return Err((
                STATUS_ERROR,
                b"backup name must be a relative path without '..', ':' or '\\\\'",
            ));
        };
        Some(path)
    } else {
        None
    };

    let mut running = shared.admin_jobs.load(Ordering::Acquire);
    loop {
        if running >= MAX_CONCURRENT_ADMIN_JOBS {
            return Err((STATUS_OVERLOADED, b"too many concurrent admin jobs"));
        }
        match shared.admin_jobs.compare_exchange_weak(
            running,
            running + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => break,
            Err(actual) => running = actual,
        }
    }

    let (tx, rx) = mpsc::channel();
    let job_shared = Arc::clone(shared);
    let job_shard = Arc::clone(shard);
    let spawned = thread::Builder::new()
        .name("storine-admin-job".into())
        .spawn(move || {
            let resp = run_admin_job(op, &body, backup_path.as_deref(), &job_shared);
            job_shared.admin_jobs.fetch_sub(1, Ordering::AcqRel);
            let _ = tx.send(resp);
            // The connection's worker may be blocked in poll with no timeout: wake it.
            job_shard.wake();
        });
    if spawned.is_err() {
        shared.admin_jobs.fetch_sub(1, Ordering::AcqRel);
        return Err((STATUS_OVERLOADED, b"could not start admin job thread"));
    }
    Ok(rx)
}

fn run_admin_job<T: ZeroCopyRecord + Indexable>(
    op: u8,
    body: &[u8],
    backup_path: Option<&std::path::Path>,
    shared: &ServerShared<T>,
) -> JobResponse {
    let engine = &shared.engine;
    let err = |e: StorageError| (STATUS_ERROR, 0u64, e.to_string().into_bytes());
    match op {
        OP_DOC_ADMIN => match shared.documents {
            Some(ref store) => documents::doc_admin(store, body, true),
            None => (STATUS_INVALID, 0, b"this server does not host the document API".to_vec()),
        },
        OP_ADMIN_BACKUP => {
            let Some(path) = backup_path else {
                return (STATUS_ERROR, 0, b"missing backup path".to_vec());
            };
            match create_online_backup(&**engine, path) {
                Ok(manifest) => {
                    let msg = format!(
                        "backup created: collection={} lsn={} files={}",
                        manifest.collection_name, manifest.base_lsn, manifest.files_copied,
                    );
                    (STATUS_OK, manifest.files_copied as u64, msg.into_bytes())
                }
                Err(e) => err(e),
            }
        }
        OP_ADMIN_COMPACT => match engine.compact_arenas(0.5) {
            Ok(removed) => (
                STATUS_OK,
                removed as u64,
                format!("compacted {removed} arena segment(s)").into_bytes(),
            ),
            Err(e) => err(e),
        },
        OP_ADMIN_CHECKPOINT => match engine.checkpoint() {
            Ok(flushed) => (
                STATUS_OK,
                flushed as u64,
                format!("checkpoint wrote {flushed} record(s)").into_bytes(),
            ),
            Err(e) => err(e),
        },
        OP_MIGRATE_VSHARD => {
            if body.len() < 4 {
                return (STATUS_ERROR, 0, b"MigrateVShard body too short".to_vec());
            }
            let shard_id = read_u16_le(body, 0);
            let dest_node = read_u16_le(body, 2);
            let items = match parse_batch_items::<T>(&body[4..], false) {
                Ok(items) => items,
                Err(msg) => return (STATUS_ERROR, 0, msg.into_bytes()),
            };
            let imported = items.len() as u64;
            // Import atomically (through consensus when clustered): every shipped record lands or
            // none does.
            let res = shared.commit(items.into_iter().map(|(id, rec)| WriteOp::put(id, rec)).collect());
            match res {
                Ok(_) => (
                    STATUS_OK,
                    imported,
                    format!("shard={shard_id} dest={dest_node} imported={imported}").into_bytes(),
                ),
                Err(e) => err(e),
            }
        }
        _ => (STATUS_ERROR, 0, b"unknown admin opcode".to_vec()),
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

    /// Connects with a connect timeout and per-call read / write timeouts of `timeout`, so a
    /// dead or partitioned server can never block the caller forever.
    pub fn connect_timeout(addr: SocketAddr, timeout: Duration) -> Result<Self> {
        let stream = cluster::connect_with_timeout(addr, timeout)?;
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
            self.rpc(OP_AUTH, NO_KEY, token.as_bytes())?;
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
        let (status, _, resp) = self.rpc(OP_BATCH_GET, NO_KEY, &body)?;
        if status != STATUS_OK || resp.len() < 4 {
            return Err(StorageError::corrupted_header("BatchGet failed"));
        }
        let count = read_u32_le(&resp, 0) as usize;
        let mut pos = 4usize;
        let mut out = Vec::with_capacity(count.min(ids.len()));
        for _ in 0..count {
            if pos + 4 > resp.len() {
                return Err(StorageError::corrupted_header("BatchGet response truncated"));
            }
            let len = read_u32_le(&resp, pos) as usize;
            pos += 4;
            if len == 0 {
                out.push(None);
            } else {
                let frame = pos
                    .checked_add(len)
                    .and_then(|end| resp.get(pos..end))
                    .ok_or_else(|| StorageError::corrupted_header("BatchGet frame truncated"))?;
                out.push(Some(AlignedBytes::try_copy_from_slice(frame)?));
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
            self.rpc(OP_BATCH_PUT, NO_KEY, &body)?;
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
            self.rpc(OP_QUERY_HYBRID, NO_KEY, &body)?;
        if status != STATUS_OK || resp.len() < 4 {
            return Err(StorageError::corrupted_header("QueryHybrid failed"));
        }
        let ids = decode_id_list(&resp);
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
            self.rpc(OP_QUERY_HYBRID, NO_KEY, &body)?;
        if status != STATUS_OK || resp.len() < 4 {
            return Err(StorageError::corrupted_header(
                "QueryHybrid paginated failed",
            ));
        }
        let ids = decode_id_list(&resp);
        Ok((ids, total_matched as usize))
    }

    /// Scrapes Prometheus/OpenMetrics text exposition from the server (`OP_METRICS`).
    pub fn scrape_metrics(&mut self) -> Result<String> {
        let (status, _, resp) = self.rpc(OP_METRICS, NO_KEY, &[])?;
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
            STATUS_OK => AlignedBytes::try_copy_from_slice(&body),
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

    /// Sends a Raft `RequestVote` RPC over TCP (`OP_RAFT_VOTE`); the body of an `Ok` response is
    /// a `RequestVoteResp` (requires the `Admin` role).
    pub fn raft_vote(&mut self, req: &RequestVoteReq) -> Result<(u8, u64, Vec<u8>)> {
        let wire = req.to_wire_bytes();
        self.rpc(OP_RAFT_VOTE, NO_KEY, &wire)
    }

    /// Sends a Raft `AppendEntries` RPC over TCP (`OP_RAFT_APPEND`); the body of an `Ok` response
    /// is an `AppendEntriesResp` (requires the `Admin` role).
    pub fn raft_append(&mut self, req: &AppendEntriesReq) -> Result<(u8, u64, Vec<u8>)> {
        let wire = req.to_wire_bytes();
        self.rpc(OP_RAFT_APPEND, NO_KEY, &wire)
    }

    /// Sends one Raft `InstallSnapshot` chunk over TCP (`OP_RAFT_SNAPSHOT`, `TASK-089`); the body
    /// of an `Ok` response is an `InstallSnapshotResp` (requires the `Admin` role).
    pub fn raft_install_snapshot(&mut self, req: &InstallSnapshotReq) -> Result<(u8, u64, Vec<u8>)> {
        let wire = req.to_wire_bytes();
        self.rpc(OP_RAFT_SNAPSHOT, NO_KEY, &wire)
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
        let (status, imported, _) = self.rpc(OP_MIGRATE_VSHARD, NO_KEY, &body)?;
        match status {
            STATUS_OK => Ok(imported),
            STATUS_UNAUTHORIZED => Err(StorageError::invalid_key("unauthorized MigrateVShard")),
            _ => Err(StorageError::corrupted_header("MigrateVShard failed")),
        }
    }

    // ── Admin Client RPCs (`TASK-061`, `F-OPS-04`) ─────────────────────────

    /// Triggers a remote online backup named `backup_name` (a relative path created under the
    /// server's configured `backup_root`) (`OP_ADMIN_BACKUP`).
    pub fn admin_backup(&mut self, backup_name: &str) -> Result<String> {
        let (status, _, resp) = self.rpc(OP_ADMIN_BACKUP, NO_KEY, backup_name.as_bytes())?;
        match status {
            STATUS_OK => Ok(String::from_utf8_lossy(&resp).into_owned()),
            STATUS_UNAUTHORIZED => Err(StorageError::invalid_key("unauthorized AdminBackup")),
            _ => Err(StorageError::corrupted_header("AdminBackup failed")),
        }
    }

    /// Triggers remote arena compaction (`OP_ADMIN_COMPACT`).
    pub fn admin_compact(&mut self) -> Result<String> {
        let (status, _, resp) = self.rpc(OP_ADMIN_COMPACT, NO_KEY, &[])?;
        match status {
            STATUS_OK => Ok(String::from_utf8_lossy(&resp).into_owned()),
            STATUS_UNAUTHORIZED => Err(StorageError::invalid_key("unauthorized AdminCompact")),
            _ => Err(StorageError::corrupted_header("AdminCompact failed")),
        }
    }

    /// Queries server health status as a JSON string (`OP_HEALTH`).
    pub fn health(&mut self) -> Result<String> {
        let (status, _, resp) = self.rpc(OP_HEALTH, NO_KEY, &[])?;
        match status {
            STATUS_OK => Ok(String::from_utf8_lossy(&resp).into_owned()),
            _ => Err(StorageError::corrupted_header("Health check failed")),
        }
    }

    /// Triggers a remote incremental checkpoint (`OP_ADMIN_CHECKPOINT`).
    pub fn admin_checkpoint(&mut self) -> Result<String> {
        let (status, _, resp) = self.rpc(OP_ADMIN_CHECKPOINT, NO_KEY, &[])?;
        match status {
            STATUS_OK => Ok(String::from_utf8_lossy(&resp).into_owned()),
            STATUS_UNAUTHORIZED => Err(StorageError::invalid_key("unauthorized AdminCheckpoint")),
            _ => Err(StorageError::corrupted_header("AdminCheckpoint failed")),
        }
    }

    // ── Document API (`TASK-098`) ──────────────────────────────────────────

    /// Reads one document (`None` if it does not exist).
    pub fn doc_get(&mut self, collection: &str, key: &str) -> Result<Option<storage_core::Document>> {
        let mut body = Vec::with_capacity(8 + collection.len() + key.len());
        storage_core::document_wire::put_str(&mut body, collection);
        storage_core::document_wire::put_str(&mut body, key);
        let (status, version, resp) = self.rpc(OP_DOC_GET, NO_KEY, &body)?;
        match status {
            STATUS_OK => {
                let fields = storage_codec::value_wire::SveReader::new(&resp).map_body()?;
                Ok(Some(storage_core::Document { key: key.to_owned(), version: Version(version), fields }))
            }
            STATUS_NOT_FOUND => Ok(None),
            s => Err(remote_error(s, version, &resp)),
        }
    }

    /// Applies document writes atomically (optionally idempotent with `request_id`).
    pub fn doc_write(
        &mut self,
        writes: &[storage_core::DocWrite],
        request_id: Option<[u8; 16]>,
    ) -> Result<storage_core::WriteSummary> {
        let mut body = Vec::new();
        storage_core::document_wire::encode_write_request(writes, request_id, &mut body);
        let (status, version, resp) = self.rpc(OP_DOC_WRITE, NO_KEY, &body)?;
        match status {
            STATUS_OK => {
                let (versions, duplicate) = storage_core::document_wire::decode_write_response(&resp)?;
                Ok(storage_core::WriteSummary { versions, duplicate })
            }
            s => Err(remote_error(s, version, &resp)),
        }
    }

    /// Runs a document query and returns one page.
    pub fn doc_query(&mut self, query: &storage_core::DocQuery) -> Result<storage_core::DocPage> {
        let mut body = Vec::new();
        storage_core::document_wire::encode_query(query, &mut body);
        let (status, version, resp) = self.rpc(OP_DOC_QUERY, NO_KEY, &body)?;
        match status {
            STATUS_OK => storage_core::document_wire::decode_page(&resp),
            s => Err(remote_error(s, version, &resp)),
        }
    }

    /// Sends a raw collection command (`OP_DOC_ADMIN` or `OP_DOC_INFO`, see `documents`).
    pub fn doc_command(&mut self, op: u8, body: &[u8]) -> Result<(u64, Vec<u8>)> {
        let (status, version, resp) = self.rpc(op, NO_KEY, body)?;
        match status {
            STATUS_OK => Ok((version, resp)),
            s => Err(remote_error(s, version, &resp)),
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
        let version = read_u64_le(&resp_hdr, 1);
        let resp_len = read_u32_le(&resp_hdr, 9) as usize;
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

/// Converts a non-OK response into a typed error.
fn remote_error(status: u8, version: u64, body: &[u8]) -> StorageError {
    match status {
        STATUS_NOT_LEADER => StorageError::NotLeader { leader: (version != 0).then_some(version) },
        STATUS_COMMIT_UNKNOWN => StorageError::CommitUnknown { index: version },
        _ => {
            // Conflict bodies start with `u32 index | u8 exists | u64 version` before the text.
            let text = if status == STATUS_CONFLICT && body.len() >= 13 { &body[13..] } else { body };
            StorageError::Rejected { status, message: String::from_utf8_lossy(text).into_owned() }
        }
    }
}

/// Decodes `[count u32 | count * 24B InlineId]`, ignoring a truncated tail or invalid ids.
fn decode_id_list(resp: &[u8]) -> Vec<InlineId> {
    let count = read_u32_le(resp, 0) as usize;
    let available = (resp.len() - 4) / 24;
    let mut ids = Vec::with_capacity(count.min(available));
    for i in 0..count.min(available) {
        if let Ok(id) = InlineId::from_raw_bytes(read_array_24(resp, 4 + i * 24)) {
            ids.push(id);
        }
    }
    ids
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
        raft::{AppendEntriesReq, RaftNode, RequestVoteReq, RequestVoteResp},
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

        // 2. Authenticate as ReadOnly -> write still rejected (rejections close the connection)
        let mut client = StorageClient::connect(addr).unwrap();
        assert_eq!(
            client.authenticate("reader-secret").unwrap(),
            RbacRole::ReadOnly
        );
        assert!(client
            .put(id1, &AgentObjectRecord::new(id1, "ReadOnly Write", 100, 10))
            .is_err());

        // 3. Upgrade to ReadWrite -> BatchPut 2 records atomically
        let mut client = StorageClient::connect(addr).unwrap();
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
        let backup_root = std::env::temp_dir().join("stge_test_tcp_admin_raft_bak");
        let _ = std::fs::remove_dir_all(&backup_root);

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
            .with_token("admin-tok", RbacRole::Admin)
            .with_backup_root(&backup_root);
        let raft = Arc::new(RaftNode::new(7, Arc::clone(&engine)).unwrap());
        let mut server = StorageServer::start_with_raft(
            "127.0.0.1:0",
            Arc::clone(&engine),
            128,
            sec,
            Some(Arc::clone(&raft)),
        )
        .unwrap();
        let addr = server.local_addr();

        // ── 1. Health check (open to ReadOnly) ─────────────────────────────
        let mut client = StorageClient::connect(addr).unwrap();
        client.authenticate("reader-tok").unwrap();
        let health = client.health().unwrap();
        assert!(health.contains("\"status\":\"ok\""));
        assert!(health.contains("\"live_records\":1"));

        // ── 2. Admin RPCs rejected for non-Admin roles (connection is closed) ──
        assert!(client.admin_compact().is_err(), "ReadOnly must not compact");
        let mut client = StorageClient::connect(addr).unwrap();
        client.authenticate("reader-tok").unwrap();
        assert!(client.admin_checkpoint().is_err());

        // ── 3. Admin RPCs (run on the background admin worker) ─────────────
        let mut client = StorageClient::connect(addr).unwrap();
        client.authenticate("admin-tok").unwrap();

        let compact_msg = client.admin_compact().unwrap();
        assert!(compact_msg.contains("compacted"));

        let backup_msg = client.admin_backup("nightly/one").unwrap();
        assert!(backup_msg.contains("backup created"));
        assert!(backup_root.join("nightly").join("one").join("backup.meta").exists());
        // Paths escaping the backup root, absolute paths and existing destinations are refused.
        for bad in ["../escape", "/etc", "C:\\Windows", "nightly/one"] {
            assert!(client.admin_backup(bad).is_err(), "{bad} must be rejected");
        }

        let ckpt_msg = client.admin_checkpoint().unwrap();
        assert!(ckpt_msg.contains("checkpoint wrote"));

        // ── 4. Raft RPCs are served by the attached RaftNode (Admin only) ──
        let mut writer_client = StorageClient::connect(addr).unwrap();
        writer_client.authenticate("writer-tok").unwrap();
        let vote_req = RequestVoteReq {
            term: 5,
            candidate_id: 1,
            last_log_index: 10,
            last_log_term: 4,
            pre_vote: false,
        };
        // ReadWrite may not drive consensus: rejected at the header stage.
        assert!(!matches!(writer_client.raft_vote(&vote_req), Ok((STATUS_OK, _, _))));

        let (status, term, resp_body) = client.raft_vote(&vote_req).unwrap();
        assert_eq!(status, STATUS_OK);
        assert_eq!(term, 5);
        let vote = RequestVoteResp::from_wire_bytes(&resp_body).unwrap();
        assert!(vote.vote_granted, "an up-to-date candidate gets the vote");
        assert_eq!(raft.voted_for(), Some(1));

        // ── 5. AppendEntries heartbeat from the leader of term 5 ───────────
        let append_req = AppendEntriesReq {
            term: 5,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        };
        let (status, term, resp_body) = client.raft_append(&append_req).unwrap();
        assert_eq!(status, STATUS_OK);
        assert_eq!(term, 5);
        let ae_resp = storage_core::raft::AppendEntriesResp::from_wire_bytes(&resp_body).unwrap();
        assert!(ae_resp.success);
        assert_eq!(ae_resp.match_index, 0);

        // ── 5b. InstallSnapshot chunk: a transfer that does not start with a valid snapshot
        //        header is refused (restart from offset 0), never applied ──
        let snap_req = storage_core::raft::InstallSnapshotReq {
            term: 5,
            leader_id: 1,
            last_included_index: 9,
            last_included_term: 5,
            offset: 0,
            data: vec![0xAB; 64],
            done: true,
        };
        let (status, term, resp_body) = client.raft_install_snapshot(&snap_req).unwrap();
        assert_eq!((status, term), (STATUS_OK, 5));
        let snap_resp = storage_core::raft::InstallSnapshotResp::from_wire_bytes(&resp_body).unwrap();
        assert_eq!((snap_resp.next_offset, snap_resp.installed), (0, false));
        assert_eq!(raft.last_applied(), 0);

        // A raft replica's engine changes only through consensus: a direct shard import into it is
        // refused (`ReadOnlyReplica`).
        let mig_id = InlineId::from_static("migrated_rec_01");
        let mig_rec = AgentObjectRecord::new(mig_id, "Migrated Shard Record", 300, 77);
        assert!(client.migrate_vshard(42u16, 2u16, &[(mig_id, mig_rec.clone())]).is_err());

        // ── 6. Shard migration RPC (Admin only) into a standalone node ──────
        let mig_dir = dir.join("standalone");
        let mig_engine = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(&mig_dir, EngineConfig::default()).unwrap(),
        );
        let mut mig_server = StorageServer::start_with_config(
            "127.0.0.1:0",
            Arc::clone(&mig_engine),
            16,
            SecurityConfig::new(true).with_token("admin-tok", RbacRole::Admin),
        )
        .unwrap();
        let mut mig_client = StorageClient::connect(mig_server.local_addr()).unwrap();
        mig_client.authenticate("admin-tok").unwrap();
        let imported = mig_client
            .migrate_vshard(42u16, 2u16, &[(mig_id, mig_rec)])
            .unwrap();
        assert_eq!(imported, 1);
        mig_server.stop();
        // Verify the migrated record actually landed in the engine
        let (_, owned) = mig_engine.get_owned(mig_id).unwrap();
        assert_eq!(owned.title, "Migrated Shard Record");
        assert_eq!(owned.score, 77);

        server.stop();
        drop(raft);
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&backup_root);
    }

    /// Every opcode (plus unknown ones) receives thousands of mutated request bodies from an
    /// Admin client; afterwards every reactor worker must still answer (`RULE-ERR-01`).
    #[test]
    fn test_hostile_request_bodies_never_crash_any_worker() {
        use storage_types::testing::SweepRng;
        let dir = std::env::temp_dir().join(format!("stge_test_tcp_fuzz_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(&dir, EngineConfig::default()).unwrap(),
        );
        let raft = Arc::new(RaftNode::new(1, Arc::clone(&engine)).unwrap());
        let sec = SecurityConfig::new(true)
            .with_token("admin-token-0123456789", RbacRole::Admin)
            .with_backup_root(dir.join("backups"));
        let mut server = StorageServer::start_with_raft(
            "127.0.0.1:0",
            Arc::clone(&engine),
            64,
            sec,
            Some(Arc::clone(&raft)),
        )
        .unwrap();
        let addr = server.local_addr();

        let id = InlineId::from_static("fuzz_rec");
        let rec = AgentObjectRecord::new(id, "fuzz", 1, 2);
        let frame = RecordCodec::encode_aligned(&rec, Version(1), 1, RecordFlags::NONE, 0);
        let frame = frame.as_slice().to_vec();
        let mut batch_put = 2u32.to_le_bytes().to_vec();
        let mut migrate = vec![1, 0, 2, 0];
        for _ in 0..2 {
            for out in [&mut batch_put, &mut migrate] {
                out.extend_from_slice(&id.to_raw_bytes());
                out.extend_from_slice(&(frame.len() as u32).to_le_bytes());
                out.extend_from_slice(&frame);
            }
        }
        let mut cas = 1u64.to_le_bytes().to_vec();
        cas.extend_from_slice(&frame);
        let mut batch_get = 2u32.to_le_bytes().to_vec();
        batch_get.extend_from_slice(&id.to_raw_bytes());
        batch_get.extend_from_slice(&NO_KEY.to_raw_bytes());
        let mut query = vec![0u8; 27];
        query[0..2].copy_from_slice(&FIELD_SCORE.to_le_bytes());
        query[10..18].copy_from_slice(&i64::MAX.to_le_bytes());
        query[18..22].copy_from_slice(&10u32.to_le_bytes());
        let vote = RequestVoteReq { term: 2, candidate_id: 9, last_log_index: 0, last_log_term: 0, pre_vote: false }
            .to_wire_bytes()
            .to_vec();
        let append = AppendEntriesReq {
            term: 2,
            leader_id: 9,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        }
        .to_wire_bytes();
        let seeds: Vec<(u8, Vec<u8>)> = vec![
            (OP_GET_RAW_FRAME, Vec::new()),
            (OP_PUT_RAW_FRAME, frame.clone()),
            (OP_DELETE, Vec::new()),
            (OP_STATS, Vec::new()),
            (OP_CAS_PUT, cas),
            (OP_BATCH_GET, batch_get),
            (OP_BATCH_PUT, batch_put),
            (OP_QUERY_HYBRID, query.clone()),
            (OP_QUERY_HYBRID, query[..22].to_vec()),
            (OP_AUTH, b"admin-token-0123456789".to_vec()),
            (OP_METRICS, Vec::new()),
            (OP_RAFT_VOTE, vote),
            (OP_RAFT_APPEND, append),
            (OP_RAFT_SNAPSHOT, vec![7u8; 45]),
            (OP_MIGRATE_VSHARD, migrate),
            (OP_ADMIN_BACKUP, b"nightly/one".to_vec()),
            (OP_HEALTH, Vec::new()),
            (OP_ADMIN_CHECKPOINT, Vec::new()),
            (0x00, Vec::new()),
            (0xFF, vec![1, 2, 3]),
        ];

        let connect = || {
            let mut c = StorageClient::connect(addr).unwrap();
            c.authenticate("admin-token-0123456789").unwrap();
            c
        };
        let mut client = connect();
        let mut rng = SweepRng::new(41);
        for _ in 0..3_000 {
            let (op, base) = &seeds[rng.below(seeds.len())];
            let op = if rng.below(50) == 0 { rng.next_u64() as u8 } else { *op };
            let body = rng.hostile_copy(base);
            let key = if rng.below(4) == 0 {
                let mut raw = [0u8; 24];
                raw.iter_mut().for_each(|b| *b = rng.next_u64() as u8);
                InlineId::from_raw_bytes(raw).unwrap_or(id)
            } else {
                id
            };
            // A rejected request may close the connection; reconnect and continue.
            if client.rpc(op, key, &body).is_err() {
                client = connect();
            }
        }

        // Every worker must still be alive: fresh connections (spread across all reactor shards)
        // answer a health check within the timeout.
        for _ in 0..32 {
            let mut s = TcpStream::connect(addr).unwrap();
            s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
            let token = b"admin-token-0123456789";
            let mut hdr = [0u8; 29];
            hdr[0] = OP_AUTH;
            hdr[1..25].copy_from_slice(&NO_KEY.to_raw_bytes());
            hdr[25..29].copy_from_slice(&(token.len() as u32).to_le_bytes());
            s.write_all(&hdr).unwrap();
            s.write_all(token).unwrap();
            let mut resp = [0u8; 13];
            s.read_exact(&mut resp).expect("reactor worker stopped responding");
            let len = u32::from_le_bytes([resp[9], resp[10], resp[11], resp[12]]) as usize;
            let mut sink = vec![0u8; len];
            s.read_exact(&mut sink).unwrap();
            hdr[0] = OP_HEALTH;
            hdr[25..29].copy_from_slice(&0u32.to_le_bytes());
            s.write_all(&hdr).unwrap();
            s.read_exact(&mut resp).expect("reactor worker stopped responding");
            assert_eq!(resp[0], STATUS_OK);
        }
        server.stop();
        drop(raft);
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_rpc_latency_histograms_and_slow_log_are_recorded_and_exported() {
        let dir = std::env::temp_dir().join(format!("stge_test_tcp_metrics_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(&dir, EngineConfig::default()).unwrap(),
        );
        let mut server = StorageServer::start("127.0.0.1:0", Arc::clone(&engine)).unwrap();
        server.set_slow_operation_threshold_ns(0);
        let mut c = StorageClient::connect(server.local_addr()).unwrap();
        let id = InlineId::from_static("m1");
        for i in 0..5 {
            c.put(id, &AgentObjectRecord::new(id, "m", 1, i)).unwrap();
            c.get_frame(id).unwrap();
        }
        let prom = c.scrape_metrics().unwrap();
        assert!(prom.contains("storine_read_latency_ns"), "{prom}");
        assert!(prom.contains("storine_write_latency_ns"), "{prom}");
        assert!(prom.contains("storine_slow_rpcs"), "{prom}");
        let slow = server.slow_operations();
        assert!(slow.iter().any(|e| e.operation == "put"));
        assert!(slow.iter().any(|e| e.operation == "get"));
        server.stop();
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_header_stage_admission_and_hostile_inputs() {
        let dir = std::env::temp_dir().join("stge_test_tcp_hostile");
        let _ = std::fs::remove_dir_all(&dir);
        let engine = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(&dir, EngineConfig::default()).unwrap(),
        );
        // No auth on a public address is refused; require_auth without tokens is refused.
        assert!(StorageServer::start("0.0.0.0:0", Arc::clone(&engine)).is_err());
        assert!(StorageServer::start_with_config(
            "127.0.0.1:0",
            Arc::clone(&engine),
            16,
            SecurityConfig::new(true)
        )
        .is_err());

        let sec = SecurityConfig::new(true).with_token("rw", RbacRole::ReadWrite);
        let mut server =
            StorageServer::start_with_config("127.0.0.1:0", Arc::clone(&engine), 16, sec).unwrap();
        let addr = server.local_addr();

        let send_raw = |hdr: [u8; 29], body: &[u8]| -> Option<u8> {
            let mut s = TcpStream::connect(addr).ok()?;
            let _ = s.set_read_timeout(Some(Duration::from_secs(5)));
            s.write_all(&hdr).ok()?;
            let _ = s.write_all(body);
            let mut resp = [0u8; 13];
            s.read_exact(&mut resp).ok()?;
            Some(resp[0])
        };
        let header = |op: u8, body_len: u32| {
            let mut h = [0u8; 29];
            h[0] = op;
            h[1..25].copy_from_slice(&NO_KEY.to_raw_bytes());
            h[25..29].copy_from_slice(&body_len.to_le_bytes());
            h
        };

        // Unauthenticated client announcing a 256 MiB body is rejected without allocation.
        assert_eq!(
            send_raw(header(OP_PUT_RAW_FRAME, MAX_PAYLOAD_SIZE), &[]),
            Some(STATUS_UNAUTHORIZED)
        );
        // Oversized auth token is rejected before reading it.
        assert_eq!(send_raw(header(OP_AUTH, 1 << 20), &[]), Some(STATUS_ERROR));

        // Authenticated BatchPut claiming u32::MAX items must not crash the server.
        let mut c = StorageClient::connect(addr).unwrap();
        c.authenticate("rw").unwrap();
        let (status, _, _) = c
            .rpc(OP_BATCH_PUT, NO_KEY, &u32::MAX.to_le_bytes())
            .unwrap();
        assert_eq!(status, STATUS_INVALID, "malformed requests are reported as Invalid");
        // The server is still alive and serving.
        let id = InlineId::from_static("alive");
        assert_eq!(c.put(id, &AgentObjectRecord::new(id, "ok", 1, 1)).unwrap(), Version(1));

        server.stop();
        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
