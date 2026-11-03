//! The range layer in real time (`TASK-143`): each node's event loop, the transport between
//! nodes, and the gateway that turns any node into a coordinator.
//!
//! - [`RangeNode`]: one thread owns a node's [`NodeServer`]. It steps the batches other nodes
//!   send, ticks the driver every [`RuntimeConfig::tick`], evaluates KV requests and sends their
//!   replies. Nothing else touches the server, so it needs no locks (the simulator drives the
//!   same code deterministically).
//! - [`Transport`]: how nodes reach each other: fire-and-forget batches (Raft traffic, one per
//!   node pair per tick at most) and KV calls (a request and its reply), both in the
//!   [`crate::dist::wire`] encoding. [`LocalTransport`] connects nodes in one process (tests);
//!   the server crate provides TCP.
//! - [`Gateway`]: a [`KvSender`] for coordinators on any node or client. It routes each request
//!   to its range's leaseholder through a range cache filled from the meta range, follows
//!   `NotLeader` redirects and refreshes on `WrongRange`, within a deadline. Clients therefore
//!   connect to any node (SDKs need no range awareness, `TASK-143`'s decision).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use storage_types::{Result, StorageError};

use crate::dist::hlc::{Hlc, Timestamp};
use crate::dist::meta::RangeIndex;
use crate::dist::mvcc::TxnId;
use crate::dist::server::{KvRequest, KvResponse, MetaOp, NodeServer};
use crate::dist::txn::KvSender;
use crate::dist::wire;
use crate::sharding::NodeId;

/// Runtime knobs.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    /// How often the driver ticks (elections, heartbeats, the allocator).
    pub tick: Duration,
    /// Most KV requests queued at a node before it answers "overloaded" (backpressure).
    pub max_queued: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self { tick: Duration::from_millis(10), max_queued: 65_536 }
    }
}

/// How nodes reach each other.
pub trait Transport: Send + Sync {
    /// Sends an encoded batch to `to` (best effort: Raft retries).
    fn send_batch(&self, to: NodeId, batch: Vec<u8>);
    /// Sends an encoded KV request to `to` and waits up to `timeout` for the encoded reply.
    fn call(&self, to: NodeId, request: Vec<u8>, timeout: Duration) -> Result<Vec<u8>>;
}

enum Input {
    Batch(Vec<u8>),
    Request(KvRequest, SyncSender<KvResponse>),
    Stop,
}

/// A running node: its event-loop thread and the queue feeding it.
pub struct RangeNode {
    id: NodeId,
    tx: Sender<Input>,
    queued: Arc<AtomicU64>,
    max_queued: usize,
    thread: Mutex<Option<JoinHandle<()>>>,
    stopped: AtomicBool,
}

impl core::fmt::Debug for RangeNode {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RangeNode").field("id", &self.id).finish_non_exhaustive()
    }
}

/// Microseconds since the Unix epoch (the driver's time base).
fn now_us() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_micros() as u64)
}

impl RangeNode {
    /// Starts `server`'s event loop. Batches it produces go out through `transport`.
    pub fn start(server: NodeServer, transport: Arc<dyn Transport>, cfg: RuntimeConfig) -> Arc<Self> {
        let id = server.driver.id();
        let (tx, rx) = mpsc::channel();
        let queued = Arc::new(AtomicU64::new(0));
        let q = Arc::clone(&queued);
        let tick = cfg.tick;
        let thread = std::thread::Builder::new()
            .name(format!("range-node-{id}"))
            .spawn(move || event_loop(server, rx, transport, tick, q))
            .ok();
        Arc::new(Self { id, tx, queued, max_queued: cfg.max_queued, thread: Mutex::new(thread), stopped: AtomicBool::new(false) })
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    /// Hands an encoded batch from another node to the event loop.
    pub fn deliver(&self, batch: Vec<u8>) {
        let _ = self.tx.send(Input::Batch(batch));
    }

    /// Evaluates `req` here (leaseholder, or follower read) and waits for the reply.
    pub fn request(&self, req: KvRequest, timeout: Duration) -> Result<KvResponse> {
        if self.queued.load(Ordering::Relaxed) as usize >= self.max_queued {
            return Err(StorageError::resource_exhausted("range node request queue full"));
        }
        let (reply_tx, reply_rx) = mpsc::sync_channel(1);
        self.queued.fetch_add(1, Ordering::Relaxed);
        if self.tx.send(Input::Request(req, reply_tx)).is_err() {
            self.queued.fetch_sub(1, Ordering::Relaxed);
            return Err(StorageError::NotLeader { leader: None });
        }
        reply_rx.recv_timeout(timeout).map_err(|_| StorageError::CommitUnknown { index: 0 })
    }

    /// Handles an encoded request and returns the encoded reply (transports call this).
    pub fn handle_encoded(&self, req: &[u8], timeout: Duration) -> Vec<u8> {
        let resp = match wire::decode_request(req) {
            Ok(r) => self.request(r, timeout).unwrap_or_else(|e| match e {
                StorageError::CommitUnknown { .. } => KvResponse::Error("timeout".into()),
                e => KvResponse::Error(e.to_string()),
            }),
            Err(e) => KvResponse::Error(e.to_string()),
        };
        wire::encode_response(&resp)
    }

    /// Stops the event loop (pending requests are dropped: their callers time out).
    pub fn stop(&self) {
        if !self.stopped.swap(true, Ordering::AcqRel) {
            let _ = self.tx.send(Input::Stop);
            if let Some(t) = self.thread.lock().unwrap_or_else(|p| p.into_inner()).take() {
                let _ = t.join();
            }
        }
    }
}

impl Drop for RangeNode {
    fn drop(&mut self) {
        self.stop();
    }
}

fn event_loop(mut srv: NodeServer, rx: Receiver<Input>, transport: Arc<dyn Transport>, tick: Duration, queued: Arc<AtomicU64>) {
    // Request ids are this loop's own: replies go back through their channels.
    let trace = std::env::var_os("STORINE_NODE_TRACE").is_some();
    if trace {
        srv.enable_trace();
    }
    let mut waiting: BTreeMap<u64, (Instant, SyncSender<KvResponse>)> = BTreeMap::new();
    let mut next_id = 0u64;
    let mut last_tick = Instant::now();
    loop {
        let wait = tick.saturating_sub(last_tick.elapsed());
        let first = match rx.recv_timeout(wait) {
            Ok(i) => Some(i),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => return,
        };
        // Drain what is queued now, then make progress once (bounded batches per pass).
        let loop_trace = std::env::var_os("STORINE_LOOP_TRACE").is_some();
        let pass_start = Instant::now();
        let mut kinds = (0usize, 0usize);
        let inputs = first.into_iter().chain(std::iter::from_fn(|| rx.try_recv().ok()).take(4096));
        let now = now_us();
        srv.driver.set_clock(srv.hlc().now());
        for input in inputs {
            match input {
                Input::Stop => return,
                Input::Batch(bytes) => {
                    kinds.0 += 1;
                    let Ok(batch) = wire::decode_batch(&bytes) else { continue };
                    // A batch from a node whose clock is beyond the offset bound is refused.
                    if srv.hlc().update(batch.ts).is_err() {
                        continue;
                    }
                    let _ = srv.driver.step(batch, now);
                }
                Input::Request(req, reply) => {
                    kinds.1 += 1;
                    queued.fetch_sub(1, Ordering::Relaxed);
                    next_id += 1;
                    waiting.insert(next_id, (Instant::now(), reply));
                    srv.submit(next_id, 0, req);
                }
            }
        }
        if last_tick.elapsed() >= tick {
            last_tick = Instant::now();
            let _ = srv.driver.tick(now);
        }
        srv.poll(now.saturating_mul(1_000));
        if trace {
            for line in srv.take_trace() {
                eprintln!("{line}");
            }
        }
        for r in srv.take_replies() {
            if let Some((_, tx)) = waiting.remove(&r.id) {
                let _ = tx.try_send(r.resp);
            }
        }
        srv.driver.set_clock(srv.hlc().now());
        let out = srv.driver.drain_outbox();
        if loop_trace && (kinds.0 + kinds.1 > 0 || out.iter().any(|(_, b)| !b.msgs.is_empty())) {
            eprintln!("loop n{} t={} batches_in {} requests_in {} out {:?} pass {}us", srv.driver.id(), now % 10_000_000, kinds.0, kinds.1, out.iter().map(|(to, b)| (*to, b.msgs.len())).collect::<Vec<_>>(), pass_start.elapsed().as_micros());
        }
        for (to, batch) in out {
            transport.send_batch(to, wire::encode_batch(&batch));
        }
        // Bounded memory: a request that never applies (its proposal was lost with a leader
        // change) is forgotten after a minute; its caller has long timed out.
        if waiting.len() > 1024 {
            waiting.retain(|_, (since, _)| since.elapsed() < Duration::from_secs(60));
        }
    }
}

/// Nodes of one process, connected directly (tests, single-process clusters).
#[derive(Default)]
pub struct LocalTransport {
    nodes: RwLock<BTreeMap<NodeId, Arc<RangeNode>>>,
    /// Pairs whose batches are dropped (partition tests).
    cut: RwLock<Vec<(NodeId, NodeId)>>,
}

impl core::fmt::Debug for LocalTransport {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("LocalTransport").finish_non_exhaustive()
    }
}

impl LocalTransport {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register(&self, node: Arc<RangeNode>) {
        self.nodes.write().unwrap_or_else(|p| p.into_inner()).insert(node.id(), node);
    }

    pub fn unregister(&self, id: NodeId) -> Option<Arc<RangeNode>> {
        self.nodes.write().unwrap_or_else(|p| p.into_inner()).remove(&id)
    }

    /// Drops all batches between `a` and `b` until [`Self::heal`].
    pub fn partition(&self, a: NodeId, b: NodeId) {
        self.cut.write().unwrap_or_else(|p| p.into_inner()).push((a, b));
    }

    pub fn heal(&self) {
        self.cut.write().unwrap_or_else(|p| p.into_inner()).clear();
    }

    fn node(&self, id: NodeId) -> Option<Arc<RangeNode>> {
        self.nodes.read().unwrap_or_else(|p| p.into_inner()).get(&id).cloned()
    }
}

impl Transport for LocalTransport {
    fn send_batch(&self, to: NodeId, batch: Vec<u8>) {
        let from = u16::from_le_bytes([batch.get(1).copied().unwrap_or(0), batch.get(2).copied().unwrap_or(0)]);
        if self.cut.read().unwrap_or_else(|p| p.into_inner()).iter().any(|&(a, b)| (a, b) == (from, to) || (b, a) == (from, to)) {
            return;
        }
        if let Some(n) = self.node(to) {
            n.deliver(batch);
        }
    }

    fn call(&self, to: NodeId, request: Vec<u8>, timeout: Duration) -> Result<Vec<u8>> {
        let n = self.node(to).ok_or(StorageError::NotLeader { leader: None })?;
        Ok(n.handle_encoded(&request, timeout))
    }
}

/// Gateway knobs.
#[derive(Clone, Debug)]
pub struct GatewayConfig {
    /// Every node that may answer (meta lookups start anywhere).
    pub nodes: Vec<NodeId>,
    /// One request's budget across redirects and retries.
    pub request_timeout: Duration,
    /// One call's wait for its reply.
    pub call_timeout: Duration,
}

/// A coordinator's view of the cluster: routes requests to leaseholders (see the module docs).
pub struct Gateway {
    transport: Arc<dyn Transport>,
    cfg: GatewayConfig,
    hlc: Arc<Hlc>,
    cache: Arc<RwLock<RangeIndex>>,
}

/// Transaction ids are unique per process (a counter) and across processes (a seed from the
/// start time, the process id and the address of a static, mixed).
static TXN_IDS: AtomicU64 = AtomicU64::new(1);
static TXN_SEED: std::sync::OnceLock<u64> = std::sync::OnceLock::new();

fn txn_seed() -> u64 {
    *TXN_SEED.get_or_init(|| now_us() ^ (u64::from(std::process::id())).rotate_left(32) ^ (&TXN_IDS as *const AtomicU64 as u64).rotate_left(17))
}

impl core::fmt::Debug for Gateway {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Gateway").field("nodes", &self.cfg.nodes).finish_non_exhaustive()
    }
}

impl Gateway {
    /// A gateway sharing `cache` (one per process is enough; it is only ever a hint).
    pub fn new(transport: Arc<dyn Transport>, cfg: GatewayConfig, hlc: Arc<Hlc>, cache: Arc<RwLock<RangeIndex>>) -> Self {
        Self { transport, cfg, hlc, cache }
    }

    fn call(&self, to: NodeId, req: &KvRequest) -> Result<KvResponse> {
        let t0 = Instant::now();
        let bytes = self.transport.call(to, wire::encode_request(req), self.cfg.call_timeout)?;
        let resp = wire::decode_response(&bytes);
        if std::env::var_os("STORINE_GW_TRACE").is_some() {
            let what = format!("{req:?}");
            eprintln!("gw {}us n{to} {:.90} -> {:.60}", t0.elapsed().as_micros(), what, format!("{resp:?}"));
        }
        resp
    }

    /// Refreshes the cache entry for `anchor` from the meta range (any node points at its
    /// leaseholder).
    fn lookup(&self, anchor: &[u8], deadline: Instant) -> Result<()> {
        let mut to = self.cfg.nodes.first().copied().ok_or(StorageError::NotLeader { leader: None })?;
        let mut tried = 0usize;
        while Instant::now() < deadline {
            match self.call(to, &KvRequest::Meta(MetaOp::Lookup { key: anchor.to_vec(), limit: 8 })) {
                Ok(KvResponse::Ranges(ds)) => {
                    let mut c = self.cache.write().unwrap_or_else(|p| p.into_inner());
                    for d in ds {
                        c.learn(d);
                    }
                    if c.lookup(anchor).is_some() {
                        return Ok(());
                    }
                    drop(c);
                    std::thread::sleep(Duration::from_millis(20)); // not published yet
                }
                Ok(KvResponse::NotLeader { leader: Some(l) }) => to = l,
                _ => {
                    tried += 1;
                    to = self.cfg.nodes[tried % self.cfg.nodes.len()];
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
        Err(StorageError::NotLeader { leader: None })
    }

    /// Sends one request to its leaseholder, following redirects until the deadline.
    pub fn route(&self, req: &KvRequest) -> Result<KvResponse> {
        let deadline = Instant::now() + self.cfg.request_timeout;
        let anchor = req.anchor();
        let mut target: Option<NodeId> = None;
        let mut spin = 0u32;
        loop {
            if Instant::now() >= deadline {
                return Err(StorageError::NotLeader { leader: None });
            }
            let to = match target {
                Some(t) => t,
                None => {
                    let cached = self.cache.read().unwrap_or_else(|p| p.into_inner()).lookup(&anchor).and_then(|d| d.candidates().first().copied());
                    match cached {
                        Some(n) => n,
                        None => {
                            self.lookup(&anchor, deadline)?;
                            continue;
                        }
                    }
                }
            };
            match self.call(to, req) {
                Ok(KvResponse::NotLeader { leader }) => {
                    target = leader.filter(|&l| l != to);
                    if target.is_none() {
                        // No leader known there: ask the cluster again shortly.
                        self.evict(&anchor);
                        spin += 1;
                        std::thread::sleep(Duration::from_millis(u64::from(spin.min(10)) * 10));
                    } else {
                        // (The read guard must be gone before the write: `RwLock` would deadlock.)
                        let range = self.cache.read().unwrap_or_else(|p| p.into_inner()).lookup(&anchor).map(|d| d.range);
                        if let (Some(r), Some(l)) = (range, target) {
                            self.cache.write().unwrap_or_else(|p| p.into_inner()).set_lease(r, l);
                        }
                    }
                }
                Ok(KvResponse::WrongRange) => {
                    self.evict(&anchor);
                    target = None;
                }
                Ok(KvResponse::Error(e)) if e == "timeout" => {
                    target = None;
                }
                Ok(other) => return Ok(other),
                Err(_) => {
                    // Unreachable: try another member of the range.
                    self.evict(&anchor);
                    target = None;
                    spin += 1;
                    std::thread::sleep(Duration::from_millis(u64::from(spin.min(10)) * 10));
                }
            }
        }
    }

    fn evict(&self, anchor: &[u8]) {
        let mut c = self.cache.write().unwrap_or_else(|p| p.into_inner());
        if let Some(r) = c.lookup(anchor).map(|d| d.range) {
            c.evict(r);
        }
    }
}

impl KvSender for Gateway {
    fn send(&mut self, reqs: Vec<KvRequest>) -> Vec<std::result::Result<KvResponse, String>> {
        if reqs.len() <= 1 {
            return reqs.iter().map(|r| self.route(r).map_err(|e| e.to_string())).collect();
        }
        // In parallel: a parallel commit is one round trip only if its parts travel together.
        let me: &Self = self;
        std::thread::scope(|s| {
            let handles: Vec<_> = reqs.iter().map(|r| s.spawn(move || me.route(r).map_err(|e| e.to_string()))).collect();
            handles.into_iter().map(|h| h.join().unwrap_or_else(|_| Err("request panicked".into()))).collect()
        })
    }

    fn now(&mut self) -> Timestamp {
        self.hlc.now()
    }

    fn max_offset_ns(&self) -> u64 {
        self.hlc.max_offset_ns()
    }

    fn sleep(&mut self, us: u64) {
        std::thread::sleep(Duration::from_micros(us));
    }

    fn new_id(&mut self) -> TxnId {
        // Unique per gateway process: its seed, a counter, and the clock (no shared state).
        let n = TXN_IDS.fetch_add(1, Ordering::Relaxed);
        let seed = txn_seed();
        let mut id = [0u8; 16];
        let mix = |x: u64| {
            let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        };
        id[..8].copy_from_slice(&mix(seed ^ n).to_le_bytes());
        id[8..].copy_from_slice(&mix(now_us() ^ n.rotate_left(17) ^ seed.rotate_left(7)).to_le_bytes());
        id
    }
}
