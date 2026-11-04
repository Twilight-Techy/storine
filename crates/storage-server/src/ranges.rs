//! Range-cluster serving (`TASK-143`): `storage-server --ranges`.
//!
//! Each node runs the range layer ([`storage_core::dist::runtime::RangeNode`]) over its engine
//! and serves the document API by coordinating distributed transactions itself
//! ([`storage_core::dist::docs`] through a [`Gateway`]): clients and SDKs connect to any node.
//!
//! Nodes talk over the same port with two Admin-role opcodes:
//! - `OP_RANGE_BATCH`: one encoded driver batch, answered as soon as it is queued (Raft retries
//!   lost batches, so delivery is best effort);
//! - `OP_KV`: one encoded KV request, answered with the encoded reply (runs on the worker
//!   pool, never on a reactor thread).
//!
//! [`TcpRangeTransport`] sends batches through one sender thread per peer with a bounded queue
//! (a slow or unreachable peer never stalls the event loop: its overflow is dropped), and KV
//! calls over a small pool of connections per peer.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use storage_codec::PropertyValue;
use storage_core::analytics::{AggregateQuery, AggregateResult, ScanPage, ScanQuery};
use storage_core::dist::docs;
use storage_core::dist::hlc::Hlc;
use storage_core::dist::meta::RangeIndex;
use storage_core::dist::runtime::{Gateway, GatewayConfig, RangeNode, Transport};
use storage_core::{ByteRange, CollectionInfo, DocPage, DocQuery, DocWrite, Document, NodeId, WriteSummary};
use storage_types::{Result, StorageError};

use crate::cluster::WorkerPool;
use crate::documents::DocBackend;
use crate::{StorageClient, OP_KV, OP_RANGE_BATCH, STATUS_OK};

/// Batches waiting for one peer before new ones are dropped.
const PEER_QUEUE: usize = 1024;
/// Idle KV connections kept per peer.
const POOLED_CONNECTIONS: usize = 8;

/// Sends range-layer traffic to peers over TCP.
pub struct TcpRangeTransport {
    me: NodeId,
    peers: HashMap<NodeId, SocketAddr>,
    token: Option<String>,
    timeout: Duration,
    senders: Mutex<HashMap<NodeId, SyncSender<Vec<u8>>>>,
    idle: Mutex<HashMap<NodeId, Vec<StorageClient>>>,
    /// The local node: calls to ourselves skip the network.
    local: RwLock<Option<Arc<RangeNode>>>,
    /// This transport (its sender threads hold it weakly).
    weak: std::sync::Weak<TcpRangeTransport>,
}

impl core::fmt::Debug for TcpRangeTransport {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TcpRangeTransport").field("me", &self.me).field("peers", &self.peers).finish_non_exhaustive()
    }
}

impl TcpRangeTransport {
    /// `peers`: node id -> address of every node (this one included); `token`: an Admin token
    /// every peer accepts.
    pub fn new(me: NodeId, peers: HashMap<NodeId, SocketAddr>, token: Option<String>, timeout: Duration) -> Arc<Self> {
        Arc::new_cyclic(|weak| Self { me, peers, token, timeout, senders: Mutex::new(HashMap::new()), idle: Mutex::new(HashMap::new()), local: RwLock::new(None), weak: weak.clone() })
    }

    pub fn set_local(&self, node: Arc<RangeNode>) {
        *self.local.write().unwrap_or_else(|p| p.into_inner()) = Some(node);
    }

    fn connect(&self, to: NodeId, timeout: Duration) -> Result<StorageClient> {
        let addr = *self.peers.get(&to).ok_or_else(|| StorageError::invalid_key("unknown range peer"))?;
        let mut c = StorageClient::connect_timeout(addr, timeout)?;
        if let Some(ref t) = self.token {
            c.authenticate(t)?;
        }
        Ok(c)
    }

    /// The sender thread of `to`, started on first use.
    fn sender(&self, to: NodeId) -> Option<SyncSender<Vec<u8>>> {
        let mut senders = self.senders.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(s) = senders.get(&to) {
            return Some(s.clone());
        }
        let (tx, rx) = mpsc::sync_channel::<Vec<u8>>(PEER_QUEUE);
        let me = self.weak.clone();
        std::thread::Builder::new().name(format!("range-send-{to}")).spawn(move || peer_sender(me, to, rx)).ok()?;
        senders.insert(to, tx.clone());
        Some(tx)
    }
}

/// Sends one peer's batches in order, reconnecting after failures (with a short pause, so an
/// unreachable peer costs no CPU).
fn peer_sender(transport: std::sync::Weak<TcpRangeTransport>, to: NodeId, rx: Receiver<Vec<u8>>) {
    let mut conn: Option<StorageClient> = None;
    while let Ok(batch) = rx.recv() {
        let Some(t) = transport.upgrade() else { return };
        if conn.is_none() {
            match t.connect(to, t.timeout) {
                Ok(c) => conn = Some(c),
                Err(_) => {
                    // Drop what queued meanwhile: Raft resends what still matters.
                    while rx.try_recv().is_ok() {}
                    std::thread::sleep(Duration::from_millis(100));
                    continue;
                }
            }
        }
        if let Some(c) = conn.as_mut() {
            if !matches!(c.raw_call(OP_RANGE_BATCH, &batch), Ok((STATUS_OK, _, _))) {
                conn = None;
            }
        }
    }
}

impl Transport for TcpRangeTransport {
    fn send_batch(&self, to: NodeId, batch: Vec<u8>) {
        if to == self.me {
            return;
        }
        if let Some(tx) = self.sender(to) {
            // Full queue: drop (bounded memory; Raft retries).
            let _ = tx.try_send(batch);
        }
    }

    fn call(&self, to: NodeId, request: Vec<u8>, timeout: Duration) -> Result<Vec<u8>> {
        if to == self.me {
            if let Some(n) = self.local.read().unwrap_or_else(|p| p.into_inner()).clone() {
                return Ok(n.handle_encoded(&request, timeout));
            }
        }
        let pooled = self.idle.lock().unwrap_or_else(|p| p.into_inner()).get_mut(&to).and_then(Vec::pop);
        let mut conn = match pooled {
            Some(c) => c,
            None => self.connect(to, timeout + Duration::from_secs(1))?,
        };
        let (status, _, body) = conn.raw_call(OP_KV, &request)?;
        if status != STATUS_OK {
            return Err(StorageError::Rejected { status, message: String::from_utf8_lossy(&body).into_owned() });
        }
        let mut idle = self.idle.lock().unwrap_or_else(|p| p.into_inner());
        let slot = idle.entry(to).or_default();
        if slot.len() < POOLED_CONNECTIONS {
            slot.push(conn);
        }
        Ok(body)
    }
}

/// What a range-mode server holds.
pub struct RangeContext {
    pub(crate) node: Arc<RangeNode>,
    pub(crate) pool: Arc<WorkerPool>,
    transport: Arc<TcpRangeTransport>,
    gateway: GatewayConfig,
    hlc: Arc<Hlc>,
    cache: Arc<RwLock<RangeIndex>>,
    plain: Arc<RwLock<BTreeMap<String, std::time::Instant>>>,
}

impl core::fmt::Debug for RangeContext {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RangeContext").field("node", &self.node.id()).finish_non_exhaustive()
    }
}

/// Everything a range-mode server needs (see [`crate::StorageServer::start_documents_ranges`]).
pub struct RangeOptions {
    pub node: Arc<RangeNode>,
    pub transport: Arc<TcpRangeTransport>,
    pub gateway: GatewayConfig,
    pub hlc: Arc<Hlc>,
}

impl core::fmt::Debug for RangeOptions {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RangeOptions").field("node", &self.node.id()).finish_non_exhaustive()
    }
}

impl RangeContext {
    pub(crate) fn new(o: RangeOptions, workers: usize) -> Self {
        Self { node: o.node, pool: WorkerPool::new(workers, 16_384), transport: o.transport, gateway: o.gateway, hlc: o.hlc, cache: Arc::new(RwLock::new(RangeIndex::new())), plain: Arc::default() }
    }

    pub(crate) fn backend(&self) -> RangeBackend {
        RangeBackend { gw: Mutex::new(Gateway::new(Arc::clone(&self.transport) as Arc<dyn Transport>, self.gateway.clone(), Arc::clone(&self.hlc), Arc::clone(&self.cache)).sharing_collections(Arc::clone(&self.plain))) }
    }

    /// `OP_CLUSTER_INFO` body: every node coordinates, so each reports itself as the node to
    /// write to; members are every node of the cluster (their client address is their range
    /// address: one port).
    pub(crate) fn cluster_info(&self) -> Vec<u8> {
        use storage_core::document_wire::put_str;
        let me = self.transport.me;
        let mut out = Vec::with_capacity(128);
        out.extend_from_slice(&u64::from(me).to_le_bytes());
        out.push(1);
        out.extend_from_slice(&u64::from(me).to_le_bytes());
        put_str(&mut out, &self.transport.peers.get(&me).map(SocketAddr::to_string).unwrap_or_default());
        for _ in 0..3 {
            out.extend_from_slice(&0u64.to_le_bytes());
        }
        let mut ids: Vec<&NodeId> = self.transport.peers.keys().collect();
        ids.sort_unstable();
        out.extend_from_slice(&(ids.len() as u32).to_le_bytes());
        for id in &ids {
            out.extend_from_slice(&u64::from(**id).to_le_bytes());
            put_str(&mut out, &self.transport.peers[*id].to_string());
        }
        out.extend(ids.iter().map(|_| 1u8));
        out
    }

    pub(crate) fn kv_timeout(&self) -> Duration {
        self.gateway.call_timeout
    }
}

/// The document API through a gateway (one per request).
pub(crate) struct RangeBackend {
    gw: Mutex<Gateway>,
}

impl RangeBackend {
    fn with<R>(&self, f: impl FnOnce(&mut Gateway) -> Result<R>) -> Result<R> {
        let mut g = self.gw.lock().unwrap_or_else(|p| p.into_inner());
        f(&mut g)
    }
}

/// Only the named paths of `fields` (dotted paths reach into nested maps).
fn project(fields: &BTreeMap<String, PropertyValue>, paths: &[String]) -> BTreeMap<String, PropertyValue> {
    let mut out = BTreeMap::new();
    for p in paths {
        let parts: Vec<&str> = p.split('.').collect();
        let mut cur = fields;
        let mut found = None;
        for (i, part) in parts.iter().enumerate() {
            match cur.get(*part) {
                Some(PropertyValue::Map(m)) if i + 1 < parts.len() => cur = m,
                Some(v) if i + 1 == parts.len() => found = Some(v.clone()),
                _ => break,
            }
        }
        let Some(v) = found else { continue };
        insert_path(&mut out, &parts, v);
    }
    out
}

/// Puts `v` at `parts` in `map`, creating the nested maps on the way.
fn insert_path(map: &mut BTreeMap<String, PropertyValue>, parts: &[&str], v: PropertyValue) {
    match parts {
        [] => {}
        [last] => {
            map.insert(last.to_string(), v);
        }
        [first, rest @ ..] => {
            let e = map.entry(first.to_string()).or_insert_with(|| PropertyValue::Map(BTreeMap::new()));
            if !matches!(e, PropertyValue::Map(_)) {
                *e = PropertyValue::Map(BTreeMap::new());
            }
            if let PropertyValue::Map(m) = e {
                insert_path(m, rest, v);
            }
        }
    }
}

impl DocBackend for RangeBackend {
    fn get(&self, c: &str, k: &str) -> Result<Option<Document>> {
        self.with(|g| docs::get(g, c, k))
    }

    fn get_fields(&self, c: &str, k: &str, paths: &[String]) -> Result<Option<Document>> {
        if paths.len() > storage_core::MAX_PROJECTION_FIELDS {
            return Err(StorageError::invalid_value("too many projected fields (at most 256)"));
        }
        Ok(self.get(c, k)?.map(|d| Document { fields: project(&d.fields, paths), ..d }))
    }

    fn read_bytes(&self, c: &str, k: &str, path: &str, offset: u64, len: usize) -> Result<Option<ByteRange>> {
        if len > storage_core::MAX_BYTE_RANGE {
            return Err(StorageError::invalid_value("a ranged read returns at most 16 MiB"));
        }
        let Some(doc) = self.get(c, k)? else { return Ok(None) };
        let value = project(&doc.fields, &[path.to_string()]);
        let mut cur = &value;
        let mut found = None;
        let parts: Vec<&str> = path.split('.').collect();
        for (i, part) in parts.iter().enumerate() {
            match cur.get(*part) {
                Some(PropertyValue::Map(m)) if i + 1 < parts.len() => cur = m,
                Some(v) if i + 1 == parts.len() => found = Some(v),
                _ => break,
            }
        }
        let bytes: &[u8] = match found {
            Some(PropertyValue::Bytes(b)) => b,
            Some(PropertyValue::Text(t)) => t.as_bytes(),
            Some(_) => return Err(StorageError::invalid_value("ranged reads need a bytes or text field")),
            None => &[],
        };
        let start = (offset as usize).min(bytes.len());
        let end = start.saturating_add(len).min(bytes.len());
        Ok(Some(ByteRange { version: doc.version, total: bytes.len() as u64, data: bytes[start..end].to_vec() }))
    }

    fn write(&self, writes: Vec<DocWrite>, request_id: Option<[u8; 16]>) -> Result<WriteSummary> {
        self.with(|g| docs::write(g, writes, request_id))
    }

    fn query(&self, q: &DocQuery) -> Result<DocPage> {
        self.with(|g| docs::query(g, q))
    }

    fn aggregate(&self, q: &AggregateQuery) -> Result<AggregateResult> {
        // On the coordinator, over one snapshot (the single-node row-store path).
        let docs = self.with(|g| docs::snapshot(g, &q.collection))?;
        storage_core::analytics::run_documents(q, &docs)
    }

    fn scan_columns(&self, q: &ScanQuery) -> Result<ScanPage> {
        let docs = self.with(|g| docs::snapshot(g, &q.collection))?;
        storage_core::analytics::scan_documents(q, &docs)
    }

    fn collection_info(&self, c: &str) -> Result<CollectionInfo> {
        self.with(|g| docs::collection_info(g, c))
    }

    fn list_collections(&self) -> Result<Vec<CollectionInfo>> {
        self.with(|g| docs::list_collections(g))
    }

    fn create_index(&self, c: &str, f: &str) -> Result<()> {
        self.with(|g| docs::create_index(g, c, f))
    }

    fn drop_index(&self, c: &str, f: &str) -> Result<()> {
        self.with(|g| docs::drop_index(g, c, f))
    }

    fn drop_collection(&self, c: &str) -> Result<usize> {
        self.with(|g| docs::drop_collection(g, c))
    }

    fn set_collection_extra(&self, c: &str, extra: BTreeMap<String, PropertyValue>) -> Result<()> {
        self.with(|g| docs::set_collection_extra(g, c, extra))
    }

    fn create_vector_index(&self, c: &str, f: &str, dims: usize) -> Result<()> {
        self.with(|g| docs::create_vector_index(g, c, f, dims))
    }

    fn drop_vector_index(&self, c: &str, f: &str) -> Result<()> {
        self.with(|g| docs::drop_vector_index(g, c, f))
    }

    fn create_columnar(&self, c: &str) -> Result<()> {
        self.with(|g| docs::set_columnar(g, c, true))
    }

    fn drop_columnar(&self, c: &str) -> Result<()> {
        self.with(|g| docs::set_columnar(g, c, false))
    }
}

/// How to start one range-cluster node ([`start_range_server`]).
#[derive(Debug, Clone)]
pub struct RangeServerConfig {
    pub data_dir: std::path::PathBuf,
    pub bind_addr: String,
    pub node_id: NodeId,
    /// Every node of the cluster (this one included) and its address.
    pub peers: HashMap<NodeId, SocketAddr>,
    /// Admin token nodes authenticate to each other with (needed with authentication).
    pub token: Option<String>,
    pub security: crate::SecurityConfig,
    pub max_connections: usize,
    pub max_offset: Duration,
}

/// Starts one node of a range cluster: on first start it holds one range over the whole key
/// space replicated on every listed node; a restart reopens its ranges.
pub fn start_range_server(cfg: RangeServerConfig) -> Result<crate::StorageServer<storage_core::DocumentRecord>> {
    use storage_core::dist::hlc::SystemClock;
    use storage_core::dist::node::{NodeConfig, NodeDriver};
    use storage_core::dist::runtime::RuntimeConfig;
    use storage_core::dist::server::NodeServer;
    use storage_core::{DocumentRecord, EngineConfig, StorageEngine};

    if !cfg.peers.contains_key(&cfg.node_id) {
        return Err(StorageError::invalid_key("the peer list must include this node"));
    }
    let mut ids: Vec<NodeId> = cfg.peers.keys().copied().collect();
    ids.sort_unstable();
    let engine = Arc::new(StorageEngine::<DocumentRecord>::open(&cfg.data_dir, EngineConfig { collection_name: "ranges".into(), wal_sync_policy: storage_core::dist::range_wal_policy(), ..EngineConfig::default() })?);
    let incarnation = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_micros() as u64);
    let node_cfg = NodeConfig {
        seed: u64::from(cfg.node_id),
        incarnation,
        cluster: ids.clone(),
        allocator: Some(storage_core::dist::alloc::AllocatorConfig::default()),
        max_offset_ns: cfg.max_offset.as_nanos() as u64,
        ..NodeConfig::default()
    };
    let mut driver = NodeDriver::new(cfg.node_id, Arc::clone(&engine), node_cfg);
    if driver.reopen_ranges()?.is_empty() {
        driver.open_range(1, storage_core::raft::KeySpan::all(), &ids)?;
    }
    let hlc = Arc::new(Hlc::new(Arc::new(SystemClock), cfg.max_offset));
    let transport = TcpRangeTransport::new(cfg.node_id, cfg.peers.clone(), cfg.token.clone(), Duration::from_secs(2));
    let node = RangeNode::start(NodeServer::new(driver, Arc::clone(&hlc)), Arc::clone(&transport) as Arc<dyn Transport>, RuntimeConfig::default());
    transport.set_local(Arc::clone(&node));
    let gateway = GatewayConfig { nodes: ids, request_timeout: Duration::from_secs(10), call_timeout: Duration::from_secs(5) };
    crate::StorageServer::start_documents_ranges(&cfg.bind_addr, engine, cfg.max_connections, cfg.security, RangeOptions { node, transport, gateway, hlc })
}
