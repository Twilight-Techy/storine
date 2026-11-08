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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use storage_codec::PropertyValue;
use storage_core::analytics::{AggregateQuery, AggregateResult, ScanPage, ScanQuery};
use storage_core::dist::backup::{self, BackupStore};
use storage_core::dist::txn::{KvSender, TxnError};
use storage_core::dist::{docs, feed};
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
    /// Bytes of batches queued for every peer, and batches dropped on a full queue (metrics).
    queued_bytes: Arc<AtomicU64>,
    dropped: AtomicU64,
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
        Arc::new_cyclic(|weak| Self { me, peers, token, timeout, senders: Mutex::new(HashMap::new()), queued_bytes: Arc::new(AtomicU64::new(0)), dropped: AtomicU64::new(0), idle: Mutex::new(HashMap::new()), local: RwLock::new(None), weak: weak.clone() })
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
        let (me, queued) = (self.weak.clone(), Arc::clone(&self.queued_bytes));
        std::thread::Builder::new().name(format!("range-send-{to}")).spawn(move || peer_sender(me, to, rx, &queued)).ok()?;
        senders.insert(to, tx.clone());
        Some(tx)
    }
}

/// Sends one peer's batches in order, reconnecting after failures (with a short pause, so an
/// unreachable peer costs no CPU).
fn peer_sender(transport: std::sync::Weak<TcpRangeTransport>, to: NodeId, rx: Receiver<Vec<u8>>, queued: &AtomicU64) {
    let mut conn: Option<StorageClient> = None;
    while let Ok(batch) = rx.recv() {
        queued.fetch_sub(batch.len() as u64, Ordering::Relaxed);
        let Some(t) = transport.upgrade() else { return };
        if conn.is_none() {
            match t.connect(to, t.timeout) {
                Ok(c) => conn = Some(c),
                Err(_) => {
                    // Drop what queued meanwhile: Raft resends what still matters.
                    while let Ok(b) = rx.try_recv() {
                        queued.fetch_sub(b.len() as u64, Ordering::Relaxed);
                    }
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
            let len = batch.len() as u64;
            self.queued_bytes.fetch_add(len, Ordering::Relaxed);
            if tx.try_send(batch).is_err() {
                self.queued_bytes.fetch_sub(len, Ordering::Relaxed);
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
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
    /// `OP_KV` from other nodes' coordinators.
    pub(crate) kv_pool: Arc<WorkerPool>,
    analytics: Arc<AtomicUsize>,
    transport: Arc<TcpRangeTransport>,
    gateway: GatewayConfig,
    hlc: Arc<Hlc>,
    cache: Arc<RwLock<RangeIndex>>,
    plain: Arc<RwLock<BTreeMap<String, std::time::Instant>>>,
    /// A backup or restore is running on this node.
    backup_running: Arc<AtomicBool>,
}

/// Holds this node's backup slot until dropped.
pub(crate) struct BackupSlot(Arc<AtomicBool>);

impl Drop for BackupSlot {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
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

/// A running analytic query's place; gives it back when dropped.
pub(crate) struct AnalyticsPermit(Arc<AtomicUsize>);

impl Drop for AnalyticsPermit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl RangeContext {
    pub(crate) fn analytics_permit(&self) -> Option<AnalyticsPermit> {
        if self.analytics.fetch_add(1, Ordering::AcqRel) >= crate::MAX_CONCURRENT_ANALYTICS {
            self.analytics.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(AnalyticsPermit(Arc::clone(&self.analytics)))
    }

    pub(crate) fn new(o: RangeOptions, workers: usize) -> Self {
        Self { node: o.node, pool: WorkerPool::new(workers, 16_384), kv_pool: WorkerPool::new(workers, 16_384), analytics: Arc::default(), transport: o.transport, gateway: o.gateway, hlc: o.hlc, cache: Arc::new(RwLock::new(RangeIndex::new())), plain: Arc::default(), backup_running: Arc::default() }
    }

    /// The node's backup slot, unless a backup or restore holds it (each occupies a worker for as
    /// long as it runs).
    pub(crate) fn backup_slot(&self) -> Option<BackupSlot> {
        (!self.backup_running.swap(true, Ordering::AcqRel)).then(|| BackupSlot(Arc::clone(&self.backup_running)))
    }

    pub(crate) fn backend(&self) -> RangeBackend {
        RangeBackend { gw: Mutex::new(Gateway::new(Arc::clone(&self.transport) as Arc<dyn Transport>, self.gateway.clone(), Arc::clone(&self.hlc), Arc::clone(&self.cache)).sharing_collections(Arc::clone(&self.plain))) }
    }

    /// `OP_CLUSTER_INFO` body: every node coordinates, so each reports itself as the node to
    /// write to; members are every node of the cluster (their client address is their range
    /// address: one port).
    /// `OP_HEALTH` in range mode: what the node holds in memory, for operators and the soak.
    pub(crate) fn health(&self) -> Vec<u8> {
        format!(
            "{{\"status\":\"ok\",\"mode\":\"ranges\",{},\"outbound_batch_bytes\":{},\"dropped_batches\":{}{}}}",
            self.node.gauges(),
            self.transport.queued_bytes.load(Ordering::Relaxed),
            self.transport.dropped.load(Ordering::Relaxed),
            crate::heap_fields()
        )
        .into_bytes()
    }

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

/// A backup as a directory: `seg-<n>` files and, last, `MANIFEST`.
#[derive(Debug)]
pub struct FileBackupStore {
    dir: std::path::PathBuf,
}

impl FileBackupStore {
    /// A new backup in a directory that must not exist yet.
    pub fn create(dir: &std::path::Path) -> Result<Self> {
        if let Some(parent) = dir.parent() {
            std::fs::create_dir_all(parent).map_err(|e| StorageError::io("create backup parent", e))?;
        }
        std::fs::create_dir(dir).map_err(|e| StorageError::io("create backup directory (the name must be new)", e))?;
        Ok(Self { dir: dir.to_path_buf() })
    }

    /// An existing backup.
    pub fn open(dir: &std::path::Path) -> Self {
        Self { dir: dir.to_path_buf() }
    }

    fn segment_path(&self, index: u64) -> std::path::PathBuf {
        self.dir.join(format!("seg-{index:08}"))
    }

    fn sync_dir(&self) -> Result<()> {
        std::fs::File::open(&self.dir).and_then(|d| d.sync_all()).map_err(|e| StorageError::io("sync backup directory", e))
    }
}

impl BackupStore for FileBackupStore {
    fn put_segment(&mut self, index: u64, bytes: &[u8]) -> Result<()> {
        use std::io::Write;
        let mut f = std::fs::File::create(self.segment_path(index)).map_err(|e| StorageError::io("create backup segment", e))?;
        f.write_all(bytes).and_then(|()| f.sync_all()).map_err(|e| StorageError::io("write backup segment", e))?;
        self.sync_dir()
    }

    fn put_manifest(&mut self, bytes: &[u8]) -> Result<()> {
        storage_mmap::atomic_write_replace(&self.dir.join("MANIFEST"), bytes, true)
    }

    fn segment(&self, index: u64) -> Result<Vec<u8>> {
        std::fs::read(self.segment_path(index)).map_err(|e| StorageError::io("read backup segment", e))
    }

    fn manifest(&self) -> Result<Vec<u8>> {
        std::fs::read(self.dir.join("MANIFEST")).map_err(|e| StorageError::io("read backup manifest", e))
    }
}

type Reply = (u8, u64, Vec<u8>);

/// Takes a cluster backup into the new directory `path`; a failed attempt leaves nothing there.
pub(crate) fn run_backup(backend: &RangeBackend, path: &std::path::Path) -> Reply {
    let mut store = match FileBackupStore::create(path) {
        Ok(s) => s,
        Err(e) => return crate::documents::error_reply(&e),
    };
    match backend.with(|g| backup::backup(g, &mut store)) {
        Ok(m) => (STATUS_OK, m.rows, format!("cluster backup created: ts={}.{} rows={} segments={}", m.ts.wall, m.ts.logical, m.rows, m.segments.len()).into_bytes()),
        Err(e) => {
            // Our own directory, created above; without a manifest it is not a backup.
            let _ = std::fs::remove_dir_all(path);
            crate::documents::error_reply(&e)
        }
    }
}

/// Restores the backup in `path` into this cluster.
pub(crate) fn run_restore(backend: &RangeBackend, path: &std::path::Path) -> Reply {
    let store = FileBackupStore::open(path);
    match backend.with(|g| backup::restore(g, &store)) {
        Ok(m) => (STATUS_OK, m.rows, format!("cluster backup restored: ts={}.{} rows={}", m.ts.wall, m.ts.logical, m.rows).into_bytes()),
        Err(e) => crate::documents::error_reply(&e),
    }
}

/// One page of a changefeed.
pub(crate) fn run_changefeed(backend: &RangeBackend, body: &[u8]) -> Reply {
    use crate::{STATUS_INVALID, STATUS_OVERLOADED};
    let (cursor, collection, max_keys) = match feed::decode_request(body) {
        Ok(r) => r,
        Err(e) => return (STATUS_INVALID, 0, e.to_string().into_bytes()),
    };
    if let Some(c) = &collection {
        if let Err(e) = storage_core::document::validate_collection_name(c, false) {
            return (STATUS_INVALID, 0, e.to_string().into_bytes());
        }
    }
    let max_keys = if max_keys == 0 { feed::DEFAULT_PAGE_KEYS } else { max_keys };
    let mut g = backend.gw.lock().unwrap_or_else(|p| p.into_inner());
    let cursor = cursor.unwrap_or_else(|| {
        let now = g.now();
        feed::Cursor::at(&mut *g, now)
    });
    match feed::poll(&mut *g, &cursor, collection.as_deref(), max_keys) {
        Ok(page) => (STATUS_OK, 0, page.encode()),
        Err(e @ (TxnError::HistoryLost(_) | TxnError::Invalid(_))) => (STATUS_INVALID, 0, e.to_string().into_bytes()),
        // Nothing was consumed: the same cursor works again.
        Err(e @ TxnError::Retry(_)) => (STATUS_OVERLOADED, 0, e.to_string().into_bytes()),
        Err(_) => crate::documents::error_reply(&StorageError::NotLeader { leader: None }),
    }
}

/// A changefeed sink: changes as JSON lines in `file`, the cursor beside it. Each page is durable
/// before its cursor is saved. On open, a line torn by a crash is cut off; the page it belonged
/// to is delivered again.
#[derive(Debug)]
pub struct JsonlSink {
    file: std::fs::File,
    cursor_path: std::path::PathBuf,
}

impl JsonlSink {
    pub fn open(file: &std::path::Path, cursor_path: &std::path::Path) -> Result<Self> {
        let existing = match std::fs::read(file) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(StorageError::io("read changefeed file", e)),
        };
        let f = std::fs::OpenOptions::new().create(true).append(true).open(file).map_err(|e| StorageError::io("open changefeed file", e))?;
        if !existing.is_empty() && existing.last() != Some(&b'\n') {
            let keep = existing.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
            f.set_len(keep as u64).map_err(|e| StorageError::io("cut torn changefeed line", e))?;
        }
        Ok(Self { file: f, cursor_path: cursor_path.to_path_buf() })
    }

    /// The saved cursor, if any.
    pub fn load_cursor(&self) -> Result<Option<feed::Cursor>> {
        match std::fs::read(&self.cursor_path) {
            Ok(b) => feed::Cursor::decode(&b).map(Some),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::io("read changefeed cursor", e)),
        }
    }

    fn append(&mut self, text: &str) -> Result<()> {
        use std::io::Write;
        self.file.write_all(text.as_bytes()).and_then(|()| self.file.sync_data()).map_err(|e| StorageError::io("write changefeed file", e))
    }
}

impl feed::Sink for JsonlSink {
    fn deliver(&mut self, changes: &[feed::Change]) -> Result<()> {
        let mut text = String::new();
        for c in changes {
            text.push_str(&c.to_json());
            text.push('\n');
        }
        self.append(&text)
    }

    fn save_cursor(&mut self, cursor: &feed::Cursor) -> Result<()> {
        storage_mmap::atomic_write_replace(&self.cursor_path, &cursor.encode(), true)
    }

    fn resolved(&mut self, ts: storage_core::dist::hlc::Timestamp) -> Result<()> {
        self.append(&format!("{{\"resolved\":\"{}.{}\"}}\n", ts.wall, ts.logical))
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
