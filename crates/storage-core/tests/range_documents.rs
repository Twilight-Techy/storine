//! The document API on a real-time range cluster in one process (`TASK-143`): three nodes on
//! their own event-loop threads, connected by `LocalTransport`, used through gateways that
//! route to leaseholders.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use storage_codec::field_ops::FieldOp;
use storage_codec::PropertyValue;
use storage_core::dist::docs;
use storage_core::dist::hlc::{Hlc, SystemClock, Timestamp};
use storage_core::dist::mvcc::TxnId;
use storage_core::dist::meta::RangeIndex;
use storage_core::dist::node::{NodeConfig, NodeDriver};
use storage_core::dist::runtime::{Gateway, GatewayConfig, LocalTransport, RangeNode, RuntimeConfig};
use storage_core::dist::server::{KvRequest, KvResponse, NodeServer};
use storage_core::dist::txn::KvSender;
use storage_core::engine::Precondition;
use storage_core::raft::KeySpan;
use storage_core::{DocOp, DocQuery, DocWrite, Direction, DocumentRecord, EngineConfig, Filter, StorageEngine};
use storage_types::{StorageError, Version};

struct Cluster {
    transport: Arc<LocalTransport>,
    nodes: Vec<Arc<RangeNode>>,
    cache: Arc<RwLock<RangeIndex>>,
    root: std::path::PathBuf,
}

const MEMBERS: [u16; 3] = [1, 2, 3];

fn hlc() -> Arc<Hlc> {
    Arc::new(Hlc::new(Arc::new(SystemClock), Duration::from_millis(500)))
}

fn start(name: &str) -> Cluster {
    start_with(name, RuntimeConfig::default())
}

fn start_with(name: &str, runtime: RuntimeConfig) -> Cluster {
    let root = std::env::temp_dir().join(format!("storine_range_docs_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let transport = LocalTransport::new();
    let mut nodes = Vec::new();
    for &id in &MEMBERS {
        let engine = StorageEngine::<DocumentRecord>::open(root.join(format!("n{id}")), EngineConfig { background_flush: false, collection_name: "node".into(), layout_mode: storage_core::StorageLayoutMode::AdaptiveArena { arena_threshold_bytes: 256 * 1024, segment_capacity: 16 * 1024 * 1024 }, wal_sync_policy: storage_wal::SyncPolicy::AsyncMemoryFirst, ..EngineConfig::default() }).unwrap();
        let cfg = NodeConfig { seed: u64::from(id), cluster: MEMBERS.to_vec(), max_offset_ns: 500_000_000, ..NodeConfig::default() };
        let mut d = NodeDriver::new(id, Arc::new(engine), cfg);
        d.open_range(1, KeySpan::all(), &MEMBERS).unwrap();
        let node = RangeNode::start(NodeServer::new(d, hlc()), transport.clone(), runtime.clone());
        transport.register(Arc::clone(&node));
        nodes.push(node);
    }
    Cluster { transport, nodes, cache: Arc::new(RwLock::new(RangeIndex::new())), root }
}

impl Cluster {
    fn gateway(&self) -> Gateway {
        let cfg = GatewayConfig { nodes: MEMBERS.to_vec(), request_timeout: Duration::from_secs(20), call_timeout: Duration::from_secs(5) };
        Gateway::new(self.transport.clone(), cfg, hlc(), Arc::clone(&self.cache))
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for n in &self.nodes {
            n.stop();
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn fields(pairs: &[(&str, PropertyValue)]) -> BTreeMap<String, PropertyValue> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

fn put(c: &str, k: &str, f: BTreeMap<String, PropertyValue>, pre: Precondition) -> DocWrite {
    DocWrite { collection: c.into(), key: k.into(), op: DocOp::Put(f), precondition: pre }
}

#[test]
fn documents_on_a_range_cluster() {
    let cluster = start("api");
    let mut g = cluster.gateway();
    let s = &mut g;

    // Insert, read back, insert again (refused), conditional replace.
    let w = docs::write(s, vec![put("users", "ada", fields(&[("age", PropertyValue::Int64(36))]), Precondition::Absent)], None).unwrap();
    let v1 = w.versions[0].unwrap();
    let doc = docs::get(s, "users", "ada").unwrap().unwrap();
    assert_eq!((doc.version, doc.fields.get("age")), (v1, Some(&PropertyValue::Int64(36))));
    assert!(matches!(docs::write(s, vec![put("users", "ada", BTreeMap::new(), Precondition::Absent)], None), Err(StorageError::ConditionFailed { .. })));
    assert!(matches!(docs::write(s, vec![put("users", "ada", BTreeMap::new(), Precondition::Version(Version(v1.0 + 7)))], None), Err(StorageError::ConditionFailed { actual: Some(v), .. }) if v == v1));
    let v2 = docs::write(s, vec![put("users", "ada", fields(&[("age", PropertyValue::Int64(37))]), Precondition::Version(v1))], None).unwrap().versions[0].unwrap();
    assert!(v2 > v1);

    // A failed write in a batch writes nothing.
    let batch = vec![put("users", "bob", BTreeMap::new(), Precondition::None), put("users", "ada", BTreeMap::new(), Precondition::Absent)];
    assert!(docs::write(s, batch, None).is_err());
    assert!(docs::get(s, "users", "bob").unwrap().is_none());

    // Money-safe field operations: checked, bounded.
    docs::write(s, vec![put("acct", "a", fields(&[("bal", PropertyValue::Int64(10))]), Precondition::None)], None).unwrap();
    let take = |n: i64| DocWrite { collection: "acct".into(), key: "a".into(), op: DocOp::Patch(vec![FieldOp::Add { path: "bal".into(), delta: PropertyValue::Int64(-n), min: Some(PropertyValue::Int64(0)), max: None }]), precondition: Precondition::None };
    docs::write(s, vec![take(4)], None).unwrap();
    assert!(docs::write(s, vec![take(7)], None).is_err(), "would go below zero");
    assert_eq!(docs::get(s, "acct", "a").unwrap().unwrap().fields.get("bal"), Some(&PropertyValue::Int64(6)));
    let missing = DocWrite { collection: "acct".into(), key: "nobody".into(), op: DocOp::Patch(vec![]), precondition: Precondition::None };
    assert!(matches!(docs::write(s, vec![missing], None), Err(StorageError::RecordNotFound { .. })));

    // Delete, then re-create: versions continue above the old ones (no ABA).
    let del = DocWrite { collection: "users".into(), key: "ada".into(), op: DocOp::Delete, precondition: Precondition::Exists };
    assert_eq!(docs::write(s, vec![del], None).unwrap().versions, vec![None]);
    assert!(docs::get(s, "users", "ada").unwrap().is_none());
    let v3 = docs::write(s, vec![put("users", "ada", BTreeMap::new(), Precondition::Absent)], None).unwrap().versions[0].unwrap();
    assert!(v3 > v2, "{v3:?} must exceed {v2:?}");

    // Idempotent request ids: a retry applies nothing and reports the first outcome.
    let rid = Some([7u8; 16]);
    let first = docs::write(s, vec![take(1)], rid).unwrap();
    let again = docs::write(s, vec![take(1)], rid).unwrap();
    assert!(again.duplicate && again.versions == first.versions);
    assert_eq!(docs::get(s, "acct", "a").unwrap().unwrap().fields.get("bal"), Some(&PropertyValue::Int64(5)));

    // Queries: filter, sort, offset / limit, key pages; long keys of long collection names.
    let long = "k".repeat(40);
    let mut batch = Vec::new();
    for i in 0..30i64 {
        batch.push(put("products", &format!("p{i:02}"), fields(&[("price", PropertyValue::Int64(i * 3 % 17)), ("i", PropertyValue::Int64(i))]), Precondition::None));
    }
    batch.push(put("products", &long, fields(&[("price", PropertyValue::Int64(100))]), Precondition::None));
    docs::write(s, batch, None).unwrap();
    let q = DocQuery { collection: "products".into(), filter: Some(Filter::Ge("price".into(), PropertyValue::Int64(10))), order_by: vec![("price".into(), Direction::Desc)], limit: Some(3), ..DocQuery::default() };
    let page = docs::query(s, &q).unwrap();
    assert_eq!(page.documents[0].key, long, "the long key is found");
    assert_eq!(page.total, 1 + (0..30).filter(|i| i * 3 % 17 >= 10).count());
    let mut keys = Vec::new();
    let mut after = None;
    loop {
        let q = DocQuery { collection: "products".into(), limit: Some(7), after_key: after.clone(), ..DocQuery::default() };
        let p = docs::query(s, &q).unwrap();
        keys.extend(p.documents.iter().map(|d| d.key.clone()));
        match p.next_after_key {
            Some(k) => after = Some(k),
            None => break,
        }
    }
    let mut want: Vec<String> = (0..30).map(|i| format!("p{i:02}")).chain([long.clone()]).collect();
    want.sort();
    assert_eq!(keys, want);

    // Metadata: collections listed, indexes and extra recorded.
    docs::create_index(s, "products", "price").unwrap();
    docs::set_collection_extra(s, "products", fields(&[("schema", PropertyValue::Text("t".into()))])).unwrap();
    let info = docs::collection_info(s, "products").unwrap();
    assert_eq!(info.indexes, vec!["price".to_string()]);
    assert_eq!(info.extra.get("schema"), Some(&PropertyValue::Text("t".into())));
    let names: Vec<String> = docs::list_collections(s).unwrap().into_iter().map(|c| c.name).collect();
    assert!(["acct", "products", "users"].iter().all(|n| names.contains(&n.to_string())), "{names:?}");
    assert_eq!(docs::drop_collection(s, "products").unwrap(), 31);
    assert_eq!(docs::query(s, &DocQuery { collection: "products".into(), ..DocQuery::default() }).unwrap().total, 0);

    // Refused precisely, never mis-executed.
    assert!(docs::get(s, "_txn", "x").is_err());

    // Time-to-live: readable, then absent; its version floor remains.
    let ttl = DocWrite { collection: "c".into(), key: "k".into(), op: DocOp::PutWithTtl(BTreeMap::new(), Duration::from_millis(300)), precondition: Precondition::None };
    let tv = docs::write(s, vec![ttl], None).unwrap().versions[0].unwrap();
    assert!(docs::get(s, "c", "k").unwrap().is_some());
    std::thread::sleep(Duration::from_millis(400));
    assert!(docs::get(s, "c", "k").unwrap().is_none(), "expired");
    let again = docs::write(s, vec![put("c", "k", BTreeMap::new(), Precondition::Absent)], None).unwrap().versions[0].unwrap();
    assert!(again > tv);

    // Vector search: exact, filtered, and the index's dimensions bind writes.
    docs::create_vector_index(s, "emb", "e", 2).unwrap();
    let vec_of = |a: f64, b: f64| PropertyValue::List(vec![PropertyValue::Float64(a), PropertyValue::Float64(b)]);
    let w = vec![put("emb", "x", fields(&[("e", vec_of(1.0, 0.0))]), Precondition::None), put("emb", "y", fields(&[("e", vec_of(0.0, 1.0))]), Precondition::None)];
    docs::write(s, w, None).unwrap();
    assert!(docs::write(s, vec![put("emb", "bad", fields(&[("e", PropertyValue::Int64(1))]), Precondition::None)], None).is_err());
    let q = DocQuery { collection: "emb".into(), nearest: Some(storage_core::document::Nearest { field: "e".into(), vector: vec![0.9, 0.1], k: 1, exact: false }), ..DocQuery::default() };
    assert_eq!(docs::query(s, &q).unwrap().documents[0].key, "x");
}

/// A range replicated on one node is its own quorum (it once never committed its first entry).
#[test]
fn a_single_voter_range_serves() {
    let root = std::env::temp_dir().join(format!("storine_range_docs_single_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let transport = LocalTransport::new();
    let engine = StorageEngine::<DocumentRecord>::open(root.join("n1"), EngineConfig { background_flush: false, collection_name: "node".into(), ..EngineConfig::default() }).unwrap();
    let mut d = NodeDriver::new(1, Arc::new(engine), NodeConfig { seed: 1, cluster: vec![1], max_offset_ns: 500_000_000, ..NodeConfig::default() });
    d.open_range(1, KeySpan::all(), &[1]).unwrap();
    let node = RangeNode::start(NodeServer::new(d, hlc()), transport.clone(), RuntimeConfig::default());
    transport.register(Arc::clone(&node));
    let cfg = GatewayConfig { nodes: vec![1], request_timeout: Duration::from_secs(20), call_timeout: Duration::from_secs(5) };
    let mut g = Gateway::new(transport.clone(), cfg, hlc(), Arc::new(RwLock::new(RangeIndex::new())));
    docs::write(&mut g, vec![put("c", "k", BTreeMap::new(), Precondition::Absent)], None).unwrap();
    assert!(docs::get(&mut g, "c", "k").unwrap().is_some());
    node.stop();
    let _ = std::fs::remove_dir_all(&root);
}

/// Concurrent transfers through gateways on several threads, while a node stops and comes
/// back: every transfer is atomic and the total is conserved.
#[test]
fn concurrent_transfers_conserve_money() {
    let cluster = start("money");
    let mut g = cluster.gateway();
    let accounts = 8;
    let batch = (0..accounts).map(|i| put("bank", &format!("a{i}"), fields(&[("bal", PropertyValue::Int64(100))]), Precondition::None)).collect();
    docs::write(&mut g, batch, None).unwrap();
    let threads: Vec<_> = (0..4)
        .map(|t| {
            let mut g = cluster.gateway();
            std::thread::spawn(move || {
                let mut ok = 0;
                for n in 0..25usize {
                    let (a, b) = ((t + n) % accounts, (t * 3 + n * 5 + 1) % accounts);
                    if a == b {
                        continue;
                    }
                    let add = |k: usize, d: i64| DocWrite {
                        collection: "bank".into(),
                        key: format!("a{k}"),
                        op: DocOp::Patch(vec![FieldOp::Add { path: "bal".into(), delta: PropertyValue::Int64(d), min: Some(PropertyValue::Int64(0)), max: None }]),
                        precondition: Precondition::None,
                    };
                    let rid = Some([(t * 64 + n) as u8; 16]);
                    if docs::write(&mut g, vec![add(a, -3), add(b, 3)], rid).is_ok() {
                        ok += 1;
                    }
                }
                ok
            })
        })
        .collect();
    std::thread::sleep(Duration::from_millis(300));
    // A node goes away for a while (its replicas stop; the others keep a quorum).
    let gone = cluster.transport.unregister(3);
    std::thread::sleep(Duration::from_millis(1500));
    if let Some(n) = gone {
        cluster.transport.register(n);
    }
    let committed: usize = threads.into_iter().map(|t| t.join().unwrap()).sum();
    assert!(committed > 0);
    let q = DocQuery { collection: "bank".into(), ..DocQuery::default() };
    let total: i64 = docs::query(&mut g, &q).unwrap().documents.iter().map(|d| match d.fields.get("bal") {
        Some(PropertyValue::Int64(v)) => *v,
        _ => 0,
    }).sum();
    assert_eq!(total, 100 * accounts as i64, "{committed} transfers committed");
}

/// Contended read-modify-writes of one document: one-phase commits queue on the key's latch, so
/// every update lands exactly once, quickly, and retries of one request id apply once.
#[test]
fn contended_updates_commit_once_each() {
    let cluster = start("contended");
    let mut g = cluster.gateway();
    docs::write(&mut g, vec![put("hot", "k", fields(&[("n", PropertyValue::Int64(0))]), Precondition::None)], None).unwrap();
    let inc = || DocWrite { collection: "hot".into(), key: "k".into(), op: DocOp::Patch(vec![FieldOp::Add { path: "n".into(), delta: PropertyValue::Int64(1), min: None, max: None }]), precondition: Precondition::None };
    let t0 = std::time::Instant::now();
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let mut g = cluster.gateway();
            std::thread::spawn(move || {
                for _ in 0..25 {
                    let inc = DocWrite { collection: "hot".into(), key: "k".into(), op: DocOp::Patch(vec![FieldOp::Add { path: "n".into(), delta: PropertyValue::Int64(1), min: None, max: None }]), precondition: Precondition::None };
                    docs::write(&mut g, vec![inc], None).unwrap();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    eprintln!("200 contended updates in {:?}", t0.elapsed());
    assert_eq!(docs::get(&mut g, "hot", "k").unwrap().unwrap().fields.get("n"), Some(&PropertyValue::Int64(200)));
    // Retries of one request id, whichever path they take, apply once.
    let rid = Some([9u8; 16]);
    assert!(!docs::write(&mut g, vec![inc()], rid).unwrap().duplicate);
    assert!(docs::write(&mut g, vec![inc()], rid).unwrap().duplicate);
    assert_eq!(docs::get(&mut g, "hot", "k").unwrap().unwrap().fields.get("n"), Some(&PropertyValue::Int64(201)));
    // History stays bounded on a hot key.
    assert!(docs::get(&mut g, "hot", "k").unwrap().is_some());
}

/// A batch writing one key twice is not its own retry, in either commit path (a full
/// transaction once read its own buffered request id back and dropped the whole batch).
#[test]
fn a_batch_may_write_one_key_twice() {
    let cluster = start("twice");
    let mut g = cluster.gateway();
    let n = |v: i64| fields(&[("n", PropertyValue::Int64(v))]);
    // A new collection: a full transaction. A known one: one phase.
    for collection in ["fresh", "fresh"] {
        let key = format!("k{}", docs::query(&mut g, &DocQuery { collection: collection.into(), ..DocQuery::default() }).unwrap().total);
        let batch = vec![put(collection, &key, n(1), Precondition::None), put(collection, "other", n(2), Precondition::None), put(collection, &key, n(3), Precondition::None)];
        let w = docs::write(&mut g, batch, Some([key.len() as u8 + key.as_bytes()[1]; 16])).unwrap();
        assert!(!w.duplicate);
        assert_eq!(docs::get(&mut g, collection, &key).unwrap().unwrap().fields.get("n"), Some(&PropertyValue::Int64(3)), "{key}");
        assert_eq!(docs::get(&mut g, collection, "other").unwrap().unwrap().fields.get("n"), Some(&PropertyValue::Int64(2)));
    }
}

/// A key rewritten beyond its history bounds: a protected feed gets every version; an unprotected
/// one is told it lost history, never handed a gap (`TASK-144`).
#[test]
fn changefeeds_get_every_version_or_fail_loudly() {
    use storage_core::dist::feed;
    use storage_core::dist::txn::{KvSender, TxnError};
    let c = start("feed");
    let mut g = c.gateway();
    let put = |n: i64| DocWrite { collection: "hot".into(), key: "k".into(), op: DocOp::Put(BTreeMap::from([("n".to_string(), PropertyValue::Int64(n))])), precondition: Precondition::None };
    docs::write(&mut g, vec![put(-1)], None).unwrap();
    let start_ts = g.now();
    let protected = feed::Cursor::at(&mut g, start_ts);
    // A first poll asks for protection...
    let first = feed::poll(&mut g, &protected, Some("hot"), 16).unwrap();
    // ...and this one claims to have just renewed it, so it never does.
    let mut bare = feed::Cursor::at(&mut g, start_ts);
    bare.renewed = g.now().wall;
    for n in 0..150 {
        docs::write(&mut g, vec![put(n)], None).unwrap();
    }
    // Protection covers every key of these nodes, so the bare cursor still works for now.
    assert!(feed::poll(&mut g, &bare, Some("hot"), 16).is_ok());
    let mut seen = Vec::new();
    let mut cur = first.cursor;
    loop {
        let page = feed::poll(&mut g, &cur, Some("hot"), 16).unwrap();
        for ch in &page.changes {
            seen.push(ch.fields().and_then(|f| f.get("n").cloned()));
        }
        cur = page.cursor;
        if page.resolved.is_some() {
            break;
        }
    }
    let want: Vec<_> = (0..150).map(|n| Some(PropertyValue::Int64(n))).collect();
    assert_eq!(seen, want, "a protected feed saw every version, in order");

    // Released, the next rewrite trims: a feed from before that is refused.
    g.protect(protected.id, start_ts, 0);
    for n in 150..160 {
        docs::write(&mut g, vec![put(n)], None).unwrap();
    }
    match feed::poll(&mut g, &bare, Some("hot"), 16) {
        Err(TxnError::HistoryLost(m)) => assert!(m.contains("hot/k"), "{m}"),
        other => panic!("expected lost history, got {other:?}"),
    }
    drop(c);
}

/// Asks one node and takes no redirect.
struct Only(Arc<RangeNode>);

impl KvSender for Only {
    fn send(&mut self, reqs: Vec<KvRequest>) -> Vec<Result<KvResponse, String>> {
        reqs.into_iter().map(|r| self.0.request(r, Duration::from_secs(3)).map_err(|e| e.to_string())).collect()
    }
    fn now(&mut self) -> Timestamp {
        hlc().now()
    }
    fn max_offset_ns(&self) -> u64 {
        500_000_000
    }
    fn sleep(&mut self, us: u64) {
        std::thread::sleep(Duration::from_micros(us));
    }
    fn new_id(&mut self) -> TxnId {
        static NEXT: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(1);
        [NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed); 16]
    }
}

/// A leader cut off from its range (`TASK-152`): it must stop answering from its own state, also for
/// writes that would propose nothing, and step down instead of waiting for a quorum forever.
#[test]
fn a_cut_off_leader_answers_nothing_stale_and_steps_down() {
    let cluster = start("cutoff");
    let mut g = cluster.gateway();
    docs::write(&mut g, vec![put("t", "seed", BTreeMap::new(), Precondition::None)], None).unwrap();
    // Whoever answers a read of the key itself, without a redirect, leads its range.
    let position = (0..MEMBERS.len()).find(|&i| docs::get(&mut Only(Arc::clone(&cluster.nodes[i])), "t", "seed").is_ok());
    let position = position.expect("a leader");
    let old = MEMBERS[position];
    let mut stale = Only(Arc::clone(&cluster.nodes[position]));
    for &id in &MEMBERS {
        if id != old {
            cluster.transport.partition(old, id);
        }
    }
    // The others elect, and create a document the old leader has never seen.
    let others: Vec<u16> = MEMBERS.iter().copied().filter(|&m| m != old).collect();
    let cfg = GatewayConfig { nodes: others, request_timeout: Duration::from_secs(20), call_timeout: Duration::from_secs(2) };
    let mut rest = Gateway::new(cluster.transport.clone(), cfg, hlc(), Arc::new(RwLock::new(RangeIndex::new())));
    docs::write(&mut rest, vec![put("t", "new", BTreeMap::new(), Precondition::None)], None).unwrap();
    // A delete there would propose nothing, on a key that exists: it must not report success.
    let delete = DocWrite { collection: "t".into(), key: "new".into(), op: DocOp::Delete, precondition: Precondition::None };
    assert!(docs::write(&mut stale, vec![delete], None).is_err());
    assert!(docs::get(&mut rest, "t", "new").unwrap().is_some());
    // The cut-off node stops claiming the range soon after, and says so at once.
    let until = std::time::Instant::now() + Duration::from_secs(5);
    while docs::get(&mut stale, "t", "seed").is_ok() {
        assert!(std::time::Instant::now() < until, "a cut-off node kept serving reads");
    }
    let at = std::time::Instant::now();
    assert!(docs::get(&mut stale, "t", "seed").is_err());
    assert!(at.elapsed() < Duration::from_secs(2), "it should answer, not wait: {:?}", at.elapsed());
    cluster.transport.heal();
}

/// Nodes checkpoint their engines on their own: the WAL is cut once what it held is flushed and
/// synced, instead of growing until the next restart (the range soak's 374 MB logs).
#[test]
fn range_nodes_checkpoint_and_cut_their_logs() {
    let cluster = start_with("checkpoint", RuntimeConfig { checkpoint_interval: Duration::from_millis(200), ..RuntimeConfig::default() });
    let mut g = cluster.gateway();
    let text = PropertyValue::Text("x".repeat(400));
    for batch in 0..20 {
        let writes = (0..50).map(|i| put("c", &format!("k{batch}-{i}"), fields(&[("v", text.clone())]), Precondition::None)).collect();
        docs::write(&mut g, writes, None).unwrap();
    }
    let wal_bytes = |id: u16| std::fs::metadata(cluster.root.join(format!("n{id}/wal/node.wal"))).map_or(0, |m| m.len());
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while MEMBERS.iter().any(|&id| wal_bytes(id) > 64 * 1024) {
        assert!(std::time::Instant::now() < deadline, "logs not cut: {:?} bytes", MEMBERS.map(wal_bytes));
        std::thread::sleep(Duration::from_millis(100));
    }
    // Everything is still there.
    assert!(docs::get(&mut g, "c", "k19-49").unwrap().is_some());
    assert!(docs::get(&mut g, "c", "k0-0").unwrap().is_some());
}
