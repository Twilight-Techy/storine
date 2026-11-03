//! The document API on a real-time range cluster in one process (`TASK-143`): three nodes on
//! their own event-loop threads, connected by `LocalTransport`, used through gateways that
//! route to leaseholders.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use storage_codec::field_ops::FieldOp;
use storage_codec::PropertyValue;
use storage_core::dist::docs;
use storage_core::dist::hlc::{Hlc, SystemClock};
use storage_core::dist::meta::RangeIndex;
use storage_core::dist::node::{NodeConfig, NodeDriver};
use storage_core::dist::runtime::{Gateway, GatewayConfig, LocalTransport, RangeNode, RuntimeConfig};
use storage_core::dist::server::NodeServer;
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
    let root = std::env::temp_dir().join(format!("storine_range_docs_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let transport = LocalTransport::new();
    let mut nodes = Vec::new();
    for &id in &MEMBERS {
        let engine = StorageEngine::<DocumentRecord>::open(root.join(format!("n{id}")), EngineConfig { background_flush: false, collection_name: "node".into(), ..EngineConfig::default() }).unwrap();
        let cfg = NodeConfig { seed: u64::from(id), cluster: MEMBERS.to_vec(), max_offset_ns: 500_000_000, ..NodeConfig::default() };
        let mut d = NodeDriver::new(id, Arc::new(engine), cfg);
        d.open_range(1, KeySpan::all(), &MEMBERS).unwrap();
        let node = RangeNode::start(NodeServer::new(d, hlc()), transport.clone(), RuntimeConfig::default());
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
    let ttl = DocWrite { collection: "c".into(), key: "k".into(), op: DocOp::PutWithTtl(BTreeMap::new(), Duration::from_secs(1)), precondition: Precondition::None };
    assert!(docs::write(s, vec![ttl], None).is_err());
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
