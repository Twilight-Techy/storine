//! Three-node Raft cluster over real TCP sockets (`TASK-101`): elections, replicated document
//! writes, follower redirects, opt-in follower reads, leader failover and catch-up.

use std::collections::{BTreeMap, HashMap};
use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::{Duration, Instant};

use storage_codec::field_ops::FieldOp;
use storage_codec::PropertyValue;
use storage_core::raft_runtime::{RaftConfig, RaftRuntime};
use storage_core::{
    DocOp, DocWrite, DocumentRecord, DocumentStore, EngineConfig, NodeId, Precondition, RaftNode,
};
use storage_server::cluster::{ClusterOptions, TcpRaftTransport};
use storage_server::{SecurityConfig, StorageClient, StorageServer, OP_CLUSTER_INFO, OP_SET_OPTIONS};
use storage_types::StorageError;

fn free_port() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap()
}

struct Node {
    id: NodeId,
    addr: SocketAddr,
    store: Arc<DocumentStore>,
    runtime: Arc<RaftRuntime<DocumentRecord>>,
    server: StorageServer<DocumentRecord>,
}

fn config() -> RaftConfig {
    RaftConfig {
        election_timeout_min: Duration::from_millis(200),
        election_timeout_max: Duration::from_millis(400),
        heartbeat_interval: Duration::from_millis(40),
        commit_timeout: Duration::from_secs(3),
        ..RaftConfig::default()
    }
}

fn start_node(dir: &std::path::Path, id: NodeId, addrs: &HashMap<NodeId, SocketAddr>) -> Node {
    let store = Arc::new(DocumentStore::open(dir.join(format!("n{id}")), EngineConfig::default()).unwrap());
    let node = Arc::new(RaftNode::new(id, Arc::clone(store.engine())).unwrap());
    let peers: HashMap<NodeId, SocketAddr> = addrs.iter().filter(|(&k, _)| k != id).map(|(&k, &v)| (k, v)).collect();
    let transport = Arc::new(TcpRaftTransport::new(peers, None, Duration::from_millis(300)));
    let ids: Vec<NodeId> = addrs.keys().copied().collect();
    let runtime = RaftRuntime::start(node, ids, transport, config()).unwrap();
    let client_addrs = addrs.iter().map(|(&k, v)| (k, v.to_string())).collect();
    let server = StorageServer::start_documents_cluster(
        &addrs[&id].to_string(),
        Arc::clone(&store),
        1000,
        SecurityConfig::default(),
        ClusterOptions { runtime: Arc::clone(&runtime), client_addrs },
    )
    .unwrap();
    Node { id, addr: addrs[&id], store, runtime, server }
}

fn wait_leader(nodes: &[&Node]) -> usize {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let leaders: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].runtime.lease_valid()).collect();
        if leaders.len() == 1 {
            return leaders[0];
        }
        assert!(Instant::now() < deadline, "no leader elected over TCP");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn deposit(amount: i64) -> DocWrite {
    DocWrite {
        collection: "acct".into(),
        key: "a".into(),
        op: DocOp::Patch(vec![FieldOp::Add { path: "bal".into(), delta: PropertyValue::Int64(amount), min: Some(PropertyValue::Int64(0)), max: None }]),
        precondition: Precondition::Exists,
    }
}

#[test]
fn test_three_node_tcp_cluster_writes_redirects_and_failover() {
    let dir = std::env::temp_dir().join(format!("stge_cluster_tcp_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addrs: HashMap<NodeId, SocketAddr> = (1..=3).map(|i| (i, free_port())).collect();
    let mut nodes: Vec<Node> = (1..=3).map(|i| start_node(&dir, i, &addrs)).collect();
    let l = wait_leader(&nodes.iter().collect::<Vec<_>>());
    let leader_addr = nodes[l].addr;

    // Writes on the leader are replicated.
    let mut c = StorageClient::connect(leader_addr).unwrap();
    let mut f = BTreeMap::new();
    f.insert("bal".to_string(), PropertyValue::Int64(100));
    c.doc_write(&[DocWrite { collection: "acct".into(), key: "a".into(), op: DocOp::Put(f), precondition: Precondition::Absent }], None).unwrap();
    for _ in 0..20 {
        c.doc_write(&[deposit(1)], None).unwrap();
    }
    assert_eq!(c.doc_get("acct", "a").unwrap().unwrap().fields["bal"], PropertyValue::Int64(120));

    // A follower redirects writes and (by default) reads to the leader's address.
    let f_idx = (l + 1) % 3;
    let mut fc = StorageClient::connect(nodes[f_idx].addr).unwrap();
    match fc.doc_write(&[deposit(1)], None).unwrap_err() {
        StorageError::NotLeader { leader } => assert_eq!(leader, Some(u64::from(nodes[l].id))),
        e => panic!("expected NotLeader, got {e:?}"),
    }
    assert!(matches!(fc.doc_get("acct", "a"), Err(StorageError::NotLeader { .. })));
    // Opting into follower reads serves (eventually consistent) local data.
    fc.doc_command(OP_SET_OPTIONS, &[1]).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(Some(d)) = fc.doc_get("acct", "a") {
            if d.fields["bal"] == PropertyValue::Int64(120) {
                break;
            }
        }
        assert!(Instant::now() < deadline, "follower never caught up");
        std::thread::sleep(Duration::from_millis(20));
    }
    let (_, info) = fc.doc_command(OP_CLUSTER_INFO, &[]).unwrap();
    assert_eq!(u64::from_le_bytes(info[9..17].try_into().unwrap()), u64::from(nodes[l].id));

    // Stop the leader: the others elect a new one and keep serving.
    let old = nodes.remove(l);
    let old_id = old.id;
    old.runtime.stop();
    let Node { mut server, store, runtime, .. } = old;
    server.stop();
    // Release every handle on the engine (its directory lock) before the node restarts.
    drop(server);
    drop(store);
    drop(runtime);
    let l2 = wait_leader(&nodes.iter().collect::<Vec<_>>());
    let mut c2 = StorageClient::connect(nodes[l2].addr).unwrap();
    for _ in 0..5 {
        c2.doc_write(&[deposit(1)], None).unwrap();
    }
    assert_eq!(c2.doc_get("acct", "a").unwrap().unwrap().fields["bal"], PropertyValue::Int64(125));

    // The old node restarts and catches up.
    let back = start_node(&dir, old_id, &addrs);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let bal = back.store.get("acct", "a").ok().flatten().map(|d| d.fields["bal"].clone());
        if bal == Some(PropertyValue::Int64(125)) {
            break;
        }
        assert!(Instant::now() < deadline, "restarted node did not catch up: {bal:?}");
        std::thread::sleep(Duration::from_millis(50));
    }
    nodes.push(back);
    for n in nodes {
        n.runtime.stop();
        let Node { mut server, .. } = n;
        server.stop();
    }
    let _ = std::fs::remove_dir_all(&dir);
}
