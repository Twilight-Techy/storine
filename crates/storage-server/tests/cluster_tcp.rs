//! Three-node Raft cluster over real TCP sockets (`TASK-101`): elections, replicated document
//! writes, follower redirects, opt-in follower reads, leader failover and catch-up.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
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

/// A port no other cluster in this test binary was given (see `free_local_addr`).
fn free_port() -> SocketAddr {
    storage_types::testing::free_local_addr()
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
    // Writes on the leader are replicated. On a slow runner leadership can move right after the
    // first election: the first write retries on `NotLeader`, as clients do.
    let mut f = BTreeMap::new();
    f.insert("bal".to_string(), PropertyValue::Int64(100));
    let first = DocWrite { collection: "acct".into(), key: "a".into(), op: DocOp::Put(f), precondition: Precondition::Absent };
    let deadline = Instant::now() + Duration::from_secs(30);
    let (l, mut c) = loop {
        let l = wait_leader(&nodes.iter().collect::<Vec<_>>());
        let mut c = StorageClient::connect(nodes[l].addr).unwrap();
        match c.doc_write(std::slice::from_ref(&first), Some([7; 16])) {
            Ok(_) => break (l, c),
            Err(StorageError::NotLeader { .. }) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(100)),
            Err(e) => panic!("first write: {e:?}"),
        }
    };
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

/// The same cluster with TLS on every node's port: Raft traffic between nodes runs over TLS
/// (each node checks its peers' CA-issued certificates), and clients reach it over TLS too.
#[test]
fn test_three_node_cluster_over_tls() {
    use storage_server::cluster::PeerTls;
    use storage_server::{ClientTls, RbacRole};
    use storage_tls::{issue, new_ca, ClientConfig, TlsConfig};

    let dir = std::env::temp_dir().join(format!("stge_cluster_tls_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addrs: HashMap<NodeId, SocketAddr> = (1..=3).map(|i| (i, free_port())).collect();
    let ca = new_ca("test cluster").unwrap();
    let roots = Arc::new(ClientConfig::with_roots_pem(&ca.cert_pem).unwrap());
    let token = "cluster-token-0123456789";
    let mut nodes = Vec::new();
    for id in 1..=3u16 {
        let cert = issue(&ca, &["127.0.0.1"]).unwrap();
        let security = SecurityConfig::new(true)
            .with_token(token, RbacRole::Admin)
            .with_tls(TlsConfig::from_pem(&cert.cert_pem, &cert.key_pem).unwrap());
        let store = Arc::new(DocumentStore::open(dir.join(format!("n{id}")), EngineConfig::default()).unwrap());
        let node = Arc::new(RaftNode::new(id, Arc::clone(store.engine())).unwrap());
        let peers: HashMap<NodeId, SocketAddr> = addrs.iter().filter(|(&k, _)| k != id).map(|(&k, &v)| (k, v)).collect();
        let tls = PeerTls { config: Arc::clone(&roots), names: HashMap::new() };
        let transport =
            Arc::new(TcpRaftTransport::new(peers, Some(token.into()), Duration::from_millis(300)).with_tls(tls));
        let runtime = RaftRuntime::start(node, addrs.keys().copied().collect(), transport, config()).unwrap();
        let client_addrs = addrs.iter().map(|(&k, v)| (k, v.to_string())).collect();
        let server = StorageServer::start_documents_cluster(
            &addrs[&id].to_string(),
            Arc::clone(&store),
            1000,
            security,
            ClusterOptions { runtime: Arc::clone(&runtime), client_addrs },
        )
        .unwrap();
        nodes.push(Node { id, addr: addrs[&id], store, runtime, server });
    }
    let l = wait_leader(&nodes.iter().collect::<Vec<_>>());
    let client_tls = ClientTls { config: Arc::clone(&roots), server_name: None };
    let mut c = StorageClient::connect_tls(nodes[l].addr, &client_tls).unwrap();
    c.authenticate(token).unwrap();
    let mut f = BTreeMap::new();
    f.insert("bal".to_string(), PropertyValue::Int64(100));
    // A write whose commit could not be confirmed in time (`CommitUnknown`, e.g. on a slow CI
    // disk) is retried with the same request id, as every client does: it applies once.
    let write = |c: &mut StorageClient, w: DocWrite, id: u8| {
        let rid = Some([id; 16]);
        for attempt in 0.. {
            match c.doc_write(std::slice::from_ref(&w), rid) {
                Ok(_) => return,
                Err(StorageError::CommitUnknown { .. }) if attempt < 10 => continue,
                Err(e) => panic!("write {id}: {e:?} after {attempt} retries"),
            }
        }
    };
    write(&mut c, DocWrite { collection: "acct".into(), key: "a".into(), op: DocOp::Put(f), precondition: Precondition::Absent }, 0);
    for i in 0..20 {
        write(&mut c, deposit(1), i + 1);
    }
    // Every write was committed by a quorum reached over TLS, once; a follower has it too.
    let f_idx = (l + 1) % 3;
    let mut fc = StorageClient::connect_tls(nodes[f_idx].addr, &client_tls).unwrap();
    fc.authenticate(token).unwrap();
    fc.doc_command(OP_SET_OPTIONS, &[1]).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(Some(d)) = fc.doc_get("acct", "a") {
            if d.fields["bal"] == PropertyValue::Int64(120) {
                break;
            }
        }
        assert!(Instant::now() < deadline, "follower never caught up over TLS");
        std::thread::sleep(Duration::from_millis(20));
    }
    for n in &mut nodes {
        n.server.stop();
    }
    drop(nodes);
    let _ = std::fs::remove_dir_all(&dir);
}
