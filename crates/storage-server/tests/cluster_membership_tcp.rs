//! Cluster membership changes over real TCP (`TASK-121`): a node started with `--join`
//! semantics is added through `OP_CLUSTER_CHANGE` (learner, then voter once caught up), and the
//! leader removes itself; followers redirect membership changes to the leader.

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
use storage_server::{
    SecurityConfig, StorageClient, StorageServer, CLUSTER_ADD_VOTER, CLUSTER_REMOVE,
    OP_CLUSTER_INFO,
};
use storage_types::StorageError;

fn free_port() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
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

/// `addrs` empty: a node joining an existing cluster (no configuration, no peers yet).
fn start_node(
    dir: &std::path::Path,
    id: NodeId,
    addr: SocketAddr,
    addrs: &HashMap<NodeId, SocketAddr>,
) -> Node {
    let store =
        Arc::new(DocumentStore::open(dir.join(format!("n{id}")), EngineConfig::default()).unwrap());
    let node = Arc::new(RaftNode::new(id, Arc::clone(store.engine())).unwrap());
    let peers = addrs
        .iter()
        .filter(|(&k, _)| k != id)
        .map(|(&k, &v)| (k, v))
        .collect();
    let transport = Arc::new(TcpRaftTransport::new(
        peers,
        None,
        Duration::from_millis(300),
    ));
    let runtime =
        RaftRuntime::start(node, addrs.keys().copied().collect(), transport, config()).unwrap();
    let server = StorageServer::start_documents_cluster(
        &addr.to_string(),
        Arc::clone(&store),
        1000,
        SecurityConfig::default(),
        ClusterOptions {
            runtime: Arc::clone(&runtime),
            client_addrs: addrs.iter().map(|(&k, v)| (k, v.to_string())).collect(),
        },
    )
    .unwrap();
    Node {
        id,
        addr,
        store,
        runtime,
        server,
    }
}

fn wait_leader(nodes: &[&Node]) -> usize {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let leaders: Vec<usize> = (0..nodes.len())
            .filter(|&i| nodes[i].runtime.lease_valid())
            .collect();
        if leaders.len() == 1 {
            return leaders[0];
        }
        assert!(Instant::now() < deadline, "no leader elected over TCP");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn deposit() -> DocWrite {
    DocWrite {
        collection: "acct".into(),
        key: "a".into(),
        op: DocOp::Patch(vec![FieldOp::Add {
            path: "bal".into(),
            delta: PropertyValue::Int64(1),
            min: None,
            max: None,
        }]),
        precondition: Precondition::Exists,
    }
}

fn wait_balance(node: &Node, want: i64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let bal = node
            .store
            .get("acct", "a")
            .ok()
            .flatten()
            .map(|d| d.fields["bal"].clone());
        if bal == Some(PropertyValue::Int64(want)) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "node {} did not reach {want}: {bal:?}",
            node.id
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// `(node id, addr, voter)` of every member in an `OP_CLUSTER_INFO` reply.
fn members(info: &[u8]) -> Vec<(u64, String, bool)> {
    let u32_at = |i: usize| u32::from_le_bytes(info[i..i + 4].try_into().unwrap()) as usize;
    let u64_at = |i: usize| u64::from_le_bytes(info[i..i + 8].try_into().unwrap());
    let mut at = 17;
    at += 4 + u32_at(at); // leader address
    at += 24; // term, commit, applied
    let n = u32_at(at);
    at += 4;
    let mut out = Vec::new();
    for _ in 0..n {
        let id = u64_at(at);
        let len = u32_at(at + 8);
        out.push((
            id,
            String::from_utf8(info[at + 12..at + 12 + len].to_vec()).unwrap(),
            false,
        ));
        at += 12 + len;
    }
    for m in &mut out {
        m.2 = info[at] == 1;
        at += 1;
    }
    assert_eq!(at, info.len());
    out
}

#[test]
fn test_cluster_grows_and_the_leader_removes_itself_over_tcp() {
    let dir = std::env::temp_dir().join(format!("stge_cluster_members_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addrs: HashMap<NodeId, SocketAddr> = (1..=3).map(|i| (i, free_port())).collect();
    let mut nodes: Vec<Node> = (1..=3)
        .map(|i| start_node(&dir, i, addrs[&i], &addrs))
        .collect();
    let l = wait_leader(&nodes.iter().collect::<Vec<_>>());
    let mut c = StorageClient::connect(nodes[l].addr).unwrap();
    let mut f = BTreeMap::new();
    f.insert("bal".to_string(), PropertyValue::Int64(100));
    c.doc_write(
        &[DocWrite {
            collection: "acct".into(),
            key: "a".into(),
            op: DocOp::Put(f),
            precondition: Precondition::Absent,
        }],
        None,
    )
    .unwrap();
    for _ in 0..10 {
        c.doc_write(&[deposit()], None).unwrap();
    }

    // Node 4 starts knowing nothing about the cluster.
    let addr4 = free_port();
    nodes.push(start_node(&dir, 4, addr4, &HashMap::new()));
    // A follower redirects the change; the leader makes it (learner, catch-up, voter).
    let f_idx = (0..3).find(|&i| i != l).unwrap();
    let mut fc = StorageClient::connect(nodes[f_idx].addr).unwrap();
    assert!(matches!(
        fc.cluster_change(CLUSTER_ADD_VOTER, 4, &addr4.to_string()),
        Err(StorageError::NotLeader { leader: Some(id) }) if id == u64::from(nodes[l].id)
    ));
    c.cluster_change(CLUSTER_ADD_VOTER, 4, &addr4.to_string())
        .unwrap();
    wait_balance(&nodes[3], 110);
    let (_, info) = c.doc_command(OP_CLUSTER_INFO, &[]).unwrap();
    let list = members(&info);
    assert_eq!(
        list.iter().map(|m| (m.0, m.2)).collect::<Vec<_>>(),
        vec![(1, true), (2, true), (3, true), (4, true)]
    );
    assert_eq!(list[3].1, addr4.to_string());

    // The leader removes itself; the remaining three (node 4 included) carry on.
    let old = nodes[l].id;
    c.cluster_change(CLUSTER_REMOVE, old, "").unwrap();
    let rest: Vec<&Node> = nodes.iter().filter(|n| n.id != old).collect();
    let l2 = wait_leader(&rest);
    let mut c2 = StorageClient::connect(rest[l2].addr).unwrap();
    for _ in 0..5 {
        c2.doc_write(&[deposit()], None).unwrap();
    }
    for n in &rest {
        wait_balance(n, 115);
        assert!(!n.runtime.membership().contains(old), "node {}", n.id);
    }
    let (_, info) = c2.doc_command(OP_CLUSTER_INFO, &[]).unwrap();
    assert_eq!(members(&info).len(), 3);
    assert!(
        !nodes[l].runtime.lease_valid(),
        "the removed leader stepped down"
    );

    for n in nodes {
        n.runtime.stop();
        let Node { mut server, .. } = n;
        server.stop();
    }
    let _ = std::fs::remove_dir_all(&dir);
}
