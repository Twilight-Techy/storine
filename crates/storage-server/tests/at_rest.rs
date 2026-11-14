//! Encryption at rest in the server (`TASK-156`): turning it on for existing data, rotating
//! keys and retiring the old one, refusing the wrong key, and a range cluster whose records,
//! WALs, Raft logs and snapshots hold no plaintext.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use storage_codec::{EncryptionKey, PropertyValue};
use storage_core::{DocOp, DocWrite, Precondition};
use storage_server::ranges::{start_range_server, RangeExtras, RangeServerConfig};
use storage_server::{SecurityConfig, StorageClient};
use storage_types::StorageError;

const MARK: &str = "PLAINTEXT-MARK-at-rest-91c4";

fn put(collection: &str, key: &str) -> DocWrite {
    let mut f = BTreeMap::new();
    f.insert("secret".to_string(), PropertyValue::Text(format!("{MARK}-{key}")));
    f.insert("pad".to_string(), PropertyValue::Text("x".repeat(300)));
    DocWrite { collection: collection.into(), key: key.into(), op: DocOp::Put(f), precondition: Precondition::None }
}

/// Files under `dir` that contain the marker.
fn plaintext_in(dir: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else { continue };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if std::fs::read(&p).is_ok_and(|b| b.windows(MARK.len()).any(|w| w == MARK.as_bytes())) {
                found.push(p);
            }
        }
    }
    found
}

fn wait_until(what: &str, secs: u64, mut ok: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !ok() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(200));
    }
}

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn(data: &Path, addr: SocketAddr, env: &[(&str, &Path)], extra: &[(&str, &str)]) -> Server {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_storage-server"));
    cmd.arg(data).arg(addr.to_string()).stdout(Stdio::null()).stderr(Stdio::null());
    for k in ["STORAGE_ENCRYPTION_KEYS", "STORAGE_ENCRYPTION_ACTIVE_KEY", "STORAGE_ENCRYPTION_ALLOW_PLAINTEXT", "STORAGE_AUTH_TOKENS"] {
        cmd.env_remove(k);
    }
    for (k, v) in env {
        cmd.env(k, v);
    }
    for (k, v) in extra {
        cmd.env(k, v);
    }
    Server(cmd.spawn().unwrap())
}

fn client(addr: SocketAddr) -> StorageClient {
    let mut last = None;
    for _ in 0..150 {
        match StorageClient::connect_timeout(addr, Duration::from_secs(1)) {
            Ok(c) => return c,
            Err(e) => last = Some(e),
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("server never listened: {last:?}");
}

fn health(addr: SocketAddr) -> String {
    client(addr).health().unwrap()
}

/// Kills the server once a checkpoint has cut its WAL (standalone servers checkpoint every 10 s
/// while written), so what remains on disk is what the engine wrote at rest.
fn settle_and_stop(s: Server) {
    std::thread::sleep(Duration::from_secs(12));
    drop(s);
}

#[test]
fn a_standalone_server_turns_encryption_on_rotates_and_refuses_the_wrong_key() {
    let root = std::env::temp_dir().join(format!("stge_at_rest_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let data = root.join("data");
    let keys = root.join("keys");
    let addr = storage_types::testing::free_local_addr();

    // 1. Plaintext, as a database before encryption was turned on.
    let s = spawn(&data, addr, &[], &[]);
    let mut c = client(addr);
    for i in 0..50 {
        c.doc_write(&[put("people", &format!("p{i}"))], None).unwrap();
    }
    assert!(health(addr).contains("\"encryption\":\"off\""));
    settle_and_stop(s);
    assert!(!plaintext_in(&data).is_empty(), "the test needs plaintext to start from");

    // 2. A key, and one restart that accepts plaintext while it reseals, then goes strict.
    let out = Command::new(env!("CARGO_BIN_EXE_storage-server")).arg("gen-key").arg(&keys).output().unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let s = spawn(&data, addr, &[("STORAGE_ENCRYPTION_KEYS", &keys)], &[("STORAGE_ENCRYPTION_ALLOW_PLAINTEXT", "1")]);
    wait_until("the reseal to finish", 60, || health(addr).contains("\"active_key\":1,\"keys\":[1],\"accept_plaintext\":false"));
    let mut c = client(addr);
    for i in 50..80 {
        c.doc_write(&[put("people", &format!("p{i}"))], None).unwrap();
    }
    assert!(c.doc_get("people", "p3").unwrap().is_some());
    settle_and_stop(s);
    let left = plaintext_in(&data);
    assert!(left.is_empty(), "plaintext left in {left:?}");

    // 3. Rotation: a second key becomes active; after the reseal, the first can go.
    Command::new(env!("CARGO_BIN_EXE_storage-server")).arg("gen-key").arg(&keys).output().unwrap();
    let s = spawn(&data, addr, &[("STORAGE_ENCRYPTION_KEYS", &keys)], &[]);
    wait_until("key 2 to be active", 30, || health(addr).contains("\"active_key\":2"));
    std::thread::sleep(Duration::from_secs(2)); // the background reseal
    settle_and_stop(s);
    let only_two: String = std::fs::read_to_string(&keys).unwrap().lines().filter(|l| l.starts_with("2 ")).map(|l| format!("{l}\n")).collect();
    let keys2 = root.join("keys2");
    std::fs::write(&keys2, only_two).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&keys2, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let s = spawn(&data, addr, &[("STORAGE_ENCRYPTION_KEYS", &keys2)], &[]);
    let mut c = client(addr);
    for i in [0, 49, 79] {
        let d = c.doc_get("people", &format!("p{i}")).unwrap().expect("readable with the new key alone");
        assert_eq!(d.fields["secret"], PropertyValue::Text(format!("{MARK}-p{i}")));
    }
    drop(s);

    // 4. The wrong key: the server does not serve this data.
    let wrong = root.join("wrong");
    storage_server::at_rest::add_key(&wrong).unwrap();
    let mut s = spawn(&data, addr, &[("STORAGE_ENCRYPTION_KEYS", &wrong)], &[]);
    let refused = (0..50).any(|_| {
        if s.0.try_wait().unwrap().is_some() {
            return true; // refused to start
        }
        if let Ok(mut c) = StorageClient::connect_timeout(addr, Duration::from_millis(500)) {
            if c.doc_get("people", "p0").is_err() {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(200));
        false
    });
    assert!(refused, "data sealed with another key was served");
    drop(s);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_range_cluster_keeps_no_plaintext_on_disk() {
    let root = std::env::temp_dir().join(format!("stge_at_rest_ranges_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let addrs: Vec<SocketAddr> = (0..3).map(|_| storage_types::testing::free_local_addr()).collect();
    let peers: HashMap<u16, SocketAddr> = addrs.iter().enumerate().map(|(i, a)| ((i + 1) as u16, *a)).collect();
    let at_rest = storage_server::at_rest::settings(&[(1, EncryptionKey::from_bytes([0x5a; 32]))], None, false).unwrap();
    let servers: Vec<_> = (0..3)
        .map(|i| {
            start_range_server(RangeServerConfig {
                data_dir: root.join(format!("n{}", i + 1)),
                bind_addr: addrs[i].to_string(),
                node_id: (i + 1) as u16,
                peers: peers.clone(),
                token: None,
                security: SecurityConfig::default(),
                max_connections: 256,
                max_offset: Duration::from_millis(100),
                peer_tls: None,
                extras: RangeExtras { encryption: Some(at_rest.clone()), ..Default::default() },
            })
            .unwrap()
        })
        .collect();
    let write = |key: &str| {
        for _ in 0..200 {
            let Ok(mut c) = StorageClient::connect(addrs[0]) else { continue };
            match c.doc_write(&[put("cards", key)], None) {
                Ok(_) => return,
                Err(StorageError::NotLeader { .. } | StorageError::Rejected { .. } | StorageError::Io { .. } | StorageError::CommitUnknown { .. }) => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(e) => panic!("{e:?}"),
            }
        }
        panic!("write never succeeded");
    };
    for i in 0..200 {
        write(&format!("c{i}"));
    }
    let mut c = StorageClient::connect(addrs[2]).unwrap();
    assert!(c.doc_get("cards", "c7").unwrap().is_some());
    assert!(c.health().unwrap().contains("\"active_key\":1"));
    // Range nodes checkpoint every 5 s while written.
    std::thread::sleep(Duration::from_secs(8));
    let left = plaintext_in(&root);
    assert!(left.is_empty(), "plaintext left in {left:?}");
    drop(servers);
    let _ = std::fs::remove_dir_all(&root);
}

fn spawn_node(data: &Path, addr: SocketAddr, id: u16, peers: &str, keys: Option<&Path>, allow_plain: bool) -> Server {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_storage-server"));
    cmd.arg(data).arg(addr.to_string()).args(["--node-id", &id.to_string(), "--peers", peers]).stdout(Stdio::null()).stderr(Stdio::null());
    for k in ["STORAGE_ENCRYPTION_KEYS", "STORAGE_ENCRYPTION_ACTIVE_KEY", "STORAGE_ENCRYPTION_ALLOW_PLAINTEXT", "STORAGE_AUTH_TOKENS", "STORAGE_CLUSTER_TOKEN"] {
        cmd.env_remove(k);
    }
    if let Some(k) = keys {
        cmd.env("STORAGE_ENCRYPTION_KEYS", k);
    }
    if allow_plain {
        cmd.env("STORAGE_ENCRYPTION_ALLOW_PLAINTEXT", "1");
    }
    Server(cmd.spawn().unwrap())
}

/// Writes through whichever node leads (followers answer `NotLeader`).
fn cluster_write(addrs: &[SocketAddr], key: &str) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        for a in addrs {
            if let Ok(mut c) = StorageClient::connect_timeout(*a, Duration::from_millis(500)) {
                if c.doc_write(&[put("people", key)], None).is_ok() {
                    return;
                }
            }
        }
        assert!(Instant::now() < deadline, "no node accepted {key}");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A single-group Raft cluster run by the real binary (`TASK-157`): plaintext at first, then
/// encrypted on every node with one restart; a node that was down catches up (its peers have
/// compacted their logs, so through a sealed snapshot), and no file of any node holds plaintext.
#[test]
fn a_raft_cluster_turns_encryption_on_and_catches_up_a_node_from_sealed_storage() {
    let root = std::env::temp_dir().join(format!("stge_at_rest_raft_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let keys = root.join("keys");
    storage_server::at_rest::add_key(&keys).unwrap();
    let addrs: Vec<SocketAddr> = (0..3).map(|_| storage_types::testing::free_local_addr()).collect();
    let peers: String = addrs.iter().enumerate().map(|(i, a)| format!("{}={a}", i + 1)).collect::<Vec<_>>().join(",");
    let data = |i: usize| root.join(format!("n{}", i + 1));
    let start = |i: usize, keys: Option<&Path>, allow: bool| spawn_node(&data(i), addrs[i], (i + 1) as u16, &peers, keys, allow);

    // 1. Plaintext.
    let nodes: Vec<Server> = (0..3).map(|i| start(i, None, false)).collect();
    for i in 0..40 {
        cluster_write(&addrs, &format!("p{i}"));
    }
    std::thread::sleep(Duration::from_secs(12)); // a checkpoint on every node
    drop(nodes);
    assert!(!plaintext_in(&root).is_empty(), "the test needs plaintext to start from");

    // 2. Every node restarts with the key, accepting plaintext until it has resealed.
    let mut nodes: Vec<Option<Server>> = (0..3).map(|i| Some(start(i, Some(&keys), true))).collect();
    for a in &addrs {
        wait_until("every node to go strict", 90, || {
            StorageClient::connect_timeout(*a, Duration::from_secs(1)).and_then(|mut c| c.health()).is_ok_and(|h| h.contains("\"accept_plaintext\":false"))
        });
    }

    // 3. Node 3 is down while the others write and compact their logs past it (a node
    // compacts once 8,192 applied entries pile up, keeping 1,024: 10,000 writes is enough).
    nodes[2] = None;
    std::thread::scope(|sc| {
        for t in 0..4 {
            let addrs = &addrs[..2];
            sc.spawn(move || {
                // One connection to the leader, kept while it accepts writes.
                let mut leader: Option<StorageClient> = None;
                for i in (40 + t..10_040).step_by(4) {
                    let key = format!("p{i}");
                    if let Some(c) = leader.as_mut() {
                        if c.doc_write(&[put("people", &key)], None).is_ok() {
                            continue;
                        }
                    }
                    cluster_write(addrs, &key);
                    leader = addrs.iter().find_map(|a| {
                        let mut c = StorageClient::connect_timeout(*a, Duration::from_millis(500)).ok()?;
                        c.doc_write(&[put("people", &key)], None).ok().map(|_| c)
                    });
                }
            });
        }
    });
    std::thread::sleep(Duration::from_secs(12));
    nodes[2] = Some(start(2, Some(&keys), false));
    let mut c = client(addrs[2]);
    c.doc_command(storage_server::OP_SET_OPTIONS, &[1]).unwrap(); // follower reads
    wait_until("node 3 to catch up", 120, || c.doc_get("people", "p10039").ok().flatten().is_some());
    assert!(c.doc_get("people", "p0").unwrap().is_some());
    // The leader had compacted its log past node 3: it caught up from a (sealed) snapshot.
    let snapshot_built = (0..2).any(|i| std::fs::read_dir(data(i)).unwrap().flatten().any(|e| e.file_name().to_string_lossy().ends_with("_raft.snapshot")));
    assert!(snapshot_built, "node 3 caught up without a snapshot: the test did not exercise sealed snapshots");
    std::thread::sleep(Duration::from_secs(12)); // checkpoints cut the WALs everywhere
    drop(nodes);
    let left = plaintext_in(&root);
    assert!(left.is_empty(), "plaintext left in {left:?}");

    // 4. A node given the wrong key does not serve its data.
    let wrong = root.join("wrong");
    storage_server::at_rest::add_key(&wrong).unwrap();
    let mut s = start(0, Some(&wrong), false);
    let refused = (0..50).any(|_| {
        if s.0.try_wait().unwrap().is_some() {
            return true;
        }
        if let Ok(mut c) = StorageClient::connect_timeout(addrs[0], Duration::from_millis(500)) {
            let _ = c.doc_command(storage_server::OP_SET_OPTIONS, &[1]);
            if c.doc_get("people", "p0").is_err() {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(200));
        false
    });
    assert!(refused, "data sealed with another key was served");
    drop(s);
    let _ = std::fs::remove_dir_all(&root);
}
