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
