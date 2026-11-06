//! Cluster backups and changefeeds over TCP (`TASK-144`): three range nodes, transfers running
//! during the backup, a restore into a second cluster, and a feed that survives its consumer's
//! restart and continues from the backup's timestamp.

use std::collections::{BTreeMap, HashMap};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use storage_codec::field_ops::FieldOp;
use storage_codec::PropertyValue;
use storage_core::dist::feed;
use storage_core::dist::hlc::Timestamp;
use storage_core::{DocOp, DocWrite, Precondition};
use storage_server::ranges::{start_range_server, JsonlSink, RangeServerConfig};
use storage_server::{SecurityConfig, StorageClient, StorageServer};
use storage_types::StorageError;

const ACCOUNTS: usize = 40;
const OPENING: i64 = 1000;

fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

struct Cluster {
    addrs: Vec<SocketAddr>,
    _servers: Vec<StorageServer<storage_core::DocumentRecord>>,
}

fn start(root: &Path, backups: &Path) -> Cluster {
    let addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
    let peers: HashMap<u16, SocketAddr> = addrs.iter().enumerate().map(|(i, a)| ((i + 1) as u16, *a)).collect();
    let servers = (0..3)
        .map(|i| {
            start_range_server(RangeServerConfig {
                data_dir: root.join(format!("n{}", i + 1)),
                bind_addr: addrs[i].to_string(),
                node_id: (i + 1) as u16,
                peers: peers.clone(),
                token: None,
                security: SecurityConfig::default().with_backup_root(backups),
                max_connections: 256,
                max_offset: Duration::from_millis(100),
            })
            .unwrap()
        })
        .collect();
    Cluster { addrs, _servers: servers }
}

/// Retries while the cluster elects its first leaders.
fn retrying<R>(addr: SocketAddr, mut f: impl FnMut(&mut StorageClient) -> Result<R, StorageError>) -> R {
    let mut last = None;
    for _ in 0..200 {
        let mut c = StorageClient::connect(addr).unwrap();
        match f(&mut c) {
            Ok(r) => return r,
            Err(e) => last = Some(e),
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("still failing: {last:?}");
}

fn add(key: String, delta: i64) -> DocWrite {
    DocWrite { collection: "bank".into(), key, op: DocOp::Patch(vec![FieldOp::Add { path: "n".into(), delta: PropertyValue::Int64(delta), min: None, max: None }]), precondition: Precondition::None }
}

fn put(collection: &str, key: String, n: i64) -> DocWrite {
    DocWrite { collection: collection.into(), key, op: DocOp::Put(BTreeMap::from([("n".to_string(), PropertyValue::Int64(n))])), precondition: Precondition::None }
}

fn int(m: &BTreeMap<String, PropertyValue>) -> i64 {
    match m.get("n") {
        Some(PropertyValue::Int64(n)) => *n,
        other => panic!("not a number: {other:?}"),
    }
}

fn balances(addr: SocketAddr) -> BTreeMap<String, i64> {
    retrying(addr, |c| {
        let page = c.doc_query(&storage_core::DocQuery { collection: "bank".into(), ..storage_core::DocQuery::default() })?;
        Ok(page.documents.iter().map(|d| (d.key.clone(), int(&d.fields))).collect())
    })
}

fn timestamp_of(msg: &str) -> Timestamp {
    let ts = msg.split("ts=").nth(1).unwrap().split(' ').next().unwrap();
    let (w, l) = ts.split_once('.').unwrap();
    Timestamp::new(w.parse().unwrap(), l.parse().unwrap())
}

/// Replays delivered lines onto `state`, returning the number of changes.
fn replay(file: &Path, state: &mut BTreeMap<String, i64>) -> usize {
    let text = std::fs::read_to_string(file).unwrap();
    let mut n = 0;
    for line in text.lines() {
        if line.starts_with("{\"resolved\"") {
            continue;
        }
        assert!(line.starts_with("{\"collection\":\"bank\""), "{line}");
        let key = line.split("\"key\":\"").nth(1).unwrap().split('"').next().unwrap().to_string();
        let v: i64 = line.split("\"fields\":{\"n\":").nth(1).unwrap().split('}').next().unwrap().parse().unwrap();
        state.insert(key, v);
        n += 1;
    }
    n
}

#[test]
fn backup_restore_and_changefeed() {
    let root: PathBuf = std::env::temp_dir().join(format!("storine_range_backup_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let backups = root.join("backups");
    let a = start(&root.join("a"), &backups);
    let addr = a.addrs[0];

    retrying(addr, |c| {
        c.doc_write(&(0..ACCOUNTS).map(|i| put("bank", format!("a{i:03}"), OPENING)).collect::<Vec<_>>(), None)?;
        c.doc_write(&[put("other", "x".to_string(), 7), put("other", "gone".to_string(), 8)], None)?;
        c.doc_write(&[DocWrite { collection: "other".into(), key: "gone".into(), op: DocOp::Delete, precondition: Precondition::None }], None)?;
        Ok(())
    });

    // Transfers keep running during the backup, against other nodes too.
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = a
        .addrs
        .iter()
        .enumerate()
        .map(|(w, addr)| {
            let (addr, stop) = (*addr, Arc::clone(&stop));
            std::thread::spawn(move || {
                let mut c = StorageClient::connect(addr).unwrap();
                let mut n = w * 7;
                let mut done = 0u32;
                while !stop.load(Ordering::Relaxed) {
                    n += 1;
                    let (x, y) = (n % ACCOUNTS, (n * 3 + 1) % ACCOUNTS);
                    if x != y && c.doc_write(&[add(format!("a{x:03}"), -1), add(format!("a{y:03}"), 1)], None).is_ok() {
                        done += 1;
                    }
                }
                done
            })
        })
        .collect();
    std::thread::sleep(Duration::from_millis(300));

    // A feed from now, followed by a consumer that "crashes" once.
    let out = root.join("feed.jsonl");
    let cursor_file = root.join("feed.cursor");
    let mut client = StorageClient::connect(addr).unwrap();
    let first = retrying(addr, |c| c.changefeed_poll(None, Some("bank"), 0));
    let mut sink = JsonlSink::open(&out, &cursor_file).unwrap();
    let start_ts = first.cursor.resolved;
    feed::deliver(&mut sink, &first).unwrap();

    // The backup, under load.
    let msg = retrying(addr, |c| c.admin_backup("full"));
    let backup_ts = timestamp_of(&msg);
    eprintln!("{msg}");
    assert!(backup_ts > start_ts);
    assert!(StorageClient::connect(addr).unwrap().admin_backup("full").is_err(), "a backup name is never reused");
    std::thread::sleep(Duration::from_millis(300));

    // Follow to a resolved timestamp, tearing the file in the middle to imitate a crash.
    let mut passes = 0;
    while passes < 2 {
        let c = sink.load_cursor().unwrap();
        let page = client.changefeed_poll(c.as_ref(), Some("bank"), 8).unwrap();
        passes += usize::from(page.resolved.is_some());
        feed::deliver(&mut sink, &page).unwrap();
        if passes == 1 {
            drop(sink);
            let mut torn = std::fs::read(&out).unwrap();
            torn.extend_from_slice(b"{\"collection\":\"bank\",\"key\":\"to");
            std::fs::write(&out, &torn).unwrap();
            sink = JsonlSink::open(&out, &cursor_file).unwrap();
        }
    }

    stop.store(true, Ordering::Relaxed);
    let done: u32 = writers.into_iter().map(|w| w.join().unwrap()).sum();
    assert!(done > 0);
    eprintln!("{done} transfers during the run");

    // Wrong uses are refused precisely.
    let e = retrying(addr, |c| match c.admin_restore("full") {
        Err(StorageError::NotLeader { .. }) => Err(StorageError::NotLeader { leader: None }),
        other => Ok(other),
    })
    .expect_err("restoring over data is refused");
    assert!(e.to_string().contains("already holds data"), "{e}");

    // Restore into a new cluster: conservation holds at the backup's one timestamp, and the rest
    // of the collection is there; the deleted document stays deleted and keeps its version floor.
    let b = start(&root.join("b"), &backups);
    let msg = retrying(b.addrs[1], |c| c.admin_restore("full"));
    eprintln!("{msg}");
    let restored = balances(b.addrs[2]);
    assert_eq!(restored.len(), ACCOUNTS);
    assert_eq!(restored.values().sum::<i64>(), OPENING * ACCOUNTS as i64, "a transfer was torn by the backup");
    let mut c = StorageClient::connect(b.addrs[0]).unwrap();
    assert_eq!(int(&c.doc_get("other", "x").unwrap().unwrap().fields), 7);
    assert!(c.doc_get("other", "gone").unwrap().is_none());
    let again = c.doc_write(&[put("other", "gone".to_string(), 9)], None).unwrap();
    assert!(again.versions[0].is_some_and(|v| v.0 > 2), "a re-created document continues above its old version: {:?}", again.versions);
    assert!(c.admin_restore("full").unwrap_err().to_string().contains("already restored"));

    // A damaged backup is refused before anything is written.
    let d = start(&root.join("d"), &backups);
    let seg = backups.join("full").join("seg-00000000");
    let good = std::fs::read(&seg).unwrap();
    let mut bad = good.clone();
    let mid = bad.len() / 2;
    bad[mid] ^= 1;
    std::fs::write(&seg, &bad).unwrap();
    let mut dc = StorageClient::connect(d.addrs[0]).unwrap();
    assert!(dc.admin_restore("full").is_err());
    std::fs::write(&seg, &good).unwrap();

    // Snapshot + feed = current state: the backup's rows, then every change after its timestamp.
    let feed_only = root.join("from-backup.jsonl");
    let mut sink2 = JsonlSink::open(&feed_only, &root.join("from-backup.cursor")).unwrap();
    let mut cur = feed::Cursor::with_id(4242, backup_ts);
    loop {
        let page = retrying(addr, |c| c.changefeed_poll(Some(&cur), Some("bank"), 16));
        feed::deliver(&mut sink2, &page).unwrap();
        cur = page.cursor;
        if page.resolved.is_some() {
            break;
        }
    }
    let mut state = restored.clone();
    let changes = replay(&feed_only, &mut state);
    assert!(changes > 0);
    assert_eq!(state, balances(addr), "backup + changefeed differs from the cluster");

    // The feed that started before the backup holds the whole interval too (at least once).
    let mut state: BTreeMap<String, i64> = BTreeMap::new();
    assert!(replay(&out, &mut state) > 0);
    assert!(state.keys().len() > 1);

    // A cursor from beyond any clock is refused.
    let far = feed::Cursor { to: Timestamp::new(u64::MAX / 2, 0), ..cur.clone() };
    assert!(StorageClient::connect(addr).unwrap().changefeed_poll(Some(&far), None, 1).unwrap_err().to_string().contains("future"));

    drop((a, b, d));
    let _ = std::fs::remove_dir_all(&root);
}

/// A range cluster with authentication on (`TASK-152`): its nodes and its clients log in.
#[test]
fn a_range_cluster_authenticates() {
    use storage_server::RbacRole;
    let root: PathBuf = std::env::temp_dir().join(format!("storine_range_auth_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
    let peers: HashMap<u16, SocketAddr> = addrs.iter().enumerate().map(|(i, a)| ((i + 1) as u16, *a)).collect();
    let _servers: Vec<_> = (0..3)
        .map(|i| {
            start_range_server(RangeServerConfig {
                data_dir: root.join(format!("n{}", i + 1)),
                bind_addr: addrs[i].to_string(),
                node_id: (i + 1) as u16,
                peers: peers.clone(),
                token: Some("secret-token-for-the-cluster".to_owned()),
                security: SecurityConfig::new(true).with_token("secret-token-for-the-cluster", RbacRole::Admin),
                max_connections: 256,
                max_offset: Duration::from_millis(100),
            })
            .unwrap()
        })
        .collect();
    // Without a token, nothing.
    let mut anon = StorageClient::connect(addrs[0]).unwrap();
    assert!(anon.doc_write(&[put("t", "x".to_owned(), 1)], None).is_err());
    let mut bad = StorageClient::connect(addrs[0]).unwrap();
    assert!(bad.authenticate("wrong").is_err());
    // With one, the nodes have talked to each other and a write commits.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let wrote = (|| -> Result<(), StorageError> {
            let mut c = StorageClient::connect_timeout(addrs[0], Duration::from_secs(5))?;
            c.authenticate("secret-token-for-the-cluster")?;
            c.doc_write(&[put("t", "x".to_owned(), 1)], None).map(|_| ())
        })();
        match wrote {
            Ok(()) => break,
            Err(e) if std::time::Instant::now() > deadline => panic!("no write committed: {e}"),
            Err(_) => std::thread::sleep(Duration::from_millis(200)),
        }
    }
    let _ = std::fs::remove_dir_all(&root);
}
