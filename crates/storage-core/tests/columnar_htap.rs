//! Columnar HTAP projection (`TASK-126`): the analytic view always equals the row store as of
//! its snapshot, through updates, deletes, merges, restarts and concurrent writers.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use storage_codec::types::PropertyValue as P;
use storage_core::{DocumentStore, EngineConfig};
use storage_types::Decimal;

fn dir(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("stge_htap_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn fields(pairs: &[(&str, P)]) -> BTreeMap<String, P> {
    pairs.iter().map(|(k, v)| ((*k).to_owned(), v.clone())).collect()
}

fn dec(s: &str) -> P {
    P::Decimal(s.parse::<Decimal>().unwrap())
}

/// The columnar view's documents equal the row store's (same keys, versions and fields).
fn assert_view_matches(store: &DocumentStore, coll: &str) {
    let c = store.columnar(coll).unwrap().expect("columnar");
    let snap = c.snapshot().unwrap();
    let view = snap.documents().unwrap();
    drop(snap);
    let rows = store.scan_collection(coll).unwrap();
    assert_eq!(view.len(), rows.len(), "document count");
    for (v, r) in view.iter().zip(&rows) {
        assert_eq!(v, r, "document {}", r.key);
    }
}

fn doc(i: u64, rev: u64) -> BTreeMap<String, P> {
    let mut m = fields(&[
        ("n", P::Int64(i as i64)),
        ("price", dec(&format!("{}.{:02}", i * 3 + rev, rev % 100))),
        ("city", P::Text(["Lagos", "Berlin", "Lima"][(i % 3) as usize].into())),
        ("rev", P::UInt64(rev)),
    ]);
    if i % 4 == 0 {
        m.insert("meta".into(), P::Map(fields(&[("ok", P::Bool(rev % 2 == 0)), ("at", P::TimestampNs(i as i64 * 1000))])));
    }
    if i % 5 == 0 {
        m.insert("tags".into(), P::List(vec![P::Text("x".into()), P::Int64(rev as i64)]));
    }
    if i % 7 == 0 {
        m.insert("gone".into(), P::Null);
        m.insert("empty".into(), P::Map(BTreeMap::new()));
    }
    // A field whose kind changes with each revision.
    m.insert("mixed".into(), if rev % 2 == 0 { P::Int64(rev as i64) } else { P::Text(format!("r{rev}")) });
    m
}

#[test]
fn view_stays_exact_through_updates_deletes_merges_and_restart() {
    let d = dir("exact");
    {
        let s = DocumentStore::open(&d, EngineConfig::default()).unwrap();
        for i in 0..3000 {
            s.put("orders", &format!("o{i:05}"), doc(i, 0)).unwrap();
        }
        s.put("other", "x", doc(1, 1)).unwrap();
        s.create_columnar("orders").unwrap();
        // Exact before the first build (everything from the row store) ...
        assert_view_matches(&s, "orders");
        let c = s.columnar("orders").unwrap().unwrap();
        // (The background merger may already have built it: either way all rows are in.)
        c.merge().unwrap();
        assert_eq!(c.segment_rows(), 3000);
        // ... and after it.
        assert_view_matches(&s, "orders");
        for i in (0..3000).step_by(3) {
            s.put("orders", &format!("o{i:05}"), doc(i, 1)).unwrap();
        }
        for i in (1..3000).step_by(10) {
            s.delete("orders", &format!("o{i:05}")).unwrap();
        }
        for i in 3000..3500 {
            s.put("orders", &format!("o{i:05}"), doc(i, 0)).unwrap();
        }
        s.put("other", "y", doc(2, 2)).unwrap();
        assert_view_matches(&s, "orders");
        c.merge().unwrap();
        assert_view_matches(&s, "orders");
        // Changes after the merge, left in the delta across a restart.
        for i in (2..3500).step_by(7) {
            s.put("orders", &format!("o{i:05}"), doc(i, 2)).unwrap();
        }
        s.delete("orders", "o00000").unwrap();
        assert_view_matches(&s, "orders");
    }
    {
        let s = DocumentStore::open(&d, EngineConfig::default()).unwrap();
        assert_view_matches(&s, "orders");
        let c = s.columnar("orders").unwrap().unwrap();
        c.merge().unwrap();
        assert_view_matches(&s, "orders");
        // Many merges: segments are compacted, never more than the limit.
        for round in 0..12u64 {
            for i in (round..3500).step_by(13) {
                s.put("orders", &format!("o{i:05}"), doc(i, 10 + round)).unwrap();
            }
            c.merge().unwrap();
            assert!(c.snapshot().unwrap().segments.len() <= 8);
        }
        assert_view_matches(&s, "orders");
        assert!(c.last_error().is_none());
        s.drop_columnar("orders").unwrap();
        assert!(s.columnar("orders").unwrap().is_none());
    }
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn seeded_random_operations_keep_the_view_exact() {
    let d = dir("random");
    let mut x: u64 = 0x2545_F491_4F6C_DD1D;
    let mut rnd = |m: u64| {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x % m
    };
    let mut store = Some(DocumentStore::open(&d, EngineConfig::default()).unwrap());
    store.as_ref().unwrap().create_columnar("t").unwrap();
    for step in 0..4000u64 {
        let s = store.as_ref().unwrap();
        match rnd(100) {
            0..=59 => {
                let k = rnd(400);
                s.put("t", &format!("k{k}"), doc(k, step)).unwrap();
            }
            60..=79 => {
                let _ = s.delete("t", &format!("k{}", rnd(400))).unwrap();
            }
            80..=89 => {
                s.columnar("t").unwrap().unwrap().merge().unwrap();
            }
            90..=96 => assert_view_matches(s, "t"),
            _ => {
                drop(store.take());
                store = Some(DocumentStore::open(&d, EngineConfig::default()).unwrap());
                assert_view_matches(store.as_ref().unwrap(), "t");
            }
        }
    }
    assert_view_matches(store.as_ref().unwrap(), "t");
    drop(store);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn snapshots_are_consistent_while_writers_and_merges_run() {
    // Transfers keep the total at exactly 100,000.00. Every snapshot's total must be exactly
    // that: a view mixing two moments would see a transfer half done.
    let d = dir("consistent");
    let s = Arc::new(DocumentStore::open(&d, EngineConfig::default()).unwrap());
    for i in 0..200 {
        s.put("acct", &format!("a{i:03}"), fields(&[("balance", dec("500.00"))])).unwrap();
    }
    s.create_columnar("acct").unwrap();
    s.columnar("acct").unwrap().unwrap().merge().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for t in 0..4u64 {
        let s = Arc::clone(&s);
        let stop = Arc::clone(&stop);
        writers.push(std::thread::spawn(move || {
            use storage_codec::field_ops::FieldOp;
            use storage_core::{DocOp, DocWrite, Precondition};
            let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ t;
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let (a, b) = (x % 200, (x >> 20) % 200);
                if a == b {
                    continue;
                }
                let amount = format!("{}.{:02}", x % 50, (x >> 8) % 100);
                let add = |key: u64, delta: P| DocWrite {
                    collection: "acct".into(),
                    key: format!("a{key:03}"),
                    op: DocOp::Patch(vec![FieldOp::Add { path: "balance".into(), delta, min: None, max: None }]),
                    precondition: Precondition::Exists,
                };
                let neg = P::Decimal(format!("-{amount}").parse::<Decimal>().unwrap());
                s.write(vec![add(a, neg), add(b, dec(&amount))], None).unwrap();
                n += 1;
            }
            n
        }));
    }
    let merger = {
        let s = Arc::clone(&s);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let c = s.columnar("acct").unwrap().unwrap();
            let mut merges = 0;
            while !stop.load(Ordering::Relaxed) {
                c.merge().unwrap();
                merges += 1;
            }
            merges
        })
    };
    let c = s.columnar("acct").unwrap().unwrap();
    let want: Decimal = "100000.00".parse().unwrap();
    let started = std::time::Instant::now();
    let mut checked = 0;
    while started.elapsed() < std::time::Duration::from_secs(2) || checked < 150 {
        checked += 1;
        let snap = c.snapshot().unwrap();
        let mut total: Decimal = "0".parse().unwrap();
        let mut count = 0;
        for doc in snap.documents().unwrap() {
            let Some(P::Decimal(b)) = doc.fields.get("balance") else { panic!("balance missing") };
            total = total.checked_add(*b).unwrap();
            count += 1;
        }
        assert_eq!(count, 200);
        assert_eq!(total, want, "a snapshot saw a transfer half done");
    }
    stop.store(true, Ordering::Relaxed);
    let transfers: u64 = writers.into_iter().map(|w| w.join().unwrap()).sum();
    let merges = merger.join().unwrap();
    assert!(transfers > 100 && merges > 2, "transfers {transfers}, merges {merges}");
    println!("{checked} consistent snapshots during {transfers} transfers and {merges} merges");
    assert_view_matches(&s, "acct");
    drop(c);
    drop(s);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn encrypted_stores_seal_segments() {
    use storage_codec::crypto::EncryptionKey;
    use storage_core::EncryptionConfig;
    let d = dir("sealed");
    let cfg = || EngineConfig { encryption: Some(EncryptionConfig::strict(3, EncryptionKey::from_bytes([7u8; 32]))), ..EngineConfig::default() };
    {
        let s = DocumentStore::open(&d, cfg()).unwrap();
        for i in 0..500 {
            s.put("secret", &format!("s{i}"), doc(i, 0)).unwrap();
        }
        s.create_columnar("secret").unwrap();
        s.columnar("secret").unwrap().unwrap().merge().unwrap();
        assert_view_matches(&s, "secret");
    }
    // No plaintext field values in the segment files.
    let col_dir = storage_core::columnar::collection_dir(&d, "secret");
    for e in std::fs::read_dir(&col_dir).unwrap().flatten() {
        let bytes = std::fs::read(e.path()).unwrap();
        assert!(!bytes.windows(5).any(|w| w == b"Lagos"), "plaintext in {:?}", e.path());
    }
    let s = DocumentStore::open(&d, cfg()).unwrap();
    assert_view_matches(&s, "secret");
    drop(s);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn files_left_by_a_crash_are_swept_and_never_trusted() {
    let d = dir("sweep");
    {
        let s = DocumentStore::open(&d, EngineConfig::default()).unwrap();
        for i in 0..400 {
            s.put("t", &format!("k{i}"), doc(i, 0)).unwrap();
        }
        s.create_columnar("t").unwrap();
        s.columnar("t").unwrap().unwrap().merge().unwrap();
    }
    // A crash after writing files but before publishing them in the manifest.
    let col = storage_core::columnar::collection_dir(&d, "t");
    std::fs::write(col.join("seg_00000000000000ff.col"), b"half-written segment").unwrap();
    std::fs::write(col.join("seg_00000000000000ff.col.tmp"), b"tmp").unwrap();
    std::fs::write(col.join("seg_0000000000000001.del9"), b"stale bitmap").unwrap();
    {
        let s = DocumentStore::open(&d, EngineConfig::default()).unwrap();
        let names: Vec<String> = std::fs::read_dir(&col).unwrap().flatten().map(|e| e.file_name().to_string_lossy().into_owned()).collect();
        assert!(names.iter().all(|n| !n.contains("ff.col") && !n.ends_with(".del9")), "{names:?}");
        assert_view_matches(&s, "t");
        // A corrupted manifest: the projection starts over from the row store, still exact.
        drop(s);
        std::fs::write(col.join("MANIFEST"), b"garbage").unwrap();
        let s = DocumentStore::open(&d, EngineConfig::default()).unwrap();
        assert_view_matches(&s, "t");
        let c = s.columnar("t").unwrap().unwrap();
        c.merge().unwrap();
        assert_view_matches(&s, "t");
    }
    let _ = std::fs::remove_dir_all(&d);
}
