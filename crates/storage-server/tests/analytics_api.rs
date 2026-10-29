//! Analytics over TCP (`TASK-129`): `OP_DOC_AGGREGATE` and `OP_DOC_SCAN_COLUMNS` against a
//! columnar collection equal the in-process results; OLTP requests keep being served while
//! analytic queries run; hostile bodies are refused.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use storage_codec::PropertyValue as P;
use storage_core::analytics::{AggFunc, Aggregate, AggregateQuery, Expr, GroupKey, ScanQuery};
use storage_core::{DocOp, DocWrite, DocumentStore, EngineConfig, Filter, Precondition};
use storage_server::documents::{ADMIN_CREATE_COLUMNAR, ADMIN_DROP_COLUMNAR};
use storage_server::{SecurityConfig, StorageClient, StorageServer, OP_DOC_ADMIN, OP_DOC_AGGREGATE, OP_DOC_SCAN_COLUMNS};
use storage_types::StorageError;

fn admin(c: &mut StorageClient, cmd: u8, coll: &str) {
    let mut body = vec![cmd];
    storage_core::document_wire::put_str(&mut body, coll);
    c.doc_command(OP_DOC_ADMIN, &body).unwrap();
}

#[test]
fn test_aggregate_and_scan_over_tcp() {
    let dir = std::env::temp_dir().join(format!("stge_olap_tcp_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let store = Arc::new(DocumentStore::open(&dir, EngineConfig::default()).unwrap());
    let mut server = StorageServer::start_documents("127.0.0.1:0", Arc::clone(&store), 1000, SecurityConfig::default(), None).unwrap();
    let addr = server.local_addr();
    let mut c = StorageClient::connect(addr).unwrap();
    let mut batch = Vec::new();
    for i in 0..30_000i64 {
        let mut m = BTreeMap::new();
        m.insert("region".to_owned(), P::Text(["eu", "us", "apac"][(i % 3) as usize].into()));
        m.insert("qty".to_owned(), P::Int64(i % 50));
        m.insert("price".to_owned(), P::Decimal(storage_types::Decimal::new(i128::from(1000 + i % 977), 2).unwrap()));
        m.insert("day".to_owned(), P::Int64(i / 1000));
        batch.push(DocWrite { collection: "sales".into(), key: format!("s{i:06}"), op: DocOp::Put(m), precondition: Precondition::None });
        if batch.len() == 2000 {
            c.doc_write(&std::mem::take(&mut batch), None).unwrap();
        }
    }
    admin(&mut c, ADMIN_CREATE_COLUMNAR, "sales");
    store.columnar("sales").unwrap().unwrap().merge().unwrap();
    // A delta on top of the segments.
    let mut m = BTreeMap::new();
    m.insert("region".to_owned(), P::Text("eu".into()));
    m.insert("qty".to_owned(), P::Int64(1000));
    m.insert("price".to_owned(), P::Decimal(storage_types::Decimal::new(1, 2).unwrap()));
    m.insert("day".to_owned(), P::Int64(99));
    c.doc_write(&[DocWrite { collection: "sales".into(), key: "s000000".into(), op: DocOp::Put(m), precondition: Precondition::None }], None).unwrap();

    let q = AggregateQuery {
        collection: "sales".into(),
        filter: Some(Filter::Ge("day".into(), P::Int64(20))),
        group_by: vec![GroupKey::Field("region".into())],
        aggregates: vec![
            Aggregate { func: AggFunc::Count, arg: None },
            Aggregate { func: AggFunc::Sum, arg: Some(Expr::Mul(Box::new(Expr::Field("price".into())), Box::new(Expr::Field("qty".into())))) },
            Aggregate { func: AggFunc::Avg, arg: Some(Expr::Field("qty".into())) },
            Aggregate { func: AggFunc::Max, arg: Some(Expr::Field("day".into())) },
        ],
        order_by: vec![(1, storage_core::Direction::Desc)],
        limit: Some(2),
        ..Default::default()
    };
    let remote = c.doc_aggregate(&q).unwrap();
    let local = store.aggregate(&q).unwrap();
    assert_eq!(remote, local);
    assert_eq!(remote.rows.len(), 2);
    assert!(remote.pruned_row_groups > 0, "day >= 20 rules out the first row group (days 0-16)");

    // Scans page through the same documents as the store, in key order.
    let mut sq = ScanQuery { collection: "sales".into(), filter: Some(Filter::Eq("region".into(), P::Text("eu".into()))), fields: vec!["qty".into(), "price".into()], after_key: None, limit: 3000 };
    let mut seen = 0;
    loop {
        let page = c.doc_scan_columns(&sq).unwrap();
        assert_eq!(page, store.scan_columns(&sq).unwrap());
        assert_eq!(page.total, 10_000);
        seen += page.rows.len();
        match page.next_after_key {
            Some(k) => sq.after_key = Some(k),
            None => break,
        }
    }
    assert_eq!(seen, 10_000);

    // OLTP stays served while analytic queries run on other connections.
    let stop = Arc::new(AtomicBool::new(false));
    let workers: Vec<_> = (0..3)
        .map(|_| {
            let (stop, q) = (Arc::clone(&stop), q.clone());
            std::thread::spawn(move || {
                let mut c = StorageClient::connect(addr).unwrap();
                let mut n = 0;
                while !stop.load(Ordering::Relaxed) {
                    let mut q = q.clone();
                    q.filter = None;
                    q.limit = None;
                    match c.doc_aggregate(&q) {
                        Ok(_) => n += 1,
                        // More than MAX_CONCURRENT_ANALYTICS at once is answered Overloaded.
                        Err(StorageError::Rejected { status: 0x03, .. }) => {}
                        Err(e) => panic!("{e:?}"),
                    }
                }
                n
            })
        })
        .collect();
    let started = std::time::Instant::now();
    let mut gets = 0;
    let mut worst = std::time::Duration::ZERO;
    while started.elapsed() < std::time::Duration::from_millis(1500) {
        let t = std::time::Instant::now();
        assert!(c.doc_get("sales", "s012345").unwrap().is_some());
        worst = worst.max(t.elapsed());
        gets += 1;
    }
    stop.store(true, Ordering::Relaxed);
    let runs: usize = workers.into_iter().map(|w| w.join().unwrap()).sum();
    assert!(runs > 0 && gets > 100, "runs {runs}, gets {gets}");
    eprintln!("{gets} gets during {runs} concurrent aggregates, worst {worst:?}");

    // Hostile bodies: Invalid, and the connection keeps working.
    for body in [&b""[..], b"\\x05\\0\\0\\0sales\\x02", &[0xFF; 40][..]] {
        let err = c.doc_command(OP_DOC_AGGREGATE, body).unwrap_err();
        assert!(matches!(err, StorageError::Rejected { status: 0x08, .. }), "{err:?}");
        let err = c.doc_command(OP_DOC_SCAN_COLUMNS, body).unwrap_err();
        assert!(matches!(err, StorageError::Rejected { status: 0x08, .. }), "{err:?}");
    }
    // Without column segments the same queries run from the row store.
    admin(&mut c, ADMIN_DROP_COLUMNAR, "sales");
    let mut rowstore = c.doc_aggregate(&q).unwrap();
    rowstore.pruned_row_groups = remote.pruned_row_groups;
    assert_eq!(rowstore, remote);
    server.stop();
    let _ = std::fs::remove_dir_all(&dir);
}
