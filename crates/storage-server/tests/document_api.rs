//! Document API over TCP (`TASK-098`): CRUD, atomic field operations from concurrent clients,
//! preconditions, idempotent retries, queries, indexes, typed `OP_TXN`, and hostile bodies.

use std::collections::BTreeMap;
use std::sync::Arc;

use storage_codec::field_ops::FieldOp;
use storage_codec::PropertyValue;
use storage_core::{
    DocOp, DocQuery, DocWrite, DocumentStore, EngineConfig, Filter, Precondition,
};
use storage_server::documents::{ADMIN_CREATE_INDEX, INFO_COLLECTION};
use storage_server::{
    SecurityConfig, StorageClient, StorageServer, OP_DOC_ADMIN, OP_DOC_GET, OP_DOC_INFO,
    OP_DOC_QUERY, OP_DOC_WRITE, OP_TXN,
};
use storage_types::{StorageError, Version};

fn dec(s: &str) -> PropertyValue {
    PropertyValue::Decimal(s.parse().unwrap())
}

fn put(c: &str, k: &str, f: BTreeMap<String, PropertyValue>, pre: Precondition) -> DocWrite {
    DocWrite { collection: c.into(), key: k.into(), op: DocOp::Put(f), precondition: pre }
}

fn deposit(key: &str, amount: &str) -> DocWrite {
    DocWrite {
        collection: "accounts".into(),
        key: key.into(),
        op: DocOp::Patch(vec![FieldOp::Add {
            path: "balance".into(),
            delta: dec(amount),
            min: Some(PropertyValue::Int64(0)),
            max: None,
        }]),
        precondition: Precondition::Exists,
    }
}

#[test]
fn test_document_api_over_tcp() {
    let dir = std::env::temp_dir().join(format!("stge_doc_tcp_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let store = Arc::new(DocumentStore::open(&dir, EngineConfig::default()).unwrap());
    let mut server =
        StorageServer::start_documents("127.0.0.1:0", Arc::clone(&store), 1000, SecurityConfig::default(), None)
            .unwrap();
    let addr = server.local_addr();
    let mut c = StorageClient::connect(addr).unwrap();

    let mut f = BTreeMap::new();
    f.insert("balance".to_string(), dec("10.00"));
    f.insert("owner".to_string(), PropertyValue::Text("alice".into()));
    let s = c.doc_write(&[put("accounts", "a1", f.clone(), Precondition::Absent)], None).unwrap();
    let v1 = s.versions[0].unwrap();
    // Create-only twice: conflict.
    let err = c.doc_write(&[put("accounts", "a1", f.clone(), Precondition::Absent)], None).unwrap_err();
    assert!(matches!(err, StorageError::Rejected { status: 0x04, .. }), "{err:?}");
    let doc = c.doc_get("accounts", "a1").unwrap().unwrap();
    assert_eq!((doc.version, &doc.fields), (v1, &f));
    assert!(c.doc_get("accounts", "nope").unwrap().is_none());

    // 4 clients x 50 concurrent +1.00 deposits: nothing is lost.
    let handles: Vec<_> = (0..4)
        .map(|_| {
            std::thread::spawn(move || {
                let mut c = StorageClient::connect(addr).unwrap();
                for _ in 0..50 {
                    c.doc_write(&[deposit("a1", "1.00")], None).unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    assert_eq!(c.doc_get("accounts", "a1").unwrap().unwrap().fields["balance"], dec("210.00"));

    // Overdraft refused atomically; idempotent retry applied once.
    assert!(c.doc_write(&[deposit("a1", "-1000")], None).is_err());
    let rid = [9u8; 16];
    let first = c.doc_write(&[deposit("a1", "-10")], Some(rid)).unwrap();
    let retry = c.doc_write(&[deposit("a1", "-10")], Some(rid)).unwrap();
    assert!(!first.duplicate && retry.duplicate);
    assert_eq!(c.doc_get("accounts", "a1").unwrap().unwrap().fields["balance"], dec("200.00"));

    // CAS with a stale version.
    let cur = c.doc_get("accounts", "a1").unwrap().unwrap().version;
    let err = c
        .doc_write(&[put("accounts", "a1", f.clone(), Precondition::Version(Version(cur.0 - 1)))], None)
        .unwrap_err();
    assert!(matches!(err, StorageError::Rejected { status: 0x04, .. }));

    // Queries, with an index built by an admin job.
    for i in 0..50i64 {
        let mut f = BTreeMap::new();
        f.insert("n".to_string(), PropertyValue::Int64(i));
        c.doc_write(&[put("items", &format!("i{i:03}"), f, Precondition::None)], None).unwrap();
    }
    let q = DocQuery {
        collection: "items".into(),
        filter: Some(Filter::Ge("n".into(), PropertyValue::Int64(40))),
        ..Default::default()
    };
    let before = c.doc_query(&q).unwrap();
    assert_eq!(before.total, 10);
    let mut cmd = vec![ADMIN_CREATE_INDEX];
    storage_core::document_wire::put_str(&mut cmd, "items");
    storage_core::document_wire::put_str(&mut cmd, "n");
    c.doc_command(OP_DOC_ADMIN, &cmd).unwrap();
    let mut info = vec![INFO_COLLECTION];
    storage_core::document_wire::put_str(&mut info, "items");
    let (_, body) = c.doc_command(OP_DOC_INFO, &info).unwrap();
    assert!(body.windows(1).any(|w| w == b"n"));
    assert_eq!(c.doc_query(&q).unwrap(), before);

    // Typed-frame transaction (`OP_TXN`) over the same engine: a failing precondition aborts all.
    let rec = storage_core::DocumentRecord::new("raw", "x");
    let frame = storage_codec::RecordCodec::encode_aligned(&rec, Version(1), 0, storage_types::RecordFlags::NONE, 0);
    let id = storage_core::document::document_id("raw", "x").unwrap();
    let mut body = Vec::new();
    body.extend_from_slice(&1u32.to_le_bytes());
    body.extend_from_slice(&id.to_raw_bytes());
    body.push(1); // absent
    body.extend_from_slice(&0u64.to_le_bytes());
    body.push(1); // put
    body.extend_from_slice(&(frame.as_slice().len() as u32).to_le_bytes());
    body.extend_from_slice(frame.as_slice());
    c.doc_command(OP_TXN, &body).unwrap();
    assert!(c.doc_command(OP_TXN, &body).is_err(), "second create-only must conflict");

    // Hostile bodies: errors, never a crash; the server keeps answering.
    let junk: Vec<Vec<u8>> = vec![
        vec![],
        vec![0xFF; 7],
        vec![1, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF],
        vec![0; 64],
        (0..255u8).collect(),
    ];
    for op in [OP_DOC_GET, OP_DOC_WRITE, OP_DOC_QUERY, OP_DOC_INFO, OP_DOC_ADMIN, OP_TXN] {
        for j in &junk {
            let mut c = StorageClient::connect(addr).unwrap();
            let _ = c.doc_command(op, j);
        }
    }
    assert!(c.doc_get("accounts", "a1").unwrap().is_some());
    server.stop();
    drop(store);
    let _ = std::fs::remove_dir_all(&dir);
}
