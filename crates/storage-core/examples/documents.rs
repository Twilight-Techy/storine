//! The embedded document store, as shown on the website's "Embedding the engine" page
//! (`website/content/embedding.md`). `cargo run -p storage-core --example documents`.

use std::collections::BTreeMap;

use storage_codec::field_ops::FieldOp;
use storage_codec::PropertyValue;
use storage_core::{
    Direction, DocOp, DocQuery, DocWrite, DocumentStore, EngineConfig, Filter, Precondition,
};
use storage_types::Decimal;

fn money(s: &str) -> PropertyValue {
    PropertyValue::Decimal(s.parse::<Decimal>().expect("a decimal"))
}

fn main() -> storage_types::Result<()> {
    let dir = std::env::temp_dir().join(format!("storine-example-{}", std::process::id()));
    let store = DocumentStore::open(&dir, EngineConfig::default())?;

    let mut alice = BTreeMap::new();
    alice.insert("owner".to_string(), PropertyValue::Text("Alice".into()));
    alice.insert("balance".to_string(), money("100.00"));
    store.insert("accounts", "alice", alice)?;
    store.put(
        "accounts",
        "bob",
        BTreeMap::from([("owner".to_string(), PropertyValue::Text("Bob".into()))]),
    )?;

    // Server-side, checked arithmetic: fails instead of going below zero.
    store.patch(
        "accounts",
        "alice",
        vec![FieldOp::Add {
            path: "balance".into(),
            delta: money("-25.00"),
            min: Some(money("0")),
            max: None,
        }],
    )?;

    // Several writes, all or nothing, applied at most once for this request id.
    let alice_version = store.get("accounts", "alice")?.expect("exists").version;
    let summary = store.write(
        vec![
            DocWrite {
                collection: "accounts".into(),
                key: "alice".into(),
                op: DocOp::Patch(vec![FieldOp::Add {
                    path: "balance".into(),
                    delta: money("-10.00"),
                    min: Some(money("0")),
                    max: None,
                }]),
                precondition: Precondition::Version(alice_version),
            },
            DocWrite {
                collection: "accounts".into(),
                key: "bob".into(),
                op: DocOp::Patch(vec![FieldOp::Add {
                    path: "balance".into(),
                    delta: money("10.00"),
                    min: None,
                    max: None,
                }]),
                precondition: Precondition::Exists,
            },
        ],
        Some(*b"transfer-0000001"),
    )?;
    assert!(!summary.duplicate);

    store.create_index("accounts", "owner")?;
    let page = store.query(&DocQuery {
        collection: "accounts".into(),
        filter: Some(Filter::Gt("balance".into(), money("50"))),
        order_by: vec![("balance".into(), Direction::Desc)],
        limit: Some(10),
        ..DocQuery::default()
    })?;
    for doc in &page.documents {
        println!(
            "{} v{} {:?}",
            doc.key,
            doc.version.0,
            doc.fields.get("balance")
        );
    }

    // An embedded store does not checkpoint on its own: this cuts the write-ahead log.
    store.engine().checkpoint()?;
    drop(store);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
