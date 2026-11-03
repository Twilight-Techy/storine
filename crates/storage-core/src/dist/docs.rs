//! The document API over distributed transactions (`F-GEO-07`, `TASK-143`).
//!
//! The same operations as [`crate::document::DocumentStore`] (get, atomic conditional write
//! batches with field operations and idempotent request ids, queries, collection metadata),
//! served by a range cluster: every call is one serializable transaction ([`crate::dist::txn`])
//! that may span ranges.
//!
//! **Layout**: a document is one MVCC key `(collection, key)` whose value is
//! `{"v": version, "f": fields}`. Deleting it writes the tombstone `{"v": version}`, so a
//! document re-created later continues above its old version: versions are never reused
//! (`RULE-SAFE-08`). Versions live beside the fields, never among them, so any field name is
//! the user's.
//!
//! **System collections** start with `$`, which user collections cannot:
//! - `$coll/<name>`: the collection's metadata (indexes, extra), written with its first document;
//! - `$req/<request id>`: the versions an idempotent request produced (retries return them).
//!
//! The range layer's own `_txn` and `_meta` collections are refused as user collection names.
//!
//! **Not served on range clusters yet**, refused with a precise error (never mis-executed,
//! `RULE-DX-05`): time-to-live writes, vector indexes and searches, columnar storage. Indexes
//! are recorded as metadata (the SQL gateway reads them back), but queries scan the
//! collection's span in every range: correct, `O(collection)` per query. Aggregates and column
//! scans run on the coordinator over one [`snapshot`] with the single-node functions (same
//! results, `O(collection)`).

use std::collections::BTreeMap;

use storage_codec::field_ops::apply_field_ops;
use storage_codec::{PropertyValue, RecordCodec};
use storage_types::RecordFlags;
use storage_types::{Result, StorageError, Version};

use crate::dist::mvcc::Value;
use crate::dist::server::Key;
use crate::dist::txn::{self, KvSender, Txn, TxnError};
use crate::dist::zone::collection_span;
use crate::document::{check_fields, compare_values, document_id, lookup_path, validate_collection_name, validate_document_key, value_ref_of, CollectionInfo, DocOp, DocPage, DocQuery, DocWrite, Document, DocumentRecord, Direction, WriteSummary, MAX_DOCUMENT_INDEXES};
use crate::engine::Precondition;
use crate::raft::KeySpan;

/// Fields of a document.
pub type Fields = BTreeMap<String, PropertyValue>;

const META: &str = "$coll";
const REQUESTS: &str = "$req";
/// Collections of the range layer itself.
const RESERVED: [&str; 2] = [crate::dist::mvcc::TXN_COLLECTION, crate::dist::meta::META_COLLECTION];

/// Converts a transaction failure into the engine's error vocabulary (so the wire layer reports
/// it as for a single node).
fn txn_err(e: TxnError) -> StorageError {
    match e {
        TxnError::Retry(_) => StorageError::resource_exhausted("the transaction kept conflicting with concurrent writes; retry"),
        TxnError::Unavailable(_) => StorageError::NotLeader { leader: None },
        TxnError::CommitUnknown(_) => StorageError::CommitUnknown { index: 0 },
        TxnError::Invalid(_) => StorageError::invalid_value("invalid transaction"),
    }
}

/// Runs `f` as one transaction. `f` returns `Ok(Err(e))` for a domain error (a failed
/// precondition, a bad value): nothing is written, and `e` is returned as is.
fn run<R>(s: &mut dyn KvSender, mut f: impl FnMut(&mut Txn, &mut dyn KvSender) -> std::result::Result<Result<R>, TxnError>) -> Result<R> {
    let (out, _) = txn::run(s, |t, s| {
        let r = f(t, s)?;
        if r.is_err() {
            t.discard_writes();
        }
        Ok(r)
    })
    .map_err(txn_err)?;
    out
}

fn check_collection(name: &str) -> Result<()> {
    validate_collection_name(name, false)?;
    if RESERVED.contains(&name) {
        return Err(StorageError::invalid_key("collection names '_txn' and '_meta' are reserved on range clusters"));
    }
    Ok(())
}

/// A stored document: `Some((version, fields))`, or `None` (absent or deleted) with the version
/// floor a re-created document continues from.
fn decode(v: &Value) -> (Option<(u64, Fields)>, u64) {
    let Some(m) = v else { return (None, 0) };
    let version = match m.get("v") {
        Some(PropertyValue::Int64(n)) => (*n).max(0) as u64,
        _ => 0,
    };
    match m.get("f") {
        Some(PropertyValue::Map(f)) => (Some((version, f.clone())), version),
        _ => (None, version),
    }
}

fn encode(version: u64, fields: Option<Fields>) -> Fields {
    let mut m = BTreeMap::from([("v".to_string(), PropertyValue::Int64(version.min(i64::MAX as u64) as i64))]);
    if let Some(f) = fields {
        m.insert("f".to_string(), PropertyValue::Map(f));
    }
    m
}

fn key_of(collection: &str, key: &str) -> Key {
    (collection.to_string(), key.to_string())
}

/// Reads one document.
pub fn get(s: &mut dyn KvSender, collection: &str, key: &str) -> Result<Option<Document>> {
    check_collection(collection)?;
    validate_document_key(key)?;
    run(s, |t, s| {
        let (doc, _) = decode(&t.get(s, &key_of(collection, key))?);
        Ok(Ok(doc.map(|(v, fields)| Document { key: key.to_string(), version: Version(v), fields })))
    })
}

fn hex16(id: &[u8; 16]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

/// An atomic batch of writes with preconditions, applied at most once per `request_id`.
pub fn write(s: &mut dyn KvSender, writes: Vec<DocWrite>, request_id: Option<[u8; 16]>) -> Result<WriteSummary> {
    for w in &writes {
        check_collection(&w.collection)?;
        validate_document_key(&w.key)?;
        match &w.op {
            DocOp::Put(f) => check_fields(f)?,
            DocOp::PutWithTtl(..) => return Err(StorageError::invalid_value("time-to-live writes are not supported on range clusters")),
            DocOp::Delete | DocOp::Patch(_) | DocOp::Check => {}
        }
    }
    if writes.is_empty() {
        return Ok(WriteSummary { versions: Vec::new(), duplicate: false });
    }
    let marker = request_id.map(|r| key_of(REQUESTS, &hex16(&r)));
    run(s, |t, s| {
        if let Some(m) = &marker {
            if let Some(done) = t.get(s, m)? {
                // Applied before: report what it produced.
                let versions = match done.get("versions") {
                    Some(PropertyValue::List(vs)) => vs.iter().map(|v| if let PropertyValue::Int64(n) = v { Some(Version(*n as u64)) } else { None }).collect(),
                    _ => Vec::new(),
                };
                return Ok(Ok(WriteSummary { versions, duplicate: true }));
            }
        }
        let mut versions = Vec::with_capacity(writes.len());
        let mut seen_collections: Vec<&str> = Vec::new();
        for w in &writes {
            let k = key_of(&w.collection, &w.key);
            let id = match document_id(&w.collection, &w.key) {
                Ok(id) => id,
                Err(e) => return Ok(Err(e)),
            };
            let (doc, floor) = decode(&t.get(s, &k)?);
            let current = doc.as_ref().map(|(v, _)| Version(*v));
            let failed = match w.precondition {
                Precondition::None => None,
                Precondition::Absent if doc.is_some() => Some("record already exists"),
                Precondition::Exists if doc.is_none() => Some("record does not exist"),
                Precondition::Version(v) if current != Some(v) => Some("version mismatch"),
                Precondition::Absent | Precondition::Exists | Precondition::Version(_) => None,
            };
            if let Some(reason) = failed {
                return Ok(Err(StorageError::condition_failed(id, reason, current)));
            }
            let next = floor.max(current.map_or(0, |v| v.0)) + 1;
            let result = match &w.op {
                DocOp::Put(f) => {
                    t.put(k, encode(next, Some(f.clone())));
                    Some(Version(next))
                }
                DocOp::Delete => {
                    if doc.is_some() {
                        t.put(k, encode(next, None));
                    }
                    None
                }
                DocOp::Patch(ops) => {
                    let Some((_, mut fields)) = doc else { return Ok(Err(StorageError::not_found(id))) };
                    if let Err(e) = apply_field_ops(&mut fields, ops).and_then(|()| check_fields(&fields)) {
                        return Ok(Err(e));
                    }
                    t.put(k, encode(next, Some(fields)));
                    Some(Version(next))
                }
                DocOp::Check => current,
                DocOp::PutWithTtl(..) => None,
            };
            versions.push(result);
            if !seen_collections.contains(&w.collection.as_str()) && !matches!(w.op, DocOp::Check) {
                seen_collections.push(&w.collection);
                // The collection's metadata, created with its first document.
                let mk = key_of(META, &w.collection);
                if t.get(s, &mk)?.is_none() {
                    t.put(mk, BTreeMap::new());
                }
            }
        }
        if let Some(m) = &marker {
            let list = versions.iter().map(|v| v.map_or(PropertyValue::Null, |v| PropertyValue::Int64(v.0 as i64))).collect();
            t.put(m.clone(), BTreeMap::from([("versions".to_string(), PropertyValue::List(list))]));
        }
        Ok(Ok(WriteSummary { versions: versions.clone(), duplicate: false }))
    })
}

/// Spans holding a collection's documents: its prefix, and (for names of 6 or more bytes) the
/// prefix long keys hash into (`InlineId::from_any_str` keeps the first 6 bytes of
/// `collection/key`, then `~`).
fn collection_spans(collection: &str) -> Vec<KeySpan> {
    let mut spans = vec![collection_span(collection)];
    let full = format!("{collection}/");
    if full.len() > 6 {
        let mut p = 6;
        while p > 0 && !full.is_char_boundary(p) {
            p -= 1;
        }
        let mut start = full.as_bytes()[..p].to_vec();
        let mut end = start.clone();
        start.push(b'~');
        end.push(b'~' + 1);
        spans.push(KeySpan::new(start, Some(end)));
    }
    spans
}

/// Every document of `collection` in one snapshot of `t`.
fn scan_collection(t: &mut Txn, s: &mut dyn KvSender, collection: &str) -> std::result::Result<Vec<(String, u64, Fields)>, TxnError> {
    let mut out = Vec::new();
    for span in collection_spans(collection) {
        for ((c, k), v) in t.scan(s, &span, 0)? {
            if c != collection {
                continue;
            }
            if let (Some((version, fields)), _) = decode(&Some(v)) {
                out.push((k, version, fields));
            }
        }
    }
    Ok(out)
}

/// The filter's verdict and the sort values of one document, through the same zero-copy
/// matcher as single-node queries (identical semantics).
fn evaluate(q: &DocQuery, key: &str, fields: &Fields) -> Option<Vec<PropertyValue>> {
    let rec = DocumentRecord { collection: String::new(), key: key.to_string(), fields: fields.clone(), index_ids: Vec::new() };
    let frame = RecordCodec::encode_aligned(&rec, Version(1), 0, RecordFlags::NONE, 0);
    let view = RecordCodec::decode_fast::<DocumentRecord>(frame.as_slice()).ok()?;
    let map = view.archived().fields.view(view.payload());
    if let Some(f) = &q.filter {
        if !f.matches(key, map) {
            return None;
        }
    }
    Some(q.order_by.iter().map(|(path, _)| lookup_path(map, path).to_owned_value()).collect())
}

/// A query: filter, sort, offset and limit, over one snapshot of every range.
pub fn query(s: &mut dyn KvSender, q: &DocQuery) -> Result<DocPage> {
    check_collection(&q.collection)?;
    if q.nearest.is_some() {
        return Err(StorageError::invalid_value("vector search is not supported on range clusters"));
    }
    if q.filter.as_ref().is_some_and(|f| f.depth() > 32) {
        return Err(StorageError::invalid_value("filter nesting is too deep (max 32)"));
    }
    if q.order_by.len() > 16 {
        return Err(StorageError::invalid_value("at most 16 sort keys"));
    }
    run(s, |t, s| {
        let mut hits: Vec<(String, u64, Fields, Vec<PropertyValue>)> = Vec::new();
        for (key, version, fields) in scan_collection(t, s, &q.collection)? {
            if q.order_by.is_empty() && q.after_key.as_ref().is_some_and(|a| key.as_str() <= a.as_str()) {
                continue;
            }
            if let Some(sv) = evaluate(q, &key, &fields) {
                hits.push((key, version, fields, sv));
            }
        }
        hits.sort_by(|a, b| {
            for (i, (_, dir)) in q.order_by.iter().enumerate() {
                let mut c = compare_values(&value_ref_of(&a.3[i]), &value_ref_of(&b.3[i]));
                if *dir == Direction::Desc {
                    c = c.reverse();
                }
                if c != core::cmp::Ordering::Equal {
                    return c;
                }
            }
            a.0.cmp(&b.0)
        });
        let total = hits.len();
        let start = q.offset.min(total);
        let end = q.limit.map_or(total, |l| start.saturating_add(l).min(total));
        let more = end < total;
        let documents: Vec<Document> = hits.drain(start..end).map(|(key, v, fields, _)| Document { key, version: Version(v), fields }).collect();
        let next_after_key = (more && q.order_by.is_empty()).then(|| documents.last().map(|d| d.key.clone())).flatten();
        Ok(Ok(DocPage { documents, total, next_after_key }))
    })
}

/// Every document of `collection` in one consistent snapshot, in key order (the input of
/// analytics computed by the coordinator: aggregates and column scans, `O(collection)`).
pub fn snapshot(s: &mut dyn KvSender, collection: &str) -> Result<Vec<Document>> {
    check_collection(collection)?;
    run(s, |t, s| {
        let mut docs: Vec<Document> = scan_collection(t, s, collection)?.into_iter().map(|(key, v, fields)| Document { key, version: Version(v), fields }).collect();
        docs.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(Ok(docs))
    })
}

fn info_from(name: &str, m: &Fields) -> CollectionInfo {
    let texts = |f: &str| match m.get(f) {
        Some(PropertyValue::List(vs)) => vs.iter().filter_map(|v| if let PropertyValue::Text(t) = v { Some(t.clone()) } else { None }).collect(),
        _ => Vec::new(),
    };
    let extra = match m.get("extra") {
        Some(PropertyValue::Map(e)) => e.clone(),
        _ => BTreeMap::new(),
    };
    CollectionInfo { name: name.to_string(), indexes: texts("indexes"), building: Vec::new(), extra, vectors: Vec::new(), columnar: false }
}

/// A collection's declared state (default if it was never written).
pub fn collection_info(s: &mut dyn KvSender, collection: &str) -> Result<CollectionInfo> {
    check_collection(collection)?;
    run(s, |t, s| Ok(Ok(info_from(collection, &t.get(s, &key_of(META, collection))?.unwrap_or_default()))))
}

/// Every collection that was ever written (and not dropped), by name.
pub fn list_collections(s: &mut dyn KvSender) -> Result<Vec<CollectionInfo>> {
    run(s, |t, s| {
        let rows = t.scan(s, &collection_span(META), 0)?;
        Ok(Ok(rows.iter().filter(|((c, _), _)| c == META).map(|((_, name), m)| info_from(name, m)).collect()))
    })
}

/// Changes a collection's metadata with `f`.
fn update_meta(s: &mut dyn KvSender, collection: &str, f: impl Fn(&mut Fields) -> Result<()>) -> Result<()> {
    check_collection(collection)?;
    run(s, |t, s| {
        let k = key_of(META, collection);
        let mut m = t.get(s, &k)?.unwrap_or_default();
        if let Err(e) = f(&mut m) {
            return Ok(Err(e));
        }
        t.put(k, m);
        Ok(Ok(()))
    })
}

/// Replaces a collection's free-form metadata (the SQL gateway's table schema).
pub fn set_collection_extra(s: &mut dyn KvSender, collection: &str, extra: Fields) -> Result<()> {
    update_meta(s, collection, |m| {
        m.insert("extra".to_string(), PropertyValue::Map(extra.clone()));
        Ok(())
    })
}

/// Records an index on `field` (see the module docs: queries still scan).
pub fn create_index(s: &mut dyn KvSender, collection: &str, field: &str) -> Result<()> {
    if field.is_empty() || field.contains('.') {
        return Err(StorageError::invalid_value("indexes are on non-empty top-level field names"));
    }
    update_meta(s, collection, |m| {
        let mut list = match m.remove("indexes") {
            Some(PropertyValue::List(vs)) => vs,
            _ => Vec::new(),
        };
        if !list.contains(&PropertyValue::Text(field.to_string())) {
            if list.len() >= MAX_DOCUMENT_INDEXES {
                m.insert("indexes".to_string(), PropertyValue::List(list));
                return Err(StorageError::invalid_value("too many indexes on this collection (max 16)"));
            }
            list.push(PropertyValue::Text(field.to_string()));
        }
        m.insert("indexes".to_string(), PropertyValue::List(list));
        Ok(())
    })
}

/// Forgets the index on `field`.
pub fn drop_index(s: &mut dyn KvSender, collection: &str, field: &str) -> Result<()> {
    update_meta(s, collection, |m| {
        if let Some(PropertyValue::List(vs)) = m.get_mut("indexes") {
            vs.retain(|v| v != &PropertyValue::Text(field.to_string()));
        }
        Ok(())
    })
}

/// Deletes every document of `collection` and its metadata, in batches of transactions (not
/// one atomic step: concurrent writers may add documents meanwhile, as with a single node's
/// drop). Returns how many documents were deleted.
pub fn drop_collection(s: &mut dyn KvSender, collection: &str) -> Result<usize> {
    check_collection(collection)?;
    let mut deleted = 0;
    loop {
        let batch = run(s, |t, s| {
            let docs = scan_collection(t, s, collection)?;
            let n = docs.len().min(256);
            for (key, version, _) in docs.into_iter().take(n) {
                t.put(key_of(collection, &key), encode(version + 1, None));
            }
            Ok(Ok(n))
        })?;
        deleted += batch;
        if batch == 0 {
            break;
        }
    }
    run(s, |t, s| {
        let k = key_of(META, collection);
        if t.get(s, &k)?.is_some() {
            t.delete(k);
        }
        Ok(Ok(()))
    })?;
    Ok(deleted)
}
