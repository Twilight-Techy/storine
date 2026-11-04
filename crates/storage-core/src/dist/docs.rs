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
//! - `$req/<request id>`: the versions a request produced through a full transaction (retries
//!   return them). A document also remembers the request ids that wrote it for 60 s (`"r"` in
//!   its value), which is what makes retries safe for one-phase commits: the memory is in the
//!   same range as the write.
//!
//! The range layer's own `_txn` and `_meta` collections are refused as user collection names.
//!
//! **Time-to-live**: `{"v", "f", "x": expiry}`, with the expiry in HLC wall nanoseconds. A
//! document whose expiry is at or below a transaction's read timestamp reads as absent, on
//! every replica alike (no clock of a replica decides); its version floor remains. Expired
//! values are not reclaimed yet (overwriting one replaces it).
//!
//! **Served correctly, not yet fast**: the whole API works, but secondary indexes, vector
//! indexes and column segments are metadata here. Queries scan the collection's span in every
//! range, vector search scores every candidate exactly (perfect recall), and aggregates and
//! column scans run on the coordinator over one [`snapshot`] with the single-node functions
//! (same results). Each is `O(collection)` per call.

use std::collections::BTreeMap;

use storage_codec::field_ops::apply_field_ops;
use storage_codec::{PropertyValue, RecordCodec};
use storage_types::RecordFlags;
use storage_types::{Result, StorageError, Version};

use crate::dist::mvcc::Value;
use crate::dist::server::{Key, KvRequest, KvResponse};
use crate::dist::txn::{self, commit_wait, KvSender, Txn, TxnError};
use crate::document_wire::encode_write_request;
use crate::dist::zone::collection_span;
use crate::document::{
    check_fields, check_vectors, compare_values, cosine, document_id, lookup_path, owned_vector_of, validate_collection_name, validate_document_key, value_ref_of, CollectionInfo, Direction, DocOp, DocPage, DocQuery, DocWrite, Document, DocumentRecord, Nearest, VectorIndexInfo, WriteSummary, MAX_DOCUMENT_INDEXES, MAX_NEAREST_K, MAX_VECTOR_DIMS, SCORE_FIELD,
};
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
        TxnError::Invalid(_) | TxnError::HistoryLost(_) => StorageError::invalid_value("invalid transaction"),
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

/// A stored document as seen at wall time `at`: `Some((version, fields))`, or `None` (absent,
/// deleted or expired) with the version floor a re-created document continues from.
fn decode(v: &Value, at: u64) -> (Option<(u64, Fields)>, u64) {
    let Some(m) = v else { return (None, 0) };
    let version = match m.get("v") {
        Some(PropertyValue::Int64(n)) => (*n).max(0) as u64,
        _ => 0,
    };
    if matches!(m.get("x"), Some(PropertyValue::Int64(x)) if (*x).max(0) as u64 <= at) {
        return (None, version);
    }
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
        let at = t.read_ts().wall;
        let (doc, _) = decode(&t.get(s, &key_of(collection, key))?, at);
        Ok(Ok(doc.map(|(v, fields)| Document { key: key.to_string(), version: Version(v), fields })))
    })
}

fn hex16(id: &[u8; 16]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

/// How long a document remembers the request ids that wrote it (seconds): longer than any SDK
/// keeps retrying one request.
const RID_WINDOW_SECS: u32 = 60;
/// Most request ids one document remembers. A batch that would exceed it goes through a full
/// transaction, which also keeps a `$req` marker.
const RID_MAX: usize = 4096;
/// `u64` id hash, then `u32` seconds.
const RID_ENTRY: usize = 12;

fn rid_hash(rid: &[u8; 16]) -> [u8; 8] {
    let mut h = [0u8; 8];
    for (i, b) in rid.iter().enumerate() {
        h[i % 8] ^= b.rotate_left(i as u32 / 8);
    }
    h
}

/// Whether the stored value `v` remembers request `rid`.
pub(crate) fn rid_seen(v: &Value, rid: &[u8; 16]) -> bool {
    let h = rid_hash(rid);
    matches!(v.as_ref().and_then(|m| m.get("r")), Some(PropertyValue::Bytes(b)) if b.chunks_exact(RID_ENTRY).any(|e| e[..8] == h))
}

/// The version of the document `v` holds at wall time `at`, if it exists then.
pub(crate) fn visible_version(v: &Value, at: u64) -> Option<u64> {
    decode(v, at).0.map(|(version, _)| version)
}

/// What one write does to a document.
pub(crate) struct WriteStep {
    /// The new stored value (`None`: nothing to write).
    pub value: Option<Fields>,
    /// The document's version afterwards (`None`: it does not exist).
    pub version: Option<u64>,
}

/// Applies `w` to the stored value `v` at wall time `at`. `one_phase`: refuse rather than forget
/// a request id still inside its window. Shared by one-phase commits and full transactions, so
/// both decide alike.
pub(crate) fn apply_write(w: &DocWrite, v: &Value, at: u64, rid: Option<&[u8; 16]>, one_phase: bool) -> Result<WriteStep> {
    let id = document_id(&w.collection, &w.key)?;
    let (doc, floor) = decode(v, at);
    let current = doc.as_ref().map(|(n, _)| Version(*n));
    let failed = match w.precondition {
        Precondition::None => None,
        Precondition::Absent if doc.is_some() => Some("record already exists"),
        Precondition::Exists if doc.is_none() => Some("record does not exist"),
        Precondition::Version(n) if current != Some(n) => Some("version mismatch"),
        Precondition::Absent | Precondition::Exists | Precondition::Version(_) => None,
    };
    if let Some(reason) = failed {
        return Err(StorageError::condition_failed(id, reason, current));
    }
    let next = floor.max(current.map_or(0, |n| n.0)) + 1;
    let (mut value, version) = match &w.op {
        DocOp::Put(f) => (Some(encode(next, Some(f.clone()))), Some(next)),
        DocOp::PutWithTtl(f, ttl) => {
            let expiry = at.saturating_add(u64::try_from(ttl.as_nanos()).unwrap_or(u64::MAX)).min(i64::MAX as u64);
            let mut m = encode(next, Some(f.clone()));
            m.insert("x".to_string(), PropertyValue::Int64(expiry as i64));
            (Some(m), Some(next))
        }
        DocOp::Delete => (doc.is_some().then(|| encode(next, None)), None),
        DocOp::Patch(ops) => {
            let Some((_, mut fields)) = doc else { return Err(StorageError::not_found(id)) };
            apply_field_ops(&mut fields, ops).and_then(|()| check_fields(&fields))?;
            (Some(encode(next, Some(fields))), Some(next))
        }
        DocOp::Check => (None, current.map(|n| n.0)),
    };
    // The request ids that wrote this document, within their window.
    if let Some(m) = value.as_mut() {
        let now = (at / 1_000_000_000) as u32;
        let mut list: Vec<u8> = match v.as_ref().and_then(|o| o.get("r")) {
            Some(PropertyValue::Bytes(b)) => b.chunks_exact(RID_ENTRY).filter(|e| now.saturating_sub(u32::from_le_bytes([e[8], e[9], e[10], e[11]])) < RID_WINDOW_SECS).flatten().copied().collect(),
            _ => Vec::new(),
        };
        if let Some(rid) = rid {
            if list.len() / RID_ENTRY >= RID_MAX {
                if one_phase {
                    return Err(StorageError::resource_exhausted("too many recent request ids on this document"));
                }
                list.drain(..RID_ENTRY); // its `$req` marker still answers
            }
            list.extend_from_slice(&rid_hash(rid));
            list.extend_from_slice(&now.to_le_bytes());
        }
        if !list.is_empty() {
            m.insert("r".to_string(), PropertyValue::Bytes(list));
        }
    }
    Ok(WriteStep { value, version })
}

/// An atomic batch of writes with preconditions, applied at most once per `request_id`.
///
/// Batches whose keys share a range and whose collections are known commit in one phase at the
/// leaseholder ([`crate::dist::server::KvRequest::Atomic`]): one consensus round, and contended
/// writers queue on latches instead of wounding each other. Everything else (and anything the
/// leaseholder declines, errors included) runs as a full transaction. A request id is always
/// used, generated if absent: a retry after an unknown outcome must not apply twice.
pub fn write(s: &mut dyn KvSender, writes: Vec<DocWrite>, request_id: Option<[u8; 16]>) -> Result<WriteSummary> {
    for w in &writes {
        check_collection(&w.collection)?;
        validate_document_key(&w.key)?;
        if let DocOp::Put(f) | DocOp::PutWithTtl(f, _) = &w.op {
            check_fields(f)?;
        }
    }
    if writes.is_empty() {
        return Ok(WriteSummary { versions: Vec::new(), duplicate: false });
    }
    let rid = match request_id {
        Some(r) => r,
        None => s.new_id(),
    };
    // Collections are looked at with a plain read (a full write transaction per collection every
    // few seconds would put intents in the way of every writer).
    for w in &writes {
        if !s.collection_is_plain(&w.collection) {
            let meta = run(s, |t, s| Ok(Ok(t.get(s, &key_of(META, &w.collection))?)))?;
            if let Some(m) = meta {
                let plain = info_from(&w.collection, &m).vectors.is_empty();
                s.note_collection(&w.collection, plain);
            }
        }
    }
    if writes.iter().all(|w| s.collection_is_plain(&w.collection)) {
        let mut ids: Vec<Vec<u8>> = Vec::with_capacity(writes.len());
        for w in &writes {
            let id = document_id(&w.collection, &w.key)?.as_bytes().to_vec();
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        let mut body = Vec::new();
        encode_write_request(&writes, Some(rid), &mut body);
        let reply = s.send(vec![KvRequest::Atomic { ids, body }]).pop();
        if let Some(Ok(KvResponse::Atomic { versions, duplicate, ts })) = reply {
            commit_wait(s, ts);
            return Ok(WriteSummary { versions: versions.into_iter().map(|v| v.map(Version)).collect(), duplicate });
        }
        // Declined, or its outcome is unknown: the full transaction finds the request id if it
        // did commit.
    }
    let marker = key_of(REQUESTS, &hex16(&rid));
    let mut plain_seen: Vec<(String, bool)> = Vec::new();
    let out = run(s, |t, s| {
        plain_seen.clear();
        let at = t.read_ts().wall;
        if let Some(done) = t.get(s, &marker)? {
            // Applied before: report what it produced.
            let versions = match done.get("versions") {
                Some(PropertyValue::List(vs)) => vs.iter().map(|v| if let PropertyValue::Int64(n) = v { Some(Version(*n as u64)) } else { None }).collect(),
                _ => Vec::new(),
            };
            return Ok(Ok(WriteSummary { versions, duplicate: true }));
        }
        let mut versions = Vec::with_capacity(writes.len());
        let mut metas: Vec<(String, Fields)> = Vec::new();
        let mut duplicate = false;
        let mut seen_keys: Vec<Key> = Vec::new();
        for w in &writes {
            let k = key_of(&w.collection, &w.key);
            let stored = t.get(s, &k)?;
            // Only state from before this batch can prove a retry: a key written earlier in the
            // batch reads back this transaction's own write, request id included.
            if !seen_keys.contains(&k) {
                duplicate |= rid_seen(&stored, &rid);
                seen_keys.push(k.clone());
            }
            let step = match apply_write(w, &stored, at, Some(&rid), false) {
                Ok(step) => step,
                Err(e) => return Ok(Err(e)),
            };
            versions.push((step.version, visible_version(&stored, at)));
            if let Some(value) = step.value {
                t.put(k, value);
            }
            if matches!(w.op, DocOp::Check) {
                continue;
            }
            // The collection's metadata, created with its first document; its vector indexes
            // bind the fields written (read once per collection, in this transaction).
            let meta = match metas.iter().find(|(c, _)| *c == w.collection) {
                Some((_, m)) => m.clone(),
                None => {
                    let mk = key_of(META, &w.collection);
                    let m = t.get(s, &mk)?;
                    if m.is_none() {
                        t.put(mk, BTreeMap::new());
                    }
                    let m = m.unwrap_or_default();
                    metas.push((w.collection.clone(), m.clone()));
                    m
                }
            };
            let info = info_from(&w.collection, &meta);
            plain_seen.push((w.collection.clone(), info.vectors.is_empty()));
            if let DocOp::Put(f) | DocOp::PutWithTtl(f, _) = &w.op {
                if let Err(e) = check_vectors(&info, f) {
                    return Ok(Err(e));
                }
            }
        }
        if duplicate {
            // A one-phase attempt already committed it: change nothing, report what is there.
            t.discard_writes();
            return Ok(Ok(WriteSummary { versions: versions.iter().map(|(_, cur)| cur.map(Version)).collect(), duplicate: true }));
        }
        let list = versions.iter().map(|(v, _)| v.map_or(PropertyValue::Null, |v| PropertyValue::Int64(v as i64))).collect();
        t.put(marker.clone(), BTreeMap::from([("versions".to_string(), PropertyValue::List(list))]));
        Ok(Ok(WriteSummary { versions: versions.iter().map(|(v, _)| v.map(Version)).collect(), duplicate: false }))
    })?;
    for (c, plain) in plain_seen {
        s.note_collection(&c, plain);
    }
    Ok(out)
}

/// Spans holding a collection's documents: its prefix, and (for names of 6 or more bytes) the
/// prefix long keys hash into (`InlineId::from_any_str` keeps the first 6 bytes of
/// `collection/key`, then `~`).
pub(crate) fn collection_spans(collection: &str) -> Vec<KeySpan> {
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
    let at = t.read_ts().wall;
    for span in collection_spans(collection) {
        for ((c, k), v) in t.scan(s, &span, 0)? {
            if c != collection {
                continue;
            }
            if let (Some((version, fields)), _) = decode(&Some(v), at) {
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
    if q.filter.as_ref().is_some_and(|f| f.depth() > 32) {
        return Err(StorageError::invalid_value("filter nesting is too deep (max 32)"));
    }
    if q.order_by.len() > 16 {
        return Err(StorageError::invalid_value("at most 16 sort keys"));
    }
    if let Some(n) = &q.nearest {
        return nearest(s, q, n);
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

/// Nearest-neighbour search: every candidate scored exactly (the same rules and results as a
/// single node's exact path).
fn nearest(s: &mut dyn KvSender, q: &DocQuery, n: &Nearest) -> Result<DocPage> {
    if n.k == 0 || n.k > MAX_NEAREST_K {
        return Err(StorageError::invalid_value("nearest k must be between 1 and 10000"));
    }
    if n.vector.is_empty() || n.vector.len() > MAX_VECTOR_DIMS || n.vector.iter().any(|x| !x.is_finite()) {
        return Err(StorageError::invalid_value("the query vector must hold 1 to 4096 finite numbers"));
    }
    if !q.order_by.is_empty() || q.after_key.is_some() {
        return Err(StorageError::invalid_value("a nearest-neighbour query is ordered by similarity: no sort keys or cursor"));
    }
    run(s, |t, s| {
        let info = info_from(&q.collection, &t.get(s, &key_of(META, &q.collection))?.unwrap_or_default());
        if info.vectors.iter().any(|v| v.field == n.field && v.dims != n.vector.len()) {
            return Ok(Err(StorageError::invalid_value("the query vector does not have the index's dimensions")));
        }
        let mut hits: Vec<(f64, String, u64, Fields)> = Vec::new();
        for (key, version, fields) in scan_collection(t, s, &q.collection)? {
            if evaluate(q, &key, &fields).is_none() {
                continue;
            }
            let score = fields.get(&n.field).and_then(owned_vector_of).and_then(|v| cosine(&n.vector, &v));
            if let Some(score) = score {
                hits.push((score, key, version, fields));
            }
        }
        hits.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.cmp(&b.1)));
        let total = hits.len();
        let start = q.offset.min(total);
        let end = start.saturating_add(n.k.min(q.limit.unwrap_or(n.k))).min(total);
        let documents = hits
            .drain(start..end)
            .map(|(score, key, version, mut fields)| {
                fields.insert(SCORE_FIELD.to_string(), PropertyValue::Float64(score));
                Document { key, version: Version(version), fields }
            })
            .collect();
        Ok(Ok(DocPage { documents, total, next_after_key: None }))
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
    let vectors = match m.get("vectors") {
        Some(PropertyValue::Map(vs)) => vs.iter().filter_map(|(field, d)| if let PropertyValue::Int64(d) = d { Some(VectorIndexInfo { field: field.clone(), dims: (*d).max(0) as usize, ready: true }) } else { None }).collect(),
        _ => Vec::new(),
    };
    CollectionInfo { name: name.to_string(), indexes: texts("indexes"), building: Vec::new(), extra, vectors, columnar: matches!(m.get("columnar"), Some(PropertyValue::Bool(true))) }
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
    })?;
    s.note_collection(collection, false);
    Ok(())
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

/// Records a vector index on `field` (searches score exactly; writes are checked against it).
pub fn create_vector_index(s: &mut dyn KvSender, collection: &str, field: &str, dims: usize) -> Result<()> {
    if field.is_empty() || field.contains('.') {
        return Err(StorageError::invalid_value("vector indexes cover one top-level field"));
    }
    if dims == 0 || dims > MAX_VECTOR_DIMS {
        return Err(StorageError::invalid_value("vector dimensions must be between 1 and 4096"));
    }
    update_meta(s, collection, |m| {
        let e = m.entry("vectors".to_string()).or_insert_with(|| PropertyValue::Map(BTreeMap::new()));
        if let PropertyValue::Map(vs) = e {
            if matches!(vs.get(field), Some(PropertyValue::Int64(d)) if *d as usize != dims) {
                return Err(StorageError::invalid_value("the field already has a vector index with other dimensions; drop it first"));
            }
            vs.insert(field.to_string(), PropertyValue::Int64(dims as i64));
        }
        Ok(())
    })
}

/// Forgets the vector index on `field`.
pub fn drop_vector_index(s: &mut dyn KvSender, collection: &str, field: &str) -> Result<()> {
    update_meta(s, collection, |m| {
        if let Some(PropertyValue::Map(vs)) = m.get_mut("vectors") {
            vs.remove(field);
        }
        Ok(())
    })
}

/// Records the columnar switch (analytics run over a snapshot either way, with the same results).
pub fn set_columnar(s: &mut dyn KvSender, collection: &str, on: bool) -> Result<()> {
    update_meta(s, collection, |m| {
        m.insert("columnar".to_string(), PropertyValue::Bool(on));
        Ok(())
    })
}

/// Deletes every document of `collection` and its metadata, in batches of transactions (not
/// one atomic step: concurrent writers may add documents meanwhile, as with a single node's
/// drop). Returns how many documents were deleted.
pub fn drop_collection(s: &mut dyn KvSender, collection: &str) -> Result<usize> {
    check_collection(collection)?;
    s.note_collection(collection, false);
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
