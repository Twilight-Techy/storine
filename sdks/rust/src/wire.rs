//! Filters, field operations and the request / response encodings (protocol/PROTOCOL.md §5-§6).

use std::ops::{BitAnd, BitOr, Not};

use crate::error::{invalid, Result};
use crate::value::{encode_fields, encode_value, put_str, put_u32, Doc, Reader, Value, MAX_DEPTH};

pub(crate) const OP_AUTH: u8 = 0x0C;
pub(crate) const OP_DOC_GET: u8 = 0x20;
pub(crate) const OP_DOC_WRITE: u8 = 0x21;
pub(crate) const OP_DOC_QUERY: u8 = 0x22;
pub(crate) const OP_DOC_ADMIN: u8 = 0x23;
pub(crate) const OP_DOC_INFO: u8 = 0x25;
pub(crate) const OP_SET_OPTIONS: u8 = 0x26;
pub(crate) const OP_CLUSTER_INFO: u8 = 0x27;
pub(crate) const ST_OK: u8 = 0;
pub(crate) const ST_NOT_FOUND: u8 = 1;
pub(crate) const ST_OVERLOADED: u8 = 3;
pub(crate) const ST_CONFLICT: u8 = 4;
pub(crate) const ST_UNAUTHORIZED: u8 = 5;
pub(crate) const ST_RATE_LIMITED: u8 = 6;
pub(crate) const ST_NOT_LEADER: u8 = 7;
pub(crate) const ST_INVALID: u8 = 8;
pub(crate) const ST_COMMIT_UNKNOWN: u8 = 9;

/// One atomic change applied server-side by `update`: [`set`], [`unset`], [`add`], [`append`].
#[derive(Debug, Clone, PartialEq)]
pub struct FieldOp {
    kind: u8,
    path: String,
    value: Value,
    min: Option<Value>,
    max: Option<Value>,
}

/// Sets the field at `path` (dotted for nested maps) to `value`.
pub fn set(path: &str, value: impl Into<Value>) -> FieldOp {
    FieldOp { kind: 1, path: path.to_owned(), value: value.into(), min: None, max: None }
}

/// Removes the field at `path`.
pub fn unset(path: &str) -> FieldOp {
    FieldOp { kind: 2, path: path.to_owned(), value: Value::Null, min: None, max: None }
}

/// Adds `amount` (negative to subtract) exactly and atomically. Bound the result with
/// [`FieldOp::min`] / [`FieldOp::max`]: `add("balance", dec("-25.00")).min(0)` never overdraws.
pub fn add(path: &str, amount: impl Into<Value>) -> FieldOp {
    FieldOp { kind: 3, path: path.to_owned(), value: amount.into(), min: None, max: None }
}

/// Appends `value` to the list at `path` (a missing field becomes a one-element list).
pub fn append(path: &str, value: impl Into<Value>) -> FieldOp {
    FieldOp { kind: 4, path: path.to_owned(), value: value.into(), min: None, max: None }
}

impl FieldOp {
    /// Refuses (`Error::Invalid`) an add whose result would be below `bound`.
    pub fn min(mut self, bound: impl Into<Value>) -> FieldOp {
        self.min = Some(bound.into());
        self
    }

    /// Refuses (`Error::Invalid`) an add whose result would be above `bound`.
    pub fn max(mut self, bound: impl Into<Value>) -> FieldOp {
        self.max = Some(bound.into());
        self
    }

    fn encode(&self, out: &mut Vec<u8>) -> Result<()> {
        out.push(self.kind);
        put_str(out, &self.path)?;
        match self.kind {
            1 | 4 => encode_value(&self.value, out, 0)?,
            3 => {
                encode_value(&self.value, out, 0)?;
                out.push(u8::from(self.min.is_some()) | (u8::from(self.max.is_some()) << 1));
                for bound in self.min.iter().chain(self.max.iter()) {
                    encode_value(bound, out, 0)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// A query condition. Combine with `&`, `|` and `!`: `gt("price", 10) & eq("kind", "book")`.
#[derive(Debug, Clone, PartialEq)]
pub struct Filter {
    kind: u8,
    path: String,
    values: Vec<Value>,
    children: Vec<Filter>,
}

fn cmp(kind: u8, path: &str, value: Value) -> Filter {
    Filter { kind, path: path.to_owned(), values: vec![value], children: Vec::new() }
}

/// Field equals value (numbers compare numerically; `Value::Null` matches missing fields).
pub fn eq(path: &str, value: impl Into<Value>) -> Filter {
    cmp(1, path, value.into())
}

pub fn ne(path: &str, value: impl Into<Value>) -> Filter {
    cmp(2, path, value.into())
}

pub fn lt(path: &str, value: impl Into<Value>) -> Filter {
    cmp(3, path, value.into())
}

pub fn le(path: &str, value: impl Into<Value>) -> Filter {
    cmp(4, path, value.into())
}

pub fn gt(path: &str, value: impl Into<Value>) -> Filter {
    cmp(5, path, value.into())
}

pub fn ge(path: &str, value: impl Into<Value>) -> Filter {
    cmp(6, path, value.into())
}

/// `low <= field <= high`.
pub fn between(path: &str, low: impl Into<Value>, high: impl Into<Value>) -> Filter {
    Filter { kind: 7, path: path.to_owned(), values: vec![low.into(), high.into()], children: Vec::new() }
}

/// Field equals any of `values`.
pub fn is_in<V: Into<Value>>(path: &str, values: impl IntoIterator<Item = V>) -> Filter {
    Filter {
        kind: 8,
        path: path.to_owned(),
        values: values.into_iter().map(Into::into).collect(),
        children: Vec::new(),
    }
}

/// Text field starts with `text`.
pub fn prefix(path: &str, text: &str) -> Filter {
    cmp(9, path, Value::Text(text.to_owned()))
}

/// Text field contains `value`, or list field has an element equal to `value`.
pub fn contains(path: &str, value: impl Into<Value>) -> Filter {
    cmp(10, path, value.into())
}

/// Field is present and not null.
pub fn exists(path: &str) -> Filter {
    Filter { kind: 11, path: path.to_owned(), values: Vec::new(), children: Vec::new() }
}

/// Field is absent or null.
pub fn missing(path: &str) -> Filter {
    Filter { kind: 12, path: path.to_owned(), values: Vec::new(), children: Vec::new() }
}

/// Document key starts with `text`.
pub fn key_prefix(text: &str) -> Filter {
    Filter { kind: 13, path: String::new(), values: vec![Value::Text(text.to_owned())], children: Vec::new() }
}

/// Every filter holds.
pub fn and(filters: impl IntoIterator<Item = Filter>) -> Filter {
    Filter { kind: 14, path: String::new(), values: Vec::new(), children: filters.into_iter().collect() }
}

/// At least one filter holds.
pub fn or(filters: impl IntoIterator<Item = Filter>) -> Filter {
    Filter { kind: 15, path: String::new(), values: Vec::new(), children: filters.into_iter().collect() }
}

/// Every field equals the given value: `matches(&doc! {"kind" => "book"})`.
pub fn matches(fields: &Doc) -> Filter {
    let mut parts: Vec<Filter> = fields.iter().map(|(k, v)| cmp(1, k, v.clone())).collect();
    if parts.len() == 1 {
        parts.remove(0)
    } else {
        and(parts)
    }
}

impl BitAnd for Filter {
    type Output = Filter;

    fn bitand(self, rhs: Filter) -> Filter {
        and([self, rhs])
    }
}

impl BitOr for Filter {
    type Output = Filter;

    fn bitor(self, rhs: Filter) -> Filter {
        or([self, rhs])
    }
}

impl Not for Filter {
    type Output = Filter;

    fn not(self) -> Filter {
        Filter { kind: 16, path: String::new(), values: Vec::new(), children: vec![self] }
    }
}

impl Filter {
    fn encode(&self, out: &mut Vec<u8>, depth: usize) -> Result<()> {
        if depth > MAX_DEPTH {
            return Err(invalid("filter nesting is too deep (max 32)"));
        }
        out.push(self.kind);
        match self.kind {
            1..=6 | 10 => {
                put_str(out, &self.path)?;
                encode_value(&self.values[0], out, 0)?;
            }
            7 => {
                put_str(out, &self.path)?;
                encode_value(&self.values[0], out, 0)?;
                encode_value(&self.values[1], out, 0)?;
            }
            8 => {
                put_str(out, &self.path)?;
                put_u32(out, self.values.len())?;
                for v in &self.values {
                    encode_value(v, out, 0)?;
                }
            }
            9 | 13 => {
                if self.kind == 9 {
                    put_str(out, &self.path)?;
                }
                put_str(out, self.values[0].as_str().unwrap_or_default())?;
            }
            11 | 12 => put_str(out, &self.path)?,
            14 | 15 => {
                put_u32(out, self.children.len())?;
                for c in &self.children {
                    c.encode(out, depth + 1)?;
                }
            }
            _ => self.children[0].encode(out, depth + 1)?,
        }
        Ok(())
    }
}

/// What `find` returns: `Query::new().filter(gt("price", 10)).order_by("-price").limit(10)`.
/// The empty query matches every document (up to 1,000). A [`Filter`] converts into a query.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Query {
    pub(crate) filter: Option<Filter>,
    pub(crate) order_by: Vec<(String, bool)>,
    pub(crate) limit: u32,
    pub(crate) offset: u32,
}

impl Query {
    /// Every document.
    pub fn new() -> Query {
        Query::default()
    }

    /// Only documents matching `filter`.
    pub fn filter(mut self, filter: Filter) -> Query {
        self.filter = Some(filter);
        self
    }

    /// Adds a sort key: `"price"` ascending, `"-price"` descending (default: key order).
    pub fn order_by(mut self, field: &str) -> Query {
        match field.strip_prefix('-') {
            Some(f) => self.order_by.push((f.to_owned(), true)),
            None => self.order_by.push((field.to_owned(), false)),
        }
        self
    }

    /// At most `n` documents (default 1,000).
    pub fn limit(mut self, n: u32) -> Query {
        self.limit = n;
        self
    }

    /// Skips the first `n` matches.
    pub fn offset(mut self, n: u32) -> Query {
        self.offset = n;
        self
    }
}

impl From<Filter> for Query {
    fn from(f: Filter) -> Query {
        Query::new().filter(f)
    }
}

/// One write of an atomic request. kind: 1 put, 2 put+ttl, 3 delete, 4 patch, 5 check.
/// pre: 0 none, 1 absent, 2 exists, 3 version.
#[derive(Debug, Clone)]
pub(crate) struct Write {
    pub collection: String,
    pub key: String,
    pub kind: u8,
    pub pre: u8,
    pub version: u64,
    pub fields: Doc,
    pub ttl_ms: u64,
    pub ops: Vec<FieldOp>,
}

impl Write {
    pub(crate) fn new(collection: &str, key: &str, kind: u8, pre: u8, version: u64) -> Write {
        Write {
            collection: collection.to_owned(),
            key: key.to_owned(),
            kind,
            pre,
            version,
            fields: Doc::new(),
            ttl_ms: 0,
            ops: Vec::new(),
        }
    }

    pub(crate) fn put(collection: &str, key: &str, fields: Doc, ttl_ms: u64, pre: u8, version: u64) -> Write {
        Write { fields, ttl_ms, ..Write::new(collection, key, if ttl_ms > 0 { 2 } else { 1 }, pre, version) }
    }

    pub(crate) fn patch(collection: &str, key: &str, pre: u8, version: u64, ops: Vec<FieldOp>) -> Write {
        Write { ops, ..Write::new(collection, key, 4, pre, version) }
    }
}

pub(crate) fn encode_get(collection: &str, key: &str) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    put_str(&mut out, collection)?;
    put_str(&mut out, key)?;
    Ok(out)
}

pub(crate) fn encode_writes(writes: &[Write], request_id: Option<&[u8; 16]>) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    match request_id {
        None => out.push(0),
        Some(id) => {
            out.push(1);
            out.extend_from_slice(id);
        }
    }
    put_u32(&mut out, writes.len())?;
    for w in writes {
        put_str(&mut out, &w.collection)?;
        put_str(&mut out, &w.key)?;
        out.push(w.pre);
        if w.pre == 3 {
            out.extend_from_slice(&w.version.to_le_bytes());
        }
        out.push(w.kind);
        match w.kind {
            1 => encode_fields(&w.fields, &mut out, 1)?,
            2 => {
                out.extend_from_slice(&w.ttl_ms.to_le_bytes());
                encode_fields(&w.fields, &mut out, 1)?;
            }
            4 => {
                if w.ops.is_empty() {
                    return Err(invalid("update needs at least one change (set, unset, add, append)"));
                }
                put_u32(&mut out, w.ops.len())?;
                for op in &w.ops {
                    op.encode(&mut out)?;
                }
            }
            _ => {}
        }
    }
    Ok(out)
}

/// The optional trailing block of a nearest-neighbour query.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Nearest<'a> {
    pub field: &'a str,
    pub vector: &'a [f32],
    pub k: u32,
    pub exact: bool,
}

pub(crate) fn encode_query(
    collection: &str,
    q: &Query,
    after_key: Option<&str>,
    nearest: Option<Nearest<'_>>,
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    put_str(&mut out, collection)?;
    match &q.filter {
        None => out.push(0),
        Some(f) => {
            out.push(1);
            f.encode(&mut out, 0)?;
        }
    }
    put_u32(&mut out, q.order_by.len())?;
    for (path, desc) in &q.order_by {
        put_str(&mut out, path)?;
        out.push(u8::from(*desc));
    }
    out.extend_from_slice(&q.limit.to_le_bytes());
    out.extend_from_slice(&q.offset.to_le_bytes());
    match after_key {
        None => out.push(0),
        Some(k) => {
            out.push(1);
            put_str(&mut out, k)?;
        }
    }
    if let Some(n) = nearest {
        // u8 1 | str field | u32 k | u8 exact | u32 dims | dims x f32 LE.
        out.push(1);
        put_str(&mut out, n.field)?;
        out.extend_from_slice(&n.k.to_le_bytes());
        out.push(u8::from(n.exact));
        put_u32(&mut out, n.vector.len())?;
        for x in n.vector {
            out.extend_from_slice(&x.to_le_bytes());
        }
    }
    Ok(out)
}

pub(crate) fn encode_admin(sub: u8, collection: &str, field: Option<&str>) -> Result<Vec<u8>> {
    let mut out = vec![sub];
    put_str(&mut out, collection)?;
    if let Some(f) = field {
        put_str(&mut out, f)?;
    }
    Ok(out)
}

/// New version of each write; `None` for a deleted / absent document.
pub(crate) fn decode_write_response(body: &[u8]) -> Result<Vec<Option<u64>>> {
    let mut r = Reader::new(body);
    r.u8()?; // duplicate flag
    let n = r.count(9)?;
    let mut versions = Vec::with_capacity(n);
    for _ in 0..n {
        let exists = r.u8()? != 0;
        let v = r.u64()?;
        versions.push(exists.then_some(v));
    }
    Ok(versions)
}

/// A stored document: its key, version and fields.
#[derive(Debug, Clone, PartialEq)]
pub struct Document {
    pub key: String,
    pub version: u64,
    pub fields: Doc,
}

impl Document {
    /// The field `name`, if present.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields.get(name)
    }

    /// Similarity of a `nearest` result (`None` for other queries).
    pub fn score(&self) -> Option<f64> {
        self.fields.get("$score").and_then(Value::as_f64)
    }
}

pub(crate) struct Page {
    pub total: u32,
    pub next_key: Option<String>,
    pub docs: Vec<Document>,
}

pub(crate) fn decode_page(body: &[u8]) -> Result<Page> {
    let mut r = Reader::new(body);
    let total = r.u32()?;
    let next_key = if r.u8()? != 0 { Some(r.str()?) } else { None };
    let n = r.count(16)?;
    let mut docs = Vec::with_capacity(n);
    for _ in 0..n {
        let key = r.str()?;
        let version = r.u64()?;
        docs.push(Document { key, version, fields: r.fields(1)? });
    }
    Ok(Page { total, next_key, docs })
}

/// A collection's indexes (`building`: being built from existing documents) and metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct CollectionInfo {
    pub name: String,
    pub indexes: Vec<String>,
    pub building: Vec<String>,
    pub metadata: Doc,
}

fn strings(r: &mut Reader<'_>) -> Result<Vec<String>> {
    let n = r.count(4)?;
    (0..n).map(|_| r.str()).collect()
}

pub(crate) fn decode_info(r: &mut Reader<'_>) -> Result<CollectionInfo> {
    let name = r.str()?;
    let indexes = strings(r)?;
    let building = strings(r)?;
    Ok(CollectionInfo { name, indexes, building, metadata: r.fields(1)? })
}

/// A vector index of a collection (see `Collection::create_vector_index`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorIndex {
    pub field: String,
    pub dims: u32,
    /// False while existing documents are still being indexed.
    pub ready: bool,
}

pub(crate) fn decode_vector_indexes(body: &[u8]) -> Result<Vec<VectorIndex>> {
    let mut r = Reader::new(body);
    let n = r.count(9)?;
    (0..n)
        .map(|_| {
            let field = r.str()?;
            let dims = r.u32()?;
            Ok(VectorIndex { field, dims, ready: r.u8()? == 1 })
        })
        .collect()
}

/// The server currently used and its view of the cluster.
#[derive(Debug, Clone, PartialEq)]
pub struct ClusterInfo {
    pub node_id: u64,
    pub is_leader: bool,
    pub leader_id: Option<u64>,
    pub leader_addr: Option<String>,
    pub term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub members: Vec<(u64, String)>,
}

pub(crate) fn decode_cluster_info(body: &[u8]) -> Result<ClusterInfo> {
    let mut r = Reader::new(body);
    let node_id = r.u64()?;
    let is_leader = r.u8()? != 0;
    let leader_id = r.u64()?;
    let leader_addr = r.str()?;
    let (term, commit_index, last_applied) = (r.u64()?, r.u64()?, r.u64()?);
    let n = r.count(12)?;
    let mut members = Vec::with_capacity(n);
    for _ in 0..n {
        let id = r.u64()?;
        members.push((id, r.str()?));
    }
    Ok(ClusterInfo {
        node_id,
        is_leader,
        leader_id: (leader_id != 0).then_some(leader_id),
        leader_addr: (!leader_addr.is_empty()).then_some(leader_addr),
        term,
        commit_index,
        last_applied,
        members,
    })
}
