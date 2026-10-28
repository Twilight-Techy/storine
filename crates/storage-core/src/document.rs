//! Schema-flexible documents (`TASK-097`, `F-DOC-01` – `F-DOC-04`): the record type behind the
//! document wire protocol, every SDK and the SQL adapter.
//!
//! A document lives in a named collection, has a string key and a map of typed fields
//! ([`PropertyValue`]: exact decimals, 128-bit integers, timestamps, text, bytes, lists, maps).
//! All collections share one [`StorageEngine<DocumentRecord>`] (one WAL), so a transaction can
//! span collections atomically.
//!
//! - **Ids**: a document's primary key is `InlineId::from_any_str("<collection>/<key>")`. The full
//!   collection and key are stored in the record and checked on every access, so the (64-bit
//!   hashed) compaction of long keys can never return or overwrite the wrong document: a hash
//!   collision is reported as an error instead.
//! - **Secondary indexes**: up to [`MAX_DOCUMENT_INDEXES`] top-level fields per collection.
//!   Numbers (all numeric types, timestamps, booleans) share one order-preserving key space;
//!   text, ids and UUIDs are hashed equality tags. Index hits are always re-checked against the
//!   exact values, so lossy keys never produce wrong results.
//! - **Atomic writes**: [`DocumentStore::write`] commits any mix of puts, deletes, field patches
//!   and checks with preconditions as one transaction, optionally idempotent (a request id makes
//!   a retried write apply at most once, crash-safely).

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use storage_codec::field_ops::{apply_field_ops, as_exact_decimal, FieldOp};
use storage_codec::{
    impl_archive_pod, xxh3_64, ArchiveBuilder, ArchivedMap, ArchivedSlice, ArchivedString,
    MapRef, PropertyValue, PropertyValueRef, RecordView, ZeroCopyRecord,
};
use storage_index::{CompressedBitmap, IndexSnapshot, Indexable};
use storage_types::{Decimal, FastHashMap, InlineId, Result, RoundingMode, StorageError, Version};

use crate::engine::{EngineConfig, Precondition, StorageEngine, WriteKind, WriteOp, WriteResult};

/// Collection id of [`DocumentRecord`] in WAL frames and catalogs.
pub const DOCUMENT_COLLECTION_ID: u16 = 2;
/// Maximum number of secondary indexes per collection (bounded by the per-record index
/// snapshot, `RULE-MEM-02`).
pub const MAX_DOCUMENT_INDEXES: usize = 4;
/// Index field id reserved for the collection-membership tag.
const COLLECTION_FIELD_ID: u16 = u16::MAX;
/// Reserved collection holding collection metadata (index declarations, SQL schemas).
pub const META_COLLECTION: &str = "$meta";
/// Reserved collection holding idempotency markers.
pub const IDEMPOTENCY_COLLECTION: &str = "$idem";
/// How long an idempotency marker is kept (a retry after this window is applied again).
pub const IDEMPOTENCY_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);
/// Longest accepted collection name / document key (bytes).
pub const MAX_NAME_LEN: usize = 255;
pub const MAX_KEY_LEN: usize = 1024;

/// A document as stored: collection, key, typed fields and the ids of indexed fields.
#[derive(Clone, Debug, PartialEq)]
pub struct DocumentRecord {
    pub collection: String,
    pub key: String,
    pub fields: BTreeMap<String, PropertyValue>,
    /// Index field ids ([`index_field_id`]) of the collection's indexes when written.
    pub index_ids: Vec<u16>,
}

/// Zero-copy archived layout (56 bytes).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ArchivedDocumentRecord {
    pub id: InlineId,
    pub collection: ArchivedString,
    pub key: ArchivedString,
    pub fields: ArchivedMap,
    pub index_ids: ArchivedSlice<u16>,
}

impl_archive_pod!(ArchivedDocumentRecord {
    id: InlineId,
    collection: ArchivedString,
    key: ArchivedString,
    fields: ArchivedMap,
    index_ids: ArchivedSlice<u16>,
});
const _: () = assert!(core::mem::size_of::<ArchivedDocumentRecord>() == 56);

/// Rejects empty, overlong or reserved-character collection names. Names starting with `$` are
/// reserved for the system (`allow_system`).
pub fn validate_collection_name(name: &str, allow_system: bool) -> Result<()> {
    let ok_char = |c: char| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.');
    let body = if allow_system { name.strip_prefix('$').unwrap_or(name) } else { name };
    if name.is_empty() || name.len() > MAX_NAME_LEN || body.is_empty() || !body.chars().all(ok_char) {
        return Err(StorageError::invalid_key(
            "collection names are 1-255 characters of A-Z, a-z, 0-9, '_', '-', '.'",
        ));
    }
    Ok(())
}

/// Rejects empty, overlong or control-character keys.
pub fn validate_document_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > MAX_KEY_LEN || key.chars().any(char::is_control) {
        return Err(StorageError::invalid_key(
            "document keys are 1-1024 bytes of UTF-8 without control characters",
        ));
    }
    Ok(())
}

/// Primary key of document `key` in `collection`.
pub fn document_id(collection: &str, key: &str) -> Result<InlineId> {
    let mut buf = String::with_capacity(collection.len() + 1 + key.len());
    buf.push_str(collection);
    buf.push('/');
    buf.push_str(key);
    InlineId::from_any_str(&buf)
}

/// Index field id of `field` in `collection` (`1..=65533`; collisions only widen index hits,
/// which are re-checked exactly).
pub fn index_field_id(collection: &str, field: &str) -> u16 {
    let mut buf = Vec::with_capacity(collection.len() + 1 + field.len());
    buf.extend_from_slice(collection.as_bytes());
    buf.push(0);
    buf.extend_from_slice(field.as_bytes());
    (xxh3_64(&buf) % 65533) as u16 + 1
}

/// Collection-membership tag.
fn collection_tag(collection: &str) -> InlineId {
    hashed_tag(collection.as_bytes())
}

/// Equality tag for text / bytes-like values: `#` + 16 hex digits of a 64-bit hash.
fn hashed_tag(bytes: &[u8]) -> InlineId {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let h = xxh3_64(bytes);
    let mut s = [0u8; 17];
    s[0] = b'#';
    for i in 0..16 {
        s[1 + i] = HEX[((h >> ((15 - i) * 4)) & 0xF) as usize];
    }
    // The 17 ASCII bytes are always a valid inline id.
    core::str::from_utf8(&s)
        .ok()
        .and_then(|t| InlineId::new(t).ok())
        .unwrap_or(InlineId::EMPTY)
}

/// How a value is indexed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexKey {
    /// Order-preserving numeric key (range index).
    Number(i64),
    /// Equality tag (tag index).
    Tag(InlineId),
}

/// Order-preserving key shared by every numeric type, so `int`, `decimal` and `float` values of
/// one field compare correctly in a single range index.
pub fn numeric_sort_key(v: &PropertyValueRef<'_>) -> Option<i64> {
    match *v {
        PropertyValueRef::Int64(x) => Some(Decimal::from_i64(x).sortable_key()),
        PropertyValueRef::UInt64(x) => Some(Decimal::from_i128(i128::from(x)).sortable_key()),
        PropertyValueRef::Int128(x) => Some(Decimal::from_i128(x).sortable_key()),
        PropertyValueRef::Decimal(d) => Some(d.sortable_key()),
        PropertyValueRef::TimestampNs(x) => Some(Decimal::from_i64(x).sortable_key()),
        PropertyValueRef::Bool(b) => Some(Decimal::from_i64(i64::from(b)).sortable_key()),
        PropertyValueRef::Float64(f) => float_sort_key(f),
        _ => None,
    }
}

fn float_sort_key(f: f64) -> Option<i64> {
    if f.is_nan() {
        return None;
    }
    if f.is_infinite() {
        return Some(if f > 0.0 { i64::MAX } else { i64::MIN });
    }
    match Decimal::from_f64_lossy(f, 6, RoundingMode::Floor) {
        Ok(d) => Some(d.sortable_key()),
        // Beyond 38 digits: the extreme keys keep the order.
        Err(_) => Some(if f > 0.0 { i64::MAX } else { i64::MIN }),
    }
}

/// Index key of a value (`None`: not indexable, e.g. null, list, map).
pub fn index_key(v: &PropertyValueRef<'_>) -> Option<IndexKey> {
    if let Some(k) = numeric_sort_key(v) {
        return Some(IndexKey::Number(k));
    }
    match *v {
        PropertyValueRef::Text(s) => Some(IndexKey::Tag(hashed_tag(s.as_bytes()))),
        PropertyValueRef::Bytes(b) => Some(IndexKey::Tag(hashed_tag(b))),
        PropertyValueRef::Uuid(u) => Some(IndexKey::Tag(hashed_tag(&u))),
        PropertyValueRef::Id(id) => Some(IndexKey::Tag(hashed_tag(&id.to_raw_bytes()))),
        _ => None,
    }
}

impl DocumentRecord {
    pub fn new(collection: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            collection: collection.into(),
            key: key.into(),
            fields: BTreeMap::new(),
            index_ids: Vec::new(),
        }
    }

    /// Builder-style field setter.
    #[must_use]
    pub fn with(mut self, field: impl Into<String>, value: PropertyValue) -> Self {
        self.fields.insert(field.into(), value);
        self
    }
}

impl ZeroCopyRecord for DocumentRecord {
    type Archived = ArchivedDocumentRecord;
    const SCHEMA_VERSION: u16 = 1;
    const COLLECTION_ID: u16 = DOCUMENT_COLLECTION_ID;

    fn archive(&self, builder: &mut ArchiveBuilder<'_>) -> Self::Archived {
        let collection = builder.write_str(&self.collection);
        let key = builder.write_str(&self.key);
        let fields = ArchivedMap::archive(builder, &self.fields);
        let index_ids = builder.write_slice(&self.index_ids);
        ArchivedDocumentRecord {
            id: document_id(&self.collection, &self.key).unwrap_or(InlineId::EMPTY),
            collection,
            key,
            fields,
            index_ids,
        }
    }

    fn validate_archived(root: &Self::Archived, payload: &[u8]) -> Result<()> {
        InlineId::from_raw_bytes(root.id.to_raw_bytes())?;
        root.collection.validate(payload)?;
        root.key.validate(payload)?;
        root.fields.validate(payload)?;
        root.index_ids.validate(payload)?;
        if root.index_ids.len() > MAX_DOCUMENT_INDEXES {
            return Err(StorageError::invalid_archive("too many document index ids"));
        }
        Ok(())
    }

    fn unarchive(root: &Self::Archived, payload: &[u8]) -> Result<Self> {
        Ok(Self {
            collection: root.collection.as_str(payload).to_owned(),
            key: root.key.as_str(payload).to_owned(),
            fields: root.fields.unarchive(payload),
            index_ids: root.index_ids.as_slice(payload).to_vec(),
        })
    }
}

impl Indexable for DocumentRecord {
    fn extract_index_snapshot(&self) -> IndexSnapshot {
        let mut snap = IndexSnapshot::new();
        snap.push_tag(COLLECTION_FIELD_ID, collection_tag(&self.collection));
        if !self.index_ids.is_empty() {
            for (name, value) in &self.fields {
                let fid = index_field_id(&self.collection, name);
                if self.index_ids.contains(&fid) {
                    push_index(&mut snap, fid, &value_ref_of(value));
                }
            }
        }
        snap
    }

    fn extract_archived_snapshot(root: &Self::Archived, payload: &[u8]) -> Option<IndexSnapshot> {
        let mut snap = IndexSnapshot::new();
        let collection = root.collection.as_str(payload);
        snap.push_tag(COLLECTION_FIELD_ID, collection_tag(collection));
        let ids = root.index_ids.as_slice(payload);
        if !ids.is_empty() {
            for (name, value) in root.fields.view(payload).iter() {
                let fid = index_field_id(collection, name);
                if ids.contains(&fid) {
                    push_index(&mut snap, fid, &value);
                }
            }
        }
        Some(snap)
    }

    fn apply_field_ops(&mut self, ops: &[FieldOp]) -> Result<()> {
        apply_field_ops(&mut self.fields, ops)
    }
}

fn push_index(snap: &mut IndexSnapshot, fid: u16, value: &PropertyValueRef<'_>) {
    match index_key(value) {
        Some(IndexKey::Number(k)) => snap.push_numeric(fid, k),
        Some(IndexKey::Tag(t)) => snap.push_tag(fid, t),
        None => {}
    }
}

/// Borrowed view of an owned value (scalars only; lists and maps are never indexed).
fn value_ref_of(v: &PropertyValue) -> PropertyValueRef<'_> {
    match v {
        PropertyValue::Null | PropertyValue::List(_) | PropertyValue::Map(_) => PropertyValueRef::Null,
        PropertyValue::Bool(b) => PropertyValueRef::Bool(*b),
        PropertyValue::Int64(x) => PropertyValueRef::Int64(*x),
        PropertyValue::UInt64(x) => PropertyValueRef::UInt64(*x),
        PropertyValue::Float64(x) => PropertyValueRef::Float64(*x),
        PropertyValue::Int128(x) => PropertyValueRef::Int128(*x),
        PropertyValue::TimestampNs(x) => PropertyValueRef::TimestampNs(*x),
        PropertyValue::Uuid(u) => PropertyValueRef::Uuid(*u),
        PropertyValue::Id(id) => PropertyValueRef::Id(*id),
        PropertyValue::Text(s) => PropertyValueRef::Text(s),
        PropertyValue::Bytes(b) => PropertyValueRef::Bytes(b),
        PropertyValue::Decimal(d) => PropertyValueRef::Decimal(*d),
    }
}

// ── Values: comparison and filters ─────────────────────────────────────────────────────────

/// Rank of a value's type in the total sort order: null < bool < numbers < text < bytes < uuid
/// < id < list < map.
fn type_rank(v: &PropertyValueRef<'_>) -> u8 {
    match v {
        PropertyValueRef::Null => 0,
        PropertyValueRef::Bool(_) => 1,
        PropertyValueRef::Int64(_)
        | PropertyValueRef::UInt64(_)
        | PropertyValueRef::Int128(_)
        | PropertyValueRef::Decimal(_)
        | PropertyValueRef::Float64(_) => 2,
        PropertyValueRef::TimestampNs(_) => 3,
        PropertyValueRef::Text(_) => 4,
        PropertyValueRef::Bytes(_) => 5,
        PropertyValueRef::Uuid(_) => 6,
        PropertyValueRef::Id(_) => 7,
        PropertyValueRef::List(_) => 8,
        PropertyValueRef::Map(_) => 9,
    }
}

fn exact_decimal_ref(v: &PropertyValueRef<'_>) -> Option<Decimal> {
    match *v {
        PropertyValueRef::Int64(x) => Some(Decimal::from_i64(x)),
        PropertyValueRef::UInt64(x) => Some(Decimal::from_i128(i128::from(x))),
        PropertyValueRef::Int128(x) => Some(Decimal::from_i128(x)),
        PropertyValueRef::Decimal(d) => Some(d),
        _ => None,
    }
}

fn f64_ref(v: &PropertyValueRef<'_>) -> Option<f64> {
    match *v {
        PropertyValueRef::Float64(f) => Some(f),
        _ => exact_decimal_ref(v).map(Decimal::to_f64_lossy),
    }
}

/// Total order over values (numbers compare numerically across types; NaN sorts last among
/// numbers). Used for sorting and range filters.
pub fn compare_values(a: &PropertyValueRef<'_>, b: &PropertyValueRef<'_>) -> core::cmp::Ordering {
    use core::cmp::Ordering;
    let (ra, rb) = (type_rank(a), type_rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (PropertyValueRef::Bool(x), PropertyValueRef::Bool(y)) => x.cmp(y),
        (PropertyValueRef::TimestampNs(x), PropertyValueRef::TimestampNs(y)) => x.cmp(y),
        (PropertyValueRef::Text(x), PropertyValueRef::Text(y)) => x.cmp(y),
        (PropertyValueRef::Bytes(x), PropertyValueRef::Bytes(y)) => x.cmp(y),
        (PropertyValueRef::Uuid(x), PropertyValueRef::Uuid(y)) => x.cmp(y),
        (PropertyValueRef::Id(x), PropertyValueRef::Id(y)) => x.cmp(y),
        (PropertyValueRef::List(x), PropertyValueRef::List(y)) => {
            for (p, q) in x.iter().zip(y.iter()) {
                let c = compare_values(&p, &q);
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        (PropertyValueRef::Map(x), PropertyValueRef::Map(y)) => {
            for ((ka, va), (kb, vb)) in x.iter().zip(y.iter()) {
                let c = ka.cmp(kb).then_with(|| compare_values(&va, &vb));
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        _ if ra == 2 => {
            if let (Some(x), Some(y)) = (exact_decimal_ref(a), exact_decimal_ref(b)) {
                return x.cmp(&y);
            }
            match (f64_ref(a), f64_ref(b)) {
                (Some(x), Some(y)) => x.total_cmp(&y),
                _ => Ordering::Equal,
            }
        }
        _ => Ordering::Equal,
    }
}

/// `a == b` with numeric equality across numeric types (`1 == 1.0 == 1.00`).
pub fn values_equal(a: &PropertyValueRef<'_>, b: &PropertyValueRef<'_>) -> bool {
    type_rank(a) == type_rank(b) && compare_values(a, b) == core::cmp::Ordering::Equal
}

/// Document filter (`F-DOC-03`). Paths are dotted field paths; comparisons use
/// [`compare_values`]. A missing field behaves like `null`.
#[derive(Clone, Debug, PartialEq)]
pub enum Filter {
    Eq(String, PropertyValue),
    Ne(String, PropertyValue),
    Lt(String, PropertyValue),
    Le(String, PropertyValue),
    Gt(String, PropertyValue),
    Ge(String, PropertyValue),
    /// `[low, high]` inclusive.
    Between(String, PropertyValue, PropertyValue),
    In(String, Vec<PropertyValue>),
    /// Text starts with the prefix.
    Prefix(String, String),
    /// Text contains the substring (case-sensitive), or a list contains the value.
    Contains(String, PropertyValue),
    /// The field exists and is not null.
    Exists(String),
    /// The field is missing or null.
    Missing(String),
    /// The document key starts with the prefix.
    KeyPrefix(String),
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
}

/// Resolves a dotted path inside a zero-copy map.
pub fn lookup_path<'a>(map: MapRef<'a>, path: &str) -> PropertyValueRef<'a> {
    let mut parts = path.split('.');
    let Some(first) = parts.next() else {
        return PropertyValueRef::Null;
    };
    let Some(mut cur) = map.get(first) else {
        return PropertyValueRef::Null;
    };
    for part in parts {
        match cur {
            PropertyValueRef::Map(m) => match m.get(part) {
                Some(v) => cur = v,
                None => return PropertyValueRef::Null,
            },
            _ => return PropertyValueRef::Null,
        }
    }
    cur
}

impl Filter {
    /// Evaluates the filter exactly against a document's fields (zero-copy).
    pub fn matches(&self, key: &str, fields: MapRef<'_>) -> bool {
        self.matches_depth(key, fields, 0)
    }

    fn matches_depth(&self, key: &str, fields: MapRef<'_>, depth: usize) -> bool {
        use core::cmp::Ordering;
        if depth > 64 {
            return false;
        }
        // Ordered comparisons only hold between comparable (same-rank, non-null) values.
        fn ordered(fields: MapRef<'_>, path: &str, v: &PropertyValue, ok: fn(Ordering) -> bool) -> bool {
            let field = lookup_path(fields, path);
            let probe = value_ref_of(v);
            !matches!(field, PropertyValueRef::Null)
                && type_rank(&field) == type_rank(&probe)
                && ok(compare_values(&field, &probe))
        }
        match self {
            Self::Eq(path, v) => eq_value(&lookup_path(fields, path), v),
            Self::Ne(..) => {
                let Self::Ne(path, v) = self else { return false };
                !Self::Eq(path.clone(), v.clone()).matches_depth(key, fields, depth + 1)
            }
            Self::Lt(p, v) => ordered(fields, p, v, |o| o == Ordering::Less),
            Self::Le(p, v) => ordered(fields, p, v, |o| o != Ordering::Greater),
            Self::Gt(p, v) => ordered(fields, p, v, |o| o == Ordering::Greater),
            Self::Ge(p, v) => ordered(fields, p, v, |o| o != Ordering::Less),
            Self::Between(p, lo, hi) => {
                ordered(fields, p, lo, |o| o != Ordering::Less) && ordered(fields, p, hi, |o| o != Ordering::Greater)
            }
            Self::In(path, values) => {
                let field = lookup_path(fields, path);
                values.iter().any(|v| eq_value(&field, v))
            }
            Self::Prefix(path, prefix) => {
                matches!(lookup_path(fields, path), PropertyValueRef::Text(s) if s.starts_with(prefix.as_str()))
            }
            Self::Contains(path, v) => match (lookup_path(fields, path), v) {
                (PropertyValueRef::Text(s), PropertyValue::Text(sub)) => s.contains(sub.as_str()),
                (PropertyValueRef::List(items), probe) => {
                    let probe_ref = value_ref_of(probe);
                    items.iter().any(|it| values_equal(&it, &probe_ref))
                }
                _ => false,
            },
            Self::Exists(path) => !matches!(lookup_path(fields, path), PropertyValueRef::Null),
            Self::Missing(path) => matches!(lookup_path(fields, path), PropertyValueRef::Null),
            Self::KeyPrefix(prefix) => key.starts_with(prefix.as_str()),
            Self::And(fs) => fs.iter().all(|f| f.matches_depth(key, fields, depth + 1)),
            Self::Or(fs) => fs.iter().any(|f| f.matches_depth(key, fields, depth + 1)),
            Self::Not(f) => !f.matches_depth(key, fields, depth + 1),
        }
    }

    /// Nesting depth (bounded by decoders).
    pub fn depth(&self) -> usize {
        match self {
            Self::And(fs) | Self::Or(fs) => 1 + fs.iter().map(Self::depth).max().unwrap_or(0),
            Self::Not(f) => 1 + f.depth(),
            _ => 0,
        }
    }
}

/// Equality of a stored value with a probe (`null` matches missing fields; lists and maps
/// compare structurally).
fn eq_value(field: &PropertyValueRef<'_>, probe: &PropertyValue) -> bool {
    match probe {
        PropertyValue::Null => matches!(field, PropertyValueRef::Null),
        PropertyValue::List(_) | PropertyValue::Map(_) => {
            matches!(field, PropertyValueRef::List(_) | PropertyValueRef::Map(_))
                && field.to_owned_value() == *probe
        }
        _ => values_equal(field, &value_ref_of(probe)),
    }
}

// ── Store ──────────────────────────────────────────────────────────────────────────────────

/// One document as returned by reads.
#[derive(Clone, Debug, PartialEq)]
pub struct Document {
    pub key: String,
    pub version: Version,
    pub fields: BTreeMap<String, PropertyValue>,
}

/// What a [`DocWrite`] does.
#[derive(Clone, Debug, PartialEq)]
pub enum DocOp {
    /// Insert or replace the whole document.
    Put(BTreeMap<String, PropertyValue>),
    /// Insert or replace with a time-to-live.
    PutWithTtl(BTreeMap<String, PropertyValue>, Duration),
    /// Delete (no-op if absent).
    Delete,
    /// Atomic field operations on the existing document.
    Patch(Vec<FieldOp>),
    /// Only check the precondition.
    Check,
}

/// One write of an atomic [`DocumentStore::write`].
#[derive(Clone, Debug, PartialEq)]
pub struct DocWrite {
    pub collection: String,
    pub key: String,
    pub op: DocOp,
    pub precondition: Precondition,
}

/// Outcome of [`DocumentStore::write`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteSummary {
    /// Version of each written document afterwards (`None` = it does not exist), in request order.
    pub versions: Vec<Option<Version>>,
    /// `true` if this request id was already applied: nothing was written again and `versions`
    /// are the documents' current versions.
    pub duplicate: bool,
}

/// Sort direction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Direction {
    #[default]
    Asc,
    Desc,
}

/// A document query.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DocQuery {
    pub collection: String,
    pub filter: Option<Filter>,
    /// Sort keys (field path, direction); the document key breaks ties. Empty = by key.
    pub order_by: Vec<(String, Direction)>,
    pub limit: Option<usize>,
    pub offset: usize,
    /// Resume after this document key (only with key order, i.e. empty `order_by`).
    pub after_key: Option<String>,
}

/// One page of query results.
#[derive(Clone, Debug, PartialEq)]
pub struct DocPage {
    pub documents: Vec<Document>,
    /// Number of matching documents (before offset / limit).
    pub total: usize,
    /// Key to pass as `after_key` for the next page (key order only), if more remain.
    pub next_after_key: Option<String>,
}

/// Declared state of a collection.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CollectionInfo {
    pub name: String,
    /// Indexed top-level fields (index-backed, at most [`MAX_DOCUMENT_INDEXES`]).
    pub indexes: Vec<String>,
    /// Free-form metadata (e.g. the SQL adapter's table schema).
    pub extra: BTreeMap<String, PropertyValue>,
}

/// Where writes go: directly to the local engine, or through Raft consensus.
pub trait CommitSink: Send + Sync {
    fn commit(&self, ops: Vec<WriteOp<DocumentRecord>>) -> Result<Vec<WriteResult>>;
}

impl CommitSink for StorageEngine<DocumentRecord> {
    fn commit(&self, ops: Vec<WriteOp<DocumentRecord>>) -> Result<Vec<WriteResult>> {
        StorageEngine::commit(self, ops)
    }
}

/// Collections of schema-flexible documents over one engine (`F-DOC-01`).
pub struct DocumentStore {
    engine: Arc<StorageEngine<DocumentRecord>>,
    sink: RwLock<Arc<dyn CommitSink>>,
    meta: RwLock<FastHashMap<String, CollectionInfo>>,
}

impl core::fmt::Debug for DocumentStore {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DocumentStore").finish_non_exhaustive()
    }
}

fn not_found_or(err: StorageError) -> Result<()> {
    match err {
        StorageError::RecordNotFound { .. } => Ok(()),
        e => Err(e),
    }
}

impl DocumentStore {
    /// Opens (or creates) a document database in `dir`.
    pub fn open(dir: impl AsRef<Path>, mut config: EngineConfig) -> Result<Self> {
        if config.collection_name == EngineConfig::default().collection_name {
            config.collection_name = "documents".to_string();
        }
        let engine = Arc::new(StorageEngine::<DocumentRecord>::open(dir.as_ref(), config)?);
        Self::from_engine(engine)
    }

    /// Wraps an existing engine (writes go straight to it).
    pub fn from_engine(engine: Arc<StorageEngine<DocumentRecord>>) -> Result<Self> {
        let sink: Arc<dyn CommitSink> = Arc::clone(&engine) as Arc<dyn CommitSink>;
        let store = Self {
            engine,
            sink: RwLock::new(sink),
            meta: RwLock::new(FastHashMap::default()),
        };
        store.reload_metadata()?;
        Ok(store)
    }

    /// Routes all writes through `sink` (e.g. Raft consensus) from now on.
    pub fn set_commit_sink(&self, sink: Arc<dyn CommitSink>) {
        *self.sink.write().unwrap_or_else(|p| p.into_inner()) = sink;
    }

    #[inline]
    pub fn engine(&self) -> &Arc<StorageEngine<DocumentRecord>> {
        &self.engine
    }

    fn sink(&self) -> Arc<dyn CommitSink> {
        Arc::clone(&self.sink.read().unwrap_or_else(|p| p.into_inner()))
    }

    /// Re-reads every collection's metadata from the `$meta` collection (after replication or
    /// a restore).
    pub fn reload_metadata(&self) -> Result<()> {
        let mut map = FastHashMap::default();
        for doc in self.scan_collection(META_COLLECTION)? {
            map.insert(doc.key.clone(), info_from_fields(&doc.key, &doc.fields));
        }
        *self.meta.write().unwrap_or_else(|p| p.into_inner()) = map;
        Ok(())
    }

    /// Metadata of `collection` (default: no indexes).
    pub fn collection_info(&self, collection: &str) -> CollectionInfo {
        self.meta
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .get(collection)
            .cloned()
            .unwrap_or_else(|| CollectionInfo { name: collection.to_owned(), ..Default::default() })
    }

    /// Collections that have metadata or at least one document... (metadata only: a collection
    /// is listed once it was declared or written through `create_collection` / `set_collection_extra`).
    pub fn list_collections(&self) -> Vec<CollectionInfo> {
        let mut v: Vec<CollectionInfo> =
            self.meta.read().unwrap_or_else(|p| p.into_inner()).values().cloned().collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    fn index_ids_for(&self, collection: &str) -> Vec<u16> {
        self.collection_info(collection)
            .indexes
            .iter()
            .map(|f| index_field_id(collection, f))
            .collect()
    }

    // ── Reads ──

    /// Reads one document (`None` if absent).
    pub fn get(&self, collection: &str, key: &str) -> Result<Option<Document>> {
        validate_collection_name(collection, true)?;
        validate_document_key(key)?;
        let id = document_id(collection, key)?;
        match self.engine.with_ref(id, |view| document_from_view(&view, collection, key)) {
            Ok(Ok(doc)) => Ok(doc),
            Ok(Err(e)) => Err(e),
            Err(StorageError::RecordNotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Every document of `collection` in key order (no filter).
    pub fn scan_collection(&self, collection: &str) -> Result<Vec<Document>> {
        Ok(self
            .query(&DocQuery { collection: collection.to_owned(), ..Default::default() })?
            .documents)
    }

    /// Runs a query (`F-DOC-03`): index-backed candidate selection for indexed equality / range
    /// predicates, then exact evaluation of the whole filter on zero-copy views.
    pub fn query(&self, q: &DocQuery) -> Result<DocPage> {
        validate_collection_name(&q.collection, true)?;
        if let Some(ref f) = q.filter {
            if f.depth() > 32 {
                return Err(StorageError::invalid_value("filter nesting is too deep (max 32)"));
            }
        }
        if q.order_by.len() > 16 {
            return Err(StorageError::invalid_value("at most 16 sort keys"));
        }
        let candidates = self.candidate_ordinals(&q.collection, q.filter.as_ref());
        let primary = self.engine.primary_index();
        let coll = q.collection.as_str();
        // Pass 1 (zero-copy): match and keep only the key, the id and the sort values, so a
        // small page of a large result never materializes every matching document.
        let mut hits: Vec<(String, InlineId, Vec<PropertyValue>)> = Vec::new();
        for ord in candidates.to_vec() {
            let Some(id) = primary.resolve_ordinal(ord) else { continue };
            let hit = self.engine.with_ref(id, |view| {
                let arch = view.archived();
                let payload = view.payload();
                if arch.collection.as_str(payload) != coll {
                    return None;
                }
                let key = arch.key.as_str(payload);
                if let Some(ref after) = q.after_key {
                    if q.order_by.is_empty() && key <= after.as_str() {
                        return None;
                    }
                }
                let fields = arch.fields.view(payload);
                if let Some(ref f) = q.filter {
                    if !f.matches(key, fields) {
                        return None;
                    }
                }
                let sort_values = q
                    .order_by
                    .iter()
                    .map(|(path, _)| lookup_path(fields, path).to_owned_value())
                    .collect();
                Some((key.to_owned(), sort_values))
            });
            match hit {
                Ok(Some((key, sv))) => hits.push((key, id, sv)),
                Ok(None) | Err(StorageError::RecordNotFound { .. }) => {}
                Err(e) => return Err(e),
            }
        }
        if q.order_by.is_empty() {
            hits.sort_by(|a, b| a.0.cmp(&b.0));
        } else {
            hits.sort_by(|a, b| {
                for (i, (_, dir)) in q.order_by.iter().enumerate() {
                    let mut c = compare_values(&value_ref_of(&a.2[i]), &value_ref_of(&b.2[i]));
                    if *dir == Direction::Desc {
                        c = c.reverse();
                    }
                    if c != core::cmp::Ordering::Equal {
                        return c;
                    }
                }
                a.0.cmp(&b.0)
            });
        }
        let total = hits.len();
        let start = q.offset.min(total);
        let end = q.limit.map_or(total, |l| start.saturating_add(l).min(total));
        let more = end < total;
        // Pass 2: materialize only the requested page.
        let mut documents = Vec::with_capacity(end - start);
        for (key, id, _) in hits.drain(start..end) {
            let doc = self.engine.with_ref(id, |view| Document {
                key: key.clone(),
                version: view.header().version(),
                fields: view.archived().fields.unarchive(view.payload()),
            });
            match doc {
                Ok(d) => documents.push(d),
                // Deleted between the passes: skip it.
                Err(StorageError::RecordNotFound { .. }) => {}
                Err(e) => return Err(e),
            }
        }
        let next_after_key = (more && q.order_by.is_empty())
            .then(|| documents.last().map(|d| d.key.clone()))
            .flatten();
        Ok(DocPage { documents, total, next_after_key })
    }

    /// Candidate ordinals: the collection bitmap narrowed by index-backed conjuncts.
    fn candidate_ordinals(&self, collection: &str, filter: Option<&Filter>) -> CompressedBitmap {
        let tags = self.engine.tag_index();
        let mut bm = tags.get_bitmap(COLLECTION_FIELD_ID, collection_tag(collection));
        let Some(filter) = filter else { return bm };
        let info = self.collection_info(collection);
        if info.indexes.is_empty() {
            return bm;
        }
        let conjuncts: Vec<&Filter> = match filter {
            Filter::And(fs) => fs.iter().collect(),
            f => vec![f],
        };
        let range = self.engine.range_index();
        for c in conjuncts {
            let (path, narrowed) = match c {
                Filter::Eq(p, _) | Filter::In(p, ..) if !p.contains('.') && info.indexes.contains(p) => {
                    let values: Vec<&PropertyValue> = match c {
                        Filter::In(_, vs) => vs.iter().collect(),
                        _ => vec![v_of(c).unwrap_or(&PropertyValue::Null)],
                    };
                    let fid = index_field_id(collection, p);
                    let mut acc = CompressedBitmap::new();
                    let mut usable = true;
                    for v in values {
                        match index_key(&value_ref_of(v)) {
                            Some(IndexKey::Number(k)) => acc = acc.union(&range.eq_bitmap(fid, k)),
                            Some(IndexKey::Tag(t)) => acc = acc.union(&tags.get_bitmap(fid, t)),
                            None => usable = false,
                        }
                    }
                    (p, usable.then_some(acc))
                }
                Filter::Lt(p, v) | Filter::Le(p, v) | Filter::Gt(p, v) | Filter::Ge(p, v)
                    if !p.contains('.') && info.indexes.contains(p) =>
                {
                    let fid = index_field_id(collection, p);
                    let k = numeric_sort_key(&value_ref_of(v));
                    let bm = k.map(|k| match c {
                        // Keys are monotonic but lossy: use inclusive bounds (exact check later).
                        Filter::Lt(..) | Filter::Le(..) => range.range_bitmap(fid, i64::MIN..=k),
                        _ => range.range_bitmap(fid, k..=i64::MAX),
                    });
                    (p, bm)
                }
                Filter::Between(p, lo, hi) if !p.contains('.') && info.indexes.contains(p) => {
                    let fid = index_field_id(collection, p);
                    let bm = match (numeric_sort_key(&value_ref_of(lo)), numeric_sort_key(&value_ref_of(hi))) {
                        (Some(a), Some(b)) if a <= b => Some(range.range_bitmap(fid, a..=b)),
                        (Some(_), Some(_)) => Some(CompressedBitmap::new()),
                        _ => None,
                    };
                    (p, bm)
                }
                _ => continue,
            };
            let _ = path;
            if let Some(n) = narrowed {
                bm = bm.intersect(&n);
            }
        }
        bm
    }

    // ── Writes ──

    /// Applies `writes` atomically. With `request_id`, a retry of an already-applied request
    /// writes nothing and reports `duplicate = true` (exactly-once within [`IDEMPOTENCY_WINDOW`]).
    pub fn write(&self, writes: Vec<DocWrite>, request_id: Option<[u8; 16]>) -> Result<WriteSummary> {
        self.write_internal(writes, request_id, false)
    }

    /// [`Self::write`], optionally allowing the reserved `$` collections (internal use).
    fn write_internal(
        &self,
        writes: Vec<DocWrite>,
        request_id: Option<[u8; 16]>,
        allow_system: bool,
    ) -> Result<WriteSummary> {
        if writes.is_empty() {
            return Ok(WriteSummary { versions: Vec::new(), duplicate: false });
        }
        let mut ops = Vec::with_capacity(writes.len() + 1);
        let mut targets = Vec::with_capacity(writes.len());
        for w in &writes {
            validate_collection_name(&w.collection, allow_system)?;
            validate_document_key(&w.key)?;
            let id = document_id(&w.collection, &w.key)?;
            self.check_no_collision(id, &w.collection, &w.key)?;
            targets.push((w.collection.clone(), w.key.clone()));
            let index_ids = self.index_ids_for(&w.collection);
            let make = |fields: &BTreeMap<String, PropertyValue>| DocumentRecord {
                collection: w.collection.clone(),
                key: w.key.clone(),
                fields: fields.clone(),
                index_ids: index_ids.clone(),
            };
            let kind = match &w.op {
                DocOp::Put(f) => {
                    check_fields(f)?;
                    WriteKind::Put(make(f))
                }
                DocOp::PutWithTtl(f, ttl) => {
                    check_fields(f)?;
                    WriteKind::PutWithTtl(make(f), *ttl)
                }
                DocOp::Delete => WriteKind::Delete,
                DocOp::Patch(p) => WriteKind::Patch(p.clone()),
                DocOp::Check => WriteKind::Check,
            };
            ops.push(WriteOp { id, kind, precondition: w.precondition });
        }
        if let Some(rid) = request_id {
            let key = hex16(&rid);
            let marker = DocumentRecord::new(IDEMPOTENCY_COLLECTION, key.clone());
            ops.push(WriteOp {
                id: document_id(IDEMPOTENCY_COLLECTION, &key)?,
                kind: WriteKind::PutWithTtl(marker, IDEMPOTENCY_WINDOW),
                precondition: Precondition::Absent,
            });
        }
        let idem_id = match request_id {
            Some(rid) => Some(document_id(IDEMPOTENCY_COLLECTION, &hex16(&rid))?),
            None => None,
        };
        match self.sink().commit(ops) {
            Ok(results) => Ok(WriteSummary {
                versions: results.iter().take(writes.len()).map(|r| r.version).collect(),
                duplicate: false,
            }),
            Err(StorageError::ConditionFailed { id, .. }) if Some(id) == idem_id => {
                // Already applied: report the current versions.
                let mut versions = Vec::with_capacity(targets.len());
                for (c, k) in &targets {
                    versions.push(self.get(c, k)?.map(|d| d.version));
                }
                Ok(WriteSummary { versions, duplicate: true })
            }
            Err(e) => Err(e),
        }
    }

    /// Long keys are hashed into the 24-byte id: refuse to touch a different document that
    /// happens to share it (never overwrite silently).
    fn check_no_collision(&self, id: InlineId, collection: &str, key: &str) -> Result<()> {
        match self.engine.with_ref(id, |view| {
            let a = view.archived();
            let p = view.payload();
            a.collection.as_str(p) == collection && a.key.as_str(p) == key
        }) {
            Ok(true) => Ok(()),
            Ok(false) => Err(StorageError::invalid_key(
                "document key hash collision with another document; use a shorter key",
            )),
            Err(e) => not_found_or(e),
        }
    }

    /// Inserts or replaces one document.
    pub fn put(&self, collection: &str, key: &str, fields: BTreeMap<String, PropertyValue>) -> Result<Version> {
        self.write_one(collection, key, DocOp::Put(fields), Precondition::None)?
            .ok_or_else(|| StorageError::invalid_value("put produced no version"))
    }

    /// Inserts a document only if it does not exist.
    pub fn insert(&self, collection: &str, key: &str, fields: BTreeMap<String, PropertyValue>) -> Result<Version> {
        self.write_one(collection, key, DocOp::Put(fields), Precondition::Absent)?
            .ok_or_else(|| StorageError::invalid_value("insert produced no version"))
    }

    /// Deletes a document; `true` if it existed.
    pub fn delete(&self, collection: &str, key: &str) -> Result<bool> {
        let existed = self.get(collection, key)?.is_some();
        self.write_one(collection, key, DocOp::Delete, Precondition::None)?;
        Ok(existed)
    }

    /// Applies atomic field operations to an existing document and returns its new version.
    pub fn patch(&self, collection: &str, key: &str, ops: Vec<FieldOp>) -> Result<Version> {
        self.write_one(collection, key, DocOp::Patch(ops), Precondition::Exists)?
            .ok_or_else(|| StorageError::invalid_value("patch produced no version"))
    }

    fn write_one(&self, collection: &str, key: &str, op: DocOp, precondition: Precondition) -> Result<Option<Version>> {
        let summary = self.write(
            vec![DocWrite { collection: collection.to_owned(), key: key.to_owned(), op, precondition }],
            None,
        )?;
        Ok(summary.versions.first().copied().flatten())
    }

    // ── Collections and indexes ──

    /// Declares `collection` (idempotent) with optional metadata.
    pub fn set_collection_extra(&self, collection: &str, extra: BTreeMap<String, PropertyValue>) -> Result<()> {
        validate_collection_name(collection, false)?;
        let mut info = self.collection_info(collection);
        info.extra = extra;
        self.save_info(&info)
    }

    fn save_info(&self, info: &CollectionInfo) -> Result<()> {
        let mut fields = BTreeMap::new();
        fields.insert(
            "indexes".to_owned(),
            PropertyValue::List(info.indexes.iter().map(|s| PropertyValue::Text(s.clone())).collect()),
        );
        fields.insert("extra".to_owned(), PropertyValue::Map(info.extra.clone()));
        self.write_internal(
            vec![DocWrite {
                collection: META_COLLECTION.to_owned(),
                key: info.name.clone(),
                op: DocOp::Put(fields),
                precondition: Precondition::None,
            }],
            None,
            true,
        )?;
        self.meta
            .write()
            .unwrap_or_else(|p| p.into_inner())
            .insert(info.name.clone(), info.clone());
        Ok(())
    }

    /// Adds a secondary index on the top-level `field` and indexes existing documents.
    /// Fails beyond [`MAX_DOCUMENT_INDEXES`] indexes per collection.
    pub fn create_index(&self, collection: &str, field: &str) -> Result<()> {
        validate_collection_name(collection, false)?;
        if field.is_empty() || field.contains('.') {
            return Err(StorageError::invalid_value("indexes cover one top-level field"));
        }
        let mut info = self.collection_info(collection);
        if info.indexes.iter().any(|f| f == field) {
            return Ok(());
        }
        if info.indexes.len() >= MAX_DOCUMENT_INDEXES {
            return Err(StorageError::invalid_value("a collection has at most 4 secondary indexes"));
        }
        info.indexes.push(field.to_owned());
        info.name = collection.to_owned();
        // Index existing documents first (rewritten at the same version: OCC versions are not
        // affected), then publish the index so queries never use a partial index.
        let ids: Vec<u16> = info.indexes.iter().map(|f| index_field_id(collection, f)).collect();
        self.reindex(collection, &ids)?;
        self.save_info(&info)
    }

    /// Removes the index on `field` (no-op if absent).
    pub fn drop_index(&self, collection: &str, field: &str) -> Result<()> {
        let mut info = self.collection_info(collection);
        let before = info.indexes.len();
        info.indexes.retain(|f| f != field);
        if info.indexes.len() == before {
            return Ok(());
        }
        info.name = collection.to_owned();
        self.save_info(&info)?;
        let ids: Vec<u16> = info.indexes.iter().map(|f| index_field_id(collection, f)).collect();
        self.reindex(collection, &ids)
    }

    fn reindex(&self, collection: &str, ids: &[u16]) -> Result<()> {
        let docs = self.scan_collection(collection)?;
        for doc in docs {
            let id = document_id(collection, &doc.key)?;
            let rec = DocumentRecord {
                collection: collection.to_owned(),
                key: doc.key.clone(),
                fields: doc.fields,
                index_ids: ids.to_vec(),
            };
            // Same-version rewrite of index metadata only; skipped if the document changed
            // meanwhile (its new version already carries the current index ids).
            match self.sink().commit(vec![WriteOp {
                id,
                kind: WriteKind::Rewrite(rec),
                precondition: Precondition::Version(doc.version),
            }]) {
                Ok(_) | Err(StorageError::VersionConflict { .. }) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Deletes every document of `collection` and its metadata.
    pub fn drop_collection(&self, collection: &str) -> Result<usize> {
        validate_collection_name(collection, false)?;
        let docs = self.scan_collection(collection)?;
        let n = docs.len();
        for chunk in docs.chunks(512) {
            let writes = chunk
                .iter()
                .map(|d| DocWrite {
                    collection: collection.to_owned(),
                    key: d.key.clone(),
                    op: DocOp::Delete,
                    precondition: Precondition::None,
                })
                .collect();
            self.write(writes, None)?;
        }
        self.write_internal(
            vec![DocWrite {
                collection: META_COLLECTION.to_owned(),
                key: collection.to_owned(),
                op: DocOp::Delete,
                precondition: Precondition::None,
            }],
            None,
            true,
        )?;
        self.meta.write().unwrap_or_else(|p| p.into_inner()).remove(collection);
        Ok(n)
    }
}

fn v_of(f: &Filter) -> Option<&PropertyValue> {
    match f {
        Filter::Eq(_, v) => Some(v),
        _ => None,
    }
}

fn hex16(bytes: &[u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(32);
    for b in bytes {
        s.push(char::from(HEX[usize::from(b >> 4)]));
        s.push(char::from(HEX[usize::from(b & 0xF)]));
    }
    s
}

fn check_fields(fields: &BTreeMap<String, PropertyValue>) -> Result<()> {
    for (k, v) in fields {
        if k.is_empty() || k.contains('.') {
            return Err(StorageError::invalid_value(
                "field names must be non-empty and must not contain '.' (use nested maps)",
            ));
        }
        if v.depth() > storage_codec::MAX_VALUE_DEPTH {
            return Err(StorageError::invalid_value("value nesting is too deep (max 32)"));
        }
    }
    Ok(())
}

fn info_from_fields(name: &str, fields: &BTreeMap<String, PropertyValue>) -> CollectionInfo {
    let indexes = match fields.get("indexes") {
        Some(PropertyValue::List(items)) => items
            .iter()
            .filter_map(|v| match v {
                PropertyValue::Text(s) => Some(s.clone()),
                _ => None,
            })
            .take(MAX_DOCUMENT_INDEXES)
            .collect(),
        _ => Vec::new(),
    };
    let extra = match fields.get("extra") {
        Some(PropertyValue::Map(m)) => m.clone(),
        _ => BTreeMap::new(),
    };
    CollectionInfo { name: name.to_owned(), indexes, extra }
}

fn document_from_view(view: &RecordView<'_, DocumentRecord>, collection: &str, key: &str) -> Result<Option<Document>> {
    let a = view.archived();
    let p = view.payload();
    if a.collection.as_str(p) != collection || a.key.as_str(p) != key {
        // A different document shares the hashed id: this key does not exist.
        return Ok(None);
    }
    Ok(Some(Document {
        key: key.to_owned(),
        version: view.header().version(),
        fields: a.fields.unarchive(p),
    }))
}

/// Exact numeric view (re-exported for SQL / SDK helpers).
pub fn exact_decimal(v: &PropertyValue) -> Option<Decimal> {
    as_exact_decimal(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> PropertyValue {
        PropertyValue::Decimal(s.parse().unwrap())
    }

    fn store(name: &str) -> (DocumentStore, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("stge_doc_{name}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        (DocumentStore::open(&dir, EngineConfig::default()).unwrap(), dir)
    }

    fn fields(pairs: &[(&str, PropertyValue)]) -> BTreeMap<String, PropertyValue> {
        pairs.iter().map(|(k, v)| ((*k).to_owned(), v.clone())).collect()
    }

    #[test]
    fn test_documents_crud_patch_and_concurrent_increments_lose_nothing() {
        let (s, dir) = store("crud");
        let s = Arc::new(s);
        let v1 = s.insert("accounts", "alice", fields(&[("balance", dec("100.00"))])).unwrap();
        assert!(s.insert("accounts", "alice", BTreeMap::new()).is_err());
        let doc = s.get("accounts", "alice").unwrap().unwrap();
        assert_eq!(doc.version, v1);
        assert_eq!(doc.fields["balance"], dec("100.00"));

        // 8 threads x 100 concurrent +0.01 deposits: the lost-update race cannot happen.
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let s = Arc::clone(&s);
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        s.patch("accounts", "alice", vec![FieldOp::Add {
                            path: "balance".into(),
                            delta: dec("0.01"),
                            min: Some(PropertyValue::Int64(0)),
                            max: None,
                        }])
                        .unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(s.get("accounts", "alice").unwrap().unwrap().fields["balance"], dec("108.00"));
        // An overdraft is refused atomically and changes nothing.
        let before = s.get("accounts", "alice").unwrap().unwrap();
        let err = s.patch("accounts", "alice", vec![FieldOp::Add {
            path: "balance".into(),
            delta: dec("-108.01"),
            min: Some(PropertyValue::Int64(0)),
            max: None,
        }]);
        assert!(err.is_err());
        assert_eq!(s.get("accounts", "alice").unwrap().unwrap(), before);
        assert!(s.delete("accounts", "alice").unwrap());
        assert!(s.get("accounts", "alice").unwrap().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_atomic_transfer_idempotency_and_preconditions() {
        let (s, dir) = store("txn");
        s.put("accounts", "a", fields(&[("balance", dec("50"))])).unwrap();
        s.put("accounts", "b", fields(&[("balance", dec("0"))])).unwrap();
        let transfer = |amount: &str| {
            vec![
                DocWrite {
                    collection: "accounts".into(),
                    key: "a".into(),
                    op: DocOp::Patch(vec![FieldOp::Add {
                        path: "balance".into(),
                        delta: dec(&format!("-{amount}")),
                        min: Some(PropertyValue::Int64(0)),
                        max: None,
                    }]),
                    precondition: Precondition::Exists,
                },
                DocWrite {
                    collection: "accounts".into(),
                    key: "b".into(),
                    op: DocOp::Patch(vec![FieldOp::Add {
                        path: "balance".into(),
                        delta: dec(amount),
                        min: None,
                        max: None,
                    }]),
                    precondition: Precondition::Exists,
                },
            ]
        };
        let rid = [7u8; 16];
        let first = s.write(transfer("20"), Some(rid)).unwrap();
        assert!(!first.duplicate);
        // The client times out and retries the same request: applied at most once.
        let retry = s.write(transfer("20"), Some(rid)).unwrap();
        assert!(retry.duplicate);
        assert_eq!(retry.versions, first.versions);
        assert_eq!(s.get("accounts", "a").unwrap().unwrap().fields["balance"], dec("30"));
        assert_eq!(s.get("accounts", "b").unwrap().unwrap().fields["balance"], dec("20"));
        // Insufficient funds aborts both sides.
        assert!(s.write(transfer("31"), None).is_err());
        assert_eq!(s.get("accounts", "b").unwrap().unwrap().fields["balance"], dec("20"));
        // Version precondition (CAS).
        let cur = s.get("accounts", "a").unwrap().unwrap().version;
        let stale = Version(cur.0 - 1);
        let w = |v| vec![DocWrite {
            collection: "accounts".into(),
            key: "a".into(),
            op: DocOp::Put(fields(&[("balance", dec("1"))])),
            precondition: Precondition::Version(v),
        }];
        assert!(matches!(s.write(w(stale), None), Err(StorageError::VersionConflict { .. })));
        assert!(s.write(w(cur), None).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_queries_with_and_without_indexes_agree() {
        let (s, dir) = store("query");
        for i in 0..200i64 {
            let mut f = fields(&[
                ("n", PropertyValue::Int64(i)),
                ("price", dec(&format!("{}.{:02}", i / 3, i % 100))),
                ("kind", PropertyValue::Text(if i % 2 == 0 { "even".into() } else { "odd".into() })),
            ]);
            if i % 10 == 0 {
                f.insert("rare".into(), PropertyValue::Bool(true));
            }
            s.put("items", &format!("k{i:04}"), f).unwrap();
        }
        s.put("other", "k0000", fields(&[("n", PropertyValue::Int64(5))])).unwrap();
        let queries = [
            Filter::Eq("kind".into(), PropertyValue::Text("odd".into())),
            Filter::And(vec![
                Filter::Ge("n".into(), PropertyValue::Int64(50)),
                Filter::Lt("price".into(), dec("40")),
            ]),
            Filter::Between("price".into(), dec("10.5"), PropertyValue::Float64(20.25)),
            Filter::In("n".into(), vec![PropertyValue::Int64(3), dec("4.0"), PropertyValue::UInt64(199)]),
            Filter::Exists("rare".into()),
            Filter::Or(vec![
                Filter::Eq("n".into(), PropertyValue::Int64(1)),
                Filter::Not(Box::new(Filter::Lt("n".into(), PropertyValue::Int64(198)))),
            ]),
            Filter::KeyPrefix("k001".into()),
        ];
        let run = |f: &Filter| {
            s.query(&DocQuery {
                collection: "items".into(),
                filter: Some(f.clone()),
                order_by: vec![("price".into(), Direction::Desc)],
                ..Default::default()
            })
            .unwrap()
            .documents
            .iter()
            .map(|d| d.key.clone())
            .collect::<Vec<_>>()
        };
        let unindexed: Vec<_> = queries.iter().map(run).collect();
        assert_eq!(unindexed[0].len(), 100);
        assert_eq!(unindexed[3].len(), 3);
        s.create_index("items", "n").unwrap();
        s.create_index("items", "price").unwrap();
        s.create_index("items", "kind").unwrap();
        let indexed: Vec<_> = queries.iter().map(run).collect();
        assert_eq!(indexed, unindexed);
        assert!(s.create_index("items", "a").is_ok());
        assert!(s.create_index("items", "b").is_err(), "max 4 indexes");
        // Survives a restart (metadata and index ids are persisted).
        drop(s);
        let s = DocumentStore::open(&dir, EngineConfig::default()).unwrap();
        let again: Vec<_> = queries
            .iter()
            .map(|f| {
                s.query(&DocQuery {
                    collection: "items".into(),
                    filter: Some(f.clone()),
                    order_by: vec![("price".into(), Direction::Desc)],
                    ..Default::default()
                })
                .unwrap()
                .documents
                .iter()
                .map(|d| d.key.clone())
                .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(again, unindexed);
        // Key-order pagination.
        let mut keys = Vec::new();
        let mut after = None;
        loop {
            let page = s
                .query(&DocQuery { collection: "items".into(), limit: Some(64), after_key: after.clone(), ..Default::default() })
                .unwrap();
            keys.extend(page.documents.iter().map(|d| d.key.clone()));
            match page.next_after_key {
                Some(k) => after = Some(k),
                None => break,
            }
        }
        assert_eq!(keys.len(), 200);
        assert!(keys.windows(2).all(|w| w[0] < w[1]));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_long_keys_and_hostile_names() {
        let (s, dir) = store("names");
        let long = "x".repeat(900);
        s.put("c", &long, fields(&[("v", PropertyValue::Int64(1))])).unwrap();
        assert_eq!(s.get("c", &long).unwrap().unwrap().fields["v"], PropertyValue::Int64(1));
        assert!(s.get("c", &format!("{long}y")).unwrap().is_none());
        assert!(s.put("bad/name", "k", BTreeMap::new()).is_err());
        assert!(s.put("$meta", "k", BTreeMap::new()).is_err());
        assert!(s.put("c", "", BTreeMap::new()).is_err());
        assert!(s.put("c", "a\u{0}b", BTreeMap::new()).is_err());
        assert!(s.put("c", "k", fields(&[("a.b", PropertyValue::Null)])).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
