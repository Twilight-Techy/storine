//! `Indexable` trait and stack-allocated differential index delta extraction (`TASK-016`, `F-INDX-05`, `RULE-MEM-02`).

use storage_types::InlineId;

const MAX_NUMERICS: usize = 4;
const MAX_TAGS: usize = 8;

/// Up to 4 stack-inlined numeric/timestamp fields, 8 stack-inlined tag fields, and a 64-bit
/// embedding fingerprint per record so computing index deltas during `db.update` performs
/// **zero heap allocations** (`RULE-MEM-02`).
///
/// Field ids and values are stored in separate arrays (struct-of-arrays) so no per-element
/// alignment padding is wasted: the snapshot is 264 bytes instead of 352 bytes with
/// `(u16, InlineId)` tuples. The engine keeps one snapshot per live record in RAM.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexSnapshot {
    tags: [InlineId; MAX_TAGS],
    numeric_values: [i64; MAX_NUMERICS],
    pub embedding_hash: u64,
    tag_fields: [u16; MAX_TAGS],
    numeric_fields: [u16; MAX_NUMERICS],
    numerics_len: u8,
    tags_len: u8,
    pub has_embedding: bool,
}

impl Default for IndexSnapshot {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl IndexSnapshot {
    #[inline]
    pub const fn new() -> Self {
        Self {
            tags: [InlineId::EMPTY; MAX_TAGS],
            numeric_values: [0; MAX_NUMERICS],
            embedding_hash: 0,
            tag_fields: [0; MAX_TAGS],
            numeric_fields: [0; MAX_NUMERICS],
            numerics_len: 0,
            tags_len: 0,
            has_embedding: false,
        }
    }

    /// `true` when this snapshot contributes nothing to any secondary index.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.numerics_len == 0 && self.tags_len == 0 && !self.has_embedding
    }

    #[inline]
    pub fn push_numeric(&mut self, field_id: u16, value: i64) {
        let n = self.numerics_len as usize;
        if n < MAX_NUMERICS {
            self.numeric_fields[n] = field_id;
            self.numeric_values[n] = value;
            self.numerics_len += 1;
        }
    }

    /// Indexes an `f64` floating-point field using IEEE-754 order-preserving `i64` encoding.
    #[inline]
    pub fn push_f64(&mut self, field_id: u16, value: f64) {
        self.push_numeric(field_id, crate::btree::sortable_f64_to_i64(value));
    }

    /// Indexes a `u64` unsigned integer field using order-preserving `i64` encoding.
    #[inline]
    pub fn push_u64(&mut self, field_id: u16, value: u64) {
        self.push_numeric(field_id, crate::btree::sortable_u64_to_i64(value));
    }

    /// Indexes an exact decimal (money) field with an order-preserving key shared with
    /// [`Self::push_i128`] (`TASK-094`). The key is monotonic but may be shared by close values
    /// beyond 6 fractional digits: re-check exact values when filtering.
    #[inline]
    pub fn push_decimal(&mut self, field_id: u16, value: storage_types::Decimal) {
        self.push_numeric(field_id, value.sortable_key());
    }

    /// Indexes a 128-bit integer field (same key space as [`Self::push_decimal`]).
    #[inline]
    pub fn push_i128(&mut self, field_id: u16, value: i128) {
        self.push_numeric(field_id, storage_types::Decimal::from_i128(value).sortable_key());
    }

    /// Indexes a boolean field (`false -> 0`, `true -> 1`).
    #[inline]
    pub fn push_bool(&mut self, field_id: u16, value: bool) {
        self.push_numeric(field_id, i64::from(value));
    }

    #[inline]
    pub fn push_tag(&mut self, field_id: u16, tag: InlineId) {
        let n = self.tags_len as usize;
        if n < MAX_TAGS {
            self.tag_fields[n] = field_id;
            self.tags[n] = tag;
            self.tags_len += 1;
        }
    }

    /// Computes a zero-allocation 64-bit fingerprint of `slice` so `IndexSnapshot` remains `Copy`.
    #[inline]
    pub fn set_embedding_slice(&mut self, slice: &[f32]) {
        if slice.is_empty() {
            self.has_embedding = false;
            self.embedding_hash = 0;
            return;
        }
        let mut h = 0x9E37_79B9_7F4A_7C15u64 ^ (slice.len() as u64);
        for &v in slice {
            h = (h ^ u64::from(v.to_bits())).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            h ^= h >> 31;
        }
        self.has_embedding = true;
        self.embedding_hash = h;
    }

    #[inline]
    pub fn set_embedding(&mut self, vec: Vec<f32>) {
        self.set_embedding_slice(&vec);
    }

    /// Iterates `(field_id, value)` numeric postings.
    #[inline]
    pub fn numerics(&self) -> impl Iterator<Item = (u16, i64)> + '_ {
        let n = self.numerics_len as usize;
        self.numeric_fields[..n]
            .iter()
            .copied()
            .zip(self.numeric_values[..n].iter().copied())
    }

    /// Iterates `(field_id, tag)` tag postings.
    #[inline]
    pub fn tags(&self) -> impl Iterator<Item = (u16, InlineId)> + '_ {
        let n = self.tags_len as usize;
        self.tag_fields[..n]
            .iter()
            .copied()
            .zip(self.tags[..n].iter().copied())
    }

    #[inline]
    fn contains_numeric(&self, item: (u16, i64)) -> bool {
        self.numerics().any(|x| x == item)
    }

    #[inline]
    fn contains_tag(&self, item: (u16, InlineId)) -> bool {
        self.tags().any(|x| x == item)
    }
}

/// Differential changes between an old `IndexSnapshot` and a new `IndexSnapshot`.
/// If `is_empty()` is true, secondary index locks are completely bypassed during `db.update`!
#[derive(Clone, Copy, Debug)]
pub struct IndexDelta {
    pub removed_numerics_len: usize,
    pub removed_numerics: [(u16, i64); MAX_NUMERICS],
    pub added_numerics_len: usize,
    pub added_numerics: [(u16, i64); MAX_NUMERICS],
    pub removed_tags_len: usize,
    pub removed_tags: [(u16, InlineId); MAX_TAGS],
    pub added_tags_len: usize,
    pub added_tags: [(u16, InlineId); MAX_TAGS],
    pub embedding_changed: bool,
}

impl Default for IndexDelta {
    #[inline]
    fn default() -> Self {
        Self {
            removed_numerics_len: 0,
            removed_numerics: [(0, 0); MAX_NUMERICS],
            added_numerics_len: 0,
            added_numerics: [(0, 0); MAX_NUMERICS],
            removed_tags_len: 0,
            removed_tags: [(0, InlineId::EMPTY); MAX_TAGS],
            added_tags_len: 0,
            added_tags: [(0, InlineId::EMPTY); MAX_TAGS],
            embedding_changed: false,
        }
    }
}

impl IndexDelta {
    /// Computes the exact minimal delta from `old` to `new` on the stack (`0` heap allocations).
    #[inline]
    pub fn diff(old: &IndexSnapshot, new: &IndexSnapshot) -> Self {
        let mut delta = Self::default();

        for item in old.numerics() {
            if !new.contains_numeric(item) {
                delta.removed_numerics[delta.removed_numerics_len] = item;
                delta.removed_numerics_len += 1;
            }
        }
        for item in new.numerics() {
            if !old.contains_numeric(item) {
                delta.added_numerics[delta.added_numerics_len] = item;
                delta.added_numerics_len += 1;
            }
        }
        for tag in old.tags() {
            if !new.contains_tag(tag) {
                delta.removed_tags[delta.removed_tags_len] = tag;
                delta.removed_tags_len += 1;
            }
        }
        for tag in new.tags() {
            if !old.contains_tag(tag) {
                delta.added_tags[delta.added_tags_len] = tag;
                delta.added_tags_len += 1;
            }
        }

        if old.has_embedding != new.has_embedding || old.embedding_hash != new.embedding_hash {
            delta.embedding_changed = true;
        }

        delta
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.removed_numerics_len == 0
            && self.added_numerics_len == 0
            && self.removed_tags_len == 0
            && self.added_tags_len == 0
            && !self.embedding_changed
    }
}

/// How a record is protected at rest when encryption is enabled (`F-SEC-02`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RecordProtection {
    /// Confidentiality + integrity (ChaCha20-Poly1305). Read via the decrypted-frame cache.
    #[default]
    Encrypt,
    /// Integrity only: the payload stays plaintext with a Poly1305 MAC trailer (cheaper; no
    /// cipher work). For records whose contents are not secret but must not be forged.
    Authenticate,
}

/// Implemented by domain records to declare secondary numeric/timestamp indexes, tag indexes,
/// and optional AI vector embeddings.
pub trait Indexable: storage_codec::ZeroCopyRecord {
    /// Per-record protection policy when encryption is enabled (default: encrypt everything).
    /// Evaluated zero-copy on each frame as it is written to disk or the WAL; changing the policy
    /// applies to existing records through `StorageEngine::reseal_all`.
    #[inline]
    fn protection(_root: &Self::Archived, _payload: &[u8]) -> RecordProtection {
        RecordProtection::Encrypt
    }

    #[inline]
    fn extract_index_snapshot(&self) -> IndexSnapshot {
        IndexSnapshot::new()
    }

    /// Applies atomic field operations (`FieldOp::Set` / `Unset` / `Add` / `Append`) to this
    /// record, for server-side read-modify-write (`TASK-095`). Record types without dynamic
    /// fields keep the default, which rejects every operation.
    fn apply_field_ops(&mut self, _ops: &[storage_codec::field_ops::FieldOp]) -> storage_types::Result<()> {
        Err(storage_types::StorageError::invalid_value(
            "this record type does not support field operations",
        ))
    }

    /// Extracts an `IndexSnapshot` directly from the zero-copy archived representation (`mmap` / `hot_dirty`)
    /// without allocating an owned record or storing a persistent `IndexSnapshot` map in RAM.
    #[inline]
    fn extract_archived_snapshot(root: &Self::Archived, payload: &[u8]) -> Option<IndexSnapshot> {
        Self::unarchive(root, payload)
            .ok()
            .map(|owned| owned.extract_index_snapshot())
    }

    #[inline]
    fn embedding_slice(&self) -> Option<&[f32]> {
        None
    }

    /// `true` if records carry *named* vector embeddings (`TASK-122`): the engine then keeps one
    /// HNSW graph per name, maintained on every write path (local writes, WAL replay, Raft
    /// apply, snapshot install) and rebuilt at open.
    const NAMED_EMBEDDINGS: bool = false;

    /// Named embeddings of this record: `(vector index id, vector)` pairs.
    #[inline]
    fn named_embeddings(&self) -> Vec<(u16, Vec<f32>)> {
        Vec::new()
    }

    /// [`Self::named_embeddings`] read from the zero-copy archived representation.
    #[inline]
    fn archived_named_embeddings(root: &Self::Archived, payload: &[u8]) -> Vec<(u16, Vec<f32>)> {
        Self::unarchive(root, payload)
            .map(|owned| owned.named_embeddings())
            .unwrap_or_default()
    }

    /// Borrows the `f32` vector embedding slice directly from the zero-copy archived payload (`mmap` / `hot_dirty`)
    /// without duplicating `Vec<f32>` in heap RAM.
    #[inline]
    fn archived_embedding_slice<'a>(
        _root: &'a Self::Archived,
        _payload: &'a [u8],
    ) -> Option<&'a [f32]> {
        None
    }
}

// Layout guard: keep the per-record RAM cost of secondary-index bookkeeping tight.
const _: () = assert!(core::mem::size_of::<IndexSnapshot>() <= 264);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_detects_changes_and_noop() {
        let t1 = InlineId::new("a").unwrap();
        let t2 = InlineId::new("b").unwrap();
        let mut old = IndexSnapshot::new();
        old.push_numeric(1, 10);
        old.push_tag(1, t1);
        let mut new = old;
        assert!(IndexDelta::diff(&old, &new).is_empty());
        new = IndexSnapshot::new();
        new.push_numeric(1, 11);
        new.push_tag(1, t2);
        let d = IndexDelta::diff(&old, &new);
        assert_eq!(d.removed_numerics[..d.removed_numerics_len], [(1, 10)]);
        assert_eq!(d.added_numerics[..d.added_numerics_len], [(1, 11)]);
        assert_eq!(d.removed_tags[..d.removed_tags_len], [(1, t1)]);
        assert_eq!(d.added_tags[..d.added_tags_len], [(1, t2)]);
    }
}
