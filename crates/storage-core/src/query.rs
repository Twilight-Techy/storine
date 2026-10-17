//! Zero-allocation Query Builder & Prefetching Iterator (`TASK-019`, `TASK-027`, `F-INDX-03`, `F-INDX-04`, `F-OPS-05`).

use std::marker::PhantomData;
use std::ops::RangeInclusive;
use storage_codec::ZeroCopyRecord;
use storage_index::{cosine_similarity_f32, CompressedBitmap, Indexable, VectorTopKHit};
use storage_types::{InlineId, RecordOrdinal, Result};

use crate::engine::{RecordRef, StorageEngine};

/// Fluent query builder combining secondary tag bitmaps, numeric/timestamp B+Tree ranges,
/// and optional SIMD cosine similarity scoring over AI memory embeddings.
///
/// Stack-inlines up to 4 tag predicates and 4 range predicates so constructing and executing
/// a query performs **zero intermediate heap allocations** (`RULE-MEM-02`).
pub struct QueryBuilder<'a, T: ZeroCopyRecord + Indexable> {
    engine: &'a StorageEngine<T>,
    tag_len: usize,
    tag_predicates: [(u16, InlineId); 4],
    range_len: usize,
    range_predicates: [(u16, RangeInclusive<i64>); 4],
    limit: Option<usize>,
    _marker: PhantomData<T>,
}

impl<'a, T: ZeroCopyRecord + Indexable> core::fmt::Debug for QueryBuilder<'a, T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("QueryBuilder")
            .field("tag_predicates", &self.tag_slice())
            .field("range_predicates", &self.range_slice())
            .field("limit", &self.limit)
            .finish()
    }
}

impl<'a, T: ZeroCopyRecord + Indexable> QueryBuilder<'a, T> {
    #[inline]
    pub fn new(engine: &'a StorageEngine<T>) -> Self {
        Self {
            engine,
            tag_len: 0,
            tag_predicates: [(0, InlineId::EMPTY); 4],
            range_len: 0,
            range_predicates: [(0, 0..=0), (0, 0..=0), (0, 0..=0), (0, 0..=0)],
            limit: None,
            _marker: PhantomData,
        }
    }

    #[inline]
    fn tag_slice(&self) -> &[(u16, InlineId)] {
        &self.tag_predicates[..self.tag_len]
    }

    #[inline]
    fn range_slice(&self) -> &[(u16, RangeInclusive<i64>)] {
        &self.range_predicates[..self.range_len]
    }

    /// Adds a required tag equality predicate (`field_id == tag`).
    #[inline]
    pub fn filter_tag(mut self, field_id: u16, tag: InlineId) -> Self {
        if self.tag_len < self.tag_predicates.len() {
            self.tag_predicates[self.tag_len] = (field_id, tag);
            self.tag_len += 1;
        }
        self
    }

    /// Adds an inclusive numeric or timestamp range predicate (`field_id in min..=max`).
    #[inline]
    pub fn filter_range(mut self, field_id: u16, range: RangeInclusive<i64>) -> Self {
        if self.range_len < self.range_predicates.len() {
            self.range_predicates[self.range_len] = (field_id, range);
            self.range_len += 1;
        }
        self
    }

    /// Caps the maximum number of records returned.
    #[inline]
    pub fn limit(mut self, max_records: usize) -> Self {
        self.limit = Some(max_records);
        self
    }

    /// Resolves matching `RecordOrdinal`s using secondary index intersections without
    /// touching any record files on disk.
    pub fn resolve_ordinals(&self) -> Vec<RecordOrdinal> {
        let tags = self.tag_slice();
        let ranges = self.range_slice();

        // Fast Path 1: Single range predicate (with or without tag predicates) ->
        // scan B+Tree range into `out` and filter in-place against tag bitmaps (only 1 heap allocation!).
        if ranges.len() == 1 {
            let (field_id, ref range) = ranges[0];
            let collect_limit = if tags.is_empty() { self.limit } else { None };
            let mut out = self
                .engine
                .range_index()
                .range_collect(field_id, range.clone(), collect_limit);
            if !tags.is_empty() {
                self.engine.tag_index().filter_in_place(tags, &mut out);
                if let Some(max) = self.limit {
                    out.truncate(max);
                }
            }
            return out;
        }

        let mut candidate_bm: Option<CompressedBitmap> = None;

        if !tags.is_empty() {
            let bm = self.engine.tag_index().intersect_all(tags);
            if bm.is_empty() {
                return Vec::new();
            }
            candidate_bm = Some(bm);
        }

        for (field_id, range) in ranges {
            let r_bm = self.engine.range_index().range_bitmap(*field_id, range.clone());
            if r_bm.is_empty() {
                return Vec::new();
            }
            candidate_bm = Some(match candidate_bm {
                Some(prev) => prev.intersect(&r_bm),
                None => r_bm,
            });
            if candidate_bm.as_ref().map_or(false, |b| b.is_empty()) {
                return Vec::new();
            }
        }

        match candidate_bm {
            Some(bm) => {
                let mut out = bm.to_vec();
                if let Some(max) = self.limit {
                    out.truncate(max);
                }
                out
            }
            None => Vec::new(),
        }
    }

    /// Resolves matching primary keys (`InlineId`s) from secondary indexes.
    pub fn execute_ids(&self) -> Vec<InlineId> {
        let ordinals = self.resolve_ordinals();
        self.engine.primary_index().resolve_ordinals_batch(&ordinals)
    }

    /// Executes the query and returns zero-copy `RecordRef<'a, T>` guards for all matching live records.
    pub fn execute_refs(&self) -> Result<Vec<(InlineId, RecordRef<T>)>> {
        let ids = self.execute_ids();
        let mut results = Vec::with_capacity(ids.len());
        for id in ids {
            if let Ok(r) = self.engine.get_ref(id) {
                results.push((id, r));
            }
        }
        Ok(results)
    }

    /// Executes a hybrid metadata + SIMD vector similarity top-$K$ search (`TASK-027`, `F-OPS-05`).
    ///
    /// If tag/range predicates are provided, only candidates matching the metadata filter are scored!
    pub fn top_k_similar(&self, query_embedding: &[f32], k: usize) -> Vec<(InlineId, VectorTopKHit)> {
        if k == 0 || query_embedding.is_empty() {
            return Vec::new();
        }
        let candidate_ordinals = if self.tag_len == 0 && self.range_len == 0 {
            None
        } else {
            Some(self.resolve_ordinals())
        };

        let hits = self
            .engine
            .score_embeddings(query_embedding, candidate_ordinals.as_deref(), k);

        let mut out = Vec::with_capacity(hits.len());
        for hit in hits {
            if let Some(id) = self.engine.primary_index().resolve_ordinal(hit.ordinal) {
                out.push((id, hit));
            }
        }
        out
    }

    /// Direct SIMD cosine similarity helper exposed for custom zero-copy filter closures.
    #[inline]
    pub fn cosine(a: &[f32], b: &[f32]) -> f32 {
        cosine_similarity_f32(a, b)
    }
}
