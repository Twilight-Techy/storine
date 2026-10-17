//! Zero-allocation Query Builder, Set Filtering & Keyset/Offset Pagination (`TASK-019`, `TASK-027`, `TASK-055`, `TASK-056`).

use std::marker::PhantomData;
use std::ops::RangeInclusive;
use storage_codec::ZeroCopyRecord;
use storage_index::{
    cosine_similarity_f32, sortable_f64_to_i64, sortable_u64_to_i64, CompressedBitmap, Indexable,
    KeysetCursor, SortOrder, VectorTopKHit,
};
use storage_types::{InlineId, RecordOrdinal, Result};

use crate::engine::{RecordRef, StorageEngine};
use crate::optimizer::{QueryExplainPlan, QueryOptimizer};

/// Stack-inlined `WHERE field_id IN (tag_1, tag_2, ...)` disjunction clause (`TASK-056`).
#[derive(Clone, Copy, Debug)]
struct AnyTagClause {
    field_id: u16,
    len: usize,
    tags: [InlineId; 8],
}

impl Default for AnyTagClause {
    fn default() -> Self {
        Self {
            field_id: 0,
            len: 0,
            tags: [InlineId::EMPTY; 8],
        }
    }
}

/// Paginated query result page with total match count, `has_more` flag, and $O(\log N)$ `next_cursor` (`TASK-056`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryPage<Item> {
    /// Records or keys in the current page window.
    pub items: Vec<Item>,
    /// Total records matching the query predicates (after `after_cursor`, if provided).
    pub total_matched: usize,
    /// `true` if additional matching records exist beyond this page.
    pub has_more: bool,
    /// Opaque keyset cursor of the last item in `items` for $O(\log N)$ seek to the next page.
    pub next_cursor: Option<KeysetCursor>,
}

/// Fluent query builder combining secondary tag bitmaps (`AND`, `OR`/`IN`, `AND NOT`),
/// numeric/timestamp B+Tree ranges (`i64`, `u64`, `f64`), bidirectional sort order (`Asc`/`Desc`),
/// `offset` + `limit` windowing, $O(\log N)$ `KeysetCursor` pagination, zero-copy payload predicates,
/// and SIMD cosine similarity scoring over AI memory embeddings.
///
/// Stack-inlines predicates so constructing and executing a query performs **zero intermediate
/// heap allocations** (`RULE-MEM-02`).
pub struct QueryBuilder<'a, T: ZeroCopyRecord + Indexable> {
    engine: &'a StorageEngine<T>,
    tag_len: usize,
    tag_predicates: [(u16, InlineId); 4],
    not_tag_len: usize,
    not_tag_predicates: [(u16, InlineId); 4],
    any_tag_len: usize,
    any_tag_clauses: [AnyTagClause; 2],
    range_len: usize,
    range_predicates: [(u16, RangeInclusive<i64>); 4],
    order: SortOrder,
    offset: usize,
    limit: Option<usize>,
    cursor: Option<KeysetCursor>,
    _marker: PhantomData<T>,
}

impl<'a, T: ZeroCopyRecord + Indexable> core::fmt::Debug for QueryBuilder<'a, T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("QueryBuilder")
            .field("tag_predicates", &self.tag_slice())
            .field("not_tag_predicates", &self.not_tag_slice())
            .field("range_predicates", &self.range_slice())
            .field("order", &self.order)
            .field("offset", &self.offset)
            .field("limit", &self.limit)
            .field("cursor", &self.cursor)
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
            not_tag_len: 0,
            not_tag_predicates: [(0, InlineId::EMPTY); 4],
            any_tag_len: 0,
            any_tag_clauses: [AnyTagClause::default(); 2],
            range_len: 0,
            range_predicates: [(0, 0..=0), (0, 0..=0), (0, 0..=0), (0, 0..=0)],
            order: SortOrder::Asc,
            offset: 0,
            limit: None,
            cursor: None,
            _marker: PhantomData,
        }
    }

    #[inline]
    fn tag_slice(&self) -> &[(u16, InlineId)] {
        &self.tag_predicates[..self.tag_len]
    }

    #[inline]
    fn not_tag_slice(&self) -> &[(u16, InlineId)] {
        &self.not_tag_predicates[..self.not_tag_len]
    }

    #[inline]
    fn range_slice(&self) -> &[(u16, RangeInclusive<i64>)] {
        &self.range_predicates[..self.range_len]
    }

    #[inline]
    fn is_simple_unpaginated(&self) -> bool {
        self.not_tag_len == 0
            && self.any_tag_len == 0
            && self.offset == 0
            && self.cursor.is_none()
            && self.order == SortOrder::Asc
    }

    /// Adds a required tag equality predicate (`field_id == tag`, bitwise `AND`).
    #[inline]
    pub fn filter_tag(mut self, field_id: u16, tag: InlineId) -> Self {
        if self.tag_len < self.tag_predicates.len() {
            self.tag_predicates[self.tag_len] = (field_id, tag);
            self.tag_len += 1;
        }
        self
    }

    /// Adds a negated tag exclusion predicate (`field_id != tag`, bitwise `AND NOT`, `TASK-056`).
    #[inline]
    pub fn filter_not_tag(mut self, field_id: u16, tag: InlineId) -> Self {
        if self.not_tag_len < self.not_tag_predicates.len() {
            self.not_tag_predicates[self.not_tag_len] = (field_id, tag);
            self.not_tag_len += 1;
        }
        self
    }

    /// Adds a set-membership disjunction predicate (`field_id IN tags`, bitwise `OR`, `TASK-056`).
    #[inline]
    pub fn filter_any_tag(mut self, field_id: u16, tags: &[InlineId]) -> Self {
        if self.any_tag_len < self.any_tag_clauses.len() {
            let mut clause = AnyTagClause {
                field_id,
                len: tags.len().min(8),
                tags: [InlineId::EMPTY; 8],
            };
            clause.tags[..clause.len].copy_from_slice(&tags[..clause.len]);
            self.any_tag_clauses[self.any_tag_len] = clause;
            self.any_tag_len += 1;
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

    /// Adds an inclusive IEEE-754 floating-point range predicate (`field_id in min..=max`, `TASK-056`).
    #[inline]
    pub fn filter_range_f64(self, field_id: u16, min: f64, max: f64) -> Self {
        self.filter_range(field_id, sortable_f64_to_i64(min)..=sortable_f64_to_i64(max))
    }

    /// Adds an inclusive unsigned 64-bit integer range predicate (`field_id in min..=max`, `TASK-056`).
    #[inline]
    pub fn filter_range_u64(self, field_id: u16, min: u64, max: u64) -> Self {
        self.filter_range(field_id, sortable_u64_to_i64(min)..=sortable_u64_to_i64(max))
    }

    /// Sets the sort direction (`SortOrder::Asc` or `SortOrder::Desc`, `TASK-056`).
    #[inline]
    pub fn order_by(mut self, order: SortOrder) -> Self {
        self.order = order;
        self
    }

    /// Sorts results in ascending order of the primary range index (or ordinal if no range is specified).
    #[inline]
    pub fn order_asc(self) -> Self {
        self.order_by(SortOrder::Asc)
    }

    /// Sorts results in descending order (`Newest First` / `Highest Score First`) without post-sorting (`TASK-056`).
    #[inline]
    pub fn order_desc(self) -> Self {
        self.order_by(SortOrder::Desc)
    }

    /// Skips the first `skip` matching records at the index level before loading records (`TASK-056`).
    #[inline]
    pub fn offset(mut self, skip: usize) -> Self {
        self.offset = skip;
        self
    }

    /// Caps the maximum number of records returned.
    #[inline]
    pub fn limit(mut self, max_records: usize) -> Self {
        self.limit = Some(max_records);
        self
    }

    /// Seeks directly past `cursor` in $O(\log N)$ time for keyset pagination (`TASK-056`).
    #[inline]
    pub fn after_cursor(mut self, cursor: KeysetCursor) -> Self {
        self.cursor = Some(cursor);
        self
    }

    /// Returns the cost-based `QueryExplainPlan` (`EXPLAIN`) for this query (`TASK-048`, `F-OPT-02`).
    #[inline]
    pub fn explain(&self) -> QueryExplainPlan {
        QueryOptimizer::build_explain_plan(
            self.engine,
            self.tag_slice(),
            self.range_slice(),
            self.limit,
        )
    }

    /// Resolves matching `RecordOrdinal`s using secondary index intersections without
    /// touching any record files on disk.
    pub fn resolve_ordinals(&self) -> Vec<RecordOrdinal> {
        if !self.is_simple_unpaginated() {
            let (items, _) = self.resolve_paginated_ordinals();
            return items.into_iter().map(|(_, ord)| ord).collect();
        }

        let tags = self.tag_slice();
        let ranges = self.range_slice();

        // Cost-Based Short-Circuit (`TASK-048`, `F-OPT-01`): if any tag predicate has 0 cardinality,
        // return immediately in O(1) without scanning any B+Tree range!
        for &(field_id, tag) in tags {
            if self.engine.tag_index().cardinality(field_id, tag) == 0 {
                return Vec::new();
            }
        }

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

    /// Resolves a paginated window of `(Option<sort_key>, RecordOrdinal)` and `total_matched` (`TASK-056`).
    fn resolve_paginated_ordinals(&self) -> (Vec<(Option<i64>, RecordOrdinal)>, usize) {
        let tags = self.tag_slice();
        let not_tags = self.not_tag_slice();
        let ranges = self.range_slice();

        // 1. O(1) zero-cardinality short-circuit on required tags
        for &(field_id, tag) in tags {
            if self.engine.tag_index().cardinality(field_id, tag) == 0 {
                return (Vec::new(), 0);
            }
        }

        // Stack-build slice references for any_tag_clauses
        let mut any_groups: [(u16, &[InlineId]); 2] = [(0, &[]), (0, &[])];
        for i in 0..self.any_tag_len {
            let c = &self.any_tag_clauses[i];
            if c.len == 0 {
                return (Vec::new(), 0);
            }
            any_groups[i] = (c.field_id, &c.tags[..c.len]);
        }
        let any_slice = &any_groups[..self.any_tag_len];

        // 2. Single-range B+Tree driver path: preserves sort key (`Asc` or `Desc`) and supports O(log N) KeysetCursor!
        if ranges.len() == 1 {
            let (field_id, ref range) = ranges[0];
            let has_tag_filters =
                !tags.is_empty() || !not_tags.is_empty() || !any_slice.is_empty();
            let (pairs, total) = self.engine.range_index().range_collect_paginated(
                field_id,
                *range.start(),
                *range.end(),
                self.order,
                self.cursor,
                self.offset,
                self.limit,
                |ord| {
                    if !has_tag_filters {
                        true
                    } else {
                        self.engine
                            .tag_index()
                            .matches_ordinal(ord, tags, not_tags, any_slice)
                    }
                },
            );
            let out = pairs
                .into_iter()
                .map(|(k, ord)| (Some(k), ord))
                .collect();
            return (out, total);
        }

        // 3. Full-collection scan path when no positive index driver (`tags`, `any_tags`, `ranges`) is specified
        if tags.is_empty() && any_slice.is_empty() && ranges.is_empty() {
            let (pairs, total) = self.engine.primary_index().scan_ordinals_paginated(
                self.order == SortOrder::Desc,
                self.cursor.map(|c| c.ordinal),
                self.offset,
                self.limit,
                |ord, _id| {
                    if not_tags.is_empty() {
                        true
                    } else {
                        self.engine
                            .tag_index()
                            .matches_ordinal(ord, &[], not_tags, &[])
                    }
                },
            );
            let out = pairs.into_iter().map(|(ord, _)| (None, ord)).collect();
            return (out, total);
        }

        // 4. Multi-index CompressedBitmap intersection + POPCNT window collection
        let mut candidate_bm: Option<CompressedBitmap> = None;

        if !tags.is_empty() {
            let bm = self.engine.tag_index().intersect_all(tags);
            if bm.is_empty() {
                return (Vec::new(), 0);
            }
            candidate_bm = Some(bm);
        }

        for &(field_id, group) in any_slice {
            let u_bm = self.engine.tag_index().union_tags(field_id, group);
            if u_bm.is_empty() {
                return (Vec::new(), 0);
            }
            candidate_bm = Some(match candidate_bm {
                Some(prev) => prev.intersect(&u_bm),
                None => u_bm,
            });
            if candidate_bm.as_ref().map_or(false, |b| b.is_empty()) {
                return (Vec::new(), 0);
            }
        }

        for (field_id, range) in ranges {
            let r_bm = self.engine.range_index().range_bitmap(*field_id, range.clone());
            if r_bm.is_empty() {
                return (Vec::new(), 0);
            }
            candidate_bm = Some(match candidate_bm {
                Some(prev) => prev.intersect(&r_bm),
                None => r_bm,
            });
            if candidate_bm.as_ref().map_or(false, |b| b.is_empty()) {
                return (Vec::new(), 0);
            }
        }

        let Some(mut bm) = candidate_bm else {
            return (Vec::new(), 0);
        };

        if !not_tags.is_empty() {
            self.engine.tag_index().exclude_from_bitmap(not_tags, &mut bm);
            if bm.is_empty() {
                return (Vec::new(), 0);
            }
        }

        let (ords, total) = bm.collect_window(
            self.offset,
            self.limit,
            self.cursor.map(|c| c.ordinal),
            self.order == SortOrder::Desc,
        );
        let out = ords.into_iter().map(|ord| (None, ord)).collect();
        (out, total)
    }

    /// Resolves matching primary keys (`InlineId`s) from secondary indexes.
    pub fn execute_ids(&self) -> Vec<InlineId> {
        let ordinals = self.resolve_ordinals();
        self.engine.primary_index().resolve_ordinals_batch(&ordinals)
    }

    /// Executes the query and returns a `QueryPage<InlineId>` with `total_matched`, `has_more`,
    /// and an $O(\log N)$ `next_cursor` without touching any record files on disk (`TASK-056`).
    pub fn execute_page_ids(&self) -> QueryPage<InlineId> {
        let (pairs, total_matched) = self.resolve_paginated_ordinals();
        let mut items = Vec::with_capacity(pairs.len());
        let mut last_cursor = None;
        for (sort_key, ord) in pairs {
            if let Some(id) = self.engine.primary_index().resolve_ordinal(ord) {
                items.push(id);
                last_cursor = Some(KeysetCursor {
                    sort_key,
                    ordinal: ord,
                });
            }
        }
        let has_more = self.offset.saturating_add(items.len()) < total_matched;
        QueryPage {
            items,
            total_matched,
            has_more,
            next_cursor: if has_more { last_cursor } else { None },
        }
    }

    /// Executes the query and returns a `QueryPage<(InlineId, RecordRef<T>)>` of zero-copy record guards (`TASK-056`).
    pub fn execute_page_refs(&self) -> Result<QueryPage<(InlineId, RecordRef<T>)>> {
        let (pairs, total_matched) = self.resolve_paginated_ordinals();
        let mut items = Vec::with_capacity(pairs.len());
        let mut last_cursor = None;
        for (sort_key, ord) in pairs {
            if let Some(id) = self.engine.primary_index().resolve_ordinal(ord) {
                if let Ok(r) = self.engine.get_ref(id) {
                    items.push((id, r));
                    last_cursor = Some(KeysetCursor {
                        sort_key,
                        ordinal: ord,
                    });
                }
            }
        }
        let has_more = self.offset.saturating_add(items.len()) < total_matched;
        Ok(QueryPage {
            items,
            total_matched,
            has_more,
            next_cursor: if has_more { last_cursor } else { None },
        })
    }

    /// Executes the query with an additional zero-copy payload predicate `predicate(&T::Archived) -> bool`
    /// evaluated directly on the memory-mapped record view **before** pagination (`offset`/`limit`/`next_cursor`, `TASK-056`).
    pub fn execute_page_filtered_with<P>(
        &self,
        predicate: P,
    ) -> Result<QueryPage<(InlineId, RecordRef<T>)>>
    where
        P: Fn(&T::Archived) -> bool,
    {
        // Resolve index candidates without offset/limit truncation so `predicate` filters before windowing
        let unwindowed = Self {
            engine: self.engine,
            tag_len: self.tag_len,
            tag_predicates: self.tag_predicates,
            not_tag_len: self.not_tag_len,
            not_tag_predicates: self.not_tag_predicates,
            any_tag_len: self.any_tag_len,
            any_tag_clauses: self.any_tag_clauses,
            range_len: self.range_len,
            range_predicates: self.range_predicates.clone(),
            order: self.order,
            offset: 0,
            limit: None,
            cursor: self.cursor,
            _marker: PhantomData,
        };
        let (candidates, _) = unwindowed.resolve_paginated_ordinals();
        let max_items = self.limit.unwrap_or(usize::MAX);
        let mut items = Vec::with_capacity(max_items.min(candidates.len().min(256)));
        let mut total_matched = 0usize;
        let mut skipped = 0usize;
        let mut last_cursor = None;

        for (sort_key, ord) in candidates {
            let Some(id) = self.engine.primary_index().resolve_ordinal(ord) else {
                continue;
            };
            let Ok(rec_ref) = self.engine.get_ref(id) else {
                continue;
            };
            if !predicate(rec_ref.archived()) {
                continue;
            }
            total_matched += 1;
            if skipped < self.offset {
                skipped += 1;
                continue;
            }
            if items.len() < max_items {
                items.push((id, rec_ref));
                last_cursor = Some(KeysetCursor {
                    sort_key,
                    ordinal: ord,
                });
            }
        }

        let has_more = self.offset.saturating_add(items.len()) < total_matched;
        Ok(QueryPage {
            items,
            total_matched,
            has_more,
            next_cursor: if has_more { last_cursor } else { None },
        })
    }

    /// Visits every matching record in the current query window using a zero-allocation `with_ref` closure (`TASK-056`).
    pub fn for_each_ref<F>(&self, mut visitor: F) -> Result<usize>
    where
        F: FnMut(InlineId, &T::Archived),
    {
        let ids = self.execute_ids();
        let mut visited = 0usize;
        for id in ids {
            if self
                .engine
                .with_ref(id, |view| visitor(id, view.archived()))
                .is_ok()
            {
                visited += 1;
            }
        }
        Ok(visited)
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
        let candidate_ordinals = if self.tag_len == 0
            && self.not_tag_len == 0
            && self.any_tag_len == 0
            && self.range_len == 0
        {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EngineConfig;
    use crate::model::{AgentObjectRecord, FIELD_PRIMARY_TAG, FIELD_SCORE};

    #[test]
    fn test_query_filtering_and_pagination_end_to_end() {
        let dir = std::env::temp_dir().join("stge_test_query_filter_pagination");
        let _ = std::fs::remove_dir_all(&dir);

        let mut cfg = EngineConfig::default();
        cfg.background_flush = false;
        let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg).unwrap();

        let tag_alpha = InlineId::from_static("alpha");
        let tag_beta = InlineId::from_static("beta");
        let tag_gamma = InlineId::from_static("gamma");
        let tag_banned = InlineId::from_static("banned");

        // Insert 30 records with scores 10..=300
        for i in 1..=30i64 {
            let id = InlineId::new(&format!("rec_{i:02}")).unwrap();
            let mut rec = AgentObjectRecord::new(id, format!("Item {i}"), 1_700_000_000 + i, i * 10);
            rec.tags.push(if i % 3 == 0 {
                tag_gamma
            } else if i % 2 == 0 {
                tag_beta
            } else {
                tag_alpha
            });
            if i % 5 == 0 {
                rec.tags.push(tag_banned);
            }
            engine.put(id, &rec).unwrap();
        }

        // 1. Test `filter_any_tag` (`OR` / `IN`) + `filter_not_tag` (`AND NOT`) + `filter_range` + `order_desc`
        // Records in 1..=30 where primary_tag IN (alpha, beta) [excludes multiples of 3 -> 20 records]
        // AND tag != banned [excludes multiples of 5 that aren't multiples of 3: 5, 10, 20, 25 -> 16 records]
        // AND score in 10..=300
        let page1 = engine
            .query()
            .filter_any_tag(FIELD_PRIMARY_TAG, &[tag_alpha, tag_beta])
            .filter_not_tag(FIELD_PRIMARY_TAG, tag_banned)
            .filter_range(FIELD_SCORE, 10..=300)
            .order_desc()
            .limit(5)
            .execute_page_ids();

        assert_eq!(page1.total_matched, 16);
        assert!(page1.has_more);
        assert_eq!(page1.items.len(), 5);
        // Highest matching scores in descending order: 290 (29), 280 (28), 260 (26), 230 (23), 220 (22)
        assert_eq!(page1.items[0], InlineId::from_static("rec_29"));
        assert_eq!(page1.items[1], InlineId::from_static("rec_28"));
        assert_eq!(page1.items[2], InlineId::from_static("rec_26"));
        assert_eq!(page1.items[3], InlineId::from_static("rec_23"));
        assert_eq!(page1.items[4], InlineId::from_static("rec_22"));

        // 2. Test O(log N) KeysetCursor seek for Page 2!
        let cursor1 = page1.next_cursor.expect("expected next_cursor on page 1");
        let page2 = engine
            .query()
            .filter_any_tag(FIELD_PRIMARY_TAG, &[tag_alpha, tag_beta])
            .filter_not_tag(FIELD_PRIMARY_TAG, tag_banned)
            .filter_range(FIELD_SCORE, 10..=300)
            .order_desc()
            .after_cursor(cursor1)
            .limit(5)
            .execute_page_ids();

        assert_eq!(page2.total_matched, 11);
        assert!(page2.has_more);
        // Next 5 descending: 190 (19), 170 (17), 160 (16), 140 (14), 130 (13)
        assert_eq!(page2.items[0], InlineId::from_static("rec_19"));
        assert_eq!(page2.items[4], InlineId::from_static("rec_13"));

        // 3. Verify `offset(5).limit(5)` produces the exact same items as `after_cursor(cursor1).limit(5)`!
        let offset_page2 = engine
            .query()
            .filter_any_tag(FIELD_PRIMARY_TAG, &[tag_alpha, tag_beta])
            .filter_not_tag(FIELD_PRIMARY_TAG, tag_banned)
            .filter_range(FIELD_SCORE, 10..=300)
            .order_desc()
            .offset(5)
            .limit(5)
            .execute_page_ids();
        assert_eq!(offset_page2.items, page2.items);

        // 4. Test zero-copy payload filter `execute_page_filtered_with` (`|arch| ...`)
        let filtered_page = engine
            .query()
            .filter_range(FIELD_SCORE, 10..=300)
            .order_asc()
            .offset(2)
            .limit(3)
            .execute_page_filtered_with(|arch| arch.score % 40 == 0)
            .unwrap();
        // Multiples of 40 in 10..=300 are: 40 (4), 80 (8), 120 (12), 160 (16), 200 (20), 240 (24), 280 (28) -> 7 total
        // With offset=2, limit=3 -> 120 (rec_12), 160 (rec_16), 200 (rec_20)
        assert_eq!(filtered_page.total_matched, 7);
        assert!(filtered_page.has_more);
        assert_eq!(filtered_page.items.len(), 3);
        assert_eq!(filtered_page.items[0].0, InlineId::from_static("rec_12"));
        assert_eq!(filtered_page.items[1].0, InlineId::from_static("rec_16"));
        assert_eq!(filtered_page.items[2].0, InlineId::from_static("rec_20"));

        let _ = std::fs::remove_dir_all(&dir);
    }
}


