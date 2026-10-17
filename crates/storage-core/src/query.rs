//! Zero-allocation Query Builder, Set Filtering & Keyset/Offset Pagination (`TASK-019`, `TASK-027`, `TASK-055`..`TASK-058`).

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::ops::RangeInclusive;
use storage_codec::{RecordView, ZeroCopyRecord};
use storage_index::{
    cosine_similarity_f32, sortable_f64_to_i64, sortable_u64_to_i64, CompressedBitmap, Indexable,
    KeysetCursor, SortOrder, VectorTopKHit,
};
use storage_types::{FastHashSet, InlineId, RecordOrdinal, Result};

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

/// Fluent query builder combining:
/// - Secondary tag bitmaps (`==`, `!=`, `IN`/`OR`, `STARTS WITH` prefix, `IS NOT NULL`, `IS NULL`),
/// - Numeric/timestamp/float B+Tree comparisons (`==`, `!=`, `>`, `>=`, `<`, `<=`, `BETWEEN`, `NOT BETWEEN`, `IN`, `IS NOT NULL`, `IS NULL`),
/// - Primary-key (`InlineId`) set & prefix filters (`IN`, `NOT IN`, `STARTS WITH`),
/// - Graph BFS neighborhood scoping (`filter_within_bfs`),
/// - Composable zero-copy memory-mapped `RecordView` predicates (`.filter(|view| ...)`),
/// - Bidirectional sort order (`Asc`/`Desc`), `offset` + `limit` windowing, and $O(\log N)$ `KeysetCursor` pagination,
/// - SIMD cosine similarity scoring over AI memory embeddings.
///
/// Stack-inlines common predicates so constructing and executing a query performs **zero intermediate
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
    extra_include_bm: Option<CompressedBitmap>,
    extra_exclude_bm: Option<CompressedBitmap>,
    view_predicate: Option<Box<dyn Fn(&RecordView<'_, T>) -> bool + 'a>>,
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
            .field("has_extra_include_bm", &self.extra_include_bm.is_some())
            .field("has_extra_exclude_bm", &self.extra_exclude_bm.is_some())
            .field("has_view_predicate", &self.view_predicate.is_some())
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
            extra_include_bm: None,
            extra_exclude_bm: None,
            view_predicate: None,
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
            && self.extra_include_bm.is_none()
            && self.extra_exclude_bm.is_none()
            && self.view_predicate.is_none()
            && self.offset == 0
            && self.cursor.is_none()
            && self.order == SortOrder::Asc
    }

    #[inline]
    fn intersect_extra_include(&mut self, bm: CompressedBitmap) {
        self.extra_include_bm = Some(match self.extra_include_bm.take() {
            Some(prev) => prev.intersect(&bm),
            None => bm,
        });
    }

    #[inline]
    fn union_extra_exclude(&mut self, bm: CompressedBitmap) {
        if bm.is_empty() {
            return;
        }
        self.extra_exclude_bm = Some(match self.extra_exclude_bm.take() {
            Some(prev) => prev.union(&bm),
            None => bm,
        });
    }

    // =========================================================================
    // 1. Tag / Categorical Filters (`==`, `!=`, `IN`, `STARTS WITH`, `IS NULL`)
    // =========================================================================

    /// Adds a required tag equality predicate (`field_id == tag`, bitwise `AND`).
    #[inline]
    pub fn filter_tag(mut self, field_id: u16, tag: InlineId) -> Self {
        if self.tag_len < self.tag_predicates.len() {
            self.tag_predicates[self.tag_len] = (field_id, tag);
            self.tag_len += 1;
        } else {
            let bm = self.engine.tag_index().get_bitmap(field_id, tag);
            self.intersect_extra_include(bm);
        }
        self
    }

    /// Adds a negated tag exclusion predicate (`field_id != tag`, bitwise `AND NOT`, `TASK-056`).
    #[inline]
    pub fn filter_not_tag(mut self, field_id: u16, tag: InlineId) -> Self {
        if self.not_tag_len < self.not_tag_predicates.len() {
            self.not_tag_predicates[self.not_tag_len] = (field_id, tag);
            self.not_tag_len += 1;
        } else {
            let bm = self.engine.tag_index().get_bitmap(field_id, tag);
            self.union_extra_exclude(bm);
        }
        self
    }

    /// Adds a set-membership disjunction predicate (`field_id IN tags`, bitwise `OR`, `TASK-056`).
    #[inline]
    pub fn filter_any_tag(mut self, field_id: u16, tags: &[InlineId]) -> Self {
        if self.any_tag_len < self.any_tag_clauses.len() && tags.len() <= 8 {
            let mut clause = AnyTagClause {
                field_id,
                len: tags.len(),
                tags: [InlineId::EMPTY; 8],
            };
            clause.tags[..clause.len].copy_from_slice(tags);
            self.any_tag_clauses[self.any_tag_len] = clause;
            self.any_tag_len += 1;
        } else {
            let bm = self.engine.tag_index().union_tags(field_id, tags);
            self.intersect_extra_include(bm);
        }
        self
    }

    /// Filters for records having any tag on `field_id` starting with `prefix` (`WHERE tag LIKE 'prefix%'`, `TASK-058`).
    #[inline]
    pub fn filter_tag_prefix(mut self, field_id: u16, prefix: &str) -> Self {
        let bm = self.engine.tag_index().union_tag_prefix(field_id, prefix);
        self.intersect_extra_include(bm);
        self
    }

    /// Filters for records that have at least one tag on `field_id` (`WHERE tag_field IS NOT NULL`, `TASK-058`).
    #[inline]
    pub fn filter_has_tag_field(mut self, field_id: u16) -> Self {
        let bm = self.engine.tag_index().all_on_field_bitmap(field_id);
        self.intersect_extra_include(bm);
        self
    }

    /// Filters for records that have no tags on `field_id` (`WHERE tag_field IS NULL`, `TASK-058`).
    #[inline]
    pub fn filter_missing_tag_field(mut self, field_id: u16) -> Self {
        let bm = self.engine.tag_index().all_on_field_bitmap(field_id);
        self.union_extra_exclude(bm);
        self
    }

    // =========================================================================
    // 2. Numeric / Timestamp / Float Comparisons (`=`, `!=`, `>`, `>=`, `<`, `<=`, `BETWEEN`, `NOT BETWEEN`, `IN`)
    // =========================================================================

    /// Adds an inclusive numeric or timestamp range predicate (`field_id in min..=max`).
    #[inline]
    pub fn filter_range(mut self, field_id: u16, range: RangeInclusive<i64>) -> Self {
        if self.range_len < self.range_predicates.len() {
            self.range_predicates[self.range_len] = (field_id, range);
            self.range_len += 1;
        } else {
            let bm = self.engine.range_index().range_bitmap(field_id, range);
            self.intersect_extra_include(bm);
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

    /// Exact numeric equality filter (`field_id == value`, `TASK-058`).
    #[inline]
    pub fn filter_eq(self, field_id: u16, value: i64) -> Self {
        self.filter_range(field_id, value..=value)
    }

    /// Exact `u64` equality filter (`field_id == value`, `TASK-058`).
    #[inline]
    pub fn filter_eq_u64(self, field_id: u16, value: u64) -> Self {
        let enc = sortable_u64_to_i64(value);
        self.filter_range(field_id, enc..=enc)
    }

    /// Exact `f64` equality filter (`field_id == value`, `TASK-058`).
    #[inline]
    pub fn filter_eq_f64(self, field_id: u16, value: f64) -> Self {
        let enc = sortable_f64_to_i64(value);
        self.filter_range(field_id, enc..=enc)
    }

    /// Numeric inequality exclusion filter (`field_id != value`, `TASK-058`).
    #[inline]
    pub fn filter_ne(mut self, field_id: u16, value: i64) -> Self {
        let bm = self.engine.range_index().eq_bitmap(field_id, value);
        self.union_extra_exclude(bm);
        self
    }

    /// Unsigned 64-bit inequality exclusion filter (`field_id != value`, `TASK-058`).
    #[inline]
    pub fn filter_ne_u64(self, field_id: u16, value: u64) -> Self {
        self.filter_ne(field_id, sortable_u64_to_i64(value))
    }

    /// Floating-point inequality exclusion filter (`field_id != value`, `TASK-058`).
    #[inline]
    pub fn filter_ne_f64(self, field_id: u16, value: f64) -> Self {
        self.filter_ne(field_id, sortable_f64_to_i64(value))
    }

    /// Strict greater-than filter (`field_id > value`, `TASK-058`).
    #[inline]
    pub fn filter_gt(mut self, field_id: u16, value: i64) -> Self {
        if value == i64::MAX {
            self.intersect_extra_include(CompressedBitmap::new());
            self
        } else {
            self.filter_range(field_id, (value + 1)..=i64::MAX)
        }
    }

    /// Greater-than-or-equal filter (`field_id >= value`, `TASK-058`).
    #[inline]
    pub fn filter_gte(self, field_id: u16, value: i64) -> Self {
        self.filter_range(field_id, value..=i64::MAX)
    }

    /// Strict less-than filter (`field_id < value`, `TASK-058`).
    #[inline]
    pub fn filter_lt(mut self, field_id: u16, value: i64) -> Self {
        if value == i64::MIN {
            self.intersect_extra_include(CompressedBitmap::new());
            self
        } else {
            self.filter_range(field_id, i64::MIN..=(value - 1))
        }
    }

    /// Less-than-or-equal filter (`field_id <= value`, `TASK-058`).
    #[inline]
    pub fn filter_lte(self, field_id: u16, value: i64) -> Self {
        self.filter_range(field_id, i64::MIN..=value)
    }

    /// Strict greater-than filter for `f64` (`field_id > value`, `TASK-058`).
    #[inline]
    pub fn filter_gt_f64(self, field_id: u16, value: f64) -> Self {
        self.filter_gt(field_id, sortable_f64_to_i64(value))
    }

    /// Greater-than-or-equal filter for `f64` (`field_id >= value`, `TASK-058`).
    #[inline]
    pub fn filter_gte_f64(self, field_id: u16, value: f64) -> Self {
        self.filter_gte(field_id, sortable_f64_to_i64(value))
    }

    /// Strict less-than filter for `f64` (`field_id < value`, `TASK-058`).
    #[inline]
    pub fn filter_lt_f64(self, field_id: u16, value: f64) -> Self {
        self.filter_lt(field_id, sortable_f64_to_i64(value))
    }

    /// Less-than-or-equal filter for `f64` (`field_id <= value`, `TASK-058`).
    #[inline]
    pub fn filter_lte_f64(self, field_id: u16, value: f64) -> Self {
        self.filter_lte(field_id, sortable_f64_to_i64(value))
    }

    /// Negated range exclusion filter (`WHERE field_id NOT BETWEEN min AND max`, `TASK-058`).
    #[inline]
    pub fn filter_not_range(mut self, field_id: u16, range: RangeInclusive<i64>) -> Self {
        let bm = self.engine.range_index().range_bitmap(field_id, range);
        self.union_extra_exclude(bm);
        self
    }

    /// Numeric set membership filter (`WHERE field_id IN (values)`, `TASK-058`).
    #[inline]
    pub fn filter_in_i64(mut self, field_id: u16, values: &[i64]) -> Self {
        let bm = self.engine.range_index().in_values_bitmap(field_id, values);
        self.intersect_extra_include(bm);
        self
    }

    /// Filters for records that have a numeric value indexed on `field_id` (`WHERE num_field IS NOT NULL`, `TASK-058`).
    #[inline]
    pub fn filter_has_numeric_field(mut self, field_id: u16) -> Self {
        let bm = self.engine.range_index().all_on_field_bitmap(field_id);
        self.intersect_extra_include(bm);
        self
    }

    /// Filters for records that have no numeric value indexed on `field_id` (`WHERE num_field IS NULL`, `TASK-058`).
    #[inline]
    pub fn filter_missing_numeric_field(mut self, field_id: u16) -> Self {
        let bm = self.engine.range_index().all_on_field_bitmap(field_id);
        self.union_extra_exclude(bm);
        self
    }

    // =========================================================================
    // 3. Primary-Key (`InlineId`) Set, Prefix & Graph Neighborhood Filters
    // =========================================================================

    /// Restricts results to a specific set of primary keys (`WHERE id IN (...)`, `TASK-058`).
    #[inline]
    pub fn filter_ids(mut self, ids: &[InlineId]) -> Self {
        let bm = self.engine.primary_index().ids_to_bitmap(ids);
        self.intersect_extra_include(bm);
        self
    }

    /// Excludes a specific set of primary keys (`WHERE id NOT IN (...)`, `TASK-058`).
    #[inline]
    pub fn filter_not_ids(mut self, ids: &[InlineId]) -> Self {
        let bm = self.engine.primary_index().ids_to_bitmap(ids);
        self.union_extra_exclude(bm);
        self
    }

    /// Filters for records whose primary key (`InlineId`) starts with `prefix` (`WHERE id LIKE 'prefix%'`, `TASK-058`).
    #[inline]
    pub fn filter_id_prefix(mut self, prefix: &str) -> Self {
        let bm = self.engine.primary_index().id_prefix_bitmap(prefix);
        self.intersect_extra_include(bm);
        self
    }

    /// Scopes the query to records reachable within `max_depth` BFS hops from `root_id` (`TASK-058`).
    ///
    /// `extract_neighbors` inspects each visited `RecordView` zero-copy and pushes neighbor `InlineId`s.
    pub fn filter_within_bfs<FN>(
        mut self,
        root_id: InlineId,
        max_depth: usize,
        extract_neighbors: FN,
    ) -> Self
    where
        FN: Fn(&RecordView<'_, T>, &mut Vec<InlineId>),
    {
        let mut visited: FastHashSet<InlineId> = FastHashSet::default();
        let mut queue: VecDeque<(InlineId, usize)> = VecDeque::new();
        let mut scratch_neighbors = Vec::new();
        let mut bm = CompressedBitmap::new();

        if let Some(root_ord) = self.engine.primary_index().with_entry(root_id, |e| e.ordinal) {
            visited.insert(root_id);
            bm.insert(root_ord);
            queue.push_back((root_id, 0));
        }

        while let Some((cur_id, depth)) = queue.pop_front() {
            if depth >= max_depth {
                continue;
            }
            scratch_neighbors.clear();
            let _ = self.engine.with_ref(cur_id, |view| {
                extract_neighbors(&view, &mut scratch_neighbors);
            });
            for &peer_id in &scratch_neighbors {
                if visited.insert(peer_id) {
                    if let Some(peer_ord) =
                        self.engine.primary_index().with_entry(peer_id, |e| e.ordinal)
                    {
                        bm.insert(peer_ord);
                        queue.push_back((peer_id, depth + 1));
                    }
                }
            }
        }

        self.intersect_extra_include(bm);
        self
    }

    // =========================================================================
    // 4. Composable Zero-Copy RecordView Predicate (`.filter(|view| ...)`)
    // =========================================================================

    /// Attaches a zero-copy record predicate closure evaluated directly on the memory-mapped
    /// `RecordView<'_, T>` (`0` heap allocations per inspected record, `TASK-058`).
    ///
    /// Multiple `.filter(...)` calls compose via logical `AND`.
    /// Because `RecordView<'_, T>` derefs to `T::Archived` and exposes `.str_contains(...)`,
    /// `.str_starts_with(...)`, `.str_contains_ignore_ascii_case(...)`, and `.get_slice(...)`,
    /// callers can filter on any fixed or variable-length field before pagination!
    pub fn filter<P>(mut self, predicate: P) -> Self
    where
        P: Fn(&RecordView<'_, T>) -> bool + 'a,
    {
        self.view_predicate = Some(match self.view_predicate.take() {
            Some(prev) => Box::new(move |view| prev(view) && predicate(view)),
            None => Box::new(predicate),
        });
        self
    }

    // =========================================================================
    // 5. Ordering & Pagination (`Asc`, `Desc`, `offset`, `limit`, `KeysetCursor`)
    // =========================================================================

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

    #[inline]
    fn check_extra_and_view(&self, ord: RecordOrdinal) -> bool {
        if let Some(ref inc) = self.extra_include_bm {
            if !inc.contains(ord) {
                return false;
            }
        }
        if let Some(ref exc) = self.extra_exclude_bm {
            if exc.contains(ord) {
                return false;
            }
        }
        if let Some(ref pred) = self.view_predicate {
            let Some(id) = self.engine.primary_index().resolve_ordinal(ord) else {
                return false;
            };
            match self.engine.with_ref(id, |view| pred(&view)) {
                Ok(matched) => matched,
                Err(_) => false,
            }
        } else {
            true
        }
    }

    /// Resolves matching `RecordOrdinal`s using secondary index intersections without
    /// touching any record files on disk (unless a `.filter(|view| ...)` payload closure was attached).
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

    /// Resolves a paginated window of `(Option<sort_key>, RecordOrdinal)` and `total_matched` (`TASK-056`, `TASK-058`).
    fn resolve_paginated_ordinals(&self) -> (Vec<(Option<i64>, RecordOrdinal)>, usize) {
        if let Some(ref inc) = self.extra_include_bm {
            if inc.is_empty() {
                return (Vec::new(), 0);
            }
        }

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
            let has_extra = self.extra_include_bm.is_some()
                || self.extra_exclude_bm.is_some()
                || self.view_predicate.is_some();
            let (pairs, total) = self.engine.range_index().range_collect_paginated(
                field_id,
                *range.start(),
                *range.end(),
                self.order,
                self.cursor,
                self.offset,
                self.limit,
                |ord| {
                    if has_tag_filters
                        && !self
                            .engine
                            .tag_index()
                            .matches_ordinal(ord, tags, not_tags, any_slice)
                    {
                        return false;
                    }
                    if has_extra && !self.check_extra_and_view(ord) {
                        return false;
                    }
                    true
                },
            );
            let out = pairs
                .into_iter()
                .map(|(k, ord)| (Some(k), ord))
                .collect();
            return (out, total);
        }

        // 3. Full-collection scan path when no positive index driver (`tags`, `any_tags`, `ranges`, `extra_include_bm`) is specified
        if tags.is_empty()
            && any_slice.is_empty()
            && ranges.is_empty()
            && self.extra_include_bm.is_none()
        {
            let has_extra = self.extra_exclude_bm.is_some() || self.view_predicate.is_some();
            let (pairs, total) = self.engine.primary_index().scan_ordinals_paginated(
                self.order == SortOrder::Desc,
                self.cursor.map(|c| c.ordinal),
                self.offset,
                self.limit,
                |ord, _id| {
                    if !not_tags.is_empty()
                        && !self
                            .engine
                            .tag_index()
                            .matches_ordinal(ord, &[], not_tags, &[])
                    {
                        return false;
                    }
                    if has_extra && !self.check_extra_and_view(ord) {
                        return false;
                    }
                    true
                },
            );
            let out = pairs.into_iter().map(|(ord, _)| (None, ord)).collect();
            return (out, total);
        }

        // 4. Multi-index CompressedBitmap intersection
        let mut candidate_bm: Option<CompressedBitmap> = self.extra_include_bm.clone();

        if !tags.is_empty() {
            let bm = self.engine.tag_index().intersect_all(tags);
            if bm.is_empty() {
                return (Vec::new(), 0);
            }
            candidate_bm = Some(match candidate_bm {
                Some(prev) => prev.intersect(&bm),
                None => bm,
            });
            if candidate_bm.as_ref().map_or(false, |b| b.is_empty()) {
                return (Vec::new(), 0);
            }
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

        if let Some(ref exc) = self.extra_exclude_bm {
            bm.and_not_in_place(exc);
            if bm.is_empty() {
                return (Vec::new(), 0);
            }
        }

        // If no custom `view_predicate` closure is attached, use hardware POPCNT windowing directly!
        if self.view_predicate.is_none() {
            let (ords, total) = bm.collect_window(
                self.offset,
                self.limit,
                self.cursor.map(|c| c.ordinal),
                self.order == SortOrder::Desc,
            );
            let out = ords.into_iter().map(|ord| (None, ord)).collect();
            return (out, total);
        }

        // When `view_predicate` is attached, evaluate `check_extra_and_view(ord)` before offset/limit
        let (all_ords, _) = bm.collect_window(
            0,
            None,
            self.cursor.map(|c| c.ordinal),
            self.order == SortOrder::Desc,
        );
        let max_items = self.limit.unwrap_or(usize::MAX);
        let mut out = Vec::with_capacity(max_items.min(all_ords.len().min(256)));
        let mut total_matched = 0usize;
        let mut skipped = 0usize;
        for ord in all_ords {
            if !self.check_extra_and_view(ord) {
                continue;
            }
            total_matched += 1;
            if skipped < self.offset {
                skipped += 1;
                continue;
            }
            if out.len() < max_items {
                out.push((None, ord));
            }
        }
        (out, total_matched)
    }

    /// Resolves matching primary keys (`InlineId`s) from secondary indexes (and optional `.filter(...)`).
    pub fn execute_ids(&self) -> Vec<InlineId> {
        let ordinals = self.resolve_ordinals();
        self.engine.primary_index().resolve_ordinals_batch(&ordinals)
    }

    /// Executes the query and returns a `QueryPage<InlineId>` with `total_matched`, `has_more`,
    /// and an $O(\log N)$ `next_cursor` (`TASK-056`).
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
        self,
        predicate: P,
    ) -> Result<QueryPage<(InlineId, RecordRef<T>)>>
    where
        P: Fn(&T::Archived) -> bool + 'a,
    {
        self.filter(move |view| predicate(view.archived()))
            .execute_page_refs()
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
    /// If any tag, range, key, graph, or `.filter(|view| ...)` predicates are provided, only candidates
    /// matching all filters are scored!
    pub fn top_k_similar(&self, query_embedding: &[f32], k: usize) -> Vec<(InlineId, VectorTopKHit)> {
        if k == 0 || query_embedding.is_empty() {
            return Vec::new();
        }
        let candidate_ordinals = if self.tag_len == 0
            && self.not_tag_len == 0
            && self.any_tag_len == 0
            && self.range_len == 0
            && self.extra_include_bm.is_none()
            && self.extra_exclude_bm.is_none()
            && self.view_predicate.is_none()
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

        let tag_alpha = InlineId::from_static("env:alpha");
        let tag_beta = InlineId::from_static("env:beta");
        let tag_gamma = InlineId::from_static("role:gamma");
        let tag_banned = InlineId::from_static("status:banned");

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
            if i == 1 {
                rec.children.link(InlineId::from_static("rec_02"));
                rec.children.link(InlineId::from_static("rec_03"));
            }
            if i == 2 {
                rec.children.link(InlineId::from_static("rec_04"));
            }
            engine.put(id, &rec).unwrap();
        }

        // 1. Test `filter_any_tag` (`OR` / `IN`) + `filter_not_tag` (`AND NOT`) + `filter_range` + `order_desc`
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

        // 4. Test `filter_tag_prefix("env:")`, `filter_gt`, `filter_lt`, `filter_ne`, and composable `.filter(|view| ...)`
        let prefix_filtered = engine
            .query()
            .filter_tag_prefix(FIELD_PRIMARY_TAG, "env:")
            .filter_gt(FIELD_SCORE, 50)
            .filter_lte(FIELD_SCORE, 150)
            .filter_ne(FIELD_SCORE, 100)
            .filter(|view| view.str_contains_ignore_ascii_case(&view.title, "ITEM"))
            .execute_ids();
        // Scores in 60..=150 (i=6..=15) where tag starts with "env:" (not multiple of 3: 7, 8, 10, 11, 13, 14)
        // and score != 100 (excludes 10) -> [rec_07, rec_08, rec_11, rec_13, rec_14]
        assert_eq!(
            prefix_filtered,
            vec![
                InlineId::from_static("rec_07"),
                InlineId::from_static("rec_08"),
                InlineId::from_static("rec_11"),
                InlineId::from_static("rec_13"),
                InlineId::from_static("rec_14"),
            ]
        );

        // 5. Test `filter_in_i64`, `filter_id_prefix`, and `filter_not_ids`
        let set_filtered = engine
            .query()
            .filter_in_i64(FIELD_SCORE, &[20, 40, 60, 80])
            .filter_id_prefix("rec_0")
            .filter_not_ids(&[InlineId::from_static("rec_04")])
            .execute_ids();
        assert_eq!(
            set_filtered,
            vec![
                InlineId::from_static("rec_02"),
                InlineId::from_static("rec_06"),
                InlineId::from_static("rec_08"),
            ]
        );

        // 6. Test Hybrid Graph BFS + Index Filtering (`filter_within_bfs`)
        let bfs_filtered = engine
            .query()
            .filter_within_bfs(InlineId::from_static("rec_01"), 2, |view, out| {
                out.extend_from_slice(view.children.as_slice(view.payload()));
            })
            .filter_gte(FIELD_SCORE, 20)
            .execute_ids();
        // Reachable within 2 hops of rec_01: rec_01 (10), rec_02 (20), rec_03 (30), rec_04 (40)
        // With score >= 20 -> rec_02, rec_03, rec_04
        assert_eq!(
            bfs_filtered,
            vec![
                InlineId::from_static("rec_02"),
                InlineId::from_static("rec_03"),
                InlineId::from_static("rec_04"),
            ]
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}



