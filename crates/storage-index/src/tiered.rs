//! Storage-tier dispatch for secondary indexes (`TASK-083`, `F-PIDX-01`, `F-PIDX-02`).
//!
//! [`RangeIndex`] / [`TagIndex`] expose one API over the in-memory indexes (fastest; RAM grows
//! with the number of postings, rebuilt from the records at open) and the paged indexes
//! (RAM bounded by pages + delta; persisted and reused across restarts). Method names and
//! semantics are identical, so query code is tier-agnostic.

use std::ops::RangeBounds;

use storage_types::{InlineId, RecordOrdinal};

use crate::bitmap::{BitmapTagIndex, CompressedBitmap};
use crate::btree::{KeysetCursor, NumericRangeIndex, SortOrder};
use crate::paged_bitmap::PagedBitmapIndex;
use crate::paged_btree::PagedBTreeIndex;

/// Numeric / timestamp range index of either storage tier.
#[derive(Debug)]
pub enum RangeIndex {
    Memory(NumericRangeIndex),
    Paged(PagedBTreeIndex),
}

macro_rules! dispatch {
    ($self:ident, $idx:ident => $body:expr) => {
        match $self {
            Self::Memory($idx) => $body,
            Self::Paged($idx) => $body,
        }
    };
}

impl RangeIndex {
    #[inline]
    pub fn insert(&self, field_id: u16, value: i64, ord: RecordOrdinal) {
        dispatch!(self, i => i.insert(field_id, value, ord))
    }

    #[inline]
    pub fn remove(&self, field_id: u16, value: i64, ord: RecordOrdinal) {
        dispatch!(self, i => i.remove(field_id, value, ord))
    }

    pub fn range_collect<R: RangeBounds<i64>>(
        &self,
        field_id: u16,
        range: R,
        limit: Option<usize>,
    ) -> Vec<RecordOrdinal> {
        dispatch!(self, i => i.range_collect(field_id, range, limit))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn range_collect_paginated<F: Fn(RecordOrdinal) -> bool>(
        &self,
        field_id: u16,
        min_inclusive: i64,
        max_inclusive: i64,
        order: SortOrder,
        cursor: Option<KeysetCursor>,
        offset: usize,
        limit: Option<usize>,
        filter_fn: F,
    ) -> (Vec<(i64, RecordOrdinal)>, usize) {
        dispatch!(self, i => i.range_collect_paginated(
            field_id, min_inclusive, max_inclusive, order, cursor, offset, limit, filter_fn
        ))
    }

    pub fn range_bitmap<R: RangeBounds<i64>>(&self, field_id: u16, range: R) -> CompressedBitmap {
        dispatch!(self, i => i.range_bitmap(field_id, range))
    }

    pub fn eq_bitmap(&self, field_id: u16, value: i64) -> CompressedBitmap {
        dispatch!(self, i => i.eq_bitmap(field_id, value))
    }

    pub fn in_values_bitmap(&self, field_id: u16, values: &[i64]) -> CompressedBitmap {
        dispatch!(self, i => i.in_values_bitmap(field_id, values))
    }

    pub fn all_on_field_bitmap(&self, field_id: u16) -> CompressedBitmap {
        dispatch!(self, i => i.all_on_field_bitmap(field_id))
    }

    pub fn count_in_range<R: RangeBounds<i64>>(&self, field_id: u16, range: R) -> usize {
        dispatch!(self, i => i.count_in_range(field_id, range))
    }

    /// The paged index, if this is the paged tier.
    #[inline]
    pub fn as_paged(&self) -> Option<&PagedBTreeIndex> {
        match self {
            Self::Paged(i) => Some(i),
            Self::Memory(_) => None,
        }
    }
}

/// Tag / equality bitmap index of either storage tier.
#[derive(Debug)]
pub enum TagIndex {
    Memory(BitmapTagIndex),
    Paged(PagedBitmapIndex),
}

impl TagIndex {
    #[inline]
    pub fn insert(&self, field_id: u16, tag: InlineId, ord: RecordOrdinal) {
        dispatch!(self, i => i.insert(field_id, tag, ord))
    }

    #[inline]
    pub fn remove(&self, field_id: u16, tag: InlineId, ord: RecordOrdinal) {
        dispatch!(self, i => i.remove(field_id, tag, ord))
    }

    pub fn get_bitmap(&self, field_id: u16, tag: InlineId) -> CompressedBitmap {
        dispatch!(self, i => i.get_bitmap(field_id, tag))
    }

    /// Exact (memory tier) or upper-bound (paged tier) posting count; `0` only if empty.
    pub fn cardinality(&self, field_id: u16, tag: InlineId) -> usize {
        dispatch!(self, i => i.cardinality(field_id, tag))
    }

    pub fn intersect_all(&self, predicates: &[(u16, InlineId)]) -> CompressedBitmap {
        dispatch!(self, i => i.intersect_all(predicates))
    }

    pub fn union_tags(&self, field_id: u16, tags: &[InlineId]) -> CompressedBitmap {
        dispatch!(self, i => i.union_tags(field_id, tags))
    }

    pub fn union_tag_prefix(&self, field_id: u16, prefix: &str) -> CompressedBitmap {
        dispatch!(self, i => i.union_tag_prefix(field_id, prefix))
    }

    pub fn all_on_field_bitmap(&self, field_id: u16) -> CompressedBitmap {
        dispatch!(self, i => i.all_on_field_bitmap(field_id))
    }

    pub fn exclude_from_bitmap(&self, not_predicates: &[(u16, InlineId)], target: &mut CompressedBitmap) {
        dispatch!(self, i => i.exclude_from_bitmap(not_predicates, target))
    }

    pub fn filter_in_place(&self, predicates: &[(u16, InlineId)], candidates: &mut Vec<RecordOrdinal>) {
        dispatch!(self, i => i.filter_in_place(predicates, candidates))
    }

    pub fn matches_ordinal(
        &self,
        ord: RecordOrdinal,
        must_tags: &[(u16, InlineId)],
        not_tags: &[(u16, InlineId)],
        any_tag_groups: &[(u16, &[InlineId])],
    ) -> bool {
        dispatch!(self, i => i.matches_ordinal(ord, must_tags, not_tags, any_tag_groups))
    }

    /// The paged index, if this is the paged tier.
    #[inline]
    pub fn as_paged(&self) -> Option<&PagedBitmapIndex> {
        match self {
            Self::Paged(i) => Some(i),
            Self::Memory(_) => None,
        }
    }
}
