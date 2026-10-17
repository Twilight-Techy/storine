//! Cache-conscious B+Tree Range Index for timestamps and numeric fields (`TASK-017`, `F-INDX-03`).

use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::RwLock;
use storage_types::{FastHashMap, RecordOrdinal};

use crate::bitmap::CompressedBitmap;

/// Compact inline-first ordinal list at each B+Tree key leaf (`[RecordOrdinal; 4]` inline before spilling).
#[derive(Clone, Debug, Default)]
struct LeafPostingList {
    inline_len: u8,
    inline: [RecordOrdinal; 4],
    spill: Vec<RecordOrdinal>,
}

impl LeafPostingList {
    #[inline]
    fn insert(&mut self, ord: RecordOrdinal) {
        let in_slice = &self.inline[..self.inline_len as usize];
        if in_slice.contains(&ord) || self.spill.binary_search(&ord).is_ok() {
            return;
        }
        if (self.inline_len as usize) < self.inline.len() {
            self.inline[self.inline_len as usize] = ord;
            self.inline_len += 1;
            self.inline[..self.inline_len as usize].sort_unstable();
        } else {
            let pos = self.spill.binary_search(&ord).unwrap_or_else(|p| p);
            self.spill.insert(pos, ord);
        }
    }

    #[inline]
    fn remove(&mut self, ord: RecordOrdinal) -> bool {
        let len = self.inline_len as usize;
        if let Some(pos) = self.inline[..len].iter().position(|&x| x == ord) {
            self.inline.copy_within(pos + 1..len, pos);
            self.inline_len -= 1;
            if !self.spill.is_empty() {
                self.inline[self.inline_len as usize] = self.spill.remove(0);
                self.inline_len += 1;
            }
            return true;
        }
        if let Ok(pos) = self.spill.binary_search(&ord) {
            self.spill.remove(pos);
            return true;
        }
        false
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.inline_len == 0 && self.spill.is_empty()
    }

    #[inline]
    fn extend_into_vec(&self, out: &mut Vec<RecordOrdinal>) {
        out.extend_from_slice(&self.inline[..self.inline_len as usize]);
        out.extend_from_slice(&self.spill);
    }

    #[inline]
    fn insert_into_bitmap(&self, bm: &mut CompressedBitmap) {
        for &ord in &self.inline[..self.inline_len as usize] {
            bm.insert(ord);
        }
        for &ord in &self.spill {
            bm.insert(ord);
        }
    }
}

/// Secondary range index over 64-bit signed integer / timestamp fields (`field_id -> BTreeMap<i64, LeafPostingList>`).
#[derive(Debug, Default)]
pub struct NumericRangeIndex {
    trees: RwLock<FastHashMap<u16, BTreeMap<i64, LeafPostingList>>>,
}

impl NumericRangeIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, field_id: u16, value: i64, ord: RecordOrdinal) {
        let mut guard = self
            .trees
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard
            .entry(field_id)
            .or_default()
            .entry(value)
            .or_default()
            .insert(ord);
    }

    pub fn remove(&self, field_id: u16, value: i64, ord: RecordOrdinal) {
        let mut guard = self
            .trees
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(tree) = guard.get_mut(&field_id) {
            let empty_leaf = if let Some(leaf) = tree.get_mut(&value) {
                leaf.remove(ord);
                leaf.is_empty()
            } else {
                false
            };
            if empty_leaf {
                tree.remove(&value);
            }
        }
    }

    /// Collects all `RecordOrdinal`s matching `range` on `field_id` in ascending key order.
    pub fn range_collect<R: RangeBounds<i64>>(
        &self,
        field_id: u16,
        range: R,
        limit: Option<usize>,
    ) -> Vec<RecordOrdinal> {
        let guard = self
            .trees
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(tree) = guard.get(&field_id) else {
            return Vec::new();
        };
        let mut out = Vec::with_capacity(limit.unwrap_or(512));
        for (_k, leaf) in tree.range(range) {
            leaf.extend_into_vec(&mut out);
            if let Some(max) = limit {
                if out.len() >= max {
                    out.truncate(max);
                    break;
                }
            }
        }
        out
    }

    /// Returns a `CompressedBitmap` of all `RecordOrdinal`s matching `range` on `field_id`
    /// so range predicates can be intersected with tag bitmaps in microseconds!
    pub fn range_bitmap<R: RangeBounds<i64>>(
        &self,
        field_id: u16,
        range: R,
    ) -> CompressedBitmap {
        let guard = self
            .trees
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut bm = CompressedBitmap::new();
        if let Some(tree) = guard.get(&field_id) {
            for (_k, leaf) in tree.range(range) {
                leaf.insert_into_bitmap(&mut bm);
            }
        }
        bm
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numeric_range_index_scan() {
        let idx = NumericRangeIndex::new();
        for i in 0..1000i64 {
            idx.insert(1, 1_700_000_000 + i, RecordOrdinal(i as u32));
        }
        let hits = idx.range_collect(1, 1_700_000_100..=1_700_000_109, None);
        assert_eq!(hits.len(), 10);
        assert_eq!(hits[0], RecordOrdinal(100));
        assert_eq!(hits[9], RecordOrdinal(109));
    }
}
