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

    /// Counts matching `RecordOrdinal`s in `range` on `field_id` with **zero heap allocations** (`TASK-048`, `F-OPT-01`).
    #[inline]
    pub fn count_in_range<R: RangeBounds<i64>>(&self, field_id: u16, range: R) -> usize {
        let guard = self
            .trees
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(tree) = guard.get(&field_id) else {
            return 0;
        };
        let mut count = 0usize;
        for (_k, leaf) in tree.range(range) {
            count += (leaf.inline_len as usize) + leaf.spill.len();
        }
        count
    }

    /// Inserts an `f64` value into `field_id` using IEEE-754 order-preserving `i64` encoding.
    #[inline]
    pub fn insert_f64(&self, field_id: u16, value: f64, ord: RecordOrdinal) {
        self.insert(field_id, sortable_f64_to_i64(value), ord);
    }

    /// Queries an inclusive `[min, max]` floating-point range on `field_id`.
    #[inline]
    pub fn range_f64_bitmap(&self, field_id: u16, min: f64, max: f64) -> CompressedBitmap {
        let lo = sortable_f64_to_i64(min);
        let hi = sortable_f64_to_i64(max);
        if lo <= hi {
            self.range_bitmap(field_id, lo..=hi)
        } else {
            CompressedBitmap::new()
        }
    }

    /// Exports all `(field_id, Vec<(key, RecordOrdinal)>)` entries for paged disk checkpointing (`TASK-034`).
    pub fn snapshot_all(&self) -> Vec<(u16, Vec<(i64, RecordOrdinal)>)> {
        let guard = self
            .trees
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut out = Vec::with_capacity(guard.len());
        for (&field_id, tree) in guard.iter() {
            let mut pairs = Vec::new();
            for (&k, leaf) in tree {
                let mut ords = Vec::new();
                leaf.extend_into_vec(&mut ords);
                for ord in ords {
                    pairs.push((k, ord));
                }
            }
            out.push((field_id, pairs));
        }
        out
    }
}

/// Bijective IEEE-754 order-preserving conversion from `f64` to `i64` so floating-point numbers
/// (`-INFINITY`, negative numbers, `-0.0`/`+0.0`, positive numbers, `+INFINITY`) sort identically
/// in `NumericRangeIndex`'s `BTreeMap<i64, LeafPostingList>`.
#[inline]
pub fn sortable_f64_to_i64(val: f64) -> i64 {
    let norm = if val == 0.0 { 0.0 } else { val };
    let bits = norm.to_bits() as i64;
    if bits < 0 {
        bits ^ 0x7FFF_FFFF_FFFF_FFFF
    } else {
        bits
    }
}

/// Order-preserving conversion from `u64` (`0..=u64::MAX`) to `i64` (`i64::MIN..=i64::MAX`).
#[inline]
pub const fn sortable_u64_to_i64(val: u64) -> i64 {
    (val ^ (1u64 << 63)) as i64
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

    #[test]
    fn test_f64_and_u64_order_preserving_range_index() {
        let idx = NumericRangeIndex::new();
        idx.insert_f64(2, -100.5, RecordOrdinal(1));
        idx.insert_f64(2, -0.25, RecordOrdinal(2));
        idx.insert_f64(2, 0.0, RecordOrdinal(3));
        idx.insert_f64(2, 42.125, RecordOrdinal(4));
        idx.insert_f64(2, 999.99, RecordOrdinal(5));

        let bm = idx.range_f64_bitmap(2, -1.0, 50.0);
        assert_eq!(bm.to_vec(), vec![RecordOrdinal(2), RecordOrdinal(3), RecordOrdinal(4)]);
        assert!(sortable_u64_to_i64(0) < sortable_u64_to_i64(u64::MAX));
    }
}
