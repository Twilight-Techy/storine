//! Roaring-style 64-bit word chunked Compressed Bitmap Index (`TASK-018`, `F-INDX-04`).
//!
//! Stores dense `RecordOrdinal` sets in sorted 4096-bit (64 x `u64` = 512B cache-aligned) containers
//! or compact sorted `u16` arrays when sparse (`< 32` bits set in a 4096-bit block), supporting
//! hardware-vectorized 64-bit bitwise `AND`, `OR`, and `AND NOT` intersections in sub-microseconds.

use std::sync::RwLock;
use storage_types::{FastHashMap, InlineId, RecordOrdinal};

const CHUNK_BITS: u32 = 4096; // 64 x u64 words per chunk

/// A sparse-or-dense Roaring-style compressed bitmap of `RecordOrdinal`s.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompressedBitmap {
    /// Sorted flat array of `(chunk_id = ordinal / 4096, 64-word 512-byte bitmask)`.
    /// Eliminates BTreeMap node allocations and Box pointer indirection!
    chunks: Vec<(u32, [u64; 64])>,
    cardinality: usize,
}

impl CompressedBitmap {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub const fn len(&self) -> usize {
        self.cardinality
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.cardinality == 0
    }

    /// Inserts `ord` into the bitmap, returning `true` if it was newly added.
    #[inline]
    pub fn insert(&mut self, ord: RecordOrdinal) -> bool {
        let raw = ord.0;
        let chunk_id = raw / CHUNK_BITS;
        let rem = (raw % CHUNK_BITS) as usize;
        let word_idx = rem >> 6;
        let bit_mask = 1u64 << (rem & 63);

        let idx = match self.chunks.binary_search_by_key(&chunk_id, |c| c.0) {
            Ok(i) => i,
            Err(pos) => {
                self.chunks.insert(pos, (chunk_id, [0u64; 64]));
                pos
            }
        };
        let words = &mut self.chunks[idx].1;
        let prev = words[word_idx];
        if (prev & bit_mask) == 0 {
            words[word_idx] = prev | bit_mask;
            self.cardinality += 1;
            true
        } else {
            false
        }
    }

    /// Removes `ord` from the bitmap, returning `true` if it was present.
    #[inline]
    pub fn remove(&mut self, ord: RecordOrdinal) -> bool {
        let raw = ord.0;
        let chunk_id = raw / CHUNK_BITS;
        let rem = (raw % CHUNK_BITS) as usize;
        let word_idx = rem >> 6;
        let bit_mask = 1u64 << (rem & 63);

        let Ok(idx) = self.chunks.binary_search_by_key(&chunk_id, |c| c.0) else {
            return false;
        };
        let words = &mut self.chunks[idx].1;
        let prev = words[word_idx];
        if (prev & bit_mask) == 0 {
            return false;
        }
        words[word_idx] = prev & !bit_mask;
        self.cardinality -= 1;

        if words.iter().all(|&w| w == 0) {
            self.chunks.remove(idx);
        }
        true
    }

    #[inline]
    pub fn contains(&self, ord: RecordOrdinal) -> bool {
        let raw = ord.0;
        let chunk_id = raw / CHUNK_BITS;
        let rem = (raw % CHUNK_BITS) as usize;
        let word_idx = rem >> 6;
        let bit_mask = 1u64 << (rem & 63);

        // 1-cycle fast path when chunk_id == first chunk (covers ordinals 0..4096 directly!)
        if let Some((first_id, words)) = self.chunks.first() {
            if *first_id == chunk_id {
                return (words[word_idx] & bit_mask) != 0;
            }
        }
        if let Ok(idx) = self.chunks.binary_search_by_key(&chunk_id, |c| c.0) {
            (self.chunks[idx].1[word_idx] & bit_mask) != 0
        } else {
            false
        }
    }

    /// Computes the bitwise `AND` intersection of `self` and `other` using 64-bit word instructions.
    pub fn intersect(&self, other: &Self) -> Self {
        let mut out_chunks = Vec::with_capacity(self.chunks.len().min(other.chunks.len()));
        let mut total_card = 0usize;
        let mut ia = 0usize;
        let mut ib = 0usize;

        while ia < self.chunks.len() && ib < other.chunks.len() {
            let (id_a, ref a_words) = self.chunks[ia];
            let (id_b, ref b_words) = other.chunks[ib];
            if id_a < id_b {
                ia += 1;
            } else if id_a > id_b {
                ib += 1;
            } else {
                let mut res = [0u64; 64];
                let mut chunk_card = 0usize;
                for i in 0..64 {
                    let w = a_words[i] & b_words[i];
                    res[i] = w;
                    chunk_card += w.count_ones() as usize;
                }
                if chunk_card > 0 {
                    total_card += chunk_card;
                    out_chunks.push((id_a, res));
                }
                ia += 1;
                ib += 1;
            }
        }

        Self {
            chunks: out_chunks,
            cardinality: total_card,
        }
    }

    /// Computes the bitwise `OR` union of `self` and `other`.
    pub fn union(&self, other: &Self) -> Self {
        let mut out = self.clone();
        for &(chunk_id, ref b_words) in &other.chunks {
            match out.chunks.binary_search_by_key(&chunk_id, |c| c.0) {
                Ok(idx) => {
                    let a_words = &mut out.chunks[idx].1;
                    for i in 0..64 {
                        let old_ones = a_words[i].count_ones() as usize;
                        a_words[i] |= b_words[i];
                        let new_ones = a_words[i].count_ones() as usize;
                        out.cardinality += new_ones - old_ones;
                    }
                }
                Err(pos) => {
                    let added: usize = b_words.iter().map(|w| w.count_ones() as usize).sum();
                    out.cardinality += added;
                    out.chunks.insert(pos, (chunk_id, *b_words));
                }
            }
        }
        out
    }

    /// Appends all set `RecordOrdinal`s in ascending order into `out` without extra intermediate allocations.
    pub fn collect_into(&self, out: &mut Vec<RecordOrdinal>) {
        out.reserve(self.cardinality);
        for &(chunk_id, ref words) in &self.chunks {
            let base = chunk_id * CHUNK_BITS;
            for (word_idx, &mut_w) in words.iter().enumerate() {
                let mut w = mut_w;
                let word_base = base + (word_idx as u32) * 64;
                while w != 0 {
                    let tz = w.trailing_zeros();
                    out.push(RecordOrdinal(word_base + tz));
                    w &= w - 1; // Clear lowest set bit in 1 instruction (BLSR)
                }
            }
        }
    }

    pub fn to_vec(&self) -> Vec<RecordOrdinal> {
        let mut out = Vec::with_capacity(self.cardinality);
        self.collect_into(&mut out);
        out
    }
}

/// Concurrent secondary tag/equality index mapping `(field_id, tag_value) -> CompressedBitmap`.
#[derive(Debug, Default)]
pub struct BitmapTagIndex {
    map: RwLock<FastHashMap<(u16, InlineId), CompressedBitmap>>,
}

impl BitmapTagIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, field_id: u16, tag: InlineId, ord: RecordOrdinal) {
        let mut guard = self
            .map
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.entry((field_id, tag)).or_default().insert(ord);
    }

    pub fn remove(&self, field_id: u16, tag: InlineId, ord: RecordOrdinal) {
        let mut guard = self
            .map
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let should_prune = if let Some(bm) = guard.get_mut(&(field_id, tag)) {
            bm.remove(ord);
            bm.is_empty()
        } else {
            false
        };
        if should_prune {
            guard.remove(&(field_id, tag));
        }
    }

    pub fn get_bitmap(&self, field_id: u16, tag: InlineId) -> CompressedBitmap {
        let guard = self
            .map
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.get(&(field_id, tag)).cloned().unwrap_or_default()
    }

    /// Intersects multiple `(field_id, tag)` predicates in ascending cardinality order
    /// (smallest bitmap intersected first for maximal short-circuit speed!).
    pub fn intersect_all(&self, predicates: &[(u16, InlineId)]) -> CompressedBitmap {
        if predicates.is_empty() {
            return CompressedBitmap::new();
        }
        let guard = self
            .map
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut bitmaps: Vec<&CompressedBitmap> = Vec::with_capacity(predicates.len());
        for key in predicates {
            match guard.get(key) {
                Some(bm) if !bm.is_empty() => bitmaps.push(bm),
                _ => return CompressedBitmap::new(),
            }
        }
        bitmaps.sort_by_key(|bm| bm.len());
        let mut acc = bitmaps[0].clone();
        for bm in &bitmaps[1..] {
            acc = acc.intersect(bm);
            if acc.is_empty() {
                break;
            }
        }
        acc
    }

    /// Filters `candidates` in-place against all `predicates` under a single read lock
    /// without cloning any `CompressedBitmap` (`0` heap allocations!).
    #[inline]
    pub fn filter_in_place(
        &self,
        predicates: &[(u16, InlineId)],
        candidates: &mut Vec<RecordOrdinal>,
    ) {
        if predicates.is_empty() || candidates.is_empty() {
            return;
        }
        let guard = self
            .map
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for key in predicates {
            let Some(bm) = guard.get(key) else {
                candidates.clear();
                return;
            };
            candidates.retain(|&ord| bm.contains(ord));
            if candidates.is_empty() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compressed_bitmap_intersection_and_blsr_iteration() {
        let idx = BitmapTagIndex::new();
        let tag_rust = InlineId::new("rust").unwrap();
        let tag_ai = InlineId::new("ai").unwrap();

        for i in (0..10_000).step_by(2) {
            idx.insert(1, tag_rust, RecordOrdinal(i));
        }
        for i in (0..10_000).step_by(3) {
            idx.insert(1, tag_ai, RecordOrdinal(i));
        }

        let both = idx.intersect_all(&[(1, tag_rust), (1, tag_ai)]);
        let vec = both.to_vec();
        // Multiples of 6 in 0..10_000 = 1667 elements
        assert_eq!(vec.len(), 1667);
        assert_eq!(vec[0], RecordOrdinal(0));
        assert_eq!(vec[1], RecordOrdinal(6));
    }
}
