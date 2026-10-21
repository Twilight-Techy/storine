//! Roaring-style 64-bit word chunked Compressed Bitmap Index (`TASK-018`, `F-INDX-04`).
//!
//! Stores `RecordOrdinal` sets as a sorted array of 4096-bit chunks (64 x `u64` = 512 B each),
//! supporting 64-bit-word bitwise `AND`, `OR`, and `AND NOT` intersections. Every non-empty chunk
//! costs 512 B regardless of how many bits it holds (there is no sparse array container), so very
//! sparse posting lists over a wide ordinal range use more memory than a Roaring bitmap would.

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

    /// Computes the bitwise `AND NOT` difference (`self \ other`) using 64-bit word instructions (`TASK-055`).
    pub fn and_not(&self, other: &Self) -> Self {
        let mut out = self.clone();
        out.and_not_in_place(other);
        out
    }

    /// Removes all bits present in `other` from `self` in-place (`self &= !other`) with zero heap allocations (`TASK-055`).
    pub fn and_not_in_place(&mut self, other: &Self) {
        if self.is_empty() || other.is_empty() {
            return;
        }
        let mut ia = 0usize;
        let mut ib = 0usize;
        while ia < self.chunks.len() && ib < other.chunks.len() {
            let id_a = self.chunks[ia].0;
            let id_b = other.chunks[ib].0;
            if id_a < id_b {
                ia += 1;
            } else if id_a > id_b {
                ib += 1;
            } else {
                let a_words = &mut self.chunks[ia].1;
                let b_words = &other.chunks[ib].1;
                let mut remaining_in_chunk = 0usize;
                for i in 0..64 {
                    let prev = a_words[i];
                    let next = prev & !b_words[i];
                    if next != prev {
                        let removed = (prev ^ next).count_ones() as usize;
                        self.cardinality -= removed;
                        a_words[i] = next;
                    }
                    remaining_in_chunk += next.count_ones() as usize;
                }
                if remaining_in_chunk == 0 {
                    self.chunks.remove(ia);
                } else {
                    ia += 1;
                }
                ib += 1;
            }
        }
    }

    /// Collects a paginated window `(items, total_matched)` from the bitmap (`TASK-055`).
    ///
    /// - Uses hardware `POPCNT` (`count_ones`) to skip whole 64-bit words during `offset` skipping in 1 CPU cycle.
    /// - Supports both ascending (`BLSR` `w &= w - 1`) and descending (`leading_zeros`) traversal.
    /// - Supports $O(1)$ keyset cursor seek via `after_ordinal`.
    pub fn collect_window(
        &self,
        offset: usize,
        limit: Option<usize>,
        after_ordinal: Option<RecordOrdinal>,
        descending: bool,
    ) -> (Vec<RecordOrdinal>, usize) {
        if self.is_empty() {
            return (Vec::new(), 0);
        }
        let max_items = limit.unwrap_or(usize::MAX);
        if after_ordinal.is_none() {
            let total = self.cardinality;
            if offset >= total || max_items == 0 {
                return (Vec::new(), total);
            }
            let cap = (total - offset).min(max_items);
            let mut out = Vec::with_capacity(cap);
            let mut skipped = 0usize;

            if !descending {
                'outer_asc: for &(chunk_id, ref words) in &self.chunks {
                    let base = chunk_id * CHUNK_BITS;
                    for (word_idx, &raw_w) in words.iter().enumerate() {
                        if raw_w == 0 {
                            continue;
                        }
                        let ones = raw_w.count_ones() as usize;
                        if skipped + ones <= offset {
                            skipped += ones;
                            continue;
                        }
                        let mut w = raw_w;
                        let word_base = base + (word_idx as u32) * 64;
                        while skipped < offset && w != 0 {
                            w &= w - 1;
                            skipped += 1;
                        }
                        while w != 0 {
                            let tz = w.trailing_zeros();
                            out.push(RecordOrdinal(word_base + tz));
                            if out.len() >= max_items {
                                break 'outer_asc;
                            }
                            w &= w - 1;
                        }
                    }
                }
            } else {
                'outer_desc: for &(chunk_id, ref words) in self.chunks.iter().rev() {
                    let base = chunk_id * CHUNK_BITS;
                    for (word_idx, &raw_w) in words.iter().enumerate().rev() {
                        if raw_w == 0 {
                            continue;
                        }
                        let ones = raw_w.count_ones() as usize;
                        if skipped + ones <= offset {
                            skipped += ones;
                            continue;
                        }
                        let mut w = raw_w;
                        let word_base = base + (word_idx as u32) * 64;
                        while skipped < offset && w != 0 {
                            let hi = 63 - w.leading_zeros();
                            w &= !(1u64 << hi);
                            skipped += 1;
                        }
                        while w != 0 {
                            let hi = 63 - w.leading_zeros();
                            out.push(RecordOrdinal(word_base + hi));
                            if out.len() >= max_items {
                                break 'outer_desc;
                            }
                            w &= !(1u64 << hi);
                        }
                    }
                }
            }
            return (out, total);
        }

        // Cursor-filtered path (`after_ordinal == Some(cursor)`)
        let Some(RecordOrdinal(cursor)) = after_ordinal else {
            // Unreachable: the `after_ordinal.is_none()` branch above already returned.
            return (Vec::new(), 0);
        };
        let mut out = Vec::with_capacity(max_items.min(self.cardinality));
        let mut total_matched = 0usize;
        let mut skipped = 0usize;

        if !descending {
            for &(chunk_id, ref words) in &self.chunks {
                let base = chunk_id * CHUNK_BITS;
                if base + CHUNK_BITS - 1 <= cursor {
                    continue;
                }
                for (word_idx, &raw_w) in words.iter().enumerate() {
                    if raw_w == 0 {
                        continue;
                    }
                    let word_base = base + (word_idx as u32) * 64;
                    if word_base + 63 <= cursor {
                        continue;
                    }
                    let mut w = if word_base <= cursor {
                        let shift = (cursor - word_base) + 1;
                        if shift >= 64 {
                            0
                        } else {
                            raw_w & (!0u64 << shift)
                        }
                    } else {
                        raw_w
                    };
                    if w == 0 {
                        continue;
                    }
                    let ones = w.count_ones() as usize;
                    total_matched += ones;
                    if out.len() >= max_items {
                        continue;
                    }
                    if skipped + ones <= offset {
                        skipped += ones;
                        continue;
                    }
                    while skipped < offset && w != 0 {
                        w &= w - 1;
                        skipped += 1;
                    }
                    while w != 0 && out.len() < max_items {
                        let tz = w.trailing_zeros();
                        out.push(RecordOrdinal(word_base + tz));
                        w &= w - 1;
                    }
                }
            }
        } else {
            for &(chunk_id, ref words) in self.chunks.iter().rev() {
                let base = chunk_id * CHUNK_BITS;
                if base >= cursor {
                    continue;
                }
                for (word_idx, &raw_w) in words.iter().enumerate().rev() {
                    if raw_w == 0 {
                        continue;
                    }
                    let word_base = base + (word_idx as u32) * 64;
                    if word_base >= cursor {
                        continue;
                    }
                    let mut w = if word_base + 64 > cursor {
                        let keep_bits = cursor - word_base;
                        raw_w & ((1u64 << keep_bits) - 1)
                    } else {
                        raw_w
                    };
                    if w == 0 {
                        continue;
                    }
                    let ones = w.count_ones() as usize;
                    total_matched += ones;
                    if out.len() >= max_items {
                        continue;
                    }
                    if skipped + ones <= offset {
                        skipped += ones;
                        continue;
                    }
                    while skipped < offset && w != 0 {
                        let hi = 63 - w.leading_zeros();
                        w &= !(1u64 << hi);
                        skipped += 1;
                    }
                    while w != 0 && out.len() < max_items {
                        let hi = 63 - w.leading_zeros();
                        out.push(RecordOrdinal(word_base + hi));
                        w &= !(1u64 << hi);
                    }
                }
            }
        }
        (out, total_matched)
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

    #[inline]
    pub fn chunks_slice(&self) -> &[(u32, [u64; 64])] {
        &self.chunks
    }

    #[inline]
    pub fn from_raw_chunks(chunks: Vec<(u32, [u64; 64])>) -> Self {
        let cardinality = chunks
            .iter()
            .flat_map(|(_, words)| words.iter())
            .map(|w| w.count_ones() as usize)
            .sum();
        Self {
            chunks,
            cardinality,
        }
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

    /// Returns the exact cardinality of `(field_id, tag)` in $O(1)$ time with **zero heap allocations** (`TASK-048`, `F-OPT-01`).
    #[inline]
    pub fn cardinality(&self, field_id: u16, tag: InlineId) -> usize {
        let guard = self
            .map
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.get(&(field_id, tag)).map_or(0, |bm| bm.len())
    }

    /// `true` if `(field_id, tag)` holds `ord`.
    pub fn contains(&self, field_id: u16, tag: InlineId, ord: RecordOrdinal) -> bool {
        self.map
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&(field_id, tag))
            .is_some_and(|bm| bm.contains(ord))
    }

    /// Every `(field_id, tag)` key currently holding a non-empty bitmap.
    pub fn keys(&self) -> Vec<(u16, InlineId)> {
        self.map
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|(_, bm)| !bm.is_empty())
            .map(|(&k, _)| k)
            .collect()
    }

    /// Exports all `((field_id, tag), CompressedBitmap)` pairs for paged disk checkpointing (`TASK-035`).
    pub fn snapshot_all(&self) -> Vec<((u16, InlineId), CompressedBitmap)> {
        let guard = self
            .map
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.iter().map(|(&k, v)| (k, v.clone())).collect()
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

    /// Computes the union (`OR`) of multiple tags on `field_id` (`WHERE field_id IN (tags)`) under a single read lock (`TASK-055`).
    pub fn union_tags(&self, field_id: u16, tags: &[InlineId]) -> CompressedBitmap {
        if tags.is_empty() {
            return CompressedBitmap::new();
        }
        let guard = self
            .map
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut acc: Option<CompressedBitmap> = None;
        for &tag in tags {
            if let Some(bm) = guard.get(&(field_id, tag)) {
                if !bm.is_empty() {
                    acc = Some(match acc {
                        Some(prev) => prev.union(bm),
                        None => bm.clone(),
                    });
                }
            }
        }
        acc.unwrap_or_default()
    }

    /// Computes the union of all tags on `field_id` whose `InlineId` starts with `prefix` (`WHERE tag LIKE 'prefix%'`, `TASK-057`).
    pub fn union_tag_prefix(&self, field_id: u16, prefix: &str) -> CompressedBitmap {
        let guard = self
            .map
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut acc: Option<CompressedBitmap> = None;
        for (&(fid, tag), bm) in guard.iter() {
            if fid == field_id && !bm.is_empty() && tag.starts_with_str(prefix) {
                acc = Some(match acc {
                    Some(prev) => prev.union(bm),
                    None => bm.clone(),
                });
            }
        }
        acc.unwrap_or_default()
    }

    /// Computes the union of all tags on `field_id` (`WHERE tag_field IS NOT NULL`, `TASK-057`).
    pub fn all_on_field_bitmap(&self, field_id: u16) -> CompressedBitmap {
        let guard = self
            .map
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut acc: Option<CompressedBitmap> = None;
        for (&(fid, _), bm) in guard.iter() {
            if fid == field_id && !bm.is_empty() {
                acc = Some(match acc {
                    Some(prev) => prev.union(bm),
                    None => bm.clone(),
                });
            }
        }
        acc.unwrap_or_default()
    }

    /// Removes all bits matching any `(field_id, not_tag)` in `not_predicates` from `target` in-place
    /// under a single read lock with **zero heap allocations** (`TASK-055`).
    #[inline]
    pub fn exclude_from_bitmap(
        &self,
        not_predicates: &[(u16, InlineId)],
        target: &mut CompressedBitmap,
    ) {
        if not_predicates.is_empty() || target.is_empty() {
            return;
        }
        let guard = self
            .map
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for key in not_predicates {
            if let Some(neg_bm) = guard.get(key) {
                target.and_not_in_place(neg_bm);
                if target.is_empty() {
                    return;
                }
            }
        }
    }

    /// Filters `candidates` in-place against all required `predicates` under a single read lock
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

    /// Filters `candidates` in-place by excluding any ordinal present in `not_predicates` (`AND NOT`)
    /// under a single read lock (`0` heap allocations, `TASK-055`).
    #[inline]
    pub fn filter_not_in_place(
        &self,
        not_predicates: &[(u16, InlineId)],
        candidates: &mut Vec<RecordOrdinal>,
    ) {
        if not_predicates.is_empty() || candidates.is_empty() {
            return;
        }
        let guard = self
            .map
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for key in not_predicates {
            if let Some(neg_bm) = guard.get(key) {
                candidates.retain(|&ord| !neg_bm.contains(ord));
                if candidates.is_empty() {
                    return;
                }
            }
        }
    }

    /// Returns `true` if `ord` satisfies all required `must_tags`, none of `not_tags`, and all `any_tag_groups` (`TASK-055`).
    #[inline]
    pub fn matches_ordinal(
        &self,
        ord: RecordOrdinal,
        must_tags: &[(u16, InlineId)],
        not_tags: &[(u16, InlineId)],
        any_tag_groups: &[(u16, &[InlineId])],
    ) -> bool {
        if must_tags.is_empty() && not_tags.is_empty() && any_tag_groups.is_empty() {
            return true;
        }
        let guard = self
            .map
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for key in must_tags {
            let Some(bm) = guard.get(key) else {
                return false;
            };
            if !bm.contains(ord) {
                return false;
            }
        }
        for key in not_tags {
            if let Some(bm) = guard.get(key) {
                if bm.contains(ord) {
                    return false;
                }
            }
        }
        for &(field_id, group) in any_tag_groups {
            if group.is_empty() {
                return false;
            }
            let mut matched_any = false;
            for &tag in group {
                if let Some(bm) = guard.get(&(field_id, tag)) {
                    if bm.contains(ord) {
                        matched_any = true;
                        break;
                    }
                }
            }
            if !matched_any {
                return false;
            }
        }
        true
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
        let tag_deprecated = InlineId::new("deprecated").unwrap();

        for i in (0..10_000).step_by(2) {
            idx.insert(1, tag_rust, RecordOrdinal(i));
        }
        for i in (0..10_000).step_by(3) {
            idx.insert(1, tag_ai, RecordOrdinal(i));
        }
        for i in (0..10_000).step_by(12) {
            idx.insert(1, tag_deprecated, RecordOrdinal(i));
        }

        let mut both = idx.intersect_all(&[(1, tag_rust), (1, tag_ai)]);
        let vec = both.to_vec();
        // Multiples of 6 in 0..10_000 = 1667 elements
        assert_eq!(vec.len(), 1667);
        assert_eq!(vec[0], RecordOrdinal(0));
        assert_eq!(vec[1], RecordOrdinal(6));

        // Exclude multiples of 12 (`AND NOT`) -> only odd multiples of 6 (6, 18, 30, ...) remain!
        idx.exclude_from_bitmap(&[(1, tag_deprecated)], &mut both);
        assert_eq!(both.len(), 833);
        let (page, total) = both.collect_window(10, Some(5), None, false);
        assert_eq!(total, 833);
        assert_eq!(
            page,
            vec![
                RecordOrdinal(126),
                RecordOrdinal(138),
                RecordOrdinal(150),
                RecordOrdinal(162),
                RecordOrdinal(174),
            ]
        );

        // Keyset cursor seek after RecordOrdinal(138)
        let (cursor_page, rem_total) =
            both.collect_window(0, Some(3), Some(RecordOrdinal(138)), false);
        assert_eq!(rem_total, 833 - 12);
        assert_eq!(
            cursor_page,
            vec![RecordOrdinal(150), RecordOrdinal(162), RecordOrdinal(174)]
        );
    }
}

