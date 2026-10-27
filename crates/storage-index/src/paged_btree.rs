//! Disk-Backed 4 KiB Paged Range Index (`TASK-034`, `TASK-083`, `F-PIDX-01`, `RULE-MEM-01`,
//! `RULE-MEM-03`, `RULE-CONC-01`).
//!
//! A sorted run of 4 KiB leaf pages (a static single-level B+Tree) with an in-RAM separator
//! directory (`SeparatorEntry`, 24 bytes per page), plus small in-RAM deltas. RAM is
//! `O(pages + delta)`, independent of the number of indexed entries.
//!
//! ## Visibility
//! Every entry lives in exactly one of three disjoint sources:
//! - the immutable, memory-mapped **base** generation file (minus tombstones);
//! - the **frozen** delta being merged into the next base by a running checkpoint;
//! - the **hot** delta receiving writes.
//!
//! `insert` / `remove` keep the sources disjoint, so counts, bitmaps and paginated scans never
//! double count.
//!
//! ## Checkpoints
//! A checkpoint freezes the hot delta (`O(1)`, under the lock), then — without holding any lock —
//! stream-merges the base pages with the frozen delta into a new generation file using `O(page)`
//! extra memory, writes an atomic manifest naming that generation, and finally swaps it in under
//! a brief lock. Readers and writers are never blocked by checkpoint I/O.
//!
//! ## Files
//! - `<path>`: 32-byte manifest `magic | version | generation | stamp | checksum`.
//! - `<path>.<generation as 16 hex digits>`: 4 KiB header page followed by the leaf pages.
//!
//! Generation files are never overwritten (a mapped file cannot be replaced on Windows); stale
//! ones are deleted after the swap and at open.
//!
//! With encryption at rest (`TASK-088`) the leaf pages are sealed as one ChaCha20-Poly1305
//! region (see `paged_files`): verified once at open, then each page is decrypted into a stack
//! copy when read. Separator keys in RAM are plaintext, as are all in-RAM indexes.

use std::fs::{self, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use storage_codec::{checksum_payload, FileSeal};
use storage_mmap::MmapRegion;
use storage_types::wire::{read_u32_le, read_u64_le};
use storage_types::{FastHashMap, FastHashSet, RecordOrdinal, Result, StorageError};

use crate::bitmap::CompressedBitmap;
use crate::btree::{sortable_f64_to_i64, KeysetCursor, NumericRangeIndex, SortOrder};
use crate::paged_files::{
    generation_path, map_path_read_only, open_generation_cipher, publish_generation, read_manifest,
    remove_stale_generations, temp_generation_path, write_manifest, GenerationSealer, PageCipher,
    SealSettings, ENC_SECTION_LEN, ENC_SECTION_OFFSET,
};

/// Physical page size for disk-backed B+Tree leaf pages (4 KiB).
pub const BTREE_PAGE_SIZE: usize = 4096;
const BTREE_PAGE_MAGIC: u32 = 0x4254_4C50; // "BTLP"
const BTREE_FILE_MAGIC: u32 = 0x4254_4958; // "BTIX"
const BTREE_MANIFEST_MAGIC: u32 = 0x4254_4D46; // "BTMF"
const PAGE_HEADER_SIZE: usize = 32;
/// Maximum `(delta_key_i64, RecordOrdinal)` entries per 4 KiB leaf page:
/// `(4096 - 32) / 12 = 338` entries per page.
pub const MAX_ENTRIES_PER_LEAF_PAGE: usize = (BTREE_PAGE_SIZE - PAGE_HEADER_SIZE) / 12;

type Tombstones = FastHashSet<(u16, i64, RecordOrdinal)>;

/// In-memory separator entry pointing to a 4 KiB leaf page on disk (`24` bytes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeparatorEntry {
    pub field_id: u16,
    pub entry_count: u16,
    pub page_index: u32,
    pub min_key: i64,
    pub max_key: i64,
}

/// One immutable on-disk generation: the mapped file and its separator directory.
struct BaseGeneration {
    mmap: MmapRegion,
    /// Per field, sorted by `(min_key, page_index)`; pages of a field are contiguous and sorted.
    separators: FastHashMap<u16, Vec<SeparatorEntry>>,
    path: PathBuf,
    /// Set when the generation's pages are encrypted at rest.
    cipher: Option<PageCipher>,
}

impl BaseGeneration {
    /// The leaf page of `sep`: borrowed from the mapping, or (encrypted generation) decrypted
    /// into the caller's stack buffer `plain`.
    #[inline]
    fn page<'a>(&'a self, sep: &SeparatorEntry, plain: &'a mut [u8; BTREE_PAGE_SIZE]) -> Option<&'a [u8]> {
        let off = (sep.page_index as usize + 1) * BTREE_PAGE_SIZE;
        let mapped = self.mmap.as_slice().get(off..off + BTREE_PAGE_SIZE)?;
        let Some(ref cipher) = self.cipher else {
            return Some(mapped);
        };
        plain.copy_from_slice(mapped);
        cipher.decrypt_at(off - BTREE_PAGE_SIZE, plain).ok()?;
        Some(plain)
    }

    /// `true` if the base holds `(field, key, ord)` (ignoring tombstones). `O(log P + log B)`.
    fn contains(&self, field: u16, key: i64, ord: RecordOrdinal) -> bool {
        let Some(seps) = self.separators.get(&field) else {
            return false;
        };
        let start = seps.partition_point(|s| s.max_key < key);
        for sep in &seps[start..] {
            if sep.min_key > key {
                break;
            }
            let mut plain = [0u8; BTREE_PAGE_SIZE];
            let Some(page) = self.page(sep, &mut plain) else {
                continue;
            };
            let count = sep.entry_count as usize;
            let mut idx = lower_bound_in_page(page, sep.min_key, count, key);
            while idx < count && read_delta_key_at(page, sep.min_key, idx) == key {
                if read_ordinal_at(page, count, idx) == ord {
                    return true;
                }
                idx += 1;
            }
        }
        false
    }

    /// Calls `f(key, ord)` for every base entry of `field` within `[lo, hi]` in `order`, stopping
    /// early when `f` returns `false`.
    fn scan(
        &self,
        field: u16,
        lo: i64,
        hi: i64,
        order: SortOrder,
        mut f: impl FnMut(i64, RecordOrdinal) -> bool,
    ) {
        let Some(seps) = self.separators.get(&field) else {
            return;
        };
        let start = seps.partition_point(|s| s.max_key < lo);
        let end = seps.partition_point(|s| s.min_key <= hi);
        if start >= end {
            return;
        }
        let window = &seps[start..end];
        let mut visit = |sep: &SeparatorEntry, idx: usize, page: &[u8]| -> Option<bool> {
            let count = sep.entry_count as usize;
            let key = read_delta_key_at(page, sep.min_key, idx);
            if key < lo || key > hi {
                return None;
            }
            Some(f(key, read_ordinal_at(page, count, idx)))
        };
        // Decryption buffer for encrypted generations (unused for plaintext ones).
        let mut plain = [0u8; BTREE_PAGE_SIZE];
        match order {
            SortOrder::Asc => {
                for sep in window {
                    let Some(page) = self.page(sep, &mut plain) else { continue };
                    let count = sep.entry_count as usize;
                    let first = if lo <= sep.min_key {
                        0
                    } else {
                        lower_bound_in_page(page, sep.min_key, count, lo)
                    };
                    for idx in first..count {
                        match visit(sep, idx, page) {
                            Some(true) => {}
                            Some(false) => return,
                            None if read_delta_key_at(page, sep.min_key, idx) > hi => break,
                            None => {}
                        }
                    }
                }
            }
            SortOrder::Desc => {
                for sep in window.iter().rev() {
                    let Some(page) = self.page(sep, &mut plain) else { continue };
                    let count = sep.entry_count as usize;
                    let past = if hi >= sep.max_key {
                        count
                    } else {
                        lower_bound_in_page(page, sep.min_key, count, hi.saturating_add(1))
                    };
                    for idx in (0..past).rev() {
                        match visit(sep, idx, page) {
                            Some(true) => {}
                            Some(false) => return,
                            None if read_delta_key_at(page, sep.min_key, idx) < lo => break,
                            None => {}
                        }
                    }
                }
            }
        }
    }
}

/// Delta being merged into the next generation by a running checkpoint.
/// In-progress checkpoint returned by `begin_checkpoint` (holds the checkpoint lock).
pub struct PendingBTreeCheckpoint<'a> {
    _guard: std::sync::MutexGuard<'a, ()>,
    frozen: Arc<FrozenDelta>,
    base: Option<Arc<BaseGeneration>>,
    generation: u64,
}

impl core::fmt::Debug for PendingBTreeCheckpoint<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PendingBTreeCheckpoint")
            .field("generation", &self.generation)
            .finish()
    }
}

#[derive(Default)]
struct FrozenDelta {
    hot: NumericRangeIndex,
    removed: Tombstones,
}

#[derive(Default)]
struct PagedBTreeState {
    base: Option<Arc<BaseGeneration>>,
    frozen: Option<Arc<FrozenDelta>>,
    /// Entries visible in neither `base` nor `frozen` (inserted since the last freeze).
    hot: NumericRangeIndex,
    /// Tombstones for entries of `base` or `frozen.hot` removed since the last freeze.
    removed: Tombstones,
    generation: u64,
    stamp: u64,
}

impl PagedBTreeState {
    /// `true` if `e` is visible through `base` or `frozen`, ignoring `self.removed`.
    fn in_persistent(&self, field: u16, key: i64, ord: RecordOrdinal) -> bool {
        let frozen_removed = self
            .frozen
            .as_ref()
            .is_some_and(|f| f.removed.contains(&(field, key, ord)));
        let in_base = !frozen_removed
            && self
                .base
                .as_ref()
                .is_some_and(|b| b.contains(field, key, ord));
        in_base
            || self
                .frozen
                .as_ref()
                .is_some_and(|f| f.hot.contains(field, key, ord))
    }

    #[inline]
    fn base_visible(&self, field: u16, key: i64, ord: RecordOrdinal) -> bool {
        let e = (field, key, ord);
        !self.removed.contains(&e)
            && !self.frozen.as_ref().is_some_and(|f| f.removed.contains(&e))
    }

    /// Visible `(key, ord)` pairs of the two in-RAM sources within `[lo, hi]`, ascending.
    fn delta_pairs(&self, field: u16, lo: i64, hi: i64) -> Vec<(i64, RecordOrdinal)> {
        let mut out = self.hot.range_pairs(field, lo, hi);
        if let Some(ref frozen) = self.frozen {
            let frozen_pairs: Vec<(i64, RecordOrdinal)> = frozen
                .hot
                .range_pairs(field, lo, hi)
                .into_iter()
                .filter(|&(k, o)| !self.removed.contains(&(field, k, o)))
                .collect();
            if !frozen_pairs.is_empty() {
                out.extend(frozen_pairs);
                out.sort_unstable();
            }
        }
        out
    }

    /// Streams every visible entry of `field` in `[lo, hi]` in `order` (sources merged).
    fn for_each_visible(
        &self,
        field: u16,
        lo: i64,
        hi: i64,
        order: SortOrder,
        mut f: impl FnMut(i64, RecordOrdinal) -> bool,
    ) {
        let mut delta = self.delta_pairs(field, lo, hi);
        if order == SortOrder::Desc {
            delta.reverse();
        }
        let before = |a: (i64, RecordOrdinal), b: (i64, RecordOrdinal)| match order {
            SortOrder::Asc => a < b,
            SortOrder::Desc => a > b,
        };
        let mut di = 0usize;
        let mut stopped = false;
        if let Some(ref base) = self.base {
            base.scan(field, lo, hi, order, |k, o| {
                if !self.base_visible(field, k, o) {
                    return true;
                }
                while di < delta.len() && before(delta[di], (k, o)) {
                    if !f(delta[di].0, delta[di].1) {
                        stopped = true;
                        return false;
                    }
                    di += 1;
                }
                if !f(k, o) {
                    stopped = true;
                    return false;
                }
                true
            });
        }
        if stopped {
            return;
        }
        for &(k, o) in &delta[di..] {
            if !f(k, o) {
                return;
            }
        }
    }
}

/// Disk-backed 4 KiB Paged B+Tree Range Index (`TASK-034`, `TASK-083`, `F-PIDX-01`).
pub struct PagedBTreeIndex {
    path: PathBuf,
    state: RwLock<PagedBTreeState>,
    /// Protection of generation files written from now on (`TASK-088`).
    seal: RwLock<SealSettings>,
    /// Serializes checkpoints (never held together with `state` across I/O).
    checkpoint_lock: Mutex<()>,
}

impl core::fmt::Debug for PagedBTreeIndex {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PagedBTreeIndex")
            .field("path", &self.path)
            .field("separator_ram_bytes", &self.separator_memory_bytes())
            .finish()
    }
}

impl PagedBTreeIndex {
    /// Opens (or creates) the index whose manifest is `path`. Corrupted files are an error so
    /// the caller can decide to rebuild; stale generation files are removed.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        Self::open_sealed(path, &FileSeal::PLAIN)
    }

    /// [`Self::open`] with encryption at rest (`TASK-088`): an encrypted generation is verified
    /// with its key from `seal`; a plaintext one is refused when `seal` forbids plaintext. New
    /// generations are written under `seal` (see [`Self::set_file_seal`]).
    pub fn open_sealed(path: impl Into<PathBuf>, seal: &FileSeal<'_>) -> Result<Self> {
        let path = path.into();
        let mut state = PagedBTreeState::default();
        if let Some((generation, stamp)) = read_manifest(&path, BTREE_MANIFEST_MAGIC)? {
            let data_path = generation_path(&path, generation);
            let mmap = map_path_read_only(&data_path)?;
            let header = mmap.as_slice().get(..BTREE_PAGE_SIZE).unwrap_or_default();
            let enc = open_generation_cipher(header, seal)?;
            let mut separators = FastHashMap::default();
            load_separators_from_mmap(
                &mmap,
                &mut separators,
                enc.as_ref().map(|(cipher, tag)| (cipher, tag, data_path.as_path())),
            )?;
            state.base = Some(Arc::new(BaseGeneration {
                mmap,
                separators,
                path: data_path,
                cipher: enc.map(|(cipher, _)| cipher),
            }));
            state.generation = generation;
            state.stamp = stamp;
        }
        remove_stale_generations(&path, state.base.as_ref().map(|b| b.path.as_path()));
        Ok(Self {
            path,
            state: RwLock::new(state),
            seal: RwLock::new(SealSettings::from_seal(seal)),
            checkpoint_lock: Mutex::new(()),
        })
    }

    /// Protection for generation files written from now on (the next checkpoint rewrites every
    /// page under it, so a rotation or enable / disable reaches the whole index).
    pub fn set_file_seal(&self, seal: &FileSeal<'_>) {
        *self.seal.write().unwrap_or_else(|poisoned| poisoned.into_inner()) = SealSettings::from_seal(seal);
    }

    #[inline]
    fn read(&self) -> std::sync::RwLockReadGuard<'_, PagedBTreeState> {
        self.state.read().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[inline]
    fn write(&self) -> std::sync::RwLockWriteGuard<'_, PagedBTreeState> {
        self.state.write().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Caller-defined stamp recorded by the last checkpoint (`0` if never checkpointed). The
    /// engine stores its checkpoint identity here to prove the index matches the records.
    pub fn stamp(&self) -> u64 {
        self.read().stamp
    }

    /// Returns the exact RAM footprint (in bytes) of the in-memory page separator directory.
    pub fn separator_memory_bytes(&self) -> usize {
        self.read().base.as_ref().map_or(0, |b| {
            b.separators
                .values()
                .map(|v| v.len() * core::mem::size_of::<SeparatorEntry>())
                .sum()
        })
    }

    /// Returns the total number of 4 KiB leaf pages currently mapped on disk.
    pub fn page_count(&self) -> usize {
        self.read()
            .base
            .as_ref()
            .map_or(0, |b| b.separators.values().map(Vec::len).sum())
    }

    /// Inserts `(field_id, value, ord)` (idempotent).
    pub fn insert(&self, field_id: u16, value: i64, ord: RecordOrdinal) {
        let mut st = self.write();
        if st.removed.remove(&(field_id, value, ord)) {
            return; // visible again through base / frozen
        }
        if st.in_persistent(field_id, value, ord) {
            return; // already visible
        }
        st.hot.insert(field_id, value, ord);
    }

    /// Inserts an `f64` value using IEEE-754 order-preserving encoding.
    #[inline]
    pub fn insert_f64(&self, field_id: u16, value: f64, ord: RecordOrdinal) {
        self.insert(field_id, sortable_f64_to_i64(value), ord);
    }

    /// Removes `(field_id, value, ord)` (no-op if absent).
    pub fn remove(&self, field_id: u16, value: i64, ord: RecordOrdinal) {
        let mut st = self.write();
        if st.hot.contains(field_id, value, ord) {
            st.hot.remove(field_id, value, ord);
        } else if st.in_persistent(field_id, value, ord) {
            st.removed.insert((field_id, value, ord));
        }
    }

    /// Returns a `CompressedBitmap` of every ordinal matching `range` on `field_id`.
    pub fn range_bitmap<R: RangeBounds<i64>>(&self, field_id: u16, range: R) -> CompressedBitmap {
        let (lo, hi) = normalize_bounds(&range);
        let mut bm = CompressedBitmap::new();
        if lo <= hi {
            self.read()
                .for_each_visible(field_id, lo, hi, SortOrder::Asc, |_, ord| {
                    bm.insert(ord);
                    true
                });
        }
        bm
    }

    /// Queries an inclusive `[min, max]` `f64` range on `field_id`.
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

    /// Ordinals with `field_id == value`. `O(log P + log B + matches)`.
    #[inline]
    pub fn eq_bitmap(&self, field_id: u16, value: i64) -> CompressedBitmap {
        self.range_bitmap(field_id, value..=value)
    }

    /// Ordinals with `field_id IN values`.
    pub fn in_values_bitmap(&self, field_id: u16, values: &[i64]) -> CompressedBitmap {
        let st = self.read();
        let mut bm = CompressedBitmap::new();
        for &v in values {
            st.for_each_visible(field_id, v, v, SortOrder::Asc, |_, ord| {
                bm.insert(ord);
                true
            });
        }
        bm
    }

    /// Ordinals with any value on `field_id` (`IS NOT NULL`).
    #[inline]
    pub fn all_on_field_bitmap(&self, field_id: u16) -> CompressedBitmap {
        self.range_bitmap(field_id, ..)
    }

    /// Number of entries in `range` on `field_id` with zero heap allocations for the base pages.
    pub fn count_in_range<R: RangeBounds<i64>>(&self, field_id: u16, range: R) -> usize {
        let (lo, hi) = normalize_bounds(&range);
        let mut count = 0usize;
        if lo <= hi {
            self.read()
                .for_each_visible(field_id, lo, hi, SortOrder::Asc, |_, _| {
                    count += 1;
                    true
                });
        }
        count
    }

    /// Inclusive-range alias of [`Self::count_in_range`].
    #[inline]
    pub fn count_in_range_zero_alloc(&self, field_id: u16, lo: i64, hi: i64) -> usize {
        if lo > hi {
            return 0;
        }
        self.count_in_range(field_id, lo..=hi)
    }

    /// Paginated, direction-aware, keyset-cursor-seekable range scan with exactly the semantics of
    /// [`NumericRangeIndex::range_collect_paginated`]. Streams the base pages, so a page of results
    /// costs `O(offset + limit)` plus the (small) in-RAM delta of the range.
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
        if min_inclusive > max_inclusive {
            return (Vec::new(), 0);
        }
        let (lo, hi) = match (order, cursor.and_then(|c| c.sort_key)) {
            (SortOrder::Asc, Some(ck)) if ck > min_inclusive => (ck, max_inclusive),
            (SortOrder::Desc, Some(ck)) if ck < max_inclusive => (min_inclusive, ck),
            _ => (min_inclusive, max_inclusive),
        };
        if lo > hi {
            return (Vec::new(), 0);
        }
        let max_items = limit.unwrap_or(usize::MAX);
        let mut out = Vec::with_capacity(max_items.min(256));
        let mut total_matched = 0usize;
        let mut skipped = 0usize;
        self.read().for_each_visible(field_id, lo, hi, order, |k, ord| {
            if let Some(cur) = cursor {
                let past_cursor = match (order, cur.sort_key) {
                    (SortOrder::Asc, Some(ck)) => !(k == ck && ord <= cur.ordinal),
                    (SortOrder::Desc, Some(ck)) => !(k == ck && ord >= cur.ordinal),
                    (SortOrder::Asc, None) => ord > cur.ordinal,
                    (SortOrder::Desc, None) => ord < cur.ordinal,
                };
                if !past_cursor {
                    return true;
                }
            }
            if !filter_fn(ord) {
                return true;
            }
            total_matched += 1;
            if skipped < offset {
                skipped += 1;
            } else if out.len() < max_items {
                out.push((k, ord));
            }
            true
        });
        (out, total_matched)
    }

    /// Ordinals matching `range` on `field_id` in ascending key order (at most `limit`).
    pub fn range_collect<R: RangeBounds<i64>>(
        &self,
        field_id: u16,
        range: R,
        limit: Option<usize>,
    ) -> Vec<RecordOrdinal> {
        let (lo, hi) = normalize_bounds(&range);
        let max = limit.unwrap_or(usize::MAX);
        let mut out = Vec::with_capacity(max.min(512));
        if lo <= hi && max > 0 {
            self.read().for_each_visible(field_id, lo, hi, SortOrder::Asc, |_, ord| {
                out.push(ord);
                out.len() < max
            });
        }
        out
    }

    /// Checkpoints with the current stamp unchanged. Returns the number of leaf pages written.
    #[inline]
    pub fn checkpoint_to_disk(&self) -> Result<usize> {
        let stamp = self.stamp();
        self.checkpoint_with_stamp(stamp)
    }

    /// Merges the base and the delta into a new generation file tagged with `stamp`.
    ///
    /// Only the freeze and the final swap take the state lock (`O(1)` each); the stream merge and
    /// all I/O run without it, so concurrent reads and writes proceed. Extra memory is `O(page)`
    /// plus the separator directory being built. On error the frozen delta is folded back, so no
    /// write is lost and a later checkpoint can retry.
    pub fn checkpoint_with_stamp(&self, stamp: u64) -> Result<usize> {
        let pending = self.begin_checkpoint();
        self.finish_checkpoint(pending, stamp)
    }

    /// Freezes the current delta (`O(1)`, brief state lock). Pair with
    /// [`Self::finish_checkpoint`]; the returned token serializes checkpoints. Callers that need a
    /// consistent cut across several structures freeze them all while their writers are paused.
    pub fn begin_checkpoint(&self) -> PendingBTreeCheckpoint<'_> {
        let guard = self
            .checkpoint_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut st = self.write();
        let frozen = Arc::new(FrozenDelta {
            hot: core::mem::take(&mut st.hot),
            removed: core::mem::take(&mut st.removed),
        });
        st.frozen = Some(Arc::clone(&frozen));
        PendingBTreeCheckpoint {
            _guard: guard,
            frozen,
            base: st.base.clone(),
            generation: st.generation + 1,
        }
    }

    /// Writes and publishes the generation frozen by [`Self::begin_checkpoint`] (no state lock is
    /// held during I/O). Returns the number of pages written. On error the frozen delta is
    /// folded back, so no write is lost.
    pub fn finish_checkpoint(&self, pending: PendingBTreeCheckpoint<'_>, stamp: u64) -> Result<usize> {
        let PendingBTreeCheckpoint {
            _guard,
            frozen,
            base,
            generation,
        } = pending;
        match self.write_generation(&frozen, base.as_deref(), generation, stamp) {
            Ok((new_base, written)) => {
                let old = {
                    let mut st = self.write();
                    st.frozen = None;
                    st.generation = generation;
                    st.stamp = stamp;
                    st.base.replace(Arc::new(new_base))
                };
                drop(base);
                if let Some(old) = old {
                    let old_path = old.path.clone();
                    drop(old);
                    // Best effort: a leftover generation is removed at the next open.
                    let _ = fs::remove_file(old_path);
                }
                Ok(written)
            }
            Err(e) => {
                self.unfreeze(&frozen);
                Err(e)
            }
        }
    }

    /// Drops every entry and publishes an empty generation tagged with `stamp` (used before a
    /// rebuild from the records).
    pub fn clear_with_stamp(&self, stamp: u64) -> Result<()> {
        let _one_at_a_time = self
            .checkpoint_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (old, generation) = {
            let mut st = self.write();
            st.hot = Default::default();
            st.removed = Default::default();
            st.frozen = None;
            (st.base.take(), st.generation + 1)
        };
        let (new_base, _) = self.write_generation(&FrozenDelta::default(), None, generation, stamp)?;
        {
            let mut st = self.write();
            st.generation = generation;
            st.stamp = stamp;
            st.base = Some(Arc::new(new_base));
        }
        if let Some(old) = old {
            let old_path = old.path.clone();
            drop(old);
            let _ = fs::remove_file(old_path);
        }
        Ok(())
    }

    /// Folds a frozen delta back into the live state after a failed checkpoint, preserving the
    /// disjoint-sources invariant.
    fn unfreeze(&self, frozen: &FrozenDelta) {
        let mut st = self.write();
        st.frozen = None;
        for (field, pairs) in frozen.hot.snapshot_all() {
            for (k, o) in pairs {
                if !st.removed.remove(&(field, k, o)) {
                    st.hot.insert(field, k, o);
                }
            }
        }
        for &(field, k, o) in &frozen.removed {
            if st.hot.contains(field, k, o) {
                // Re-inserted after the freeze while hidden in the base: visible via base again.
                st.hot.remove(field, k, o);
            } else {
                st.removed.insert((field, k, o));
            }
        }
    }

    /// Streams `(base - frozen.removed) ∪ frozen.hot` into generation file `generation`, then
    /// writes the manifest. Returns the mapped new generation and its page count.
    fn write_generation(
        &self,
        frozen: &FrozenDelta,
        base: Option<&BaseGeneration>,
        generation: u64,
        stamp: u64,
    ) -> Result<(BaseGeneration, usize)> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|e| StorageError::io("create paged btree dir", e))?;
        }
        let data_path = generation_path(&self.path, generation);
        let tmp_path = temp_generation_path(&self.path, generation);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(|e| StorageError::io("create btree generation file", e))?;
        let mut writer = BufWriter::with_capacity(16 * BTREE_PAGE_SIZE, file);
        writer
            .write_all(&[0u8; BTREE_PAGE_SIZE])
            .map_err(|e| StorageError::io("write btree header placeholder", e))?;
        let settings = self
            .seal
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let mut sealer = GenerationSealer::begin(&settings.as_seal(), &data_path);

        let mut fields: Vec<u16> = base
            .map(|b| b.separators.keys().copied().collect())
            .unwrap_or_default();
        let hot = frozen.hot.snapshot_all();
        fields.extend(hot.iter().map(|(f, _)| *f));
        fields.sort_unstable();
        fields.dedup();

        let mut separators: FastHashMap<u16, Vec<SeparatorEntry>> = FastHashMap::default();
        let mut page_buf = vec![0u8; BTREE_PAGE_SIZE];
        let mut pending: Vec<(i64, RecordOrdinal)> = Vec::with_capacity(MAX_ENTRIES_PER_LEAF_PAGE);
        let mut page_index = 0u32;
        let mut io_err: Option<std::io::Error> = None;

        for field in fields {
            let mut delta: Vec<(i64, RecordOrdinal)> = hot
                .iter()
                .find(|(f, _)| *f == field)
                .map(|(_, pairs)| pairs.clone())
                .unwrap_or_default();
            delta.sort_unstable();
            let mut emit = |k: i64, o: RecordOrdinal, pending: &mut Vec<(i64, RecordOrdinal)>| {
                pending.push((k, o));
                if pending.len() == MAX_ENTRIES_PER_LEAF_PAGE && io_err.is_none() {
                    let sep = encode_leaf_page(&mut page_buf, field, page_index, pending);
                    if let Err(e) = seal_and_write(&mut sealer, &mut writer, &mut page_buf) {
                        io_err = Some(e);
                    }
                    separators.entry(field).or_default().push(sep);
                    page_index += 1;
                    pending.clear();
                }
            };
            let mut di = 0usize;
            if let Some(b) = base {
                b.scan(field, i64::MIN, i64::MAX, SortOrder::Asc, |k, o| {
                    if frozen.removed.contains(&(field, k, o)) {
                        return true;
                    }
                    while di < delta.len() && delta[di] < (k, o) {
                        emit(delta[di].0, delta[di].1, &mut pending);
                        di += 1;
                    }
                    emit(k, o, &mut pending);
                    true
                });
            }
            for &(k, o) in &delta[di..] {
                emit(k, o, &mut pending);
            }
            if !pending.is_empty() && io_err.is_none() {
                let sep = encode_leaf_page(&mut page_buf, field, page_index, &pending);
                if let Err(e) = seal_and_write(&mut sealer, &mut writer, &mut page_buf) {
                    io_err = Some(e);
                }
                separators.entry(field).or_default().push(sep);
                page_index += 1;
                pending.clear();
            }
            if let Some(e) = io_err.take() {
                return Err(StorageError::io("write btree leaf page", e));
            }
        }

        let mut file = writer
            .into_inner()
            .map_err(|e| StorageError::io("flush btree generation file", e.into_error()))?;
        let mut header = [0u8; ENC_SECTION_OFFSET + ENC_SECTION_LEN];
        header[0..4].copy_from_slice(&BTREE_FILE_MAGIC.to_le_bytes());
        header[4..8].copy_from_slice(&page_index.to_le_bytes());
        let hdr_crc = checksum_payload(&header[0..8]);
        header[8..16].copy_from_slice(&hdr_crc.to_le_bytes());
        let cipher = sealer.map(|s| {
            let (section, cipher) = s.finish();
            header[ENC_SECTION_OFFSET..].copy_from_slice(&section);
            cipher
        });
        file.seek(SeekFrom::Start(0))
            .and_then(|_| file.write_all(&header))
            .and_then(|()| file.sync_all())
            .map_err(|e| StorageError::io("finalize btree generation file", e))?;
        drop(file);
        publish_generation(&tmp_path, &data_path)?;
        // The manifest's atomic replace also fsyncs the directory, covering the rename above.
        write_manifest(&self.path, BTREE_MANIFEST_MAGIC, generation, stamp)?;

        let mmap = map_path_read_only(&data_path)?;
        Ok((
            BaseGeneration {
                mmap,
                separators,
                path: data_path,
                cipher,
            },
            page_index as usize,
        ))
    }
}

/// Encrypts `page` in place when the generation is sealed, then writes it.
fn seal_and_write(
    sealer: &mut Option<GenerationSealer>,
    writer: &mut impl Write,
    page: &mut [u8],
) -> std::io::Result<()> {
    if let Some(s) = sealer {
        s.seal(page)?;
    }
    writer.write_all(page)
}

fn normalize_bounds<R: RangeBounds<i64>>(range: &R) -> (i64, i64) {
    let lo = match range.start_bound() {
        Bound::Included(&x) => x,
        Bound::Excluded(&x) => x.saturating_add(1),
        Bound::Unbounded => i64::MIN,
    };
    let hi = match range.end_bound() {
        Bound::Included(&x) => x,
        Bound::Excluded(&x) => x.saturating_sub(1),
        Bound::Unbounded => i64::MAX,
    };
    (lo, hi)
}

/// Checksum of a leaf page covering its header (`[0..24]`: magic, field id, entry count, min / max
/// key) and its body (`[32..]`), i.e. every byte except the stored checksum at `[24..32]`, so a
/// flipped bit in a separator key can never silently change range results.
#[inline]
pub(crate) fn page_crc(page: &[u8]) -> u64 {
    let header = page.get(..24).unwrap_or_default();
    let body = page.get(PAGE_HEADER_SIZE..).unwrap_or_default();
    checksum_payload(body) ^ checksum_payload(header).rotate_left(29)
}

/// Encodes a 4 KiB leaf page (overwriting all of `page`) with delta-encoded keys
/// (`delta = key - min_key`) and `RecordOrdinal`s. `entries` must be non-empty, sorted, and at
/// most [`MAX_ENTRIES_PER_LEAF_PAGE`] long.
///
/// Layout of 4 KiB page:
/// - `[0..4]`: `BTREE_PAGE_MAGIC` (`u32`)
/// - `[4..6]`: `field_id` (`u16`)
/// - `[6..8]`: `entry_count` (`u16`)
/// - `[8..16]`: `min_key` (`i64`)
/// - `[16..24]`: `max_key` (`i64`)
/// - `[24..32]`: `crc64` (`u64`) over the whole page except this field (see [`page_crc`])
/// - `[32..32 + count * 8]`: sorted array of `delta_key: i64` (`key.wrapping_sub(min_key)`)
/// - `[32 + count * 8 .. 32 + count * 12]`: parallel array of `RecordOrdinal` (`u32`)
fn encode_leaf_page(
    page: &mut [u8],
    field_id: u16,
    page_index: u32,
    entries: &[(i64, RecordOrdinal)],
) -> SeparatorEntry {
    page.fill(0);
    let count = entries.len().min(MAX_ENTRIES_PER_LEAF_PAGE);
    let entries = &entries[..count];
    let min_key = entries.first().map_or(0, |e| e.0);
    let max_key = entries.last().map_or(0, |e| e.0);

    page[0..4].copy_from_slice(&BTREE_PAGE_MAGIC.to_le_bytes());
    page[4..6].copy_from_slice(&field_id.to_le_bytes());
    page[6..8].copy_from_slice(&(count as u16).to_le_bytes());
    page[8..16].copy_from_slice(&min_key.to_le_bytes());
    page[16..24].copy_from_slice(&max_key.to_le_bytes());

    let keys_off = PAGE_HEADER_SIZE;
    let ords_off = PAGE_HEADER_SIZE + count * 8;
    for (i, &(k, ord)) in entries.iter().enumerate() {
        let delta = k.wrapping_sub(min_key);
        let k_pos = keys_off + i * 8;
        page[k_pos..k_pos + 8].copy_from_slice(&delta.to_le_bytes());
        let o_pos = ords_off + i * 4;
        page[o_pos..o_pos + 4].copy_from_slice(&ord.0.to_le_bytes());
    }

    let crc = page_crc(page);
    page[24..32].copy_from_slice(&crc.to_le_bytes());

    SeparatorEntry {
        field_id,
        entry_count: count as u16,
        page_index,
        min_key,
        max_key,
    }
}

/// Reads key `idx` of a validated page (`idx < entry_count <= 338` keeps every offset in bounds).
#[inline]
fn read_delta_key_at(page: &[u8], min_key: i64, idx: usize) -> i64 {
    let pos = PAGE_HEADER_SIZE + idx * 8;
    min_key.wrapping_add(read_u64_le(page, pos) as i64)
}

#[inline]
fn read_ordinal_at(page: &[u8], count: usize, idx: usize) -> RecordOrdinal {
    RecordOrdinal(read_u32_le(page, PAGE_HEADER_SIZE + count * 8 + idx * 4))
}

/// Binary searches `[0..count)` inside a 4 KiB leaf page for the first index where `key >= target`.
#[inline]
fn lower_bound_in_page(page: &[u8], min_key: i64, count: usize, target: i64) -> usize {
    let mut left = 0usize;
    let mut right = count;
    while left < right {
        let mid = left + (right - left) / 2;
        if read_delta_key_at(page, min_key, mid) < target {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left
}

/// Validates every page once at open (`RULE-SAFE-04`) and builds the separator directory. For
/// an encrypted generation (`enc` = cipher, tag, file path) each page is decrypted into a copy
/// for validation and the region's tag is verified over the ciphertext.
fn load_separators_from_mmap(
    mmap: &MmapRegion,
    out: &mut FastHashMap<u16, Vec<SeparatorEntry>>,
    enc: Option<(&PageCipher, &[u8; 16], &Path)>,
) -> Result<()> {
    let mut verifier = enc.map(|(cipher, _, path)| cipher.verifier(path));
    let mut plain = [0u8; BTREE_PAGE_SIZE];
    let slice = mmap.as_slice();
    if slice.len() < BTREE_PAGE_SIZE {
        return Err(StorageError::corrupted_header("btree.idx smaller than 4 KiB"));
    }
    if read_u32_le(slice, 0) != BTREE_FILE_MAGIC {
        return Err(StorageError::corrupted_header("btree.idx invalid file magic"));
    }
    let page_count = read_u32_le(slice, 4);
    let expected_crc = read_u64_le(slice, 8);
    let actual_crc = checksum_payload(&slice[0..8]);
    if actual_crc != expected_crc {
        return Err(StorageError::checksum_mismatch(expected_crc, actual_crc));
    }

    for page_idx in 0..page_count {
        let off = (page_idx as usize + 1) * BTREE_PAGE_SIZE;
        let raw = slice
            .get(off..off + BTREE_PAGE_SIZE)
            .ok_or_else(|| StorageError::corrupted_header("btree.idx truncated leaf page"))?;
        let page: &[u8] = match (enc, verifier.as_mut()) {
            (Some((cipher, _, _)), Some(v)) => {
                v.absorb_ciphertext(raw)?;
                plain.copy_from_slice(raw);
                cipher.decrypt_at(off - BTREE_PAGE_SIZE, &mut plain)?;
                &plain
            }
            _ => raw,
        };
        if read_u32_le(page, 0) != BTREE_PAGE_MAGIC {
            return Err(StorageError::corrupted_header("btree.idx invalid leaf magic"));
        }
        let field_id = u16::from_le_bytes([page[4], page[5]]);
        let entry_count = u16::from_le_bytes([page[6], page[7]]);
        if entry_count == 0 || entry_count as usize > MAX_ENTRIES_PER_LEAF_PAGE {
            return Err(StorageError::corrupted_header(
                "btree.idx leaf entry count out of range",
            ));
        }
        if page_crc(page) != read_u64_le(page, 24) {
            return Err(StorageError::corrupted_header("btree.idx leaf page checksum mismatch"));
        }
        let min_key = read_u64_le(page, 8) as i64;
        let max_key = read_u64_le(page, 16) as i64;
        // Keys must be sorted inside the page and bounded by the header (scans rely on it).
        let count = entry_count as usize;
        let mut prev: Option<(i64, RecordOrdinal)> = None;
        for idx in 0..count {
            let e = (read_delta_key_at(page, min_key, idx), read_ordinal_at(page, count, idx));
            if e.0 < min_key || e.0 > max_key || prev.is_some_and(|p| p >= e) {
                return Err(StorageError::corrupted_header("btree.idx leaf entries out of order"));
            }
            prev = Some(e);
        }
        let seps = out.entry(field_id).or_default();
        if seps.last().is_some_and(|last: &SeparatorEntry| {
            last.max_key > min_key || last.page_index + 1 != page_idx
        }) {
            return Err(StorageError::corrupted_header(
                "btree.idx pages of a field are not contiguous and sorted",
            ));
        }
        seps.push(SeparatorEntry {
            field_id,
            entry_count,
            page_index: page_idx,
            min_key,
            max_key,
        });
    }
    if let (Some(v), Some((_, tag, _))) = (verifier, enc) {
        v.verify(tag)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sealed_generations_roundtrip_refuse_downgrade_and_rotate() {
        use storage_codec::{EncryptionKey, KeyRing};
        let path = temp_path("sealed");
        let ring = KeyRing::new().with_key(1, EncryptionKey::from_bytes([1; 32]));
        let seal = FileSeal { keys: Some(&ring), active_key: Some(1), accept_plain: false };
        let marker = 0x5EC2_E7AB_CDEF_0123_i64;
        {
            let idx = PagedBTreeIndex::open_sealed(&path, &seal).unwrap();
            for i in 0..2000u32 {
                idx.insert(3, marker + i64::from(i % 700), RecordOrdinal(i));
            }
            idx.checkpoint_with_stamp(5).unwrap();
            assert_eq!(idx.count_in_range(3, marker..marker + 100), 2000 / 700 * 100 + 100);
        }
        let generation = fs::read_dir(path.parent().unwrap())
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| p.to_string_lossy().contains("btree.idx."))
            .unwrap();
        let bytes = fs::read(&generation).unwrap();
        assert_eq!(&bytes[128..132], b"PENC");
        assert!(!bytes.windows(8).any(|w| w == marker.to_le_bytes()), "plaintext key on disk");

        // Reopen with the key: verified, pages decrypted on access.
        let idx = PagedBTreeIndex::open_sealed(&path, &seal).unwrap();
        assert_eq!(idx.count_in_range(3, marker..marker + 100), 2000 / 700 * 100 + 100);
        drop(idx);
        // No key, or a plaintext-only reader: refused.
        assert!(PagedBTreeIndex::open(&path).is_err());
        // Rotate to plaintext: the next checkpoint rewrites every page in the clear.
        let idx = PagedBTreeIndex::open_sealed(&path, &seal).unwrap();
        idx.set_file_seal(&FileSeal { active_key: None, accept_plain: true, ..seal });
        idx.insert(3, marker - 1, RecordOrdinal(5000));
        idx.checkpoint_with_stamp(6).unwrap();
        drop(idx);
        let plain = PagedBTreeIndex::open(&path).unwrap();
        assert_eq!(plain.count_in_range(3, marker - 1..marker + 100), 2000 / 700 * 100 + 101);
        drop(plain);
        // ... which a strict reader now refuses (no downgrade).
        assert!(PagedBTreeIndex::open_sealed(&path, &seal).is_err());
        let _ = fs::remove_dir_all(path.parent().unwrap());
    }

    fn temp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("storine_paged_btree_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir.join("btree.idx")
    }

    #[test]
    fn test_paged_btree_4kb_checkpoint_and_zero_alloc_range_scan() -> Result<()> {
        let path = temp_path("basic");
        let btree = PagedBTreeIndex::open(&path)?;
        // 2,000 entries across multiple 4 KiB pages (338 entries per page -> 6 pages)
        for i in 0..2_000i64 {
            btree.insert(1, 10_000 + i * 10, RecordOrdinal(i as u32));
        }
        let pages = btree.checkpoint_with_stamp(7)?;
        assert_eq!(pages, 6);
        assert!(btree.separator_memory_bytes() <= 6 * 24);

        let reopened = PagedBTreeIndex::open(&path)?;
        assert_eq!(reopened.page_count(), 6);
        assert_eq!(reopened.stamp(), 7);
        assert_eq!(reopened.count_in_range_zero_alloc(1, 15_000, 15_990), 100);
        let bm = reopened.range_bitmap(1, 15_000..=15_090);
        assert_eq!(bm.len(), 10);
        assert!(bm.contains(RecordOrdinal(500)));
        assert!(bm.contains(RecordOrdinal(509)));
        Ok(())
    }

    /// The paged index answers every query exactly like the in-memory index across inserts,
    /// removes, re-inserts, duplicate keys and multiple checkpoints (including one interleaved
    /// with writes between freeze and swap is exercised by the engine tests).
    #[test]
    fn test_paged_btree_matches_in_memory_index_through_checkpoints() -> Result<()> {
        use storage_types::testing::SweepRng;
        let path = temp_path("parity");
        let paged = PagedBTreeIndex::open(&path)?;
        let mem = NumericRangeIndex::new();
        let mut rng = SweepRng::new(5);
        let check = |paged: &PagedBTreeIndex, mem: &NumericRangeIndex, rng: &mut SweepRng| {
            for field in 1..=2u16 {
                let a = rng.below(400) as i64 - 50;
                let b = a + rng.below(200) as i64;
                assert_eq!(paged.range_bitmap(field, a..=b).to_vec(), mem.range_bitmap(field, a..=b).to_vec());
                assert_eq!(paged.count_in_range(field, a..=b), mem.count_in_range(field, a..=b));
                assert_eq!(paged.eq_bitmap(field, a).to_vec(), mem.eq_bitmap(field, a).to_vec());
                assert_eq!(
                    paged.in_values_bitmap(field, &[a, b, a + 3]).to_vec(),
                    mem.in_values_bitmap(field, &[a, b, a + 3]).to_vec()
                );
                assert_eq!(paged.all_on_field_bitmap(field).to_vec(), mem.all_on_field_bitmap(field).to_vec());
                assert_eq!(paged.range_collect(field, a..=b, Some(17)), mem.range_collect(field, a..=b, Some(17)));
                for order in [SortOrder::Asc, SortOrder::Desc] {
                    let cursor = (rng.below(3) == 0).then(|| {
                        KeysetCursor::from_key_and_ordinal(a + 20, RecordOrdinal(rng.below(300) as u32))
                    });
                    let off = rng.below(5);
                    let lim = Some(1 + rng.below(40));
                    let even = |o: RecordOrdinal| o.0 % 2 == 0;
                    assert_eq!(
                        paged.range_collect_paginated(field, a, b, order, cursor, off, lim, even),
                        mem.range_collect_paginated(field, a, b, order, cursor, off, lim, even),
                        "field {field} [{a}, {b}] {order:?} cursor {cursor:?}"
                    );
                }
            }
        };
        for round in 0..6 {
            for _ in 0..1_500 {
                let field = 1 + rng.below(2) as u16;
                let key = rng.below(300) as i64; // many duplicate keys
                let ord = RecordOrdinal(rng.below(400) as u32);
                if rng.below(3) == 0 {
                    paged.remove(field, key, ord);
                    mem.remove(field, key, ord);
                } else {
                    paged.insert(field, key, ord);
                    mem.insert(field, key, ord);
                }
            }
            check(&paged, &mem, &mut rng);
            if round % 2 == 1 {
                paged.checkpoint_with_stamp(round)?;
                check(&paged, &mem, &mut rng);
            }
        }
        drop(paged);
        let reopened = PagedBTreeIndex::open(&path)?;
        check(&reopened, &mem, &mut rng);
        Ok(())
    }

    #[test]
    fn test_paged_btree_checkpoint_never_blocks_writers_and_keeps_concurrent_writes() -> Result<()> {
        use std::sync::atomic::{AtomicBool, Ordering};
        let path = temp_path("concurrent");
        let idx = Arc::new(PagedBTreeIndex::open(&path)?);
        for i in 0..50_000u32 {
            idx.insert(1, i64::from(i), RecordOrdinal(i));
        }
        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let idx = Arc::clone(&idx);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut n = 0u32;
                while !stop.load(Ordering::Relaxed) && n < 200_000 {
                    idx.insert(2, i64::from(n), RecordOrdinal(n));
                    idx.remove(1, i64::from(n), RecordOrdinal(n));
                    n += 1;
                }
                n
            })
        };
        for stamp in 1..=4 {
            idx.checkpoint_with_stamp(stamp)?;
        }
        stop.store(true, Ordering::Relaxed);
        let n = writer.join().unwrap_or(0);
        assert_eq!(idx.count_in_range(2, ..), n as usize);
        assert_eq!(idx.count_in_range(1, ..), 50_000usize.saturating_sub(n as usize));
        idx.checkpoint_with_stamp(5)?;
        drop(idx);
        let reopened = PagedBTreeIndex::open(&path)?;
        assert_eq!(reopened.count_in_range(2, ..), n as usize);
        // Exactly one generation file remains.
        let dir = path.parent().unwrap();
        let gens = fs::read_dir(dir)
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().starts_with("btree.idx."))
            .count();
        assert_eq!(gens, 1);
        Ok(())
    }
}
