//! Disk-Backed 4 KiB Paged B+Tree Range Index (`TASK-034`, `F-INDX-06`, `RULE-MEM-01`, `RULE-DATA-03`).
//!
//! Stores sorted `(key: i64, RecordOrdinal)` pairs in 4096-byte (4 KiB) disk pages mapped via
//! `MmapRegion`. Only a compact separator directory (`SeparatorEntry`: 24 bytes per 4 KiB page)
//! resides in RAM for cold pages, keeping RAM usage `< 2.5 MiB` for 10M+ indexed entries while
//! executing $O(\log P + \log B)$ binary searches directly over OS page-cached `mmap` slices.

use std::collections::BTreeMap;
use std::fs;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use storage_codec::checksum_payload;
use storage_mmap::{atomic_write_replace, MmapRegion};
use storage_types::{FastHashMap, FastHashSet, RecordOrdinal, Result, StorageError};

use crate::bitmap::CompressedBitmap;
use crate::btree::{sortable_f64_to_i64, NumericRangeIndex};

/// Physical page size for disk-backed B+Tree leaf pages (4 KiB).
pub const BTREE_PAGE_SIZE: usize = 4096;
const BTREE_PAGE_MAGIC: u32 = 0x4254_4C50; // "BTLP"
const BTREE_FILE_MAGIC: u32 = 0x4254_4958; // "BTIX"
const PAGE_HEADER_SIZE: usize = 32;
/// Maximum `(delta_key_i64, RecordOrdinal)` entries per 4 KiB leaf page:
/// `(4096 - 32) / 12 = 338` entries per page.
pub const MAX_ENTRIES_PER_LEAF_PAGE: usize = (BTREE_PAGE_SIZE - PAGE_HEADER_SIZE) / 12;

/// In-memory separator entry pointing to a 4 KiB leaf page on disk (`24` bytes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeparatorEntry {
    pub field_id: u16,
    pub entry_count: u16,
    pub page_index: u32,
    pub min_key: i64,
    pub max_key: i64,
}

#[derive(Default)]
struct PagedBTreeState {
    /// Compact in-memory separator directory grouped by `field_id`, sorted by `min_key`.
    separators: FastHashMap<u16, Vec<SeparatorEntry>>,
    /// Memory-mapped `btree.idx` file containing contiguous 4 KiB leaf pages after a 4 KiB file header.
    mmap: Option<MmapRegion>,
    /// Mutable hot delta buffer for newly inserted `(field_id, key, ord)` triples since last checkpoint.
    hot_added: NumericRangeIndex,
    /// Tombstone set of `(field_id, key, ord)` removed since last checkpoint.
    hot_removed: FastHashSet<(u16, i64, RecordOrdinal)>,
}

/// Disk-backed 4 KiB Paged B+Tree Range Index (`TASK-034`, `F-INDX-06`).
pub struct PagedBTreeIndex {
    path: PathBuf,
    state: RwLock<PagedBTreeState>,
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
    /// Opens or creates a disk-backed `PagedBTreeIndex` at `path`.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let mut state = PagedBTreeState::default();
        if path.exists() {
            let meta = fs::metadata(&path).map_err(|e| StorageError::io("stat btree.idx", e))?;
            if meta.len() >= BTREE_PAGE_SIZE as u64 {
                let mmap = map_path_read_only(&path)?;
                load_separators_from_mmap(&mmap, &mut state.separators)?;
                state.mmap = Some(mmap);
            }
        }
        Ok(Self {
            path,
            state: RwLock::new(state),
        })
    }

    /// Returns the exact RAM footprint (in bytes) of the in-memory page separator directory.
    pub fn separator_memory_bytes(&self) -> usize {
        let guard = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard
            .separators
            .values()
            .map(|v| v.len() * core::mem::size_of::<SeparatorEntry>())
            .sum()
    }

    /// Returns the total number of 4 KiB leaf pages currently mapped on disk.
    pub fn page_count(&self) -> usize {
        let guard = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.separators.values().map(|v| v.len()).sum()
    }

    /// Inserts `(field_id, value, ord)` into the hot delta layer.
    pub fn insert(&self, field_id: u16, value: i64, ord: RecordOrdinal) {
        let mut guard = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.hot_removed.remove(&(field_id, value, ord));
        guard.hot_added.insert(field_id, value, ord);
    }

    /// Inserts an `f64` value using IEEE-754 order-preserving encoding.
    #[inline]
    pub fn insert_f64(&self, field_id: u16, value: f64, ord: RecordOrdinal) {
        self.insert(field_id, sortable_f64_to_i64(value), ord);
    }

    /// Removes `(field_id, value, ord)` from the index.
    pub fn remove(&self, field_id: u16, value: i64, ord: RecordOrdinal) {
        let mut guard = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.hot_added.remove(field_id, value, ord);
        guard.hot_removed.insert((field_id, value, ord));
    }

    /// Executes an $O(\log P + \log B)$ range scan over the 4 KiB mmap leaf pages and hot delta buffer,
    /// returning a `CompressedBitmap` of matching `RecordOrdinal`s.
    pub fn range_bitmap<R: RangeBounds<i64>>(
        &self,
        field_id: u16,
        range: R,
    ) -> CompressedBitmap {
        let (lo, hi) = normalize_bounds(&range);
        if lo > hi {
            return CompressedBitmap::new();
        }

        let guard = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut bm = guard.hot_added.range_bitmap(field_id, lo..=hi);

        if let (Some(ref mmap), Some(seps)) = (&guard.mmap, guard.separators.get(&field_id)) {
            let file_slice = mmap.as_slice();
            // Binary search for the first page whose `max_key >= lo`
            let start_idx = seps.partition_point(|s| s.max_key < lo);
            for sep in &seps[start_idx..] {
                if sep.min_key > hi {
                    break;
                }
                scan_leaf_page_into_bitmap(
                    file_slice,
                    sep,
                    lo,
                    hi,
                    &guard.hot_removed,
                    &mut bm,
                );
            }
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

    /// Zero-allocation point/range existence check directly over the 4 KiB mmap leaf pages (`0` heap allocations!).
    pub fn count_in_range_zero_alloc(&self, field_id: u16, lo: i64, hi: i64) -> usize {
        if lo > hi {
            return 0;
        }
        let guard = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(ref mmap) = guard.mmap else {
            return 0;
        };
        let Some(seps) = guard.separators.get(&field_id) else {
            return 0;
        };
        let file_slice = mmap.as_slice();
        let start_idx = seps.partition_point(|s| s.max_key < lo);
        let mut count = 0usize;
        for sep in &seps[start_idx..] {
            if sep.min_key > hi {
                break;
            }
            count += count_leaf_page_entries(file_slice, sep, lo, hi, &guard.hot_removed);
        }
        count
    }

    /// Compacts all existing 4 KiB disk pages and hot delta mutations into a fresh, tightly packed
    /// 4 KiB-paged `btree.idx` file on disk and maps it via `MmapRegion`.
    pub fn checkpoint_to_disk(&self) -> Result<usize> {
        let mut guard = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // Merge all live `(field_id -> BTreeMap<i64, Vec<RecordOrdinal>>)` entries
        let mut merged: BTreeMap<u16, BTreeMap<i64, Vec<RecordOrdinal>>> = BTreeMap::new();

        if let Some(ref mmap) = guard.mmap {
            let file_slice = mmap.as_slice();
            for (&field_id, seps) in &guard.separators {
                let tree = merged.entry(field_id).or_default();
                for sep in seps {
                    decode_leaf_page_into_tree(file_slice, sep, &guard.hot_removed, tree)?;
                }
            }
        }

        // Merge hot_added entries
        let hot_snapshot = guard.hot_added.snapshot_all();
        for (field_id, pairs) in hot_snapshot {
            let tree = merged.entry(field_id).or_default();
            for (k, ord) in pairs {
                if !guard.hot_removed.contains(&(field_id, k, ord)) {
                    let list = tree.entry(k).or_default();
                    if let Err(pos) = list.binary_search(&ord) {
                        list.insert(pos, ord);
                    }
                }
            }
        }

        // Release old mmap before replacing file on Windows
        guard.mmap = None;

        // Build 4 KiB leaf pages
        let mut pages_buf: Vec<u8> = Vec::new();
        let mut new_separators: FastHashMap<u16, Vec<SeparatorEntry>> = FastHashMap::default();
        let mut page_index = 0u32;

        for (field_id, tree) in merged {
            let mut cur_entries: Vec<(i64, RecordOrdinal)> =
                Vec::with_capacity(MAX_ENTRIES_PER_LEAF_PAGE);
            for (k, ords) in tree {
                for ord in ords {
                    cur_entries.push((k, ord));
                    if cur_entries.len() == MAX_ENTRIES_PER_LEAF_PAGE {
                        let sep = write_leaf_page(&mut pages_buf, field_id, page_index, &cur_entries);
                        new_separators.entry(field_id).or_default().push(sep);
                        page_index += 1;
                        cur_entries.clear();
                    }
                }
            }
            if !cur_entries.is_empty() {
                let sep = write_leaf_page(&mut pages_buf, field_id, page_index, &cur_entries);
                new_separators.entry(field_id).or_default().push(sep);
                page_index += 1;
            }
        }

        // Write 4 KiB file header page followed by all 4 KiB leaf pages
        let mut full_file = Vec::with_capacity(BTREE_PAGE_SIZE + pages_buf.len());
        full_file.resize(BTREE_PAGE_SIZE, 0u8);
        full_file[0..4].copy_from_slice(&BTREE_FILE_MAGIC.to_le_bytes());
        full_file[4..8].copy_from_slice(&page_index.to_le_bytes());
        let hdr_crc = checksum_payload(&full_file[0..8]);
        full_file[8..16].copy_from_slice(&hdr_crc.to_le_bytes());
        full_file.extend_from_slice(&pages_buf);

        if let Some(parent) = self.path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)
                    .map_err(|e| StorageError::io("create paged btree dir", e))?;
            }
        }
        atomic_write_replace(&self.path, &full_file, true)?;

        let new_mmap = map_path_read_only(&self.path)?;
        guard.mmap = Some(new_mmap);
        guard.separators = new_separators;
        guard.hot_added = NumericRangeIndex::new();
        guard.hot_removed.clear();

        Ok(page_index as usize)
    }
}

fn map_path_read_only(path: &Path) -> Result<MmapRegion> {
    let file = fs::File::open(path).map_err(|e| StorageError::io("open index file for mmap", e))?;
    let len = file
        .metadata()
        .map_err(|e| StorageError::io("stat index file for mmap", e))?
        .len() as usize;
    MmapRegion::map_read_only(&file, len)
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

/// Encodes a 4 KiB leaf page with delta-encoded keys (`delta = key - min_key`) and `RecordOrdinal`s.
/// Layout of 4 KiB page:
/// - `[0..4]`: `BTREE_PAGE_MAGIC` (`u32`)
/// - `[4..6]`: `field_id` (`u16`)
/// - `[6..8]`: `entry_count` (`u16`)
/// - `[8..16]`: `min_key` (`i64`)
/// - `[16..24]`: `max_key` (`i64`)
/// - `[24..32]`: `crc64` (`u64`)
/// - `[32..32 + count * 8]`: sorted array of `delta_key: i64` (`key.wrapping_sub(min_key)`)
/// - `[32 + count * 8 .. 32 + count * 12]`: parallel array of `RecordOrdinal` (`u32`)
fn write_leaf_page(
    out: &mut Vec<u8>,
    field_id: u16,
    page_index: u32,
    entries: &[(i64, RecordOrdinal)],
) -> SeparatorEntry {
    let start_len = out.len();
    out.resize(start_len + BTREE_PAGE_SIZE, 0u8);
    let page = &mut out[start_len..start_len + BTREE_PAGE_SIZE];

    let count = entries.len() as u16;
    let min_key = entries[0].0;
    let max_key = entries[entries.len() - 1].0;

    page[0..4].copy_from_slice(&BTREE_PAGE_MAGIC.to_le_bytes());
    page[4..6].copy_from_slice(&field_id.to_le_bytes());
    page[6..8].copy_from_slice(&count.to_le_bytes());
    page[8..16].copy_from_slice(&min_key.to_le_bytes());
    page[16..24].copy_from_slice(&max_key.to_le_bytes());

    let keys_off = PAGE_HEADER_SIZE;
    let ords_off = PAGE_HEADER_SIZE + entries.len() * 8;

    for (i, &(k, ord)) in entries.iter().enumerate() {
        let delta = k.wrapping_sub(min_key);
        let k_pos = keys_off + i * 8;
        page[k_pos..k_pos + 8].copy_from_slice(&delta.to_le_bytes());
        let o_pos = ords_off + i * 4;
        page[o_pos..o_pos + 4].copy_from_slice(&ord.0.to_le_bytes());
    }

    let crc = checksum_payload(&page[PAGE_HEADER_SIZE..]);
    page[24..32].copy_from_slice(&crc.to_le_bytes());

    SeparatorEntry {
        field_id,
        entry_count: count,
        page_index,
        min_key,
        max_key,
    }
}

#[inline]
fn read_delta_key_at(page: &[u8], min_key: i64, idx: usize) -> i64 {
    let pos = PAGE_HEADER_SIZE + idx * 8;
    let delta = i64::from_le_bytes([
        page[pos],
        page[pos + 1],
        page[pos + 2],
        page[pos + 3],
        page[pos + 4],
        page[pos + 5],
        page[pos + 6],
        page[pos + 7],
    ]);
    min_key.wrapping_add(delta)
}

#[inline]
fn read_ordinal_at(page: &[u8], count: usize, idx: usize) -> RecordOrdinal {
    let pos = PAGE_HEADER_SIZE + count * 8 + idx * 4;
    RecordOrdinal(u32::from_le_bytes([
        page[pos],
        page[pos + 1],
        page[pos + 2],
        page[pos + 3],
    ]))
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

fn scan_leaf_page_into_bitmap(
    file_slice: &[u8],
    sep: &SeparatorEntry,
    lo: i64,
    hi: i64,
    removed: &FastHashSet<(u16, i64, RecordOrdinal)>,
    bm: &mut CompressedBitmap,
) {
    let page_off = (sep.page_index as usize + 1) * BTREE_PAGE_SIZE;
    let Some(page) = file_slice.get(page_off..page_off + BTREE_PAGE_SIZE) else {
        return;
    };
    let count = sep.entry_count as usize;
    let start = if lo <= sep.min_key {
        0
    } else {
        lower_bound_in_page(page, sep.min_key, count, lo)
    };
    for idx in start..count {
        let key = read_delta_key_at(page, sep.min_key, idx);
        if key > hi {
            break;
        }
        let ord = read_ordinal_at(page, count, idx);
        if !removed.contains(&(sep.field_id, key, ord)) {
            bm.insert(ord);
        }
    }
}

fn count_leaf_page_entries(
    file_slice: &[u8],
    sep: &SeparatorEntry,
    lo: i64,
    hi: i64,
    removed: &FastHashSet<(u16, i64, RecordOrdinal)>,
) -> usize {
    let page_off = (sep.page_index as usize + 1) * BTREE_PAGE_SIZE;
    let Some(page) = file_slice.get(page_off..page_off + BTREE_PAGE_SIZE) else {
        return 0;
    };
    let count = sep.entry_count as usize;
    let start = if lo <= sep.min_key {
        0
    } else {
        lower_bound_in_page(page, sep.min_key, count, lo)
    };
    let mut matched = 0usize;
    for idx in start..count {
        let key = read_delta_key_at(page, sep.min_key, idx);
        if key > hi {
            break;
        }
        let ord = read_ordinal_at(page, count, idx);
        if !removed.contains(&(sep.field_id, key, ord)) {
            matched += 1;
        }
    }
    matched
}

fn decode_leaf_page_into_tree(
    file_slice: &[u8],
    sep: &SeparatorEntry,
    removed: &FastHashSet<(u16, i64, RecordOrdinal)>,
    tree: &mut BTreeMap<i64, Vec<RecordOrdinal>>,
) -> Result<()> {
    let page_off = (sep.page_index as usize + 1) * BTREE_PAGE_SIZE;
    let page = file_slice
        .get(page_off..page_off + BTREE_PAGE_SIZE)
        .ok_or_else(|| StorageError::corrupted_header("paged btree leaf out of bounds"))?;
    let expected_crc = u64::from_le_bytes([
        page[24], page[25], page[26], page[27], page[28], page[29], page[30], page[31],
    ]);
    let actual_crc = checksum_payload(&page[PAGE_HEADER_SIZE..]);
    if actual_crc != expected_crc {
        return Err(StorageError::checksum_mismatch(expected_crc, actual_crc));
    }
    let count = sep.entry_count as usize;
    for idx in 0..count {
        let key = read_delta_key_at(page, sep.min_key, idx);
        let ord = read_ordinal_at(page, count, idx);
        if !removed.contains(&(sep.field_id, key, ord)) {
            let list = tree.entry(key).or_default();
            if let Err(pos) = list.binary_search(&ord) {
                list.insert(pos, ord);
            }
        }
    }
    Ok(())
}

fn load_separators_from_mmap(
    mmap: &MmapRegion,
    out: &mut FastHashMap<u16, Vec<SeparatorEntry>>,
) -> Result<()> {
    let slice = mmap.as_slice();
    if slice.len() < BTREE_PAGE_SIZE {
        return Err(StorageError::corrupted_header("btree.idx smaller than 4 KiB"));
    }
    let magic = u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]);
    if magic != BTREE_FILE_MAGIC {
        return Err(StorageError::corrupted_header("btree.idx invalid file magic"));
    }
    let page_count = u32::from_le_bytes([slice[4], slice[5], slice[6], slice[7]]);
    let expected_crc = u64::from_le_bytes([
        slice[8], slice[9], slice[10], slice[11], slice[12], slice[13], slice[14], slice[15],
    ]);
    if checksum_payload(&slice[0..8]) != expected_crc {
        return Err(StorageError::checksum_mismatch(
            expected_crc,
            checksum_payload(&slice[0..8]),
        ));
    }

    for page_idx in 0..page_count {
        let off = (page_idx as usize + 1) * BTREE_PAGE_SIZE;
        let page = slice
            .get(off..off + BTREE_PAGE_SIZE)
            .ok_or_else(|| StorageError::corrupted_header("btree.idx truncated leaf page"))?;
        let p_magic = u32::from_le_bytes([page[0], page[1], page[2], page[3]]);
        if p_magic != BTREE_PAGE_MAGIC {
            return Err(StorageError::corrupted_header("btree.idx invalid leaf magic"));
        }
        let field_id = u16::from_le_bytes([page[4], page[5]]);
        let entry_count = u16::from_le_bytes([page[6], page[7]]);
        let min_key = i64::from_le_bytes([
            page[8], page[9], page[10], page[11], page[12], page[13], page[14], page[15],
        ]);
        let max_key = i64::from_le_bytes([
            page[16], page[17], page[18], page[19], page[20], page[21], page[22], page[23],
        ]);
        out.entry(field_id).or_default().push(SeparatorEntry {
            field_id,
            entry_count,
            page_index: page_idx,
            min_key,
            max_key,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_paged_btree_4kb_checkpoint_and_zero_alloc_range_scan() -> Result<()> {
        let path = std::env::temp_dir().join("storine_test_paged_btree.idx");
        let _ = fs::remove_file(&path);

        let btree = PagedBTreeIndex::open(&path)?;
        // Insert 2,000 entries across multiple 4 KiB pages (338 entries per 4 KiB page -> 6 pages)
        for i in 0..2_000i64 {
            btree.insert(1, 10_000 + i * 10, RecordOrdinal(i as u32));
        }

        let pages = btree.checkpoint_to_disk()?;
        assert_eq!(pages, 6);
        assert!(btree.separator_memory_bytes() <= 6 * 24);

        // Re-open from disk and run zero-alloc range count + bitmap range query
        let reopened = PagedBTreeIndex::open(&path)?;
        assert_eq!(reopened.page_count(), 6);

        let count = reopened.count_in_range_zero_alloc(1, 15_000, 15_990);
        assert_eq!(count, 100);

        let bm = reopened.range_bitmap(1, 15_000..=15_090);
        assert_eq!(bm.len(), 10);
        assert!(bm.contains(RecordOrdinal(500)));
        assert!(bm.contains(RecordOrdinal(509)));

        let _ = fs::remove_file(&path);
        Ok(())
    }
}
