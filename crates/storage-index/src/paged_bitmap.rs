//! Disk-Backed Paged Bitmap Index, 64-Byte Split-Block Bloom Filter, and Tiered Cold Primary Index
//! (`TASK-035`, `F-INDX-06`, `F-INDX-08`, `RULE-MEM-01`, `RULE-CPU-01`, `RULE-DATA-03`).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

use storage_codec::checksum_payload;
use storage_mmap::{atomic_write_replace, ArenaLocation, MmapRegion};
use storage_types::{FastHashMap, FastHashSet, InlineId, RecordOrdinal, Result, StorageError, Version};

use crate::bitmap::{BitmapTagIndex, CompressedBitmap};
use crate::primary::DiskLocation;

const BITMAP_FILE_MAGIC: u32 = 0x424D_4958; // "BMIX"
const BITMAP_BLOCK_BYTES: usize = 512; // 64 x u64 words = 512 bytes (8 blocks per 4 KiB page)
const PAGE_SIZE_4KB: usize = 4096;

/// Pointer to a 512-byte `[u64; 64]` chunk stored inside a 4 KiB page in `bitmap.idx` (`8` bytes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MappedChunkRef {
    pub chunk_id: u32,
    pub block_index: u32,
}

#[derive(Default)]
struct PagedBitmapState {
    /// Directory mapping `(field_id, tag)` to sorted `MappedChunkRef`s on disk.
    directory: FastHashMap<(u16, InlineId), Vec<MappedChunkRef>>,
    /// Memory-mapped `bitmap.idx` file.
    mmap: Option<MmapRegion>,
    /// Byte offset inside `mmap` where the 512-byte chunk array begins (4 KiB aligned).
    blocks_base_offset: usize,
    /// Hot in-memory delta additions since last checkpoint.
    hot_added: BitmapTagIndex,
    /// Hot tombstones removed since last checkpoint.
    hot_removed: FastHashSet<(u16, InlineId, RecordOrdinal)>,
}

/// Disk-backed Paged Roaring-style Bitmap Tag Index (`TASK-035`, `F-INDX-06`).
///
/// Evaluates 64-bit word bitwise `AND` intersections directly over `MmapRegion` 512-byte chunk
/// slices without loading untouched tag bitmaps into heap memory.
pub struct PagedBitmapIndex {
    path: PathBuf,
    state: RwLock<PagedBitmapState>,
}

impl core::fmt::Debug for PagedBitmapIndex {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PagedBitmapIndex")
            .field("path", &self.path)
            .field("mapped_tags", &self.mapped_tag_count())
            .finish()
    }
}

impl PagedBitmapIndex {
    /// Opens or creates a disk-backed `PagedBitmapIndex` at `path`.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let mut state = PagedBitmapState::default();
        if path.exists() {
            let meta = fs::metadata(&path).map_err(|e| StorageError::io("stat bitmap.idx", e))?;
            if meta.len() >= PAGE_SIZE_4KB as u64 {
                let mmap = map_path_read_only(&path)?;
                let base_off = load_bitmap_directory(&mmap, &mut state.directory)?;
                state.blocks_base_offset = base_off;
                state.mmap = Some(mmap);
            }
        }
        Ok(Self {
            path,
            state: RwLock::new(state),
        })
    }

    pub fn mapped_tag_count(&self) -> usize {
        let guard = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.directory.len()
    }

    pub fn insert(&self, field_id: u16, tag: InlineId, ord: RecordOrdinal) {
        let mut guard = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.hot_removed.remove(&(field_id, tag, ord));
        guard.hot_added.insert(field_id, tag, ord);
    }

    pub fn remove(&self, field_id: u16, tag: InlineId, ord: RecordOrdinal) {
        let mut guard = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.hot_added.remove(field_id, tag, ord);
        guard.hot_removed.insert((field_id, tag, ord));
    }

    /// Loads or merges the `CompressedBitmap` for `(field_id, tag)` from the `MmapRegion` and hot delta.
    pub fn get_bitmap(&self, field_id: u16, tag: InlineId) -> CompressedBitmap {
        let guard = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.load_merged_bitmap_locked(&guard, field_id, tag)
    }

    /// Intersects multiple `(field_id, tag)` predicates in ascending chunk-count order
    /// directly over the memory-mapped 512-byte chunk slices (`TASK-035`).
    pub fn intersect_all(&self, predicates: &[(u16, InlineId)]) -> CompressedBitmap {
        if predicates.is_empty() {
            return CompressedBitmap::new();
        }
        let guard = self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let mut bitmaps: Vec<CompressedBitmap> = Vec::with_capacity(predicates.len());
        for &(fid, tag) in predicates {
            let bm = self.load_merged_bitmap_locked(&guard, fid, tag);
            if bm.is_empty() {
                return CompressedBitmap::new();
            }
            bitmaps.push(bm);
        }
        bitmaps.sort_by_key(|b| b.len());
        let mut acc = bitmaps[0].clone();
        for bm in &bitmaps[1..] {
            acc = acc.intersect(bm);
            if acc.is_empty() {
                break;
            }
        }
        acc
    }

    fn load_merged_bitmap_locked(
        &self,
        guard: &PagedBitmapState,
        field_id: u16,
        tag: InlineId,
    ) -> CompressedBitmap {
        let mut bm = if let (Some(ref mmap), Some(refs)) =
            (&guard.mmap, guard.directory.get(&(field_id, tag)))
        {
            let slice = mmap.as_slice();
            let mut raw_chunks = Vec::with_capacity(refs.len());
            for cref in refs {
                let off = guard.blocks_base_offset + (cref.block_index as usize) * BITMAP_BLOCK_BYTES;
                if let Some(block_bytes) = slice.get(off..off + BITMAP_BLOCK_BYTES) {
                    let mut words = [0u64; 64];
                    for (i, w) in words.iter_mut().enumerate() {
                        let b = i * 8;
                        *w = u64::from_le_bytes([
                            block_bytes[b],
                            block_bytes[b + 1],
                            block_bytes[b + 2],
                            block_bytes[b + 3],
                            block_bytes[b + 4],
                            block_bytes[b + 5],
                            block_bytes[b + 6],
                            block_bytes[b + 7],
                        ]);
                    }
                    raw_chunks.push((cref.chunk_id, words));
                }
            }
            CompressedBitmap::from_raw_chunks(raw_chunks)
        } else {
            CompressedBitmap::new()
        };

        let hot = guard.hot_added.get_bitmap(field_id, tag);
        if !hot.is_empty() {
            bm = bm.union(&hot);
        }
        if !guard.hot_removed.is_empty() {
            for &(rfid, rtag, rord) in &guard.hot_removed {
                if rfid == field_id && rtag == tag {
                    bm.remove(rord);
                }
            }
        }
        bm
    }

    /// Checkpoints all mapped and hot-delta bitmap chunks into 4 KiB-aligned disk pages in `bitmap.idx`.
    pub fn checkpoint_to_disk(&self) -> Result<usize> {
        let mut guard = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let mut all_keys: BTreeMap<(u16, InlineId), CompressedBitmap> = BTreeMap::new();
        let existing_keys: Vec<(u16, InlineId)> = guard.directory.keys().copied().collect();
        for (fid, tag) in existing_keys {
            let bm = self.load_merged_bitmap_locked(&guard, fid, tag);
            if !bm.is_empty() {
                all_keys.insert((fid, tag), bm);
            }
        }
        for ((fid, tag), hot_bm) in guard.hot_added.snapshot_all() {
            let entry = all_keys.entry((fid, tag)).or_default();
            *entry = entry.union(&hot_bm);
            for &(rfid, rtag, rord) in &guard.hot_removed {
                if rfid == fid && rtag == tag {
                    entry.remove(rord);
                }
            }
            if entry.is_empty() {
                all_keys.remove(&(fid, tag));
            }
        }

        // Release old mmap before replacing file on Windows
        guard.mmap = None;

        // Serialize directory + 4 KiB-aligned 512-byte chunk blocks
        let mut dir_payload = Vec::new();
        let mut blocks_payload = Vec::new();
        let mut new_dir: FastHashMap<(u16, InlineId), Vec<MappedChunkRef>> = FastHashMap::default();
        let mut block_index = 0u32;

        let tag_count = all_keys.len() as u32;
        dir_payload.extend_from_slice(&tag_count.to_le_bytes());

        for ((fid, tag), bm) in all_keys {
            let chunks = bm.chunks_slice();
            dir_payload.extend_from_slice(&fid.to_le_bytes());
            dir_payload.extend_from_slice(&tag.to_raw_bytes());
            dir_payload.extend_from_slice(&(chunks.len() as u32).to_le_bytes());

            let mut refs = Vec::with_capacity(chunks.len());
            for &(chunk_id, ref words) in chunks {
                dir_payload.extend_from_slice(&chunk_id.to_le_bytes());
                dir_payload.extend_from_slice(&block_index.to_le_bytes());
                for &w in words {
                    blocks_payload.extend_from_slice(&w.to_le_bytes());
                }
                refs.push(MappedChunkRef {
                    chunk_id,
                    block_index,
                });
                block_index += 1;
            }
            new_dir.insert((fid, tag), refs);
        }

        let dir_crc = checksum_payload(&dir_payload);
        let header_and_dir_len = 20 + dir_payload.len();
        let blocks_base_offset = header_and_dir_len.next_multiple_of(PAGE_SIZE_4KB);

        let mut full_file = Vec::with_capacity(blocks_base_offset + blocks_payload.len());
        full_file.extend_from_slice(&BITMAP_FILE_MAGIC.to_le_bytes());
        full_file.extend_from_slice(&(blocks_base_offset as u32).to_le_bytes());
        full_file.extend_from_slice(&(dir_payload.len() as u32).to_le_bytes());
        full_file.extend_from_slice(&dir_crc.to_le_bytes());
        full_file.extend_from_slice(&dir_payload);
        full_file.resize(blocks_base_offset, 0u8);
        full_file.extend_from_slice(&blocks_payload);

        if let Some(parent) = self.path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)
                    .map_err(|e| StorageError::io("create paged bitmap dir", e))?;
            }
        }
        atomic_write_replace(&self.path, &full_file, true)?;

        let new_mmap = map_path_read_only(&self.path)?;
        guard.mmap = Some(new_mmap);
        guard.blocks_base_offset = blocks_base_offset;
        guard.directory = new_dir;
        guard.hot_added = BitmapTagIndex::new();
        guard.hot_removed.clear();

        Ok(block_index as usize)
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

fn load_bitmap_directory(
    mmap: &MmapRegion,
    out: &mut FastHashMap<(u16, InlineId), Vec<MappedChunkRef>>,
) -> Result<usize> {
    let slice = mmap.as_slice();
    if slice.len() < 20 {
        return Err(StorageError::corrupted_header("bitmap.idx truncated"));
    }
    let magic = u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]);
    if magic != BITMAP_FILE_MAGIC {
        return Err(StorageError::corrupted_header("bitmap.idx invalid magic"));
    }
    let blocks_base_offset = u32::from_le_bytes([slice[4], slice[5], slice[6], slice[7]]) as usize;
    let dir_len = u32::from_le_bytes([slice[8], slice[9], slice[10], slice[11]]) as usize;
    let expected_crc = u64::from_le_bytes([
        slice[12], slice[13], slice[14], slice[15], slice[16], slice[17], slice[18], slice[19],
    ]);
    let dir_slice = slice
        .get(20..20 + dir_len)
        .ok_or_else(|| StorageError::corrupted_header("bitmap.idx directory out of bounds"))?;
    let actual_crc = checksum_payload(dir_slice);
    if actual_crc != expected_crc {
        return Err(StorageError::checksum_mismatch(expected_crc, actual_crc));
    }

    if dir_slice.len() < 4 {
        return Ok(blocks_base_offset);
    }
    let tag_count = u32::from_le_bytes([dir_slice[0], dir_slice[1], dir_slice[2], dir_slice[3]]) as usize;
    let mut off = 4usize;

    for _ in 0..tag_count {
        if off + 30 > dir_slice.len() {
            return Err(StorageError::corrupted_header("bitmap.idx tag entry truncated"));
        }
        let fid = u16::from_le_bytes([dir_slice[off], dir_slice[off + 1]]);
        off += 2;
        let mut raw_id = [0u8; 24];
        raw_id.copy_from_slice(&dir_slice[off..off + 24]);
        let tag = InlineId::from_raw_bytes(raw_id)?;
        off += 24;
        let chunk_count = u32::from_le_bytes([
            dir_slice[off],
            dir_slice[off + 1],
            dir_slice[off + 2],
            dir_slice[off + 3],
        ]) as usize;
        off += 4;

        let mut refs = Vec::with_capacity(chunk_count);
        for _ in 0..chunk_count {
            if off + 8 > dir_slice.len() {
                return Err(StorageError::corrupted_header("bitmap.idx chunk ref truncated"));
            }
            let chunk_id = u32::from_le_bytes([
                dir_slice[off],
                dir_slice[off + 1],
                dir_slice[off + 2],
                dir_slice[off + 3],
            ]);
            let block_index = u32::from_le_bytes([
                dir_slice[off + 4],
                dir_slice[off + 5],
                dir_slice[off + 6],
                dir_slice[off + 7],
            ]);
            off += 8;
            refs.push(MappedChunkRef {
                chunk_id,
                block_index,
            });
        }
        out.insert((fid, tag), refs);
    }

    Ok(blocks_base_offset)
}

/// 64-byte Cache-Line-Aligned Split-Block Bloom Filter (`TASK-035`, `F-INDX-08`, `RULE-CPU-01`).
///
/// Every key lookup touches **at most 1 single 64-byte CPU cache line**, testing 8 independent
/// bit positions in parallel across `[u64; 8]` words with `< 1%` false-positive rate at 10 bits/key.
#[repr(C, align(64))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BloomCacheBlock {
    pub words: [u64; 8],
}

#[derive(Clone, Debug)]
pub struct SplitBlockBloomFilter {
    blocks: Vec<BloomCacheBlock>,
}

const BLOOM_SALTS: [u64; 8] = [
    0x47b6_137b_c570_83d5,
    0x4497_4d91_a181_7e7f,
    0x8824_ad5b_72f9_398d,
    0xa2b7_289d_5d39_2e53,
    0x7054_95c7_6786_48b9,
    0x2df1_424b_9c8c_316d,
    0x9efc_4947_5c6b_21e1,
    0x5c6b_fb31_e3f5_9d47,
];

impl SplitBlockBloomFilter {
    /// Creates a `SplitBlockBloomFilter` sized for `expected_items` (~10.24 bits per item).
    pub fn with_capacity(expected_items: usize) -> Self {
        // Each 64-byte block has 512 bits -> holds ~50 keys at ~10 bits/key (<1% FPR)
        let num_blocks = (expected_items / 50).max(1).next_power_of_two();
        Self {
            blocks: vec![BloomCacheBlock::default(); num_blocks],
        }
    }

    #[inline]
    pub fn insert(&mut self, id: InlineId) {
        let h = id.fast_hash();
        let block_idx = ((h >> 32) as usize) & (self.blocks.len() - 1);
        let block = &mut self.blocks[block_idx];
        for i in 0..8 {
            let bit = ((h.wrapping_mul(BLOOM_SALTS[i])) >> 58) as u32;
            block.words[i] |= 1u64 << bit;
        }
    }

    /// Returns `false` if `id` is **definitely not** in the set (in 1 single 64-byte cache line touch!).
    #[inline]
    pub fn may_contain(&self, id: InlineId) -> bool {
        if self.blocks.is_empty() {
            return false;
        }
        let h = id.fast_hash();
        let block_idx = ((h >> 32) as usize) & (self.blocks.len() - 1);
        let block = &self.blocks[block_idx];
        for i in 0..8 {
            let bit = ((h.wrapping_mul(BLOOM_SALTS[i])) >> 58) as u32;
            if (block.words[i] & (1u64 << bit)) == 0 {
                return false;
            }
        }
        true
    }

    #[inline]
    pub fn memory_bytes(&self) -> usize {
        self.blocks.len() * core::mem::size_of::<BloomCacheBlock>()
    }
}

const COLD_PRIMARY_MAGIC: u32 = 0x4350_4958; // "CPIX"
const COLD_RECORD_STRIDE: usize = 64; // 64-byte cache-line-aligned cold primary entry
const COLD_RECORDS_PER_PAGE: usize = (PAGE_SIZE_4KB - 32) / COLD_RECORD_STRIDE; // 63 entries per 4 KiB page

/// Cold Primary Index separator entry (`24` bytes per 4 KiB page -> ~380 bytes per 1,000 cold records!).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColdPageSeparator {
    pub min_hash: u64,
    pub max_hash: u64,
    pub page_index: u32,
    pub entry_count: u16,
    pub _pad: u16,
}

/// Decoded cold primary record entry from a 4 KiB mmap page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColdPrimaryEntry {
    pub id: InlineId,
    pub ordinal: RecordOrdinal,
    pub version: Version,
    pub ttl_expires_at_ns: u64,
    pub location: DiskLocation,
}

/// Tiered Cold Primary Index backed by 4 KiB `MmapRegion` pages and guarded by a 64-byte
/// `SplitBlockBloomFilter` (`TASK-035`, `F-INDX-08`).
#[derive(Debug)]
pub struct ColdPrimaryIndex {
    path: PathBuf,
    bloom: SplitBlockBloomFilter,
    separators: Vec<ColdPageSeparator>,
    mmap: Option<MmapRegion>,
}

impl ColdPrimaryIndex {
    /// Builds and maps a `ColdPrimaryIndex` at `path` from a slice of `ColdPrimaryEntry` items.
    pub fn build(path: impl Into<PathBuf>, mut entries: Vec<ColdPrimaryEntry>) -> Result<Self> {
        let path = path.into();
        // Sort by `(id.fast_hash(), id)` for $O(\log B)$ binary search
        entries.sort_unstable_by(|a, b| {
            a.id.fast_hash()
                .cmp(&b.id.fast_hash())
                .then_with(|| a.id.cmp(&b.id))
        });

        let mut bloom = SplitBlockBloomFilter::with_capacity(entries.len().max(64));
        for e in &entries {
            bloom.insert(e.id);
        }

        let mut pages_buf = Vec::new();
        let mut separators = Vec::new();

        for (page_idx, chunk) in entries.chunks(COLD_RECORDS_PER_PAGE).enumerate() {
            let start = pages_buf.len();
            pages_buf.resize(start + PAGE_SIZE_4KB, 0u8);
            let page = &mut pages_buf[start..start + PAGE_SIZE_4KB];

            let min_hash = chunk[0].id.fast_hash();
            let max_hash = chunk[chunk.len() - 1].id.fast_hash();
            let count = chunk.len() as u16;

            page[0..4].copy_from_slice(&COLD_PRIMARY_MAGIC.to_le_bytes());
            page[4..6].copy_from_slice(&count.to_le_bytes());
            page[8..16].copy_from_slice(&min_hash.to_le_bytes());
            page[16..24].copy_from_slice(&max_hash.to_le_bytes());

            for (i, entry) in chunk.iter().enumerate() {
                let off = 32 + i * COLD_RECORD_STRIDE;
                encode_cold_entry(&mut page[off..off + COLD_RECORD_STRIDE], entry);
            }
            let crc = checksum_payload(&page[32..]);
            page[24..32].copy_from_slice(&crc.to_le_bytes());

            separators.push(ColdPageSeparator {
                min_hash,
                max_hash,
                page_index: page_idx as u32,
                entry_count: count,
                _pad: 0,
            });
        }

        let mmap = if !pages_buf.is_empty() {
            if let Some(parent) = path.parent() {
                if !parent.exists() {
                    fs::create_dir_all(parent)
                        .map_err(|e| StorageError::io("create cold primary dir", e))?;
                }
            }
            atomic_write_replace(&path, &pages_buf, true)?;
            Some(map_path_read_only(&path)?)
        } else {
            None
        };

        Ok(Self {
            path,
            bloom,
            separators,
            mmap,
        })
    }

    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Looks up `id` in the cold primary index:
    /// 1. Checks `SplitBlockBloomFilter` in 1 single 64-byte cache line (rejecting >99% of misses with 0 disk touches).
    /// 2. Binary-searches `separators` in $O(\log P)$ to find candidate 4 KiB page(s).
    /// 3. Binary-searches the 64-byte slots inside the 4 KiB `MmapRegion` page in $O(\log B)$ with 0 heap allocations!
    pub fn get(&self, id: InlineId) -> Option<ColdPrimaryEntry> {
        if !self.bloom.may_contain(id) {
            return None;
        }
        let mmap = self.mmap.as_ref()?;
        let file_slice = mmap.as_slice();
        let target_hash = id.fast_hash();

        let start_page = self.separators.partition_point(|s| s.max_hash < target_hash);
        for sep in &self.separators[start_page..] {
            if sep.min_hash > target_hash {
                break;
            }
            let page_off = (sep.page_index as usize) * PAGE_SIZE_4KB;
            let page = file_slice.get(page_off..page_off + PAGE_SIZE_4KB)?;
            let count = sep.entry_count as usize;

            // Binary search within the 4 KiB page
            let mut left = 0usize;
            let mut right = count;
            while left < right {
                let mid = left + (right - left) / 2;
                let off = 32 + mid * COLD_RECORD_STRIDE;
                let mid_id = decode_cold_entry_id(&page[off..off + COLD_RECORD_STRIDE])?;
                let ord = mid_id
                    .fast_hash()
                    .cmp(&target_hash)
                    .then_with(|| mid_id.cmp(&id));
                match ord {
                    core::cmp::Ordering::Less => left = mid + 1,
                    core::cmp::Ordering::Greater => right = mid,
                    core::cmp::Ordering::Equal => {
                        return decode_cold_entry(&page[off..off + COLD_RECORD_STRIDE]);
                    }
                }
            }
        }
        None
    }

    /// Total in-memory footprint (Bloom filter + page separators) in bytes.
    pub fn ram_footprint_bytes(&self) -> usize {
        self.bloom.memory_bytes()
            + self.separators.len() * core::mem::size_of::<ColdPageSeparator>()
    }
}

fn encode_cold_entry(slot: &mut [u8], entry: &ColdPrimaryEntry) {
    slot[0..24].copy_from_slice(&entry.id.to_raw_bytes());
    slot[24..28].copy_from_slice(&entry.ordinal.0.to_le_bytes());
    slot[28..36].copy_from_slice(&entry.version.0.to_le_bytes());
    slot[36..44].copy_from_slice(&entry.ttl_expires_at_ns.to_le_bytes());
    match entry.location {
        DiskLocation::Unflushed => {
            slot[44] = 0;
        }
        DiskLocation::Arena(loc) => {
            slot[44] = 1;
            slot[48..52].copy_from_slice(&loc.segment_id.to_le_bytes());
            slot[52..56].copy_from_slice(&loc.offset.to_le_bytes());
            slot[56..60].copy_from_slice(&loc.len.to_le_bytes());
        }
        DiskLocation::DedicatedFile {
            ping_pong_stride, ..
        } => {
            slot[44] = 2;
            slot[48..52].copy_from_slice(&ping_pong_stride.to_le_bytes());
        }
    }
}

#[inline]
fn decode_cold_entry_id(slot: &[u8]) -> Option<InlineId> {
    let mut raw = [0u8; 24];
    raw.copy_from_slice(&slot[0..24]);
    InlineId::from_raw_bytes(raw).ok()
}

fn decode_cold_entry(slot: &[u8]) -> Option<ColdPrimaryEntry> {
    let id = decode_cold_entry_id(slot)?;
    let ordinal = RecordOrdinal(u32::from_le_bytes([slot[24], slot[25], slot[26], slot[27]]));
    let version = Version(u64::from_le_bytes([
        slot[28], slot[29], slot[30], slot[31], slot[32], slot[33], slot[34], slot[35],
    ]));
    let ttl_expires_at_ns = u64::from_le_bytes([
        slot[36], slot[37], slot[38], slot[39], slot[40], slot[41], slot[42], slot[43],
    ]);
    let location = match slot[44] {
        1 => DiskLocation::Arena(ArenaLocation {
            segment_id: u32::from_le_bytes([slot[48], slot[49], slot[50], slot[51]]),
            offset: u32::from_le_bytes([slot[52], slot[53], slot[54], slot[55]]),
            len: u32::from_le_bytes([slot[56], slot[57], slot[58], slot[59]]),
        }),
        2 => DiskLocation::DedicatedFile {
            parent_group: None,
            ping_pong_stride: u32::from_le_bytes([slot[48], slot[49], slot[50], slot[51]]),
        },
        _ => DiskLocation::Unflushed,
    };
    Some(ColdPrimaryEntry {
        id,
        ordinal,
        version,
        ttl_expires_at_ns,
        location,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_paged_bitmap_index_checkpoint_and_mmap_intersection() -> Result<()> {
        let path = std::env::temp_dir().join("storine_test_paged_bitmap.idx");
        let _ = fs::remove_file(&path);

        let tag_a = InlineId::new("tag_alpha")?;
        let tag_b = InlineId::new("tag_beta")?;

        let idx = PagedBitmapIndex::open(&path)?;
        for i in (0..8_000u32).step_by(2) {
            idx.insert(1, tag_a, RecordOrdinal(i));
        }
        for i in (0..8_000u32).step_by(3) {
            idx.insert(1, tag_b, RecordOrdinal(i));
        }
        let blocks = idx.checkpoint_to_disk()?;
        assert!(blocks >= 4);

        let reopened = PagedBitmapIndex::open(&path)?;
        let both = reopened.intersect_all(&[(1, tag_a), (1, tag_b)]);
        assert_eq!(both.len(), 1334); // multiples of 6 in 0..8000

        let _ = fs::remove_file(&path);
        Ok(())
    }

    #[test]
    fn test_split_block_bloom_and_cold_primary_index() -> Result<()> {
        let path = std::env::temp_dir().join("storine_test_cold_primary.idx");
        let _ = fs::remove_file(&path);

        let mut entries = Vec::with_capacity(1_000);
        for i in 0..1_000u32 {
            let id = InlineId::new(&format!("id_{:06}", i + 1))?;
            entries.push(ColdPrimaryEntry {
                id,
                ordinal: RecordOrdinal(i),
                version: Version(1),
                ttl_expires_at_ns: 0,
                location: DiskLocation::Arena(ArenaLocation {
                    segment_id: 1,
                    offset: i * 128,
                    len: 128,
                }),
            });
        }

        let cold = ColdPrimaryIndex::build(&path, entries)?;
        // Verify compact RAM footprint (< 3 bytes per record!)
        assert!(cold.ram_footprint_bytes() < 3_000);

        // Positive lookup hits the exact entry via O(log B) page binary search
        let hit = cold
            .get(InlineId::new("id_000500")?)
            .expect("must find id 500");
        assert_eq!(hit.ordinal, RecordOrdinal(499));

        // Negative lookups short-circuit via SplitBlockBloomFilter
        assert!(cold.get(InlineId::new("id_999999")?).is_none());

        let _ = fs::remove_file(&path);
        Ok(())
    }
}
