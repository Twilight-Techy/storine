//! 64-Byte Split-Block Bloom Filter and Tiered Cold Primary Index (`TASK-035`, `F-PIDX-03`,
//! `F-INDX-08`, `RULE-CPU-01`, `RULE-MEM-03`).

use std::fs;
use std::path::{Path, PathBuf};

use storage_codec::checksum_payload;
use storage_mmap::{atomic_write_replace, ArenaLocation, MmapRegion};
use storage_types::{InlineId, RecordOrdinal, Result, StorageError, Version};

use crate::paged_files::map_path_read_only;
use crate::primary::DiskLocation;

const PAGE_SIZE_4KB: usize = 4096;

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
        for (i, &salt) in BLOOM_SALTS.iter().enumerate() {
            let bit = ((h.wrapping_mul(salt)) >> 58) as u32;
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
        for (i, &salt) in BLOOM_SALTS.iter().enumerate() {
            let bit = ((h.wrapping_mul(salt)) >> 58) as u32;
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
    ///
    /// The 64-byte slot format has no room for a dedicated file's parent group, so entries with
    /// `DiskLocation::DedicatedFile { parent_group: Some(_), .. }` are rejected rather than
    /// silently losing their location. The Bloom filter and page directory live in RAM and are
    /// rebuilt by calling `build` again; this is a standalone building block not yet used by
    /// `StorageEngine`.
    pub fn build(path: impl Into<PathBuf>, mut entries: Vec<ColdPrimaryEntry>) -> Result<Self> {
        let path = path.into();
        if entries.iter().any(|e| {
            matches!(
                e.location,
                DiskLocation::DedicatedFile {
                    parent_group: Some(_),
                    ..
                }
            )
        }) {
            return Err(StorageError::invalid_key(
                "ColdPrimaryIndex cannot encode DedicatedFile parent groups",
            ));
        }
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
