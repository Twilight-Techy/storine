//! 64 MB Slotted Arena Segments (`seg_XXXXX.dat`) & Compactor for sub-4KB records (`TASK-025`, `F-CORE-06`).
//!
//! Solves OS inode and file-descriptor exhaustion at 10M+ record scale by packing small records
//! at 64-byte cache-line-aligned offsets inside shared memory-mapped segment files.

use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use storage_types::{Result, StorageError, HEADER_SIZE};

use crate::raw::MmapRegion;

/// Default maximum Arena Segment rollover ceiling (`4 MiB` default, configurable up to `64 MiB+`).
pub const DEFAULT_SEGMENT_CAPACITY: usize = 4 * 1024 * 1024;
/// Initial physical file allocation size on disk (`4 KiB` = 1 OS page) so small databases
/// have a tiny 4 KiB disk footprint and grow exponentially without upfront bloat.
const INITIAL_SEGMENT_BYTES: usize = 4096;
const SEGMENT_MAGIC: &[u8; 8] = b"STGESEG1";

/// Physical coordinate of a record packed inside an Arena Segment (`12 bytes`).
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ArenaLocation {
    pub segment_id: u32,
    pub offset: u32,
    pub len: u32,
}

#[derive(Debug)]
struct SegmentWriter {
    file: File,
    write_head: usize,
    capacity: usize,
    max_capacity: usize,
    dead_bytes: usize,
}

/// A single memory-mapped Arena Segment file (`seg_XXXXX.dat`).
#[derive(Debug)]
pub struct ArenaSegment {
    pub segment_id: u32,
    pub path: PathBuf,
    writer: Mutex<SegmentWriter>,
    region: RwLock<Arc<MmapRegion>>,
}

impl ArenaSegment {
    pub fn open_or_create(segment_id: u32, path: PathBuf, max_capacity: usize) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)
                    .map_err(|e| StorageError::io("create arena directory", e))?;
            }
        }
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| StorageError::io("open arena segment file", e))?;

        let current_len = file
            .metadata()
            .map_err(|e| StorageError::io("stat arena segment file", e))?
            .len() as usize;

        let max_capacity = max_capacity
            .max(INITIAL_SEGMENT_BYTES)
            .next_multiple_of(HEADER_SIZE);

        let (capacity, write_head) = if current_len < HEADER_SIZE {
            let init_cap = INITIAL_SEGMENT_BYTES.min(max_capacity);
            file.set_len(init_cap as u64)
                .map_err(|e| StorageError::io("preallocate initial 4KB arena segment", e))?;
            // Write 64-byte segment superblock at offset 0
            let mut superblock = [0u8; HEADER_SIZE];
            superblock[0..8].copy_from_slice(SEGMENT_MAGIC);
            superblock[8..12].copy_from_slice(&segment_id.to_le_bytes());
            superblock[12..20].copy_from_slice(&(HEADER_SIZE as u64).to_le_bytes());
            file.seek(SeekFrom::Start(0))
                .map_err(|e| StorageError::io("seek arena superblock", e))?;
            file.write_all(&superblock)
                .map_err(|e| StorageError::io("write arena superblock", e))?;
            (init_cap, HEADER_SIZE)
        } else {
            let cap = current_len.next_multiple_of(HEADER_SIZE);
            if cap > current_len {
                file.set_len(cap as u64)
                    .map_err(|e| StorageError::io("align arena segment length", e))?;
            }
            let mut sb = [0u8; HEADER_SIZE];
            use std::io::Read;
            file.seek(SeekFrom::Start(0))
                .map_err(|e| StorageError::io("seek arena superblock", e))?;
            file.read_exact(&mut sb)
                .map_err(|e| StorageError::io("read arena superblock", e))?;
            if &sb[0..8] != SEGMENT_MAGIC {
                return Err(StorageError::corrupted_header(
                    "invalid arena segment superblock magic",
                ));
            }
            let head = u64::from_le_bytes(sb[12..20].try_into().unwrap()) as usize;
            (cap, head.clamp(HEADER_SIZE, cap))
        };

        let region = Arc::new(MmapRegion::map_read_only(&file, capacity)?);
        Ok(Self {
            segment_id,
            path,
            writer: Mutex::new(SegmentWriter {
                file,
                write_head,
                capacity,
                max_capacity: max_capacity.max(capacity),
                dead_bytes: 0,
            }),
            region: RwLock::new(region),
        })
    }

    /// Remaining bytes before this segment reaches its rollover ceiling (`max_capacity`).
    #[inline]
    pub fn remaining_bytes(&self) -> usize {
        let w = self
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        w.max_capacity.saturating_sub(w.write_head)
    }

    #[inline]
    pub fn dead_bytes(&self) -> usize {
        self.writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .dead_bytes
    }

    #[inline]
    pub fn live_bytes(&self) -> usize {
        let w = self
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        w.write_head
            .saturating_sub(HEADER_SIZE)
            .saturating_sub(w.dead_bytes)
    }

    #[inline]
    pub fn fragmentation_ratio(&self) -> f64 {
        let w = self
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let used = w.write_head.saturating_sub(HEADER_SIZE);
        if used == 0 {
            0.0
        } else {
            (w.dead_bytes as f64) / (used as f64)
        }
    }

    /// Appends a 64-byte-aligned record `frame` to this segment, growing the underlying file
    /// in tight powers-of-two from `4 KiB` up to `max_capacity`.
    pub fn append_frame(&self, frame: &[u8], sync_data: bool) -> Result<ArenaLocation> {
        let aligned_len = frame.len().next_multiple_of(HEADER_SIZE);
        let mut w = self
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let offset = w.write_head;
        let next_head = offset
            .checked_add(aligned_len)
            .ok_or_else(|| StorageError::corrupted_header("arena offset overflow"))?;

        if next_head > w.capacity {
            let new_cap = next_head
                .max(w.capacity.saturating_mul(2))
                .next_multiple_of(INITIAL_SEGMENT_BYTES);
            w.file
                .set_len(new_cap as u64)
                .map_err(|e| StorageError::io("grow arena segment file", e))?;
            w.capacity = new_cap;
            w.max_capacity = w.max_capacity.max(new_cap);
            let new_region = Arc::new(MmapRegion::map_read_only(&w.file, w.capacity)?);
            *self
                .region
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = new_region;
        }

        w.file
            .seek(SeekFrom::Start(offset as u64))
            .map_err(|e| StorageError::io("seek arena append offset", e))?;
        w.file
            .write_all(frame)
            .map_err(|e| StorageError::io("write frame to arena segment", e))?;

        // Update persisted write_head in superblock bytes 12..20
        w.write_head = next_head;
        w.file
            .seek(SeekFrom::Start(12))
            .map_err(|e| StorageError::io("seek arena superblock head", e))?;
        w.file
            .write_all(&(next_head as u64).to_le_bytes())
            .map_err(|e| StorageError::io("update arena superblock head", e))?;

        if sync_data {
            w.file
                .sync_data()
                .map_err(|e| StorageError::io("fsync arena segment", e))?;
        }

        Ok(ArenaLocation {
            segment_id: self.segment_id,
            offset: offset as u32,
            len: frame.len() as u32,
        })
    }

    #[inline]
    pub fn mark_dead(&self, len: u32) {
        let aligned = (len as usize).next_multiple_of(HEADER_SIZE);
        let mut w = self
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        w.dead_bytes = w.dead_bytes.saturating_add(aligned);
    }

    #[inline]
    pub fn mapped_region(&self) -> Arc<MmapRegion> {
        Arc::clone(
            &self
                .region
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }
}

/// Manages active and sealed Slotted Arena Segments (`F-CORE-06`) using a flat dense `Vec`
/// indexed by `segment_id` for $O(1)$ 1-cycle read lookups without `HashMap` hashing.
#[derive(Debug)]
pub struct ArenaManager {
    base_dir: PathBuf,
    segment_capacity: usize,
    /// Dense table indexed directly by `segment_id as usize` (index 0 unused).
    segments: RwLock<Vec<Option<Arc<ArenaSegment>>>>,
    active_segment_id: Mutex<u32>,
}

impl ArenaManager {
    pub fn open(base_dir: impl Into<PathBuf>, segment_capacity: usize) -> Result<Self> {
        let base_dir = base_dir.into();
        let seg_dir = base_dir.join("segments");
        if !seg_dir.exists() {
            fs::create_dir_all(&seg_dir)
                .map_err(|e| StorageError::io("create segments directory", e))?;
        }

        let segment_capacity = segment_capacity.max(INITIAL_SEGMENT_BYTES);
        let mut table: Vec<Option<Arc<ArenaSegment>>> = vec![None, None];
        let mut max_id = 0u32;

        if let Ok(entries) = fs::read_dir(&seg_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Some(hex_id) = stem.strip_prefix("seg_") {
                        if let Ok(id) = hex_id.parse::<u32>() {
                            if id > 0 {
                                let seg =
                                    ArenaSegment::open_or_create(id, path, segment_capacity)?;
                                max_id = max_id.max(id);
                                let idx = id as usize;
                                if table.len() <= idx {
                                    table.resize_with(idx + 1, || None);
                                }
                                table[idx] = Some(Arc::new(seg));
                            }
                        }
                    }
                }
            }
        }

        if max_id == 0 {
            let path = seg_dir.join("seg_00001.dat");
            let seg = ArenaSegment::open_or_create(1, path, segment_capacity)?;
            table[1] = Some(Arc::new(seg));
            max_id = 1;
        }

        Ok(Self {
            base_dir,
            segment_capacity,
            segments: RwLock::new(table),
            active_segment_id: Mutex::new(max_id),
        })
    }

    /// Writes `frame` into the current active Arena Segment, rolling over to a new segment
    /// when `segment_capacity` is reached.
    pub fn write_record(&self, frame: &[u8], sync_data: bool) -> Result<ArenaLocation> {
        let aligned_len = frame.len().next_multiple_of(HEADER_SIZE);
        let mut active_id = self
            .active_segment_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let seg_arc = {
            let segs = self
                .segments
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            segs.get(*active_id as usize).and_then(|opt| opt.clone())
        };

        if let Some(seg) = seg_arc {
            if seg.remaining_bytes() >= aligned_len {
                return seg.append_frame(frame, sync_data);
            }
        }

        // Roll over to a new segment
        *active_id += 1;
        let new_id = *active_id;
        let path = self
            .base_dir
            .join("segments")
            .join(format!("seg_{new_id:05}.dat"));
        let new_seg =
            ArenaSegment::open_or_create(new_id, path, self.segment_capacity.max(aligned_len * 2))?;
        let loc = new_seg.append_frame(frame, sync_data)?;

        let mut segs = self
            .segments
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let idx = new_id as usize;
        if segs.len() <= idx {
            segs.resize_with(idx + 1, || None);
        }
        segs[idx] = Some(Arc::new(new_seg));
        Ok(loc)
    }

    /// Reads a zero-copy `MmapRegion` for `segment_id` via $O(1)$ slice indexing.
    #[inline]
    pub fn get_region(&self, segment_id: u32) -> Result<Arc<MmapRegion>> {
        let segs = self
            .segments
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let seg = segs
            .get(segment_id as usize)
            .and_then(|opt| opt.as_ref())
            .ok_or_else(|| StorageError::corrupted_header("referenced arena segment not found"))?;
        Ok(seg.mapped_region())
    }

    /// Zero-clone, zero-allocation borrowed slice access inside an Arena Segment (`1` array index lookup!).
    #[inline]
    pub fn with_slice<R>(&self, loc: ArenaLocation, f: impl FnOnce(&[u8]) -> Result<R>) -> Result<R> {
        let segs = self
            .segments
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let seg = segs
            .get(loc.segment_id as usize)
            .and_then(|opt| opt.as_ref())
            .ok_or_else(|| StorageError::corrupted_header("referenced arena segment not found"))?;
        let reg = seg
            .region
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let slice = reg.subslice_aligned(loc.offset as usize, loc.len as usize)?;
        f(slice)
    }

    /// Marks an old `ArenaLocation` as dead space so the compactor can reclaim it.
    #[inline]
    pub fn mark_dead(&self, loc: ArenaLocation) {
        let segs = self
            .segments
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(Some(seg)) = segs.get(loc.segment_id as usize) {
            seg.mark_dead(loc.len);
        }
    }

    /// Returns all segment IDs whose dead-byte fragmentation ratio meets or exceeds `min_ratio`.
    pub fn fragmented_segment_ids(&self, min_ratio: f64) -> Vec<u32> {
        let segs = self
            .segments
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut out = Vec::new();
        for seg in segs.iter().flatten() {
            if seg.dead_bytes() > 0 && seg.fragmentation_ratio() >= min_ratio {
                out.push(seg.segment_id);
            }
        }
        out
    }

    /// Forces `ArenaManager` to roll over to a fresh active segment, returning the new `segment_id`.
    /// Used before compacting fragmented segments so evacuated records are written to a clean segment.
    pub fn roll_active_segment(&self) -> Result<u32> {
        let mut active_id = self
            .active_segment_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *active_id += 1;
        let new_id = *active_id;
        let path = self
            .base_dir
            .join("segments")
            .join(format!("seg_{new_id:05}.dat"));
        let new_seg = ArenaSegment::open_or_create(new_id, path, self.segment_capacity)?;
        let mut segs = self
            .segments
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let idx = new_id as usize;
        if segs.len() <= idx {
            segs.resize_with(idx + 1, || None);
        }
        segs[idx] = Some(Arc::new(new_seg));
        Ok(new_id)
    }

    /// Removes a compacted segment from the manager and deletes its backing `.dat` file.
    pub fn remove_segment(&self, segment_id: u32) -> bool {
        let removed = {
            let mut segs = self
                .segments
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            segs.get_mut(segment_id as usize).and_then(|slot| slot.take())
        };
        if let Some(seg) = removed {
            let path = seg.path.clone();
            drop(seg);
            let _ = fs::remove_file(path);
            true
        } else {
            false
        }
    }

    pub fn segment_count(&self) -> usize {
        self.segments
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|s| s.is_some())
            .count()
    }
}
