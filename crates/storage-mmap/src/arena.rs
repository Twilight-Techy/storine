//! 64 MB Slotted Arena Segments (`seg_XXXXX.dat`) & Compactor for sub-4KB records (`TASK-025`, `F-CORE-06`).
//!
//! Solves OS inode and file-descriptor exhaustion at 10M+ record scale by packing small records
//! at 64-byte cache-line-aligned offsets inside shared memory-mapped segment files.

use std::fs::{self, File, OpenOptions};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use storage_types::{FastHashMap, RecordHeader, Result, StorageError, HEADER_SIZE};

use crate::raw::MmapRegion;

/// Default maximum Arena Segment rollover ceiling (`4 MiB` default, configurable up to `64 MiB+`).
pub const DEFAULT_SEGMENT_CAPACITY: usize = 4 * 1024 * 1024;
/// Initial physical file allocation size on disk (`4 KiB` = 1 OS page) so small databases
/// have a tiny 4 KiB disk footprint and grow exponentially without upfront bloat.
const INITIAL_SEGMENT_BYTES: usize = 4096;
const SEGMENT_MAGIC: &[u8; 8] = b"STGESEG1";
/// Number of append operations between lazy superblock `write_head` flushes.
/// On startup, `ArenaSegment::open_or_create` fast-forwards across valid 64B-aligned `RecordHeader`
/// frames beyond the last persisted superblock `write_head`, eliminating 75% of arena write syscalls.
const SUPERBLOCK_SYNC_INTERVAL: u32 = 64;

#[inline]
fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> std::io::Result<()> {
    while !buf.is_empty() {
        #[cfg(windows)]
        let n = std::os::windows::fs::FileExt::seek_write(file, buf, offset)?;
        #[cfg(unix)]
        let n = std::os::unix::fs::FileExt::write_at(file, buf, offset)?;
        #[cfg(not(any(windows, unix)))]
        let n = {
            use std::io::{Seek, SeekFrom, Write};
            let mut f = file;
            f.seek(SeekFrom::Start(offset))?;
            f.write(buf)?
        };
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "positional write_all_at wrote 0 bytes",
            ));
        }
        buf = &buf[n..];
        offset += n as u64;
    }
    Ok(())
}

#[inline]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    while !buf.is_empty() {
        #[cfg(windows)]
        let n = std::os::windows::fs::FileExt::seek_read(file, buf, offset)?;
        #[cfg(unix)]
        let n = std::os::unix::fs::FileExt::read_at(file, buf, offset)?;
        #[cfg(not(any(windows, unix)))]
        let n = {
            use std::io::{Read, Seek, SeekFrom};
            let mut f = file;
            f.seek(SeekFrom::Start(offset))?;
            f.read(buf)?
        };
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "positional read_exact_at hit EOF",
            ));
        }
        buf = &mut buf[n..];
        offset += n as u64;
    }
    Ok(())
}

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
    persisted_head: usize,
    unsynced_appends: u32,
    capacity: usize,
    max_capacity: usize,
    dead_bytes: usize,
    /// Recycled dead slots bucketed by 64B-aligned size class (`aligned_len -> Vec<offset>`) (`TASK-052`).
    free_slots: FastHashMap<u32, Vec<u32>>,
}

/// A single memory-mapped Arena Segment file (`seg_XXXXX.dat`).
#[derive(Debug)]
pub struct ArenaSegment {
    pub segment_id: u32,
    pub path: PathBuf,
    writer: Mutex<SegmentWriter>,
    region: RwLock<Arc<MmapRegion>>,
}

impl Drop for ArenaSegment {
    fn drop(&mut self) {
        if let Ok(mut w) = self.writer.lock() {
            if w.write_head != w.persisted_head {
                let _ = write_all_at(&w.file, &(w.write_head as u64).to_le_bytes(), 12);
                w.persisted_head = w.write_head;
            }
        }
    }
}

impl ArenaSegment {
    pub fn open_or_create(segment_id: u32, path: PathBuf, max_capacity: usize) -> Result<Self> {
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)
                    .map_err(|e| StorageError::io("create arena directory", e))?;
            }
        }
        let file = OpenOptions::new()
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
            // Write 64-byte segment superblock at offset 0 in 1 positional syscall
            let mut superblock = [0u8; HEADER_SIZE];
            superblock[0..8].copy_from_slice(SEGMENT_MAGIC);
            superblock[8..12].copy_from_slice(&segment_id.to_le_bytes());
            superblock[12..20].copy_from_slice(&(HEADER_SIZE as u64).to_le_bytes());
            write_all_at(&file, &superblock, 0)
                .map_err(|e| StorageError::io("write arena superblock", e))?;
            (init_cap, HEADER_SIZE)
        } else {
            let cap = current_len.next_multiple_of(HEADER_SIZE);
            if cap > current_len {
                file.set_len(cap as u64)
                    .map_err(|e| StorageError::io("align arena segment length", e))?;
            }
            let mut sb = [0u8; HEADER_SIZE];
            read_exact_at(&file, &mut sb, 0)
                .map_err(|e| StorageError::io("read arena superblock", e))?;
            if &sb[0..8] != SEGMENT_MAGIC {
                return Err(StorageError::corrupted_header(
                    "invalid arena segment superblock magic",
                ));
            }
            let persisted_head = u64::from_le_bytes(sb[12..20].try_into().unwrap()) as usize;
            let mut cursor = persisted_head
                .clamp(HEADER_SIZE, cap)
                .next_multiple_of(HEADER_SIZE);

            // Fast-forward recovery (`TASK-051`): scan forward across any valid checksummed
            // `RecordHeader` frames written after the last lazy superblock update.
            let mut hdr_buf = [0u8; HEADER_SIZE];
            while cursor + HEADER_SIZE <= cap {
                if read_exact_at(&file, &mut hdr_buf, cursor as u64).is_err() {
                    break;
                }
                let Ok(hdr) = RecordHeader::from_slice_verified(&hdr_buf) else {
                    break;
                };
                if hdr.updated_at_ns == 0 && hdr.payload_len == 0 {
                    break;
                }
                let frame_len = HEADER_SIZE.saturating_add(hdr.payload_len as usize);
                let aligned_step = frame_len.next_multiple_of(HEADER_SIZE);
                if aligned_step == 0 || cursor + aligned_step > cap {
                    break;
                }
                cursor += aligned_step;
            }
            if cursor != persisted_head {
                let _ = write_all_at(&file, &(cursor as u64).to_le_bytes(), 12);
            }
            (cap, cursor)
        };

        let region = Arc::new(MmapRegion::map_read_only(&file, capacity)?);
        Ok(Self {
            segment_id,
            path,
            writer: Mutex::new(SegmentWriter {
                file,
                write_head,
                persisted_head: write_head,
                unsynced_appends: 0,
                capacity,
                max_capacity: max_capacity.max(capacity),
                dead_bytes: 0,
                free_slots: FastHashMap::default(),
            }),
            region: RwLock::new(region),
        })
    }

    /// Remaining bytes before this segment reaches its rollover ceiling (`max_capacity`),
    /// or returns at least `aligned_len` if a recycled slot of that exact size class is available.
    #[inline]
    pub fn can_accommodate(&self, aligned_len: usize) -> bool {
        let w = self
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if w.free_slots
            .get(&(aligned_len as u32))
            .map_or(false, |list| !list.is_empty())
        {
            return true;
        }
        w.max_capacity.saturating_sub(w.write_head) >= aligned_len
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

    /// Appends a 64-byte-aligned record `frame` to this segment (or reuses a recycled dead slot
    /// of the exact same 64B size class in-place via `free_slots`), using a single positional
    /// `write_all_at` syscall (`TASK-051`, `TASK-052`).
    pub fn append_frame(&self, frame: &[u8], sync_data: bool) -> Result<ArenaLocation> {
        let aligned_len = frame.len().next_multiple_of(HEADER_SIZE);
        let mut w = self
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // Fast Path (`TASK-052`): Reuse a recycled dead slot of the same 64B size class in-place!
        if let Some(reused_offset) = w
            .free_slots
            .get_mut(&(aligned_len as u32))
            .and_then(|slots| slots.pop())
        {
            write_all_at(&w.file, frame, reused_offset as u64)
                .map_err(|e| StorageError::io("write frame to recycled arena slot", e))?;
            w.dead_bytes = w.dead_bytes.saturating_sub(aligned_len);
            if sync_data {
                w.file
                    .sync_data()
                    .map_err(|e| StorageError::io("fsync arena segment", e))?;
            }
            return Ok(ArenaLocation {
                segment_id: self.segment_id,
                offset: reused_offset,
                len: frame.len() as u32,
            });
        }

        let offset = w.write_head;
        let next_head = offset
            .checked_add(aligned_len)
            .ok_or_else(|| StorageError::corrupted_header("arena offset overflow"))?;

        let mut grew_capacity = false;
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
            grew_capacity = true;
        }

        // Single positional write syscall (`TASK-051`)
        write_all_at(&w.file, frame, offset as u64)
            .map_err(|e| StorageError::io("write frame to arena segment", e))?;

        w.write_head = next_head;
        w.unsynced_appends = w.unsynced_appends.wrapping_add(1);

        // Lazily update superblock `write_head` on segment growth, explicit sync, or every 64 appends
        if grew_capacity || sync_data || w.unsynced_appends >= SUPERBLOCK_SYNC_INTERVAL {
            write_all_at(&w.file, &(next_head as u64).to_le_bytes(), 12)
                .map_err(|e| StorageError::io("update arena superblock head", e))?;
            w.persisted_head = next_head;
            w.unsynced_appends = 0;
        }

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

    /// Marks an old slot at `(offset, len)` as dead AND pushes its 64B-aligned coordinate into
    /// the segment's size-class free list for immediate in-place reuse (`TASK-052`).
    #[inline]
    pub fn reclaim_dead_slot(&self, offset: u32, len: u32) {
        let aligned = (len as usize).next_multiple_of(HEADER_SIZE);
        let mut w = self
            .writer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        w.dead_bytes = w.dead_bytes.saturating_add(aligned);
        w.free_slots
            .entry(aligned as u32)
            .or_default()
            .push(offset);
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
            if seg.can_accommodate(aligned_len) {
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

    /// Marks an old `ArenaLocation` as dead AND recycles its 64B-aligned slot into the segment's
    /// size-class free list for immediate in-place reuse (`TASK-052`).
    #[inline]
    pub fn reclaim_dead_slot(&self, loc: ArenaLocation) {
        let segs = self
            .segments
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(Some(seg)) = segs.get(loc.segment_id as usize) {
            seg.reclaim_dead_slot(loc.offset, loc.len);
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

#[cfg(test)]
mod tests {
    use super::*;
    use storage_types::{RecordFlags, Version};

    #[test]
    fn test_arena_positional_write_fast_forward_recovery_and_slot_reuse() {
        let dir = std::env::temp_dir().join("storage_arena_opt_test");
        let _ = fs::remove_dir_all(&dir);
        let seg_path = dir.join("seg_00001.dat");

        // Create a valid 128-byte (64B header + 64B payload) frame
        let payload = [0x42u8; 64];
        let hdr = RecordHeader::new(
            1,
            Version(1),
            123_456_789,
            0xDEAD_BEEF,
            payload.len() as u32,
            payload.len() as u32,
            0,
            RecordFlags::NONE,
            1,
            0,
        );
        let mut frame = Vec::with_capacity(128);
        frame.extend_from_slice(&hdr.to_bytes());
        frame.extend_from_slice(&payload);

        let loc1 = {
            let seg = ArenaSegment::open_or_create(1, seg_path.clone(), 64 * 1024).unwrap();
            let l1 = seg.append_frame(&frame, false).unwrap();
            assert_eq!(l1.offset, 64);

            // Reclaim slot l1 (64..192) and verify next append of same 64B size class reuses offset 64!
            seg.reclaim_dead_slot(l1.offset, l1.len);
            assert_eq!(seg.dead_bytes(), 128);

            let l2 = seg.append_frame(&frame, false).unwrap();
            assert_eq!(l2.offset, 64, "expected recycled offset 64 to be reused in-place");
            assert_eq!(seg.dead_bytes(), 0);

            // Simulate power loss before superblock flush by resetting superblock head to 64
            let f = OpenOptions::new().write(true).open(&seg_path).unwrap();
            write_all_at(&f, &64u64.to_le_bytes(), 12).unwrap();
            // Prevent Drop from overwriting our simulated stale superblock head
            std::mem::forget(seg);
            l2
        };

        // Reopen segment: fast-forward header recovery must advance write_head past loc1 (64 + 128 = 192)!
        let reopened = ArenaSegment::open_or_create(1, seg_path, 64 * 1024).unwrap();
        let l3 = reopened.append_frame(&frame, false).unwrap();
        assert_eq!(l3.offset, loc1.offset + 128);

        let _ = fs::remove_dir_all(&dir);
    }
}
