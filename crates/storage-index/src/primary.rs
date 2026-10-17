//! 64-shard Primary Index with `HotDirtyArc` (`AlignedBytes`) in-flight visibility (`TASK-011`, `F-INDX-01`, `F-CONC-02`).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;

use storage_codec::AlignedBytes;
use storage_mmap::ArenaLocation;
use storage_types::{CachePadded, FastHashMap, InlineId, RecordOrdinal, Version};

const PRIMARY_SHARDS: usize = 64;
const SHARD_MASK: usize = PRIMARY_SHARDS - 1;

/// Physical on-disk location of a persisted record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiskLocation {
    /// Record is currently only in `hot_dirty` RAM and WAL (not yet flushed by background worker).
    Unflushed,
    /// Dedicated record file (`records/<coll>/<h1>/<h2>/<id>.rec` or `groups/<coll>/...`).
    DedicatedFile {
        parent_group: Option<InlineId>,
        ping_pong_stride: u32,
    },
    /// Packed inside a Slotted Arena Segment (`segments/seg_XXXXX.dat`).
    Arena(ArenaLocation),
}

/// Metadata entry stored in the Primary Index for each live record.
#[derive(Clone, Debug)]
pub struct PrimaryEntry {
    pub id: InlineId,
    pub ordinal: RecordOrdinal,
    pub version: Version,
    pub ttl_expires_at_ns: u64,
    pub location: DiskLocation,
    /// `true` only once the on-disk frame at `location` has been fully validated for the
    /// engine's record type, is uncompressed, and matches the current schema. Hot read paths
    /// may only use `decode_unchecked` on arena bytes when this is set (`RULE-SAFE-04`).
    pub disk_verified: bool,
    /// When `Some`, holds the latest 64-byte-aligned serialized frame in memory waiting for
    /// background coalesced disk flush. Readers immediately read this zero-copy buffer!
    pub hot_dirty: Option<AlignedBytes>,
}

#[derive(Default)]
struct PrimaryShard {
    by_id: FastHashMap<InlineId, PrimaryEntry>,
}

/// Dense ordinal table with a free list so deleted ordinals are recycled and the table
/// never grows beyond the peak number of simultaneously live records.
#[derive(Default)]
struct OrdinalTable {
    ids: Vec<Option<InlineId>>,
    free: Vec<u32>,
}

/// 64-shard concurrent Primary Key Index (`InlineId <-> RecordOrdinal`).
pub struct PrimaryIndex {
    shards: Box<[CachePadded<RwLock<PrimaryShard>>; PRIMARY_SHARDS]>,
    ordinals: RwLock<OrdinalTable>,
    live_count: CachePadded<AtomicUsize>,
    hot_dirty_bytes: CachePadded<AtomicUsize>,
}

impl core::fmt::Debug for PrimaryIndex {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PrimaryIndex")
            .field("live_count", &self.len())
            .field("hot_dirty_bytes", &self.hot_dirty_bytes())
            .finish()
    }
}

impl Default for PrimaryIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl PrimaryIndex {
    pub fn new() -> Self {
        let shards_vec: Vec<CachePadded<RwLock<PrimaryShard>>> = (0..PRIMARY_SHARDS)
            .map(|_| CachePadded::new(RwLock::new(PrimaryShard::default())))
            .collect();
        let shards = shards_vec
            .into_boxed_slice()
            .try_into()
            .unwrap_or_else(|_| unreachable!());

        Self {
            shards,
            ordinals: RwLock::new(OrdinalTable::default()),
            live_count: CachePadded::new(AtomicUsize::new(0)),
            hot_dirty_bytes: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    #[inline]
    fn shard_idx(id: InlineId) -> usize {
        (id.fast_hash() as usize) & SHARD_MASK
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.live_count.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn hot_dirty_bytes(&self) -> usize {
        self.hot_dirty_bytes.load(Ordering::Relaxed)
    }

    /// Allocates a dense 32-bit `RecordOrdinal`, reusing a released ordinal when available.
    pub fn allocate_ordinal(&self, id: InlineId) -> RecordOrdinal {
        let mut table = self
            .ordinals
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let raw = match table.free.pop() {
            Some(raw) => raw,
            None => {
                let raw = u32::try_from(table.ids.len()).unwrap_or(u32::MAX);
                table.ids.push(None);
                raw
            }
        };
        if let Some(slot) = table.ids.get_mut(raw as usize) {
            *slot = Some(id);
        }
        RecordOrdinal(raw)
    }

    /// Makes an ordinal previously freed by `remove` reusable in $O(1)$ time. Callers must invoke
    /// this only after the ordinal has been removed from every secondary index, so a new record
    /// can never inherit (or have wiped) postings that belong to the deleted record.
    pub fn release_ordinal(&self, ord: RecordOrdinal) {
        let mut table = self
            .ordinals
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(slot) = table.ids.get_mut(ord.as_usize()) {
            if matches!(slot, Some(id) if id.is_empty()) {
                *slot = None;
                table.free.push(ord.0);
            }
        }
    }

    /// Resolves a `RecordOrdinal` back to its `InlineId` in $O(1)$ array indexing.
    #[inline]
    pub fn resolve_ordinal(&self, ord: RecordOrdinal) -> Option<InlineId> {
        let table = self
            .ordinals
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        table
            .ids
            .get(ord.as_usize())
            .copied()
            .flatten()
            .filter(|id| !id.is_empty())
    }

    /// Resolves a slice of `RecordOrdinal`s in a single read lock acquisition.
    #[inline]
    pub fn resolve_ordinals_batch(&self, ordinals: &[RecordOrdinal]) -> Vec<InlineId> {
        let table = self
            .ordinals
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut out = Vec::with_capacity(ordinals.len());
        for &ord in ordinals {
            if let Some(Some(id)) = table.ids.get(ord.as_usize()) {
                if !id.is_empty() {
                    out.push(*id);
                }
            }
        }
        out
    }

    /// Fast-path lookup returning a clone of `PrimaryEntry` (where `AlignedBytes` is a cheap `Arc` bump).
    #[inline]
    pub fn get(&self, id: InlineId) -> Option<PrimaryEntry> {
        let s_idx = Self::shard_idx(id);
        let shard = self.shards[s_idx]
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        shard.by_id.get(&id).cloned()
    }

    /// Returns only the current `Version` of `id` without cloning the entry (OCC checks).
    #[inline]
    pub fn version_of(&self, id: InlineId) -> Option<Version> {
        self.with_entry(id, |e| e.version)
    }

    /// Runs a zero-allocation read closure `f` while holding only the single shard's read lock (`< 25 ns`).
    #[inline]
    pub fn with_entry<R>(&self, id: InlineId, f: impl FnOnce(&PrimaryEntry) -> R) -> Option<R> {
        let s_idx = Self::shard_idx(id);
        let shard = self.shards[s_idx]
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        shard.by_id.get(&id).map(f)
    }

    /// Inserts or updates a `PrimaryEntry`, tracking `live_count` and `hot_dirty_bytes`.
    pub fn upsert(&self, entry: PrimaryEntry) -> Option<PrimaryEntry> {
        let s_idx = Self::shard_idx(entry.id);
        let new_dirty_len = entry.hot_dirty.as_ref().map_or(0, |b| b.len());
        let mut shard = self.shards[s_idx]
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let prev = shard.by_id.insert(entry.id, entry);
        match &prev {
            Some(old) => {
                let old_dirty_len = old.hot_dirty.as_ref().map_or(0, |b| b.len());
                if new_dirty_len >= old_dirty_len {
                    self.hot_dirty_bytes
                        .fetch_add(new_dirty_len - old_dirty_len, Ordering::Relaxed);
                } else {
                    self.hot_dirty_bytes
                        .fetch_sub(old_dirty_len - new_dirty_len, Ordering::Relaxed);
                }
            }
            None => {
                self.live_count.fetch_add(1, Ordering::Relaxed);
                if new_dirty_len > 0 {
                    self.hot_dirty_bytes
                        .fetch_add(new_dirty_len, Ordering::Relaxed);
                }
            }
        }
        prev
    }

    /// Atomically updates `location` and clears `hot_dirty` ONLY IF the record's current version
    /// still matches `flushed_version` (if a newer write arrived while the background flusher was
    /// writing to disk, `hot_dirty` keeps the newer version!).
    pub fn complete_background_flush(
        &self,
        id: InlineId,
        flushed_version: Version,
        new_location: DiskLocation,
        disk_verified: bool,
    ) -> Option<DiskLocation> {
        let s_idx = Self::shard_idx(id);
        let mut shard = self.shards[s_idx]
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = shard.by_id.get_mut(&id)?;
        let prev_loc = entry.location;
        entry.location = new_location;
        entry.disk_verified = disk_verified;

        if entry.version == flushed_version {
            if let Some(removed_buf) = entry.hot_dirty.take() {
                self.hot_dirty_bytes
                    .fetch_sub(removed_buf.len(), Ordering::Relaxed);
            }
        }
        Some(prev_loc)
    }

    /// Marks the on-disk frame of `id` as validated, but only if it is still at `location`.
    pub fn mark_disk_verified(&self, id: InlineId, location: DiskLocation) {
        let s_idx = Self::shard_idx(id);
        let mut shard = self.shards[s_idx]
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(entry) = shard.by_id.get_mut(&id) {
            if entry.location == location {
                entry.disk_verified = true;
            }
        }
    }

    /// Removes `id` from the primary index. Its ordinal becomes reusable only after
    /// `release_ordinal` is called once secondary-index cleanup has completed.
    pub fn remove(&self, id: InlineId) -> Option<PrimaryEntry> {
        let s_idx = Self::shard_idx(id);
        let removed = {
            let mut shard = self.shards[s_idx]
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            shard.by_id.remove(&id)
        };
        if let Some(ref entry) = removed {
            self.live_count.fetch_sub(1, Ordering::Relaxed);
            if let Some(ref dirty) = entry.hot_dirty {
                self.hot_dirty_bytes
                    .fetch_sub(dirty.len(), Ordering::Relaxed);
            }
            {
                let mut table = self
                    .ordinals
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if let Some(slot) = table.ids.get_mut(entry.ordinal.as_usize()) {
                    *slot = Some(InlineId::EMPTY);
                }
            }
        }
        removed
    }

    /// Collects a snapshot of all live entries across all 64 shards (for checkpointing or iteration).
    pub fn entries_snapshot(&self) -> Vec<PrimaryEntry> {
        let mut out = Vec::with_capacity(self.len());
        for shard_lock in self.shards.iter() {
            let shard = shard_lock
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            out.extend(shard.by_id.values().cloned());
        }
        out
    }

    /// Collects only the live primary keys (cheaper than `entries_snapshot` when only IDs are needed).
    pub fn ids_snapshot(&self) -> Vec<InlineId> {
        let mut out = Vec::with_capacity(self.len());
        for shard_lock in self.shards.iter() {
            let shard = shard_lock
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            out.extend(shard.by_id.keys().copied());
        }
        out
    }

    /// Builds a `CompressedBitmap` of all currently live `RecordOrdinal`s from the dense ordinal table (`TASK-055`).
    pub fn live_ordinals_bitmap(&self) -> crate::bitmap::CompressedBitmap {
        let table = self
            .ordinals
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut bm = crate::bitmap::CompressedBitmap::new();
        for (idx, slot) in table.ids.iter().enumerate() {
            if let Some(id) = slot {
                if !id.is_empty() {
                    bm.insert(RecordOrdinal(idx as u32));
                }
            }
        }
        bm
    }

    /// Scans live `(RecordOrdinal, InlineId)` pairs from the dense ordinal table with $O(1)$ array-indexed
    /// keyset cursor seeking, `offset`, `limit`, and ascending/descending order (`TASK-055`).
    pub fn scan_ordinals_paginated<F: Fn(RecordOrdinal, InlineId) -> bool>(
        &self,
        descending: bool,
        after_ordinal: Option<RecordOrdinal>,
        offset: usize,
        limit: Option<usize>,
        filter_fn: F,
    ) -> (Vec<(RecordOrdinal, InlineId)>, usize) {
        let table = self
            .ordinals
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let len = table.ids.len();
        if len == 0 {
            return (Vec::new(), 0);
        }
        let max_items = limit.unwrap_or(usize::MAX);
        let mut out = Vec::with_capacity(max_items.min(self.len().min(256)));
        let mut total_matched = 0usize;
        let mut skipped = 0usize;

        if !descending {
            let start_idx = match after_ordinal {
                Some(cur) => (cur.0 as usize).saturating_add(1),
                None => 0,
            };
            if start_idx >= len {
                return (Vec::new(), 0);
            }
            for (idx, slot) in table.ids[start_idx..].iter().enumerate() {
                if let Some(id) = slot {
                    if !id.is_empty() {
                        let ord = RecordOrdinal((start_idx + idx) as u32);
                        if !filter_fn(ord, *id) {
                            continue;
                        }
                        total_matched += 1;
                        if skipped < offset {
                            skipped += 1;
                            continue;
                        }
                        if out.len() < max_items {
                            out.push((ord, *id));
                        }
                    }
                }
            }
        } else {
            let end_exclusive = match after_ordinal {
                Some(cur) => (cur.0 as usize).min(len),
                None => len,
            };
            if end_exclusive == 0 {
                return (Vec::new(), 0);
            }
            for (idx, slot) in table.ids[..end_exclusive].iter().enumerate().rev() {
                if let Some(id) = slot {
                    if !id.is_empty() {
                        let ord = RecordOrdinal(idx as u32);
                        if !filter_fn(ord, *id) {
                            continue;
                        }
                        total_matched += 1;
                        if skipped < offset {
                            skipped += 1;
                            continue;
                        }
                        if out.len() < max_items {
                            out.push((ord, *id));
                        }
                    }
                }
            }
        }
        (out, total_matched)
    }

    /// Restores an entry loaded from a checkpoint on startup. A fresh dense ordinal is always
    /// allocated (the on-disk checkpoint never dictates ordinal values, so a corrupted or
    /// malicious `primary.idx` cannot force a huge ordinal-table allocation).
    pub fn restore_entry(&self, mut entry: PrimaryEntry) {
        if let Some(existing) = self.with_entry(entry.id, |e| e.ordinal) {
            entry.ordinal = existing;
        } else {
            entry.ordinal = self.allocate_ordinal(entry.id);
        }
        self.upsert(entry);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: InlineId) -> PrimaryEntry {
        PrimaryEntry {
            id,
            ordinal: RecordOrdinal(0),
            version: Version::INITIAL,
            ttl_expires_at_ns: 0,
            location: DiskLocation::Unflushed,
            disk_verified: false,
            hot_dirty: None,
        }
    }

    #[test]
    fn test_ordinals_are_recycled_after_delete() {
        let idx = PrimaryIndex::new();
        let a = InlineId::new("a").unwrap();
        let b = InlineId::new("b").unwrap();
        idx.restore_entry(entry(a));
        let ord_a = idx.get(a).unwrap().ordinal;
        idx.remove(a);
        // Not reusable until secondary-index cleanup has finished.
        idx.restore_entry(entry(b));
        assert_ne!(idx.get(b).unwrap().ordinal, ord_a);
        idx.release_ordinal(ord_a);
        idx.restore_entry(entry(a));
        assert_eq!(idx.get(a).unwrap().ordinal, ord_a);
        assert_eq!(idx.resolve_ordinal(ord_a), Some(a));
    }
}
