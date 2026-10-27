//! 64-shard Primary Index with `HotDirtyArc` (`AlignedBytes`) in-flight visibility (`TASK-011`, `F-INDX-01`, `F-CONC-02`).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use storage_codec::{AlignedBytes, FileSeal};
use storage_mmap::ArenaLocation;
use storage_types::{
    CachePadded, FastHashMap, FastHashSet, InlineId, RecordOrdinal, Result, StorageError, Version,
};

use crate::cold_tier::ColdTier;
use crate::paged_files::{
    generation_path, read_manifest, remove_stale_generations, temp_generation_path, write_manifest,
};

const COLD_MANIFEST_MAGIC: u32 = 0x4350_4D46; // "FMPC"

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
    /// only use the O(1) `RecordCodec::decode_fast` on arena bytes when this is set (`RULE-SAFE-04`).
    pub disk_verified: bool,
    /// When `Some`, holds the latest 64-byte-aligned serialized frame in memory waiting for
    /// background coalesced disk flush. Readers immediately read this zero-copy buffer!
    pub hot_dirty: Option<AlignedBytes>,
}

#[derive(Default)]
struct PrimaryShard {
    by_id: FastHashMap<InlineId, PrimaryEntry>,
    /// Cold tier only: ids deleted since the tier was built (tombstones over the tier). Never
    /// holds an id that is also in `by_id`.
    removed: FastHashSet<InlineId>,
    /// The cold tier backing this shard (`None` in the memory tier).
    cold: Option<Arc<ColdTier>>,
}

/// Dense ordinal table with a free list so deleted ordinals are recycled and the table
/// never grows beyond the peak number of simultaneously live records.
///
/// Slot states: `None` = free, `Some(InlineId::EMPTY)` = removed but not yet released,
/// `Some(id)` = live. With a cold tier the slots live in the tier file and only slots changed
/// since it was built are kept in RAM (`overrides`).
#[derive(Default)]
struct OrdinalTable {
    ids: Vec<Option<InlineId>>,
    free: Vec<u32>,
    cold: Option<Arc<ColdTier>>,
    overrides: FastHashMap<u32, Option<InlineId>>,
    /// Slot count in cold mode (`ids.len()` otherwise).
    slots: usize,
}

impl OrdinalTable {
    #[inline]
    fn len(&self) -> usize {
        if self.cold.is_some() {
            self.slots
        } else {
            self.ids.len()
        }
    }

    #[inline]
    fn slot(&self, idx: usize) -> Option<InlineId> {
        match self.cold {
            None => self.ids.get(idx).copied().flatten(),
            Some(ref tier) => {
                let ord = u32::try_from(idx).ok()?;
                match self.overrides.get(&ord) {
                    Some(v) => *v,
                    None => tier.resolve(ord),
                }
            }
        }
    }

    /// Sets slot `idx`, growing the table as needed.
    fn set(&mut self, idx: usize, value: Option<InlineId>) -> Result<()> {
        if self.cold.is_some() {
            let ord = u32::try_from(idx).map_err(|_| StorageError::resource_exhausted("ordinal space"))?;
            self.overrides.insert(ord, value);
            self.slots = self.slots.max(idx + 1);
            return Ok(());
        }
        if idx >= self.ids.len() {
            let additional = idx + 1 - self.ids.len();
            self.ids
                .try_reserve(additional)
                .map_err(|_| StorageError::resource_exhausted("ordinal table memory"))?;
            self.ids.resize(idx + 1, None);
        }
        if let Some(slot) = self.ids.get_mut(idx) {
            *slot = value;
        }
        Ok(())
    }

    /// Live `(ordinal, id)` of slot `idx`.
    #[inline]
    fn live(&self, idx: usize) -> Option<InlineId> {
        self.slot(idx).filter(|id| !id.is_empty())
    }
}

/// 64-shard concurrent Primary Key Index (`InlineId <-> RecordOrdinal`).
///
/// **Tiers** (`TASK-092`, `F-PIDX-03`): in the memory tier every entry and ordinal lives in RAM.
/// With a cold tier attached ([`Self::attach_cold_tier`], [`Self::finish_cold_checkpoint`]) the
/// entries and ordinal slots as of the last checkpoint live in an immutable on-disk generation;
/// RAM holds only entries written, deleted or verified since then (plus the tier's separators and
/// Bloom filter). Lookups check RAM, then the shard's tombstones, then the tier.
pub struct PrimaryIndex {
    shards: Box<[CachePadded<RwLock<PrimaryShard>>; PRIMARY_SHARDS]>,
    /// Cache-line padded: its lock word changes on every ordinal resolve / allocate, and must
    /// not share a line with the read-mostly `shards` pointer used by every lookup (`RULE-CPU-01`).
    ordinals: CachePadded<RwLock<OrdinalTable>>,
    live_count: CachePadded<AtomicUsize>,
    hot_dirty_bytes: CachePadded<AtomicUsize>,
    /// `true` once a cold tier has been attached (never reset): the memory tier skips all tier
    /// bookkeeping with one relaxed load.
    tiered: AtomicBool,
}

impl core::fmt::Debug for PrimaryIndex {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PrimaryIndex")
            .field("live_count", &self.len())
            .field("hot_dirty_bytes", &self.hot_dirty_bytes())
            .field("tiered", &self.is_tiered())
            .finish()
    }
}

impl Default for PrimaryIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// State captured by [`PrimaryIndex::begin_cold_checkpoint`]: a consistent cut of the RAM layer.
pub struct ColdCheckpoint {
    /// RAM entries, sorted by `(fast_hash, id)`.
    ram: Vec<PrimaryEntry>,
    /// Tombstones at the cut.
    removed: FastHashSet<InlineId>,
    /// The tier the cut was taken against.
    base: Option<Arc<ColdTier>>,
    /// Ordinal slots changed since `base` (or, before the first tier, the whole table).
    overrides: FastHashMap<u32, Option<InlineId>>,
    mem_ids: Vec<Option<InlineId>>,
    slots: usize,
}

impl core::fmt::Debug for ColdCheckpoint {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ColdCheckpoint")
            .field("ram_entries", &self.ram.len())
            .field("tombstones", &self.removed.len())
            .finish()
    }
}

#[inline]
fn order_key(id: InlineId) -> (u64, InlineId) {
    (id.fast_hash(), id)
}

impl PrimaryIndex {
    pub fn new() -> Self {
        let shards = CachePadded::boxed_array(|| RwLock::new(PrimaryShard::default()));

        Self {
            shards,
            ordinals: CachePadded::new(RwLock::new(OrdinalTable::default())),
            live_count: CachePadded::new(AtomicUsize::new(0)),
            hot_dirty_bytes: CachePadded::new(AtomicUsize::new(0)),
            tiered: AtomicBool::new(false),
        }
    }

    #[inline]
    fn shard_idx(id: InlineId) -> usize {
        (id.fast_hash() as usize) & SHARD_MASK
    }

    #[inline]
    fn read_shard(&self, idx: usize) -> std::sync::RwLockReadGuard<'_, PrimaryShard> {
        self.shards[idx].read().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[inline]
    fn write_shard(&self, idx: usize) -> std::sync::RwLockWriteGuard<'_, PrimaryShard> {
        self.shards[idx].write().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[inline]
    fn read_table(&self) -> std::sync::RwLockReadGuard<'_, OrdinalTable> {
        self.ordinals.read().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[inline]
    fn write_table(&self) -> std::sync::RwLockWriteGuard<'_, OrdinalTable> {
        self.ordinals.write().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// `true` once a cold tier backs this index.
    #[inline]
    pub fn is_tiered(&self) -> bool {
        self.tiered.load(Ordering::Relaxed)
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

    /// Entries resident in RAM (all of them in the memory tier; those changed or verified since
    /// the last checkpoint with a cold tier).
    pub fn resident_entries(&self) -> usize {
        (0..PRIMARY_SHARDS).map(|i| self.read_shard(i).by_id.len()).sum()
    }

    /// RAM used by the cold tier's directory (separators + Bloom filter), `0` without one.
    pub fn cold_tier_ram_bytes(&self) -> usize {
        self.read_table().cold.as_ref().map_or(0, |t| t.ram_bytes())
    }

    /// Allocates a dense 32-bit `RecordOrdinal`, reusing a released ordinal when available.
    pub fn allocate_ordinal(&self, id: InlineId) -> RecordOrdinal {
        let mut table = self.write_table();
        let raw = match table.free.pop() {
            Some(raw) => raw,
            None => u32::try_from(table.len()).unwrap_or(u32::MAX),
        };
        // Growth only fails when the ordinal space or memory is exhausted; the ordinal is then
        // still returned (as before) and simply not resolvable.
        let _ = table.set(raw as usize, Some(id));
        RecordOrdinal(raw)
    }

    /// Makes an ordinal previously freed by `remove` reusable in $O(1)$ time. Callers must invoke
    /// this only after the ordinal has been removed from every secondary index, so a new record
    /// can never inherit (or have wiped) postings that belong to the deleted record.
    pub fn release_ordinal(&self, ord: RecordOrdinal) {
        let mut table = self.write_table();
        if matches!(table.slot(ord.as_usize()), Some(id) if id.is_empty()) {
            let _ = table.set(ord.as_usize(), None);
            table.free.push(ord.0);
        }
    }

    /// Resolves a `RecordOrdinal` back to its `InlineId` in $O(1)$ array indexing.
    #[inline]
    pub fn resolve_ordinal(&self, ord: RecordOrdinal) -> Option<InlineId> {
        self.read_table().live(ord.as_usize())
    }

    /// Resolves a slice of `RecordOrdinal`s in a single read lock acquisition.
    #[inline]
    pub fn resolve_ordinals_batch(&self, ordinals: &[RecordOrdinal]) -> Vec<InlineId> {
        let table = self.read_table();
        let mut out = Vec::with_capacity(ordinals.len());
        for &ord in ordinals {
            if let Some(id) = table.live(ord.as_usize()) {
                out.push(id);
            }
        }
        out
    }

    /// Fast-path lookup returning a clone of `PrimaryEntry` (where `AlignedBytes` is a cheap `Arc` bump).
    #[inline]
    pub fn get(&self, id: InlineId) -> Option<PrimaryEntry> {
        self.with_entry(id, PrimaryEntry::clone)
    }

    /// Returns only the current `Version` of `id` without cloning the entry (OCC checks).
    #[inline]
    pub fn version_of(&self, id: InlineId) -> Option<Version> {
        self.with_entry(id, |e| e.version)
    }

    /// Runs a zero-allocation read closure `f` while holding only the single shard's read lock (`< 25 ns`).
    /// With a cold tier, a RAM miss is answered from the tier after the lock is released (the
    /// tier is immutable; no shard lock is ever held across its page reads, `RULE-CONC-01`).
    #[inline]
    pub fn with_entry<R>(&self, id: InlineId, f: impl FnOnce(&PrimaryEntry) -> R) -> Option<R> {
        let shard = self.read_shard(Self::shard_idx(id));
        if let Some(entry) = shard.by_id.get(&id) {
            return Some(f(entry));
        }
        let tier = shard.cold.clone()?;
        if shard.removed.contains(&id) {
            return None;
        }
        drop(shard);
        tier.get(id).map(|entry| f(&entry))
    }

    /// The live tier copy of `id` as seen by a shard holding `tier` (looked up without locks).
    #[inline]
    fn tier_entry(tier: Option<&Arc<ColdTier>>, id: InlineId) -> Option<PrimaryEntry> {
        tier.and_then(|t| t.get(id))
    }

    /// Runs `apply` on `id`'s shard under its write lock with `tier_copy` = the tier's entry for
    /// `id` (read before locking, retried if the tier was swapped meanwhile).
    fn with_tier_copy<R>(
        &self,
        id: InlineId,
        apply: impl Fn(&mut PrimaryShard, Option<PrimaryEntry>) -> R,
    ) -> R {
        let s_idx = Self::shard_idx(id);
        loop {
            let tier = self.read_shard(s_idx).cold.clone();
            let copy = Self::tier_entry(tier.as_ref(), id);
            let mut shard = self.write_shard(s_idx);
            let same = match (&shard.cold, &tier) {
                (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                (None, None) => true,
                _ => false,
            };
            if same {
                return apply(&mut shard, copy);
            }
        }
    }

    /// Inserts or updates a `PrimaryEntry`, tracking `live_count` and `hot_dirty_bytes`.
    /// Returns the previous RAM entry (a cold-tier copy it shadows is not returned).
    pub fn upsert(&self, entry: PrimaryEntry) -> Option<PrimaryEntry> {
        if !self.is_tiered() {
            let s_idx = Self::shard_idx(entry.id);
            let mut shard = self.write_shard(s_idx);
            return self.insert_locked(&mut shard, entry, false);
        }
        let id = entry.id;
        let slot = std::cell::Cell::new(Some(entry));
        self.with_tier_copy(id, |shard, copy| {
            // A tier copy not tombstoned is the same live record: this is an update.
            let tier_alive = copy.is_some() && !shard.removed.contains(&id);
            shard.removed.remove(&id);
            match slot.take() {
                Some(e) => self.insert_locked(shard, e, tier_alive),
                None => None,
            }
        })
    }

    fn insert_locked(&self, shard: &mut PrimaryShard, entry: PrimaryEntry, tier_alive: bool) -> Option<PrimaryEntry> {
        let new_dirty_len = entry.hot_dirty.as_ref().map_or(0, |b| b.len());
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
                if !tier_alive {
                    self.live_count.fetch_add(1, Ordering::Relaxed);
                }
                if new_dirty_len > 0 {
                    self.hot_dirty_bytes
                        .fetch_add(new_dirty_len, Ordering::Relaxed);
                }
            }
        }
        prev
    }

    /// Publishes a completed flush: updates `location` and clears `hot_dirty`, but ONLY IF the
    /// record still exists at `flushed_version`. Returns `false` when the flush is stale (the
    /// record was rewritten or deleted meanwhile), in which case nothing is changed and the
    /// caller must discard the location it just wrote.
    pub fn complete_background_flush(
        &self,
        id: InlineId,
        flushed_version: Version,
        new_location: DiskLocation,
        disk_verified: bool,
    ) -> bool {
        // Records being flushed carry `hot_dirty`, so they are always resident in RAM.
        let mut shard = self.write_shard(Self::shard_idx(id));
        let Some(entry) = shard.by_id.get_mut(&id) else {
            return false;
        };
        if entry.version != flushed_version {
            return false;
        }
        entry.location = new_location;
        entry.disk_verified = disk_verified;
        if let Some(removed_buf) = entry.hot_dirty.take() {
            self.hot_dirty_bytes
                .fetch_sub(removed_buf.len(), Ordering::Relaxed);
        }
        true
    }

    /// Marks the on-disk frame of `id` as validated, but only if it is still at `location`.
    ///
    /// Tier-only entries are not promoted into RAM (a scan would otherwise pull the whole tier
    /// back into memory): their frames are simply re-validated on each read.
    pub fn mark_disk_verified(&self, id: InlineId, location: DiskLocation) {
        let mut shard = self.write_shard(Self::shard_idx(id));
        if let Some(entry) = shard.by_id.get_mut(&id) {
            if entry.location == location {
                entry.disk_verified = true;
            }
        }
    }

    /// Removes `id` from the primary index. Its ordinal becomes reusable only after
    /// `release_ordinal` is called once secondary-index cleanup has completed.
    pub fn remove(&self, id: InlineId) -> Option<PrimaryEntry> {
        let removed = if self.is_tiered() {
            self.with_tier_copy(id, |shard, copy| {
                let ram = shard.by_id.remove(&id);
                let tier_alive = copy.is_some() && !shard.removed.contains(&id);
                // Tombstone unconditionally: a checkpoint in progress may be writing this id
                // into the next tier (pruned when that tier is installed).
                shard.removed.insert(id);
                ram.or(if tier_alive { copy } else { None })
            })
        } else {
            self.write_shard(Self::shard_idx(id)).by_id.remove(&id)
        };
        if let Some(ref entry) = removed {
            self.live_count.fetch_sub(1, Ordering::Relaxed);
            if let Some(ref dirty) = entry.hot_dirty {
                self.hot_dirty_bytes
                    .fetch_sub(dirty.len(), Ordering::Relaxed);
            }
            let _ = self.write_table().set(entry.ordinal.as_usize(), Some(InlineId::EMPTY));
        }
        removed
    }

    /// Collects a snapshot of all live entries across all 64 shards (for checkpointing or iteration).
    pub fn entries_snapshot(&self) -> Vec<PrimaryEntry> {
        let mut out = Vec::with_capacity(self.len());
        self.for_each_live(|e| out.push(e.clone()));
        out
    }

    /// Collects only the live primary keys (cheaper than `entries_snapshot` when only IDs are needed).
    pub fn ids_snapshot(&self) -> Vec<InlineId> {
        let mut out = Vec::with_capacity(self.len());
        self.for_each_live(|e| out.push(e.id));
        out
    }

    /// Calls `f` for every live entry: RAM entries first, then tier entries they do not shadow.
    fn for_each_live(&self, mut f: impl FnMut(&PrimaryEntry)) {
        if !self.is_tiered() {
            for i in 0..PRIMARY_SHARDS {
                self.read_shard(i).by_id.values().for_each(&mut f);
            }
            return;
        }
        let cut = self.begin_cold_checkpoint();
        for e in &cut.ram {
            f(e);
        }
        if let Some(ref tier) = cut.base {
            tier.for_each_entry(|e| {
                if cut.ram.binary_search_by(|r| order_key(r.id).cmp(&order_key(e.id))).is_err()
                    && !cut.removed.contains(&e.id)
                {
                    f(&e);
                }
            });
        }
    }

    /// Builds a `CompressedBitmap` of all currently live `RecordOrdinal`s from the dense ordinal table (`TASK-055`).
    pub fn live_ordinals_bitmap(&self) -> crate::bitmap::CompressedBitmap {
        let table = self.read_table();
        let mut bm = crate::bitmap::CompressedBitmap::new();
        for idx in 0..table.len() {
            if table.live(idx).is_some() {
                bm.insert(RecordOrdinal(idx as u32));
            }
        }
        bm
    }

    /// Resolves a slice of `InlineId`s (`WHERE id IN (...)`) into a `CompressedBitmap` of their live `RecordOrdinal`s (`TASK-057`).
    pub fn ids_to_bitmap(&self, ids: &[InlineId]) -> crate::bitmap::CompressedBitmap {
        let mut bm = crate::bitmap::CompressedBitmap::new();
        for &id in ids {
            if let Some(ord) = self.with_entry(id, |e| e.ordinal) {
                bm.insert(ord);
            }
        }
        bm
    }

    /// Builds a `CompressedBitmap` of all live records whose `InlineId` starts with `prefix` (`WHERE id LIKE 'prefix%'`, `TASK-057`).
    pub fn id_prefix_bitmap(&self, prefix: &str) -> crate::bitmap::CompressedBitmap {
        let table = self.read_table();
        let mut bm = crate::bitmap::CompressedBitmap::new();
        for idx in 0..table.len() {
            if table.live(idx).is_some_and(|id| id.starts_with_str(prefix)) {
                bm.insert(RecordOrdinal(idx as u32));
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
        let table = self.read_table();
        let len = table.len();
        if len == 0 {
            return (Vec::new(), 0);
        }
        let max_items = limit.unwrap_or(usize::MAX);
        let mut out = Vec::with_capacity(max_items.min(self.len().min(256)));
        let mut total_matched = 0usize;
        let mut skipped = 0usize;
        let mut visit = |idx: usize| {
            let Some(id) = table.live(idx) else {
                return;
            };
            let ord = RecordOrdinal(idx as u32);
            if !filter_fn(ord, id) {
                return;
            }
            total_matched += 1;
            if skipped < offset {
                skipped += 1;
                return;
            }
            if out.len() < max_items {
                out.push((ord, id));
            }
        };

        if !descending {
            let start_idx = match after_ordinal {
                Some(cur) => (cur.0 as usize).saturating_add(1),
                None => 0,
            };
            if start_idx >= len {
                return (Vec::new(), 0);
            }
            (start_idx..len).for_each(&mut visit);
        } else {
            let end_exclusive = match after_ordinal {
                Some(cur) => (cur.0 as usize).min(len),
                None => len,
            };
            if end_exclusive == 0 {
                return (Vec::new(), 0);
            }
            (0..end_exclusive).rev().for_each(&mut visit);
        }
        (out, total_matched)
    }

    /// Restores `entry` at exactly `entry.ordinal` (checkpoint format v3), so ordinal-keyed
    /// structures persisted alongside the checkpoint (HNSW graph, paged indexes) stay valid across
    /// restarts. Call [`Self::finish_restore`] once every base and delta entry is loaded.
    ///
    /// An entry without an ordinal, or one whose ordinal is held by a different record (an
    /// inconsistent checkpoint), gets a fresh ordinal instead; ordinal-keyed indexes are
    /// reconciled against the records afterwards, so this only costs rebuild work.
    pub fn restore_entry_at(&self, mut entry: PrimaryEntry) -> Result<()> {
        let want = entry.ordinal;
        if want == RecordOrdinal::INVALID {
            self.restore_entry(entry);
            return Ok(());
        }
        let existing = self.with_entry(entry.id, |e| e.ordinal);
        let placed = {
            let mut table = self.write_table();
            let idx = want.as_usize();
            let held_by_other = table.slot(idx).is_some_and(|owner| owner != entry.id);
            if held_by_other {
                false
            } else {
                if let Some(old) = existing.filter(|&old| old != want) {
                    table.set(old.as_usize(), None)?;
                }
                table.set(idx, Some(entry.id))?;
                true
            }
        };
        if !placed {
            entry.ordinal = match existing {
                Some(ord) => ord,
                None => self.allocate_ordinal(entry.id),
            };
        }
        self.upsert(entry);
        Ok(())
    }

    /// Rebuilds the ordinal free list from the table's holes after a restore placed ordinals
    /// out of order. Lowest ordinals are reused first.
    pub fn finish_restore(&self) {
        let mut table = self.write_table();
        let free: Vec<u32> = (0..table.len())
            .rev()
            .filter(|&i| table.slot(i).is_none())
            .map(|i| i as u32)
            .collect();
        table.free = free;
    }

    pub fn restore_entry(&self, mut entry: PrimaryEntry) {
        if let Some(existing) = self.with_entry(entry.id, |e| e.ordinal) {
            entry.ordinal = existing;
        } else {
            entry.ordinal = self.allocate_ordinal(entry.id);
        }
        self.upsert(entry);
    }

    // ── Cold tier (`TASK-092`, `F-PIDX-03`) ──────────────────────────────────────────────────

    /// Opens the cold generation at `data_path` as this (empty) index's base, as at startup:
    /// every entry and ordinal of the tier becomes live without being loaded into RAM.
    pub fn attach_cold_tier(&self, data_path: &Path, seal: &FileSeal<'_>) -> Result<()> {
        let tier = Arc::new(ColdTier::open(data_path, seal)?);
        if !self.is_empty() || self.resident_entries() != 0 {
            return Err(StorageError::invalid_key("a cold tier can only be attached to an empty primary index"));
        }
        for i in 0..PRIMARY_SHARDS {
            self.write_shard(i).cold = Some(Arc::clone(&tier));
        }
        {
            let mut table = self.write_table();
            table.ids = Vec::new();
            table.overrides = FastHashMap::default();
            table.slots = tier.ordinal_slots() as usize;
            table.cold = Some(Arc::clone(&tier));
        }
        self.finish_restore();
        self.live_count.store(tier.entry_count() as usize, Ordering::Relaxed);
        self.tiered.store(true, Ordering::Release);
        Ok(())
    }

    /// Attaches the cold tier named by `manifest` (`<collection>_primary.cold`) to this empty
    /// index and returns its checkpoint stamp, or `None` when there is no cold tier.
    pub fn attach_cold_manifest(&self, manifest: &Path, seal: &FileSeal<'_>) -> Result<Option<u64>> {
        let Some((generation, stamp)) = read_manifest(manifest, COLD_MANIFEST_MAGIC)? else {
            return Ok(None);
        };
        let data = generation_path(manifest, generation);
        self.attach_cold_tier(&data, seal)?;
        remove_stale_generations(manifest, Some(&data));
        Ok(Some(stamp))
    }

    /// Writes `cut` as the next cold generation of `manifest` and publishes it with `stamp`
    /// (the previous generation is deleted; a crash before the manifest switch keeps it).
    pub fn write_cold_checkpoint(
        &self,
        cut: ColdCheckpoint,
        manifest: &Path,
        stamp: u64,
        seal: &FileSeal<'_>,
    ) -> Result<()> {
        let generation = read_manifest(manifest, COLD_MANIFEST_MAGIC)?.map_or(1, |(g, _)| g + 1);
        let data = generation_path(manifest, generation);
        let old = self.cold_tier_path();
        self.finish_cold_checkpoint(cut, &data, &temp_generation_path(manifest, generation), seal)?;
        write_manifest(manifest, COLD_MANIFEST_MAGIC, generation, stamp)?;
        if let Some(old) = old.filter(|o| *o != data) {
            // Best effort: still mapped by in-flight readers on Windows; removed at next open.
            let _ = std::fs::remove_file(old);
        }
        Ok(())
    }

    /// Deletes a cold tier's manifest and generations (after converting back to the memory tier).
    pub fn remove_cold_files(manifest: &Path) {
        remove_stale_generations(manifest, None);
        let _ = std::fs::remove_file(manifest);
    }

    /// Path of the cold generation backing this index (`None` in the memory tier).
    pub fn cold_tier_path(&self) -> Option<PathBuf> {
        self.read_table().cold.as_ref().map(|t| t.path().to_path_buf())
    }

    /// Takes a consistent cut of the RAM layer (all shard read locks held together) for
    /// [`Self::finish_cold_checkpoint`]. `O(resident entries + tombstones)`.
    pub fn begin_cold_checkpoint(&self) -> ColdCheckpoint {
        let guards: Vec<_> = (0..PRIMARY_SHARDS).map(|i| self.read_shard(i)).collect();
        let mut ram: Vec<PrimaryEntry> = Vec::with_capacity(guards.iter().map(|g| g.by_id.len()).sum());
        let mut removed = FastHashSet::default();
        for g in &guards {
            ram.extend(g.by_id.values().cloned());
            removed.extend(g.removed.iter().copied());
        }
        let base = guards.first().and_then(|g| g.cold.clone());
        // Ordinals under the same cut (lock order: shards, then the ordinal table).
        let (overrides, mem_ids, slots) = {
            let table = self.read_table();
            (table.overrides.clone(), table.ids.clone(), table.len())
        };
        drop(guards);
        ram.sort_unstable_by_key(|e| order_key(e.id));
        ColdCheckpoint { ram, removed, base, overrides, mem_ids, slots }
    }

    /// Writes the cut as a new cold generation (`O(page + Bloom filter)` memory: RAM entries are
    /// stream-merged with the previous tier in `(hash, id)` order), then installs it: RAM entries
    /// unchanged since the cut are evicted, tombstones for ids absent from the new tier are
    /// dropped, and ordinal overrides equal to the new tier's slots are released. Writers keep
    /// running throughout; each shard is swapped under its own write lock.
    pub fn finish_cold_checkpoint(
        &self,
        cut: ColdCheckpoint,
        data_path: &Path,
        tmp_path: &Path,
        seal: &FileSeal<'_>,
    ) -> Result<()> {
        let ColdCheckpoint { ram, removed, base, overrides, mem_ids, slots } = cut;
        let ram_pos = |id: InlineId| ram.binary_search_by(|r| order_key(r.id).cmp(&order_key(id))).ok();
        let in_ram = |id: InlineId| ram_pos(id).is_some();
        let slots = u32::try_from(slots).map_err(|_| StorageError::resource_exhausted("ordinal space"))?;
        // Slot `ord` holds `id` in the new tier iff that entry is in the merged sequence with
        // this ordinal: changed slots must match a RAM entry; unchanged ones keep the previous
        // tier's id unless it was deleted. `O(1)` per slot, no `O(N)` map.
        let ram_owns = |id: InlineId, ord: u32| ram_pos(id).is_some_and(|p| ram[p].ordinal.0 == ord);
        let resolve = |ord: u32| -> Option<InlineId> {
            let changed = if base.is_some() {
                overrides.get(&ord).copied()
            } else {
                Some(mem_ids.get(ord as usize).copied().flatten())
            };
            match changed {
                Some(Some(id)) if !id.is_empty() && ram_owns(id, ord) => Some(id),
                Some(_) => None,
                None => base
                    .as_ref()
                    .and_then(|t| t.resolve(ord))
                    .filter(|id| !removed.contains(id) && (!in_ram(*id) || ram_owns(*id, ord))),
            }
        };
        let expected = ram.len() + base.as_ref().map_or(0, |t| t.entry_count() as usize);
        let merged = MergeByKey::new(
            ram.iter().cloned(),
            base.as_deref()
                .into_iter()
                .flat_map(ColdTier::iter)
                .filter(|e| !in_ram(e.id) && !removed.contains(&e.id)),
        );
        let new_tier = Arc::new(ColdTier::write(data_path, tmp_path, seal, merged, expected, slots, resolve)?);

        // Install, shard by shard.
        for i in 0..PRIMARY_SHARDS {
            let candidates: Vec<InlineId> = self.read_shard(i).removed.iter().copied().collect();
            let keep: FastHashSet<InlineId> =
                candidates.iter().copied().filter(|&id| new_tier.contains(id)).collect();
            let known: FastHashSet<InlineId> = candidates.into_iter().collect();
            let mut shard = self.write_shard(i);
            shard.by_id.retain(|id, e| {
                let Ok(pos) = ram.binary_search_by(|r| order_key(r.id).cmp(&order_key(*id))) else {
                    return true;
                };
                let s = &ram[pos];
                // Keep anything changed since the cut (or still dirty).
                e.hot_dirty.is_some()
                    || e.version != s.version
                    || e.location != s.location
                    || e.ordinal != s.ordinal
                    || e.ttl_expires_at_ns != s.ttl_expires_at_ns
            });
            shard.removed.retain(|id| !known.contains(id) || keep.contains(id));
            shard.cold = Some(Arc::clone(&new_tier));
        }
        {
            let mut table = self.write_table();
            let mut overrides = core::mem::take(&mut table.overrides);
            if table.cold.is_none() {
                // First tier: the in-RAM table becomes overrides of the new tier.
                for (idx, slot) in table.ids.iter().enumerate() {
                    overrides.insert(idx as u32, *slot);
                }
                table.slots = table.ids.len();
                table.ids = Vec::new();
            }
            overrides.retain(|&ord, v| *v != new_tier.resolve(ord));
            table.overrides = overrides;
            table.slots = table.slots.max(new_tier.ordinal_slots() as usize);
            table.cold = Some(Arc::clone(&new_tier));
        }
        self.tiered.store(true, Ordering::Release);
        Ok(())
    }
}

/// Merges two iterators already sorted by `order_key` (keys are disjoint by construction).
struct MergeByKey<A: Iterator<Item = PrimaryEntry>, B: Iterator<Item = PrimaryEntry>> {
    a: core::iter::Peekable<A>,
    b: core::iter::Peekable<B>,
}

impl<A: Iterator<Item = PrimaryEntry>, B: Iterator<Item = PrimaryEntry>> MergeByKey<A, B> {
    fn new(a: A, b: B) -> Self {
        Self { a: a.peekable(), b: b.peekable() }
    }
}

impl<A: Iterator<Item = PrimaryEntry>, B: Iterator<Item = PrimaryEntry>> Iterator for MergeByKey<A, B> {
    type Item = PrimaryEntry;

    fn next(&mut self) -> Option<PrimaryEntry> {
        match (self.a.peek(), self.b.peek()) {
            (Some(x), Some(y)) => {
                if order_key(x.id) <= order_key(y.id) {
                    self.a.next()
                } else {
                    self.b.next()
                }
            }
            (Some(_), None) => self.a.next(),
            (None, _) => self.b.next(),
        }
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

    /// Observable state of an index: live entries (id, ordinal, version, location) and the
    /// live ordinal table.
    type Observed = (Vec<(InlineId, u32, u64, DiskLocation)>, Vec<(u32, InlineId)>, usize);

    fn observe(idx: &PrimaryIndex) -> Observed {
        let mut entries: Vec<_> = idx
            .entries_snapshot()
            .into_iter()
            .map(|e| (e.id, e.ordinal.0, e.version.0, e.location))
            .collect();
        entries.sort_by_key(|e| e.0);
        let (ords, _) = idx.scan_ordinals_paginated(false, None, 0, None, |_, _| true);
        (entries, ords.into_iter().map(|(o, id)| (o.0, id)).collect(), idx.len())
    }

    #[test]
    fn test_cold_tier_matches_memory_tier_through_checkpoints_and_restarts() {
        use storage_codec::{EncryptionKey, KeyRing};
        use storage_types::testing::SweepRng;
        let dir = std::env::temp_dir().join(format!("stge_cold_tier_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let ring = KeyRing::new().with_key(1, EncryptionKey::from_bytes([6; 32]));
        for (variant, seal) in [
            FileSeal::PLAIN,
            FileSeal { keys: Some(&ring), active_key: Some(1), accept_plain: false },
        ]
        .into_iter()
        .enumerate()
        {
            let mut rng = SweepRng::new(0xC01D + variant as u64);
            let model = PrimaryIndex::new();
            let mut cold = PrimaryIndex::new();
            let id = |i: usize| InlineId::new(&format!("k{i:05}")).unwrap();
            let mut generation = 0u64;
            let put = |idx: &PrimaryIndex, i: usize, v: u64, loc: DiskLocation| {
                let ord = match idx.with_entry(id(i), |e| e.ordinal) {
                    Some(o) => o,
                    None => idx.allocate_ordinal(id(i)),
                };
                idx.upsert(PrimaryEntry {
                    ordinal: ord,
                    version: Version(v),
                    location: loc,
                    ..entry(id(i))
                });
            };
            for step in 0..6_000u64 {
                let i = rng.below(1_500);
                let loc = DiskLocation::Arena(ArenaLocation { segment_id: 1, offset: 64 * (step as u32 + 1), len: 100 });
                match rng.below(10) {
                    0..=5 => {
                        put(&model, i, step, loc);
                        put(&cold, i, step, loc);
                    }
                    6 | 7 => {
                        let a = model.remove(id(i));
                        let b = cold.remove(id(i));
                        assert_eq!(a.is_some(), b.is_some(), "remove {i} at {step}");
                        if let (Some(a), Some(b)) = (a, b) {
                            assert_eq!(a.ordinal, b.ordinal);
                            model.release_ordinal(a.ordinal);
                            cold.release_ordinal(b.ordinal);
                        }
                    }
                    8 => {
                        // Lookups agree, including tier-only entries.
                        assert_eq!(model.get(id(i)).map(|e| (e.ordinal, e.version)), cold.get(id(i)).map(|e| (e.ordinal, e.version)));
                    }
                    _ => {
                        // Resident entries take the flag; tier-only ones stay out of RAM.
                        let resident = cold.resident_entries();
                        if let Some(e) = cold.get(id(i)) {
                            cold.mark_disk_verified(id(i), e.location);
                            assert_eq!(cold.resident_entries(), resident);
                        }
                    }
                }
                if step % 1_000 == 999 {
                    generation += 1;
                    let data = dir.join(format!("v{variant}_g{generation}.cold"));
                    let cut = cold.begin_cold_checkpoint();
                    cold.finish_cold_checkpoint(cut, &data, &dir.join("tmp.cold"), &seal).unwrap();
                    assert!(cold.is_tiered());
                    assert_eq!(observe(&cold), observe(&model), "after checkpoint {generation}");
                    // Only entries changed after the cut stay resident; here: none.
                    assert_eq!(cold.resident_entries(), 0);
                    // Restart from the tier alone.
                    let reopened = PrimaryIndex::new();
                    reopened.attach_cold_tier(&data, &seal).unwrap();
                    assert_eq!(observe(&reopened), observe(&model), "restart {generation}");
                    if generation % 2 == 0 {
                        cold = reopened;
                    }
                }
            }
            assert_eq!(observe(&cold), observe(&model));
            assert_eq!(cold.resolve_ordinals_batch(&[RecordOrdinal(0), RecordOrdinal(3)]), model.resolve_ordinals_batch(&[RecordOrdinal(0), RecordOrdinal(3)]));
            assert_eq!(cold.live_ordinals_bitmap().len(), model.live_ordinals_bitmap().len());
            assert_eq!(cold.id_prefix_bitmap("k00").len(), model.id_prefix_bitmap("k00").len());

            // The tier file is validated at open: any flipped byte is rejected (sealed: by the
            // tag; plaintext: by checksums), never served.
            let data = dir.join(format!("v{variant}_g{generation}.cold"));
            let bytes = std::fs::read(&data).unwrap();
            if variant == 1 {
                assert!(!bytes.windows(6).any(|w| w == b"k00012"), "plaintext id in sealed tier");
                assert!(PrimaryIndex::new().attach_cold_tier(&data, &FileSeal::PLAIN).is_err());
            }
            // Header fields, the encryption section (sealed only), a page, the middle, the Bloom
            // filter. (Unused header padding is covered by neither, and changes nothing.)
            let offsets: &[usize] = if variant == 1 { &[20, 140, 4096 + 20] } else { &[20, 4096 + 20] };
            for &at in offsets.iter().chain(&[bytes.len() / 2, bytes.len() - 5]) {
                let mut bad = bytes.clone();
                bad[at] ^= 0x08;
                let tampered = dir.join("tampered.cold");
                std::fs::write(&tampered, &bad).unwrap();
                assert!(PrimaryIndex::new().attach_cold_tier(&tampered, &seal).is_err(), "variant {variant} byte {at}");
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
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
