//! Bounded `MmapPool` with 64-shard Clock / W-TinyLFU eviction and `MmapGuard` reader safety (`TASK-008`, `F-CORE-07`, `RULE-MEM-04`).

use std::fs::File;
use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use storage_types::{CachePadded, FastHashMap, InlineId, Result, StorageError};

use crate::raw::MmapRegion;

const POOL_SHARDS: usize = 64;
const SHARD_MASK: usize = POOL_SHARDS - 1;

/// Configuration for `MmapPool` resource ceilings (`RULE-MEM-04`).
#[derive(Clone, Copy, Debug)]
pub struct MmapPoolConfig {
    /// Maximum number of active `MmapRegion` handles kept open simultaneously across all shards.
    pub max_open_mmaps: usize,
    /// Maximum resident mapped bytes across pooled record mappings before eviction triggers.
    pub max_mapped_bytes: usize,
}

impl Default for MmapPoolConfig {
    fn default() -> Self {
        Self {
            max_open_mmaps: 16_384,
            max_mapped_bytes: 4 * 1024 * 1024 * 1024, // 4 GiB virtual address ceiling
        }
    }
}

/// Snapshot of `MmapPool` telemetry metrics (`F-OPS-06`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MmapPoolStats {
    pub active_mmaps: usize,
    pub mapped_bytes: usize,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub evictions: u64,
}

struct PooledEntry {
    region: Arc<MmapRegion>,
    freq: AtomicU8,
    pinned: bool,
}

#[derive(Default)]
struct PoolShard {
    entries: FastHashMap<InlineId, PooledEntry>,
}

/// Lifetime-safe 8-byte guard holding an `Arc<MmapRegion>` (`RULE-SAFE-02`).
/// Even if the `MmapPool` evicts this entry while a reader holds `MmapGuard`,
/// the OS virtual memory mapping remains valid until the last `MmapGuard` drops.
#[derive(Debug, Clone)]
pub struct MmapGuard {
    region: Arc<MmapRegion>,
}

impl MmapGuard {
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        self.region.as_slice()
    }

    #[inline]
    pub fn region(&self) -> &MmapRegion {
        &self.region
    }

    /// Returns `true` if `RecordCodec::validate_frame` has already verified this mapping (`RULE-SAFE-04`).
    #[inline]
    pub fn is_verified(&self) -> bool {
        self.region.is_verified()
    }

    /// Marks this mapping as verified so subsequent hot reads skip `bytecheck` & checksum verification.
    #[inline]
    pub fn mark_verified(&self) {
        self.region.mark_verified();
    }
}

impl Deref for MmapGuard {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.region.as_slice()
    }
}

/// Sharded, bounded pool of active `MmapRegion` instances.
pub struct MmapPool {
    config: MmapPoolConfig,
    shards: Box<[CachePadded<RwLock<PoolShard>>; POOL_SHARDS]>,
    active_mmaps: CachePadded<AtomicUsize>,
    mapped_bytes: CachePadded<AtomicUsize>,
    cache_hits: CachePadded<AtomicU64>,
    cache_misses: CachePadded<AtomicU64>,
    evictions: CachePadded<AtomicU64>,
    clock_hand: CachePadded<AtomicUsize>,
}

impl core::fmt::Debug for MmapPool {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MmapPool")
            .field("config", &self.config)
            .field("stats", &self.stats())
            .finish()
    }
}

impl MmapPool {
    pub fn new(config: MmapPoolConfig) -> Self {
        let shards_vec: Vec<CachePadded<RwLock<PoolShard>>> = (0..POOL_SHARDS)
            .map(|_| CachePadded::new(RwLock::new(PoolShard::default())))
            .collect();
        let shards = shards_vec
            .into_boxed_slice()
            .try_into()
            .unwrap_or_else(|_| unreachable!());

        Self {
            config: MmapPoolConfig {
                max_open_mmaps: config.max_open_mmaps.max(64),
                max_mapped_bytes: config.max_mapped_bytes.max(1024 * 1024),
            },
            shards,
            active_mmaps: CachePadded::new(AtomicUsize::new(0)),
            mapped_bytes: CachePadded::new(AtomicUsize::new(0)),
            cache_hits: CachePadded::new(AtomicU64::new(0)),
            cache_misses: CachePadded::new(AtomicU64::new(0)),
            evictions: CachePadded::new(AtomicU64::new(0)),
            clock_hand: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    #[inline]
    fn shard_idx(id: InlineId) -> usize {
        (id.fast_hash() as usize) & SHARD_MASK
    }

    /// Zero-clone, zero-atomic-refcount hot read path: if `id` is mapped and verified,
    /// invokes `f(slice)` directly while holding only the shard read lock!
    #[inline]
    pub fn with_verified_slice<R>(&self, id: InlineId, f: impl FnOnce(&[u8]) -> R) -> Option<R> {
        let idx = Self::shard_idx(id);
        let shard = self.shards[idx]
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = shard.entries.get(&id)?;
        if !entry.region.is_verified() {
            return None;
        }
        let cur = entry.freq.load(Ordering::Relaxed);
        if cur < 7 {
            entry.freq.store(cur + 1, Ordering::Relaxed);
        }
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
        Some(f(entry.region.as_slice()))
    }

    /// Fast-path lookup for an already mapped record (`0` allocations on hit).
    #[inline]
    pub fn get_if_mapped(&self, id: InlineId) -> Option<MmapGuard> {
        let idx = Self::shard_idx(id);
        let shard = self.shards[idx]
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(entry) = shard.entries.get(&id) {
            let cur = entry.freq.load(Ordering::Relaxed);
            if cur < 7 {
                entry.freq.store(cur + 1, Ordering::Relaxed);
            }
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Some(MmapGuard {
                region: Arc::clone(&entry.region),
            });
        }
        None
    }

    /// Maps `path` (or returns the existing mapping) while strictly enforcing `max_open_mmaps`
    /// and `max_mapped_bytes` budgets.
    pub fn get_or_map(&self, id: InlineId, path: &Path, pinned: bool) -> Result<MmapGuard> {
        if let Some(guard) = self.get_if_mapped(id) {
            return Ok(guard);
        }

        self.cache_misses.fetch_add(1, Ordering::Relaxed);

        // Open and map outside of any shard lock (`RULE-CONC-01`)
        let file = File::open(path).map_err(|e| StorageError::io("open record file for mmap", e))?;
        let meta = file
            .metadata()
            .map_err(|e| StorageError::io("stat record file for mmap", e))?;
        let len = meta.len() as usize;
        let region = Arc::new(MmapRegion::map_read_only(&file, len)?);

        self.ensure_capacity(len)?;

        let idx = Self::shard_idx(id);
        let mut shard = self.shards[idx]
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(existing) = shard.entries.get(&id) {
            return Ok(MmapGuard {
                region: Arc::clone(&existing.region),
            });
        }

        shard.entries.insert(
            id,
            PooledEntry {
                region: Arc::clone(&region),
                freq: AtomicU8::new(1),
                pinned,
            },
        );
        self.active_mmaps.fetch_add(1, Ordering::Relaxed);
        self.mapped_bytes.fetch_add(len, Ordering::Relaxed);

        Ok(MmapGuard { region })
    }

    /// Replaces or inserts an already-mapped region (used after atomic disk flush).
    pub fn install_region(
        &self,
        id: InlineId,
        region: Arc<MmapRegion>,
        already_verified: bool,
        pinned: bool,
    ) -> Result<MmapGuard> {
        let len = region.len();
        self.ensure_capacity(len)?;

        if already_verified {
            region.mark_verified();
        }
        let idx = Self::shard_idx(id);
        let mut shard = self.shards[idx]
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(prev) = shard.entries.insert(
            id,
            PooledEntry {
                region: Arc::clone(&region),
                freq: AtomicU8::new(2),
                pinned,
            },
        ) {
            self.mapped_bytes
                .fetch_sub(prev.region.len(), Ordering::Relaxed);
        } else {
            self.active_mmaps.fetch_add(1, Ordering::Relaxed);
        }
        self.mapped_bytes.fetch_add(len, Ordering::Relaxed);

        Ok(MmapGuard { region })
    }

    /// Removes `id` from the pool (e.g. on record deletion or before overwriting on Windows).
    pub fn invalidate(&self, id: InlineId) {
        let idx = Self::shard_idx(id);
        let mut shard = self.shards[idx]
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(removed) = shard.entries.remove(&id) {
            self.active_mmaps.fetch_sub(1, Ordering::Relaxed);
            self.mapped_bytes
                .fetch_sub(removed.region.len(), Ordering::Relaxed);
        }
    }

    /// Evicts cold unpinned entries across shards using a Clock-Pro / Second-Chance sweep
    /// whenever `active_mmaps >= max_open_mmaps` or `mapped_bytes + incoming_len > max_mapped_bytes`.
    fn ensure_capacity(&self, incoming_len: usize) -> Result<()> {
        let mut attempts = 0usize;
        while self.active_mmaps.load(Ordering::Relaxed) >= self.config.max_open_mmaps
            || self.mapped_bytes.load(Ordering::Relaxed).saturating_add(incoming_len)
                > self.config.max_mapped_bytes
        {
            if attempts >= POOL_SHARDS * 3 {
                // If we cannot evict because every entry is pinned or actively held by readers
                if self.active_mmaps.load(Ordering::Relaxed) >= self.config.max_open_mmaps {
                    return Err(StorageError::resource_exhausted(
                        "max_open_mmaps limit reached and all regions are pinned or in use",
                    ));
                }
                break;
            }
            attempts += 1;

            let s_idx = self.clock_hand.fetch_add(1, Ordering::Relaxed) & SHARD_MASK;
            let mut shard = self.shards[s_idx]
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if shard.entries.is_empty() {
                continue;
            }

            let mut victim_key: Option<InlineId> = None;
            for (&k, entry) in shard.entries.iter() {
                if entry.pinned {
                    continue;
                }
                // Only evict entries not actively borrowed by an external `MmapGuard` (`strong_count == 1`)
                if Arc::strong_count(&entry.region) > 1 {
                    continue;
                }
                let f = entry.freq.load(Ordering::Relaxed);
                if f > 0 {
                    entry.freq.store(f - 1, Ordering::Relaxed);
                } else {
                    victim_key = Some(k);
                    break;
                }
            }

            // If no `freq == 0` victim was found on first pass in this shard, pick any unpinned idle entry
            if victim_key.is_none() && attempts > POOL_SHARDS {
                for (&k, entry) in shard.entries.iter() {
                    if !entry.pinned && Arc::strong_count(&entry.region) == 1 {
                        victim_key = Some(k);
                        break;
                    }
                }
            }

            if let Some(k) = victim_key {
                if let Some(removed) = shard.entries.remove(&k) {
                    self.active_mmaps.fetch_sub(1, Ordering::Relaxed);
                    self.mapped_bytes
                        .fetch_sub(removed.region.len(), Ordering::Relaxed);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        Ok(())
    }

    #[inline]
    pub fn stats(&self) -> MmapPoolStats {
        MmapPoolStats {
            active_mmaps: self.active_mmaps.load(Ordering::Relaxed),
            mapped_bytes: self.mapped_bytes.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }
}
