//! Bounded cache of decrypted, validated record frames for engines with encryption at rest
//! (`TASK-084`, `F-SEC-02`, `RULE-MEM-01`, `RULE-MEM-04`).
//!
//! Encrypted frames cannot be read in place from `mmap`, so the first read of a record decrypts
//! it into a 64-byte-aligned buffer; later reads of the same version reuse that buffer with zero
//! allocations. Memory is bounded by a byte budget split across shards; when a shard exceeds its
//! share, arbitrary entries are evicted (random replacement — no per-hit bookkeeping, so hits only
//! take a shard read lock).

use std::sync::RwLock;

use storage_codec::AlignedBytes;
use storage_types::{CachePadded, FastHashMap, InlineId, Version};

const SHARDS: usize = 16;

#[derive(Default)]
struct Shard {
    frames: FastHashMap<InlineId, (Version, AlignedBytes)>,
    bytes: usize,
}

pub(crate) struct DecryptedCache {
    shards: Box<[CachePadded<RwLock<Shard>>; SHARDS]>,
    budget_per_shard: usize,
}

impl DecryptedCache {
    pub(crate) fn new(budget_bytes: usize) -> Self {
        Self {
            shards: CachePadded::boxed_array(|| RwLock::new(Shard::default())),
            budget_per_shard: budget_bytes / SHARDS,
        }
    }

    #[inline]
    fn shard(&self, id: InlineId) -> &RwLock<Shard> {
        &self.shards[(id.fast_hash() >> 32) as usize & (SHARDS - 1)]
    }

    /// The cached plaintext frame of `id` at exactly `version` (an `Arc` bump, 0 allocations).
    #[inline]
    pub(crate) fn get(&self, id: InlineId, version: Version) -> Option<AlignedBytes> {
        let shard = self.shard(id).read().unwrap_or_else(|p| p.into_inner());
        shard
            .frames
            .get(&id)
            .filter(|(v, _)| *v == version)
            .map(|(_, f)| f.clone())
    }

    /// Caches `frame` as the plaintext of `id` at `version`, evicting to stay within budget.
    /// Frames larger than a shard's whole budget are not cached.
    pub(crate) fn insert(&self, id: InlineId, version: Version, frame: AlignedBytes) {
        let len = frame.len();
        if len > self.budget_per_shard {
            self.remove(id);
            return;
        }
        let mut shard = self.shard(id).write().unwrap_or_else(|p| p.into_inner());
        if let Some((_, old)) = shard.frames.insert(id, (version, frame)) {
            shard.bytes -= old.len();
        }
        shard.bytes += len;
        while shard.bytes > self.budget_per_shard {
            let Some(victim) = shard.frames.keys().find(|k| **k != id).copied() else {
                break;
            };
            if let Some((_, f)) = shard.frames.remove(&victim) {
                shard.bytes -= f.len();
            }
        }
    }

    pub(crate) fn remove(&self, id: InlineId) {
        let mut shard = self.shard(id).write().unwrap_or_else(|p| p.into_inner());
        if let Some((_, f)) = shard.frames.remove(&id) {
            shard.bytes -= f.len();
        }
    }

    /// Total cached bytes (for telemetry and tests).
    pub(crate) fn bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|s| s.read().unwrap_or_else(|p| p.into_inner()).bytes)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_respects_budget_and_versions() {
        let cache = DecryptedCache::new(16 * 4096);
        let frame = AlignedBytes::copy_from_slice(&[7u8; 1000]);
        for i in 0..500u32 {
            let id = InlineId::new(&format!("k{i}")).unwrap();
            cache.insert(id, Version(1), frame.clone());
        }
        assert!(cache.bytes() <= 16 * 4096);
        let id = InlineId::new("fresh").unwrap();
        cache.insert(id, Version(3), frame.clone());
        assert!(cache.get(id, Version(3)).is_some());
        assert!(cache.get(id, Version(4)).is_none(), "stale version must miss");
        cache.remove(id);
        assert!(cache.get(id, Version(3)).is_none());
    }
}
