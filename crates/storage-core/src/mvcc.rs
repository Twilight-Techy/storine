//! Multi-Version Concurrency Control (MVCC), Point-in-Time `ReadSnapshot`, and Watermark Epoch GC
//! (`TASK-039`, `F-CONC-08`, `RULE-MEM-04`, `RULE-CONC-01`).
//!
//! Enables long-running analytical queries, full-collection scans, and online backups to read a
//! consistent point-in-time `ReadSnapshot` at a monotonic `CommitTs` without blocking concurrent
//! writers and without seeing partial post-snapshot mutations.
//!
//! To prevent memory bloat (`RULE-MEM-04`), `MvccManager::gc_obsolete_versions` tracks the minimum
//! `read_ts` across all active `ReadSnapshot` guards and immediately reclaims any version chain
//! node that is strictly older than the newest version `<= min_active_read_ts`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, RwLock, RwLockReadGuard};

use storage_codec::{AlignedBytes, RecordCodec, ZeroCopyRecord};
use storage_index::Indexable;
use storage_types::{CachePadded, FastHashMap, InlineId, Result, Version};

use crate::engine::StorageEngine;

const MVCC_SHARDS: usize = 64;
const MVCC_SHARD_MASK: usize = MVCC_SHARDS - 1;

/// Monotonic 64-bit MVCC commit / snapshot timestamp (`F-CONC-08`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommitTs(pub u64);

/// A single historical or current version entry in an `InlineId`'s MVCC version chain.
#[derive(Clone, Debug)]
pub struct VersionChainNode {
    pub commit_ts: CommitTs,
    pub version: Version,
    /// `None` indicates a deletion tombstone committed at `commit_ts`.
    pub frame: Option<AlignedBytes>,
}

#[derive(Default)]
struct MvccShard {
    /// Per-record version chains ordered from oldest (`front`) to newest (`back`).
    chains: FastHashMap<InlineId, VecDeque<VersionChainNode>>,
}

/// 64-shard MVCC Version Chain Manager & Active Snapshot Watermark Tracker (`TASK-039`, `F-CONC-08`).
pub struct MvccManager {
    clock: CachePadded<AtomicU64>,
    active_count: CachePadded<AtomicUsize>,
    /// Number of per-record version chains currently retained (fast "nothing to forget" check).
    chain_count: CachePadded<AtomicUsize>,
    shards: Box<[CachePadded<RwLock<MvccShard>>; MVCC_SHARDS]>,
    active_snapshots: Mutex<Vec<CommitTs>>,
    /// Writers hold this shared for the whole "check snapshots -> record version -> publish"
    /// sequence; `register_snapshot` takes it exclusively, so a snapshot can never be registered
    /// in the middle of a write (which would let it observe a post-snapshot version).
    write_gate: RwLock<()>,
}

impl core::fmt::Debug for MvccManager {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MvccManager")
            .field("latest_ts", &self.current_ts())
            .field("active_snapshots", &self.active_snapshot_count())
            .field("total_retained_versions", &self.retained_version_count())
            .finish()
    }
}

impl Default for MvccManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MvccManager {
    pub fn new() -> Self {
        let shards = CachePadded::boxed_array(|| RwLock::new(MvccShard::default()));
        Self {
            clock: CachePadded::new(AtomicU64::new(1)),
            active_count: CachePadded::new(AtomicUsize::new(0)),
            chain_count: CachePadded::new(AtomicUsize::new(0)),
            shards,
            active_snapshots: Mutex::new(Vec::new()),
            write_gate: RwLock::new(()),
        }
    }

    #[inline]
    fn shard_idx(id: InlineId) -> usize {
        (id.fast_hash() as usize) & MVCC_SHARD_MASK
    }

    /// Returns the latest committed timestamp without advancing the clock.
    #[inline]
    pub fn current_ts(&self) -> CommitTs {
        CommitTs(self.clock.load(Ordering::Acquire))
    }

    /// Allocates the next strictly increasing `CommitTs`.
    #[inline]
    pub fn next_commit_ts(&self) -> CommitTs {
        CommitTs(self.clock.fetch_add(1, Ordering::AcqRel) + 1)
    }

    /// Fast 1-instruction lock-free check whether any `ReadSnapshot` is currently open.
    #[inline]
    pub fn has_active_snapshots(&self) -> bool {
        self.active_count.load(Ordering::Acquire) > 0
    }

    /// Shared guard every writer holds while it records MVCC history and publishes a new version.
    #[inline]
    pub fn writer_guard(&self) -> RwLockReadGuard<'_, ()> {
        self.write_gate
            .read()
            .unwrap_or_else(|p| p.into_inner())
    }

    /// Drops the version chain of `id` (called by writers when no snapshot is open, so a chain
    /// left over from an earlier snapshot can never be mistaken for the current state).
    #[inline]
    pub fn forget(&self, id: InlineId) {
        if self.chain_count.load(Ordering::Acquire) == 0 {
            return;
        }
        let s_idx = Self::shard_idx(id);
        let mut shard = self.shards[s_idx]
            .write()
            .unwrap_or_else(|p| p.into_inner());
        if shard.chains.remove(&id).is_some() {
            self.chain_count.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Returns `true` if `id` currently has a retained version chain.
    #[inline]
    pub fn has_chain(&self, id: InlineId) -> bool {
        if self.chain_count.load(Ordering::Acquire) == 0 {
            return false;
        }
        let s_idx = Self::shard_idx(id);
        self.shards[s_idx]
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .chains
            .contains_key(&id)
    }

    /// Registers a new active reader snapshot at `current_ts()`.
    ///
    /// Waits for in-flight writes to finish publishing (exclusive `write_gate`), so every write
    /// is either fully visible to the snapshot or recorded as history it can skip.
    pub fn register_snapshot(&self) -> CommitTs {
        let _gate = self
            .write_gate
            .write()
            .unwrap_or_else(|p| p.into_inner());
        let mut active = self
            .active_snapshots
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let ts = self.current_ts();
        active.push(ts);
        self.active_count.store(active.len(), Ordering::Release);
        ts
    }

    /// Unregisters a reader snapshot when its `ReadSnapshot` guard drops.
    pub fn unregister_snapshot(&self, ts: CommitTs) {
        let mut active = self
            .active_snapshots
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if let Some(pos) = active.iter().position(|&x| x == ts) {
            active.swap_remove(pos);
        }
        self.active_count.store(active.len(), Ordering::Release);
    }

    /// Returns the number of currently open `ReadSnapshot`s.
    #[inline]
    pub fn active_snapshot_count(&self) -> usize {
        self.active_count.load(Ordering::Acquire)
    }

    /// Returns the oldest active `CommitTs` watermark across all open `ReadSnapshot`s,
    /// or `current_ts()` if no snapshots are open.
    pub fn min_active_watermark(&self) -> CommitTs {
        if !self.has_active_snapshots() {
            return self.current_ts();
        }
        let active = self
            .active_snapshots
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        active
            .iter()
            .copied()
            .min()
            .unwrap_or_else(|| self.current_ts())
    }

    /// Seeds the pre-mutation baseline version of `id` (at `CommitTs(1)`) if `id` is not yet in
    /// the MVCC chain, ensuring open snapshots taken before this mutation see the prior state.
    pub fn seed_baseline_if_absent(
        &self,
        id: InlineId,
        prior_version: Version,
        prior_frame: Option<AlignedBytes>,
    ) {
        let s_idx = Self::shard_idx(id);
        let mut shard = self.shards[s_idx]
            .write()
            .unwrap_or_else(|p| p.into_inner());
        let chain = shard.chains.entry(id).or_insert_with(|| {
            self.chain_count.fetch_add(1, Ordering::AcqRel);
            VecDeque::new()
        });
        if chain.is_empty() {
            chain.push_back(VersionChainNode {
                commit_ts: CommitTs(1),
                version: prior_version,
                frame: prior_frame,
            });
        }
    }

    /// Records a newly committed version (or deletion tombstone when `frame.is_none()`) for `id`.
    /// Automatically prunes bounded per-key history if no active snapshot requires older versions.
    pub fn record_version(
        &self,
        id: InlineId,
        version: Version,
        frame: Option<AlignedBytes>,
    ) -> CommitTs {
        let commit_ts = self.next_commit_ts();
        let watermark = self.min_active_watermark();
        let s_idx = Self::shard_idx(id);
        let mut shard = self.shards[s_idx]
            .write()
            .unwrap_or_else(|p| p.into_inner());
        let chain = shard.chains.entry(id).or_insert_with(|| {
            self.chain_count.fetch_add(1, Ordering::AcqRel);
            VecDeque::new()
        });
        chain.push_back(VersionChainNode {
            commit_ts,
            version,
            frame,
        });

        // Opportunistic inline GC on this key's chain so chain length stays O(active_snapshots)
        prune_chain_to_watermark(chain, watermark);
        commit_ts
    }

    /// Resolves the visible `VersionChainNode` for `id` as of `read_ts` (`commit_ts <= read_ts`).
    pub fn get_visible_at(&self, id: InlineId, read_ts: CommitTs) -> Option<VersionChainNode> {
        let s_idx = Self::shard_idx(id);
        let shard = self.shards[s_idx]
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let chain = shard.chains.get(&id)?;
        // Search from newest (`back`) to oldest (`front`) for the first version with `commit_ts <= read_ts`
        chain
            .iter()
            .rev()
            .find(|node| node.commit_ts <= read_ts)
            .cloned()
    }

    /// Sweeps all 64 shards and reclaims any obsolete historical version chain nodes older than
    /// `min_active_watermark()`, returning the number of reclaimed version nodes (`RULE-MEM-04`).
    ///
    /// A chain whose only remaining node is visible to every open snapshot (or any chain at all
    /// when no snapshot is open) is dropped entirely: its newest node always equals the record's
    /// current state, which readers then fetch from the engine directly.
    pub fn gc_obsolete_versions(&self) -> usize {
        // Hold the gate exclusively so no writer records history while chains are being dropped.
        let _gate = self
            .write_gate
            .write()
            .unwrap_or_else(|p| p.into_inner());
        let no_snapshots = !self.has_active_snapshots();
        let watermark = self.min_active_watermark();
        let mut reclaimed = 0usize;
        let mut dropped_chains = 0usize;
        for shard_lock in self.shards.iter() {
            let mut shard = shard_lock.write().unwrap_or_else(|p| p.into_inner());
            shard.chains.retain(|_id, chain| {
                reclaimed += prune_chain_to_watermark(chain, watermark);
                let drop_chain =
                    no_snapshots || (chain.len() == 1 && chain[0].commit_ts <= watermark);
                if drop_chain {
                    reclaimed += chain.len();
                    dropped_chains += 1;
                }
                !drop_chain
            });
        }
        self.chain_count.fetch_sub(dropped_chains, Ordering::AcqRel);
        reclaimed
    }

    /// Total number of version chain nodes currently retained across all 64 shards.
    pub fn retained_version_count(&self) -> usize {
        self.shards
            .iter()
            .map(|s| {
                s.read()
                    .unwrap_or_else(|p| p.into_inner())
                    .chains
                    .values()
                    .map(|c| c.len())
                    .sum::<usize>()
            })
            .sum()
    }
}

/// Prunes any version in `chain` that is superseded by a newer version whose `commit_ts <= watermark`.
#[inline]
fn prune_chain_to_watermark(
    chain: &mut VecDeque<VersionChainNode>,
    watermark: CommitTs,
) -> usize {
    let mut reclaimed = 0usize;
    while chain.len() >= 2 {
        if chain[1].commit_ts <= watermark {
            chain.pop_front();
            reclaimed += 1;
        } else {
            break;
        }
    }
    reclaimed
}

/// RAII Point-in-Time Snapshot Isolation Read Guard (`TASK-039`, `F-CONC-08`).
///
/// Automatically unregisters its `read_ts` from `MvccManager` on `Drop` so epoch GC can
/// immediately reclaim superseded versions.
pub struct ReadSnapshot<'a, T: ZeroCopyRecord + Indexable> {
    engine: &'a StorageEngine<T>,
    read_ts: CommitTs,
}

impl<'a, T: ZeroCopyRecord + Indexable> core::fmt::Debug for ReadSnapshot<'a, T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ReadSnapshot")
            .field("collection", &self.engine.collection_name())
            .field("read_ts", &self.read_ts)
            .finish()
    }
}

impl<'a, T: ZeroCopyRecord + Indexable> ReadSnapshot<'a, T> {
    pub(crate) fn new(engine: &'a StorageEngine<T>, read_ts: CommitTs) -> Self {
        Self { engine, read_ts }
    }

    #[inline]
    pub const fn read_ts(&self) -> CommitTs {
        self.read_ts
    }

    /// Reads the state of `id` as of `self.read_ts()` under Snapshot Isolation (`F-CONC-08`).
    /// Concurrent writes committed after `self.read_ts()` are invisible to this snapshot!
    pub fn get(&self, id: InlineId) -> Result<Option<(Version, T)>> {
        if let Some(found) = self.get_from_chain(id)? {
            return Ok(found);
        }
        // No history: the record has not changed since this snapshot, so the engine's current
        // state is the snapshot state. Writers create the chain *before* publishing a new
        // version, so re-checking the chain after the read closes the race with a concurrent write.
        let current = match self.engine.get_owned(id) {
            Ok(pair) => Some(pair),
            Err(storage_types::StorageError::RecordNotFound { .. }) => None,
            Err(e) => return Err(e),
        };
        if let Some(found) = self.get_from_chain(id)? {
            return Ok(found);
        }
        Ok(current)
    }

    /// Returns `Some(state)` when `id` has a version chain (`state` may be a tombstone `None`).
    fn get_from_chain(&self, id: InlineId) -> Result<Option<Option<(Version, T)>>> {
        let Some(node) = self.engine.mvcc().get_visible_at(id, self.read_ts) else {
            return Ok(None);
        };
        let Some(ref frame) = node.frame else {
            return Ok(Some(None));
        };
        let (_hdr, owned) = RecordCodec::decode_owned::<T>(frame.as_slice())?;
        Ok(Some(Some((node.version, owned))))
    }
}

impl<'a, T: ZeroCopyRecord + Indexable> Drop for ReadSnapshot<'a, T> {
    fn drop(&mut self) {
        self.engine.mvcc().unregister_snapshot(self.read_ts);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{EngineConfig, StorageEngine};
    use crate::model::AgentObjectRecord;

    #[test]
    fn test_mvcc_snapshot_isolation_and_epoch_gc() {
        let dir = std::env::temp_dir().join("stge_test_mvcc_snapshot");
        let _ = std::fs::remove_dir_all(&dir);

        let engine =
            StorageEngine::<AgentObjectRecord>::open(&dir, EngineConfig::default()).unwrap();
        let id1 = InlineId::from_static("mvcc_rec_01");
        let id2 = InlineId::from_static("mvcc_rec_02");

        // Write baseline v1 before opening snapshot
        let r1_v1 = AgentObjectRecord::new(id1, "Baseline V1", 1_000, 10);
        engine.put(id1, &r1_v1).unwrap();

        // Open Snapshot 1
        let snap1 = engine.begin_read_snapshot();
        assert_eq!(engine.mvcc().active_snapshot_count(), 1);

        // Concurrent mutations while snap1 is open:
        // 1. Update id1 -> V2, then V3
        let r1_v2 = AgentObjectRecord::new(id1, "Updated V2", 2_000, 20);
        engine.put(id1, &r1_v2).unwrap();
        let r1_v3 = AgentObjectRecord::new(id1, "Updated V3", 3_000, 30);
        engine.put(id1, &r1_v3).unwrap();

        // 2. Insert new id2 after snap1 opened
        let r2_v1 = AgentObjectRecord::new(id2, "Inserted After Snap1", 2_500, 25);
        engine.put(id2, &r2_v1).unwrap();

        // Snap1 MUST still see id1 at V1 ("Baseline V1") and MUST NOT see id2!
        let (ver1, s1_r1) = snap1.get(id1).unwrap().expect("id1 visible in snap1");
        assert_eq!(ver1, Version(1));
        assert_eq!(s1_r1.title, "Baseline V1");
        assert_eq!(s1_r1.score, 10);
        assert!(
            snap1.get(id2).unwrap().is_none(),
            "id2 inserted after snap1 must be invisible to snap1"
        );

        // Open Snapshot 2 after V3 and id2 were committed
        let snap2 = engine.begin_read_snapshot();
        let (ver2, s2_r1) = snap2.get(id1).unwrap().expect("id1 visible in snap2");
        assert_eq!(ver2, Version(3));
        assert_eq!(s2_r1.title, "Updated V3");
        assert!(snap2.get(id2).unwrap().is_some());

        // Delete id1 while snap1 & snap2 are still open
        engine.delete(id1).unwrap();
        assert_eq!(snap1.get(id1).unwrap().unwrap().1.title, "Baseline V1");
        assert_eq!(snap2.get(id1).unwrap().unwrap().1.title, "Updated V3");

        // Drop snap1 and snap2, then run epoch GC (`gc_mvcc`)
        drop(snap1);
        drop(snap2);
        assert_eq!(engine.mvcc().active_snapshot_count(), 0);
        let reclaimed = engine.gc_mvcc();
        assert!(reclaimed >= 2, "epoch GC must reclaim obsolete version chain nodes");
        assert_eq!(engine.mvcc().retained_version_count(), 0);

        // Regression: history from an earlier snapshot must never shadow later writes.
        let id3 = InlineId::from_static("mvcc_rec_03");
        engine.put(id3, &AgentObjectRecord::new(id3, "A", 1, 1)).unwrap();
        let s_a = engine.begin_read_snapshot();
        engine.put(id3, &AgentObjectRecord::new(id3, "B", 2, 2)).unwrap();
        drop(s_a);
        // No snapshot open: this write must not be hidden by the stale chain [A, B].
        engine.put(id3, &AgentObjectRecord::new(id3, "C", 3, 3)).unwrap();
        let s_b = engine.begin_read_snapshot();
        assert_eq!(s_b.get(id3).unwrap().unwrap().1.title, "C");
        drop(s_b);

        drop(engine);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

