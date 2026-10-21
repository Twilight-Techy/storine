//! 2048-way `CachePadded` striped per-record lock table (`TASK-013`, `F-CONC-03`, `RULE-CPU-01`, `RULE-CONC-02`).
//!
//! Ensures writes to different records execute in parallel without contention while writes to the
//! same record serialize deterministically. Supports deadlock-free multi-record lock acquisition
//! by sorting stripe indices in canonical ascending order.

use std::sync::{Mutex, MutexGuard};
use storage_types::{CachePadded, InlineId};

pub const WRITE_LOCK_STRIPES: usize = 2048;
const STRIPE_MASK: usize = WRITE_LOCK_STRIPES - 1;

/// Striped lock table with 2048 cache-line-padded mutexes (`128 KB` total footprint).
pub struct StripedLockTable {
    stripes: Box<[CachePadded<Mutex<()>>; WRITE_LOCK_STRIPES]>,
}

impl core::fmt::Debug for StripedLockTable {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StripedLockTable")
            .field("stripes", &WRITE_LOCK_STRIPES)
            .finish()
    }
}

impl Default for StripedLockTable {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RecordLockGuard<'a> {
    _guard: MutexGuard<'a, ()>,
}

impl<'a> core::fmt::Debug for RecordLockGuard<'a> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("RecordLockGuard")
    }
}

pub struct MultiRecordLockGuard<'a> {
    _guards: Vec<MutexGuard<'a, ()>>,
}

impl<'a> core::fmt::Debug for MultiRecordLockGuard<'a> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("MultiRecordLockGuard")
            .field("locked_stripes", &self._guards.len())
            .finish()
    }
}

impl StripedLockTable {
    pub fn new() -> Self {
        let stripes = CachePadded::boxed_array(|| Mutex::new(()));
        Self { stripes }
    }

    #[inline]
    pub const fn stripe_index(id: InlineId) -> usize {
        // Use upper mixed bits of `fast_hash` (orthogonal to the lower 6 bits used by the 64-shard PrimaryIndex)
        ((id.fast_hash() >> 16) as usize) & STRIPE_MASK
    }

    /// Acquires the per-record write queue lock for a single `id`.
    #[inline]
    pub fn lock_one(&self, id: InlineId) -> RecordLockGuard<'_> {
        let idx = Self::stripe_index(id);
        let guard = self.stripes[idx]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        RecordLockGuard { _guard: guard }
    }

    /// Acquires write locks for an arbitrary set of `InlineId`s in canonical ascending stripe
    /// index order (`RULE-CONC-02`), making deadlocks structurally impossible even under
    /// overlapping multi-record transactions or bidirectional graph links.
    #[inline]
    pub fn lock_many(&self, ids: &[InlineId]) -> MultiRecordLockGuard<'_> {
        let mut indices: Vec<usize> = ids.iter().map(|&id| Self::stripe_index(id)).collect();
        indices.sort_unstable();
        indices.dedup();

        let mut guards = Vec::with_capacity(indices.len());
        for idx in indices {
            let g = self.stripes[idx]
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guards.push(g);
        }
        MultiRecordLockGuard { _guards: guards }
    }

    /// Acquires every stripe in ascending order, excluding all writers until the guard drops.
    pub fn lock_all(&self) -> MultiRecordLockGuard<'_> {
        let mut guards = Vec::with_capacity(WRITE_LOCK_STRIPES);
        for stripe in self.stripes.iter() {
            guards.push(stripe.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
        }
        MultiRecordLockGuard { _guards: guards }
    }

    /// Acquires and immediately releases every stripe in ascending order.
    ///
    /// Every writer holds its stripe lock from WAL append until its in-memory apply completes,
    /// so once this returns, every write whose WAL record was appended before the call has been
    /// fully applied. Used to pick a safe WAL checkpoint cutoff without pausing writers.
    pub fn barrier(&self) {
        for stripe in self.stripes.iter() {
            drop(stripe.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
        }
    }
}
