//! Cache-line alignment wrapper (`RULE-CPU-01`) to eliminate false sharing.

use core::ops::{Deref, DerefMut};

/// Pads and aligns `T` to a 64-byte CPU cache line boundary so concurrent threads
/// mutating adjacent slots in an array or struct never invalidate each other's L1/L2 cache lines.
#[repr(C, align(64))]
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct CachePadded<T> {
    value: T,
}

impl<T> CachePadded<T> {
    #[inline]
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    /// Builds a heap-allocated `[CachePadded<T>; N]` shard table without a panic path and without
    /// a large stack temporary (a `2,048`-stripe table is `128 KiB`).
    ///
    /// Fixed-size arrays let shard lookups (`hash & (N - 1)`) compile without bounds checks.
    pub fn boxed_array<const N: usize>(mut make: impl FnMut() -> T) -> Box<[Self; N]> {
        let shards: Box<[Self]> = (0..N).map(|_| Self::new(make())).collect();
        match shards.try_into() {
            Ok(array) => array,
            // Unreachable (`shards.len() == N` by construction); kept panic-free for `RULE-ERR-01`.
            Err(_) => Box::new(core::array::from_fn(|_| Self::new(make()))),
        }
    }

    #[inline]
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T> Deref for CachePadded<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> DerefMut for CachePadded<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

/// Number of cache-line slots in a [`StripedCounter`] (power of two).
const COUNTER_STRIPES: usize = 64;

/// Contention-free monotonic counter for hot-path telemetry (`RULE-CPU-01`, `RULE-SCALE-02`).
///
/// A single shared `AtomicU64` bounces its cache line between every core that increments it,
/// capping aggregate throughput regardless of core count. Each thread instead increments its own
/// cache-line-padded slot (chosen once per thread), and readers sum all slots.
pub struct StripedCounter {
    slots: Box<[CachePadded<std::sync::atomic::AtomicU64>; COUNTER_STRIPES]>,
}

impl StripedCounter {
    pub fn new() -> Self {
        Self {
            slots: CachePadded::boxed_array(|| std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Adds `n` to the calling thread's slot (one uncontended atomic add).
    #[inline]
    pub fn add(&self, n: u64) {
        self.slots[thread_stripe() & (COUNTER_STRIPES - 1)]
            .fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    }

    /// Sum of all slots (exact once concurrent writers have finished).
    pub fn sum(&self) -> u64 {
        self.slots
            .iter()
            .map(|s| s.load(std::sync::atomic::Ordering::Relaxed))
            .fold(0u64, u64::wrapping_add)
    }
}

impl Default for StripedCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl core::fmt::Debug for StripedCounter {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("StripedCounter").field(&self.sum()).finish()
    }
}

/// Stable per-thread stripe index, assigned round-robin on a thread's first use.
#[inline]
fn thread_stripe() -> usize {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static STRIPE: usize = NEXT.fetch_add(1, Ordering::Relaxed);
    }
    STRIPE.with(|s| *s)
}
