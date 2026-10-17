//! Allocation-Counting Global Allocator & Latency Percentile Harness (`TASK-002`, `RULE-MEM-01`, `RULE-TEST-02`).

#![deny(unsafe_op_in_unsafe_fn)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::time::{Duration, Instant};

thread_local! {
    static TLS_ALLOC_COUNT: Cell<u64> = const { Cell::new(0) };
    static TLS_ALLOC_BYTES: Cell<u64> = const { Cell::new(0) };
}

/// Custom `#[global_allocator]` wrapper over the OS allocator that tracks exact thread-local
/// heap allocation counts and bytes with `< 2 ns` overhead so hot-path functions can be
/// strictly verified for zero heap allocations (`RULE-MEM-01`).
#[derive(Debug, Default)]
pub struct TrackingAllocator;

// SAFETY: Delegates directly to `std::alloc::System` while incrementing thread-local counters.
unsafe impl GlobalAlloc for TrackingAllocator {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = TLS_ALLOC_COUNT.try_with(|c| c.set(c.get().wrapping_add(1)));
        let _ = TLS_ALLOC_BYTES.try_with(|b| b.set(b.get().wrapping_add(layout.size() as u64)));
        // SAFETY: Forwarded to `System::alloc`.
        unsafe { System.alloc(layout) }
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: Forwarded to `System::dealloc`.
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// Snapshot of heap allocations performed by the current thread during a measured closure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AllocStats {
    pub allocations: u64,
    pub bytes: u64,
}

/// Measures the exact number of heap allocations and bytes allocated by `f` on the current thread.
#[inline]
pub fn measure_allocations<R>(f: impl FnOnce() -> R) -> (R, AllocStats) {
    let start_cnt = TLS_ALLOC_COUNT.with(|c| c.get());
    let start_bytes = TLS_ALLOC_BYTES.with(|b| b.get());
    let out = f();
    let end_cnt = TLS_ALLOC_COUNT.with(|c| c.get());
    let end_bytes = TLS_ALLOC_BYTES.with(|b| b.get());
    (
        out,
        AllocStats {
            allocations: end_cnt.saturating_sub(start_cnt),
            bytes: end_bytes.saturating_sub(start_bytes),
        },
    )
}

/// Asserts that executing `f` performs **zero** heap allocations on the current thread (`RULE-MEM-01`).
#[inline]
pub fn assert_zero_alloc<R>(label: &str, f: impl FnOnce() -> R) -> R {
    let (res, stats) = measure_allocations(f);
    assert_eq!(
        stats.allocations, 0,
        "RULE-MEM-01 violation in '{label}': expected 0 heap allocations, found {} ({} bytes)",
        stats.allocations, stats.bytes
    );
    res
}

/// Latency percentile summary (`p50`, `p95`, `p99`, `p99.9`) and throughput metrics.
#[derive(Clone, Debug)]
pub struct BenchmarkMetrics {
    pub name: String,
    pub iterations: usize,
    pub total_duration: Duration,
    pub ops_per_sec: f64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub allocs_per_op: f64,
}

pub fn run_latency_bench(
    name: impl Into<String>,
    iterations: usize,
    mut op: impl FnMut(usize),
) -> BenchmarkMetrics {
    // Warmup & calibrate whether single op is faster than OS QPC tick (~100ns)
    let warmup_n = iterations.min(256).max(16);
    let t_warm = Instant::now();
    for i in 0..warmup_n {
        op(i);
    }
    let avg_warm_ns = t_warm.elapsed().as_nanos() as u64 / (warmup_n as u64);
    let batch_size = if avg_warm_ns < 50 {
        64usize
    } else if avg_warm_ns < 500 {
        16usize
    } else {
        1usize
    };

    let num_samples = (iterations / batch_size).max(100);
    let total_ops = num_samples * batch_size;
    let mut samples_ns = Vec::with_capacity(num_samples);

    let (_, alloc_stats) = measure_allocations(|| {
        let mut idx = 0usize;
        for _ in 0..num_samples {
            let t0 = Instant::now();
            for _ in 0..batch_size {
                op(idx);
                idx = idx.wrapping_add(1);
            }
            let elapsed_per_op = (t0.elapsed().as_nanos() as u64) / (batch_size as u64);
            samples_ns.push(elapsed_per_op);
        }
    });

    let total_ns: u64 = samples_ns.iter().map(|&s| s * (batch_size as u64)).sum();
    let total_duration = Duration::from_nanos(total_ns.max(1));
    samples_ns.sort_unstable();

    let pct = |p: f64| -> u64 {
        if samples_ns.is_empty() {
            return 0;
        }
        let idx = ((samples_ns.len() as f64 - 1.0) * p).round() as usize;
        samples_ns[idx.min(samples_ns.len() - 1)]
    };

    BenchmarkMetrics {
        name: name.into(),
        iterations: total_ops,
        total_duration,
        ops_per_sec: (total_ops as f64) / total_duration.as_secs_f64(),
        p50_ns: pct(0.50),
        p95_ns: pct(0.95),
        p99_ns: pct(0.99),
        p999_ns: pct(0.999),
        allocs_per_op: (alloc_stats.allocations as f64) / (total_ops.max(1) as f64),
    }
}
