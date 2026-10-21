//! Lock-Free 64-Bucket Logarithmic Latency Histograms, Bounded Slow-Query Ring Buffer,
//! and Prometheus/OpenMetrics Text Exporter (`TASK-049`, `F-OBS-01`, `F-OBS-02`, `F-OBS-03`).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::engine::EngineStats;

/// Lock-free 64-bucket logarithmic latency histogram (`[AtomicU64; 64]`) (`TASK-049`, `F-OBS-01`).
///
/// Maps any latency `ns` (`u64`) to bucket `0..64` in 1 CPU cycle via `(63 - (ns | 1).leading_zeros())`
/// (`LZCNT` / `BSR`) and records a sample with a single `Relaxed` atomic fetch-add (**0 heap allocations**).
#[repr(align(64))]
#[derive(Debug)]
pub struct LatencyHistogram {
    buckets: [AtomicU64; 64],
    count: AtomicU64,
    sum_ns: AtomicU64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    pub const fn new() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; 64],
            count: AtomicU64::new(0),
            sum_ns: AtomicU64::new(0),
        }
    }

    /// Records a latency sample in nanoseconds in $O(1)$ time with **zero heap allocations**.
    #[inline(always)]
    pub fn record_ns(&self, ns: u64) {
        let bucket = (63 - (ns | 1).leading_zeros()) as usize;
        self.buckets[bucket & 63].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
    }

    /// Adds every sample of `other` into `self` (used to merge per-worker histograms for export).
    pub fn merge_from(&self, other: &Self) {
        for (dst, src) in self.buckets.iter().zip(other.buckets.iter()) {
            dst.fetch_add(src.load(Ordering::Relaxed), Ordering::Relaxed);
        }
        self.count
            .fetch_add(other.count.load(Ordering::Relaxed), Ordering::Relaxed);
        self.sum_ns
            .fetch_add(other.sum_ns.load(Ordering::Relaxed), Ordering::Relaxed);
    }

    #[inline]
    pub fn total_samples(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn mean_ns(&self) -> u64 {
        let c = self.total_samples();
        self.sum_ns.load(Ordering::Relaxed).checked_div(c).unwrap_or(0)
    }

    /// Estimates the latency in nanoseconds at `quantile` (e.g., `0.50`, `0.99`, `0.999`).
    pub fn percentile_ns(&self, quantile: f64) -> u64 {
        let total = self.total_samples();
        if total == 0 {
            return 0;
        }
        let q = quantile.clamp(0.0, 1.0);
        let target_rank = ((total as f64) * q).ceil().max(1.0) as u64;
        let mut cumulative = 0u64;
        for (i, b) in self.buckets.iter().enumerate() {
            let c = b.load(Ordering::Relaxed);
            if c == 0 {
                continue;
            }
            cumulative += c;
            if cumulative >= target_rank {
                return if i == 63 { u64::MAX } else { 1u64 << i };
            }
        }
        0
    }
}

/// Entry recorded in the bounded slow-operation ring buffer (`F-OBS-03`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlowOpEntry {
    pub operation: &'static str,
    pub duration_ns: u64,
    pub details: String,
}

/// Bounded ring buffer capturing operations slower than `threshold_ns` (`F-OBS-03`, `RULE-MEM-01`).
#[derive(Debug)]
pub struct SlowOperationLog {
    threshold_ns: AtomicU64,
    capacity: usize,
    entries: Mutex<VecDeque<SlowOpEntry>>,
}

impl SlowOperationLog {
    pub fn new(threshold_ns: u64, capacity: usize) -> Self {
        let cap = capacity.max(1);
        Self {
            threshold_ns: AtomicU64::new(threshold_ns),
            capacity: cap,
            entries: Mutex::new(VecDeque::with_capacity(cap)),
        }
    }

    /// Changes the slowness threshold at runtime.
    pub fn set_threshold_ns(&self, threshold_ns: u64) {
        self.threshold_ns.store(threshold_ns, Ordering::Relaxed);
    }

    /// Number of slow operations currently retained (at most the capacity).
    pub fn len(&self) -> usize {
        self.entries.lock().unwrap_or_else(|p| p.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn record_if_slow(
        &self,
        operation: &'static str,
        duration_ns: u64,
        details_fn: impl FnOnce() -> String,
    ) -> bool {
        if duration_ns < self.threshold_ns.load(Ordering::Relaxed) {
            return false;
        }
        let mut guard = self.entries.lock().unwrap_or_else(|p| p.into_inner());
        if guard.len() >= self.capacity {
            guard.pop_front();
        }
        guard.push_back(SlowOpEntry {
            operation,
            duration_ns,
            details: details_fn(),
        });
        true
    }

    pub fn snapshot(&self) -> Vec<SlowOpEntry> {
        self.entries
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .cloned()
            .collect()
    }
}

impl EngineStats {
    /// Formats `EngineStats` and optional read/write `LatencyHistogram`s as a standard
    /// Prometheus / OpenMetrics text exposition payload (`TASK-049`, `F-OBS-02`).
    pub fn to_prometheus_text(
        &self,
        read_hist: Option<&LatencyHistogram>,
        write_hist: Option<&LatencyHistogram>,
    ) -> String {
        let mut out = String::with_capacity(512);
        out.push_str(&format!(
            "# HELP storine_live_records Total live records in collection\n\
             # TYPE storine_live_records gauge\n\
             storine_live_records {}\n\
             # HELP storine_total_reads Cumulative read operations\n\
             # TYPE storine_total_reads counter\n\
             storine_total_reads {}\n\
             # HELP storine_total_writes Cumulative write operations\n\
             # TYPE storine_total_writes counter\n\
             storine_total_writes {}\n\
             # HELP storine_coalesced_flushes Background flushes coalesced in RAM\n\
             # TYPE storine_coalesced_flushes counter\n\
             storine_coalesced_flushes {}\n\
             # HELP storine_hot_dirty_bytes Current unflushed HotDirtyArc bytes\n\
             # TYPE storine_hot_dirty_bytes gauge\n\
             storine_hot_dirty_bytes {}\n",
            self.live_records,
            self.total_reads,
            self.total_writes,
            self.coalesced_flushes,
            self.hot_dirty_bytes,
        ));
        if let Some(rh) = read_hist {
            out.push_str(&format!(
                "# TYPE storine_read_latency_ns summary\n\
                 storine_read_latency_ns{{quantile=\"0.5\"}} {}\n\
                 storine_read_latency_ns{{quantile=\"0.99\"}} {}\n\
                 storine_read_latency_ns{{quantile=\"0.999\"}} {}\n\
                 storine_read_latency_ns_count {}\n",
                rh.percentile_ns(0.50),
                rh.percentile_ns(0.99),
                rh.percentile_ns(0.999),
                rh.total_samples(),
            ));
        }
        if let Some(wh) = write_hist {
            out.push_str(&format!(
                "# TYPE storine_write_latency_ns summary\n\
                 storine_write_latency_ns{{quantile=\"0.5\"}} {}\n\
                 storine_write_latency_ns{{quantile=\"0.99\"}} {}\n\
                 storine_write_latency_ns{{quantile=\"0.999\"}} {}\n\
                 storine_write_latency_ns_count {}\n",
                wh.percentile_ns(0.50),
                wh.percentile_ns(0.99),
                wh.percentile_ns(0.999),
                wh.total_samples(),
            ));
        }
        out
    }
}
