//! Automatic arena compaction cost (`TASK-151`): write throughput and worst write latency with
//! the checkpoint the single-group server runs every 10 s, before and after it compacted.
//!
//! ```text
//! cargo run --release -p storage-benches --bin compaction-cost [secs per run] [pairs]
//! ```
//!
//! Mode `none` never checkpoints (the baseline); `flush` checkpoints with `flush_all` only (the
//! old checkpoint); `compact` with `checkpoint`, which then runs `maybe_compact_arenas`. Runs alternate, so drift on the machine
//! falls on both modes alike. Writers overwrite 200,000 records with bodies whose 64-byte size
//! class changes, so freed slots are seldom reused in place and dead space builds up.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use storage_core::document::DOCUMENT_LAYOUT;
use storage_core::{AgentObjectRecord, EngineConfig, StorageEngine};
use storage_types::InlineId;
use storage_wal::SyncPolicy;

const KEYS: u64 = 200_000;
const WRITERS: u64 = 4;
const CHECKPOINT_EVERY: Duration = Duration::from_secs(10);

fn dat_bytes(dir: &std::path::Path) -> u64 {
    let Ok(rd) = std::fs::read_dir(dir) else { return 0 };
    rd.flatten()
        .map(|e| {
            let p = e.path();
            if p.is_dir() {
                dat_bytes(&p)
            } else if p.extension().is_some_and(|x| x == "dat") {
                e.metadata().map_or(0, |m| m.len())
            } else {
                0
            }
        })
        .sum()
}

#[derive(Clone, Copy)]
enum Mode {
    /// No checkpoint: the baseline for write stalls from anything else.
    None,
    /// `flush_all` only (the checkpoint before automatic compaction).
    Flush,
    /// `checkpoint` (`flush_all`, then `maybe_compact_arenas`).
    Compact,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Mode::None => "none",
            Mode::Flush => "flush",
            Mode::Compact => "compact",
        }
    }
}

struct Outcome {
    writes: u64,
    worst_us: u64,
    checkpoint_ms_max: u128,
    disk_mib: u64,
    peak_disk_mib: u64,
}

fn run(mode: Mode, secs: u64) -> storage_types::Result<Outcome> {
    let dir = std::env::temp_dir().join(format!("stge_compaction_cost_{}_{}", std::process::id(), mode.name()));
    let _ = std::fs::remove_dir_all(&dir);
    // Memory-first WAL: fsync per write would cap the run at a few hundred writes a second,
    // too few to build up the dead space compaction starts at.
    let cfg = EngineConfig { layout_mode: DOCUMENT_LAYOUT, wal_sync_policy: SyncPolicy::AsyncMemoryFirst, ..EngineConfig::default() };
    let engine = Arc::new(StorageEngine::<AgentObjectRecord>::open(&dir, cfg)?);
    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicU64::new(0));
    let worst = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for w in 0..WRITERS {
        let (engine, stop, writes, worst) = (Arc::clone(&engine), Arc::clone(&stop), Arc::clone(&writes), Arc::clone(&worst));
        handles.push(std::thread::spawn(move || -> storage_types::Result<()> {
            let mut x = 0x9E37_79B9_7F4A_7C15u64 ^ (w + 1);
            while !stop.load(Ordering::Relaxed) {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                let k = x % KEYS;
                let id = InlineId::new(&format!("k{k:06}"))?;
                let mut rec = AgentObjectRecord::new(id, "t", k as i64, x as i64);
                rec.body = vec![b'x'; 64 + (x >> 40) as usize % 6 * 64];
                let t = Instant::now();
                engine.put(id, &rec)?;
                worst.fetch_max(t.elapsed().as_micros() as u64, Ordering::Relaxed);
                writes.fetch_add(1, Ordering::Relaxed);
            }
            Ok(())
        }));
    }
    let end = Instant::now() + Duration::from_secs(secs);
    let mut checkpoint_ms_max = 0;
    let mut peak_disk_mib = 0;
    while Instant::now() < end {
        std::thread::sleep(CHECKPOINT_EVERY.min(end.saturating_duration_since(Instant::now())));
        let t = Instant::now();
        match mode {
            Mode::Compact => drop(engine.checkpoint()?),
            Mode::Flush => drop(engine.flush_all()?),
            Mode::None => {}
        }
        checkpoint_ms_max = checkpoint_ms_max.max(t.elapsed().as_millis());
        peak_disk_mib = peak_disk_mib.max(dat_bytes(&dir) >> 20);
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().map_err(|_| storage_types::StorageError::resource_exhausted("writer panicked"))??;
    }
    let disk_mib = dat_bytes(&dir) >> 20;
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(Outcome { writes: writes.load(Ordering::Relaxed), worst_us: worst.load(Ordering::Relaxed), checkpoint_ms_max, disk_mib, peak_disk_mib })
}

fn main() -> storage_types::Result<()> {
    let mut args = std::env::args().skip(1).map(|a| a.parse::<u64>().unwrap_or(0));
    let secs = args.next().filter(|&s| s > 0).unwrap_or(60);
    let pairs = args.next().filter(|&p| p > 0).unwrap_or(2);
    println!("{:>8} | {:>10} | {:>12} | {:>16} | {:>14} | {:>14}", "mode", "writes/s", "worst put µs", "worst checkpoint", "end disk MiB", "peak disk MiB");
    for _ in 0..pairs {
        for mode in [Mode::None, Mode::Flush, Mode::Compact] {
            let o = run(mode, secs)?;
            println!(
                "{:>8} | {:>10.0} | {:>12} | {:>13} ms | {:>14} | {:>14}",
                mode.name(),
                o.writes as f64 / secs as f64,
                o.worst_us,
                o.checkpoint_ms_max,
                o.disk_mib,
                o.peak_disk_mib
            );
        }
    }
    Ok(())
}
