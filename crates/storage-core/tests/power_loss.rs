//! Simulated power loss and crash points against the whole engine (`F-SIM-01`, `F-SIM-05`,
//! `TASK-132`, `TASK-136`, `RULE-TEST-03`).
//!
//! A seeded workload of puts, deletes, multi-record transactions and checkpoints runs on a
//! [`SimVfs`] until an I/O call fails at a chosen crash point. The disk then loses power (or the
//! process just dies), the engine reopens, and the test checks:
//! - recovery succeeds;
//! - every acknowledged write is there, with its value; nothing else changed, except that the
//!   one write in flight at the crash may or may not have happened;
//! - an interrupted transaction is all or nothing;
//! - the secondary index equals what the records say (no stale or missing postings);
//! - the files under the data directory all went through the seam.
//!
//! With background flushing off, a run is a pure function of its seed and crash point, so a
//! failure message names everything needed to replay it.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use storage_core::{AgentObjectRecord, EngineConfig, IndexTier, PrimaryTier, StorageEngine, StorageLayoutMode, FIELD_SCORE};
use storage_types::testing::SweepRng;
use storage_types::vfs::{CutMode, SimVfs};
use storage_types::{InlineId, StorageError};

const IDS: u64 = 16;

fn id(i: u64) -> InlineId {
    InlineId::new(&format!("rec_{i:02}")).unwrap()
}

/// What the engine has acknowledged, per record (`None`: absent).
type Model = BTreeMap<u64, Option<i64>>;

/// The one operation in flight when the process stopped.
#[derive(Debug, Clone)]
enum InFlight {
    None,
    Put(u64, i64),
    Delete(u64),
    Txn(Vec<u64>, i64),
    Checkpoint,
}

fn layouts() -> Vec<(&'static str, EngineConfig)> {
    let base = EngineConfig { background_flush: false, ..EngineConfig::default() };
    vec![
        ("dedicated", EngineConfig { layout_mode: StorageLayoutMode::DedicatedFiles, ..base.clone() }),
        ("pingpong", EngineConfig { layout_mode: StorageLayoutMode::PingPongFiles { slot_stride: 4096 }, ..base.clone() }),
        (
            "arena",
            EngineConfig {
                layout_mode: StorageLayoutMode::AdaptiveArena { arena_threshold_bytes: 4096, segment_capacity: 64 * 1024 },
                ..base.clone()
            },
        ),
        (
            "paged",
            EngineConfig {
                layout_mode: StorageLayoutMode::AdaptiveArena { arena_threshold_bytes: 4096, segment_capacity: 64 * 1024 },
                index_tier: IndexTier::Paged,
                primary_tier: PrimaryTier::Cold,
                ..base
            },
        ),
    ]
}

/// Runs `ops` random operations (or until an I/O call fails). Returns the operation in flight
/// at the failure, `None` if all completed.
fn workload(engine: &StorageEngine<AgentObjectRecord>, rng: &mut SweepRng, model: &mut Model, next: &mut i64, ops: usize) -> InFlight {
    for _ in 0..ops {
        *next += 1;
        let v = *next;
        let roll = rng.below(100);
        let (op, res): (InFlight, Result<(), StorageError>) = if roll < 50 {
            let i = rng.below(IDS as usize) as u64;
            let rec = AgentObjectRecord::new(id(i), format!("t{v}"), v, v);
            (InFlight::Put(i, v), engine.put(id(i), &rec).map(|_| ()))
        } else if roll < 65 {
            let i = rng.below(IDS as usize) as u64;
            (InFlight::Delete(i), engine.delete(id(i)).map(|_| ()))
        } else if roll < 90 {
            let mut ids: Vec<u64> = (0..3).map(|_| rng.below(IDS as usize) as u64).collect();
            ids.sort_unstable();
            ids.dedup();
            let res = engine.transaction(|tx| {
                for &i in &ids {
                    tx.put(id(i), AgentObjectRecord::new(id(i), format!("t{v}"), v, v));
                }
                Ok(())
            });
            (InFlight::Txn(ids, v), res)
        } else {
            (InFlight::Checkpoint, engine.flush_all().map(|_| ()))
        };
        match res {
            Ok(()) => match op {
                InFlight::Put(i, v) => {
                    model.insert(i, Some(v));
                }
                InFlight::Delete(i) => {
                    model.insert(i, None);
                }
                InFlight::Txn(ids, v) => {
                    for i in ids {
                        model.insert(i, Some(v));
                    }
                }
                InFlight::Checkpoint | InFlight::None => {}
            },
            Err(_) => return op,
        }
    }
    InFlight::None
}

/// Checks the reopened engine against the model and returns what it holds (the new truth).
fn verify(engine: &StorageEngine<AgentObjectRecord>, model: &Model, inflight: &InFlight, ctx: &str) -> Model {
    let mut got = Model::new();
    for i in 0..IDS {
        let v = match engine.get_owned(id(i)) {
            Ok((_, r)) => {
                assert_eq!(r.timestamp, r.score, "{ctx}: rec {i} torn (fields from two writes)");
                assert_eq!(r.title, format!("t{}", r.score), "{ctx}: rec {i} torn title");
                Some(r.score)
            }
            Err(StorageError::RecordNotFound { .. }) => None,
            Err(e) => panic!("{ctx}: rec {i} unreadable after recovery: {e}"),
        };
        got.insert(i, v);
    }
    for i in 0..IDS {
        let want = model.get(&i).copied().flatten();
        let have = got[&i];
        if have == want {
            continue;
        }
        let in_flight_ok = match inflight {
            InFlight::Put(j, v) => *j == i && have == Some(*v),
            InFlight::Delete(j) => *j == i && have.is_none(),
            InFlight::Txn(ids, v) => ids.contains(&i) && have == Some(*v),
            InFlight::Checkpoint | InFlight::None => false,
        };
        assert!(in_flight_ok, "{ctx}: rec {i} is {have:?}, acknowledged {want:?} (in flight: {inflight:?})");
    }
    if let InFlight::Txn(ids, v) = inflight {
        let applied = ids.iter().filter(|i| got[i] == Some(*v)).count();
        assert!(applied == 0 || applied == ids.len(), "{ctx}: transaction {v} half applied ({applied} of {})", ids.len());
    }
    // The score index lists exactly the live records.
    let mut indexed: Vec<InlineId> = engine.query().filter_range(FIELD_SCORE, i64::MIN..=i64::MAX).execute_ids();
    indexed.sort();
    let mut live: Vec<InlineId> = got.iter().filter(|(_, v)| v.is_some()).map(|(i, _)| id(*i)).collect();
    live.sort();
    assert_eq!(indexed, live, "{ctx}: score index differs from the records");
    for (i, v) in &got {
        if let Some(v) = v {
            let hits = engine.query().filter_range(FIELD_SCORE, *v..=*v).execute_ids();
            assert!(hits.contains(&id(*i)), "{ctx}: rec {i} missing from the index at {v}");
        }
    }
    got
}

fn untracked_ok(sim: &SimVfs, ctx: &str) {
    let stray: Vec<_> = sim.untracked().into_iter().filter(|p| !p.to_string_lossy().contains("spill")).collect();
    assert!(stray.is_empty(), "{ctx}: files written around the Vfs seam: {stray:?}");
}

fn fresh(dir: &Path) {
    let _ = fs::remove_dir_all(dir);
    fs::create_dir_all(dir).unwrap();
}

/// Random crash points, random power cuts, three lives per seed, every layout.
#[test]
fn acknowledged_writes_survive_power_loss() {
    let seeds: u64 = std::env::var("STORINE_SIM_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(24);
    for (name, cfg) in layouts() {
        let dir = std::env::temp_dir().join(format!("storine_power_loss_{name}_{}", std::process::id()));
        let (mut acked, mut lost_sectors, mut lost_dir_ops, mut cuts) = (0usize, 0u64, 0usize, 0usize);
        for seed in 1..=seeds {
            fresh(&dir);
            let sim = SimVfs::new(&dir, seed).unwrap();
            let _mount = sim.mount();
            if seed % 6 == 0 {
                sim.set_mode(CutMode::LoseAll);
            }
            let mut rng = SweepRng::new(seed * 0x9E37_79B9 + 1);
            let mut model = Model::new();
            let mut next = 0i64;
            for life in 0..3 {
                let ctx = format!("layout {name} seed {seed} life {life}");
                let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg.clone()).unwrap_or_else(|e| panic!("{ctx}: open failed: {e}"));
                if life > 0 {
                    model = verify(&engine, &model, &InFlight::None, &ctx);
                }
                let crash_at = 20 + rng.below(400) as u64;
                sim.fail_at(crash_at);
                let before = next;
                let inflight = workload(&engine, &mut rng, &mut model, &mut next, 200);
                acked += (next - before) as usize - usize::from(!matches!(inflight, InFlight::None));
                drop(engine);
                let ctx = format!("{ctx} crash_at {crash_at}");
                let cut = if life == 1 { sim.crash() } else { sim.power_cut() }.unwrap();
                cuts += 1;
                lost_sectors += cut.sectors_lost + cut.sectors_zeroed;
                lost_dir_ops += cut.dir_ops_lost;
                let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg.clone()).unwrap_or_else(|e| panic!("{ctx}: recovery failed: {e} ({cut:?})"));
                model = verify(&engine, &model, &inflight, &format!("{ctx} {cut:?}"));
                drop(engine);
                untracked_ok(&sim, &ctx);
                // Recovery's own writes are durable before the next life.
                sim.crash().unwrap();
            }
        }
        eprintln!("{name}: {seeds} seeds, {cuts} crashes, {acked} acknowledged operations, {lost_sectors} unsynced sectors and {lost_dir_ops} directory changes lost");
        assert!(acked > 0 && lost_sectors > 0, "{name}: the workload never exercised a power cut");
        let _ = fs::remove_dir_all(&dir);
    }
}

/// `TASK-136`: a fixed workload is stopped at every `k`-th I/O call in turn, under a power cut
/// and under a process crash, and recovered.
#[test]
fn crash_at_every_io_call() {
    let ops = 40;
    let stride: u64 = std::env::var("STORINE_SWEEP_STRIDE").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
    for (name, cfg) in layouts() {
        let dir = std::env::temp_dir().join(format!("storine_sweep_{name}_{}", std::process::id()));
        // Count the I/O calls of an uninterrupted run.
        fresh(&dir);
        let total = {
            let sim = SimVfs::new(&dir, 1).unwrap();
            let _mount = sim.mount();
            let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg.clone()).unwrap();
            let before = sim.ops();
            let (mut model, mut next) = (Model::new(), 0);
            let r = workload(&engine, &mut SweepRng::new(77), &mut model, &mut next, ops);
            assert!(matches!(r, InFlight::None));
            let n = sim.ops() - before;
            drop(engine);
            n
        };
        assert!(total > ops as u64, "{name}: only {total} I/O calls");
        let mut k = 1;
        while k <= total + 1 {
            for (mode, cut) in [("power", CutMode::Random), ("kill", CutMode::KeepAll), ("lose", CutMode::LoseAll)] {
                fresh(&dir);
                let sim = SimVfs::new(&dir, k * 31 + 7).unwrap();
                let _mount = sim.mount();
                let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg.clone()).unwrap();
                sim.fail_at(k);
                let (mut model, mut next) = (Model::new(), 0);
                let inflight = workload(&engine, &mut SweepRng::new(77), &mut model, &mut next, ops);
                drop(engine);
                sim.set_mode(cut);
                let report = sim.power_cut().unwrap();
                let ctx = format!("layout {name} k {k}/{total} {mode} {report:?}");
                let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg.clone()).unwrap_or_else(|e| panic!("{ctx}: recovery failed: {e}"));
                verify(&engine, &model, &inflight, &ctx);
                drop(engine);
                untracked_ok(&sim, &ctx);
            }
            k += stride;
        }
        let _ = fs::remove_dir_all(&dir);
    }
}
