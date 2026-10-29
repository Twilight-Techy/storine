//! Multi-range clusters under the deterministic simulator (`TASK-137`, `TASK-142`).
//!
//! `STORINE_SIM_SEEDS=n` widens the sweep; `STORINE_SIM_SEED=s` replays one seed.

use std::sync::Arc;

use storage_core::dist::node::{NodeConfig, NodeDriver};
use storage_core::dist::sim::{run, DistSimConfig, DistSimReport};
use storage_core::raft::KeySpan;
use storage_core::{DocumentRecord, EngineConfig, StorageEngine};

fn seeds(default: u64) -> Vec<u64> {
    if let Some(s) = std::env::var("STORINE_SIM_SEED").ok().and_then(|s| s.parse().ok()) {
        return vec![s];
    }
    let n = std::env::var("STORINE_SIM_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(default);
    (1..=n).collect()
}

fn sweep(name: &str, cfg: &DistSimConfig, default: u64) -> Vec<DistSimReport> {
    let root = std::env::temp_dir().join(format!("storine_dist_sim_{name}_{}", std::process::id()));
    let mut out = Vec::new();
    for seed in seeds(default) {
        match run(&root, seed, cfg) {
            Ok(r) => out.push(r),
            Err(f) => panic!("{name}: {f}\nreplay: STORINE_SIM_SEED={seed}"),
        }
    }
    let s = |f: fn(&DistSimReport) -> usize| out.iter().map(f).sum::<usize>();
    eprintln!(
        "{name}: {} seeds, max {} ranges, {} splits, {} crashes, {} partitions ({} region outages), {} writes ok / {} unknown, {} reads",
        out.len(),
        out.iter().map(|r| r.ranges).max().unwrap_or(0),
        s(|r| r.splits),
        s(|r| r.crashes),
        s(|r| r.partitions),
        s(|r| r.region_outages),
        s(|r| r.writes_ok),
        s(|r| r.writes_unknown),
        s(|r| r.reads_ok)
    );
    out
}

#[test]
fn many_ranges_split_crash_and_partition_safely() {
    let reports = sweep("ranges", &DistSimConfig::default(), 12);
    assert!(reports.iter().map(|r| r.splits).sum::<usize>() > 0);
    assert!(reports.iter().map(|r| r.writes_ok).sum::<usize>() > 0);
}

#[test]
fn three_regions_with_region_outages() {
    let cfg = DistSimConfig {
        nodes: 5,
        regions: vec![0, 0, 1, 1, 2],
        region_outages: true,
        client_timeout: 3_000_000,
        ..DistSimConfig::default()
    };
    sweep("regions", &cfg, 6);
}

#[test]
fn replicas_move_and_leases_transfer_safely() {
    let cfg = DistSimConfig { nodes: 5, regions: vec![0, 0, 0, 0, 0], replication: 3, moves: true, fault_every: 250_000, ..DistSimConfig::default() };
    let reports = sweep("moves", &cfg, 10);
    let moves: usize = reports.iter().map(|r| r.moves).sum();
    let transfers: usize = reports.iter().map(|r| r.transfers).sum();
    eprintln!("moves: {moves} replica moves, {transfers} lease transfers");
    assert!(moves > 0 && transfers > 0);
}

/// Idle cost scales with nodes, not ranges: 3 nodes sharing 200 idle ranges exchange about
/// one batch per node pair per heartbeat once the ranges quiesce.
#[test]
fn idle_ranges_quiesce() {
    let root = std::env::temp_dir().join(format!("storine_quiesce_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let members = [1u16, 2, 3];
    let cfg = NodeConfig { election_min_us: 300_000, election_max_us: 600_000, heartbeat_us: 50_000, ..NodeConfig::default() };
    let mut nodes: Vec<NodeDriver> = members
        .iter()
        .map(|&id| {
            let engine = StorageEngine::<DocumentRecord>::open(root.join(format!("n{id}")), EngineConfig { background_flush: false, collection_name: "node".into(), ..EngineConfig::default() }).unwrap();
            NodeDriver::new(id, Arc::new(engine), NodeConfig { seed: u64::from(id), ..cfg.clone() })
        })
        .collect();
    let ranges = 200u64;
    for n in &mut nodes {
        for r in 0..ranges {
            let start = if r == 0 { Vec::new() } else { format!("k{r:05}").into_bytes() };
            let end = (r + 1 < ranges).then(|| format!("k{:05}", r + 1).into_bytes());
            n.open_range(r + 1, KeySpan::new(start, end), &members).unwrap();
        }
    }
    // A lossless network with 1 ms latency, ticked every 10 ms.
    let mut now = 0u64;
    let mut inflight: Vec<(u64, usize, storage_core::dist::node::Batch)> = Vec::new();
    let mut run_for = |nodes: &mut Vec<NodeDriver>, now: &mut u64, us: u64| -> (u64, u64) {
        let (mut batches, mut msgs) = (0u64, 0u64);
        let end = *now + us;
        while *now < end {
            *now += 10_000;
            for n in nodes.iter_mut() {
                n.tick(*now).unwrap();
            }
            let due: Vec<_> = {
                let (d, rest): (Vec<_>, Vec<_>) = std::mem::take(&mut inflight).into_iter().partition(|(t, _, _)| *t <= *now);
                inflight = rest;
                d
            };
            for (_, to, b) in due {
                nodes[to].step(b, *now).unwrap();
            }
            for n in nodes.iter_mut() {
                for (to, b) in n.drain_outbox() {
                    batches += 1;
                    msgs += b.msgs.len() as u64;
                    inflight.push((*now + 1_000, usize::from(to) - 1, b));
                }
            }
        }
        (batches, msgs)
    };
    let _ = run_for(&mut nodes, &mut now, 5_000_000); // elections, then quiescence
    let leaders: usize = (1..=ranges).map(|r| nodes.iter().filter(|n| n.is_leader(r)).count()).sum();
    assert_eq!(leaders, ranges as usize, "every range has exactly one leader");
    let (batches, msgs) = run_for(&mut nodes, &mut now, 2_000_000);
    // 2 s idle = 40 heartbeat intervals x 6 directed pairs = 240 batches; no range messages.
    eprintln!("idle 2 s with {ranges} ranges: {batches} batches, {msgs} range messages");
    assert!(batches <= 6 * 45, "idle batches grow with ranges: {batches}");
    assert!(msgs < ranges, "quiescent ranges still talk: {msgs} messages");
    assert!(nodes.iter().map(|n| n.quiescent_ranges()).sum::<usize>() >= 3 * ranges as usize - 3);
    drop(nodes);
    let _ = std::fs::remove_dir_all(&root);
}
