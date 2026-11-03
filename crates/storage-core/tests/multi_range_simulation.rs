//! Multi-range clusters under the deterministic simulator (`TASK-137`, `TASK-142`).
//!
//! `STORINE_SIM_SEEDS=n` widens the sweep; `STORINE_SIM_SEED=s` replays one seed;
//! `STORINE_SIM_SEED_RANGE=a-b` runs seeds `a..=b` (shards of `tools/modal/sweep.py`).

use std::sync::Arc;

use storage_core::dist::alloc::AllocatorConfig;
use storage_core::dist::node::{NodeConfig, NodeDriver};
use storage_core::dist::sim::{run, DistSimConfig, DistSimReport};
use storage_core::raft::KeySpan;
use storage_core::{DocumentRecord, EngineConfig, StorageEngine};

fn seeds(default: u64) -> Vec<u64> {
    if let Some(s) = std::env::var("STORINE_SIM_SEED").ok().and_then(|s| s.parse().ok()) {
        return vec![s];
    }
    // A shard of a distributed sweep (`tools/modal/sweep.py`): seeds `a..=b`.
    if let Some((a, b)) = std::env::var("STORINE_SIM_SEED_RANGE").ok().and_then(|r| r.split_once('-').and_then(|(a, b)| Some((a.parse::<u64>().ok()?, b.parse::<u64>().ok()?)))) {
        return (a..=b).collect();
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

#[test]
fn ranges_merge_safely() {
    let cfg = DistSimConfig { nodes: 4, regions: vec![0, 0, 0, 0], replication: 3, merges: true, moves: true, ops_per_client: 80, ..DistSimConfig::default() };
    let reports = sweep("merges", &cfg, 10);
    let merges: usize = reports.iter().map(|r| r.merges).sum();
    eprintln!("merges: {merges} merges");
    assert!(merges > 0);
}

/// No simulator-driven changes: the drivers' allocators split hot and large ranges, merge
/// small ones, repair and rebalance replicas and leases, under crashes and partitions.
#[test]
fn allocator_splits_merges_and_rebalances_safely() {
    let alloc = AllocatorConfig { replication: 3, split_keys: 8, split_writes_per_sec: 12, merge_keys: 7, interval_us: 300_000, dead_after_us: 3_000_000, rebalance_slack: 2 };
    let node = NodeConfig { cluster: vec![1, 2, 3, 4, 5], allocator: Some(alloc), ..DistSimConfig::default().node };
    let cfg = DistSimConfig { nodes: 5, regions: vec![0; 5], initial_ranges: 6, replication: 3, splits: false, ops_per_client: 80, keys: 16, node, ..DistSimConfig::default() };
    let reports = sweep("allocator", &cfg, 10);
    let s = |f: fn(&DistSimReport) -> u64| reports.iter().map(f).sum::<u64>();
    eprintln!("allocator: {} actions, {} splits, {} merges applied", s(|r| r.allocator_actions), s(|r| r.applied_splits), s(|r| r.applied_merges));
    assert!(s(|r| r.applied_splits) > 0 && s(|r| r.applied_merges) > 0);
    for r in &reports {
        let (lo, hi) = (r.replicas_per_node.iter().min().copied().unwrap_or(0), r.replicas_per_node.iter().max().copied().unwrap_or(0));
        assert!(hi - lo <= 3, "seed {}: replicas per node {:?}", r.seed, r.replicas_per_node);
    }
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

/// Zone configurations (`TASK-141`) on 6 nodes in 3 regions, two zones each, with the
/// allocator alone placing replicas under crashes and partitions:
/// - `kv/k000`..`kv/k007`: 3 voters only in regions 0 and 1, leases preferring region 1;
/// - `kv/k008`..: 5 voters surviving the loss of any region, leases preferring region 2.
///
/// Once settled, every range honours its configuration and no range straddles two.
#[test]
fn zone_configs_place_replicas_and_leases() {
    use storage_core::dist::zone::{Constraint, Locality, Survival, Tier, ZoneConfig, ZoneConfigs};
    let regions = vec![0u8, 0, 1, 1, 2, 2];
    let localities = regions.iter().enumerate().map(|(i, &r)| ((i + 1) as u16, Locality::new(&format!("r{r}"), &format!("z{i}")))).collect();
    let mut zones = ZoneConfigs::default();
    let low = ZoneConfig { replicas: 3, constraints: vec![Constraint::prohibited(Tier::Region, "r2")], lease_preferences: vec!["r1".into()], ..ZoneConfig::default() };
    let high = ZoneConfig { replicas: 5, survival: Survival::Region, lease_preferences: vec!["r2".into()], ..ZoneConfig::default() };
    zones.set(KeySpan::new(b"kv/k000".to_vec(), Some(b"kv/k008".to_vec())), low.clone()).expect("zone");
    zones.set(KeySpan::new(b"kv/k008".to_vec(), Some(b"kv0".to_vec())), high.clone()).expect("zone");
    let alloc = AllocatorConfig { replication: 3, split_keys: 1_000, split_writes_per_sec: 100_000, merge_keys: 0, interval_us: 300_000, dead_after_us: 3_000_000, rebalance_slack: 2 };
    let node = NodeConfig { cluster: vec![1, 2, 3, 4, 5, 6], allocator: Some(alloc), localities, zones: zones.clone(), ..DistSimConfig::default().node };
    let cfg = DistSimConfig { nodes: 6, regions: regions.clone(), initial_ranges: 1, replication: 3, splits: false, ops_per_client: 40, keys: 16, node, ..DistSimConfig::default() };
    // Regression seeds: 39 once created two ranges over one span (a split proposed twice); 89
    // never settled (two nodes rebalanced onto one store in back-to-back turns, and back); 166
    // acknowledged a lost write (it took the outcome of a later proposal at the same index).
    let mut reports = sweep("zones", &cfg, 6);
    if std::env::var("STORINE_SIM_SEED").is_err() && std::env::var("STORINE_SIM_SEED_RANGE").is_err() {
        for seed in [39, 89, 166] {
            reports.push(run(&std::env::temp_dir().join(format!("storine_zones_{seed}_{}", std::process::id())), seed, &cfg).unwrap_or_else(|f| panic!("zones seed {seed}: {f}")));
        }
    }
    let region = |n: u16| regions[usize::from(n) - 1];
    for r in &reports {
        let configured = |z: &ZoneConfig| r.final_ranges.iter().any(|(span, _, _)| zones.get(&span.start) == z);
        assert!(configured(&low) && configured(&high), "seed {}: a configured span has no range", r.seed);
        for (span, members, leader) in &r.final_ranges {
            assert_eq!(zones.boundary_inside(span), None, "seed {}: {span:?} straddles zone configurations", r.seed);
            let z = zones.get(&span.start);
            if z == &zones.default {
                continue;
            }
            let voters: Vec<u16> = members.iter().filter(|m| m.1).map(|m| m.0).collect();
            assert_eq!(voters.len(), z.replicas, "seed {}: {span:?} has voters {voters:?}", r.seed);
            let in_region = |want: u8| voters.iter().filter(|&&v| region(v) == want).count();
            if z == &low {
                assert_eq!(in_region(2), 0, "seed {}: {span:?} has a voter in the prohibited region: {voters:?}", r.seed);
                assert_eq!(region(*leader), 1, "seed {}: {span:?} lease on n{leader}, not in region 1", r.seed);
            } else {
                assert!((0..3).all(|g| in_region(g) < 3), "seed {}: {span:?} has a region holding a quorum: {voters:?}", r.seed);
                assert_eq!(region(*leader), 2, "seed {}: {span:?} lease on n{leader}, not in region 2", r.seed);
            }
        }
    }
}

/// Zone configuration changes (`TASK-141`): invalid configurations are refused, and dropping a
/// global span keeps writes above what it may have closed ahead of time until that passes.
#[test]
fn zone_changes_keep_closed_promises() {
    use storage_core::dist::hlc::Timestamp;
    use storage_core::dist::zone::{self, Locality, Survival, ZoneConfig, ZoneConfigs};
    let root = std::env::temp_dir().join(format!("storine_zone_change_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let engine = StorageEngine::<DocumentRecord>::open(root.join("n1"), EngineConfig { background_flush: false, collection_name: "node".into(), ..EngineConfig::default() }).unwrap();
    let localities = [(1u16, "eu", "a"), (2, "eu", "b"), (3, "us", "a")].iter().map(|&(n, r, z)| (n, Locality::new(r, z))).collect();
    let max_offset = 100_000_000;
    let mut zones = ZoneConfigs::default();
    zone::global(&mut zones, "rates", 1_000_000_000, &ZoneConfig::default()).unwrap();
    let mut d = NodeDriver::new(1, Arc::new(engine), NodeConfig { cluster: vec![1, 2, 3], localities, zones, max_offset_ns: max_offset, ..NodeConfig::default() });
    let now = Timestamp { wall: 10_000_000_000, logical: 0 };
    d.set_clock(now);
    // New leaseholders start above the largest lead plus the offset.
    assert!(d.fresh_floor(now).wall >= now.wall + 1_000_000_000 + max_offset);
    // Region survival over two regions cannot be met: refused, nothing changes.
    let bad = ZoneConfigs::new(ZoneConfig { replicas: 3, survival: Survival::Region, ..ZoneConfig::default() });
    assert!(d.set_zones(bad).is_err());
    assert_eq!(d.lead_floor(), Timestamp::ZERO);
    // Dropping the global span: its promises still bind writes.
    d.set_zones(ZoneConfigs::default()).unwrap();
    assert!(d.lead_floor().wall >= now.wall + 1_000_000_000 + max_offset);
    assert!(d.fresh_floor(now) >= d.lead_floor());
    let _ = std::fs::remove_dir_all(&root);
}

/// A split proposed twice at one key (the allocator plans again before the first applies)
/// splits once: the second is refused, never announced as a second range over the same keys
/// (found by `zone_configs_place_replicas_and_leases`, seed 39).
#[test]
fn a_repeated_split_splits_once() {
    let root = std::env::temp_dir().join(format!("storine_double_split_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let members = [1u16, 2, 3];
    let mut nodes: Vec<NodeDriver> = members
        .iter()
        .map(|&id| {
            let engine = StorageEngine::<DocumentRecord>::open(root.join(format!("n{id}")), EngineConfig { background_flush: false, collection_name: "node".into(), ..EngineConfig::default() }).unwrap();
            let mut d = NodeDriver::new(id, Arc::new(engine), NodeConfig { seed: u64::from(id), cluster: members.to_vec(), ..NodeConfig::default() });
            d.open_range(1, KeySpan::all(), &members).unwrap();
            d
        })
        .collect();
    // Instant, lossless delivery.
    let mut now = 0u64;
    let pump = |nodes: &mut Vec<NodeDriver>, now: &mut u64, rounds: usize| {
        for _ in 0..rounds {
            *now += 10_000;
            for n in nodes.iter_mut() {
                n.tick(*now).unwrap();
            }
            for _ in 0..4 {
                let out: Vec<_> = nodes.iter_mut().flat_map(|n| n.drain_outbox()).collect();
                for (to, b) in out {
                    nodes[usize::from(to) - 1].step(b, *now).unwrap();
                }
            }
        }
    };
    let leader = loop {
        pump(&mut nodes, &mut now, 1);
        if let Some(i) = (0..3).find(|&i| nodes[i].is_leader(1)) {
            break i;
        }
    };
    // Both proposed before either commits.
    nodes[leader].propose_split(1, b"m".to_vec(), 100).unwrap();
    nodes[leader].propose_split(1, b"m".to_vec(), 101).unwrap();
    pump(&mut nodes, &mut now, 50);
    for d in &nodes {
        let mut spans: Vec<KeySpan> = d.ranges().into_iter().filter_map(|r| d.replica(r).map(|x| x.span())).collect();
        spans.sort_by(|a, b| a.start.cmp(&b.start));
        assert_eq!(spans, vec![KeySpan::new(Vec::new(), Some(b"m".to_vec())), KeySpan::new(b"m".to_vec(), None)], "n{} ranges {:?}", d.id(), d.ranges());
    }
    let _ = std::fs::remove_dir_all(&root);
}
