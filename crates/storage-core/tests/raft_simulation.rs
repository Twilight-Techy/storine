//! Seeded Raft cluster simulation (`F-SIM-02`, `F-SIM-03`, `TASK-133`, `TASK-134`).
//!
//! `STORINE_SIM_SEEDS=n` runs more seeds; `STORINE_SIM_SEED=s` replays one.

use storage_core::raft_sim::{run, SimConfig, SimReport};

fn seeds(default: u64) -> Vec<u64> {
    if let Some(s) = std::env::var("STORINE_SIM_SEED").ok().and_then(|s| s.parse().ok()) {
        return vec![s];
    }
    let n = std::env::var("STORINE_SIM_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(default);
    (1..=n).collect()
}

fn run_all(name: &str, cfg: &SimConfig, default_seeds: u64) -> Vec<SimReport> {
    let root = std::env::temp_dir().join(format!("storine_raft_sim_{name}_{}", std::process::id()));
    let mut reports = Vec::new();
    for seed in seeds(default_seeds) {
        match run(&root, seed, cfg) {
            Ok(r) => reports.push(r),
            Err(f) => panic!("{name}: {f}\nreplay with STORINE_SIM_SEED={seed}"),
        }
    }
    let sum = |f: fn(&SimReport) -> usize| reports.iter().map(f).sum::<usize>();
    eprintln!(
        "{name}: {} seeds, {} events, {} leaders, {} crashes ({} power cuts), {} partitions, {} snapshots installed, {} writes ok / {} unknown, {} reads, {} messages dropped",
        reports.len(),
        reports.iter().map(|r| r.events).sum::<u64>(),
        sum(|r| r.leaders_elected),
        sum(|r| r.crashes),
        sum(|r| r.power_cuts),
        sum(|r| r.partitions),
        sum(|r| r.snapshots_installed),
        sum(|r| r.writes_ok),
        sum(|r| r.writes_unknown),
        sum(|r| r.reads_ok),
        reports.iter().map(|r| r.dropped).sum::<u64>(),
    );
    reports
}

#[test]
fn three_nodes_under_every_fault() {
    let reports = run_all("3n", &SimConfig::default(), 30);
    assert!(reports.iter().map(|r| r.writes_ok).sum::<usize>() > 0);
    assert!(reports.iter().map(|r| r.crashes).sum::<usize>() > 0);
}

#[test]
fn five_nodes_under_every_fault() {
    let cfg = SimConfig { nodes: 5, clients: 4, ..SimConfig::default() };
    run_all("5n", &cfg, 12);
}

#[test]
fn same_seed_same_run() {
    let root = std::env::temp_dir().join(format!("storine_raft_sim_replay_{}", std::process::id()));
    let cfg = SimConfig::default();
    let a = run(&root, 7, &cfg).unwrap();
    let b = run(&root, 7, &cfg).unwrap();
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
}
