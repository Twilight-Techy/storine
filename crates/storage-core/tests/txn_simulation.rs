//! Distributed transactions under the deterministic simulator (`TASK-140`, `TASK-142`).
//!
//! `STORINE_SIM_SEEDS=n` widens the sweep; `STORINE_SIM_SEED=s` replays one seed.

use storage_core::dist::alloc::AllocatorConfig;
use storage_core::dist::node::NodeConfig;
use storage_core::dist::txn_sim::{run, TxnSimConfig, TxnSimReport};
use storage_core::dist::zone::{self, ClosedTs, ZoneConfig, ZoneConfigs};

fn seeds(default: u64) -> Vec<u64> {
    if let Some(s) = std::env::var("STORINE_SIM_SEED").ok().and_then(|s| s.parse().ok()) {
        return vec![s];
    }
    let n = std::env::var("STORINE_SIM_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(default);
    (1..=n).collect()
}

fn sweep(name: &str, cfg: &TxnSimConfig, default: u64) -> Vec<TxnSimReport> {
    let root = std::env::temp_dir().join(format!("storine_txn_sim_{name}_{}", std::process::id()));
    let mut out = Vec::new();
    for seed in seeds(default) {
        match run(&root, seed, cfg) {
            Ok(r) => out.push(r),
            Err(e) => panic!("{name} seed {seed}: {e}\nreplay: STORINE_SIM_SEED={seed}"),
        }
    }
    let s = |f: fn(&TxnSimReport) -> usize| out.iter().map(f).sum::<usize>();
    eprintln!("{name}: offset refusals {}", s(|r| r.offset_refusals));
    eprintln!(
        "{name}: {} seeds: {} transfers committed, {} audits, {} retried, {} unknown then settled, {} unavailable, {} crashes, {} partitions",
        out.len(),
        s(|r| r.committed),
        s(|r| r.audits),
        s(|r| r.retried),
        s(|r| r.unknown_then_settled),
        s(|r| r.unavailable),
        s(|r| r.crashes),
        s(|r| r.partitions)
    );
    out
}

#[test]
fn single_client_commits() {
    let cfg = TxnSimConfig { crashes: false, partitions: false, drop_ppm: 0, clients: 1, txns_per_client: 3, audit_every: 100, ..TxnSimConfig::default() };
    let reports = sweep("single", &cfg, 1);
    eprintln!("first error: {:?}", reports.iter().find_map(|r| r.first_error.clone()));
    assert_eq!(reports[0].committed, 3);
}

#[test]
fn bank_transfers_without_faults() {
    let cfg = TxnSimConfig { crashes: false, partitions: false, drop_ppm: 0, ..TxnSimConfig::default() };
    let reports = sweep("calm", &cfg, 2);
    eprintln!("first error: {:?}", reports.iter().find_map(|r| r.first_error.clone()));
    assert!(reports.iter().map(|r| r.committed).sum::<usize>() > 0);
}

#[test]
fn bank_transfers_are_serializable_under_faults() {
    let reports = sweep("bank", &TxnSimConfig::default(), 8);
    assert!(reports.iter().map(|r| r.committed).sum::<usize>() > 0);
}

#[test]
fn bank_transfers_across_regions() {
    let cfg = TxnSimConfig { nodes: 5, regions: vec![0, 0, 1, 1, 2], request_timeout: 3_000_000, ..TxnSimConfig::default() };
    sweep("regions", &cfg, 4);
}

/// Clients route through range caches filled by meta-range lookups while the allocator
/// splits, merges and moves ranges under them (`TASK-138`).
#[test]
fn bank_transfers_with_meta_routing_and_allocator() {
    let alloc = AllocatorConfig { replication: 3, split_keys: 6, split_writes_per_sec: 30, merge_keys: 5, interval_us: 400_000, dead_after_us: 3_000_000, rebalance_slack: 2 };
    let base = TxnSimConfig::default();
    let node = NodeConfig { cluster: vec![1, 2, 3, 4, 5], allocator: Some(alloc), ..base.node.clone() };
    let cfg = TxnSimConfig { nodes: 5, regions: vec![0; 5], ranges: 2, accounts: 12, meta_routing: true, node, ..base };
    let reports = sweep("meta", &cfg, 6);
    let lookups: usize = reports.iter().map(|r| r.meta_lookups).sum();
    let misroutes: usize = reports.iter().map(|r| r.misroutes).sum();
    eprintln!("meta routing: {lookups} lookups, {misroutes} misroutes");
    assert!(lookups > 0 && reports.iter().map(|r| r.committed).sum::<usize>() > 0);
}

/// Closed timestamps (`TASK-141`): audits read `AS OF` the recent past from random replicas,
/// which serve them below their closed timestamps (or redirect), under crashes and partitions.
/// Every audit must still see the conserved total and fit the serial order at its timestamp.
#[test]
fn follower_reads_at_closed_timestamps() {
    let base = TxnSimConfig::default();
    let zones = ZoneConfigs::new(ZoneConfig { closed_ts: ClosedTs::Lag(200_000_000), ..ZoneConfig::default() });
    let node = storage_core::dist::node::NodeConfig { zones, max_offset_ns: base.max_offset_ns, ..base.node.clone() };
    let cfg = TxnSimConfig { nearest_reads: true, audit_staleness_ns: 400_000_000, audit_every: 2, node, ..base };
    let reports = sweep("follower", &cfg, 8);
    let served: u64 = reports.iter().map(|r| r.follower_reads).sum();
    eprintln!("follower: {served} reads served by replicas below their closed timestamps");
    assert!(served > 0, "no follower read was served");
}

/// Global tables (`TASK-141`): the accounts close ahead of time, so any replica serves
/// present-time reads; writers wait out the lead. Transfers and audits stay serializable and
/// linearizable across three regions with outages.
#[test]
fn global_table_reads_from_any_replica() {
    let base = TxnSimConfig { nodes: 5, regions: vec![0, 0, 1, 1, 2], request_timeout: 3_000_000, ..TxnSimConfig::default() };
    let mut zones = ZoneConfigs::default();
    // Lead: twice the offset bound plus heartbeat and cross-region propagation.
    zone::global(&mut zones, "acct", 2 * base.max_offset_ns + 250_000_000, &ZoneConfig::default()).expect("zone");
    let node = storage_core::dist::node::NodeConfig { zones, max_offset_ns: base.max_offset_ns, ..base.node.clone() };
    let cfg = TxnSimConfig { nearest_reads: true, audit_every: 2, node, ..base };
    let reports = sweep("global", &cfg, 4);
    let served: u64 = reports.iter().map(|r| r.follower_reads).sum();
    eprintln!("global: {served} present-time reads served by non-leaseholders");
    assert!(served > 0, "no follower read was served");
}

/// Span scans (`TASK-143`): audits read every account with one scan across ranges, under
/// crashes and partitions, and must see the conserved total and fit the serial order.
#[test]
fn scan_audits_are_serializable() {
    let cfg = TxnSimConfig { scan_audits: true, audit_every: 2, ..TxnSimConfig::default() };
    let reports = sweep("scan", &cfg, 8);
    assert!(reports.iter().map(|r| r.audits).sum::<usize>() > 0);
}

/// Seeds that once failed, replayed on every run (`RULE-TEST-03`).
/// - Coordinators resolved intents after a failed explicit commit while the record still read
///   "staging"; a later recovery saw an intent missing and aborted a committed transfer
///   (regions 106, meta 33, follower 232, scan 58, global 127).
/// - Range servers conditioned replicated record writes on node-local engine versions, so a
///   write applied on one replica and failed on another: replicas diverged (meta 146).
#[test]
fn regression_seeds() {
    let root = std::env::temp_dir().join(format!("storine_txn_regress_{}", std::process::id()));
    let base = TxnSimConfig::default();
    let regions = TxnSimConfig { nodes: 5, regions: vec![0, 0, 1, 1, 2], request_timeout: 3_000_000, ..TxnSimConfig::default() };
    let alloc = AllocatorConfig { replication: 3, split_keys: 6, split_writes_per_sec: 30, merge_keys: 5, interval_us: 400_000, dead_after_us: 3_000_000, rebalance_slack: 2 };
    let meta_node = NodeConfig { cluster: vec![1, 2, 3, 4, 5], allocator: Some(alloc), ..base.node.clone() };
    let meta = TxnSimConfig { nodes: 5, regions: vec![0; 5], ranges: 2, accounts: 12, meta_routing: true, node: meta_node, ..base.clone() };
    let lag = ZoneConfigs::new(ZoneConfig { closed_ts: ClosedTs::Lag(200_000_000), ..ZoneConfig::default() });
    let follower = TxnSimConfig { nearest_reads: true, audit_staleness_ns: 400_000_000, audit_every: 2, node: NodeConfig { zones: lag, max_offset_ns: base.max_offset_ns, ..base.node.clone() }, ..base.clone() };
    let scan = TxnSimConfig { scan_audits: true, audit_every: 2, ..base.clone() };
    let mut zones = ZoneConfigs::default();
    zone::global(&mut zones, "acct", 2 * base.max_offset_ns + 250_000_000, &ZoneConfig::default()).expect("zone");
    let global = TxnSimConfig { nearest_reads: true, audit_every: 2, node: NodeConfig { zones, max_offset_ns: base.max_offset_ns, ..regions.node.clone() }, ..regions.clone() };
    for (name, cfg, seed) in [("regions", &regions, 106), ("meta", &meta, 33), ("meta", &meta, 146), ("follower", &follower, 232), ("scan", &scan, 58), ("global", &global, 127)] {
        if let Err(e) = run(&root, seed, cfg) {
            panic!("{name} seed {seed}: {e}");
        }
    }
}
