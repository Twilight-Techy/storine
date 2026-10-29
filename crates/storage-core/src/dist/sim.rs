//! Deterministic simulation of multi-range clusters (`F-SIM-02`, `F-GEO-06`, `TASK-137`,
//! `TASK-142`).
//!
//! Several [`NodeDriver`]s run on one seeded event queue. Each node has its own engine on its
//! own simulated disk ([`SimVfs`]) and hosts many range groups. The network delivers one batch
//! per node pair with per-region latency, loss, duplication and reordering.
//!
//! Faults:
//! - partitions (a node, a random split, or a whole region);
//! - node crashes with power cuts, then restart from disk;
//! - range splits while clients run.
//!
//! Clients route each key to its range, write through the range's leaseholder and read with
//! read-index.
//!
//! Checked:
//! - per range, after every event: election safety, log matching, state-machine safety, and
//!   leader completeness for new leaders;
//! - at the end: convergence to identical contents, and a linearizable history per key across
//!   splits.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use storage_codec::PropertyValue;
use storage_types::testing::SweepRng;
use storage_types::vfs::{CutMode, MountGuard, SimVfs};
use storage_types::{InlineId, StorageError};

use crate::dist::node::{Batch, NodeConfig, NodeDriver, RangeId, ReadToken};
use crate::document::{document_id, DocumentRecord};
use crate::engine::{EngineConfig, StorageEngine, WriteOp};
use crate::linearizability::{check_keyed, CheckError, Event, RegisterOp};
use crate::raft::KeySpan;
use crate::sharding::NodeId;

/// Knobs of one run (times in simulated microseconds).
#[derive(Clone, Debug)]
pub struct DistSimConfig {
    pub nodes: usize,
    /// Region of each node (`regions[i]` for node `i + 1`); latency depends on the pair.
    pub regions: Vec<u8>,
    /// One-way latency within a region and across regions.
    pub local_latency: (u64, u64),
    pub remote_latency: (u64, u64),
    pub drop_ppm: u64,
    pub dup_ppm: u64,
    pub initial_ranges: usize,
    pub keys: usize,
    pub clients: usize,
    pub ops_per_client: usize,
    pub fault_every: u64,
    pub crashes: bool,
    pub partitions: bool,
    pub region_outages: bool,
    pub splits: bool,
    /// Replicas per range (`<= nodes`; placed round-robin).
    pub replication: usize,
    /// Move replicas between nodes and transfer leases while clients run.
    pub moves: bool,
    /// Merge adjacent ranges while clients run (lease co-location, freeze, merge).
    pub merges: bool,
    pub tick_us: u64,
    pub client_timeout: u64,
    pub max_events: u64,
    pub node: NodeConfig,
}

impl Default for DistSimConfig {
    fn default() -> Self {
        Self {
            nodes: 3,
            regions: vec![0, 0, 0],
            local_latency: (200, 2_000),
            remote_latency: (20_000, 60_000),
            drop_ppm: 20_000,
            dup_ppm: 10_000,
            initial_ranges: 4,
            keys: 12,
            clients: 3,
            ops_per_client: 30,
            fault_every: 400_000,
            crashes: true,
            partitions: true,
            region_outages: false,
            splits: true,
            replication: 3,
            moves: false,
            merges: false,
            tick_us: 10_000,
            client_timeout: 1_500_000,
            max_events: 2_000_000,
            node: NodeConfig { election_min_us: 300_000, election_max_us: 600_000, heartbeat_us: 50_000, snapshot_chunk: 1024, ..NodeConfig::default() },
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct DistSimReport {
    pub seed: u64,
    pub events: u64,
    pub end_time: u64,
    pub ranges: usize,
    pub splits: usize,
    pub crashes: usize,
    pub partitions: usize,
    pub region_outages: usize,
    pub writes_ok: usize,
    pub writes_unknown: usize,
    pub reads_ok: usize,
    pub refused: usize,
    pub batches: u64,
    pub messages: u64,
    pub moves: usize,
    pub transfers: usize,
    pub merges: usize,
    /// Allocator changes and the splits / merges the drivers applied (summed over nodes).
    pub allocator_actions: u64,
    pub applied_splits: u64,
    pub applied_merges: u64,
    /// Replicas per node at the end.
    pub replicas_per_node: Vec<usize>,
}

#[derive(Clone, Debug)]
pub struct DistSimFailure {
    pub seed: u64,
    pub time: u64,
    pub what: String,
    pub trace: Vec<String>,
}

impl core::fmt::Display for DistSimFailure {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        writeln!(f, "seed {} failed at t={}us: {}", self.seed, self.time, self.what)?;
        for t in &self.trace {
            writeln!(f, "  {t}")?;
        }
        Ok(())
    }
}

#[derive(Debug)]
enum Ev {
    Deliver { to: usize, batch: Batch },
    Tick { node: usize },
    Client { client: usize },
    Fault,
    Restart { node: usize },
    Heal { epoch: u64 },
    /// Replica move of `range` to `node`: 1 = promote once caught up, 2 = remove an old voter.
    Move { range: RangeId, node: NodeId, phase: u8, tries: u8 },
    /// Merge of `right` into `left`: 0 = co-locate the leases, 1 = freeze, 2 = merge.
    Merge { left: RangeId, right: RangeId, phase: u8, tries: u8 },
}

struct SimNode {
    dir: PathBuf,
    sim: SimVfs,
    _mount: MountGuard,
    driver: Option<NodeDriver>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Write { node: usize, range: RangeId, index: u64, value: i64 },
    Read { node: usize, token: ReadToken },
}

struct Client {
    busy: Option<(u64, usize, usize, Op)>, // (since, history index, key, op)
    issued: usize,
    done: bool,
}

struct Sim<'c> {
    cfg: &'c DistSimConfig,
    seed: u64,
    rng: SweepRng,
    now: u64,
    seq: u64,
    queue: BinaryHeap<Reverse<(u64, u64)>>,
    events: BTreeMap<u64, Ev>,
    nodes: Vec<SimNode>,
    clients: Vec<Client>,
    cut: Vec<Vec<bool>>,
    epoch: u64,
    faults_on: bool,
    history: Vec<(usize, Event<RegisterOp>)>,
    next_value: i64,
    next_range: RangeId,
    leaders: BTreeMap<(RangeId, u64), NodeId>,
    /// `(range, index) -> (entry term, upper bound of the term it was committed in)`: the
    /// observer's term when the commit was first seen.
    committed: BTreeMap<(RangeId, u64), (u64, u64)>,
    seen_applied: BTreeMap<(usize, RangeId), u64>,
    report: DistSimReport,
    trace: VecDeque<String>,
    processed: u64,
}

fn key_name(k: usize) -> String {
    format!("k{k:03}")
}

fn key_id(k: usize) -> InlineId {
    document_id("kv", &key_name(k)).unwrap_or_default()
}

fn engine_config() -> EngineConfig {
    EngineConfig { background_flush: false, collection_name: "node".into(), ..EngineConfig::default() }
}

/// Runs one simulation in `root` (wiped first).
pub fn run(root: &Path, seed: u64, cfg: &DistSimConfig) -> Result<DistSimReport, DistSimFailure> {
    let _ = std::fs::remove_dir_all(root);
    let mut sim = Sim::new(root, seed, cfg).map_err(|e| DistSimFailure { seed, time: 0, what: format!("setup: {e}"), trace: Vec::new() })?;
    let res = sim.main();
    for n in &mut sim.nodes {
        n.driver = None;
    }
    drop(sim);
    let _ = std::fs::remove_dir_all(root);
    res
}

impl<'c> Sim<'c> {
    fn new(root: &Path, seed: u64, cfg: &'c DistSimConfig) -> Result<Self, StorageError> {
        let mut rng = SweepRng::new(seed.wrapping_mul(0xA24B_AED4_963E_E407) | 1);
        let repl = cfg.replication.clamp(1, cfg.nodes);
        // Initial ranges: split points spread over the key space.
        let mut bounds: Vec<Vec<u8>> = (1..cfg.initial_ranges).map(|i| key_id(i * cfg.keys / cfg.initial_ranges).as_bytes().to_vec()).collect();
        bounds.sort();
        bounds.dedup();
        let mut spans = Vec::new();
        let mut start = Vec::new();
        for b in &bounds {
            spans.push(KeySpan::new(start.clone(), Some(b.clone())));
            start = b.clone();
        }
        spans.push(KeySpan::new(start, None));
        let mut nodes = Vec::new();
        for i in 0..cfg.nodes {
            let dir = root.join(format!("n{}", i + 1));
            let sim = SimVfs::new(&dir, rng.next_u64()).map_err(|e| StorageError::io("simulated disk", e))?;
            let mount = sim.mount();
            let engine = Arc::new(StorageEngine::<DocumentRecord>::open(&dir, engine_config())?);
            let mut d = NodeDriver::new((i + 1) as NodeId, engine, NodeConfig { seed: seed ^ (i as u64 + 1), trace: std::env::var_os("STORINE_SIM_TRACE").is_some(), ..cfg.node.clone() });
            for (r, span) in spans.iter().enumerate() {
                let members: Vec<NodeId> = (0..repl).map(|j| ((r + j) % cfg.nodes + 1) as NodeId).collect();
                if members.contains(&((i + 1) as NodeId)) {
                    d.open_range(r as RangeId + 1, span.clone(), &members)?;
                }
            }
            nodes.push(SimNode { dir, sim, _mount: mount, driver: Some(d) });
        }
        let n = cfg.nodes;
        Ok(Self {
            cfg,
            seed,
            rng,
            now: 0,
            seq: 0,
            queue: BinaryHeap::new(),
            events: BTreeMap::new(),
            nodes,
            clients: (0..cfg.clients).map(|_| Client { busy: None, issued: 0, done: false }).collect(),
            cut: vec![vec![false; n]; n],
            epoch: 0,
            faults_on: true,
            history: Vec::new(),
            next_value: 0,
            next_range: spans.len() as RangeId + 1,
            leaders: BTreeMap::new(),
            committed: BTreeMap::new(),
            seen_applied: BTreeMap::new(),
            report: DistSimReport { seed, ..DistSimReport::default() },
            trace: VecDeque::new(),
            processed: 0,
        })
    }

    fn fail(&self, what: impl Into<String>) -> DistSimFailure {
        let mut trace: Vec<String> = self.trace.iter().cloned().collect();
        for (i, n) in self.nodes.iter().enumerate() {
            match &n.driver {
                None => trace.push(format!("n{}: down", i + 1)),
                Some(d) => {
                    for r in d.ranges() {
                        let v = d.replica(r).map(|x| x.log_view());
                        if let Some(v) = v {
                            trace.push(format!(
                                "n{} r{r}: {:?} term {} commit {} applied {} last {} {}",
                                i + 1,
                                v.role,
                                v.current_term,
                                v.commit_index,
                                v.last_applied,
                                v.last_index(),
                                d.describe(r)
                            ));
                        }
                    }
                }
            }
        }
        for r in self.all_ranges() {
            let hosts: Vec<NodeId> = self.nodes.iter().filter_map(|n| n.driver.as_ref()).filter(|d| d.replica(r).is_some()).map(|d| d.id()).collect();
            let configs: Vec<String> = self.nodes.iter().filter_map(|n| n.driver.as_ref()).filter(|d| d.replica(r).is_some()).map(|d| format!("n{}:{:?}", d.id(), d.members_of(r))).collect();
            trace.push(format!("r{r} hosted on {hosts:?}; configs {configs:?}"));
        }
        DistSimFailure { seed: self.seed, time: self.now, what: what.into(), trace }
    }

    fn log(&mut self, line: String) {
        if self.trace.len() >= 40 {
            self.trace.pop_front();
        }
        self.trace.push_back(format!("t={} {line}", self.now));
    }

    fn schedule(&mut self, delay: u64, ev: Ev) {
        self.seq += 1;
        self.queue.push(Reverse((self.now + delay, self.seq)));
        self.events.insert(self.seq, ev);
    }

    fn latency(&mut self, a: usize, b: usize) -> u64 {
        let same = self.cfg.regions.get(a) == self.cfg.regions.get(b);
        let (lo, hi) = if same { self.cfg.local_latency } else { self.cfg.remote_latency };
        lo + self.rng.below((hi - lo).max(1) as usize) as u64
    }

    fn flush(&mut self, node: usize) {
        let Some(d) = self.nodes[node].driver.as_mut() else { return };
        let out = d.drain_outbox();
        for (to, batch) in out {
            let to = usize::from(to) - 1;
            self.report.batches += 1;
            self.report.messages += batch.msgs.len() as u64;
            if self.cut[node][to] || (self.rng.below(1_000_000) as u64) < self.cfg.drop_ppm {
                continue;
            }
            let d = self.latency(node, to);
            if (self.rng.below(1_000_000) as u64) < self.cfg.dup_ppm {
                let d2 = d + self.latency(node, to);
                self.schedule(d2, Ev::Deliver { to, batch: batch.clone() });
            }
            self.schedule(d, Ev::Deliver { to, batch });
        }
    }

    fn main(&mut self) -> Result<DistSimReport, DistSimFailure> {
        for i in 0..self.nodes.len() {
            let d = self.rng.below(self.cfg.tick_us as usize) as u64;
            self.schedule(d, Ev::Tick { node: i });
        }
        for c in 0..self.clients.len() {
            let d = 800_000 + self.rng.below(200_000) as u64;
            self.schedule(d, Ev::Client { client: c });
        }
        self.schedule(self.cfg.fault_every, Ev::Fault);
        let mut wind_down: Option<u64> = None;
        while let Some(Reverse((t, seq))) = self.queue.pop() {
            let Some(ev) = self.events.remove(&seq) else { continue };
            self.now = t;
            self.processed += 1;
            if self.processed > self.cfg.max_events {
                return Err(self.fail("event budget exhausted"));
            }
            self.handle(ev)?;
            self.check()?;
            if wind_down.is_none() && self.clients.iter().all(|c| c.done) {
                wind_down = Some(self.now);
                self.faults_on = false;
                self.cut.iter_mut().for_each(|r| r.iter_mut().for_each(|c| *c = false));
                for i in 0..self.nodes.len() {
                    if self.nodes[i].driver.is_none() {
                        self.schedule(1_000, Ev::Restart { node: i });
                    }
                }
            }
            if let Some(since) = wind_down {
                if self.now - since > 2_000_000 && self.converged() {
                    for d in self.nodes.iter().filter_map(|n| n.driver.as_ref()) {
                        let st = d.stats();
                        self.report.allocator_actions += st.allocator_actions;
                        self.report.applied_splits += st.splits;
                        self.report.applied_merges += st.merges;
                        self.report.replicas_per_node.push(d.ranges().len());
                    }
                    break;
                }
                if self.now - since > 60_000_000 {
                    return Err(self.fail("no convergence 60 s after the faults stopped"));
                }
            }
        }
        self.final_checks()?;
        self.report.events = self.processed;
        self.report.end_time = self.now;
        self.report.ranges = self.nodes.iter().filter_map(|n| n.driver.as_ref()).map(|d| d.ranges().len()).max().unwrap_or(0);
        Ok(self.report.clone())
    }

    fn handle(&mut self, ev: Ev) -> Result<(), DistSimFailure> {
        match ev {
            Ev::Deliver { to, batch } => {
                let now = self.now;
                if let Some(d) = self.nodes[to].driver.as_mut() {
                    d.step(batch, now).map_err(|e| e.to_string()).map_err(|e| self.fail(format!("n{} step: {e}", to + 1)))?;
                }
                self.flush(to);
                self.complete_ops(to)?;
            }
            Ev::Tick { node } => {
                let now = self.now;
                if let Some(d) = self.nodes[node].driver.as_mut() {
                    d.tick(now).map_err(|e| e.to_string()).map_err(|e| self.fail(format!("n{} tick: {e}", node + 1)))?;
                    self.flush(node);
                    self.complete_ops(node)?;
                    let t = self.cfg.tick_us;
                    self.schedule(t, Ev::Tick { node });
                }
            }
            Ev::Client { client } => self.client_step(client)?,
            Ev::Fault => {
                if self.faults_on {
                    self.fault()?;
                    let d = self.cfg.fault_every / 2 + self.rng.below(self.cfg.fault_every as usize) as u64;
                    self.schedule(d, Ev::Fault);
                }
            }
            Ev::Restart { node } => self.restart(node)?,
            Ev::Move { range, node, phase, tries } => self.move_step(range, node, phase, tries),
            Ev::Merge { left, right, phase, tries } => self.merge_step(left, right, phase, tries),
            Ev::Heal { epoch } => {
                if epoch == self.epoch {
                    self.cut.iter_mut().for_each(|r| r.iter_mut().for_each(|c| *c = false));
                    self.log("healed".into());
                }
            }
        }
        Ok(())
    }

    /// The range holding key `k` per node `n`'s view, and its leader per that view.
    fn route(&self, k: usize) -> Option<(RangeId, Option<NodeId>)> {
        let id = key_id(k);
        let mut best: Option<(RangeId, Option<NodeId>, u64)> = None;
        for n in self.nodes.iter().filter_map(|n| n.driver.as_ref()) {
            for r in n.ranges() {
                let Some(rep) = n.replica(r) else { continue };
                if !rep.is_frozen() && rep.span().contains_id(id) {
                    let term = rep.current_term();
                    if best.as_ref().is_none_or(|b| term > b.2) {
                        best = Some((r, n.leader_of(r), term));
                    }
                }
            }
        }
        best.map(|(r, l, _)| (r, l))
    }

    fn client_step(&mut self, client: usize) -> Result<(), DistSimFailure> {
        if self.clients[client].done {
            return Ok(());
        }
        if let Some((since, h, _, _)) = self.clients[client].busy {
            if self.now - since >= self.cfg.client_timeout {
                if matches!(self.history[h].1.op, RegisterOp::Write(_)) {
                    self.report.writes_unknown += 1;
                }
                self.clients[client].busy = None;
            } else {
                let d = self.cfg.client_timeout - (self.now - since);
                self.schedule(d, Ev::Client { client });
                return Ok(());
            }
        }
        if self.clients[client].issued >= self.cfg.ops_per_client {
            self.clients[client].done = true;
            return Ok(());
        }
        let k = self.rng.below(self.cfg.keys);
        let refuse = |s: &mut Self| {
            s.report.refused += 1;
            let d = 20_000 + s.rng.below(50_000) as u64;
            s.schedule(d, Ev::Client { client });
        };
        let Some((range, Some(leader))) = self.route(k) else {
            refuse(self);
            return Ok(());
        };
        let node = usize::from(leader) - 1;
        let now = self.now;
        let Some(d) = self.nodes[node].driver.as_mut() else {
            refuse(self);
            return Ok(());
        };
        let call = now;
        if self.rng.below(2) == 0 {
            self.next_value += 1;
            let value = self.next_value;
            let rec = DocumentRecord::new("kv", key_name(k)).with("v", PropertyValue::Int64(value));
            match d.propose(range, &[WriteOp::put(key_id(k), rec)], now * 1_000) {
                Ok(index) => {
                    let h = self.history.len();
                    self.history.push((k, Event { call, ret: None, op: RegisterOp::Write(value) }));
                    self.clients[client].busy = Some((call, h, k, Op::Write { node, range, index, value }));
                    self.clients[client].issued += 1;
                }
                Err(_) => {
                    refuse(self);
                    return Ok(());
                }
            }
        } else {
            match d.read_index(range) {
                Ok(token) => {
                    let h = self.history.len();
                    self.history.push((k, Event { call, ret: None, op: RegisterOp::Read(None) }));
                    self.clients[client].busy = Some((call, h, k, Op::Read { node, token }));
                    self.clients[client].issued += 1;
                }
                Err(_) => {
                    refuse(self);
                    return Ok(());
                }
            }
        }
        self.flush(node);
        let t = self.cfg.client_timeout;
        self.schedule(t, Ev::Client { client });
        Ok(())
    }

    fn complete_ops(&mut self, node: usize) -> Result<(), DistSimFailure> {
        let ready: Vec<ReadToken> = self.nodes[node].driver.as_mut().map_or_else(Vec::new, |d| d.take_ready_reads());
        for c in 0..self.clients.len() {
            let Some((_, h, k, op)) = self.clients[c].busy else { continue };
            match op {
                Op::Write { node: n, range, index, value } if n == node => {
                    let Some(d) = self.nodes[node].driver.as_ref() else { continue };
                    if d.applied(range) < index {
                        continue;
                    }
                    let Some(rep) = d.replica(range) else { continue };
                    match rep.wait_applied(index, std::time::Duration::ZERO) {
                        Ok(_) => {
                            self.history[h].1.ret = Some(self.now);
                            self.history[h].1.op = RegisterOp::Write(value);
                            self.report.writes_ok += 1;
                        }
                        Err(_) => self.report.writes_unknown += 1,
                    }
                    self.clients[c].busy = None;
                    let d = 1_000 + self.rng.below(40_000) as u64;
                    self.schedule(d, Ev::Client { client: c });
                }
                Op::Read { node: n, token } if n == node && ready.contains(&token) => {
                    let Some(d) = self.nodes[node].driver.as_ref() else { continue };
                    // Serve only if the key is still in the range (it may have split away).
                    let in_span = d.replica(token.range).is_some_and(|r| r.span().contains_id(key_id(k)));
                    if in_span {
                        let v = match d.engine().get_owned(key_id(k)) {
                            Ok((_, rec)) => match rec.fields.get("v") {
                                Some(PropertyValue::Int64(v)) => Some(*v),
                                _ => None,
                            },
                            Err(StorageError::RecordNotFound { .. }) => None,
                            Err(e) => return Err(self.fail(format!("read: {e}"))),
                        };
                        self.history[h].1.ret = Some(self.now);
                        self.history[h].1.op = RegisterOp::Read(v);
                        self.report.reads_ok += 1;
                    }
                    self.clients[c].busy = None;
                    let d = 1_000 + self.rng.below(40_000) as u64;
                    self.schedule(d, Ev::Client { client: c });
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn check(&mut self) -> Result<(), DistSimFailure> {
        let mut new_leaders = Vec::new();
        for (ni, n) in self.nodes.iter().enumerate() {
            let Some(d) = &n.driver else { continue };
            for r in d.ranges() {
                let Some(rep) = d.replica(r) else { continue };
                let v = rep.log_view();
                if rep.is_leader() {
                    match self.leaders.get(&(r, v.current_term)) {
                        Some(&l) if l != d.id() => return Err(self.fail(format!("election safety: r{r} term {} led by n{l} and n{}", v.current_term, d.id()))),
                        None => new_leaders.push((ni, r, v.current_term)),
                        _ => {}
                    }
                }
                if v.last_applied > v.commit_index {
                    return Err(self.fail(format!("n{} r{r} applied beyond commit", ni + 1)));
                }
                let lo = self.seen_applied.get(&(ni, r)).copied().unwrap_or(0).max(v.snapshot_index);
                for i in lo + 1..=v.commit_index {
                    let Some(t) = v.term_at(i) else { continue };
                    match self.committed.get(&(r, i)) {
                        Some(&(ct, _)) if ct != t => return Err(self.fail(format!("state-machine safety: r{r} index {i} committed as term {ct} and {t}"))),
                        None => {
                            self.committed.insert((r, i), (t, v.current_term));
                        }
                        _ => {}
                    }
                }
                self.seen_applied.insert((ni, r), v.last_applied.max(lo));
            }
        }
        for (ni, r, term) in new_leaders {
            let Some(d) = &self.nodes[ni].driver else { continue };
            let Some(rep) = d.replica(r) else { continue };
            let v = rep.log_view();
            // Raft promises a leader every entry committed in an earlier term only.
            for (&(cr, i), &(t, by)) in self.committed.range((r, 0)..=(r, u64::MAX)) {
                debug_assert_eq!(cr, r);
                if term > by && i > v.snapshot_index && v.term_at(i) != Some(t) {
                    return Err(self.fail(format!("leader completeness: n{} leads r{r} term {term} without committed {i}", ni + 1)));
                }
            }
            self.leaders.insert((r, term), d.id());
        }
        if self.processed % 32 == 0 {
            self.check_log_matching()?;
        }
        Ok(())
    }

    fn check_log_matching(&self) -> Result<(), DistSimFailure> {
        let mut views: BTreeMap<RangeId, Vec<(usize, crate::raft::RaftLogView)>> = BTreeMap::new();
        for (ni, n) in self.nodes.iter().enumerate() {
            let Some(d) = &n.driver else { continue };
            for r in d.ranges() {
                if let Some(rep) = d.replica(r) {
                    views.entry(r).or_default().push((ni, rep.log_view()));
                }
            }
        }
        for (r, vs) in views {
            for a in 0..vs.len() {
                for b in a + 1..vs.len() {
                    let (va, vb) = (&vs[a].1, &vs[b].1);
                    let lo = va.snapshot_index.max(vb.snapshot_index) + 1;
                    let hi = va.last_index().min(vb.last_index());
                    let Some(top) = (lo..=hi).rev().find(|&i| va.term_at(i) == vb.term_at(i)) else { continue };
                    if let Some(i) = (lo..=top).find(|&i| va.term_at(i) != vb.term_at(i)) {
                        return Err(self.fail(format!("log matching: r{r} n{} n{} differ at {i} below agreement at {top}", vs[a].0 + 1, vs[b].0 + 1)));
                    }
                }
            }
        }
        Ok(())
    }

    fn fault(&mut self) -> Result<(), DistSimFailure> {
        let n = self.nodes.len();
        if self.cfg.merges && self.rng.below(3) == 0 {
            // Merge a random range with its right-hand neighbour.
            let ranges = self.all_ranges();
            let Some(&left) = ranges.get(self.rng.below(ranges.len().max(1))) else { return Ok(()) };
            let Some(l) = self.leader_index(left) else { return Ok(()) };
            let Some(end) = self.nodes[l].driver.as_ref().and_then(|d| d.replica(left)).and_then(|x| x.span().end) else { return Ok(()) };
            let right = ranges.iter().copied().find(|&r| {
                self.nodes.iter().filter_map(|x| x.driver.as_ref()).any(|d| d.replica(r).is_some_and(|x| !x.is_frozen() && x.span().start == end))
            });
            if let Some(right) = right {
                self.log(format!("merge r{right} into r{left}: start"));
                self.schedule(1_000, Ev::Merge { left, right, phase: 0, tries: 0 });
            }
            return Ok(());
        }
        match self.rng.below(10) {
            0..=2 if self.cfg.partitions => {
                self.epoch += 1;
                let mut side = vec![false; n];
                if self.cfg.region_outages && self.rng.below(2) == 0 {
                    let region = self.cfg.regions[self.rng.below(n)];
                    for (i, s) in side.iter_mut().enumerate() {
                        *s = self.cfg.regions[i] == region;
                    }
                    self.report.region_outages += 1;
                } else {
                    side[self.rng.below(n)] = true;
                }
                for a in 0..n {
                    for b in 0..n {
                        self.cut[a][b] = side[a] != side[b];
                    }
                }
                self.report.partitions += 1;
                self.log(format!("partition {side:?}"));
                let epoch = self.epoch;
                let d = 500_000 + self.rng.below(2_000_000) as u64;
                self.schedule(d, Ev::Heal { epoch });
            }
            3..=4 if self.cfg.crashes => {
                let node = self.rng.below(n);
                if self.nodes[node].driver.is_some() && self.nodes.iter().filter(|x| x.driver.is_some()).count() == n {
                    self.crash(node)?;
                }
            }
            8 if self.cfg.moves => {
                // Move a replica of a random range to a node that lacks one.
                let ranges = self.all_ranges();
                if ranges.is_empty() {
                    return Ok(());
                }
                let range = ranges[self.rng.below(ranges.len())];
                if let Some(l) = self.leader_index(range) {
                    let members: Vec<NodeId> = self.nodes[l].driver.as_ref().map(|d| d.members_of(range).into_iter().map(|(id, _)| id).collect()).unwrap_or_default();
                    let candidates: Vec<NodeId> = (1..=n as NodeId).filter(|id| !members.contains(id) && self.nodes[usize::from(*id) - 1].driver.is_some()).collect();
                    if !candidates.is_empty() && members.len() == self.cfg.replication {
                        let target = candidates[self.rng.below(candidates.len())];
                        if let Some(d) = self.nodes[l].driver.as_mut() {
                            if d.add_learner(range, target).is_ok() {
                                self.log(format!("move r{range}: add learner n{target}"));
                                self.flush(l);
                                self.schedule(300_000, Ev::Move { range, node: target, phase: 1, tries: 0 });
                            }
                        }
                    }
                }
            }
            9 if self.cfg.moves => {
                // Transfer a random range's lease to a caught-up voter.
                let ranges = self.all_ranges();
                if ranges.is_empty() {
                    return Ok(());
                }
                let range = ranges[self.rng.below(ranges.len())];
                if let Some(l) = self.leader_index(range) {
                    let voters: Vec<NodeId> = self.nodes[l].driver.as_ref().map(|d| d.members_of(range).into_iter().filter(|&(id, v)| v && usize::from(id) - 1 != l).map(|(id, _)| id).collect()).unwrap_or_default();
                    if !voters.is_empty() {
                        let to = voters[self.rng.below(voters.len())];
                        if self.nodes[l].driver.as_mut().is_some_and(|d| d.transfer_lease(range, to).is_ok()) {
                            self.report.transfers += 1;
                            self.log(format!("transfer r{range} lease to n{to}"));
                            self.flush(l);
                        }
                    }
                }
            }
            5..=7 if self.cfg.splits => {
                // Split a random range at a key strictly inside it.
                let k = 1 + self.rng.below(self.cfg.keys.saturating_sub(1).max(1));
                let at = key_id(k).as_bytes().to_vec();
                if let Some((range, Some(leader))) = self.route(k) {
                    let node = usize::from(leader) - 1;
                    let right = self.next_range;
                    if let Some(d) = self.nodes[node].driver.as_mut() {
                        if d.propose_split(range, at, right).is_ok() {
                            self.next_range += 1;
                            self.report.splits += 1;
                            self.log(format!("split r{range} at k{k} -> r{right}"));
                            self.flush(node);
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn merge_step(&mut self, left: RangeId, right: RangeId, phase: u8, tries: u8) {
        // Once frozen, the merge is seen through (a frozen range serves nothing).
        if phase < 2 && (!self.faults_on || tries > 30) {
            return;
        }
        let tries = tries.min(100);
        let retry = |s: &mut Self, phase: u8| s.schedule(200_000, Ev::Merge { left, right, phase, tries: tries + 1 });
        let (Some(l), Some(r)) = (self.leader_index(left), self.leader_index(right)) else { return retry(self, phase) };
        match phase {
            0 => {
                if l == r {
                    return self.schedule(1_000, Ev::Merge { left, right, phase: 1, tries: 0 });
                }
                let to = (l + 1) as NodeId;
                if self.nodes[r].driver.as_mut().is_some_and(|d| d.transfer_lease(right, to).is_ok()) {
                    self.log(format!("merge r{right} into r{left}: lease to n{to}"));
                    self.flush(r);
                }
                retry(self, 0);
            }
            1 => {
                if l != r {
                    return retry(self, 0);
                }
                let d = self.nodes[l].driver.as_mut().expect("leader has a driver");
                let mut lm = d.members_of(left);
                let mut rm = d.members_of(right);
                lm.sort_unstable();
                rm.sort_unstable();
                if lm.iter().any(|&(_, v)| !v) {
                    return retry(self, 1); // a move is still running on the left
                }
                if lm != rm {
                    // Align the right range's replicas with the left's, one change at a time:
                    // promote a caught-up learner, add a missing replica, remove an extra one.
                    let want: Vec<NodeId> = lm.iter().map(|&(id, _)| id).collect();
                    let me = d.id();
                    let changed = if let Some(&(x, _)) = rm.iter().find(|&&(id, v)| !v && want.contains(&id)) {
                        d.caught_up(right, x) && d.promote(right, x).is_ok()
                    } else if let Some(&x) = want.iter().find(|&&id| !rm.iter().any(|&(r, _)| r == id)) {
                        rm.iter().all(|&(_, v)| v) && d.add_learner(right, x).is_ok()
                    } else if let Some(&(y, _)) = rm.iter().find(|&&(id, _)| !want.contains(&id) && id != me) {
                        d.remove_member(right, y).is_ok()
                    } else {
                        false
                    };
                    if changed {
                        self.log(format!("merge r{right} into r{left}: align replicas"));
                        self.flush(l);
                    }
                    return retry(self, 1);
                }
                if d.freeze(right).is_ok() {
                    self.log(format!("merge r{right} into r{left}: freeze"));
                    self.flush(l);
                    self.schedule(100_000, Ev::Merge { left, right, phase: 2, tries: 0 });
                } else {
                    retry(self, 1);
                }
            }
            _ => {
                let d = self.nodes[l].driver.as_mut().expect("leader has a driver");
                if d.propose_merge(left, right).is_ok() {
                    self.report.merges += 1;
                    self.log(format!("merge r{right} into r{left}: merge"));
                    self.flush(l);
                } else if l != r {
                    // The right range's lease moved off (a frozen range still elects
                    // leaders): bring it back.
                    let to = (l + 1) as NodeId;
                    if self.nodes[r].driver.as_mut().is_some_and(|d| d.transfer_lease(right, to).is_ok()) {
                        self.flush(r);
                    }
                    retry(self, 2);
                } else {
                    retry(self, 2);
                }
            }
        }
    }

    fn all_ranges(&self) -> Vec<RangeId> {
        let mut rs: Vec<RangeId> = self.nodes.iter().filter_map(|n| n.driver.as_ref()).flat_map(|d| d.ranges()).collect();
        rs.sort_unstable();
        rs.dedup();
        rs
    }

    fn leader_index(&self, range: RangeId) -> Option<usize> {
        self.nodes.iter().position(|n| n.driver.as_ref().is_some_and(|d| d.is_leader(range)))
    }

    fn move_step(&mut self, range: RangeId, node: NodeId, phase: u8, tries: u8) {
        if !self.faults_on || tries > 20 {
            return;
        }
        let retry = |s: &mut Self| s.schedule(200_000, Ev::Move { range, node, phase, tries: tries + 1 });
        let Some(l) = self.leader_index(range) else { return retry(self) };
        let d = self.nodes[l].driver.as_mut().expect("leader has a driver");
        match phase {
            1 => {
                let is_learner = d.members_of(range).iter().any(|&(id, v)| id == node && !v);
                if !is_learner {
                    return; // the move was lost with a leader change
                }
                if d.caught_up(range, node) && d.promote(range, node).is_ok() {
                    self.log(format!("move r{range}: promote n{node}"));
                    self.flush(l);
                    self.schedule(300_000, Ev::Move { range, node, phase: 2, tries: 0 });
                } else {
                    retry(self);
                }
            }
            _ => {
                let leader_id = d.id();
                let voters: Vec<NodeId> = d.members_of(range).into_iter().filter(|&(id, v)| v && id != node && id != leader_id).map(|(id, _)| id).collect();
                if voters.is_empty() || d.members_of(range).len() <= self.cfg.replication {
                    return;
                }
                let victim = voters[self.rng.below(voters.len())];
                let d = self.nodes[l].driver.as_mut().expect("leader has a driver");
                if d.remove_member(range, victim).is_ok() {
                    self.report.moves += 1;
                    self.log(format!("move r{range}: remove n{victim}"));
                    self.flush(l);
                } else {
                    retry(self);
                }
            }
        }
    }

    fn crash(&mut self, node: usize) -> Result<(), DistSimFailure> {
        self.log(format!("n{} crashes", node + 1));
        self.report.crashes += 1;
        self.nodes[node].sim.freeze();
        self.nodes[node].driver = None;
        for c in 0..self.clients.len() {
            if let Some((_, h, _, op)) = self.clients[c].busy {
                let on_node = match op {
                    Op::Write { node: n, .. } | Op::Read { node: n, .. } => n == node,
                };
                if on_node {
                    if matches!(self.history[h].1.op, RegisterOp::Write(_)) {
                        self.report.writes_unknown += 1;
                    }
                    self.clients[c].busy = None;
                    self.schedule(1_000, Ev::Client { client: c });
                }
            }
        }
        let sim = &self.nodes[node].sim;
        sim.set_mode(if self.rng.below(2) == 0 { CutMode::Random } else { CutMode::KeepAll });
        sim.power_cut().map_err(|e| e.to_string()).map_err(|e| self.fail(format!("power cut: {e}")))?;
        for r in self.seen_applied.iter_mut().filter(|((n, _), _)| *n == node) {
            *r.1 = 0;
        }
        let d = 200_000 + self.rng.below(1_000_000) as u64;
        self.schedule(d, Ev::Restart { node });
        Ok(())
    }

    fn restart(&mut self, node: usize) -> Result<(), DistSimFailure> {
        if self.nodes[node].driver.is_some() {
            return Ok(());
        }
        let dir = self.nodes[node].dir.clone();
        let engine = StorageEngine::<DocumentRecord>::open(&dir, engine_config()).map_err(|e| self.fail(format!("n{} engine recovery: {e}", node + 1)))?;
        let mut d = NodeDriver::new((node + 1) as NodeId, Arc::new(engine), NodeConfig { seed: self.seed ^ (node as u64 + 1) ^ self.now, incarnation: self.now, trace: std::env::var_os("STORINE_SIM_TRACE").is_some(), ..self.cfg.node.clone() });
        d.reopen_ranges().map_err(|e| self.fail(format!("n{} range recovery: {e}", node + 1)))?;
        self.nodes[node].driver = Some(d);
        self.log(format!("n{} restarted", node + 1));
        self.schedule(self.cfg.tick_us, Ev::Tick { node });
        Ok(())
    }

    fn converged(&self) -> bool {
        let drivers: Vec<&NodeDriver> = self.nodes.iter().filter_map(|n| n.driver.as_ref()).collect();
        if drivers.len() != self.nodes.len() {
            return false;
        }
        // Allocators settled: nothing changed for 2 s (they do not oscillate).
        if self.cfg.node.allocator.is_some() && drivers.iter().any(|d| self.now.saturating_sub(d.last_allocator_action()) < 2_000_000) {
            return false;
        }
        for r in self.all_ranges() {
            let hosts: Vec<&&NodeDriver> = drivers.iter().filter(|d| d.replica(r).is_some()).collect();
            let leaders = hosts.iter().filter(|d| d.is_leader(r)).count();
            let Some(leader) = hosts.iter().find(|d| d.is_leader(r)) else { return false };
            // Every member of the leader's configuration hosts it, and no one else does.
            let members: Vec<NodeId> = leader.members_of(r).into_iter().map(|(id, _)| id).collect();
            if hosts.len() != members.len() || !hosts.iter().all(|d| members.contains(&d.id())) {
                return false;
            }
            let views: Vec<_> = hosts.iter().filter_map(|d| d.replica(r).map(|x| x.log_view())).collect();
            let c = views[0].commit_index;
            if leaders != 1 || !views.iter().all(|v| v.commit_index == c && v.last_applied == c && v.last_index() == c) {
                return false;
            }
        }
        true
    }

    fn final_checks(&mut self) -> Result<(), DistSimFailure> {
        self.check_log_matching()?;
        // Every key: the same value on every node hosting its range.
        for k in 0..self.cfg.keys {
            let id = key_id(k);
            let mut seen: Option<(NodeId, Option<i64>)> = None;
            for n in &self.nodes {
                let Some(d) = &n.driver else { continue };
                let hosts = d.ranges().into_iter().any(|r| d.replica(r).is_some_and(|x| !x.is_frozen() && x.span().contains_id(id)));
                if !hosts {
                    continue;
                }
                let v = d.engine().get_owned(id).ok().and_then(|(_, r)| match r.fields.get("v") {
                    Some(PropertyValue::Int64(v)) => Some(*v),
                    _ => None,
                });
                match seen {
                    Some((other, ov)) if ov != v => return Err(self.fail(format!("replicas diverged on k{k}: n{other} {ov:?} vs n{} {v:?}", d.id()))),
                    None => seen = Some((d.id(), v)),
                    _ => {}
                }
            }
        }
        match check_keyed(&self.history, 5_000_000) {
            Ok(()) => Ok(()),
            Err((k, CheckError::Inconclusive)) => Err(self.fail(format!("linearizability inconclusive for key {k}"))),
            Err((k, e)) => {
                let ops: Vec<String> = self.history.iter().filter(|(kk, _)| *kk == k).map(|(_, e)| format!("[{}..{:?}] {:?}", e.call, e.ret, e.op)).collect();
                Err(self.fail(format!("key {k} not linearizable ({e:?}): {ops:?}")))
            }
        }
    }
}
