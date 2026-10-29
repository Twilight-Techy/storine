//! Deterministic cluster simulation (`F-SIM-02`, `TASK-133`, `RULE-TEST-03`).
//!
//! Real [`RaftNode`]s, each with its own engine on its own simulated disk ([`SimVfs`]), are
//! driven by one seeded event queue. Nothing runs on a thread or reads a real clock, so a
//! seed fixes the whole run: a failure names the seed, and the same seed replays it.
//!
//! Faults:
//! - **Network**: delay, loss, duplication and reordering of every message; partitions that
//!   isolate a node or split the cluster, then heal.
//! - **Crashes**: a node stops at an arbitrary point, its disk loses power (unsynced writes
//!   dropped, torn or reordered) or just the process dies, and it restarts from its disk.
//! - **Clocks**: per-node rate skew and forward jumps of the election timer.
//! - **Compaction**: random log compaction, so lagging nodes catch up through chunked
//!   snapshot installs.
//!
//! The driver plays the part of the networked runtime: elections with pre-vote, pipelined
//! replication, commit by quorum, read-index reads. It checks after every event:
//! - **election safety**: at most one leader per term;
//! - **log matching**: same index and term means the same prefix;
//! - **leader completeness**: every committed entry is in every later leader's log;
//! - **state-machine safety**: no two nodes apply different entries at one index, and no
//!   index commits twice with different terms.
//!
//! At the end the faults stop and the cluster must converge to identical engine contents, and
//! the clients' history (writes and linearizable reads of a key-value register) must be
//! linearizable ([`crate::linearizability`]).

use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::cmp::Reverse;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use storage_types::testing::SweepRng;
use storage_types::vfs::{CutMode, MountGuard, SimVfs};
use storage_types::{InlineId, StorageError};

use crate::engine::{EngineConfig, StorageEngine, WriteOp};
use crate::linearizability::{check_keyed, CheckError, Event, RegisterOp};
use crate::model::AgentObjectRecord;
use crate::raft::{
    AppendEntriesReq, AppendEntriesResp, InstallSnapshotReq, InstallSnapshotResp, RaftNode, RequestVoteReq, RequestVoteResp,
};
use crate::sharding::NodeId;

type Node = RaftNode<AgentObjectRecord>;

/// Knobs of one simulated run. Times are in simulated microseconds.
#[derive(Clone, Debug)]
pub struct SimConfig {
    pub nodes: usize,
    pub clients: usize,
    /// Operations each client issues before the run winds down.
    pub ops_per_client: usize,
    pub keys: usize,
    pub min_delay: u64,
    pub max_delay: u64,
    /// Per-message probabilities, in parts per million.
    pub drop_ppm: u64,
    pub dup_ppm: u64,
    /// Messages held back for up to `late_max` (a request arriving long after newer ones).
    pub late_ppm: u64,
    pub late_max: u64,
    /// Mean time between fault events (partition, crash, clock, compaction).
    pub fault_every: u64,
    pub crashes: bool,
    pub partitions: bool,
    pub election_min: u64,
    pub election_max: u64,
    pub heartbeat: u64,
    /// A client gives up on an operation (outcome unknown) after this long.
    pub client_timeout: u64,
    /// Snapshot chunk size (small, so installs take several chunks).
    pub snapshot_chunk: usize,
    /// Hard limit on processed events (guards against livelock).
    pub max_events: u64,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            nodes: 3,
            clients: 3,
            ops_per_client: 40,
            keys: 4,
            min_delay: 100,
            max_delay: 5_000,
            drop_ppm: 30_000,
            dup_ppm: 20_000,
            late_ppm: 10_000,
            late_max: 800_000,
            fault_every: 60_000,
            crashes: true,
            partitions: true,
            election_min: 150_000,
            election_max: 300_000,
            heartbeat: 40_000,
            client_timeout: 400_000,
            snapshot_chunk: 512,
            max_events: 400_000,
        }
    }
}

/// What a run did.
#[derive(Clone, Debug, Default)]
pub struct SimReport {
    pub seed: u64,
    pub events: u64,
    pub end_time: u64,
    pub max_term: u64,
    pub leaders_elected: usize,
    pub messages: u64,
    pub dropped: u64,
    pub duplicated: u64,
    pub crashes: usize,
    pub power_cuts: usize,
    pub partitions: usize,
    pub clock_jumps: usize,
    pub compactions: usize,
    pub snapshots_installed: usize,
    pub writes_ok: usize,
    pub writes_unknown: usize,
    pub reads_ok: usize,
    pub refused: usize,
    pub committed: u64,
}

/// A violated invariant (or a run that never converged), with the trace leading up to it.
#[derive(Clone, Debug)]
pub struct SimFailure {
    pub seed: u64,
    pub time: u64,
    pub event: u64,
    pub what: String,
    pub trace: Vec<String>,
}

impl core::fmt::Display for SimFailure {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        writeln!(f, "seed {} failed at t={}us (event {}): {}", self.seed, self.time, self.event, self.what)?;
        writeln!(f, "last events:")?;
        for t in &self.trace {
            writeln!(f, "  {t}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
enum Msg {
    Vote(RequestVoteReq),
    VoteResp { req: RequestVoteReq, resp: RequestVoteResp },
    Append { req: AppendEntriesReq, hb: u64 },
    AppendResp { resp: AppendEntriesResp, req_term: u64, last_sent: u64, hb: u64 },
    Snap(InstallSnapshotReq),
    SnapResp { resp: InstallSnapshotResp, req_term: u64, last_included: u64 },
}

#[derive(Debug)]
enum Ev {
    Deliver { from: NodeId, to: NodeId, msg: Msg },
    Election { node: usize, gen: u64 },
    Heartbeat { node: usize, gen: u64 },
    Client { client: usize },
    Fault,
    Restart { node: usize },
    Heal { epoch: u64 },
}

#[derive(Debug, Default, Clone, Copy)]
struct Progress {
    next: u64,
    matched: u64,
    snapshot_offset: Option<u64>,
}

#[derive(Debug)]
struct PendingWrite {
    client: usize,
    key: usize,
    value: i64,
    index: u64,
    call: u64,
}

#[derive(Debug)]
struct PendingRead {
    client: usize,
    key: usize,
    read_index: u64,
    hb_needed: u64,
    call: u64,
}

#[derive(Debug)]
enum Campaign {
    Pre { term: u64, votes: BTreeSet<NodeId> },
    Real { term: u64, votes: BTreeSet<NodeId> },
}

struct SimNode {
    id: NodeId,
    dir: PathBuf,
    sim: SimVfs,
    _mount: MountGuard,
    raft: Option<Arc<Node>>,
    timer_gen: u64,
    hb_gen: u64,
    /// Local clock rate in parts per million of real (simulated) time.
    rate_ppm: u64,
    campaign: Option<Campaign>,
    lead_term: u64,
    progress: BTreeMap<NodeId, Progress>,
    hb_seq: u64,
    acks: BTreeMap<NodeId, u64>,
    writes: Vec<PendingWrite>,
    reads: Vec<PendingRead>,
    seen_applied: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClientState {
    Idle,
    Busy { since: u64, op: usize },
    Done,
}

struct Client {
    state: ClientState,
    issued: usize,
    leader_hint: usize,
}

struct Sim<'c> {
    cfg: &'c SimConfig,
    seed: u64,
    rng: SweepRng,
    now: u64,
    seq: u64,
    queue: BinaryHeap<Reverse<(u64, u64)>>,
    events: BTreeMap<u64, Ev>,
    nodes: Vec<SimNode>,
    clients: Vec<Client>,
    /// `cut[a][b]`: messages from `a` to `b` are dropped.
    cut: Vec<Vec<bool>>,
    partition_epoch: u64,
    faults_on: bool,
    history: Vec<(usize, Event<RegisterOp>)>,
    /// Index into `history` of each client's open operation.
    open_op: Vec<Option<usize>>,
    next_value: i64,
    leaders: BTreeMap<u64, NodeId>,
    committed: BTreeMap<u64, u64>,
    report: SimReport,
    trace: VecDeque<String>,
    processed: u64,
}

fn key_id(key: usize) -> InlineId {
    InlineId::new(&format!("k{key}")).unwrap_or_default()
}

fn engine_config() -> EngineConfig {
    EngineConfig { background_flush: false, ..EngineConfig::default() }
}

fn open_node(dir: &Path, id: NodeId) -> Result<Arc<Node>, StorageError> {
    let engine = Arc::new(StorageEngine::<AgentObjectRecord>::open(dir, engine_config())?);
    Ok(Arc::new(RaftNode::new(id, engine)?))
}

/// Runs one simulation in `root` (wiped first). `Ok` holds what happened; `Err` the first
/// violated invariant.
pub fn run(root: &Path, seed: u64, cfg: &SimConfig) -> Result<SimReport, SimFailure> {
    let _ = std::fs::remove_dir_all(root);
    let mut sim = Sim::new(root, seed, cfg).map_err(|e| SimFailure { seed, time: 0, event: 0, what: format!("setup: {e}"), trace: Vec::new() })?;
    let res = sim.main();
    drop(sim.nodes.drain(..));
    let _ = std::fs::remove_dir_all(root);
    res
}

impl<'c> Sim<'c> {
    fn new(root: &Path, seed: u64, cfg: &'c SimConfig) -> Result<Self, StorageError> {
        let mut rng = SweepRng::new(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let mut nodes = Vec::new();
        for i in 0..cfg.nodes {
            let dir = root.join(format!("n{i}"));
            let sim = SimVfs::new(&dir, rng.next_u64()).map_err(|e| StorageError::io("create simulated disk", e))?;
            let mount = sim.mount();
            let id = (i + 1) as NodeId;
            let raft = open_node(&dir, id)?;
            nodes.push(SimNode {
                id,
                dir,
                sim,
                _mount: mount,
                raft: Some(raft),
                timer_gen: 0,
                hb_gen: 0,
                rate_ppm: 950_000 + rng.below(100_001) as u64,
                campaign: None,
                lead_term: 0,
                progress: BTreeMap::new(),
                hb_seq: 0,
                acks: BTreeMap::new(),
                writes: Vec::new(),
                reads: Vec::new(),
                seen_applied: 0,
            });
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
            clients: (0..cfg.clients).map(|c| Client { state: ClientState::Idle, issued: 0, leader_hint: c % n }).collect(),
            cut: vec![vec![false; n]; n],
            partition_epoch: 0,
            faults_on: true,
            history: Vec::new(),
            open_op: vec![None; cfg.clients],
            next_value: 0,
            leaders: BTreeMap::new(),
            committed: BTreeMap::new(),
            report: SimReport { seed, ..SimReport::default() },
            trace: VecDeque::new(),
            processed: 0,
        })
    }

    fn fail(&self, what: impl Into<String>) -> SimFailure {
        let mut trace: Vec<String> = self.trace.iter().cloned().collect();
        for n in &self.nodes {
            trace.push(match &n.raft {
                None => format!("state n{}: down", n.id),
                Some(r) => {
                    let v = r.log_view();
                    format!(
                        "state n{}: {:?} term {} voted {:?} leader {:?} commit {} applied {} snapshot {} last {} (last term {:?}) campaign {:?}",
                        n.id,
                        v.role,
                        v.current_term,
                        r.voted_for(),
                        r.leader_id(),
                        v.commit_index,
                        v.last_applied,
                        v.snapshot_index,
                        v.last_index(),
                        v.term_at(v.last_index()),
                        n.campaign
                    )
                }
            });
        }
        SimFailure { seed: self.seed, time: self.now, event: self.processed, what: what.into(), trace }
    }

    fn log(&mut self, line: impl FnOnce() -> String) {
        if self.trace.len() >= 60 {
            self.trace.pop_front();
        }
        let t = self.now;
        self.trace.push_back(format!("t={t} {}", line()));
    }

    fn schedule(&mut self, delay: u64, ev: Ev) {
        self.seq += 1;
        self.queue.push(Reverse((self.now + delay, self.seq)));
        self.events.insert(self.seq, ev);
    }

    fn quorum(&self) -> usize {
        self.cfg.nodes / 2 + 1
    }

    fn idx(&self, id: NodeId) -> usize {
        usize::from(id) - 1
    }

    /// A node-local duration in simulated time (its clock runs at `rate_ppm`).
    fn local(&self, node: usize, d: u64) -> u64 {
        d.saturating_mul(1_000_000) / self.nodes[node].rate_ppm.max(1)
    }

    fn election_timeout(&mut self, node: usize) -> u64 {
        let span = self.cfg.election_max.saturating_sub(self.cfg.election_min).max(1);
        let d = self.cfg.election_min + self.rng.below(span as usize) as u64;
        self.local(node, d)
    }

    fn reset_election(&mut self, node: usize) {
        self.nodes[node].timer_gen += 1;
        let gen = self.nodes[node].timer_gen;
        let d = self.election_timeout(node);
        self.schedule(d, Ev::Election { node, gen });
    }

    fn send(&mut self, from: NodeId, to: NodeId, msg: Msg) {
        self.report.messages += 1;
        let (a, b) = (self.idx(from), self.idx(to));
        if self.cut[a][b] {
            self.report.dropped += 1;
            return;
        }
        if (self.rng.below(1_000_000) as u64) < self.cfg.drop_ppm {
            self.report.dropped += 1;
            return;
        }
        let span = self.cfg.max_delay.saturating_sub(self.cfg.min_delay).max(1);
        let mut d = self.cfg.min_delay + self.rng.below(span as usize) as u64;
        if (self.rng.below(1_000_000) as u64) < self.cfg.late_ppm {
            d += self.rng.below(self.cfg.late_max.max(1) as usize) as u64;
        }
        if (self.rng.below(1_000_000) as u64) < self.cfg.dup_ppm {
            self.report.duplicated += 1;
            let d2 = self.cfg.min_delay + self.rng.below(span as usize) as u64 * 2;
            self.schedule(d2, Ev::Deliver { from, to, msg: msg.clone() });
        }
        self.schedule(d, Ev::Deliver { from, to, msg });
    }

    fn main(&mut self) -> Result<SimReport, SimFailure> {
        for i in 0..self.nodes.len() {
            self.reset_election(i);
        }
        for c in 0..self.clients.len() {
            let d = 200_000 + self.rng.below(100_000) as u64;
            self.schedule(d, Ev::Client { client: c });
        }
        if self.cfg.crashes || self.cfg.partitions {
            let d = self.cfg.fault_every;
            self.schedule(d, Ev::Fault);
        }
        let mut winding_down_since: Option<u64> = None;
        while let Some(Reverse((t, seq))) = self.queue.pop() {
            let Some(ev) = self.events.remove(&seq) else { continue };
            self.now = t;
            self.processed += 1;
            if self.processed > self.cfg.max_events {
                return Err(self.fail("event budget exhausted (livelock?)"));
            }
            self.handle(ev)?;
            self.after_event()?;
            if self.processed % 16 == 0 {
                self.check_log_matching()?;
            }
            // Wind down: all clients finished -> stop faults, heal, wait for convergence.
            if winding_down_since.is_none() && self.clients.iter().all(|c| c.state == ClientState::Done) {
                winding_down_since = Some(self.now);
                self.faults_on = false;
                for row in &mut self.cut {
                    row.iter_mut().for_each(|c| *c = false);
                }
                for i in 0..self.nodes.len() {
                    if self.nodes[i].raft.is_none() {
                        self.schedule(1_000, Ev::Restart { node: i });
                    }
                }
            }
            if let Some(since) = winding_down_since {
                if self.converged() {
                    break;
                }
                if self.now - since > 30_000_000 {
                    return Err(self.fail("cluster did not converge within 30 s after the faults stopped"));
                }
            }
        }
        self.final_checks()?;
        self.report.events = self.processed;
        self.report.end_time = self.now;
        self.report.leaders_elected = self.leaders.len();
        self.report.max_term = self.leaders.keys().next_back().copied().unwrap_or(0);
        self.report.committed = self.committed.keys().next_back().copied().unwrap_or(0);
        Ok(self.report.clone())
    }

    fn handle(&mut self, ev: Ev) -> Result<(), SimFailure> {
        match ev {
            Ev::Deliver { from, to, msg } => self.deliver(from, to, msg),
            Ev::Election { node, gen } => {
                if self.nodes[node].timer_gen == gen && self.nodes[node].raft.is_some() {
                    self.campaign(node)?;
                }
                Ok(())
            }
            Ev::Heartbeat { node, gen } => {
                if self.nodes[node].hb_gen == gen && self.is_leader(node) {
                    self.broadcast(node);
                    let d = self.local(node, self.cfg.heartbeat);
                    self.schedule(d, Ev::Heartbeat { node, gen });
                }
                Ok(())
            }
            Ev::Client { client } => self.client_step(client),
            Ev::Fault => {
                if self.faults_on {
                    self.fault()?;
                    let d = self.cfg.fault_every / 2 + self.rng.below(self.cfg.fault_every as usize) as u64;
                    self.schedule(d, Ev::Fault);
                }
                Ok(())
            }
            Ev::Restart { node } => self.restart(node),
            Ev::Heal { epoch } => {
                if epoch == self.partition_epoch {
                    for row in &mut self.cut {
                        row.iter_mut().for_each(|c| *c = false);
                    }
                    self.log(|| "partition healed".into());
                }
                Ok(())
            }
        }
    }

    fn is_leader(&self, node: usize) -> bool {
        self.nodes[node].raft.as_ref().is_some_and(|r| r.is_leader())
    }

    // ── elections ──

    fn campaign(&mut self, node: usize) -> Result<(), SimFailure> {
        let Some(raft) = self.nodes[node].raft.clone() else { return Ok(()) };
        self.reset_election(node);
        if raft.is_leader() {
            return Ok(());
        }
        let me = self.nodes[node].id;
        let req = raft.pre_vote_request();
        self.nodes[node].campaign = Some(Campaign::Pre { term: req.term, votes: BTreeSet::from([me]) });
        self.log(|| format!("n{me} pre-vote for term {}", req.term));
        for p in self.peers(node) {
            self.send(me, p, Msg::Vote(req));
        }
        Ok(())
    }

    fn peers(&self, node: usize) -> Vec<NodeId> {
        let me = self.nodes[node].id;
        self.nodes.iter().map(|n| n.id).filter(|&id| id != me).collect()
    }

    fn on_vote_resp(&mut self, node: usize, from: NodeId, req: RequestVoteReq, resp: RequestVoteResp) -> Result<(), SimFailure> {
        let Some(raft) = self.nodes[node].raft.clone() else { return Ok(()) };
        if !req.pre_vote && resp.term > raft.current_term() {
            raft.observe_term(resp.term).map_err(|e| self.fail(format!("observe_term: {e}")))?;
            self.nodes[node].campaign = None;
            return Ok(());
        }
        let me = self.nodes[node].id;
        let quorum = self.quorum();
        match &mut self.nodes[node].campaign {
            Some(Campaign::Pre { term, votes }) if req.pre_vote && req.term == *term => {
                if resp.vote_granted {
                    votes.insert(from);
                }
                if votes.len() >= quorum {
                    // Won the pre-vote: run the real election.
                    let real = raft.start_election().map_err(|e| self.fail(format!("start_election: {e}")))?;
                    self.nodes[node].campaign = Some(Campaign::Real { term: real.term, votes: BTreeSet::from([me]) });
                    self.log(|| format!("n{me} campaigns in term {}", real.term));
                    self.reset_election(node);
                    for p in self.peers(node) {
                        self.send(me, p, Msg::Vote(real));
                    }
                }
            }
            Some(Campaign::Real { term, votes }) if !req.pre_vote && req.term == *term && resp.term == *term => {
                if resp.vote_granted {
                    votes.insert(from);
                }
                if votes.len() >= quorum {
                    let term = *term;
                    self.nodes[node].campaign = None;
                    if raft.become_leader(term) {
                        self.on_new_leader(node, term)?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn on_new_leader(&mut self, node: usize, term: u64) -> Result<(), SimFailure> {
        let Some(raft) = self.nodes[node].raft.clone() else { return Ok(()) };
        let me = self.nodes[node].id;
        if let Some(&other) = self.leaders.get(&term) {
            if other != me {
                return Err(self.fail(format!("election safety: n{other} and n{me} both lead term {term}")));
            }
        }
        self.leaders.insert(term, me);
        // Leader completeness: every committed entry is in the new leader's log.
        let view = raft.log_view();
        for (&i, &t) in &self.committed {
            if i <= view.snapshot_index {
                continue;
            }
            if view.term_at(i) != Some(t) {
                return Err(self.fail(format!(
                    "leader completeness: n{me} leads term {term} without committed entry {i} (term {t}); has {:?}",
                    view.term_at(i)
                )));
            }
        }
        self.log(|| format!("n{me} leads term {term} (last index {})", view.last_index()));
        let next = view.last_index() + 1;
        let progress = self.nodes.iter().filter(|x| x.id != me).map(|x| (x.id, Progress { next, matched: 0, snapshot_offset: None })).collect();
        let n = &mut self.nodes[node];
        n.lead_term = term;
        n.progress = progress;
        n.acks.clear();
        n.hb_gen += 1;
        let gen = n.hb_gen;
        self.broadcast(node);
        let d = self.local(node, self.cfg.heartbeat);
        self.schedule(d, Ev::Heartbeat { node, gen });
        Ok(())
    }

    // ── replication ──

    fn broadcast(&mut self, node: usize) {
        for p in self.peers(node) {
            self.replicate(node, p);
        }
    }

    fn replicate(&mut self, node: usize, peer: NodeId) {
        let Some(raft) = self.nodes[node].raft.clone() else { return };
        if !raft.is_leader() || raft.current_term() != self.nodes[node].lead_term {
            return;
        }
        let me = self.nodes[node].id;
        let pr = self.nodes[node].progress.get(&peer).copied().unwrap_or_default();
        if raft.needs_snapshot(pr.next) {
            // (Re)send the chunk in flight: lost chunks are retried on every heartbeat, and
            // the follower answers duplicates with the offset it expects.
            let offset = pr.snapshot_offset.unwrap_or(0);
            if let Ok(req) = raft.build_install_snapshot(offset, self.cfg.snapshot_chunk) {
                if let Some(p) = self.nodes[node].progress.get_mut(&peer) {
                    p.snapshot_offset = Some(offset);
                }
                self.send(me, peer, Msg::Snap(req));
            }
            return;
        }
        // Short, varied batches: a request then often covers only part of a follower's log.
        let batch = 1 + self.rng.below(8);
        let Ok(req) = raft.build_append_entries_bounded(pr.next, batch, 64 * 1024) else { return };
        let hb = self.nodes[node].hb_seq;
        self.send(me, peer, Msg::Append { req, hb });
    }

    fn advance_commit(&mut self, node: usize) -> Result<(), SimFailure> {
        let Some(raft) = self.nodes[node].raft.clone() else { return Ok(()) };
        if !raft.is_leader() {
            return Ok(());
        }
        let mut matches: Vec<u64> = self.nodes[node].progress.values().map(|p| p.matched).collect();
        matches.push(raft.last_log_index());
        matches.sort_unstable_by(|a, b| b.cmp(a));
        let q = matches[self.quorum() - 1];
        match raft.advance_leader_commit(q) {
            Ok(_) | Err(StorageError::InvalidKey { .. }) => Ok(()),
            Err(e) => Err(self.fail(format!("advance_leader_commit: {e}"))),
        }
    }

    fn deliver(&mut self, from: NodeId, to: NodeId, msg: Msg) -> Result<(), SimFailure> {
        let node = self.idx(to);
        let Some(raft) = self.nodes[node].raft.clone() else { return Ok(()) };
        match msg {
            Msg::Vote(req) => {
                let resp = raft.handle_request_vote(req);
                if resp.vote_granted && !req.pre_vote {
                    self.reset_election(node);
                }
                self.send(to, from, Msg::VoteResp { req, resp });
            }
            Msg::VoteResp { req, resp } => self.on_vote_resp(node, from, req, resp)?,
            Msg::Append { req, hb } => {
                let req_term = req.term;
                let last_sent = req.prev_log_index + req.entries.len() as u64;
                let resp = raft.handle_append_entries(req).map_err(|e| self.fail(format!("n{to} rejected AppendEntries from n{from}: {e}")))?;
                if resp.term == req_term {
                    self.nodes[node].campaign = None;
                    self.reset_election(node);
                }
                self.send(to, from, Msg::AppendResp { resp, req_term, last_sent, hb });
            }
            Msg::AppendResp { resp, req_term, last_sent, hb } => {
                if resp.term > raft.current_term() {
                    raft.observe_term(resp.term).map_err(|e| self.fail(format!("observe_term: {e}")))?;
                    return Ok(());
                }
                if !raft.is_leader() || raft.current_term() != req_term || self.nodes[node].lead_term != req_term {
                    return Ok(());
                }
                let acks = self.nodes[node].acks.entry(from).or_insert(0);
                *acks = (*acks).max(hb);
                let Some(p) = self.nodes[node].progress.get_mut(&from) else { return Ok(()) };
                if resp.success {
                    p.matched = p.matched.max(resp.match_index.min(last_sent));
                    p.next = p.next.max(p.matched + 1);
                    let behind = p.matched < raft.last_log_index();
                    self.advance_commit(node)?;
                    if behind {
                        self.replicate(node, from);
                    }
                } else {
                    p.next = (resp.match_index + 1).min(p.next.saturating_sub(1)).max(p.matched + 1).max(1);
                    self.replicate(node, from);
                }
            }
            Msg::Snap(req) => {
                let req_term = req.term;
                let last_included = req.last_included_index;
                let resp = raft.handle_install_snapshot(req).map_err(|e| self.fail(format!("n{to} install snapshot: {e}")))?;
                if resp.term == req_term {
                    self.reset_election(node);
                }
                if resp.installed {
                    self.report.snapshots_installed += 1;
                    self.log(|| format!("n{to} installed snapshot through {last_included}"));
                }
                self.send(to, from, Msg::SnapResp { resp, req_term, last_included });
            }
            Msg::SnapResp { resp, req_term, last_included } => {
                if resp.term > raft.current_term() {
                    raft.observe_term(resp.term).map_err(|e| self.fail(format!("observe_term: {e}")))?;
                    return Ok(());
                }
                if !raft.is_leader() || raft.current_term() != req_term {
                    return Ok(());
                }
                let Some(p) = self.nodes[node].progress.get_mut(&from) else { return Ok(()) };
                if resp.installed {
                    p.matched = p.matched.max(last_included);
                    p.next = p.matched + 1;
                    p.snapshot_offset = None;
                    self.advance_commit(node)?;
                    self.replicate(node, from);
                } else if p.snapshot_offset.is_some() {
                    p.snapshot_offset = Some(resp.next_offset);
                    if let Ok(req) = raft.build_install_snapshot(resp.next_offset, self.cfg.snapshot_chunk) {
                        self.send(to, from, Msg::Snap(req));
                    } else if let Some(p) = self.nodes[node].progress.get_mut(&from) {
                        p.snapshot_offset = None;
                    }
                }
            }
        }
        Ok(())
    }

    // ── clients ──

    fn client_step(&mut self, client: usize) -> Result<(), SimFailure> {
        match self.clients[client].state {
            ClientState::Done => return Ok(()),
            ClientState::Busy { since, op } => {
                if self.now - since >= self.cfg.client_timeout {
                    // Give up: the outcome stays unknown (`ret: None`).
                    self.abandon(client, op);
                } else {
                    self.schedule(self.cfg.client_timeout - (self.now - since), Ev::Client { client });
                    return Ok(());
                }
            }
            ClientState::Idle => {}
        }
        if self.clients[client].issued >= self.cfg.ops_per_client {
            self.clients[client].state = ClientState::Done;
            return Ok(());
        }
        let target = self.clients[client].leader_hint;
        let key = self.rng.below(self.cfg.keys);
        let is_write = self.rng.below(2) == 0;
        let Some(raft) = self.nodes[target].raft.clone() else {
            self.refuse(client);
            return Ok(());
        };
        if !raft.is_leader() || self.nodes[target].lead_term != raft.current_term() {
            if let Some(l) = raft.leader_id() {
                self.clients[client].leader_hint = self.idx(l).min(self.nodes.len() - 1);
            }
            self.refuse(client);
            return Ok(());
        }
        self.clients[client].issued += 1;
        let call = self.now;
        if is_write {
            self.next_value += 1;
            let value = self.next_value;
            let rec = AgentObjectRecord::new(key_id(key), format!("v{value}"), value, value);
            match raft.propose_ops_at(&[WriteOp::put(key_id(key), rec)], self.now * 1_000) {
                Ok(index) => {
                    let h = self.history.len();
                    self.history.push((key, Event { call, ret: None, op: RegisterOp::Write(value) }));
                    self.open_op[client] = Some(h);
                    self.nodes[target].writes.push(PendingWrite { client, key, value, index, call });
                    self.clients[client].state = ClientState::Busy { since: call, op: h };
                    self.broadcast(target);
                }
                Err(_) => {
                    self.clients[client].issued -= 1;
                    self.refuse(client);
                    return Ok(());
                }
            }
        } else {
            // Read index: the leader's commit index, valid once a quorum confirms it still
            // leads (heartbeat round) and the leader has committed an entry of its term.
            let view = raft.log_view();
            if view.term_at(view.commit_index) != Some(view.current_term) {
                self.clients[client].issued -= 1;
                self.refuse(client);
                return Ok(());
            }
            let n = &mut self.nodes[target];
            n.hb_seq += 1;
            let hb_needed = n.hb_seq;
            let h = self.history.len();
            self.history.push((key, Event { call, ret: None, op: RegisterOp::Read(None) }));
            self.open_op[client] = Some(h);
            self.nodes[target].reads.push(PendingRead { client, key, read_index: view.commit_index, hb_needed, call });
            self.clients[client].state = ClientState::Busy { since: call, op: h };
            self.broadcast(target);
        }
        self.schedule(self.cfg.client_timeout, Ev::Client { client });
        Ok(())
    }

    /// A definite refusal (not leader, node down): nothing happened; retry soon.
    fn refuse(&mut self, client: usize) {
        self.report.refused += 1;
        let n = self.nodes.len();
        let c = &mut self.clients[client];
        if self.rng.below(3) == 0 {
            c.leader_hint = self.rng.below(n);
        }
        c.state = ClientState::Idle;
        let d = 5_000 + self.rng.below(20_000) as u64;
        self.schedule(d, Ev::Client { client });
    }

    fn abandon(&mut self, client: usize, op: usize) {
        if matches!(self.history[op].1.op, RegisterOp::Write(_)) {
            self.report.writes_unknown += 1;
        }
        self.open_op[client] = None;
        self.clients[client].state = ClientState::Idle;
        for n in &mut self.nodes {
            n.writes.retain(|w| w.client != client);
            n.reads.retain(|r| r.client != client);
        }
    }

    fn complete(&mut self, client: usize, op: RegisterOp) {
        let Some(h) = self.open_op[client].take() else { return };
        self.history[h].1.ret = Some(self.now);
        self.history[h].1.op = op;
        self.clients[client].state = ClientState::Idle;
        let d = 1_000 + self.rng.below(30_000) as u64;
        self.schedule(d, Ev::Client { client });
    }

    /// Completes client operations whose conditions now hold, and checks invariants.
    fn after_event(&mut self) -> Result<(), SimFailure> {
        for node in 0..self.nodes.len() {
            let Some(raft) = self.nodes[node].raft.clone() else { continue };
            let view = raft.log_view();
            // Election safety (also catches leaders the driver did not create).
            if raft.is_leader() {
                let me = self.nodes[node].id;
                match self.leaders.get(&view.current_term) {
                    Some(&other) if other != me => {
                        return Err(self.fail(format!("election safety: n{other} and n{me} both lead term {}", view.current_term)));
                    }
                    _ => {}
                }
            }
            // Commit and apply safety.
            if view.last_applied > view.commit_index {
                return Err(self.fail(format!("n{} applied {} beyond commit {}", node + 1, view.last_applied, view.commit_index)));
            }
            let lo = self.nodes[node].seen_applied.max(view.snapshot_index);
            for i in lo + 1..=view.commit_index {
                let Some(t) = view.term_at(i) else { continue };
                match self.committed.get(&i) {
                    Some(&ct) if ct != t => {
                        return Err(self.fail(format!(
                            "state-machine safety: n{} has term {t} committed at index {i}, another node term {ct}",
                            node + 1
                        )));
                    }
                    None => {
                        self.committed.insert(i, t);
                    }
                    _ => {}
                }
            }
            self.nodes[node].seen_applied = self.nodes[node].seen_applied.max(view.last_applied);

            // Writes: done once applied (outcome from the node, or unknown if replaced).
            let applied = view.last_applied;
            let ready: Vec<usize> = self.nodes[node].writes.iter().enumerate().filter(|(_, w)| w.index <= applied).map(|(i, _)| i).collect();
            for i in ready.into_iter().rev() {
                let w = self.nodes[node].writes.remove(i);
                let _ = (w.key, w.call);
                match raft.wait_applied(w.index, std::time::Duration::ZERO) {
                    Ok(_) => {
                        self.report.writes_ok += 1;
                        self.complete(w.client, RegisterOp::Write(w.value));
                    }
                    Err(_) => {
                        // Replaced after a leader change, or unknown: leave `ret: None`.
                        if let Some(h) = self.open_op[w.client] {
                            self.abandon(w.client, h);
                            let d = 1_000;
                            self.schedule(d, Ev::Client { client: w.client });
                        }
                    }
                }
            }
            // Reads: a quorum (counting this node) acknowledged a heartbeat sent after the
            // read started, in this term, and the read index is applied.
            if !self.nodes[node].reads.is_empty() {
                let leading = raft.is_leader() && self.nodes[node].lead_term == view.current_term;
                let quorum = self.quorum();
                let n = &self.nodes[node];
                let mut done = Vec::new();
                for (i, r) in n.reads.iter().enumerate() {
                    if !leading {
                        continue;
                    }
                    let confirmed = 1 + n.acks.values().filter(|&&a| a >= r.hb_needed).count();
                    if confirmed >= quorum && applied >= r.read_index {
                        done.push(i);
                    }
                }
                for i in done.into_iter().rev() {
                    let r = self.nodes[node].reads.remove(i);
                    let _ = r.call;
                    let value = match raft.engine().get_owned(key_id(r.key)) {
                        Ok((_, rec)) => Some(rec.score),
                        Err(StorageError::RecordNotFound { .. }) => None,
                        Err(e) => return Err(self.fail(format!("read failed: {e}"))),
                    };
                    self.report.reads_ok += 1;
                    self.complete(r.client, RegisterOp::Read(value));
                }
            }
        }
        Ok(())
    }

    fn check_log_matching(&self) -> Result<(), SimFailure> {
        let views: Vec<_> = self.nodes.iter().map(|n| n.raft.as_ref().map(|r| r.log_view())).collect();
        for a in 0..views.len() {
            for b in a + 1..views.len() {
                let (Some(va), Some(vb)) = (&views[a], &views[b]) else { continue };
                let lo = va.snapshot_index.max(vb.snapshot_index) + 1;
                let hi = va.last_index().min(vb.last_index());
                // Highest index where both agree; everything below must agree too.
                let Some(top) = (lo..=hi).rev().find(|&i| va.term_at(i) == vb.term_at(i)) else { continue };
                for i in lo..=top {
                    if va.term_at(i) != vb.term_at(i) {
                        return Err(self.fail(format!(
                            "log matching: n{} and n{} agree at index {top} but differ at {i} ({:?} vs {:?})",
                            a + 1,
                            b + 1,
                            va.term_at(i),
                            vb.term_at(i)
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    // ── faults ──

    fn fault(&mut self) -> Result<(), SimFailure> {
        let n = self.nodes.len();
        match self.rng.below(10) {
            0..=2 if self.cfg.partitions => {
                // Isolate one node, or split the cluster in two.
                self.partition_epoch += 1;
                let epoch = self.partition_epoch;
                let mut side = vec![false; n];
                if self.rng.below(2) == 0 {
                    side[self.rng.below(n)] = true;
                } else {
                    for s in side.iter_mut() {
                        *s = self.rng.below(2) == 0;
                    }
                }
                for a in 0..n {
                    for b in 0..n {
                        // Sometimes one-way (half-open links).
                        let one_way = self.rng.below(5) == 0 && side[a];
                        self.cut[a][b] = side[a] != side[b] && !one_way;
                    }
                }
                self.report.partitions += 1;
                self.log(|| format!("partition {side:?}"));
                let d = 100_000 + self.rng.below(900_000) as u64;
                self.schedule(d, Ev::Heal { epoch });
            }
            3..=4 if self.cfg.crashes => {
                let node = self.rng.below(n);
                if self.nodes[node].raft.is_some() {
                    self.crash(node)?;
                }
            }
            5 => {
                // Clock jump: this node's election timer fires now.
                let node = self.rng.below(n);
                self.report.clock_jumps += 1;
                self.nodes[node].timer_gen += 1;
                let gen = self.nodes[node].timer_gen;
                self.schedule(0, Ev::Election { node, gen });
            }
            6 => {
                // Clock skew change.
                let node = self.rng.below(n);
                self.nodes[node].rate_ppm = 900_000 + self.rng.below(200_001) as u64;
            }
            _ => {
                // Compaction on a random node: lagging peers then need a snapshot.
                let node = self.rng.below(n);
                if let Some(raft) = self.nodes[node].raft.clone() {
                    let applied = raft.last_applied();
                    if applied > 2 {
                        let to = applied - self.rng.below(3) as u64;
                        raft.compact_log_through(to).map_err(|e| self.fail(format!("compaction: {e}")))?;
                        self.report.compactions += 1;
                    }
                }
            }
        }
        Ok(())
    }

    fn crash(&mut self, node: usize) -> Result<(), SimFailure> {
        let power = self.rng.below(2) == 0;
        let id = self.nodes[node].id;
        self.log(|| format!("n{id} crashes{}", if power { " (power cut)" } else { "" }));
        self.nodes[node].sim.freeze();
        self.nodes[node].raft = None;
        self.nodes[node].campaign = None;
        self.nodes[node].progress.clear();
        self.nodes[node].lead_term = 0;
        self.nodes[node].hb_gen += 1;
        self.nodes[node].timer_gen += 1;
        // Operations waiting on this node have unknown outcomes.
        let clients: Vec<usize> = self.nodes[node].writes.iter().map(|w| w.client).chain(self.nodes[node].reads.iter().map(|r| r.client)).collect();
        self.nodes[node].writes.clear();
        self.nodes[node].reads.clear();
        for c in clients {
            if let Some(h) = self.open_op[c] {
                self.abandon(c, h);
                self.schedule(1_000, Ev::Client { client: c });
            }
        }
        let sim = &self.nodes[node].sim;
        if power {
            sim.set_mode(CutMode::Random);
            sim.power_cut().map_err(|e| self.fail(format!("power cut: {e}")))?;
            self.report.power_cuts += 1;
        } else {
            sim.crash().map_err(|e| self.fail(format!("crash: {e}")))?;
        }
        self.report.crashes += 1;
        let d = 50_000 + self.rng.below(500_000) as u64;
        self.schedule(d, Ev::Restart { node });
        Ok(())
    }

    fn restart(&mut self, node: usize) -> Result<(), SimFailure> {
        if self.nodes[node].raft.is_some() {
            return Ok(());
        }
        let id = self.nodes[node].id;
        let raft = open_node(&self.nodes[node].dir, id).map_err(|e| self.fail(format!("n{id} failed to recover: {e}")))?;
        // A restarted node's committed prefix must still match what the cluster committed.
        let view = raft.log_view();
        for i in view.snapshot_index + 1..=view.commit_index {
            if let (Some(t), Some(&ct)) = (view.term_at(i), self.committed.get(&i)) {
                if t != ct {
                    return Err(self.fail(format!("n{id} recovered term {t} at committed index {i}, cluster committed term {ct}")));
                }
            }
        }
        self.nodes[node].seen_applied = 0;
        self.nodes[node].raft = Some(raft);
        self.log(|| format!("n{id} restarted"));
        self.reset_election(node);
        Ok(())
    }

    // ── the end ──

    fn converged(&self) -> bool {
        let views: Vec<_> = self.nodes.iter().filter_map(|n| n.raft.as_ref().map(|r| r.log_view())).collect();
        if views.len() != self.nodes.len() {
            return false;
        }
        let leader = self.nodes.iter().filter(|n| n.raft.as_ref().is_some_and(|r| r.is_leader())).count();
        let c = views[0].commit_index;
        leader == 1
            && c > 0
            && views.iter().all(|v| v.commit_index == c && v.last_applied == c && v.last_index() == c)
            && views.iter().all(|v| v.term_at(c) == Some(v.current_term))
    }

    fn final_checks(&mut self) -> Result<(), SimFailure> {
        self.check_log_matching()?;
        // Identical engine contents everywhere.
        let mut states = Vec::new();
        for n in &self.nodes {
            let Some(raft) = &n.raft else { continue };
            let mut s = Vec::new();
            for k in 0..self.cfg.keys {
                s.push(raft.engine().get_owned(key_id(k)).ok().map(|(_, r)| r.score));
            }
            states.push((n.id, s));
        }
        for w in states.windows(2) {
            if w[0].1 != w[1].1 {
                return Err(self.fail(format!("replicas diverged: n{} {:?} vs n{} {:?}", w[0].0, w[0].1, w[1].0, w[1].1)));
            }
        }
        // Linearizability of what the clients saw.
        match check_keyed(&self.history, 2_000_000) {
            Ok(()) => Ok(()),
            Err((key, CheckError::Inconclusive)) => Err(self.fail(format!("linearizability check inconclusive for key {key}"))),
            Err((key, CheckError::NotLinearizable { partial })) => {
                let ops: Vec<String> = self
                    .history
                    .iter()
                    .filter(|(k, _)| *k == key)
                    .map(|(_, e)| format!("[{}..{}] {:?}", e.call, e.ret.map_or("?".to_string(), |r| r.to_string()), e.op))
                    .collect();
                Err(self.fail(format!("history of key {key} is not linearizable (longest prefix {} ops): {ops:?}", partial.len())))
            }
        }
    }
}
