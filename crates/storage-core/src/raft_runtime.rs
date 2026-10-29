//! Networked Raft runtime (`TASK-100`, `F-DIST-02`, `F-DIST-05`): election timers, heartbeats,
//! per-peer replication, quorum commit, leader lease / read index, and the write path that
//! proposes a conditional batch and waits for its replicated outcome.
//!
//! The runtime is transport-agnostic: `storage-server` provides a TCP [`RaftTransport`]
//! (`OP_RAFT_*` RPCs); tests use an in-process one with simulated partitions.
//!
//! Safety properties (Raft §5, §8):
//! - one vote per term, persisted before it is sent; logs only grow on the leader;
//! - an entry commits only once a quorum stores it **and** it is from the leader's current term
//!   (a new leader's no-op commits earlier entries);
//! - every replica applies committed entries in log order through the deterministic apply path
//!   (versions from log indexes, TTL decisions at the proposal time), and replica engines refuse
//!   direct writes, so replicas never diverge;
//! - a leader serves reads only while it holds a lease (a quorum acknowledged it within the
//!   minimum election timeout, measured on the leader's monotonic clock from the time each
//!   acknowledged request was **sent**), otherwise after a read-index round, so a deposed leader
//!   never serves stale data;
//! - a write whose outcome cannot be confirmed returns [`StorageError::CommitUnknown`]; clients
//!   retry with the same request id (applied at most once);
//! - membership follows the configuration in the log (`TASK-121`): quorums, votes and leases
//!   count voters only, learners just receive the log, a new member joins as a learner and is
//!   promoted once caught up, and a node that is not a voter never starts an election.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use storage_codec::ZeroCopyRecord;
use storage_index::Indexable;
use storage_types::{Result, StorageError};

use crate::engine::{WriteOp, WriteResult};
use crate::raft::{
    AppendEntriesReq, AppendEntriesResp, InstallSnapshotReq, InstallSnapshotResp, Member,
    Membership, RaftCommand, RaftNode, RequestVoteReq, RequestVoteResp, RAFT_SNAPSHOT_CHUNK,
};
use crate::sharding::NodeId;

/// Delivers Raft RPCs to peers. Implementations must time out (never block forever).
pub trait RaftTransport: Send + Sync {
    fn request_vote(&self, peer: NodeId, req: &RequestVoteReq) -> Result<RequestVoteResp>;
    fn append_entries(&self, peer: NodeId, req: &AppendEntriesReq) -> Result<AppendEntriesResp>;
    fn install_snapshot(&self, peer: NodeId, req: &InstallSnapshotReq) -> Result<InstallSnapshotResp>;
    /// The cluster configuration changed: learn the addresses of new members.
    fn update_members(&self, _members: &Membership) {}
}

/// Timing and batching of the runtime.
#[derive(Clone, Copy, Debug)]
pub struct RaftConfig {
    /// Election timeouts are drawn uniformly from `[min, max]`.
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    /// Leader heartbeat period (well below `election_timeout_min`).
    pub heartbeat_interval: Duration,
    /// How long a write waits for its replicated outcome before reporting `CommitUnknown`.
    pub commit_timeout: Duration,
    /// Entries per `AppendEntries` message.
    pub max_entries_per_append: usize,
    /// Payload bytes per `AppendEntries` message.
    pub max_bytes_per_append: usize,
    /// How often the leader proposes a TTL sweep (only when TTL records exist).
    pub sweep_interval: Duration,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: Duration::from_millis(300),
            election_timeout_max: Duration::from_millis(600),
            heartbeat_interval: Duration::from_millis(60),
            commit_timeout: Duration::from_secs(5),
            max_entries_per_append: 512,
            max_bytes_per_append: 4 * 1024 * 1024,
            sweep_interval: Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct PeerProgress {
    next_index: u64,
    match_index: u64,
    /// Send time of the latest request this peer acknowledged in the current term.
    acked_sent_at: Option<Instant>,
}

/// Snapshot of a node's cluster state (for clients and operators).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClusterStatus {
    pub node_id: NodeId,
    pub is_leader: bool,
    pub leader_id: Option<NodeId>,
    pub term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
}

struct Shared<T: ZeroCopyRecord + Indexable> {
    node: Arc<RaftNode<T>>,
    transport: Arc<dyn RaftTransport>,
    config: RaftConfig,
    shutdown: AtomicBool,
    /// Bumped whenever replicators should send now (new entries, new leadership).
    wake_gen: Mutex<u64>,
    wake_cv: Condvar,
    progress: Mutex<HashMap<NodeId, PeerProgress>>,
    /// Term in which `progress` was initialized (reset on every new leadership).
    progress_term: AtomicU64,
    rng: AtomicU64,
    /// Peers with a running replicator thread.
    replicating: Mutex<HashSet<NodeId>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    /// Configuration last handed to the transport.
    known_members: Mutex<Membership>,
}

/// A running Raft replica: owns the election timer and one replicator thread per peer.
pub struct RaftRuntime<T: ZeroCopyRecord + Indexable> {
    shared: Arc<Shared<T>>,
}

impl<T: ZeroCopyRecord + Indexable> core::fmt::Debug for RaftRuntime<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RaftRuntime")
            .field("node_id", &self.shared.node.node_id())
            .field("members", &self.shared.node.membership())
            .finish_non_exhaustive()
    }
}

impl<T: ZeroCopyRecord + Indexable> RaftRuntime<T> {
    /// Starts the runtime for `node`; `members` lists every voter of a new cluster, this node
    /// included (all voters, no addresses). An empty list starts a node that joins an existing
    /// cluster: it waits for a leader to add it ([`Self::add_voter`]).
    pub fn start(
        node: Arc<RaftNode<T>>,
        members: Vec<NodeId>,
        transport: Arc<dyn RaftTransport>,
        config: RaftConfig,
    ) -> Result<Arc<Self>> {
        let members = Membership::voters_from_ids(members)?;
        Self::start_with_membership(node, members, transport, config)
    }

    /// [`Self::start`] with a full bootstrap configuration (addresses, learners). The bootstrap
    /// only applies until the log carries a configuration of its own.
    pub fn start_with_membership(
        node: Arc<RaftNode<T>>,
        bootstrap: Membership,
        transport: Arc<dyn RaftTransport>,
        config: RaftConfig,
    ) -> Result<Arc<Self>> {
        if config.heartbeat_interval * 2 >= config.election_timeout_min
            || config.election_timeout_min > config.election_timeout_max
        {
            return Err(StorageError::invalid_value(
                "raft timing: heartbeat must be < half the minimum election timeout, and min <= max",
            ));
        }
        node.set_bootstrap_membership(bootstrap);
        node.set_vote_stickiness(Some(config.election_timeout_min));
        let seed = 0x9E37_79B9_7F4A_7C15u64 ^ (u64::from(node.node_id()) << 32) ^ u64::from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.subsec_nanos()),
        );
        let shared = Arc::new(Shared {
            node,
            transport,
            config,
            shutdown: AtomicBool::new(false),
            wake_gen: Mutex::new(0),
            wake_cv: Condvar::new(),
            progress: Mutex::new(HashMap::new()),
            progress_term: AtomicU64::new(0),
            rng: AtomicU64::new(seed | 1),
            replicating: Mutex::new(HashSet::new()),
            threads: Mutex::new(Vec::new()),
            known_members: Mutex::new(Membership::default()),
        });
        {
            let s = Arc::clone(&shared);
            let timer = thread::Builder::new()
                .name("storine-raft-timer".into())
                .spawn(move || timer_loop(&s))
                .map_err(|e| StorageError::io("spawn raft timer", e))?;
            shared.threads.lock().unwrap_or_else(|p| p.into_inner()).push(timer);
        }
        reconcile_members(&shared);
        Ok(Arc::new(Self { shared }))
    }

    #[inline]
    pub fn node(&self) -> &Arc<RaftNode<T>> {
        &self.shared.node
    }

    /// Current cluster view of this node.
    pub fn status(&self) -> ClusterStatus {
        let n = &self.shared.node;
        ClusterStatus {
            node_id: n.node_id(),
            is_leader: n.is_leader(),
            leader_id: n.leader_id(),
            term: n.current_term(),
            commit_index: n.commit_index(),
            last_applied: n.last_applied(),
        }
    }

    /// Replicates an atomic conditional batch and returns its applied outcome
    /// (`NotLeader` on followers, `CommitUnknown` if it could not be confirmed in time).
    pub fn commit(&self, ops: Vec<WriteOp<T>>) -> Result<Vec<WriteResult>> {
        let index = self.shared.node.propose_ops(&ops)?;
        self.after_propose();
        self.shared.node.wait_applied(index, self.shared.config.commit_timeout)
    }

    /// Proposes any command (no-op, sweep) without waiting.
    pub fn propose(&self, command: RaftCommand) -> Result<u64> {
        let index = self.shared.node.propose(command)?;
        self.after_propose();
        Ok(index)
    }

    fn after_propose(&self) {
        reconcile_members(&self.shared);
        // A lone voter is a quorum by itself.
        let _ = advance_commit(&self.shared);
        wake_replicators(&self.shared);
    }

    // ── Membership changes (`TASK-121`) ─────────────────────────────────────────────────────

    /// The cluster configuration in force on this node.
    pub fn membership(&self) -> Membership {
        self.shared.node.membership()
    }

    /// Leader: adds `id` (serving on `addr`) as a learner: it receives the log but does not vote.
    pub fn add_learner(&self, id: NodeId, addr: &str) -> Result<()> {
        self.change(|m| {
            if m.contains(id) {
                return Err(StorageError::invalid_value("the node is already a cluster member"));
            }
            let mut members = m.members().to_vec();
            members.push(Member { id, addr: addr.to_owned(), voter: false });
            Membership::new(members)
        })
    }

    /// Leader: makes the learner `id` a voter, once it has caught up with the log (waits up to
    /// `catch_up` for that).
    pub fn promote(&self, id: NodeId, catch_up: Duration) -> Result<()> {
        if !self.membership().get(id).is_some_and(|m| !m.voter) {
            return Err(StorageError::invalid_value("only a learner can be promoted"));
        }
        self.wait_caught_up(id, catch_up)?;
        self.change(|m| {
            let members = m
                .members()
                .iter()
                .map(|x| Member { voter: x.voter || x.id == id, ..x.clone() })
                .collect();
            Membership::new(members)
        })
    }

    /// Leader: adds `id` as a voter the safe way: as a learner first, promoted once caught up
    /// (so the cluster never waits on a member that has no data yet).
    pub fn add_voter(&self, id: NodeId, addr: &str, catch_up: Duration) -> Result<()> {
        match self.membership().get(id) {
            Some(m) if m.voter => return Ok(()),
            Some(_) => {}
            None => self.add_learner(id, addr)?,
        }
        self.promote(id, catch_up)
    }

    /// Leader: removes `id` (a voter or a learner). Removing the leader itself makes it step
    /// down once the change commits; the remaining voters elect a new one.
    pub fn remove_member(&self, id: NodeId) -> Result<()> {
        self.change(|m| {
            if !m.contains(id) {
                return Err(StorageError::invalid_value("the node is not a cluster member"));
            }
            Membership::new(m.members().iter().filter(|x| x.id != id).cloned().collect())
        })
    }

    fn change(&self, f: impl FnOnce(&Membership) -> Result<Membership>) -> Result<()> {
        let next = f(&self.shared.node.membership())?;
        let index = self.shared.node.propose_membership(next)?;
        self.after_propose();
        self.shared.node.wait_applied(index, self.shared.config.commit_timeout).map(|_| ())
    }

    /// Waits until the leader knows `id` stores the whole committed log.
    fn wait_caught_up(&self, id: NodeId, timeout: Duration) -> Result<()> {
        let s = &self.shared;
        let deadline = Instant::now() + timeout;
        loop {
            if !s.node.is_leader() {
                return Err(StorageError::NotLeader { leader: s.node.leader_id().map(u64::from) });
            }
            let target = s.node.commit_index();
            let matched = s
                .progress
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&id)
                .map_or(0, |p| p.match_index);
            if matched >= target {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(StorageError::resource_exhausted(
                    "the new member did not catch up with the log in time (is it running and reachable?)",
                ));
            }
            wake_replicators(s);
            thread::sleep(Duration::from_millis(20));
        }
    }

    /// Linearizable read barrier: returns once this node may serve a read that reflects every
    /// write acknowledged before the call (leader lease, else a read-index round).
    pub fn read_barrier(&self) -> Result<()> {
        let s = &self.shared;
        if !s.node.is_leader() {
            return Err(StorageError::NotLeader { leader: s.node.leader_id().map(u64::from) });
        }
        let read_index = s.node.commit_index();
        if !self.lease_valid() {
            heartbeat_round(s)?;
            if !self.lease_valid() {
                return Err(StorageError::NotLeader { leader: None });
            }
        }
        // Wait until the state machine reflects `read_index`.
        let deadline = Instant::now() + s.config.commit_timeout;
        while s.node.last_applied() < read_index {
            if Instant::now() >= deadline {
                return Err(StorageError::CommitUnknown { index: read_index });
            }
            thread::sleep(Duration::from_micros(200));
        }
        Ok(())
    }

    /// `true` while a quorum acknowledged this leader recently enough that no other leader can
    /// have been elected (lease = 90% of the minimum election timeout, `RULE-SAFE-09`).
    pub fn lease_valid(&self) -> bool {
        let s = &self.shared;
        if !s.node.is_leader() {
            return false;
        }
        let members = s.node.membership();
        let self_votes = members.is_voter(s.node.node_id());
        let peers: Vec<NodeId> = members.voters().filter(|&v| v != s.node.node_id()).collect();
        if peers.is_empty() {
            return self_votes;
        }
        let lease = s.config.election_timeout_min.mul_f64(0.9);
        let term = s.node.current_term();
        let now = Instant::now();
        // Lock order: node state is never taken while `progress` is held.
        let progress = s.progress.lock().unwrap_or_else(|p| p.into_inner());
        if s.progress_term.load(Ordering::Acquire) != term {
            return false;
        }
        let mut fresh = usize::from(self_votes);
        for p in &peers {
            if progress
                .get(p)
                .and_then(|pp| pp.acked_sent_at)
                .is_some_and(|t| now.saturating_duration_since(t) < lease)
            {
                fresh += 1;
            }
        }
        fresh >= quorum(&members)
    }

    /// Stops all runtime threads (the node keeps its durable state).
    pub fn stop(&self) {
        self.shared.shutdown.store(true, Ordering::Release);
        wake_replicators(&self.shared);
        loop {
            let threads = core::mem::take(&mut *self.shared.threads.lock().unwrap_or_else(|p| p.into_inner()));
            if threads.is_empty() {
                break;
            }
            for t in threads {
                let _ = t.join();
            }
        }
    }
}

impl<T: ZeroCopyRecord + Indexable> Drop for RaftRuntime<T> {
    fn drop(&mut self) {
        self.stop();
    }
}

impl crate::document::CommitSink for RaftRuntime<crate::document::DocumentRecord> {
    fn commit(&self, ops: Vec<WriteOp<crate::document::DocumentRecord>>) -> Result<Vec<WriteResult>> {
        RaftRuntime::commit(self, ops)
    }
}

/// Votes needed among the voters of `members`.
#[inline]
fn quorum(members: &Membership) -> usize {
    members.voters().count() / 2 + 1
}

/// Voters other than this node.
fn voter_peers<T: ZeroCopyRecord + Indexable>(s: &Shared<T>, members: &Membership) -> Vec<NodeId> {
    members.voters().filter(|&v| v != s.node.node_id()).collect()
}

/// Starts a replicator for every member without one and hands new addresses to the transport.
/// Replicators of removed members stop by themselves once the removal commits.
fn reconcile_members<T: ZeroCopyRecord + Indexable + 'static>(s: &Arc<Shared<T>>) {
    if s.shutdown.load(Ordering::Acquire) {
        return;
    }
    let members = s.node.membership();
    {
        let mut known = s.known_members.lock().unwrap_or_else(|p| p.into_inner());
        if *known != members {
            s.transport.update_members(&members);
            *known = members.clone();
        }
    }
    let me = s.node.node_id();
    let mut running = s.replicating.lock().unwrap_or_else(|p| p.into_inner());
    for m in members.members().iter().filter(|m| m.id != me) {
        if running.contains(&m.id) {
            continue;
        }
        let (sh, peer) = (Arc::clone(s), m.id);
        if let Ok(h) = thread::Builder::new()
            .name(format!("storine-raft-peer-{peer}"))
            .spawn(move || replicator_loop(&sh, peer))
        {
            running.insert(peer);
            s.threads.lock().unwrap_or_else(|p| p.into_inner()).push(h);
        }
    }
}

fn wake_replicators<T: ZeroCopyRecord + Indexable>(s: &Shared<T>) {
    let mut g = s.wake_gen.lock().unwrap_or_else(|p| p.into_inner());
    *g = g.wrapping_add(1);
    s.wake_cv.notify_all();
}

fn next_random<T: ZeroCopyRecord + Indexable>(s: &Shared<T>) -> u64 {
    // xorshift64*: only used to spread election timeouts.
    let mut x = s.rng.load(Ordering::Relaxed);
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    s.rng.store(x, Ordering::Relaxed);
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

fn election_timeout<T: ZeroCopyRecord + Indexable>(s: &Shared<T>) -> Duration {
    let min = s.config.election_timeout_min.as_micros() as u64;
    let max = s.config.election_timeout_max.as_micros() as u64;
    let span = max.saturating_sub(min).max(1);
    Duration::from_micros(min + next_random(s) % span)
}

/// Election timer and leader housekeeping. Sleeps until the next deadline (no busy loop).
fn timer_loop<T: ZeroCopyRecord + Indexable + 'static>(s: &Arc<Shared<T>>) {
    let mut timeout = election_timeout(s);
    let mut last_sweep = Instant::now();
    while !s.shutdown.load(Ordering::Acquire) {
        reconcile_members(s);
        if s.node.is_leader() {
            if s.node.engine().has_ttl_records() && last_sweep.elapsed() >= s.config.sweep_interval {
                last_sweep = Instant::now();
                let now_ns = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
                if s.node.propose(RaftCommand::SweepExpired { now_ns }).is_ok() {
                    let _ = advance_commit(s);
                    wake_replicators(s);
                }
            }
            sleep_interruptibly(s, s.config.heartbeat_interval);
            continue;
        }
        let since = s.node.since_contact();
        if since >= timeout && !s.node.membership().is_voter(s.node.node_id()) {
            // Learners, removed nodes and nodes still waiting to join never campaign.
            s.node.touch_contact();
            continue;
        }
        if since >= timeout {
            run_election(s);
            timeout = election_timeout(s);
            continue;
        }
        sleep_interruptibly(s, (timeout - since).min(Duration::from_millis(100)));
    }
}

fn sleep_interruptibly<T: ZeroCopyRecord + Indexable>(s: &Shared<T>, d: Duration) {
    let g = s.wake_gen.lock().unwrap_or_else(|p| p.into_inner());
    if s.shutdown.load(Ordering::Acquire) {
        return;
    }
    let _ = s.wake_cv.wait_timeout(g, d);
}

/// Asks every peer in parallel; returns the responses that arrived.
fn ask_votes<T: ZeroCopyRecord + Indexable>(
    s: &Shared<T>,
    voters: &[NodeId],
    req: RequestVoteReq,
) -> Vec<RequestVoteResp> {
    thread::scope(|scope| {
        let handles: Vec<_> = voters
            .iter()
            .map(|&p| {
                let t = Arc::clone(&s.transport);
                scope.spawn(move || t.request_vote(p, &req).ok())
            })
            .collect();
        handles.into_iter().filter_map(|h| h.join().ok().flatten()).collect()
    })
}

fn run_election<T: ZeroCopyRecord + Indexable>(s: &Arc<Shared<T>>) {
    let members = s.node.membership();
    let peers = voter_peers(s, &members);
    // Pre-vote first: only a candidate that could win increments its term.
    if !peers.is_empty() {
        let pre = s.node.pre_vote_request();
        let granted = 1 + ask_votes(s, &peers, pre).iter().filter(|r| r.vote_granted).count();
        if granted < quorum(&members) {
            s.node.touch_contact();
            return;
        }
    }
    let Ok(req) = s.node.start_election() else {
        return;
    };
    s.node.touch_contact();
    let mut votes = 1usize;
    for resp in ask_votes(s, &peers, req) {
        if resp.term > req.term {
            let _ = s.node.observe_term(resp.term);
            return;
        }
        if resp.vote_granted && resp.term == req.term {
            votes += 1;
        }
    }
    if votes >= quorum(&members) && s.node.become_leader(req.term) {
        let next = s.node.last_log_index() + 1;
        {
            let mut progress = s.progress.lock().unwrap_or_else(|p| p.into_inner());
            progress.clear();
            for m in members.members().iter().filter(|m| m.id != s.node.node_id()) {
                progress.insert(m.id, PeerProgress { next_index: next, match_index: 0, acked_sent_at: None });
            }
            s.progress_term.store(req.term, Ordering::Release);
        }
        let _ = advance_commit(s);
        wake_replicators(s);
    }
}

/// Commits the highest index stored on a quorum (current-term rule enforced by the node).
/// Commits the highest index stored on a quorum of voters (current-term rule enforced by the
/// node). The leader counts itself only while it is a voter.
fn advance_commit<T: ZeroCopyRecord + Indexable>(s: &Shared<T>) -> Result<u64> {
    if !s.node.is_leader() {
        return Ok(s.node.commit_index());
    }
    let members = s.node.membership();
    let mut matches: Vec<u64> = {
        let progress = s.progress.lock().unwrap_or_else(|p| p.into_inner());
        voter_peers(s, &members)
            .iter()
            .map(|p| progress.get(p).map_or(0, |pp| pp.match_index))
            .collect()
    };
    if members.is_voter(s.node.node_id()) {
        matches.push(s.node.last_log_index());
    }
    matches.sort_unstable_by(|a, b| b.cmp(a));
    let Some(&quorum_index) = matches.get(quorum(&members) - 1) else {
        return Ok(s.node.commit_index());
    };
    s.node.advance_leader_commit(quorum_index)
}

/// One replication round to `peer`; returns `true` if the peer acknowledged the current term.
fn replicate_once<T: ZeroCopyRecord + Indexable>(s: &Shared<T>, peer: NodeId) -> bool {
    let term = s.node.current_term();
    if !s.node.is_leader() || s.progress_term.load(Ordering::Acquire) != term {
        return false;
    }
    let next = s
        .progress
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&peer)
        .map_or(s.node.last_log_index() + 1, |pp| pp.next_index);
    let sent_at = Instant::now();
    if s.node.needs_snapshot(next) {
        return send_snapshot(s, peer, term);
    }
    let Ok(req) = s.node.build_append_entries_bounded(next, s.config.max_entries_per_append, s.config.max_bytes_per_append) else {
        return false;
    };
    let last_sent = req.prev_log_index + req.entries.len() as u64;
    let Ok(resp) = s.transport.append_entries(peer, &req) else {
        return false;
    };
    if resp.term > term {
        let _ = s.node.observe_term(resp.term);
        return false;
    }
    let mut progress = s.progress.lock().unwrap_or_else(|p| p.into_inner());
    if s.progress_term.load(Ordering::Acquire) != term {
        return false;
    }
    let pp = progress.entry(peer).or_default();
    if resp.success {
        pp.match_index = pp.match_index.max(resp.match_index.min(last_sent));
        pp.next_index = pp.match_index + 1;
        pp.acked_sent_at = Some(pp.acked_sent_at.map_or(sent_at, |t| t.max(sent_at)));
        drop(progress);
        let _ = advance_commit(s);
        true
    } else {
        // The follower still answered in our term: it acknowledges our leadership.
        pp.acked_sent_at = Some(pp.acked_sent_at.map_or(sent_at, |t| t.max(sent_at)));
        pp.next_index = (resp.match_index + 1).min(next.saturating_sub(1)).max(1);
        true
    }
}

fn send_snapshot<T: ZeroCopyRecord + Indexable>(s: &Shared<T>, peer: NodeId, term: u64) -> bool {
    let sent_at = Instant::now();
    let mut offset = 0u64;
    for _ in 0..1_000_000 {
        if s.shutdown.load(Ordering::Acquire) || !s.node.is_leader() {
            return false;
        }
        let Ok(req) = s.node.build_install_snapshot(offset, RAFT_SNAPSHOT_CHUNK) else {
            return false;
        };
        let last = req.last_included_index;
        let Ok(resp) = s.transport.install_snapshot(peer, &req) else {
            return false;
        };
        if resp.term > term {
            let _ = s.node.observe_term(resp.term);
            return false;
        }
        if resp.installed {
            let mut progress = s.progress.lock().unwrap_or_else(|p| p.into_inner());
            let pp = progress.entry(peer).or_default();
            pp.match_index = pp.match_index.max(last);
            pp.next_index = pp.match_index + 1;
            pp.acked_sent_at = Some(sent_at);
            return true;
        }
        offset = resp.next_offset;
    }
    false
}

fn replicator_loop<T: ZeroCopyRecord + Indexable>(s: &Arc<Shared<T>>, peer: NodeId) {
    let mut seen_gen = 0u64;
    while !s.shutdown.load(Ordering::Acquire) {
        {
            // A removed member keeps receiving the log until its removal commits.
            let (members, committed) = s.node.membership_state();
            if !members.contains(peer) && committed {
                let mut running = s.replicating.lock().unwrap_or_else(|p| p.into_inner());
                if !s.node.membership().contains(peer) {
                    running.remove(&peer);
                    s.progress.lock().unwrap_or_else(|p| p.into_inner()).remove(&peer);
                    return;
                }
            }
        }
        if s.node.is_leader() {
            let ok = replicate_once(s, peer);
            // Keep sending while the peer is behind (catch-up), otherwise wait for new entries or
            // the next heartbeat.
            let last = s.node.last_log_index();
            let behind = ok
                && s.progress
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get(&peer)
                    .is_some_and(|pp| pp.match_index < last);
            if behind {
                continue;
            }
        }
        let g = s.wake_gen.lock().unwrap_or_else(|p| p.into_inner());
        if *g == seen_gen && !s.shutdown.load(Ordering::Acquire) {
            let (g, _) = s
                .wake_cv
                .wait_timeout(g, s.config.heartbeat_interval)
                .unwrap_or_else(|p| p.into_inner());
            seen_gen = *g;
        } else {
            seen_gen = *g;
        }
    }
}

/// Sends one heartbeat round to every peer in parallel (read-index confirmation).
fn heartbeat_round<T: ZeroCopyRecord + Indexable>(s: &Shared<T>) -> Result<()> {
    let members = s.node.membership();
    let peers = voter_peers(s, &members);
    thread::scope(|scope| {
        for &p in &peers {
            scope.spawn(move || {
                let _ = replicate_once(s, p);
            });
        }
    });
    Ok(())
}

// ── In-process transport (tests, single-process clusters) ───────────────────────────────────

/// In-process network of Raft nodes with simulated partitions (tests, single-process clusters).
pub struct LocalNetwork<T: ZeroCopyRecord + Indexable> {
    nodes: Mutex<HashMap<NodeId, Arc<RaftNode<T>>>>,
    isolated: Mutex<Vec<NodeId>>,
}

impl<T: ZeroCopyRecord + Indexable> core::fmt::Debug for LocalNetwork<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("LocalNetwork").finish_non_exhaustive()
    }
}

impl<T: ZeroCopyRecord + Indexable> LocalNetwork<T> {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { nodes: Mutex::new(HashMap::new()), isolated: Mutex::new(Vec::new()) })
    }

    pub fn add(&self, node: Arc<RaftNode<T>>) {
        self.nodes.lock().unwrap_or_else(|p| p.into_inner()).insert(node.node_id(), node);
    }

    /// Cuts (`true`) or restores (`false`) every link of `node`.
    pub fn set_isolated(&self, node: NodeId, isolated: bool) {
        let mut iso = self.isolated.lock().unwrap_or_else(|p| p.into_inner());
        iso.retain(|&n| n != node);
        if isolated {
            iso.push(node);
        }
    }

    /// A transport sending from `from`.
    pub fn transport(self: &Arc<Self>, from: NodeId) -> Arc<dyn RaftTransport> {
        Arc::new(NetTransport { net: Arc::clone(self), from })
    }

    fn target(&self, from: NodeId, to: NodeId) -> Result<Arc<RaftNode<T>>> {
        let iso = self.isolated.lock().unwrap_or_else(|p| p.into_inner());
        if iso.contains(&from) || iso.contains(&to) {
            return Err(StorageError::resource_exhausted("simulated network partition"));
        }
        drop(iso);
        self.nodes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&to)
            .cloned()
            .ok_or_else(|| StorageError::invalid_key("unknown raft peer"))
    }
}

struct NetTransport<T: ZeroCopyRecord + Indexable> {
    net: Arc<LocalNetwork<T>>,
    from: NodeId,
}

impl<T: ZeroCopyRecord + Indexable> RaftTransport for NetTransport<T> {
    fn request_vote(&self, peer: NodeId, req: &RequestVoteReq) -> Result<RequestVoteResp> {
        Ok(self.net.target(self.from, peer)?.handle_request_vote(*req))
    }

    fn append_entries(&self, peer: NodeId, req: &AppendEntriesReq) -> Result<AppendEntriesResp> {
        // Round-trip through the wire format so tests exercise the real encoding.
        let decoded = AppendEntriesReq::from_wire_bytes(&req.to_wire_bytes())?;
        self.net.target(self.from, peer)?.handle_append_entries(decoded)
    }

    fn install_snapshot(&self, peer: NodeId, req: &InstallSnapshotReq) -> Result<InstallSnapshotResp> {
        self.net.target(self.from, peer)?.handle_install_snapshot(req.clone())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::{DocOp, DocWrite, DocumentRecord, DocumentStore};
    use crate::engine::{EngineConfig, Precondition, StorageEngine};
    use std::collections::BTreeMap;
    use storage_codec::field_ops::FieldOp;
    use storage_codec::PropertyValue;
    use storage_types::{InlineId, Version};

    fn fast() -> RaftConfig {
        // Heartbeats stay fast; elections wait long enough that a test machine busy with other
        // tests' fsyncs does not depose a healthy leader mid-test.
        RaftConfig {
            election_timeout_min: Duration::from_millis(400),
            election_timeout_max: Duration::from_millis(800),
            heartbeat_interval: Duration::from_millis(25),
            commit_timeout: Duration::from_secs(3),
            sweep_interval: Duration::from_millis(100),
            ..RaftConfig::default()
        }
    }

    struct Cluster {
        base: std::path::PathBuf,
        net: Arc<LocalNetwork<DocumentRecord>>,
        engines: Vec<Arc<StorageEngine<DocumentRecord>>>,
        nodes: Vec<Arc<RaftNode<DocumentRecord>>>,
        runtimes: Vec<Arc<RaftRuntime<DocumentRecord>>>,
    }

    impl Cluster {
        fn new(name: &str, n: u16) -> Self {
            let base = std::env::temp_dir().join(format!("stge_raftrt_{name}_{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&base);
            let net = LocalNetwork::new();
            let mut engines = Vec::new();
            let mut nodes = Vec::new();
            for i in 1..=n {
                let e = Arc::new(StorageEngine::<DocumentRecord>::open(base.join(format!("n{i}")), EngineConfig::default()).unwrap());
                let node = Arc::new(RaftNode::new(i, Arc::clone(&e)).unwrap());
                net.add(Arc::clone(&node));
                engines.push(e);
                nodes.push(node);
            }
            let ids: Vec<NodeId> = (1..=n).collect();
            let runtimes = nodes
                .iter()
                .map(|node| RaftRuntime::start(Arc::clone(node), ids.clone(), net.transport(node.node_id()), fast()).unwrap())
                .collect();
            Self { base, net, engines, nodes, runtimes }
        }

        /// Starts node `id` with no configuration: it waits to be added to the cluster.
        fn join(&mut self, id: NodeId) -> usize {
            let e = Arc::new(
                StorageEngine::<DocumentRecord>::open(self.base.join(format!("n{id}")), EngineConfig::default()).unwrap(),
            );
            let node = Arc::new(RaftNode::new(id, Arc::clone(&e)).unwrap());
            self.net.add(Arc::clone(&node));
            let rt = RaftRuntime::start(Arc::clone(&node), Vec::new(), self.net.transport(id), fast()).unwrap();
            self.engines.push(e);
            self.nodes.push(node);
            self.runtimes.push(rt);
            self.nodes.len() - 1
        }

        fn put(&self, l: usize, key: &str) -> Result<Vec<WriteResult>> {
            self.runtimes[l].commit(vec![WriteOp::put(did("m", key), doc("m", key, &[]))])
        }

        fn leader(&self, timeout: Duration) -> usize {
            let deadline = Instant::now() + timeout;
            loop {
                let leaders: Vec<usize> = (0..self.runtimes.len())
                    .filter(|&i| self.runtimes[i].lease_valid())
                    .collect();
                if leaders.len() == 1 {
                    return leaders[0];
                }
                assert!(Instant::now() < deadline, "no single leader elected");
                thread::sleep(Duration::from_millis(10));
            }
        }

        /// A leader among `idx` only.
        fn leader_among(&self, idx: &[usize], timeout: Duration) -> usize {
            let deadline = Instant::now() + timeout;
            loop {
                if let Some(&i) = idx.iter().find(|&&i| self.runtimes[i].lease_valid()) {
                    return i;
                }
                assert!(Instant::now() < deadline, "no leader among {idx:?}");
                thread::sleep(Duration::from_millis(10));
            }
        }

        fn wait_converged(&self, idx: &[usize]) {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let applied: Vec<u64> = idx.iter().map(|&i| self.nodes[i].last_applied()).collect();
                let commit = idx.iter().map(|&i| self.nodes[i].commit_index()).max().unwrap_or(0);
                if applied.iter().all(|&a| a == commit) && commit > 0 {
                    return;
                }
                assert!(Instant::now() < deadline, "replicas did not converge: {applied:?} vs {commit}");
                thread::sleep(Duration::from_millis(10));
            }
        }

        fn state(&self, i: usize) -> Vec<(InlineId, Version, BTreeMap<String, PropertyValue>, u64)> {
            let e = &self.engines[i];
            let mut ids = e.live_ids();
            ids.sort();
            ids.into_iter()
                .filter_map(|id| {
                    let (v, rec) = e.get_owned(id).ok()?;
                    let ttl = e.version_and_ttl(id).map_or(0, |(_, t)| t);
                    Some((id, v, rec.fields, ttl))
                })
                .collect()
        }
    }

    impl Drop for Cluster {
        fn drop(&mut self) {
            for r in &self.runtimes {
                r.stop();
            }
            let _ = std::fs::remove_dir_all(&self.base);
        }
    }

    fn doc(coll: &str, key: &str, fields: &[(&str, PropertyValue)]) -> DocumentRecord {
        let mut d = DocumentRecord::new(coll, key);
        for (k, v) in fields {
            d.fields.insert((*k).to_owned(), v.clone());
        }
        d
    }

    fn did(coll: &str, key: &str) -> InlineId {
        crate::document::document_id(coll, key).unwrap()
    }

    #[test]
    fn test_replicas_agree_refuse_direct_writes_and_reject_deterministically() {
        let c = Cluster::new("agree", 3);
        let l = c.leader(Duration::from_secs(5));
        let rt = &c.runtimes[l];
        let id = did("acct", "a");
        let res = rt
            .commit(vec![WriteOp::put(id, doc("acct", "a", &[("bal", PropertyValue::Int64(10))])).when(Precondition::Absent)])
            .unwrap();
        let v = res[0].version.unwrap();
        // Versions come from the log index: identical everywhere.
        assert!(v.0 >= 2);
        // A failed precondition is a replicated, deterministic outcome; the cluster keeps going.
        let err = rt
            .commit(vec![WriteOp::put(id, doc("acct", "a", &[])).when(Precondition::Absent)])
            .unwrap_err();
        assert!(matches!(err, StorageError::ConditionFailed { .. }), "{err:?}");
        rt.commit(vec![WriteOp::patch(id, vec![FieldOp::Add {
            path: "bal".into(),
            delta: PropertyValue::Int64(5),
            min: Some(PropertyValue::Int64(0)),
            max: None,
        }])])
        .unwrap();
        c.wait_converged(&[0, 1, 2]);
        let s0 = c.state(0);
        assert_eq!(s0, c.state(1));
        assert_eq!(s0, c.state(2));
        assert_eq!(s0[0].2["bal"], PropertyValue::Int64(15));
        // Replica engines (leader included) refuse direct writes; followers refuse proposals.
        for e in &c.engines {
            assert!(matches!(e.put(id, &doc("acct", "a", &[])), Err(StorageError::ReadOnlyReplica)));
        }
        let f = (l + 1) % 3;
        assert!(matches!(
            c.runtimes[f].commit(vec![WriteOp::delete(id)]),
            Err(StorageError::NotLeader { .. })
        ));
        assert!(c.runtimes[f].read_barrier().is_err());
        assert!(rt.read_barrier().is_ok());
    }

    #[test]
    fn test_partitioned_leader_loses_lease_reports_commit_unknown_and_retries_apply_once() {
        let c = Cluster::new("partition", 3);
        let l = c.leader(Duration::from_secs(5));
        let make_store = |i: usize| {
            let store = DocumentStore::from_engine(Arc::clone(&c.engines[i])).unwrap();
            store.set_commit_sink(Arc::clone(&c.runtimes[i]) as Arc<dyn crate::document::CommitSink>);
            store
        };
        let store = make_store(l);
        let mut f = BTreeMap::new();
        f.insert("bal".to_string(), PropertyValue::Int64(100));
        store.put("acct", "a", f).unwrap();
        let deposit = |s: &DocumentStore, rid: [u8; 16]| {
            s.write(
                vec![DocWrite {
                    collection: "acct".into(),
                    key: "a".into(),
                    op: DocOp::Patch(vec![FieldOp::Add { path: "bal".into(), delta: PropertyValue::Int64(1), min: None, max: None }]),
                    precondition: Precondition::Exists,
                }],
                Some(rid),
            )
        };
        // Isolate the leader: its write cannot reach a quorum.
        let old = c.nodes[l].node_id();
        c.net.set_isolated(old, true);
        let rid = [42u8; 16];
        let err = deposit(&store, rid).unwrap_err();
        assert!(matches!(err, StorageError::CommitUnknown { .. }), "{err:?}");
        // The others elect a new leader; the old one's lease is gone.
        let deadline = Instant::now() + Duration::from_secs(5);
        let new = loop {
            if let Some(i) = (0..3).find(|&i| i != l && c.runtimes[i].lease_valid()) {
                break i;
            }
            assert!(Instant::now() < deadline, "no new leader");
            thread::sleep(Duration::from_millis(10));
        };
        assert!(!c.runtimes[l].lease_valid(), "a deposed leader must not serve reads");
        // The client retries the same request on the new leader: applied once.
        let store2 = make_store(new);
        let first = deposit(&store2, rid).unwrap();
        assert!(!first.duplicate);
        assert!(deposit(&store2, rid).unwrap().duplicate);
        // Heal: the old leader's unacknowledged entry is discarded, everyone converges.
        c.net.set_isolated(old, false);
        thread::sleep(Duration::from_millis(400));
        c.wait_converged(&[0, 1, 2]);
        for i in 0..3 {
            assert_eq!(c.state(i), c.state(new), "replica {i}");
        }
        assert_eq!(store2.get("acct", "a").unwrap().unwrap().fields["bal"], PropertyValue::Int64(101));
    }

    #[test]
    fn test_ttl_sweep_is_replicated_and_snapshot_keeps_versions() {
        let c = Cluster::new("ttl", 3);
        let l = c.leader(Duration::from_secs(5));
        let rt = &c.runtimes[l];
        let short = did("t", "short");
        rt.commit(vec![WriteOp {
            id: short,
            kind: crate::engine::WriteKind::PutWithTtl(doc("t", "short", &[]), Duration::from_millis(50)),
            precondition: Precondition::None,
        }])
        .unwrap();
        for i in 0..50 {
            let key = format!("k{i}");
            rt.commit(vec![WriteOp::put(did("t", &key), doc("t", &key, &[("i", PropertyValue::Int64(i))]))]).unwrap();
        }
        // The leader's periodic sweep removes the expired record on every replica.
        let deadline = Instant::now() + Duration::from_secs(5);
        while c.engines.iter().any(|e| e.live_ids().contains(&short)) {
            assert!(Instant::now() < deadline, "expired record not swept everywhere");
            thread::sleep(Duration::from_millis(20));
        }
        c.wait_converged(&[0, 1, 2]);
        // A lagging follower catches up through InstallSnapshot with identical versions.
        let lag = (l + 1) % 3;
        c.net.set_isolated(c.nodes[lag].node_id(), true);
        for i in 0..20 {
            let key = format!("late{i}");
            rt.commit(vec![WriteOp::put(did("t", &key), doc("t", &key, &[]))]).unwrap();
        }
        let applied = c.nodes[l].last_applied();
        c.nodes[l].compact_log_through(applied).unwrap();
        c.net.set_isolated(c.nodes[lag].node_id(), false);
        let deadline = Instant::now() + Duration::from_secs(5);
        while c.nodes[lag].last_applied() < applied {
            assert!(Instant::now() < deadline, "snapshot not installed");
            thread::sleep(Duration::from_millis(20));
        }
        c.wait_converged(&[0, 1, 2]);
        assert_eq!(c.state(lag), c.state(l));
    }

    fn voters(m: &Membership) -> Vec<NodeId> {
        m.voters().collect()
    }

    #[test]
    fn test_membership_add_voter_then_remove_the_leader() {
        let mut c = Cluster::new("members", 3);
        let l = c.leader(Duration::from_secs(5));
        for i in 0..30 {
            c.put(l, &format!("k{i}")).unwrap();
        }
        // A fourth node joins: learner first, promoted once it has the log.
        let n4 = c.join(4);
        c.runtimes[l].add_voter(4, "n4:1", Duration::from_secs(5)).unwrap();
        c.wait_converged(&[0, 1, 2, n4]);
        for i in [0, 1, 2, n4] {
            assert_eq!(voters(&c.nodes[i].membership()), vec![1, 2, 3, 4], "node {i}");
            assert_eq!(c.state(i), c.state(l), "node {i}");
        }
        assert_eq!(c.nodes[n4].membership().get(4).unwrap().addr, "n4:1");
        // Four voters need three acknowledgements: the leader and node 4 alone cannot commit.
        let others: Vec<usize> = (0..3).filter(|&i| i != l).collect();
        for &i in &others {
            c.net.set_isolated(c.nodes[i].node_id(), true);
        }
        assert!(matches!(c.put(l, "lonely"), Err(StorageError::CommitUnknown { .. })));
        for &i in &others {
            c.net.set_isolated(c.nodes[i].node_id(), false);
        }
        let l = c.leader(Duration::from_secs(5));
        c.put(l, "after-heal").unwrap();
        // Remove the leader itself: it steps down once the change commits, the rest elect a new one.
        let old = c.nodes[l].node_id();
        c.runtimes[l].remove_member(old).unwrap();
        let rest: Vec<usize> = (0..4).filter(|&i| i != l).collect();
        let new = c.leader_among(&rest, Duration::from_secs(5));
        assert!(!c.nodes[l].is_leader());
        c.put(new, "without-old-leader").unwrap();
        c.wait_converged(&rest);
        for &i in &rest {
            assert!(!c.nodes[i].membership().contains(old), "node {i}");
            assert_eq!(c.state(i), c.state(new));
        }
        // The removed node never takes over again.
        let until = Instant::now() + Duration::from_millis(800);
        while Instant::now() < until {
            assert!(!c.runtimes[l].lease_valid());
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn test_learners_do_not_count_and_change_rules_hold() {
        let mut c = Cluster::new("learner", 3);
        let l = c.leader(Duration::from_secs(5));
        c.put(l, "a").unwrap();
        let n4 = c.join(4);
        c.runtimes[l].add_learner(4, "n4:1").unwrap();
        c.put(l, "b").unwrap();
        c.wait_converged(&[0, 1, 2, n4]);
        assert_eq!(c.state(n4), c.state(l), "a learner receives the log");
        assert_eq!(voters(&c.nodes[n4].membership()), vec![1, 2, 3]);
        // Leader + learner are not a quorum of three voters.
        let others: Vec<usize> = (0..3).filter(|&i| i != l).collect();
        for &i in &others {
            c.net.set_isolated(c.nodes[i].node_id(), true);
        }
        assert!(matches!(c.put(l, "c"), Err(StorageError::CommitUnknown { .. })));
        for &i in &others {
            c.net.set_isolated(c.nodes[i].node_id(), false);
        }
        let l = c.leader_among(&[0, 1, 2], Duration::from_secs(5));
        // Rules: followers refuse, two voters at once are refused, voters cannot be promoted,
        // unknown members cannot be removed.
        let f = (0..3).find(|&i| i != l).unwrap();
        assert!(matches!(c.runtimes[f].add_learner(9, "x"), Err(StorageError::NotLeader { .. })));
        let two_new = Membership::new(
            c.nodes[l]
                .membership()
                .members()
                .iter()
                .cloned()
                .chain([5, 6].map(|id| Member { id, addr: String::new(), voter: true }))
                .collect(),
        )
        .unwrap();
        assert!(c.nodes[l].propose_membership(two_new).is_err());
        assert!(c.runtimes[l].promote(1, Duration::from_millis(100)).is_err());
        assert!(c.runtimes[l].remove_member(42).is_err());
        assert!(Membership::new(vec![
            Member { id: 1, addr: String::new(), voter: true },
            Member { id: 1, addr: String::new(), voter: false },
        ])
        .is_err());
        // The learner is promoted later, and removed again.
        c.runtimes[l].promote(4, Duration::from_secs(5)).unwrap();
        assert_eq!(voters(&c.nodes[l].membership()), vec![1, 2, 3, 4]);
        c.runtimes[l].remove_member(4).unwrap();
        assert_eq!(voters(&c.nodes[l].membership()), vec![1, 2, 3]);
        c.put(l, "d").unwrap();
    }

    #[test]
    fn test_membership_survives_compaction_snapshots_and_restart() {
        let mut c = Cluster::new("members_snap", 3);
        let l = c.leader(Duration::from_secs(5));
        let n4 = c.join(4);
        c.runtimes[l].add_voter(4, "n4:1", Duration::from_secs(5)).unwrap();
        for i in 0..40 {
            c.put(l, &format!("k{i}")).unwrap();
        }
        c.wait_converged(&[0, 1, 2, n4]);
        // Compact past the configuration entry everywhere it matters.
        let applied = c.nodes[l].last_applied();
        c.nodes[l].compact_log_through(applied).unwrap();
        let f = (0..3).find(|&i| i != l).unwrap();
        c.nodes[f].compact_log_through(c.nodes[f].last_applied()).unwrap();
        // Node 5 joins after the compaction: it catches up through a snapshot that carries the
        // configuration (node 4 included), then gets the entry that adds it.
        let n5 = c.join(5);
        c.runtimes[l].add_voter(5, "n5:1", Duration::from_secs(5)).unwrap();
        c.put(l, "after-5").unwrap();
        c.wait_converged(&[0, 1, 2, n4, n5]);
        assert!(c.nodes[n5].snapshot_index() > 0, "node 5 was caught up by a snapshot");
        assert_eq!(voters(&c.nodes[n5].membership()), vec![1, 2, 3, 4, 5]);
        assert_eq!(c.state(n5), c.state(l));
        // A restarted replica (same files) reads its configuration back: compaction point
        // configuration from `_raft.members`, newer ones from the log.
        for i in [f, n5] {
            c.nodes[i].compact_log_through(c.nodes[i].last_applied()).unwrap();
            let again = RaftNode::new(c.nodes[i].node_id(), Arc::clone(&c.engines[i])).unwrap();
            assert_eq!(again.membership(), c.nodes[i].membership(), "node {i}");
            assert_eq!(voters(&again.membership()), vec![1, 2, 3, 4, 5]);
        }
    }

    #[test]
    fn test_vector_search_is_identical_on_every_replica() {
        use crate::document::{DocQuery, Nearest};
        let c = Cluster::new("vectors", 3);
        let l = c.leader(Duration::from_secs(5));
        let store = |i: usize| {
            let st = DocumentStore::from_engine(Arc::clone(&c.engines[i])).unwrap();
            st.set_commit_sink(Arc::clone(&c.runtimes[i]) as Arc<dyn crate::document::CommitSink>);
            st
        };
        let leader = store(l);
        leader.create_vector_index("v", "emb", 4).unwrap();
        for i in 0..50i64 {
            let mut f = BTreeMap::new();
            let x = i as f64;
            f.insert(
                "emb".to_string(),
                PropertyValue::List(vec![x, 50.0 - x, (x * 0.3).sin(), 1.0].into_iter().map(PropertyValue::Float64).collect()),
            );
            leader.put("v", &format!("k{i:02}"), f).unwrap();
        }
        c.wait_converged(&[0, 1, 2]);
        let q = DocQuery {
            collection: "v".into(),
            nearest: Some(Nearest { field: "emb".into(), vector: vec![10.0, 40.0, 0.0, 1.0], k: 5, exact: false }),
            ..Default::default()
        };
        let expected = leader.query(&q).unwrap().documents;
        assert_eq!(expected.len(), 5);
        for i in 0..3 {
            let st = DocumentStore::from_engine(Arc::clone(&c.engines[i])).unwrap();
            assert_eq!(st.query(&q).unwrap().documents, expected, "replica {i}");
            assert_eq!(c.engines[i].named_vector_count(crate::document::vector_field_id("v", "emb")), 50);
        }
    }

    #[test]
    fn test_columnar_views_follow_replicated_writes_on_every_replica() {
        let c = Cluster::new("columnar", 3);
        let l = c.leader(Duration::from_secs(5));
        let stores: Vec<DocumentStore> = (0..3)
            .map(|i| {
                let st = DocumentStore::from_engine(Arc::clone(&c.engines[i])).unwrap();
                st.set_commit_sink(Arc::clone(&c.runtimes[i]) as Arc<dyn crate::document::CommitSink>);
                st
            })
            .collect();
        let doc = |i: i64, rev: i64| {
            let mut f = BTreeMap::new();
            f.insert("n".to_string(), PropertyValue::Int64(i));
            f.insert("rev".to_string(), PropertyValue::Int64(rev));
            f.insert("city".to_string(), PropertyValue::Text(["a", "b", "c"][i as usize % 3].into()));
            f
        };
        stores[l].create_columnar("t").unwrap();
        for i in 0..300 {
            stores[l].put("t", &format!("k{i:03}"), doc(i, 0)).unwrap();
        }
        c.wait_converged(&[0, 1, 2]);
        // Followers open the projection from the replicated metadata and build it.
        for st in &stores {
            st.columnar("t").unwrap().expect("columnar on every replica").merge().unwrap();
        }
        // Later writes reach followers only through Raft apply.
        for i in (0..300).step_by(3) {
            stores[l].put("t", &format!("k{i:03}"), doc(i, 1)).unwrap();
        }
        for i in (1..300).step_by(5) {
            stores[l].delete("t", &format!("k{i:03}")).unwrap();
        }
        c.wait_converged(&[0, 1, 2]);
        let expected = stores[l].scan_collection("t").unwrap();
        for (i, st) in stores.iter().enumerate() {
            let col = st.columnar("t").unwrap().unwrap();
            let view = col.snapshot().unwrap().documents().unwrap();
            assert_eq!(view, st.scan_collection("t").unwrap(), "replica {i}: view vs its row store");
            assert_eq!(view, expected, "replica {i}: view vs the leader");
            col.merge().unwrap();
            assert_eq!(col.snapshot().unwrap().documents().unwrap(), expected, "replica {i} after merge");
        }
    }

    #[test]
    fn test_replicated_document_deletes() {
        let c = Cluster::new("docdel", 3);
        let l = c.leader(Duration::from_secs(5));
        let st = DocumentStore::from_engine(Arc::clone(&c.engines[l])).unwrap();
        st.set_commit_sink(Arc::clone(&c.runtimes[l]) as Arc<dyn crate::document::CommitSink>);
        let mut f = BTreeMap::new();
        f.insert("n".to_string(), PropertyValue::Int64(1));
        st.put("t", "a", f).unwrap();
        st.delete("t", "a").unwrap();
    }
}
