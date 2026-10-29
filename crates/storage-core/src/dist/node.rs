//! Multi-Raft node driver (`F-GEO-01`, `TASK-137`, `RULE-DIST-02`).
//!
//! One node hosts many ranges. Each is a [`RaftNode`] group over a key span of the node's one
//! engine and WAL. The driver runs elections (with pre-vote), replication, commit by quorum,
//! read-index reads and snapshot catch-up for all of them, without I/O of its own:
//! - the host calls [`NodeDriver::tick`] with the time and [`NodeDriver::step`] with each
//!   batch received;
//! - the driver queues outgoing traffic, which [`NodeDriver::drain_outbox`] returns as **one
//!   batch per peer node**.
//!
//! The same code runs under a TCP runtime and under the deterministic simulator.
//!
//! Idle cost scales with nodes, not ranges:
//! - **Coalesced heartbeats**: every range's messages to one peer travel in that peer's batch,
//!   so a node pair exchanges one message per heartbeat interval however many ranges they
//!   share.
//! - **Quiescence**: a leader whose followers have everything and that has nothing to do sends
//!   a `Quiesce` and stops heartbeating that range. Followers of a quiescent range keep no
//!   election timer while the leader's *node* is alive, and the node-level batches (even
//!   empty ones) prove that. Any proposal, read or vote request wakes the range.
//! - If the leader's node goes silent, its quiescent ranges wake and elect new leaders.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use storage_types::testing::SweepRng;
use storage_types::{Result, StorageError};

use crate::dist::hlc::Timestamp;
use crate::document::DocumentRecord;
use crate::engine::{StorageEngine, WriteOp};
use crate::raft::{
    AppendEntriesReq, AppendEntriesResp, InstallSnapshotReq, InstallSnapshotResp, KeySpan, Membership, RaftNode, RequestVoteReq, RequestVoteResp,
};
use crate::sharding::NodeId;

pub type RangeId = u64;
pub type Replica = RaftNode<DocumentRecord>;

/// Timing and batching knobs (microseconds).
#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub election_min_us: u64,
    pub election_max_us: u64,
    pub heartbeat_us: u64,
    /// A leader quiesces a range after this many idle heartbeat intervals.
    pub quiesce_after: u32,
    pub snapshot_chunk: usize,
    pub max_entries_per_append: usize,
    pub max_bytes_per_append: usize,
    /// Seeds the election-timeout jitter (deterministic per node).
    pub seed: u64,
    /// Distinguishes this start of the node from earlier ones (e.g. the start time): a
    /// restart wakes every range that was quiescent on the old incarnation.
    pub incarnation: u64,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            election_min_us: 300_000,
            election_max_us: 600_000,
            heartbeat_us: 50_000,
            quiesce_after: 3,
            snapshot_chunk: 64 * 1024,
            max_entries_per_append: 64,
            max_bytes_per_append: 1 << 20,
            seed: 1,
            incarnation: 0,
        }
    }
}

/// One Raft message for one range.
#[derive(Clone, Debug)]
pub enum RaftMsg {
    Vote(RequestVoteReq),
    VoteResp { req: RequestVoteReq, resp: RequestVoteResp },
    Append { req: AppendEntriesReq, hb: u64 },
    AppendResp { resp: AppendEntriesResp, req_term: u64, last_sent: u64, hb: u64 },
    Snap(InstallSnapshotReq),
    SnapResp { resp: InstallSnapshotResp, req_term: u64, last_included: u64 },
    /// The leader of `term` has nothing to send: stop the election timer while its node lives.
    Quiesce { term: u64, commit: u64 },
    /// Create this range here as a learner (a replica being added): its span and the
    /// configuration that includes it. A snapshot follows.
    Init { span: crate::raft::KeySpan, members: Vec<(NodeId, bool)> },
    /// Leadership transfer: campaign now, without pre-vote (the leader asked).
    TimeoutNow { term: u64 },
    /// From a leader whose committed configuration (at log index `config_index`) does not
    /// include the receiver: its replica is gone (unless it knows a newer configuration).
    Gone { config_index: u64 },
}

/// Everything one node sends another in one go.
#[derive(Clone, Debug, Default)]
pub struct Batch {
    pub from: NodeId,
    /// The sender's incarnation ([`NodeConfig::incarnation`]).
    pub incarnation: u64,
    /// The sender's clock (hybrid logical time), for the receiver's [`crate::dist::hlc::Hlc`].
    pub ts: Timestamp,
    pub msgs: Vec<(RangeId, RaftMsg)>,
}

#[derive(Debug, Default, Clone, Copy)]
struct Progress {
    next: u64,
    matched: u64,
    snapshot_offset: Option<u64>,
    /// A new replica starts from a snapshot, never from replaying the log: old entries
    /// (splits, configurations) describe a range it was not part of.
    force_snapshot: bool,
    /// The snapshot must be at or after this index (the configuration that added the replica).
    snapshot_min: u64,
}

#[derive(Debug)]
enum Campaign {
    Pre { term: u64, votes: BTreeSet<NodeId> },
    Real { term: u64, votes: BTreeSet<NodeId> },
}

/// A pending read-index read: ready once a quorum confirmed leadership after it started and
/// the read index is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadToken {
    pub range: RangeId,
    pub seq: u64,
}

#[derive(Debug)]
struct PendingRead {
    seq: u64,
    read_index: u64,
    hb_needed: u64,
    term: u64,
}

struct ReplicaState {
    raft: Arc<Replica>,
    election_deadline: u64,
    campaign: Option<Campaign>,
    lead_term: u64,
    progress: BTreeMap<NodeId, Progress>,
    hb_seq: u64,
    acks: BTreeMap<NodeId, u64>,
    last_heartbeat: u64,
    idle_beats: u32,
    /// Leader: members being removed and the time the removal was proposed. They are kept
    /// informed until they acknowledge the configuration that removed them (thesis §4.2.2), or
    /// give up after 10 s (a vote request from them is then answered with `Gone`).
    removing: Vec<(NodeId, u64)>,
    /// Created by `Init` at this time and not yet initialized by a snapshot (orphan GC).
    uninitialized_since: Option<u64>,
    /// Follower: quiesced by the leader on `leader_node`.
    quiescent: Option<NodeId>,
    /// Leader: this range is quiescent.
    leader_quiet: bool,
    /// Last time a leader of the current term was heard from (pre-vote check, thesis §9.6).
    leader_contact: u64,
    reads: Vec<PendingRead>,
}

/// Counters for tests and metrics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NodeStats {
    pub batches_sent: u64,
    pub messages_sent: u64,
    pub elections: u64,
    pub quiesced: u64,
    pub splits: u64,
    pub replicas_added: u64,
    pub replicas_removed: u64,
}

/// The multi-Raft driver of one node (see the module docs).
pub struct NodeDriver {
    id: NodeId,
    engine: Arc<StorageEngine<DocumentRecord>>,
    cfg: NodeConfig,
    rng: SweepRng,
    now: u64,
    replicas: BTreeMap<RangeId, ReplicaState>,
    outbox: BTreeMap<NodeId, Vec<(RangeId, RaftMsg)>>,
    /// Last time each peer node was heard from (any batch).
    heard: BTreeMap<NodeId, u64>,
    /// Last incarnation seen from each peer node.
    incarnations: BTreeMap<NodeId, u64>,
    /// Last time a batch went to each peer node.
    sent: BTreeMap<NodeId, u64>,
    peers: BTreeSet<NodeId>,
    next_read: u64,
    ready_reads: Vec<ReadToken>,
    stats: NodeStats,
    clock: Timestamp,
}

impl core::fmt::Debug for NodeDriver {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("NodeDriver").field("id", &self.id).field("ranges", &self.replicas.len()).finish()
    }
}

fn voters(raft: &Replica) -> Vec<NodeId> {
    raft.membership().voters().collect()
}

impl NodeDriver {
    pub fn new(id: NodeId, engine: Arc<StorageEngine<DocumentRecord>>, cfg: NodeConfig) -> Self {
        let rng = SweepRng::new(cfg.seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ u64::from(id) | 1);
        Self {
            id,
            engine,
            cfg,
            rng,
            now: 0,
            replicas: BTreeMap::new(),
            outbox: BTreeMap::new(),
            heard: BTreeMap::new(),
            incarnations: BTreeMap::new(),
            sent: BTreeMap::new(),
            peers: BTreeSet::new(),
            next_read: 0,
            ready_reads: Vec::new(),
            stats: NodeStats::default(),
            clock: Timestamp::ZERO,
        }
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn engine(&self) -> &Arc<StorageEngine<DocumentRecord>> {
        &self.engine
    }

    pub fn stats(&self) -> NodeStats {
        self.stats
    }

    /// Sets the timestamp stamped on outgoing batches.
    pub fn set_clock(&mut self, ts: Timestamp) {
        self.clock = ts;
    }

    /// Opens (or creates) range `range` over `span`, replicated by `members` (all voters).
    pub fn open_range(&mut self, range: RangeId, span: KeySpan, members: &[NodeId]) -> Result<Arc<Replica>> {
        let raft = Arc::new(RaftNode::open_group(self.id, Arc::clone(&self.engine), Some(range), span)?);
        if raft.membership().is_empty() {
            raft.initialize_membership(Membership::voters_from_ids(members.iter().copied())?)?;
        }
        self.add_replica(range, Arc::clone(&raft));
        Ok(raft)
    }

    /// Reopens every range persisted on this node (after a restart).
    pub fn reopen_ranges(&mut self) -> Result<Vec<RangeId>> {
        let ids = RaftNode::persisted_groups(&self.engine)?;
        for &r in &ids {
            let raft = Arc::new(RaftNode::open_group(self.id, Arc::clone(&self.engine), Some(r), KeySpan::all())?);
            self.add_replica(r, raft);
        }
        Ok(ids)
    }

    fn add_replica(&mut self, range: RangeId, raft: Arc<Replica>) {
        for v in voters(&raft) {
            if v != self.id {
                self.peers.insert(v);
            }
        }
        let deadline = self.now + self.election_timeout();
        self.replicas.insert(
            range,
            ReplicaState {
                raft,
                election_deadline: deadline,
                campaign: None,
                lead_term: 0,
                progress: BTreeMap::new(),
                hb_seq: 0,
                acks: BTreeMap::new(),
                last_heartbeat: 0,
                idle_beats: 0,
                removing: Vec::new(),
                uninitialized_since: None,
                quiescent: None,
                leader_quiet: false,
                leader_contact: 0,
                reads: Vec::new(),
            },
        );
    }

    /// Stops hosting `range` (its replica moved away).
    pub fn remove_range(&mut self, range: RangeId) -> Option<Arc<Replica>> {
        self.replicas.remove(&range).map(|r| r.raft)
    }

    pub fn ranges(&self) -> Vec<RangeId> {
        self.replicas.keys().copied().collect()
    }

    pub fn replica(&self, range: RangeId) -> Option<&Arc<Replica>> {
        self.replicas.get(&range).map(|r| &r.raft)
    }

    pub fn is_leader(&self, range: RangeId) -> bool {
        self.replicas.get(&range).is_some_and(|r| r.raft.is_leader() && r.lead_term == r.raft.current_term())
    }

    pub fn leader_of(&self, range: RangeId) -> Option<NodeId> {
        self.replicas.get(&range).and_then(|r| r.raft.leader_id())
    }

    /// Driver-side state of `range`, for failure reports.
    pub fn describe(&self, range: RangeId) -> String {
        self.replicas.get(&range).map_or_else(String::new, |r| {
            format!(
                "quiescent_on={:?} leader_quiet={} lead_term={} deadline_in={} contact_ago={} campaign={:?}",
                r.quiescent,
                r.leader_quiet,
                r.lead_term,
                r.election_deadline as i64 - self.now as i64,
                self.now.saturating_sub(r.leader_contact),
                r.campaign
            )
        })
    }

    /// Ranges whose leader currently sends nothing (quiescent), for tests and metrics.
    pub fn quiescent_ranges(&self) -> usize {
        self.replicas.values().filter(|r| r.leader_quiet || r.quiescent.is_some()).count()
    }

    fn election_timeout(&mut self) -> u64 {
        let span = self.cfg.election_max_us.saturating_sub(self.cfg.election_min_us).max(1);
        self.cfg.election_min_us + self.rng.below(span as usize) as u64
    }

    fn quorum(n: usize) -> usize {
        n / 2 + 1
    }

    fn push(&mut self, to: NodeId, range: RangeId, msg: RaftMsg) {
        if to != self.id {
            self.outbox.entry(to).or_default().push((range, msg));
        }
    }

    /// Takes the queued traffic: one batch per destination node. Peers without traffic get an
    /// empty batch once per heartbeat interval (node liveness for quiescent ranges).
    pub fn drain_outbox(&mut self) -> Vec<(NodeId, Batch)> {
        let now = self.now;
        let hb = self.cfg.heartbeat_us;
        let peers: Vec<NodeId> = self.peers.iter().copied().collect();
        for p in peers {
            if !self.outbox.contains_key(&p) && now.saturating_sub(self.sent.get(&p).copied().unwrap_or(0)) >= hb {
                self.outbox.insert(p, Vec::new());
            }
        }
        let out: Vec<(NodeId, Batch)> = std::mem::take(&mut self.outbox)
            .into_iter()
            .map(|(to, msgs)| (to, Batch { from: self.id, incarnation: self.cfg.incarnation, ts: self.clock, msgs }))
            .collect();
        for (to, b) in &out {
            self.sent.insert(*to, now);
            self.stats.batches_sent += 1;
            self.stats.messages_sent += b.msgs.len() as u64;
        }
        out
    }

    /// Advances time: election timers, heartbeats, quiescence, wake-ups of quiescent ranges
    /// whose leader node went silent, and read completion.
    pub fn tick(&mut self, now: u64) -> Result<()> {
        self.now = self.now.max(now);
        let ranges: Vec<RangeId> = self.replicas.keys().copied().collect();
        for range in ranges {
            self.tick_range(range)?;
        }
        self.after_progress()
    }

    fn tick_range(&mut self, range: RangeId) -> Result<()> {
        let now = self.now;
        // An Init-created replica that never got its snapshot is an orphan (the move it was
        // for was lost): remove it; a live move re-creates it.
        if let Some(r) = self.replicas.get(&range) {
            if r.uninitialized_since.is_some_and(|t| now.saturating_sub(t) > 10_000_000) && r.raft.snapshot_index() == 0 && r.raft.last_log_index() == 0 {
                self.destroy_replica(range)?;
                return Ok(());
            }
        }
        let hb = self.cfg.heartbeat_us;
        let quiesce_after = self.cfg.quiesce_after;
        let Some(r) = self.replicas.get_mut(&range) else { return Ok(()) };
        let raft = Arc::clone(&r.raft);
        if raft.is_leader() && r.lead_term == raft.current_term() {
            if r.leader_quiet {
                return Ok(());
            }
            if now.saturating_sub(r.last_heartbeat) >= hb {
                r.last_heartbeat = now;
                let caught_up = r.progress.values().all(|p| p.matched >= raft.last_log_index()) && raft.commit_index() == raft.last_log_index();
                if caught_up && r.reads.is_empty() {
                    r.idle_beats += 1;
                } else {
                    r.idle_beats = 0;
                }
                if r.idle_beats > quiesce_after && !r.progress.is_empty() && r.removing.is_empty() {
                    r.leader_quiet = true;
                    self.stats.quiesced += 1;
                    let (term, commit) = (raft.current_term(), raft.commit_index());
                    for p in self.peer_ids(range) {
                        self.push(p, range, RaftMsg::Quiesce { term, commit });
                    }
                } else {
                    self.broadcast(range);
                }
            }
            return Ok(());
        }
        // Follower or candidate.
        if let Some(leader_node) = r.quiescent {
            let heard = self.heard.get(&leader_node).copied().unwrap_or(0);
            if now.saturating_sub(heard) < self.cfg.election_min_us {
                return Ok(());
            }
            // The leader's node went silent: wake up and run the election timer again.
            r.quiescent = None;
            r.election_deadline = now;
        }
        let r = self.replicas.get_mut(&range).ok_or_else(|| StorageError::invalid_key("range vanished"))?;
        if now >= r.election_deadline {
            let m = raft.membership();
            if m.is_voter(self.id) {
                self.campaign(range)?;
            } else if !m.is_empty() && !m.contains(self.id) {
                // Removed, but not sure the removal committed: ask the members (they answer
                // `Gone` once it has).
                self.reset_election(range);
                let req = raft.pre_vote_request();
                let me = self.id;
                let targets: Vec<NodeId> = m.members().iter().map(|x| x.id).filter(|&p| p != me).collect();
                for p in targets {
                    self.push(p, range, RaftMsg::Vote(req));
                }
            }
        }
        Ok(())
    }

    fn peer_ids(&self, range: RangeId) -> Vec<NodeId> {
        self.replicas.get(&range).map_or_else(Vec::new, |r| {
            r.raft.membership().members().iter().map(|m| m.id).chain(r.removing.iter().map(|&(n, _)| n)).filter(|&v| v != self.id).collect::<BTreeSet<_>>().into_iter().collect()
        })
    }

    fn reset_election(&mut self, range: RangeId) {
        let t = self.election_timeout();
        if let Some(r) = self.replicas.get_mut(&range) {
            r.election_deadline = self.now + t;
        }
    }

    fn wake(&mut self, range: RangeId) {
        if let Some(r) = self.replicas.get_mut(&range) {
            r.leader_quiet = false;
            r.idle_beats = 0;
            r.quiescent = None;
        }
    }

    fn campaign(&mut self, range: RangeId) -> Result<()> {
        self.reset_election(range);
        let Some(r) = self.replicas.get_mut(&range) else { return Ok(()) };
        let raft = Arc::clone(&r.raft);
        if raft.is_leader() {
            return Ok(());
        }
        let req = raft.pre_vote_request();
        let me = self.id;
        r.campaign = Some(Campaign::Pre { term: req.term, votes: BTreeSet::from([me]) });
        let peers = voters(&raft).into_iter().filter(|&v| v != me).collect::<Vec<_>>();
        if peers.is_empty() {
            // A single-voter range wins at once.
            let real = raft.start_election()?;
            if raft.become_leader(real.term) {
                self.on_new_leader(range, real.term);
            }
            return Ok(());
        }
        for p in peers {
            self.push(p, range, RaftMsg::Vote(req));
        }
        Ok(())
    }

    fn on_new_leader(&mut self, range: RangeId, term: u64) {
        self.stats.elections += 1;
        let me = self.id;
        let peers = self.peer_ids(range);
        let Some(r) = self.replicas.get_mut(&range) else { return };
        let next = r.raft.last_log_index() + 1;
        r.lead_term = term;
        let learners: Vec<NodeId> = r.raft.membership().members().iter().filter(|m| !m.voter).map(|m| m.id).collect();
        let min = next - 1;
        // A learner's state is unknown to a new leader: start it from a fresh snapshot.
        r.progress = peers
            .iter()
            .filter(|&&p| p != me)
            .map(|&p| (p, Progress { next, matched: 0, snapshot_offset: None, force_snapshot: learners.contains(&p), snapshot_min: if learners.contains(&p) { min } else { 0 } }))
            .collect();
        r.acks.clear();
        r.leader_quiet = false;
        r.idle_beats = 0;
        r.quiescent = None;
        r.last_heartbeat = self.now;
        self.broadcast(range);
    }

    fn broadcast(&mut self, range: RangeId) {
        for p in self.peer_ids(range) {
            self.replicate(range, p);
        }
    }

    fn replicate(&mut self, range: RangeId, peer: NodeId) {
        let Some(r) = self.replicas.get_mut(&range) else { return };
        let raft = Arc::clone(&r.raft);
        if !raft.is_leader() || raft.current_term() != r.lead_term {
            return;
        }
        let pr = r.progress.get(&peer).copied().unwrap_or(Progress { next: raft.last_log_index() + 1, matched: 0, snapshot_offset: None, force_snapshot: false, snapshot_min: 0 });
        if pr.force_snapshot && raft.last_applied() < pr.snapshot_min {
            return; // the new replica's snapshot must include its own addition: wait for it
        }
        if pr.force_snapshot || raft.needs_snapshot(pr.next) {
            let offset = pr.snapshot_offset.unwrap_or(0);
            if let Ok(req) = raft.build_install_snapshot_from(offset, self.cfg.snapshot_chunk, pr.snapshot_min) {
                r.progress.entry(peer).or_insert(pr).snapshot_offset = Some(offset);
                if pr.force_snapshot && offset == 0 {
                    // A new replica may not exist yet (lost Init): (re)create it first.
                    let members = raft.membership().members().iter().map(|m| (m.id, m.voter)).collect();
                    self.push(peer, range, RaftMsg::Init { span: raft.span(), members });
                }
                self.push(peer, range, RaftMsg::Snap(req));
            }
            return;
        }
        let hb = r.hb_seq;
        if let Ok(req) = raft.build_append_entries_bounded(pr.next, self.cfg.max_entries_per_append, self.cfg.max_bytes_per_append) {
            self.push(peer, range, RaftMsg::Append { req, hb });
        }
    }

    fn advance_commit(&mut self, range: RangeId) -> Result<()> {
        let Some(r) = self.replicas.get(&range) else { return Ok(()) };
        let raft = Arc::clone(&r.raft);
        if !raft.is_leader() {
            return Ok(());
        }
        let vs = voters(&raft);
        let mut matches: Vec<u64> = vs.iter().filter(|&&v| v != self.id).map(|v| r.progress.get(v).map_or(0, |p| p.matched)).collect();
        if vs.contains(&self.id) {
            matches.push(raft.last_log_index());
        }
        if matches.is_empty() {
            return Ok(());
        }
        matches.sort_unstable_by(|a, b| b.cmp(a));
        let q = matches[Self::quorum(vs.len()).min(matches.len()) - 1];
        match raft.advance_leader_commit(q) {
            Ok(_) | Err(StorageError::InvalidKey { .. }) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Handles one batch from another node.
    pub fn step(&mut self, batch: Batch, now: u64) -> Result<()> {
        self.now = self.now.max(now);
        self.heard.insert(batch.from, self.now);
        if self.incarnations.insert(batch.from, batch.incarnation).is_some_and(|old| old != batch.incarnation) {
            // The node restarted: it leads nothing now, so ranges quiescent on it wake up.
            for r in self.replicas.values_mut() {
                if r.quiescent == Some(batch.from) {
                    r.quiescent = None;
                    r.leader_contact = 0;
                }
            }
        }
        for (range, msg) in batch.msgs {
            if let RaftMsg::Init { span, members } = &msg {
                if !self.replicas.contains_key(&range) {
                    self.create_learner(range, span.clone(), members)?;
                }
                continue;
            }
            if self.replicas.contains_key(&range) {
                self.step_one(batch.from, range, msg)?;
            }
        }
        self.after_progress()
    }

    fn step_one(&mut self, from: NodeId, range: RangeId, msg: RaftMsg) -> Result<()> {
        let raft = Arc::clone(&self.replicas[&range].raft);
        match msg {
            RaftMsg::Vote(req) => {
                let (m, committed) = raft.membership_state();
                if committed && !m.is_empty() && !m.contains(from) {
                    // A removed replica that never learned it (any replica whose committed
                    // configuration excludes it knows): tell it.
                    self.push(from, range, RaftMsg::Gone { config_index: raft.config_index() });
                    return Ok(());
                }
                if self.replicas[&range].quiescent == Some(from) {
                    // The node we were quiescent on campaigns: it no longer leads.
                    if let Some(r) = self.replicas.get_mut(&range) {
                        r.quiescent = None;
                        r.leader_contact = 0;
                    }
                }
                let r = &self.replicas[&range];
                let leading = raft.is_leader() && r.lead_term == raft.current_term();
                // Pre-vote (thesis §9.6): refused by a live leader (which also resumes
                // heartbeats: the candidate is missing them) and by a follower that heard from
                // a leader within the minimum election timeout or follows a quiescent one.
                let recently_led = r.quiescent.is_some() || self.now.saturating_sub(r.leader_contact) < self.cfg.election_min_us && r.leader_contact > 0;
                if req.pre_vote && (leading || recently_led) {
                    if leading {
                        self.wake(range);
                        self.replicate(range, from);
                    }
                    let resp = RequestVoteResp { term: raft.current_term(), vote_granted: false };
                    self.push(from, range, RaftMsg::VoteResp { req, resp });
                    return Ok(());
                }
                if !req.pre_vote {
                    self.wake(range);
                }
                let resp = raft.handle_request_vote(req);
                if resp.vote_granted && !req.pre_vote {
                    self.reset_election(range);
                }
                self.push(from, range, RaftMsg::VoteResp { req, resp });
            }
            RaftMsg::VoteResp { req, resp } => self.on_vote_resp(range, from, req, resp)?,
            RaftMsg::Append { req, hb } => {
                let req_term = req.term;
                let last_sent = req.prev_log_index + req.entries.len() as u64;
                let resp = raft.handle_append_entries(req)?;
                if resp.term == req_term {
                    let now = self.now;
                    if let Some(r) = self.replicas.get_mut(&range) {
                        r.campaign = None;
                        r.quiescent = None;
                        r.leader_contact = now;
                    }
                    self.reset_election(range);
                }
                self.push(from, range, RaftMsg::AppendResp { resp, req_term, last_sent, hb });
            }
            RaftMsg::AppendResp { resp, req_term, last_sent, hb } => {
                if resp.term > raft.current_term() {
                    raft.observe_term(resp.term)?;
                    return Ok(());
                }
                let Some(r) = self.replicas.get_mut(&range) else { return Ok(()) };
                if !raft.is_leader() || raft.current_term() != req_term || r.lead_term != req_term {
                    return Ok(());
                }
                let a = r.acks.entry(from).or_insert(0);
                *a = (*a).max(hb);
                let Some(p) = r.progress.get_mut(&from) else { return Ok(()) };
                if resp.success {
                    p.matched = p.matched.max(resp.match_index.min(last_sent));
                    p.next = p.next.max(p.matched + 1);
                    let behind = p.matched < raft.last_log_index();
                    self.advance_commit(range)?;
                    if behind {
                        self.replicate(range, from);
                    }
                } else {
                    p.next = (resp.match_index + 1).min(p.next.saturating_sub(1)).max(p.matched + 1).max(1);
                    self.replicate(range, from);
                }
            }
            RaftMsg::Snap(req) => {
                let req_term = req.term;
                let last_included = req.last_included_index;
                let resp = raft.handle_install_snapshot(req)?;
                if resp.installed {
                    if let Some(r) = self.replicas.get_mut(&range) {
                        r.uninitialized_since = None;
                    }
                }
                if resp.term == req_term {
                    self.reset_election(range);
                }
                self.push(from, range, RaftMsg::SnapResp { resp, req_term, last_included });
            }
            RaftMsg::SnapResp { resp, req_term, last_included } => {
                if resp.term > raft.current_term() {
                    raft.observe_term(resp.term)?;
                    return Ok(());
                }
                if !raft.is_leader() || raft.current_term() != req_term {
                    return Ok(());
                }
                let chunk = self.cfg.snapshot_chunk;
                let Some(r) = self.replicas.get_mut(&range) else { return Ok(()) };
                let Some(p) = r.progress.get_mut(&from) else { return Ok(()) };
                if resp.installed {
                    p.matched = p.matched.max(last_included);
                    p.next = p.matched + 1;
                    p.snapshot_offset = None;
                    p.force_snapshot = false;
                    self.advance_commit(range)?;
                    self.replicate(range, from);
                } else if p.snapshot_offset.is_some() {
                    p.snapshot_offset = Some(resp.next_offset);
                    match raft.build_install_snapshot_from(resp.next_offset, chunk, p.snapshot_min) {
                        Ok(req) => self.push(from, range, RaftMsg::Snap(req)),
                        Err(_) => p.snapshot_offset = None,
                    }
                }
            }
            RaftMsg::Init { .. } => {}
            RaftMsg::Gone { config_index } => {
                if raft.config_index() <= config_index && !raft.is_leader() {
                    self.destroy_replica(range)?;
                }
            }
            RaftMsg::TimeoutNow { term } => {
                if term == raft.current_term() && !raft.is_leader() && raft.membership().is_voter(self.id) {
                    // Real election straight away (the leader stepped aside for us).
                    self.wake(range);
                    let real = raft.start_election()?;
                    let me = self.id;
                    if let Some(r) = self.replicas.get_mut(&range) {
                        r.campaign = Some(Campaign::Real { term: real.term, votes: BTreeSet::from([me]) });
                    }
                    self.reset_election(range);
                    for p in voters(&raft).into_iter().filter(|&v| v != me) {
                        self.push(p, range, RaftMsg::Vote(real));
                    }
                }
            }
            RaftMsg::Quiesce { term, commit } => {
                // Quiesce only when caught up with that leader's term and commit.
                if term == raft.current_term() && raft.leader_id() == Some(from) && raft.commit_index() >= commit && raft.last_applied() >= commit {
                    let now = self.now;
                    if let Some(r) = self.replicas.get_mut(&range) {
                        r.quiescent = Some(from);
                        r.campaign = None;
                        r.leader_contact = now;
                    }
                }
            }
        }
        Ok(())
    }

    fn on_vote_resp(&mut self, range: RangeId, from: NodeId, req: RequestVoteReq, resp: RequestVoteResp) -> Result<()> {
        let raft = Arc::clone(&self.replicas[&range].raft);
        if !req.pre_vote && resp.term > raft.current_term() {
            raft.observe_term(resp.term)?;
            if let Some(r) = self.replicas.get_mut(&range) {
                r.campaign = None;
            }
            return Ok(());
        }
        let me = self.id;
        let quorum = Self::quorum(voters(&raft).len());
        let Some(r) = self.replicas.get_mut(&range) else { return Ok(()) };
        match &mut r.campaign {
            Some(Campaign::Pre { term, votes }) if req.pre_vote && req.term == *term => {
                if resp.vote_granted {
                    votes.insert(from);
                }
                if votes.len() >= quorum {
                    let real = raft.start_election()?;
                    r.campaign = Some(Campaign::Real { term: real.term, votes: BTreeSet::from([me]) });
                    self.reset_election(range);
                    for p in voters(&raft).into_iter().filter(|&v| v != me) {
                        self.push(p, range, RaftMsg::Vote(real));
                    }
                }
            }
            Some(Campaign::Real { term, votes }) if !req.pre_vote && req.term == *term && resp.term == *term => {
                if resp.vote_granted {
                    votes.insert(from);
                }
                if votes.len() >= quorum {
                    let term = *term;
                    r.campaign = None;
                    if raft.become_leader(term) {
                        self.on_new_leader(range, term);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Proposes a write batch on the leaseholder of `range`; returns its log index (wait for it
    /// with the replica's `wait_applied` once [`Self::applied`] reaches it).
    pub fn propose(&mut self, range: RangeId, ops: &[WriteOp<DocumentRecord>], now_ns: u64) -> Result<u64> {
        let r = self.replicas.get(&range).ok_or_else(|| StorageError::invalid_key("range not hosted on this node"))?;
        if !(r.raft.is_leader() && r.lead_term == r.raft.current_term()) {
            return Err(StorageError::NotLeader { leader: r.raft.leader_id().map(u64::from) });
        }
        let span = r.raft.span();
        if ops.iter().any(|o| !span.contains_id(o.id)) {
            return Err(StorageError::invalid_key("key outside this range: re-route"));
        }
        let index = r.raft.propose_ops_at(ops, now_ns)?;
        self.wake(range);
        self.broadcast(range);
        self.advance_commit(range)?;
        Ok(index)
    }

    /// Proposes splitting `range` at `at` into a new range `right` (same replicas).
    pub fn propose_split(&mut self, range: RangeId, at: Vec<u8>, right: RangeId) -> Result<u64> {
        let r = self.replicas.get(&range).ok_or_else(|| StorageError::invalid_key("range not hosted on this node"))?;
        if !r.raft.is_leader() {
            return Err(StorageError::NotLeader { leader: r.raft.leader_id().map(u64::from) });
        }
        if r.raft.span().split_at(&at).is_none() {
            return Err(StorageError::invalid_key("split key outside the range"));
        }
        let index = r.raft.propose(crate::raft::RaftCommand::Split { at, right })?;
        self.wake(range);
        self.broadcast(range);
        self.advance_commit(range)?;
        Ok(index)
    }

    /// Removes a replica, deleting its records except those another local replica holds.
    fn destroy_replica(&mut self, range: RangeId) -> Result<()> {
        if let Some(r) = self.replicas.remove(&range) {
            let others: Vec<crate::raft::KeySpan> = self.replicas.values().map(|o| o.raft.span()).collect();
            r.raft.destroy_keeping(|id| others.iter().any(|sp| sp.contains_id(id)))?;
            self.stats.replicas_removed += 1;
        }
        Ok(())
    }

    fn create_learner(&mut self, range: RangeId, span: crate::raft::KeySpan, members: &[(NodeId, bool)]) -> Result<()> {
        let raft = Arc::new(RaftNode::open_group(self.id, Arc::clone(&self.engine), Some(range), span)?);
        if raft.membership().is_empty() {
            let m = Membership::new(members.iter().map(|&(id, voter)| crate::raft::Member { id, addr: String::new(), voter }).collect())?;
            raft.initialize_membership(m)?;
        }
        self.stats.replicas_added += 1;
        self.add_replica(range, raft);
        let now = self.now;
        if let Some(r) = self.replicas.get_mut(&range) {
            r.uninitialized_since = Some(now);
        }
        Ok(())
    }

    /// The configuration of `range` as `(node, voter)` pairs.
    pub fn members_of(&self, range: RangeId) -> Vec<(NodeId, bool)> {
        self.replicas.get(&range).map_or_else(Vec::new, |r| r.raft.membership().members().iter().map(|m| (m.id, m.voter)).collect())
    }

    fn propose_config(&mut self, range: RangeId, next: Vec<(NodeId, bool)>) -> Result<u64> {
        let r = self.replicas.get(&range).ok_or_else(|| StorageError::invalid_key("range not hosted on this node"))?;
        if !(r.raft.is_leader() && r.lead_term == r.raft.current_term()) {
            return Err(StorageError::NotLeader { leader: r.raft.leader_id().map(u64::from) });
        }
        let m = Membership::new(next.iter().map(|&(id, voter)| crate::raft::Member { id, addr: String::new(), voter }).collect())?;
        let index = r.raft.propose_membership(m)?;
        for &(id, _) in &next {
            if id != self.id {
                self.peers.insert(id);
                if let Some(r) = self.replicas.get_mut(&range) {
                    let next_index = r.raft.last_log_index();
                    r.progress.entry(id).or_insert(Progress { next: next_index, matched: 0, snapshot_offset: None, force_snapshot: false, snapshot_min: 0 });
                }
            }
        }
        self.wake(range);
        self.broadcast(range);
        self.advance_commit(range)?;
        Ok(index)
    }

    /// Adds `node` to `range` as a learner (it is created there and caught up by snapshot).
    pub fn add_learner(&mut self, range: RangeId, node: NodeId) -> Result<u64> {
        let mut next = self.members_of(range);
        if next.iter().any(|&(id, _)| id == node) {
            return Err(StorageError::invalid_value("already a member"));
        }
        next.push((node, false));
        let index = self.propose_config(range, next.clone())?;
        if let Some(r) = self.replicas.get_mut(&range) {
            let p = r.progress.entry(node).or_default();
            p.force_snapshot = true;
            p.snapshot_offset = None;
            p.snapshot_min = index;
        }
        let span = self.replicas.get(&range).map(|r| r.raft.span()).unwrap_or_default();
        self.push(node, range, RaftMsg::Init { span, members: next });
        Ok(index)
    }

    /// Makes learner `node` a voter (once it has caught up).
    pub fn promote(&mut self, range: RangeId, node: NodeId) -> Result<u64> {
        let next: Vec<(NodeId, bool)> = self.members_of(range).into_iter().map(|(id, v)| (id, v || id == node)).collect();
        self.propose_config(range, next)
    }

    /// Removes `node` from `range` (it deletes its replica once it learns the change committed).
    pub fn remove_member(&mut self, range: RangeId, node: NodeId) -> Result<u64> {
        let next: Vec<(NodeId, bool)> = self.members_of(range).into_iter().filter(|&(id, _)| id != node).collect();
        let index = self.propose_config(range, next)?;
        let now = self.now;
        if let Some(r) = self.replicas.get_mut(&range) {
            r.removing.push((node, now));
        }
        Ok(index)
    }

    /// `true` once `node`'s replica of `range` has every entry the leader has.
    pub fn caught_up(&self, range: RangeId, node: NodeId) -> bool {
        self.replicas.get(&range).is_some_and(|r| r.progress.get(&node).is_some_and(|p| p.matched >= r.raft.last_log_index()))
    }

    /// Hands leadership of `range` to `to` (a caught-up voter).
    pub fn transfer_lease(&mut self, range: RangeId, to: NodeId) -> Result<()> {
        if !self.is_leader(range) {
            return Err(StorageError::NotLeader { leader: self.leader_of(range).map(u64::from) });
        }
        if !self.caught_up(range, to) {
            return Err(StorageError::invalid_value("transfer target has not caught up"));
        }
        let term = self.replicas.get(&range).map_or(0, |r| r.raft.current_term());
        self.push(to, range, RaftMsg::TimeoutNow { term });
        Ok(())
    }

    /// Starts a linearizable read on the leaseholder of `range`.
    pub fn read_index(&mut self, range: RangeId) -> Result<ReadToken> {
        let r = self.replicas.get_mut(&range).ok_or_else(|| StorageError::invalid_key("range not hosted on this node"))?;
        let raft = Arc::clone(&r.raft);
        if !(raft.is_leader() && r.lead_term == raft.current_term()) {
            return Err(StorageError::NotLeader { leader: raft.leader_id().map(u64::from) });
        }
        let view = raft.log_view();
        if view.term_at(view.commit_index) != Some(view.current_term) {
            return Err(StorageError::NotLeader { leader: Some(u64::from(self.id)) });
        }
        r.hb_seq += 1;
        self.next_read += 1;
        let token = ReadToken { range, seq: self.next_read };
        r.reads.push(PendingRead { seq: token.seq, read_index: view.commit_index, hb_needed: r.hb_seq, term: view.current_term });
        self.wake(range);
        self.broadcast(range);
        Ok(token)
    }

    /// Reads that became safe to serve since the last call.
    pub fn take_ready_reads(&mut self) -> Vec<ReadToken> {
        std::mem::take(&mut self.ready_reads)
    }

    /// Applied index of `range` on this node.
    pub fn applied(&self, range: RangeId) -> u64 {
        self.replicas.get(&range).map_or(0, |r| r.raft.last_applied())
    }

    /// Finishes reads and starts the right-hand groups of applied splits.
    fn after_progress(&mut self) -> Result<()> {
        let ranges: Vec<RangeId> = self.replicas.keys().copied().collect();
        let mut splits = Vec::new();
        for range in ranges {
            let Some(r) = self.replicas.get_mut(&range) else { continue };
            splits.extend(r.raft.take_splits());
            if r.reads.is_empty() {
                continue;
            }
            let raft = Arc::clone(&r.raft);
            let leading = raft.is_leader() && r.lead_term == raft.current_term();
            let vs = voters(&raft);
            let quorum = Self::quorum(vs.len());
            let applied = raft.last_applied();
            let me = self.id;
            let mut keep = Vec::new();
            for read in std::mem::take(&mut r.reads) {
                if !leading || read.term != raft.current_term() {
                    continue; // dropped: the caller retries elsewhere
                }
                let confirmed = usize::from(vs.contains(&me)) + vs.iter().filter(|&&v| v != me && r.acks.get(&v).is_some_and(|&a| a >= read.hb_needed)).count();
                if confirmed >= quorum && applied >= read.read_index {
                    self.ready_reads.push(ReadToken { range, seq: read.seq });
                } else {
                    keep.push(read);
                }
            }
            r.reads = keep;
        }
        // Leaders: once a removal commits, tell the removed member (one last append carries
        // the commit) and stop replicating to it.
        let leading: Vec<RangeId> = self.replicas.iter().filter(|(_, r)| !r.removing.is_empty() && r.raft.is_leader()).map(|(&id, _)| id).collect();
        let now = self.now;
        for range in leading {
            let Some(r) = self.replicas.get_mut(&range) else { continue };
            if !r.raft.membership_state().1 {
                continue;
            }
            let config_index = r.raft.config_index();
            let mut done = Vec::new();
            let mut pending = Vec::new();
            for &(n, since) in &r.removing {
                let acked = r.progress.get(&n).is_some_and(|p| p.matched >= config_index);
                if acked || now.saturating_sub(since) > 10_000_000 {
                    done.push(n);
                } else {
                    pending.push(n);
                }
            }
            r.removing.retain(|(n, _)| !done.contains(n));
            for n in &done {
                r.progress.remove(n);
            }
            for n in pending {
                self.replicate(range, n);
            }
        }
        // Replicas whose committed configuration no longer includes this node go away.
        let gone: Vec<RangeId> = self
            .replicas
            .iter()
            .filter(|(_, r)| {
                let (m, committed) = r.raft.membership_state();
                committed && !m.is_empty() && !m.contains(self.id)
            })
            .map(|(&id, _)| id)
            .collect();
        for range in gone {
            self.destroy_replica(range)?;
        }
        for ev in splits {
            if self.replicas.contains_key(&ev.right) {
                continue;
            }
            self.stats.splits += 1;
            let raft = Arc::new(RaftNode::open_group(self.id, Arc::clone(&self.engine), Some(ev.right), ev.right_span.clone())?);
            if raft.membership().is_empty() && !ev.members.is_empty() {
                raft.initialize_membership(ev.members.clone())?;
            }
            self.add_replica(ev.right, raft);
        }
        Ok(())
    }
}
