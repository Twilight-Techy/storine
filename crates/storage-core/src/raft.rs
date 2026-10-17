//! Raft Consensus State Machine, Quorum Log Replication & Follower Read Scaling
//! (`TASK-046`, `F-DIST-02`, `F-DIST-04`).
//!
//! Provides:
//! - Deterministic Raft state machine (`Follower`, `Candidate`, `Leader`) with monotonic `term` and `index`.
//! - `RequestVote` and `AppendEntries` RPC handlers enforcing the Raft Log Matching Property and
//!   Leader Completeness Property.
//! - Quorum commit advancement ($\lfloor N/2 \rfloor + 1$) applying zero-copy `AlignedBytes` frames to
//!   the underlying `StorageEngine<T>`.
//! - Read-Index / Follower Read scaling (`read_follower_at_commit_index`) so read-heavy workloads
//!   scale horizontally across all replicas (`F-DIST-04`).

use std::sync::{Arc, Mutex};

use storage_codec::{AlignedBytes, RecordCodec, RecordView, ZeroCopyRecord};
use storage_index::Indexable;
use storage_types::{InlineId, RecordFlags, Result, StorageError, Version};

use crate::engine::StorageEngine;
use crate::sharding::NodeId;

/// Role of a node in a Raft replication group (`F-DIST-02`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

/// Replicated Raft log entry carrying a 64B-aligned zero-copy record frame (or deletion tombstone).
#[derive(Clone, Debug)]
pub struct RaftLogEntry {
    pub term: u64,
    pub index: u64,
    pub id: InlineId,
    pub version: Version,
    /// `Some(frame)` for `Upsert`, `None` for `Delete`.
    pub frame: Option<AlignedBytes>,
}

/// Raft `RequestVote` RPC request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestVoteReq {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

/// Raft `RequestVote` RPC response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestVoteResp {
    pub term: u64,
    pub vote_granted: bool,
}

/// Raft `AppendEntries` RPC request (heartbeat when `entries` is empty).
#[derive(Clone, Debug)]
pub struct AppendEntriesReq {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<RaftLogEntry>,
    pub leader_commit: u64,
}

/// Raft `AppendEntries` RPC response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendEntriesResp {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

#[derive(Debug)]
struct RaftState {
    role: RaftRole,
    current_term: u64,
    voted_for: Option<NodeId>,
    leader_id: Option<NodeId>,
    log: Vec<RaftLogEntry>,
    commit_index: u64,
    last_applied: u64,
}

impl RaftState {
    fn new() -> Self {
        Self {
            role: RaftRole::Follower,
            current_term: 0,
            voted_for: None,
            leader_id: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
        }
    }

    #[inline]
    fn last_log_index_and_term(&self) -> (u64, u64) {
        self.log
            .last()
            .map_or((0, 0), |e| (e.index, e.term))
    }

    #[inline]
    fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.log.get((index - 1) as usize).map(|e| e.term)
    }
}

/// Single Raft Consensus Replica wrapping a local `StorageEngine<T>` (`TASK-046`, `F-DIST-02`).
pub struct RaftNode<T: ZeroCopyRecord + Indexable> {
    node_id: NodeId,
    engine: Arc<StorageEngine<T>>,
    state: Mutex<RaftState>,
}

impl<T: ZeroCopyRecord + Indexable> core::fmt::Debug for RaftNode<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        f.debug_struct("RaftNode")
            .field("node_id", &self.node_id)
            .field("role", &st.role)
            .field("current_term", &st.current_term)
            .field("commit_index", &st.commit_index)
            .field("last_applied", &st.last_applied)
            .finish()
    }
}

impl<T: ZeroCopyRecord + Indexable> RaftNode<T> {
    pub fn new(node_id: NodeId, engine: Arc<StorageEngine<T>>) -> Self {
        Self {
            node_id,
            engine,
            state: Mutex::new(RaftState::new()),
        }
    }

    #[inline]
    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[inline]
    pub fn engine(&self) -> &Arc<StorageEngine<T>> {
        &self.engine
    }

    pub fn role(&self) -> RaftRole {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).role
    }

    pub fn current_term(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .current_term
    }

    pub fn commit_index(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .commit_index
    }

    pub fn last_applied(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .last_applied
    }

    /// Transitions this node to `Candidate`, increments `current_term`, votes for itself,
    /// and returns a `RequestVoteReq` to broadcast to peers.
    pub fn start_election(&self) -> RequestVoteReq {
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        st.current_term += 1;
        st.role = RaftRole::Candidate;
        st.voted_for = Some(self.node_id);
        st.leader_id = None;
        let (last_log_index, last_log_term) = st.last_log_index_and_term();
        RequestVoteReq {
            term: st.current_term,
            candidate_id: self.node_id,
            last_log_index,
            last_log_term,
        }
    }

    /// Processes a `RequestVoteReq` according to the Raft Election Safety and Log Up-To-Date rules.
    pub fn handle_request_vote(&self, req: RequestVoteReq) -> RequestVoteResp {
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if req.term > st.current_term {
            st.current_term = req.term;
            st.role = RaftRole::Follower;
            st.voted_for = None;
            st.leader_id = None;
        }
        if req.term < st.current_term {
            return RequestVoteResp {
                term: st.current_term,
                vote_granted: false,
            };
        }

        let (my_last_idx, my_last_term) = st.last_log_index_and_term();
        let candidate_up_to_date = req.last_log_term > my_last_term
            || (req.last_log_term == my_last_term && req.last_log_index >= my_last_idx);
        let can_vote = st.voted_for.is_none() || st.voted_for == Some(req.candidate_id);

        if can_vote && candidate_up_to_date {
            st.voted_for = Some(req.candidate_id);
            RequestVoteResp {
                term: st.current_term,
                vote_granted: true,
            }
        } else {
            RequestVoteResp {
                term: st.current_term,
                vote_granted: false,
            }
        }
    }

    /// Promotes a `Candidate` that has won a majority of votes in `term` to `Leader`.
    pub fn become_leader(&self, term: u64) -> bool {
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if st.current_term == term && st.role == RaftRole::Candidate {
            st.role = RaftRole::Leader;
            st.leader_id = Some(self.node_id);
            true
        } else {
            false
        }
    }

    /// Appends a new entry to the leader's local log (prior to replication).
    pub fn append_leader_entry(
        &self,
        id: InlineId,
        record: Option<&T>,
    ) -> Result<(RaftLogEntry, u64, u64)> {
        let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if st.role != RaftRole::Leader {
            return Err(StorageError::invalid_key("node is not Raft Leader"));
        }
        let (prev_idx, prev_term) = st.last_log_index_and_term();
        let next_idx = prev_idx + 1;
        let next_ver = Version(next_idx);
        let frame = record.map(|rec| {
            RecordCodec::encode_aligned(rec, next_ver, 0, RecordFlags::NONE, 0)
        });
        let entry = RaftLogEntry {
            term: st.current_term,
            index: next_idx,
            id,
            version: next_ver,
            frame,
        };
        st.log.push(entry.clone());
        Ok((entry, prev_idx, prev_term))
    }

    /// Builds an `AppendEntriesReq` carrying all log entries from `from_index` (1-based) onward.
    pub fn build_append_entries_from(&self, from_index: u64) -> Result<AppendEntriesReq> {
        let st = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if st.role != RaftRole::Leader {
            return Err(StorageError::invalid_key("node is not Raft Leader"));
        }
        let prev_log_index = from_index.saturating_sub(1);
        let prev_log_term = st.term_at(prev_log_index).unwrap_or(0);
        let start_pos = prev_log_index as usize;
        let entries = if start_pos < st.log.len() {
            st.log[start_pos..].to_vec()
        } else {
            Vec::new()
        };
        Ok(AppendEntriesReq {
            term: st.current_term,
            leader_id: self.node_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: st.commit_index,
        })
    }

    /// Processes an `AppendEntriesReq` from the leader and applies newly committed entries to `StorageEngine<T>`.
    pub fn handle_append_entries(&self, req: AppendEntriesReq) -> Result<AppendEntriesResp> {
        let to_apply = {
            let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if req.term < st.current_term {
                return Ok(AppendEntriesResp {
                    term: st.current_term,
                    success: false,
                    match_index: st.commit_index,
                });
            }
            if req.term > st.current_term || st.role != RaftRole::Follower {
                st.current_term = req.term;
                st.role = RaftRole::Follower;
                st.voted_for = None;
            }
            st.leader_id = Some(req.leader_id);

            // Verify Raft Log Matching Property at `prev_log_index`
            let Some(local_prev_term) = st.term_at(req.prev_log_index) else {
                return Ok(AppendEntriesResp {
                    term: st.current_term,
                    success: false,
                    match_index: st.log.len() as u64,
                });
            };
            if local_prev_term != req.prev_log_term {
                st.log.truncate(req.prev_log_index.saturating_sub(1) as usize);
                return Ok(AppendEntriesResp {
                    term: st.current_term,
                    success: false,
                    match_index: st.log.len() as u64,
                });
            }

            // Append new entries, truncating any conflicting uncommitted suffix
            for entry in req.entries {
                let pos = (entry.index - 1) as usize;
                if pos < st.log.len() {
                    if st.log[pos].term != entry.term {
                        st.log.truncate(pos);
                        st.log.push(entry);
                    }
                } else if pos == st.log.len() {
                    st.log.push(entry);
                }
            }

            let match_index = st.log.len() as u64;
            if req.leader_commit > st.commit_index {
                st.commit_index = req.leader_commit.min(match_index);
            }

            let mut ready = Vec::new();
            while st.last_applied < st.commit_index {
                let next_apply = st.last_applied + 1;
                if let Some(e) = st.log.get((next_apply - 1) as usize) {
                    ready.push(e.clone());
                }
                st.last_applied = next_apply;
            }
            (st.current_term, match_index, ready)
        };

        for entry in to_apply.2 {
            self.apply_entry_to_engine(&entry)?;
        }

        Ok(AppendEntriesResp {
            term: to_apply.0,
            success: true,
            match_index: to_apply.1,
        })
    }

    /// Advances the leader's `commit_index` to `quorum_index` once a majority of nodes have replicated it,
    /// and applies newly committed entries to the leader's local `StorageEngine<T>`.
    pub fn advance_leader_commit(&self, quorum_index: u64) -> Result<u64> {
        let to_apply = {
            let mut st = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if st.role != RaftRole::Leader {
                return Err(StorageError::invalid_key("node is not Raft Leader"));
            }
            if quorum_index > st.commit_index && quorum_index <= st.log.len() as u64 {
                st.commit_index = quorum_index;
            }
            let mut ready = Vec::new();
            while st.last_applied < st.commit_index {
                let next_apply = st.last_applied + 1;
                if let Some(e) = st.log.get((next_apply - 1) as usize) {
                    ready.push(e.clone());
                }
                st.last_applied = next_apply;
            }
            (st.commit_index, ready)
        };

        for entry in to_apply.1 {
            self.apply_entry_to_engine(&entry)?;
        }
        Ok(to_apply.0)
    }

    /// Follower Read Scaling (`F-DIST-04`):
    /// Executes a zero-copy read closure `f(view)` on this replica ONLY if `last_applied >= min_commit_index`,
    /// guaranteeing causal/read-your-writes consistency across follower replicas!
    pub fn read_at_commit_index<R>(
        &self,
        id: InlineId,
        min_commit_index: u64,
        f: impl FnOnce(RecordView<'_, T>) -> R,
    ) -> Result<R> {
        let applied = self.last_applied();
        if applied < min_commit_index {
            return Err(StorageError::invalid_key(
                "follower replica has not yet caught up to min_commit_index",
            ));
        }
        self.engine.with_ref(id, f)
    }

    fn apply_entry_to_engine(&self, entry: &RaftLogEntry) -> Result<()> {
        match entry.frame {
            Some(ref frame) => {
                let (_hdr, owned) = RecordCodec::decode_owned::<T>(frame.as_slice())?;
                self.engine.put(entry.id, &owned)?;
            }
            None => {
                let _ = self.engine.delete(entry.id)?;
            }
        }
        Ok(())
    }
}

/// Multi-Node Raft Replication Group Coordinator (`TASK-046`, `F-DIST-02`, `F-DIST-04`).
///
/// Manages a quorum of `RaftNode<T>` replicas (e.g., 3 or 5 nodes), handles leader election,
/// replicates WAL/log batches to followers, commits on $\lfloor N/2 \rfloor + 1$ acknowledgments,
/// and supports automatic failover when a node is partitioned/down.
pub struct RaftReplicationGroup<T: ZeroCopyRecord + Indexable> {
    nodes: Vec<Arc<RaftNode<T>>>,
    isolated_nodes: Mutex<Vec<NodeId>>,
}

impl<T: ZeroCopyRecord + Indexable> core::fmt::Debug for RaftReplicationGroup<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RaftReplicationGroup")
            .field("node_count", &self.nodes.len())
            .finish()
    }
}

impl<T: ZeroCopyRecord + Indexable> RaftReplicationGroup<T> {
    pub fn new(nodes: Vec<Arc<RaftNode<T>>>) -> Result<Self> {
        if nodes.is_empty() {
            return Err(StorageError::invalid_key(
                "RaftReplicationGroup requires at least 1 node",
            ));
        }
        Ok(Self {
            nodes,
            isolated_nodes: Mutex::new(Vec::new()),
        })
    }

    #[inline]
    pub fn quorum_size(&self) -> usize {
        (self.nodes.len() / 2) + 1
    }

    /// Simulates network isolation/failure of `node_id` for failover testing.
    pub fn set_node_isolated(&self, node_id: NodeId, isolated: bool) {
        let mut iso = self
            .isolated_nodes
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if isolated {
            if !iso.contains(&node_id) {
                iso.push(node_id);
            }
        } else {
            iso.retain(|&n| n != node_id);
        }
    }

    #[inline]
    pub fn is_isolated(&self, node_id: NodeId) -> bool {
        self.isolated_nodes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(&node_id)
    }

    /// Runs a Raft election initiated by `candidate_node_id`.
    /// Returns `Ok(term)` if `candidate_node_id` wins a quorum of votes and becomes `Leader`.
    pub fn elect_leader(&self, candidate_node_id: NodeId) -> Result<u64> {
        if self.is_isolated(candidate_node_id) {
            return Err(StorageError::invalid_key("isolated node cannot win election"));
        }
        let candidate = self
            .nodes
            .iter()
            .find(|n| n.node_id() == candidate_node_id)
            .ok_or_else(|| StorageError::invalid_key("candidate node not found"))?;

        let req = candidate.start_election();
        let mut votes = 1usize; // self vote
        for peer in &self.nodes {
            if peer.node_id() == candidate_node_id || self.is_isolated(peer.node_id()) {
                continue;
            }
            let resp = peer.handle_request_vote(req);
            if resp.vote_granted {
                votes += 1;
            }
        }

        if votes >= self.quorum_size() && candidate.become_leader(req.term) {
            // Send initial empty heartbeat so reachable followers recognize the new leader
            for peer in &self.nodes {
                if peer.node_id() == candidate_node_id || self.is_isolated(peer.node_id()) {
                    continue;
                }
                if let Ok(hb) = candidate.build_append_entries_from(peer.commit_index() + 1) {
                    let _ = peer.handle_append_entries(hb);
                }
            }
            Ok(req.term)
        } else {
            Err(StorageError::invalid_key("candidate failed to reach Raft quorum"))
        }
    }

    /// Returns the current active non-isolated `Leader` node with the highest Raft term.
    pub fn current_leader(&self) -> Option<Arc<RaftNode<T>>> {
        self.nodes
            .iter()
            .filter(|n| n.role() == RaftRole::Leader && !self.is_isolated(n.node_id()))
            .max_by_key(|n| n.current_term())
            .cloned()
    }

    /// Proposes a replicated `PUT` through the current Raft leader and waits for quorum commit ($\lfloor N/2 \rfloor + 1$).
    /// Returns the committed Raft log index.
    pub fn replicate_put(&self, id: InlineId, record: &T) -> Result<u64> {
        let leader = self
            .current_leader()
            .ok_or_else(|| StorageError::invalid_key("no active Raft leader"))?;
        let (entry, prev_idx, prev_term) = leader.append_leader_entry(id, Some(record))?;

        let mut acks = 1usize; // leader itself
        for peer in &self.nodes {
            if peer.node_id() == leader.node_id() || self.is_isolated(peer.node_id()) {
                continue;
            }
            // Fast path: replicate just the new entry if follower is caught up to `prev_idx`,
            // or catch up from `peer.commit_index() + 1` if lagging.
            let req = if peer.commit_index() == prev_idx {
                AppendEntriesReq {
                    term: entry.term,
                    leader_id: leader.node_id(),
                    prev_log_index: prev_idx,
                    prev_log_term: prev_term,
                    entries: vec![entry.clone()],
                    leader_commit: leader.commit_index(),
                }
            } else {
                leader.build_append_entries_from(peer.commit_index() + 1)?
            };
            if let Ok(resp) = peer.handle_append_entries(req) {
                if resp.success && resp.match_index >= entry.index {
                    acks += 1;
                }
            }
        }

        if acks < self.quorum_size() {
            return Err(StorageError::invalid_key(
                "write failed to reach Raft replication quorum",
            ));
        }

        let committed_idx = leader.advance_leader_commit(entry.index)?;

        // Broadcast updated `leader_commit` so reachable followers immediately apply up to `committed_idx`
        for peer in &self.nodes {
            if peer.node_id() == leader.node_id() || self.is_isolated(peer.node_id()) {
                continue;
            }
            if let Ok(req) = leader.build_append_entries_from(peer.last_applied() + 1) {
                let _ = peer.handle_append_entries(req);
            }
        }

        Ok(committed_idx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EngineConfig;
    use crate::model::AgentObjectRecord;

    #[test]
    fn test_raft_3_node_quorum_replication_failover_and_follower_reads() {
        let base = std::env::temp_dir().join("stge_test_raft_suite");
        let _ = std::fs::remove_dir_all(&base);

        let e1 = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(&base.join("r1"), EngineConfig::default())
                .unwrap(),
        );
        let e2 = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(&base.join("r2"), EngineConfig::default())
                .unwrap(),
        );
        let e3 = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(&base.join("r3"), EngineConfig::default())
                .unwrap(),
        );

        let n1 = Arc::new(RaftNode::new(1, Arc::clone(&e1)));
        let n2 = Arc::new(RaftNode::new(2, Arc::clone(&e2)));
        let n3 = Arc::new(RaftNode::new(3, Arc::clone(&e3)));

        let group = RaftReplicationGroup::new(vec![
            Arc::clone(&n1),
            Arc::clone(&n2),
            Arc::clone(&n3),
        ])
        .unwrap();

        // 1. Elect Node 1 as Leader in Term 1
        let term1 = group.elect_leader(1).unwrap();
        assert_eq!(term1, 1);
        assert_eq!(n1.role(), RaftRole::Leader);

        // 2. Replicate PUT across the 3-node group and verify Follower Read scaling on Node 2 & Node 3
        let id1 = InlineId::from_static("raft_rec_01");
        let rec1 = AgentObjectRecord::new(id1, "Raft Replicated 1", 1000, 111);
        let idx1 = group.replicate_put(id1, &rec1).unwrap();
        assert_eq!(idx1, 1);

        let follower_score = n2
            .read_at_commit_index(id1, idx1, |v| v.score)
            .unwrap();
        assert_eq!(follower_score, 111);

        // 3. Isolate Leader Node 1 -> Elect Node 2 as new Leader in Term 2 (2/3 quorum: Node 2 + Node 3)
        group.set_node_isolated(1, true);
        let term2 = group.elect_leader(2).unwrap();
        assert_eq!(term2, 2);
        assert_eq!(n2.role(), RaftRole::Leader);

        // 4. Replicate a second write while Node 1 is down -> succeeds via 2/3 quorum (Node 2 + Node 3)!
        let id2 = InlineId::from_static("raft_rec_02");
        let rec2 = AgentObjectRecord::new(id2, "Raft Failover Write", 2000, 222);
        let idx2 = group.replicate_put(id2, &rec2).unwrap();
        assert_eq!(idx2, 2);

        assert_eq!(
            n3.read_at_commit_index(id2, idx2, |v| v.score).unwrap(),
            222
        );

        // 5. Heal Node 1 and replicate a third write -> Node 1 steps down to Follower and catches up all missing entries!
        group.set_node_isolated(1, false);
        let id3 = InlineId::from_static("raft_rec_03");
        let rec3 = AgentObjectRecord::new(id3, "Raft Healed Write", 3000, 333);
        let idx3 = group.replicate_put(id3, &rec3).unwrap();
        assert_eq!(idx3, 3);
        assert_eq!(n1.role(), RaftRole::Follower);
        assert_eq!(
            n1.read_at_commit_index(id2, idx3, |v| v.score).unwrap(),
            222
        );
        assert_eq!(
            n1.read_at_commit_index(id3, idx3, |v| v.score).unwrap(),
            333
        );

        drop(group);
        drop(n1);
        drop(n2);
        drop(n3);
        drop(e1);
        drop(e2);
        drop(e3);
        let _ = std::fs::remove_dir_all(&base);
    }
}
