//! Raft Consensus State Machine, Quorum Log Replication & Follower Read Scaling
//! (`TASK-046`, `F-DIST-02`, `F-DIST-04`).
//!
//! Provides:
//! - Deterministic Raft state machine (`Follower`, `Candidate`, `Leader`) with monotonic `term` and `index`.
//! - `RequestVote` and `AppendEntries` RPC handlers enforcing the Raft Log Matching Property and
//!   Leader Completeness Property.
//! - Quorum commit advancement ($\lfloor N/2 \rfloor + 1$) applying zero-copy `AlignedBytes` frames to
//!   the underlying `StorageEngine<T>`.
//! - Follower reads (`read_at_commit_index`) with read-your-writes consistency (`F-DIST-04`).
//! - Durable hard state (fsynced before any vote/ack) and an append-only checksummed log.
//!
//! Transport and election timers are driven by the caller: `RaftReplicationGroup` delivers
//! messages in-process, and `storage-server` exposes `RequestVote` / `AppendEntries` over TCP
//! when started with a `RaftNode`.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use storage_codec::{checksum_payload, AlignedBytes, RecordCodec, RecordView, ZeroCopyRecord};
use storage_index::Indexable;
use storage_mmap::atomic_write_replace;
use storage_types::{InlineId, RecordFlags, Result, StorageError, Version};
use storage_types::wire::{read_u16_le, read_u32_le, read_u64_le, read_array_24};

use crate::engine::StorageEngine;
use crate::sharding::NodeId;

const RAFT_META_MAGIC: u32 = 0x5241_4654; // "RAFT"

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

impl RequestVoteReq {
    pub fn to_wire_bytes(&self) -> [u8; 26] {
        let mut out = [0u8; 26];
        out[0..8].copy_from_slice(&self.term.to_le_bytes());
        out[8..10].copy_from_slice(&self.candidate_id.to_le_bytes());
        out[10..18].copy_from_slice(&self.last_log_index.to_le_bytes());
        out[18..26].copy_from_slice(&self.last_log_term.to_le_bytes());
        out
    }

    pub fn from_wire_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 26 {
            return Err(StorageError::corrupted_header("RequestVoteReq wire frame < 26B"));
        }
        Ok(Self {
            term: read_u64_le(bytes, 0),
            candidate_id: read_u16_le(bytes, 8),
            last_log_index: read_u64_le(bytes, 10),
            last_log_term: read_u64_le(bytes, 18),
        })
    }
}

/// Raft `RequestVote` RPC response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestVoteResp {
    pub term: u64,
    pub vote_granted: bool,
}

impl RequestVoteResp {
    pub fn to_wire_bytes(&self) -> [u8; 9] {
        let mut out = [0u8; 9];
        out[0..8].copy_from_slice(&self.term.to_le_bytes());
        out[8] = u8::from(self.vote_granted);
        out
    }

    pub fn from_wire_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 9 {
            return Err(StorageError::corrupted_header("RequestVoteResp wire frame < 9B"));
        }
        Ok(Self {
            term: read_u64_le(bytes, 0),
            vote_granted: bytes[8] != 0,
        })
    }
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

impl AppendEntriesReq {
    pub fn to_wire_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(38 + self.entries.len() * 64);
        out.extend_from_slice(&self.term.to_le_bytes());
        out.extend_from_slice(&self.leader_id.to_le_bytes());
        out.extend_from_slice(&self.prev_log_index.to_le_bytes());
        out.extend_from_slice(&self.prev_log_term.to_le_bytes());
        out.extend_from_slice(&self.leader_commit.to_le_bytes());
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for entry in &self.entries {
            encode_log_entry(entry, &mut out);
        }
        out
    }

    pub fn from_wire_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 38 {
            return Err(StorageError::corrupted_header("AppendEntriesReq wire frame < 38B"));
        }
        let term = read_u64_le(bytes, 0);
        let leader_id = read_u16_le(bytes, 8);
        let prev_log_index = read_u64_le(bytes, 10);
        let prev_log_term = read_u64_le(bytes, 18);
        let leader_commit = read_u64_le(bytes, 26);
        let count = read_u32_le(bytes, 34) as usize;
        let mut cursor = 38usize;
        let mut entries = Vec::with_capacity(count.min(4096));
        for _ in 0..count {
            let (entry, next_cursor) = decode_log_entry(bytes, cursor)?;
            entries.push(entry);
            cursor = next_cursor;
        }
        Ok(Self {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        })
    }
}

/// Raft `AppendEntries` RPC response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppendEntriesResp {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

impl AppendEntriesResp {
    pub fn to_wire_bytes(&self) -> [u8; 17] {
        let mut out = [0u8; 17];
        out[0..8].copy_from_slice(&self.term.to_le_bytes());
        out[8] = u8::from(self.success);
        out[9..17].copy_from_slice(&self.match_index.to_le_bytes());
        out
    }

    pub fn from_wire_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 17 {
            return Err(StorageError::corrupted_header("AppendEntriesResp wire frame < 17B"));
        }
        Ok(Self {
            term: read_u64_le(bytes, 0),
            success: bytes[8] != 0,
            match_index: read_u64_le(bytes, 9),
        })
    }
}

fn encode_log_entry(entry: &RaftLogEntry, out: &mut Vec<u8>) {
    out.extend_from_slice(&entry.term.to_le_bytes());
    out.extend_from_slice(&entry.index.to_le_bytes());
    out.extend_from_slice(&entry.id.to_raw_bytes());
    out.extend_from_slice(&entry.version.0.to_le_bytes());
    match entry.frame {
        Some(ref frame) => {
            out.push(1u8);
            let slice = frame.as_slice();
            out.extend_from_slice(&(slice.len() as u32).to_le_bytes());
            out.extend_from_slice(slice);
        }
        None => {
            out.push(0u8);
            out.extend_from_slice(&0u32.to_le_bytes());
        }
    }
}

fn decode_log_entry(bytes: &[u8], cursor: usize) -> Result<(RaftLogEntry, usize)> {
    if cursor + 53 > bytes.len() {
        return Err(StorageError::corrupted_header("RaftLogEntry truncated header"));
    }
    let term = read_u64_le(bytes, cursor);
    let index = read_u64_le(bytes, cursor + 8);
    let id = InlineId::from_raw_bytes(read_array_24(bytes, cursor + 16))?;
    let ver = Version(read_u64_le(bytes, cursor + 40));
    let has_frame = bytes[cursor + 48] != 0;
    let frame_len = read_u32_le(bytes, cursor + 49) as usize;
    let end = cursor + 53 + frame_len;
    if end > bytes.len() {
        return Err(StorageError::corrupted_header("RaftLogEntry truncated frame"));
    }
    let frame = if has_frame {
        Some(AlignedBytes::try_copy_from_slice(&bytes[cursor + 53..end])?)
    } else {
        None
    };
    Ok((
        RaftLogEntry {
            term,
            index,
            id,
            version: ver,
            frame,
        },
        end,
    ))
}

/// Compact the log once this many applied entries are retained in it.
const RAFT_COMPACT_THRESHOLD: u64 = 8_192;
/// Applied entries kept after compaction so briefly lagging followers can catch up from the log.
const RAFT_RETAIN_TAIL: u64 = 1_024;

#[derive(Debug)]
struct RaftState {
    role: RaftRole,
    current_term: u64,
    voted_for: Option<NodeId>,
    leader_id: Option<NodeId>,
    /// Entries with index `snapshot_index + 1 ..`; `log[i]` has index `snapshot_index + 1 + i`.
    log: Vec<RaftLogEntry>,
    commit_index: u64,
    last_applied: u64,
    /// Last log index discarded by compaction. Everything up to it is applied and durable in the
    /// engine, which is therefore the snapshot (Raft §7).
    snapshot_index: u64,
    /// Term of the entry at `snapshot_index` (`0` when nothing was compacted).
    snapshot_term: u64,
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
            snapshot_index: 0,
            snapshot_term: 0,
        }
    }

    #[inline]
    fn last_log_index_and_term(&self) -> (u64, u64) {
        self.log
            .last()
            .map_or((self.snapshot_index, self.snapshot_term), |e| (e.index, e.term))
    }

    #[inline]
    fn last_log_index(&self) -> u64 {
        self.snapshot_index + self.log.len() as u64
    }

    /// Position of log index `index` inside `self.log` (`None` if compacted or index `0`).
    #[inline]
    fn pos_of(&self, index: u64) -> Option<usize> {
        index
            .checked_sub(self.snapshot_index + 1)
            .map(|p| p as usize)
    }

    #[inline]
    fn entry_at(&self, index: u64) -> Option<&RaftLogEntry> {
        self.log.get(self.pos_of(index)?)
    }

    /// Term of the entry at `index`: `Some(0)` for index `0`, the snapshot term at the compaction
    /// point, `None` for compacted or missing entries.
    #[inline]
    fn term_at(&self, index: u64) -> Option<u64> {
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        self.entry_at(index).map(|e| e.term)
    }
}

/// On-disk persistence for a Raft replica:
/// - `<stem>_raft.meta`: hard state (`term`, `voted_for`, `commit_index`, `last_applied`),
///   replaced atomically and `fsync`ed before any vote or acknowledgement is sent.
/// - `<stem>_raft.log`: append-only `[len u32 | checksum u64 | entry]` frames, `fsync`ed per
///   append; only a conflicting-suffix truncation rewrites the file.
#[derive(Debug)]
struct RaftStorage {
    meta_path: PathBuf,
    log_path: PathBuf,
}

impl RaftStorage {
    fn save_hard_state(&self, st: &RaftState) -> Result<()> {
        let mut payload = Vec::with_capacity(47);
        payload.extend_from_slice(&st.current_term.to_le_bytes());
        match st.voted_for {
            Some(node) => {
                payload.push(1u8);
                payload.extend_from_slice(&node.to_le_bytes());
            }
            None => {
                payload.push(0u8);
                payload.extend_from_slice(&0u16.to_le_bytes());
            }
        }
        payload.extend_from_slice(&st.commit_index.to_le_bytes());
        payload.extend_from_slice(&st.last_applied.to_le_bytes());
        // Entry count of the legacy embedded-log format: always 0 now.
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(&st.snapshot_index.to_le_bytes());
        payload.extend_from_slice(&st.snapshot_term.to_le_bytes());

        let crc = checksum_payload(&payload);
        let mut full = Vec::with_capacity(12 + payload.len());
        full.extend_from_slice(&RAFT_META_MAGIC.to_le_bytes());
        full.extend_from_slice(&crc.to_le_bytes());
        full.extend_from_slice(&payload);
        atomic_write_replace(&self.meta_path, &full, true)
    }

    fn encode_log_frame(entry: &RaftLogEntry, out: &mut Vec<u8>) {
        let mut body = Vec::with_capacity(64);
        encode_log_entry(entry, &mut body);
        out.extend_from_slice(&(body.len() as u32).to_le_bytes());
        out.extend_from_slice(&checksum_payload(&body).to_le_bytes());
        out.extend_from_slice(&body);
    }

    fn append_log(&self, entries: &[RaftLogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for e in entries {
            Self::encode_log_frame(e, &mut buf);
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)
            .map_err(|e| StorageError::io("open raft log", e))?;
        file.write_all(&buf)
            .map_err(|e| StorageError::io("append raft log", e))?;
        file.sync_data()
            .map_err(|e| StorageError::io("fsync raft log", e))
    }

    fn rewrite_log(&self, log: &[RaftLogEntry]) -> Result<()> {
        let mut buf = Vec::new();
        for e in log {
            Self::encode_log_frame(e, &mut buf);
        }
        atomic_write_replace(&self.log_path, &buf, true)
    }

    fn load(&self) -> Result<Option<RaftState>> {
        if !self.meta_path.exists() && !self.log_path.exists() {
            return Ok(None);
        }
        let mut st = RaftState::new();
        let mut legacy_log = Vec::new();
        if self.meta_path.exists() {
            let bytes = fs::read(&self.meta_path).map_err(|e| StorageError::io("read raft.meta", e))?;
            if bytes.len() < 43 {
                return Err(StorageError::corrupted_header("raft.meta truncated"));
            }
            if read_u32_le(&bytes, 0) != RAFT_META_MAGIC {
                return Err(StorageError::corrupted_header("raft.meta invalid magic"));
            }
            let expected_crc = read_u64_le(&bytes, 4);
            let payload = &bytes[12..];
            let actual_crc = checksum_payload(payload);
            if actual_crc != expected_crc {
                return Err(StorageError::checksum_mismatch(expected_crc, actual_crc));
            }
            st.current_term = read_u64_le(payload, 0);
            st.voted_for = (payload[8] != 0).then(|| read_u16_le(payload, 9));
            st.commit_index = read_u64_le(payload, 11);
            st.last_applied = read_u64_le(payload, 19);
            // Legacy format embedded the whole log in raft.meta.
            let count = read_u32_le(payload, 27) as usize;
            let mut cursor = 31usize;
            for _ in 0..count {
                let (entry, next) = decode_log_entry(payload, cursor)?;
                legacy_log.push(entry);
                cursor = next;
            }
            // Current format: compaction point after the (empty) legacy log.
            if count == 0 && payload.len() >= 47 {
                st.snapshot_index = read_u64_le(payload, 31);
                st.snapshot_term = read_u64_le(payload, 39);
            }
        }

        if self.log_path.exists() {
            let raw = fs::read(&self.log_path).map_err(|e| StorageError::io("read raft log", e))?;
            let mut pos = 0usize;
            while pos + 12 <= raw.len() {
                let len = read_u32_le(&raw, pos) as usize;
                let sum = read_u64_le(&raw, pos + 4);
                let Some(end) = (pos + 12).checked_add(len).filter(|&e| e <= raw.len()) else {
                    break; // torn tail
                };
                let body = &raw[pos + 12..end];
                if checksum_payload(body) != sum {
                    break;
                }
                let (entry, _) = decode_log_entry(body, 0)?;
                pos = end;
                // Entries covered by the snapshot remain only if a crash hit between writing the
                // new compaction point and rewriting the log: skip them.
                if entry.index <= st.snapshot_index {
                    continue;
                }
                if entry.index != st.last_log_index() + 1 {
                    return Err(StorageError::corrupted_header("raft log index gap"));
                }
                st.log.push(entry);
            }
            if pos < raw.len() {
                self.rewrite_log(&st.log)?;
            }
        } else if !legacy_log.is_empty() {
            st.log = legacy_log;
            self.rewrite_log(&st.log)?;
            self.save_hard_state(&st)?;
        }

        // Durable entries can never be fewer than the committed prefix we claim, and only applied
        // entries are ever compacted.
        let len = st.last_log_index();
        if st.commit_index > len
            || st.last_applied > st.commit_index
            || st.snapshot_index > st.last_applied
        {
            return Err(StorageError::corrupted_header(
                "raft hard state references entries missing from the log",
            ));
        }
        Ok(Some(st))
    }
}

/// Single Raft Consensus Replica wrapping a local `StorageEngine<T>` (`TASK-046`, `TASK-060`, `F-DIST-02`).
///
/// This is the consensus state machine; message transport and election timers are driven by the
/// caller (in-process via [`RaftReplicationGroup`], or over TCP via the `storage-server` Raft RPCs).
pub struct RaftNode<T: ZeroCopyRecord + Indexable> {
    node_id: NodeId,
    engine: Arc<StorageEngine<T>>,
    storage: RaftStorage,
    state: Mutex<RaftState>,
    /// Serializes applying committed entries to the engine, in log order.
    apply_lock: Mutex<()>,
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
    /// Opens the replica, restoring persisted hard state and log.
    ///
    /// Corrupted Raft state is an error: silently starting over could let this node vote twice
    /// in a term or forget committed entries.
    pub fn new(node_id: NodeId, engine: Arc<StorageEngine<T>>) -> Result<Self> {
        let stem = crate::engine::collection_file_stem(engine.collection_name())?;
        let storage = RaftStorage {
            meta_path: engine.base_dir().join(format!("{stem}_raft.meta")),
            log_path: engine.base_dir().join(format!("{stem}_raft.log")),
        };
        let initial_state = storage.load()?.unwrap_or_else(RaftState::new);
        let node = Self {
            node_id,
            engine,
            storage,
            state: Mutex::new(initial_state),
            apply_lock: Mutex::new(()),
        };
        // Finish applying entries that were committed but not yet applied before a restart.
        node.apply_committed()?;
        Ok(node)
    }

    #[inline]
    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    #[inline]
    pub fn engine(&self) -> &Arc<StorageEngine<T>> {
        &self.engine
    }

    #[inline]
    fn lock_state(&self) -> std::sync::MutexGuard<'_, RaftState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn role(&self) -> RaftRole {
        self.lock_state().role
    }

    pub fn current_term(&self) -> u64 {
        self.lock_state().current_term
    }

    pub fn voted_for(&self) -> Option<NodeId> {
        self.lock_state().voted_for
    }

    pub fn commit_index(&self) -> u64 {
        self.lock_state().commit_index
    }

    pub fn last_applied(&self) -> u64 {
        self.lock_state().last_applied
    }

    /// Index of the last entry in this replica's log.
    pub fn last_log_index(&self) -> u64 {
        self.lock_state().last_log_index_and_term().0
    }

    /// Transitions this node to `Candidate`, increments `current_term`, votes for itself,
    /// durably persists the hard state, and returns a `RequestVoteReq` to broadcast to peers.
    pub fn start_election(&self) -> Result<RequestVoteReq> {
        let mut st = self.lock_state();
        st.current_term += 1;
        st.role = RaftRole::Candidate;
        st.voted_for = Some(self.node_id);
        st.leader_id = None;
        self.storage.save_hard_state(&st)?;
        let (last_log_index, last_log_term) = st.last_log_index_and_term();
        Ok(RequestVoteReq {
            term: st.current_term,
            candidate_id: self.node_id,
            last_log_index,
            last_log_term,
        })
    }

    /// Processes a `RequestVoteReq` according to the Raft Election Safety and Log Up-To-Date rules.
    /// A vote is only granted after it has been durably persisted.
    pub fn handle_request_vote(&self, req: RequestVoteReq) -> RequestVoteResp {
        let mut st = self.lock_state();
        let mut dirty = false;
        if req.term > st.current_term {
            st.current_term = req.term;
            st.role = RaftRole::Follower;
            st.voted_for = None;
            st.leader_id = None;
            dirty = true;
        }
        if req.term < st.current_term {
            if dirty {
                let _ = self.storage.save_hard_state(&st);
            }
            return RequestVoteResp {
                term: st.current_term,
                vote_granted: false,
            };
        }

        let (my_last_idx, my_last_term) = st.last_log_index_and_term();
        let candidate_up_to_date = req.last_log_term > my_last_term
            || (req.last_log_term == my_last_term && req.last_log_index >= my_last_idx);
        let can_vote = st.voted_for.is_none() || st.voted_for == Some(req.candidate_id);

        let mut granted = false;
        if can_vote && candidate_up_to_date {
            let prev_vote = st.voted_for;
            st.voted_for = Some(req.candidate_id);
            if self.storage.save_hard_state(&st).is_ok() {
                granted = true;
            } else {
                st.voted_for = prev_vote;
            }
        } else if dirty {
            let _ = self.storage.save_hard_state(&st);
        }
        RequestVoteResp {
            term: st.current_term,
            vote_granted: granted,
        }
    }

    /// Promotes a `Candidate` that has won a majority of votes in `term` to `Leader`.
    pub fn become_leader(&self, term: u64) -> bool {
        let mut st = self.lock_state();
        if st.current_term == term && st.role == RaftRole::Candidate {
            st.role = RaftRole::Leader;
            st.leader_id = Some(self.node_id);
            true
        } else {
            false
        }
    }

    /// Appends a new entry to the leader's durable log (prior to replication).
    pub fn append_leader_entry(
        &self,
        id: InlineId,
        record: Option<&T>,
    ) -> Result<(RaftLogEntry, u64, u64)> {
        let mut st = self.lock_state();
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
        self.storage.append_log(core::slice::from_ref(&entry))?;
        st.log.push(entry.clone());
        Ok((entry, prev_idx, prev_term))
    }

    /// Builds an `AppendEntriesReq` carrying all log entries from `from_index` (1-based) onward.
    pub fn build_append_entries_from(&self, from_index: u64) -> Result<AppendEntriesReq> {
        let st = self.lock_state();
        if st.role != RaftRole::Leader {
            return Err(StorageError::invalid_key("node is not Raft Leader"));
        }
        let from_index = from_index.max(1);
        let prev_log_index = (from_index - 1).min(st.last_log_index());
        if prev_log_index < st.snapshot_index {
            return Err(StorageError::invalid_key(
                "follower is behind the compacted Raft log (snapshot required): reseed it from a backup",
            ));
        }
        let prev_log_term = st.term_at(prev_log_index).unwrap_or(0);
        let start = st.pos_of(prev_log_index + 1).unwrap_or(0).min(st.log.len());
        let entries = st.log[start..].to_vec();
        Ok(AppendEntriesReq {
            term: st.current_term,
            leader_id: self.node_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: st.commit_index,
        })
    }

    /// Processes an `AppendEntriesReq` from the leader, durably appends its entries, and applies
    /// newly committed entries to `StorageEngine<T>`.
    pub fn handle_append_entries(&self, req: AppendEntriesReq) -> Result<AppendEntriesResp> {
        let (term, match_index) = {
            let mut st = self.lock_state();
            if req.term < st.current_term {
                return Ok(AppendEntriesResp {
                    term: st.current_term,
                    success: false,
                    match_index: st.commit_index,
                });
            }
            if req.term > st.current_term {
                st.current_term = req.term;
                st.voted_for = None;
                self.storage.save_hard_state(&st)?;
            }
            // Same-term step-down keeps `voted_for` (a node votes at most once per term).
            st.role = RaftRole::Follower;
            st.leader_id = Some(req.leader_id);

            // Entries must be contiguous and directly follow `prev_log_index`.
            let well_formed = req
                .entries
                .iter()
                .enumerate()
                .all(|(i, e)| Some(e.index) == req.prev_log_index.checked_add(i as u64 + 1));
            if !well_formed {
                return Ok(AppendEntriesResp {
                    term: st.current_term,
                    success: false,
                    match_index: st.last_log_index(),
                });
            }

            // Entries at or below our compaction point are committed, hence identical to ours
            // (Log Matching): skip them and match from the snapshot point instead.
            let (prev_log_index, prev_log_term, entries) = if req.prev_log_index < st.snapshot_index {
                let snapshot_index = st.snapshot_index;
                let rest: Vec<RaftLogEntry> = req
                    .entries
                    .into_iter()
                    .filter(|e| e.index > snapshot_index)
                    .collect();
                (snapshot_index, st.snapshot_term, rest)
            } else {
                (req.prev_log_index, req.prev_log_term, req.entries)
            };

            // Verify Raft Log Matching Property at `prev_log_index`
            match st.term_at(prev_log_index) {
                Some(t) if t == prev_log_term => {}
                _ => {
                    return Ok(AppendEntriesResp {
                        term: st.current_term,
                        success: false,
                        match_index: st.commit_index.min(st.last_log_index()),
                    });
                }
            }

            // Append new entries, truncating a conflicting (necessarily uncommitted) suffix.
            let mut new_from: Option<usize> = None;
            let mut truncated = false;
            for entry in entries {
                let Some(pos) = st.pos_of(entry.index) else {
                    continue;
                };
                if pos < st.log.len() {
                    if st.log[pos].term == entry.term {
                        continue;
                    }
                    if entry.index <= st.commit_index {
                        return Err(StorageError::corrupted_header(
                            "leader tried to overwrite a committed Raft entry",
                        ));
                    }
                    st.log.truncate(pos);
                    truncated = true;
                }
                new_from.get_or_insert(st.log.len());
                st.log.push(entry);
            }
            if truncated {
                self.storage.rewrite_log(&st.log)?;
            } else if let Some(from) = new_from {
                self.storage.append_log(&st.log[from..])?;
            }

            let match_index = st.last_log_index();
            if req.leader_commit > st.commit_index {
                st.commit_index = req.leader_commit.min(match_index);
                self.storage.save_hard_state(&st)?;
            }
            (st.current_term, match_index)
        };

        self.apply_committed()?;

        Ok(AppendEntriesResp {
            term,
            success: true,
            match_index,
        })
    }

    /// Advances the leader's `commit_index` to `quorum_index` once a majority of nodes have
    /// replicated it, and applies newly committed entries to the local `StorageEngine<T>`.
    ///
    /// Per the Raft commitment rule, only an entry from the leader's current term can be
    /// committed by counting replicas (earlier entries are committed indirectly).
    pub fn advance_leader_commit(&self, quorum_index: u64) -> Result<u64> {
        {
            let mut st = self.lock_state();
            if st.role != RaftRole::Leader {
                return Err(StorageError::invalid_key("node is not Raft Leader"));
            }
            if quorum_index > st.commit_index
                && quorum_index <= st.last_log_index()
                && st.term_at(quorum_index) == Some(st.current_term)
            {
                st.commit_index = quorum_index;
                self.storage.save_hard_state(&st)?;
            }
        }
        self.apply_committed()?;
        Ok(self.commit_index())
    }

    /// Applies committed-but-unapplied entries in order. `last_applied` only advances after an
    /// entry has been applied successfully, so a crash or error never skips an entry.
    fn apply_committed(&self) -> Result<()> {
        let _apply = self.apply_lock.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            let next = {
                let st = self.lock_state();
                if st.last_applied >= st.commit_index {
                    break;
                }
                st.entry_at(st.last_applied + 1).cloned()
            };
            let Some(entry) = next else {
                break;
            };
            self.apply_entry_to_engine(&entry)?;
            let mut st = self.lock_state();
            st.last_applied = st.last_applied.max(entry.index);
            self.storage.save_hard_state(&st)?;
        }
        // Bound log memory and disk (anti-bloat): compact once enough applied entries pile up,
        // keeping a tail for lagging followers.
        let (applied, snapshot) = {
            let st = self.lock_state();
            (st.last_applied, st.snapshot_index)
        };
        if applied.saturating_sub(snapshot) > RAFT_COMPACT_THRESHOLD {
            self.compact_locked(applied - RAFT_RETAIN_TAIL)?;
        }
        Ok(())
    }

    /// Discards every log entry up to `index` (clamped to `last_applied`) from memory and disk,
    /// recording it as the snapshot point (Raft §7). Applied entries live on in the engine, whose
    /// WAL is synced first, so nothing durable is lost. Returns the new compaction point.
    ///
    /// A follower whose next needed entry was compacted can no longer catch up from this log and
    /// must be reseeded from a backup.
    pub fn compact_log_through(&self, index: u64) -> Result<u64> {
        let _apply = self.apply_lock.lock().unwrap_or_else(|p| p.into_inner());
        self.compact_locked(index)
    }

    /// [`Self::compact_log_through`] with `apply_lock` already held.
    fn compact_locked(&self, index: u64) -> Result<u64> {
        // Applied entries must be durable in the engine before the log forgets them.
        self.engine.sync_wal()?;
        let mut st = self.lock_state();
        let index = index.min(st.last_applied);
        if index <= st.snapshot_index {
            return Ok(st.snapshot_index);
        }
        let term = st
            .term_at(index)
            .ok_or_else(|| StorageError::corrupted_header("raft compaction point is missing"))?;
        let drop_count = (index - st.snapshot_index) as usize;
        st.snapshot_index = index;
        st.snapshot_term = term;
        // Compaction point first, then the shorter log: the loader skips covered entries, so a
        // crash between the two writes is harmless.
        self.storage.save_hard_state(&st)?;
        let drop_count = drop_count.min(st.log.len());
        st.log.drain(..drop_count);
        self.storage.rewrite_log(&st.log)?;
        Ok(index)
    }

    /// Last log index discarded by compaction (`0` if never compacted).
    pub fn snapshot_index(&self) -> u64 {
        self.lock_state().snapshot_index
    }

    /// Number of log entries currently held in memory.
    pub fn log_len(&self) -> usize {
        self.lock_state().log.len()
    }

    /// Follower read: executes `f(view)` on this replica only if it has applied at least
    /// `min_commit_index` (e.g. the index returned by a write), giving read-your-writes
    /// consistency. This is NOT a linearizable read: it may miss writes committed after
    /// `min_commit_index` (`F-DIST-04`).
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

/// In-process Multi-Node Raft Replication Group Coordinator (`TASK-046`, `F-DIST-02`, `F-DIST-04`).
///
/// Drives a quorum of `RaftNode<T>` replicas living in the same process (message delivery is a
/// direct call, partitions are simulated with `set_node_isolated`). It tracks a per-follower
/// `next_index` and backs off on log mismatches like a real Raft leader.
pub struct RaftReplicationGroup<T: ZeroCopyRecord + Indexable> {
    nodes: Vec<Arc<RaftNode<T>>>,
    isolated_nodes: Mutex<Vec<NodeId>>,
    next_index: Mutex<std::collections::HashMap<NodeId, u64>>,
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
            next_index: Mutex::new(std::collections::HashMap::new()),
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

    /// Replicates the leader's log to `peer`, backing off `next_index` on mismatches.
    /// Returns the follower's match index on success.
    fn sync_follower(&self, leader: &RaftNode<T>, peer: &RaftNode<T>) -> Option<u64> {
        let leader_last = leader.last_log_index();
        let mut next = self
            .next_index
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&peer.node_id())
            .copied()
            .unwrap_or(leader_last + 1)
            .clamp(1, leader_last + 1);
        for _ in 0..=leader_last + 1 {
            let req = leader.build_append_entries_from(next).ok()?;
            let resp = peer.handle_append_entries(req).ok()?;
            if resp.term > leader.current_term() {
                return None;
            }
            if resp.success {
                self.next_index
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(peer.node_id(), resp.match_index + 1);
                return Some(resp.match_index);
            }
            // Back off to just past the follower's reported progress (at least one step).
            next = (resp.match_index + 1).min(next.saturating_sub(1)).max(1);
        }
        None
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

        let req = candidate.start_election()?;
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
            // A new leader resets every follower's `next_index` to its own log end + 1.
            self.next_index
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clear();
            for peer in &self.nodes {
                if peer.node_id() == candidate_node_id || self.is_isolated(peer.node_id()) {
                    continue;
                }
                let _ = self.sync_follower(candidate, peer);
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
        let (entry, _prev_idx, _prev_term) = leader.append_leader_entry(id, Some(record))?;

        let mut acks = 1usize; // leader itself
        for peer in &self.nodes {
            if peer.node_id() == leader.node_id() || self.is_isolated(peer.node_id()) {
                continue;
            }
            if self
                .sync_follower(&leader, peer)
                .is_some_and(|m| m >= entry.index)
            {
                acks += 1;
            }
        }

        if acks < self.quorum_size() {
            return Err(StorageError::invalid_key(
                "write failed to reach Raft replication quorum",
            ));
        }

        let committed_idx = leader.advance_leader_commit(entry.index)?;

        // Broadcast the new `leader_commit` so reachable followers apply up to `committed_idx`.
        for peer in &self.nodes {
            if peer.node_id() == leader.node_id() || self.is_isolated(peer.node_id()) {
                continue;
            }
            let _ = self.sync_follower(&leader, peer);
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
    fn test_raft_log_compaction_bounds_memory_and_survives_restart() {
        let base = std::env::temp_dir().join(format!("stge_test_raft_compact_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let open = |name: &str| {
            Arc::new(
                StorageEngine::<AgentObjectRecord>::open(base.join(name), EngineConfig::default())
                    .unwrap(),
            )
        };
        let engines = [open("c1"), open("c2"), open("c3")];
        let nodes: Vec<Arc<RaftNode<AgentObjectRecord>>> = engines
            .iter()
            .enumerate()
            .map(|(i, e)| Arc::new(RaftNode::new(i as NodeId + 1, Arc::clone(e)).unwrap()))
            .collect();
        let group = RaftReplicationGroup::new(nodes.clone()).unwrap();
        group.elect_leader(1).unwrap();
        let ids: Vec<InlineId> = (0..30).map(|i| InlineId::new(&format!("k{i}")).unwrap()).collect();
        for (i, &id) in ids.iter().enumerate() {
            group
                .replicate_put(id, &AgentObjectRecord::new(id, "v", 1, i as i64))
                .unwrap();
        }
        // Every node has applied all 30 entries; compact through 25 everywhere.
        let raft_log_path = std::fs::read_dir(base.join("c2"))
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| p.to_string_lossy().ends_with("_raft.log"))
            .expect("raft log file");
        let full_log = std::fs::read(&raft_log_path).unwrap();
        for n in &nodes {
            assert_eq!(n.last_applied(), 30);
            assert_eq!(n.compact_log_through(25).unwrap(), 25);
            assert_eq!(n.snapshot_index(), 25);
            assert_eq!(n.log_len(), 5);
            // Compaction never goes past what was applied.
            assert_eq!(n.compact_log_through(u64::MAX).unwrap(), 30);
            assert_eq!(n.log_len(), 0);
            assert_eq!(n.last_log_index(), 30);
        }
        drop(group);

        // Restart: compaction point, term and indexes survive; a stale pre-compaction log file
        // (crash between the two compaction writes) is tolerated.
        std::fs::write(&raft_log_path, full_log).unwrap();
        let term = nodes[0].current_term();
        drop(nodes);
        let nodes: Vec<Arc<RaftNode<AgentObjectRecord>>> = engines
            .iter()
            .enumerate()
            .map(|(i, e)| Arc::new(RaftNode::new(i as NodeId + 1, Arc::clone(e)).unwrap()))
            .collect();
        for n in &nodes {
            assert_eq!(n.snapshot_index(), 30);
            assert_eq!(n.last_log_index(), 30);
            assert_eq!(n.log_len(), 0);
            assert!(n.current_term() >= term);
        }

        // Replication continues on top of the compacted logs.
        let group = RaftReplicationGroup::new(nodes.clone()).unwrap();
        group.elect_leader(2).unwrap();
        let extra = InlineId::new("after_compaction").unwrap();
        let idx = group
            .replicate_put(extra, &AgentObjectRecord::new(extra, "v", 1, 7))
            .unwrap();
        assert_eq!(idx, 31);
        for e in &engines {
            assert_eq!(e.get_owned(extra).unwrap().1.score, 7);
            assert_eq!(e.get_owned(ids[29]).unwrap().1.score, 29);
        }

        // A follower that needs a compacted entry gets an explicit "snapshot required" error.
        let leader = group.current_leader().unwrap();
        leader.compact_log_through(31).unwrap();
        assert!(leader.build_append_entries_from(5).is_err());
        assert!(leader.build_append_entries_from(32).is_ok());
        drop(group);
        drop(nodes);
        drop(engines);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn test_raft_3_node_quorum_replication_failover_and_follower_reads() {
        let base = std::env::temp_dir().join("stge_test_raft_suite");
        let _ = std::fs::remove_dir_all(&base);

        let e1 = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(base.join("r1"), EngineConfig::default())
                .unwrap(),
        );
        let e2 = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(base.join("r2"), EngineConfig::default())
                .unwrap(),
        );
        let e3 = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(base.join("r3"), EngineConfig::default())
                .unwrap(),
        );

        let n1 = Arc::new(RaftNode::new(1, Arc::clone(&e1)).unwrap());
        let n2 = Arc::new(RaftNode::new(2, Arc::clone(&e2)).unwrap());
        let n3 = Arc::new(RaftNode::new(3, Arc::clone(&e3)).unwrap());

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

        // 6. Persistence: hard state and log survive a restart.
        let reloaded = RaftNode::new(2, Arc::clone(&e2)).unwrap();
        assert_eq!(reloaded.current_term(), 2);
        assert_eq!(reloaded.voted_for(), Some(2));
        assert_eq!(reloaded.last_log_index(), 3);
        drop(reloaded);

        drop(e1);
        drop(e2);
        drop(e3);
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn test_raft_vote_safety_rules() {
        let base = std::env::temp_dir().join("stge_test_raft_vote_safety");
        let _ = std::fs::remove_dir_all(&base);
        let eng = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(&base, EngineConfig::default()).unwrap(),
        );
        let node = RaftNode::new(1, Arc::clone(&eng)).unwrap();

        // Candidate in term 1 (voted for itself) hears from a leader of the same term.
        let _ = node.start_election().unwrap();
        let hb = AppendEntriesReq {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        };
        assert!(node.handle_append_entries(hb).unwrap().success);
        assert_eq!(node.role(), RaftRole::Follower);
        // It must NOT vote for a third candidate in the same term.
        let resp = node.handle_request_vote(RequestVoteReq {
            term: 1,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
        });
        assert!(!resp.vote_granted);

        // Malformed entries (index gap / zero index) are rejected without panicking.
        let bad = AppendEntriesReq {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![RaftLogEntry {
                term: 1,
                index: 0,
                id: InlineId::from_static("x"),
                version: Version(1),
                frame: None,
            }],
            leader_commit: 0,
        };
        assert!(!node.handle_append_entries(bad).unwrap().success);

        // Corrupted persisted state is an error, never a silent reset.
        drop(node);
        let meta = base.join("default_raft.meta");
        let mut bytes = std::fs::read(&meta).unwrap();
        bytes[14] ^= 0xFF;
        std::fs::write(&meta, &bytes).unwrap();
        assert!(RaftNode::new(1, Arc::clone(&eng)).is_err());

        drop(eng);
        let _ = std::fs::remove_dir_all(&base);
    }
}
