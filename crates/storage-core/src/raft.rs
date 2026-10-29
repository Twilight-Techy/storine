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
//! - `InstallSnapshot` (Raft §7, `TASK-089`): a follower behind the leader's compacted log is
//!   caught up by streaming a point-in-time snapshot of the leader's engine in chunks.
//! - Membership changes (Raft thesis §4, `TASK-121`): the cluster configuration is a log entry
//!   ([`RaftCommand::Config`]) that takes effect as soon as it is appended. Changes are made one
//!   voter at a time (any two majorities of consecutive configurations overlap), only one may be
//!   in flight, and a leader proposes one only after committing an entry of its own term.
//!   Learners receive the log but neither vote nor count towards quorum. Snapshots carry the
//!   configuration, and the configuration at the compaction point is persisted.
//!
//! Transport and election timers are driven by the caller: `RaftReplicationGroup` delivers
//! messages in-process, and `storage-server` exposes `RequestVote` / `AppendEntries` over TCP
//! when started with a `RaftNode`.

use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use storage_codec::{checksum_payload, AlignedBytes, RecordCodec, RecordView, ZeroCopyRecord};
use storage_index::Indexable;
use storage_mmap::atomic_write_replace;
use storage_types::vfs::OpenOpts;
use storage_types::{FastHashMap, FastHashSet, InlineId, RecordFlags, Result, StorageError, Version};
use storage_types::wire::{read_u16_le, read_u32_le, read_u64_le, read_array_24};

use crate::engine::{Precondition, StorageEngine, WriteKind, WriteOp, WriteResult};
use crate::sharding::NodeId;

const RAFT_META_MAGIC: u32 = 0x5241_4654; // "RAFT"
const RAFT_SPAN_MAGIC: u32 = 0x5241_5350; // "RASP"

/// Role of a node in a Raft replication group (`F-DIST-02`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

/// What a committed Raft log entry does to the replicated state machine (`TASK-099`).
#[derive(Clone, Debug)]
pub enum RaftCommand {
    /// Blind insert-or-replace of one record (legacy `replicate_put`).
    Upsert { id: InlineId, frame: AlignedBytes },
    /// Deletion of one record.
    Delete { id: InlineId },
    /// An atomic conditional batch ([`encode_raft_ops`]): puts, deletes, field patches, checks
    /// and rewrites with preconditions, evaluated deterministically at apply time.
    Ops(AlignedBytes),
    /// No-op appended by a new leader so entries of earlier terms commit (Raft §5.4.2, §8).
    Noop,
    /// Removes every record whose TTL expired at `now_ns` (deterministic on every replica).
    SweepExpired { now_ns: u64 },
    /// New cluster configuration (`TASK-121`), effective from the moment it is appended.
    Config(Membership),
    /// Range split (`TASK-138`): this group keeps `[start, at)`; `[at, end)` becomes the new
    /// group `right`, whose replicas are this group's (the data is already in every replica's
    /// engine, so a split moves no data).
    Split { at: Vec<u8>, right: u64 },
}

/// The slice of the key space (raw id bytes, `start <= key < end`) a Raft group replicates.
/// A whole-engine group covers everything.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct KeySpan {
    pub start: Vec<u8>,
    /// `None`: to the end of the key space.
    pub end: Option<Vec<u8>>,
}

impl KeySpan {
    /// Everything.
    pub const fn all() -> Self {
        Self { start: Vec::new(), end: None }
    }

    pub fn new(start: impl Into<Vec<u8>>, end: Option<Vec<u8>>) -> Self {
        Self { start: start.into(), end }
    }

    #[inline]
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.start.as_slice() && self.end.as_deref().is_none_or(|e| key < e)
    }

    #[inline]
    pub fn contains_id(&self, id: InlineId) -> bool {
        self.contains(id.as_bytes())
    }

    pub fn is_all(&self) -> bool {
        self.start.is_empty() && self.end.is_none()
    }

    /// `[start, at)` and `[at, end)`, if `at` lies strictly inside.
    pub fn split_at(&self, at: &[u8]) -> Option<(KeySpan, KeySpan)> {
        if at <= self.start.as_slice() || !self.contains(at) {
            return None;
        }
        Some((KeySpan::new(self.start.clone(), Some(at.to_vec())), KeySpan::new(at.to_vec(), self.end.clone())))
    }

    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&(self.start.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.start);
        match &self.end {
            Some(e) => {
                out.push(1);
                out.extend_from_slice(&(e.len() as u32).to_le_bytes());
                out.extend_from_slice(e);
            }
            None => out.push(0),
        }
    }

    pub fn decode(r: &mut storage_codec::value_wire::SveReader<'_>) -> Result<Self> {
        let start = r.bytes()?.to_vec();
        let end = match r.u8()? {
            0 => None,
            1 => Some(r.bytes()?.to_vec()),
            _ => return Err(StorageError::corrupted_header("bad key span flag")),
        };
        Ok(Self { start, end })
    }
}

/// A split applied by a group, for its host to start the right-hand group.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SplitEvent {
    /// Index of the split entry: the right-hand group starts with this as its snapshot point.
    pub index: u64,
    pub term: u64,
    pub right: u64,
    pub right_span: KeySpan,
    /// Voters of the group at the split (the right-hand group starts with the same).
    pub members: Membership,
}

/// One cluster member: its node id, the address it serves on (Raft RPCs and clients), and
/// whether it votes (a non-voting *learner* only receives the log).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub id: NodeId,
    pub addr: String,
    pub voter: bool,
}

/// Largest cluster configuration accepted (members) and member address length (bytes).
const MAX_MEMBERS: usize = 256;
const MAX_MEMBER_ADDR: usize = 255;

/// A cluster configuration: members sorted by id, ids unique.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Membership {
    members: Vec<Member>,
}

impl Membership {
    /// Builds a configuration; fails on duplicate ids, id `0`, too many members or long addresses.
    pub fn new(mut members: Vec<Member>) -> Result<Self> {
        members.sort_by_key(|m| m.id);
        if members.len() > MAX_MEMBERS
            || members.windows(2).any(|w| w[0].id == w[1].id)
            || members.iter().any(|m| m.id == 0 || m.addr.len() > MAX_MEMBER_ADDR)
        {
            return Err(StorageError::invalid_value(
                "cluster configuration: member ids must be unique and non-zero, addresses at most 255 bytes, at most 256 members",
            ));
        }
        Ok(Self { members })
    }

    /// Every id a voter, with no addresses (static bootstrap from a list of ids).
    pub fn voters_from_ids(ids: impl IntoIterator<Item = NodeId>) -> Result<Self> {
        Self::new(ids.into_iter().map(|id| Member { id, addr: String::new(), voter: true }).collect())
    }

    pub fn members(&self) -> &[Member] {
        &self.members
    }

    pub fn get(&self, id: NodeId) -> Option<&Member> {
        self.members.iter().find(|m| m.id == id)
    }

    pub fn contains(&self, id: NodeId) -> bool {
        self.get(id).is_some()
    }

    pub fn is_voter(&self, id: NodeId) -> bool {
        self.get(id).is_some_and(|m| m.voter)
    }

    pub fn voters(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.members.iter().filter(|m| m.voter).map(|m| m.id)
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// `count u16 | (id u16 | voter u8 | addr_len u16 | addr)*`
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(2 + self.members.len() * 24);
        out.extend_from_slice(&(self.members.len() as u16).to_le_bytes());
        for m in &self.members {
            out.extend_from_slice(&m.id.to_le_bytes());
            out.push(u8::from(m.voter));
            out.extend_from_slice(&(m.addr.len() as u16).to_le_bytes());
            out.extend_from_slice(m.addr.as_bytes());
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let bad = || StorageError::corrupted_header("malformed cluster configuration");
        let count = usize::from(read_u16_le(bytes.get(..2).ok_or_else(bad)?, 0));
        if count > MAX_MEMBERS {
            return Err(bad());
        }
        let mut at = 2usize;
        let mut members = Vec::with_capacity(count);
        for _ in 0..count {
            let head = bytes.get(at..at + 5).ok_or_else(bad)?;
            let len = usize::from(read_u16_le(head, 3));
            let addr = bytes.get(at + 5..at + 5 + len).ok_or_else(bad)?;
            members.push(Member {
                id: read_u16_le(head, 0),
                voter: match head[2] {
                    0 => false,
                    1 => true,
                    _ => return Err(bad()),
                },
                addr: String::from_utf8(addr.to_vec()).map_err(|_| bad())?,
            });
            at += 5 + len;
        }
        if at != bytes.len() {
            return Err(bad());
        }
        Self::new(members).map_err(|_| bad())
    }
}

/// Replicated Raft log entry.
#[derive(Clone, Debug)]
pub struct RaftLogEntry {
    pub term: u64,
    pub index: u64,
    pub command: RaftCommand,
}

impl RaftLogEntry {
    /// Convenience constructor for a blind upsert (`frame`) or deletion (`None`).
    pub fn record(term: u64, index: u64, id: InlineId, frame: Option<AlignedBytes>) -> Self {
        let command = match frame {
            Some(frame) => RaftCommand::Upsert { id, frame },
            None => RaftCommand::Delete { id },
        };
        Self { term, index, command }
    }
}

/// Outcome of applying one entry: the per-operation results, or the deterministic rejection
/// (failed precondition, invalid value, ...) every replica reached identically.
pub type ApplyOutcome = core::result::Result<Vec<WriteResult>, StorageError>;

const OPS_PUT: u8 = 1;
const OPS_PUT_TTL: u8 = 2;
const OPS_DELETE: u8 = 3;
const OPS_PATCH: u8 = 4;
const OPS_CHECK: u8 = 5;
const OPS_REWRITE: u8 = 6;

/// Encodes an atomic batch for [`RaftCommand::Ops`]:
/// `now_ns u64 | count u32 | (id 24B | precondition u8 | version u64 | kind u8 | ttl_ms u64 |
/// len u32 | payload)*`, where `payload` is a record frame (put / rewrite) or encoded field
/// operations (patch).
pub fn encode_raft_ops<T: ZeroCopyRecord>(ops: &[WriteOp<T>], now_ns: u64) -> Result<AlignedBytes> {
    let mut out = Vec::with_capacity(16 + ops.len() * 64);
    out.extend_from_slice(&now_ns.to_le_bytes());
    out.extend_from_slice(&(ops.len() as u32).to_le_bytes());
    for op in ops {
        out.extend_from_slice(&op.id.to_raw_bytes());
        let (pre, ver) = match op.precondition {
            Precondition::None => (0u8, 0u64),
            Precondition::Absent => (1, 0),
            Precondition::Exists => (2, 0),
            Precondition::Version(v) => (3, v.0),
        };
        out.push(pre);
        out.extend_from_slice(&ver.to_le_bytes());
        let mut payload = Vec::new();
        let (kind, ttl_ms) = match &op.kind {
            WriteKind::Put(r) => {
                payload.extend_from_slice(RecordCodec::encode_aligned(r, Version(0), 0, RecordFlags::NONE, 0).as_slice());
                (OPS_PUT, 0u64)
            }
            WriteKind::PutWithTtl(r, ttl) => {
                payload.extend_from_slice(RecordCodec::encode_aligned(r, Version(0), 0, RecordFlags::NONE, 0).as_slice());
                (OPS_PUT_TTL, u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX))
            }
            WriteKind::Delete => (OPS_DELETE, 0),
            WriteKind::Patch(fops) => {
                storage_codec::field_ops::encode_field_ops(fops, &mut payload);
                (OPS_PATCH, 0)
            }
            WriteKind::Check => (OPS_CHECK, 0),
            WriteKind::Rewrite(r) => {
                payload.extend_from_slice(RecordCodec::encode_aligned(r, Version(0), 0, RecordFlags::NONE, 0).as_slice());
                (OPS_REWRITE, 0)
            }
        };
        out.push(kind);
        out.extend_from_slice(&ttl_ms.to_le_bytes());
        let len = u32::try_from(payload.len())
            .map_err(|_| StorageError::invalid_value("raft operation payload too large"))?;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&payload);
    }
    AlignedBytes::try_copy_from_slice(&out)
}

/// Decodes [`encode_raft_ops`] bytes (untrusted: every length is bounds-checked).
pub fn decode_raft_ops<T: ZeroCopyRecord>(bytes: &[u8]) -> Result<(u64, Vec<WriteOp<T>>)> {
    let mut r = storage_codec::value_wire::SveReader::new(bytes);
    let now_ns = r.u64()?;
    // Smallest operation: id 24 | precondition 1 | version 8 | kind 1 | ttl 8 | len 4 (a
    // delete or check has an empty payload).
    let n = r.count(46)?;
    let mut ops = Vec::with_capacity(n);
    for _ in 0..n {
        let id = InlineId::from_raw_bytes(read_array_24(r.take(24)?, 0))?;
        let pre = r.u8()?;
        let ver = Version(r.u64()?);
        let precondition = match pre {
            0 => Precondition::None,
            1 => Precondition::Absent,
            2 => Precondition::Exists,
            3 => Precondition::Version(ver),
            _ => return Err(StorageError::corrupted_header("unknown raft precondition")),
        };
        let kind = r.u8()?;
        let ttl_ms = r.u64()?;
        let len = r.u32()? as usize;
        let payload = r.take(len)?;
        let record = |p: &[u8]| -> Result<T> {
            let aligned = AlignedBytes::try_copy_from_slice(p)?;
            Ok(RecordCodec::decode_owned::<T>(aligned.as_slice())?.1)
        };
        let kind = match kind {
            OPS_PUT => WriteKind::Put(record(payload)?),
            OPS_PUT_TTL => WriteKind::PutWithTtl(record(payload)?, std::time::Duration::from_millis(ttl_ms)),
            OPS_DELETE => WriteKind::Delete,
            OPS_PATCH => {
                let mut pr = storage_codec::value_wire::SveReader::new(payload);
                WriteKind::Patch(storage_codec::field_ops::decode_field_ops(&mut pr)?)
            }
            OPS_CHECK => WriteKind::Check,
            OPS_REWRITE => WriteKind::Rewrite(record(payload)?),
            _ => return Err(StorageError::corrupted_header("unknown raft operation kind")),
        };
        ops.push(WriteOp { id, kind, precondition });
    }
    if r.remaining() != 0 {
        return Err(StorageError::corrupted_header("trailing bytes after raft operations"));
    }
    Ok((now_ns, ops))
}

/// A replica's log and indexes as seen by [`RaftNode::log_view`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftLogView {
    pub snapshot_index: u64,
    pub snapshot_term: u64,
    pub terms: Vec<u64>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub current_term: u64,
    pub role: RaftRole,
}

impl RaftLogView {
    pub fn last_index(&self) -> u64 {
        self.snapshot_index + self.terms.len() as u64
    }

    /// Term of the entry at `index` (`None` if compacted or past the end; the snapshot point
    /// itself answers with the snapshot term).
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == self.snapshot_index {
            return Some(self.snapshot_term);
        }
        let pos = index.checked_sub(self.snapshot_index + 1)?;
        self.terms.get(usize::try_from(pos).ok()?).copied()
    }
}

/// Raft `RequestVote` RPC request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestVoteReq {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
    /// Pre-vote (Raft thesis §9.6): "would you vote for me in `term`?" — answered without
    /// changing any state, so a partitioned node cannot disrupt a healthy cluster when it returns.
    pub pre_vote: bool,
}

impl RequestVoteReq {
    pub fn to_wire_bytes(&self) -> [u8; 27] {
        let mut out = [0u8; 27];
        out[0..8].copy_from_slice(&self.term.to_le_bytes());
        out[8..10].copy_from_slice(&self.candidate_id.to_le_bytes());
        out[10..18].copy_from_slice(&self.last_log_index.to_le_bytes());
        out[18..26].copy_from_slice(&self.last_log_term.to_le_bytes());
        out[26] = u8::from(self.pre_vote);
        out
    }

    /// Accepts the 26-byte format of earlier versions (no pre-vote flag).
    pub fn from_wire_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 26 {
            return Err(StorageError::corrupted_header("RequestVoteReq wire frame < 26B"));
        }
        Ok(Self {
            term: read_u64_le(bytes, 0),
            candidate_id: read_u16_le(bytes, 8),
            last_log_index: read_u64_le(bytes, 10),
            last_log_term: read_u64_le(bytes, 18),
            pre_vote: bytes.get(26).is_some_and(|&b| b != 0),
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

/// Raft `InstallSnapshot` RPC request (Raft §7): one chunk of the leader's snapshot file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstallSnapshotReq {
    pub term: u64,
    pub leader_id: NodeId,
    /// The snapshot replaces every log entry up to and including this index.
    pub last_included_index: u64,
    pub last_included_term: u64,
    /// Byte offset of `data` in the snapshot file.
    pub offset: u64,
    pub data: Vec<u8>,
    /// `true` for the last chunk.
    pub done: bool,
}

/// Fixed part of an `InstallSnapshotReq` on the wire.
const INSTALL_SNAPSHOT_HEADER: usize = 8 + 2 + 8 + 8 + 8 + 1 + 4;

impl InstallSnapshotReq {
    pub fn to_wire_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(INSTALL_SNAPSHOT_HEADER + self.data.len());
        out.extend_from_slice(&self.term.to_le_bytes());
        out.extend_from_slice(&self.leader_id.to_le_bytes());
        out.extend_from_slice(&self.last_included_index.to_le_bytes());
        out.extend_from_slice(&self.last_included_term.to_le_bytes());
        out.extend_from_slice(&self.offset.to_le_bytes());
        out.push(u8::from(self.done));
        out.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.data);
        out
    }

    pub fn from_wire_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < INSTALL_SNAPSHOT_HEADER {
            return Err(StorageError::corrupted_header("InstallSnapshotReq wire frame truncated"));
        }
        let len = read_u32_le(bytes, 35) as usize;
        let data = bytes
            .get(INSTALL_SNAPSHOT_HEADER..)
            .filter(|d| d.len() == len)
            .ok_or_else(|| StorageError::corrupted_header("InstallSnapshotReq data length mismatch"))?;
        Ok(Self {
            term: read_u64_le(bytes, 0),
            leader_id: read_u16_le(bytes, 8),
            last_included_index: read_u64_le(bytes, 10),
            last_included_term: read_u64_le(bytes, 18),
            offset: read_u64_le(bytes, 26),
            done: bytes[34] != 0,
            data: data.to_vec(),
        })
    }
}

/// Raft `InstallSnapshot` RPC response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstallSnapshotResp {
    pub term: u64,
    /// Offset of the next chunk the follower expects (`0`: restart the transfer).
    pub next_offset: u64,
    /// `true` once the follower's state covers the snapshot (installed now or already).
    pub installed: bool,
}

impl InstallSnapshotResp {
    pub fn to_wire_bytes(&self) -> [u8; 17] {
        let mut out = [0u8; 17];
        out[0..8].copy_from_slice(&self.term.to_le_bytes());
        out[8..16].copy_from_slice(&self.next_offset.to_le_bytes());
        out[16] = u8::from(self.installed);
        out
    }

    pub fn from_wire_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 17 {
            return Err(StorageError::corrupted_header("InstallSnapshotResp wire frame < 17B"));
        }
        Ok(Self {
            term: read_u64_le(bytes, 0),
            next_offset: read_u64_le(bytes, 8),
            installed: bytes[16] != 0,
        })
    }
}

/// Largest snapshot chunk sent in one `InstallSnapshot` RPC (`1 MiB`).
pub const RAFT_SNAPSHOT_CHUNK: usize = 1 << 20;

// Snapshot file: `magic | last_index u64 | last_term u64 | count u64 | header_check u64`, then
// (version 2) the cluster configuration `config_index u64 | len u32 | Membership | check u64`
// (`len` 0: none), then (version 3) the key span `len u32 | KeySpan | check u64` the snapshot
// covers (a learner adopts it: its log replays later splits from that span), then `count` x `[id 24B | frame_len u32 | check u64 | frame]` with
// `check = checksum(frame) ^ checksum(id).rotate_left(29)`. Version 1 (no configuration) and 2
// (no span) files are still read.
const SNAPSHOT_MAGIC_V1: &[u8; 8] = b"STGESNP1";
const SNAPSHOT_MAGIC_V2: &[u8; 8] = b"STGESNP2";
const SNAPSHOT_MAGIC: &[u8; 8] = b"STGESNP3";
const SNAPSHOT_HEADER_LEN: usize = 40;
const SNAPSHOT_RECORD_HEADER: usize = 24 + 4 + 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SnapshotMeta {
    last_index: u64,
    last_term: u64,
    count: u64,
    /// 2: a cluster configuration block follows the header; 3: then a key span block.
    version: u8,
}

/// Upper bound of an encoded configuration block.
const MAX_CONFIG_BLOCK: usize = 2 + MAX_MEMBERS * (5 + MAX_MEMBER_ADDR);

fn encode_config_block(config: Option<&(u64, Membership)>) -> Vec<u8> {
    let (index, bytes) = config.map_or((0, Vec::new()), |(i, m)| (*i, m.encode()));
    let mut out = index.to_le_bytes().to_vec();
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(&bytes);
    let check = checksum_payload(&out);
    out.extend_from_slice(&check.to_le_bytes());
    out
}

fn encode_span_block(span: &KeySpan) -> Vec<u8> {
    let mut body = Vec::new();
    span.encode(&mut body);
    let mut out = (body.len() as u32).to_le_bytes().to_vec();
    out.extend_from_slice(&body);
    let check = checksum_payload(&out);
    out.extend_from_slice(&check.to_le_bytes());
    out
}

fn read_span_block(r: &mut impl Read) -> Result<KeySpan> {
    let io = |e| StorageError::io("read raft snapshot span", e);
    let mut head = [0u8; 4];
    r.read_exact(&mut head).map_err(io)?;
    let len = read_u32_le(&head, 0) as usize;
    if len > 256 {
        return Err(StorageError::corrupted_header("raft snapshot span too large"));
    }
    let mut rest = vec![0u8; len + 8];
    r.read_exact(&mut rest).map_err(io)?;
    let mut covered = head.to_vec();
    covered.extend_from_slice(&rest[..len]);
    if checksum_payload(&covered) != read_u64_le(&rest, len) {
        return Err(StorageError::corrupted_header("raft snapshot span checksum mismatch"));
    }
    let mut sr = storage_codec::value_wire::SveReader::new(&rest[..len]);
    let span = KeySpan::decode(&mut sr)?;
    if sr.remaining() != 0 {
        return Err(StorageError::corrupted_header("trailing bytes after raft snapshot span"));
    }
    Ok(span)
}

fn read_config_block(r: &mut impl Read) -> Result<Option<(u64, Membership)>> {
    let io = |e| StorageError::io("read raft snapshot configuration", e);
    let mut head = [0u8; 12];
    r.read_exact(&mut head).map_err(io)?;
    let len = read_u32_le(&head, 8) as usize;
    if len > MAX_CONFIG_BLOCK {
        return Err(StorageError::corrupted_header("raft snapshot configuration too large"));
    }
    let mut rest = vec![0u8; len + 8];
    r.read_exact(&mut rest).map_err(io)?;
    let mut covered = head.to_vec();
    covered.extend_from_slice(&rest[..len]);
    if checksum_payload(&covered) != read_u64_le(&rest, len) {
        return Err(StorageError::corrupted_header("raft snapshot configuration checksum mismatch"));
    }
    if len == 0 {
        return Ok(None);
    }
    Ok(Some((read_u64_le(&head, 0), Membership::decode(&rest[..len])?)))
}

impl SnapshotMeta {
    fn encode(&self) -> [u8; SNAPSHOT_HEADER_LEN] {
        let mut out = [0u8; SNAPSHOT_HEADER_LEN];
        out[0..8].copy_from_slice(SNAPSHOT_MAGIC);
        out[8..16].copy_from_slice(&self.last_index.to_le_bytes());
        out[16..24].copy_from_slice(&self.last_term.to_le_bytes());
        out[24..32].copy_from_slice(&self.count.to_le_bytes());
        let check = checksum_payload(&out[0..32]);
        out[32..40].copy_from_slice(&check.to_le_bytes());
        out
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let hdr = bytes
            .get(..SNAPSHOT_HEADER_LEN)
            .ok_or_else(|| StorageError::corrupted_header("raft snapshot header truncated"))?;
        let version = match &hdr[0..8] {
            m if m == SNAPSHOT_MAGIC => 3,
            m if m == SNAPSHOT_MAGIC_V2 => 2,
            m if m == SNAPSHOT_MAGIC_V1 => 1,
            _ => 0,
        };
        if version == 0 || checksum_payload(&hdr[0..32]) != read_u64_le(hdr, 32) {
            return Err(StorageError::corrupted_header("raft snapshot header invalid"));
        }
        Ok(Self {
            last_index: read_u64_le(hdr, 8),
            last_term: read_u64_le(hdr, 16),
            count: read_u64_le(hdr, 24),
            version,
        })
    }

    /// Header of the snapshot file at `path` (`None` if missing or unreadable).
    fn read(path: &Path) -> Option<Self> {
        let mut hdr = [0u8; SNAPSHOT_HEADER_LEN];
        File::open(path).ok()?.read_exact(&mut hdr).ok()?;
        Self::decode(&hdr).ok()
    }
}

#[inline]
/// Syncs the directory holding `path`, making a rename into it durable.
fn sync_parent(path: &Path) -> std::io::Result<()> {
    match path.parent() {
        Some(dir) => storage_types::vfs::at(dir).sync_dir(dir),
        None => Ok(()),
    }
}

fn snapshot_record_check(id: &[u8; 24], frame: &[u8]) -> u64 {
    checksum_payload(frame) ^ checksum_payload(id).rotate_left(29)
}

/// Streams and verifies every record of the snapshot file at `path` (`O(largest frame)` memory),
/// calling `f(id, frame)`; the frame is 64-byte aligned for zero-copy decoding. Fails on any
/// truncation, checksum mismatch or trailing byte.
/// A snapshot's header, configuration (version 2+) and span (version 3).
type SnapshotContents = (SnapshotMeta, Option<(u64, Membership)>, Option<KeySpan>);

fn for_each_snapshot_record(
    path: &Path,
    mut f: impl FnMut(InlineId, &AlignedBytes) -> Result<()>,
) -> Result<SnapshotContents> {
    let io = |e| StorageError::io("read raft snapshot", e);
    let mut r = BufReader::with_capacity(RAFT_SNAPSHOT_CHUNK, File::open(path).map_err(io)?);
    let mut hdr = [0u8; SNAPSHOT_HEADER_LEN];
    r.read_exact(&mut hdr).map_err(io)?;
    let meta = SnapshotMeta::decode(&hdr)?;
    let config = if meta.version >= 2 { read_config_block(&mut r)? } else { None };
    let span = if meta.version >= 3 { Some(read_span_block(&mut r)?) } else { None };
    let mut frame = Vec::new();
    for _ in 0..meta.count {
        let mut rh = [0u8; SNAPSHOT_RECORD_HEADER];
        r.read_exact(&mut rh).map_err(io)?;
        let id_raw = read_array_24(&rh, 0);
        let len = read_u32_le(&rh, 24);
        if len > storage_types::MAX_PAYLOAD_SIZE + storage_types::HEADER_SIZE as u32 {
            return Err(StorageError::corrupted_header("raft snapshot frame too large"));
        }
        frame.clear();
        frame
            .try_reserve(len as usize)
            .map_err(|_| StorageError::resource_exhausted("raft snapshot frame memory"))?;
        frame.resize(len as usize, 0);
        r.read_exact(&mut frame).map_err(io)?;
        if snapshot_record_check(&id_raw, &frame) != read_u64_le(&rh, 28) {
            return Err(StorageError::corrupted_header("raft snapshot record checksum mismatch"));
        }
        f(InlineId::from_raw_bytes(id_raw)?, &AlignedBytes::try_copy_from_slice(&frame)?)?;
    }
    if r.read(&mut [0u8; 1]).map_err(io)? != 0 {
        return Err(StorageError::corrupted_header("trailing bytes after raft snapshot records"));
    }
    Ok((meta, config, span))
}

// Entry layout: `term u64 | index u64 | id 24B | aux u64 | kind u8 | len u32 | payload`.
// `kind`: 0 delete, 1 upsert (payload = frame), 2 ops batch, 3 no-op, 4 TTL sweep (`aux` =
// `now_ns`), 5 cluster configuration (payload = `Membership::encode`). Kinds 0 and 1 are the
// original format, so existing logs stay readable.
const ENTRY_DELETE: u8 = 0;
const ENTRY_UPSERT: u8 = 1;
const ENTRY_OPS: u8 = 2;
const ENTRY_NOOP: u8 = 3;
const ENTRY_SWEEP: u8 = 4;
const ENTRY_CONFIG: u8 = 5;
const ENTRY_SPLIT: u8 = 6;

fn encode_log_entry(entry: &RaftLogEntry, out: &mut Vec<u8>) {
    out.extend_from_slice(&entry.term.to_le_bytes());
    out.extend_from_slice(&entry.index.to_le_bytes());
    let config;
    let (id, aux, kind, payload): (InlineId, u64, u8, &[u8]) = match &entry.command {
        RaftCommand::Upsert { id, frame } => (*id, entry.index, ENTRY_UPSERT, frame.as_slice()),
        RaftCommand::Delete { id } => (*id, entry.index, ENTRY_DELETE, &[]),
        RaftCommand::Ops(bytes) => (InlineId::EMPTY, 0, ENTRY_OPS, bytes.as_slice()),
        RaftCommand::Noop => (InlineId::EMPTY, 0, ENTRY_NOOP, &[]),
        RaftCommand::SweepExpired { now_ns } => (InlineId::EMPTY, *now_ns, ENTRY_SWEEP, &[]),
        RaftCommand::Config(m) => {
            config = m.encode();
            (InlineId::EMPTY, 0, ENTRY_CONFIG, config.as_slice())
        }
        RaftCommand::Split { at, right } => {
            config = at.clone();
            (InlineId::EMPTY, *right, ENTRY_SPLIT, config.as_slice())
        }
    };
    out.extend_from_slice(&id.to_raw_bytes());
    out.extend_from_slice(&aux.to_le_bytes());
    out.push(kind);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
}

fn decode_log_entry(bytes: &[u8], cursor: usize) -> Result<(RaftLogEntry, usize)> {
    if cursor.checked_add(53).is_none_or(|e| e > bytes.len()) {
        return Err(StorageError::corrupted_header("RaftLogEntry truncated header"));
    }
    let term = read_u64_le(bytes, cursor);
    let index = read_u64_le(bytes, cursor + 8);
    let raw_id = read_array_24(bytes, cursor + 16);
    let aux = read_u64_le(bytes, cursor + 40);
    let kind = bytes[cursor + 48];
    let len = read_u32_le(bytes, cursor + 49) as usize;
    let end = cursor
        .checked_add(53 + len)
        .filter(|&e| e <= bytes.len())
        .ok_or_else(|| StorageError::corrupted_header("RaftLogEntry truncated payload"))?;
    let payload = &bytes[cursor + 53..end];
    let command = match kind {
        ENTRY_UPSERT => RaftCommand::Upsert {
            id: InlineId::from_raw_bytes(raw_id)?,
            frame: AlignedBytes::try_copy_from_slice(payload)?,
        },
        ENTRY_DELETE => RaftCommand::Delete { id: InlineId::from_raw_bytes(raw_id)? },
        ENTRY_OPS => RaftCommand::Ops(AlignedBytes::try_copy_from_slice(payload)?),
        ENTRY_NOOP => RaftCommand::Noop,
        ENTRY_SWEEP => RaftCommand::SweepExpired { now_ns: aux },
        ENTRY_CONFIG => RaftCommand::Config(Membership::decode(payload)?),
        ENTRY_SPLIT => RaftCommand::Split { at: payload.to_vec(), right: aux },
        _ => return Err(StorageError::corrupted_header("unknown RaftLogEntry kind")),
    };
    Ok((RaftLogEntry { term, index, command }, end))
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
    /// Cluster configuration in force at the compaction point, with the index of the entry that
    /// set it (persisted in `<stem>_raft.members`).
    base_config: Option<(u64, Membership)>,
    /// Configuration entries currently in `log`, in log order (kept in step with `log`).
    configs: Vec<(u64, Membership)>,
    /// Configuration used when neither the log nor the compaction point has one (the static
    /// member list a node is started with; empty for a node joining an existing cluster).
    bootstrap: Membership,
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
            base_config: None,
            configs: Vec::new(),
            bootstrap: Membership::default(),
        }
    }

    // Every change to `log` goes through these four so `configs` stays in step with it.

    fn push_entry(&mut self, entry: RaftLogEntry) {
        if let RaftCommand::Config(m) = &entry.command {
            self.configs.push((entry.index, m.clone()));
        }
        self.log.push(entry);
    }

    /// Drops `log[pos..]` (a conflicting suffix): configurations in it are rolled back.
    fn truncate_log(&mut self, pos: usize) {
        if let Some(first) = self.log.get(pos).map(|e| e.index) {
            self.configs.retain(|(i, _)| *i < first);
        }
        self.log.truncate(pos);
    }

    /// Drops `log[..count]` (compaction / snapshot install); the latest configuration among the
    /// dropped entries becomes the base configuration.
    fn drain_log(&mut self, count: usize) {
        let count = count.min(self.log.len());
        let Some(last) = count.checked_sub(1).map(|i| self.log[i].index) else {
            return;
        };
        let keep = self.configs.iter().position(|(i, _)| *i > last).unwrap_or(self.configs.len());
        if let Some(latest) = self.configs.drain(..keep).next_back() {
            self.base_config = Some(latest);
        }
        self.log.drain(..count);
    }

    fn clear_log(&mut self) {
        self.configs.clear();
        self.log.clear();
    }

    /// The configuration in force (latest appended), with the index that set it (`0`: bootstrap).
    fn membership(&self) -> (u64, &Membership) {
        self.configs
            .last()
            .or(self.base_config.as_ref())
            .map_or((0, &self.bootstrap), |(i, m)| (*i, m))
    }

    /// The latest configuration at or before `index` (what a snapshot at `index` carries).
    fn membership_at(&self, index: u64) -> Option<(u64, Membership)> {
        self.configs
            .iter()
            .rev()
            .find(|(i, _)| *i <= index)
            .or(self.base_config.as_ref().filter(|(i, _)| *i <= index))
            .cloned()
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
    /// This node's snapshot (generated as leader, or the last one installed as follower).
    snapshot_path: PathBuf,
    /// Snapshot being received from the leader.
    recv_path: PathBuf,
    /// Present while a received snapshot is being applied to the engine.
    installing_path: PathBuf,
    /// Cluster configuration at the compaction point (`TASK-121`).
    members_path: PathBuf,
    /// Key span of a range group (`TASK-137`); absent for a whole-engine group.
    span_path: PathBuf,
}

impl RaftStorage {
    fn for_group(dir: &Path, stem: &str, group: Option<u64>) -> Self {
        let stem = match group {
            Some(g) => format!("{stem}_r{g}"),
            None => stem.to_string(),
        };
        Self {
            meta_path: dir.join(format!("{stem}_raft.meta")),
            log_path: dir.join(format!("{stem}_raft.log")),
            snapshot_path: dir.join(format!("{stem}_raft.snapshot")),
            recv_path: dir.join(format!("{stem}_raft.snapshot.recv")),
            installing_path: dir.join(format!("{stem}_raft.installing")),
            members_path: dir.join(format!("{stem}_raft.members")),
            span_path: dir.join(format!("{stem}_raft.span")),
        }
    }

    fn save_span(&self, span: &KeySpan) -> Result<()> {
        let mut body = Vec::new();
        span.encode(&mut body);
        let mut full = Vec::with_capacity(12 + body.len());
        full.extend_from_slice(&RAFT_SPAN_MAGIC.to_le_bytes());
        full.extend_from_slice(&checksum_payload(&body).to_le_bytes());
        full.extend_from_slice(&body);
        atomic_write_replace(&self.span_path, &full, true)
    }

    fn load_span(&self) -> Result<Option<KeySpan>> {
        if !self.span_path.exists() {
            return Ok(None);
        }
        let raw = fs::read(&self.span_path).map_err(|e| StorageError::io("read raft span", e))?;
        if raw.len() < 12 || read_u32_le(&raw, 0) != RAFT_SPAN_MAGIC {
            return Err(StorageError::corrupted_header("raft span file invalid"));
        }
        let expected = read_u64_le(&raw, 4);
        let actual = checksum_payload(&raw[12..]);
        if expected != actual {
            return Err(StorageError::checksum_mismatch(expected, actual));
        }
        let mut r = storage_codec::value_wire::SveReader::new(&raw[12..]);
        let span = KeySpan::decode(&mut r)?;
        if r.remaining() != 0 {
            return Err(StorageError::corrupted_header("trailing bytes after raft span"));
        }
        Ok(Some(span))
    }
}

const MEMBERS_MAGIC: u32 = 0x4d46_5452; // "RTFM": raft members

impl RaftStorage {
    /// Persists the configuration at the compaction point (before the entries carrying it are
    /// dropped from the log): `magic u32 | check u64 | index u64 | Membership::encode`.
    fn save_members(&self, base: &(u64, Membership)) -> Result<()> {
        let mut payload = base.0.to_le_bytes().to_vec();
        payload.extend_from_slice(&base.1.encode());
        let mut full = MEMBERS_MAGIC.to_le_bytes().to_vec();
        full.extend_from_slice(&checksum_payload(&payload).to_le_bytes());
        full.extend_from_slice(&payload);
        atomic_write_replace(&self.members_path, &full, true)
    }

    fn load_members(&self) -> Result<Option<(u64, Membership)>> {
        if !self.members_path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&self.members_path).map_err(|e| StorageError::io("read raft members", e))?;
        if bytes.len() < 20 || read_u32_le(&bytes, 0) != MEMBERS_MAGIC {
            return Err(StorageError::corrupted_header("raft members file invalid"));
        }
        let payload = &bytes[12..];
        let (expected, actual) = (read_u64_le(&bytes, 4), checksum_payload(payload));
        if expected != actual {
            return Err(StorageError::checksum_mismatch(expected, actual));
        }
        Ok(Some((read_u64_le(payload, 0), Membership::decode(&payload[8..])?)))
    }
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
        let vfs = storage_types::vfs::at(&self.log_path);
        let created = !vfs.exists(&self.log_path);
        let mut file = vfs.open(&self.log_path, storage_types::vfs::OpenOpts::append())
            .map_err(|e| StorageError::io("open raft log", e))?;
        file.write_all(&buf)
            .map_err(|e| StorageError::io("append raft log", e))?;
        file.sync_data()
            .map_err(|e| StorageError::io("fsync raft log", e))?;
        if created {
            // Acknowledged entries in a new log file survive a power cut only once its
            // directory entry is durable too.
            sync_parent(&self.log_path).map_err(|e| StorageError::io("fsync raft log directory", e))?;
        }
        Ok(())
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
        st.base_config = self.load_members()?;
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
                st.push_entry(entry);
            }
            if pos < raw.len() {
                self.rewrite_log(&st.log)?;
            }
        } else if !legacy_log.is_empty() {
            for entry in legacy_log {
                st.push_entry(entry);
            }
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
    /// Serializes snapshot generation (leader) and chunk reception (follower).
    snapshot_lock: Mutex<()>,
    /// Outcomes of applied entries a local proposer is waiting for (only awaited indexes are
    /// kept, so memory stays bounded by in-flight proposals).
    outcomes: Mutex<Outcomes>,
    /// Signalled whenever entries are applied.
    applied_cv: Condvar,
    /// Last time a valid leader contacted this node or this node granted a vote (election timer).
    last_contact: Mutex<Instant>,
    /// When set, votes are refused while a leader was heard from within this window ("leader
    /// stickiness", Raft thesis §4.2.3), which keeps leader leases safe.
    vote_stickiness: Mutex<Option<Duration>>,
    /// Range group id (`None`: the group replicates the whole engine).
    group: Option<u64>,
    /// The keys this group replicates; shrinks when the group splits.
    span: std::sync::RwLock<KeySpan>,
    /// Splits applied since the host last asked ([`Self::take_splits`]).
    splits: Mutex<Vec<SplitEvent>>,
    /// Engine directory and file stem (to create right-hand groups on a split).
    stem: String,
}

#[derive(Debug, Default)]
struct Outcomes {
    /// Index -> term of the entry the local proposer appended there. An entry applied at that
    /// index with another term replaced the proposal (leader change): its outcome is not ours.
    awaited: FastHashMap<u64, u64>,
    done: FastHashMap<u64, ApplyOutcome>,
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
        Self::open_group(node_id, engine, None, KeySpan::all())
    }

    /// Opens (or creates) range group `group` over the part `span` of a shared engine
    /// (`TASK-137`): its own log and hard state, the engine (and its WAL) shared with every
    /// other group on this node. A persisted span (after splits) wins over `span`.
    pub fn open_group(node_id: NodeId, engine: Arc<StorageEngine<T>>, group: Option<u64>, span: KeySpan) -> Result<Self> {
        let stem = crate::engine::collection_file_stem(engine.collection_name())?;
        let dir = engine.base_dir().to_path_buf();
        let storage = RaftStorage::for_group(&dir, &stem, group);
        let span = match storage.load_span()? {
            Some(saved) => saved,
            None => {
                if group.is_some() {
                    storage.save_span(&span)?;
                }
                span
            }
        };
        let initial_state = storage.load()?.unwrap_or_else(RaftState::new);
        let node = Self {
            node_id,
            engine,
            storage,
            state: Mutex::new(initial_state),
            apply_lock: Mutex::new(()),
            snapshot_lock: Mutex::new(()),
            outcomes: Mutex::new(Outcomes::default()),
            applied_cv: Condvar::new(),
            last_contact: Mutex::new(Instant::now()),
            vote_stickiness: Mutex::new(None),
            group,
            span: std::sync::RwLock::new(span),
            splits: Mutex::new(Vec::new()),
            stem,
        };
        // A replicated engine changes only through consensus: refuse direct writes
        // (`ReadOnlyReplica`) on every replica, leader included.
        node.engine.set_replica_mode(true);
        if node.storage.installing_path.exists() {
            node.recover_interrupted_install()?;
        }
        // Finish applying entries that were committed but not yet applied before a restart.
        node.apply_committed()?;
        Ok(node)
    }

    #[inline]
    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Range group id (`None` for a whole-engine group).
    pub const fn group(&self) -> Option<u64> {
        self.group
    }

    /// The keys this group replicates.
    pub fn span(&self) -> KeySpan {
        self.span.read().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// Splits applied since the last call (the host starts the right-hand groups).
    pub fn take_splits(&self) -> Vec<SplitEvent> {
        std::mem::take(&mut *self.splits.lock().unwrap_or_else(|p| p.into_inner()))
    }

    /// Range groups persisted in `engine`'s directory (`(group id, span)`), to reopen at start.
    pub fn persisted_groups(engine: &StorageEngine<T>) -> Result<Vec<u64>> {
        let stem = crate::engine::collection_file_stem(engine.collection_name())?;
        let prefix = format!("{stem}_r");
        let mut out = Vec::new();
        let Ok(entries) = fs::read_dir(engine.base_dir()) else { return Ok(out) };
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if let Some(id) = name.strip_prefix(&prefix).and_then(|r| r.strip_suffix("_raft.span")).and_then(|d| d.parse().ok()) {
                out.push(id);
            }
        }
        out.sort_unstable();
        Ok(out)
    }

    /// Removes this replica from its node after it left the group (`TASK-138`): the span's
    /// records are deleted from the shared engine first, then the group's files. A crash in
    /// between leaves a replica that is not in its own configuration; its host removes it
    /// again at the next start.
    pub fn destroy(&self) -> Result<()> {
        self.destroy_keeping(|_| false)
    }

    /// [`Self::destroy`], keeping the records `keep` says another local replica still holds
    /// (spans of different groups on one node can overlap while one of them catches up).
    pub fn destroy_keeping(&self, keep: impl Fn(InlineId) -> bool) -> Result<()> {
        let _apply = self.apply_lock.lock().unwrap_or_else(|p| p.into_inner());
        if self.group.is_none() {
            return Err(StorageError::invalid_key("only range groups can be destroyed"));
        }
        for id in self.span_ids() {
            if !keep(id) {
                let _ = self.engine.delete_internal(id)?;
            }
        }
        self.engine.sync_wal()?;
        let st = &self.storage;
        for p in [&st.span_path, &st.meta_path, &st.log_path, &st.members_path, &st.snapshot_path, &st.recv_path, &st.installing_path] {
            if p.exists() {
                storage_types::vfs::at(p).remove_file(p).map_err(|e| StorageError::io("remove raft group file", e))?;
            }
        }
        sync_parent(&st.span_path).map_err(|e| StorageError::io("fsync raft group directory", e))
    }

    /// Ids of live records in this group's span.
    fn span_ids(&self) -> Vec<InlineId> {
        let span = self.span();
        let ids = self.engine.live_ids();
        if span.is_all() {
            return ids;
        }
        ids.into_iter().filter(|id| span.contains_id(*id)).collect()
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
            pre_vote: false,
        })
    }

    /// A pre-vote request for the next term (changes nothing locally).
    pub fn pre_vote_request(&self) -> RequestVoteReq {
        let st = self.lock_state();
        let (last_log_index, last_log_term) = st.last_log_index_and_term();
        RequestVoteReq {
            term: st.current_term + 1,
            candidate_id: self.node_id,
            last_log_index,
            last_log_term,
            pre_vote: true,
        }
    }

    /// Processes a `RequestVoteReq` according to the Raft Election Safety and Log Up-To-Date rules.
    /// A vote is only granted after it has been durably persisted.
    pub fn handle_request_vote(&self, req: RequestVoteReq) -> RequestVoteResp {
        let sticky = *self.vote_stickiness.lock().unwrap_or_else(|p| p.into_inner());
        let mut st = self.lock_state();
        if let Some(window) = sticky {
            let heard_from_leader = st.leader_id.is_some_and(|l| l != req.candidate_id)
                && (st.role == RaftRole::Leader || self.since_contact() < window);
            if heard_from_leader {
                // A live leader exists: do not help depose it (and do not adopt the term).
                return RequestVoteResp { term: st.current_term, vote_granted: false };
            }
        }
        if req.pre_vote {
            let (my_last_idx, my_last_term) = st.last_log_index_and_term();
            let up_to_date = req.last_log_term > my_last_term
                || (req.last_log_term == my_last_term && req.last_log_index >= my_last_idx);
            return RequestVoteResp {
                term: st.current_term,
                vote_granted: req.term > st.current_term && up_to_date,
            };
        }
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
                self.touch_contact();
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
            // A no-op of the new term lets earlier-term entries commit (Raft §5.4.2) and gives
            // read-index a committed entry of the current term (§8).
            let (prev, _) = st.last_log_index_and_term();
            let entry = RaftLogEntry { term: st.current_term, index: prev + 1, command: RaftCommand::Noop };
            if self.storage.append_log(core::slice::from_ref(&entry)).is_err() {
                st.role = RaftRole::Follower;
                st.leader_id = None;
                return false;
            }
            st.push_entry(entry);
            true
        } else {
            false
        }
    }

    /// Leader: appends `command` to the durable log and returns its index. The caller replicates
    /// it and waits with [`Self::wait_applied`].
    pub fn propose(&self, command: RaftCommand) -> Result<u64> {
        let mut st = self.lock_state();
        if st.role != RaftRole::Leader {
            return Err(StorageError::NotLeader { leader: st.leader_id.map(u64::from) });
        }
        let (prev, _) = st.last_log_index_and_term();
        let entry = RaftLogEntry { term: st.current_term, index: prev + 1, command };
        self.storage.append_log(core::slice::from_ref(&entry))?;
        let term = entry.term;
        st.push_entry(entry);
        self.outcomes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .awaited
            .insert(prev + 1, term);
        Ok(prev + 1)
    }

    /// Leader: proposes an atomic conditional batch (see [`StorageEngine::commit`]) stamped with
    /// the current time (TTL decisions use it on every replica).
    pub fn propose_ops(&self, ops: &[WriteOp<T>]) -> Result<u64> {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
        self.propose_ops_at(ops, now_ns)
    }

    /// [`Self::propose_ops`] stamped with `now_ns` (the simulator passes its own clock).
    pub fn propose_ops_at(&self, ops: &[WriteOp<T>], now_ns: u64) -> Result<u64> {
        self.propose(RaftCommand::Ops(encode_raft_ops(ops, now_ns)?))
    }

    /// Waits up to `timeout` until the entry at `index` is applied and returns its outcome.
    /// On timeout the outcome is unknown ([`StorageError::CommitUnknown`]): the entry may still
    /// commit. If another entry replaced it (leader change), the result is `CommitUnknown` too.
    pub fn wait_applied(&self, index: u64, timeout: Duration) -> Result<Vec<WriteResult>> {
        let deadline = Instant::now() + timeout;
        loop {
            // Never hold `outcomes` while taking `state` (`propose` locks `state -> outcomes`).
            let applied = self.last_applied() >= index;
            let mut out = self.outcomes.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(result) = out.done.remove(&index) {
                out.awaited.remove(&index);
                return result;
            }
            if applied {
                // Applied without an outcome for us: our entry was replaced after a leader change.
                out.awaited.remove(&index);
                return Err(StorageError::CommitUnknown { index });
            }
            let now = Instant::now();
            if now >= deadline {
                out.awaited.remove(&index);
                return Err(StorageError::CommitUnknown { index });
            }
            // Bounded wait: re-check `last_applied` (outside this lock) at least every 50 ms.
            let wait = (deadline - now).min(Duration::from_millis(50));
            drop(
                self.applied_cv
                    .wait_timeout(out, wait)
                    .unwrap_or_else(|p| p.into_inner()),
            );
        }
    }

    /// Refuse votes while a leader was heard from within `window` (`None` disables). The networked
    /// runtime sets this to the minimum election timeout so a leader lease can never overlap a
    /// newer leader.
    pub fn set_vote_stickiness(&self, window: Option<Duration>) {
        *self.vote_stickiness.lock().unwrap_or_else(|p| p.into_inner()) = window;
    }

    /// Records that a valid leader (or a candidate we voted for) contacted this node now.
    #[inline]
    pub fn touch_contact(&self) {
        *self.last_contact.lock().unwrap_or_else(|p| p.into_inner()) = Instant::now();
    }

    /// Time since the last valid leader contact or granted vote.
    pub fn since_contact(&self) -> Duration {
        self.last_contact.lock().unwrap_or_else(|p| p.into_inner()).elapsed()
    }

    /// The log as terms, for invariant checks (`F-SIM-02`): entry `snapshot_index + 1 + i`
    /// has term `terms[i]`.
    pub fn log_view(&self) -> RaftLogView {
        let st = self.lock_state();
        RaftLogView {
            snapshot_index: st.snapshot_index,
            snapshot_term: st.snapshot_term,
            terms: st.log.iter().map(|e| e.term).collect(),
            commit_index: st.commit_index,
            last_applied: st.last_applied,
            current_term: st.current_term,
            role: st.role,
        }
    }

    /// Current leader known to this node.
    pub fn leader_id(&self) -> Option<NodeId> {
        self.lock_state().leader_id
    }

    /// Steps down to follower if `term` is newer than ours (seen in an RPC response).
    pub fn observe_term(&self, term: u64) -> Result<bool> {
        let mut st = self.lock_state();
        if term > st.current_term {
            st.current_term = term;
            st.role = RaftRole::Follower;
            st.voted_for = None;
            st.leader_id = None;
            self.storage.save_hard_state(&st)?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Appends a blind upsert (`Some`) or deletion (`None`) of `id` to the leader's durable log
    /// (prior to replication).
    pub fn append_leader_entry(
        &self,
        id: InlineId,
        record: Option<&T>,
    ) -> Result<(RaftLogEntry, u64, u64)> {
        let mut st = self.lock_state();
        if st.role != RaftRole::Leader {
            return Err(StorageError::NotLeader { leader: st.leader_id.map(u64::from) });
        }
        let (prev_idx, prev_term) = st.last_log_index_and_term();
        let next_idx = prev_idx + 1;
        let frame = record.map(|rec| {
            RecordCodec::encode_aligned(rec, Version(next_idx), 0, RecordFlags::NONE, 0)
        });
        let entry = RaftLogEntry::record(st.current_term, next_idx, id, frame);
        self.storage.append_log(core::slice::from_ref(&entry))?;
        st.push_entry(entry.clone());
        self.outcomes.lock().unwrap_or_else(|p| p.into_inner()).awaited.insert(next_idx, entry.term);
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
                "follower is behind the compacted Raft log: send it a snapshot (build_install_snapshot)",
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

    /// [`Self::build_append_entries_from`] carrying at most `max_entries` entries and roughly
    /// `max_bytes` of payload (at least one entry), so a long-lagging follower is caught up in
    /// bounded messages.
    pub fn build_append_entries_bounded(
        &self,
        from_index: u64,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<AppendEntriesReq> {
        let st = self.lock_state();
        if st.role != RaftRole::Leader {
            return Err(StorageError::NotLeader { leader: st.leader_id.map(u64::from) });
        }
        let from_index = from_index.max(1);
        let prev_log_index = (from_index - 1).min(st.last_log_index());
        if prev_log_index < st.snapshot_index {
            return Err(StorageError::invalid_key(
                "follower is behind the compacted Raft log: send it a snapshot (build_install_snapshot)",
            ));
        }
        let prev_log_term = st.term_at(prev_log_index).unwrap_or(0);
        let start = st.pos_of(prev_log_index + 1).unwrap_or(0).min(st.log.len());
        let mut end = start;
        let mut bytes = 0usize;
        while end < st.log.len() && end - start < max_entries.max(1) {
            let size = match &st.log[end].command {
                RaftCommand::Upsert { frame, .. } => frame.len(),
                RaftCommand::Ops(b) => b.len(),
                _ => 0,
            } + 53;
            if end > start && bytes + size > max_bytes {
                break;
            }
            bytes += size;
            end += 1;
        }
        Ok(AppendEntriesReq {
            term: st.current_term,
            leader_id: self.node_id,
            prev_log_index,
            prev_log_term,
            entries: st.log[start..end].to_vec(),
            leader_commit: st.commit_index,
        })
    }

    // ── Membership (`TASK-121`) ─────────────────────────────────────────────────────────────

    /// Sets the configuration used while neither the log nor the compaction point has one: the
    /// static member list the node is started with (empty for a node joining a cluster).
    pub fn set_bootstrap_membership(&self, members: Membership) {
        self.lock_state().bootstrap = members;
    }

    /// Persists the first configuration of a new group (a range created on this node): unlike
    /// [`Self::set_bootstrap_membership`] it survives restarts. A no-op once the group has a
    /// configuration or any log.
    pub fn initialize_membership(&self, members: Membership) -> Result<()> {
        let mut st = self.lock_state();
        if st.base_config.is_some() || !st.log.is_empty() || st.snapshot_index != 0 {
            return Ok(());
        }
        self.storage.save_members(&(0, members.clone()))?;
        st.base_config = Some((0, members));
        self.storage.save_hard_state(&st)
    }

    /// The configuration in force on this node (the latest appended one).
    pub fn membership(&self) -> Membership {
        self.lock_state().membership().1.clone()
    }

    /// The configuration in force and whether it is committed (no change in flight).
    /// Log index of the configuration in force (0: the initial one).
    pub fn config_index(&self) -> u64 {
        self.lock_state().membership().0
    }

    pub fn membership_state(&self) -> (Membership, bool) {
        let st = self.lock_state();
        let (index, m) = st.membership();
        (m.clone(), index <= st.commit_index)
    }

    /// Leader: proposes the configuration `next` and returns its log index. Refused unless the
    /// previous change is committed, this leader has committed an entry of its own term (Raft
    /// thesis §4.1, the single-server change fix), and `next` changes at most one voter.
    pub fn propose_membership(&self, next: Membership) -> Result<u64> {
        {
            let st = self.lock_state();
            if st.role != RaftRole::Leader {
                return Err(StorageError::NotLeader { leader: st.leader_id.map(u64::from) });
            }
            let (index, current) = st.membership();
            if index > st.commit_index {
                return Err(StorageError::invalid_value(
                    "a cluster membership change is already in progress; retry once it commits",
                ));
            }
            if st.term_at(st.commit_index) != Some(st.current_term) {
                return Err(StorageError::invalid_value(
                    "the new leader has not committed an entry of its term yet; retry shortly",
                ));
            }
            let changed = current
                .voters()
                .filter(|&v| !next.is_voter(v))
                .chain(next.voters().filter(|&v| !current.is_voter(v)))
                .count();
            if changed > 1 {
                return Err(StorageError::invalid_value(
                    "cluster membership changes add, promote or remove one voter at a time",
                ));
            }
            if next.voters().next().is_none() {
                return Err(StorageError::invalid_value("a cluster needs at least one voter"));
            }
        }
        // `propose` re-checks leadership under the same lock it appends with.
        self.propose(RaftCommand::Config(next))
    }

    /// `true` while this node believes it is the leader.
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.lock_state().role == RaftRole::Leader
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
            self.touch_contact();

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

            // The last index this request vouches for. Entries past it (a stale suffix from an
            // older term) are unverified: they may neither commit nor count as matching.
            let last_new = (req.prev_log_index + req.entries.len() as u64).max(st.snapshot_index);
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
                    st.truncate_log(pos);
                    truncated = true;
                }
                new_from.get_or_insert(st.log.len());
                st.push_entry(entry);
            }
            if truncated {
                self.storage.rewrite_log(&st.log)?;
            } else if let Some(from) = new_from {
                self.storage.append_log(&st.log[from..])?;
            }

            let match_index = last_new.min(st.last_log_index());
            if req.leader_commit > st.commit_index && req.leader_commit.min(match_index) > st.commit_index {
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
        // A leader that the committed configuration no longer counts as a voter steps down
        // (it kept replicating until then, without counting itself, Raft thesis §4.2.2).
        let mut st = self.lock_state();
        let (index, m) = st.membership();
        // (An empty configuration means none is known: the in-process group is unmanaged.)
        if index <= st.commit_index && !m.is_empty() && !m.is_voter(self.node_id) && st.role == RaftRole::Leader {
            st.role = RaftRole::Follower;
            st.leader_id = None;
        }
        Ok(st.commit_index)
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
            let outcome = self.apply_entry_to_engine(&entry)?;
            // Publish the outcome BEFORE advancing `last_applied`: a waiter that observes the
            // index as applied is then guaranteed to find its result. Lock order is always
            // `state -> outcomes`; neither lock is held while taking the other here.
            {
                let mut out = self.outcomes.lock().unwrap_or_else(|p| p.into_inner());
                if out.awaited.get(&entry.index) == Some(&entry.term) {
                    out.done.insert(entry.index, outcome);
                }
            }
            {
                let mut st = self.lock_state();
                st.last_applied = st.last_applied.max(entry.index);
                self.storage.save_hard_state(&st)?;
            }
            // Notify under the outcomes lock so a waiter between its check and its wait cannot
            // miss the wake-up.
            let _out = self.outcomes.lock().unwrap_or_else(|p| p.into_inner());
            self.applied_cv.notify_all();
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
    /// A follower whose next needed entry was compacted catches up through `InstallSnapshot`
    /// ([`Self::build_install_snapshot`]).
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
        // The configuration at the new compaction point outlives the entries that carry it.
        if let Some(base) = st.membership_at(index).filter(|b| Some(b) != st.base_config.as_ref()) {
            self.storage.save_members(&base)?;
        }
        st.snapshot_index = index;
        st.snapshot_term = term;
        // Compaction point first, then the shorter log: the loader skips covered entries, so a
        // crash between the two writes is harmless.
        self.storage.save_hard_state(&st)?;
        st.drain_log(drop_count);
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

    // ── InstallSnapshot (Raft §7, `TASK-089`) ──────────────────────────────────────────────

    /// `true` if a follower whose next needed entry is `next_index` can only be caught up with a
    /// snapshot (that entry was compacted away).
    pub fn needs_snapshot(&self, next_index: u64) -> bool {
        next_index <= self.lock_state().snapshot_index
    }

    /// Leader: chunk `[offset, offset + max_chunk)` of a snapshot covering at least the
    /// compaction point. The snapshot is (re)generated when none covers it.
    pub fn build_install_snapshot(&self, offset: u64, max_chunk: usize) -> Result<InstallSnapshotReq> {
        self.build_install_snapshot_from(offset, max_chunk, 0)
    }

    /// [`Self::build_install_snapshot`] of a snapshot taken at or after log index `min_index`
    /// (a new replica must start from a state that includes its own addition).
    pub fn build_install_snapshot_from(&self, offset: u64, max_chunk: usize, min_index: u64) -> Result<InstallSnapshotReq> {
        let (term, needed) = {
            let st = self.lock_state();
            if st.role != RaftRole::Leader {
                return Err(StorageError::invalid_key("node is not Raft Leader"));
            }
            (st.current_term, st.snapshot_index.max(min_index))
        };
        let _gen = self.snapshot_lock.lock().unwrap_or_else(|p| p.into_inner());
        let meta = match SnapshotMeta::read(&self.storage.snapshot_path) {
            Some(m) if m.last_index >= needed => m,
            _ => self.generate_snapshot()?,
        };
        let io = |e| StorageError::io("read raft snapshot chunk", e);
        let mut file = File::open(&self.storage.snapshot_path).map_err(io)?;
        let total = file.metadata().map_err(io)?.len();
        if offset > total {
            return Err(StorageError::invalid_key("snapshot chunk offset beyond the snapshot"));
        }
        let len = (total - offset).min(max_chunk.max(1) as u64) as usize;
        let mut data = Vec::new();
        data.try_reserve_exact(len)
            .map_err(|_| StorageError::resource_exhausted("raft snapshot chunk memory"))?;
        data.resize(len, 0);
        file.seek(SeekFrom::Start(offset)).map_err(io)?;
        file.read_exact(&mut data).map_err(io)?;
        Ok(InstallSnapshotReq {
            term,
            leader_id: self.node_id,
            last_included_index: meta.last_index,
            last_included_term: meta.last_term,
            offset,
            done: offset + len as u64 == total,
            data,
        })
    }

    /// Writes a point-in-time snapshot of the engine at `last_applied` (caller holds
    /// `snapshot_lock`). The cut is taken under `apply_lock` with an MVCC read snapshot, then
    /// streamed to disk without blocking the apply path; memory is `O(ids)`.
    ///
    /// Assumes the replicated engine is written only through Raft (as replication requires).
    fn generate_snapshot(&self) -> Result<SnapshotMeta> {
        let (last_index, last_term, config, span, read, ids) = {
            let _apply = self.apply_lock.lock().unwrap_or_else(|p| p.into_inner());
            let st = self.lock_state();
            let last_index = st.last_applied;
            let last_term = st
                .term_at(last_index)
                .ok_or_else(|| StorageError::corrupted_header("raft snapshot point is missing"))?;
            let config = st.membership_at(last_index);
            drop(st);
            (last_index, last_term, config, self.span(), self.engine.begin_read_snapshot(), self.span_ids())
        };
        let mut tmp = self.storage.snapshot_path.as_os_str().to_os_string();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        let io = |e| StorageError::io("write raft snapshot", e);
        let vfs = storage_types::vfs::at(&tmp);
        let mut w = BufWriter::with_capacity(RAFT_SNAPSHOT_CHUNK, vfs.open(&tmp, OpenOpts::create_truncate()).map_err(io)?);
        w.write_all(&[0u8; SNAPSHOT_HEADER_LEN]).map_err(io)?;
        w.write_all(&encode_config_block(config.as_ref())).map_err(io)?;
        w.write_all(&encode_span_block(&span)).map_err(io)?;
        let mut count = 0u64;
        for id in ids {
            let Some((version, ttl, record)) = read.get_with_ttl(id)? else {
                continue;
            };
            let flags = if ttl > 0 { RecordFlags::HAS_TTL } else { RecordFlags::NONE };
            let frame = RecordCodec::encode_aligned(&record, version, 0, flags, ttl);
            let id_raw = id.to_raw_bytes();
            let len = u32::try_from(frame.len())
                .map_err(|_| StorageError::invalid_archive("snapshot frame exceeds u32"))?;
            w.write_all(&id_raw).map_err(io)?;
            w.write_all(&len.to_le_bytes()).map_err(io)?;
            w.write_all(&snapshot_record_check(&id_raw, &frame).to_le_bytes()).map_err(io)?;
            w.write_all(&frame).map_err(io)?;
            count += 1;
        }
        drop(read);
        let meta = SnapshotMeta { last_index, last_term, count, version: 3 };
        let mut file = w.into_inner().map_err(|e| io(e.into_error()))?;
        file.seek(SeekFrom::Start(0))
            .and_then(|_| file.write_all(&meta.encode()))
            .and_then(|()| file.sync_all())
            .map_err(io)?;
        drop(file);
        vfs.rename(&tmp, &self.storage.snapshot_path).map_err(io)?;
        // The log is compacted behind this snapshot next: its name must survive a power cut.
        sync_parent(&self.storage.snapshot_path).map_err(io)?;
        Ok(meta)
    }

    /// Follower: stores one snapshot chunk; on the last one, verifies the whole snapshot and
    /// installs it (engine state replaced, log discarded up to `last_included_index`, Raft §7).
    pub fn handle_install_snapshot(&self, req: InstallSnapshotReq) -> Result<InstallSnapshotResp> {
        let term = {
            let mut st = self.lock_state();
            if req.term < st.current_term {
                return Ok(InstallSnapshotResp { term: st.current_term, next_offset: 0, installed: false });
            }
            if req.term > st.current_term {
                st.current_term = req.term;
                st.voted_for = None;
                self.storage.save_hard_state(&st)?;
            }
            st.role = RaftRole::Follower;
            st.leader_id = Some(req.leader_id);
            self.touch_contact();
            if req.last_included_index <= st.last_applied {
                // Our state already covers the snapshot.
                return Ok(InstallSnapshotResp { term: st.current_term, next_offset: 0, installed: true });
            }
            st.current_term
        };
        let _recv = self.snapshot_lock.lock().unwrap_or_else(|p| p.into_inner());
        let path = &self.storage.recv_path;
        let restart = InstallSnapshotResp { term, next_offset: 0, installed: false };
        let staged_matches = || {
            SnapshotMeta::read(path)
                .is_some_and(|m| m.last_index == req.last_included_index && m.last_term == req.last_included_term)
        };
        let io = |e| StorageError::io("write received raft snapshot", e);
        if req.offset == 0 {
            // A new transfer must start with the header of the snapshot it announces.
            match SnapshotMeta::decode(&req.data) {
                Ok(m) if m.last_index == req.last_included_index && m.last_term == req.last_included_term => {}
                _ => return Ok(restart),
            }
        } else {
            let staged = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
            if !staged_matches() {
                return Ok(restart);
            }
            if staged != req.offset {
                return Ok(InstallSnapshotResp { term, next_offset: staged, installed: false });
            }
        }
        let opts = if req.offset == 0 { OpenOpts::create_truncate() } else { OpenOpts::create() };
        let mut file = storage_types::vfs::at(path).open(path, opts).map_err(io)?;
        file.seek(SeekFrom::Start(req.offset))
            .and_then(|_| file.write_all(&req.data))
            .map_err(io)?;
        let next_offset = req.offset + req.data.len() as u64;
        if !req.done {
            return Ok(InstallSnapshotResp { term, next_offset, installed: false });
        }
        file.sync_all().map_err(io)?;
        drop(file);
        match self.install_received_snapshot(req.last_included_index, req.last_included_term) {
            Ok(()) => Ok(InstallSnapshotResp { term, next_offset, installed: true }),
            Err(StorageError::CorruptedHeader { .. } | StorageError::ChecksumMismatch { .. } | StorageError::InvalidArchiveLayout { .. }) => {
                // A damaged transfer is discarded and restarted, never applied.
                let _ = storage_types::vfs::at(path).remove_file(path);
                Ok(restart)
            }
            Err(e) => Err(e),
        }
    }

    /// Verifies the received snapshot completely, then applies it. Idempotent, so an install
    /// interrupted by a crash is simply redone at the next start.
    fn install_received_snapshot(&self, last_index: u64, last_term: u64) -> Result<()> {
        let _apply = self.apply_lock.lock().unwrap_or_else(|p| p.into_inner());
        let recv = &self.storage.recv_path;
        // 1. Verify everything before touching the engine.
        let mut ids = FastHashSet::default();
        let (meta, config, snap_span) = for_each_snapshot_record(recv, |id, _| {
            ids.insert(id);
            Ok(())
        })?;
        if meta.last_index != last_index || meta.last_term != last_term {
            return Err(StorageError::corrupted_header("raft snapshot does not match its announcement"));
        }
        // 2. The engine is about to hold a mix of old and snapshot state until step 4.
        atomic_write_replace(&self.storage.installing_path, &last_index.to_le_bytes(), true)?;
        // 3. Engine state := snapshot state. A group adopts the snapshot's span: the replica may
        //    have been created with a later (narrower) one, and its log replays the splits after
        //    the snapshot from the span in force at it.
        if let (Some(sp), true) = (snap_span, self.group.is_some()) {
            if sp != self.span() {
                self.storage.save_span(&sp)?;
                *self.span.write().unwrap_or_else(|p| p.into_inner()) = sp;
            }
        }
        let span = self.span();
        for_each_snapshot_record(recv, |id, frame| {
            if !span.contains_id(id) {
                return Ok(()); // another group's key: never ours to write
            }
            let (hdr, record) = RecordCodec::decode_owned::<T>(frame.as_slice())?;
            // Exact version and TTL: every replica holds identical versions.
            self.engine.install_exact(id, &record, hdr.version(), hdr.ttl_expires_at_ns)
        })?;
        for id in self.span_ids() {
            if !ids.contains(&id) {
                let _ = self.engine.delete_internal(id)?;
            }
        }
        self.engine.sync_wal()?;
        // 4. Raft state: keep log entries after the snapshot only if the log agrees at its end.
        //    The snapshot's configuration becomes the base one (persisted first).
        {
            let mut st = self.lock_state();
            if let Some(ref c) = config {
                self.storage.save_members(c)?;
            }
            match st.pos_of(last_index) {
                Some(pos) if st.log.get(pos).is_some_and(|e| e.term == last_term) => {
                    st.drain_log(pos + 1);
                }
                _ => st.clear_log(),
            }
            if config.is_some() {
                st.base_config = config;
            }
            st.snapshot_index = last_index;
            st.snapshot_term = last_term;
            st.commit_index = st.commit_index.max(last_index);
            st.last_applied = last_index;
            self.storage.save_hard_state(&st)?;
            self.storage.rewrite_log(&st.log)?;
        }
        // 5. The installed snapshot becomes this node's own (served if it is elected leader).
        storage_types::vfs::at(&self.storage.snapshot_path).rename(recv, &self.storage.snapshot_path)
            .and_then(|()| sync_parent(&self.storage.snapshot_path))
            .map_err(|e| StorageError::io("keep installed raft snapshot", e))?;
        storage_types::vfs::at(&self.storage.installing_path).remove_file(&self.storage.installing_path)
            .map_err(|e| StorageError::io("clear raft install marker", e))
    }

    /// Start-up after a crash during a snapshot install: redo it from the received file, or, if
    /// that file is unusable, reset this replica to empty (engine and Raft state) so the leader
    /// rebuilds it from scratch — never serve a half-installed state.
    fn recover_interrupted_install(&self) -> Result<()> {
        let applied = self.lock_state().snapshot_index;
        let marker = fs::read(&self.storage.installing_path).unwrap_or_default();
        let target = (marker.len() == 8).then(|| read_u64_le(&marker, 0));
        if target.is_some_and(|t| t <= applied) {
            // The install completed; only the marker removal was lost.
            return storage_types::vfs::at(&self.storage.installing_path).remove_file(&self.storage.installing_path)
                .map_err(|e| StorageError::io("clear raft install marker", e));
        }
        if let Some(meta) = SnapshotMeta::read(&self.storage.recv_path).filter(|m| Some(m.last_index) == target) {
            if self.install_received_snapshot(meta.last_index, meta.last_term).is_ok() {
                return Ok(());
            }
        }
        {
            let _apply = self.apply_lock.lock().unwrap_or_else(|p| p.into_inner());
            for id in self.span_ids() {
                let _ = self.engine.delete_internal(id)?;
            }
            self.engine.sync_wal()?;
            let mut st = self.lock_state();
            st.clear_log();
            st.snapshot_index = 0;
            st.snapshot_term = 0;
            st.commit_index = 0;
            st.last_applied = 0;
            self.storage.save_hard_state(&st)?;
            self.storage.rewrite_log(&st.log)?;
        }
        let _ = storage_types::vfs::at(&self.storage.recv_path).remove_file(&self.storage.recv_path);
        storage_types::vfs::at(&self.storage.installing_path).remove_file(&self.storage.installing_path)
            .map_err(|e| StorageError::io("clear raft install marker", e))
    }

    /// Applies one committed entry deterministically. Every new version is at least the entry's
    /// log index (identical on every replica, monotonic across delete + re-create), and TTL
    /// decisions use the leader's proposal time. A rejected batch (failed precondition, invalid
    /// value) is a normal, replicated outcome; only environment failures (I/O, memory) are errors
    /// that stop the apply loop.
    /// Applies a split (idempotent: a replay after a crash finds it done). Order: the right-hand
    /// group's state first, then this group's shorter span, then `last_applied` (by the caller),
    /// so a crash at any point either redoes the split or finds it complete.
    fn apply_split(&self, entry: &RaftLogEntry, at: &[u8], right: u64) -> Result<()> {
        let span = self.span();
        if span.end.as_deref() == Some(at) && self.group.is_some() {
            // Already applied before a restart: re-announce so the host opens the right group.
        } else {
            let Some((_, right_span)) = span.split_at(at) else {
                // A split at a key this group does not hold is refused identically everywhere.
                return Err(StorageError::invalid_key("split key outside the range"));
            };
            let dir = self.engine.base_dir().to_path_buf();
            let rs = RaftStorage::for_group(&dir, &self.stem, Some(right));
            // A replica of the right-hand range may already live here (added by a move before
            // this replica replayed the split): never overwrite its state.
            let exists = rs.load_span()?.is_some();
            // The configuration in force at the split entry (a learner replaying an old split
            // must not put itself into the right-hand range).
            let members = self.lock_state().membership_at(entry.index).map(|(_, m)| m.clone()).unwrap_or_default();
            let mut st = RaftState::new();
            // Terms never decrease along a log: the right-hand group starts in the split
            // entry's term, so its first leader (term + 1 or later) appends after it.
            st.current_term = entry.term;
            st.snapshot_index = entry.index;
            st.snapshot_term = entry.term;
            st.commit_index = entry.index;
            st.last_applied = entry.index;
            if !exists {
                if !members.is_empty() {
                    rs.save_members(&(entry.index, members.clone()))?;
                    st.base_config = Some((entry.index, members));
                }
                rs.save_span(&right_span)?;
                rs.rewrite_log(&[])?;
                rs.save_hard_state(&st)?;
            }
            let left = KeySpan::new(span.start.clone(), Some(at.to_vec()));
            if self.group.is_some() {
                self.storage.save_span(&left)?;
            }
            *self.span.write().unwrap_or_else(|p| p.into_inner()) = left;
        }
        let right_span = KeySpan::new(at.to_vec(), span.end.clone().filter(|e| e.as_slice() != at).or_else(|| {
            // Replay: the right span is the one persisted for the right group.
            let dir = self.engine.base_dir().to_path_buf();
            RaftStorage::for_group(&dir, &self.stem, Some(right)).load_span().ok().flatten().and_then(|s| s.end)
        }));
        let members = self.lock_state().membership_at(entry.index).map(|(_, m)| m.clone()).unwrap_or_default();
        self.splits.lock().unwrap_or_else(|p| p.into_inner()).push(SplitEvent { index: entry.index, term: entry.term, right, right_span, members });
        Ok(())
    }

    fn apply_entry_to_engine(&self, entry: &RaftLogEntry) -> Result<ApplyOutcome> {
        let min = Some(Version(entry.index));
        let now_fallback = || {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        };
        let span = self.span();
        let outside = |ids: &mut dyn Iterator<Item = InlineId>| {
            if span.is_all() {
                return false;
            }
            for id in ids {
                if !span.contains_id(id) {
                    return true;
                }
            }
            false
        };
        let outcome = match &entry.command {
            // A write proposed before a split and applied after it, for keys the group no
            // longer holds, is refused the same way on every replica.
            RaftCommand::Upsert { id, .. } | RaftCommand::Delete { id } if outside(&mut core::iter::once(*id)) => {
                Err(StorageError::invalid_key("key outside this range (it split): re-route"))
            }
            RaftCommand::Split { at, right } => self.apply_split(entry, at, *right).map(|()| Vec::new()),
            RaftCommand::Upsert { id, frame } => {
                let (_hdr, owned) = RecordCodec::decode_owned::<T>(frame.as_slice())?;
                self.engine.commit_at(vec![WriteOp::put(*id, owned)], min, now_fallback())
            }
            RaftCommand::Delete { id } => {
                self.engine.commit_at(vec![WriteOp::delete(*id)], min, now_fallback())
            }
            RaftCommand::Ops(bytes) => match decode_raft_ops::<T>(bytes.as_slice()) {
                Ok((_, ops)) if outside(&mut ops.iter().map(|o| o.id)) => {
                    Err(StorageError::invalid_key("key outside this range (it split): re-route"))
                }
                Ok((now_ns, ops)) => self.engine.commit_at(ops, min, now_ns),
                Err(e) => Err(e),
            },
            RaftCommand::Noop | RaftCommand::Config(_) => Ok(Vec::new()),
            RaftCommand::SweepExpired { now_ns } if span.is_all() => self.engine.sweep_expired_at(*now_ns).map(|_| Vec::new()),
            // TTL sweeps of a shared engine run in its whole-engine group only.
            RaftCommand::SweepExpired { .. } => Ok(Vec::new()),
        };
        match outcome {
            Err(e @ (StorageError::Io { .. } | StorageError::ResourceExhausted { .. })) => Err(e),
            other => Ok(other),
        }
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
            if leader.needs_snapshot(next) {
                next = Self::send_snapshot(leader, peer)? + 1;
                continue;
            }
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

    /// Streams the leader's snapshot to `peer` chunk by chunk (resuming where the follower asks);
    /// returns the snapshot's last included index once installed.
    fn send_snapshot(leader: &RaftNode<T>, peer: &RaftNode<T>) -> Option<u64> {
        let mut offset = 0u64;
        let mut restarts = 0u32;
        loop {
            let req = leader.build_install_snapshot(offset, RAFT_SNAPSHOT_CHUNK).ok()?;
            let last = req.last_included_index;
            let resp = peer.handle_install_snapshot(req).ok()?;
            if resp.term > leader.current_term() {
                return None;
            }
            if resp.installed {
                return Some(last);
            }
            if resp.next_offset == 0 {
                restarts += 1;
                if restarts > 3 {
                    return None;
                }
            }
            offset = resp.next_offset;
        }
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

    /// Proposes a replicated `PUT` through the current Raft leader and waits for quorum commit
    /// ($\lfloor N/2 \rfloor + 1$). Returns the committed Raft log index.
    ///
    /// If no quorum acknowledges the entry the outcome is unknown ([`StorageError::CommitUnknown`]):
    /// the entry stays in the leader's log and may still commit later.
    pub fn replicate_put(&self, id: InlineId, record: &T) -> Result<u64> {
        let leader = self
            .current_leader()
            .ok_or(StorageError::NotLeader { leader: None })?;
        let (entry, _prev_idx, _prev_term) = leader.append_leader_entry(id, Some(record))?;
        self.replicate_index(&leader, entry.index)?;
        let _ = leader.wait_applied(entry.index, Duration::from_secs(5));
        Ok(entry.index)
    }

    /// Replicates an atomic conditional batch through the leader and returns the per-operation
    /// results once applied (a failed precondition is returned as the deterministic error every
    /// replica reached).
    pub fn replicate_ops(&self, ops: &[WriteOp<T>]) -> Result<Vec<WriteResult>> {
        let leader = self
            .current_leader()
            .ok_or(StorageError::NotLeader { leader: None })?;
        let index = leader.propose_ops(ops)?;
        self.replicate_index(&leader, index)?;
        leader.wait_applied(index, Duration::from_secs(5))
    }

    /// Replicates the leader's log up to `index`, commits it on a quorum and broadcasts the commit.
    fn replicate_index(&self, leader: &Arc<RaftNode<T>>, index: u64) -> Result<()> {
        let mut acks = 1usize; // leader itself
        for peer in &self.nodes {
            if peer.node_id() == leader.node_id() || self.is_isolated(peer.node_id()) {
                continue;
            }
            if self.sync_follower(leader, peer).is_some_and(|m| m >= index) {
                acks += 1;
            }
        }
        if acks < self.quorum_size() {
            return Err(StorageError::CommitUnknown { index });
        }
        leader.advance_leader_commit(index)?;
        // Broadcast the new `leader_commit` so reachable followers apply it.
        for peer in &self.nodes {
            if peer.node_id() == leader.node_id() || self.is_isolated(peer.node_id()) {
                continue;
            }
            let _ = self.sync_follower(leader, peer);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EngineConfig;
    use crate::model::AgentObjectRecord;

    /// A follower may hold stale entries past the end of what an AppendEntries verified (a
    /// delayed or duplicated request). It must commit only up to the last entry the request
    /// covered, and report only that as matching: committing its own stale suffix would apply
    /// an entry the cluster never committed.
    #[test]
    fn test_follower_never_commits_unverified_stale_suffix() {
        let dir = std::env::temp_dir().join(format!("stge_raft_stale_suffix_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = Arc::new(StorageEngine::<AgentObjectRecord>::open(&dir, EngineConfig::default()).unwrap());
        let f = RaftNode::new(9, engine).unwrap();
        let e = |term, index| RaftLogEntry { term, index, command: RaftCommand::Noop };
        let ae = |term, leader_id, prev_log_index, prev_log_term, entries, leader_commit| AppendEntriesReq {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        };
        // Term 1 leader: entries 1..=3. Term 2 leader: stale entries 4, 5 (never committed).
        assert!(f.handle_append_entries(ae(1, 1, 0, 0, vec![e(1, 1), e(1, 2), e(1, 3)], 0)).unwrap().success);
        assert!(f.handle_append_entries(ae(2, 2, 3, 1, vec![e(2, 4), e(2, 5)], 0)).unwrap().success);
        // Term 3 leader (its entry 4 has term 3, committed at 4) sends a late request covering
        // only entry 3.
        let resp = f.handle_append_entries(ae(3, 3, 2, 1, vec![e(1, 3)], 4)).unwrap();
        assert!(resp.success);
        assert_eq!(resp.match_index, 3, "only the verified prefix matches");
        let v = f.log_view();
        assert_eq!(v.commit_index, 3, "the stale entry 4 (term 2) must not commit");
        assert_eq!(v.last_applied, 3);
        // The next request truncates the stale suffix; then entry 4 (term 3) commits.
        let resp = f.handle_append_entries(ae(3, 3, 3, 1, vec![e(3, 4)], 4)).unwrap();
        assert!(resp.success);
        let v = f.log_view();
        assert_eq!((v.commit_index, v.last_index(), v.term_at(4)), (4, 4, Some(3)));
        drop(f);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A proposal replaced after a leader change (another entry applied at its index) is
    /// `CommitUnknown`, never the replacement's outcome reported as success.
    #[test]
    fn test_replaced_proposal_is_never_reported_as_committed() {
        let dir = std::env::temp_dir().join(format!("stge_raft_replaced_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let engine = Arc::new(StorageEngine::<AgentObjectRecord>::open(&dir, EngineConfig::default()).unwrap());
        let a = RaftNode::new(1, engine).unwrap();
        let req = a.start_election().unwrap();
        assert!(a.become_leader(req.term));
        let mine = a.propose(RaftCommand::Noop).unwrap();
        assert_eq!(mine, 2);
        // A term-2 leader overwrites index 2 and commits it.
        let ae = AppendEntriesReq {
            term: 2,
            leader_id: 2,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![RaftLogEntry { term: 2, index: 2, command: RaftCommand::Noop }],
            leader_commit: 2,
        };
        assert!(a.handle_append_entries(ae).unwrap().success);
        assert_eq!(a.last_applied(), 2);
        assert!(matches!(a.wait_applied(mine, Duration::ZERO), Err(StorageError::CommitUnknown { index: 2 })));
        drop(a);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_raft_ops_roundtrip_including_payload_free_operations() {
        // Deletes and checks carry no payload (46 bytes each): batches of only those must
        // decode (a document delete is a delete plus a check on `$meta`).
        let id = |i: u32| InlineId::new(&format!("k{i}")).unwrap();
        let ops: Vec<WriteOp<AgentObjectRecord>> = vec![
            WriteOp { id: id(1), kind: WriteKind::Delete, precondition: Precondition::Exists },
            WriteOp { id: id(2), kind: WriteKind::Check, precondition: Precondition::Version(Version(9)) },
        ];
        for n in 1..=ops.len() {
            let bytes = encode_raft_ops(&ops[..n], 77).unwrap();
            assert_eq!(bytes.as_slice().len(), 12 + 46 * n);
            let (now, back) = decode_raft_ops::<AgentObjectRecord>(bytes.as_slice()).unwrap();
            assert_eq!(now, 77);
            assert_eq!(back.len(), n);
            assert!(matches!(back[0].kind, WriteKind::Delete));
            // Every truncation is refused.
            for cut in 0..bytes.as_slice().len() {
                assert!(decode_raft_ops::<AgentObjectRecord>(&bytes.as_slice()[..cut]).is_err());
            }
        }
    }

    #[test]
    fn test_install_snapshot_catches_up_a_follower_behind_the_compacted_log() {
        let base = std::env::temp_dir().join(format!("stge_test_raft_snapshot_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let open = |name: &str| {
            Arc::new(
                StorageEngine::<AgentObjectRecord>::open(base.join(name), EngineConfig::default())
                    .unwrap(),
            )
        };
        let engines = [open("s1"), open("s2"), open("s3")];
        let nodes: Vec<Arc<RaftNode<AgentObjectRecord>>> = engines
            .iter()
            .enumerate()
            .map(|(i, e)| Arc::new(RaftNode::new(i as NodeId + 1, Arc::clone(e)).unwrap()))
            .collect();
        let group = RaftReplicationGroup::new(nodes.clone()).unwrap();
        group.elect_leader(1).unwrap();
        let id = |i: u32| InlineId::new(&format!("snap{i:04}")).unwrap();
        let rec = |i: u32, v: i64| AgentObjectRecord::new(id(i), "v", 1, v);
        for i in 0..20 {
            group.replicate_put(id(i), &rec(i, 1)).unwrap();
        }
        // Node 3 falls behind; the others move on and compact their logs past it.
        group.set_node_isolated(3, true);
        for i in 0..300 {
            group.replicate_put(id(i % 200), &rec(i % 200, i64::from(i))).unwrap();
        }
        for n in &nodes[..2] {
            let applied = n.last_applied();
            assert_eq!(n.compact_log_through(applied).unwrap(), applied);
        }
        // A stray local record on the lagging node must disappear with the install.
        // (Simulated divergence: bypass the replica guard on purpose.)
        engines[2].set_replica_mode(false);
        engines[2].put(InlineId::new("stray").unwrap(), &AgentObjectRecord::new(InlineId::new("stray").unwrap(), "x", 1, 0)).unwrap();
        engines[2].set_replica_mode(true);
        assert!(nodes[0].needs_snapshot(nodes[2].last_log_index() + 1));

        group.set_node_isolated(3, false);
        let last = group.replicate_put(id(500), &rec(500, 5)).unwrap();
        assert_eq!(nodes[2].last_applied(), last, "snapshot installed, then the tail replicated");
        assert!(nodes[2].snapshot_index() >= 320);
        for i in (0..200).chain([500]) {
            assert_eq!(
                engines[2].get_owned(id(i)).unwrap().1,
                engines[0].get_owned(id(i)).unwrap().1,
                "record {i}"
            );
        }
        assert!(engines[2].get_owned(InlineId::new("stray").unwrap()).is_err());
        assert_eq!(engines[2].live_ids().len(), engines[0].live_ids().len());

        // Chunked transfer by hand: tiny chunks, a duplicate and a gap are answered with the
        // offset the follower expects; a corrupted transfer is restarted, never applied.
        let follower = &nodes[2];
        for n in &nodes {
            let applied = n.last_applied();
            n.compact_log_through(applied).unwrap();
        }
        let leader = &nodes[0];
        group.set_node_isolated(3, true);
        group.replicate_put(id(600), &rec(600, 6)).unwrap();
        let applied = leader.last_applied();
        leader.compact_log_through(applied).unwrap();
        let fresh = |offset| leader.build_install_snapshot(offset, 700).unwrap();
        let first = fresh(0);
        let target = first.last_included_index;
        assert!(target > follower.last_applied());
        let r = follower.handle_install_snapshot(first.clone()).unwrap();
        assert_eq!((r.next_offset, r.installed), (700, false));
        assert_eq!(follower.handle_install_snapshot(first).unwrap().next_offset, 700, "duplicate");
        assert_eq!(follower.handle_install_snapshot(fresh(2100)).unwrap().next_offset, 700, "gap");
        let mut offset = 700;
        let mut corrupted = false;
        loop {
            let mut req = fresh(offset);
            if !corrupted && req.done {
                // Damage the last chunk once: the whole transfer must restart.
                req.data[0] ^= 0xFF;
                corrupted = true;
                let r = follower.handle_install_snapshot(req).unwrap();
                assert_eq!((r.next_offset, r.installed), (0, false));
                assert!(follower.last_applied() < target, "damaged snapshot not applied");
                offset = 0;
                continue;
            }
            let r = follower.handle_install_snapshot(req).unwrap();
            if r.installed {
                break;
            }
            offset = r.next_offset;
        }
        assert_eq!(follower.last_applied(), target);
        assert_eq!(engines[2].get_owned(id(600)).unwrap().1.score, 6);
        group.set_node_isolated(3, false);

        // Crash during an install with no usable received file: the replica resets to empty
        // (engine and Raft state) instead of serving a half-installed state.
        let stem = crate::engine::collection_file_stem(engines[2].collection_name()).unwrap();
        let marker = base.join("s3").join(format!("{stem}_raft.installing"));
        std::fs::write(&marker, (target + 100).to_le_bytes()).unwrap();
        drop(group);
        let reopened = RaftNode::new(3, Arc::clone(&engines[2])).unwrap();
        assert_eq!(reopened.last_applied(), 0);
        assert!(engines[2].live_ids().is_empty());
        assert!(!marker.exists());
        drop(reopened);
        drop(nodes);
        drop(engines);
        let _ = std::fs::remove_dir_all(&base);
    }

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
        // Every node has applied all 31 entries (the leader's no-op + 30 puts); compact through
        // 25 everywhere.
        let raft_log_path = std::fs::read_dir(base.join("c2"))
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| p.to_string_lossy().ends_with("_raft.log"))
            .expect("raft log file");
        let full_log = std::fs::read(&raft_log_path).unwrap();
        for n in &nodes {
            assert_eq!(n.last_applied(), 31);
            assert_eq!(n.compact_log_through(25).unwrap(), 25);
            assert_eq!(n.snapshot_index(), 25);
            assert_eq!(n.log_len(), 6);
            // Compaction never goes past what was applied.
            assert_eq!(n.compact_log_through(u64::MAX).unwrap(), 31);
            assert_eq!(n.log_len(), 0);
            assert_eq!(n.last_log_index(), 31);
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
            assert_eq!(n.snapshot_index(), 31);
            assert_eq!(n.last_log_index(), 31);
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
        assert_eq!(idx, 33, "32 is the new leader's no-op");
        for e in &engines {
            assert_eq!(e.get_owned(extra).unwrap().1.score, 7);
            assert_eq!(e.get_owned(ids[29]).unwrap().1.score, 29);
        }

        // A follower that needs a compacted entry gets an explicit "snapshot required" error.
        let leader = group.current_leader().unwrap();
        leader.compact_log_through(33).unwrap();
        assert!(leader.build_append_entries_from(5).is_err());
        assert!(leader.build_append_entries_from(34).is_ok());
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
        assert_eq!(idx1, 2, "index 1 is the new leader's no-op");

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
        assert_eq!(idx2, 4, "index 3 is the second leader's no-op");

        assert_eq!(
            n3.read_at_commit_index(id2, idx2, |v| v.score).unwrap(),
            222
        );

        // 5. Heal Node 1 and replicate a third write -> Node 1 steps down to Follower and catches up all missing entries!
        group.set_node_isolated(1, false);
        let id3 = InlineId::from_static("raft_rec_03");
        let rec3 = AgentObjectRecord::new(id3, "Raft Healed Write", 3000, 333);
        let idx3 = group.replicate_put(id3, &rec3).unwrap();
        assert_eq!(idx3, 5);
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
        assert_eq!(reloaded.last_log_index(), 5);
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
            pre_vote: false,
        });
        assert!(!resp.vote_granted);

        // Malformed entries (index gap / zero index) are rejected without panicking.
        let bad = AppendEntriesReq {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![RaftLogEntry::record(1, 0, InlineId::from_static("x"), None)],
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
