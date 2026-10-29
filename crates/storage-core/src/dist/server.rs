//! Range server: transactional key-value requests on the leaseholders of a node
//! (`F-GEO-04`, `TASK-140`).
//!
//! Wraps a [`NodeDriver`].
//!
//! **Evaluation.** A request is evaluated on the range's leaseholder against the local
//! state, and its effect is proposed as one record write with a version precondition. Every
//! replica applies the same decision. A precondition failure (a concurrent change) re-runs the
//! evaluation.
//!
//! **Latches.** At most one write per key is in flight, and reads of that key wait for it. A
//! read can never miss a proposed intent at or below its timestamp.
//!
//! **Timestamp cache.** It holds the highest read timestamp served per key, and writes land
//! above it. A new leaseholder knows nothing of reads its predecessor served, so it starts the
//! cache at `now + max_offset`: no read the old leaseholder served can be above that.
//!
//! **Reads** are linearizable read-index reads (quorum-confirmed leadership, then applied
//! state).
//!
//! Requests and responses are plain values. The host passes them in and out, so the same code
//! runs over TCP and in the simulator.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use storage_types::{InlineId, StorageError};

use crate::dist::hlc::{Hlc, Timestamp};
use crate::dist::mvcc::{eval_read, eval_refresh, eval_resolve, eval_write, MvccKey, ReadOutcome, TxnId, TxnMeta, TxnRecord, TxnStatus, Value, WriteOutcome, TXN_COLLECTION};
use crate::dist::node::{NodeDriver, RangeId, ReadToken};
use crate::document::{document_id, DocumentRecord};
use crate::engine::{Precondition, WriteOp};
use crate::sharding::NodeId;

/// A key of the distributed key space: (collection, key).
pub type Key = (String, String);

/// A coordinator stops heartbeating its transaction: after this long it counts as abandoned.
pub const TXN_EXPIRY_NS: u64 = 2_000_000_000;

#[derive(Clone, Debug)]
pub enum KvRequest {
    /// Read at `ts` with uncertainty `limit`, by `txn` (sees its own intent).
    Get { key: Key, ts: Timestamp, limit: Timestamp, txn: Option<TxnId> },
    /// Place `txn`'s intent at `ts` or later.
    Put { key: Key, txn: TxnMeta, ts: Timestamp, value: Value },
    /// Resolve `txn`'s intent: commit at `commit_ts`, or abort (`None`).
    Resolve { key: Key, txn: TxnId, commit_ts: Option<Timestamp> },
    /// Is a read at `from` still valid at `to`?
    Refresh { key: Key, from: Timestamp, to: Timestamp, txn: TxnId },
    /// Does `txn`'s intent exist at or below `ts`? With `prevent`, make sure it never will
    /// (staging recovery).
    QueryIntent { key: Key, txn: TxnId, ts: Timestamp, prevent: bool },
    /// Transaction record writes.
    Record { id: TxnId, op: RecordOp },
}

#[derive(Clone, Debug)]
pub enum RecordOp {
    /// Create (pending) or heartbeat.
    Heartbeat { now: Timestamp, priority: (Timestamp, TxnId), epoch: u32 },
    Stage { ts: Timestamp, in_flight: Vec<Key>, epoch: u32, priority: (Timestamp, TxnId) },
    /// Commit at `ts`. With `staged`, only if the record is still staged at exactly that
    /// `(timestamp, epoch)`: a recovery decides on the record it examined, never on a newer
    /// one.
    Commit { ts: Timestamp, staged: Option<(Timestamp, u32)> },
    /// Abort, unconditionally (`None`: the coordinator itself) or only if the record is still
    /// pending or staged at exactly `(timestamp, epoch)` (a recovery).
    Abort { staged: Option<(Timestamp, u32)> },
    /// Wound-wait: abort the transaction if the pusher is older (lower priority value) or the
    /// transaction is abandoned (no heartbeat, or for a transaction without a record yet, an
    /// intent older than [`TXN_EXPIRY_NS`]); otherwise just report its record.
    Push { pusher: (Timestamp, TxnId), now: Timestamp, intent_ts: Timestamp, pushee_priority: (Timestamp, TxnId) },
    Get,
}

#[derive(Clone, Debug)]
pub enum KvResponse {
    Value { value: Value, ts: Option<Timestamp> },
    Uncertain { ts: Timestamp },
    Conflict { intent_txn: TxnMeta, intent_ts: Timestamp },
    Written { ts: Timestamp },
    Resolved,
    Refreshed(bool),
    IntentFound(bool),
    Record(Option<TxnRecord>),
    /// Not the leaseholder here (try `leader`), or the key moved to another range.
    NotLeader { leader: Option<NodeId> },
    WrongRange,
    Error(String),
}

/// A response for request `id`, to send to `client`.
#[derive(Clone, Debug)]
pub struct Reply {
    pub id: u64,
    pub client: u64,
    pub resp: KvResponse,
}

#[derive(Debug)]
struct Pending {
    id: u64,
    client: u64,
    range: RangeId,
    req: KvRequest,
    state: PendingState,
}

#[derive(Debug)]
enum PendingState {
    /// Not started.
    New,
    /// Waiting for the read-index confirmation of `token`, issued in `term`.
    AwaitRead { token: ReadToken, term: u64 },
    /// Leadership confirmed and applied state current: evaluate once the latch is free.
    Confirmed,
    /// Proposed at `index`; `resp` is sent once it applies.
    Proposed { index: u64, resp: KvResponse },
}

/// Per key: the highest read timestamp and its transaction, plus the highest read timestamp
/// of any other reader. A transaction's own reads never push its own writes.
#[derive(Debug, Default, Clone, Copy)]
struct ReadMark {
    top: Timestamp,
    top_txn: Option<TxnId>,
    others: Timestamp,
}

#[derive(Debug, Default)]
struct TsCache {
    term: u64,
    low_water: Timestamp,
    keys: BTreeMap<Vec<u8>, ReadMark>,
}

impl TsCache {
    /// The timestamp a write by `txn` must exceed.
    fn floor(&self, k: &[u8], txn: Option<&TxnId>) -> Timestamp {
        let m = self.keys.get(k).copied().unwrap_or_default();
        let reads = if txn.is_some() && m.top_txn.as_ref() == txn { m.others } else { m.top };
        reads.max(self.low_water)
    }

    /// Records a read at `ts` by `txn` (`None`: a plain read or a recovery probe).
    fn bump(&mut self, k: &[u8], ts: Timestamp, txn: Option<TxnId>) {
        let m = self.keys.entry(k.to_vec()).or_default();
        let same = txn.is_some() && txn == m.top_txn;
        if same {
            m.top = m.top.max(ts);
        } else if ts > m.top {
            m.others = m.others.max(m.top);
            m.top = ts;
            m.top_txn = txn;
        } else {
            m.others = m.others.max(ts);
        }
        if self.keys.len() > 100_000 {
            // Bounded memory: forget the entries, keep their maximum as the floor.
            let max = self.keys.values().map(|m| m.top).max().unwrap_or(Timestamp::ZERO);
            self.low_water = self.low_water.max(max);
            self.keys.clear();
        }
    }
}

/// The transactional server of one node.
pub struct NodeServer {
    pub driver: NodeDriver,
    hlc: Arc<Hlc>,
    pending: Vec<Pending>,
    latches: BTreeMap<Vec<u8>, u64>,
    tscache: BTreeMap<RangeId, TsCache>,
    replies: VecDeque<Reply>,
    /// Evaluation log for debugging (`enable_trace`).
    trace: Option<Vec<String>>,
}

impl core::fmt::Debug for NodeServer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("NodeServer").field("node", &self.driver.id()).field("pending", &self.pending.len()).finish()
    }
}

fn key_id(k: &Key) -> Option<InlineId> {
    document_id(&k.0, &k.1).ok()
}

fn record_key_of(id: &TxnId) -> Key {
    (TXN_COLLECTION.to_string(), crate::dist::mvcc::record_key(id))
}

impl KvRequest {
    pub fn key(&self) -> Key {
        match self {
            Self::Get { key, .. } | Self::Put { key, .. } | Self::Resolve { key, .. } | Self::Refresh { key, .. } | Self::QueryIntent { key, .. } => key.clone(),
            Self::Record { id, .. } => record_key_of(id),
        }
    }

    fn is_write(&self) -> bool {
        !matches!(self, Self::Get { .. } | Self::Refresh { .. } | Self::Record { op: RecordOp::Get, .. })
    }

    /// The timestamp the request carries (moves the server's clock forward: causality).
    fn ts(&self) -> Option<Timestamp> {
        match self {
            Self::Get { ts, .. } | Self::Put { ts, .. } | Self::QueryIntent { ts, .. } => Some(*ts),
            Self::Refresh { to, .. } => Some(*to),
            Self::Resolve { commit_ts, .. } => *commit_ts,
            Self::Record { op: RecordOp::Heartbeat { now, .. } | RecordOp::Push { now, .. }, .. } => Some(*now),
            Self::Record { op: RecordOp::Stage { ts, .. } | RecordOp::Commit { ts, .. }, .. } => Some(*ts),
            Self::Record { .. } => None,
        }
    }
}

impl NodeServer {
    pub fn new(driver: NodeDriver, hlc: Arc<Hlc>) -> Self {
        Self { driver, hlc, pending: Vec::new(), latches: BTreeMap::new(), tscache: BTreeMap::new(), replies: VecDeque::new(), trace: None }
    }

    /// Records every evaluation (tests and debugging).
    pub fn enable_trace(&mut self) {
        self.trace = Some(Vec::new());
    }

    pub fn take_trace(&mut self) -> Vec<String> {
        self.trace.as_mut().map(std::mem::take).unwrap_or_default()
    }

    pub fn hlc(&self) -> &Arc<Hlc> {
        &self.hlc
    }

    /// The range of this node holding `key`, if any.
    pub fn range_of(&self, key: &Key) -> Option<RangeId> {
        let id = key_id(key)?;
        self.driver.ranges().into_iter().find(|&r| self.driver.replica(r).is_some_and(|x| x.span().contains_id(id)))
    }

    /// Accepts request `id` from `client`.
    pub fn submit(&mut self, id: u64, client: u64, req: KvRequest) {
        if let Some(ts) = req.ts() {
            if let Err(e) = self.hlc.update(ts) {
                self.replies.push_back(Reply { id, client, resp: KvResponse::Error(e.to_string()) });
                return;
            }
        }
        let key = req.key();
        let Some(range) = self.range_of(&key) else {
            self.replies.push_back(Reply { id, client, resp: KvResponse::WrongRange });
            return;
        };
        if !self.driver.is_leader(range) {
            let leader = self.driver.leader_of(range);
            self.replies.push_back(Reply { id, client, resp: KvResponse::NotLeader { leader } });
            return;
        }
        self.pending.push(Pending { id, client, range, req, state: PendingState::New });
    }

    /// Replies ready to send.
    pub fn take_replies(&mut self) -> Vec<Reply> {
        self.replies.drain(..).collect()
    }

    fn cache(&mut self, range: RangeId) -> &mut TsCache {
        let term = self.driver.replica(range).map_or(0, |r| r.current_term());
        let now = self.hlc.now();
        let c = self.tscache.entry(range).or_default();
        if c.term != term {
            // New leaseholder. Every read its predecessor can still serve has a timestamp at
            // or below this node's clock now:
            // - such a read is confirmed by a quorum acknowledging a heartbeat sent after the
            //   read was registered;
            // - that heartbeat carried the old leader's clock, at least the read's timestamp;
            // - every acknowledging node's clock is therefore at least the read's timestamp,
            //   and some acknowledging node voted for this leader afterwards;
            // - votes carry clocks too.
            // Writes land above this floor.
            *c = TsCache { term, low_water: now, keys: BTreeMap::new() };
        }
        c
    }

    fn load(&self, key: &Key) -> Result<(MvccKey, Precondition), StorageError> {
        let id = key_id(key).ok_or_else(|| StorageError::invalid_key("invalid key"))?;
        match self.driver.engine().get_owned(id) {
            Ok((v, rec)) => Ok((MvccKey::from_record(Some(&rec))?, Precondition::Version(v))),
            Err(StorageError::RecordNotFound { .. }) => Ok((MvccKey::default(), Precondition::Absent)),
            Err(e) => Err(e),
        }
    }

    fn load_record(&self, id: &TxnId) -> Result<(Option<TxnRecord>, Precondition), StorageError> {
        let key = record_key_of(id);
        let rid = key_id(&key).ok_or_else(|| StorageError::invalid_key("invalid record key"))?;
        match self.driver.engine().get_owned(rid) {
            Ok((v, rec)) => Ok((Some(TxnRecord::from_record(*id, &rec)?), Precondition::Version(v))),
            Err(StorageError::RecordNotFound { .. }) => Ok((None, Precondition::Absent)),
            Err(e) => Err(e),
        }
    }

    /// Makes progress on pending requests (call after every `tick` / `step` of the driver).
    pub fn poll(&mut self, now_ns: u64) {
        let ready_reads = self.driver.take_ready_reads();
        let pending = std::mem::take(&mut self.pending);
        let mut keep = Vec::new();
        for mut p in pending {
            match self.advance(&mut p, &ready_reads, now_ns) {
                Some(resp) => {
                    self.release(&p);
                    self.replies.push_back(Reply { id: p.id, client: p.client, resp });
                }
                None => keep.push(p),
            }
        }
        self.pending = keep;
    }

    fn release(&mut self, p: &Pending) {
        if let Some(id) = key_id(&p.req.key()) {
            if self.latches.get(id.as_bytes()) == Some(&p.id) {
                self.latches.remove(id.as_bytes());
            }
        }
    }

    /// One step of a pending request; `Some(response)` when it is done.
    fn advance(&mut self, p: &mut Pending, ready: &[ReadToken], now_ns: u64) -> Option<KvResponse> {
        // Proposed: wait for the apply.
        if let PendingState::Proposed { index, resp } = &p.state {
            let rep = self.driver.replica(p.range)?;
            if rep.last_applied() < *index {
                return None; // the client times out if it never applies
            }
            return match rep.wait_applied(*index, Duration::ZERO) {
                Ok(_) => Some(resp.clone()),
                Err(StorageError::ConditionFailed { .. } | StorageError::VersionConflict { .. } | StorageError::RecordAlreadyExists { .. } | StorageError::RecordNotFound { .. }) => {
                    // A concurrent change of the key: evaluate again (keeping the latch).
                    p.state = PendingState::New;
                    None
                }
                Err(StorageError::CommitUnknown { .. }) => Some(KvResponse::NotLeader { leader: self.driver.leader_of(p.range) }),
                Err(e) => Some(KvResponse::Error(e.to_string())),
            };
        }
        if !self.driver.is_leader(p.range) {
            return Some(KvResponse::NotLeader { leader: self.driver.leader_of(p.range) });
        }
        let term = self.driver.replica(p.range).map_or(0, |r| r.current_term());
        if let PendingState::AwaitRead { token, term: t } = &p.state {
            if ready.contains(token) {
                p.state = PendingState::Confirmed;
            } else if *t != term {
                p.state = PendingState::New; // the read was dropped with the old term
            }
        }
        let key = p.req.key();
        let kid = key_id(&key)?;
        if !self.driver.replica(p.range).is_some_and(|r| r.span().contains_id(kid)) {
            return Some(KvResponse::WrongRange);
        }
        // Latch: one in-flight write per key; reads wait for it.
        if self.latches.get(kid.as_bytes()).is_some_and(|&holder| holder != p.id) {
            return None;
        }
        if p.req.is_write() {
            self.latches.insert(kid.as_bytes().to_vec(), p.id);
        }
        match p.state {
            PendingState::New => {
                // Read index: evaluate against linearizable, fully applied state.
                return match self.driver.read_index(p.range) {
                    Ok(token) => {
                        p.state = PendingState::AwaitRead { token, term };
                        None
                    }
                    Err(_) => Some(KvResponse::NotLeader { leader: self.driver.leader_of(p.range) }),
                };
            }
            PendingState::AwaitRead { .. } | PendingState::Proposed { .. } => return None,
            PendingState::Confirmed => {}
        }
        let result = self.evaluate(p.range, &p.req, now_ns);
        if self.trace.is_some() {
            let term = self.driver.replica(p.range).map_or(0, |r| r.current_term());
            let floor = self.tscache.get(&p.range).map(|c| c.floor(kid.as_bytes(), None));
            let what = match &result {
                Ok(Eval::Done(r)) => format!("done {r:?}"),
                Ok(Eval::Propose(_, r)) => format!("propose {r:?}"),
                Err(e) => format!("error {e}"),
            };
            let line = format!("n{} r{} t{} hlc {} floor {:?}: {:?} => {what}", self.driver.id(), p.range, term, self.hlc.peek(), floor, p.req);
            if let Some(t) = self.trace.as_mut() {
                t.push(line);
            }
        }
        match result {
            Ok(Eval::Done(resp)) => Some(resp),
            Ok(Eval::Propose(op, resp)) => match self.driver.propose(p.range, &[op], now_ns) {
                Ok(index) => {
                    p.state = PendingState::Proposed { index, resp };
                    None
                }
                Err(_) => Some(KvResponse::NotLeader { leader: self.driver.leader_of(p.range) }),
            },
            Err(e) => Some(KvResponse::Error(e.to_string())),
        }
    }

    fn evaluate(&mut self, range: RangeId, req: &KvRequest, _now_ns: u64) -> Result<Eval, StorageError> {
        let key = req.key();
        let kid = key_id(&key).ok_or_else(|| StorageError::invalid_key("invalid key"))?;
        let put = |m: &MvccKey, pre: Precondition| -> WriteOp<DocumentRecord> {
            WriteOp { id: kid, kind: crate::engine::WriteKind::Put(m.to_record(&key.0, &key.1)), precondition: pre }
        };
        Ok(match req {
            KvRequest::Get { ts, limit, txn, .. } => {
                let (k, _) = self.load(&key)?;
                let out = eval_read(&k, *ts, *limit, txn.as_ref());
                if let ReadOutcome::Value(..) = out {
                    self.cache(range).bump(kid.as_bytes(), *ts, *txn);
                }
                Eval::Done(match out {
                    ReadOutcome::Value(value, ts) => KvResponse::Value { value, ts },
                    ReadOutcome::Uncertain { ts } => KvResponse::Uncertain { ts },
                    ReadOutcome::Conflict(i) => KvResponse::Conflict { intent_txn: i.txn, intent_ts: i.ts },
                })
            }
            KvRequest::Refresh { from, to, txn, .. } => {
                let (k, _) = self.load(&key)?;
                let ok = eval_refresh(&k, *from, *to, txn);
                if ok {
                    self.cache(range).bump(kid.as_bytes(), *to, Some(*txn));
                }
                Eval::Done(KvResponse::Refreshed(ok))
            }
            KvRequest::Put { txn, ts, value, .. } => {
                let (k, pre) = self.load(&key)?;
                let floor = self.cache(range).floor(kid.as_bytes(), Some(&txn.id));
                match eval_write(&k, txn, *ts, value.clone(), floor) {
                    WriteOutcome::Conflict(i) => Eval::Done(KvResponse::Conflict { intent_txn: i.txn, intent_ts: i.ts }),
                    WriteOutcome::Placed { key: nk, ts } => {
                        if nk == k {
                            Eval::Done(KvResponse::Written { ts })
                        } else {
                            Eval::Propose(put(&nk, pre), KvResponse::Written { ts })
                        }
                    }
                }
            }
            KvRequest::Resolve { txn, commit_ts, .. } => {
                let (k, pre) = self.load(&key)?;
                match eval_resolve(&k, txn, *commit_ts) {
                    Some(nk) => Eval::Propose(put(&nk, pre), KvResponse::Resolved),
                    None => Eval::Done(KvResponse::Resolved),
                }
            }
            KvRequest::QueryIntent { txn, ts, prevent, .. } => {
                let (k, _) = self.load(&key)?;
                // Only staging transactions are recovered, and their intents are resolved only
                // after the record is final: the intent is either there or was never written.
                let found = k.intent.as_ref().is_some_and(|i| &i.txn.id == txn && i.ts <= *ts);
                if !found && *prevent {
                    // The intent can never land at or below `ts` now.
                    self.cache(range).bump(kid.as_bytes(), *ts, None);
                }
                Eval::Done(KvResponse::IntentFound(found))
            }
            KvRequest::Record { id, op } => {
                let (rec, pre) = self.load_record(id)?;
                let hlc_now = self.hlc.now();
                let write = |r: &TxnRecord, resp: KvResponse| -> Eval {
                    Eval::Propose(WriteOp { id: kid, kind: crate::engine::WriteKind::Put(r.to_record()), precondition: pre }, resp)
                };
                match op {
                    RecordOp::Get => Eval::Done(KvResponse::Record(rec)),
                    RecordOp::Heartbeat { now, priority, epoch } => match rec {
                        Some(r) if r.is_final() || matches!(r.status, TxnStatus::Staging { .. }) => Eval::Done(KvResponse::Record(Some(r))),
                        Some(mut r) => {
                            r.heartbeat = r.heartbeat.max(*now);
                            r.epoch = r.epoch.max(*epoch);
                            let resp = KvResponse::Record(Some(r.clone()));
                            write(&r, resp)
                        }
                        None => {
                            let r = TxnRecord { id: *id, status: TxnStatus::Pending, ts: *now, epoch: *epoch, heartbeat: *now, priority: *priority };
                            let resp = KvResponse::Record(Some(r.clone()));
                            write(&r, resp)
                        }
                    },
                    RecordOp::Stage { ts, in_flight, epoch, priority } => match rec {
                        Some(r) if r.is_final() => Eval::Done(KvResponse::Record(Some(r))),
                        Some(r) if r.epoch > *epoch => Eval::Done(KvResponse::Record(Some(r))),
                        other => {
                            let heartbeat = other.as_ref().map_or(hlc_now, |r| r.heartbeat.max(hlc_now));
                            let r = TxnRecord { id: *id, status: TxnStatus::Staging { in_flight: in_flight.clone() }, ts: *ts, epoch: *epoch, heartbeat, priority: *priority };
                            let resp = KvResponse::Record(Some(r.clone()));
                            write(&r, resp)
                        }
                    },
                    RecordOp::Commit { ts, staged } => match rec {
                        Some(r) if r.is_final() => Eval::Done(KvResponse::Record(Some(r))),
                        Some(r) if staged.is_some_and(|(sts, ep)| !(matches!(r.status, TxnStatus::Staging { .. }) && r.ts == sts && r.epoch == ep)) => {
                            Eval::Done(KvResponse::Record(Some(r)))
                        }
                        Some(mut r) => {
                            r.status = TxnStatus::Committed;
                            r.ts = r.ts.max(*ts);
                            let resp = KvResponse::Record(Some(r.clone()));
                            write(&r, resp)
                        }
                        None => {
                            // Committing without a record (a lost staging write) is refused:
                            // the record is the source of truth.
                            Eval::Done(KvResponse::Record(None))
                        }
                    },
                    RecordOp::Abort { staged } => match rec {
                        Some(r) if r.is_final() => Eval::Done(KvResponse::Record(Some(r))),
                        Some(r) if staged.is_some_and(|(sts, ep)| matches!(r.status, TxnStatus::Staging { .. }) && !(r.ts == sts && r.epoch == ep)) => {
                            Eval::Done(KvResponse::Record(Some(r)))
                        }
                        other => {
                            let mut r = other.unwrap_or(TxnRecord { id: *id, status: TxnStatus::Aborted, ts: hlc_now, epoch: 0, heartbeat: hlc_now, priority: (hlc_now, *id) });
                            r.status = TxnStatus::Aborted;
                            let resp = KvResponse::Record(Some(r.clone()));
                            write(&r, resp)
                        }
                    },
                    RecordOp::Push { pusher, now, intent_ts, pushee_priority } => match rec {
                        Some(r) if r.is_final() || matches!(r.status, TxnStatus::Staging { .. }) => Eval::Done(KvResponse::Record(Some(r))),
                        Some(mut r) => {
                            let abandoned = now.wall.saturating_sub(r.heartbeat.wall) > TXN_EXPIRY_NS;
                            if *pusher < r.priority || abandoned {
                                r.status = TxnStatus::Aborted;
                                let resp = KvResponse::Record(Some(r.clone()));
                                write(&r, resp)
                            } else {
                                Eval::Done(KvResponse::Record(Some(r)))
                            }
                        }
                        None => {
                            // No record yet (its commit is under way, or its coordinator died
                            // after writing intents). Abort it (its later staging finds the
                            // abort; safe, it has not committed) if the pusher is older or the
                            // intent is stale; otherwise the pusher waits.
                            let abandoned = now.wall.saturating_sub(intent_ts.wall) > TXN_EXPIRY_NS;
                            if *pusher < *pushee_priority || abandoned {
                                let r = TxnRecord { id: *id, status: TxnStatus::Aborted, ts: *now, epoch: 0, heartbeat: *now, priority: *pushee_priority };
                                let resp = KvResponse::Record(Some(r.clone()));
                                write(&r, resp)
                            } else {
                                Eval::Done(KvResponse::Record(None))
                            }
                        }
                    },
                }
            }
        })
    }

    /// Requests waiting here (tests, metrics).
    pub fn pending(&self) -> usize {
        self.pending.len()
    }
}

enum Eval {
    Done(KvResponse),
    Propose(WriteOp<DocumentRecord>, KvResponse),
}

