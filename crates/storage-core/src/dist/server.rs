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
use crate::dist::meta::{meta_id, RangeDescriptor, RangeIndex, META_COLLECTION, META_START};
use crate::dist::mvcc::{eval_read, eval_refresh, eval_resolve, eval_write, MvccKey, ReadOutcome, TxnId, TxnMeta, TxnRecord, TxnStatus, Value, WriteOutcome, TXN_COLLECTION};
use crate::dist::node::{NodeDriver, RangeId, ReadToken};
use crate::document::{document_id, DocumentRecord};
use crate::engine::{Precondition, WriteOp};
use crate::raft::KeySpan;
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
    /// The meta range (`TASK-138`): descriptor lookups and publications.
    Meta(MetaOp),
    /// Rows of `span` (raw id bytes) visible at `ts` (uncertainty `limit`) to `txn`, at most
    /// `max_keys`, from the range holding `span.start` (`TASK-143`). The reply says where to
    /// resume: the next key, the range's end, or nothing when the span is done.
    Scan { span: KeySpan, ts: Timestamp, limit: Timestamp, txn: Option<TxnId>, max_keys: u32 },
    /// Is a scan of `span` (within one range) at `from` still valid at `to`?
    RefreshSpan { span: KeySpan, from: Timestamp, to: Timestamp, txn: TxnId },
}

#[derive(Clone, Debug)]
pub enum MetaOp {
    /// Descriptors from the range holding `key` (raw id bytes) on, at most `limit`.
    Lookup { key: Vec<u8>, limit: u32 },
    /// A leaseholder's descriptor of its range, and the ranges it absorbed by merges.
    Publish { desc: RangeDescriptor, absorbed: Vec<RangeId> },
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
    /// Range descriptors (meta lookups).
    Ranges(Vec<RangeDescriptor>),
    /// A scan met another transaction's intent on `key`.
    KeyConflict { key: Key, intent_txn: TxnMeta, intent_ts: Timestamp },
    /// Scan rows (key, version timestamp, fields) and where to resume (`None`: done).
    Rows { rows: Vec<(Key, Timestamp, BTreeMap<String, storage_codec::PropertyValue>)>, resume: Option<Vec<u8>> },
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
    /// Proposed at `(index, term)`; `resp` is sent once that entry applies.
    Proposed { index: u64, term: u64, resp: KvResponse },
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
    /// The range's span change (split / merge) the cache was started under.
    span_index: u64,
    low_water: Timestamp,
    keys: BTreeMap<Vec<u8>, ReadMark>,
    /// Scanned spans `[start, end)`: no write may land under them (phantoms). Bounded.
    spans: Vec<(Vec<u8>, Option<Vec<u8>>, ReadMark)>,
}

/// Most scanned spans a timestamp cache remembers before folding them into its floor.
const MAX_CACHED_SPANS: usize = 256;

impl TsCache {
    /// The timestamp a write by `txn` must exceed.
    fn floor(&self, k: &[u8], txn: Option<&TxnId>) -> Timestamp {
        let pick = |m: &ReadMark| if txn.is_some() && m.top_txn.as_ref() == txn { m.others } else { m.top };
        let m = self.keys.get(k).copied().unwrap_or_default();
        let spans = self.spans.iter().filter(|(lo, hi, _)| k >= lo.as_slice() && hi.as_deref().is_none_or(|h| k < h)).map(|(_, _, m)| pick(m)).max().unwrap_or(Timestamp::ZERO);
        pick(&m).max(spans).max(self.low_water)
    }

    /// Records a scan of `[lo, hi)` at `ts` by `txn`.
    fn bump_span(&mut self, lo: Vec<u8>, hi: Option<Vec<u8>>, ts: Timestamp, txn: Option<TxnId>) {
        self.spans.push((lo, hi, ReadMark { top: ts, top_txn: txn, others: Timestamp::ZERO }));
        if self.spans.len() > MAX_CACHED_SPANS {
            // Bounded memory: fold the spans into the floor.
            let max = self.spans.iter().map(|(_, _, m)| m.top).max().unwrap_or(Timestamp::ZERO);
            self.low_water = self.low_water.max(max);
            self.spans.clear();
        }
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
    /// Key -> the (client, request id) writing it.
    latches: BTreeMap<Vec<u8>, (u64, u64)>,
    tscache: BTreeMap<RangeId, TsCache>,
    replies: VecDeque<Reply>,
    /// Evaluation log for debugging (`enable_trace`).
    trace: Option<Vec<String>>,
    /// The meta range's descriptor index, while this node leads it: (range, term, index).
    meta: Option<(RangeId, u64, RangeIndex)>,
    /// Descriptors this node's leaseholders published: range -> (descriptor, acknowledged,
    /// last sent, absorbed ranges sent with it).
    published: BTreeMap<RangeId, (RangeDescriptor, bool, u64, Vec<RangeId>)>,
    /// Publications received from other nodes, being written: request id -> sender.
    meta_waiting: BTreeMap<u64, NodeId>,
    next_internal: u64,
    /// Reads served from closed timestamps (tests, metrics).
    follower_reads: u64,
}

/// Client id of requests the server makes itself (descriptor publications).
const INTERNAL: u64 = u64::MAX;

/// Re-sent if not acknowledged after this long.
const PUBLISH_RETRY_NS: u64 = 1_000_000_000;

/// Server-to-server messages over the driver's batches.
enum MetaMsg {
    Publish(RangeDescriptor, Vec<RangeId>),
    Ack { range: RangeId, generation: u64, term: u64 },
}

impl MetaMsg {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Self::Publish(d, absorbed) => {
                out.push(1);
                let b = d.encode();
                out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                out.extend_from_slice(&b);
                out.extend_from_slice(&(absorbed.len() as u32).to_le_bytes());
                for a in absorbed {
                    out.extend_from_slice(&a.to_le_bytes());
                }
            }
            Self::Ack { range, generation, term } => {
                out.push(2);
                for v in [range, generation, term] {
                    out.extend_from_slice(&v.to_le_bytes());
                }
            }
        }
        out
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        let mut r = storage_codec::value_wire::SveReader::new(bytes);
        match r.u8().ok()? {
            1 => {
                let d = RangeDescriptor::decode(r.bytes().ok()?).ok()?;
                let n = r.count(8).ok()?;
                let absorbed = (0..n).map(|_| r.u64()).collect::<Result<Vec<_>, _>>().ok()?;
                Some(Self::Publish(d, absorbed))
            }
            2 => Some(Self::Ack { range: r.u64().ok()?, generation: r.u64().ok()?, term: r.u64().ok()? }),
            _ => None,
        }
    }
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
    /// Raw id bytes the request is routed by: its key's id, or a scan's start.
    pub fn anchor(&self) -> Vec<u8> {
        match self {
            Self::Scan { span, .. } | Self::RefreshSpan { span, .. } => span.start.clone(),
            _ => key_id(&self.key()).map(|i| i.as_bytes().to_vec()).unwrap_or_default(),
        }
    }

    /// The span a scan covers (`None` for single-key requests).
    pub fn span(&self) -> Option<&KeySpan> {
        match self {
            Self::Scan { span, .. } | Self::RefreshSpan { span, .. } => Some(span),
            _ => None,
        }
    }

    /// The key of a single-key request (scans have none: see [`Self::anchor`]).
    pub fn key(&self) -> Key {
        match self {
            Self::Scan { .. } | Self::RefreshSpan { .. } => (String::new(), String::new()),
            Self::Get { key, .. } | Self::Put { key, .. } | Self::Resolve { key, .. } | Self::Refresh { key, .. } | Self::QueryIntent { key, .. } => key.clone(),
            Self::Record { id, .. } => record_key_of(id),
            // Serialized on one key inside the meta prefix.
            Self::Meta(_) => (META_COLLECTION.to_string(), "index".to_string()),
        }
    }

    fn is_write(&self) -> bool {
        !matches!(self, Self::Get { .. } | Self::Refresh { .. } | Self::Record { op: RecordOp::Get, .. } | Self::Meta(MetaOp::Lookup { .. }) | Self::Scan { .. } | Self::RefreshSpan { .. })
    }

    /// The timestamp the request carries (moves the server's clock forward: causality).
    fn ts(&self) -> Option<Timestamp> {
        match self {
            Self::Get { ts, .. } | Self::Put { ts, .. } | Self::QueryIntent { ts, .. } => Some(*ts),
            Self::Refresh { to, .. } => Some(*to),
            Self::Resolve { commit_ts, .. } => *commit_ts,
            Self::Record { op: RecordOp::Heartbeat { now, .. } | RecordOp::Push { now, .. }, .. } => Some(*now),
            Self::Record { op: RecordOp::Stage { ts, .. } | RecordOp::Commit { ts, .. }, .. } => Some(*ts),
            Self::Scan { ts, .. } => Some(*ts),
            Self::RefreshSpan { to, .. } => Some(*to),
            Self::Record { .. } | Self::Meta(_) => None,
        }
    }
}

impl NodeServer {
    pub fn new(driver: NodeDriver, hlc: Arc<Hlc>) -> Self {
        // Scans walk ids in key order.
        driver.engine().enable_ordered_ids();
        Self { driver, hlc, pending: Vec::new(), latches: BTreeMap::new(), tscache: BTreeMap::new(), replies: VecDeque::new(), trace: None, meta: None, published: BTreeMap::new(), meta_waiting: BTreeMap::new(), next_internal: 0, follower_reads: 0 }
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
        self.range_at(key_id(key)?.as_bytes())
    }

    /// The range of this node holding the raw id bytes `at`, if any.
    pub fn range_at(&self, at: &[u8]) -> Option<RangeId> {
        // A frozen range (being merged) serves nothing: the client re-routes.
        self.driver.ranges().into_iter().find(|&r| self.driver.replica(r).is_some_and(|x| !x.is_frozen() && x.span().contains(at)))
    }

    /// Accepts request `id` from `client`.
    pub fn submit(&mut self, id: u64, client: u64, req: KvRequest) {
        if let Some(ts) = req.ts() {
            // A timestamp further ahead than any clock may be is synthetic (a write closed
            // ahead of time on a global range, `TASK-141`), not a clock reading: it orders the
            // write but does not move the clock.
            let synthetic = ts.wall > self.hlc.physical_now().saturating_add(self.hlc.max_offset_ns());
            if !synthetic {
                if let Err(e) = self.hlc.update(ts) {
                    self.replies.push_back(Reply { id, client, resp: KvResponse::Error(e.to_string()) });
                    return;
                }
            }
        }
        let key = req.key();
        let Some(range) = self.range_at(&req.anchor()) else {
            // Meta requests to a node without the meta range: point at its leaseholder.
            let resp = if matches!(req, KvRequest::Meta(_)) { KvResponse::NotLeader { leader: self.driver.meta_leaseholder() } } else { KvResponse::WrongRange };
            self.replies.push_back(Reply { id, client, resp });
            return;
        };
        if !self.driver.is_leader(range) {
            // Follower read: a read at or below the closed timestamp (its whole uncertainty
            // window included) sees every write it can ever see, and needs no timestamp-cache
            // entry (no write lands at or below the closed timestamp).
            if let KvRequest::Get { ts, limit, txn, .. } = &req {
                if self.driver.closed_timestamp(range).is_some_and(|c| c >= (*ts).max(*limit)) {
                    let resp = match self.load(&key) {
                        Ok((k, _)) => match eval_read(&k, *ts, *limit, txn.as_ref()) {
                            ReadOutcome::Value(value, ts) => KvResponse::Value { value, ts },
                            ReadOutcome::Uncertain { ts } => KvResponse::Uncertain { ts },
                            ReadOutcome::Conflict(i) => KvResponse::Conflict { intent_txn: i.txn, intent_ts: i.ts },
                        },
                        Err(e) => KvResponse::Error(e.to_string()),
                    };
                    self.follower_reads += 1;
                    self.replies.push_back(Reply { id, client, resp });
                    return;
                }
            }
            let leader = self.driver.leader_of(range);
            self.replies.push_back(Reply { id, client, resp: KvResponse::NotLeader { leader } });
            return;
        }
        self.pending.push(Pending { id, client, range, req, state: PendingState::New });
    }

    /// Replies ready to send.
    pub fn take_replies(&mut self) -> Vec<Reply> {
        let mut out = Vec::new();
        for r in self.replies.drain(..).collect::<Vec<_>>() {
            if r.client != INTERNAL {
                out.push(r);
                continue;
            }
            // A publication finished: acknowledge it to its sender (or ourselves).
            let from = self.meta_waiting.remove(&r.id);
            if let KvResponse::Ranges(ds) = &r.resp {
                if let Some(desc) = ds.first() {
                    let (range, generation, term) = (desc.range, desc.generation, desc.lease.map_or(0, |l| l.1));
                    match from {
                        Some(n) => self.driver.send_app(n, MetaMsg::Ack { range, generation, term }.encode()),
                        None => self.on_ack(range, generation, term),
                    }
                }
            }
        }
        out
    }

    fn on_ack(&mut self, range: RangeId, generation: u64, term: u64) {
        if let Some(p) = self.published.get_mut(&range) {
            if p.0.generation == generation && p.0.lease.map_or(0, |l| l.1) == term {
                p.1 = true;
                let done = std::mem::take(&mut p.3);
                self.driver.absorbed_published(range, &done);
            }
        }
    }

    /// Publishes changed descriptors of the ranges led here and serves publications that
    /// arrived from other nodes (`TASK-138`).
    fn publish(&mut self, now_ns: u64) {
        for (from, bytes) in self.driver.take_app_inbox() {
            match MetaMsg::decode(&bytes) {
                Some(MetaMsg::Publish(desc, absorbed)) => {
                    self.next_internal += 1;
                    let id = self.next_internal;
                    self.meta_waiting.insert(id, from);
                    self.submit(id, INTERNAL, KvRequest::Meta(MetaOp::Publish { desc, absorbed }));
                }
                Some(MetaMsg::Ack { range, generation, term }) => self.on_ack(range, generation, term),
                None => {}
            }
        }
        let led: Vec<RangeId> = self.driver.ranges().into_iter().filter(|&r| self.driver.is_leader(r) && self.driver.replica(r).is_some_and(|x| !x.is_frozen())).collect();
        self.published.retain(|r, _| led.contains(r));
        for range in led {
            let Some(desc) = self.driver.descriptor(range) else { continue };
            let changed = self.published.get(&range).is_none_or(|p| p.0 != desc);
            if changed {
                self.published.insert(range, (desc.clone(), false, 0, Vec::new()));
            }
            let Some(p) = self.published.get_mut(&range) else { continue };
            if p.1 || (p.2 != 0 && now_ns.saturating_sub(p.2) < PUBLISH_RETRY_NS) {
                continue;
            }
            p.2 = now_ns.max(1);
            p.3 = self.driver.absorbed_into(range);
            let absorbed = p.3.clone();
            match self.driver.meta_leaseholder() {
                Some(n) if n == self.driver.id() => {
                    self.next_internal += 1;
                    let id = self.next_internal;
                    self.submit(id, INTERNAL, KvRequest::Meta(MetaOp::Publish { desc, absorbed }));
                }
                Some(n) => self.driver.send_app(n, MetaMsg::Publish(desc, absorbed).encode()),
                None => {}
            }
        }
    }

    /// The meta index for `range` in the current term: rebuilt from the descriptor records on
    /// a new lease (they are few: one per range).
    fn meta_index(&mut self, range: RangeId) -> Result<&mut RangeIndex, StorageError> {
        let term = self.driver.replica(range).map_or(0, |r| r.current_term());
        if self.meta.as_ref().is_none_or(|(r, t, _)| *r != range || *t != term) {
            let mut index = RangeIndex::new();
            let engine = self.driver.engine();
            for id in engine.live_ids() {
                if !id.as_bytes().starts_with(META_START) {
                    continue;
                }
                if let Ok((_, rec)) = engine.get_owned(id) {
                    if let Some(storage_codec::PropertyValue::Bytes(b)) = rec.fields.get("d") {
                        if let Ok(d) = RangeDescriptor::decode(b) {
                            index.publish(d, &[]);
                        }
                    }
                }
            }
            self.meta = Some((range, term, index));
        }
        Ok(&mut self.meta.as_mut().expect("just set").2)
    }

    fn cache(&mut self, range: RangeId) -> &mut TsCache {
        let term = self.driver.replica(range).map_or(0, |r| r.current_term());
        let now = self.hlc.now();
        // A predecessor may have closed up to its clock plus its offset, and its clock may lead
        // ours by the maximum offset; after a split or merge, the keys' old range may have
        // closed ahead of time under another policy. Start above all of it (`TASK-141`).
        let closed_floor = self.driver.closed_offset(range).map_or(now, |off| shift(now, off.saturating_add(self.hlc.max_offset_ns().min(i64::MAX as u64) as i64)));
        let now = now.max(closed_floor).max(self.driver.fresh_floor(now));
        let span_index = self.driver.replica(range).map_or(0, |r| r.span_index());
        let c = self.tscache.entry(range).or_default();
        // A split or merge also starts it over: merged-in keys carry reads served by another
        // leaseholder, all at or below this node's clock now (the merge happened after them).
        if c.term != term || c.span_index != span_index {
            // New leaseholder. Every read its predecessor can still serve has a timestamp at
            // or below this node's clock now:
            // - such a read is confirmed by a quorum acknowledging a heartbeat sent after the
            //   read was registered;
            // - that heartbeat carried the old leader's clock, at least the read's timestamp;
            // - every acknowledging node's clock is therefore at least the read's timestamp,
            //   and some acknowledging node voted for this leader afterwards;
            // - votes carry clocks too.
            // Writes land above this floor.
            *c = TsCache { term, span_index, low_water: now, keys: BTreeMap::new(), spans: Vec::new() };
        }
        c
    }

    fn load(&self, key: &Key) -> Result<(MvccKey, Precondition), StorageError> {
        let id = key_id(key).ok_or_else(|| StorageError::invalid_key("invalid key"))?;
        match self.driver.engine().get_owned(id) {
            // Long keys are hashed into ids: never serve or overwrite another key's record.
            Ok((_, rec)) if rec.collection != key.0 || rec.key != key.1 => Err(StorageError::invalid_key("document key hash collision with another document; use a shorter key")),
            // Existence, never the engine version: versions come from each node's own engine
            // (shared by many ranges), so they differ between replicas, and a version check
            // would apply on one replica and fail on another (`RULE-SAFE-09`). Latches and
            // read-index ordering already keep evaluations from interleaving.
            Ok((_, rec)) => Ok((MvccKey::from_record(Some(&rec))?, Precondition::Exists)),
            Err(StorageError::RecordNotFound { .. }) => Ok((MvccKey::default(), Precondition::Absent)),
            Err(e) => Err(e),
        }
    }

    fn load_record(&self, id: &TxnId) -> Result<(Option<TxnRecord>, Precondition), StorageError> {
        let key = record_key_of(id);
        let rid = key_id(&key).ok_or_else(|| StorageError::invalid_key("invalid record key"))?;
        match self.driver.engine().get_owned(rid) {
            Ok((_, rec)) => Ok((Some(TxnRecord::from_record(*id, &rec)?), Precondition::Exists)),
            Err(StorageError::RecordNotFound { .. }) => Ok((None, Precondition::Absent)),
            Err(e) => Err(e),
        }
    }

    /// Makes progress on pending requests (call after every `tick` / `step` of the driver).
    pub fn poll(&mut self, now_ns: u64) {
        self.publish(now_ns);
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
        if p.req.span().is_some() {
            return; // scans read: they never hold a latch
        }
        if let Some(id) = key_id(&p.req.key()) {
            if self.latches.get(id.as_bytes()) == Some(&(p.client, p.id)) {
                self.latches.remove(id.as_bytes());
            }
        }
    }

    /// One step of a pending request; `Some(response)` when it is done.
    fn advance(&mut self, p: &mut Pending, ready: &[ReadToken], now_ns: u64) -> Option<KvResponse> {
        // Proposed: wait for the apply.
        if let PendingState::Proposed { index, term, resp } = &p.state {
            let rep = self.driver.replica(p.range)?;
            if rep.last_applied() < *index {
                return None; // the client times out if it never applies
            }
            return match rep.wait_applied(*index, *term, Duration::ZERO) {
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
        let anchor = p.req.anchor();
        if !self.driver.replica(p.range).is_some_and(|r| r.span().contains(&anchor)) {
            return Some(KvResponse::WrongRange);
        }
        if let Some(span) = p.req.span() {
            // A scan waits for every in-flight write in its span (it must see their intents).
            let (lo, hi) = clip(span, &self.driver.replica(p.range).map(|r| r.span()).unwrap_or_else(KeySpan::all));
            let busy = match &hi {
                Some(h) => self.latches.range::<[u8], _>((std::ops::Bound::Included(lo.as_slice()), std::ops::Bound::Excluded(h.as_slice()))).next().is_some(),
                None => self.latches.range::<[u8], _>((std::ops::Bound::Included(lo.as_slice()), std::ops::Bound::Unbounded)).next().is_some(),
            };
            if busy {
                return None;
            }
        } else {
            // Latch: one in-flight write per key; reads wait for it.
            if self.latches.get(anchor.as_slice()).is_some_and(|&holder| holder != (p.client, p.id)) {
                return None;
            }
            if p.req.is_write() {
                self.latches.insert(anchor.clone(), (p.client, p.id));
            }
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
            let floor = self.tscache.get(&p.range).map(|c| c.floor(&anchor, None));
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
            Ok(Eval::Propose(ops, resp)) => match self.driver.propose(p.range, &ops, now_ns) {
                Ok((index, term)) => {
                    p.state = PendingState::Proposed { index, term, resp };
                    None
                }
                Err(_) => Some(KvResponse::NotLeader { leader: self.driver.leader_of(p.range) }),
            },
            Err(e) => Some(KvResponse::Error(e.to_string())),
        }
    }

    fn evaluate(&mut self, range: RangeId, req: &KvRequest, _now_ns: u64) -> Result<Eval, StorageError> {
        match req {
            KvRequest::Scan { span, ts, limit, txn, max_keys } => return self.eval_scan(range, span, *ts, *limit, txn.as_ref(), *max_keys).map(Eval::Done),
            KvRequest::RefreshSpan { span, from, to, txn } => return self.eval_refresh_span(range, span, *from, *to, txn).map(Eval::Done),
            _ => {}
        }
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
                // Above the timestamp cache and the closed timestamp (`TASK-141`).
                let closed = self.driver.closed_offset(range).map_or(Timestamp::ZERO, |off| shift(self.hlc.now(), off)).max(self.driver.lead_floor());
                let floor = self.cache(range).floor(kid.as_bytes(), Some(&txn.id)).max(closed);
                match eval_write(&k, txn, *ts, value.clone(), floor) {
                    WriteOutcome::Conflict(i) => Eval::Done(KvResponse::Conflict { intent_txn: i.txn, intent_ts: i.ts }),
                    WriteOutcome::Placed { key: nk, ts } => {
                        if nk == k {
                            Eval::Done(KvResponse::Written { ts })
                        } else {
                            Eval::Propose(vec![put(&nk, pre)], KvResponse::Written { ts })
                        }
                    }
                }
            }
            KvRequest::Resolve { txn, commit_ts, .. } => {
                let (k, pre) = self.load(&key)?;
                match eval_resolve(&k, txn, *commit_ts) {
                    Some(nk) => Eval::Propose(vec![put(&nk, pre)], KvResponse::Resolved),
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
            KvRequest::Meta(MetaOp::Lookup { key, limit }) => {
                let index = self.meta_index(range)?;
                Eval::Done(KvResponse::Ranges(index.scan_from(key, (*limit).clamp(1, 1024) as usize)))
            }
            KvRequest::Meta(MetaOp::Publish { desc, absorbed }) => {
                let index = self.meta_index(range)?;
                // Dry run: an older generation (or lease term) than recorded changes nothing.
                let mut probe = index.clone();
                if !probe.publish(desc.clone(), absorbed) {
                    return Ok(Eval::Done(KvResponse::Ranges(vec![desc.clone()])));
                }
                let gone: Vec<RangeId> = absorbed.iter().copied().filter(|a| index.get(*a).is_some() && probe.get(*a).is_none()).collect();
                *index = probe;
                let mut rec = DocumentRecord::new(META_COLLECTION, crate::dist::meta::meta_key(desc.range));
                rec = rec.with("d", storage_codec::PropertyValue::Bytes(desc.encode()));
                let mut ops = vec![WriteOp::put(meta_id(desc.range)?, rec)];
                for a in gone {
                    let id = meta_id(a)?;
                    if self.driver.engine().get_owned(id).is_ok() {
                        ops.push(WriteOp::delete(id));
                    }
                }
                Eval::Propose(ops, KvResponse::Ranges(vec![desc.clone()]))
            }
            KvRequest::Record { id, op } => {
                let (rec, pre) = self.load_record(id)?;
                let hlc_now = self.hlc.now();
                let write = |r: &TxnRecord, resp: KvResponse| -> Eval {
                    Eval::Propose(vec![WriteOp { id: kid, kind: crate::engine::WriteKind::Put(r.to_record()), precondition: pre }], resp)
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
            KvRequest::Scan { .. } | KvRequest::RefreshSpan { .. } => return Err(StorageError::invalid_value("scans are evaluated separately")),
        })
    }

    /// One page of a scan within this range: `O(log N + max_keys)` over the ordered ids.
    fn eval_scan(&mut self, range: RangeId, span: &KeySpan, ts: Timestamp, limit: Timestamp, txn: Option<&TxnId>, max_keys: u32) -> Result<KvResponse, StorageError> {
        let rspan = self.driver.replica(range).map(|r| r.span()).ok_or_else(|| StorageError::invalid_key("range not hosted here"))?;
        let (lo, hi) = clip(span, &rspan);
        let max = (max_keys as usize).clamp(1, MAX_SCAN_KEYS);
        let ids = self.driver.engine().ids_in_span(&lo, hi.as_deref(), max + 1);
        let mut rows = Vec::new();
        let mut resume = None;
        for (i, id) in ids.iter().enumerate() {
            if i == max {
                resume = Some(id.as_bytes().to_vec());
                break;
            }
            let Ok((_, rec)) = self.driver.engine().get_owned(*id) else { continue };
            let k = MvccKey::from_record(Some(&rec))?;
            match eval_read(&k, ts, limit, txn) {
                ReadOutcome::Value(Some(v), vts) => rows.push(((rec.collection.clone(), rec.key.clone()), vts.unwrap_or(ts), v)),
                ReadOutcome::Value(None, _) => {}
                ReadOutcome::Uncertain { ts } => return Ok(KvResponse::Uncertain { ts }),
                ReadOutcome::Conflict(i) => return Ok(KvResponse::KeyConflict { key: (rec.collection.clone(), rec.key.clone()), intent_txn: i.txn, intent_ts: i.ts }),
            }
        }
        if resume.is_none() {
            // The span goes on past this range: the client continues at its end.
            if let Some(end) = &rspan.end {
                if span.end.as_ref().is_none_or(|e| e > end) {
                    resume = Some(end.clone());
                }
            }
        }
        // No write may land under what was scanned (including keys not there yet).
        let covered = resume.clone().or(hi);
        self.cache(range).bump_span(lo, covered, ts, txn.copied());
        Ok(KvResponse::Rows { rows, resume })
    }

    /// Whether nothing in `span` (which must lie within this range) changed in `(from, to]`.
    fn eval_refresh_span(&mut self, range: RangeId, span: &KeySpan, from: Timestamp, to: Timestamp, txn: &TxnId) -> Result<KvResponse, StorageError> {
        let rspan = self.driver.replica(range).map(|r| r.span()).ok_or_else(|| StorageError::invalid_key("range not hosted here"))?;
        let inside = span.start >= rspan.start && match (&span.end, &rspan.end) {
            (_, None) => true,
            (Some(e), Some(re)) => e <= re,
            (None, Some(_)) => false,
        };
        if !inside {
            return Ok(KvResponse::WrongRange);
        }
        for id in self.driver.engine().ids_in_span(&span.start, span.end.as_deref(), usize::MAX) {
            let Ok((_, rec)) = self.driver.engine().get_owned(id) else { continue };
            if !eval_refresh(&MvccKey::from_record(Some(&rec))?, from, to, txn) {
                return Ok(KvResponse::Refreshed(false));
            }
        }
        self.cache(range).bump_span(span.start.clone(), span.end.clone(), to, Some(*txn));
        Ok(KvResponse::Refreshed(true))
    }

    /// Reads this node served as a follower, below its closed timestamps.
    pub fn follower_reads(&self) -> u64 {
        self.follower_reads
    }

    /// Requests waiting here (tests, metrics).
    pub fn pending(&self) -> usize {
        self.pending.len()
    }
}

/// Most rows one scan page returns.
pub const MAX_SCAN_KEYS: usize = 10_000;

/// `span` clipped to `range`: `(start, end)`.
fn clip(span: &KeySpan, range: &KeySpan) -> (Vec<u8>, Option<Vec<u8>>) {
    let lo = span.start.clone().max(range.start.clone());
    let hi = match (&span.end, &range.end) {
        (None, None) => None,
        (Some(e), None) | (None, Some(e)) => Some(e.clone()),
        (Some(a), Some(b)) => Some(a.clone().min(b.clone())),
    };
    (lo, hi)
}

/// `ts` moved by `off` nanoseconds (logical part dropped: a closed bound, not an event).
fn shift(ts: Timestamp, off: i64) -> Timestamp {
    let wall = if off < 0 { ts.wall.saturating_sub(off.unsigned_abs()) } else { ts.wall.saturating_add(off as u64) };
    if off == 0 {
        ts
    } else {
        Timestamp { wall, logical: 0 }
    }
}

enum Eval {
    Done(KvResponse),
    Propose(Vec<WriteOp<DocumentRecord>>, KvResponse),
}

