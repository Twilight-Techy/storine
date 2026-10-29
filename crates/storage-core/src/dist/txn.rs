//! Distributed transaction coordinator (`F-GEO-04`, `TASK-140`, `RULE-DIST-02`).
//!
//! A transaction reads at one timestamp and buffers its writes. Commit is a **parallel commit**,
//! one round trip when nothing conflicts:
//! 1. The intents of every write and a *staging* transaction record (listing the written keys
//!    and the commit timestamp) are sent together. When every intent lands at or below that
//!    timestamp, the transaction is committed: anyone can check this from the record and the
//!    intents. The coordinator then records the commit explicitly and resolves the intents.
//!
//! **Serializable**:
//! - Reads leave their timestamp in the leaseholders' timestamp caches, so later writes land
//!   above them.
//! - If a write is pushed above the commit timestamp (a newer read or write), the
//!   transaction's reads are **refreshed**: they are still valid at the new timestamp, or the
//!   transaction restarts.
//! - Reads in the uncertainty window restart at the uncertain value's timestamp (also with a
//!   refresh), so no read misses a write that happened before it in real time.
//!
//! **Deadlock-free (wound-wait)**:
//! - Conflicting intents are resolved by *pushing* their transaction's record. A pusher older
//!   than the holder aborts it ("wounds"); a younger one waits.
//! - Every wait points from a younger to an older transaction, so the wait-for graph is
//!   acyclic. A restarted transaction keeps its original priority, so it eventually is the
//!   oldest and wins.
//! - Abandoned transactions (no heartbeat, stale intents) are aborted by whoever finds them.
//!
//! **Exactly once**: the transaction record is the single source of truth.
//! - Every commit step is idempotent (same id, same intents).
//! - A coordinator that loses contact queries the record (recovering a staging record if
//!   needed) and reports a definite outcome.
//! - Only when the record is unreachable is the outcome reported as unknown.

use std::collections::BTreeMap;

use crate::dist::hlc::Timestamp;
use crate::dist::mvcc::{record_key, TxnId, TxnMeta, TxnRecord, TxnStatus, Value};
use crate::dist::server::{Key, KvRequest, KvResponse, RecordOp};

/// How a coordinator reaches the cluster: requests are routed to their keys' leaseholders and
/// sent in parallel; a result per request (in order).
pub trait KvSender {
    fn send(&mut self, reqs: Vec<KvRequest>) -> Vec<Result<KvResponse, String>>;
    /// The coordinator's hybrid-logical clock.
    fn now(&mut self) -> Timestamp;
    fn max_offset_ns(&self) -> u64;
    /// Waits `us` microseconds (backoff while an older transaction holds a key).
    fn sleep(&mut self, us: u64);
    /// A fresh transaction id.
    fn new_id(&mut self) -> TxnId;
}

/// Why a transaction did not commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxnError {
    /// It must restart (serialization conflict, wounded by an older transaction): retry.
    Retry(String),
    /// The cluster could not be reached; nothing was committed.
    Unavailable(String),
    /// The commit was sent but its outcome could not be learned (`RULE-SAFE-09`).
    CommitUnknown(String),
}

impl core::fmt::Display for TxnError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Retry(m) => write!(f, "transaction must retry: {m}"),
            Self::Unavailable(m) => write!(f, "cluster unavailable: {m}"),
            Self::CommitUnknown(m) => write!(f, "commit outcome unknown: {m}"),
        }
    }
}

const MAX_ATTEMPTS: usize = 64;

/// An open transaction.
#[derive(Debug)]
pub struct Txn {
    meta: TxnMeta,
    read_ts: Timestamp,
    limit: Timestamp,
    write_ts: Timestamp,
    reads: Vec<Key>,
    writes: BTreeMap<Key, Value>,
}

impl Txn {
    /// Starts a transaction; `priority` keeps a restarted transaction's age (`None`: new).
    pub fn begin(s: &mut dyn KvSender, priority: Option<(Timestamp, TxnId)>) -> Self {
        let id = s.new_id();
        let now = s.now();
        let priority = priority.unwrap_or((now, id));
        let limit = now.add_nanos(s.max_offset_ns());
        Txn { meta: TxnMeta { id, record_key: record_key(&id), priority, epoch: 0 }, read_ts: now, limit, write_ts: now, reads: Vec::new(), writes: BTreeMap::new() }
    }

    pub fn id(&self) -> TxnId {
        self.meta.id
    }

    pub fn priority(&self) -> (Timestamp, TxnId) {
        self.meta.priority
    }

    pub fn read_ts(&self) -> Timestamp {
        self.read_ts
    }

    /// Reads `key` (the transaction's own buffered write if any).
    pub fn get(&mut self, s: &mut dyn KvSender, key: &Key) -> Result<Value, TxnError> {
        if let Some(v) = self.writes.get(key) {
            return Ok(v.clone());
        }
        for _ in 0..MAX_ATTEMPTS {
            let req = KvRequest::Get { key: key.clone(), ts: self.read_ts, limit: self.limit, txn: Some(self.meta.id) };
            match one(s, req)? {
                KvResponse::Value { value, .. } => {
                    self.reads.push(key.clone());
                    return Ok(value);
                }
                KvResponse::Uncertain { ts } => {
                    // Possibly written before this read in real time: read above it instead,
                    // which needs the earlier reads to still hold at the new timestamp.
                    self.advance_read_ts(s, ts)?;
                }
                KvResponse::Conflict { intent_txn, intent_ts } => self.push(s, key, &intent_txn, intent_ts)?,
                other => return Err(TxnError::Unavailable(format!("unexpected reply {other:?}"))),
            }
        }
        Err(TxnError::Retry("too many read restarts".into()))
    }

    pub fn put(&mut self, key: Key, fields: std::collections::BTreeMap<String, storage_codec::PropertyValue>) {
        self.writes.insert(key, Some(fields));
    }

    pub fn delete(&mut self, key: Key) {
        self.writes.insert(key, None);
    }

    fn advance_read_ts(&mut self, s: &mut dyn KvSender, to: Timestamp) -> Result<(), TxnError> {
        if to <= self.read_ts {
            return Ok(());
        }
        if !self.refresh(s, self.read_ts, to)? {
            return Err(TxnError::Retry("a read was invalidated by a newer write (refresh failed)".into()));
        }
        self.read_ts = to;
        self.write_ts = self.write_ts.max(to);
        Ok(())
    }

    /// Are all reads still valid at `to`?
    fn refresh(&self, s: &mut dyn KvSender, from: Timestamp, to: Timestamp) -> Result<bool, TxnError> {
        if self.reads.is_empty() {
            return Ok(true);
        }
        let reqs = self.reads.iter().map(|k| KvRequest::Refresh { key: k.clone(), from, to, txn: self.meta.id }).collect();
        for r in s.send(reqs) {
            match r {
                Ok(KvResponse::Refreshed(true)) => {}
                Ok(KvResponse::Refreshed(false)) => return Ok(false),
                Ok(other) => return Err(TxnError::Unavailable(format!("refresh: {other:?}"))),
                Err(e) => return Err(TxnError::Unavailable(e)),
            }
        }
        Ok(true)
    }

    /// Deals with another transaction's intent on `key`: push its record, then resolve the
    /// intent if it is final, or wait if it is older and alive.
    fn push(&mut self, s: &mut dyn KvSender, key: &Key, other: &TxnMeta, intent_ts: Timestamp) -> Result<(), TxnError> {
        let now = s.now();
        let req = KvRequest::Record { id: other.id, op: RecordOp::Push { pusher: self.meta.priority, now, intent_ts, pushee_priority: other.priority } };
        let rec = match one(s, req)? {
            KvResponse::Record(r) => r,
            other => return Err(TxnError::Unavailable(format!("push: {other:?}"))),
        };
        let outcome = match &rec {
            Some(r) if matches!(r.status, TxnStatus::Staging { .. }) => match recover(s, r) {
                Ok(x) => Some(x),
                Err(TxnError::Retry(_)) => None, // re-staged meanwhile: wait and push again
                Err(e) => return Err(e),
            },
            Some(r) if r.status == TxnStatus::Committed => Some(Some(r.ts)),
            Some(r) if r.status == TxnStatus::Aborted => Some(None),
            _ => None, // pending and older (or no record yet and young): wait
        };
        match outcome {
            Some(commit_ts) => {
                one(s, KvRequest::Resolve { key: key.clone(), txn: other.id, commit_ts })?;
            }
            None => s.sleep(20_000),
        }
        Ok(())
    }

    /// Commits; returns the commit timestamp.
    ///
    /// Once the first intent or staging record may have landed, a failure never reports
    /// "nothing committed" on a guess: the outcome is learned from the transaction record
    /// (recovering a staging one), and only if that is unreachable is it `CommitUnknown`.
    pub fn commit(self, s: &mut dyn KvSender) -> Result<Timestamp, TxnError> {
        if self.writes.is_empty() {
            return Ok(self.read_ts); // read-only: one consistent snapshot
        }
        let id = self.meta.id;
        let keys: Vec<Key> = self.writes.keys().cloned().collect();
        match self.commit_attempts(s) {
            Ok(ts) => Ok(ts),
            Err(TxnError::Unavailable(e)) => settle(s, id, &keys).map_err(|_| TxnError::CommitUnknown(e)).and_then(|o| o.ok_or_else(|| TxnError::Retry("aborted".into()))),
            Err(e) => Err(e),
        }
    }

    fn commit_attempts(self, s: &mut dyn KvSender) -> Result<Timestamp, TxnError> {
        let mut me = self;
        let keys: Vec<Key> = me.writes.keys().cloned().collect();
        let mut commit_ts = me.write_ts.max(me.read_ts);
        let mut sent_any = false;
        for _ in 0..MAX_ATTEMPTS {
            let mut reqs: Vec<KvRequest> = me.writes.iter().map(|(k, v)| KvRequest::Put { key: k.clone(), txn: me.meta.clone(), ts: commit_ts, value: v.clone() }).collect();
            reqs.push(KvRequest::Record { id: me.meta.id, op: RecordOp::Stage { ts: commit_ts, in_flight: keys.clone(), epoch: me.meta.epoch, priority: me.meta.priority } });
            let results = s.send(reqs);
            sent_any = true;
            let (puts, stage) = results.split_at(keys.len());
            let mut highest = commit_ts;
            let mut conflict = None;
            let mut failed = None;
            for (k, r) in keys.iter().zip(puts) {
                match r {
                    Ok(KvResponse::Written { ts }) => highest = highest.max(*ts),
                    Ok(KvResponse::Conflict { intent_txn, intent_ts }) => conflict = Some((k.clone(), intent_txn.clone(), *intent_ts)),
                    Ok(other) => failed = Some(format!("{other:?}")),
                    Err(e) => failed = Some(e.clone()),
                }
            }
            let staged = match &stage[0] {
                Ok(KvResponse::Record(Some(r))) => Some(r.clone()),
                Ok(KvResponse::Record(None)) => None,
                Ok(other) => {
                    failed = Some(format!("{other:?}"));
                    None
                }
                Err(e) => {
                    failed = Some(e.clone());
                    None
                }
            };
            if let Some(r) = &staged {
                match r.status {
                    TxnStatus::Aborted => {
                        me.cleanup(s, None);
                        return Err(TxnError::Retry("aborted by an older transaction".into()));
                    }
                    TxnStatus::Committed => {
                        me.cleanup(s, Some(r.ts));
                        return Ok(r.ts);
                    }
                    _ => {}
                }
            }
            if let Some(e) = failed {
                // Some part did not arrive: try again (every step is idempotent); learn the
                // outcome from the record if the cluster stays unreachable.
                if s.send(vec![KvRequest::Record { id: me.meta.id, op: RecordOp::Get }]).into_iter().all(|r| r.is_err()) {
                    return Err(TxnError::CommitUnknown(e));
                }
                continue;
            }
            if let Some((k, other, its)) = conflict {
                me.push(s, &k, &other, its)?;
                continue;
            }
            if highest > commit_ts {
                // Pushed (a newer read or write): commit later, if the reads still hold.
                if !me.refresh(s, me.read_ts, highest)? {
                    me.abort(s);
                    return Err(TxnError::Retry("serialization conflict: a read was overwritten (refresh failed)".into()));
                }
                me.read_ts = highest;
                commit_ts = highest;
                continue;
            }
            // Every intent at or below the staged timestamp: committed.
            if staged.as_ref().is_some_and(|r| matches!(r.status, TxnStatus::Staging { .. }) && r.ts == commit_ts) {
                me.cleanup(s, Some(commit_ts));
                return Ok(commit_ts);
            }
        }
        if sent_any {
            // Learn the definite outcome.
            if let Ok(KvResponse::Record(Some(r))) = one(s, KvRequest::Record { id: me.meta.id, op: RecordOp::Get }) {
                let outcome = if matches!(r.status, TxnStatus::Staging { .. }) { recover(s, &r)? } else if r.status == TxnStatus::Committed { Some(r.ts) } else { None };
                me.cleanup(s, outcome);
                return outcome.ok_or_else(|| TxnError::Retry("could not commit".into()));
            }
            return Err(TxnError::CommitUnknown("commit did not settle".into()));
        }
        Err(TxnError::Retry("too many commit attempts".into()))
    }

    /// Records the final status and resolves the intents (asynchronous work in a real
    /// deployment; done inline here, best effort: anyone finding an intent can resolve it).
    fn cleanup(&self, s: &mut dyn KvSender, commit_ts: Option<Timestamp>) {
        let op = match commit_ts {
            Some(ts) => RecordOp::Commit { ts, staged: None },
            None => RecordOp::Abort { staged: None },
        };
        let _ = s.send(vec![KvRequest::Record { id: self.meta.id, op }]);
        let reqs = self.writes.keys().map(|k| KvRequest::Resolve { key: k.clone(), txn: self.meta.id, commit_ts }).collect();
        let _ = s.send(reqs);
    }

    /// Abandons the transaction.
    pub fn abort(&self, s: &mut dyn KvSender) {
        let r = s.send(vec![KvRequest::Record { id: self.meta.id, op: RecordOp::Abort { staged: None } }]);
        // If it committed meanwhile (a recovery found every intent), resolve as committed.
        let commit_ts = match r.first() {
            Some(Ok(KvResponse::Record(Some(rec)))) if rec.status == TxnStatus::Committed => Some(rec.ts),
            _ => None,
        };
        let reqs = self.writes.keys().map(|k| KvRequest::Resolve { key: k.clone(), txn: self.meta.id, commit_ts }).collect();
        let _ = s.send(reqs);
    }
}

/// Learns a transaction's definite outcome from its record, deciding a staging one and
/// resolving the intents accordingly: `Ok(Some(commit_ts))`, `Ok(None)` (aborted), or `Err` if
/// the record cannot be reached.
fn settle(s: &mut dyn KvSender, id: TxnId, keys: &[Key]) -> Result<Option<Timestamp>, TxnError> {
    for _ in 0..8 {
        let rec = match one(s, KvRequest::Record { id, op: RecordOp::Get }) {
            Ok(KvResponse::Record(r)) => r,
            _ => {
                s.sleep(50_000);
                continue;
            }
        };
        let outcome = match rec {
            None => {
                // No record: abort it so a late staging write cannot commit it afterwards.
                match one(s, KvRequest::Record { id, op: RecordOp::Abort { staged: None } }) {
                    Ok(KvResponse::Record(Some(r))) if r.status == TxnStatus::Committed => Some(r.ts),
                    Ok(KvResponse::Record(Some(_))) => None,
                    _ => continue,
                }
            }
            Some(r) => match r.status {
                TxnStatus::Committed => Some(r.ts),
                TxnStatus::Aborted => None,
                TxnStatus::Staging { .. } => match recover(s, &r) {
                    Ok(o) => o,
                    Err(_) => continue,
                },
                TxnStatus::Pending => match one(s, KvRequest::Record { id, op: RecordOp::Abort { staged: None } }) {
                    Ok(KvResponse::Record(Some(r))) if r.status == TxnStatus::Committed => Some(r.ts),
                    Ok(KvResponse::Record(Some(_))) => None,
                    _ => continue,
                },
            },
        };
        let reqs = keys.iter().map(|k| KvRequest::Resolve { key: k.clone(), txn: id, commit_ts: outcome }).collect();
        let _ = s.send(reqs);
        return Ok(outcome);
    }
    Err(TxnError::CommitUnknown("transaction record unreachable".into()))
}

fn one(s: &mut dyn KvSender, req: KvRequest) -> Result<KvResponse, TxnError> {
    s.send(vec![req]).pop().unwrap_or_else(|| Err("no reply".into())).map_err(TxnError::Unavailable)
}

/// Decides a staging transaction: committed iff every in-flight intent is present at or below
/// its timestamp. Missing intents are prevented from landing later. Returns the commit
/// timestamp, or `None` if aborted.
pub fn recover(s: &mut dyn KvSender, r: &TxnRecord) -> Result<Option<Timestamp>, TxnError> {
    let TxnStatus::Staging { in_flight } = &r.status else {
        return Ok((r.status == TxnStatus::Committed).then_some(r.ts));
    };
    let reqs = in_flight.iter().map(|k| KvRequest::QueryIntent { key: k.clone(), txn: r.id, ts: r.ts, prevent: true }).collect();
    let mut all = true;
    for res in s.send(reqs) {
        match res {
            Ok(KvResponse::IntentFound(true)) => {}
            Ok(KvResponse::IntentFound(false)) => all = false,
            Ok(other) => return Err(TxnError::Unavailable(format!("recover: {other:?}"))),
            Err(e) => return Err(TxnError::Unavailable(e)),
        }
    }
    // Decide on the record as examined: if it was re-staged meanwhile, the decision is
    // refused and the caller pushes again.
    let staged = Some((r.ts, r.epoch));
    let op = if all { RecordOp::Commit { ts: r.ts, staged } } else { RecordOp::Abort { staged } };
    match one(s, KvRequest::Record { id: r.id, op })? {
        KvResponse::Record(Some(rec)) => match rec.status {
            TxnStatus::Committed => Ok(Some(rec.ts)),
            TxnStatus::Aborted => Ok(None),
            // Re-staged meanwhile: undecided; report "not final" by recovering again.
            _ => Err(TxnError::Retry("transaction re-staged during recovery".into())),
        },
        other => Err(TxnError::Unavailable(format!("recover: {other:?}"))),
    }
}

/// Runs `f` in a transaction and commits, restarting on serialization conflicts (with the
/// original priority, so the transaction ages and eventually wins).
pub fn run<R>(s: &mut dyn KvSender, mut f: impl FnMut(&mut Txn, &mut dyn KvSender) -> Result<R, TxnError>) -> Result<(R, Timestamp), TxnError> {
    let mut priority = None;
    let mut last = TxnError::Retry("no attempt".into());
    for _ in 0..MAX_ATTEMPTS {
        let mut t = Txn::begin(s, priority);
        priority = Some(t.priority());
        match f(&mut t, s) {
            Ok(out) => match t.commit(s) {
                Ok(ts) => return Ok((out, ts)),
                Err(e @ TxnError::Retry(_)) => last = e,
                Err(e) => return Err(e),
            },
            Err(e @ TxnError::Retry(_)) => {
                t.abort(s);
                last = e;
            }
            Err(e) => {
                t.abort(s);
                return Err(e);
            }
        }
        s.sleep(1_000);
    }
    Err(last)
}
