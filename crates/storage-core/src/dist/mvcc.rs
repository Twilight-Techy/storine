//! Multi-version keys with write intents, and transaction records (`F-GEO-04`, `TASK-140`).
//!
//! Each key of the distributed key space is one engine record whose fields hold:
//! - its committed versions, newest first, each at a hybrid-logical commit timestamp (a value,
//!   or a tombstone for a delete), trimmed below a GC threshold;
//! - at most one **write intent**: a provisional value of one transaction at its write
//!   timestamp, naming the transaction's record so anyone can find out what became of it.
//!
//! This module holds the pure rules a leaseholder applies to one key. The resulting record is
//! then replicated with a version precondition, so every replica applies the same decision:
//! - reads at a timestamp, with uncertainty restarts and conflict detection;
//! - intent placement, with write-too-old pushes and the timestamp-cache floor;
//! - resolution (commit moves the intent into the history, abort drops it);
//! - read refresh (has anything been written in `(from, to]`?).
//!
//! Transaction records live as ordinary keys (`TXN_COLLECTION`), so they replicate, split and
//! route like data.

use std::collections::BTreeMap;

use storage_codec::PropertyValue;
use storage_types::{Result, StorageError};

use crate::dist::hlc::Timestamp;
use crate::document::DocumentRecord;

/// Collection of transaction records.
pub const TXN_COLLECTION: &str = "_txn";

/// A document value: its fields, or `None` for a deletion.
pub type Value = Option<BTreeMap<String, PropertyValue>>;

/// A transaction id (random, 16 bytes).
pub type TxnId = [u8; 16];

/// What an intent says about its transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxnMeta {
    pub id: TxnId,
    /// Key of the transaction record (`TXN_COLLECTION` / hex id).
    pub record_key: String,
    /// Wound-wait priority: lower = older = wins (its begin timestamp, then id).
    pub priority: (Timestamp, TxnId),
    /// Bumped when the transaction restarts; an intent of an older epoch is stale.
    pub epoch: u32,
}

/// A provisional write.
#[derive(Clone, Debug, PartialEq)]
pub struct Intent {
    pub txn: TxnMeta,
    pub ts: Timestamp,
    pub value: Value,
}

/// One key's versions and intent.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MvccKey {
    /// Newest first; strictly decreasing timestamps.
    pub versions: Vec<(Timestamp, Value)>,
    pub intent: Option<Intent>,
    /// Transactions whose intents here were committed recently (newest first, bounded): a
    /// replayed write of one of them (a coordinator retry arriving late) is a no-op instead of
    /// a new intent.
    pub committed_by: Vec<(TxnId, Timestamp)>,
}

/// How many committing transactions a key remembers for replay detection. Older replays are
/// still harmless: their intent lands above the commit and resolution drops it.
const COMMITTED_MEMORY: usize = 16;

const F_VERSIONS: &str = "_mvcc_versions";
const F_INTENT: &str = "_mvcc_intent";
const F_COMMITTED: &str = "_mvcc_committed";

fn ts_val(ts: Timestamp) -> PropertyValue {
    PropertyValue::Bytes(ts.to_bytes().to_vec())
}

fn val_ts(v: Option<&PropertyValue>) -> Result<Timestamp> {
    match v {
        Some(PropertyValue::Bytes(b)) => Timestamp::from_bytes(b).ok_or_else(|| StorageError::corrupted_header("bad mvcc timestamp")),
        _ => Err(StorageError::corrupted_header("missing mvcc timestamp")),
    }
}

fn value_val(v: &Value) -> PropertyValue {
    v.as_ref().map_or(PropertyValue::Null, |m| PropertyValue::Map(m.clone()))
}

fn val_value(v: Option<&PropertyValue>) -> Result<Value> {
    match v {
        Some(PropertyValue::Null) | None => Ok(None),
        Some(PropertyValue::Map(m)) => Ok(Some(m.clone())),
        _ => Err(StorageError::corrupted_header("bad mvcc value")),
    }
}

impl MvccKey {
    /// Reads the layout from a stored record (an absent record is an empty key).
    pub fn from_record(rec: Option<&DocumentRecord>) -> Result<Self> {
        let Some(rec) = rec else { return Ok(Self::default()) };
        let mut out = Self::default();
        if let Some(PropertyValue::List(vs)) = rec.fields.get(F_VERSIONS) {
            for v in vs {
                let PropertyValue::Map(m) = v else { return Err(StorageError::corrupted_header("bad mvcc version")) };
                out.versions.push((val_ts(m.get("t"))?, val_value(m.get("v"))?));
            }
        }
        if let Some(PropertyValue::List(cs)) = rec.fields.get(F_COMMITTED) {
            for c in cs {
                let PropertyValue::List(p) = c else { return Err(StorageError::corrupted_header("bad mvcc commit mark")) };
                match p.as_slice() {
                    [PropertyValue::Bytes(id), t] if id.len() == 16 => {
                        let id: TxnId = id.as_slice().try_into().map_err(|_| StorageError::corrupted_header("bad txn id"))?;
                        out.committed_by.push((id, val_ts(Some(t))?));
                    }
                    _ => return Err(StorageError::corrupted_header("bad mvcc commit mark")),
                }
            }
        }
        if let Some(PropertyValue::Map(m)) = rec.fields.get(F_INTENT) {
            let id = match m.get("id") {
                Some(PropertyValue::Bytes(b)) if b.len() == 16 => b.as_slice().try_into().map_err(|_| StorageError::corrupted_header("bad txn id"))?,
                _ => return Err(StorageError::corrupted_header("bad intent txn id")),
            };
            let record_key = match m.get("rec") {
                Some(PropertyValue::Text(s)) => s.clone(),
                _ => return Err(StorageError::corrupted_header("bad intent record key")),
            };
            let epoch = match m.get("ep") {
                Some(PropertyValue::Int64(e)) => u32::try_from(*e).unwrap_or(0),
                _ => 0,
            };
            out.intent = Some(Intent {
                txn: TxnMeta { id, record_key, priority: (val_ts(m.get("pt"))?, id), epoch },
                ts: val_ts(m.get("t"))?,
                value: val_value(m.get("v"))?,
            });
        }
        Ok(out)
    }

    /// The record storing this key.
    pub fn to_record(&self, collection: &str, key: &str) -> DocumentRecord {
        let mut rec = DocumentRecord::new(collection, key);
        if !self.versions.is_empty() {
            let vs = self
                .versions
                .iter()
                .map(|(t, v)| {
                    let mut m = BTreeMap::new();
                    m.insert("t".to_string(), ts_val(*t));
                    m.insert("v".to_string(), value_val(v));
                    PropertyValue::Map(m)
                })
                .collect();
            rec.fields.insert(F_VERSIONS.to_string(), PropertyValue::List(vs));
        }
        if !self.committed_by.is_empty() {
            let cs = self.committed_by.iter().map(|(id, t)| PropertyValue::List(vec![PropertyValue::Bytes(id.to_vec()), ts_val(*t)])).collect();
            rec.fields.insert(F_COMMITTED.to_string(), PropertyValue::List(cs));
        }
        if let Some(i) = &self.intent {
            let mut m = BTreeMap::new();
            m.insert("id".to_string(), PropertyValue::Bytes(i.txn.id.to_vec()));
            m.insert("rec".to_string(), PropertyValue::Text(i.txn.record_key.clone()));
            m.insert("pt".to_string(), ts_val(i.txn.priority.0));
            m.insert("ep".to_string(), PropertyValue::Int64(i64::from(i.txn.epoch)));
            m.insert("t".to_string(), ts_val(i.ts));
            m.insert("v".to_string(), value_val(&i.value));
            rec.fields.insert(F_INTENT.to_string(), PropertyValue::Map(m));
        }
        rec
    }

    pub fn is_empty(&self) -> bool {
        self.versions.is_empty() && self.intent.is_none()
    }

    /// Newest committed version at or below `ts`.
    pub fn visible_at(&self, ts: Timestamp) -> Option<&(Timestamp, Value)> {
        self.versions.iter().find(|(t, _)| *t <= ts)
    }

    pub fn newest_commit(&self) -> Option<Timestamp> {
        self.versions.first().map(|(t, _)| *t)
    }

    /// Drops versions no read at or above `threshold` can see (keeps the newest at or below).
    pub fn gc(&mut self, threshold: Timestamp) {
        if let Some(pos) = self.versions.iter().position(|(t, _)| *t <= threshold) {
            self.versions.truncate(pos + 1);
            // A tombstone that is the oldest remaining version is invisible at every timestamp.
            if matches!(self.versions.last(), Some((t, None)) if *t <= threshold) {
                self.versions.pop();
            }
        }
    }
}

/// The outcome of a read.
#[derive(Clone, Debug, PartialEq)]
pub enum ReadOutcome {
    /// The value visible at the read timestamp (`None`: absent), and its commit timestamp.
    Value(Value, Option<Timestamp>),
    /// A value was committed in the uncertainty window: restart the read at `ts`.
    Uncertain { ts: Timestamp },
    /// Another transaction's intent at or below the read (or uncertainty) timestamp: find out
    /// what became of it first.
    Conflict(Intent),
}

/// Evaluates a read at `ts` with uncertainty `limit` by transaction `txn` (`None`: a plain read).
pub fn eval_read(k: &MvccKey, ts: Timestamp, limit: Timestamp, txn: Option<&TxnId>) -> ReadOutcome {
    if let Some(i) = &k.intent {
        if Some(&i.txn.id) == txn {
            return ReadOutcome::Value(i.value.clone(), Some(i.ts));
        }
        if i.ts <= limit {
            return ReadOutcome::Conflict(i.clone());
        }
    }
    if let Some((t, _)) = k.versions.iter().find(|(t, _)| *t > ts && *t <= limit) {
        // The newest uncertain version: everything up to it may precede the read.
        let newest_uncertain = k.versions.iter().filter(|(t2, _)| *t2 <= limit).map(|(t2, _)| *t2).max().unwrap_or(*t);
        return ReadOutcome::Uncertain { ts: newest_uncertain };
    }
    match k.visible_at(ts) {
        Some((t, v)) => ReadOutcome::Value(v.clone(), Some(*t)),
        None => ReadOutcome::Value(None, None),
    }
}

/// The outcome of placing an intent.
#[derive(Clone, Debug, PartialEq)]
pub enum WriteOutcome {
    /// Placed (or replaced this transaction's own intent) at `ts`, which may be later than
    /// asked: pushed above the timestamp cache or a newer commit.
    Placed { key: MvccKey, ts: Timestamp },
    /// Another transaction holds an intent on the key.
    Conflict(Intent),
}

/// Places (or rewrites) `txn`'s intent at `ts` or later. `floor`: the timestamp cache (a read
/// at that timestamp was served, so the write must come after it).
pub fn eval_write(k: &MvccKey, txn: &TxnMeta, ts: Timestamp, value: Value, floor: Timestamp) -> WriteOutcome {
    if let Some(i) = &k.intent {
        if i.txn.id != txn.id {
            return WriteOutcome::Conflict(i.clone());
        }
        // A re-sent write of the same transaction changes nothing. Its intent already counts
        // for a parallel commit at any timestamp at or above its own; moving it could break
        // an implicit commit already in force (the keys would commit at different
        // timestamps).
        if i.value == value {
            return WriteOutcome::Placed { key: k.clone(), ts: i.ts };
        }
    }
    // A replay of a write whose transaction already committed here.
    if let Some((_, t)) = k.committed_by.iter().find(|(id, _)| *id == txn.id) {
        return WriteOutcome::Placed { key: k.clone(), ts: *t };
    }
    let mut ts = ts;
    if let Some(newest) = k.newest_commit() {
        if newest >= ts {
            ts = newest.next(); // write-too-old: the write moves after the newer commit
        }
    }
    if floor >= ts {
        ts = floor.next();
    }
    // A rewrite by the same transaction keeps the later timestamp.
    if let Some(i) = &k.intent {
        ts = ts.max(i.ts);
    }
    let mut key = k.clone();
    key.intent = Some(Intent { txn: txn.clone(), ts, value });
    WriteOutcome::Placed { key, ts }
}

/// Resolves `txn`'s intent: committed at `commit_ts`, or aborted (`None`). A no-op if the
/// intent is gone or belongs to another transaction (resolution is idempotent).
pub fn eval_resolve(k: &MvccKey, txn: &TxnId, commit_ts: Option<Timestamp>) -> Option<MvccKey> {
    let i = k.intent.as_ref()?;
    if &i.txn.id != txn {
        return None;
    }
    let mut key = k.clone();
    let intent = key.intent.take()?;
    // Commit exactly at the transaction's timestamp. An intent above it was never part of the
    // commit (a retried write that landed after it): it is dropped, not committed later,
    // which would split the transaction across timestamps.
    if let Some(ts) = commit_ts.filter(|ts| intent.ts <= *ts) {
        let pos = key.versions.iter().position(|(t, _)| *t < ts).unwrap_or(key.versions.len());
        if key.versions.get(pos.wrapping_sub(1)).is_none_or(|(t, _)| *t != ts) {
            key.versions.insert(pos, (ts, intent.value));
        }
        key.committed_by.insert(0, (*txn, ts));
        key.committed_by.truncate(COMMITTED_MEMORY);
    }
    Some(key)
}

/// Read refresh: `true` if nothing was committed or provisionally written by another
/// transaction in `(from, to]`, so a read at `from` is still valid at `to`.
pub fn eval_refresh(k: &MvccKey, from: Timestamp, to: Timestamp, txn: &TxnId) -> bool {
    if let Some(i) = &k.intent {
        if &i.txn.id != txn && i.ts <= to {
            return false;
        }
    }
    !k.versions.iter().any(|(t, _)| *t > from && *t <= to)
}

// ── transaction records ──

/// The state of a transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxnStatus {
    Pending,
    /// Parallel commit in flight: committed iff every key in `in_flight` holds this
    /// transaction's intent at a timestamp at or below the record's `ts`.
    Staging { in_flight: Vec<(String, String)> },
    Committed,
    Aborted,
}

/// A transaction record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxnRecord {
    pub id: TxnId,
    pub status: TxnStatus,
    /// Commit timestamp (staging / committed), or the provisional write timestamp.
    pub ts: Timestamp,
    pub epoch: u32,
    /// Last liveness signal from the coordinator (HLC time).
    pub heartbeat: Timestamp,
    pub priority: (Timestamp, TxnId),
}

pub fn record_key(id: &TxnId) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

impl TxnRecord {
    pub fn to_record(&self) -> DocumentRecord {
        let mut f = BTreeMap::new();
        let (st, inflight) = match &self.status {
            TxnStatus::Pending => (0, None),
            TxnStatus::Staging { in_flight } => (1, Some(in_flight)),
            TxnStatus::Committed => (2, None),
            TxnStatus::Aborted => (3, None),
        };
        f.insert("s".to_string(), PropertyValue::Int64(st));
        f.insert("t".to_string(), ts_val(self.ts));
        f.insert("h".to_string(), ts_val(self.heartbeat));
        f.insert("p".to_string(), ts_val(self.priority.0));
        f.insert("e".to_string(), PropertyValue::Int64(i64::from(self.epoch)));
        if let Some(keys) = inflight {
            f.insert(
                "w".to_string(),
                PropertyValue::List(keys.iter().map(|(c, k)| PropertyValue::List(vec![PropertyValue::Text(c.clone()), PropertyValue::Text(k.clone())])).collect()),
            );
        }
        let mut rec = DocumentRecord::new(TXN_COLLECTION, record_key(&self.id));
        rec.fields = f;
        rec
    }

    pub fn from_record(id: TxnId, rec: &DocumentRecord) -> Result<Self> {
        let bad = || StorageError::corrupted_header("bad transaction record");
        let status = match rec.fields.get("s") {
            Some(PropertyValue::Int64(0)) => TxnStatus::Pending,
            Some(PropertyValue::Int64(1)) => {
                let mut in_flight = Vec::new();
                if let Some(PropertyValue::List(ws)) = rec.fields.get("w") {
                    for w in ws {
                        match w {
                            PropertyValue::List(p) if p.len() == 2 => match (&p[0], &p[1]) {
                                (PropertyValue::Text(c), PropertyValue::Text(k)) => in_flight.push((c.clone(), k.clone())),
                                _ => return Err(bad()),
                            },
                            _ => return Err(bad()),
                        }
                    }
                }
                TxnStatus::Staging { in_flight }
            }
            Some(PropertyValue::Int64(2)) => TxnStatus::Committed,
            Some(PropertyValue::Int64(3)) => TxnStatus::Aborted,
            _ => return Err(bad()),
        };
        let epoch = match rec.fields.get("e") {
            Some(PropertyValue::Int64(e)) => u32::try_from(*e).unwrap_or(0),
            _ => 0,
        };
        Ok(Self { id, status, ts: val_ts(rec.fields.get("t"))?, epoch, heartbeat: val_ts(rec.fields.get("h"))?, priority: (val_ts(rec.fields.get("p"))?, id) })
    }

    pub fn is_final(&self) -> bool {
        matches!(self.status, TxnStatus::Committed | TxnStatus::Aborted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(w: u64) -> Timestamp {
        Timestamp::new(w, 0)
    }
    fn val(x: i64) -> Value {
        let mut m = BTreeMap::new();
        m.insert("x".to_string(), PropertyValue::Int64(x));
        Some(m)
    }
    fn meta(n: u8) -> TxnMeta {
        let id = [n; 16];
        TxnMeta { id, record_key: record_key(&id), priority: (t(u64::from(n)), id), epoch: 0 }
    }
    fn committed(vs: &[(u64, i64)]) -> MvccKey {
        MvccKey { versions: vs.iter().map(|&(w, x)| (t(w), val(x))).collect(), intent: None, committed_by: Vec::new() }
    }

    #[test]
    fn reads_see_the_newest_version_at_their_timestamp() {
        let k = committed(&[(30, 3), (20, 2), (10, 1)]);
        assert_eq!(eval_read(&k, t(25), t(25), None), ReadOutcome::Value(val(2), Some(t(20))));
        assert_eq!(eval_read(&k, t(5), t(5), None), ReadOutcome::Value(None, None));
        assert_eq!(eval_read(&k, t(99), t(99), None), ReadOutcome::Value(val(3), Some(t(30))));
    }

    #[test]
    fn uncertainty_restarts_reads() {
        let k = committed(&[(30, 3), (20, 2)]);
        assert_eq!(eval_read(&k, t(25), t(35), None), ReadOutcome::Uncertain { ts: t(30) });
        assert_eq!(eval_read(&k, t(30), t(35), None), ReadOutcome::Value(val(3), Some(t(30))));
        assert_eq!(eval_read(&k, t(25), t(29), None), ReadOutcome::Value(val(2), Some(t(20))));
    }

    #[test]
    fn intents_conflict_with_other_readers_but_not_their_own() {
        let mut k = committed(&[(10, 1)]);
        k.intent = Some(Intent { txn: meta(1), ts: t(20), value: val(9) });
        assert!(matches!(eval_read(&k, t(25), t(25), None), ReadOutcome::Conflict(_)));
        assert!(matches!(eval_read(&k, t(15), t(25), None), ReadOutcome::Conflict(_)), "in the uncertainty window");
        assert_eq!(eval_read(&k, t(15), t(15), None), ReadOutcome::Value(val(1), Some(t(10))));
        assert_eq!(eval_read(&k, t(15), t(15), Some(&[1; 16])), ReadOutcome::Value(val(9), Some(t(20))));
    }

    #[test]
    fn writes_move_above_newer_commits_and_the_timestamp_cache() {
        let k = committed(&[(30, 3)]);
        let WriteOutcome::Placed { ts, .. } = eval_write(&k, &meta(1), t(20), val(5), Timestamp::ZERO) else { panic!() };
        assert!(ts > t(30), "write-too-old pushes the write: {ts}");
        let WriteOutcome::Placed { ts, .. } = eval_write(&k, &meta(1), t(40), val(5), t(50)) else { panic!() };
        assert!(ts > t(50), "a served read at 50 pushes the write: {ts}");
        let WriteOutcome::Placed { key, ts } = eval_write(&k, &meta(1), t(40), val(5), Timestamp::ZERO) else { panic!() };
        assert_eq!(ts, t(40));
        // Another transaction conflicts; the same one rewrites.
        assert!(matches!(eval_write(&key, &meta(2), t(60), val(6), Timestamp::ZERO), WriteOutcome::Conflict(_)));
        let WriteOutcome::Placed { key: k2, ts: t2 } = eval_write(&key, &meta(1), t(35), val(7), Timestamp::ZERO) else { panic!() };
        assert_eq!(t2, t(40), "a rewrite never moves an intent back");
        assert_eq!(k2.intent.unwrap().value, val(7));
        // A re-sent identical write is a no-op, whatever the floor says.
        let WriteOutcome::Placed { key: k3, ts: t3 } = eval_write(&key, &meta(1), t(90), val(5), t(95)) else { panic!() };
        assert_eq!((k3, t3), (key.clone(), t(40)), "re-sending an intent must not move it");
    }

    #[test]
    fn resolution_commits_or_drops_and_is_idempotent() {
        let k = committed(&[(10, 1)]);
        let WriteOutcome::Placed { key, .. } = eval_write(&k, &meta(1), t(20), val(2), Timestamp::ZERO) else { panic!() };
        let c = eval_resolve(&key, &[1; 16], Some(t(25))).unwrap();
        assert_eq!(c.versions, vec![(t(25), val(2)), (t(10), val(1))]);
        assert!(c.intent.is_none());
        assert_eq!(eval_resolve(&c, &[1; 16], Some(t(25))), None, "already resolved");
        // A late replay of the committed write is a no-op, not a new intent.
        let WriteOutcome::Placed { key: replay, ts } = eval_write(&c, &meta(1), t(5), val(2), t(99)) else { panic!() };
        assert_eq!((replay, ts), (c.clone(), t(25)));
        let a = eval_resolve(&key, &[1; 16], None).unwrap();
        assert_eq!(a, k);
        // An intent above the commit timestamp (it landed after the commit) is dropped.
        let mut late = k.clone();
        late.intent = Some(Intent { txn: meta(3), ts: t(50), value: val(9) });
        assert_eq!(eval_resolve(&late, &[3; 16], Some(t(40))).unwrap().versions, k.versions);
        assert_eq!(eval_resolve(&key, &[2; 16], None), None, "someone else's intent");
    }

    #[test]
    fn refresh_detects_writes_in_the_window() {
        let k = committed(&[(30, 3), (10, 1)]);
        assert!(eval_refresh(&k, t(10), t(29), &[1; 16]));
        assert!(!eval_refresh(&k, t(10), t(30), &[1; 16]));
        let mut k2 = k.clone();
        k2.intent = Some(Intent { txn: meta(2), ts: t(40), value: val(4) });
        assert!(!eval_refresh(&k2, t(30), t(45), &[1; 16]));
        assert!(eval_refresh(&k2, t(30), t(39), &[1; 16]));
        assert!(eval_refresh(&k2, t(30), t(45), &[2; 16]), "its own intent does not invalidate");
    }

    #[test]
    fn gc_keeps_what_reads_above_the_threshold_can_see() {
        let mut k = committed(&[(40, 4), (30, 3), (20, 2), (10, 1)]);
        k.gc(t(25));
        assert_eq!(k.versions, vec![(t(40), val(4)), (t(30), val(3)), (t(20), val(2))]);
        let mut k = MvccKey { versions: vec![(t(30), val(3)), (t(20), None)], intent: None, committed_by: Vec::new() };
        k.gc(t(25));
        assert_eq!(k.versions, vec![(t(30), val(3))], "an old tombstone goes");
    }

    #[test]
    fn records_round_trip() {
        let mut k = committed(&[(30, 3), (10, 1)]);
        k.versions.insert(1, (t(20), None));
        k.intent = Some(Intent { txn: meta(7), ts: t(40), value: val(4) });
        k.committed_by = vec![([5; 16], t(30)), ([4; 16], t(10))];
        let rec = k.to_record("acct", "a1");
        assert_eq!(MvccKey::from_record(Some(&rec)).unwrap(), k);
        let r = TxnRecord {
            id: [7; 16],
            status: TxnStatus::Staging { in_flight: vec![("acct".into(), "a1".into()), ("acct".into(), "b2".into())] },
            ts: t(40),
            epoch: 2,
            heartbeat: t(41),
            priority: (t(7), [7; 16]),
        };
        assert_eq!(TxnRecord::from_record([7; 16], &r.to_record()).unwrap(), r);
    }
}
