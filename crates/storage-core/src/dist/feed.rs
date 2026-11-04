//! Resumable change data capture (`F-GEO-08`, `TASK-144`).
//!
//! A changefeed turns the MVCC history into a stream. Its whole state is a [`Cursor`] the consumer
//! keeps, so any node can serve the next page and a consumer resumes after any failure.
//!
//! **A pass** reads every version in `(from, to]` of every key in order, a page at a time, like a
//! read at `to`: intents at or below `to` are resolved first, and later writes land above `to`.
//! When a pass ends, the feed **resolves** `to`: every change at or below it has been delivered.
//! The next pass starts there. So:
//! - changes of one key arrive in timestamp order; across keys, order only by resolved marks
//!   (sort a resolved prefix by timestamp to get commit order);
//! - delivery is at least once: a consumer that loses a page before saving its cursor gets the
//!   page again, so it deduplicates by `(collection, key, ts)`;
//! - a transaction's changes all have one timestamp.
//!
//! **Starting.** A cursor starts at a timestamp. Chain it from a backup's timestamp
//! ([`crate::dist::backup::Manifest::ts`]) to get a snapshot and every change after it.
//!
//! **History.** A feed protects the history above its cursor on every node, for
//! [`PROTECT_TTL_NS`] after each renewal. A consumer away longer, or a key that outruns its bounds
//! ([`crate::dist::mvcc::HISTORY_PROTECTED_MAX_VERSIONS`]), loses history. That is detected,
//! never silent: the feed fails with [`TxnError::HistoryLost`], and the consumer starts over from a
//! new backup.
//!
//! **Cost.** Each pass reads every key of the feed's spans (`O(keys)`, not `O(changes)`):
//! consumers should pace passes. Expiring a document is not a change (its expiry was one).

use std::collections::BTreeMap;

use storage_codec::value_wire::SveReader;
use storage_codec::PropertyValue;
use storage_types::{Result, StorageError};

use crate::dist::backup::{data_spans, is_internal, new_protection_id, strip};
use crate::dist::docs::collection_spans;
use crate::dist::hlc::Timestamp;
use crate::dist::mvcc::Value;
use crate::dist::txn::{commit_wait, KvSender, Txn, TxnError};
use crate::dist::wire::{finish, flag, key, put_bytes, put_key, put_ts, put_u32, put_u64, put_value, string, ts, value, FORMAT};
use crate::raft::KeySpan;

/// How long a feed's protection lasts after its last renewal (nanoseconds).
pub const PROTECT_TTL_NS: u64 = 600_000_000_000;
const PROTECT_RENEW_NS: u64 = 60_000_000_000;
/// Keys per page unless asked otherwise.
pub const DEFAULT_PAGE_KEYS: u32 = 1024;

/// Where a feed stands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cursor {
    /// Names this feed's protection.
    pub id: u64,
    /// Everything at or below this has been delivered.
    pub resolved: Timestamp,
    /// The pass under way: its upper bound (`ZERO`: none) and where it stands (`None`: at the
    /// start).
    pub to: Timestamp,
    pub position: Option<Vec<u8>>,
    /// HLC wall time of the last protection renewal.
    pub renewed: u64,
}

impl Cursor {
    /// A feed of everything after `ts`.
    pub fn at(s: &mut dyn KvSender, ts: Timestamp) -> Self {
        Self::with_id(new_protection_id(s), ts)
    }

    /// A feed of everything after `ts`, protecting its history under `id`.
    pub fn with_id(id: u64, ts: Timestamp) -> Self {
        Self { id, resolved: ts, to: Timestamp::ZERO, position: None, renewed: 0 }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut o = vec![FORMAT];
        put_u64(&mut o, self.id);
        put_ts(&mut o, self.resolved);
        put_ts(&mut o, self.to);
        put_u64(&mut o, self.renewed);
        match &self.position {
            None => o.push(0),
            Some(p) => {
                o.push(1);
                put_bytes(&mut o, p);
            }
        }
        o
    }

    /// Decodes a cursor from untrusted bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut r = SveReader::new(bytes);
        if r.u8()? != FORMAT {
            return Err(StorageError::corrupted_header("unknown changefeed cursor format"));
        }
        let (id, resolved, to, renewed) = (r.u64()?, ts(&mut r)?, ts(&mut r)?, r.u64()?);
        let position = if flag(&mut r)? { Some(r.bytes()?.to_vec()) } else { None };
        finish(&r, Self { id, resolved, to, position, renewed })
    }
}

/// One committed change of one key.
#[derive(Clone, Debug, PartialEq)]
pub struct Change {
    pub collection: String,
    pub key: String,
    pub ts: Timestamp,
    /// The stored value (`None`: deleted; see the accessors for the document layout).
    pub value: Value,
}

impl Change {
    /// The document's fields (`None`: deleted).
    pub fn fields(&self) -> Option<&BTreeMap<String, PropertyValue>> {
        match self.value.as_ref()?.get("f") {
            Some(PropertyValue::Map(f)) => Some(f),
            _ => None,
        }
    }

    /// The document's version after this change.
    pub fn version(&self) -> Option<u64> {
        match self.value.as_ref()?.get("v") {
            Some(PropertyValue::Int64(v)) => Some((*v).max(0) as u64),
            _ => None,
        }
    }

    /// When the document expires (HLC wall nanoseconds), if it does.
    pub fn expires(&self) -> Option<u64> {
        match self.value.as_ref()?.get("x") {
            Some(PropertyValue::Int64(x)) => Some((*x).max(0) as u64),
            _ => None,
        }
    }

    /// One line of JSON (no newline): `{"collection","key","ts","op","version","fields","expires"}`.
    /// `ts` is `"<wall>.<logical>"`, which sorts as strings only per width: compare it as a pair.
    pub fn to_json(&self) -> String {
        let mut o = String::with_capacity(128);
        o.push_str("{\"collection\":");
        json_str(&self.collection, &mut o);
        o.push_str(",\"key\":");
        json_str(&self.key, &mut o);
        o.push_str(&format!(",\"ts\":\"{}.{}\"", self.ts.wall, self.ts.logical));
        match self.fields() {
            Some(f) => {
                o.push_str(",\"op\":\"put\"");
                if let Some(v) = self.version() {
                    o.push_str(&format!(",\"version\":{v}"));
                }
                if let Some(x) = self.expires() {
                    o.push_str(&format!(",\"expires\":{x}"));
                }
                o.push_str(",\"fields\":");
                json_map(f, &mut o);
            }
            None => {
                o.push_str(",\"op\":\"delete\"");
                if let Some(v) = self.version() {
                    o.push_str(&format!(",\"version\":{v}"));
                }
            }
        }
        o.push('}');
        o
    }
}

fn json_str(s: &str, o: &mut String) {
    o.push('"');
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            '\t' => o.push_str("\\t"),
            c if (c as u32) < 0x20 => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push(c),
        }
    }
    o.push('"');
}

fn json_hex(b: &[u8], o: &mut String) {
    for x in b {
        o.push_str(&format!("{x:02x}"));
    }
}

fn json_map(m: &BTreeMap<String, PropertyValue>, o: &mut String) {
    o.push('{');
    for (i, (k, v)) in m.iter().enumerate() {
        if i > 0 {
            o.push(',');
        }
        json_str(k, o);
        o.push(':');
        json_value(v, o);
    }
    o.push('}');
}

/// JSON of a value. Types JSON lacks, and integers beyond what JSON numbers carry exactly in
/// common parsers, are tagged objects or strings, never approximated.
fn json_value(v: &PropertyValue, o: &mut String) {
    const EXACT: i64 = 1 << 53;
    match v {
        PropertyValue::Null => o.push_str("null"),
        PropertyValue::Bool(b) => o.push_str(if *b { "true" } else { "false" }),
        PropertyValue::Int64(n) if n.abs_diff(0) <= EXACT as u64 => o.push_str(&n.to_string()),
        PropertyValue::Int64(n) => o.push_str(&format!("{{\"$int\":\"{n}\"}}")),
        PropertyValue::UInt64(n) if *n <= EXACT as u64 => o.push_str(&n.to_string()),
        PropertyValue::UInt64(n) => o.push_str(&format!("{{\"$int\":\"{n}\"}}")),
        PropertyValue::Int128(n) => o.push_str(&format!("{{\"$int\":\"{n}\"}}")),
        PropertyValue::Float64(f) if f.is_finite() => o.push_str(&format!("{f:?}")),
        PropertyValue::Float64(f) => o.push_str(&format!("{{\"$float\":\"{f}\"}}")),
        PropertyValue::TimestampNs(t) => o.push_str(&format!("{{\"$timestamp_ns\":\"{t}\"}}")),
        PropertyValue::Uuid(u) => {
            o.push_str("{\"$uuid\":\"");
            json_hex(u, o);
            o.push_str("\"}");
        }
        PropertyValue::Id(id) => {
            o.push_str("{\"$id\":");
            json_str(&String::from_utf8_lossy(id.as_bytes()), o);
            o.push('}');
        }
        PropertyValue::Text(s) => json_str(s, o),
        PropertyValue::Bytes(b) => {
            o.push_str("{\"$bytes\":\"");
            json_hex(b, o);
            o.push_str("\"}");
        }
        PropertyValue::Decimal(d) => o.push_str(&format!("{{\"$decimal\":\"{d}\"}}")),
        PropertyValue::List(l) => {
            o.push('[');
            for (i, x) in l.iter().enumerate() {
                if i > 0 {
                    o.push(',');
                }
                json_value(x, o);
            }
            o.push(']');
        }
        PropertyValue::Map(m) => json_map(m, o),
    }
}

/// One step of a feed.
#[derive(Clone, Debug, PartialEq)]
pub struct FeedPage {
    pub changes: Vec<Change>,
    /// Where to continue, once `changes` are safely delivered.
    pub cursor: Cursor,
    /// A pass ended: nothing more will arrive at or below this.
    pub resolved: Option<Timestamp>,
}

impl FeedPage {
    pub fn encode(&self) -> Vec<u8> {
        let mut o = vec![FORMAT];
        put_bytes(&mut o, &self.cursor.encode());
        match self.resolved {
            None => o.push(0),
            Some(t) => {
                o.push(1);
                put_ts(&mut o, t);
            }
        }
        put_u32(&mut o, self.changes.len() as u32);
        for c in &self.changes {
            put_key(&mut o, &(c.collection.clone(), c.key.clone()));
            put_ts(&mut o, c.ts);
            put_value(&mut o, &c.value);
        }
        o
    }

    /// Decodes a page from untrusted bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut r = SveReader::new(bytes);
        if r.u8()? != FORMAT {
            return Err(StorageError::corrupted_header("unknown changefeed page format"));
        }
        let cursor = Cursor::decode(r.bytes()?)?;
        let resolved = if flag(&mut r)? { Some(ts(&mut r)?) } else { None };
        let n = r.count(21)?;
        let mut changes = Vec::with_capacity(n);
        for _ in 0..n {
            let (collection, k) = key(&mut r)?;
            changes.push(Change { collection, key: k, ts: ts(&mut r)?, value: value(&mut r)? });
        }
        finish(&r, Self { changes, cursor, resolved })
    }
}

/// The spans of a feed, in key order.
fn spans_of(collection: Option<&str>) -> Vec<KeySpan> {
    let mut spans = collection.map_or_else(data_spans, collection_spans);
    spans.sort_by(|a, b| a.start.cmp(&b.start));
    spans
}

/// Reads one page of a feed: of `collection`, or of every user collection.
pub fn poll(s: &mut dyn KvSender, cursor: &Cursor, collection: Option<&str>, max_keys: u32) -> std::result::Result<FeedPage, TxnError> {
    let mut next = cursor.clone();
    if next.to == Timestamp::ZERO {
        next.to = s.now();
        next.position = None;
        if next.to <= next.resolved {
            // The clock is behind a timestamp handed in (a backup's, from a clock ahead of ours).
            next.to = Timestamp::ZERO;
            s.sleep(1_000);
            return Ok(FeedPage { changes: Vec::new(), cursor: next, resolved: None });
        }
    }
    let now = s.now().wall;
    if now.saturating_sub(next.renewed) > PROTECT_RENEW_NS {
        s.protect(next.id, next.resolved, now.saturating_add(PROTECT_TTL_NS));
        next.renewed = now;
    }
    let spans = spans_of(collection);
    // The span the position falls in or before.
    let at = next.position.clone();
    let Some(span) = spans.iter().find(|sp| at.as_ref().is_none_or(|p| sp.end.as_ref().is_none_or(|e| p < e))) else {
        return Ok(end_pass(next));
    };
    let start = at.filter(|p| *p > span.start).unwrap_or_else(|| span.start.clone());
    let page = KeySpan::new(start, span.end.clone());
    // A cursor from a node whose clock leads ours by up to the offset: wait for our clock. Further
    // ahead than any clock may be, it was not made by this cluster.
    let clock = s.now();
    if next.to.wall > clock.wall.saturating_add(s.max_offset_ns()) {
        return Err(TxnError::Invalid("this cursor is from the future".into()));
    }
    commit_wait(s, next.to);
    let mut t = Txn::begin_as_of(s, next.to);
    if t.read_ts() != next.to {
        return Err(TxnError::Unavailable("the clock did not reach the cursor's timestamp".into()));
    }
    let (rows, resume) = t.versions_page(s, &page, next.resolved, max_keys.max(1))?;
    let changes = rows
        .into_iter()
        .filter(|((c, _), _, _)| !is_internal(c) && !c.starts_with('$') && collection.is_none_or(|w| w == c))
        .map(|((collection, key), ts, value)| Change { collection, key, ts, value: value.map(strip) })
        .collect();
    let following = match resume {
        Some(r) => Some(r),
        // This span is done: on to the next, if any.
        None => {
            let i = spans.iter().position(|sp| sp.start == span.start).unwrap_or(spans.len());
            spans.get(i + 1).map(|sp| sp.start.clone())
        }
    };
    match following {
        Some(p) => {
            next.position = Some(p);
            Ok(FeedPage { changes, cursor: next, resolved: None })
        }
        None => Ok(FeedPage { changes, ..end_pass(next) }),
    }
}

fn end_pass(mut c: Cursor) -> FeedPage {
    let to = c.to;
    c.resolved = to;
    c.to = Timestamp::ZERO;
    c.position = None;
    FeedPage { changes: Vec::new(), cursor: c, resolved: Some(to) }
}

/// Where a feed's changes go. A sink makes `changes` durable before returning from `deliver`.
pub trait Sink {
    fn deliver(&mut self, changes: &[Change]) -> Result<()>;
    /// Saves where to resume from (after the changes before it are durable).
    fn save_cursor(&mut self, cursor: &Cursor) -> Result<()>;
    /// A pass finished.
    fn resolved(&mut self, _ts: Timestamp) -> Result<()> {
        Ok(())
    }
}

/// Feeds one fetched page to `sink`: the cursor last, so a crash in between means the page is
/// delivered again (at least once), never skipped.
pub fn deliver(sink: &mut dyn Sink, page: &FeedPage) -> Result<()> {
    if !page.changes.is_empty() {
        sink.deliver(&page.changes)?;
    }
    if let Some(ts) = page.resolved {
        sink.resolved(ts)?;
    }
    sink.save_cursor(&page.cursor)
}

/// The request body of `OP_CHANGEFEED`: the cursor (empty: start now), an optional collection,
/// and a page size.
pub fn encode_request(cursor: Option<&Cursor>, collection: Option<&str>, max_keys: u32) -> Vec<u8> {
    let mut o = vec![FORMAT];
    put_bytes(&mut o, &cursor.map(Cursor::encode).unwrap_or_default());
    match collection {
        None => o.push(0),
        Some(c) => {
            o.push(1);
            put_bytes(&mut o, c.as_bytes());
        }
    }
    put_u32(&mut o, max_keys);
    o
}

/// Decodes an `OP_CHANGEFEED` request from untrusted bytes.
pub fn decode_request(bytes: &[u8]) -> Result<(Option<Cursor>, Option<String>, u32)> {
    let mut r = SveReader::new(bytes);
    if r.u8()? != FORMAT {
        return Err(StorageError::corrupted_header("unknown changefeed request format"));
    }
    let c = r.bytes()?;
    let cursor = if c.is_empty() { None } else { Some(Cursor::decode(c)?) };
    let collection = if flag(&mut r)? { Some(string(&mut r)?) } else { None };
    let max_keys = r.u32()?;
    finish(&r, (cursor, collection, max_keys))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cursor() -> Cursor {
        Cursor { id: 7, resolved: Timestamp::new(10, 1), to: Timestamp::new(20, 2), position: Some(b"acct/b".to_vec()), renewed: 5 }
    }

    #[test]
    fn cursor_round_trips_and_refuses_damage() {
        for c in [cursor(), Cursor { to: Timestamp::ZERO, position: None, ..cursor() }] {
            let b = c.encode();
            assert_eq!(Cursor::decode(&b).unwrap(), c);
            for cut in 0..b.len() {
                assert!(Cursor::decode(&b[..cut]).is_err());
            }
            let mut extra = b.clone();
            extra.push(0);
            assert!(Cursor::decode(&extra).is_err());
        }
        assert!(Cursor::decode(&[99]).is_err());
    }

    #[test]
    fn page_round_trips_and_truncations_never_panic() {
        let page = FeedPage {
            changes: vec![
                Change { collection: "c".into(), key: "k".into(), ts: Timestamp::new(1, 0), value: Some(BTreeMap::from([("v".to_string(), PropertyValue::Int64(2))])) },
                Change { collection: "c".into(), key: "k".into(), ts: Timestamp::new(2, 0), value: None },
            ],
            cursor: cursor(),
            resolved: Some(Timestamp::new(20, 2)),
        };
        let b = page.encode();
        assert_eq!(FeedPage::decode(&b).unwrap(), page);
        for cut in 0..b.len() {
            assert!(FeedPage::decode(&b[..cut]).is_err());
        }
    }

    #[test]
    fn request_round_trips() {
        let c = cursor();
        assert_eq!(decode_request(&encode_request(Some(&c), Some("users"), 10)).unwrap(), (Some(c), Some("users".to_string()), 10));
        assert_eq!(decode_request(&encode_request(None, None, 0)).unwrap(), (None, None, 0));
        assert!(decode_request(&[]).is_err());
    }

    #[test]
    fn json_is_exact_and_escaped() {
        let fields = BTreeMap::from([
            ("a\"b".to_string(), PropertyValue::Text("x\ny".into())),
            ("big".to_string(), PropertyValue::Int64(i64::MAX)),
            ("n".to_string(), PropertyValue::Int64(-3)),
            ("f".to_string(), PropertyValue::Float64(f64::NAN)),
            ("b".to_string(), PropertyValue::Bytes(vec![0xAB, 0x01])),
        ]);
        let value = BTreeMap::from([("v".to_string(), PropertyValue::Int64(4)), ("f".to_string(), PropertyValue::Map(fields))]);
        let c = Change { collection: "c".into(), key: "k\"1".into(), ts: Timestamp::new(9, 2), value: Some(value) };
        assert_eq!(
            c.to_json(),
            r#"{"collection":"c","key":"k\"1","ts":"9.2","op":"put","version":4,"fields":{"a\"b":"x\ny","b":{"$bytes":"ab01"},"big":{"$int":"9223372036854775807"},"f":{"$float":"NaN"},"n":-3}}"#
        );
        let d = Change { collection: "c".into(), key: "k".into(), ts: Timestamp::new(1, 0), value: Some(BTreeMap::from([("v".to_string(), PropertyValue::Int64(5))])) };
        assert_eq!(d.to_json(), r#"{"collection":"c","key":"k","ts":"1.0","op":"delete","version":5}"#);
    }
}
