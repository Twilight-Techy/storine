//! Wire encodings of the range layer (`TASK-143`): node batches between drivers and the KV
//! requests and responses between gateways and range servers.
//!
//! - Little-endian integers, `u32`-length-prefixed byte strings and maps in the SVE encoding
//!   ([`storage_codec::value_wire`]); timestamps as their 12 sortable bytes.
//! - Every decoder takes untrusted bytes: it bounds-checks each field, refuses element counts
//!   larger than the bytes left (no hostile allocation), rejects unknown tags and trailing
//!   bytes, and never panics (`RULE-ERR-01`). Encodings are identical on every platform and
//!   pinned by golden tests (`RULE-SAFE-05`).
//! - A leading format byte ([`FORMAT`]) lets a future version be refused cleanly.

use std::collections::BTreeMap;

use storage_codec::value_wire::{encode_map_body, SveReader};
use storage_types::{Result, StorageError};

use crate::dist::alloc::StoreLoad;
use crate::dist::hlc::Timestamp;
use crate::dist::meta::RangeDescriptor;
use crate::dist::mvcc::{TxnId, TxnMeta, TxnRecord, TxnStatus, Value};
use crate::dist::node::{Batch, RaftMsg};
use crate::dist::server::{Key, KvRequest, KvResponse, MetaOp, RecordOp};
use crate::raft::{AppendEntriesReq, AppendEntriesResp, InstallSnapshotReq, InstallSnapshotResp, KeySpan, RequestVoteReq, RequestVoteResp};

/// Format of every message here.
pub const FORMAT: u8 = 1;

#[cold]
#[inline(never)]
fn bad(what: &'static str) -> StorageError {
    StorageError::corrupted_header(what)
}

// ── Writers ─────────────────────────────────────────────────────────────────────────────

pub(crate) fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_u32(out, b.len() as u32);
    out.extend_from_slice(b);
}

pub(crate) fn put_ts(out: &mut Vec<u8>, ts: Timestamp) {
    out.extend_from_slice(&ts.to_bytes());
}

pub(crate) fn put_key(out: &mut Vec<u8>, k: &Key) {
    put_bytes(out, k.0.as_bytes());
    put_bytes(out, k.1.as_bytes());
}

pub(crate) fn put_value(out: &mut Vec<u8>, v: &Value) {
    match v {
        None => out.push(0),
        Some(m) => {
            out.push(1);
            encode_map_body(m, out);
        }
    }
}

fn put_priority(out: &mut Vec<u8>, p: &(Timestamp, TxnId)) {
    put_ts(out, p.0);
    out.extend_from_slice(&p.1);
}

fn put_meta(out: &mut Vec<u8>, m: &TxnMeta) {
    out.extend_from_slice(&m.id);
    put_bytes(out, m.record_key.as_bytes());
    put_priority(out, &m.priority);
    put_u32(out, m.epoch);
}

fn put_opt_staged(out: &mut Vec<u8>, s: &Option<(Timestamp, u32)>) {
    match s {
        None => out.push(0),
        Some((ts, epoch)) => {
            out.push(1);
            put_ts(out, *ts);
            put_u32(out, *epoch);
        }
    }
}

fn put_record(out: &mut Vec<u8>, r: &TxnRecord) {
    out.extend_from_slice(&r.id);
    match &r.status {
        TxnStatus::Pending => out.push(0),
        TxnStatus::Staging { in_flight } => {
            out.push(1);
            put_u32(out, in_flight.len() as u32);
            for k in in_flight {
                put_key(out, k);
            }
        }
        TxnStatus::Committed => out.push(2),
        TxnStatus::Aborted => out.push(3),
    }
    put_ts(out, r.ts);
    put_u32(out, r.epoch);
    put_ts(out, r.heartbeat);
    put_priority(out, &r.priority);
}

// ── Readers ─────────────────────────────────────────────────────────────────────────────

pub(crate) fn ts(r: &mut SveReader<'_>) -> Result<Timestamp> {
    Timestamp::from_bytes(r.take(12)?).ok_or_else(|| bad("bad timestamp"))
}

fn txn_id(r: &mut SveReader<'_>) -> Result<TxnId> {
    let mut id = [0u8; 16];
    id.copy_from_slice(r.take(16)?);
    Ok(id)
}

pub(crate) fn string(r: &mut SveReader<'_>) -> Result<String> {
    Ok(r.str()?.to_string())
}

pub(crate) fn key(r: &mut SveReader<'_>) -> Result<Key> {
    Ok((string(r)?, string(r)?))
}

pub(crate) fn value(r: &mut SveReader<'_>) -> Result<Value> {
    match r.u8()? {
        0 => Ok(None),
        1 => Ok(Some(r.map_body()?)),
        _ => Err(bad("bad value flag")),
    }
}

pub(crate) fn flag(r: &mut SveReader<'_>) -> Result<bool> {
    match r.u8()? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(bad("bad flag")),
    }
}

fn priority(r: &mut SveReader<'_>) -> Result<(Timestamp, TxnId)> {
    Ok((ts(r)?, txn_id(r)?))
}

fn meta(r: &mut SveReader<'_>) -> Result<TxnMeta> {
    Ok(TxnMeta { id: txn_id(r)?, record_key: string(r)?, priority: priority(r)?, epoch: r.u32()? })
}

fn opt_staged(r: &mut SveReader<'_>) -> Result<Option<(Timestamp, u32)>> {
    Ok(if flag(r)? { Some((ts(r)?, r.u32()?)) } else { None })
}

fn keys(r: &mut SveReader<'_>) -> Result<Vec<Key>> {
    let n = r.count(8)?;
    (0..n).map(|_| key(r)).collect()
}

fn record(r: &mut SveReader<'_>) -> Result<TxnRecord> {
    let id = txn_id(r)?;
    let status = match r.u8()? {
        0 => TxnStatus::Pending,
        1 => TxnStatus::Staging { in_flight: keys(r)? },
        2 => TxnStatus::Committed,
        3 => TxnStatus::Aborted,
        _ => return Err(bad("bad transaction status")),
    };
    Ok(TxnRecord { id, status, ts: ts(r)?, epoch: r.u32()?, heartbeat: ts(r)?, priority: priority(r)? })
}

pub(crate) fn finish<T>(r: &SveReader<'_>, v: T) -> Result<T> {
    if r.remaining() != 0 {
        return Err(bad("trailing bytes"));
    }
    Ok(v)
}

fn start<'a>(bytes: &'a [u8]) -> Result<SveReader<'a>> {
    let mut r = SveReader::new(bytes);
    if r.u8()? != FORMAT {
        return Err(bad("unknown range-layer message format"));
    }
    Ok(r)
}

// ── Batches ─────────────────────────────────────────────────────────────────────────────

/// Encodes a driver batch.
pub fn encode_batch(b: &Batch) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + b.msgs.len() * 48);
    out.push(FORMAT);
    out.extend_from_slice(&b.from.to_le_bytes());
    put_u64(&mut out, b.incarnation);
    put_ts(&mut out, b.ts);
    put_u32(&mut out, b.load.replicas);
    put_u32(&mut out, b.load.leases);
    put_u64(&mut out, b.load.bytes);
    put_u32(&mut out, b.load.moving);
    out.push(u8::from(b.meta_leader));
    put_u32(&mut out, b.msgs.len() as u32);
    for (range, msg) in &b.msgs {
        put_u64(&mut out, *range);
        encode_msg(msg, &mut out);
    }
    out
}

fn encode_msg(msg: &RaftMsg, out: &mut Vec<u8>) {
    match msg {
        RaftMsg::Vote(req) => {
            out.push(1);
            out.extend_from_slice(&req.to_wire_bytes());
        }
        RaftMsg::VoteResp { req, resp } => {
            out.push(2);
            out.extend_from_slice(&req.to_wire_bytes());
            out.extend_from_slice(&resp.to_wire_bytes());
        }
        RaftMsg::Append { req, hb, closed } => {
            out.push(3);
            put_bytes(out, &req.to_wire_bytes());
            put_u64(out, *hb);
            put_ts(out, closed.0);
            put_u64(out, closed.1);
        }
        RaftMsg::AppendResp { resp, req_term, last_sent, hb, applied } => {
            out.push(4);
            out.extend_from_slice(&resp.to_wire_bytes());
            for v in [req_term, last_sent, hb, applied] {
                put_u64(out, *v);
            }
        }
        RaftMsg::Snap(req) => {
            out.push(5);
            put_bytes(out, &req.to_wire_bytes());
        }
        RaftMsg::SnapResp { resp, req_term, last_included } => {
            out.push(6);
            out.extend_from_slice(&resp.to_wire_bytes());
            put_u64(out, *req_term);
            put_u64(out, *last_included);
        }
        RaftMsg::Quiesce { term, commit } => {
            out.push(7);
            put_u64(out, *term);
            put_u64(out, *commit);
        }
        RaftMsg::Init { span, members, config } => {
            out.push(8);
            span.encode(out);
            put_u32(out, members.len() as u32);
            for &(id, voter) in members {
                out.extend_from_slice(&id.to_le_bytes());
                out.push(u8::from(voter));
            }
            put_u64(out, config.0);
            put_u64(out, config.1);
        }
        RaftMsg::TimeoutNow { term } => {
            out.push(9);
            put_u64(out, *term);
        }
        RaftMsg::Gone { config_index } => {
            out.push(10);
            put_u64(out, *config_index);
        }
        RaftMsg::Missing => out.push(11),
        RaftMsg::App(bytes) => {
            out.push(12);
            put_bytes(out, bytes);
        }
    }
}

/// Decodes a driver batch from untrusted bytes.
pub fn decode_batch(bytes: &[u8]) -> Result<Batch> {
    let mut r = start(bytes)?;
    let from = u16::from_le_bytes([r.u8()?, r.u8()?]);
    let incarnation = r.u64()?;
    let ts = ts(&mut r)?;
    let load = StoreLoad { replicas: r.u32()?, leases: r.u32()?, bytes: r.u64()?, moving: r.u32()? };
    let meta_leader = flag(&mut r)?;
    let n = r.count(9)?;
    let mut msgs = Vec::with_capacity(n);
    for _ in 0..n {
        let range = r.u64()?;
        msgs.push((range, decode_msg(&mut r)?));
    }
    finish(&r, Batch { from, incarnation, ts, load, meta_leader, msgs })
}

fn decode_msg(r: &mut SveReader<'_>) -> Result<RaftMsg> {
    Ok(match r.u8()? {
        1 => RaftMsg::Vote(RequestVoteReq::from_wire_bytes(r.take(27)?)?),
        2 => RaftMsg::VoteResp { req: RequestVoteReq::from_wire_bytes(r.take(27)?)?, resp: RequestVoteResp::from_wire_bytes(r.take(9)?)? },
        3 => {
            let req = AppendEntriesReq::from_wire_bytes(r.bytes()?)?;
            let hb = r.u64()?;
            let closed = (ts(r)?, r.u64()?);
            RaftMsg::Append { req, hb, closed }
        }
        4 => RaftMsg::AppendResp { resp: AppendEntriesResp::from_wire_bytes(r.take(17)?)?, req_term: r.u64()?, last_sent: r.u64()?, hb: r.u64()?, applied: r.u64()? },
        5 => RaftMsg::Snap(InstallSnapshotReq::from_wire_bytes(r.bytes()?)?),
        6 => RaftMsg::SnapResp { resp: InstallSnapshotResp::from_wire_bytes(r.take(17)?)?, req_term: r.u64()?, last_included: r.u64()? },
        7 => RaftMsg::Quiesce { term: r.u64()?, commit: r.u64()? },
        8 => {
            let span = KeySpan::decode(r)?;
            let n = r.count(3)?;
            let mut members = Vec::with_capacity(n);
            for _ in 0..n {
                let id = u16::from_le_bytes([r.u8()?, r.u8()?]);
                members.push((id, flag(r)?));
            }
            RaftMsg::Init { span, members, config: (r.u64()?, r.u64()?) }
        }
        9 => RaftMsg::TimeoutNow { term: r.u64()? },
        10 => RaftMsg::Gone { config_index: r.u64()? },
        11 => RaftMsg::Missing,
        12 => RaftMsg::App(r.bytes()?.to_vec()),
        _ => return Err(bad("unknown raft message tag")),
    })
}

// ── KV requests ─────────────────────────────────────────────────────────────────────────

/// Encodes a gateway-to-range-server request.
pub fn encode_request(req: &KvRequest) -> Vec<u8> {
    let mut out = vec![FORMAT];
    let o = &mut out;
    match req {
        KvRequest::Get { key, ts, limit, txn } => {
            o.push(1);
            put_key(o, key);
            put_ts(o, *ts);
            put_ts(o, *limit);
            match txn {
                None => o.push(0),
                Some(id) => {
                    o.push(1);
                    o.extend_from_slice(id);
                }
            }
        }
        KvRequest::Put { key, txn, ts, value } => {
            o.push(2);
            put_key(o, key);
            put_meta(o, txn);
            put_ts(o, *ts);
            put_value(o, value);
        }
        KvRequest::Resolve { key, txn, commit_ts } => {
            o.push(3);
            put_key(o, key);
            o.extend_from_slice(txn);
            match commit_ts {
                None => o.push(0),
                Some(t) => {
                    o.push(1);
                    put_ts(o, *t);
                }
            }
        }
        KvRequest::Refresh { key, from, to, txn } => {
            o.push(4);
            put_key(o, key);
            put_ts(o, *from);
            put_ts(o, *to);
            o.extend_from_slice(txn);
        }
        KvRequest::QueryIntent { key, txn, ts, prevent } => {
            o.push(5);
            put_key(o, key);
            o.extend_from_slice(txn);
            put_ts(o, *ts);
            o.push(u8::from(*prevent));
        }
        KvRequest::Record { id, op } => {
            o.push(6);
            o.extend_from_slice(id);
            match op {
                RecordOp::Heartbeat { now, priority, epoch } => {
                    o.push(1);
                    put_ts(o, *now);
                    put_priority(o, priority);
                    put_u32(o, *epoch);
                }
                RecordOp::Stage { ts, in_flight, epoch, priority } => {
                    o.push(2);
                    put_ts(o, *ts);
                    put_u32(o, in_flight.len() as u32);
                    for k in in_flight {
                        put_key(o, k);
                    }
                    put_u32(o, *epoch);
                    put_priority(o, priority);
                }
                RecordOp::Commit { ts, staged } => {
                    o.push(3);
                    put_ts(o, *ts);
                    put_opt_staged(o, staged);
                }
                RecordOp::Abort { staged } => {
                    o.push(4);
                    put_opt_staged(o, staged);
                }
                RecordOp::Push { pusher, now, intent_ts, pushee_priority } => {
                    o.push(5);
                    put_priority(o, pusher);
                    put_ts(o, *now);
                    put_ts(o, *intent_ts);
                    put_priority(o, pushee_priority);
                }
                RecordOp::Get => o.push(6),
            }
        }
        KvRequest::Meta(MetaOp::Lookup { key, limit }) => {
            o.push(7);
            put_bytes(o, key);
            put_u32(o, *limit);
        }
        KvRequest::Meta(MetaOp::Publish { desc, absorbed }) => {
            o.push(8);
            put_bytes(o, &desc.encode());
            put_u32(o, absorbed.len() as u32);
            for a in absorbed {
                put_u64(o, *a);
            }
        }
        KvRequest::Scan { span, ts, limit, txn, max_keys } => {
            o.push(9);
            span.encode(o);
            put_ts(o, *ts);
            put_ts(o, *limit);
            match txn {
                None => o.push(0),
                Some(id) => {
                    o.push(1);
                    o.extend_from_slice(id);
                }
            }
            put_u32(o, *max_keys);
        }
        KvRequest::Atomic { ids, body } => {
            o.push(11);
            put_u32(o, ids.len() as u32);
            for id in ids {
                put_bytes(o, id);
            }
            put_bytes(o, body);
        }
        KvRequest::RefreshSpan { span, from, to, txn } => {
            o.push(10);
            span.encode(o);
            put_ts(o, *from);
            put_ts(o, *to);
            o.extend_from_slice(txn);
        }
        KvRequest::Versions { span, from, to, max_keys } => {
            o.push(12);
            span.encode(o);
            put_ts(o, *from);
            put_ts(o, *to);
            put_u32(o, *max_keys);
        }
        KvRequest::Protect { id, ts, expires } => {
            o.push(13);
            put_u64(o, *id);
            put_ts(o, *ts);
            put_u64(o, *expires);
        }
    }
    out
}

fn opt_txn(r: &mut SveReader<'_>) -> Result<Option<TxnId>> {
    Ok(if flag(r)? { Some(txn_id(r)?) } else { None })
}

/// Decodes a request from untrusted bytes.
pub fn decode_request(bytes: &[u8]) -> Result<KvRequest> {
    let mut r = start(bytes)?;
    let r = &mut r;
    let req = match r.u8()? {
        1 => KvRequest::Get { key: key(r)?, ts: ts(r)?, limit: ts(r)?, txn: opt_txn(r)? },
        2 => KvRequest::Put { key: key(r)?, txn: meta(r)?, ts: ts(r)?, value: value(r)? },
        3 => {
            let key = key(r)?;
            let txn = txn_id(r)?;
            let commit_ts = if flag(r)? { Some(ts(r)?) } else { None };
            KvRequest::Resolve { key, txn, commit_ts }
        }
        4 => KvRequest::Refresh { key: key(r)?, from: ts(r)?, to: ts(r)?, txn: txn_id(r)? },
        5 => KvRequest::QueryIntent { key: key(r)?, txn: txn_id(r)?, ts: ts(r)?, prevent: flag(r)? },
        6 => {
            let id = txn_id(r)?;
            let op = match r.u8()? {
                1 => RecordOp::Heartbeat { now: ts(r)?, priority: priority(r)?, epoch: r.u32()? },
                2 => RecordOp::Stage { ts: ts(r)?, in_flight: keys(r)?, epoch: r.u32()?, priority: priority(r)? },
                3 => RecordOp::Commit { ts: ts(r)?, staged: opt_staged(r)? },
                4 => RecordOp::Abort { staged: opt_staged(r)? },
                5 => RecordOp::Push { pusher: priority(r)?, now: ts(r)?, intent_ts: ts(r)?, pushee_priority: priority(r)? },
                6 => RecordOp::Get,
                _ => return Err(bad("unknown record op")),
            };
            KvRequest::Record { id, op }
        }
        7 => KvRequest::Meta(MetaOp::Lookup { key: r.bytes()?.to_vec(), limit: r.u32()? }),
        8 => {
            let desc = RangeDescriptor::decode(r.bytes()?)?;
            let n = r.count(8)?;
            let absorbed = (0..n).map(|_| r.u64()).collect::<Result<Vec<_>>>()?;
            KvRequest::Meta(MetaOp::Publish { desc, absorbed })
        }
        9 => KvRequest::Scan { span: KeySpan::decode(r)?, ts: ts(r)?, limit: ts(r)?, txn: opt_txn(r)?, max_keys: r.u32()? },
        10 => KvRequest::RefreshSpan { span: KeySpan::decode(r)?, from: ts(r)?, to: ts(r)?, txn: txn_id(r)? },
        12 => KvRequest::Versions { span: KeySpan::decode(r)?, from: ts(r)?, to: ts(r)?, max_keys: r.u32()? },
        13 => KvRequest::Protect { id: r.u64()?, ts: ts(r)?, expires: r.u64()? },
        11 => {
            let n = r.count(4)?;
            let ids = (0..n).map(|_| r.bytes().map(<[u8]>::to_vec)).collect::<Result<Vec<_>>>()?;
            KvRequest::Atomic { ids, body: r.bytes()?.to_vec() }
        }
        _ => return Err(bad("unknown kv request tag")),
    };
    finish(r, req)
}

// ── KV responses ────────────────────────────────────────────────────────────────────────

/// Encodes a range server's response.
pub fn encode_response(resp: &KvResponse) -> Vec<u8> {
    let mut out = vec![FORMAT];
    let o = &mut out;
    match resp {
        KvResponse::Value { value, ts } => {
            o.push(1);
            put_value(o, value);
            match ts {
                None => o.push(0),
                Some(t) => {
                    o.push(1);
                    put_ts(o, *t);
                }
            }
        }
        KvResponse::Uncertain { ts } => {
            o.push(2);
            put_ts(o, *ts);
        }
        KvResponse::Conflict { intent_txn, intent_ts } => {
            o.push(3);
            put_meta(o, intent_txn);
            put_ts(o, *intent_ts);
        }
        KvResponse::Written { ts } => {
            o.push(4);
            put_ts(o, *ts);
        }
        KvResponse::Resolved => o.push(5),
        KvResponse::Refreshed(ok) => {
            o.push(6);
            o.push(u8::from(*ok));
        }
        KvResponse::IntentFound(found) => {
            o.push(7);
            o.push(u8::from(*found));
        }
        KvResponse::Record(rec) => {
            o.push(8);
            match rec {
                None => o.push(0),
                Some(r) => {
                    o.push(1);
                    put_record(o, r);
                }
            }
        }
        KvResponse::Ranges(ds) => {
            o.push(9);
            put_u32(o, ds.len() as u32);
            for d in ds {
                put_bytes(o, &d.encode());
            }
        }
        KvResponse::NotLeader { leader } => {
            o.push(10);
            match leader {
                None => o.push(0),
                Some(l) => {
                    o.push(1);
                    o.extend_from_slice(&l.to_le_bytes());
                }
            }
        }
        KvResponse::WrongRange => o.push(11),
        KvResponse::Error(e) => {
            o.push(12);
            put_bytes(o, e.as_bytes());
        }
        KvResponse::Atomic { versions, duplicate, ts } => {
            o.push(15);
            put_u32(o, versions.len() as u32);
            for v in versions {
                match v {
                    None => o.push(0),
                    Some(n) => {
                        o.push(1);
                        put_u64(o, *n);
                    }
                }
            }
            o.push(u8::from(*duplicate));
            put_ts(o, *ts);
        }
        KvResponse::NotOnePhase => o.push(16),
        KvResponse::Versions { rows, resume } => {
            o.push(17);
            put_u32(o, rows.len() as u32);
            for (k, ver, v) in rows {
                put_key(o, k);
                put_ts(o, *ver);
                put_value(o, v);
            }
            match resume {
                None => o.push(0),
                Some(k) => {
                    o.push(1);
                    put_bytes(o, k);
                }
            }
        }
        KvResponse::HistoryLost { key, below } => {
            o.push(18);
            put_key(o, key);
            put_ts(o, *below);
        }
        KvResponse::KeyConflict { key, intent_txn, intent_ts } => {
            o.push(14);
            put_key(o, key);
            put_meta(o, intent_txn);
            put_ts(o, *intent_ts);
        }
        KvResponse::Rows { rows, resume } => {
            o.push(13);
            put_u32(o, rows.len() as u32);
            for (k, ver, v) in rows {
                put_key(o, k);
                put_ts(o, *ver);
                encode_map_body(v, o);
            }
            match resume {
                None => o.push(0),
                Some(k) => {
                    o.push(1);
                    put_bytes(o, k);
                }
            }
        }
    }
    out
}

/// Decodes a response from untrusted bytes.
pub fn decode_response(bytes: &[u8]) -> Result<KvResponse> {
    let mut r = start(bytes)?;
    let r = &mut r;
    let resp = match r.u8()? {
        1 => {
            let value = value(r)?;
            let ts = if flag(r)? { Some(ts(r)?) } else { None };
            KvResponse::Value { value, ts }
        }
        2 => KvResponse::Uncertain { ts: ts(r)? },
        3 => KvResponse::Conflict { intent_txn: meta(r)?, intent_ts: ts(r)? },
        4 => KvResponse::Written { ts: ts(r)? },
        5 => KvResponse::Resolved,
        6 => KvResponse::Refreshed(flag(r)?),
        7 => KvResponse::IntentFound(flag(r)?),
        8 => KvResponse::Record(if flag(r)? { Some(record(r)?) } else { None }),
        9 => {
            let n = r.count(4)?;
            let ds = (0..n).map(|_| r.bytes().and_then(RangeDescriptor::decode)).collect::<Result<Vec<_>>>()?;
            KvResponse::Ranges(ds)
        }
        10 => KvResponse::NotLeader { leader: if flag(r)? { Some(u16::from_le_bytes([r.u8()?, r.u8()?])) } else { None } },
        11 => KvResponse::WrongRange,
        12 => KvResponse::Error(String::from_utf8_lossy(r.bytes()?).into_owned()),
        13 => {
            let n = r.count(24)?;
            let mut rows = Vec::with_capacity(n);
            for _ in 0..n {
                let k = key(r)?;
                let ver = ts(r)?;
                let v: BTreeMap<String, storage_codec::PropertyValue> = r.map_body()?;
                rows.push((k, ver, v));
            }
            let resume = if flag(r)? { Some(r.bytes()?.to_vec()) } else { None };
            KvResponse::Rows { rows, resume }
        }
        14 => KvResponse::KeyConflict { key: key(r)?, intent_txn: meta(r)?, intent_ts: ts(r)? },
        15 => {
            let n = r.count(1)?;
            let mut versions = Vec::with_capacity(n);
            for _ in 0..n {
                versions.push(if flag(r)? { Some(r.u64()?) } else { None });
            }
            KvResponse::Atomic { versions, duplicate: flag(r)?, ts: ts(r)? }
        }
        16 => KvResponse::NotOnePhase,
        17 => {
            let n = r.count(21)?;
            let mut rows = Vec::with_capacity(n);
            for _ in 0..n {
                rows.push((key(r)?, ts(r)?, value(r)?));
            }
            let resume = if flag(r)? { Some(r.bytes()?.to_vec()) } else { None };
            KvResponse::Versions { rows, resume }
        }
        18 => KvResponse::HistoryLost { key: key(r)?, below: ts(r)? },
        _ => return Err(bad("unknown kv response tag")),
    };
    finish(r, resp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::RaftLogEntry;
    use storage_codec::PropertyValue;

    fn t(w: u64, l: u32) -> Timestamp {
        Timestamp { wall: w, logical: l }
    }

    fn sample_requests() -> Vec<KvRequest> {
        let key: Key = ("acct".into(), "a1".into());
        let meta = TxnMeta { id: [7; 16], record_key: "rec".into(), priority: (t(5, 1), [9; 16]), epoch: 3 };
        let value: Value = Some(BTreeMap::from([("bal".to_string(), PropertyValue::Int64(42))]));
        vec![
            KvRequest::Get { key: key.clone(), ts: t(10, 0), limit: t(20, 0), txn: Some([1; 16]) },
            KvRequest::Get { key: key.clone(), ts: t(10, 0), limit: t(10, 0), txn: None },
            KvRequest::Put { key: key.clone(), txn: meta.clone(), ts: t(11, 2), value: value.clone() },
            KvRequest::Put { key: key.clone(), txn: meta, ts: t(11, 2), value: None },
            KvRequest::Resolve { key: key.clone(), txn: [2; 16], commit_ts: Some(t(12, 0)) },
            KvRequest::Resolve { key: key.clone(), txn: [2; 16], commit_ts: None },
            KvRequest::Refresh { key: key.clone(), from: t(1, 0), to: t(2, 0), txn: [3; 16] },
            KvRequest::QueryIntent { key: key.clone(), txn: [4; 16], ts: t(3, 0), prevent: true },
            KvRequest::Record { id: [5; 16], op: RecordOp::Heartbeat { now: t(4, 0), priority: (t(1, 0), [5; 16]), epoch: 1 } },
            KvRequest::Record { id: [5; 16], op: RecordOp::Stage { ts: t(4, 0), in_flight: vec![key.clone()], epoch: 1, priority: (t(1, 0), [5; 16]) } },
            KvRequest::Record { id: [5; 16], op: RecordOp::Commit { ts: t(4, 0), staged: Some((t(4, 0), 1)) } },
            KvRequest::Record { id: [5; 16], op: RecordOp::Abort { staged: None } },
            KvRequest::Record { id: [5; 16], op: RecordOp::Push { pusher: (t(1, 0), [1; 16]), now: t(9, 0), intent_ts: t(8, 0), pushee_priority: (t(2, 0), [2; 16]) } },
            KvRequest::Record { id: [5; 16], op: RecordOp::Get },
            KvRequest::Meta(MetaOp::Lookup { key: b"acct/".to_vec(), limit: 8 }),
            KvRequest::Meta(MetaOp::Publish {
                desc: RangeDescriptor { range: 3, span: KeySpan::new(b"a".to_vec(), Some(b"m".to_vec())), members: vec![(1, true), (2, false)], generation: 9, lease: Some((1, 4)) },
                absorbed: vec![4, 5],
            }),
            KvRequest::Scan { span: KeySpan::new(b"acct/".to_vec(), Some(b"acct0".to_vec())), ts: t(10, 0), limit: t(20, 0), txn: None, max_keys: 100 },
            KvRequest::RefreshSpan { span: KeySpan::all(), from: t(1, 0), to: t(2, 0), txn: [6; 16] },
            KvRequest::Atomic { ids: vec![b"c/a".to_vec(), b"c/b".to_vec()], body: vec![1, 2, 3] },
            KvRequest::Versions { span: KeySpan::new(b"a".to_vec(), None), from: t(1, 0), to: t(2, 1), max_keys: 64 },
            KvRequest::Protect { id: 9, ts: t(3, 0), expires: 77 },
        ]
    }

    fn sample_responses() -> Vec<KvResponse> {
        let meta = TxnMeta { id: [7; 16], record_key: "rec".into(), priority: (t(5, 1), [9; 16]), epoch: 3 };
        let fields = BTreeMap::from([("bal".to_string(), PropertyValue::Int64(42))]);
        vec![
            KvResponse::Value { value: Some(fields.clone()), ts: Some(t(1, 0)) },
            KvResponse::Value { value: None, ts: None },
            KvResponse::Uncertain { ts: t(2, 0) },
            KvResponse::Conflict { intent_txn: meta, intent_ts: t(3, 0) },
            KvResponse::Written { ts: t(4, 0) },
            KvResponse::Resolved,
            KvResponse::Refreshed(true),
            KvResponse::IntentFound(false),
            KvResponse::Record(None),
            KvResponse::Record(Some(TxnRecord {
                id: [1; 16],
                status: TxnStatus::Staging { in_flight: vec![("acct".into(), "a".into())] },
                ts: t(5, 0),
                epoch: 2,
                heartbeat: t(6, 0),
                priority: (t(1, 0), [1; 16]),
            })),
            KvResponse::Ranges(vec![RangeDescriptor { range: 1, span: KeySpan::all(), members: vec![(1, true)], generation: 1, lease: None }]),
            KvResponse::NotLeader { leader: Some(3) },
            KvResponse::NotLeader { leader: None },
            KvResponse::WrongRange,
            KvResponse::Error("boom".into()),
            KvResponse::Rows { rows: vec![(("acct".into(), "a".into()), t(1, 0), fields)], resume: Some(b"acct/b".to_vec()) },
            KvResponse::Rows { rows: Vec::new(), resume: None },
            KvResponse::Atomic { versions: vec![Some(3), None], duplicate: true, ts: t(9, 1) },
            KvResponse::NotOnePhase,
            KvResponse::Versions { rows: vec![(("acct".into(), "a".into()), t(1, 0), Some(BTreeMap::from([("x".to_string(), PropertyValue::Int64(1))]))), (("acct".into(), "a".into()), t(2, 0), None)], resume: Some(b"acct/b".to_vec()) },
            KvResponse::Versions { rows: Vec::new(), resume: None },
            KvResponse::HistoryLost { key: ("acct".into(), "a".into()), below: t(4, 0) },
            KvResponse::KeyConflict { key: ("acct".into(), "b".into()), intent_txn: TxnMeta { id: [2; 16], record_key: "r".into(), priority: (t(1, 0), [2; 16]), epoch: 0 }, intent_ts: t(7, 0) },
        ]
    }

    fn sample_batch() -> Batch {
        let append = AppendEntriesReq { term: 3, leader_id: 1, prev_log_index: 4, prev_log_term: 2, entries: vec![RaftLogEntry { term: 3, index: 5, command: crate::raft::RaftCommand::Noop }], leader_commit: 4 };
        let vote = RequestVoteReq { term: 3, candidate_id: 2, last_log_index: 4, last_log_term: 2, pre_vote: true };
        Batch {
            from: 2,
            incarnation: 77,
            ts: t(1_000, 3),
            load: StoreLoad { replicas: 4, leases: 2, bytes: 1 << 20, moving: 1 },
            meta_leader: true,
            msgs: vec![
                (1, RaftMsg::Vote(vote)),
                (1, RaftMsg::VoteResp { req: vote, resp: RequestVoteResp { term: 3, vote_granted: true } }),
                (2, RaftMsg::Append { req: append, hb: 9, closed: (t(900, 0), 4) }),
                (2, RaftMsg::AppendResp { resp: AppendEntriesResp { term: 3, success: true, match_index: 5 }, req_term: 3, last_sent: 5, hb: 9, applied: 5 }),
                (3, RaftMsg::Snap(InstallSnapshotReq { term: 3, leader_id: 1, last_included_index: 8, last_included_term: 2, offset: 0, data: vec![9; 10], done: true })),
                (3, RaftMsg::SnapResp { resp: InstallSnapshotResp { term: 3, next_offset: 10, installed: true }, req_term: 3, last_included: 8 }),
                (4, RaftMsg::Quiesce { term: 3, commit: 8 }),
                (5, RaftMsg::Init { span: KeySpan::new(b"k".to_vec(), None), members: vec![(1, true), (4, false)], config: (3, 12) }),
                (6, RaftMsg::TimeoutNow { term: 3 }),
                (7, RaftMsg::Gone { config_index: crate::dist::node::GONE_MERGED }),
                (8, RaftMsg::Missing),
                (0, RaftMsg::App(vec![1, 2])),
            ],
        }
    }

    #[test]
    fn everything_round_trips() {
        for req in sample_requests() {
            let bytes = encode_request(&req);
            assert_eq!(format!("{:?}", decode_request(&bytes).unwrap()), format!("{req:?}"));
        }
        for resp in sample_responses() {
            let bytes = encode_response(&resp);
            assert_eq!(format!("{:?}", decode_response(&bytes).unwrap()), format!("{resp:?}"));
        }
        let b = sample_batch();
        assert_eq!(format!("{:?}", decode_batch(&encode_batch(&b)).unwrap()), format!("{b:?}"));
    }

    #[test]
    fn hostile_bytes_are_refused_never_panic() {
        // Every truncation, every single-bit flip and trailing garbage: an error or a value,
        // never a panic or a huge allocation.
        let mut inputs = vec![encode_batch(&sample_batch())];
        inputs.extend(sample_requests().iter().map(encode_request));
        inputs.extend(sample_responses().iter().map(encode_response));
        // A truncated encoding never decodes as its own kind.
        let batch = encode_batch(&sample_batch());
        for cut in 0..batch.len() {
            assert!(decode_batch(&batch[..cut]).is_err(), "batch cut at {cut}");
        }
        for bytes in sample_requests().iter().map(encode_request) {
            for cut in 0..bytes.len() {
                assert!(decode_request(&bytes[..cut]).is_err(), "request cut at {cut}");
            }
        }
        for bytes in sample_responses().iter().map(encode_response) {
            for cut in 0..bytes.len() {
                assert!(decode_response(&bytes[..cut]).is_err(), "response cut at {cut}");
            }
        }
        for bytes in &inputs {
            for cut in 0..bytes.len() {
                let _ = decode_batch(&bytes[..cut]);
                let _ = decode_request(&bytes[..cut]);
                let _ = decode_response(&bytes[..cut]);
            }
            for bit in 0..bytes.len() * 8 {
                let mut b = bytes.clone();
                b[bit / 8] ^= 1 << (bit % 8);
                let _ = decode_batch(&b);
                let _ = decode_request(&b);
                let _ = decode_response(&b);
            }
            let mut long = bytes.clone();
            long.push(0);
            assert!(decode_batch(&long).is_err() && decode_request(&long).is_err() && decode_response(&long).is_err());
        }
        // A count claiming more elements than bytes left is refused before allocating.
        let mut huge = vec![FORMAT, 13];
        huge.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_response(&huge).is_err());
        assert!(decode_request(&[2, 1]).is_err(), "unknown format refused");
    }

    #[test]
    fn golden_bytes() {
        // Pins the format (`RULE-SAFE-05`): any change here is a wire-format change.
        let req = KvRequest::Get { key: ("c".into(), "k".into()), ts: t(1, 2), limit: t(3, 4), txn: None };
        assert_eq!(
            encode_request(&req),
            [1, 1, 1, 0, 0, 0, b'c', 1, 0, 0, 0, b'k', 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 4, 0]
        );
        assert_eq!(encode_response(&KvResponse::Written { ts: t(1, 0) }), [1, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0]);
        let b = Batch { from: 1, incarnation: 2, ts: t(3, 0), load: StoreLoad::default(), meta_leader: false, msgs: vec![(9, RaftMsg::Missing)] };
        let mut want = vec![1, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0];
        want.extend_from_slice(&t(3, 0).to_bytes());
        want.extend_from_slice(&[0; 20]);
        want.push(0);
        want.extend_from_slice(&1u32.to_le_bytes());
        want.extend_from_slice(&9u64.to_le_bytes());
        want.push(11);
        assert_eq!(encode_batch(&b), want);
    }
}
