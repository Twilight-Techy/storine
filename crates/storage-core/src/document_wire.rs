//! Wire encoding of document requests and responses (`TASK-098`, `protocol/PROTOCOL.md` §5–§7).
//!
//! Shared by `storage-server` (decoding requests) and the Rust client (encoding them); every
//! other SDK implements the same byte layout from the protocol spec. Strings are SVE strings
//! (`u32` length + UTF-8), values and maps are SVE ([`storage_codec::value_wire`]).

use std::collections::BTreeMap;
use std::time::Duration;

use storage_codec::field_ops::{decode_field_ops, encode_field_ops};
use storage_codec::value_wire::{encode_map_body, encode_value, SveReader};
use storage_codec::PropertyValue;
use storage_types::{Result, StorageError, Version};

use crate::document::{DocOp, DocPage, DocQuery, DocWrite, Direction, Document, Filter};
use crate::engine::Precondition;

/// Largest page a query may return in one response.
pub const MAX_PAGE_SIZE: usize = 10_000;
/// Default page size when a query does not set a limit.
pub const DEFAULT_PAGE_SIZE: usize = 1_000;

#[inline]
pub fn put_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

// ── Writes ──

const PRE_NONE: u8 = 0;
const PRE_ABSENT: u8 = 1;
const PRE_EXISTS: u8 = 2;
const PRE_VERSION: u8 = 3;

const W_PUT: u8 = 1;
const W_PUT_TTL: u8 = 2;
const W_DELETE: u8 = 3;
const W_PATCH: u8 = 4;
const W_CHECK: u8 = 5;

/// `[u8 has_request_id | 16B request_id?] [u32 count] write*`.
pub fn encode_write_request(writes: &[DocWrite], request_id: Option<[u8; 16]>, out: &mut Vec<u8>) {
    match request_id {
        Some(rid) => {
            out.push(1);
            out.extend_from_slice(&rid);
        }
        None => out.push(0),
    }
    out.extend_from_slice(&(writes.len() as u32).to_le_bytes());
    for w in writes {
        put_str(out, &w.collection);
        put_str(out, &w.key);
        match w.precondition {
            Precondition::None => out.push(PRE_NONE),
            Precondition::Absent => out.push(PRE_ABSENT),
            Precondition::Exists => out.push(PRE_EXISTS),
            Precondition::Version(v) => {
                out.push(PRE_VERSION);
                out.extend_from_slice(&v.0.to_le_bytes());
            }
        }
        match &w.op {
            DocOp::Put(f) => {
                out.push(W_PUT);
                encode_map_body(f, out);
            }
            DocOp::PutWithTtl(f, ttl) => {
                out.push(W_PUT_TTL);
                let ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
                out.extend_from_slice(&ms.to_le_bytes());
                encode_map_body(f, out);
            }
            DocOp::Delete => out.push(W_DELETE),
            DocOp::Patch(ops) => {
                out.push(W_PATCH);
                encode_field_ops(ops, out);
            }
            DocOp::Check => out.push(W_CHECK),
        }
    }
}

/// Decodes a write request (untrusted bytes; every count is bounded by the bytes present).
pub fn decode_write_request(body: &[u8]) -> Result<(Vec<DocWrite>, Option<[u8; 16]>)> {
    let mut r = SveReader::new(body);
    let request_id = match r.u8()? {
        0 => None,
        1 => {
            let mut rid = [0u8; 16];
            rid.copy_from_slice(r.take(16)?);
            Some(rid)
        }
        _ => return Err(StorageError::invalid_value("bad request-id flag")),
    };
    let n = r.count(11)?;
    let mut writes = Vec::with_capacity(n);
    for _ in 0..n {
        let collection = r.str()?.to_owned();
        let key = r.str()?.to_owned();
        let precondition = match r.u8()? {
            PRE_NONE => Precondition::None,
            PRE_ABSENT => Precondition::Absent,
            PRE_EXISTS => Precondition::Exists,
            PRE_VERSION => Precondition::Version(Version(r.u64()?)),
            _ => return Err(StorageError::invalid_value("unknown precondition")),
        };
        let op = match r.u8()? {
            W_PUT => DocOp::Put(r.map_body()?),
            W_PUT_TTL => {
                let ms = r.u64()?;
                DocOp::PutWithTtl(r.map_body()?, Duration::from_millis(ms))
            }
            W_DELETE => DocOp::Delete,
            W_PATCH => DocOp::Patch(decode_field_ops(&mut r)?),
            W_CHECK => DocOp::Check,
            _ => return Err(StorageError::invalid_value("unknown write kind")),
        };
        writes.push(DocWrite { collection, key, op, precondition });
    }
    if r.remaining() != 0 {
        return Err(StorageError::invalid_value("trailing bytes after write request"));
    }
    Ok((writes, request_id))
}

/// Response: `[u8 duplicate] [u32 count] ([u8 exists] [u64 version])*`.
pub fn encode_write_response(versions: &[Option<Version>], duplicate: bool, out: &mut Vec<u8>) {
    out.push(u8::from(duplicate));
    out.extend_from_slice(&(versions.len() as u32).to_le_bytes());
    for v in versions {
        match v {
            Some(v) => {
                out.push(1);
                out.extend_from_slice(&v.0.to_le_bytes());
            }
            None => {
                out.push(0);
                out.extend_from_slice(&0u64.to_le_bytes());
            }
        }
    }
}

pub fn decode_write_response(body: &[u8]) -> Result<(Vec<Option<Version>>, bool)> {
    let mut r = SveReader::new(body);
    let duplicate = r.u8()? != 0;
    let n = r.count(9)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let exists = r.u8()? != 0;
        let v = r.u64()?;
        out.push(exists.then_some(Version(v)));
    }
    Ok((out, duplicate))
}

// ── Filters ──

const F_EQ: u8 = 1;
const F_NE: u8 = 2;
const F_LT: u8 = 3;
const F_LE: u8 = 4;
const F_GT: u8 = 5;
const F_GE: u8 = 6;
const F_BETWEEN: u8 = 7;
const F_IN: u8 = 8;
const F_PREFIX: u8 = 9;
const F_CONTAINS: u8 = 10;
const F_EXISTS: u8 = 11;
const F_MISSING: u8 = 12;
const F_KEY_PREFIX: u8 = 13;
const F_AND: u8 = 14;
const F_OR: u8 = 15;
const F_NOT: u8 = 16;

pub fn encode_filter(f: &Filter, out: &mut Vec<u8>) {
    let pv = |out: &mut Vec<u8>, tag: u8, p: &str, v: &PropertyValue| {
        out.push(tag);
        put_str(out, p);
        encode_value(v, out);
    };
    match f {
        Filter::Eq(p, v) => pv(out, F_EQ, p, v),
        Filter::Ne(p, v) => pv(out, F_NE, p, v),
        Filter::Lt(p, v) => pv(out, F_LT, p, v),
        Filter::Le(p, v) => pv(out, F_LE, p, v),
        Filter::Gt(p, v) => pv(out, F_GT, p, v),
        Filter::Ge(p, v) => pv(out, F_GE, p, v),
        Filter::Between(p, lo, hi) => {
            pv(out, F_BETWEEN, p, lo);
            encode_value(hi, out);
        }
        Filter::In(p, vs) => {
            out.push(F_IN);
            put_str(out, p);
            out.extend_from_slice(&(vs.len() as u32).to_le_bytes());
            for v in vs {
                encode_value(v, out);
            }
        }
        Filter::Prefix(p, s) => {
            out.push(F_PREFIX);
            put_str(out, p);
            put_str(out, s);
        }
        Filter::Contains(p, v) => pv(out, F_CONTAINS, p, v),
        Filter::Exists(p) => {
            out.push(F_EXISTS);
            put_str(out, p);
        }
        Filter::Missing(p) => {
            out.push(F_MISSING);
            put_str(out, p);
        }
        Filter::KeyPrefix(s) => {
            out.push(F_KEY_PREFIX);
            put_str(out, s);
        }
        Filter::And(fs) | Filter::Or(fs) => {
            out.push(if matches!(f, Filter::And(_)) { F_AND } else { F_OR });
            out.extend_from_slice(&(fs.len() as u32).to_le_bytes());
            for x in fs {
                encode_filter(x, out);
            }
        }
        Filter::Not(x) => {
            out.push(F_NOT);
            encode_filter(x, out);
        }
    }
}

pub fn decode_filter(r: &mut SveReader<'_>) -> Result<Filter> {
    decode_filter_depth(r, 0)
}

fn decode_filter_depth(r: &mut SveReader<'_>, depth: usize) -> Result<Filter> {
    if depth > 32 {
        return Err(StorageError::invalid_value("filter nesting is too deep (max 32)"));
    }
    let tag = r.u8()?;
    let path = |r: &mut SveReader<'_>| -> Result<String> { Ok(r.str()?.to_owned()) };
    Ok(match tag {
        F_EQ => Filter::Eq(path(r)?, r.value()?),
        F_NE => Filter::Ne(path(r)?, r.value()?),
        F_LT => Filter::Lt(path(r)?, r.value()?),
        F_LE => Filter::Le(path(r)?, r.value()?),
        F_GT => Filter::Gt(path(r)?, r.value()?),
        F_GE => Filter::Ge(path(r)?, r.value()?),
        F_BETWEEN => {
            let p = path(r)?;
            let lo = r.value()?;
            Filter::Between(p, lo, r.value()?)
        }
        F_IN => {
            let p = path(r)?;
            let n = r.count(1)?;
            let mut vs = Vec::with_capacity(n);
            for _ in 0..n {
                vs.push(r.value()?);
            }
            Filter::In(p, vs)
        }
        F_PREFIX => {
            let p = path(r)?;
            Filter::Prefix(p, r.str()?.to_owned())
        }
        F_CONTAINS => Filter::Contains(path(r)?, r.value()?),
        F_EXISTS => Filter::Exists(path(r)?),
        F_MISSING => Filter::Missing(path(r)?),
        F_KEY_PREFIX => Filter::KeyPrefix(r.str()?.to_owned()),
        F_AND | F_OR => {
            let n = r.count(1)?;
            let mut fs = Vec::with_capacity(n);
            for _ in 0..n {
                fs.push(decode_filter_depth(r, depth + 1)?);
            }
            if tag == F_AND {
                Filter::And(fs)
            } else {
                Filter::Or(fs)
            }
        }
        F_NOT => Filter::Not(Box::new(decode_filter_depth(r, depth + 1)?)),
        _ => return Err(StorageError::invalid_value("unknown filter kind")),
    })
}

// ── Queries ──

/// `str collection | u8 has_filter | filter? | u32 order_count | (str path, u8 desc)* |
///  u32 limit (0 = default) | u32 offset | u8 has_after | str after?`
pub fn encode_query(q: &DocQuery, out: &mut Vec<u8>) {
    put_str(out, &q.collection);
    match &q.filter {
        Some(f) => {
            out.push(1);
            encode_filter(f, out);
        }
        None => out.push(0),
    }
    out.extend_from_slice(&(q.order_by.len() as u32).to_le_bytes());
    for (p, d) in &q.order_by {
        put_str(out, p);
        out.push(u8::from(*d == Direction::Desc));
    }
    let limit = q.limit.map_or(0u32, |l| u32::try_from(l).unwrap_or(u32::MAX));
    out.extend_from_slice(&limit.to_le_bytes());
    out.extend_from_slice(&u32::try_from(q.offset).unwrap_or(u32::MAX).to_le_bytes());
    match &q.after_key {
        Some(k) => {
            out.push(1);
            put_str(out, k);
        }
        None => out.push(0),
    }
}

/// Decodes a query; the limit is clamped to [`MAX_PAGE_SIZE`] (default [`DEFAULT_PAGE_SIZE`]).
pub fn decode_query(body: &[u8]) -> Result<DocQuery> {
    let mut r = SveReader::new(body);
    let collection = r.str()?.to_owned();
    let filter = match r.u8()? {
        0 => None,
        1 => Some(decode_filter(&mut r)?),
        _ => return Err(StorageError::invalid_value("bad filter flag")),
    };
    let n = r.count(5)?;
    if n > 16 {
        return Err(StorageError::invalid_value("at most 16 sort keys"));
    }
    let mut order_by = Vec::with_capacity(n);
    for _ in 0..n {
        let p = r.str()?.to_owned();
        let d = if r.u8()? != 0 { Direction::Desc } else { Direction::Asc };
        order_by.push((p, d));
    }
    let limit = r.u32()? as usize;
    let offset = r.u32()? as usize;
    let after_key = match r.u8()? {
        0 => None,
        1 => Some(r.str()?.to_owned()),
        _ => return Err(StorageError::invalid_value("bad cursor flag")),
    };
    if r.remaining() != 0 {
        return Err(StorageError::invalid_value("trailing bytes after query"));
    }
    let limit = if limit == 0 { DEFAULT_PAGE_SIZE } else { limit.min(MAX_PAGE_SIZE) };
    Ok(DocQuery { collection, filter, order_by, limit: Some(limit), offset, after_key })
}

/// `u32 total | u8 has_next | str next? | u32 count | (str key, u64 version, map)*`
pub fn encode_page(page: &DocPage, out: &mut Vec<u8>) {
    out.extend_from_slice(&u32::try_from(page.total).unwrap_or(u32::MAX).to_le_bytes());
    match &page.next_after_key {
        Some(k) => {
            out.push(1);
            put_str(out, k);
        }
        None => out.push(0),
    }
    out.extend_from_slice(&(page.documents.len() as u32).to_le_bytes());
    for d in &page.documents {
        encode_document(d, out);
    }
}

pub fn decode_page(body: &[u8]) -> Result<DocPage> {
    let mut r = SveReader::new(body);
    let total = r.u32()? as usize;
    let next_after_key = match r.u8()? {
        0 => None,
        _ => Some(r.str()?.to_owned()),
    };
    let n = r.count(16)?;
    let mut documents = Vec::with_capacity(n);
    for _ in 0..n {
        documents.push(decode_document(&mut r)?);
    }
    Ok(DocPage { documents, total, next_after_key })
}

/// `str key | u64 version | map`
pub fn encode_document(d: &Document, out: &mut Vec<u8>) {
    put_str(out, &d.key);
    out.extend_from_slice(&d.version.0.to_le_bytes());
    encode_map_body(&d.fields, out);
}

pub fn decode_document(r: &mut SveReader<'_>) -> Result<Document> {
    let key = r.str()?.to_owned();
    let version = Version(r.u64()?);
    let fields: BTreeMap<String, PropertyValue> = r.map_body()?;
    Ok(Document { key, version, fields })
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage_codec::field_ops::FieldOp;

    #[test]
    fn test_document_wire_roundtrips_and_rejects_truncation() {
        let mut fields = BTreeMap::new();
        fields.insert("a".to_string(), PropertyValue::Int64(1));
        let writes = vec![
            DocWrite { collection: "c".into(), key: "k".into(), op: DocOp::Put(fields.clone()), precondition: Precondition::Absent },
            DocWrite { collection: "c".into(), key: "k2".into(), op: DocOp::PutWithTtl(fields.clone(), Duration::from_millis(1500)), precondition: Precondition::Version(Version(9)) },
            DocWrite { collection: "c".into(), key: "k3".into(), op: DocOp::Delete, precondition: Precondition::Exists },
            DocWrite {
                collection: "c".into(),
                key: "k4".into(),
                op: DocOp::Patch(vec![FieldOp::Unset { path: "x".into() }]),
                precondition: Precondition::None,
            },
            DocWrite { collection: "c".into(), key: "k5".into(), op: DocOp::Check, precondition: Precondition::Absent },
        ];
        let mut buf = Vec::new();
        encode_write_request(&writes, Some([3; 16]), &mut buf);
        assert_eq!(decode_write_request(&buf).unwrap(), (writes, Some([3; 16])));
        for cut in 0..buf.len() {
            assert!(decode_write_request(&buf[..cut]).is_err());
        }

        let q = DocQuery {
            collection: "c".into(),
            filter: Some(Filter::And(vec![
                Filter::Between("n".into(), PropertyValue::Int64(1), PropertyValue::Int64(5)),
                Filter::Not(Box::new(Filter::In("s".into(), vec![PropertyValue::Text("x".into())]))),
                Filter::Prefix("t".into(), "ab".into()),
                Filter::KeyPrefix("k".into()),
                Filter::Or(vec![Filter::Exists("e".into()), Filter::Missing("m".into())]),
            ])),
            order_by: vec![("n".into(), Direction::Desc)],
            limit: Some(10),
            offset: 2,
            after_key: Some("k1".into()),
        };
        let mut buf = Vec::new();
        encode_query(&q, &mut buf);
        assert_eq!(decode_query(&buf).unwrap(), q);
        for cut in 0..buf.len() {
            assert!(decode_query(&buf[..cut]).is_err());
        }

        let page = DocPage {
            documents: vec![Document { key: "k".into(), version: Version(3), fields }],
            total: 7,
            next_after_key: Some("k".into()),
        };
        let mut buf = Vec::new();
        encode_page(&page, &mut buf);
        assert_eq!(decode_page(&buf).unwrap(), page);
    }
}
