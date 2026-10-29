//! Structure-aware fuzzing of every `storage-codec` / `storage-core` decoder (`F-SIM-04`,
//! `TASK-135`, `RULE-ERR-01`).
//!
//! Inputs come from two sources:
//! - **Corpus**: the published conformance vectors (`protocol/vectors/vectors.json`).
//! - **Generators**: random, structurally valid values, documents, write batches, filters,
//!   queries, field operations, Raft messages, Raft op batches and record frames, built with
//!   the real encoders.
//!
//! Each input is decoded as is, then mutated:
//! - byte flips, truncation and extension, inserted or deleted ranges;
//! - splices with other corpus entries;
//! - length and count fields overwritten with edge values (0, MAX, off-by-one, powers of two).
//!
//! The checks:
//! - no decoder panics, whatever the bytes;
//! - every generated value round-trips (`encode(decode(encode(x))) == encode(x)`, which also
//!   covers NaN floats);
//! - whatever a decoder accepts re-encodes to bytes that decode again.
//!
//! `STORINE_FUZZ_ITERS` scales the iteration count (default 1 = a quick run; the release
//! run with debug assertions and overflow checks on uses 100+).

use std::collections::BTreeMap;

use storage_codec::field_ops::{decode_field_ops, encode_field_ops, FieldOp};
use storage_codec::value_wire::{decode_value, encode_map_body, encode_value, SveReader};
use storage_codec::{compress_lz4, decompress_lz4, AlignedBytes, PropertyValue, RecordCodec};
use storage_core::document_wire::*;
use storage_core::raft::{
    decode_raft_ops, encode_raft_ops, AppendEntriesReq, AppendEntriesResp, InstallSnapshotReq, InstallSnapshotResp, Membership,
    RaftCommand, RaftLogEntry, RequestVoteReq, RequestVoteResp,
};
use storage_core::{Direction, DocOp, DocQuery, DocWrite, DocumentRecord, Filter, Precondition, WriteOp};
use storage_types::testing::SweepRng;
use storage_types::{Decimal, InlineId, RecordFlags, RecordHeader, Version};

fn scale() -> usize {
    storage_types::testing::fuzz_scale()
}

// ── corpus ──

fn hex(s: &str) -> Vec<u8> {
    (0..s.len() / 2).filter_map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()).collect()
}

/// `(opcode or 0, bytes)` for every vector (values, documents, requests, responses).
fn corpus() -> Vec<(u8, Vec<u8>)> {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../protocol/vectors/vectors.json");
    let text = std::fs::read_to_string(path).expect("conformance vectors");
    let mut out = Vec::new();
    for obj in text.split("{\"name\"").skip(1) {
        let opcode = obj
            .find("\"opcode\":")
            .and_then(|i| obj[i + 9..].trim_start().split(|c: char| !c.is_ascii_digit()).next().and_then(|d| d.parse().ok()))
            .unwrap_or(0u8);
        for field in ["\"body_hex\":\"", "\"hex\":\""] {
            if let Some(i) = obj.find(field) {
                let rest = &obj[i + field.len()..];
                if let Some(end) = rest.find('"') {
                    out.push((opcode, hex(&rest[..end])));
                }
            }
        }
    }
    assert!(out.len() >= 60, "vectors not found or not parsed ({} entries)", out.len());
    out
}

// ── generators ──

struct Gen(SweepRng);

impl Gen {
    fn n(&mut self, n: usize) -> usize {
        self.0.below(n)
    }

    fn text(&mut self) -> String {
        match self.n(6) {
            0 => String::new(),
            1 => "é漢🙂\u{0}x".to_string(),
            2 => "a.b.c".to_string(),
            3 => "x".repeat(self.n(300)),
            _ => (0..self.n(12)).map(|_| (b'a' + self.n(26) as u8) as char).collect(),
        }
    }

    /// A valid field path (encoders do not validate; the decoders reject invalid ones, which
    /// the mutations exercise).
    fn path(&mut self) -> String {
        match self.n(4) {
            0 => "a".into(),
            1 => "a.b".into(),
            2 => format!("f{}", self.n(5)),
            _ => (0..1 + self.n(3)).map(|_| format!("p{}", self.n(50))).collect::<Vec<_>>().join("."),
        }
    }

    /// A valid document key or collection name.
    fn name(&mut self) -> String {
        match self.n(4) {
            0 => "c".into(),
            1 => "é漢".into(),
            _ => (0..1 + self.n(20)).map(|_| (b'a' + self.n(26) as u8) as char).collect(),
        }
    }

    fn value(&mut self, depth: usize) -> PropertyValue {
        let edge_i = |g: &mut Self| [0, 1, -1, i64::MIN, i64::MAX, g.0.next_u64() as i64][g.n(6)];
        match self.n(if depth > 3 { 12 } else { 14 }) {
            0 => PropertyValue::Null,
            1 => PropertyValue::Bool(self.n(2) == 0),
            2 => PropertyValue::Int64(edge_i(self)),
            3 => PropertyValue::UInt64([0, u64::MAX, self.0.next_u64()][self.n(3)]),
            4 => PropertyValue::Float64([0.0, -0.0, f64::NAN, f64::INFINITY, f64::MIN_POSITIVE, 1.5e300, self.0.next_u64() as f64][self.n(7)]),
            5 => PropertyValue::Int128([0, i128::MIN, i128::MAX, i128::from(self.0.next_u64()) << 64][self.n(4)]),
            6 => PropertyValue::TimestampNs(edge_i(self)),
            7 => {
                let mut u = [0u8; 16];
                u.iter_mut().for_each(|b| *b = self.n(256) as u8);
                PropertyValue::Uuid(u)
            }
            8 => PropertyValue::Id(InlineId::new(&format!("id{}", self.n(1000))).unwrap()),
            9 => PropertyValue::Text(self.text()),
            10 => PropertyValue::Bytes((0..self.n(64)).map(|_| self.n(256) as u8).collect()),
            11 => {
                let scale = self.n(19) as u8;
                let m = [0i128, 1, -1, 10i128.pow(30), -(10i128.pow(30)), i128::from(self.0.next_u64())][self.n(6)];
                Decimal::new(m, scale).map_or(PropertyValue::Null, PropertyValue::Decimal)
            }
            12 => PropertyValue::List((0..self.n(5)).map(|_| self.value(depth + 1)).collect()),
            _ => PropertyValue::Map(self.fields(depth + 1)),
        }
    }

    fn fields(&mut self, depth: usize) -> BTreeMap<String, PropertyValue> {
        (0..self.n(6)).map(|_| (self.text(), self.value(depth))).collect()
    }

    fn filter(&mut self, depth: usize) -> Filter {
        let p = self.path();
        match self.n(if depth > 2 { 13 } else { 16 }) {
            0 => Filter::Eq(p, self.value(3)),
            1 => Filter::Ne(p, self.value(3)),
            2 => Filter::Lt(p, self.value(3)),
            3 => Filter::Le(p, self.value(3)),
            4 => Filter::Gt(p, self.value(3)),
            5 => Filter::Ge(p, self.value(3)),
            6 => Filter::Between(p, self.value(3), self.value(3)),
            7 => Filter::In(p, (0..self.n(4)).map(|_| self.value(3)).collect()),
            8 => Filter::Prefix(p, self.text()),
            9 => Filter::Contains(p, self.value(3)),
            10 => Filter::Exists(p),
            11 => Filter::Missing(p),
            12 => Filter::KeyPrefix(self.text()),
            13 => Filter::And((0..self.n(4)).map(|_| self.filter(depth + 1)).collect()),
            14 => Filter::Or((0..self.n(4)).map(|_| self.filter(depth + 1)).collect()),
            _ => Filter::Not(Box::new(self.filter(depth + 1))),
        }
    }

    fn field_op(&mut self) -> FieldOp {
        let path = self.path();
        match self.n(4) {
            0 => FieldOp::Set { path, value: self.value(1) },
            1 => FieldOp::Unset { path },
            2 => FieldOp::Add {
                path,
                delta: self.value(3),
                min: (self.n(2) == 0).then(|| self.value(3)),
                max: (self.n(2) == 0).then(|| self.value(3)),
            },
            _ => FieldOp::Append { path, value: self.value(2) },
        }
    }

    fn write(&mut self) -> DocWrite {
        let op = match self.n(5) {
            0 => DocOp::Put(self.fields(0)),
            1 => DocOp::PutWithTtl(self.fields(0), std::time::Duration::from_millis(self.n(1_000_000) as u64)),
            2 => DocOp::Delete,
            3 => DocOp::Patch((0..self.n(4)).map(|_| self.field_op()).collect()),
            _ => DocOp::Check,
        };
        let precondition = match self.n(4) {
            0 => Precondition::None,
            1 => Precondition::Absent,
            2 => Precondition::Exists,
            _ => Precondition::Version(Version(self.0.next_u64())),
        };
        DocWrite { collection: self.name(), key: self.name(), op, precondition }
    }

    fn query(&mut self) -> DocQuery {
        DocQuery {
            collection: self.text(),
            filter: (self.n(2) == 0).then(|| self.filter(0)),
            order_by: (0..self.n(3)).map(|_| (self.path(), if self.n(2) == 0 { Direction::Asc } else { Direction::Desc })).collect(),
            limit: (self.n(2) == 0).then(|| self.n(1000)),
            offset: self.n(100),
            after_key: (self.n(2) == 0).then(|| self.text()),
            nearest: None,
        }
    }

    fn entry(&mut self, index: u64) -> RaftLogEntry {
        let command = match self.n(4) {
            0 => RaftCommand::Noop,
            1 => RaftCommand::SweepExpired { now_ns: self.0.next_u64() },
            2 => RaftCommand::Delete { id: InlineId::new(&self.name()).unwrap() },
            _ => RaftCommand::Ops(AlignedBytes::copy_from_slice(&self.ops_bytes())),
        };
        RaftLogEntry { term: self.n(10) as u64, index, command }
    }

    fn ops_bytes(&mut self) -> Vec<u8> {
        let ops: Vec<WriteOp<DocumentRecord>> = (0..self.n(4))
            .map(|i| {
                let id = InlineId::new(&format!("r{i}")).unwrap();
                if self.n(2) == 0 {
                    let mut rec = DocumentRecord::new(self.name(), self.name());
                    rec.fields = self.fields(1);
                    WriteOp::put(id, rec)
                } else {
                    WriteOp::delete(id)
                }
            })
            .collect();
        encode_raft_ops(&ops, self.0.next_u64()).map(|b| b.as_slice().to_vec()).unwrap_or_default()
    }
}

// ── mutation ──

fn mutate(rng: &mut SweepRng, base: &[u8], corpus: &[(u8, Vec<u8>)]) -> Vec<u8> {
    let mut b = base.to_vec();
    for _ in 0..1 + rng.below(3) {
        match rng.below(8) {
            0 => b = rng.hostile_copy(&b),
            1 if !b.is_empty() => b.truncate(rng.below(b.len())),
            2 if b.len() >= 4 => {
                // A length / count field set to an edge value.
                let at = rng.below(b.len() - 3);
                let cur = u32::from_le_bytes([b[at], b[at + 1], b[at + 2], b[at + 3]]);
                b[at..at + 4].copy_from_slice(&rng.edge_u32(cur).to_le_bytes());
            }
            3 if b.len() >= 8 => {
                let at = rng.below(b.len() - 7);
                let cur = u64::from_le_bytes(b[at..at + 8].try_into().unwrap());
                b[at..at + 8].copy_from_slice(&rng.edge_u64(cur).to_le_bytes());
            }
            4 => {
                let other = &corpus[rng.below(corpus.len())].1;
                let cut = rng.below(b.len() + 1);
                let from = rng.below(other.len() + 1);
                b.truncate(cut);
                b.extend_from_slice(&other[from..]);
            }
            5 if !b.is_empty() => {
                let at = rng.below(b.len());
                let n = rng.below(16).min(b.len() - at);
                b.drain(at..at + n);
            }
            6 => {
                let at = rng.below(b.len() + 1);
                let junk: Vec<u8> = (0..rng.below(16)).map(|_| rng.below(256) as u8).collect();
                b.splice(at..at, junk);
            }
            _ => {
                if !b.is_empty() {
                    let at = rng.below(b.len());
                    b[at] ^= 1 << rng.below(8);
                }
            }
        }
    }
    b
}

// ── decoders ──

/// Feeds `bytes` to every decoder; whatever decodes must re-encode and decode again.
fn decode_all(bytes: &[u8]) {
    if let Ok(v) = decode_value(bytes) {
        let mut e = Vec::new();
        encode_value(&v, &mut e);
        let again = decode_value(&e).expect("re-encoded value decodes");
        let mut e2 = Vec::new();
        encode_value(&again, &mut e2);
        assert_eq!(e, e2);
    }
    let _ = SveReader::new(bytes).map_body();
    let _ = decode_field_ops(&mut SveReader::new(bytes));
    let _ = decode_filter(&mut SveReader::new(bytes));
    let _ = decode_document(&mut SveReader::new(bytes));
    if let Ok((writes, rid)) = decode_write_request(bytes) {
        let mut e = Vec::new();
        encode_write_request(&writes, rid, &mut e);
        decode_write_request(&e).expect("re-encoded write request decodes");
    }
    let _ = decode_write_response(bytes);
    if let Ok(q) = decode_query(bytes) {
        let mut e = Vec::new();
        encode_query(&q, &mut e);
        decode_query(&e).expect("re-encoded query decodes");
    }
    let _ = decode_page(bytes);
    if let Ok(q) = decode_aggregate(bytes) {
        let mut e = Vec::new();
        encode_aggregate(&q, &mut e);
        decode_aggregate(&e).expect("re-encoded aggregate decodes");
    }
    let _ = decode_aggregate_result(bytes);
    if let Ok(q) = decode_scan(bytes) {
        let mut e = Vec::new();
        encode_scan(&q, &mut e);
        decode_scan(&e).expect("re-encoded scan decodes");
    }
    let _ = decode_scan_page(bytes);
    let _ = Membership::decode(bytes);
    let _ = decode_raft_ops::<DocumentRecord>(bytes);
    let _ = RequestVoteReq::from_wire_bytes(bytes);
    let _ = RequestVoteResp::from_wire_bytes(bytes);
    if let Ok(req) = AppendEntriesReq::from_wire_bytes(bytes) {
        AppendEntriesReq::from_wire_bytes(&req.to_wire_bytes()).expect("re-encoded append decodes");
    }
    let _ = AppendEntriesResp::from_wire_bytes(bytes);
    let _ = InstallSnapshotReq::from_wire_bytes(bytes);
    let _ = InstallSnapshotResp::from_wire_bytes(bytes);
    let _ = RecordHeader::from_slice_verified(bytes);
    // Record frames: also through an aligned copy, so the zero-copy paths are reached.
    let aligned = AlignedBytes::copy_from_slice(bytes);
    let _ = RecordCodec::decode_verified::<DocumentRecord>(aligned.as_slice()).map(|v| v.to_owned_record());
    let _ = RecordCodec::decode_owned::<DocumentRecord>(aligned.as_slice());
    for expect in [bytes.len(), bytes.len() * 4 + 16, 0, 1 << 20] {
        let _ = decompress_lz4(bytes, expect);
    }
}

#[test]
fn corpus_and_mutations_never_panic() {
    let corpus = corpus();
    let mut rng = SweepRng::new(0xF022);
    for (_, c) in &corpus {
        decode_all(c);
        for cut in 0..c.len().min(256) {
            decode_all(&c[..cut]);
        }
    }
    let iters = 20_000 * scale();
    for _ in 0..iters {
        let base = &corpus[rng.below(corpus.len())].1;
        decode_all(&mutate(&mut rng, base, &corpus));
    }
}

#[test]
fn generated_values_round_trip_and_survive_mutation() {
    let corpus = corpus();
    let mut g = Gen(SweepRng::new(0xBEEF));
    let mut rng = SweepRng::new(0x5EED);
    for _ in 0..4_000 * scale() {
        // Values and documents.
        let v = g.value(0);
        let mut e = Vec::new();
        encode_value(&v, &mut e);
        let d = decode_value(&e).unwrap_or_else(|err| panic!("generated value does not decode: {err} {v:?}"));
        let mut e2 = Vec::new();
        encode_value(&d, &mut e2);
        assert_eq!(e, e2, "value round trip: {v:?}");
        decode_all(&mutate(&mut rng, &e, &corpus));

        let fields = g.fields(0);
        let mut e = Vec::new();
        encode_map_body(&fields, &mut e);
        let back = SveReader::new(&e).map_body().expect("map body decodes");
        let mut e2 = Vec::new();
        encode_map_body(&back, &mut e2);
        assert_eq!(e, e2);

        // Field operations.
        let ops: Vec<FieldOp> = (0..g.n(5)).map(|_| g.field_op()).collect();
        let mut e = Vec::new();
        encode_field_ops(&ops, &mut e);
        let back = decode_field_ops(&mut SveReader::new(&e)).expect("field ops decode");
        let mut e2 = Vec::new();
        encode_field_ops(&back, &mut e2);
        assert_eq!(e, e2);
        decode_all(&mutate(&mut rng, &e, &corpus));

        // Write batches.
        let writes: Vec<DocWrite> = (0..g.n(4)).map(|_| g.write()).collect();
        let rid = (g.n(2) == 0).then(|| [g.n(256) as u8; 16]);
        let mut e = Vec::new();
        encode_write_request(&writes, rid, &mut e);
        let (back, rid2) = decode_write_request(&e).expect("write request decodes");
        assert_eq!(rid, rid2);
        let mut e2 = Vec::new();
        encode_write_request(&back, rid2, &mut e2);
        assert_eq!(e, e2);
        decode_all(&mutate(&mut rng, &e, &corpus));

        // Queries (filters nested a few levels). The decoder normalises the limit (0 means the
        // default page, large limits are capped: PROTOCOL.md), so compare after one pass.
        let q = g.query();
        let mut e = Vec::new();
        encode_query(&q, &mut e);
        let back = decode_query(&e).expect("query decodes");
        assert_eq!((&back.collection, &back.order_by, back.offset, &back.after_key), (&q.collection, &q.order_by, q.offset, &q.after_key));
        let mut e2 = Vec::new();
        encode_query(&back, &mut e2);
        let mut e3 = Vec::new();
        encode_query(&decode_query(&e2).expect("normalised query decodes"), &mut e3);
        assert_eq!(e2, e3);
        decode_all(&mutate(&mut rng, &e, &corpus));
    }
}

#[test]
fn raft_messages_and_record_frames_round_trip_and_survive_mutation() {
    let corpus = corpus();
    let mut g = Gen(SweepRng::new(0xACE));
    let mut rng = SweepRng::new(0xD00D);
    for _ in 0..3_000 * scale() {
        let prev = g.n(100) as u64;
        let req = AppendEntriesReq {
            term: g.n(10) as u64,
            leader_id: g.n(5) as u16,
            prev_log_index: prev,
            prev_log_term: g.n(10) as u64,
            entries: (0..g.n(4) as u64).map(|i| g.entry(prev + 1 + i)).collect(),
            leader_commit: g.n(200) as u64,
        };
        let e = req.to_wire_bytes();
        let back = AppendEntriesReq::from_wire_bytes(&e).expect("append decodes");
        assert_eq!(back.to_wire_bytes(), e);
        decode_all(&mutate(&mut rng, &e, &corpus));

        let vote = RequestVoteReq { term: g.0.next_u64(), candidate_id: g.n(9) as u16, last_log_index: g.0.next_u64(), last_log_term: g.0.next_u64(), pre_vote: g.n(2) == 0 };
        assert_eq!(RequestVoteReq::from_wire_bytes(&vote.to_wire_bytes()).unwrap(), vote);
        decode_all(&mutate(&mut rng, &vote.to_wire_bytes(), &corpus));

        let ops = g.ops_bytes();
        if !ops.is_empty() {
            let (now, decoded) = decode_raft_ops::<DocumentRecord>(&ops).expect("raft ops decode");
            let again = encode_raft_ops(&decoded, now).unwrap();
            assert_eq!(again.as_slice(), &ops[..]);
            decode_all(&mutate(&mut rng, &ops, &corpus));
        }

        let mut rec = DocumentRecord::new(g.name(), g.name());
        rec.fields = g.fields(0);
        let frame = RecordCodec::encode_aligned(&rec, Version(g.n(1000) as u64 + 1), g.0.next_u64(), RecordFlags::NONE, 0);
        let (_, owned) = RecordCodec::decode_owned::<DocumentRecord>(frame.as_slice()).expect("frame decodes");
        assert_eq!(owned.fields.len(), rec.fields.len());
        decode_all(&mutate(&mut rng, frame.as_slice(), &corpus));

        let plain: Vec<u8> = (0..g.n(5000)).map(|i| if g.n(3) == 0 { g.n(256) as u8 } else { (i % 7) as u8 }).collect();
        let packed = compress_lz4(&plain);
        assert_eq!(decompress_lz4(&packed, plain.len()).unwrap(), plain);
        let bad = mutate(&mut rng, &packed, &corpus);
        let _ = decompress_lz4(&bad, plain.len());
    }
}

