//! Structure-aware fuzzing of column chunks (`F-SIM-04`, `TASK-135`, `RULE-ERR-01`).
//!
//! Random columns, of every kind and of mixed kinds, with nulls, are encoded with the real
//! encoders:
//! - integer chunks: constant, narrow-range, sequential, random and extreme values;
//! - value chunks: every `Val` kind.
//!
//! Each must decode back to the same values. The chunks are then mutated, and parsing or
//! decoding them must fail cleanly or read safely, never panic.
//!
//! `STORINE_FUZZ_ITERS` scales the iteration count.

use storage_columnar::bits::Reader;
use storage_columnar::column::encode_chunk;
use storage_columnar::int::{encode, IntChunk};
use storage_columnar::{Chunk, Val};
use storage_types::testing::SweepRng;

fn scale() -> usize {
    storage_types::testing::fuzz_scale()
}

fn ints(rng: &mut SweepRng) -> Vec<i64> {
    let n = rng.below(2000);
    let base = rng.next_u64() as i64;
    match rng.below(6) {
        0 => vec![base; n],
        1 => (0..n).map(|_| base.wrapping_add(rng.below(16) as i64)).collect(),
        2 => (0..n as i64).map(|i| base.wrapping_add(i * 3)).collect(),
        3 => (0..n).map(|_| [i64::MIN, i64::MAX, 0, -1, 1][rng.below(5)]).collect(),
        4 => (0..n).map(|_| rng.next_u64() as i64).collect(),
        _ => (0..n).map(|_| (rng.below(1000) as i64) - 500).collect(),
    }
}

fn val(rng: &mut SweepRng, kind: usize) -> Val {
    match kind {
        0 => Val::Bool(rng.below(2) == 0),
        1 => Val::Int([0, -1, i64::MIN, i64::MAX, rng.next_u64() as i64, rng.below(100) as i64][rng.below(6)]),
        2 => Val::UInt([0, u64::MAX, rng.next_u64()][rng.below(3)]),
        3 => Val::Float([0.0, -0.0, f64::NAN, f64::INFINITY, rng.next_u64() as f64 / 7.0][rng.below(5)]),
        4 => Val::Int128([0, i128::MIN, i128::MAX, i128::from(rng.next_u64())][rng.below(4)]),
        5 => Val::Timestamp(rng.next_u64() as i64),
        6 => {
            let mut u = [0u8; 16];
            u[0] = rng.below(256) as u8;
            Val::Uuid(u)
        }
        7 => Val::Text(["", "a", "é漢🙂", &"x".repeat(rng.below(300))][rng.below(4)].to_string()),
        8 => Val::Bytes((0..rng.below(40)).map(|_| rng.below(256) as u8).collect()),
        _ => Val::Decimal([0, 1, -1, 10i128.pow(30)][rng.below(4)], rng.below(19) as u8),
    }
}

fn rows(rng: &mut SweepRng) -> Vec<Option<Val>> {
    let n = 1 + rng.below(600);
    let mixed = rng.below(4) == 0;
    let kind = rng.below(10);
    let null_pct = [0, 5, 50, 100][rng.below(4)];
    (0..n)
        .map(|_| {
            if rng.below(100) < null_pct {
                None
            } else {
                let k = if mixed { rng.below(10) } else { kind };
                Some(val(rng, k))
            }
        })
        .collect()
}

fn same(a: &Option<Val>, b: &Option<Val>) -> bool {
    match (a, b) {
        (Some(Val::Float(x)), Some(Val::Float(y))) => x.to_bits() == y.to_bits(),
        _ => a == b,
    }
}

fn mutate(rng: &mut SweepRng, b: &[u8]) -> Vec<u8> {
    let mut out = b.to_vec();
    match rng.below(4) {
        0 => out = rng.hostile_copy(&out),
        1 if !out.is_empty() => out.truncate(rng.below(out.len())),
        2 if out.len() >= 4 => {
            let at = rng.below(out.len() - 3);
            let cur = u32::from_le_bytes(out[at..at + 4].try_into().unwrap());
            out[at..at + 4].copy_from_slice(&rng.edge_u32(cur).to_le_bytes());
        }
        _ => {
            if !out.is_empty() {
                let at = rng.below(out.len());
                out[at] = rng.below(256) as u8;
            }
        }
    }
    out
}

#[test]
fn integer_chunks_round_trip_and_survive_mutation() {
    let mut rng = SweepRng::new(0x1A7);
    for _ in 0..2_000 * scale() {
        let v = ints(&mut rng);
        let mut enc = Vec::new();
        encode(&v, &mut enc);
        let chunk = IntChunk::parse(&mut Reader::new(&enc), v.len()).expect("encoded chunk parses");
        let mut got = vec![0i64; v.len()];
        chunk.decode_into(0, &mut got);
        assert_eq!(got, v);
        if !v.is_empty() {
            let i = rng.below(v.len());
            assert_eq!(chunk.get(i), v[i]);
        }
        for _ in 0..4 {
            let bad = mutate(&mut rng, &enc);
            let n = if rng.below(4) == 0 { rng.edge_u32(v.len() as u32) as usize % 100_000 } else { v.len() };
            if let Ok(c) = IntChunk::parse(&mut Reader::new(&bad), n) {
                let len = c.len();
                let mut out = vec![0i64; len.min(100_000)];
                c.decode_into(0, &mut out);
                if len > 0 {
                    let _ = c.get(rng.below(len));
                    // `gather` takes a bitmap of the chunk's rows (64 per word).
                    let len = len.min(100_000);
                    let mut bits = vec![0u64; len.div_ceil(64)];
                    for _ in 0..8 {
                        let i = rng.below(len);
                        bits[i / 64] |= 1 << (i % 64);
                    }
                    let mut g = vec![0i64; len];
                    let _ = c.gather(&bits, &mut g);
                }
            }
        }
    }
}

#[test]
fn value_chunks_round_trip_and_survive_mutation() {
    let mut rng = SweepRng::new(0xC01);
    for _ in 0..1_500 * scale() {
        let r = rows(&mut rng);
        let mut enc = Vec::new();
        encode_chunk(&r, &mut enc);
        let chunk = Chunk::parse(&enc, r.len()).expect("encoded chunk parses");
        let back = chunk.decode_all().expect("encoded chunk decodes");
        assert_eq!(back.len(), r.len());
        for (i, (a, b)) in r.iter().zip(&back).enumerate() {
            assert!(same(a, b), "row {i}: wrote {a:?}, read {b:?}");
            assert_eq!(chunk.is_present(i), a.is_some());
        }
        for _ in 0..4 {
            let bad = mutate(&mut rng, &enc);
            let n = if rng.below(4) == 0 { rng.edge_u32(r.len() as u32) as usize % 100_000 } else { r.len() };
            if let Ok(c) = Chunk::parse(&bad, n) {
                let _ = c.decode_all();
                for _ in 0..(if n > 0 { 4 } else { 0 }) {
                    let _ = c.is_present(rng.below(n));
                }
            }
        }
    }
}
