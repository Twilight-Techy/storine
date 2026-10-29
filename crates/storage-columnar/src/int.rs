//! Integer chunks: the encoding of every `i64`-domain column (integers, timestamps, unsigned
//! integers mapped order-preservingly, dictionary codes, run lengths, text offsets).
//!
//! ```text
//! int-chunk := u8 encoding | body
//!   0 CONST   i64 value
//!   1 FOR     i64 min | packed (v - min)                       (frame of reference)
//!   2 DELTA   i64 min_delta | (n / 2048 + 1) x i64 anchors | packed (delta - min_delta)
//!   3 DICT    u32 d | simple-chunk(d sorted values) | packed codes
//!   4 RLE     u32 runs | simple-chunk(run values) | simple-chunk(run ends, cumulative)
//! simple-chunk := CONST | FOR
//! ```
//!
//! The encoding is chosen per chunk by computed size (ties go to the cheaper decode). DELTA
//! restarts at every 2,048-value block (`anchors`), so any aligned batch decodes on its own.
//! Arithmetic is done in `i128` / wrapping `u64`, so every `i64` value round-trips exactly.

use storage_types::{FastHashMap, Result};

use crate::bits::{corrupt, packed_words, put_packed, put_u32, Packed, Reader};

/// Values per DELTA restart block (and the batch size of the vectorized engine).
pub const BLOCK: usize = 2048;

const ENC_CONST: u8 = 0;
const ENC_FOR: u8 = 1;
const ENC_DELTA: u8 = 2;
const ENC_DICT: u8 = 3;
const ENC_RLE: u8 = 4;

/// Most distinct values a dictionary-encoded integer chunk may have.
const MAX_INT_DICT: usize = 4096;

#[inline]
fn width_of_range(range: u128) -> u8 {
    (128 - range.leading_zeros()).min(64) as u8
}

/// Encodes `values` (`n = values.len()`) choosing the smallest encoding.
pub fn encode(values: &[i64], out: &mut Vec<u8>) {
    let n = values.len();
    let Some((&first, _)) = values.split_first() else {
        out.push(ENC_CONST);
        out.extend_from_slice(&0i64.to_le_bytes());
        return;
    };
    let (mut min, mut max) = (first, first);
    let (mut dmin, mut dmax) = (i128::MAX, i128::MIN);
    let mut runs = 1usize;
    let mut prev = first;
    for (i, &v) in values.iter().enumerate() {
        min = min.min(v);
        max = max.max(v);
        if i > 0 {
            if v != prev {
                runs += 1;
            }
            if i % BLOCK != 0 {
                let d = i128::from(v) - i128::from(prev);
                dmin = dmin.min(d);
                dmax = dmax.max(d);
            }
        }
        prev = v;
    }
    if min == max {
        out.push(ENC_CONST);
        out.extend_from_slice(&min.to_le_bytes());
        return;
    }
    let for_width = width_of_range((i128::from(max) - i128::from(min)) as u128);
    let for_size = 9 + 1 + packed_words(n, for_width) * 8;
    let blocks = n.div_ceil(BLOCK);
    let delta = (dmin <= dmax && dmax - dmin <= i128::from(u64::MAX)).then(|| {
        let w = width_of_range((dmax - dmin) as u128);
        (w, 9 + blocks * 8 + 1 + packed_words(n, w) * 8)
    });
    let rle_size = (runs * 8 <= n).then(|| 1 + 4 + simple_size(runs, min, max) + simple_size(runs, 1, n as i64));
    let dict = (n >= 64).then(|| distinct_capped(values, MAX_INT_DICT.min(n / 4))).flatten();
    let dict_size = dict.as_ref().map(|d| {
        let dw = crate::bits::bit_width(d.len() as u64 - 1);
        1 + 4 + simple_size(d.len(), d[0], d[d.len() - 1]) + 1 + packed_words(n, dw) * 8
    });

    let mut best = (for_size, ENC_FOR);
    if let Some((_, s)) = delta {
        if s < best.0 {
            best = (s, ENC_DELTA);
        }
    }
    if let Some(s) = dict_size {
        if s < best.0 {
            best = (s, ENC_DICT);
        }
    }
    if let Some(s) = rle_size {
        if s < best.0 {
            best = (s, ENC_RLE);
        }
    }
    match best.1 {
        ENC_DELTA => {
            let dmin = dmin as i64; // fits: dmin is a difference of two i64 within a u64 range
            out.push(ENC_DELTA);
            out.extend_from_slice(&dmin.to_le_bytes());
            for b in 0..blocks {
                out.extend_from_slice(&values[b * BLOCK].to_le_bytes());
            }
            let mut packed = Vec::with_capacity(n);
            for (i, &v) in values.iter().enumerate() {
                if i % BLOCK == 0 {
                    packed.push(0);
                } else {
                    let d = (v as u64).wrapping_sub(values[i - 1] as u64);
                    packed.push(d.wrapping_sub(dmin as u64));
                }
            }
            put_packed(out, &packed);
        }
        ENC_DICT => {
            let Some(d) = dict else { return };
            out.push(ENC_DICT);
            put_u32(out, d.len() as u32);
            encode_simple(&d, out);
            let pos: FastHashMap<i64, u64> = d.iter().enumerate().map(|(i, &v)| (v, i as u64)).collect();
            let codes: Vec<u64> = values.iter().map(|v| pos.get(v).copied().unwrap_or(0)).collect();
            put_packed(out, &codes);
        }
        ENC_RLE => {
            let mut vals = Vec::with_capacity(runs);
            let mut ends = Vec::with_capacity(runs);
            for (i, &v) in values.iter().enumerate() {
                if i == 0 || v != values[i - 1] {
                    if i > 0 {
                        ends.push(i as i64);
                    }
                    vals.push(v);
                }
            }
            ends.push(n as i64);
            out.push(ENC_RLE);
            put_u32(out, runs as u32);
            encode_simple(&vals, out);
            encode_simple(&ends, out);
        }
        _ => {
            out.push(ENC_FOR);
            out.extend_from_slice(&min.to_le_bytes());
            let packed: Vec<u64> = values.iter().map(|&v| (v as u64).wrapping_sub(min as u64)).collect();
            put_packed(out, &packed);
        }
    }
}

fn simple_size(n: usize, min: i64, max: i64) -> usize {
    if min == max {
        9
    } else {
        9 + 1 + packed_words(n, width_of_range((i128::from(max) - i128::from(min)) as u128)) * 8
    }
}

/// CONST or FOR only (nested chunks never nest further).
fn encode_simple(values: &[i64], out: &mut Vec<u8>) {
    let min = values.iter().copied().min().unwrap_or(0);
    let max = values.iter().copied().max().unwrap_or(0);
    if min == max {
        out.push(ENC_CONST);
        out.extend_from_slice(&min.to_le_bytes());
    } else {
        out.push(ENC_FOR);
        out.extend_from_slice(&min.to_le_bytes());
        let packed: Vec<u64> = values.iter().map(|&v| (v as u64).wrapping_sub(min as u64)).collect();
        put_packed(out, &packed);
    }
}

/// Sorted distinct values, or `None` if there are more than `cap`.
fn distinct_capped(values: &[i64], cap: usize) -> Option<Vec<i64>> {
    if cap < 2 {
        return None;
    }
    let mut seen: FastHashMap<i64, ()> = FastHashMap::default();
    for &v in values {
        seen.insert(v, ());
        if seen.len() > cap {
            return None;
        }
    }
    let mut d: Vec<i64> = seen.into_keys().collect();
    d.sort_unstable();
    Some(d)
}

/// A parsed integer chunk over `n` values (borrows the segment bytes).
#[derive(Clone, Debug)]
pub enum IntChunk<'a> {
    Const { value: i64, n: usize },
    For { min: i64, packed: Packed<'a> },
    Delta { min_delta: i64, anchors: &'a [u8], packed: Packed<'a> },
    Dict { dict: Vec<i64>, codes: Packed<'a> },
    Rle { values: Vec<i64>, ends: Vec<u32> },
}

impl<'a> IntChunk<'a> {
    pub fn parse(r: &mut Reader<'a>, n: usize) -> Result<Self> {
        match r.u8()? {
            ENC_CONST => Ok(Self::Const { value: r.i64()?, n }),
            ENC_FOR => {
                let min = r.i64()?;
                Ok(Self::For { min, packed: r.packed(n)? })
            }
            ENC_DELTA => {
                let min_delta = r.i64()?;
                let anchors = r.take(n.div_ceil(BLOCK) * 8)?;
                Ok(Self::Delta { min_delta, anchors, packed: r.packed(n)? })
            }
            ENC_DICT => {
                let d = r.u32()? as usize;
                if d == 0 || d > MAX_INT_DICT {
                    return Err(corrupt("integer dictionary size"));
                }
                let dict = parse_simple(r, d)?;
                Ok(Self::Dict { dict, codes: r.packed(n)? })
            }
            ENC_RLE => {
                let runs = r.u32()? as usize;
                if runs == 0 || runs > n {
                    return Err(corrupt("run count"));
                }
                let values = parse_simple(r, runs)?;
                let ends_raw = parse_simple(r, runs)?;
                let mut ends = Vec::with_capacity(runs);
                let mut last = 0i64;
                for e in ends_raw {
                    if e <= last || e > n as i64 {
                        return Err(corrupt("run ends"));
                    }
                    last = e;
                    ends.push(e as u32);
                }
                if last != n as i64 {
                    return Err(corrupt("run ends"));
                }
                Ok(Self::Rle { values, ends })
            }
            _ => Err(corrupt("integer encoding")),
        }
    }

    /// Number of values.
    pub fn len(&self) -> usize {
        match self {
            Self::Const { n, .. } => *n,
            Self::For { packed, .. } | Self::Delta { packed, .. } => packed.len(),
            Self::Dict { codes, .. } => codes.len(),
            Self::Rle { ends, .. } => ends.last().map_or(0, |&e| e as usize),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Values `start .. start + out.len()` into `out`. The range must be in bounds.
    pub fn decode_into(&self, start: usize, out: &mut [i64]) {
        match self {
            Self::Const { value, .. } => out.fill(*value),
            Self::For { min, packed } => {
                let mut buf = [0u64; BLOCK];
                let mut done = 0;
                while done < out.len() {
                    let k = (out.len() - done).min(buf.len());
                    packed.unpack_into(start + done, &mut buf[..k]);
                    storage_codec::simd::add_base(*min as u64, &buf[..k], &mut out[done..done + k]);
                    done += k;
                }
            }
            Self::Delta { min_delta, anchors, packed } => {
                let mut deltas = [0u64; BLOCK];
                let mut i = start;
                let end = start + out.len();
                let md = *min_delta as u64;
                while i < end {
                    let block = i / BLOCK;
                    let block_start = block * BLOCK;
                    let block_end = ((block + 1) * BLOCK).min(end).min(packed.len());
                    if block_end <= block_start {
                        break;
                    }
                    let a = anchors.get(block * 8..block * 8 + 8).unwrap_or(&[0; 8]);
                    let mut acc = u64::from_le_bytes([a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7]]);
                    // Prefix sum from the block anchor (vectorized unpack, then one add per value).
                    let span = block_end - block_start;
                    packed.unpack_into(block_start, &mut deltas[..span]);
                    for (j, d) in deltas[..span].iter().enumerate().skip(1) {
                        acc = acc.wrapping_add(*d).wrapping_add(md);
                        let pos = block_start + j;
                        if pos >= i {
                            out[pos - start] = acc as i64;
                        }
                    }
                    if block_start >= i {
                        out[block_start - start] = u64::from_le_bytes([a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7]]) as i64;
                    }
                    i = block_end;
                }
            }
            Self::Dict { dict, codes } => {
                let mut buf = [0u64; BLOCK];
                let mut done = 0;
                while done < out.len() {
                    let k = (out.len() - done).min(buf.len());
                    codes.unpack_into(start + done, &mut buf[..k]);
                    // A code outside the dictionary only exists in a hostile file whose checksums
                    // were forged: it is clamped (a wrong value, never out of bounds).
                    let last = dict.len().saturating_sub(1);
                    for (o, &c) in out[done..done + k].iter_mut().zip(&buf[..k]) {
                        *o = dict[(c as usize).min(last)];
                    }
                    done += k;
                }
            }
            Self::Rle { values, ends } => {
                let mut run = ends.partition_point(|&e| e as usize <= start);
                for (k, o) in out.iter_mut().enumerate() {
                    while run < ends.len() && ends[run] as usize <= start + k {
                        run += 1;
                    }
                    *o = values.get(run).copied().unwrap_or(0);
                }
            }
        }
    }

    /// Writes the values of the rows whose bits are set in `rows` into `out` (same indexes;
    /// other entries are left as they are). `false` for encodings without random access
    /// (DELTA, RLE): decode those with [`Self::decode_into`].
    pub fn gather(&self, rows: &[u64], out: &mut [i64]) -> bool {
        let each = |f: &mut dyn FnMut(usize)| {
            for (w, &word) in rows.iter().enumerate() {
                let mut m = word;
                while m != 0 {
                    f(w * 64 + m.trailing_zeros() as usize);
                    m &= m - 1;
                }
            }
        };
        match self {
            Self::Const { value, .. } => {
                each(&mut |i| out[i] = *value);
                true
            }
            Self::For { min, packed } => {
                each(&mut |i| out[i] = (*min as u64).wrapping_add(packed.get(i)) as i64);
                true
            }
            Self::Dict { dict, codes } => {
                let last = dict.len().saturating_sub(1);
                each(&mut |i| out[i] = dict[(codes.get(i) as usize).min(last)]);
                true
            }
            Self::Delta { .. } | Self::Rle { .. } => false,
        }
    }

    /// Value `i` (slow path).
    pub fn get(&self, i: usize) -> i64 {
        let mut one = [0i64];
        self.decode_into(i, &mut one);
        one[0]
    }
}

fn parse_simple(r: &mut Reader<'_>, n: usize) -> Result<Vec<i64>> {
    let c = IntChunk::parse(r, n)?;
    if !matches!(c, IntChunk::Const { .. } | IntChunk::For { .. }) {
        return Err(corrupt("nested integer encoding"));
    }
    let mut v = vec![0i64; n];
    c.decode_into(0, &mut v);
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(values: &[i64]) -> u8 {
        let mut out = Vec::new();
        encode(values, &mut out);
        let enc = out[0];
        let mut r = Reader::new(&out);
        let c = IntChunk::parse(&mut r, values.len()).unwrap();
        assert_eq!(r.remaining(), 0);
        let mut got = vec![0i64; values.len()];
        c.decode_into(0, &mut got);
        assert_eq!(got, values, "encoding {enc}");
        // Unaligned ranges decode the same.
        if values.len() > 3000 {
            let mut part = vec![0i64; 1500];
            c.decode_into(1777, &mut part);
            assert_eq!(part, values[1777..3277]);
        }
        enc
    }

    #[test]
    fn every_encoding_roundtrips_exactly() {
        assert_eq!(roundtrip(&[7; 5000]), ENC_CONST);
        let seq: Vec<i64> = (0..10_000).map(|i| 1_700_000_000_000 + i * 1000).collect();
        assert_eq!(roundtrip(&seq), ENC_DELTA);
        let small: Vec<i64> = (0..10_000).map(|i| (i * 7919) % 1000).collect();
        assert_eq!(roundtrip(&small), ENC_FOR);
        let cats: Vec<i64> = (0..10_000).map(|i| [10_i64.pow(12), -5, 99_999_999_999][i % 3]).collect();
        assert_eq!(roundtrip(&cats), ENC_DICT);
        let runs: Vec<i64> = (0..10_000).map(|i| i64::from(i / 1000) * 1_000_000_007).collect();
        assert_eq!(roundtrip(&runs), ENC_RLE);
        let extremes: Vec<i64> = (0..5000).map(|i| if i % 2 == 0 { i64::MIN } else { i64::MAX - i }).collect();
        roundtrip(&extremes);
        let random: Vec<i64> = (0..5000u64).map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15) as i64).collect();
        roundtrip(&random);
        roundtrip(&[]);
        roundtrip(&[i64::MIN, i64::MAX]);
    }

    #[test]
    fn hostile_chunks_fail_or_read_safely() {
        let vals: Vec<i64> = (0..300).map(|i| i % 7).collect();
        let mut out = Vec::new();
        encode(&vals, &mut out);
        for cut in 0..out.len() {
            let mut r = Reader::new(&out[..cut]);
            assert!(IntChunk::parse(&mut r, vals.len()).is_err() || cut == out.len());
        }
        for i in 0..out.len() {
            let mut bad = out.clone();
            bad[i] ^= 0xFF;
            let mut r = Reader::new(&bad);
            if let Ok(c) = IntChunk::parse(&mut r, vals.len()) {
                let mut got = vec![0i64; vals.len()];
                c.decode_into(0, &mut got); // never panics
            }
        }
    }
}
