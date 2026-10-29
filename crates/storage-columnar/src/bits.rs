//! Bit packing and a bounds-checked reader for untrusted segment bytes.
//!
//! Packed values are little-endian bit streams in `u64` words: value `i` occupies bits
//! `i * width .. (i + 1) * width`. Every packed area is followed by one zero word so a value
//! straddling the last word boundary is read with two plain word loads (no branch).

use storage_types::{Result, StorageError};

/// Bits needed to represent `v` (`0` for `0`).
#[inline]
pub fn bit_width(v: u64) -> u8 {
    (64 - v.leading_zeros()) as u8
}

/// Words a packed area of `n` values of `width` bits occupies, including the padding word.
#[inline]
pub fn packed_words(n: usize, width: u8) -> usize {
    (n * width as usize).div_ceil(64) + 1
}

/// Appends `values` packed at `width` bits (each must fit) as little-endian words.
pub fn pack(values: impl Iterator<Item = u64>, n: usize, width: u8, out: &mut Vec<u8>) {
    let words = packed_words(n, width);
    let start = out.len();
    out.resize(start + words * 8, 0);
    if width == 0 {
        return;
    }
    let w = u32::from(width);
    let mut acc: u128 = 0;
    let mut bits: u32 = 0;
    let mut pos = start;
    for v in values.take(n) {
        acc |= u128::from(v) << bits;
        bits += w;
        if bits >= 64 {
            out[pos..pos + 8].copy_from_slice(&(acc as u64).to_le_bytes());
            pos += 8;
            acc >>= 64;
            bits -= 64;
        }
    }
    if bits > 0 {
        out[pos..pos + 8].copy_from_slice(&(acc as u64).to_le_bytes());
    }
}

/// A validated packed area: `n` values of `width` bits.
#[derive(Clone, Copy, Debug)]
pub struct Packed<'a> {
    words: &'a [u8],
    width: u8,
    n: usize,
}

impl<'a> Packed<'a> {
    pub fn new(words: &'a [u8], width: u8, n: usize) -> Result<Self> {
        if width > 64 || words.len() != packed_words(n, width) * 8 {
            return Err(corrupt("packed area length"));
        }
        Ok(Self { words, width, n })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.n
    }

    #[inline]
    pub fn width(&self) -> u8 {
        self.width
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    #[inline]
    fn word(&self, i: usize) -> u64 {
        let b = &self.words[i * 8..i * 8 + 8];
        u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
    }

    /// Value `i` (`i < len`).
    #[inline]
    pub fn get(&self, i: usize) -> u64 {
        if self.width == 0 {
            return 0;
        }
        let bit = i * self.width as usize;
        let (w, off) = (bit >> 6, (bit & 63) as u32);
        let two = u128::from(self.word(w)) | (u128::from(self.word(w + 1)) << 64);
        let v = (two >> off) as u64;
        if self.width == 64 {
            v
        } else {
            v & ((1u64 << self.width) - 1)
        }
    }

    /// Values `start .. start + out.len()` into `out` (vectorized, `storage_codec::simd`).
    pub fn unpack_into(&self, start: usize, out: &mut [u64]) {
        storage_codec::simd::unpack(self.words, self.width, start, out);
    }
}

#[cold]
#[inline(never)]
pub fn corrupt(what: &'static str) -> StorageError {
    StorageError::corrupted_header(what)
}

/// Bounds-checked cursor over untrusted bytes: every read fails cleanly instead of panicking.
#[derive(Clone, Debug)]
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).filter(|&e| e <= self.buf.len()).ok_or_else(|| corrupt("truncated column data"))?;
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    pub fn u64(&mut self) -> Result<u64> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
    }

    pub fn i64(&mut self) -> Result<i64> {
        Ok(self.u64()? as i64)
    }

    pub fn i128(&mut self) -> Result<i128> {
        let b = self.take(16)?;
        let mut a = [0u8; 16];
        a.copy_from_slice(b);
        Ok(i128::from_le_bytes(a))
    }

    pub fn f64(&mut self) -> Result<f64> {
        Ok(f64::from_bits(self.u64()?))
    }

    /// A length-prefixed (`u32`) byte string.
    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }

    /// A packed area of `n` values: `u8 width | words`.
    pub fn packed(&mut self, n: usize) -> Result<Packed<'a>> {
        let width = self.u8()?;
        if width > 64 {
            return Err(corrupt("bit width"));
        }
        let words = self.take(packed_words(n, width).checked_mul(8).ok_or_else(|| corrupt("packed size"))?)?;
        Packed::new(words, width, n)
    }
}

pub fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_u32(out, b.len() as u32);
    out.extend_from_slice(b);
}

/// Appends `u8 width | words` for `values`.
pub fn put_packed(out: &mut Vec<u8>, values: &[u64]) {
    let width = values.iter().copied().max().map_or(0, bit_width);
    out.push(width);
    pack(values.iter().copied(), values.len(), width, out);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_roundtrips_every_width() {
        for width in 0..=64u8 {
            let mask = if width == 64 { u64::MAX } else { (1u64 << width) - 1 };
            let vals: Vec<u64> = (0..1000u64).map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15) & mask).collect();
            let mut out = Vec::new();
            pack(vals.iter().copied(), vals.len(), width, &mut out);
            let p = Packed::new(&out, width, vals.len()).unwrap();
            for (i, v) in vals.iter().enumerate() {
                assert_eq!(p.get(i), *v, "width {width} index {i}");
            }
            let mut buf = vec![0u64; 333];
            p.unpack_into(500, &mut buf);
            assert_eq!(buf, vals[500..833]);
        }
    }

    #[test]
    fn reader_rejects_truncation() {
        let mut r = Reader::new(&[1, 2, 3]);
        assert!(r.u32().is_err());
        let mut r = Reader::new(&[65]);
        assert!(r.packed(4).is_err());
        assert!(Packed::new(&[0; 8], 1, 100).is_err());
    }
}
