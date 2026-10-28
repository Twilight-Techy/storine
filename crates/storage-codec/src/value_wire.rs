//! Storine Value Encoding (SVE): the self-describing binary encoding of [`PropertyValue`]s used
//! by the document wire protocol and every SDK (`TASK-096`, `protocol/PROTOCOL.md` §4).
//!
//! It is deliberately simple to implement in any language: one tag byte, then fixed-width
//! little-endian fields or a `u32` length prefix. There are no varints, no padding and no
//! checksums (TCP already protects the transport; the server re-encodes values into checksummed
//! archive frames before they reach disk).
//!
//! | Tag  | Type        | Body                                                   |
//! |------|-------------|--------------------------------------------------------|
//! | 0x00 | null        | –                                                      |
//! | 0x01 | bool        | `u8` (0 or 1)                                          |
//! | 0x02 | int64       | `i64`                                                  |
//! | 0x03 | uint64      | `u64`                                                  |
//! | 0x04 | float64     | IEEE-754 bits `u64`                                    |
//! | 0x05 | int128      | `i128` (16 bytes)                                      |
//! | 0x06 | timestamp   | `i64` nanoseconds since the Unix epoch (UTC)           |
//! | 0x07 | uuid        | 16 bytes (RFC 4122 byte order)                         |
//! | 0x08 | id          | 24 raw `InlineId` bytes                                |
//! | 0x09 | text        | `u32` byte length + UTF-8                              |
//! | 0x0A | bytes       | `u32` length + bytes                                   |
//! | 0x0B | decimal     | `i128` mantissa (16 bytes) + `u8` scale (`<= 38`)      |
//! | 0x0C | list        | `u32` count + values                                   |
//! | 0x0D | map         | `u32` count + (`u32` key length + UTF-8 key + value)*  |
//!
//! Decoding is bounded: nesting is limited to [`MAX_VALUE_DEPTH`], every length is checked
//! against the bytes actually present before anything is allocated, map keys must be unique,
//! and trailing bytes are rejected by [`decode_value`].

use std::collections::BTreeMap;

use storage_types::{Decimal, InlineId, Result, StorageError, MAX_DECIMAL_SCALE};

use crate::types::{PropertyValue, MAX_VALUE_DEPTH};

pub const SVE_NULL: u8 = 0x00;
pub const SVE_BOOL: u8 = 0x01;
pub const SVE_INT64: u8 = 0x02;
pub const SVE_UINT64: u8 = 0x03;
pub const SVE_FLOAT64: u8 = 0x04;
pub const SVE_INT128: u8 = 0x05;
pub const SVE_TIMESTAMP: u8 = 0x06;
pub const SVE_UUID: u8 = 0x07;
pub const SVE_ID: u8 = 0x08;
pub const SVE_TEXT: u8 = 0x09;
pub const SVE_BYTES: u8 = 0x0A;
pub const SVE_DECIMAL: u8 = 0x0B;
pub const SVE_LIST: u8 = 0x0C;
pub const SVE_MAP: u8 = 0x0D;

/// Appends the SVE encoding of `value` to `out`.
pub fn encode_value(value: &PropertyValue, out: &mut Vec<u8>) {
    match value {
        PropertyValue::Null => out.push(SVE_NULL),
        PropertyValue::Bool(b) => {
            out.push(SVE_BOOL);
            out.push(u8::from(*b));
        }
        PropertyValue::Int64(v) => {
            out.push(SVE_INT64);
            out.extend_from_slice(&v.to_le_bytes());
        }
        PropertyValue::UInt64(v) => {
            out.push(SVE_UINT64);
            out.extend_from_slice(&v.to_le_bytes());
        }
        PropertyValue::Float64(v) => {
            out.push(SVE_FLOAT64);
            out.extend_from_slice(&v.to_bits().to_le_bytes());
        }
        PropertyValue::Int128(v) => {
            out.push(SVE_INT128);
            out.extend_from_slice(&v.to_le_bytes());
        }
        PropertyValue::TimestampNs(v) => {
            out.push(SVE_TIMESTAMP);
            out.extend_from_slice(&v.to_le_bytes());
        }
        PropertyValue::Uuid(u) => {
            out.push(SVE_UUID);
            out.extend_from_slice(u);
        }
        PropertyValue::Id(id) => {
            out.push(SVE_ID);
            out.extend_from_slice(&id.to_raw_bytes());
        }
        PropertyValue::Text(s) => {
            out.push(SVE_TEXT);
            push_len_bytes(out, s.as_bytes());
        }
        PropertyValue::Bytes(b) => {
            out.push(SVE_BYTES);
            push_len_bytes(out, b);
        }
        PropertyValue::Decimal(d) => {
            out.push(SVE_DECIMAL);
            out.extend_from_slice(&d.mantissa().to_le_bytes());
            out.push(d.scale());
        }
        PropertyValue::List(items) => {
            out.push(SVE_LIST);
            out.extend_from_slice(&(items.len() as u32).to_le_bytes());
            for item in items {
                encode_value(item, out);
            }
        }
        PropertyValue::Map(map) => {
            out.push(SVE_MAP);
            encode_map_body(map, out);
        }
    }
}

/// Appends `count + (key, value)*` (the body of an SVE map, also used for whole documents).
pub fn encode_map_body(map: &BTreeMap<String, PropertyValue>, out: &mut Vec<u8>) {
    out.extend_from_slice(&(map.len() as u32).to_le_bytes());
    for (k, v) in map {
        push_len_bytes(out, k.as_bytes());
        encode_value(v, out);
    }
}

#[inline]
fn push_len_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

/// Cursor over untrusted SVE bytes.
#[derive(Debug)]
pub struct SveReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

#[cold]
#[inline(never)]
fn truncated() -> StorageError {
    StorageError::invalid_value("truncated value encoding")
}

impl<'a> SveReader<'a> {
    #[inline]
    pub const fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    #[inline]
    pub const fn position(&self) -> usize {
        self.pos
    }

    #[inline]
    pub const fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    #[inline]
    pub fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or_else(truncated)?;
        let s = self.buf.get(self.pos..end).ok_or_else(truncated)?;
        self.pos = end;
        Ok(s)
    }

    #[inline]
    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.take(N)?);
        Ok(out)
    }

    #[inline]
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    #[inline]
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    #[inline]
    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    #[inline]
    pub fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.array()?))
    }

    /// A `u32`-length-prefixed UTF-8 string.
    pub fn str(&mut self) -> Result<&'a str> {
        let len = self.u32()? as usize;
        let bytes = self.take(len)?;
        core::str::from_utf8(bytes).map_err(|_| StorageError::invalid_value("text is not valid UTF-8"))
    }

    /// A `u32`-length-prefixed byte string.
    pub fn bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    /// A `u32` element count that cannot exceed the bytes left (each element is >= `min_size`
    /// bytes), so no hostile count can trigger a large allocation.
    pub fn count(&mut self, min_size: usize) -> Result<usize> {
        let n = self.u32()? as usize;
        if n.saturating_mul(min_size.max(1)) > self.remaining() {
            return Err(truncated());
        }
        Ok(n)
    }

    /// One SVE value.
    pub fn value(&mut self) -> Result<PropertyValue> {
        self.value_depth(0)
    }

    fn value_depth(&mut self, depth: usize) -> Result<PropertyValue> {
        let tag = self.u8()?;
        Ok(match tag {
            SVE_NULL => PropertyValue::Null,
            SVE_BOOL => match self.u8()? {
                0 => PropertyValue::Bool(false),
                1 => PropertyValue::Bool(true),
                _ => return Err(StorageError::invalid_value("bool must be 0 or 1")),
            },
            SVE_INT64 => PropertyValue::Int64(self.i64()?),
            SVE_UINT64 => PropertyValue::UInt64(self.u64()?),
            SVE_FLOAT64 => PropertyValue::Float64(f64::from_bits(self.u64()?)),
            SVE_INT128 => PropertyValue::Int128(i128::from_le_bytes(self.array()?)),
            SVE_TIMESTAMP => PropertyValue::TimestampNs(self.i64()?),
            SVE_UUID => PropertyValue::Uuid(self.array()?),
            SVE_ID => PropertyValue::Id(InlineId::from_raw_bytes(self.array()?)?),
            SVE_TEXT => PropertyValue::Text(self.str()?.to_owned()),
            SVE_BYTES => PropertyValue::Bytes(self.bytes()?.to_vec()),
            SVE_DECIMAL => {
                let m = i128::from_le_bytes(self.array()?);
                let scale = self.u8()?;
                if scale > MAX_DECIMAL_SCALE {
                    return Err(StorageError::invalid_value("decimal scale must be at most 38"));
                }
                PropertyValue::Decimal(Decimal::new(m, scale)?)
            }
            SVE_LIST => {
                if depth >= MAX_VALUE_DEPTH {
                    return Err(StorageError::invalid_value("value nesting is too deep (max 32)"));
                }
                let n = self.count(1)?;
                let mut items = Vec::with_capacity(n);
                for _ in 0..n {
                    items.push(self.value_depth(depth + 1)?);
                }
                PropertyValue::List(items)
            }
            SVE_MAP => {
                if depth >= MAX_VALUE_DEPTH {
                    return Err(StorageError::invalid_value("value nesting is too deep (max 32)"));
                }
                PropertyValue::Map(self.map_body_depth(depth + 1)?)
            }
            _ => return Err(StorageError::invalid_value("unknown value type tag")),
        })
    }

    /// `count + (key, value)*` with unique keys.
    pub fn map_body(&mut self) -> Result<BTreeMap<String, PropertyValue>> {
        self.map_body_depth(0)
    }

    fn map_body_depth(&mut self, depth: usize) -> Result<BTreeMap<String, PropertyValue>> {
        let n = self.count(5)?;
        let mut map = BTreeMap::new();
        for _ in 0..n {
            let key = self.str()?.to_owned();
            let value = self.value_depth(depth)?;
            if map.insert(key, value).is_some() {
                return Err(StorageError::invalid_value("duplicate map key"));
            }
        }
        Ok(map)
    }
}

/// Decodes exactly one value from `bytes` (trailing bytes are an error).
pub fn decode_value(bytes: &[u8]) -> Result<PropertyValue> {
    let mut r = SveReader::new(bytes);
    let v = r.value()?;
    if r.remaining() != 0 {
        return Err(StorageError::invalid_value("trailing bytes after value"));
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> PropertyValue {
        let mut inner = BTreeMap::new();
        inner.insert("amount".to_string(), PropertyValue::Decimal("12.34".parse().unwrap()));
        inner.insert("tags".to_string(), PropertyValue::List(vec![
            PropertyValue::Text("a".into()),
            PropertyValue::Null,
            PropertyValue::Bool(true),
        ]));
        let mut m = BTreeMap::new();
        m.insert("i".into(), PropertyValue::Int64(-5));
        m.insert("u".into(), PropertyValue::UInt64(u64::MAX));
        m.insert("f".into(), PropertyValue::Float64(1.25));
        m.insert("big".into(), PropertyValue::Int128(i128::MIN));
        m.insert("ts".into(), PropertyValue::TimestampNs(1_700_000_000_000_000_000));
        m.insert("uuid".into(), PropertyValue::Uuid([7; 16]));
        m.insert("id".into(), PropertyValue::Id(InlineId::new("k").unwrap()));
        m.insert("bytes".into(), PropertyValue::Bytes(vec![0, 255]));
        m.insert("nested".into(), PropertyValue::Map(inner));
        PropertyValue::Map(m)
    }

    #[test]
    fn test_sve_roundtrip_and_golden_bytes() {
        let v = sample();
        let mut out = Vec::new();
        encode_value(&v, &mut out);
        assert_eq!(decode_value(&out).unwrap(), v);

        // Golden encodings pin the format (`RULE-SAFE-05`).
        let mut b = Vec::new();
        encode_value(&PropertyValue::Decimal("-1.50".parse().unwrap()), &mut b);
        let mut want = vec![SVE_DECIMAL];
        want.extend_from_slice(&(-150i128).to_le_bytes());
        want.push(2);
        assert_eq!(b, want);
        let mut b = Vec::new();
        encode_value(&PropertyValue::Text("hé".into()), &mut b);
        assert_eq!(b, [0x09, 3, 0, 0, 0, b'h', 0xC3, 0xA9]);
    }

    #[test]
    fn test_sve_rejects_hostile_input_without_panicking() {
        let mut good = Vec::new();
        encode_value(&sample(), &mut good);
        for cut in 0..good.len() {
            assert!(decode_value(&good[..cut]).is_err(), "truncation at {cut}");
        }
        for i in 0..good.len() {
            for bit in [0x01u8, 0x80, 0xFF] {
                let mut b = good.clone();
                b[i] ^= bit;
                let _ = decode_value(&b); // must not panic
            }
        }
        // Huge counts are rejected before allocation.
        assert!(decode_value(&[SVE_LIST, 0xFF, 0xFF, 0xFF, 0xFF]).is_err());
        assert!(decode_value(&[SVE_TEXT, 0xFF, 0xFF, 0xFF, 0x7F]).is_err());
        // Too deep.
        let mut deep = Vec::new();
        for _ in 0..40 {
            deep.extend_from_slice(&[SVE_LIST, 1, 0, 0, 0]);
        }
        deep.push(SVE_NULL);
        assert!(decode_value(&deep).is_err());
        // Bad bool, bad decimal scale, duplicate keys, trailing bytes, invalid UTF-8.
        assert!(decode_value(&[SVE_BOOL, 2]).is_err());
        let mut d = vec![SVE_DECIMAL];
        d.extend_from_slice(&[0; 16]);
        d.push(39);
        assert!(decode_value(&d).is_err());
        let dup = [SVE_MAP, 2, 0, 0, 0, 1, 0, 0, 0, b'a', 0, 1, 0, 0, 0, b'a', 0];
        assert!(decode_value(&dup).is_err());
        assert!(decode_value(&[SVE_NULL, 0]).is_err());
        assert!(decode_value(&[SVE_TEXT, 1, 0, 0, 0, 0xFF]).is_err());
    }
}
