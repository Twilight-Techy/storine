//! Column chunks: one field path over one row group, in every value kind a document can hold.
//!
//! ```text
//! chunk    := u8 kind | validity | body           (kind 0: every row null, no body)
//!             u8 KIND_VARIANT | validity | u8 k | k x u8 kind | int-chunk tags | k x (u32 len | body)
//! validity := u8 0                                (no nulls)
//!           | u8 1 | packed bits (1 = present)
//! body (by kind):
//!   BOOL                packed bits
//!   INT, TIMESTAMP      int-chunk
//!   UINT                int-chunk of (v XOR 2^63)  (order-preserving)
//!   FLOAT               u8 0 CONST f64 | 1 PLAIN n x f64 | 2 DICT u32 d, d x f64, packed codes
//!                     | 3 INTEGRAL int-chunk      (every value an exact integer)
//!   INT128              u8 0 int-chunk (all fit i64) | 1 PLAIN n x i128
//!   DECIMAL             int128-body mantissas | int-chunk scales
//!   UUID, ID, TEXT, BYTES, NESTED
//!                       bytes-body: u8 0 CONST bytes | 1 DICT u32 d, int-chunk offsets(d + 1),
//!                       blob, packed codes | 2 PLAIN int-chunk offsets(n + 1), blob
//!   blob                := u8 lz4 | u32 raw_len | bytes(stored)
//! ```
//!
//! Null rows hold a filler (the previous value) so they never cost compression. Dictionaries
//! of bytes are sorted, so code order is value order (range filters can run on codes).
//! Nested lists are stored as their SVE encoding (`value_wire`), opaque to filters.

use std::collections::BTreeMap;

use storage_codec::types::{MapRef, PropertyValue, PropertyValueRef};
use storage_codec::{compress_lz4, decompress_lz4};
use storage_types::{Decimal, FastHashMap, InlineId, Result};

use crate::bits::{corrupt, pack, put_packed, put_u32, Packed, Reader};
use crate::int::{self, IntChunk};

/// Value kinds of a column (`u8` on disk).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
pub enum Kind {
    Null = 0,
    Bool = 1,
    Int = 2,
    UInt = 3,
    Float = 4,
    Int128 = 5,
    Timestamp = 6,
    Uuid = 7,
    Id = 8,
    Text = 9,
    Bytes = 10,
    Decimal = 11,
    Nested = 12,
}

pub const KIND_VARIANT: u8 = 255;

impl Kind {
    pub fn from_u8(b: u8) -> Result<Self> {
        Ok(match b {
            0 => Self::Null,
            1 => Self::Bool,
            2 => Self::Int,
            3 => Self::UInt,
            4 => Self::Float,
            5 => Self::Int128,
            6 => Self::Timestamp,
            7 => Self::Uuid,
            8 => Self::Id,
            9 => Self::Text,
            10 => Self::Bytes,
            11 => Self::Decimal,
            12 => Self::Nested,
            _ => return Err(corrupt("column kind")),
        })
    }
}

/// One owned column value (what a chunk stores per row).
#[derive(Clone, Debug, PartialEq)]
pub enum Val {
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Int128(i128),
    Timestamp(i64),
    Uuid([u8; 16]),
    Id([u8; 24]),
    Text(String),
    Bytes(Vec<u8>),
    Decimal(i128, u8),
    /// SVE encoding of a list (or of a value nested in one).
    Nested(Vec<u8>),
}

impl Val {
    pub fn kind(&self) -> Kind {
        match self {
            Self::Bool(_) => Kind::Bool,
            Self::Int(_) => Kind::Int,
            Self::UInt(_) => Kind::UInt,
            Self::Float(_) => Kind::Float,
            Self::Int128(_) => Kind::Int128,
            Self::Timestamp(_) => Kind::Timestamp,
            Self::Uuid(_) => Kind::Uuid,
            Self::Id(_) => Kind::Id,
            Self::Text(_) => Kind::Text,
            Self::Bytes(_) => Kind::Bytes,
            Self::Decimal(..) => Kind::Decimal,
            Self::Nested(_) => Kind::Nested,
        }
    }

    /// The document value this column value stands for.
    pub fn to_property(&self) -> PropertyValue {
        match self {
            Self::Bool(b) => PropertyValue::Bool(*b),
            Self::Int(v) => PropertyValue::Int64(*v),
            Self::UInt(v) => PropertyValue::UInt64(*v),
            Self::Float(v) => PropertyValue::Float64(*v),
            Self::Int128(v) => PropertyValue::Int128(*v),
            Self::Timestamp(v) => PropertyValue::TimestampNs(*v),
            Self::Uuid(u) => PropertyValue::Uuid(*u),
            Self::Id(b) => InlineId::from_raw_bytes(*b).map_or(PropertyValue::Null, PropertyValue::Id),
            Self::Text(s) => PropertyValue::Text(s.clone()),
            Self::Bytes(b) => PropertyValue::Bytes(b.clone()),
            Self::Decimal(m, s) => Decimal::new(*m, *s).map_or(PropertyValue::Null, PropertyValue::Decimal),
            Self::Nested(sve) => storage_codec::value_wire::SveReader::new(sve)
                .value()
                .unwrap_or(PropertyValue::Null),
        }
    }

    /// Bytes of a byte-like value (`Uuid`, `Id`, `Text`, `Bytes`, `Nested`).
    fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Uuid(u) => u,
            Self::Id(b) => b,
            Self::Text(s) => s.as_bytes(),
            Self::Bytes(b) | Self::Nested(b) => b,
            _ => &[],
        }
    }
}

/// Flattens a document into `(path, value)` cells: nested maps become dotted paths; lists,
/// explicit nulls and empty maps are stored whole (SVE-encoded `Nested`), so the document can be
/// rebuilt exactly.
pub fn flatten(fields: MapRef<'_>, out: &mut Vec<(String, Val)>) {
    flatten_into(fields, &mut String::new(), out, 0);
}

fn flatten_into(map: MapRef<'_>, prefix: &mut String, out: &mut Vec<(String, Val)>, depth: usize) {
    for (name, v) in map.iter() {
        let len = prefix.len();
        if !prefix.is_empty() {
            prefix.push('.');
        }
        prefix.push_str(name);
        match v {
            PropertyValueRef::Map(m) if depth < 32 && !m.is_empty() => flatten_into(m, prefix, out, depth + 1),
            other => {
                if let Some(val) = val_of_ref(other) {
                    out.push((prefix.clone(), val));
                }
            }
        }
        prefix.truncate(len);
    }
}

/// Flattens an owned document (tests and callers without archived views).
pub fn flatten_owned(fields: &BTreeMap<String, PropertyValue>, out: &mut Vec<(String, Val)>) {
    fn go(map: &BTreeMap<String, PropertyValue>, prefix: &mut String, out: &mut Vec<(String, Val)>) {
        for (name, v) in map {
            let len = prefix.len();
            if !prefix.is_empty() {
                prefix.push('.');
            }
            prefix.push_str(name);
            match v {
                PropertyValue::Map(m) if !m.is_empty() => go(m, prefix, out),
                PropertyValue::Null | PropertyValue::Map(_) | PropertyValue::List(_) => {
                    let mut sve = Vec::new();
                    storage_codec::value_wire::encode_value(v, &mut sve);
                    out.push((prefix.clone(), Val::Nested(sve)));
                }
                scalar => {
                    if let Some(val) = val_of_owned(scalar) {
                        out.push((prefix.clone(), val));
                    }
                }
            }
            prefix.truncate(len);
        }
    }
    go(fields, &mut String::new(), out);
}

fn val_of_owned(v: &PropertyValue) -> Option<Val> {
    Some(match v {
        PropertyValue::Null | PropertyValue::Map(_) | PropertyValue::List(_) => return None,
        PropertyValue::Bool(b) => Val::Bool(*b),
        PropertyValue::Int64(x) => Val::Int(*x),
        PropertyValue::UInt64(x) => Val::UInt(*x),
        PropertyValue::Float64(x) => Val::Float(*x),
        PropertyValue::Int128(x) => Val::Int128(*x),
        PropertyValue::TimestampNs(x) => Val::Timestamp(*x),
        PropertyValue::Uuid(u) => Val::Uuid(*u),
        PropertyValue::Id(id) => Val::Id(id.to_raw_bytes()),
        PropertyValue::Text(s) => Val::Text(s.clone()),
        PropertyValue::Bytes(b) => Val::Bytes(b.clone()),
        PropertyValue::Decimal(d) => Val::Decimal(d.mantissa(), d.scale()),
    })
}

fn val_of_ref(v: PropertyValueRef<'_>) -> Option<Val> {
    Some(match v {
        PropertyValueRef::Null => {
            let mut sve = Vec::with_capacity(1);
            storage_codec::value_wire::encode_value(&PropertyValue::Null, &mut sve);
            Val::Nested(sve)
        }
        PropertyValueRef::Bool(b) => Val::Bool(b),
        PropertyValueRef::Int64(x) => Val::Int(x),
        PropertyValueRef::UInt64(x) => Val::UInt(x),
        PropertyValueRef::Float64(x) => Val::Float(x),
        PropertyValueRef::Int128(x) => Val::Int128(x),
        PropertyValueRef::TimestampNs(x) => Val::Timestamp(x),
        PropertyValueRef::Uuid(u) => Val::Uuid(u),
        PropertyValueRef::Id(id) => Val::Id(id.to_raw_bytes()),
        PropertyValueRef::Text(s) => Val::Text(s.to_owned()),
        PropertyValueRef::Bytes(b) => Val::Bytes(b.to_vec()),
        PropertyValueRef::Decimal(d) => Val::Decimal(d.mantissa(), d.scale()),
        other @ (PropertyValueRef::List(_) | PropertyValueRef::Map(_)) => {
            let mut sve = Vec::new();
            storage_codec::value_wire::encode_value(&other.to_owned_value(), &mut sve);
            Val::Nested(sve)
        }
    })
}

// ── zone maps ──

/// Min / max summary of a chunk's non-null values, used to skip chunks a filter cannot match.
#[derive(Clone, Debug, PartialEq)]
pub enum Zone {
    /// No summary (every row null, mixed kinds, nested values).
    None,
    Bool { has_false: bool, has_true: bool },
    /// `Int`, `Timestamp`; `UInt` in its own domain.
    Int { min: i64, max: i64 },
    UInt { min: u64, max: u64 },
    /// Non-NaN extremes, and whether a NaN occurs.
    Float { min: f64, max: f64, has_nan: bool },
    Int128 { min: i128, max: i128 },
    Decimal { min: (i128, u8), max: (i128, u8) },
    /// Byte-wise extremes, truncated to 32 bytes (`max_truncated`: the true maximum may be
    /// longer than `max`, so `max` is only a prefix bound).
    Bytes { min: Vec<u8>, max: Vec<u8>, max_truncated: bool },
}

const ZONE_PREFIX: usize = 32;

impl Zone {
    fn write(&self, out: &mut Vec<u8>) {
        match self {
            Self::None => out.push(0),
            Self::Bool { has_false, has_true } => {
                out.push(1);
                out.push(u8::from(*has_false) | (u8::from(*has_true) << 1));
            }
            Self::Int { min, max } => {
                out.push(2);
                out.extend_from_slice(&min.to_le_bytes());
                out.extend_from_slice(&max.to_le_bytes());
            }
            Self::UInt { min, max } => {
                out.push(3);
                out.extend_from_slice(&min.to_le_bytes());
                out.extend_from_slice(&max.to_le_bytes());
            }
            Self::Float { min, max, has_nan } => {
                out.push(4);
                out.extend_from_slice(&min.to_bits().to_le_bytes());
                out.extend_from_slice(&max.to_bits().to_le_bytes());
                out.push(u8::from(*has_nan));
            }
            Self::Int128 { min, max } => {
                out.push(5);
                out.extend_from_slice(&min.to_le_bytes());
                out.extend_from_slice(&max.to_le_bytes());
            }
            Self::Decimal { min, max } => {
                out.push(6);
                out.extend_from_slice(&min.0.to_le_bytes());
                out.push(min.1);
                out.extend_from_slice(&max.0.to_le_bytes());
                out.push(max.1);
            }
            Self::Bytes { min, max, max_truncated } => {
                out.push(7);
                out.push(min.len() as u8);
                out.extend_from_slice(min);
                out.push(max.len() as u8);
                out.extend_from_slice(max);
                out.push(u8::from(*max_truncated));
            }
        }
    }

    pub(crate) fn read(r: &mut Reader<'_>) -> Result<Self> {
        Ok(match r.u8()? {
            0 => Self::None,
            1 => {
                let b = r.u8()?;
                Self::Bool { has_false: b & 1 != 0, has_true: b & 2 != 0 }
            }
            2 => Self::Int { min: r.i64()?, max: r.i64()? },
            3 => Self::UInt { min: r.u64()?, max: r.u64()? },
            4 => Self::Float { min: r.f64()?, max: r.f64()?, has_nan: r.u8()? != 0 },
            5 => Self::Int128 { min: r.i128()?, max: r.i128()? },
            6 => {
                let min = (r.i128()?, r.u8()?);
                let max = (r.i128()?, r.u8()?);
                Self::Decimal { min, max }
            }
            7 => {
                let n = r.u8()? as usize;
                let min = r.take(n)?.to_vec();
                let n = r.u8()? as usize;
                let max = r.take(n)?.to_vec();
                Self::Bytes { min, max, max_truncated: r.u8()? != 0 }
            }
            _ => return Err(corrupt("zone map")),
        })
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut v = Vec::new();
        self.write(&mut v);
        v
    }
}

fn decimal_cmp(a: (i128, u8), b: (i128, u8)) -> core::cmp::Ordering {
    match (Decimal::new(a.0, a.1), Decimal::new(b.0, b.1)) {
        (Ok(x), Ok(y)) => x.cmp(&y),
        _ => a.cmp(&b),
    }
}

fn zone_of(kind: Kind, vals: &[&Val]) -> Zone {
    let Some(first) = vals.first() else { return Zone::None };
    match kind {
        Kind::Bool => Zone::Bool {
            has_false: vals.iter().any(|v| matches!(v, Val::Bool(false))),
            has_true: vals.iter().any(|v| matches!(v, Val::Bool(true))),
        },
        Kind::Int | Kind::Timestamp => {
            let it = vals.iter().filter_map(|v| match v {
                Val::Int(x) | Val::Timestamp(x) => Some(*x),
                _ => None,
            });
            let (min, max) = it.fold((i64::MAX, i64::MIN), |(a, b), x| (a.min(x), b.max(x)));
            Zone::Int { min, max }
        }
        Kind::UInt => {
            let it = vals.iter().filter_map(|v| if let Val::UInt(x) = v { Some(*x) } else { None });
            let (min, max) = it.fold((u64::MAX, 0), |(a, b), x| (a.min(x), b.max(x)));
            Zone::UInt { min, max }
        }
        Kind::Float => {
            let mut has_nan = false;
            let (mut min, mut max) = (f64::INFINITY, f64::NEG_INFINITY);
            for v in vals {
                if let Val::Float(x) = v {
                    if x.is_nan() {
                        has_nan = true;
                    } else {
                        min = min.min(*x);
                        max = max.max(*x);
                    }
                }
            }
            Zone::Float { min, max, has_nan }
        }
        Kind::Int128 => {
            let it = vals.iter().filter_map(|v| if let Val::Int128(x) = v { Some(*x) } else { None });
            let (min, max) = it.fold((i128::MAX, i128::MIN), |(a, b), x| (a.min(x), b.max(x)));
            Zone::Int128 { min, max }
        }
        Kind::Decimal => {
            let Val::Decimal(m, s) = first else { return Zone::None };
            let (mut min, mut max) = ((*m, *s), (*m, *s));
            for v in vals {
                if let Val::Decimal(m, s) = v {
                    if decimal_cmp((*m, *s), min).is_lt() {
                        min = (*m, *s);
                    }
                    if decimal_cmp((*m, *s), max).is_gt() {
                        max = (*m, *s);
                    }
                }
            }
            Zone::Decimal { min, max }
        }
        Kind::Text | Kind::Bytes | Kind::Uuid | Kind::Id => {
            let min = vals.iter().map(|v| v.as_bytes()).min().unwrap_or_default();
            let max = vals.iter().map(|v| v.as_bytes()).max().unwrap_or_default();
            Zone::Bytes {
                min: min[..min.len().min(ZONE_PREFIX)].to_vec(),
                max: max[..max.len().min(ZONE_PREFIX)].to_vec(),
                max_truncated: max.len() > ZONE_PREFIX,
            }
        }
        Kind::Nested | Kind::Null => Zone::None,
    }
}

// ── encoding ──

/// The summary of an encoded chunk, kept in the segment footer.
#[derive(Clone, Debug, PartialEq)]
pub struct ChunkMeta {
    pub null_count: u32,
    pub zone: Zone,
}

/// Encodes `rows` (one entry per row, `None` = null) as one chunk.
pub fn encode_chunk(rows: &[Option<Val>], out: &mut Vec<u8>) -> ChunkMeta {
    let n = rows.len();
    let present: Vec<&Val> = rows.iter().flatten().collect();
    let null_count = (n - present.len()) as u32;
    if present.is_empty() {
        out.push(Kind::Null as u8);
        return ChunkMeta { null_count, zone: Zone::None };
    }
    let mut kinds: Vec<Kind> = present.iter().map(|v| v.kind()).collect();
    kinds.sort_unstable();
    kinds.dedup();
    let validity = |out: &mut Vec<u8>| {
        if null_count == 0 {
            out.push(0);
        } else {
            out.push(1);
            put_packed_bits(out, rows.iter().map(Option::is_some), n);
        }
    };
    if let [kind] = kinds[..] {
        out.push(kind as u8);
        validity(out);
        // Null rows repeat the previous value (or the first one) so they compress away.
        let mut filler: &Val = present[0];
        let dense: Vec<&Val> = rows
            .iter()
            .map(|r| {
                if let Some(v) = r {
                    filler = v;
                }
                filler
            })
            .collect();
        encode_body(kind, &dense, out);
        return ChunkMeta { null_count, zone: zone_of(kind, &present) };
    }
    out.push(KIND_VARIANT);
    validity(out);
    out.push(kinds.len() as u8);
    for k in &kinds {
        out.push(*k as u8);
    }
    let mut last_tag = 0i64;
    let tags: Vec<i64> = rows
        .iter()
        .map(|r| {
            if let Some(v) = r {
                last_tag = kinds.iter().position(|k| *k == v.kind()).unwrap_or(0) as i64;
            }
            last_tag
        })
        .collect();
    int::encode(&tags, out);
    for k in &kinds {
        let child: Vec<&Val> = present.iter().copied().filter(|v| v.kind() == *k).collect();
        let mut body = Vec::new();
        encode_body(*k, &child, &mut body);
        put_u32(out, body.len() as u32);
        out.extend_from_slice(&body);
    }
    ChunkMeta { null_count, zone: Zone::None }
}

fn put_packed_bits(out: &mut Vec<u8>, bits: impl Iterator<Item = bool>, n: usize) {
    out.push(1);
    pack(bits.map(u64::from), n, 1, out);
}

/// Encodes values that all have `kind` (no nulls).
fn encode_body(kind: Kind, vals: &[&Val], out: &mut Vec<u8>) {
    let ints = |f: fn(&Val) -> i64| -> Vec<i64> { vals.iter().map(|v| f(v)).collect() };
    match kind {
        Kind::Null => {}
        Kind::Bool => put_packed_bits(out, vals.iter().map(|v| matches!(v, Val::Bool(true))), vals.len()),
        Kind::Int | Kind::Timestamp => int::encode(
            &ints(|v| match v {
                Val::Int(x) | Val::Timestamp(x) => *x,
                _ => 0,
            }),
            out,
        ),
        Kind::UInt => int::encode(&ints(|v| if let Val::UInt(x) = v { (*x ^ (1 << 63)) as i64 } else { 0 }), out),
        Kind::Float => encode_floats(&vals.iter().map(|v| if let Val::Float(x) = v { *x } else { 0.0 }).collect::<Vec<_>>(), out),
        Kind::Int128 => encode_i128(&vals.iter().map(|v| if let Val::Int128(x) = v { *x } else { 0 }).collect::<Vec<_>>(), out),
        Kind::Decimal => {
            let m: Vec<i128> = vals.iter().map(|v| if let Val::Decimal(m, _) = v { *m } else { 0 }).collect();
            encode_i128(&m, out);
            int::encode(&ints(|v| if let Val::Decimal(_, s) = v { i64::from(*s) } else { 0 }), out);
        }
        Kind::Uuid | Kind::Id | Kind::Text | Kind::Bytes | Kind::Nested => {
            encode_bytes(&vals.iter().map(|v| v.as_bytes()).collect::<Vec<_>>(), out);
        }
    }
}

fn encode_floats(vals: &[f64], out: &mut Vec<u8>) {
    let first = vals.first().copied().unwrap_or(0.0);
    if vals.iter().all(|v| v.to_bits() == first.to_bits()) {
        out.push(0);
        out.extend_from_slice(&first.to_bits().to_le_bytes());
        return;
    }
    // Exact integers (counts, cents stored as floats): bit-exact round trip through i64.
    let integral = vals.iter().all(|&v| v.fract() == 0.0 && v.abs() < 9.007_199_254_740_992e15 && !(v == 0.0 && v.is_sign_negative()));
    if integral {
        out.push(3);
        int::encode(&vals.iter().map(|&v| v as i64).collect::<Vec<_>>(), out);
        return;
    }
    let mut distinct: FastHashMap<u64, ()> = FastHashMap::default();
    let cap = (vals.len() / 4).min(4096);
    let small = vals.iter().all(|v| {
        distinct.insert(v.to_bits(), ());
        distinct.len() <= cap
    });
    if small && vals.len() >= 64 {
        let mut dict: Vec<f64> = distinct.into_keys().map(f64::from_bits).collect();
        dict.sort_unstable_by(f64::total_cmp);
        let pos: FastHashMap<u64, u64> = dict.iter().enumerate().map(|(i, v)| (v.to_bits(), i as u64)).collect();
        out.push(2);
        put_u32(out, dict.len() as u32);
        for d in &dict {
            out.extend_from_slice(&d.to_bits().to_le_bytes());
        }
        let codes: Vec<u64> = vals.iter().map(|v| pos.get(&v.to_bits()).copied().unwrap_or(0)).collect();
        put_packed(out, &codes);
        return;
    }
    out.push(1);
    for v in vals {
        out.extend_from_slice(&v.to_bits().to_le_bytes());
    }
}

fn encode_i128(vals: &[i128], out: &mut Vec<u8>) {
    if vals.iter().all(|v| i64::try_from(*v).is_ok()) {
        out.push(0);
        int::encode(&vals.iter().map(|&v| v as i64).collect::<Vec<_>>(), out);
    } else {
        out.push(1);
        for v in vals {
            out.extend_from_slice(&v.to_le_bytes());
        }
    }
}

fn put_blob(out: &mut Vec<u8>, raw: &[u8]) {
    let compressed = if raw.len() >= 256 { compress_lz4(raw) } else { Vec::new() };
    if !compressed.is_empty() && compressed.len() + compressed.len() / 8 < raw.len() {
        out.push(1);
        put_u32(out, raw.len() as u32);
        put_u32(out, compressed.len() as u32);
        out.extend_from_slice(&compressed);
    } else {
        out.push(0);
        put_u32(out, raw.len() as u32);
        put_u32(out, raw.len() as u32);
        out.extend_from_slice(raw);
    }
}

fn encode_bytes(vals: &[&[u8]], out: &mut Vec<u8>) {
    let first = vals.first().copied().unwrap_or_default();
    if vals.iter().all(|v| *v == first) {
        out.push(0);
        crate::bits::put_bytes(out, first);
        return;
    }
    let cap = (vals.len() / 2).min(1 << 16);
    let mut distinct: FastHashMap<&[u8], ()> = FastHashMap::default();
    let small = vals.iter().all(|v| {
        distinct.insert(v, ());
        distinct.len() <= cap
    });
    if small {
        let mut dict: Vec<&[u8]> = distinct.into_keys().collect();
        dict.sort_unstable();
        let pos: FastHashMap<&[u8], u64> = dict.iter().enumerate().map(|(i, v)| (*v, i as u64)).collect();
        out.push(1);
        put_u32(out, dict.len() as u32);
        put_offsets_and_blob(&dict, out);
        let codes: Vec<u64> = vals.iter().map(|v| pos.get(v).copied().unwrap_or(0)).collect();
        put_packed(out, &codes);
        return;
    }
    out.push(2);
    put_offsets_and_blob(vals, out);
}

fn put_offsets_and_blob(vals: &[&[u8]], out: &mut Vec<u8>) {
    let mut offsets = Vec::with_capacity(vals.len() + 1);
    let mut raw = Vec::new();
    offsets.push(0i64);
    for v in vals {
        raw.extend_from_slice(v);
        offsets.push(raw.len() as i64);
    }
    int::encode(&offsets, out);
    put_blob(out, &raw);
}

// ── decoding ──

/// A parsed chunk of `n` rows (borrows the segment bytes, except decompressed blobs).
#[derive(Debug)]
pub struct Chunk<'a> {
    pub n: usize,
    /// Present-bits (`None`: no nulls).
    pub validity: Option<Packed<'a>>,
    pub body: Body<'a>,
}

#[derive(Debug)]
pub enum Body<'a> {
    AllNull,
    Bool(Packed<'a>),
    /// `Int`, `Timestamp`, `UInt` (values `XOR 2^63`): the kind says which.
    Int(Kind, IntChunk<'a>),
    Float(FloatBody<'a>),
    Int128(I128Body<'a>),
    Decimal(I128Body<'a>, IntChunk<'a>),
    Bytes(Kind, BytesBody<'a>),
    /// Rows tagged with an index into `kinds`; child `j` holds the values of kind `kinds[j]`
    /// densely, in row order.
    Variant { kinds: Vec<Kind>, tags: IntChunk<'a>, children: Vec<(usize, Body<'a>)> },
}

#[derive(Debug)]
pub enum FloatBody<'a> {
    Const(f64),
    Plain(&'a [u8]),
    Dict(Vec<f64>, Packed<'a>),
    Integral(IntChunk<'a>),
}

#[derive(Debug)]
pub enum I128Body<'a> {
    Small(IntChunk<'a>),
    Plain(&'a [u8]),
}

#[derive(Debug)]
pub enum BytesBody<'a> {
    Const(&'a [u8]),
    /// Sorted dictionary `(offsets, blob)` and per-row codes.
    Dict { offsets: Vec<i64>, blob: Blob<'a>, codes: Packed<'a> },
    Plain { offsets: Vec<i64>, blob: Blob<'a> },
}

/// A byte blob: borrowed from the segment, or owned once decompressed.
#[derive(Debug)]
pub enum Blob<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}

impl Blob<'_> {
    pub fn bytes(&self) -> &[u8] {
        match self {
            Self::Borrowed(b) => b,
            Self::Owned(v) => v,
        }
    }
}

impl<'a> Chunk<'a> {
    /// Parses a chunk of `n` rows (structure only: values decode on demand).
    pub fn parse(bytes: &'a [u8], n: usize) -> Result<Self> {
        let mut r = Reader::new(bytes);
        let kind = r.u8()?;
        if kind == Kind::Null as u8 {
            return Ok(Self { n, validity: None, body: Body::AllNull });
        }
        let validity = match r.u8()? {
            0 => None,
            1 => Some(r.packed(n)?),
            _ => return Err(corrupt("validity flag")),
        };
        if validity.as_ref().is_some_and(|p| p.width() != 1 && n > 0) {
            return Err(corrupt("validity width"));
        }
        let body = if kind == KIND_VARIANT {
            let k = r.u8()? as usize;
            if !(2..=12).contains(&k) {
                return Err(corrupt("variant kinds"));
            }
            let mut kinds = Vec::with_capacity(k);
            for _ in 0..k {
                kinds.push(Kind::from_u8(r.u8()?)?);
            }
            let tags = IntChunk::parse(&mut r, n)?;
            let mut counts = vec![0usize; k];
            let mut buf = vec![0i64; n.min(int::BLOCK)];
            let mut i = 0;
            while i < n {
                let m = (n - i).min(buf.len());
                tags.decode_into(i, &mut buf[..m]);
                for (j, t) in buf[..m].iter().enumerate() {
                    if validity.as_ref().is_none_or(|v| v.get(i + j) == 1) {
                        *counts.get_mut(*t as usize).ok_or_else(|| corrupt("variant tag"))? += 1;
                    }
                }
                i += m;
            }
            let mut children = Vec::with_capacity(k);
            for (j, kind) in kinds.iter().enumerate() {
                let body = r.bytes()?;
                let mut cr = Reader::new(body);
                children.push((counts[j], parse_body(*kind, &mut cr, counts[j])?));
                if cr.remaining() != 0 {
                    return Err(corrupt("variant child length"));
                }
            }
            Body::Variant { kinds, tags, children }
        } else {
            parse_body(Kind::from_u8(kind)?, &mut r, n)?
        };
        if r.remaining() != 0 {
            return Err(corrupt("trailing chunk bytes"));
        }
        Ok(Self { n, validity, body })
    }

    /// Whether row `i` holds a value (`i < n`, the row count the chunk was parsed with).
    #[inline]
    pub fn is_present(&self, i: usize) -> bool {
        match (&self.body, &self.validity) {
            (Body::AllNull, _) => false,
            (_, None) => true,
            (_, Some(v)) => v.get(i) == 1,
        }
    }

    /// Every row as an owned value (`None` = null). Slow path: tests, row reconstruction.
    pub fn decode_all(&self) -> Result<Vec<Option<Val>>> {
        let present: Vec<bool> = (0..self.n).map(|i| self.is_present(i)).collect();
        match &self.body {
            Body::AllNull => Ok(vec![None; self.n]),
            Body::Variant { kinds, tags, children } => {
                let mut child_vals = Vec::with_capacity(children.len());
                for (k, (count, c)) in kinds.iter().zip(children) {
                    child_vals.push(decode_body(*k, c, *count)?.into_iter());
                }
                let mut out = Vec::with_capacity(self.n);
                for (i, p) in present.iter().enumerate() {
                    if *p {
                        let t = tags.get(i) as usize;
                        let v = child_vals.get_mut(t).and_then(Iterator::next).ok_or_else(|| corrupt("variant rows"))?;
                        out.push(Some(v));
                    } else {
                        out.push(None);
                    }
                }
                Ok(out)
            }
            other => {
                let kind = body_kind(other);
                let vals = decode_body(kind, other, self.n)?;
                Ok(vals.into_iter().zip(present).map(|(v, p)| p.then_some(v)).collect())
            }
        }
    }
}

impl Body<'_> {
    /// Values of an `Int128` / `Decimal` mantissa body (`n` values).
    pub fn i128_values(b: &I128Body<'_>, n: usize) -> Vec<i128> {
        i128_values(b, n)
    }

    /// Values of a float body (`n` values).
    pub fn float_values(f: &FloatBody<'_>, n: usize) -> Vec<f64> {
        float_values(f, n)
    }
}

impl Val {
    /// The column value of a document value (`None` for null-like nothing; lists, maps and
    /// explicit nulls become `Nested`, as when segments are written).
    pub fn from_property(v: &PropertyValue) -> Option<Val> {
        match v {
            PropertyValue::List(_) | PropertyValue::Map(_) | PropertyValue::Null => {
                let mut sve = Vec::new();
                storage_codec::value_wire::encode_value(v, &mut sve);
                Some(Val::Nested(sve))
            }
            other => val_of_owned(other),
        }
    }
}

fn body_kind(b: &Body<'_>) -> Kind {
    match b {
        Body::AllNull | Body::Variant { .. } => Kind::Null,
        Body::Bool(_) => Kind::Bool,
        Body::Int(k, _) | Body::Bytes(k, _) => *k,
        Body::Float(_) => Kind::Float,
        Body::Int128(_) => Kind::Int128,
        Body::Decimal(..) => Kind::Decimal,
    }
}

fn parse_body<'a>(kind: Kind, r: &mut Reader<'a>, n: usize) -> Result<Body<'a>> {
    Ok(match kind {
        Kind::Null => return Err(corrupt("null body")),
        Kind::Bool => {
            let p = r.packed(n)?;
            if p.width() != 1 && n > 0 {
                return Err(corrupt("bool width"));
            }
            Body::Bool(p)
        }
        Kind::Int | Kind::Timestamp | Kind::UInt => Body::Int(kind, IntChunk::parse(r, n)?),
        Kind::Float => Body::Float(match r.u8()? {
            0 => FloatBody::Const(r.f64()?),
            1 => FloatBody::Plain(r.take(n.checked_mul(8).ok_or_else(|| corrupt("float size"))?)?),
            2 => {
                let d = r.u32()? as usize;
                if d == 0 || d > 4096 {
                    return Err(corrupt("float dictionary"));
                }
                let mut dict = Vec::with_capacity(d);
                for _ in 0..d {
                    dict.push(r.f64()?);
                }
                FloatBody::Dict(dict, r.packed(n)?)
            }
            3 => FloatBody::Integral(IntChunk::parse(r, n)?),
            _ => return Err(corrupt("float encoding")),
        }),
        Kind::Int128 => Body::Int128(parse_i128(r, n)?),
        Kind::Decimal => {
            let m = parse_i128(r, n)?;
            Body::Decimal(m, IntChunk::parse(r, n)?)
        }
        Kind::Uuid | Kind::Id | Kind::Text | Kind::Bytes | Kind::Nested => Body::Bytes(kind, parse_bytes(r, n)?),
    })
}

fn parse_i128<'a>(r: &mut Reader<'a>, n: usize) -> Result<I128Body<'a>> {
    Ok(match r.u8()? {
        0 => I128Body::Small(IntChunk::parse(r, n)?),
        1 => I128Body::Plain(r.take(n.checked_mul(16).ok_or_else(|| corrupt("int128 size"))?)?),
        _ => return Err(corrupt("int128 encoding")),
    })
}

fn parse_blob<'a>(r: &mut Reader<'a>) -> Result<Blob<'a>> {
    let compressed = r.u8()?;
    let raw_len = r.u32()? as usize;
    let stored = r.u32()? as usize;
    let bytes = r.take(stored)?;
    match compressed {
        0 if stored == raw_len => Ok(Blob::Borrowed(bytes)),
        1 => Ok(Blob::Owned(decompress_lz4(bytes, raw_len).map_err(|_| corrupt("compressed text block"))?)),
        _ => Err(corrupt("blob header")),
    }
}

fn parse_offsets(r: &mut Reader<'_>, count: usize) -> Result<Vec<i64>> {
    let c = IntChunk::parse(r, count)?;
    let mut offsets = vec![0i64; count];
    c.decode_into(0, &mut offsets);
    if offsets.first() != Some(&0) || offsets.windows(2).any(|w| w[1] < w[0]) {
        return Err(corrupt("offsets"));
    }
    Ok(offsets)
}

fn parse_bytes<'a>(r: &mut Reader<'a>, n: usize) -> Result<BytesBody<'a>> {
    Ok(match r.u8()? {
        0 => BytesBody::Const(r.bytes()?),
        1 => {
            let d = r.u32()? as usize;
            if d == 0 || d > n.max(1) {
                return Err(corrupt("bytes dictionary"));
            }
            let offsets = parse_offsets(r, d + 1)?;
            let blob = parse_blob(r)?;
            if offsets[d] as usize != blob.bytes().len() {
                return Err(corrupt("dictionary blob length"));
            }
            BytesBody::Dict { offsets, blob, codes: r.packed(n)? }
        }
        2 => {
            let offsets = parse_offsets(r, n + 1)?;
            let blob = parse_blob(r)?;
            if offsets[n] as usize != blob.bytes().len() {
                return Err(corrupt("blob length"));
            }
            BytesBody::Plain { offsets, blob }
        }
        _ => return Err(corrupt("bytes encoding")),
    })
}

impl BytesBody<'_> {
    /// Bytes of value `i` (empty for a hostile out-of-range code).
    pub fn get(&self, i: usize) -> &[u8] {
        fn slice<'b>(offsets: &[i64], blob: &'b [u8], j: usize) -> &'b [u8] {
            match (offsets.get(j), offsets.get(j + 1)) {
                (Some(&a), Some(&b)) => blob.get(a as usize..b as usize).unwrap_or_default(),
                _ => &[],
            }
        }
        match self {
            Self::Const(b) => b,
            Self::Dict { offsets, blob, codes } => slice(offsets, blob.bytes(), codes.get(i) as usize),
            Self::Plain { offsets, blob } => slice(offsets, blob.bytes(), i),
        }
    }
}

fn i128_values(b: &I128Body<'_>, n: usize) -> Vec<i128> {
    match b {
        I128Body::Small(c) => {
            let mut v = vec![0i64; n];
            c.decode_into(0, &mut v);
            v.into_iter().map(i128::from).collect()
        }
        I128Body::Plain(bytes) => bytes
            .chunks_exact(16)
            .map(|c| {
                let mut a = [0u8; 16];
                a.copy_from_slice(c);
                i128::from_le_bytes(a)
            })
            .collect(),
    }
}

/// The `n` values of a body whose rows all have `kind`.
fn decode_body(kind: Kind, b: &Body<'_>, n: usize) -> Result<Vec<Val>> {
    Ok(match b {
        Body::AllNull | Body::Variant { .. } => return Err(corrupt("nested variant")),
        Body::Bool(p) => (0..n).map(|i| Val::Bool(p.get(i) == 1)).collect(),
        Body::Int(k, c) => {
            let mut v = vec![0i64; n];
            c.decode_into(0, &mut v);
            v.into_iter()
                .map(|x| match k {
                    Kind::UInt => Val::UInt(x as u64 ^ (1 << 63)),
                    Kind::Timestamp => Val::Timestamp(x),
                    _ => Val::Int(x),
                })
                .collect()
        }
        Body::Float(f) => float_values(f, n).into_iter().map(Val::Float).collect(),
        Body::Int128(m) => i128_values(m, n).into_iter().map(Val::Int128).collect(),
        Body::Decimal(m, s) => {
            let ms = i128_values(m, n);
            let mut sc = vec![0i64; n];
            s.decode_into(0, &mut sc);
            ms.into_iter().zip(sc).map(|(m, s)| Val::Decimal(m, s as u8)).collect()
        }
        Body::Bytes(_, bb) => (0..n)
            .map(|i| {
                let raw = bb.get(i);
                match kind {
                    Kind::Text => Val::Text(String::from_utf8_lossy(raw).into_owned()),
                    Kind::Uuid => {
                        let mut u = [0u8; 16];
                        if raw.len() == 16 {
                            u.copy_from_slice(raw);
                        }
                        Val::Uuid(u)
                    }
                    Kind::Id => {
                        let mut u = [0u8; 24];
                        if raw.len() == 24 {
                            u.copy_from_slice(raw);
                        }
                        Val::Id(u)
                    }
                    Kind::Nested => Val::Nested(raw.to_vec()),
                    _ => Val::Bytes(raw.to_vec()),
                }
            })
            .collect(),
    })
}

fn float_values(f: &FloatBody<'_>, n: usize) -> Vec<f64> {
    match f {
        FloatBody::Const(v) => vec![*v; n],
        FloatBody::Plain(b) => b
            .chunks_exact(8)
            .map(|c| f64::from_bits(u64::from_le_bytes([c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]])))
            .collect(),
        FloatBody::Dict(d, codes) => (0..n).map(|i| d.get(codes.get(i) as usize).copied().unwrap_or(0.0)).collect(),
        FloatBody::Integral(c) => {
            let mut v = vec![0i64; n];
            c.decode_into(0, &mut v);
            v.into_iter().map(|x| x as f64).collect()
        }
    }
}
