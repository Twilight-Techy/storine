//! Exact filter evaluation on column chunks.
//!
//! Every leaf is decided by [`Filter::matches_value`], the function document queries use:
//! - dictionary chunks: the leaf runs once per dictionary entry, rows take their code's answer;
//! - ordered chunks (integers, timestamps, unsigned, small decimals of one scale, floats by
//!   their total-order key): the rows a convex leaf (`=`, `<`, `<=`, `>`, `>=`, `BETWEEN`)
//!   selects form one interval of encoded values. Its exact ends are found by binary search with
//!   `matches_value` itself, then a vectorized range kernel marks the rows. `!=` is the
//!   complement of `=`, `IN` the union of `=`;
//! - anything else (mixed kinds, nested values, map-valued paths): row by row.
//!
//! Absent and null values get `matches_value(Null)`.

use std::cell::OnceCell;
use std::collections::BTreeMap;

use storage_codec::simd;
use storage_codec::types::{PropertyValue, PropertyValueRef};
use storage_columnar::column::{BytesBody, FloatBody, I128Body};
use storage_columnar::int::{IntChunk, BLOCK};
use storage_columnar::{Body, Chunk, Kind, Val};
use storage_types::{Decimal, FastHashMap, InlineId};

use crate::document::{compare_values, Filter};

use super::values::with_ref;

/// One row group as the executor sees it.
pub(crate) struct RowGroupView<'a> {
    pub n: usize,
    /// Parsed chunks by column name (absent: no value in this row group).
    pub chunks: FastHashMap<&'a str, Chunk<'a>>,
    /// Every column name of the segment (to find map-valued paths).
    pub names: &'a [String],
    /// Row-wise decoded columns, built on first use (slow paths).
    pub decoded: FastHashMap<&'a str, OnceCell<Vec<Option<Val>>>>,
    /// Row keys (`$key`), decoded on first use.
    pub keys: OnceCell<Vec<String>>,
}

impl<'a> RowGroupView<'a> {
    /// Whether some column is nested under `path` (the path holds a map in some rows).
    pub fn has_children(&self, path: &str) -> bool {
        self.names.iter().any(|n| n.len() > path.len() && n.starts_with(path) && n.as_bytes()[path.len()] == b'.')
    }

    pub fn vals(&self, path: &str) -> Option<&Vec<Option<Val>>> {
        let chunk = self.chunks.get(path)?;
        let cell = self.decoded.get(path)?;
        Some(cell.get_or_init(|| chunk.decode_all().unwrap_or_else(|_| vec![None; self.n])))
    }

    pub fn row_keys(&self) -> &Vec<String> {
        self.keys.get_or_init(|| match self.vals(storage_columnar::segment::KEY_NAME) {
            Some(v) => v
                .iter()
                .map(|k| match k {
                    Some(Val::Text(s)) => s.clone(),
                    _ => String::new(),
                })
                .collect(),
            None => vec![String::new(); self.n],
        })
    }

    /// The document value at `path` in `row`, rebuilding a map from nested columns.
    pub fn value_at(&self, path: &str, row: usize) -> PropertyValue {
        if let Some(Some(v)) = self.vals(path).and_then(|v| v.get(row)) {
            return v.to_property();
        }
        if !self.has_children(path) {
            return PropertyValue::Null;
        }
        let mut map = BTreeMap::new();
        let prefix_len = path.len() + 1;
        for name in self.names {
            if name.len() > path.len() && name.starts_with(path) && name.as_bytes()[path.len()] == b'.' {
                if let Some(Some(v)) = self.vals(name).and_then(|v| v.get(row)) {
                    insert_path(&mut map, &name[prefix_len..], v.to_property());
                }
            }
        }
        if map.is_empty() {
            PropertyValue::Null
        } else {
            PropertyValue::Map(map)
        }
    }
}

fn insert_path(map: &mut BTreeMap<String, PropertyValue>, path: &str, v: PropertyValue) {
    match path.split_once('.') {
        None => {
            map.insert(path.to_owned(), v);
        }
        Some((head, rest)) => {
            let slot = map.entry(head.to_owned()).or_insert_with(|| PropertyValue::Map(BTreeMap::new()));
            if !matches!(slot, PropertyValue::Map(_)) {
                *slot = PropertyValue::Map(BTreeMap::new());
            }
            if let PropertyValue::Map(m) = slot {
                insert_path(m, rest, v);
            }
        }
    }
}

#[inline]
fn set_bit(out: &mut [u64], i: usize) {
    out[i / 64] |= 1 << (i % 64);
}

fn fill(out: &mut [u64], n: usize, value: bool) {
    out.fill(if value { u64::MAX } else { 0 });
    if value && n % 64 != 0 {
        if let Some(last) = out.get_mut(n / 64) {
            *last = (1u64 << (n % 64)) - 1;
        }
    }
}

/// Bits of rows matching `f` (`out` has `ceil(n / 64)` words).
pub(crate) fn eval(f: &Filter, rg: &RowGroupView<'_>, out: &mut [u64]) {
    let n = rg.n;
    match f {
        Filter::And(fs) => {
            fill(out, n, true);
            let mut tmp = vec![0u64; out.len()];
            for x in fs {
                eval(x, rg, &mut tmp);
                simd::and_words(out, &tmp);
            }
        }
        Filter::Or(fs) => {
            fill(out, n, false);
            let mut tmp = vec![0u64; out.len()];
            for x in fs {
                eval(x, rg, &mut tmp);
                simd::or_words(out, &tmp);
            }
        }
        Filter::Not(x) => {
            eval(x, rg, out);
            let mut all = vec![0u64; out.len()];
            fill(&mut all, n, true);
            for (o, a) in out.iter_mut().zip(&all) {
                *o = !*o & a;
            }
        }
        Filter::KeyPrefix(prefix) => {
            out.fill(0);
            for (i, k) in rg.row_keys().iter().enumerate() {
                if k.starts_with(prefix.as_str()) {
                    set_bit(out, i);
                }
            }
        }
        leaf => eval_leaf(leaf, rg, out),
    }
}

/// Row-by-row: `matches_value` on each row's value (exact for every kind).
fn rows_slow(f: &Filter, rg: &RowGroupView<'_>, path: &str, out: &mut [u64]) {
    out.fill(0);
    for i in 0..rg.n {
        let v = rg.value_at(path, i);
        if with_ref(&v, |r| f.matches_value(r)) {
            set_bit(out, i);
        }
    }
}

fn eval_leaf(f: &Filter, rg: &RowGroupView<'_>, out: &mut [u64]) {
    let n = rg.n;
    let Some(path) = f.path() else {
        fill(out, n, false);
        return;
    };
    let null_result = f.matches_value(PropertyValueRef::Null);
    // A path that holds a map in some rows, or a probe that is itself a list or map.
    let complex_probe = match f {
        Filter::Eq(_, v) | Filter::Ne(_, v) | Filter::Contains(_, v) => matches!(v, PropertyValue::List(_) | PropertyValue::Map(_)),
        Filter::In(_, vs) => vs.iter().any(|v| matches!(v, PropertyValue::List(_) | PropertyValue::Map(_))),
        _ => false,
    };
    if rg.has_children(path) || complex_probe {
        rows_slow(f, rg, path, out);
        return;
    }
    let Some(chunk) = rg.chunks.get(path) else {
        fill(out, n, null_result);
        return;
    };
    let decided = match &chunk.body {
        Body::AllNull => {
            fill(out, n, null_result);
            return;
        }
        Body::Bool(p) => {
            let (t, fl) = (f.matches_value(PropertyValueRef::Bool(true)), f.matches_value(PropertyValueRef::Bool(false)));
            out.fill(0);
            for i in 0..n {
                if if p.get(i) == 1 { t } else { fl } {
                    set_bit(out, i);
                }
            }
            true
        }
        Body::Int(kind, ic) => {
            let ctor = |x: i64| -> PropertyValue {
                match kind {
                    Kind::UInt => PropertyValue::UInt64(x as u64 ^ (1 << 63)),
                    Kind::Timestamp => PropertyValue::TimestampNs(x),
                    _ => PropertyValue::Int64(x),
                }
            };
            int_leaf(f, ic, n, &ctor, out)
        }
        Body::Int128(I128Body::Small(ic)) => int_leaf(f, ic, n, &|x| PropertyValue::Int128(i128::from(x)), out),
        Body::Decimal(I128Body::Small(ic), scales) => match scales {
            IntChunk::Const { value, .. } if (0..=38).contains(value) => {
                let s = *value as u8;
                int_leaf(f, ic, n, &|x| Decimal::new(i128::from(x), s).map_or(PropertyValue::Null, PropertyValue::Decimal), out)
            }
            _ => false,
        },
        Body::Float(fb) => float_leaf(f, fb, n, out),
        Body::Bytes(kind, bb) => bytes_leaf(f, *kind, bb, n, out),
        _ => false,
    };
    if !decided {
        rows_slow(f, rg, path, out);
        return;
    }
    // Rows without a value take the null answer.
    if let Some(v) = &chunk.validity {
        for i in 0..n {
            if v.get(i) == 0 {
                if null_result {
                    set_bit(out, i);
                } else {
                    out[i / 64] &= !(1 << (i % 64));
                }
            }
        }
    }
}

/// The probe constants of a convex leaf.
fn probes(f: &Filter) -> Vec<&PropertyValue> {
    match f {
        Filter::Eq(_, v) | Filter::Lt(_, v) | Filter::Le(_, v) | Filter::Gt(_, v) | Filter::Ge(_, v) => vec![v],
        Filter::Between(_, a, b) => vec![a, b],
        _ => Vec::new(),
    }
}

/// The interval of `[a, b]` (in the order of `ctor`) where the convex leaf `f` holds.
fn convex_interval(f: &Filter, a: i64, b: i64, ctor: &dyn Fn(i64) -> PropertyValue) -> Option<(i64, i64)> {
    let pred = |x: i64| with_ref(&ctor(x), |r| f.matches_value(r));
    let mut cands = vec![a, b];
    for c in probes(f) {
        // Smallest x with ctor(x) >= c.
        let (mut lo, mut hi) = (a, b);
        let ge = |x: i64| with_ref(&ctor(x), |r| with_ref(c, |cr| compare_values(&r, &cr) != core::cmp::Ordering::Less));
        if !ge(hi) {
            continue;
        }
        while lo < hi {
            let mid = lo + ((i128::from(hi) - i128::from(lo)) / 2) as i64;
            if ge(mid) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        cands.push(lo);
        if lo > a {
            cands.push(lo - 1);
        }
    }
    let t = cands.into_iter().find(|&x| pred(x))?;
    // First true in [a, t] (false .. false true .. true), last true in [t, b].
    let (mut lo, mut hi) = (a, t);
    while lo < hi {
        let mid = lo + ((i128::from(hi) - i128::from(lo)) / 2) as i64;
        if pred(mid) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    let first = lo;
    let (mut lo, mut hi) = (t, b);
    while lo < hi {
        let mid = lo + ((i128::from(hi) - i128::from(lo) + 1) / 2) as i64;
        if pred(mid) {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    Some((first, lo))
}

/// Marks rows of `vals` matching `f` given the `ctor` order of the domain. `false` if the leaf
/// is not handled here.
fn ordered_rows(f: &Filter, vals: &[i64], ctor: &dyn Fn(i64) -> PropertyValue, out: &mut [u64]) -> bool {
    let n = vals.len();
    let (Some(&a), Some(&b)) = (vals.iter().min(), vals.iter().max()) else {
        out.fill(0);
        return true;
    };
    let mark = |f: &Filter, out: &mut [u64]| match convex_interval(f, a, b, ctor) {
        Some((lo, hi)) => simd::range_i64(vals, lo, hi, out),
        None => out.fill(0),
    };
    match f {
        Filter::Eq(..) | Filter::Lt(..) | Filter::Le(..) | Filter::Gt(..) | Filter::Ge(..) | Filter::Between(..) => mark(f, out),
        Filter::Ne(p, v) => {
            mark(&Filter::Eq(p.clone(), v.clone()), out);
            let mut all = vec![0u64; out.len()];
            fill(&mut all, n, true);
            for (o, x) in out.iter_mut().zip(&all) {
                *o = !*o & x;
            }
        }
        Filter::In(p, vs) => {
            out.fill(0);
            let mut tmp = vec![0u64; out.len()];
            for v in vs {
                mark(&Filter::Eq(p.clone(), v.clone()), &mut tmp);
                simd::or_words(out, &tmp);
            }
        }
        // Constant over a single-kind chunk: one evaluation decides every row.
        other => {
            let v = with_ref(&ctor(a), |r| other.matches_value(r));
            fill(out, n, v);
        }
    }
    true
}

fn int_leaf(f: &Filter, ic: &IntChunk<'_>, n: usize, ctor: &dyn Fn(i64) -> PropertyValue, out: &mut [u64]) -> bool {
    match ic {
        IntChunk::Const { value, .. } => {
            let v = with_ref(&ctor(*value), |r| f.matches_value(r));
            fill(out, n, v);
            true
        }
        IntChunk::Dict { dict, codes } => {
            let allowed: Vec<bool> = dict.iter().map(|&x| with_ref(&ctor(x), |r| f.matches_value(r))).collect();
            out.fill(0);
            let mut buf = [0u64; BLOCK];
            let mut i = 0;
            while i < n {
                let k = (n - i).min(BLOCK);
                codes.unpack_into(i, &mut buf[..k]);
                for (j, &c) in buf[..k].iter().enumerate() {
                    if allowed.get(c as usize).copied().unwrap_or(false) {
                        set_bit(out, i + j);
                    }
                }
                i += k;
            }
            true
        }
        _ => {
            let mut vals = vec![0i64; n];
            ic.decode_into(0, &mut vals);
            ordered_rows(f, &vals, ctor, out)
        }
    }
}

/// Order-preserving key of a float in the total order (`-NaN < -inf < ... < inf < NaN`).
#[inline]
fn float_order_key(f: f64) -> i64 {
    let b = f.to_bits() as i64;
    b ^ (((b >> 63) as u64) >> 1) as i64
}

#[inline]
fn float_from_key(k: i64) -> f64 {
    f64::from_bits((k ^ (((k >> 63) as u64) >> 1) as i64) as u64)
}

fn float_leaf(f: &Filter, fb: &FloatBody<'_>, n: usize, out: &mut [u64]) -> bool {
    let vals = Body::float_values(fb, n);
    // compare_values orders floats by `total_cmp`, which is exactly this key's order.
    let keys: Vec<i64> = vals.iter().map(|&v| float_order_key(v)).collect();
    ordered_rows(f, &keys, &|k| PropertyValue::Float64(float_from_key(k)), out)
}

fn bytes_value(kind: Kind, raw: &[u8]) -> PropertyValue {
    match kind {
        Kind::Text => PropertyValue::Text(String::from_utf8_lossy(raw).into_owned()),
        Kind::Uuid => {
            let mut u = [0u8; 16];
            if raw.len() == 16 {
                u.copy_from_slice(raw);
            }
            PropertyValue::Uuid(u)
        }
        Kind::Id => {
            let mut u = [0u8; 24];
            if raw.len() == 24 {
                u.copy_from_slice(raw);
            }
            InlineId::from_raw_bytes(u).map_or(PropertyValue::Null, PropertyValue::Id)
        }
        Kind::Nested => storage_codec::value_wire::SveReader::new(raw).value().unwrap_or(PropertyValue::Null),
        _ => PropertyValue::Bytes(raw.to_vec()),
    }
}

fn bytes_leaf(f: &Filter, kind: Kind, bb: &BytesBody<'_>, n: usize, out: &mut [u64]) -> bool {
    let test = |raw: &[u8]| -> bool {
        // Text compares and matches without allocating in the common case.
        if kind == Kind::Text {
            if let Ok(s) = core::str::from_utf8(raw) {
                return f.matches_value(PropertyValueRef::Text(s));
            }
        }
        if kind == Kind::Bytes {
            return f.matches_value(PropertyValueRef::Bytes(raw));
        }
        with_ref(&bytes_value(kind, raw), |r| f.matches_value(r))
    };
    match bb {
        BytesBody::Const(raw) => {
            let v = test(raw);
            fill(out, n, v);
        }
        BytesBody::Dict { codes, offsets, blob } => {
            let entries = offsets.len().saturating_sub(1);
            let allowed: Vec<bool> = (0..entries)
                .map(|e| {
                    let (a, b) = (offsets[e] as usize, offsets[e + 1] as usize);
                    test(blob.bytes().get(a..b).unwrap_or_default())
                })
                .collect();
            out.fill(0);
            let mut buf = [0u64; BLOCK];
            let mut i = 0;
            while i < n {
                let k = (n - i).min(BLOCK);
                codes.unpack_into(i, &mut buf[..k]);
                for (j, &c) in buf[..k].iter().enumerate() {
                    if allowed.get(c as usize).copied().unwrap_or(false) {
                        set_bit(out, i + j);
                    }
                }
                i += k;
            }
        }
        BytesBody::Plain { .. } => {
            out.fill(0);
            for i in 0..n {
                if test(bb.get(i)) {
                    set_bit(out, i);
                }
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_keys_preserve_the_total_order() {
        let vals = [f64::NEG_INFINITY, -1e300, -1.0, -0.0, 0.0, 1e-300, 1.0, f64::INFINITY, f64::NAN];
        for w in vals.windows(2) {
            assert!(float_order_key(w[0]) < float_order_key(w[1]), "{w:?}");
            assert_eq!(w[0].total_cmp(&w[1]), core::cmp::Ordering::Less);
        }
        for v in vals {
            assert_eq!(float_from_key(float_order_key(v)).to_bits(), v.to_bits());
        }
    }
}
