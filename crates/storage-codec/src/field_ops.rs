//! Atomic field operations (`TASK-095`, `F-CONC-08`): server-side read-modify-write on dynamic
//! records, so clients never read a balance, change it locally and write it back (the
//! lost-update race).
//!
//! An operation addresses a field by a dotted `path` (`"balance"`, `"limits.daily"`). All the
//! operations of one request are applied in order to a private copy of the record, under the
//! record's write lock; if any fails (type mismatch, overflow, bound violated), nothing is written.

use std::collections::BTreeMap;

use storage_types::{Decimal, Result, RoundingMode, StorageError};

use crate::types::{PropertyValue, MAX_VALUE_DEPTH};
use crate::value_wire::SveReader;

/// One atomic change to a record field.
#[derive(Clone, Debug, PartialEq)]
pub enum FieldOp {
    /// Sets `path` to `value` (creating intermediate maps).
    Set { path: String, value: PropertyValue },
    /// Removes `path` (no-op if absent).
    Unset { path: String },
    /// Adds `delta` to the number at `path` (a missing field counts as zero) with checked
    /// arithmetic; fails if the result is below `min` or above `max` (e.g. `min = 0` forbids
    /// an overdraft).
    Add {
        path: String,
        delta: PropertyValue,
        min: Option<PropertyValue>,
        max: Option<PropertyValue>,
    },
    /// Appends `value` to the list at `path` (a missing field becomes a one-element list).
    Append { path: String, value: PropertyValue },
}

const OP_SET: u8 = 1;
const OP_UNSET: u8 = 2;
const OP_ADD: u8 = 3;
const OP_APPEND: u8 = 4;

impl FieldOp {
    pub fn path(&self) -> &str {
        match self {
            Self::Set { path, .. }
            | Self::Unset { path }
            | Self::Add { path, .. }
            | Self::Append { path, .. } => path,
        }
    }

    /// Wire encoding (`protocol/PROTOCOL.md` §5): `kind u8 | u32 path len | path | ...`.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let push_path = |out: &mut Vec<u8>, p: &str| {
            out.extend_from_slice(&(p.len() as u32).to_le_bytes());
            out.extend_from_slice(p.as_bytes());
        };
        match self {
            Self::Set { path, value } => {
                out.push(OP_SET);
                push_path(out, path);
                crate::value_wire::encode_value(value, out);
            }
            Self::Unset { path } => {
                out.push(OP_UNSET);
                push_path(out, path);
            }
            Self::Add { path, delta, min, max } => {
                out.push(OP_ADD);
                push_path(out, path);
                crate::value_wire::encode_value(delta, out);
                let flags = u8::from(min.is_some()) | (u8::from(max.is_some()) << 1);
                out.push(flags);
                if let Some(m) = min {
                    crate::value_wire::encode_value(m, out);
                }
                if let Some(m) = max {
                    crate::value_wire::encode_value(m, out);
                }
            }
            Self::Append { path, value } => {
                out.push(OP_APPEND);
                push_path(out, path);
                crate::value_wire::encode_value(value, out);
            }
        }
    }

    /// Decodes one operation from untrusted bytes.
    pub fn decode(r: &mut SveReader<'_>) -> Result<Self> {
        let kind = r.u8()?;
        let path = r.str()?.to_owned();
        validate_path(&path)?;
        Ok(match kind {
            OP_SET => Self::Set { path, value: r.value()? },
            OP_UNSET => Self::Unset { path },
            OP_ADD => {
                let delta = r.value()?;
                let flags = r.u8()?;
                if flags > 3 {
                    return Err(StorageError::invalid_value("unknown add flags"));
                }
                let min = if flags & 1 != 0 { Some(r.value()?) } else { None };
                let max = if flags & 2 != 0 { Some(r.value()?) } else { None };
                Self::Add { path, delta, min, max }
            }
            OP_APPEND => Self::Append { path, value: r.value()? },
            _ => return Err(StorageError::invalid_value("unknown field operation")),
        })
    }
}

/// Encodes a list of operations: `u32 count | op*`.
pub fn encode_field_ops(ops: &[FieldOp], out: &mut Vec<u8>) {
    out.extend_from_slice(&(ops.len() as u32).to_le_bytes());
    for op in ops {
        op.encode(out);
    }
}

/// Decodes `u32 count | op*`.
pub fn decode_field_ops(r: &mut SveReader<'_>) -> Result<Vec<FieldOp>> {
    let n = r.count(6)?;
    let mut ops = Vec::with_capacity(n);
    for _ in 0..n {
        ops.push(FieldOp::decode(r)?);
    }
    Ok(ops)
}

/// Rejects empty paths, empty segments and paths deeper than the value nesting limit.
pub fn validate_path(path: &str) -> Result<()> {
    if path.is_empty() || path.split('.').any(str::is_empty) {
        return Err(StorageError::invalid_value("field path must be non-empty dot-separated names"));
    }
    if path.split('.').count() > MAX_VALUE_DEPTH {
        return Err(StorageError::invalid_value("field path is too deep"));
    }
    Ok(())
}

/// Reads the value at a dotted `path`.
pub fn get_path<'a>(fields: &'a BTreeMap<String, PropertyValue>, path: &str) -> Option<&'a PropertyValue> {
    let mut parts = path.split('.');
    let mut cur = fields.get(parts.next()?)?;
    for part in parts {
        match cur {
            PropertyValue::Map(m) => cur = m.get(part)?,
            _ => return None,
        }
    }
    Some(cur)
}

/// Mutable slot for `path`, creating intermediate maps; fails if a non-map is in the way.
fn slot_mut<'a>(
    fields: &'a mut BTreeMap<String, PropertyValue>,
    path: &str,
) -> Result<(&'a mut BTreeMap<String, PropertyValue>, String)> {
    let mut parts: Vec<&str> = path.split('.').collect();
    let last = parts.pop().unwrap_or_default().to_owned();
    let mut cur = fields;
    for part in parts {
        let entry = cur
            .entry(part.to_owned())
            .or_insert_with(|| PropertyValue::Map(BTreeMap::new()));
        match entry {
            PropertyValue::Map(m) => cur = m,
            _ => {
                return Err(StorageError::invalid_value(
                    "field path goes through a value that is not a map",
                ))
            }
        }
    }
    Ok((cur, last))
}

/// Removes the value at a dotted `path` if present.
fn unset_path(fields: &mut BTreeMap<String, PropertyValue>, path: &str) {
    match path.split_once('.') {
        None => {
            fields.remove(path);
        }
        Some((head, rest)) => {
            if let Some(PropertyValue::Map(m)) = fields.get_mut(head) {
                unset_path(m, rest);
            }
        }
    }
}

/// Applies `ops` in order to `fields`. On error `fields` may be partially modified: callers
/// apply to a private copy and discard it on failure (the engine does).
pub fn apply_field_ops(fields: &mut BTreeMap<String, PropertyValue>, ops: &[FieldOp]) -> Result<()> {
    for op in ops {
        validate_path(op.path())?;
        match op {
            FieldOp::Set { path, value } => {
                let (map, key) = slot_mut(fields, path)?;
                map.insert(key, value.clone());
            }
            FieldOp::Unset { path } => {
                unset_path(fields, path);
            }
            FieldOp::Add { path, delta, min, max } => {
                let (map, key) = slot_mut(fields, path)?;
                let current = map.get(&key).cloned().unwrap_or(PropertyValue::Null);
                let next = add_values(&current, delta)?;
                if let Some(lo) = min {
                    if compare_numbers(&next, lo)? == core::cmp::Ordering::Less {
                        return Err(StorageError::invalid_value("result would be below the allowed minimum"));
                    }
                }
                if let Some(hi) = max {
                    if compare_numbers(&next, hi)? == core::cmp::Ordering::Greater {
                        return Err(StorageError::invalid_value("result would be above the allowed maximum"));
                    }
                }
                map.insert(key, next);
            }
            FieldOp::Append { path, value } => {
                let (map, key) = slot_mut(fields, path)?;
                match map.entry(key).or_insert_with(|| PropertyValue::List(Vec::new())) {
                    PropertyValue::List(items) => items.push(value.clone()),
                    _ => return Err(StorageError::invalid_value("append target is not a list")),
                }
            }
        }
    }
    Ok(())
}

/// Exact decimal view of an integer or decimal value (`None` for floats and non-numbers).
pub fn as_exact_decimal(v: &PropertyValue) -> Option<Decimal> {
    match *v {
        PropertyValue::Int64(x) => Some(Decimal::from_i64(x)),
        PropertyValue::UInt64(x) => Some(Decimal::from_i128(i128::from(x))),
        PropertyValue::Int128(x) => Some(Decimal::from_i128(x)),
        PropertyValue::Decimal(d) => Some(d),
        _ => None,
    }
}

#[cold]
#[inline(never)]
fn not_a_number() -> StorageError {
    StorageError::invalid_value("add needs numbers (int64, uint64, int128, decimal or float64)")
}

#[cold]
#[inline(never)]
fn overflow() -> StorageError {
    StorageError::invalid_value("numeric overflow: the result does not fit the field's type")
}

/// Checked `current + delta`. A missing (`Null`) current value counts as zero of `delta`'s type.
/// Integers keep their type (overflow is an error); mixing with a decimal yields a decimal;
/// floats only combine with numbers when either side is a float.
pub fn add_values(current: &PropertyValue, delta: &PropertyValue) -> Result<PropertyValue> {
    use PropertyValue as V;
    Ok(match (current, delta) {
        (V::Null, d) => match d {
            V::Int64(_) | V::UInt64(_) | V::Int128(_) | V::Decimal(_) | V::Float64(_) => d.clone(),
            _ => return Err(not_a_number()),
        },
        (V::Int64(a), V::Int64(b)) => V::Int64(a.checked_add(*b).ok_or_else(overflow)?),
        (V::UInt64(a), V::UInt64(b)) => V::UInt64(a.checked_add(*b).ok_or_else(overflow)?),
        (V::UInt64(a), V::Int64(b)) => {
            let r = i128::from(*a) + i128::from(*b);
            V::UInt64(u64::try_from(r).map_err(|_| overflow())?)
        }
        (V::Int64(a), V::UInt64(b)) => {
            let r = i128::from(*a) + i128::from(*b);
            V::Int64(i64::try_from(r).map_err(|_| overflow())?)
        }
        (V::Int128(a), b) if as_exact_decimal(b).is_some_and(|d| d.scale() == 0) => {
            let b = as_exact_decimal(b).ok_or_else(not_a_number)?.to_i128_exact()?;
            V::Int128(a.checked_add(b).ok_or_else(overflow)?)
        }
        (a, V::Int128(b)) if matches!(a, V::Int64(_) | V::UInt64(_)) => {
            let a = as_exact_decimal(a).ok_or_else(not_a_number)?.to_i128_exact()?;
            V::Int128(a.checked_add(*b).ok_or_else(overflow)?)
        }
        (V::Float64(a), b) => V::Float64(a + as_f64(b)?),
        (a, V::Float64(b)) => V::Float64(as_f64(a)? + b),
        (a, b) => {
            let (Some(x), Some(y)) = (as_exact_decimal(a), as_exact_decimal(b)) else {
                return Err(not_a_number());
            };
            V::Decimal(x.checked_add(y)?)
        }
    })
}

fn as_f64(v: &PropertyValue) -> Result<f64> {
    match *v {
        PropertyValue::Float64(f) => Ok(f),
        _ => as_exact_decimal(v).map(Decimal::to_f64_lossy).ok_or_else(not_a_number),
    }
}

/// Numeric comparison across numeric types (exact for integers and decimals).
pub fn compare_numbers(a: &PropertyValue, b: &PropertyValue) -> Result<core::cmp::Ordering> {
    if let (Some(x), Some(y)) = (as_exact_decimal(a), as_exact_decimal(b)) {
        return Ok(x.cmp(&y));
    }
    let (x, y) = (as_f64(a)?, as_f64(b)?);
    x.partial_cmp(&y).ok_or_else(|| StorageError::invalid_value("cannot compare NaN"))
}

/// Rounds a decimal field to `scale` (helper for money columns).
pub fn round_decimal(v: &PropertyValue, scale: u8, mode: RoundingMode) -> Result<PropertyValue> {
    match v {
        PropertyValue::Decimal(d) => Ok(PropertyValue::Decimal(d.rescale(scale, mode)?)),
        other => Ok(other.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> PropertyValue {
        PropertyValue::Decimal(s.parse().unwrap())
    }

    #[test]
    fn test_add_with_bounds_prevents_overdraft_and_overflow() {
        let mut f = BTreeMap::new();
        f.insert("balance".into(), dec("100.00"));
        let withdraw = |amt: &str| FieldOp::Add {
            path: "balance".into(),
            delta: dec(&format!("-{amt}")),
            min: Some(PropertyValue::Int64(0)),
            max: None,
        };
        apply_field_ops(&mut f, &[withdraw("60.00")]).unwrap();
        assert_eq!(f["balance"], dec("40.00"));
        let mut copy = f.clone();
        assert!(apply_field_ops(&mut copy, &[withdraw("40.01")]).is_err());
        assert_eq!(f["balance"], dec("40.00"));
        let mut g = BTreeMap::new();
        g.insert("n".into(), PropertyValue::Int64(i64::MAX));
        let op = FieldOp::Add { path: "n".into(), delta: PropertyValue::Int64(1), min: None, max: None };
        assert!(apply_field_ops(&mut g, &[op]).is_err());
        // Missing field counts as zero; nested paths create maps.
        let mut h = BTreeMap::new();
        let op = FieldOp::Add { path: "stats.views".into(), delta: PropertyValue::Int64(1), min: None, max: None };
        apply_field_ops(&mut h, &[op.clone(), op]).unwrap();
        assert_eq!(get_path(&h, "stats.views"), Some(&PropertyValue::Int64(2)));
        // Integer + decimal promotes to decimal exactly.
        assert_eq!(add_values(&PropertyValue::Int64(1), &dec("0.10")).unwrap(), dec("1.10"));
        assert!(add_values(&PropertyValue::Text("x".into()), &PropertyValue::Int64(1)).is_err());
    }

    #[test]
    fn test_set_unset_append_and_wire_roundtrip() {
        let ops = vec![
            FieldOp::Set { path: "a.b".into(), value: PropertyValue::Text("x".into()) },
            FieldOp::Append { path: "log".into(), value: PropertyValue::Int64(1) },
            FieldOp::Append { path: "log".into(), value: PropertyValue::Int64(2) },
            FieldOp::Unset { path: "a.b".into() },
            FieldOp::Add { path: "c".into(), delta: dec("1.5"), min: Some(dec("0")), max: Some(dec("10")) },
        ];
        let mut f = BTreeMap::new();
        apply_field_ops(&mut f, &ops).unwrap();
        assert_eq!(f["log"], PropertyValue::List(vec![PropertyValue::Int64(1), PropertyValue::Int64(2)]));
        assert_eq!(f["a"], PropertyValue::Map(BTreeMap::new()));
        let mut buf = Vec::new();
        encode_field_ops(&ops, &mut buf);
        let mut r = SveReader::new(&buf);
        assert_eq!(decode_field_ops(&mut r).unwrap(), ops);
        for cut in 0..buf.len() {
            assert!(decode_field_ops(&mut SveReader::new(&buf[..cut])).is_err());
        }
        assert!(validate_path("a..b").is_err());
        assert!(validate_path("").is_err());
        let mut f2 = BTreeMap::new();
        f2.insert("a".into(), PropertyValue::Int64(1));
        let bad = FieldOp::Set { path: "a.b".into(), value: PropertyValue::Null };
        assert!(apply_field_ops(&mut f2, &[bad]).is_err());
    }
}
