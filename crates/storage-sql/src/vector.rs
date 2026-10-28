//! pgvector compatibility (`TASK-122`): the `vector(n)` type, its text and binary formats, and
//! the distance functions behind `<->` (L2), `<=>` (cosine), `<#>` (negative inner product) and
//! `<+>` (L1). Values are stored as lists of numbers, which the document store's vector indexes
//! read directly.

use crate::datum::Datum;
use crate::error::{SqlError, SqlResult};

/// Largest dimension accepted (pgvector's limit for `vector`).
pub const MAX_DIMS: usize = 16_000;

/// Type OIDs reported for `vector` and `vector[]`. pgvector's are assigned at `CREATE EXTENSION`
/// time, so clients look them up in `pg_type` by name, as they do with pgvector.
pub const VECTOR_OID: u32 = 16_388;
pub const VECTOR_ARRAY_OID: u32 = 16_393;

/// `[1,2,3]` (whitespace allowed) as a vector.
pub fn parse(text: &str) -> SqlResult<Vec<f32>> {
    let bad =
        || SqlError::invalid_text(format!("invalid input syntax for type vector: \"{text}\""));
    let inner = text
        .trim()
        .strip_prefix('[')
        .and_then(|t| t.strip_suffix(']'))
        .ok_or_else(bad)?;
    if inner.trim().is_empty() {
        return Err(SqlError::invalid_text(
            "vector must have at least 1 dimension",
        ));
    }
    let mut out = Vec::new();
    for part in inner.split(',') {
        let x: f32 = part.trim().parse().map_err(|_| bad())?;
        if !x.is_finite() {
            return Err(SqlError::invalid_text(
                "NaN and infinite values not allowed in vector",
            ));
        }
        out.push(x);
    }
    check_dims(out.len())?;
    Ok(out)
}

fn check_dims(n: usize) -> SqlResult<()> {
    if n == 0 {
        return Err(SqlError::invalid_text(
            "vector must have at least 1 dimension",
        ));
    }
    if n > MAX_DIMS {
        return Err(SqlError::invalid_text(format!(
            "vector cannot have more than {MAX_DIMS} dimensions"
        )));
    }
    Ok(())
}

/// pgvector's text output: shortest round-tripping `f32` values.
pub fn format(v: &[f32]) -> String {
    let mut out = String::with_capacity(v.len() * 6 + 2);
    out.push('[');
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&x.to_string());
    }
    out.push(']');
    out
}

/// Binary format (`vector_send`): `u16 dims | u16 0 | dims × f32`, big-endian.
pub fn encode_binary(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + v.len() * 4);
    out.extend_from_slice(&(v.len() as u16).to_be_bytes());
    out.extend_from_slice(&0u16.to_be_bytes());
    for x in v {
        out.extend_from_slice(&x.to_be_bytes());
    }
    out
}

pub fn decode_binary(b: &[u8]) -> SqlResult<Vec<f32>> {
    let bad = || SqlError::invalid_text("invalid binary vector");
    let dims = usize::from(u16::from_be_bytes([
        *b.first().ok_or_else(bad)?,
        *b.get(1).ok_or_else(bad)?,
    ]));
    if b.get(2..4) != Some(&[0, 0]) || b.len() != 4 + dims * 4 {
        return Err(bad());
    }
    check_dims(dims)?;
    let v: Vec<f32> = b[4..]
        .chunks_exact(4)
        .map(|c| f32::from_be_bytes([c[0], c[1], c[2], c[3]]))
        .collect();
    if v.iter().any(|x| !x.is_finite()) {
        return Err(SqlError::invalid_text(
            "NaN and infinite values not allowed in vector",
        ));
    }
    Ok(v)
}

/// A value as a vector of `dims` dimensions (any if `None`): vectors, `'[...]'` text and
/// numeric arrays convert, like pgvector's casts.
pub fn coerce(d: &Datum, dims: Option<u32>) -> SqlResult<Vec<f32>> {
    let v = match d {
        Datum::Vector(v) => v.clone(),
        Datum::Text(s) | Datum::Json(s) => parse(s)?,
        Datum::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for x in items {
                let f = crate::datum::to_f64(x).ok_or_else(|| {
                    SqlError::datatype_mismatch("cannot cast this array to vector")
                })?;
                if !f.is_finite() {
                    return Err(SqlError::invalid_text(
                        "NaN and infinite values not allowed in vector",
                    ));
                }
                out.push(f as f32);
            }
            check_dims(out.len())?;
            out
        }
        _ => {
            return Err(SqlError::datatype_mismatch(
                "cannot cast this value to vector",
            ))
        }
    };
    if let Some(n) = dims {
        if v.len() != n as usize {
            // pgvector reports dimension errors as data exceptions (SQLSTATE 22000).
            return Err(SqlError::new(
                "22000",
                format!("expected {n} dimensions, not {}", v.len()),
            ));
        }
    }
    Ok(v)
}

fn same_dims(a: &[f32], b: &[f32]) -> SqlResult<()> {
    if a.len() != b.len() {
        return Err(SqlError::new(
            "22000",
            format!("different vector dimensions {} and {}", a.len(), b.len()),
        ));
    }
    Ok(())
}

/// The distances pgvector defines (computed in `f64`, as pgvector does for its results).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    L2,
    Cosine,
    NegInner,
    L1,
}

pub fn distance(metric: Metric, a: &[f32], b: &[f32]) -> SqlResult<f64> {
    same_dims(a, b)?;
    let pairs = a.iter().zip(b).map(|(x, y)| (f64::from(*x), f64::from(*y)));
    Ok(match metric {
        Metric::L2 => pairs.map(|(x, y)| (x - y) * (x - y)).sum::<f64>().sqrt(),
        Metric::L1 => pairs.map(|(x, y)| (x - y).abs()).sum(),
        Metric::NegInner => -pairs.map(|(x, y)| x * y).sum::<f64>(),
        Metric::Cosine => {
            let (mut dot, mut na, mut nb) = (0f64, 0f64, 0f64);
            for (x, y) in pairs {
                dot += x * y;
                na += x * x;
                nb += y * y;
            }
            if na == 0.0 || nb == 0.0 {
                f64::NAN // pgvector: cosine distance with a zero vector is NaN
            } else {
                1.0 - (dot / (na.sqrt() * nb.sqrt())).clamp(-1.0, 1.0)
            }
        }
    })
}

/// Element-wise `+`, `-`, `*`.
pub fn elementwise(op: char, a: &[f32], b: &[f32]) -> SqlResult<Vec<f32>> {
    same_dims(a, b)?;
    let out: Vec<f32> = a
        .iter()
        .zip(b)
        .map(|(x, y)| match op {
            '+' => x + y,
            '-' => x - y,
            _ => x * y,
        })
        .collect();
    if out.iter().any(|x| !x.is_finite()) {
        return Err(SqlError::numeric_out_of_range(
            "value out of range: overflow",
        ));
    }
    Ok(out)
}

pub fn norm(v: &[f32]) -> f64 {
    v.iter()
        .map(|x| f64::from(*x) * f64::from(*x))
        .sum::<f64>()
        .sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_and_distances() {
        let v = parse(" [1, 2.5,-3 ] ").unwrap();
        assert_eq!(v, vec![1.0, 2.5, -3.0]);
        assert_eq!(format(&v), "[1,2.5,-3]");
        assert_eq!(decode_binary(&encode_binary(&v)).unwrap(), v);
        assert!(parse("[]").is_err() && parse("[1,x]").is_err() && parse("1,2").is_err());
        assert!(coerce(&Datum::Text("[1,2]".into()), Some(3)).is_err());
        let (a, b) = ([1.0, 0.0], [0.0, 2.0]);
        assert!((distance(Metric::L2, &a, &b).unwrap() - 5f64.sqrt()).abs() < 1e-12);
        assert_eq!(distance(Metric::L1, &a, &b).unwrap(), 3.0);
        assert_eq!(
            distance(Metric::NegInner, &[1.0, 2.0], &[3.0, 4.0]).unwrap(),
            -11.0
        );
        assert!((distance(Metric::Cosine, &a, &b).unwrap() - 1.0).abs() < 1e-12);
        assert!(distance(Metric::Cosine, &[0.0, 0.0], &b).unwrap().is_nan());
        assert!(distance(Metric::L2, &[1.0], &b).is_err());
    }
}
