//! SQL values: comparison, arithmetic, casts, text formats and the mapping to stored values.

use std::cmp::Ordering;
use std::fmt::Write as _;

use storage_types::{Decimal, RoundingMode};

use crate::ast::DataType;
use crate::error::{SqlError, SqlResult};

/// Exact decimal from text (`1.50`, `-3`, `1e3`).
#[allow(clippy::result_unit_err)]
pub fn parse_dec(s: &str) -> Result<Decimal, ()> {
    s.parse::<Decimal>().map_err(|_| ())
}

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_DAY: i64 = 86_400 * NS_PER_SEC;

/// One SQL value.
#[derive(Debug, Clone, PartialEq)]
pub enum Datum {
    Null,
    Bool(bool),
    Int(i64),
    Numeric(Decimal),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    /// Nanoseconds since 1970-01-01 00:00:00 UTC.
    Timestamp(i64),
    /// Days since 1970-01-01.
    Date(i32),
    /// Nanoseconds since midnight.
    Time(i64),
    Interval {
        months: i64,
        days: i64,
        ns: i64,
    },
    Uuid([u8; 16]),
    Json(String),
    Array(Vec<Datum>),
}

impl Datum {
    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    /// SQL truth value: `None` for NULL (unknown).
    pub fn truthy(&self) -> SqlResult<Option<bool>> {
        Ok(match self {
            Datum::Null => None,
            Datum::Bool(b) => Some(*b),
            Datum::Int(i) => Some(*i != 0),
            Datum::Numeric(d) => Some(!d.is_zero()),
            Datum::Float(f) => Some(*f != 0.0),
            Datum::Text(s) => Some(parse_bool(s).ok_or_else(|| {
                SqlError::invalid_text(format!("invalid input syntax for type boolean: \"{s}\""))
            })?),
            other => {
                return Err(SqlError::datatype_mismatch(format!(
                    "argument of WHERE must be type boolean, not {}",
                    other.type_name()
                )))
            }
        })
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            Datum::Null => "unknown",
            Datum::Bool(_) => "boolean",
            Datum::Int(_) => "bigint",
            Datum::Numeric(_) => "numeric",
            Datum::Float(_) => "double precision",
            Datum::Text(_) => "text",
            Datum::Bytes(_) => "bytea",
            Datum::Timestamp(_) => "timestamp",
            Datum::Date(_) => "date",
            Datum::Time(_) => "time",
            Datum::Interval { .. } => "interval",
            Datum::Uuid(_) => "uuid",
            Datum::Json(_) => "json",
            Datum::Array(_) => "array",
        }
    }

    /// The natural column type of this value (for result metadata when nothing better is known).
    pub fn natural_type(&self) -> DataType {
        match self {
            Datum::Null | Datum::Text(_) => DataType::Text,
            Datum::Bool(_) => DataType::Bool,
            Datum::Int(_) => DataType::BigInt,
            Datum::Numeric(_) => DataType::Numeric(None),
            Datum::Float(_) => DataType::Double,
            Datum::Bytes(_) => DataType::Bytes,
            Datum::Timestamp(_) => DataType::Timestamp,
            Datum::Date(_) => DataType::Date,
            Datum::Time(_) => DataType::Time,
            Datum::Interval { .. } => DataType::Interval,
            Datum::Uuid(_) => DataType::Uuid,
            Datum::Json(_) => DataType::Json,
            Datum::Array(a) => DataType::Array(Box::new(
                a.iter()
                    .find(|d| !d.is_null())
                    .map_or(DataType::Text, Datum::natural_type),
            )),
        }
    }

    pub fn as_i64(&self) -> SqlResult<i64> {
        match self {
            Datum::Int(i) => Ok(*i),
            Datum::Bool(b) => Ok(i64::from(*b)),
            Datum::Numeric(d) => d
                .rescale(0, RoundingMode::HalfUp)
                .ok()
                .and_then(|d| i64::try_from(d.mantissa()).ok())
                .ok_or_else(|| SqlError::numeric_out_of_range("bigint out of range")),
            Datum::Float(f) if f.is_finite() && f.abs() < 9.2e18 => Ok(f.round() as i64),
            Datum::Text(s) => {
                let t = s.trim();
                t.parse::<i64>()
                    .or_else(|_| {
                        parse_dec(t)
                            .map_err(|_| ())
                            .and_then(|d| Datum::Numeric(d).as_i64().map_err(|_| ()))
                    })
                    .map_err(|_| {
                        SqlError::invalid_text(format!(
                            "invalid input syntax for type integer: \"{s}\""
                        ))
                    })
            }
            other => Err(SqlError::datatype_mismatch(format!(
                "cannot use {} as an integer",
                other.type_name()
            ))),
        }
    }

    pub fn as_text(&self) -> String {
        self.to_text(false)
    }

    /// PostgreSQL text format (`mysql` changes booleans to `1` / `0` and bytes to raw bytes).
    pub fn to_text(&self, mysql: bool) -> String {
        match self {
            Datum::Null => String::new(),
            Datum::Bool(b) => match (mysql, b) {
                (true, true) => "1".into(),
                (true, false) => "0".into(),
                (false, true) => "t".into(),
                (false, false) => "f".into(),
            },
            Datum::Int(i) => i.to_string(),
            Datum::Numeric(d) => d.to_string(),
            Datum::Float(f) => format_float(*f),
            Datum::Text(s) | Datum::Json(s) => s.clone(),
            Datum::Bytes(b) => {
                if mysql {
                    String::from_utf8_lossy(b).into_owned()
                } else {
                    let mut s = String::with_capacity(2 + 2 * b.len());
                    s.push_str("\\x");
                    for x in b {
                        let _ = write!(s, "{x:02x}");
                    }
                    s
                }
            }
            Datum::Timestamp(ns) => format_timestamp(*ns, false),
            Datum::Date(d) => format_date(*d),
            Datum::Time(ns) => format_time(*ns),
            Datum::Interval { months, days, ns } => format_interval(*months, *days, *ns),
            Datum::Uuid(u) => format_uuid(u),
            Datum::Array(items) => {
                let mut s = String::from("{");
                for (i, d) in items.iter().enumerate() {
                    if i > 0 {
                        s.push(',');
                    }
                    match d {
                        Datum::Null => s.push_str("NULL"),
                        Datum::Text(t) => {
                            let needs = t.is_empty()
                                || t.chars()
                                    .any(|c| matches!(c, ',' | '{' | '}' | '"' | '\\' | ' '))
                                || t.eq_ignore_ascii_case("null");
                            if needs {
                                s.push('"');
                                for c in t.chars() {
                                    if c == '"' || c == '\\' {
                                        s.push('\\');
                                    }
                                    s.push(c);
                                }
                                s.push('"');
                            } else {
                                s.push_str(t);
                            }
                        }
                        other => s.push_str(&other.to_text(mysql)),
                    }
                }
                s.push('}');
                s
            }
        }
    }

    /// Raw bytes for binary-safe text protocols (MySQL rows): bytes stay bytes.
    pub fn to_wire_bytes(&self, mysql: bool) -> Vec<u8> {
        match self {
            Datum::Bytes(b) if mysql => b.clone(),
            other => other.to_text(mysql).into_bytes(),
        }
    }
}

/// Total order used by ORDER BY, DISTINCT, GROUP BY and comparisons (NULLs sort last).
pub fn compare(a: &Datum, b: &Datum) -> Ordering {
    match (a, b) {
        (Datum::Null, Datum::Null) => Ordering::Equal,
        (Datum::Null, _) => Ordering::Greater,
        (_, Datum::Null) => Ordering::Less,
        _ => sql_cmp(a, b).unwrap_or_else(|| {
            rank(a)
                .cmp(&rank(b))
                .then_with(|| a.as_text().cmp(&b.as_text()))
        }),
    }
}

fn rank(d: &Datum) -> u8 {
    match d {
        Datum::Null => 255,
        Datum::Bool(_) => 0,
        Datum::Int(_) | Datum::Numeric(_) | Datum::Float(_) => 1,
        Datum::Text(_) | Datum::Json(_) => 2,
        Datum::Bytes(_) => 3,
        Datum::Timestamp(_) | Datum::Date(_) => 4,
        Datum::Time(_) => 5,
        Datum::Interval { .. } => 6,
        Datum::Uuid(_) => 7,
        Datum::Array(_) => 8,
    }
}

/// SQL comparison: `None` when either side is NULL or the types cannot be compared.
pub fn sql_cmp(a: &Datum, b: &Datum) -> Option<Ordering> {
    use Datum::*;
    Some(match (a, b) {
        (Null, _) | (_, Null) => return None,
        (Bool(x), Bool(y)) => x.cmp(y),
        (Int(x), Int(y)) => x.cmp(y),
        (Int(_) | Numeric(_), Int(_) | Numeric(_)) => to_decimal(a)?.cmp(&to_decimal(b)?),
        (Float(x), _) if is_number(b) => x.partial_cmp(&to_f64(b)?)?,
        (_, Float(y)) if is_number(a) => to_f64(a)?.partial_cmp(y)?,
        (Bool(_), Int(_)) | (Int(_), Bool(_)) => to_i64_loose(a)?.cmp(&to_i64_loose(b)?),
        (Text(x), Text(y)) => x.cmp(y),
        (Json(x), Json(y)) | (Json(x), Text(y)) | (Text(x), Json(y)) => x.cmp(y),
        (Bytes(x), Bytes(y)) => x.cmp(y),
        (Uuid(x), Uuid(y)) => x.cmp(y),
        (Timestamp(_) | Date(_), Timestamp(_) | Date(_)) => to_ns(a)?.cmp(&to_ns(b)?),
        (Time(x), Time(y)) => x.cmp(y),
        (Interval { .. }, Interval { .. }) => interval_ns(a).cmp(&interval_ns(b)),
        (Array(x), Array(y)) => {
            for (p, q) in x.iter().zip(y) {
                match compare(p, q) {
                    Ordering::Equal => {}
                    o => return Some(o),
                }
            }
            x.len().cmp(&y.len())
        }
        // A text literal compared with a typed value is read as that type ('5' = 5, '2024-01-01' < ts).
        (Text(s), other) => return sql_cmp(&coerce_text(s, other)?, other),
        (other, Text(s)) => return sql_cmp(other, &coerce_text(s, other)?),
        _ => return None,
    })
}

/// Reads text as the type of `like` (for comparisons between a literal and a typed value).
fn coerce_text(s: &str, like: &Datum) -> Option<Datum> {
    let ty = match like {
        Datum::Int(_) | Datum::Numeric(_) => DataType::Numeric(None),
        Datum::Float(_) => DataType::Double,
        Datum::Bool(_) => DataType::Bool,
        Datum::Timestamp(_) => DataType::Timestamp,
        Datum::Date(_) => DataType::Date,
        Datum::Time(_) => DataType::Time,
        Datum::Uuid(_) => DataType::Uuid,
        Datum::Bytes(_) => return Some(Datum::Bytes(s.as_bytes().to_vec())),
        Datum::Interval { .. } => DataType::Interval,
        _ => return None,
    };
    cast(&Datum::Text(s.to_owned()), &ty).ok()
}

fn interval_ns(d: &Datum) -> i128 {
    match d {
        Datum::Interval { months, days, ns } => {
            i128::from(*months) * 30 * i128::from(NS_PER_DAY)
                + i128::from(*days) * i128::from(NS_PER_DAY)
                + i128::from(*ns)
        }
        _ => 0,
    }
}

fn is_number(d: &Datum) -> bool {
    matches!(d, Datum::Int(_) | Datum::Numeric(_) | Datum::Float(_))
}

fn to_i64_loose(d: &Datum) -> Option<i64> {
    d.as_i64().ok()
}

pub fn to_decimal(d: &Datum) -> Option<Decimal> {
    match d {
        Datum::Int(i) => Some(Decimal::from_i64(*i)),
        Datum::Numeric(x) => Some(*x),
        Datum::Bool(b) => Some(Decimal::from_i64(i64::from(*b))),
        Datum::Float(f) => Decimal::from_f64_lossy(*f, 15, RoundingMode::HalfEven)
            .ok()
            .map(Decimal::normalize),
        Datum::Text(s) => parse_dec(s.trim()).ok(),
        _ => None,
    }
}

pub fn to_f64(d: &Datum) -> Option<f64> {
    match d {
        Datum::Int(i) => Some(*i as f64),
        Datum::Numeric(x) => Some(x.to_f64_lossy()),
        Datum::Float(f) => Some(*f),
        Datum::Bool(b) => Some(f64::from(u8::from(*b))),
        Datum::Text(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn to_ns(d: &Datum) -> Option<i64> {
    match d {
        Datum::Timestamp(ns) => Some(*ns),
        Datum::Date(days) => i64::from(*days).checked_mul(NS_PER_DAY),
        _ => None,
    }
}

/// Equality for GROUP BY / DISTINCT / UNION (NULL equals NULL here).
pub fn group_eq(a: &Datum, b: &Datum) -> bool {
    compare(a, b) == Ordering::Equal
}

/// A canonical key for hashing (GROUP BY, DISTINCT, hash joins): equal values give equal keys.
pub fn hash_key(d: &Datum) -> String {
    match d {
        Datum::Null => "\u{0}N".into(),
        Datum::Int(_) | Datum::Numeric(_) => to_decimal(d)
            .map(|x| format!("n{}", x.normalize()))
            .unwrap_or_default(),
        Datum::Float(f) => {
            if f.fract() == 0.0 && f.abs() < 1e15 {
                format!("n{}", *f as i64)
            } else {
                format!("f{f}")
            }
        }
        Datum::Bool(b) => format!("b{b}"),
        Datum::Text(s) | Datum::Json(s) => format!("t{s}"),
        Datum::Bytes(b) => format!("x{}", crate::hash::hex(b)),
        Datum::Timestamp(_) | Datum::Date(_) => format!("d{}", to_ns(d).unwrap_or(0)),
        other => format!("o{}", other.as_text()),
    }
}

// ── arithmetic ──

/// `+ - * / %` with numeric promotion: int op int stays int (checked), anything with numeric is
/// exact numeric, anything with a float is float.
pub fn arith(op: char, a: &Datum, b: &Datum) -> SqlResult<Datum> {
    use Datum::*;
    if a.is_null() || b.is_null() {
        return Ok(Null);
    }
    // Date / time arithmetic.
    match (op, a, b) {
        ('+', Timestamp(_) | Date(_), Interval { .. }) => return add_interval(a, b, 1),
        ('+', Interval { .. }, Timestamp(_) | Date(_)) => return add_interval(b, a, 1),
        ('-', Timestamp(_) | Date(_), Interval { .. }) => return add_interval(a, b, -1),
        ('+', Date(d), Int(n)) => return Ok(Date(d.saturating_add(*n as i32))),
        ('-', Date(d), Int(n)) => return Ok(Date(d.saturating_sub(*n as i32))),
        ('-', Date(x), Date(y)) => return Ok(Int(i64::from(*x) - i64::from(*y))),
        ('-', Timestamp(_) | Date(_), Timestamp(_) | Date(_)) => {
            let ns = to_ns(a).unwrap_or(0) - to_ns(b).unwrap_or(0);
            return Ok(Interval {
                months: 0,
                days: ns / NS_PER_DAY,
                ns: ns % NS_PER_DAY,
            });
        }
        (
            '+' | '-',
            Interval {
                months: m1,
                days: d1,
                ns: n1,
            },
            Interval {
                months: m2,
                days: d2,
                ns: n2,
            },
        ) => {
            let s = if op == '+' { 1 } else { -1 };
            return Ok(Interval {
                months: m1 + s * m2,
                days: d1 + s * d2,
                ns: n1 + s * n2,
            });
        }
        _ => {}
    }
    let num = |d: &Datum| -> SqlResult<Datum> {
        match d {
            Text(s) => {
                let t = s.trim();
                if let Ok(i) = t.parse::<i64>() {
                    Ok(Int(i))
                } else if let Ok(x) = parse_dec(t) {
                    Ok(Numeric(x))
                } else if let Ok(f) = t.parse::<f64>() {
                    Ok(Float(f))
                } else {
                    Err(SqlError::invalid_text(format!(
                        "invalid input syntax for type numeric: \"{s}\""
                    )))
                }
            }
            Bool(b) => Ok(Int(i64::from(*b))),
            Int(_) | Numeric(_) | Float(_) => Ok(d.clone()),
            other => Err(SqlError::datatype_mismatch(format!(
                "operator does not exist: {} {op} ...",
                other.type_name()
            ))),
        }
    };
    let (a, b) = (num(a)?, num(b)?);
    let overflow = || SqlError::numeric_out_of_range("value out of range");
    match (&a, &b) {
        (Int(x), Int(y)) => Ok(Int(match op {
            '+' => x.checked_add(*y).ok_or_else(overflow)?,
            '-' => x.checked_sub(*y).ok_or_else(overflow)?,
            '*' => x.checked_mul(*y).ok_or_else(overflow)?,
            '/' => {
                if *y == 0 {
                    return Err(SqlError::division_by_zero());
                }
                x.checked_div(*y).ok_or_else(overflow)?
            }
            '%' => {
                if *y == 0 {
                    return Err(SqlError::division_by_zero());
                }
                x.checked_rem(*y).ok_or_else(overflow)?
            }
            _ => return Err(SqlError::internal("bad operator")),
        })),
        (Float(_), _) | (_, Float(_)) => {
            let (x, y) = (to_f64(&a).unwrap_or(0.0), to_f64(&b).unwrap_or(0.0));
            if matches!(op, '/' | '%') && y == 0.0 {
                return Err(SqlError::division_by_zero());
            }
            Ok(Float(match op {
                '+' => x + y,
                '-' => x - y,
                '*' => x * y,
                '/' => x / y,
                _ => x % y,
            }))
        }
        _ => {
            let (x, y) = (
                to_decimal(&a).ok_or_else(overflow)?,
                to_decimal(&b).ok_or_else(overflow)?,
            );
            let r = match op {
                '+' => x.checked_add(y),
                '-' => x.checked_sub(y),
                '*' => x.checked_mul(y),
                '/' => {
                    if y.is_zero() {
                        return Err(SqlError::division_by_zero());
                    }
                    // PostgreSQL keeps at least 16 significant digits; use the larger input
                    // scale plus 16 (capped at 38) and round half away from zero.
                    let scale = (x.scale().max(y.scale()) + 16).min(20);
                    x.checked_div(y, scale, RoundingMode::HalfUp)
                        .map(trim_division)
                }
                _ => {
                    if y.is_zero() {
                        return Err(SqlError::division_by_zero());
                    }
                    // x - trunc(x / y) * y
                    x.checked_div(y, 0, RoundingMode::Down)
                        .and_then(|q| q.checked_mul(y))
                        .and_then(|p| x.checked_sub(p))
                }
            };
            r.map(Numeric).map_err(|_| overflow())
        }
    }
}

/// Drops trailing zeros a division introduced beyond the inputs' natural scale.
fn trim_division(d: Decimal) -> Decimal {
    let n = d.normalize();
    if n.scale() < d.scale() {
        // Keep the original scale if trimming would leave fewer digits than PostgreSQL shows
        // (it pads division results); readability beats padding here.
        n
    } else {
        d
    }
}

fn add_interval(t: &Datum, iv: &Datum, sign: i64) -> SqlResult<Datum> {
    let Datum::Interval { months, days, ns } = iv else {
        return Err(SqlError::internal("not an interval"));
    };
    let base = to_ns(t).ok_or_else(|| SqlError::internal("not a timestamp"))?;
    let day = base.div_euclid(NS_PER_DAY);
    let rem = base.rem_euclid(NS_PER_DAY);
    let (y, m, d) = civil_from_days(day as i32);
    let total = i64::from(y) * 12 + i64::from(m - 1) + sign * months;
    let (ny, nm) = (
        total.div_euclid(12) as i32,
        (total.rem_euclid(12) + 1) as u32,
    );
    let nd = d.min(days_in_month(ny, nm));
    let new_day = i64::from(days_from_civil(ny, nm, nd)) + sign * days;
    let out = new_day * NS_PER_DAY + rem + sign * ns;
    Ok(match t {
        Datum::Date(_) if *ns == 0 => Datum::Date(new_day as i32),
        _ => Datum::Timestamp(out),
    })
}

// ── casts ──

pub fn parse_bool(s: &str) -> Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "t" | "true" | "y" | "yes" | "on" | "1" => Some(true),
        "f" | "false" | "n" | "no" | "off" | "0" => Some(false),
        _ => None,
    }
}

/// Converts `d` to `ty` (`CAST`, assignment to a column, parameter binding).
pub fn cast(d: &Datum, ty: &DataType) -> SqlResult<Datum> {
    use Datum::*;
    if d.is_null() {
        return Ok(Null);
    }
    let bad = |d: &Datum| {
        SqlError::invalid_text(format!(
            "invalid input syntax for type {}: \"{}\"",
            type_sql(ty),
            d.as_text()
        ))
    };
    Ok(match ty {
        DataType::Bool => match d {
            Bool(b) => Bool(*b),
            Int(i) => Bool(*i != 0),
            Text(s) => Bool(parse_bool(s).ok_or_else(|| bad(d))?),
            Numeric(x) => Bool(!x.is_zero()),
            _ => return Err(cast_err(d, ty)),
        },
        DataType::SmallInt | DataType::Int | DataType::BigInt => {
            let v = match d {
                Bool(b) => i64::from(*b),
                Text(s) => {
                    let t = s.trim();
                    match t.parse::<i64>() {
                        Ok(v) => v,
                        Err(_) => match parse_dec(t) {
                            Ok(x) => Numeric(x).as_i64()?,
                            Err(_) => return Err(bad(d)),
                        },
                    }
                }
                Int(_) | Numeric(_) | Float(_) => d.as_i64()?,
                _ => return Err(cast_err(d, ty)),
            };
            let (lo, hi, name) = match ty {
                DataType::SmallInt => (i64::from(i16::MIN), i64::from(i16::MAX), "smallint"),
                DataType::Int => (i64::from(i32::MIN), i64::from(i32::MAX), "integer"),
                _ => (i64::MIN, i64::MAX, "bigint"),
            };
            if v < lo || v > hi {
                return Err(SqlError::numeric_out_of_range(format!(
                    "{name} out of range"
                )));
            }
            Int(v)
        }
        DataType::Numeric(spec) => {
            let x = match d {
                Text(s) => parse_dec(s.trim()).map_err(|_| bad(d))?,
                Float(f) => {
                    if !f.is_finite() {
                        return Err(SqlError::numeric_out_of_range(
                            "cannot convert infinity or NaN to numeric",
                        ));
                    }
                    parse_dec(&format_float(*f)).map_err(|_| bad(d))?
                }
                Int(_) | Numeric(_) | Bool(_) => to_decimal(d).ok_or_else(|| bad(d))?,
                _ => return Err(cast_err(d, ty)),
            };
            Numeric(fit_numeric(x, *spec)?)
        }
        DataType::Real | DataType::Double => match d {
            Text(s) => {
                let t = s.trim().to_ascii_lowercase();
                Float(match t.as_str() {
                    "nan" => f64::NAN,
                    "infinity" | "inf" => f64::INFINITY,
                    "-infinity" | "-inf" => f64::NEG_INFINITY,
                    _ => t.parse().map_err(|_| bad(d))?,
                })
            }
            Int(_) | Numeric(_) | Float(_) | Bool(_) => {
                let f = to_f64(d).unwrap_or(0.0);
                Float(if *ty == DataType::Real {
                    f64::from(f as f32)
                } else {
                    f
                })
            }
            _ => return Err(cast_err(d, ty)),
        },
        DataType::Text | DataType::Name | DataType::Varchar(_) | DataType::Char(_) => {
            let s = match d {
                Text(s) => s.clone(),
                Bytes(b) => String::from_utf8(b.clone()).unwrap_or_else(|_| d.as_text()),
                other => other.as_text(),
            };
            match ty {
                DataType::Varchar(Some(n)) | DataType::Char(Some(n))
                    if s.chars().count() > *n as usize =>
                {
                    // Like PostgreSQL: trailing spaces beyond the limit are trimmed silently.
                    let trimmed: String = s.trim_end_matches(' ').to_owned();
                    if trimmed.chars().count() > *n as usize {
                        return Err(SqlError::new(
                            "22001",
                            format!("value too long for type {}", type_sql(ty)),
                        ));
                    }
                    Text(trimmed)
                }
                _ => Text(s),
            }
        }
        DataType::Enum(values) => {
            let s = d.as_text();
            if !values.contains(&s) {
                return Err(SqlError::invalid_text(format!(
                    "invalid input value for enum: \"{s}\""
                )));
            }
            Text(s)
        }
        DataType::Bytes => match d {
            Bytes(b) => Bytes(b.clone()),
            Text(s) => Bytes(parse_bytea(s)),
            other => Bytes(other.as_text().into_bytes()),
        },
        DataType::Timestamp | DataType::TimestampTz => match d {
            Timestamp(ns) => Timestamp(*ns),
            Date(days) => Timestamp(i64::from(*days) * NS_PER_DAY),
            Text(s) => Timestamp(parse_timestamp(s).ok_or_else(|| bad(d))?),
            Int(secs) => Timestamp(secs.saturating_mul(NS_PER_SEC)),
            _ => return Err(cast_err(d, ty)),
        },
        DataType::Date => match d {
            Date(x) => Date(*x),
            Timestamp(ns) => Date(ns.div_euclid(NS_PER_DAY) as i32),
            Text(s) => Date(
                parse_timestamp(s)
                    .ok_or_else(|| bad(d))?
                    .div_euclid(NS_PER_DAY) as i32,
            ),
            _ => return Err(cast_err(d, ty)),
        },
        DataType::Time => match d {
            Time(ns) => Time(*ns),
            Timestamp(ns) => Time(ns.rem_euclid(NS_PER_DAY)),
            Text(s) => Time(parse_time(s.trim()).ok_or_else(|| bad(d))?),
            _ => return Err(cast_err(d, ty)),
        },
        DataType::Interval => match d {
            Interval { .. } => d.clone(),
            Text(s) => parse_interval(s).ok_or_else(|| bad(d))?,
            _ => return Err(cast_err(d, ty)),
        },
        DataType::Uuid => match d {
            Uuid(u) => Uuid(*u),
            Text(s) => Uuid(parse_uuid(s).ok_or_else(|| bad(d))?),
            Bytes(b) if b.len() == 16 => Uuid(b.as_slice().try_into().unwrap_or([0; 16])),
            _ => return Err(cast_err(d, ty)),
        },
        DataType::Json | DataType::Jsonb => match d {
            Json(s) => Json(s.clone()),
            Text(s) => Json(s.clone()),
            other => Json(json_of(other)),
        },
        DataType::Array(inner) => match d {
            Array(items) => Array(
                items
                    .iter()
                    .map(|x| cast(x, inner))
                    .collect::<SqlResult<_>>()?,
            ),
            Text(s) => Array(
                parse_array(s)?
                    .into_iter()
                    .map(|x| cast(&x, inner))
                    .collect::<SqlResult<_>>()?,
            ),
            other => Array(vec![cast(other, inner)?]),
        },
    })
}

fn cast_err(d: &Datum, ty: &DataType) -> SqlError {
    SqlError::new(
        "42846",
        format!("cannot cast type {} to {}", d.type_name(), type_sql(ty)),
    )
}

/// Rounds / checks a decimal against `NUMERIC(precision, scale)`.
pub fn fit_numeric(x: Decimal, spec: Option<(u8, u8)>) -> SqlResult<Decimal> {
    let Some((p, s)) = spec else { return Ok(x) };
    let r = x
        .rescale(s, RoundingMode::HalfUp)
        .map_err(|_| SqlError::numeric_out_of_range("numeric field overflow"))?;
    let digits = r.mantissa().unsigned_abs().to_string().len() as u32;
    if p > 0 && digits > u32::from(p) && !r.is_zero() {
        return Err(SqlError::numeric_out_of_range(format!(
            "numeric field overflow: a field with precision {p}, scale {s} must round to an absolute value less than 10^{}",
            p - s
        )));
    }
    Ok(r)
}

/// SQL spelling of a type (for messages and catalogs).
pub fn type_sql(ty: &DataType) -> String {
    match ty {
        DataType::Bool => "boolean".into(),
        DataType::SmallInt => "smallint".into(),
        DataType::Int => "integer".into(),
        DataType::BigInt => "bigint".into(),
        DataType::Numeric(None) => "numeric".into(),
        DataType::Numeric(Some((p, s))) => format!("numeric({p},{s})"),
        DataType::Real => "real".into(),
        DataType::Double => "double precision".into(),
        DataType::Text => "text".into(),
        DataType::Name => "name".into(),
        DataType::Varchar(None) => "character varying".into(),
        DataType::Varchar(Some(n)) => format!("character varying({n})"),
        DataType::Char(n) => format!("character({})", n.unwrap_or(1)),
        DataType::Bytes => "bytea".into(),
        DataType::Timestamp => "timestamp without time zone".into(),
        DataType::TimestampTz => "timestamp with time zone".into(),
        DataType::Date => "date".into(),
        DataType::Time => "time without time zone".into(),
        DataType::Interval => "interval".into(),
        DataType::Uuid => "uuid".into(),
        DataType::Json => "json".into(),
        DataType::Jsonb => "jsonb".into(),
        DataType::Enum(v) => format!(
            "enum({})",
            v.iter()
                .map(|s| format!("'{}'", s.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(",")
        ),
        DataType::Array(inner) => format!("{}[]", type_sql(inner)),
    }
}

// ── storage mapping ──

/// The stored form of a column value.
pub fn to_value(d: &Datum, ty: &DataType) -> storine::Value {
    use storine::Value as V;
    match d {
        Datum::Null => V::Null,
        Datum::Bool(b) => V::Bool(*b),
        Datum::Int(i) => V::Int(*i),
        Datum::Numeric(x) => match storine::Decimal::new(x.mantissa(), x.scale()) {
            Ok(v) => V::Decimal(v),
            Err(_) => V::Text(x.to_string()),
        },
        Datum::Float(f) => V::Float(*f),
        Datum::Text(s) | Datum::Json(s) => V::Text(s.clone()),
        Datum::Bytes(b) => V::Bytes(b.clone()),
        Datum::Timestamp(ns) => V::Timestamp(*ns),
        Datum::Date(days) => V::Timestamp(i64::from(*days) * NS_PER_DAY),
        Datum::Time(ns) => V::Text(format_time(*ns)),
        Datum::Interval { .. } => V::Text(d.as_text()),
        Datum::Uuid(u) => V::Uuid(*u),
        Datum::Array(items) => {
            let inner = match ty {
                DataType::Array(t) => (**t).clone(),
                _ => DataType::Text,
            };
            V::List(items.iter().map(|x| to_value(x, &inner)).collect())
        }
    }
}

/// A stored value read back as a column of type `ty` (`None`: no declared type, keep it natural).
pub fn from_value(v: &storine::Value, ty: Option<&DataType>) -> Datum {
    use storine::Value as V;
    let natural = match v {
        V::Null => Datum::Null,
        V::Bool(b) => Datum::Bool(*b),
        V::Int(i) => Datum::Int(*i),
        V::UInt(u) => i64::try_from(*u)
            .map(Datum::Int)
            .unwrap_or_else(|_| Datum::Numeric(Decimal::from_i128(i128::from(*u)))),
        V::Int128(i) => Datum::Numeric(Decimal::from_i128(*i)),
        V::Float(f) => Datum::Float(*f),
        V::Decimal(d) => Decimal::new(d.mantissa(), d.scale())
            .map(Datum::Numeric)
            .unwrap_or(Datum::Null),
        V::Timestamp(ns) => Datum::Timestamp(*ns),
        V::Uuid(u) => Datum::Uuid(*u),
        V::Id(id) => Datum::Bytes(id.to_vec()),
        V::Text(s) => Datum::Text(s.clone()),
        V::Bytes(b) => Datum::Bytes(b.clone()),
        V::List(items) => {
            let inner = match ty {
                Some(DataType::Array(t)) => Some(&**t),
                _ => None,
            };
            Datum::Array(items.iter().map(|x| from_value(x, inner)).collect())
        }
        V::Map(_) => Datum::Json(json_of_value(v)),
    };
    match ty {
        None => natural,
        Some(DataType::Array(_)) => natural,
        Some(DataType::Json | DataType::Jsonb) => match natural {
            Datum::Text(s) => Datum::Json(s),
            other => other,
        },
        Some(t) => match (&natural, t) {
            (Datum::Timestamp(ns), DataType::Date) => Datum::Date(ns.div_euclid(NS_PER_DAY) as i32),
            (Datum::Text(s), DataType::Time) => parse_time(s).map(Datum::Time).unwrap_or(natural),
            (Datum::Text(s), DataType::Interval) => parse_interval(s).unwrap_or(natural),
            (Datum::Text(s), DataType::Numeric(_)) => {
                parse_dec(s).map(Datum::Numeric).unwrap_or(natural)
            }
            (Datum::Int(i), DataType::Bool) => Datum::Bool(*i != 0),
            (Datum::Int(_), DataType::Numeric(_))
            | (Datum::Numeric(_), DataType::Int | DataType::BigInt | DataType::SmallInt)
            | (Datum::Float(_), DataType::Numeric(_))
            | (Datum::Int(_), DataType::Real | DataType::Double) => {
                cast(&natural, t).unwrap_or(natural)
            }
            _ => natural,
        },
    }
}

fn json_of_value(v: &storine::Value) -> String {
    use storine::Value as V;
    match v {
        V::Null => "null".into(),
        V::Bool(b) => b.to_string(),
        V::Int(i) => i.to_string(),
        V::UInt(u) => u.to_string(),
        V::Int128(i) => i.to_string(),
        V::Float(f) => format_float(*f),
        V::Decimal(d) => d.to_string(),
        V::Text(s) => json_string(s),
        V::Map(m) => {
            let parts: Vec<String> = m
                .iter()
                .map(|(k, x)| format!("{}:{}", json_string(k), json_of_value(x)))
                .collect();
            format!("{{{}}}", parts.join(","))
        }
        V::List(l) => format!(
            "[{}]",
            l.iter().map(json_of_value).collect::<Vec<_>>().join(",")
        ),
        other => json_string(&from_value(other, None).as_text()),
    }
}

/// JSON text of a datum (`to_json`, casts to json).
pub fn json_of(d: &Datum) -> String {
    match d {
        Datum::Null => "null".into(),
        Datum::Bool(b) => b.to_string(),
        Datum::Int(i) => i.to_string(),
        Datum::Numeric(x) => x.to_string(),
        Datum::Float(f) => format_float(*f),
        Datum::Json(s) => s.clone(),
        Datum::Array(items) => format!(
            "[{}]",
            items.iter().map(json_of).collect::<Vec<_>>().join(",")
        ),
        other => json_string(&other.as_text()),
    }
}

pub fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

// ── text formats ──

/// Shortest representation that round-trips (PostgreSQL 12+ `float8out`).
pub fn format_float(f: f64) -> String {
    if f.is_nan() {
        return "NaN".into();
    }
    if f.is_infinite() {
        return if f > 0.0 {
            "Infinity".into()
        } else {
            "-Infinity".into()
        };
    }
    let s = format!("{f}");
    let a = f.abs();
    if a != 0.0 && !(1e-4..1e16).contains(&a) {
        // Scientific like PostgreSQL: 1e+20, 1.5e-07.
        let e = format!("{f:e}");
        if let Some((m, exp)) = e.split_once('e') {
            let n: i32 = exp.parse().unwrap_or(0);
            return format!("{m}e{}{:02}", if n < 0 { '-' } else { '+' }, n.abs());
        }
    }
    s
}

pub fn days_from_civil(y: i32, m: u32, d: u32) -> i32 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i32 - 719_468
}

pub fn civil_from_days(z: i32) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

pub fn days_in_month(y: i32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
    }
}

pub fn format_date(days: i32) -> String {
    let (y, m, d) = civil_from_days(days);
    if y <= 0 {
        format!("{:04}-{m:02}-{d:02} BC", 1 - y)
    } else {
        format!("{y:04}-{m:02}-{d:02}")
    }
}

/// `HH:MM:SS[.ffffff]` (microseconds, trailing zeros trimmed like PostgreSQL).
pub fn format_time(ns: i64) -> String {
    let secs = ns.div_euclid(NS_PER_SEC);
    let frac = ns.rem_euclid(NS_PER_SEC);
    let mut s = format!(
        "{:02}:{:02}:{:02}",
        secs / 3600,
        (secs / 60) % 60,
        secs % 60
    );
    let micros = frac / 1000;
    if micros > 0 {
        let f = format!("{micros:06}");
        s.push('.');
        s.push_str(f.trim_end_matches('0'));
    }
    s
}

pub fn format_timestamp(ns: i64, with_zone: bool) -> String {
    let day = ns.div_euclid(NS_PER_DAY) as i32;
    let rem = ns.rem_euclid(NS_PER_DAY);
    let mut s = format!("{} {}", format_date(day), format_time(rem));
    if with_zone {
        s.push_str("+00");
    }
    s
}

fn format_interval(months: i64, days: i64, ns: i64) -> String {
    let mut parts = Vec::new();
    let (y, mo) = (months / 12, months % 12);
    let plural = |n: i64, u: &str| format!("{n} {u}{}", if n.abs() == 1 { "" } else { "s" });
    if y != 0 {
        parts.push(plural(y, "year"));
    }
    if mo != 0 {
        parts.push(plural(mo, "mon"));
    }
    if days != 0 {
        parts.push(plural(days, "day"));
    }
    if ns != 0 || parts.is_empty() {
        let neg = ns < 0;
        let t = format_time(ns.abs());
        parts.push(if neg { format!("-{t}") } else { t });
    }
    parts.join(" ")
}

pub fn format_uuid(u: &[u8; 16]) -> String {
    let h = crate::hash::hex(u);
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}

pub fn parse_uuid(s: &str) -> Option<[u8; 16]> {
    let h: String = s
        .trim()
        .trim_matches(|c| c == '{' || c == '}')
        .chars()
        .filter(|c| *c != '-')
        .collect();
    if h.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(h.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(out)
}

/// PostgreSQL `bytea` input: `\x0102` hex or escape format; anything else is taken as bytes.
pub fn parse_bytea(s: &str) -> Vec<u8> {
    if let Some(h) = s.strip_prefix("\\x") {
        if h.len() % 2 == 0 {
            if let Some(v) = (0..h.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&h[i..i + 2], 16).ok())
                .collect()
            {
                return v;
            }
        }
    }
    s.as_bytes().to_vec()
}

/// `HH:MM[:SS[.fff]]`.
pub fn parse_time(s: &str) -> Option<i64> {
    let s = s.split(['+', 'Z']).next()?.trim();
    let mut it = s.split(':');
    let h: i64 = it.next()?.trim().parse().ok()?;
    let m: i64 = it.next()?.trim().parse().ok()?;
    let (sec, frac) = match it.next() {
        Some(x) => {
            let (a, b) = x.split_once('.').unwrap_or((x, ""));
            (a.trim().parse::<i64>().ok()?, b)
        }
        None => (0, ""),
    };
    if h > 24 || m > 59 || sec > 60 {
        return None;
    }
    let mut frac_ns = 0i64;
    for (i, c) in frac.chars().take(9).enumerate() {
        frac_ns += i64::from(c.to_digit(10)?) * 10i64.pow(8 - i as u32);
    }
    Some(((h * 60 + m) * 60 + sec) * NS_PER_SEC + frac_ns)
}

/// `YYYY-MM-DD[( |T)HH:MM[:SS[.fff]]][Z|±HH[:MM]]` (and `now` / `epoch`) → UTC nanoseconds.
pub fn parse_timestamp(s: &str) -> Option<i64> {
    let s = s.trim();
    match s.to_ascii_lowercase().as_str() {
        "epoch" => return Some(0),
        "now" | "today" => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()?
                .as_nanos() as i64;
            return Some(if s.eq_ignore_ascii_case("today") {
                now - now.rem_euclid(NS_PER_DAY)
            } else {
                now
            });
        }
        _ => {}
    }
    let (date, rest) = match s.find(['T', ' ', 't']) {
        Some(i) => (&s[..i], s[i + 1..].trim()),
        None => (s, ""),
    };
    let mut dp = date.splitn(3, '-');
    let y: i32 = dp.next()?.parse().ok()?;
    let m: u32 = dp.next()?.parse().ok()?;
    let d: u32 = dp.next()?.parse().ok()?;
    if !(1..=12).contains(&m) || d == 0 || d > days_in_month(y, m) {
        return None;
    }
    let mut ns = i64::from(days_from_civil(y, m, d)) * NS_PER_DAY;
    if rest.is_empty() {
        return Some(ns);
    }
    // Split off a zone: Z, +hh, +hh:mm, -hhmm, or " UTC".
    let rest = rest.trim_end_matches(" UTC").trim_end_matches(" utc");
    let (time, offset) = if let Some(t) = rest.strip_suffix(['Z', 'z']) {
        (t, 0)
    } else if let Some(i) = rest.rfind(['+', '-']).filter(|i| *i >= 5) {
        let z = &rest[i + 1..];
        let (hh, mm) = if let Some((a, b)) = z.split_once(':') {
            (a.parse::<i64>().ok()?, b.parse::<i64>().ok()?)
        } else if z.len() == 4 {
            (z[..2].parse::<i64>().ok()?, z[2..].parse::<i64>().ok()?)
        } else {
            (z.parse::<i64>().ok()?, 0)
        };
        let sign = if &rest[i..=i] == "-" { -1 } else { 1 };
        (&rest[..i], sign * (hh * 3600 + mm * 60) * NS_PER_SEC)
    } else {
        (rest, 0)
    };
    ns += parse_time(time.trim())?;
    Some(ns - offset)
}

/// `1 day`, `2 hours 30 minutes`, `1 year 2 mons 3 days 04:05:06`, `P1D` is not supported.
pub fn parse_interval(s: &str) -> Option<Datum> {
    let mut months = 0i64;
    let mut days = 0i64;
    let mut ns = 0i64;
    let toks: Vec<&str> = s.split_whitespace().collect();
    let mut i = 0;
    while i < toks.len() {
        let t = toks[i];
        if t.contains(':') {
            let neg = t.starts_with('-');
            let v = parse_time(t.trim_start_matches(['-', '+']))?;
            ns += if neg { -v } else { v };
            i += 1;
            continue;
        }
        let n: f64 = t.parse().ok()?;
        let unit = toks
            .get(i + 1)
            .map(|u| u.to_ascii_lowercase())
            .unwrap_or_else(|| "second".into());
        let unit = unit.trim_end_matches(',').trim_end_matches('s');
        match unit {
            "year" | "yr" | "y" => months += (n * 12.0) as i64,
            "mon" | "month" => months += n as i64,
            "week" | "w" => days += (n * 7.0) as i64,
            "day" | "d" => days += n as i64,
            "hour" | "hr" | "h" => ns += (n * 3600.0 * 1e9) as i64,
            "minute" | "min" | "m" => ns += (n * 60.0 * 1e9) as i64,
            "second" | "sec" | "" => ns += (n * 1e9) as i64,
            "millisecond" | "msec" | "ms" => ns += (n * 1e6) as i64,
            "microsecond" | "u" => ns += (n * 1e3) as i64,
            "quarter" => months += (n * 3.0) as i64,
            _ => return None,
        }
        i += 2;
    }
    Some(Datum::Interval { months, days, ns })
}

/// PostgreSQL array literal `{a,"b c",NULL}` (one dimension).
pub fn parse_array(s: &str) -> SqlResult<Vec<Datum>> {
    let t = s.trim();
    // int2vector / oidvector text ("1 3"), as pg_index.indkey reads.
    if !t.is_empty() && t.bytes().all(|b| b.is_ascii_digit() || b == b' ') {
        return Ok(t
            .split_whitespace()
            .map(|n| n.parse().map_or(Datum::Null, Datum::Int))
            .collect());
    }
    let inner = t
        .strip_prefix('{')
        .and_then(|x| x.strip_suffix('}'))
        .ok_or_else(|| SqlError::invalid_text(format!("malformed array literal: \"{s}\"")))?;
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    let mut was_quoted = false;
    let mut chars = inner.chars().peekable();
    if inner.trim().is_empty() {
        return Ok(out);
    }
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                quoted = !quoted;
                was_quoted = true;
            }
            '\\' if quoted => {
                if let Some(n) = chars.next() {
                    cur.push(n);
                }
            }
            ',' if !quoted => {
                out.push(array_item(&cur, was_quoted));
                cur.clear();
                was_quoted = false;
            }
            c => cur.push(c),
        }
    }
    out.push(array_item(&cur, was_quoted));
    Ok(out)
}

fn array_item(s: &str, quoted: bool) -> Datum {
    if !quoted && s.trim().eq_ignore_ascii_case("null") {
        Datum::Null
    } else if quoted {
        Datum::Text(s.to_owned())
    } else {
        Datum::Text(s.trim().to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> Datum {
        Datum::Numeric(parse_dec(s).unwrap())
    }

    #[test]
    fn exact_money_arithmetic() {
        assert_eq!(arith('+', &dec("0.1"), &dec("0.2")).unwrap(), dec("0.3"));
        assert_eq!(
            arith('-', &dec("100.00"), &Datum::Int(25))
                .unwrap()
                .as_text(),
            "75.00"
        );
        assert_eq!(
            arith('*', &dec("19.99"), &Datum::Int(3)).unwrap().as_text(),
            "59.97"
        );
        assert_eq!(
            arith('/', &Datum::Int(7), &Datum::Int(2)).unwrap(),
            Datum::Int(3)
        );
        assert_eq!(arith('/', &dec("10"), &dec("4")).unwrap().as_text(), "2.5");
        assert!(arith('/', &Datum::Int(1), &Datum::Int(0)).is_err());
        assert!(arith('+', &Datum::Int(i64::MAX), &Datum::Int(1)).is_err());
        assert_eq!(
            cast(
                &Datum::Text("12.345".into()),
                &DataType::Numeric(Some((10, 2)))
            )
            .unwrap()
            .as_text(),
            "12.35"
        );
        assert!(cast(
            &Datum::Text("123456".into()),
            &DataType::Numeric(Some((5, 2)))
        )
        .is_err());
        assert_eq!(
            cast(&Datum::Float(0.1), &DataType::Numeric(None))
                .unwrap()
                .as_text(),
            "0.1"
        );
    }

    #[test]
    fn comparisons_and_coercion() {
        assert_eq!(sql_cmp(&Datum::Int(5), &dec("5.00")), Some(Ordering::Equal));
        assert_eq!(
            sql_cmp(&Datum::Text("5".into()), &Datum::Int(5)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            sql_cmp(&Datum::Float(1.5), &dec("1.5")),
            Some(Ordering::Equal)
        );
        assert_eq!(sql_cmp(&Datum::Null, &Datum::Int(1)), None);
        assert_eq!(compare(&Datum::Null, &Datum::Int(1)), Ordering::Greater);
        assert_eq!(hash_key(&Datum::Int(2)), hash_key(&dec("2.0")));
        let ts = parse_timestamp("2024-05-01 12:30:00").unwrap();
        assert_eq!(
            sql_cmp(
                &Datum::Timestamp(ts),
                &Datum::Text("2024-05-01T12:30:00Z".into())
            ),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn dates_and_formats() {
        let ts = parse_timestamp("2024-02-29T23:59:58.5+01:00").unwrap();
        assert_eq!(format_timestamp(ts, false), "2024-02-29 22:59:58.5");
        assert_eq!(format_date(days_from_civil(1970, 1, 1)), "1970-01-01");
        assert_eq!(
            civil_from_days(days_from_civil(2000, 12, 31)),
            (2000, 12, 31)
        );
        let next = arith(
            '+',
            &Datum::Date(days_from_civil(2024, 1, 31)),
            &parse_interval("1 month").unwrap(),
        )
        .unwrap();
        assert_eq!(next.as_text(), "2024-02-29");
        assert_eq!(
            parse_interval("1 day 02:00:00").unwrap().as_text(),
            "1 day 02:00:00"
        );
        assert_eq!(format_float(0.1), "0.1");
        assert_eq!(format_float(1e20), "1e+20");
        assert_eq!(Datum::Bytes(vec![1, 255]).as_text(), "\\x01ff");
        assert_eq!(parse_bytea("\\x01ff"), vec![1, 255]);
        assert_eq!(
            parse_array("{1,\"a b\",NULL}").unwrap(),
            vec![
                Datum::Text("1".into()),
                Datum::Text("a b".into()),
                Datum::Null
            ]
        );
        assert_eq!(
            Datum::Array(vec![Datum::Int(1), Datum::Text("a b".into())]).as_text(),
            "{1,\"a b\"}"
        );
        let u = parse_uuid("123e4567-e89b-12d3-a456-426614174000").unwrap();
        assert_eq!(format_uuid(&u), "123e4567-e89b-12d3-a456-426614174000");
    }

    #[test]
    fn storage_round_trip() {
        let ty = DataType::Numeric(Some((12, 2)));
        let v = to_value(&dec("10.50"), &ty);
        assert_eq!(from_value(&v, Some(&ty)), dec("10.50"));
        let d = Datum::Date(days_from_civil(2024, 5, 1));
        assert_eq!(
            from_value(&to_value(&d, &DataType::Date), Some(&DataType::Date)),
            d
        );
        let t = Datum::Time(parse_time("12:30:00.25").unwrap());
        assert_eq!(
            from_value(&to_value(&t, &DataType::Time), Some(&DataType::Time)),
            t
        );
    }
}
