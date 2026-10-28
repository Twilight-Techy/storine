//! Values and their wire encoding (protocol/PROTOCOL.md §3).

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::{invalid, Result};

/// A document's fields. `BTreeMap` keeps keys in byte order, which is the wire order.
pub type Doc = BTreeMap<String, Value>;

/// One stored value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Int128(i128),
    /// Nanoseconds since the Unix epoch.
    Timestamp(i64),
    Uuid([u8; 16]),
    Id([u8; 24]),
    Text(String),
    Bytes(Vec<u8>),
    Decimal(Decimal),
    List(Vec<Value>),
    Map(Doc),
}

impl Value {
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// The value as an `i64` (from int, uint or int128 when it fits).
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            Value::UInt(v) => i64::try_from(*v).ok(),
            Value::Int128(v) => i64::try_from(*v).ok(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_decimal(&self) -> Option<Decimal> {
        match self {
            Value::Decimal(d) => Some(*d),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_map(&self) -> Option<&Doc> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }
}

macro_rules! from_int {
    ($($t:ty),*) => {$(
        impl From<$t> for Value {
            fn from(v: $t) -> Self {
                Value::Int(i64::from(v))
            }
        }
    )*};
}
from_int!(i8, i16, i32, i64, u8, u16, u32);

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::UInt(v)
    }
}

impl From<i128> for Value {
    /// Stored as int64 when it fits, otherwise int128.
    fn from(v: i128) -> Self {
        i64::try_from(v).map(Value::Int).unwrap_or(Value::Int128(v))
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::Float(f64::from(v))
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.to_owned())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Text(v)
    }
}

impl From<Decimal> for Value {
    fn from(v: Decimal) -> Self {
        Value::Decimal(v)
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Bytes(v)
    }
}

impl From<Doc> for Value {
    fn from(v: Doc) -> Self {
        Value::Map(v)
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        v.map(Into::into).unwrap_or(Value::Null)
    }
}

impl From<Vec<Value>> for Value {
    fn from(v: Vec<Value>) -> Self {
        Value::List(v)
    }
}

impl From<SystemTime> for Value {
    /// Nanoseconds since the epoch (saturating outside years 1677-2262).
    fn from(t: SystemTime) -> Self {
        let ns = match t.duration_since(UNIX_EPOCH) {
            Ok(d) => i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
            Err(e) => i64::try_from(e.duration().as_nanos()).map(|n| -n).unwrap_or(i64::MIN),
        };
        Value::Timestamp(ns)
    }
}

impl Value {
    /// A timestamp value as a `SystemTime`.
    pub fn as_system_time(&self) -> Option<SystemTime> {
        match self {
            Value::Timestamp(ns) if *ns >= 0 => Some(UNIX_EPOCH + Duration::from_nanos(*ns as u64)),
            Value::Timestamp(ns) => Some(UNIX_EPOCH - Duration::from_nanos(ns.unsigned_abs())),
            _ => None,
        }
    }
}

/// Builds a [`Doc`]: `doc! { "owner" => "Alice", "balance" => dec("100.00") }`.
#[macro_export]
macro_rules! doc {
    () => { $crate::Doc::new() };
    ($($key:expr => $value:expr),+ $(,)?) => {{
        let mut d = $crate::Doc::new();
        $( d.insert(::std::string::String::from($key), $crate::Value::from($value)); )+
        d
    }};
}

/// Builds a list value: `list![1, "a", true]`.
#[macro_export]
macro_rules! list {
    ($($value:expr),* $(,)?) => { $crate::Value::List(vec![$($crate::Value::from($value)),*]) };
}

/// An exact decimal number (money): `mantissa / 10^scale`, up to 38 digits. Never a binary float.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal {
    mantissa: i128,
    scale: u8,
}

const MAX_SCALE: u8 = 38;

impl Decimal {
    /// `mantissa / 10^scale`; at most 38 digits and 38 fractional digits.
    pub fn new(mantissa: i128, scale: u8) -> Result<Decimal> {
        if scale > MAX_SCALE {
            return Err(invalid("decimal scale must be at most 38"));
        }
        Ok(Decimal { mantissa, scale })
    }

    /// Parses "19.99", "-0.001", "1E+3". The scale is the number of fractional digits written.
    pub fn parse(text: &str) -> Result<Decimal> {
        let bad = || invalid(format!("not a decimal number: {text:?}"));
        let s = text.trim();
        let (s, neg) = match s.as_bytes().first() {
            Some(b'-') => (&s[1..], true),
            Some(b'+') => (&s[1..], false),
            _ => (s, false),
        };
        let (num, exp) = match s.find(['e', 'E']) {
            Some(i) => (&s[..i], s[i + 1..].parse::<i32>().map_err(|_| bad())?),
            None => (s, 0),
        };
        let (int, frac) = num.split_once('.').unwrap_or((num, ""));
        if (int.is_empty() && frac.is_empty()) || !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) {
            return Err(bad());
        }
        let digits = format!("{int}{frac}");
        let digits = digits.trim_start_matches('0');
        if digits.is_empty() {
            return Ok(Decimal { mantissa: 0, scale: frac.len().min(MAX_SCALE as usize) as u8 });
        }
        let mut scale = frac.len() as i64 - i64::from(exp);
        if !(-40..=200).contains(&scale) {
            return Err(invalid(format!("decimal out of range: {text:?}")));
        }
        let mut digits = digits.to_owned();
        while scale < 0 {
            digits.push('0');
            scale += 1;
        }
        while scale > i64::from(MAX_SCALE) && digits.ends_with('0') {
            digits.pop();
            scale -= 1;
        }
        if scale > i64::from(MAX_SCALE) {
            return Err(invalid(format!("decimals have at most 38 fractional digits: {text:?}")));
        }
        let mantissa: i128 =
            digits.parse().map_err(|_| invalid(format!("decimal does not fit in 38 digits: {text:?}")))?;
        Ok(Decimal { mantissa: if neg { -mantissa } else { mantissa }, scale: scale as u8 })
    }

    pub fn mantissa(&self) -> i128 {
        self.mantissa
    }

    pub fn scale(&self) -> u8 {
        self.scale
    }
}

/// [`Decimal::parse`] that panics on invalid input; for literals: `dec("19.99")`.
pub fn dec(text: &str) -> Decimal {
    match Decimal::parse(text) {
        Ok(d) => d,
        Err(e) => panic!("{e}"),
    }
}

impl FromStr for Decimal {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Decimal> {
        Decimal::parse(s)
    }
}

impl fmt::Display for Decimal {
    /// Plain notation with exactly `scale` fractional digits.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = self.mantissa.unsigned_abs().to_string();
        let scale = self.scale as usize;
        if scale > 0 {
            if s.len() <= scale {
                s = "0".repeat(scale - s.len() + 1) + &s;
            }
            s.insert(s.len() - scale, '.');
        }
        if self.mantissa < 0 {
            s.insert(0, '-');
        }
        f.write_str(&s)
    }
}

// ── encoding ──

const NULL: u8 = 0;
const BOOL: u8 = 1;
const INT64: u8 = 2;
const UINT64: u8 = 3;
const FLOAT64: u8 = 4;
const INT128: u8 = 5;
const TIMESTAMP: u8 = 6;
const UUID: u8 = 7;
const ID: u8 = 8;
const TEXT: u8 = 9;
const BYTES: u8 = 10;
const DECIMAL: u8 = 11;
const LIST: u8 = 12;
const MAP: u8 = 13;
pub(crate) const MAX_DEPTH: usize = 32;

pub(crate) fn put_u32(out: &mut Vec<u8>, v: usize) -> Result<()> {
    let v = u32::try_from(v).map_err(|_| invalid("value too large"))?;
    out.extend_from_slice(&v.to_le_bytes());
    Ok(())
}

pub(crate) fn put_str(out: &mut Vec<u8>, s: &str) -> Result<()> {
    put_u32(out, s.len())?;
    out.extend_from_slice(s.as_bytes());
    Ok(())
}

pub(crate) fn encode_value(v: &Value, out: &mut Vec<u8>, depth: usize) -> Result<()> {
    if depth > MAX_DEPTH {
        return Err(invalid("value nesting is too deep (max 32)"));
    }
    match v {
        Value::Null => out.push(NULL),
        Value::Bool(b) => out.extend_from_slice(&[BOOL, u8::from(*b)]),
        Value::Int(n) => {
            out.push(INT64);
            out.extend_from_slice(&n.to_le_bytes());
        }
        Value::UInt(n) => {
            out.push(UINT64);
            out.extend_from_slice(&n.to_le_bytes());
        }
        Value::Float(x) => {
            out.push(FLOAT64);
            out.extend_from_slice(&x.to_bits().to_le_bytes());
        }
        Value::Int128(n) => {
            out.push(INT128);
            out.extend_from_slice(&n.to_le_bytes());
        }
        Value::Timestamp(ns) => {
            out.push(TIMESTAMP);
            out.extend_from_slice(&ns.to_le_bytes());
        }
        Value::Uuid(u) => {
            out.push(UUID);
            out.extend_from_slice(u);
        }
        Value::Id(id) => {
            out.push(ID);
            out.extend_from_slice(id);
        }
        Value::Text(s) => {
            out.push(TEXT);
            put_str(out, s)?;
        }
        Value::Bytes(b) => {
            out.push(BYTES);
            put_u32(out, b.len())?;
            out.extend_from_slice(b);
        }
        Value::Decimal(d) => {
            out.push(DECIMAL);
            out.extend_from_slice(&d.mantissa.to_le_bytes());
            out.push(d.scale);
        }
        Value::List(items) => {
            out.push(LIST);
            put_u32(out, items.len())?;
            for item in items {
                encode_value(item, out, depth + 1)?;
            }
        }
        Value::Map(m) => {
            out.push(MAP);
            encode_fields(m, out, depth + 1)?;
        }
    }
    Ok(())
}

/// `u32 count | (str key, value)*`; `BTreeMap<String, _>` iterates in byte order already.
pub(crate) fn encode_fields(fields: &Doc, out: &mut Vec<u8>, depth: usize) -> Result<()> {
    put_u32(out, fields.len())?;
    for (k, v) in fields {
        put_str(out, k)?;
        encode_value(v, out, depth)?;
    }
    Ok(())
}

/// The wire encoding of one value.
pub fn encode(v: &Value) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    encode_value(v, &mut out, 0)?;
    Ok(out)
}

/// Decodes exactly one value; `Error::Invalid` on malformed input.
pub fn decode(bytes: &[u8]) -> Result<Value> {
    let mut r = Reader::new(bytes);
    let v = r.value(0)?;
    if r.remaining() != 0 {
        return Err(invalid("trailing bytes after value"));
    }
    Ok(v)
}

/// Bounded reader: every count is checked against the bytes present before allocating.
pub(crate) struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(b: &'a [u8]) -> Self {
        Reader { b, pos: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.b.len() - self.pos
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.remaining() {
            return Err(invalid("truncated response"));
        }
        let s = &self.b[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut a = [0u8; N];
        a.copy_from_slice(self.take(N)?);
        Ok(a)
    }

    pub(crate) fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    pub(crate) fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    pub(crate) fn str(&mut self) -> Result<String> {
        let n = self.u32()? as usize;
        let raw = self.take(n)?;
        String::from_utf8(raw.to_vec()).map_err(|_| invalid("invalid UTF-8 in response"))
    }

    pub(crate) fn count(&mut self, min_size: usize) -> Result<usize> {
        let n = self.u32()? as usize;
        if n.saturating_mul(min_size.max(1)) > self.remaining() {
            return Err(invalid("truncated response"));
        }
        Ok(n)
    }

    pub(crate) fn value(&mut self, depth: usize) -> Result<Value> {
        Ok(match self.u8()? {
            NULL => Value::Null,
            BOOL => match self.u8()? {
                0 => Value::Bool(false),
                1 => Value::Bool(true),
                _ => return Err(invalid("bool byte must be 0 or 1")),
            },
            INT64 => Value::Int(i64::from_le_bytes(self.array()?)),
            UINT64 => Value::UInt(self.u64()?),
            FLOAT64 => Value::Float(f64::from_bits(self.u64()?)),
            INT128 => Value::Int128(i128::from_le_bytes(self.array()?)),
            TIMESTAMP => Value::Timestamp(i64::from_le_bytes(self.array()?)),
            UUID => Value::Uuid(self.array()?),
            ID => Value::Id(self.array()?),
            TEXT => Value::Text(self.str()?),
            BYTES => {
                let n = self.u32()? as usize;
                Value::Bytes(self.take(n)?.to_vec())
            }
            DECIMAL => {
                let m = i128::from_le_bytes(self.array()?);
                Value::Decimal(Decimal::new(m, self.u8()?)?)
            }
            LIST => {
                if depth >= MAX_DEPTH {
                    return Err(invalid("value nesting is too deep"));
                }
                let n = self.count(1)?;
                let mut items = Vec::with_capacity(n);
                for _ in 0..n {
                    items.push(self.value(depth + 1)?);
                }
                Value::List(items)
            }
            MAP => {
                if depth >= MAX_DEPTH {
                    return Err(invalid("value nesting is too deep"));
                }
                Value::Map(self.fields(depth + 1)?)
            }
            tag => return Err(invalid(format!("unknown value tag {tag}"))),
        })
    }

    pub(crate) fn fields(&mut self, depth: usize) -> Result<Doc> {
        let n = self.count(5)?;
        let mut doc = Doc::new();
        for _ in 0..n {
            let k = self.str()?;
            let v = self.value(depth)?;
            if doc.insert(k, v).is_some() {
                return Err(invalid("duplicate map key"));
            }
        }
        Ok(doc)
    }
}
