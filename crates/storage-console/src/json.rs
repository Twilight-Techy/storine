//! JSON text, and the mapping between JSON and Storine values.
//!
//! Plain JSON covers null, booleans, text, lists, maps and numbers that a browser holds
//! exactly. Every other value is a one-key object, so a document survives the round trip
//! through the console's editor unchanged:
//!
//! | Value | JSON |
//! | :--- | :--- |
//! | integer within ±2^53 | `42` |
//! | larger integer | `{"$int": "18446744073709551615"}` |
//! | float | `1.5`, `1.0` (always with a point or exponent); `{"$float": "NaN"}`, `"inf"`, `"-inf"` |
//! | decimal | `{"$decimal": "12.50"}` |
//! | timestamp | `{"$time": "2026-11-13T08:00:00.000000001Z"}` |
//! | bytes | `{"$bytes": "<base64>"}` |
//! | uuid | `{"$uuid": "0f8fad5b-d9cb-469f-a165-70867728950e"}` |
//! | id (24 bytes) | `{"$id": "<48 hex digits>"}` |
//!
//! Integer literals read back as integers and literals with a point or exponent as floats, so
//! `1` and `1.0` keep their kinds.

use std::fmt::Write as _;

use storine::{Decimal, Doc, Value};

/// A parsed JSON value. Numbers keep their literal text, so integers stay exact and `1.0`
/// stays a float; objects keep their key order.
#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Num(String),
    Str(String),
    Arr(Vec<Json>),
    Obj(Vec<(String, Json)>),
}

impl Json {
    /// The member `key` of an object.
    pub fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Obj(members) => members.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Json::Num(n) => n.parse().ok(),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Json]> {
        match self {
            Json::Arr(a) => Some(a),
            _ => None,
        }
    }

    pub fn str(s: impl Into<String>) -> Json {
        Json::Str(s.into())
    }

    pub fn num(n: impl std::fmt::Display) -> Json {
        Json::Num(n.to_string())
    }

    pub fn obj<const N: usize>(members: [(&str, Json); N]) -> Json {
        Json::Obj(members.into_iter().map(|(k, v)| (k.to_owned(), v)).collect())
    }
}

// ---------------------------------------------------------------------------------------------
// Text
// ---------------------------------------------------------------------------------------------

/// Deepest nesting accepted, so a hostile body cannot exhaust the stack.
const MAX_DEPTH: usize = 128;

/// Parses JSON text (RFC 8259).
pub fn parse(text: &str) -> Result<Json, String> {
    let mut p = Parser { s: text.as_bytes(), i: 0 };
    p.ws();
    let v = p.value(0)?;
    p.ws();
    if p.i != p.s.len() {
        return Err(p.err("unexpected text after the value"));
    }
    Ok(v)
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
}

impl Parser<'_> {
    fn err(&self, what: &str) -> String {
        let line = 1 + self.s[..self.i.min(self.s.len())].iter().filter(|&&b| b == b'\n').count();
        format!("JSON: {what} (line {line})")
    }

    fn ws(&mut self) {
        while self.i < self.s.len() && matches!(self.s[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }

    fn eat(&mut self, word: &str) -> bool {
        if self.s[self.i..].starts_with(word.as_bytes()) {
            self.i += word.len();
            true
        } else {
            false
        }
    }

    fn value(&mut self, depth: usize) -> Result<Json, String> {
        if depth > MAX_DEPTH {
            return Err(self.err("nested too deeply"));
        }
        match self.s.get(self.i) {
            None => Err(self.err("unexpected end")),
            Some(b'{') => {
                self.i += 1;
                let mut members = Vec::new();
                self.ws();
                if self.eat("}") {
                    return Ok(Json::Obj(members));
                }
                loop {
                    self.ws();
                    if self.s.get(self.i) != Some(&b'"') {
                        return Err(self.err("expected a quoted key"));
                    }
                    let key = self.string()?;
                    self.ws();
                    if !self.eat(":") {
                        return Err(self.err("expected ':'"));
                    }
                    self.ws();
                    let v = self.value(depth + 1)?;
                    if members.iter().any(|(k, _)| *k == key) {
                        return Err(self.err(&format!("duplicate key \"{key}\"")));
                    }
                    members.push((key, v));
                    self.ws();
                    if self.eat(",") {
                        continue;
                    }
                    if self.eat("}") {
                        return Ok(Json::Obj(members));
                    }
                    return Err(self.err("expected ',' or '}'"));
                }
            }
            Some(b'[') => {
                self.i += 1;
                let mut items = Vec::new();
                self.ws();
                if self.eat("]") {
                    return Ok(Json::Arr(items));
                }
                loop {
                    self.ws();
                    items.push(self.value(depth + 1)?);
                    self.ws();
                    if self.eat(",") {
                        continue;
                    }
                    if self.eat("]") {
                        return Ok(Json::Arr(items));
                    }
                    return Err(self.err("expected ',' or ']'"));
                }
            }
            Some(b'"') => self.string().map(Json::Str),
            Some(b't') if self.eat("true") => Ok(Json::Bool(true)),
            Some(b'f') if self.eat("false") => Ok(Json::Bool(false)),
            Some(b'n') if self.eat("null") => Ok(Json::Null),
            Some(b'-' | b'0'..=b'9') => self.number(),
            Some(_) => Err(self.err("unexpected character")),
        }
    }

    fn number(&mut self) -> Result<Json, String> {
        let start = self.i;
        let digits = |p: &mut Self| {
            let s = p.i;
            while p.i < p.s.len() && p.s[p.i].is_ascii_digit() {
                p.i += 1;
            }
            p.i > s
        };
        self.eat("-");
        if self.eat("0") {
            if self.s.get(self.i).is_some_and(u8::is_ascii_digit) {
                return Err(self.err("leading zero in a number"));
            }
        } else if !digits(self) {
            return Err(self.err("bad number"));
        }
        if self.eat(".") && !digits(self) {
            return Err(self.err("bad number"));
        }
        if self.eat("e") || self.eat("E") {
            let _ = self.eat("+") || self.eat("-");
            if !digits(self) {
                return Err(self.err("bad number"));
            }
        }
        Ok(Json::Num(String::from_utf8_lossy(&self.s[start..self.i]).into_owned()))
    }

    fn hex4(&mut self) -> Result<u32, String> {
        let h = self.s.get(self.i..self.i + 4).ok_or_else(|| self.err("bad \\u escape"))?;
        let v = u32::from_str_radix(std::str::from_utf8(h).unwrap_or("x"), 16).map_err(|_| self.err("bad \\u escape"))?;
        self.i += 4;
        Ok(v)
    }

    fn string(&mut self) -> Result<String, String> {
        self.i += 1; // opening quote
        let mut out = Vec::new();
        loop {
            let Some(&b) = self.s.get(self.i) else {
                return Err(self.err("unterminated string"));
            };
            self.i += 1;
            match b {
                b'"' => return String::from_utf8(out).map_err(|_| self.err("invalid UTF-8")),
                b'\\' => {
                    let Some(&e) = self.s.get(self.i) else {
                        return Err(self.err("unterminated string"));
                    };
                    self.i += 1;
                    let c = match e {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{8}',
                        b'f' => '\u{c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        b'u' => {
                            let hi = self.hex4()?;
                            let code = if (0xD800..0xDC00).contains(&hi) {
                                if !self.eat("\\u") {
                                    return Err(self.err("lone surrogate"));
                                }
                                let lo = self.hex4()?;
                                if !(0xDC00..0xE000).contains(&lo) {
                                    return Err(self.err("lone surrogate"));
                                }
                                0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
                            } else {
                                hi
                            };
                            char::from_u32(code).ok_or_else(|| self.err("lone surrogate"))?
                        }
                        _ => return Err(self.err("bad escape")),
                    };
                    let mut buf = [0u8; 4];
                    out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                }
                0..=0x1F => return Err(self.err("control character in a string")),
                _ => out.push(b),
            }
        }
    }
}

/// JSON text of `v`: compact, or indented by two spaces.
pub fn write(v: &Json, pretty: bool) -> String {
    let mut out = String::new();
    write_into(&mut out, v, pretty, 0);
    out
}

fn indent(out: &mut String, pretty: bool, level: usize) {
    if pretty {
        out.push('\n');
        for _ in 0..level {
            out.push_str("  ");
        }
    }
}

fn write_into(out: &mut String, v: &Json, pretty: bool, level: usize) {
    match v {
        Json::Null => out.push_str("null"),
        Json::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Json::Num(n) => out.push_str(n),
        Json::Str(s) => write_str(out, s),
        Json::Arr(items) => {
            if items.is_empty() {
                out.push_str("[]");
                return;
            }
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                indent(out, pretty, level + 1);
                write_into(out, item, pretty, level + 1);
            }
            indent(out, pretty, level);
            out.push(']');
        }
        Json::Obj(members) => {
            if members.is_empty() {
                out.push_str("{}");
                return;
            }
            out.push('{');
            for (i, (k, item)) in members.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                indent(out, pretty, level + 1);
                write_str(out, k);
                out.push_str(if pretty { ": " } else { ":" });
                write_into(out, item, pretty, level + 1);
            }
            indent(out, pretty, level);
            out.push('}');
        }
    }
}

fn write_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            // `<` too, so JSON never closes a <script> it might be embedded in.
            c if (c as u32) < 0x20 || c == '<' || c == '\u{2028}' || c == '\u{2029}' => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

// ---------------------------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------------------------

/// Largest integer a browser's number holds exactly.
const EXACT: i128 = 1 << 53;

fn int_json(v: i128) -> Json {
    if (-EXACT..=EXACT).contains(&v) {
        Json::num(v)
    } else {
        Json::obj([("$int", Json::str(v.to_string()))])
    }
}

/// JSON of a stored value.
pub fn from_value(v: &Value) -> Json {
    match v {
        Value::Null => Json::Null,
        Value::Bool(b) => Json::Bool(*b),
        Value::Int(i) => int_json(i128::from(*i)),
        Value::UInt(u) => int_json(i128::from(*u)),
        Value::Int128(i) => int_json(*i),
        Value::Float(f) if f.is_finite() => Json::Num(format!("{f:?}")),
        Value::Float(f) => Json::obj([("$float", Json::str(if f.is_nan() { "NaN" } else if *f > 0.0 { "inf" } else { "-inf" }))]),
        Value::Decimal(d) => Json::obj([("$decimal", Json::str(d.to_string()))]),
        Value::Timestamp(ns) => Json::obj([("$time", Json::str(format_time(*ns)))]),
        Value::Text(s) => Json::str(s.clone()),
        Value::Bytes(b) => Json::obj([("$bytes", Json::str(storage_codec::sha256::base64_encode(b)))]),
        Value::Uuid(u) => Json::obj([("$uuid", Json::str(format_uuid(u)))]),
        Value::Id(id) => Json::obj([("$id", Json::str(hex(id)))]),
        Value::List(items) => Json::Arr(items.iter().map(from_value).collect()),
        Value::Map(doc) => from_doc(doc),
    }
}

/// JSON object of a document's fields.
pub fn from_doc(doc: &Doc) -> Json {
    Json::Obj(doc.iter().map(|(k, v)| (k.clone(), from_value(v))).collect())
}

/// The stored value a JSON value stands for.
pub fn to_value(j: &Json) -> Result<Value, String> {
    Ok(match j {
        Json::Null => Value::Null,
        Json::Bool(b) => Value::Bool(*b),
        Json::Str(s) => Value::Text(s.clone()),
        Json::Num(n) => number(n)?,
        Json::Arr(items) => Value::List(items.iter().map(to_value).collect::<Result<_, _>>()?),
        Json::Obj(members) => {
            if let [(tag, Json::Str(text))] = members.as_slice() {
                if let Some(v) = tagged(tag, text)? {
                    return Ok(v);
                }
            }
            Value::Map(to_doc(j)?)
        }
    })
}

/// The fields of a document given as a JSON object.
pub fn to_doc(j: &Json) -> Result<Doc, String> {
    let Json::Obj(members) = j else {
        return Err("a document is a JSON object".into());
    };
    members.iter().map(|(k, v)| Ok((k.clone(), to_value(v)?))).collect()
}

fn number(n: &str) -> Result<Value, String> {
    if n.contains(['.', 'e', 'E']) {
        let f: f64 = n.parse().map_err(|_| format!("bad number {n}"))?;
        if !f.is_finite() {
            return Err(format!("{n} is out of a float's range"));
        }
        return Ok(Value::Float(f));
    }
    integer(n)
}

fn integer(n: &str) -> Result<Value, String> {
    if let Ok(i) = n.parse::<i64>() {
        Ok(Value::Int(i))
    } else if let Ok(u) = n.parse::<u64>() {
        Ok(Value::UInt(u))
    } else if let Ok(i) = n.parse::<i128>() {
        Ok(Value::Int128(i))
    } else {
        Err(format!("{n} is not an integer within 128 bits"))
    }
}

fn tagged(tag: &str, text: &str) -> Result<Option<Value>, String> {
    Ok(Some(match tag {
        "$int" => integer(text.trim())?,
        "$float" => Value::Float(match text {
            "NaN" => f64::NAN,
            "inf" => f64::INFINITY,
            "-inf" => f64::NEG_INFINITY,
            _ => text.parse().map_err(|_| format!("bad $float {text}"))?,
        }),
        "$decimal" => Value::Decimal(Decimal::parse(text).map_err(|e| format!("bad $decimal {text}: {e}"))?),
        "$time" => Value::Timestamp(parse_time(text).ok_or_else(|| format!("bad $time {text}: use 2026-11-13T08:00:00Z"))?),
        "$bytes" => Value::Bytes(storage_codec::sha256::base64_decode(text).ok_or_else(|| "bad $bytes: not base64".to_owned())?),
        "$uuid" => Value::Uuid(parse_hex::<16>(&text.replace('-', "")).ok_or_else(|| format!("bad $uuid {text}"))?),
        "$id" => Value::Id(parse_hex::<24>(text).ok_or_else(|| format!("bad $id {text}: 48 hex digits"))?),
        _ => return Ok(None),
    }))
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn parse_hex<const N: usize>(text: &str) -> Option<[u8; N]> {
    if text.len() != N * 2 || !text.is_ascii() {
        return None;
    }
    let mut out = [0u8; N];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

fn format_uuid(u: &[u8; 16]) -> String {
    let h = hex(u);
    format!("{}-{}-{}-{}-{}", &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32])
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Howard Hinnant's algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// RFC 3339 in UTC with as many fraction digits as needed (none, 3, 6 or 9).
pub fn format_time(ns: i64) -> String {
    let secs = ns.div_euclid(1_000_000_000);
    let frac = ns.rem_euclid(1_000_000_000);
    let (y, mo, d) = civil_from_days(secs.div_euclid(86_400));
    let sod = secs.rem_euclid(86_400);
    let mut s = format!("{y:04}-{mo:02}-{d:02}T{:02}:{:02}:{:02}", sod / 3600, sod / 60 % 60, sod % 60);
    if frac != 0 {
        let digits = format!("{frac:09}");
        let keep = if frac % 1_000_000 == 0 { 3 } else if frac % 1_000 == 0 { 6 } else { 9 };
        s.push('.');
        s.push_str(&digits[..keep]);
    }
    s.push('Z');
    s
}

/// Nanoseconds since the epoch of an RFC 3339 time (`Z` or a `±hh:mm` offset; a date alone is
/// midnight UTC).
pub fn parse_time(text: &str) -> Option<i64> {
    let t = text.trim();
    let num = |s: &str| -> Option<i64> { if s.bytes().all(|b| b.is_ascii_digit()) && !s.is_empty() { s.parse().ok() } else { None } };
    let (date, rest) = match t.find(['T', 't', ' ']) {
        Some(i) => (&t[..i], &t[i + 1..]),
        None => (t, ""),
    };
    let mut parts = date.splitn(3, '-');
    let (y, mo, d) = (num(parts.next()?)?, num(parts.next()?)?, num(parts.next()?)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
        return None;
    }
    let mut ns = days_from_civil(y, mo, d).checked_mul(86_400_000_000_000)?;
    if rest.is_empty() {
        return Some(ns);
    }
    let (clock, offset_secs) = if let Some(c) = rest.strip_suffix(['Z', 'z']) {
        (c, 0)
    } else {
        let i = rest.rfind(['+', '-'])?;
        let (h, m) = rest[i + 1..].split_once(':')?;
        let off = num(h)? * 3600 + num(m)? * 60;
        (&rest[..i], if &rest[i..=i] == "-" { -off } else { off })
    };
    let (hms, frac) = match clock.split_once('.') {
        Some((a, f)) => (a, f),
        None => (clock, ""),
    };
    let mut p = hms.splitn(3, ':');
    let (h, mi, s) = (num(p.next()?)?, num(p.next()?)?, num(p.next().unwrap_or("0"))?);
    if h > 23 || mi > 59 || s > 60 {
        return None;
    }
    let frac_ns = if frac.is_empty() {
        0
    } else {
        if frac.len() > 9 {
            return None;
        }
        num(frac)? * 10i64.pow(9 - frac.len() as u32)
    };
    ns = ns.checked_add(((h * 3600 + mi * 60 + s - offset_secs) * 1_000_000_000) + frac_ns)?;
    Some(ns)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(v: Value) {
        let text = write(&from_value(&v), true);
        let back = to_value(&parse(&text).unwrap()).unwrap();
        match (&v, &back) {
            (Value::Float(a), Value::Float(b)) if a.is_nan() => assert!(b.is_nan()),
            _ => assert_eq!(back, v, "{text}"),
        }
    }

    #[test]
    fn every_value_kind_survives_the_round_trip() {
        for v in [
            Value::Null,
            Value::Bool(true),
            Value::Int(-42),
            Value::Int(i64::MAX),
            Value::UInt(u64::MAX),
            Value::Int128(-(1i128 << 100)),
            Value::Float(1.0),
            Value::Float(-0.25e-300),
            Value::Float(f64::NAN),
            Value::Float(f64::NEG_INFINITY),
            Value::Decimal(Decimal::parse("-12.50").unwrap()),
            Value::Timestamp(1_763_020_800_123_456_789),
            Value::Timestamp(-1),
            Value::Text("é \"quoted\" </script> \u{1}".into()),
            Value::Bytes(vec![0, 1, 254, 255]),
            Value::Uuid([0x0f; 16]),
            Value::Id([7; 24]),
            Value::List(vec![Value::Int(1), Value::Text("a".into())]),
            Value::Map([("a".to_owned(), Value::Map([("b".to_owned(), Value::Float(2.0))].into()))].into()),
        ] {
            round_trip(v);
        }
    }

    #[test]
    fn integers_and_floats_keep_their_kinds() {
        assert_eq!(to_value(&parse("1").unwrap()).unwrap(), Value::Int(1));
        assert_eq!(to_value(&parse("1.0").unwrap()).unwrap(), Value::Float(1.0));
        assert_eq!(to_value(&parse("1e3").unwrap()).unwrap(), Value::Float(1000.0));
        assert_eq!(to_value(&parse("9007199254740993").unwrap()).unwrap(), Value::Int(9_007_199_254_740_993));
        assert_eq!(write(&from_value(&Value::Int(9_007_199_254_740_993)), false), r#"{"$int":"9007199254740993"}"#);
        assert_eq!(write(&from_value(&Value::Int(9_007_199_254_740_992)), false), "9007199254740992");
    }

    #[test]
    fn a_tagged_object_with_an_unknown_tag_is_a_map() {
        let v = to_value(&parse(r#"{"$other": "x"}"#).unwrap()).unwrap();
        assert_eq!(v, Value::Map([("$other".to_owned(), Value::Text("x".into()))].into()));
    }

    #[test]
    fn times_parse_with_offsets_and_dates() {
        assert_eq!(parse_time("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_time("1970-01-01"), Some(0));
        assert_eq!(parse_time("1970-01-01T01:00:00+01:00"), Some(0));
        assert_eq!(parse_time("2000-02-29T12:30:15.5Z"), Some(951_827_415_500_000_000));
        assert_eq!(format_time(951_827_415_500_000_000), "2000-02-29T12:30:15.500Z");
        assert_eq!(format_time(-1), "1969-12-31T23:59:59.999999999Z");
        assert_eq!(parse_time("2000-13-01"), None);
        assert_eq!(parse_time("yesterday"), None);
    }

    #[test]
    fn malformed_json_is_rejected_with_a_line() {
        for bad in ["", "{", "[1,]", "{\"a\":1,}", "01", "1.", "\"\\x\"", "{\"a\":1,\"a\":2}", "nul", "\"\u{1}\"", "1 2"] {
            assert!(parse(bad).is_err(), "{bad:?} parsed");
        }
        assert!(parse("{\n\n  \"a\" 1}").unwrap_err().contains("line 3"));
        let deep = "[".repeat(200) + &"]".repeat(200);
        assert!(parse(&deep).is_err());
        assert_eq!(parse(r#""\ud83d\ude00""#).unwrap(), Json::str("😀"));
    }
}
