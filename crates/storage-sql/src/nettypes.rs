//! Network and range types (`F-ENT-05`, `TASK-149`): `inet`, `cidr`, `macaddr`, and PostgreSQL's
//! built-in ranges. Values are their canonical text, validated on the way in, so they travel,
//! compare and display as PostgreSQL's text forms do.

use std::cmp::Ordering;
use std::net::IpAddr;

use crate::ast::{DataType, RangeKind};
use crate::datum::{compare, to_f64, Datum};
use crate::error::{SqlError, SqlResult};

fn bad(ty: &str, text: &str) -> SqlError {
    SqlError::new("22P02", format!("invalid input syntax for type {ty}: \"{text}\""))
}

/// An address and its prefix length.
fn parse_net(text: &str) -> Option<(IpAddr, u8)> {
    let (addr, prefix) = match text.split_once('/') {
        Some((a, p)) => (a, Some(p)),
        None => (text, None),
    };
    let ip: IpAddr = addr.parse().ok()?;
    let max = if ip.is_ipv4() { 32 } else { 128 };
    let bits = match prefix {
        Some(p) => p.parse::<u8>().ok().filter(|b| *b <= max)?,
        None => max,
    };
    Some((ip, bits))
}

fn host_bits_of(ip: &IpAddr, bits: u8) -> bool {
    let (value, width): (u128, u32) = match ip {
        IpAddr::V4(v) => (u128::from(u32::from(*v)), 32),
        IpAddr::V6(v) => (u128::from(*v), 128),
    };
    let host = width - u32::from(bits);
    host > 0 && (value & ((1u128 << host.min(127)) - 1)) != 0
}

fn network_value(ip: &IpAddr, bits: u8) -> (bool, u128) {
    let (value, width): (u128, u32) = match ip {
        IpAddr::V4(v) => (u128::from(u32::from(*v)), 32),
        IpAddr::V6(v) => (u128::from(*v), 128),
    };
    let host = width - u32::from(bits);
    let masked = if host >= 128 { 0 } else { (value >> host) << host };
    (ip.is_ipv4(), masked)
}

/// `inet`: an address with an optional prefix (`/32` is not shown).
pub fn canon_inet(text: &str) -> SqlResult<String> {
    let (ip, bits) = parse_net(text.trim()).ok_or_else(|| bad("inet", text))?;
    let max = if ip.is_ipv4() { 32 } else { 128 };
    Ok(if bits == max { ip.to_string() } else { format!("{ip}/{bits}") })
}

/// `cidr`: a network; bits beyond the prefix are refused.
pub fn canon_cidr(text: &str) -> SqlResult<String> {
    let (ip, bits) = parse_net(text.trim()).ok_or_else(|| bad("cidr", text))?;
    if host_bits_of(&ip, bits) {
        return Err(SqlError::new("22P02", format!("invalid cidr value: \"{text}\": value has bits set to right of mask")));
    }
    Ok(format!("{ip}/{bits}"))
}

/// `macaddr`: six bytes, any of the usual separators, written `08:00:2b:01:02:03`.
pub fn canon_macaddr(text: &str) -> SqlResult<String> {
    let digits: String = text.trim().chars().filter(|c| !matches!(c, ':' | '-' | '.')).collect();
    if digits.len() != 12 || !digits.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(bad("macaddr", text));
    }
    let lower = digits.to_ascii_lowercase();
    Ok((0..6).map(|i| &lower[i * 2..i * 2 + 2]).collect::<Vec<_>>().join(":"))
}

/// `a << b` (`or_equal`: `<<=`): is network `a` inside network `b`?
pub fn net_within(a: &str, b: &str, or_equal: bool) -> SqlResult<bool> {
    let (ia, ba) = parse_net(a).ok_or_else(|| bad("inet", a))?;
    let (ib, bb) = parse_net(b).ok_or_else(|| bad("inet", b))?;
    if ia.is_ipv4() != ib.is_ipv4() {
        return Ok(false);
    }
    if ba < bb || (ba == bb && !or_equal) {
        return Ok(false);
    }
    Ok(network_value(&ia, bb) == network_value(&ib, bb))
}

/// `host`, `masklen`, `network`, `broadcast` / `family` of an address, when `name` is one of them.
pub fn net_function(name: &str, text: &str) -> SqlResult<Option<Datum>> {
    if !matches!(name, "host" | "masklen" | "family" | "network" | "netmask") {
        return Ok(None);
    }
    let (ip, bits) = parse_net(text).ok_or_else(|| bad("inet", text))?;
    Ok(Some(match name {
        "host" => Datum::Text(ip.to_string()),
        "masklen" => Datum::Int(i64::from(bits)),
        "family" => Datum::Int(if ip.is_ipv4() { 4 } else { 6 }),
        "network" => {
            let (v4, value) = network_value(&ip, bits);
            let addr: IpAddr = if v4 { IpAddr::V4(std::net::Ipv4Addr::from(value as u32)) } else { IpAddr::V6(std::net::Ipv6Addr::from(value)) };
            Datum::Text(format!("{addr}/{bits}"))
        }
        _ => {
            let width = if ip.is_ipv4() { 32 } else { 128 };
            let host = width - u32::from(bits);
            let mask: u128 = if host >= 128 { 0 } else { (u128::MAX >> (128 - width)) >> host << host };
            let addr: IpAddr = if ip.is_ipv4() { IpAddr::V4(std::net::Ipv4Addr::from(mask as u32)) } else { IpAddr::V6(std::net::Ipv6Addr::from(mask)) };
            Datum::Text(addr.to_string())
        }
    }))
}

/// A parsed range. `lo` / `hi` of `None`: unbounded.
#[derive(Debug, Clone, PartialEq)]
pub struct Range {
    pub empty: bool,
    pub lo: Option<Datum>,
    pub hi: Option<Datum>,
    pub lo_inclusive: bool,
    pub hi_inclusive: bool,
}

fn bound(kind: RangeKind, text: &str) -> SqlResult<Datum> {
    let t = text.trim().trim_matches('"');
    let ty = match kind {
        RangeKind::Int4 | RangeKind::Int8 => DataType::BigInt,
        RangeKind::Num => DataType::Numeric(None),
        RangeKind::Ts => DataType::Timestamp,
        RangeKind::TsTz => DataType::TimestampTz,
        RangeKind::Date => DataType::Date,
    };
    crate::datum::cast(&Datum::Text(t.to_owned()), &ty)
}

fn bound_text(d: &Datum) -> String {
    let t = d.as_text();
    if t.contains([' ', ',']) {
        format!("\"{t}\"")
    } else {
        t
    }
}

/// Parses `[1,5)`, `(,3]`, `empty`.
pub fn parse_range(kind: RangeKind, text: &str) -> SqlResult<Range> {
    let t = text.trim();
    if t.eq_ignore_ascii_case("empty") {
        return Ok(Range { empty: true, lo: None, hi: None, lo_inclusive: false, hi_inclusive: false });
    }
    let malformed = || SqlError::new("22P02", format!("malformed range literal: \"{text}\""));
    let lo_inclusive = match t.chars().next() {
        Some('[') => true,
        Some('(') => false,
        _ => return Err(malformed()),
    };
    let hi_inclusive = match t.chars().last() {
        Some(']') => true,
        Some(')') => false,
        _ => return Err(malformed()),
    };
    let inner = &t[1..t.len() - 1];
    let (a, b) = inner.split_once(',').ok_or_else(malformed)?;
    let value = |s: &str| -> SqlResult<Option<Datum>> {
        if s.trim().is_empty() {
            Ok(None)
        } else {
            bound(kind, s).map(Some)
        }
    };
    normalize(kind, value(a)?, value(b)?, lo_inclusive, hi_inclusive)
}

/// Discrete ranges take the form `[a,b)`, as PostgreSQL shows them; a range with nothing in it is
/// `empty`; bounds out of order are an error.
pub fn normalize(kind: RangeKind, mut lo: Option<Datum>, mut hi: Option<Datum>, mut lo_inclusive: bool, mut hi_inclusive: bool) -> SqlResult<Range> {
    if let (Some(a), Some(b)) = (&lo, &hi) {
        if compare(a, b) == Ordering::Greater {
            return Err(SqlError::new("22000", "range lower bound must be less than or equal to range upper bound"));
        }
    }
    let discrete = matches!(kind, RangeKind::Int4 | RangeKind::Int8 | RangeKind::Date);
    let step = |d: &Datum, by: i64| -> Option<Datum> {
        match d {
            Datum::Int(n) => n.checked_add(by).map(Datum::Int),
            Datum::Date(n) => n.checked_add(by as i32).map(Datum::Date),
            _ => None,
        }
    };
    if discrete {
        if !lo_inclusive {
            lo = lo.as_ref().and_then(|d| step(d, 1)).or(lo);
            lo_inclusive = true;
        }
        if hi_inclusive {
            hi = hi.as_ref().and_then(|d| step(d, 1)).or(hi);
            hi_inclusive = false;
        }
    }
    if lo.is_none() {
        lo_inclusive = false;
    }
    if hi.is_none() {
        hi_inclusive = false;
    }
    let empty = match (&lo, &hi) {
        (Some(a), Some(b)) => compare(a, b) == Ordering::Equal && !(lo_inclusive && hi_inclusive),
        _ => false,
    };
    if empty {
        return Ok(Range { empty: true, lo: None, hi: None, lo_inclusive: false, hi_inclusive: false });
    }
    Ok(Range { empty: false, lo, hi, lo_inclusive, hi_inclusive })
}

pub fn range_text(r: &Range) -> String {
    if r.empty {
        return "empty".into();
    }
    format!("{}{},{}{}", if r.lo_inclusive { '[' } else { '(' }, r.lo.as_ref().map(bound_text).unwrap_or_default(), r.hi.as_ref().map(bound_text).unwrap_or_default(), if r.hi_inclusive { ']' } else { ')' })
}

/// `[kind]range(a, b [, '[)'])`.
pub fn construct(kind: RangeKind, args: &[Datum]) -> SqlResult<Datum> {
    let spec = args.get(2).map_or_else(|| "[)".to_owned(), Datum::as_text);
    if !matches!(spec.as_str(), "[)" | "[]" | "()" | "(]") {
        return Err(SqlError::new("42601", "invalid range bound flags"));
    }
    let value = |d: Option<&Datum>| -> SqlResult<Option<Datum>> {
        match d {
            None | Some(Datum::Null) => Ok(None),
            Some(v) => bound(kind, &v.as_text()).map(Some),
        }
    };
    let r = normalize(kind, value(args.first())?, value(args.get(1))?, spec.starts_with('['), spec.ends_with(']'))?;
    Ok(Datum::Text(range_text(&r)))
}

fn before_lo(x: &Datum, r: &Range) -> bool {
    r.lo.as_ref().is_some_and(|lo| match compare(x, lo) {
        Ordering::Less => true,
        Ordering::Equal => !r.lo_inclusive,
        Ordering::Greater => false,
    })
}

fn after_hi(x: &Datum, r: &Range) -> bool {
    r.hi.as_ref().is_some_and(|hi| match compare(x, hi) {
        Ordering::Greater => true,
        Ordering::Equal => !r.hi_inclusive,
        Ordering::Less => false,
    })
}

/// `range @> element`.
pub fn contains_element(r: &Range, x: &Datum) -> bool {
    !r.empty && !before_lo(x, r) && !after_hi(x, r)
}

/// `a @> b`.
pub fn contains_range(a: &Range, b: &Range) -> bool {
    if b.empty {
        return true;
    }
    if a.empty {
        return false;
    }
    let low_ok = match (&a.lo, &b.lo) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(x), Some(y)) => match compare(x, y) {
            Ordering::Less => true,
            Ordering::Equal => a.lo_inclusive || !b.lo_inclusive,
            Ordering::Greater => false,
        },
    };
    let high_ok = match (&a.hi, &b.hi) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(x), Some(y)) => match compare(x, y) {
            Ordering::Greater => true,
            Ordering::Equal => a.hi_inclusive || !b.hi_inclusive,
            Ordering::Less => false,
        },
    };
    low_ok && high_ok
}

/// `a && b`.
pub fn overlaps(a: &Range, b: &Range) -> bool {
    if a.empty || b.empty {
        return false;
    }
    let ends_before = |x: &Range, y: &Range| match (&x.hi, &y.lo) {
        (Some(h), Some(l)) => match compare(h, l) {
            Ordering::Less => true,
            Ordering::Equal => !(x.hi_inclusive && y.lo_inclusive),
            Ordering::Greater => false,
        },
        _ => false,
    };
    !ends_before(a, b) && !ends_before(b, a)
}

/// The range kind of text that reads as a range of numbers, for operators given plain text: `None`
/// when it is no range (JSON and arrays use the same operators).
pub fn guess_range(text: &str) -> Option<Range> {
    let t = text.trim();
    if t.eq_ignore_ascii_case("empty") {
        return parse_range(RangeKind::Num, t).ok();
    }
    if !matches!(t.chars().next(), Some('[' | '(')) || !matches!(t.chars().last(), Some(']' | ')')) || !t.contains(',') {
        return None;
    }
    // `[1,2]` is also JSON; ranges of dates and times hold `-` or `:`.
    let inner = &t[1..t.len() - 1];
    let parts: Vec<&str> = inner.split(',').collect();
    if parts.len() != 2 {
        return None;
    }
    let numeric = parts.iter().all(|p| p.trim().is_empty() || p.trim().parse::<f64>().is_ok());
    let kind = if numeric { RangeKind::Num } else if inner.contains(':') { RangeKind::Ts } else { RangeKind::Date };
    parse_range(kind, t).ok()
}

/// `true` when a number stands against a range bound's type (`@> 3`).
pub fn is_element(d: &Datum) -> bool {
    to_f64(d).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses() {
        assert_eq!(canon_inet("192.168.0.1/32").unwrap(), "192.168.0.1");
        assert_eq!(canon_inet("192.168.0.1/24").unwrap(), "192.168.0.1/24");
        assert_eq!(canon_inet("::1").unwrap(), "::1");
        assert_eq!(canon_cidr("10.0.0.0/8").unwrap(), "10.0.0.0/8");
        assert_eq!(canon_cidr("10.1.2.3").unwrap(), "10.1.2.3/32");
        assert!(canon_cidr("10.0.0.1/8").is_err());
        for bad in ["", "1.2.3", "1.2.3.4/33", "x", "1.2.3.4/-1", "::1/129"] {
            assert!(canon_inet(bad).is_err(), "{bad}");
        }
        assert_eq!(canon_macaddr("08-00-2B-01-02-03").unwrap(), "08:00:2b:01:02:03");
        assert_eq!(canon_macaddr("0800.2b01.0203").unwrap(), "08:00:2b:01:02:03");
        assert!(canon_macaddr("08:00:2b").is_err());
        assert!(net_within("10.1.2.3", "10.0.0.0/8", false).unwrap());
        assert!(!net_within("10.0.0.0/8", "10.0.0.0/8", false).unwrap());
        assert!(net_within("10.0.0.0/8", "10.0.0.0/8", true).unwrap());
        assert!(!net_within("11.0.0.1", "10.0.0.0/8", true).unwrap());
        assert!(!net_within("::1", "10.0.0.0/8", true).unwrap());
        assert!(net_within("1.2.3.4", "0.0.0.0/0", false).unwrap());
    }

    #[test]
    fn ranges_take_their_canonical_form() {
        let c = |t: &str| range_text(&parse_range(RangeKind::Int4, t).unwrap());
        assert_eq!(c("[1,5]"), "[1,6)");
        assert_eq!(c("(1,5)"), "[2,5)");
        assert_eq!(c("(,5)"), "(,5)");
        assert_eq!(c("[3,3)"), "empty");
        assert_eq!(c("empty"), "empty");
        assert!(parse_range(RangeKind::Int4, "[5,1)").is_err());
        assert!(parse_range(RangeKind::Int4, "1,5").is_err());
        assert_eq!(range_text(&parse_range(RangeKind::Num, "[1.5,2.5]").unwrap()), "[1.5,2.5]");
    }

    #[test]
    fn ranges_answer_questions() {
        let r = |t: &str| parse_range(RangeKind::Int4, t).unwrap();
        assert!(contains_element(&r("[1,5)"), &Datum::Int(1)));
        assert!(!contains_element(&r("[1,5)"), &Datum::Int(5)));
        assert!(contains_element(&r("(,5)"), &Datum::Int(-100)));
        assert!(!contains_element(&r("empty"), &Datum::Int(1)));
        assert!(contains_range(&r("[1,10)"), &r("[2,5)")));
        assert!(!contains_range(&r("[2,5)"), &r("[1,10)")));
        assert!(contains_range(&r("[2,5)"), &r("empty")));
        assert!(contains_range(&r("(,)"), &r("[1,2)")));
        assert!(overlaps(&r("[1,5)"), &r("[4,8)")));
        assert!(!overlaps(&r("[1,5)"), &r("[5,8)")), "touching is not overlapping");
        assert!(!overlaps(&r("empty"), &r("(,)")));
    }
}
