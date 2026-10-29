//! Values, group keys and accumulators of analytic queries.

use core::cmp::Ordering;

use storage_codec::types::{PropertyValue, PropertyValueRef};
use storage_codec::xxh3_64;
use storage_columnar::Val;
use storage_types::{Decimal, Result, StorageError};

use crate::document::{compare_values, value_ref_of};

/// `10^n` for `n <= 38`.
pub(crate) fn pow10(n: u32) -> Option<i128> {
    10i128.checked_pow(n)
}

/// A group key component: canonical, so equal values (by the document equality rules that
/// form an equivalence: `1 == 1.0 == 1.00`) share one group.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum GroupValue {
    Null,
    Bool(bool),
    /// Every exact number (integers, `u64`, `i128`, decimals, integral floats within 2^53):
    /// mantissa with trailing zeros stripped, and its scale.
    Exact(i128, u8),
    /// Other floats by value (`-0.0` folded into `0.0`, every NaN one value).
    Float(u64),
    Timestamp(i64),
    Text(String),
    Bytes(Vec<u8>),
    Uuid([u8; 16]),
    Id([u8; 24]),
    /// Lists and other nested values, by encoding.
    Nested(Vec<u8>),
}

fn normalize(mut m: i128, mut s: u8) -> (i128, u8) {
    while s > 0 && m % 10 == 0 {
        m /= 10;
        s -= 1;
    }
    (m, s)
}

/// The exact decimal value of a finite float, if it fits 38 digits (`0.5`, `0.125`, `3.0`;
/// not `0.1`, whose exact binary value has 55 significant digits).
pub(crate) fn float_exact(f: f64) -> Option<(i128, u8)> {
    if !f.is_finite() {
        return None;
    }
    if f == 0.0 {
        return Some((0, 0));
    }
    let bits = f.to_bits();
    let exp = ((bits >> 52) & 0x7FF) as i32;
    let frac = bits & ((1u64 << 52) - 1);
    let (mut m, mut e) = if exp == 0 { (frac, -1074) } else { (frac | (1 << 52), exp - 1075) };
    let tz = m.trailing_zeros();
    m >>= tz;
    e += tz as i32;
    let m = i128::from(m) * if f < 0.0 { -1 } else { 1 };
    if e >= 0 {
        let v = m.checked_mul(1i128.checked_shl(e as u32)?)?;
        return (e < 127).then_some(normalize(v, 0));
    }
    // m * 2^e = m * 5^k / 10^k with k = -e.
    let k = (-e) as u32;
    if k > 38 {
        return None;
    }
    let v = m.checked_mul(5i128.checked_pow(k)?)?;
    Some(normalize(v, k as u8))
}

fn float_key(f: f64) -> GroupValue {
    if let Some((m, s)) = float_exact(f) {
        return GroupValue::Exact(m, s);
    }
    if f.is_nan() {
        return GroupValue::Float(f64::NAN.to_bits());
    }
    GroupValue::Float(f.to_bits())
}

impl GroupValue {
    pub(crate) fn of_val(v: Option<&Val>) -> Self {
        let Some(v) = v else { return Self::Null };
        match v {
            Val::Bool(b) => Self::Bool(*b),
            Val::Int(x) => Self::Exact(i128::from(*x), 0),
            Val::UInt(x) => Self::Exact(i128::from(*x), 0),
            Val::Int128(x) => Self::Exact(*x, 0),
            Val::Decimal(m, s) => {
                let (m, s) = normalize(*m, *s);
                Self::Exact(m, s)
            }
            Val::Float(f) => float_key(*f),
            Val::Timestamp(t) => Self::Timestamp(*t),
            Val::Text(s) => Self::Text(s.clone()),
            Val::Bytes(b) => Self::Bytes(b.clone()),
            Val::Uuid(u) => Self::Uuid(*u),
            Val::Id(i) => Self::Id(*i),
            Val::Nested(n) => {
                // An explicit null is null.
                if n.as_slice() == [storage_codec::value_wire::SVE_NULL] {
                    Self::Null
                } else {
                    Self::Nested(n.clone())
                }
            }
        }
    }

    /// Bytes for hashing (HyperLogLog).
    fn hash(&self) -> u64 {
        let mut buf = Vec::with_capacity(32);
        match self {
            Self::Null => buf.push(0),
            Self::Bool(b) => buf.extend_from_slice(&[1, u8::from(*b)]),
            Self::Exact(m, s) => {
                buf.push(2);
                buf.extend_from_slice(&m.to_le_bytes());
                buf.push(*s);
            }
            Self::Float(b) => {
                buf.push(3);
                buf.extend_from_slice(&b.to_le_bytes());
            }
            Self::Timestamp(t) => {
                buf.push(4);
                buf.extend_from_slice(&t.to_le_bytes());
            }
            Self::Text(s) => {
                buf.push(5);
                buf.extend_from_slice(s.as_bytes());
            }
            Self::Bytes(b) | Self::Nested(b) => {
                buf.push(6);
                buf.extend_from_slice(b);
            }
            Self::Uuid(u) => {
                buf.push(7);
                buf.extend_from_slice(u);
            }
            Self::Id(i) => {
                buf.push(8);
                buf.extend_from_slice(i);
            }
        }
        xxh3_64(&buf)
    }

    /// Approximate heap + inline size (memory budget).
    pub(crate) fn size(&self) -> usize {
        48 + match self {
            Self::Text(s) => s.len(),
            Self::Bytes(b) | Self::Nested(b) => b.len(),
            _ => 0,
        }
    }
}

/// Compares two output values in the document total order.
pub(crate) fn cmp_props(a: &PropertyValue, b: &PropertyValue) -> Ordering {
    with_ref(a, |x| with_ref(b, |y| compare_values(&x, &y)))
}

/// Runs `f` on a zero-copy view of an owned value (lists and maps are archived first).
pub(crate) fn with_ref<R>(v: &PropertyValue, f: impl FnOnce(PropertyValueRef<'_>) -> R) -> R {
    match v {
        PropertyValue::List(_) | PropertyValue::Map(_) => {
            let mut buf = storage_codec::AlignedScratchBuf::with_capacity(256);
            let archived = {
                let mut b = storage_codec::archive::ArchiveBuilder::new(&mut buf, 0);
                v.archive(&mut b)
            };
            f(archived.as_ref(buf.as_slice()))
        }
        other => f(value_ref_of(other)),
    }
}

/// Exactly rounded sum of floats (Shewchuk's partials, as Python's `math.fsum`): the result
/// depends only on the values added, never on their order.
#[derive(Clone, Debug, Default)]
pub struct FloatSum {
    partials: Vec<f64>,
    /// Sum of infinities / NaNs (they make the result non-finite whatever else is added).
    special: f64,
    has_special: bool,
}

impl FloatSum {
    pub fn add(&mut self, x: f64) {
        if !x.is_finite() {
            self.special += x;
            self.has_special = true;
            return;
        }
        let mut x = x;
        let mut i = 0;
        for j in 0..self.partials.len() {
            let mut y = self.partials[j];
            if x.abs() < y.abs() {
                core::mem::swap(&mut x, &mut y);
            }
            let hi = x + y;
            if !hi.is_finite() {
                // Overflow of the running sum: the exact result is out of f64 range.
                self.special += hi;
                self.has_special = true;
                self.partials.clear();
                return;
            }
            let lo = y - (hi - x);
            if lo != 0.0 {
                self.partials[i] = lo;
                i += 1;
            }
            x = hi;
        }
        self.partials.truncate(i);
        self.partials.push(x);
    }

    pub fn merge(&mut self, other: &FloatSum) {
        for &p in &other.partials {
            self.add(p);
        }
        if other.has_special {
            self.special += other.special;
            self.has_special = true;
        }
    }

    /// The correctly rounded sum.
    pub fn value(&self) -> f64 {
        if self.has_special {
            return self.special;
        }
        let p = &self.partials;
        let Some(mut n) = p.len().checked_sub(1) else { return 0.0 };
        let mut hi = p[n];
        let mut lo = 0.0;
        while n > 0 {
            n -= 1;
            let x = hi;
            let y = p[n];
            hi = x + y;
            lo = y - (hi - x);
            if lo != 0.0 {
                break;
            }
        }
        // Round half-even correctly when the remaining partials push the result across.
        if n > 0 && ((lo < 0.0 && p[n - 1] < 0.0) || (lo > 0.0 && p[n - 1] > 0.0)) {
            let y = lo * 2.0;
            let x = hi + y;
            if y == x - hi {
                hi = x;
            }
        }
        hi
    }
}

/// An exact-or-float sum (`SUM`, and `AVG` with a count).
#[derive(Clone, Debug, Default)]
pub struct SumAcc {
    exact: i128,
    scale: u8,
    exact_seen: bool,
    decimal_seen: bool,
    float: FloatSum,
    float_seen: bool,
    overflow: bool,
}

impl SumAcc {
    /// Adds an exact value `m * 10^-s` (`decimal`: it came from a decimal).
    #[inline]
    pub fn add_exact(&mut self, m: i128, s: u8, decimal: bool) {
        self.exact_seen = true;
        self.decimal_seen |= decimal;
        if s > self.scale {
            match pow10(u32::from(s - self.scale)).and_then(|f| self.exact.checked_mul(f)) {
                Some(v) => {
                    self.exact = v;
                    self.scale = s;
                }
                None => {
                    self.overflow = true;
                    return;
                }
            }
        }
        let aligned = if s < self.scale {
            pow10(u32::from(self.scale - s)).and_then(|f| m.checked_mul(f))
        } else {
            Some(m)
        };
        match aligned.and_then(|a| self.exact.checked_add(a)) {
            Some(v) => self.exact = v,
            None => self.overflow = true,
        }
    }

    #[inline]
    pub fn add_float(&mut self, f: f64) {
        self.float_seen = true;
        self.float.add(f);
    }

    pub fn add_val(&mut self, v: &Val) {
        match v {
            Val::Int(x) => self.add_exact(i128::from(*x), 0, false),
            Val::UInt(x) => self.add_exact(i128::from(*x), 0, false),
            Val::Int128(x) => self.add_exact(*x, 0, false),
            Val::Decimal(m, s) => self.add_exact(*m, *s, true),
            Val::Float(f) => self.add_float(*f),
            _ => {}
        }
    }

    pub fn merge(&mut self, o: &SumAcc) {
        if o.exact_seen {
            self.add_exact(o.exact, o.scale, o.decimal_seen);
        }
        if o.float_seen {
            self.float_seen = true;
            self.float.merge(&o.float);
        }
        self.overflow |= o.overflow;
    }

    fn seen(&self) -> bool {
        self.exact_seen || self.float_seen
    }

    fn exact_f64(&self) -> f64 {
        // Exact decimal to the nearest float (via text: correctly rounded by the parser).
        format!("{}e-{}", self.exact, self.scale).parse().unwrap_or(f64::NAN)
    }

    /// The sum (`Null` when nothing was added).
    pub fn sum(&self) -> Result<PropertyValue> {
        if self.overflow {
            return Err(StorageError::invalid_value("numeric overflow: the sum does not fit 38 digits"));
        }
        if !self.seen() {
            return Ok(PropertyValue::Null);
        }
        if self.float_seen {
            let mut f = self.float.clone();
            if self.exact_seen {
                f.add(self.exact_f64());
            }
            return Ok(PropertyValue::Float64(f.value()));
        }
        if self.decimal_seen {
            return Decimal::new(self.exact, self.scale).map(PropertyValue::Decimal);
        }
        Ok(match i64::try_from(self.exact) {
            Ok(v) => PropertyValue::Int64(v),
            Err(_) => PropertyValue::Int128(self.exact),
        })
    }

    /// The mean of `count` values: a decimal (6 more fractional digits, half-even) for exact
    /// inputs, a float otherwise.
    pub fn avg(&self, count: u64) -> Result<PropertyValue> {
        if count == 0 || !self.seen() {
            return Ok(PropertyValue::Null);
        }
        if self.overflow {
            return Err(StorageError::invalid_value("numeric overflow in average"));
        }
        if self.float_seen {
            let mut f = self.float.clone();
            if self.exact_seen {
                f.add(self.exact_f64());
            }
            return Ok(PropertyValue::Float64(f.value() / count as f64));
        }
        let extra = 6u8.min(38 - self.scale.min(38));
        let scaled = pow10(u32::from(extra)).and_then(|p| self.exact.checked_mul(p));
        let Some(scaled) = scaled else {
            let f = self.exact_f64() / count as f64;
            return Ok(PropertyValue::Float64(f));
        };
        let c = i128::from(count);
        let (q, r) = (scaled / c, scaled % c);
        // Half-even rounding of q + r / c.
        let twice = r.abs() * 2;
        let q = match twice.cmp(&c) {
            Ordering::Greater => q + scaled.signum(),
            Ordering::Equal if q % 2 != 0 => q + scaled.signum(),
            _ => q,
        };
        Decimal::new(q, self.scale + extra).map(PropertyValue::Decimal)
    }
}

/// Keeps the smallest or largest value in the document total order.
#[derive(Clone, Debug, Default)]
pub struct MinMax {
    pub value: Option<PropertyValue>,
}

impl MinMax {
    #[inline]
    pub fn offer(&mut self, v: PropertyValue, max: bool) {
        if matches!(v, PropertyValue::Null) {
            return;
        }
        let replace = match &self.value {
            None => true,
            Some(cur) => {
                let o = cmp_props(&v, cur);
                if max {
                    o == Ordering::Greater
                } else {
                    o == Ordering::Less
                }
            }
        };
        if replace {
            self.value = Some(v);
        }
    }
}

/// HyperLogLog with 2^14 registers (standard error ~0.81 %).
#[derive(Clone, Debug)]
pub struct Hll {
    regs: Vec<u8>,
}

const HLL_P: u32 = 14;

impl Default for Hll {
    fn default() -> Self {
        Self { regs: vec![0; 1 << HLL_P] }
    }
}

impl Hll {
    pub fn add(&mut self, v: &GroupValue) {
        if matches!(v, GroupValue::Null) {
            return;
        }
        let h = v.hash();
        let idx = (h >> (64 - HLL_P)) as usize;
        let rest = (h << HLL_P) | (1 << (HLL_P - 1));
        let rank = rest.leading_zeros() as u8 + 1;
        if rank > self.regs[idx] {
            self.regs[idx] = rank;
        }
    }

    pub fn merge(&mut self, o: &Hll) {
        for (a, b) in self.regs.iter_mut().zip(&o.regs) {
            *a = (*a).max(*b);
        }
    }

    pub fn estimate(&self) -> u64 {
        let m = self.regs.len() as f64;
        let sum: f64 = self.regs.iter().map(|&r| 2f64.powi(-i32::from(r))).sum();
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let raw = alpha * m * m / sum;
        let zeros = self.regs.iter().filter(|&&r| r == 0).count();
        let e = if raw <= 2.5 * m && zeros > 0 { m * (m / zeros as f64).ln() } else { raw };
        e.round() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_sums_are_exactly_rounded_and_order_free() {
        let vals = [1e100, 1.0, -1e100, 1e-16, 0.1, 0.2, -0.3, 3.0];
        let mut a = FloatSum::default();
        for v in vals {
            a.add(v);
        }
        let mut b = FloatSum::default();
        for v in vals.iter().rev() {
            b.add(*v);
        }
        assert_eq!(a.value().to_bits(), b.value().to_bits());
        // 1 + 3 + 1e-16 + (0.1 + 0.2 - 0.3, exactly 2.7755575615628914e-17)
        assert_eq!(a.value(), 4.0);
        let mut c = FloatSum::default();
        for _ in 0..10 {
            c.add(0.1);
        }
        assert_eq!(c.value(), 1.0);
    }

    #[test]
    fn exact_sums_align_scales_and_fail_on_overflow() {
        let mut s = SumAcc::default();
        s.add_exact(150, 2, true); // 1.50
        s.add_exact(3, 0, false); // 3
        s.add_exact(5, 3, true); // 0.005
        assert_eq!(s.sum().unwrap(), PropertyValue::Decimal(Decimal::new(4505, 3).unwrap()));
        assert_eq!(s.avg(3).unwrap(), PropertyValue::Decimal(Decimal::new(1_501_666_667, 9).unwrap()));
        let mut o = SumAcc::default();
        o.add_exact(i128::MAX, 0, false);
        o.add_exact(1, 0, false);
        assert!(o.sum().is_err());
        let mut i = SumAcc::default();
        i.add_exact(i128::from(i64::MAX), 0, false);
        i.add_exact(1, 0, false);
        assert_eq!(i.sum().unwrap(), PropertyValue::Int128(i128::from(i64::MAX) + 1));
    }

    #[test]
    fn group_keys_follow_numeric_equality() {
        assert_eq!(GroupValue::of_val(Some(&Val::Int(1))), GroupValue::of_val(Some(&Val::Decimal(100, 2))));
        assert_eq!(GroupValue::of_val(Some(&Val::Float(1.0))), GroupValue::of_val(Some(&Val::UInt(1))));
        assert_eq!(GroupValue::of_val(Some(&Val::Float(0.5))), GroupValue::of_val(Some(&Val::Decimal(5, 1))));
        assert_eq!(GroupValue::of_val(Some(&Val::Float(0.125))), GroupValue::Exact(125, 3));
        assert!(matches!(GroupValue::of_val(Some(&Val::Float(0.1))), GroupValue::Float(_)));
        assert_eq!(float_exact(-3.0), Some((-3, 0)));
        assert_eq!(float_exact(1e30), Some((1_000_000_000_000_000_019_884_624_838_656, 0)));
        assert_eq!(GroupValue::of_val(Some(&Val::Float(-0.0))), GroupValue::of_val(Some(&Val::Float(0.0))));
        let mut h = Hll::default();
        for i in 0..100_000i64 {
            h.add(&GroupValue::Exact(i128::from(i), 0));
        }
        let e = h.estimate() as f64;
        assert!((e - 100_000.0).abs() < 3000.0, "{e}");
    }
}
