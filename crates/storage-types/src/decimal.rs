//! Exact fixed-point decimal numbers for money and other values that must never be rounded by
//! accident (`TASK-094`, `F-TYPE-01`).
//!
//! A [`Decimal`] is `mantissa / 10^scale` with an `i128` mantissa and `scale <= 38`, so it holds
//! every value with up to 38 significant digits exactly. Arithmetic is checked: an operation
//! that cannot be represented exactly returns [`StorageError::InvalidValue`] instead of wrapping,
//! saturating or silently rounding. Division and rescaling take an explicit [`RoundingMode`].
//!
//! Equality and ordering are numeric (`1.50 == 1.5`); [`Decimal::normalize`] gives the canonical
//! form used for hashing.

use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};
use core::str::FromStr;

use crate::error::{Result, StorageError};

/// Largest supported scale (number of digits after the decimal point).
pub const MAX_DECIMAL_SCALE: u8 = 38;

/// `10^n` for `n` in `0..=38` (`10^38 < 2^127`).
const POW10: [u128; 39] = {
    let mut t = [1u128; 39];
    let mut i = 1;
    while i < 39 {
        t[i] = t[i - 1] * 10;
        i += 1;
    }
    t
};

/// How to round when a result has more digits than the requested scale.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum RoundingMode {
    /// Round to nearest; ties go to the even digit ("banker's rounding"). The default.
    #[default]
    HalfEven,
    /// Round to nearest; ties go away from zero (common commercial rounding).
    HalfUp,
    /// Round to nearest; ties go toward zero.
    HalfDown,
    /// Toward zero (truncate).
    Down,
    /// Away from zero.
    Up,
    /// Toward negative infinity.
    Floor,
    /// Toward positive infinity.
    Ceiling,
}

/// Exact decimal number `mantissa / 10^scale`.
#[derive(Clone, Copy)]
pub struct Decimal {
    mantissa: i128,
    scale: u8,
}

#[cold]
#[inline(never)]
fn overflow() -> StorageError {
    StorageError::invalid_value("decimal result does not fit in 38 digits")
}

/// Divides magnitude `num` by `den` (`den > 0`) and rounds the quotient per `mode`;
/// `negative` is the sign of the exact result.
fn div_round(num: u128, den: u128, negative: bool, mode: RoundingMode) -> u128 {
    let q = num / den;
    let rem = num % den;
    if rem == 0 {
        return q;
    }
    // Compare rem with den / 2 without overflowing: rem vs den - rem.
    let other = den - rem;
    let up = match mode {
        RoundingMode::Down => false,
        RoundingMode::Up => true,
        RoundingMode::Floor => negative,
        RoundingMode::Ceiling => !negative,
        RoundingMode::HalfUp => rem >= other,
        RoundingMode::HalfDown => rem > other,
        RoundingMode::HalfEven => rem > other || (rem == other && q & 1 == 1),
    };
    q + u128::from(up)
}

/// Builds a signed mantissa from a magnitude, rejecting values outside `i128`.
#[inline]
fn signed(magnitude: u128, negative: bool) -> Result<i128> {
    if negative {
        if magnitude == i128::MIN.unsigned_abs() {
            Ok(i128::MIN)
        } else {
            i128::try_from(magnitude).map(|m| -m).map_err(|_| overflow())
        }
    } else {
        i128::try_from(magnitude).map_err(|_| overflow())
    }
}

impl Decimal {
    pub const ZERO: Self = Self { mantissa: 0, scale: 0 };
    pub const ONE: Self = Self { mantissa: 1, scale: 0 };

    /// `mantissa / 10^scale`; fails if `scale > 38`.
    pub const fn new(mantissa: i128, scale: u8) -> Result<Self> {
        if scale > MAX_DECIMAL_SCALE {
            return Err(StorageError::InvalidValue {
                reason: "decimal scale must be at most 38",
            });
        }
        Ok(Self { mantissa, scale })
    }

    /// An integer value (`scale = 0`).
    #[inline]
    pub const fn from_i128(value: i128) -> Self {
        Self { mantissa: value, scale: 0 }
    }

    /// An integer value (`scale = 0`).
    #[inline]
    pub const fn from_i64(value: i64) -> Self {
        Self { mantissa: value as i128, scale: 0 }
    }

    #[inline]
    pub const fn mantissa(self) -> i128 {
        self.mantissa
    }

    #[inline]
    pub const fn scale(self) -> u8 {
        self.scale
    }

    #[inline]
    pub const fn is_zero(self) -> bool {
        self.mantissa == 0
    }

    #[inline]
    pub const fn is_negative(self) -> bool {
        self.mantissa < 0
    }

    /// `-1`, `0` or `1`.
    #[inline]
    pub const fn signum(self) -> i32 {
        if self.mantissa > 0 {
            1
        } else if self.mantissa < 0 {
            -1
        } else {
            0
        }
    }

    /// `-self` (fails only for the most negative mantissa).
    pub fn checked_neg(self) -> Result<Self> {
        let m = self.mantissa.checked_neg().ok_or_else(overflow)?;
        Ok(Self { mantissa: m, scale: self.scale })
    }

    /// `|self|` (fails only for the most negative mantissa).
    pub fn checked_abs(self) -> Result<Self> {
        let m = self.mantissa.checked_abs().ok_or_else(overflow)?;
        Ok(Self { mantissa: m, scale: self.scale })
    }

    /// The same value with exactly `scale` fractional digits, rounding per `mode` when digits
    /// are dropped. Fails if the value does not fit.
    pub fn rescale(self, scale: u8, mode: RoundingMode) -> Result<Self> {
        if scale > MAX_DECIMAL_SCALE {
            return Err(StorageError::invalid_value("decimal scale must be at most 38"));
        }
        match scale.cmp(&self.scale) {
            Ordering::Equal => Ok(self),
            Ordering::Greater => {
                let factor = POW10[usize::from(scale - self.scale)] as i128;
                let m = self.mantissa.checked_mul(factor).ok_or_else(overflow)?;
                Ok(Self { mantissa: m, scale })
            }
            Ordering::Less => {
                let den = POW10[usize::from(self.scale - scale)];
                let neg = self.mantissa < 0;
                let q = div_round(self.mantissa.unsigned_abs(), den, neg, mode);
                Ok(Self { mantissa: signed(q, neg)?, scale })
            }
        }
    }

    /// The same value at a larger-or-equal `scale`, exactly (no rounding), or an error.
    #[inline]
    fn widen(self, scale: u8) -> Result<i128> {
        if scale == self.scale {
            return Ok(self.mantissa);
        }
        let factor = POW10[usize::from(scale - self.scale)] as i128;
        self.mantissa.checked_mul(factor).ok_or_else(overflow)
    }

    /// Exact `self + other` (result scale = the larger scale).
    pub fn checked_add(self, other: Self) -> Result<Self> {
        let scale = self.scale.max(other.scale);
        let m = self.widen(scale)?.checked_add(other.widen(scale)?).ok_or_else(overflow)?;
        Ok(Self { mantissa: m, scale })
    }

    /// Exact `self - other` (result scale = the larger scale).
    pub fn checked_sub(self, other: Self) -> Result<Self> {
        let scale = self.scale.max(other.scale);
        let m = self.widen(scale)?.checked_sub(other.widen(scale)?).ok_or_else(overflow)?;
        Ok(Self { mantissa: m, scale })
    }

    /// Exact `self * other` (result scale = sum of scales, trailing zeros removed if it would
    /// exceed 38). Use [`Decimal::rescale`] afterwards to round to a currency's scale.
    pub fn checked_mul(self, other: Self) -> Result<Self> {
        let mut m = self.mantissa.checked_mul(other.mantissa).ok_or_else(overflow)?;
        let mut scale = u32::from(self.scale) + u32::from(other.scale);
        while scale > u32::from(MAX_DECIMAL_SCALE) {
            if m % 10 != 0 {
                return Err(StorageError::invalid_value(
                    "decimal product needs more than 38 fractional digits; rescale the operands first",
                ));
            }
            m /= 10;
            scale -= 1;
        }
        Ok(Self { mantissa: m, scale: scale as u8 })
    }

    /// `self / other` rounded to `scale` fractional digits per `mode`.
    pub fn checked_div(self, other: Self, scale: u8, mode: RoundingMode) -> Result<Self> {
        if other.mantissa == 0 {
            return Err(StorageError::invalid_value("decimal division by zero"));
        }
        if scale > MAX_DECIMAL_SCALE {
            return Err(StorageError::invalid_value("decimal scale must be at most 38"));
        }
        // q = a.m * 10^(scale + b.s - a.s) / b.m
        let exp = i32::from(scale) + i32::from(other.scale) - i32::from(self.scale);
        let mut num = self.mantissa.unsigned_abs();
        let mut den = other.mantissa.unsigned_abs();
        if exp >= 0 {
            let mut e = exp as usize;
            while e > 0 {
                let step = e.min(38);
                num = num.checked_mul(POW10[step]).ok_or_else(overflow)?;
                e -= step;
            }
        } else {
            den = den.checked_mul(POW10[(-exp) as usize]).ok_or_else(overflow)?;
        }
        let neg = (self.mantissa < 0) != (other.mantissa < 0);
        let q = div_round(num, den, neg, mode);
        Ok(Self { mantissa: signed(q, neg)?, scale })
    }

    /// The canonical form: trailing fractional zeros removed (`1.500 -> 1.5`, `2.00 -> 2`).
    pub fn normalize(self) -> Self {
        let mut m = self.mantissa;
        let mut s = self.scale;
        while s > 0 && m % 10 == 0 {
            m /= 10;
            s -= 1;
        }
        Self { mantissa: m, scale: s }
    }

    /// The integer value if `self` has no fractional part.
    pub fn to_i128_exact(self) -> Result<i128> {
        let n = self.normalize();
        if n.scale != 0 {
            return Err(StorageError::invalid_value("decimal has a fractional part"));
        }
        Ok(n.mantissa)
    }

    /// Nearest `f64` (lossy: for display or interop only, never for money arithmetic).
    pub fn to_f64_lossy(self) -> f64 {
        self.mantissa as f64 / POW10[usize::from(self.scale)] as f64
    }

    /// Converts an `f64` to a decimal with `scale` fractional digits (lossy by nature: binary
    /// floats cannot represent most decimal fractions). Fails for NaN and infinities.
    pub fn from_f64_lossy(value: f64, scale: u8, mode: RoundingMode) -> Result<Self> {
        if !value.is_finite() {
            return Err(StorageError::invalid_value("NaN or infinity is not a decimal"));
        }
        // Shortest round-trip text of the float, parsed exactly, then rounded once.
        let text = format!("{value:e}");
        text.parse::<Self>()?.rescale(scale, mode)
    }

    /// Order-preserving (monotonic, non-decreasing) 64-bit key for range indexes: equal values
    /// map to equal keys and `a < b` implies `key(a) <= key(b)`. Distinct values may share a key
    /// (beyond 6 fractional digits or for huge magnitudes), so index hits are re-checked exactly.
    pub fn sortable_key(self) -> i64 {
        const KEY_SCALE: u8 = 6;
        let scaled: i128 = if self.scale <= KEY_SCALE {
            let factor = POW10[usize::from(KEY_SCALE - self.scale)] as i128;
            self.mantissa.saturating_mul(factor)
        } else {
            let den = POW10[usize::from(self.scale - KEY_SCALE)];
            let neg = self.mantissa < 0;
            let q = div_round(self.mantissa.unsigned_abs(), den, neg, RoundingMode::Floor);
            signed(q, neg).unwrap_or(if neg { i128::MIN } else { i128::MAX })
        };
        compress_i128_to_i64(scaled)
    }
}

/// Monotonic compression of `i128` into `i64`: exact for `|x| < 2^62`, logarithmic
/// (exponent + 55-bit mantissa) beyond, so order is preserved over the whole range.
pub fn compress_i128_to_i64(x: i128) -> i64 {
    const LIMIT: u128 = 1 << 62;
    let neg = x < 0;
    let mag = x.unsigned_abs();
    if mag < LIMIT {
        return x as i64;
    }
    let e = 127 - mag.leading_zeros(); // 62..=127
    let m = ((mag >> (e - 55)) & ((1u128 << 55) - 1)) as u64;
    let key = (1u64 << 62) + (u64::from(e - 62) << 55) + m; // < 2^62 + 2^61 + 2^55 < 2^63
    if neg {
        -(key as i64)
    } else {
        key as i64
    }
}

impl Default for Decimal {
    #[inline]
    fn default() -> Self {
        Self::ZERO
    }
}

impl PartialEq for Decimal {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Decimal {}

impl PartialOrd for Decimal {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.scale == other.scale {
            return self.mantissa.cmp(&other.mantissa);
        }
        let sa = self.signum();
        let sb = other.signum();
        if sa != sb {
            return sa.cmp(&sb);
        }
        // Same sign: widen the smaller-scale operand. If that overflows, its magnitude exceeds
        // every mantissa at the larger scale, so the sign decides.
        let scale = self.scale.max(other.scale);
        match (self.widen(scale), other.widen(scale)) {
            (Ok(a), Ok(b)) => a.cmp(&b),
            (Err(_), _) => {
                if sa > 0 {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (_, Err(_)) => {
                if sa > 0 {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
        }
    }
}

impl Hash for Decimal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let n = self.normalize();
        n.mantissa.hash(state);
        n.scale.hash(state);
    }
}

impl fmt::Debug for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Decimal({self})")
    }
}

impl fmt::Display for Decimal {
    /// Plain notation with exactly `scale` fractional digits (`-12.340`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mag = self.mantissa.unsigned_abs();
        let scale = usize::from(self.scale);
        let digits = mag.to_string();
        let sign = if self.mantissa < 0 { "-" } else { "" };
        if scale == 0 {
            return write!(f, "{sign}{digits}");
        }
        if digits.len() > scale {
            let (int, frac) = digits.split_at(digits.len() - scale);
            write!(f, "{sign}{int}.{frac}")
        } else {
            write!(f, "{sign}0.{:0>width$}", digits, width = scale)
        }
    }
}

impl FromStr for Decimal {
    type Err = StorageError;

    /// Parses `[+-]digits[.digits][(e|E)[+-]digits]`. The scale is the number of fractional
    /// digits written (after applying the exponent): `"1.50"` has scale 2.
    fn from_str(s: &str) -> Result<Self> {
        let bad = || StorageError::invalid_value("not a decimal number");
        let s = s.trim();
        let (neg, rest) = match s.as_bytes().first() {
            Some(b'-') => (true, &s[1..]),
            Some(b'+') => (false, &s[1..]),
            _ => (false, s),
        };
        let (num, exp) = match rest.find(['e', 'E']) {
            Some(i) => {
                let e: i32 = rest[i + 1..].parse().map_err(|_| bad())?;
                (&rest[..i], e)
            }
            None => (rest, 0),
        };
        let (int, frac) = match num.find('.') {
            Some(i) => (&num[..i], &num[i + 1..]),
            None => (num, ""),
        };
        if int.is_empty() && frac.is_empty() {
            return Err(bad());
        }
        if !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) {
            return Err(bad());
        }
        let mut mag: u128 = 0;
        for b in int.bytes().chain(frac.bytes()) {
            mag = mag
                .checked_mul(10)
                .and_then(|m| m.checked_add(u128::from(b - b'0')))
                .ok_or_else(overflow)?;
        }
        if mag == 0 {
            // Zero (any exponent): never loop over a hostile exponent.
            let scale = frac.len().min(usize::from(MAX_DECIMAL_SCALE)) as u8;
            return Ok(Self { mantissa: 0, scale });
        }
        let mut scale = frac.len() as i64 - i64::from(exp);
        // A non-zero value needs at most 39 digits: anything further is out of range.
        if !(-40..=200).contains(&scale) {
            return Err(overflow());
        }
        if scale < 0 {
            // Positive exponent beyond the fraction: append zeros.
            while scale < 0 {
                mag = mag.checked_mul(10).ok_or_else(overflow)?;
                scale += 1;
            }
        }
        let mut scale = scale as u64;
        // Too many fractional digits: drop trailing zeros only (never round silently).
        while scale > u64::from(MAX_DECIMAL_SCALE) {
            if mag % 10 != 0 {
                return Err(StorageError::invalid_value(
                    "decimal has more than 38 fractional digits",
                ));
            }
            mag /= 10;
            scale -= 1;
        }
        Ok(Self {
            mantissa: signed(mag, neg)?,
            scale: scale as u8,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn test_decimal_parse_format_roundtrip() {
        for s in ["0", "1", "-1", "0.10", "-0.001", "123456789.123456789", "1.50", "100"] {
            assert_eq!(d(s).to_string(), s, "{s}");
        }
        assert_eq!(d("1.5e2").to_string(), "150");
        assert_eq!(d("1.5E-3").to_string(), "0.0015");
        assert_eq!(d("+.5").to_string(), "0.5");
        assert_eq!(d("5.").to_string(), "5");
        assert!("0e999999999".parse::<Decimal>().unwrap().is_zero());
        assert!("-0e-999999999".parse::<Decimal>().unwrap().is_zero());
        assert!("1e999999999".parse::<Decimal>().is_err());
        assert!("1e-999999999".parse::<Decimal>().is_err());
        for bad in ["", "-", ".", "1..2", "abc", "1e", "1.2.3", "NaN", "1 2"] {
            assert!(bad.parse::<Decimal>().is_err(), "{bad}");
        }
        let max = i128::MAX.to_string();
        assert_eq!(d(&max).mantissa(), i128::MAX);
        assert!(format!("{max}0").parse::<Decimal>().is_err());
        assert_eq!(d(&i128::MIN.to_string()).mantissa(), i128::MIN);
    }

    #[test]
    fn test_decimal_exact_money_arithmetic() {
        // The classic float failure: 0.1 + 0.2 == 0.3 exactly.
        assert_eq!(d("0.1").checked_add(d("0.2")).unwrap(), d("0.3"));
        assert_eq!(d("10.00").checked_sub(d("0.01")).unwrap().to_string(), "9.99");
        assert_eq!(d("19.99").checked_mul(d("3")).unwrap().to_string(), "59.97");
        assert_eq!(d("1.50"), d("1.5"));
        assert!(d("1.5") < d("1.51"));
        assert!(d("-2") < d("-1.999"));
        // Overflow is an error, never a wrap.
        let big = Decimal::from_i128(i128::MAX);
        assert!(big.checked_add(Decimal::ONE).is_err());
        assert!(Decimal::from_i128(i128::MIN).checked_neg().is_err());
        assert!(big.checked_mul(d("2")).is_err());
    }

    #[test]
    fn test_decimal_division_and_rounding_modes() {
        let third = d("10").checked_div(d("3"), 2, RoundingMode::HalfEven).unwrap();
        assert_eq!(third.to_string(), "3.33");
        assert!(d("1").checked_div(Decimal::ZERO, 2, RoundingMode::HalfEven).is_err());
        let cases = [
            ("2.5", RoundingMode::HalfEven, "2"),
            ("3.5", RoundingMode::HalfEven, "4"),
            ("2.5", RoundingMode::HalfUp, "3"),
            ("2.5", RoundingMode::HalfDown, "2"),
            ("-2.5", RoundingMode::HalfUp, "-3"),
            ("2.1", RoundingMode::Up, "3"),
            ("-2.1", RoundingMode::Floor, "-3"),
            ("-2.9", RoundingMode::Ceiling, "-2"),
            ("2.9", RoundingMode::Down, "2"),
        ];
        for (v, mode, want) in cases {
            assert_eq!(d(v).rescale(0, mode).unwrap().to_string(), want, "{v} {mode:?}");
        }
        assert_eq!(
            d("-7").checked_div(d("2"), 0, RoundingMode::HalfEven).unwrap().to_string(),
            "-4"
        );
        assert_eq!(d("1.005").rescale(2, RoundingMode::HalfUp).unwrap().to_string(), "1.01");
        assert_eq!(d("12.3").rescale(4, RoundingMode::HalfEven).unwrap().to_string(), "12.3000");
    }

    #[test]
    fn test_decimal_sortable_key_is_monotonic() {
        let mut values: Vec<Decimal> = [
            "-1e30", "-170141183460469231731687303715884105728", "-12345678901234.5", "-1", "-0.0000001",
            "0", "0.0000001", "0.000001", "1", "1.5", "2", "99999999999.999999", "1e20", "1e37",
            "170141183460469231731687303715884105727",
        ]
        .iter()
        .map(|s| d(s))
        .collect();
        values.sort();
        for w in values.windows(2) {
            assert!(w[0].sortable_key() <= w[1].sortable_key(), "{} {}", w[0], w[1]);
        }
        assert_eq!(d("1.50").sortable_key(), d("1.5").sortable_key());
        let mut prev = i64::MIN;
        for i in -2000i128..2000 {
            let x = i * (1i128 << 100) / 7;
            let k = compress_i128_to_i64(x);
            assert!(k >= prev);
            prev = k;
        }
    }

    #[test]
    fn test_decimal_from_f64_and_hash_consistency() {
        use std::collections::HashSet;
        assert_eq!(
            Decimal::from_f64_lossy(0.1, 2, RoundingMode::HalfEven).unwrap().to_string(),
            "0.10"
        );
        assert!(Decimal::from_f64_lossy(f64::NAN, 2, RoundingMode::HalfEven).is_err());
        let set: HashSet<Decimal> = [d("1.5"), d("1.50"), d("1.500")].into_iter().collect();
        assert_eq!(set.len(), 1);
    }
}
