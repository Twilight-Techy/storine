//! Vectorized column kernels (`TASK-127`, `F-OLAP-03`, `RULE-OLAP-03`).
//!
//! Every kernel is written once as safe, autovectorizable Rust. On x86-64 a second copy is
//! compiled with AVX2 enabled and chosen at runtime when the CPU has it; both copies execute the
//! same operations in the same order, so results are bit-identical on every machine (no float
//! reassociation, no FMA contraction: Rust performs neither on its own). The only `unsafe` is
//! the call into the AVX2 copy after the feature check.
//!
//! Bit-packed data is a little-endian stream of `u64` words in which value `i` occupies bits
//! `i * w .. (i + 1) * w`; 64 consecutive values starting at a multiple of 64 occupy exactly `w`
//! words, which is what the width-specialized unpackers exploit.

use core::sync::atomic::{AtomicU8, Ordering};

/// 0 = not yet detected, 1 = portable, 2 = AVX2.
static LEVEL: AtomicU8 = AtomicU8::new(0);

/// Whether the AVX2 copies are used (`STORINE_SIMD=portable` forces the portable ones).
#[inline]
pub fn avx2_enabled() -> bool {
    let l = LEVEL.load(Ordering::Relaxed);
    if l != 0 {
        return l == 2;
    }
    let forced_portable = std::env::var_os("STORINE_SIMD").is_some_and(|v| v == "portable");
    #[cfg(target_arch = "x86_64")]
    let avx2 = !forced_portable && std::arch::is_x86_feature_detected!("avx2");
    #[cfg(not(target_arch = "x86_64"))]
    let avx2 = {
        let _ = forced_portable;
        false
    };
    LEVEL.store(if avx2 { 2 } else { 1 }, Ordering::Relaxed);
    avx2
}

/// Defines `pub fn $name` from a safe body, plus an AVX2-compiled copy chosen at runtime.
macro_rules! multiversion {
    ($(#[$m:meta])* pub fn $name:ident($($arg:ident: $ty:ty),* $(,)?) $(-> $ret:ty)? $body:block) => {
        $(#[$m])*
        #[inline]
        pub fn $name($($arg: $ty),*) $(-> $ret)? {
            #[inline(always)]
            fn imp($($arg: $ty),*) $(-> $ret)? $body
            #[cfg(target_arch = "x86_64")]
            {
                #[target_feature(enable = "avx2,bmi1,bmi2,popcnt")]
                unsafe fn avx2($($arg: $ty),*) $(-> $ret)? {
                    imp($($arg),*)
                }
                if avx2_enabled() {
                    // SAFETY: `avx2_enabled` verified at runtime that this CPU supports AVX2,
                    // BMI1/2 and POPCNT (`is_x86_feature_detected!("avx2")` implies a CPU with
                    // all of them in practice; the body is safe code either way).
                    return unsafe { avx2($($arg),*) };
                }
            }
            imp($($arg),*)
        }
    };
}

#[inline(always)]
fn word(words: &[u8], i: usize) -> u64 {
    match words.get(i * 8..i * 8 + 8) {
        Some(b) => u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]),
        None => 0,
    }
}

#[inline(always)]
fn get_one(words: &[u8], width: usize, i: usize) -> u64 {
    if width == 0 {
        return 0;
    }
    let bit = i * width;
    let (w, off) = (bit >> 6, (bit & 63) as u32);
    let two = u128::from(word(words, w)) | (u128::from(word(words, w + 1)) << 64);
    let v = (two >> off) as u64;
    if width == 64 {
        v
    } else {
        v & ((1u64 << width) - 1)
    }
}

/// Unpacks 64 values from exactly `W` words (`W` is the bit width).
#[inline(always)]
fn unpack64<const W: usize>(src: &[u64; 64], out: &mut [u64]) {
    let mask = if W == 64 { u64::MAX } else { (1u64 << W) - 1 };
    for (j, o) in out.iter_mut().take(64).enumerate() {
        let bit = j * W;
        let (w, off) = (bit >> 6, bit & 63);
        let lo = src[w] >> off;
        let v = if off + W > 64 { lo | (src[w + 1] << (64 - off)) } else { lo };
        *o = v & mask;
    }
}

/// 64 values of `W <= 56` bits from the `W * 8 + 8` bytes starting at `blk`: value `j` lies
/// within the unaligned 8-byte word at byte `j * W / 8`, so each value is one load, one shift
/// and one mask (offsets are constants once `W` is).
#[inline(always)]
fn unpack64_bytes<const W: usize>(blk: &[u8], out: &mut [u64]) {
    let mask = (1u64 << W) - 1;
    // Fixed lengths let every offset below fold to a constant with no bounds checks: value `j`
    // ends by byte `(63 * W) / 8 + 8 <= W * 8 + 8`.
    let (Some(blk), Some(out)) = (blk.get(..W * 8 + 8), out.get_mut(..64)) else {
        return;
    };
    // Every 8 values span exactly `W` bytes, so each group starts byte-aligned and the 8
    // offsets inside a group are constants (the inner loop unrolls).
    for g in 0..8 {
        let gb = &blk[g * W..g * W + W + 8];
        let o = &mut out[g * 8..g * 8 + 8];
        for k in 0..8 {
            let bit = k * W;
            let at = bit >> 3;
            let b = [gb[at], gb[at + 1], gb[at + 2], gb[at + 3], gb[at + 4], gb[at + 5], gb[at + 6], gb[at + 7]];
            o[k] = (u64::from_le_bytes(b) >> (bit & 7)) & mask;
        }
    }
}

#[inline(always)]
fn unpack_w<const W: usize>(words: &[u8], start: usize, out: &mut [u64]) {
    let n = out.len();
    let mut i = 0;
    // Scalar until the position is a multiple of 64 values.
    while i < n && (start + i) % 64 != 0 {
        out[i] = get_one(words, W, start + i);
        i += 1;
    }
    let mut src = [0u64; 64];
    while i + 64 <= n {
        let first_word = (start + i) / 64 * W;
        if W <= 56 {
            // One bounds check per 64 values; the block always has its padding word.
            if let Some(blk) = words.get(first_word * 8..first_word * 8 + W * 8 + 8) {
                unpack64_bytes::<W>(blk, &mut out[i..i + 64]);
                i += 64;
                continue;
            }
        }
        for (k, s) in src.iter_mut().take(W).enumerate() {
            *s = word(words, first_word + k);
        }
        unpack64::<W>(&src, &mut out[i..i + 64]);
        i += 64;
    }
    while i < n {
        out[i] = get_one(words, W, start + i);
        i += 1;
    }
}

macro_rules! dispatch_width {
    ($width:expr, $words:expr, $start:expr, $out:expr; $($w:literal)*) => {
        match $width {
            0 => $out.fill(0),
            $($w => unpack_w::<$w>($words, $start, $out),)*
            _ => $out.fill(0),
        }
    };
}

multiversion! {
    /// Values `start .. start + out.len()` of a packed area of `width`-bit values. Reads past
    /// the end of `words` yield zero bits (callers validate lengths; this never panics).
    pub fn unpack(words: &[u8], width: u8, start: usize, out: &mut [u64]) {
        dispatch_width!(width as usize, words, start, out;
            1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32
            33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64)
    }
}

multiversion! {
    /// `out[i] = base + vals[i]` (wrapping): frame-of-reference decoding.
    pub fn add_base(base: u64, vals: &[u64], out: &mut [i64]) {
        for (o, &v) in out.iter_mut().zip(vals) {
            *o = base.wrapping_add(v) as i64;
        }
    }
}

/// Packs 8 bytes that are each 0 or 1 into 8 bits (byte `i` -> bit `i`). Every partial product
/// is a distinct power of two, so the multiplication never carries into the top byte.
#[inline(always)]
fn pack8(b: [u8; 8]) -> u64 {
    (u64::from_le_bytes(b).wrapping_mul(0x0102_0408_1020_4080)) >> 56
}

/// Sets bits of `out` (one per value, LSB first) from `pred`, 64 values per word: the
/// comparisons fill a byte array (vectorizes), which is then packed 8 bytes per multiply.
#[inline(always)]
fn bits_where<T: Copy>(vals: &[T], out: &mut [u64], pred: impl Fn(T) -> bool) {
    for (w, chunk) in out.iter_mut().zip(vals.chunks(64)) {
        let mut bytes = [0u8; 64];
        for (b, &v) in bytes.iter_mut().zip(chunk) {
            *b = u8::from(pred(v));
        }
        let mut m = 0u64;
        for (k, eight) in bytes.chunks_exact(8).enumerate() {
            m |= pack8([eight[0], eight[1], eight[2], eight[3], eight[4], eight[5], eight[6], eight[7]]) << (k * 8);
        }
        *w = m;
    }
}

multiversion! {
    /// Bit `i` of `out` = `lo <= vals[i] <= hi` (unsigned). `out` has `ceil(len / 64)` words.
    pub fn range_u64(vals: &[u64], lo: u64, hi: u64, out: &mut [u64]) {
        let span = hi.wrapping_sub(lo);
        if hi < lo {
            out.fill(0);
            return;
        }
        bits_where(vals, out, |v| v.wrapping_sub(lo) <= span);
    }
}

multiversion! {
    /// Bit `i` of `out` = `lo <= vals[i] <= hi` (signed).
    pub fn range_i64(vals: &[i64], lo: i64, hi: i64, out: &mut [u64]) {
        if hi < lo {
            out.fill(0);
            return;
        }
        let span = (hi as u64).wrapping_sub(lo as u64);
        bits_where(vals, out, |v| (v as u64).wrapping_sub(lo as u64) <= span);
    }
}

multiversion! {
    /// Bit `i` of `out` = `vals[i]` within `[lo, hi]`, each bound inclusive or not (NaN never
    /// matches).
    pub fn range_f64(vals: &[f64], lo: f64, lo_incl: bool, hi: f64, hi_incl: bool, out: &mut [u64]) {
        bits_where(vals, out, |v| {
            let above = if lo_incl { v >= lo } else { v > lo };
            let below = if hi_incl { v <= hi } else { v < hi };
            above & below
        });
    }
}

multiversion! {
    /// Bit `i` of `out` = `vals[i] == x`.
    pub fn eq_u64(vals: &[u64], x: u64, out: &mut [u64]) {
        bits_where(vals, out, |v| v == x);
    }
}

multiversion! {
    /// Bit `i` of `out` = `table[codes[i]] != 0` (codes past the table use its last entry):
    /// dictionary codes to selection bits.
    pub fn lookup_bits(codes: &[u64], table: &[u8], out: &mut [u64]) {
        let last = table.len().saturating_sub(1);
        if table.is_empty() {
            out.fill(0);
            return;
        }
        if table.len() <= 64 {
            // Small tables: one register holds the whole table.
            let bitmap = table.iter().enumerate().fold(0u64, |m, (i, &t)| m | (u64::from(t != 0) << i));
            bits_where(codes, out, |c| (bitmap >> c.min(last as u64)) & 1 != 0);
            return;
        }
        for (w, chunk) in out.iter_mut().zip(codes.chunks(64)) {
            let mut bytes = [0u8; 64];
            for (b, &c) in bytes.iter_mut().zip(chunk) {
                *b = u8::from(table[(c as usize).min(last)] != 0);
            }
            let mut m = 0u64;
            for (k, eight) in bytes.chunks_exact(8).enumerate() {
                m |= pack8([eight[0], eight[1], eight[2], eight[3], eight[4], eight[5], eight[6], eight[7]]) << (k * 8);
            }
            *w = m;
        }
    }
}

multiversion! {
    /// A bound on `|v|` over `vals`: every `|v| <= abs_bound_i64(vals)` (within a factor of two of
    /// the largest magnitude). Bitwise only, so it vectorizes without 64-bit min/max.
    pub fn abs_bound_i64(vals: &[i64]) -> u64 {
        // `v ^ (v >> 63)` is `|v| - 1` for negatives and `v` otherwise; OR keeps every top bit.
        // A comparison, not `>> 63`: AVX2 has 64-bit compares but no arithmetic shift.
        let r = vals.iter().fold(0u64, |m, &v| m | (v ^ -i64::from(v < 0)) as u64);
        // Every value is below the next power of two of the OR (plus one for the negative case).
        if r == 0 {
            if vals.iter().any(|&v| v != 0) { 1 } else { 0 }
        } else {
            (u64::MAX >> r.leading_zeros()).saturating_add(1)
        }
    }
}

multiversion! {
    /// `(min, max)` of `vals`, `None` when empty.
    pub fn min_max_i64(vals: &[i64]) -> Option<(i64, i64)> {
        let first = *vals.first()?;
        // Plain folds: they vectorize as reductions (an 8-lane array would be transposed).
        let lo = vals.iter().fold(first, |m, &v| m.min(v));
        let hi = vals.iter().fold(first, |m, &v| m.max(v));
        Some((lo, hi))
    }
}

multiversion! {
    /// `a[i] &= b[i]`.
    pub fn and_words(a: &mut [u64], b: &[u64]) {
        for (x, y) in a.iter_mut().zip(b) {
            *x &= *y;
        }
    }
}

multiversion! {
    /// `a[i] |= b[i]`.
    pub fn or_words(a: &mut [u64], b: &[u64]) {
        for (x, y) in a.iter_mut().zip(b) {
            *x |= *y;
        }
    }
}

multiversion! {
    /// `a[i] &= !b[i]`.
    pub fn and_not_words(a: &mut [u64], b: &[u64]) {
        for (x, y) in a.iter_mut().zip(b) {
            *x &= !*y;
        }
    }
}

multiversion! {
    /// Number of set bits.
    pub fn count_ones(words: &[u64]) -> u64 {
        words.iter().map(|w| u64::from(w.count_ones())).sum()
    }
}

multiversion! {
    /// Exact sum of `vals[i]` where bit `i` of `mask` is set. Values are split into 32-bit
    /// halves summed in 64-bit lanes (exact for up to `2^31` values per call), so the loop
    /// vectorizes without 128-bit lanes.
    pub fn sum_i64_masked(vals: &[i64], mask: &[u64]) -> i128 {
        let mut lo: u64 = 0;
        let mut hi: i64 = 0;
        for (w, chunk) in mask.iter().zip(vals.chunks(64)) {
            match *w {
                0 => {}
                u64::MAX => {
                    // Whole word selected (the common dense case): a plain vectorized sum.
                    for &v in chunk {
                        lo = lo.wrapping_add((v as u64) & 0xFFFF_FFFF);
                        hi = hi.wrapping_add(v >> 32);
                    }
                }
                w => {
                    for (j, &v) in chunk.iter().enumerate() {
                        let keep = 0u64.wrapping_sub((w >> j) & 1);
                        let v = (v as u64) & keep;
                        lo = lo.wrapping_add(v & 0xFFFF_FFFF);
                        hi = hi.wrapping_add((v as i64) >> 32);
                    }
                }
            }
        }
        (i128::from(hi) << 32) + i128::from(lo)
    }
}

multiversion! {
    /// Exact sum of all `vals` (see [`sum_i64_masked`]).
    pub fn sum_i64(vals: &[i64]) -> i128 {
        let mut lo: u64 = 0;
        let mut hi: i64 = 0;
        for &v in vals {
            lo = lo.wrapping_add((v as u64) & 0xFFFF_FFFF);
            hi = hi.wrapping_add(v >> 32);
        }
        (i128::from(hi) << 32) + i128::from(lo)
    }
}

multiversion! {
    /// `(min, max)` of the selected values (`None` if none is selected).
    pub fn min_max_i64_masked(vals: &[i64], mask: &[u64]) -> Option<(i64, i64)> {
        let mut min = i64::MAX;
        let mut max = i64::MIN;
        let mut any = 0u64;
        for (w, chunk) in mask.iter().zip(vals.chunks(64)) {
            any |= *w;
            for (j, &v) in chunk.iter().enumerate() {
                let keep = (w >> j) & 1 == 1;
                min = if keep && v < min { v } else { min };
                max = if keep && v > max { v } else { max };
            }
        }
        (any != 0).then_some((min, max))
    }
}

/// `out[i] = vals[i] as f64` with no rounding surprises: exact for `|v| <= 2^53`.
pub fn i64_to_f64(vals: &[i64], out: &mut [f64]) {
    for (o, &v) in out.iter_mut().zip(vals) {
        *o = v as f64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pack_ref(vals: &[u64], width: u32) -> Vec<u8> {
        let mut bits: Vec<bool> = Vec::new();
        for &v in vals {
            for b in 0..width {
                bits.push(v >> b & 1 == 1);
            }
        }
        let words = bits.len().div_ceil(64) + 1;
        let mut out = vec![0u8; words * 8];
        for (i, b) in bits.iter().enumerate() {
            if *b {
                out[i / 8] |= 1 << (i % 8);
            }
        }
        out
    }

    fn rng(seed: u64) -> impl FnMut() -> u64 {
        let mut x = seed | 1;
        move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        }
    }

    #[test]
    fn unpack_matches_reference_for_every_width_start_and_length() {
        let mut r = rng(7);
        for width in 0..=64u32 {
            let mask = if width == 64 { u64::MAX } else { (1u64 << width) - 1 };
            let vals: Vec<u64> = (0..700).map(|_| r() & mask).collect();
            let packed = pack_ref(&vals, width);
            for (start, len) in [(0, 700), (1, 64), (63, 129), (64, 64), (100, 500), (699, 1), (5, 0)] {
                let mut out = vec![u64::MAX; len];
                unpack(&packed, width as u8, start, &mut out);
                assert_eq!(out, vals[start..start + len], "width {width} start {start} len {len}");
            }
        }
        // Short input never panics.
        let mut out = vec![0u64; 100];
        unpack(&[1, 2, 3], 17, 50, &mut out);
    }

    #[test]
    fn predicates_and_sums_match_scalar_reference() {
        let mut r = rng(11);
        for len in [0usize, 1, 63, 64, 65, 2048, 3000] {
            let vals: Vec<i64> = (0..len).map(|_| r() as i64 >> (r() % 60)).collect();
            let uvals: Vec<u64> = vals.iter().map(|&v| v as u64).collect();
            let fvals: Vec<f64> = vals.iter().map(|&v| v as f64 / 7.0).collect();
            let words = len.div_ceil(64);
            let (lo, hi) = (-(1i64 << 40), 1i64 << 41);
            let mut m = vec![0u64; words];
            range_i64(&vals, lo, hi, &mut m);
            for (i, v) in vals.iter().enumerate() {
                assert_eq!(m[i / 64] >> (i % 64) & 1 == 1, (lo..=hi).contains(v));
            }
            let mut mu = vec![0u64; words];
            range_u64(&uvals, 5, u64::MAX / 3, &mut mu);
            for (i, v) in uvals.iter().enumerate() {
                assert_eq!(mu[i / 64] >> (i % 64) & 1 == 1, (5..=u64::MAX / 3).contains(v));
            }
            let mut mf = vec![0u64; words];
            range_f64(&fvals, -1e9, false, 1e10, true, &mut mf);
            for (i, v) in fvals.iter().enumerate() {
                assert_eq!(mf[i / 64] >> (i % 64) & 1 == 1, *v > -1e9 && *v <= 1e10);
            }
            let want: i128 = vals.iter().enumerate().filter(|(i, _)| m[i / 64] >> (i % 64) & 1 == 1).map(|(_, &v)| i128::from(v)).sum();
            assert_eq!(sum_i64_masked(&vals, &m), want);
            assert_eq!(sum_i64(&vals), vals.iter().map(|&v| i128::from(v)).sum::<i128>());
            let mm = vals.iter().enumerate().filter(|(i, _)| m[i / 64] >> (i % 64) & 1 == 1).map(|(_, &v)| v);
            let want_mm = mm.clone().min().zip(mm.max());
            assert_eq!(min_max_i64_masked(&vals, &m), want_mm);
            assert_eq!(min_max_i64(&vals), vals.iter().copied().min().zip(vals.iter().copied().max()));
            let b = abs_bound_i64(&vals);
            let top = vals.iter().map(|v| v.unsigned_abs()).max().unwrap_or(0);
            assert!(top <= b && b / 2 <= top.max(1), "bound {b} for max {top}");
            assert_eq!(count_ones(&m), vals.iter().filter(|v| (lo..=hi).contains(*v)).count() as u64);
        }
        // Extremes sum exactly.
        let ext = vec![i64::MAX; 2048];
        assert_eq!(sum_i64(&ext), i128::from(i64::MAX) * 2048);
        let ext = vec![i64::MIN; 2048];
        assert_eq!(sum_i64_masked(&ext, &[u64::MAX; 32]), i128::from(i64::MIN) * 2048);
    }

    #[test]
    fn avx2_and_portable_copies_are_bit_identical() {
        // The dispatcher picked one copy; the inner bodies are the same code. Compare the
        // public function against a direct scalar recomputation for float predicates, the only
        // kernels where evaluation order could matter.
        let vals: Vec<f64> = (0..5000)
            .map(|i| (f64::from(i) * 0.37).sin() * 1e6)
            .collect();
        let mut a = vec![0u64; vals.len().div_ceil(64)];
        range_f64(&vals, -3e5, true, 2.5e5, false, &mut a);
        let mut b = vec![0u64; a.len()];
        for (i, v) in vals.iter().enumerate() {
            if *v >= -3e5 && *v < 2.5e5 {
                b[i / 64] |= 1 << (i % 64);
            }
        }
        assert_eq!(a, b);
    }
}
