//! `f32` dot product and cosine similarity for vector search (`TASK-027`, `F-OPS-05`).
//!
//! The kernels use `f32::mul_add`. On a CPU target with FMA it is one instruction; without it
//! (the portable `x86-64` release build) it is a call to the C library's `fmaf`, about 20
//! times slower. So on x86-64 a second copy is compiled with AVX2 and FMA and chosen at run
//! time. Both copies compute the same fused operations in the same order (`fmaf` is correctly
//! rounded, like the instruction), so scores are bit-identical on every machine. On arm64 FMA
//! is part of the baseline.

use core::sync::atomic::{AtomicU8, Ordering};

/// 0 = not yet detected, 1 = portable, 2 = AVX2 + FMA.
static LEVEL: AtomicU8 = AtomicU8::new(0);

/// Whether the AVX2 + FMA copies are used.
#[inline]
pub fn fma_enabled() -> bool {
    let l = LEVEL.load(Ordering::Relaxed);
    if l != 0 {
        return l == 2;
    }
    #[cfg(target_arch = "x86_64")]
    let fast = std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("fma");
    #[cfg(not(target_arch = "x86_64"))]
    let fast = false;
    LEVEL.store(if fast { 2 } else { 1 }, Ordering::Relaxed);
    fast
}

macro_rules! with_fma {
    ($(#[$m:meta])* pub fn $name:ident($($arg:ident: $ty:ty),* $(,)?) -> $ret:ty $body:block) => {
        $(#[$m])*
        #[inline]
        pub fn $name($($arg: $ty),*) -> $ret {
            #[inline(always)]
            fn imp($($arg: $ty),*) -> $ret $body
            #[cfg(all(target_arch = "x86_64", not(target_feature = "fma")))]
            {
                #[target_feature(enable = "avx2,fma")]
                unsafe fn fast($($arg: $ty),*) -> $ret {
                    imp($($arg),*)
                }
                if fma_enabled() {
                    // SAFETY: `fma_enabled` verified at run time that this CPU has AVX2 and
                    // FMA; the body is safe code either way.
                    return unsafe { fast($($arg),*) };
                }
            }
            imp($($arg),*)
        }
    };
}

with_fma! {
    /// Dot product of two `f32` slices (over the shorter length), with 8 accumulators that
    /// vectorize to AVX2 / AVX-512 / NEON.
    #[allow(clippy::chunks_exact_to_as_chunks)]
    pub fn dot_product_f32(a: &[f32], b: &[f32]) -> f32 {
        let n = a.len().min(b.len());
        let (a, b) = (&a[..n], &b[..n]);
        let mut acc = [0.0f32; 8];
        let mut chunks_a = a.chunks_exact(8);
        let mut chunks_b = b.chunks_exact(8);
        for (ca, cb) in (&mut chunks_a).zip(&mut chunks_b) {
            for i in 0..8 {
                acc[i] = ca[i].mul_add(cb[i], acc[i]);
            }
        }
        let mut sum = ((acc[0] + acc[1]) + (acc[2] + acc[3])) + ((acc[4] + acc[5]) + (acc[6] + acc[7]));
        for (&xa, &xb) in chunks_a.remainder().iter().zip(chunks_b.remainder().iter()) {
            sum = xa.mul_add(xb, sum);
        }
        sum
    }
}

with_fma! {
    /// Cosine similarity in `[-1.0, 1.0]` of `a` and `b` in one pass (`0.0` for an empty or
    /// zero vector).
    #[allow(clippy::chunks_exact_to_as_chunks)]
    pub fn cosine_similarity_f32(a: &[f32], b: &[f32]) -> f32 {
        let n = a.len().min(b.len());
        if n == 0 {
            return 0.0;
        }
        let (a, b) = (&a[..n], &b[..n]);
        let (mut dot0, mut dot1, mut dot2, mut dot3) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
        let (mut na0, mut na1, mut nb0, mut nb1) = (0.0f32, 0.0f32, 0.0f32, 0.0f32);
        let mut chunks_a = a.chunks_exact(4);
        let mut chunks_b = b.chunks_exact(4);
        for (ca, cb) in (&mut chunks_a).zip(&mut chunks_b) {
            dot0 = ca[0].mul_add(cb[0], dot0);
            dot1 = ca[1].mul_add(cb[1], dot1);
            dot2 = ca[2].mul_add(cb[2], dot2);
            dot3 = ca[3].mul_add(cb[3], dot3);
            na0 = ca[0].mul_add(ca[0], ca[1].mul_add(ca[1], na0));
            na1 = ca[2].mul_add(ca[2], ca[3].mul_add(ca[3], na1));
            nb0 = cb[0].mul_add(cb[0], cb[1].mul_add(cb[1], nb0));
            nb1 = cb[2].mul_add(cb[2], cb[3].mul_add(cb[3], nb1));
        }
        let mut dot = (dot0 + dot1) + (dot2 + dot3);
        let mut norm_a = na0 + na1;
        let mut norm_b = nb0 + nb1;
        for (&xa, &xb) in chunks_a.remainder().iter().zip(chunks_b.remainder().iter()) {
            dot = xa.mul_add(xb, dot);
            norm_a = xa.mul_add(xa, norm_a);
            norm_b = xb.mul_add(xb, norm_b);
        }
        let denom = (norm_a * norm_b).sqrt();
        if denom <= f32::EPSILON {
            0.0
        } else {
            dot / denom
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_copies_score_bit_for_bit_alike() {
        let a: Vec<f32> = (0..131).map(|i| ((i * 37 % 101) as f32 - 50.0) / 7.0).collect();
        let b: Vec<f32> = (0..131).map(|i| ((i * 53 % 97) as f32 - 40.0) / 3.0).collect();
        for n in [0, 1, 3, 4, 7, 8, 9, 64, 131] {
            let (x, y) = (&a[..n], &b[..n]);
            // `imp` is private to each function; compare the dispatched result with a plain
            // fused reference computed in the same order.
            let mut acc = [0.0f32; 8];
            let full = n / 8 * 8;
            for c in 0..n / 8 {
                for i in 0..8 {
                    acc[i] = x[c * 8 + i].mul_add(y[c * 8 + i], acc[i]);
                }
            }
            let mut want = ((acc[0] + acc[1]) + (acc[2] + acc[3])) + ((acc[4] + acc[5]) + (acc[6] + acc[7]));
            for i in full..n {
                want = x[i].mul_add(y[i], want);
            }
            assert_eq!(dot_product_f32(x, y).to_bits(), want.to_bits(), "dot n={n}");
        }
        assert_eq!(cosine_similarity_f32(&[1.0, 0.0], &[1.0, 0.0]), 1.0);
        assert_eq!(cosine_similarity_f32(&[1.0, 0.0], &[0.0, 1.0]), 0.0);
        assert_eq!(cosine_similarity_f32(&[], &[]), 0.0);
        assert!((cosine_similarity_f32(&a, &a) - 1.0).abs() < 1e-5);
    }
}
