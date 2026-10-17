//! Auto-vectorizing 8-lane SIMD dot-product and cosine similarity scorer for AI memory
//! embeddings (`TASK-027`, `F-OPS-05`).

use storage_types::RecordOrdinal;

/// Result hit from a SIMD top-$K$ vector similarity search.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VectorTopKHit {
    pub ordinal: RecordOrdinal,
    pub score: f32,
}

/// Computes the dot product of two `f32` slices using 8-way unrolled accumulators
/// that auto-vectorize cleanly to AVX2 / AVX-512 / NEON instructions under `target-cpu=native`.
#[inline]
pub fn dot_product_f32(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len().min(b.len());
    let a = &a[..n];
    let b = &b[..n];

    let mut acc0 = 0.0f32;
    let mut acc1 = 0.0f32;
    let mut acc2 = 0.0f32;
    let mut acc3 = 0.0f32;
    let mut acc4 = 0.0f32;
    let mut acc5 = 0.0f32;
    let mut acc6 = 0.0f32;
    let mut acc7 = 0.0f32;

    let mut chunks_a = a.chunks_exact(8);
    let mut chunks_b = b.chunks_exact(8);

    for (ca, cb) in (&mut chunks_a).zip(&mut chunks_b) {
        acc0 = ca[0].mul_add(cb[0], acc0);
        acc1 = ca[1].mul_add(cb[1], acc1);
        acc2 = ca[2].mul_add(cb[2], acc2);
        acc3 = ca[3].mul_add(cb[3], acc3);
        acc4 = ca[4].mul_add(cb[4], acc4);
        acc5 = ca[5].mul_add(cb[5], acc5);
        acc6 = ca[6].mul_add(cb[6], acc6);
        acc7 = ca[7].mul_add(cb[7], acc7);
    }

    let mut sum = ((acc0 + acc1) + (acc2 + acc3)) + ((acc4 + acc5) + (acc6 + acc7));
    for (&xa, &xb) in chunks_a.remainder().iter().zip(chunks_b.remainder().iter()) {
        sum = xa.mul_add(xb, sum);
    }
    sum
}

/// Computes the cosine similarity in `[-1.0, 1.0]` between `a` and `b` in a single 4-way unrolled pass.
#[inline]
pub fn cosine_similarity_f32(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len().min(b.len());
    if n == 0 {
        return 0.0;
    }
    let a = &a[..n];
    let b = &b[..n];

    let mut dot0 = 0.0f32;
    let mut dot1 = 0.0f32;
    let mut dot2 = 0.0f32;
    let mut dot3 = 0.0f32;

    let mut na0 = 0.0f32;
    let mut na1 = 0.0f32;
    let mut nb0 = 0.0f32;
    let mut nb1 = 0.0f32;

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
