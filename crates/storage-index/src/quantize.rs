//! `SQ8` 8-Bit Scalar Vector Quantization & 8-Way Unrolled Integer SIMD Kernels
//! (`TASK-036`, `F-INDX-07`, `RULE-CPU-02`, `RULE-DATA-02`).
//!
//! Compresses `f32` embedding vectors by `4x` (`4 bytes/dim -> 1 byte/dim`) via affine min-max
//! quantization:
//! $$x_i \approx \text{offset} + \text{scale} \cdot q_i, \quad q_i \in \{0, \dots, 255\}$$
//!
//! Computes inner products in the quantized domain using 8-way unrolled integer accumulators
//! (`u32`) and performs a single floating-point affine reconstruction at the end of the loop:
//! $$\sum_{i=1}^D x_i y_i \approx D \cdot o_x o_y + o_x s_y \sum q_{y,i} + o_y s_x \sum q_{x,i} + s_x s_y \sum q_{x,i} q_{y,i}$$

/// Compact 8-bit scalar-quantized embedding vector (`4x` smaller than `[f32]`).
#[derive(Clone, Debug, PartialEq)]
pub struct QuantizedVecSq8 {
    /// Affine scale `(max - min) / 255.0`.
    pub scale: f32,
    /// Affine minimum offset `min`.
    pub offset: f32,
    /// Precomputed L2 norm `||x||_2` of the original `f32` vector for 1-instruction cosine normalization.
    pub l2_norm: f32,
    /// Precomputed sum of quantized bytes $\sum_{i=1}^D q_i$ so inner products never re-sum `q_i`!
    pub sum_q: u32,
    /// Quantized `u8` codes (`1 byte` per dimension).
    pub codes: Vec<u8>,
}

impl QuantizedVecSq8 {
    /// Quantizes an `f32` slice into a `4x`-compressed `QuantizedVecSq8`.
    pub fn quantize(vec: &[f32]) -> Self {
        if vec.is_empty() {
            return Self {
                scale: 0.0,
                offset: 0.0,
                l2_norm: 0.0,
                sum_q: 0,
                codes: Vec::new(),
            };
        }

        let mut min_v = f32::INFINITY;
        let mut max_v = f32::NEG_INFINITY;
        let mut norm_sq = 0.0f32;

        for &v in vec {
            if v < min_v {
                min_v = v;
            }
            if v > max_v {
                max_v = v;
            }
            norm_sq += v * v;
        }

        let l2_norm = norm_sq.sqrt();
        let range = max_v - min_v;
        if range <= 1e-12 || !range.is_finite() {
            return Self {
                scale: 0.0,
                offset: if min_v.is_finite() { min_v } else { 0.0 },
                l2_norm,
                sum_q: 0,
                codes: vec![0u8; vec.len()],
            };
        }

        let scale = range / 255.0;
        let inv_scale = 255.0 / range;
        let mut codes = Vec::with_capacity(vec.len());
        let mut sum_q = 0u32;

        for &v in vec {
            let q = ((v - min_v) * inv_scale + 0.5).clamp(0.0, 255.0) as u8;
            sum_q += u32::from(q);
            codes.push(q);
        }

        Self {
            scale,
            offset: min_v,
            l2_norm,
            sum_q,
            codes,
        }
    }

    /// Reconstructs the approximate `f32` vector from its `SQ8` codes.
    pub fn dequantize(&self) -> Vec<f32> {
        self.codes
            .iter()
            .map(|&q| self.offset + self.scale * (q as f32))
            .collect()
    }

    #[inline]
    pub fn dim(&self) -> usize {
        self.codes.len()
    }

    /// Serializes this `QuantizedVecSq8` into `out` (`16` header bytes + `D` code bytes).
    pub fn write_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.scale.to_le_bytes());
        out.extend_from_slice(&self.offset.to_le_bytes());
        out.extend_from_slice(&self.l2_norm.to_le_bytes());
        out.extend_from_slice(&self.sum_q.to_le_bytes());
        out.extend_from_slice(&self.codes);
    }

    /// Deserializes a `QuantizedVecSq8` of dimension `dim` from `slice`.
    pub fn read_from(slice: &[u8], dim: usize) -> Option<Self> {
        if slice.len() < 16 + dim {
            return None;
        }
        let scale = f32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]);
        let offset = f32::from_le_bytes([slice[4], slice[5], slice[6], slice[7]]);
        let l2_norm = f32::from_le_bytes([slice[8], slice[9], slice[10], slice[11]]);
        let sum_q = u32::from_le_bytes([slice[12], slice[13], slice[14], slice[15]]);
        let codes = slice[16..16 + dim].to_vec();
        Some(Self {
            scale,
            offset,
            l2_norm,
            sum_q,
            codes,
        })
    }
}

/// 8-way unrolled integer dot product $\sum_{i=0}^{n-1} a_i b_i$ over two `u8` slices (`RULE-CPU-02`).
/// Compiles to vectorized `pmaddubsw` / `vpdpbusd` / `udot` instructions under `-C opt-level=3`.
#[inline]
pub fn dot_u8_unrolled(a: &[u8], b: &[u8]) -> u32 {
    let n = a.len().min(b.len());
    let a = &a[..n];
    let b = &b[..n];

    let chunks = n / 8;
    let mut s0 = 0u32;
    let mut s1 = 0u32;
    let mut s2 = 0u32;
    let mut s3 = 0u32;
    let mut s4 = 0u32;
    let mut s5 = 0u32;
    let mut s6 = 0u32;
    let mut s7 = 0u32;

    for i in 0..chunks {
        let base = i * 8;
        s0 += u32::from(a[base]) * u32::from(b[base]);
        s1 += u32::from(a[base + 1]) * u32::from(b[base + 1]);
        s2 += u32::from(a[base + 2]) * u32::from(b[base + 2]);
        s3 += u32::from(a[base + 3]) * u32::from(b[base + 3]);
        s4 += u32::from(a[base + 4]) * u32::from(b[base + 4]);
        s5 += u32::from(a[base + 5]) * u32::from(b[base + 5]);
        s6 += u32::from(a[base + 6]) * u32::from(b[base + 6]);
        s7 += u32::from(a[base + 7]) * u32::from(b[base + 7]);
    }

    let mut total = (s0 + s1) + (s2 + s3) + (s4 + s5) + (s6 + s7);
    for i in (chunks * 8)..n {
        total += u32::from(a[i]) * u32::from(b[i]);
    }
    total
}

/// Computes the reconstructed dot product between two `QuantizedVecSq8` vectors using
/// `dot_u8_unrolled` and precomputed `sum_q` terms.
#[inline]
pub fn sq8_dot_product(a: &QuantizedVecSq8, b: &QuantizedVecSq8) -> f32 {
    let n = a.codes.len().min(b.codes.len());
    if n == 0 {
        return 0.0;
    }
    let q_dot = dot_u8_unrolled(&a.codes[..n], &b.codes[..n]) as f32;
    let dim_f = n as f32;
    dim_f * a.offset * b.offset
        + a.offset * b.scale * (b.sum_q as f32)
        + b.offset * a.scale * (a.sum_q as f32)
        + a.scale * b.scale * q_dot
}

/// Computes the cosine similarity between two `QuantizedVecSq8` vectors in `[-1.0, 1.0]`.
#[inline]
pub fn sq8_cosine_similarity(a: &QuantizedVecSq8, b: &QuantizedVecSq8) -> f32 {
    let denom = a.l2_norm * b.l2_norm;
    if denom <= 1e-12 {
        return 0.0;
    }
    (sq8_dot_product(a, b) / denom).clamp(-1.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::cosine_similarity_f32;

    #[test]
    fn test_sq8_quantization_4x_compression_and_accuracy() {
        let a: Vec<f32> = (0..128).map(|i| ((i as f32) * 0.13).sin()).collect();
        let b: Vec<f32> = (0..128).map(|i| ((i as f32) * 0.13 + 0.2).sin()).collect();

        let qa = QuantizedVecSq8::quantize(&a);
        let qb = QuantizedVecSq8::quantize(&b);

        // 1 byte per dimension instead of 4 bytes per dimension
        assert_eq!(qa.codes.len(), 128);

        let exact_cos = cosine_similarity_f32(&a, &b);
        let sq8_cos = sq8_cosine_similarity(&qa, &qb);
        assert!(
            (exact_cos - sq8_cos).abs() < 0.01,
            "exact={exact_cos} vs sq8={sq8_cos}"
        );
    }
}
