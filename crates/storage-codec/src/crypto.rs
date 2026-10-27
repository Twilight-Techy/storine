//! Zero-Dependency Authenticated At-Rest Encryption (`TASK-044`, `F-SEC-02`).
//!
//! Implements the standard RFC 8439 `ChaCha20-Poly1305` AEAD construction (ChaCha20 20-round
//! stream cipher with a 256-bit key and 96-bit nonce, authenticated by a Poly1305 one-time MAC
//! whose key is derived from keystream block 0). Tags are compared in constant time.
//!
//! Nonces must never repeat under the same key. [`seal`] generates a fresh unique nonce per call
//! and stores it in front of the ciphertext, so callers never have to manage nonces themselves.
//!
//! The engine seals record frames with it (`RecordCodec::encrypt_frame` /
//! `RecordCodec::authenticate_frame`) using the in-place primitives [`seal_in_place`] and
//! [`open_in_place`], which work directly on the destination buffer (no intermediate copies).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use storage_types::wire::{read_u32_le, read_u64_le};
use storage_types::{Result, StorageError};

/// Size of the appended authentication tag in bytes (`128 bits`).
pub const AEAD_TAG_LEN: usize = 16;
/// Size of the ChaCha20-Poly1305 nonce in bytes (`96 bits`).
pub const AEAD_NONCE_LEN: usize = 12;

/// "expand 32-byte k".
const SIGMA: [u32; 4] = [0x6170_7865, 0x3320_646e, 0x7962_2d32, 0x6b20_6574];

/// RFC 8439 §2.3 initial state for block `counter`.
#[inline(always)]
fn initial_state(key: &[u8; 32], counter: u32, nonce: &[u8; 12]) -> [u32; 16] {
    let mut state = [0u32; 16];
    state[..4].copy_from_slice(&SIGMA);
    for (i, dst) in state[4..12].iter_mut().enumerate() {
        *dst = read_u32_le(key, 4 * i);
    }
    state[12] = counter;
    for (i, dst) in state[13..].iter_mut().enumerate() {
        *dst = read_u32_le(nonce, 4 * i);
    }
    state
}

#[inline(always)]
fn quarter_round(state: &mut [u32; 16], a: usize, b: usize, c: usize, d: usize) {
    state[a] = state[a].wrapping_add(state[b]);
    state[d] ^= state[a];
    state[d] = state[d].rotate_left(16);

    state[c] = state[c].wrapping_add(state[d]);
    state[b] ^= state[c];
    state[b] = state[b].rotate_left(12);

    state[a] = state[a].wrapping_add(state[b]);
    state[d] ^= state[a];
    state[d] = state[d].rotate_left(8);

    state[c] = state[c].wrapping_add(state[d]);
    state[b] ^= state[c];
    state[b] = state[b].rotate_left(7);
}

/// 20 rounds + feed-forward of one block.
#[inline(always)]
fn block_words(state: &[u32; 16]) -> [u32; 16] {
    let mut working = *state;
    for _ in 0..10 {
        // Column rounds
        quarter_round(&mut working, 0, 4, 8, 12);
        quarter_round(&mut working, 1, 5, 9, 13);
        quarter_round(&mut working, 2, 6, 10, 14);
        quarter_round(&mut working, 3, 7, 11, 15);
        // Diagonal rounds
        quarter_round(&mut working, 0, 5, 10, 15);
        quarter_round(&mut working, 1, 6, 11, 12);
        quarter_round(&mut working, 2, 7, 8, 13);
        quarter_round(&mut working, 3, 4, 9, 14);
    }
    for (w, s) in working.iter_mut().zip(state) {
        *w = w.wrapping_add(*s);
    }
    working
}

/// Generates a single 64-byte RFC 8439 ChaCha20 keystream block.
#[inline]
pub fn chacha20_block(key: &[u8; 32], counter: u32, nonce: &[u8; 12]) -> [u8; 64] {
    let words = block_words(&initial_state(key, counter, nonce));
    let mut out = [0u8; 64];
    for (dst, w) in out.chunks_exact_mut(4).zip(words) {
        dst.copy_from_slice(&w.to_le_bytes());
    }
    out
}

/// x86-64 multi-block ChaCha20 (`TASK-087`): 8 blocks per step with AVX2 (runtime-detected),
/// 4 blocks per step with SSE2 (part of the x86-64 baseline). Each vector register holds the
/// same state word of 4 / 8 consecutive blocks, so every round operation covers all of them;
/// the keystream is transposed back into block order only for the final XOR.
#[cfg(target_arch = "x86_64")]
mod x86 {
    use core::arch::x86_64::*;

    /// Encrypts the largest multiple-of-256-bytes prefix of `data`, advancing `state[12]`, and
    /// returns the unprocessed tail (`< 256` bytes).
    pub(super) fn xor_bulk<'a>(state: &mut [u32; 16], data: &'a mut [u8]) -> &'a mut [u8] {
        let data = if std::arch::is_x86_feature_detected!("avx2") {
            let (octets, tail) = data.split_at_mut(data.len() / 512 * 512);
            for chunk in octets.chunks_exact_mut(512) {
                // SAFETY: AVX2 support was detected at runtime just above; `chunk` is 512 bytes.
                unsafe { xor_blocks8(state, chunk) };
                state[12] = state[12].wrapping_add(8);
            }
            tail
        } else {
            data
        };
        let (quads, tail) = data.split_at_mut(data.len() / 256 * 256);
        for chunk in quads.chunks_exact_mut(256) {
            // SAFETY: SSE2 is part of the x86-64 baseline, so every x86-64 CPU supports it;
            // `chunk` is 256 bytes.
            unsafe { xor_blocks4(state, chunk) };
            state[12] = state[12].wrapping_add(4);
        }
        tail
    }

    /// XORs the 16 keystream bytes `ks` into `chunk[off..off + 16]`.
    #[inline]
    #[target_feature(enable = "sse2")]
    #[allow(unused_unsafe)]
    unsafe fn xor16(chunk: &mut [u8], off: usize, ks: __m128i) {
        let dst = &mut chunk[off..off + 16];
        let p = dst.as_mut_ptr().cast::<__m128i>();
        // SAFETY: `p` points to the 16 bytes of `dst` (exclusively borrowed); unaligned loads and
        // stores are allowed for `__m128i` via `loadu` / `storeu`; SSE2 is enabled for this fn.
        unsafe { _mm_storeu_si128(p, _mm_xor_si128(_mm_loadu_si128(p), ks)) };
    }

    macro_rules! rotl128 {
        ($x:expr, $r:literal) => {
            _mm_or_si128(_mm_slli_epi32::<$r>($x), _mm_srli_epi32::<{ 32 - $r }>($x))
        };
    }

    #[inline]
    #[target_feature(enable = "sse2")]
    #[allow(unused_unsafe)]
    unsafe fn quarter_round4(v: &mut [__m128i; 16], a: usize, b: usize, c: usize, d: usize) {
        // SAFETY: the caller guarantees the CPU supports this fn's target feature.
        unsafe {
            v[a] = _mm_add_epi32(v[a], v[b]);
            v[d] = _mm_xor_si128(v[d], v[a]);
            v[d] = rotl128!(v[d], 16);
            v[c] = _mm_add_epi32(v[c], v[d]);
            v[b] = _mm_xor_si128(v[b], v[c]);
            v[b] = rotl128!(v[b], 12);
            v[a] = _mm_add_epi32(v[a], v[b]);
            v[d] = _mm_xor_si128(v[d], v[a]);
            v[d] = rotl128!(v[d], 8);
            v[c] = _mm_add_epi32(v[c], v[d]);
            v[b] = _mm_xor_si128(v[b], v[c]);
            v[b] = rotl128!(v[b], 7);        }

    }

    #[inline]
    #[target_feature(enable = "sse2")]
    #[allow(unused_unsafe)]
    unsafe fn xor_blocks4(state: &[u32; 16], chunk: &mut [u8]) {
        // SAFETY: the caller guarantees the CPU supports this fn's target feature.
        unsafe {
            let mut init = [_mm_setzero_si128(); 16];
            for (v, &s) in init.iter_mut().zip(state) {
                *v = _mm_set1_epi32(s as i32);
            }
            let c = state[12] as i32;
            init[12] = _mm_set_epi32(c.wrapping_add(3), c.wrapping_add(2), c.wrapping_add(1), c);
            let mut v = init;
            for _ in 0..10 {
                quarter_round4(&mut v, 0, 4, 8, 12);
                quarter_round4(&mut v, 1, 5, 9, 13);
                quarter_round4(&mut v, 2, 6, 10, 14);
                quarter_round4(&mut v, 3, 7, 11, 15);
                quarter_round4(&mut v, 0, 5, 10, 15);
                quarter_round4(&mut v, 1, 6, 11, 12);
                quarter_round4(&mut v, 2, 7, 8, 13);
                quarter_round4(&mut v, 3, 4, 9, 14);
            }
            for (x, i) in v.iter_mut().zip(&init) {
                *x = _mm_add_epi32(*x, *i);
            }
            // Words 4g..4g+3 of the 4 blocks -> 4x4 transpose -> 16 contiguous bytes per block.
            for g in 0..4 {
                let (a, b, c, d) = (v[4 * g], v[4 * g + 1], v[4 * g + 2], v[4 * g + 3]);
                let (t0, t1) = (_mm_unpacklo_epi32(a, b), _mm_unpacklo_epi32(c, d));
                let (t2, t3) = (_mm_unpackhi_epi32(a, b), _mm_unpackhi_epi32(c, d));
                let rows = [
                    _mm_unpacklo_epi64(t0, t1),
                    _mm_unpackhi_epi64(t0, t1),
                    _mm_unpacklo_epi64(t2, t3),
                    _mm_unpackhi_epi64(t2, t3),
                ];
                for (block, ks) in rows.into_iter().enumerate() {
                    xor16(chunk, 64 * block + 16 * g, ks);
                }
            }        }

    }

    macro_rules! rotl256 {
        ($x:expr, $r:literal) => {
            _mm256_or_si256(_mm256_slli_epi32::<$r>($x), _mm256_srli_epi32::<{ 32 - $r }>($x))
        };
    }

    #[inline]
    #[target_feature(enable = "avx2")]
    #[allow(unused_unsafe)]
    unsafe fn quarter_round8(v: &mut [__m256i; 16], a: usize, b: usize, c: usize, d: usize) {
        // SAFETY: the caller guarantees the CPU supports this fn's target feature.
        unsafe {
            // Byte shuffles rotate by 16 and 8 in one instruction.
            let rot16 = _mm256_set_epi8(
                13, 12, 15, 14, 9, 8, 11, 10, 5, 4, 7, 6, 1, 0, 3, 2, 13, 12, 15, 14, 9, 8, 11, 10, 5, 4,
                7, 6, 1, 0, 3, 2,
            );
            let rot8 = _mm256_set_epi8(
                14, 13, 12, 15, 10, 9, 8, 11, 6, 5, 4, 7, 2, 1, 0, 3, 14, 13, 12, 15, 10, 9, 8, 11, 6, 5,
                4, 7, 2, 1, 0, 3,
            );
            v[a] = _mm256_add_epi32(v[a], v[b]);
            v[d] = _mm256_shuffle_epi8(_mm256_xor_si256(v[d], v[a]), rot16);
            v[c] = _mm256_add_epi32(v[c], v[d]);
            v[b] = _mm256_xor_si256(v[b], v[c]);
            v[b] = rotl256!(v[b], 12);
            v[a] = _mm256_add_epi32(v[a], v[b]);
            v[d] = _mm256_shuffle_epi8(_mm256_xor_si256(v[d], v[a]), rot8);
            v[c] = _mm256_add_epi32(v[c], v[d]);
            v[b] = _mm256_xor_si256(v[b], v[c]);
            v[b] = rotl256!(v[b], 7);        }

    }

    #[inline]
    #[target_feature(enable = "avx2")]
    #[allow(unused_unsafe)]
    unsafe fn xor_blocks8(state: &[u32; 16], chunk: &mut [u8]) {
        // SAFETY: the caller guarantees the CPU supports this fn's target feature.
        unsafe {
            let mut init = [_mm256_setzero_si256(); 16];
            for (v, &s) in init.iter_mut().zip(state) {
                *v = _mm256_set1_epi32(s as i32);
            }
            let c = state[12] as i32;
            init[12] = _mm256_set_epi32(
                c.wrapping_add(7),
                c.wrapping_add(6),
                c.wrapping_add(5),
                c.wrapping_add(4),
                c.wrapping_add(3),
                c.wrapping_add(2),
                c.wrapping_add(1),
                c,
            );
            let mut v = init;
            for _ in 0..10 {
                quarter_round8(&mut v, 0, 4, 8, 12);
                quarter_round8(&mut v, 1, 5, 9, 13);
                quarter_round8(&mut v, 2, 6, 10, 14);
                quarter_round8(&mut v, 3, 7, 11, 15);
                quarter_round8(&mut v, 0, 5, 10, 15);
                quarter_round8(&mut v, 1, 6, 11, 12);
                quarter_round8(&mut v, 2, 7, 8, 13);
                quarter_round8(&mut v, 3, 4, 9, 14);
            }
            for (x, i) in v.iter_mut().zip(&init) {
                *x = _mm256_add_epi32(*x, *i);
            }
            // Per 128-bit lane the same 4x4 transpose as SSE2: the low lane holds blocks 0..3, the
            // high lane blocks 4..7.
            for g in 0..4 {
                let (a, b, c, d) = (v[4 * g], v[4 * g + 1], v[4 * g + 2], v[4 * g + 3]);
                let (t0, t1) = (_mm256_unpacklo_epi32(a, b), _mm256_unpacklo_epi32(c, d));
                let (t2, t3) = (_mm256_unpackhi_epi32(a, b), _mm256_unpackhi_epi32(c, d));
                let rows = [
                    _mm256_unpacklo_epi64(t0, t1),
                    _mm256_unpackhi_epi64(t0, t1),
                    _mm256_unpacklo_epi64(t2, t3),
                    _mm256_unpackhi_epi64(t2, t3),
                ];
                for (block, ks) in rows.into_iter().enumerate() {
                    xor16(chunk, 64 * block + 16 * g, _mm256_castsi256_si128(ks));
                    xor16(chunk, 64 * (block + 4) + 16 * g, _mm256_extracti128_si256::<1>(ks));
                }
            }        }

    }
}

/// Encrypts or decrypts `data` in-place using ChaCha20 starting at `initial_counter`.
///
/// On x86-64, whole 256 / 512-byte chunks take the SIMD multi-block path (SSE2 / AVX2, about
/// 3-5x the one-block loop); the tail and other targets use one block at a time with word-wide XOR.
pub fn chacha20_xor_in_place(
    key: &[u8; 32],
    nonce: &[u8; 12],
    initial_counter: u32,
    data: &mut [u8],
) {
    let mut state = initial_state(key, initial_counter, nonce);
    #[cfg(target_arch = "x86_64")]
    let data = x86::xor_bulk(&mut state, data);
    let mut blocks = data.chunks_exact_mut(64);
    for block in &mut blocks {
        let ks = block_words(&state);
        state[12] = state[12].wrapping_add(1);
        for (w, k) in block.chunks_exact_mut(4).zip(ks) {
            let x = read_u32_le(w, 0) ^ k;
            w.copy_from_slice(&x.to_le_bytes());
        }
    }
    let rest = blocks.into_remainder();
    if !rest.is_empty() {
        let ks = block_words(&state);
        for (i, b) in rest.iter_mut().enumerate() {
            *b ^= (ks[i / 4] >> (8 * (i % 4))) as u8;
        }
    }
}

const M44: u64 = 0xfff_ffff_ffff;
const M42: u64 = 0x3ff_ffff_ffff;
/// The `2^128` bit appended to every full 16-byte block (bit 40 of the top limb).
const HIBIT: u64 = 1 << 40;

/// RFC 8439 §2.5 Poly1305 one-time authenticator: radix 2^44 (three 64-bit limbs, 9 widening
/// multiplies per block instead of 25 with 26-bit limbs; about 1.4x faster, `TASK-087`).
struct Poly1305 {
    r: [u64; 3],
    /// `r1 * 20`, `r2 * 20` (the `* 5` reduction folded with the 2-bit limb offset).
    s: [u64; 2],
    h: [u64; 3],
    pad: [u64; 2],
}

impl Poly1305 {
    fn new(key: &[u8; 32]) -> Self {
        let (t0, t1) = (read_u64_le(key, 0), read_u64_le(key, 8));
        // Clamp `r` while splitting it into 44 / 44 / 42-bit limbs.
        let r = [
            t0 & 0xffc_0fff_ffff,
            ((t0 >> 44) | (t1 << 20)) & 0xfff_ffc0_ffff,
            (t1 >> 24) & 0x00f_ffff_fc0f,
        ];
        Self {
            r,
            s: [r[1] * 20, r[2] * 20],
            h: [0; 3],
            pad: [read_u64_le(key, 16), read_u64_le(key, 24)],
        }
    }

    /// Absorbs one 16-byte block; `hibit` is [`HIBIT`] for full blocks, `0` for the final
    /// partial block (which carries its own appended `0x01` byte).
    #[inline(always)]
    fn block(&mut self, m: &[u8; 16], hibit: u64) {
        let (t0, t1) = (read_u64_le(m, 0), read_u64_le(m, 8));
        let h0 = u128::from(self.h[0] + (t0 & M44));
        let h1 = u128::from(self.h[1] + (((t0 >> 44) | (t1 << 20)) & M44));
        let h2 = u128::from(self.h[2] + (((t1 >> 24) & M42) | hibit));
        let [r0, r1, r2] = self.r.map(u128::from);
        let [s1, s2] = self.s.map(u128::from);

        let d0 = h0 * r0 + h1 * s2 + h2 * s1;
        let mut d1 = h0 * r1 + h1 * r0 + h2 * s2;
        let mut d2 = h0 * r2 + h1 * r1 + h2 * r0;

        let mut c = (d0 >> 44) as u64;
        let mut n0 = (d0 as u64) & M44;
        d1 += u128::from(c);
        c = (d1 >> 44) as u64;
        let mut n1 = (d1 as u64) & M44;
        d2 += u128::from(c);
        c = (d2 >> 42) as u64;
        let n2 = (d2 as u64) & M42;
        n0 += c * 5;
        c = n0 >> 44;
        n0 &= M44;
        n1 += c;
        self.h = [n0, n1, n2];
    }

    /// Absorbs `data`, zero-padding the final partial block to 16 bytes (AEAD `pad16` semantics).
    #[allow(clippy::chunks_exact_to_as_chunks)] // `as_chunks` needs Rust 1.88 (MSRV 1.82)
    fn update_padded(&mut self, data: &[u8]) {
        let mut blocks = data.chunks_exact(16);
        for block in &mut blocks {
            let mut b = [0u8; 16];
            b.copy_from_slice(block);
            self.block(&b, HIBIT);
        }
        let rest = blocks.remainder();
        if !rest.is_empty() {
            let mut block = [0u8; 16];
            block[..rest.len()].copy_from_slice(rest);
            self.block(&block, HIBIT);
        }
    }

    fn finish(self) -> [u8; 16] {
        let [mut h0, mut h1, mut h2] = self.h;

        // Full carry of h.
        let mut c = h1 >> 44;
        h1 &= M44;
        h2 += c;
        c = h2 >> 42;
        h2 &= M42;
        h0 += c * 5;
        c = h0 >> 44;
        h0 &= M44;
        h1 += c;
        c = h1 >> 44;
        h1 &= M44;
        h2 += c;
        c = h2 >> 42;
        h2 &= M42;
        h0 += c * 5;
        c = h0 >> 44;
        h0 &= M44;
        h1 += c;

        // g = h + -p = h - (2^130 - 5); select h or g in constant time.
        let mut g0 = h0 + 5;
        c = g0 >> 44;
        g0 &= M44;
        let mut g1 = h1 + c;
        c = g1 >> 44;
        g1 &= M44;
        let mut g2 = (h2 + c).wrapping_sub(1 << 42);
        let mask = (g2 >> 63).wrapping_sub(1); // all ones if g2 did not underflow
        g0 &= mask;
        g1 &= mask;
        g2 &= mask;
        let keep = !mask;
        h0 = (h0 & keep) | g0;
        h1 = (h1 & keep) | g1;
        h2 = (h2 & keep) | g2;

        // tag = (h + s) mod 2^128
        let [t0, t1] = self.pad;
        h0 += t0 & M44;
        c = h0 >> 44;
        h0 &= M44;
        h1 += (((t0 >> 44) | (t1 << 20)) & M44) + c;
        c = h1 >> 44;
        h1 &= M44;
        h2 += ((t1 >> 24) & M42) + c;
        h2 &= M42;
        let lo = h0 | (h1 << 44);
        let hi = (h1 >> 20) | (h2 << 24);

        let mut tag = [0u8; 16];
        tag[..8].copy_from_slice(&lo.to_le_bytes());
        tag[8..].copy_from_slice(&hi.to_le_bytes());
        tag
    }
}

/// Computes a raw Poly1305 tag of `msg` under the 32-byte one-time `key` (RFC 8439 §2.5).
#[allow(clippy::chunks_exact_to_as_chunks)] // `as_chunks` needs Rust 1.88 (MSRV 1.82)
pub fn poly1305_tag(key: &[u8; 32], msg: &[u8]) -> [u8; 16] {
    let mut p = Poly1305::new(key);
    let mut blocks = msg.chunks_exact(16);
    for block in &mut blocks {
        let mut b = [0u8; 16];
        b.copy_from_slice(block);
        p.block(&b, HIBIT);
    }
    let rem = blocks.remainder();
    if !rem.is_empty() {
        let mut block = [0u8; 16];
        block[..rem.len()].copy_from_slice(rem);
        block[rem.len()] = 1;
        p.block(&block, 0);
    }
    p.finish()
}

/// RFC 8439 §2.8 AEAD tag over `aad` and `ciphertext`.
pub(crate) fn aead_tag(key: &[u8; 32], nonce: &[u8; 12], aad: &[u8], ciphertext: &[u8]) -> [u8; 16] {
    let block0 = chacha20_block(key, 0, nonce);
    let mut otk = [0u8; 32];
    otk.copy_from_slice(&block0[..32]);
    let mut p = Poly1305::new(&otk);
    p.update_padded(aad);
    p.update_padded(ciphertext);
    let mut lens = [0u8; 16];
    lens[..8].copy_from_slice(&(aad.len() as u64).to_le_bytes());
    lens[8..].copy_from_slice(&(ciphertext.len() as u64).to_le_bytes());
    p.block(&lens, HIBIT);
    p.finish()
}

/// XORs the ChaCha20 keystream of AEAD message byte `offset` onwards into `buf` (block counter
/// `1 + offset / 64`, the RFC 8439 payload numbering). Random access into a region sealed with
/// [`RegionSealer`]: decrypts any byte range without touching the rest. Fails if the region is
/// beyond the 256 GiB a 32-bit block counter can address.
pub fn chacha20_xor_at(key: &EncryptionKey, nonce: &[u8; 12], offset: u64, buf: &mut [u8]) -> Result<()> {
    let in_range = offset
        .checked_add(buf.len() as u64)
        .is_some_and(|end| end.div_ceil(64) < u64::from(u32::MAX));
    if !in_range {
        return Err(StorageError::invalid_archive("encrypted region exceeds the ChaCha20 counter range"));
    }
    let first_block = (offset / 64) as u32 + 1;
    let skip = (offset % 64) as usize;
    if skip == 0 {
        chacha20_xor_in_place(&key.0, nonce, first_block, buf);
        return Ok(());
    }
    let head = (64 - skip).min(buf.len());
    let ks = chacha20_block(&key.0, first_block, nonce);
    for (b, k) in buf[..head].iter_mut().zip(&ks[skip..]) {
        *b ^= *k;
    }
    chacha20_xor_in_place(&key.0, nonce, first_block.wrapping_add(1), &mut buf[head..]);
    Ok(())
}

/// Streaming RFC 8439 ChaCha20-Poly1305 over one contiguous region written or read in pieces
/// (`TASK-088`: paged index files). The result is byte-identical to sealing the whole region at
/// once with `aad`. Every piece but the last must be a multiple of 64 bytes.
pub struct RegionSealer {
    /// Own copy (zeroed on drop), so a sealer can outlive the key ring it came from.
    key: EncryptionKey,
    nonce: [u8; 12],
    poly: Poly1305,
    aad_len: u64,
    len: u64,
    closed: bool,
}

impl core::fmt::Debug for RegionSealer {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RegionSealer").field("len", &self.len).finish_non_exhaustive()
    }
}

impl RegionSealer {
    pub fn new(key: &EncryptionKey, nonce: [u8; 12], aad: &[u8]) -> Self {
        let block0 = chacha20_block(&key.0, 0, &nonce);
        let mut otk = [0u8; 32];
        otk.copy_from_slice(&block0[..32]);
        let mut poly = Poly1305::new(&otk);
        poly.update_padded(aad);
        Self { key: key.clone(), nonce, poly, aad_len: aad.len() as u64, len: 0, closed: false }
    }

    fn advance(&mut self, n: usize) -> Result<u64> {
        if self.closed {
            return Err(StorageError::invalid_archive("region piece after a partial piece"));
        }
        let at = self.len;
        self.len += n as u64;
        self.closed = n % 64 != 0;
        Ok(at)
    }

    /// Encrypts the next piece of the region in place and authenticates it.
    pub fn seal_piece(&mut self, buf: &mut [u8]) -> Result<()> {
        let at = self.advance(buf.len())?;
        chacha20_xor_at(&self.key, &self.nonce, at, buf)?;
        self.poly.update_padded(buf);
        Ok(())
    }

    /// Authenticates the next piece of ciphertext (verification side; nothing is decrypted).
    pub fn absorb_ciphertext(&mut self, ciphertext: &[u8]) -> Result<()> {
        self.advance(ciphertext.len())?;
        self.poly.update_padded(ciphertext);
        Ok(())
    }

    /// The region's tag.
    pub fn finish(mut self) -> [u8; 16] {
        let mut lens = [0u8; 16];
        lens[..8].copy_from_slice(&self.aad_len.to_le_bytes());
        lens[8..].copy_from_slice(&self.len.to_le_bytes());
        self.poly.block(&lens, HIBIT);
        self.poly.finish()
    }

    /// Verifies the region's tag in constant time.
    pub fn verify(self, tag: &[u8; 16]) -> Result<()> {
        if verify_mac_tag_ct(tag, &self.finish()) {
            Ok(())
        } else {
            Err(StorageError::corrupted_header(
                "encrypted region authentication failed (tampered file or wrong key)",
            ))
        }
    }
}

/// Constant-time 16-byte MAC comparison to prevent timing side-channels.
#[inline]
pub fn verify_mac_tag_ct(a: &[u8; 16], b: &[u8; 16]) -> bool {
    let mut diff = 0u8;
    for i in 0..16 {
        diff |= a[i] ^ b[i];
    }
    core::hint::black_box(diff) == 0
}

/// A 256-bit at-rest encryption key (`F-SEC-02`).
///
/// Never printed (`Debug` is redacted) and overwritten with zeros when dropped. Key management
/// (generation, storage, rotation) is the caller's responsibility.
#[derive(Clone)]
pub struct EncryptionKey([u8; 32]);

impl EncryptionKey {
    #[inline]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[inline]
    pub(crate) const fn bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Encryption keys by id (`F-SEC-02`). Every sealed frame records the id of the key that sealed
/// it, so keys can be rotated: new writes use the active key while older ids still decrypt.
#[derive(Clone, Default)]
pub struct KeyRing {
    keys: Vec<(u32, EncryptionKey)>,
}

impl KeyRing {
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds (or replaces) key `id`.
    pub fn with_key(mut self, id: u32, key: EncryptionKey) -> Self {
        self.keys.retain(|(k, _)| *k != id);
        self.keys.push((id, key));
        self
    }

    /// Removes key `id` (frames sealed with it can no longer be read).
    pub fn without_key(mut self, id: u32) -> Self {
        self.keys.retain(|(k, _)| *k != id);
        self
    }

    #[inline]
    pub fn get(&self, id: u32) -> Option<&EncryptionKey> {
        self.keys.iter().find(|(k, _)| *k == id).map(|(_, key)| key)
    }

    /// Ids of every key in the ring.
    pub fn ids(&self) -> Vec<u32> {
        self.keys.iter().map(|(k, _)| *k).collect()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

impl core::fmt::Debug for KeyRing {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("KeyRing").field("key_ids", &self.ids()).finish()
    }
}

impl core::fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("EncryptionKey(<redacted>)")
    }
}

impl Drop for EncryptionKey {
    fn drop(&mut self) {
        for b in self.0.iter_mut() {
            // SAFETY: `b` is a valid, aligned, exclusively borrowed byte of `self.0`; the volatile
            // write only prevents the compiler from eliding the wipe of a value being dropped.
            unsafe { core::ptr::write_volatile(b, 0) };
        }
        core::sync::atomic::compiler_fence(core::sync::atomic::Ordering::SeqCst);
    }
}

/// Encrypts `buf` in place (RFC 8439 AEAD) and returns its 16-byte tag over `aad` and the
/// ciphertext. `nonce` MUST be unique for every message encrypted under `key`.
#[inline]
pub fn seal_in_place(key: &[u8; 32], nonce: &[u8; 12], aad: &[u8], buf: &mut [u8]) -> [u8; 16] {
    chacha20_xor_in_place(key, nonce, 1, buf);
    aead_tag(key, nonce, aad, buf)
}

/// Verifies `tag` over `aad` and the ciphertext in `buf` (constant-time comparison), then
/// decrypts `buf` in place. On failure `buf` is left untouched (still ciphertext).
#[inline]
pub fn open_in_place(
    key: &[u8; 32],
    nonce: &[u8; 12],
    aad: &[u8],
    buf: &mut [u8],
    tag: &[u8; 16],
) -> Result<()> {
    let expected = aead_tag(key, nonce, aad, buf);
    if !verify_mac_tag_ct(tag, &expected) {
        return Err(StorageError::corrupted_header(
            "AEAD authentication tag verification failed (tampered payload or wrong key)",
        ));
    }
    chacha20_xor_in_place(key, nonce, 1, buf);
    Ok(())
}

/// RFC 8439 `ChaCha20-Poly1305` encryption returning `[ciphertext | 16B tag]`.
///
/// `nonce` MUST be unique for every message encrypted under `key`. Prefer [`seal`], which
/// generates and embeds a unique nonce automatically.
pub fn encrypt_payload_aead(
    key: &[u8; 32],
    nonce: &[u8; 12],
    aad: &[u8],
    plaintext: &[u8],
) -> Vec<u8> {
    let mut out = Vec::with_capacity(plaintext.len() + AEAD_TAG_LEN);
    out.extend_from_slice(plaintext);
    let tag = seal_in_place(key, nonce, aad, &mut out);
    out.extend_from_slice(&tag);
    out
}

/// Verifies the Poly1305 tag in constant time and decrypts `encrypted` (`[ciphertext | 16B tag]`).
pub fn decrypt_payload_aead(
    key: &[u8; 32],
    nonce: &[u8; 12],
    aad: &[u8],
    encrypted: &[u8],
) -> Result<Vec<u8>> {
    if encrypted.len() < AEAD_TAG_LEN {
        return Err(StorageError::corrupted_header(
            "encrypted payload shorter than 16-byte AEAD authentication tag",
        ));
    }
    let ct_len = encrypted.len() - AEAD_TAG_LEN;
    let mut provided_tag = [0u8; 16];
    provided_tag.copy_from_slice(&encrypted[ct_len..]);
    let mut plaintext = encrypted[..ct_len].to_vec();
    open_in_place(key, nonce, aad, &mut plaintext, &provided_tag)?;
    Ok(plaintext)
}

/// Returns a 96-bit nonce that is unique for the lifetime of this process and, with
/// overwhelming probability, across processes: `[64-bit per-process random seed | 32-bit counter]`.
///
/// The seed comes from the OS-seeded `std::collections::hash_map::RandomState`. A fresh seed is
/// drawn (and the counter restarted) whenever the 32-bit counter wraps, so no value repeats.
pub fn generate_unique_nonce() -> [u8; 12] {
    static SEED: OnceLock<AtomicU64> = OnceLock::new();
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let seed_cell = SEED.get_or_init(|| AtomicU64::new(fresh_random_u64()));
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let epoch = n >> 32;
    // Mix the epoch into the seed so each 2^32 counter window uses a distinct prefix.
    let seed = seed_cell.load(Ordering::Relaxed) ^ epoch.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut nonce = [0u8; 12];
    nonce[0..8].copy_from_slice(&seed.to_le_bytes());
    nonce[8..12].copy_from_slice(&(n as u32).to_le_bytes());
    nonce
}

fn fresh_random_u64() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    let mut h = RandomState::new().build_hasher();
    h.write_u64(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64),
    );
    h.write_u32(std::process::id());
    h.finish()
}

/// Encrypts `plaintext` with a freshly generated unique nonce, returning
/// `[12B nonce | ciphertext | 16B tag]`. `aad` is authenticated but not encrypted.
pub fn seal(key: &[u8; 32], aad: &[u8], plaintext: &[u8]) -> Vec<u8> {
    let nonce = generate_unique_nonce();
    let mut out = Vec::with_capacity(AEAD_NONCE_LEN + plaintext.len() + AEAD_TAG_LEN);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(plaintext);
    let tag = seal_in_place(key, &nonce, aad, &mut out[AEAD_NONCE_LEN..]);
    out.extend_from_slice(&tag);
    out
}

/// Opens a buffer produced by [`seal`].
pub fn open(key: &[u8; 32], aad: &[u8], sealed: &[u8]) -> Result<Vec<u8>> {
    if sealed.len() < AEAD_NONCE_LEN + AEAD_TAG_LEN {
        return Err(StorageError::corrupted_header(
            "sealed payload shorter than nonce + authentication tag",
        ));
    }
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&sealed[..AEAD_NONCE_LEN]);
    decrypt_payload_aead(key, &nonce, aad, &sealed[AEAD_NONCE_LEN..])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(s: &str) -> Vec<u8> {
        let clean: String = s.chars().filter(|c| c.is_ascii_hexdigit()).collect();
        (0..clean.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&clean[i..i + 2], 16).unwrap())
            .collect()
    }

    /// The previous radix-2^26 Poly1305 (validated against RFC 8439), kept as a differential
    /// reference for the radix-2^44 implementation.
    mod reference {
        #![allow(clippy::all)]
        /// RFC 8439 §2.5 Poly1305 one-time authenticator (radix 2^26, 5 limbs).
        struct RefPoly1305 {
            r: [u32; 5],
            h: [u32; 5],
            pad: [u32; 4],
        }

        impl RefPoly1305 {
            fn new(key: &[u8; 32]) -> Self {
                let le = |i: usize| u32::from_le_bytes([key[i], key[i + 1], key[i + 2], key[i + 3]]);
                // Clamp `r` while splitting it into 26-bit limbs.
                let r = [
                    le(0) & 0x03ff_ffff,
                    (le(3) >> 2) & 0x03ff_ff03,
                    (le(6) >> 4) & 0x03ff_c0ff,
                    (le(9) >> 6) & 0x03f0_3fff,
                    (le(12) >> 8) & 0x000f_ffff,
                ];
                Self {
                    r,
                    h: [0; 5],
                    pad: [le(16), le(20), le(24), le(28)],
                }
            }

            /// Absorbs one 16-byte block; `hibit` is `1 << 24` for full blocks (the appended `0x01` byte).
            fn block(&mut self, m: &[u8; 16], hibit: u32) {
                let le = |i: usize| u32::from_le_bytes([m[i], m[i + 1], m[i + 2], m[i + 3]]);
                let [r0, r1, r2, r3, r4] = self.r.map(u64::from);
                let (s1, s2, s3, s4) = (r1 * 5, r2 * 5, r3 * 5, r4 * 5);

                let h0 = u64::from(self.h[0] + (le(0) & 0x03ff_ffff));
                let h1 = u64::from(self.h[1] + ((le(3) >> 2) & 0x03ff_ffff));
                let h2 = u64::from(self.h[2] + ((le(6) >> 4) & 0x03ff_ffff));
                let h3 = u64::from(self.h[3] + ((le(9) >> 6) & 0x03ff_ffff));
                let h4 = u64::from(self.h[4] + ((le(12) >> 8) | hibit));

                let d0 = h0 * r0 + h1 * s4 + h2 * s3 + h3 * s2 + h4 * s1;
                let mut d1 = h0 * r1 + h1 * r0 + h2 * s4 + h3 * s3 + h4 * s2;
                let mut d2 = h0 * r2 + h1 * r1 + h2 * r0 + h3 * s4 + h4 * s3;
                let mut d3 = h0 * r3 + h1 * r2 + h2 * r1 + h3 * r0 + h4 * s4;
                let mut d4 = h0 * r4 + h1 * r3 + h2 * r2 + h3 * r1 + h4 * r0;

                let mut c = d0 >> 26;
                let mut n0 = d0 & 0x03ff_ffff;
                d1 += c;
                c = d1 >> 26;
                let n1 = d1 & 0x03ff_ffff;
                d2 += c;
                c = d2 >> 26;
                let n2 = d2 & 0x03ff_ffff;
                d3 += c;
                c = d3 >> 26;
                let n3 = d3 & 0x03ff_ffff;
                d4 += c;
                c = d4 >> 26;
                let n4 = d4 & 0x03ff_ffff;
                n0 += c * 5;
                let c2 = n0 >> 26;
                n0 &= 0x03ff_ffff;

                self.h = [
                    n0 as u32,
                    (n1 + c2) as u32,
                    n2 as u32,
                    n3 as u32,
                    n4 as u32,
                ];
            }

            fn finish(self) -> [u8; 16] {
                let [mut h0, mut h1, mut h2, mut h3, mut h4] = self.h;

                // Full carry of h.
                let mut c = h1 >> 26;
                h1 &= 0x03ff_ffff;
                h2 += c;
                c = h2 >> 26;
                h2 &= 0x03ff_ffff;
                h3 += c;
                c = h3 >> 26;
                h3 &= 0x03ff_ffff;
                h4 += c;
                c = h4 >> 26;
                h4 &= 0x03ff_ffff;
                h0 += c * 5;
                c = h0 >> 26;
                h0 &= 0x03ff_ffff;
                h1 += c;

                // Compute g = h + -p = h - (2^130 - 5) and select h or g in constant time.
                let mut g0 = h0.wrapping_add(5);
                c = g0 >> 26;
                g0 &= 0x03ff_ffff;
                let mut g1 = h1.wrapping_add(c);
                c = g1 >> 26;
                g1 &= 0x03ff_ffff;
                let mut g2 = h2.wrapping_add(c);
                c = g2 >> 26;
                g2 &= 0x03ff_ffff;
                let mut g3 = h3.wrapping_add(c);
                c = g3 >> 26;
                g3 &= 0x03ff_ffff;
                let g4 = h4.wrapping_add(c).wrapping_sub(1 << 26);

                let mask = (g4 >> 31).wrapping_sub(1); // all ones if g4 did not underflow
                let nmask = !mask;
                h0 = (h0 & nmask) | (g0 & mask);
                h1 = (h1 & nmask) | (g1 & mask);
                h2 = (h2 & nmask) | (g2 & mask);
                h3 = (h3 & nmask) | (g3 & mask);
                h4 = (h4 & nmask) | (g4 & mask);

                // Pack into 4 x 32-bit words.
                let w0 = h0 | (h1 << 26);
                let w1 = (h1 >> 6) | (h2 << 20);
                let w2 = (h2 >> 12) | (h3 << 14);
                let w3 = (h3 >> 18) | (h4 << 8);

                // tag = (h + s) mod 2^128
                let mut f = u64::from(w0) + u64::from(self.pad[0]);
                let t0 = f as u32;
                f = u64::from(w1) + u64::from(self.pad[1]) + (f >> 32);
                let t1 = f as u32;
                f = u64::from(w2) + u64::from(self.pad[2]) + (f >> 32);
                let t2 = f as u32;
                f = u64::from(w3) + u64::from(self.pad[3]) + (f >> 32);
                let t3 = f as u32;

                let mut tag = [0u8; 16];
                tag[0..4].copy_from_slice(&t0.to_le_bytes());
                tag[4..8].copy_from_slice(&t1.to_le_bytes());
                tag[8..12].copy_from_slice(&t2.to_le_bytes());
                tag[12..16].copy_from_slice(&t3.to_le_bytes());
                tag
            }
        }

        fn reference_tag(key: &[u8; 32], msg: &[u8]) -> [u8; 16] {
            let mut p = RefPoly1305::new(key);
            let mut chunks = msg.chunks_exact(16);
            for chunk in &mut chunks {
                let mut block = [0u8; 16];
                block.copy_from_slice(chunk);
                p.block(&block, 1 << 24);
            }
            let rem = chunks.remainder();
            if !rem.is_empty() {
                let mut block = [0u8; 16];
                block[..rem.len()].copy_from_slice(rem);
                block[rem.len()] = 1;
                p.block(&block, 0);
            }
            p.finish()
        }
        pub(super) fn tag(key: &[u8; 32], msg: &[u8]) -> [u8; 16] {
            reference_tag(key, msg)
        }
    }

    #[test]
    fn test_poly1305_radix44_matches_radix26_reference() {
        let mut rng = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        for case in 0..400 {
            let len = case % 300;
            let fill = match case % 4 {
                0 => 0xffu8,
                1 => 0x00,
                _ => 0x5a,
            };
            let mut key = [fill; 32];
            let mut msg = vec![fill; len];
            if case % 4 >= 2 {
                key.iter_mut().for_each(|b| *b = next() as u8);
                msg.iter_mut().for_each(|b| *b = next() as u8);
            }
            assert_eq!(poly1305_tag(&key, &msg), reference::tag(&key, &msg), "case {case}");
        }
    }

    #[test]
    fn test_chacha20_block_rfc8439_vector() {
        // RFC 8439 §2.3.2
        let key: [u8; 32] = core::array::from_fn(|i| i as u8);
        let nonce: [u8; 12] = hex("000000090000004a00000000").try_into().unwrap();
        let block = chacha20_block(&key, 1, &nonce);
        assert_eq!(&block[..16], hex("10f1e7e4d13b5915500fdd1fa32071c4").as_slice());
    }

    #[test]
    fn test_poly1305_rfc8439_vector() {
        // RFC 8439 §2.5.2
        let key: [u8; 32] = hex(
            "85d6be7857556d337f4452fe42d506a80103808afb0db2fd4abff6af4149f51b",
        )
        .try_into()
        .unwrap();
        let tag = poly1305_tag(&key, b"Cryptographic Forum Research Group");
        assert_eq!(tag.as_slice(), hex("a8061dc1305136c6c22b8baf0c0127a9").as_slice());
    }

    #[test]
    fn test_aead_rfc8439_vector() {
        // RFC 8439 §2.8.2
        let key: [u8; 32] = core::array::from_fn(|i| 0x80 + i as u8);
        let nonce: [u8; 12] = hex("070000004041424344454647").try_into().unwrap();
        let aad = hex("50515253c0c1c2c3c4c5c6c7");
        let pt = b"Ladies and Gentlemen of the class of '99: If I could offer you only one tip for the future, sunscreen would be it.";
        let ct = encrypt_payload_aead(&key, &nonce, &aad, pt);
        assert_eq!(&ct[..16], hex("d31a8d34648e60db7b86afbc53ef7ec2").as_slice());
        assert_eq!(&ct[ct.len() - 16..], hex("1ae10b594f09e26a7e902ecbd0600691").as_slice());
        assert_eq!(decrypt_payload_aead(&key, &nonce, &aad, &ct).unwrap(), pt);
    }

    #[test]
    fn test_chacha20_xor_matches_block_reference_at_all_lengths() {
        let key: [u8; 32] = core::array::from_fn(|i| (i * 7 + 3) as u8);
        let nonce: [u8; 12] = core::array::from_fn(|i| (i * 13 + 1) as u8);
        for counter in [0u32, 1, u32::MAX - 2] {
            for len in (0..=600).chain([4096, 4097, 65_535]) {
                let data: Vec<u8> = (0..len).map(|i| (i * 31 + 5) as u8).collect();
                let mut fast = data.clone();
                chacha20_xor_in_place(&key, &nonce, counter, &mut fast);
                let mut reference = data.clone();
                for (n, chunk) in reference.chunks_mut(64).enumerate() {
                    let ks = chacha20_block(&key, counter.wrapping_add(n as u32), &nonce);
                    for (b, k) in chunk.iter_mut().zip(ks) {
                        *b ^= k;
                    }
                }
                assert_eq!(fast, reference, "len {len} counter {counter}");
            }
        }
    }

    #[test]
    fn test_in_place_open_rejects_tampering_without_touching_the_buffer() {
        let key = [9u8; 32];
        let nonce = [4u8; 12];
        let msg: Vec<u8> = (0..1000u32).map(|i| i as u8).collect();
        let mut buf = msg.clone();
        let tag = seal_in_place(&key, &nonce, b"aad", &mut buf);
        let ciphertext = buf.clone();
        assert!(open_in_place(&key, &nonce, b"aaD", &mut buf, &tag).is_err());
        assert_eq!(buf, ciphertext, "failed open must not release plaintext");
        open_in_place(&key, &nonce, b"aad", &mut buf, &tag).unwrap();
        assert_eq!(buf, msg);
    }

    #[test]
    fn test_region_sealer_matches_one_shot_aead_and_random_access() {
        let key = EncryptionKey::from_bytes([0x33; 32]);
        let nonce = [7u8; 12];
        let msg: Vec<u8> = (0..10_000u32).map(|i| (i * 17) as u8).collect();
        let one_shot = encrypt_payload_aead(&key.0, &nonce, b"ctx", &msg);

        let mut streamed = msg.clone();
        let mut sealer = RegionSealer::new(&key, nonce, b"ctx");
        let (a, rest) = streamed.split_at_mut(4096);
        let (b, c) = rest.split_at_mut(512);
        sealer.seal_piece(a).unwrap();
        sealer.seal_piece(b).unwrap();
        sealer.seal_piece(c).unwrap();
        assert!(sealer.seal_piece(&mut [0u8; 64]).is_err(), "piece after a partial piece");
        assert_eq!(&one_shot[..msg.len()], streamed.as_slice());
        assert_eq!(sealer.finish().as_slice(), &one_shot[msg.len()..]);

        let mut verifier = RegionSealer::new(&key, nonce, b"ctx");
        verifier.absorb_ciphertext(&streamed[..4096]).unwrap();
        verifier.absorb_ciphertext(&streamed[4096..]).unwrap();
        let mut expected = [0u8; 16];
        expected.copy_from_slice(&one_shot[msg.len()..]);
        verifier.verify(&expected).unwrap();

        // Random access at unaligned offsets decrypts exactly that range.
        for (at, len) in [(0usize, 8usize), (63, 2), (100, 1000), (4093, 7), (9990, 10)] {
            let mut piece = streamed[at..at + len].to_vec();
            chacha20_xor_at(&key, &nonce, at as u64, &mut piece).unwrap();
            assert_eq!(piece, &msg[at..at + len], "at {at}");
        }
    }

    #[test]
    fn test_seal_open_and_tamper_detection() {
        let key = [0x42u8; 32];
        let msg = b"Top-secret zero-copy agent memory payload";
        let mut sealed = seal(&key, b"rec:1", msg);
        assert_eq!(open(&key, b"rec:1", &sealed).unwrap(), msg);
        assert!(open(&key, b"rec:2", &sealed).is_err(), "wrong AAD must fail");
        sealed[AEAD_NONCE_LEN + 3] ^= 0x01;
        assert!(open(&key, b"rec:1", &sealed).is_err(), "tampered ciphertext must fail");

        let a = generate_unique_nonce();
        let b = generate_unique_nonce();
        assert_ne!(a, b);
    }
}
