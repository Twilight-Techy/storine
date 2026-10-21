//! Zero-Dependency Authenticated At-Rest Encryption (`TASK-044`, `F-SEC-02`).
//!
//! Implements the standard RFC 8439 `ChaCha20-Poly1305` AEAD construction (ChaCha20 20-round
//! stream cipher with a 256-bit key and 96-bit nonce, authenticated by a Poly1305 one-time MAC
//! whose key is derived from keystream block 0). Tags are compared in constant time.
//!
//! Nonces must never repeat under the same key. [`seal`] generates a fresh unique nonce per call
//! and stores it in front of the ciphertext, so callers never have to manage nonces themselves.
//!
//! Note: this module is a building block; the engine does not yet encrypt records automatically
//! (`RecordFlags::ENCRYPTED` frames, see `RecordCodec::encrypt_frame`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use storage_types::{Result, StorageError};

/// Size of the appended authentication tag in bytes (`128 bits`).
pub const AEAD_TAG_LEN: usize = 16;
/// Size of the ChaCha20-Poly1305 nonce in bytes (`96 bits`).
pub const AEAD_NONCE_LEN: usize = 12;

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

/// Generates a single 64-byte RFC 8439 ChaCha20 keystream block.
#[inline]
pub fn chacha20_block(key: &[u8; 32], counter: u32, nonce: &[u8; 12]) -> [u8; 64] {
    let mut state = [0u32; 16];
    // "expand 32-byte k"
    state[0] = 0x6170_7865;
    state[1] = 0x3320_646e;
    state[2] = 0x7962_2d32;
    state[3] = 0x6b20_6574;

    for i in 0..8 {
        let off = i * 4;
        state[4 + i] = u32::from_le_bytes([key[off], key[off + 1], key[off + 2], key[off + 3]]);
    }
    state[12] = counter;
    state[13] = u32::from_le_bytes([nonce[0], nonce[1], nonce[2], nonce[3]]);
    state[14] = u32::from_le_bytes([nonce[4], nonce[5], nonce[6], nonce[7]]);
    state[15] = u32::from_le_bytes([nonce[8], nonce[9], nonce[10], nonce[11]]);

    let mut working = state;
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

    let mut out = [0u8; 64];
    for i in 0..16 {
        let word = working[i].wrapping_add(state[i]).to_le_bytes();
        let off = i * 4;
        out[off..off + 4].copy_from_slice(&word);
    }
    out
}

/// Encrypts or decrypts `data` in-place using ChaCha20 starting at `initial_counter`.
pub fn chacha20_xor_in_place(
    key: &[u8; 32],
    nonce: &[u8; 12],
    initial_counter: u32,
    data: &mut [u8],
) {
    let mut counter = initial_counter;
    for chunk in data.chunks_mut(64) {
        let block = chacha20_block(key, counter, nonce);
        counter = counter.wrapping_add(1);
        for (b, k) in chunk.iter_mut().zip(block.iter()) {
            *b ^= *k;
        }
    }
}

/// RFC 8439 §2.5 Poly1305 one-time authenticator (radix 2^26, 5 limbs).
struct Poly1305 {
    r: [u32; 5],
    h: [u32; 5],
    pad: [u32; 4],
}

impl Poly1305 {
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

    /// Absorbs `data`, zero-padding the final partial block to 16 bytes (AEAD `pad16` semantics).
    fn update_padded(&mut self, data: &[u8]) {
        for chunk in data.chunks(16) {
            let mut block = [0u8; 16];
            block[..chunk.len()].copy_from_slice(chunk);
            self.block(&block, 1 << 24);
        }
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

/// Computes a raw Poly1305 tag of `msg` under the 32-byte one-time `key` (RFC 8439 §2.5).
#[allow(clippy::chunks_exact_to_as_chunks)]
pub fn poly1305_tag(key: &[u8; 32], msg: &[u8]) -> [u8; 16] {
    let mut p = Poly1305::new(key);
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

/// RFC 8439 §2.8 AEAD tag over `aad` and `ciphertext`.
fn aead_tag(key: &[u8; 32], nonce: &[u8; 12], aad: &[u8], ciphertext: &[u8]) -> [u8; 16] {
    let block0 = chacha20_block(key, 0, nonce);
    let mut otk = [0u8; 32];
    otk.copy_from_slice(&block0[..32]);
    let mut p = Poly1305::new(&otk);
    p.update_padded(aad);
    p.update_padded(ciphertext);
    let mut lens = [0u8; 16];
    lens[..8].copy_from_slice(&(aad.len() as u64).to_le_bytes());
    lens[8..].copy_from_slice(&(ciphertext.len() as u64).to_le_bytes());
    p.block(&lens, 1 << 24);
    p.finish()
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
    chacha20_xor_in_place(key, nonce, 1, &mut out);
    let tag = aead_tag(key, nonce, aad, &out);
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
    let ciphertext = &encrypted[..ct_len];
    let mut provided_tag = [0u8; 16];
    provided_tag.copy_from_slice(&encrypted[ct_len..]);
    let expected_tag = aead_tag(key, nonce, aad, ciphertext);
    if !verify_mac_tag_ct(&provided_tag, &expected_tag) {
        return Err(StorageError::corrupted_header(
            "AEAD authentication tag verification failed (tampered payload or wrong key)",
        ));
    }
    let mut plaintext = ciphertext.to_vec();
    chacha20_xor_in_place(key, nonce, 1, &mut plaintext);
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
    out.extend_from_slice(&encrypt_payload_aead(key, &nonce, aad, plaintext));
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
