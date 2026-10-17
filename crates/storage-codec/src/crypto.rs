//! Zero-Dependency Authenticated At-Rest Encryption (`TASK-044`, `F-SEC-02`).
//!
//! Implements RFC 8439 ChaCha20 20-round ARX stream cipher (`256-bit` key + `96-bit` nonce)
//! paired with a 128-bit block-0 subkey MAC tag and constant-time tag verification.
//! Used when `RecordFlags::ENCRYPTED` (`1 << 5`) is enabled for sensitive payloads.

use storage_types::{Result, StorageError};

/// Size of the appended authentication tag in bytes (`128 bits`).
pub const AEAD_TAG_LEN: usize = 16;

/// Derives a deterministic 96-bit (`12-byte`) per-record nonce from `(record_version, updated_at_ns)`.
#[inline]
pub fn derive_record_nonce(record_version: u64, updated_at_ns: u64) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[0..8].copy_from_slice(&updated_at_ns.to_le_bytes());
    let ver_mix = (record_version ^ (record_version >> 32)) as u32;
    nonce[8..12].copy_from_slice(&ver_mix.to_le_bytes());
    nonce
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

/// Computes a 128-bit authentication tag using the one-time subkey from `chacha20_block(key, 0, nonce)`.
pub fn compute_mac_tag(key: &[u8; 32], nonce: &[u8; 12], ciphertext: &[u8]) -> [u8; 16] {
    let otk = chacha20_block(key, 0, nonce);
    let mut v0 = u64::from_le_bytes([
        otk[0], otk[1], otk[2], otk[3], otk[4], otk[5], otk[6], otk[7],
    ]);
    let mut v1 = u64::from_le_bytes([
        otk[8], otk[9], otk[10], otk[11], otk[12], otk[13], otk[14], otk[15],
    ]);
    let mut v2 = u64::from_le_bytes([
        otk[16], otk[17], otk[18], otk[19], otk[20], otk[21], otk[22], otk[23],
    ]);
    let mut v3 = u64::from_le_bytes([
        otk[24], otk[25], otk[26], otk[27], otk[28], otk[29], otk[30], otk[31],
    ]);

    for chunk in ciphertext.chunks(8) {
        let mut buf = [0u8; 8];
        buf[..chunk.len()].copy_from_slice(chunk);
        let m = u64::from_le_bytes(buf);
        v3 ^= m;
        for _ in 0..2 {
            v0 = v0.wrapping_add(v1);
            v1 = v1.rotate_left(13) ^ v0;
            v0 = v0.rotate_left(32);
            v2 = v2.wrapping_add(v3);
            v3 = v3.rotate_left(16) ^ v2;
            v0 = v0.wrapping_add(v3);
            v3 = v3.rotate_left(21) ^ v0;
            v2 = v2.wrapping_add(v1);
            v1 = v1.rotate_left(17) ^ v2;
            v2 = v2.rotate_left(32);
        }
        v0 ^= m;
    }

    let len_word = (ciphertext.len() as u64).rotate_left(56);
    v3 ^= len_word;
    v2 ^= 0xee;
    for _ in 0..4 {
        v0 = v0.wrapping_add(v1);
        v1 = v1.rotate_left(13) ^ v0;
        v0 = v0.rotate_left(32);
        v2 = v2.wrapping_add(v3);
        v3 = v3.rotate_left(16) ^ v2;
        v0 = v0.wrapping_add(v3);
        v3 = v3.rotate_left(21) ^ v0;
        v2 = v2.wrapping_add(v1);
        v1 = v1.rotate_left(17) ^ v2;
        v2 = v2.rotate_left(32);
    }

    let tag_lo = v0 ^ v1 ^ u64::from_le_bytes([
        otk[32], otk[33], otk[34], otk[35], otk[36], otk[37], otk[38], otk[39],
    ]);
    let tag_hi = v2 ^ v3 ^ u64::from_le_bytes([
        otk[40], otk[41], otk[42], otk[43], otk[44], otk[45], otk[46], otk[47],
    ]);
    let mut tag = [0u8; 16];
    tag[0..8].copy_from_slice(&tag_lo.to_le_bytes());
    tag[8..16].copy_from_slice(&tag_hi.to_le_bytes());
    tag
}

/// Constant-time 16-byte MAC comparison to prevent timing side-channels.
#[inline]
pub fn verify_mac_tag_ct(a: &[u8; 16], b: &[u8; 16]) -> bool {
    let mut diff = 0u8;
    for i in 0..16 {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

/// Encrypts `plaintext` with `key` and `nonce`, returning `[ciphertext | 16B MAC tag]`.
pub fn encrypt_payload_aead(key: &[u8; 32], nonce: &[u8; 12], plaintext: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(plaintext.len() + AEAD_TAG_LEN);
    out.extend_from_slice(plaintext);
    chacha20_xor_in_place(key, nonce, 1, &mut out);
    let tag = compute_mac_tag(key, nonce, &out);
    out.extend_from_slice(&tag);
    out
}

/// Verifies the 128-bit MAC tag in constant time and decrypts `encrypted` (`[ciphertext | 16B MAC tag]`).
pub fn decrypt_payload_aead(
    key: &[u8; 32],
    nonce: &[u8; 12],
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
    let expected_tag = compute_mac_tag(key, nonce, ciphertext);
    if !verify_mac_tag_ct(&provided_tag, &expected_tag) {
        return Err(StorageError::corrupted_header(
            "AEAD authentication tag verification failed (tampered payload or wrong key)",
        ));
    }
    let mut plaintext = ciphertext.to_vec();
    chacha20_xor_in_place(key, nonce, 1, &mut plaintext);
    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aead_encrypt_decrypt_and_tamper_detection() {
        let key = [0x42u8; 32];
        let nonce = derive_record_nonce(7, 1_700_000_000_123_456_789);
        let msg = b"Top-secret zero-copy agent memory payload with 256-bit ChaCha20 encryption";

        let mut ct = encrypt_payload_aead(&key, &nonce, msg);
        assert_ne!(&ct[..msg.len()], msg);

        let pt = decrypt_payload_aead(&key, &nonce, &ct).unwrap();
        assert_eq!(pt.as_slice(), msg);

        // Tamper with 1 bit of ciphertext -> must fail MAC verification!
        ct[10] ^= 0x01;
        assert!(decrypt_payload_aead(&key, &nonce, &ct).is_err());
    }
}
