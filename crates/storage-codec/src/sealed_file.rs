//! Whole-file / whole-frame sealing of index files at rest (`TASK-088`, `F-SEC-02`).
//!
//! Sealed layout: `magic "STGESEL1" (8B) | key_id u32 | 12 B nonce | ChaCha20 ciphertext | 16 B tag`.
//! The authenticated data is `magic | key_id | position u64 | context`, where `context` names the
//! file (so a sealed index cannot be moved to another collection or file kind) and `position` is
//! the byte offset of the sealed unit in its file (so appended frames cannot be reordered or
//! duplicated). Sealing and opening work in place on the caller's buffer: no extra copies.

use core::ops::Range;

use storage_types::{Result, StorageError};

use crate::crypto::{
    generate_unique_nonce, open_in_place, seal_in_place, KeyRing, AEAD_NONCE_LEN, AEAD_TAG_LEN,
};

/// Magic of a sealed unit. Never the prefix of a plaintext index file or frame.
pub const SEALED_FILE_MAGIC: &[u8; 8] = b"STGESEL1";
/// Bytes in front of the ciphertext (`magic | key_id | nonce`).
pub const SEALED_FILE_HEADER_LEN: usize = 8 + 4 + AEAD_NONCE_LEN;

/// How index files are protected: borrowed from the engine's encryption settings.
#[derive(Clone, Copy, Debug)]
pub struct FileSeal<'a> {
    /// Keys that may open sealed files (`None`: no encryption configured).
    pub keys: Option<&'a KeyRing>,
    /// Key that seals newly written files (`None`: files are written in plaintext).
    pub active_key: Option<u32>,
    /// Accept plaintext files on read. `false` with keys configured enforces no-downgrade.
    pub accept_plain: bool,
}

impl FileSeal<'static> {
    /// No encryption: files are written and read in plaintext.
    pub const PLAIN: Self = Self { keys: None, active_key: None, accept_plain: true };
}

impl FileSeal<'_> {
    /// Bytes a writer must reserve at the start of its buffer before [`Self::seal_in_place`]
    /// (`0` when files are written in plaintext).
    #[inline]
    pub fn header_len(&self) -> usize {
        if self.sealing() {
            SEALED_FILE_HEADER_LEN
        } else {
            0
        }
    }

    /// `true` if newly written files are sealed.
    #[inline]
    pub fn sealing(&self) -> bool {
        matches!((self.keys, self.active_key), (Some(ring), Some(id)) if ring.get(id).is_some())
    }

    /// Seals `buf` in place. `buf[..self.header_len()]` must be the reserved prefix; the
    /// plaintext follows it. Appends the tag. A no-op when not sealing.
    pub fn seal_in_place(&self, context: &[u8], position: u64, buf: &mut Vec<u8>) -> Result<()> {
        let (Some(ring), Some(key_id)) = (self.keys, self.active_key) else {
            return Ok(());
        };
        let Some(key) = ring.get(key_id) else {
            return Ok(());
        };
        if buf.len() < SEALED_FILE_HEADER_LEN {
            return Err(StorageError::invalid_archive("sealed file buffer missing its header"));
        }
        buf.try_reserve_exact(AEAD_TAG_LEN)
            .map_err(|_| StorageError::resource_exhausted("heap memory (sealed index file)"))?;
        let nonce = generate_unique_nonce();
        buf[..8].copy_from_slice(SEALED_FILE_MAGIC);
        buf[8..12].copy_from_slice(&key_id.to_le_bytes());
        buf[12..SEALED_FILE_HEADER_LEN].copy_from_slice(&nonce);
        let aad = sealed_file_aad(key_id, position, context);
        let tag = seal_in_place(key.bytes(), &nonce, &aad, &mut buf[SEALED_FILE_HEADER_LEN..]);
        buf.extend_from_slice(&tag);
        Ok(())
    }

    /// Verifies and decrypts `bytes` in place if it is sealed and returns the plaintext range.
    /// A plaintext input is returned whole when plaintext is acceptable, and rejected otherwise
    /// (no downgrade). Fails on an unknown key id, wrong `context` / `position`, or any modified byte.
    pub fn open_in_place(&self, context: &[u8], position: u64, bytes: &mut [u8]) -> Result<Range<usize>> {
        if !is_sealed(bytes) {
            if self.accept_plain || self.keys.is_none() {
                return Ok(0..bytes.len());
            }
            return Err(StorageError::invalid_key(
                "plaintext index file rejected: encryption is configured (no downgrade)",
            ));
        }
        let ring = self.keys.ok_or_else(|| {
            StorageError::invalid_key("index file is encrypted but no encryption key is configured")
        })?;
        if bytes.len() < SEALED_FILE_HEADER_LEN + AEAD_TAG_LEN {
            return Err(StorageError::corrupted_header("sealed index file truncated"));
        }
        let key_id = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        let key = ring
            .get(key_id)
            .ok_or_else(|| StorageError::invalid_key("index file sealed with a key id not in the key ring"))?;
        let mut nonce = [0u8; AEAD_NONCE_LEN];
        nonce.copy_from_slice(&bytes[12..SEALED_FILE_HEADER_LEN]);
        let tag_at = bytes.len() - AEAD_TAG_LEN;
        let mut tag = [0u8; AEAD_TAG_LEN];
        tag.copy_from_slice(&bytes[tag_at..]);
        let aad = sealed_file_aad(key_id, position, context);
        open_in_place(key.bytes(), &nonce, &aad, &mut bytes[SEALED_FILE_HEADER_LEN..tag_at], &tag)
            .map_err(|_| {
                StorageError::corrupted_header("index file authentication failed (tampered file or wrong key)")
            })?;
        Ok(SEALED_FILE_HEADER_LEN..tag_at)
    }
}

/// `true` if `bytes` starts with [`SEALED_FILE_MAGIC`].
#[inline]
pub fn is_sealed(bytes: &[u8]) -> bool {
    bytes.starts_with(SEALED_FILE_MAGIC)
}

fn sealed_file_aad(key_id: u32, position: u64, context: &[u8]) -> Vec<u8> {
    let mut aad = Vec::with_capacity(20 + context.len());
    aad.extend_from_slice(SEALED_FILE_MAGIC);
    aad.extend_from_slice(&key_id.to_le_bytes());
    aad.extend_from_slice(&position.to_le_bytes());
    aad.extend_from_slice(context);
    aad
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::EncryptionKey;

    fn sealed(seal: &FileSeal<'_>, ctx: &[u8], pos: u64, body: &[u8]) -> Vec<u8> {
        let mut buf = vec![0u8; seal.header_len()];
        buf.extend_from_slice(body);
        seal.seal_in_place(ctx, pos, &mut buf).unwrap();
        buf
    }

    #[test]
    fn test_sealed_file_roundtrip_binding_and_no_downgrade() {
        let ring = KeyRing::new().with_key(1, EncryptionKey::from_bytes([5; 32]));
        let seal = FileSeal { keys: Some(&ring), active_key: Some(1), accept_plain: false };
        let body = b"STGEIDX3 secret index body".repeat(20);
        let file = sealed(&seal, b"a_primary.idx", 0, &body);
        assert!(is_sealed(&file));
        assert!(!file.windows(9).any(|w| w == b"secret in"), "plaintext leaked");

        let mut ok = file.clone();
        let range = seal.open_in_place(b"a_primary.idx", 0, &mut ok).unwrap();
        assert_eq!(&ok[range], body.as_slice());

        // Wrong context (moved file), wrong position (reordered frame), flipped byte, missing key.
        assert!(seal.open_in_place(b"b_primary.idx", 0, &mut file.clone()).is_err());
        assert!(seal.open_in_place(b"a_primary.idx", 64, &mut file.clone()).is_err());
        for i in 0..file.len() {
            let mut bad = file.clone();
            bad[i] ^= 0x10;
            assert!(seal.open_in_place(b"a_primary.idx", 0, &mut bad).is_err(), "byte {i}");
        }
        assert!(FileSeal::PLAIN.open_in_place(b"a_primary.idx", 0, &mut file.clone()).is_err());

        // No downgrade: plaintext rejected with keys configured unless explicitly accepted.
        let mut plain = body.clone();
        assert!(seal.open_in_place(b"a_primary.idx", 0, &mut plain).is_err());
        let migrating = FileSeal { accept_plain: true, ..seal };
        assert_eq!(migrating.open_in_place(b"a_primary.idx", 0, &mut plain).unwrap(), 0..plain.len());

        // Plain mode writes plaintext untouched; truncations never panic.
        assert_eq!(sealed(&FileSeal::PLAIN, b"x", 0, &body), body);
        for n in 0..file.len() {
            let _ = seal.open_in_place(b"a_primary.idx", 0, &mut file[..n].to_vec());
        }
    }
}
