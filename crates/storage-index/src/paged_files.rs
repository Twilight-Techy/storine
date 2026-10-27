//! Generation-file plumbing shared by the paged indexes (`TASK-083`).
//!
//! A paged index named `<path>` consists of:
//! - `<path>`: a 32-byte manifest `magic | version | generation | stamp | checksum`, replaced
//!   atomically (`fsync` + rename + directory `fsync`);
//! - `<path>.<generation as 16 hex digits>`: the immutable data file of that generation.
//!
//! Data files are never overwritten (a mapped file cannot be replaced on Windows): a checkpoint
//! writes `<path>.<g + 1>`, publishes it through the manifest, then deletes `<path>.<g>`.

use std::fs::{self, File};
use std::path::{Path, PathBuf};

use storage_codec::checksum_payload;
use storage_mmap::{atomic_write_replace, MmapRegion};
use storage_types::wire::{read_u32_le, read_u64_le};
use storage_types::{Result, StorageError};

const MANIFEST_LEN: usize = 32;
const MANIFEST_VERSION: u32 = 1;

/// Path of generation `generation`'s data file for the index whose manifest is `manifest`.
pub(crate) fn generation_path(manifest: &Path, generation: u64) -> PathBuf {
    let mut os = manifest.as_os_str().to_os_string();
    os.push(format!(".{generation:016x}"));
    PathBuf::from(os)
}

/// Temporary path a generation is written to before being published.
pub(crate) fn temp_generation_path(manifest: &Path, generation: u64) -> PathBuf {
    let mut os = generation_path(manifest, generation).into_os_string();
    os.push(".tmp");
    PathBuf::from(os)
}

/// Deletes generation files (and interrupted `.tmp` writes) of `manifest` other than `keep`.
pub(crate) fn remove_stale_generations(manifest: &Path, keep: Option<&Path>) {
    let (Some(dir), Some(name)) = (manifest.parent(), manifest.file_name()) else {
        return;
    };
    let dir = if dir.as_os_str().is_empty() { Path::new(".") } else { dir };
    let prefix = format!("{}.", name.to_string_lossy());
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let file_name = entry.file_name().to_string_lossy().into_owned();
        let Some(suffix) = file_name.strip_prefix(&prefix) else {
            continue;
        };
        let hex = suffix.strip_suffix(".tmp").unwrap_or(suffix);
        let is_generation = hex.len() == 16 && hex.bytes().all(|b| b.is_ascii_hexdigit());
        let path = entry.path();
        if is_generation && Some(path.as_path()) != keep {
            let _ = fs::remove_file(&path);
        }
    }
}

/// Atomically publishes `generation` / `stamp` in the manifest.
pub(crate) fn write_manifest(manifest: &Path, magic: u32, generation: u64, stamp: u64) -> Result<()> {
    let mut m = [0u8; MANIFEST_LEN];
    m[0..4].copy_from_slice(&magic.to_le_bytes());
    m[4..8].copy_from_slice(&MANIFEST_VERSION.to_le_bytes());
    m[8..16].copy_from_slice(&generation.to_le_bytes());
    m[16..24].copy_from_slice(&stamp.to_le_bytes());
    let crc = checksum_payload(&m[0..24]);
    m[24..32].copy_from_slice(&crc.to_le_bytes());
    atomic_write_replace(manifest, &m, true)
}

/// Reads `(generation, stamp)` from the manifest (`None` if it does not exist).
pub(crate) fn read_manifest(manifest: &Path, magic: u32) -> Result<Option<(u64, u64)>> {
    if !manifest.exists() {
        return Ok(None);
    }
    let m = fs::read(manifest).map_err(|e| StorageError::io("read index manifest", e))?;
    if m.len() != MANIFEST_LEN || read_u32_le(&m, 0) != magic || read_u32_le(&m, 4) != MANIFEST_VERSION
    {
        return Err(StorageError::corrupted_header("index manifest invalid or unsupported"));
    }
    let expected = read_u64_le(&m, 24);
    let actual = checksum_payload(&m[0..24]);
    if expected != actual {
        return Err(StorageError::checksum_mismatch(expected, actual));
    }
    Ok(Some((read_u64_le(&m, 8), read_u64_le(&m, 16))))
}

/// Publishes a finished temporary generation file under its final name.
pub(crate) fn publish_generation(tmp: &Path, final_path: &Path) -> Result<()> {
    fs::rename(tmp, final_path).map_err(|e| StorageError::io("publish index generation file", e))
}

pub(crate) fn map_path_read_only(path: &Path) -> Result<MmapRegion> {
    let file = File::open(path).map_err(|e| StorageError::io("open index file for mmap", e))?;
    let len = file
        .metadata()
        .map_err(|e| StorageError::io("stat index file for mmap", e))?
        .len() as usize;
    MmapRegion::map_read_only(&file, len)
}

// ---------------------------------------------------------------------------------------------
// Encryption of generation files (`TASK-088`, `F-SEC-02`)
// ---------------------------------------------------------------------------------------------
//
// Everything after the 4 KiB header page (pages, blocks, directory) is one ChaCha20-Poly1305
// region sealed with the active key while it is streamed to disk. The header page carries an
// encryption section `magic | key_id | nonce | tag` at `ENC_SECTION_OFFSET`; the tag covers the
// whole region and the generation file's name. Open verifies the tag once (the region is read
// in full there anyway for validation); afterwards a page or block is decrypted on access by
// random-access keystream (`chacha20_xor_at`), with no MAC work on the read path.

use storage_codec::{chacha20_xor_at, generate_unique_nonce, EncryptionKey, FileSeal, KeyRing, RegionSealer};

/// Offset of the encryption section in a generation file's header page.
pub(crate) const ENC_SECTION_OFFSET: usize = 128;
/// `magic u32 | key_id u32 | nonce [12] | tag [16]`.
pub(crate) const ENC_SECTION_LEN: usize = 4 + 4 + 12 + 16;
const ENC_MAGIC: u32 = 0x434E_4550; // "PENC"

/// Owned copy of the index-file protection settings (the engine can change them at runtime).
#[derive(Clone, Debug)]
pub(crate) struct SealSettings {
    keys: Option<KeyRing>,
    active_key: Option<u32>,
    accept_plain: bool,
}

impl Default for SealSettings {
    fn default() -> Self {
        Self::from_seal(&FileSeal::PLAIN)
    }
}

impl SealSettings {
    pub(crate) fn from_seal(seal: &FileSeal<'_>) -> Self {
        Self {
            keys: seal.keys.cloned(),
            active_key: seal.active_key,
            accept_plain: seal.accept_plain,
        }
    }

    pub(crate) fn as_seal(&self) -> FileSeal<'_> {
        FileSeal {
            keys: self.keys.as_ref(),
            active_key: self.active_key,
            accept_plain: self.accept_plain,
        }
    }
}

/// Key and nonce of a sealed generation: decrypts any range of its region on demand.
pub(crate) struct PageCipher {
    key: EncryptionKey,
    nonce: [u8; 12],
}

impl PageCipher {
    /// Decrypts `buf`, which holds the region bytes starting at region offset `at`.
    #[inline]
    pub(crate) fn decrypt_at(&self, at: usize, buf: &mut [u8]) -> Result<()> {
        chacha20_xor_at(&self.key, &self.nonce, at as u64, buf)
    }

    /// A verifier for the region's tag (absorbs the ciphertext in file order).
    pub(crate) fn verifier(&self, data_path: &Path) -> RegionSealer {
        RegionSealer::new(&self.key, self.nonce, seal_context(data_path))
    }
}

/// The authenticated name of a generation file (binds the region to its index and generation).
fn seal_context(data_path: &Path) -> &[u8] {
    data_path.file_name().map_or(&[][..], |n| n.as_encoded_bytes())
}

/// Encrypts a generation's region while it is streamed to disk.
pub(crate) struct GenerationSealer {
    sealer: RegionSealer,
    key: EncryptionKey,
    key_id: u32,
    nonce: [u8; 12],
}

impl GenerationSealer {
    /// `Some` when `seal` seals new files.
    pub(crate) fn begin(seal: &FileSeal<'_>, data_path: &Path) -> Option<Self> {
        let (ring, key_id) = (seal.keys?, seal.active_key?);
        let key = ring.get(key_id)?.clone();
        let nonce = generate_unique_nonce();
        Some(Self {
            sealer: RegionSealer::new(&key, nonce, seal_context(data_path)),
            key,
            key_id,
            nonce,
        })
    }

    /// Encrypts the next piece of the region in place (pieces in file order; every piece but
    /// the last a multiple of 64 bytes).
    pub(crate) fn seal(&mut self, piece: &mut [u8]) -> std::io::Result<()> {
        self.sealer
            .seal_piece(piece)
            .map_err(|e| std::io::Error::other(e.to_string()))
    }

    /// The header-page encryption section and the cipher that reads the sealed generation.
    pub(crate) fn finish(self) -> ([u8; ENC_SECTION_LEN], PageCipher) {
        let mut section = [0u8; ENC_SECTION_LEN];
        section[0..4].copy_from_slice(&ENC_MAGIC.to_le_bytes());
        section[4..8].copy_from_slice(&self.key_id.to_le_bytes());
        section[8..20].copy_from_slice(&self.nonce);
        section[20..36].copy_from_slice(&self.sealer.finish());
        (section, PageCipher { key: self.key, nonce: self.nonce })
    }
}

/// Reads the encryption section of a generation's header page: `Some((cipher, tag))` if the
/// generation is sealed (its tag must still be verified over the region), `None` if it is
/// plaintext and plaintext is acceptable under `seal`. Plaintext with keys configured is refused
/// unless `accept_plain` (no downgrade); a sealed file needs its key in the ring.
pub(crate) fn open_generation_cipher(
    header: &[u8],
    seal: &FileSeal<'_>,
) -> Result<Option<(PageCipher, [u8; 16])>> {
    let section = header
        .get(ENC_SECTION_OFFSET..ENC_SECTION_OFFSET + ENC_SECTION_LEN)
        .ok_or_else(|| StorageError::corrupted_header("generation header page truncated"))?;
    if read_u32_le(section, 0) != ENC_MAGIC {
        if seal.keys.is_some() && !seal.accept_plain {
            return Err(StorageError::invalid_key(
                "plaintext index generation rejected: encryption is configured (no downgrade)",
            ));
        }
        return Ok(None);
    }
    let ring = seal.keys.ok_or_else(|| {
        StorageError::invalid_key("index generation is encrypted but no encryption key is configured")
    })?;
    let key = ring
        .get(read_u32_le(section, 4))
        .ok_or_else(|| StorageError::invalid_key("index generation sealed with a key id not in the key ring"))?;
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&section[8..20]);
    let mut tag = [0u8; 16];
    tag.copy_from_slice(&section[20..36]);
    Ok(Some((PageCipher { key: key.clone(), nonce }, tag)))
}
