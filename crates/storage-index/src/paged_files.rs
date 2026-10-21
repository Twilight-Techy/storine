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
