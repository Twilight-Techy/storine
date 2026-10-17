//! Primary Index Checkpointing (`primary.idx`) (`TASK-012`, `F-INDX-02`).
//!
//! Persists every `PrimaryEntry` location into a compact, checksummed binary table so startup
//! never scans directory trees.
//!
//! ## Format v2 (`STGEIDX2`, written by default)
//! `magic (8B) | count (u64 LE) | entries... | XXH3-64 checksum of all preceding bytes (8B)`
//!
//! Each entry is variable-length (typically **~16-24 bytes** versus 88 bytes in v1):
//! - `InlineId` compact form: `len_tag (1B) | bytes[..len]`
//! - `version`: LEB128 varint
//! - `ttl_expires_at_ns`: LEB128 varint (`0` = 1 byte)
//! - `tag (1B)`: bits `0..2` = location kind (0 Unflushed, 1 DedicatedFile, 2 Arena), bit 2 = has parent group
//! - DedicatedFile: `ping_pong_stride` varint, then optional compact parent `InlineId`
//! - Arena: `segment_id` varint, `offset / 64` varint (offsets are 64-byte aligned), `len` varint
//!
//! Ordinals are **not** persisted: they are reassigned densely on load (`PrimaryIndex::restore_entry`).
//! Legacy v1 (`STGEIDX1`, fixed 88-byte rows) checkpoints are still readable.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use storage_codec::checksum_payload;
use storage_mmap::{atomic_write_replace, ArenaLocation};
use storage_types::{InlineId, RecordOrdinal, Result, StorageError, Version, HEADER_SIZE};

use crate::primary::{DiskLocation, PrimaryEntry, PrimaryIndex};

const CHECKPOINT_MAGIC_V1: &[u8; 8] = b"STGEIDX1";
const CHECKPOINT_MAGIC_V2: &[u8; 8] = b"STGEIDX2";
const CHECKPOINT_DELTA_MAGIC: &[u8; 8] = b"STGEDLT2";
const V1_ENTRY_STRIDE: usize = 88;
/// Smallest possible v2 entry: 2B id + 1B version + 1B ttl + 1B tag.
const V2_MIN_ENTRY_LEN: usize = 5;

const LOC_UNFLUSHED: u8 = 0;
const LOC_DEDICATED: u8 = 1;
const LOC_ARENA: u8 = 2;
const LOC_KIND_MASK: u8 = 0b011;
const LOC_HAS_PARENT: u8 = 0b100;

/// Returns the companion incremental `.delta` checkpoint path for `base_idx_path` (`TASK-054`).
#[inline]
pub fn delta_checkpoint_path(base_idx_path: &Path) -> PathBuf {
    let mut os = base_idx_path.as_os_str().to_os_string();
    os.push(".delta");
    PathBuf::from(os)
}

#[inline]
fn write_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

#[inline]
fn read_varint(buf: &[u8], cursor: &mut usize) -> Result<u64> {
    let mut result = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *buf
            .get(*cursor)
            .ok_or_else(|| StorageError::corrupted_header("truncated varint in checkpoint"))?;
        *cursor += 1;
        let low = (byte & 0x7F) as u64;
        if shift == 63 && low > 1 {
            return Err(StorageError::corrupted_header("varint overflow in checkpoint"));
        }
        result |= low << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
    }
    Err(StorageError::corrupted_header("varint too long in checkpoint"))
}

#[inline]
fn read_varint_u32(buf: &[u8], cursor: &mut usize) -> Result<u32> {
    u32::try_from(read_varint(buf, cursor)?)
        .map_err(|_| StorageError::corrupted_header("u32 field overflow in checkpoint"))
}

#[inline]
fn read_u8(buf: &[u8], cursor: &mut usize) -> Result<u8> {
    let b = *buf
        .get(*cursor)
        .ok_or_else(|| StorageError::corrupted_header("truncated checkpoint entry"))?;
    *cursor += 1;
    Ok(b)
}

#[inline]
fn encode_v2_entry(buf: &mut Vec<u8>, entry: &PrimaryEntry) {
    entry.id.write_compact_to(buf);
    write_varint(buf, entry.version.0);
    write_varint(buf, entry.ttl_expires_at_ns);
    match entry.location {
        DiskLocation::Unflushed => buf.push(LOC_UNFLUSHED),
        DiskLocation::DedicatedFile {
            parent_group,
            ping_pong_stride,
        } => {
            let tag = LOC_DEDICATED | if parent_group.is_some() { LOC_HAS_PARENT } else { 0 };
            buf.push(tag);
            write_varint(buf, u64::from(ping_pong_stride));
            if let Some(parent) = parent_group {
                parent.write_compact_to(buf);
            }
        }
        DiskLocation::Arena(loc) => {
            buf.push(LOC_ARENA);
            write_varint(buf, u64::from(loc.segment_id));
            write_varint(buf, u64::from(loc.offset) / HEADER_SIZE as u64);
            write_varint(buf, u64::from(loc.len));
        }
    }
}

#[inline]
fn decode_v2_entry(body: &[u8], cursor: &mut usize) -> Result<PrimaryEntry> {
    let id = InlineId::read_compact_from(body, cursor)?;
    let version = Version(read_varint(body, cursor)?);
    let ttl_expires_at_ns = read_varint(body, cursor)?;
    let tag = read_u8(body, cursor)?;
    let location = match tag & LOC_KIND_MASK {
        LOC_UNFLUSHED => DiskLocation::Unflushed,
        LOC_DEDICATED => {
            let ping_pong_stride = read_varint_u32(body, cursor)?;
            let parent_group = if tag & LOC_HAS_PARENT != 0 {
                Some(InlineId::read_compact_from(body, cursor)?)
            } else {
                None
            };
            DiskLocation::DedicatedFile {
                parent_group,
                ping_pong_stride,
            }
        }
        LOC_ARENA => {
            let segment_id = read_varint_u32(body, cursor)?;
            let offset_units = read_varint_u32(body, cursor)?;
            let offset = offset_units
                .checked_mul(HEADER_SIZE as u32)
                .ok_or_else(|| StorageError::corrupted_header("arena offset overflow"))?;
            let len = read_varint_u32(body, cursor)?;
            DiskLocation::Arena(ArenaLocation {
                segment_id,
                offset,
                len,
            })
        }
        _ => {
            return Err(StorageError::corrupted_header(
                "invalid location tag in primary checkpoint",
            ))
        }
    };
    Ok(restored(id, version, ttl_expires_at_ns, location))
}

/// Saves a full atomic checkpoint of `index` to `path` (`primary.idx`) in compact v2 format,
/// and removes any superseded `<path>.delta` incremental log.
/// Returns the number of entries written.
pub fn save_primary_checkpoint(index: &PrimaryIndex, path: &Path) -> Result<usize> {
    let entries = index.entries_snapshot();
    let mut buf = Vec::with_capacity(24 + entries.len() * 24);
    buf.extend_from_slice(CHECKPOINT_MAGIC_V2);
    buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());

    for entry in &entries {
        encode_v2_entry(&mut buf, entry);
    }

    let sum = checksum_payload(&buf);
    buf.extend_from_slice(&sum.to_le_bytes());

    atomic_write_replace(path, &buf, true)?;
    let _ = std::fs::remove_file(delta_checkpoint_path(path));
    Ok(entries.len())
}

/// Appends an incremental $O(\Delta)$ delta frame (`STGEDLT2`) of modified `upserted_ids` and
/// `deleted_ids` to `<base_idx_path>.delta` without rewriting the full `primary.idx` (`TASK-054`).
pub fn append_primary_checkpoint_delta(
    index: &PrimaryIndex,
    base_idx_path: &Path,
    upserted_ids: &[InlineId],
    deleted_ids: &[InlineId],
) -> Result<usize> {
    let mut upserts = Vec::with_capacity(upserted_ids.len());
    for &id in upserted_ids {
        if let Some(entry) = index.get(id) {
            upserts.push(entry);
        }
    }
    if upserts.is_empty() && deleted_ids.is_empty() {
        return Ok(0);
    }

    let mut payload = Vec::with_capacity((upserts.len() + deleted_ids.len()) * 24 + 8);
    write_varint(&mut payload, upserts.len() as u64);
    for entry in &upserts {
        encode_v2_entry(&mut payload, entry);
    }
    write_varint(&mut payload, deleted_ids.len() as u64);
    for &del_id in deleted_ids {
        del_id.write_compact_to(&mut payload);
    }

    let sum = checksum_payload(&payload);
    let mut frame = Vec::with_capacity(20 + payload.len());
    frame.extend_from_slice(CHECKPOINT_DELTA_MAGIC);
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&sum.to_le_bytes());
    frame.extend_from_slice(&payload);

    let delta_path = delta_checkpoint_path(base_idx_path);
    if let Some(parent) = delta_path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)
                .map_err(|e| StorageError::io("create checkpoint delta directory", e))?;
        }
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&delta_path)
        .map_err(|e| StorageError::io("open primary checkpoint delta file", e))?;
    file.write_all(&frame)
        .map_err(|e| StorageError::io("write primary checkpoint delta frame", e))?;
    file.sync_data()
        .map_err(|e| StorageError::io("fsync primary checkpoint delta file", e))?;

    Ok(upserts.len() + deleted_ids.len())
}

/// Loads a `primary.idx` checkpoint file (v2 or legacy v1) into `index` and automatically
/// replays any companion `<path>.delta` incremental frames (`TASK-054`).
pub fn load_primary_checkpoint(index: &PrimaryIndex, path: &Path) -> Result<usize> {
    let mut base_loaded = 0usize;
    if path.exists() {
        let bytes = std::fs::read(path)
            .map_err(|e| StorageError::io("read primary index checkpoint", e))?;
        if bytes.len() < 24 {
            return Err(StorageError::corrupted_header(
                "primary index checkpoint shorter than 24 bytes",
            ));
        }

        let body_len = bytes.len() - 8;
        let body = &bytes[..body_len];
        let expected_sum = u64::from_le_bytes(bytes[body_len..].try_into().unwrap());
        let actual_sum = checksum_payload(body);
        if actual_sum != expected_sum {
            return Err(StorageError::checksum_mismatch(expected_sum, actual_sum));
        }

        let count = u64::from_le_bytes(body[8..16].try_into().unwrap());
        base_loaded = match &body[0..8] {
            m if m == CHECKPOINT_MAGIC_V2 => load_v2(index, body, count)?,
            m if m == CHECKPOINT_MAGIC_V1 => load_v1(index, body, count)?,
            _ => {
                return Err(StorageError::corrupted_header(
                    "invalid primary index checkpoint magic",
                ))
            }
        };
    }

    let delta_path = delta_checkpoint_path(path);
    if delta_path.exists() {
        let _ = replay_primary_checkpoint_deltas(index, &delta_path)?;
        return Ok(index.len());
    }

    Ok(base_loaded)
}

fn replay_primary_checkpoint_deltas(index: &PrimaryIndex, delta_path: &Path) -> Result<usize> {
    let raw = std::fs::read(delta_path)
        .map_err(|e| StorageError::io("read primary checkpoint delta file", e))?;
    let mut pos = 0usize;
    let mut applied = 0usize;

    while pos + 20 <= raw.len() {
        if &raw[pos..pos + 8] != CHECKPOINT_DELTA_MAGIC {
            break;
        }
        let payload_len = u32::from_le_bytes(raw[pos + 8..pos + 12].try_into().unwrap()) as usize;
        let expected_sum = u64::from_le_bytes(raw[pos + 12..pos + 20].try_into().unwrap());
        let Some(frame_end) = (pos + 20).checked_add(payload_len) else {
            break;
        };
        if frame_end > raw.len() {
            break;
        }
        let payload = &raw[pos + 20..frame_end];
        if checksum_payload(payload) != expected_sum {
            break;
        }

        let mut cursor = 0usize;
        let upsert_count = read_varint(payload, &mut cursor)? as usize;
        for _ in 0..upsert_count {
            let entry = decode_v2_entry(payload, &mut cursor)?;
            index.restore_entry(entry);
            applied += 1;
        }
        let delete_count = read_varint(payload, &mut cursor)? as usize;
        for _ in 0..delete_count {
            let del_id = InlineId::read_compact_from(payload, &mut cursor)?;
            if let Some(removed) = index.remove(del_id) {
                index.release_ordinal(removed.ordinal);
            }
            applied += 1;
        }
        pos = frame_end;
    }

    Ok(applied)
}

fn load_v2(index: &PrimaryIndex, body: &[u8], count: u64) -> Result<usize> {
    // Reject impossible counts before doing any work (defends against crafted headers).
    let max_possible = ((body.len() - 16) / V2_MIN_ENTRY_LEN) as u64;
    if count > max_possible {
        return Err(StorageError::corrupted_header(
            "checkpoint entry count exceeds file size",
        ));
    }
    let mut cursor = 16usize;
    for _ in 0..count {
        let entry = decode_v2_entry(body, &mut cursor)?;
        index.restore_entry(entry);
    }
    if cursor != body.len() {
        return Err(StorageError::corrupted_header(
            "trailing bytes after primary checkpoint entries",
        ));
    }
    Ok(count as usize)
}

fn load_v1(index: &PrimaryIndex, body: &[u8], count: u64) -> Result<usize> {
    let count = usize::try_from(count)
        .map_err(|_| StorageError::corrupted_header("checkpoint count overflow"))?;
    let expected_len = count
        .checked_mul(V1_ENTRY_STRIDE)
        .and_then(|n| n.checked_add(16))
        .ok_or_else(|| StorageError::corrupted_header("checkpoint total length overflow"))?;
    if body.len() != expected_len {
        return Err(StorageError::corrupted_header(
            "primary index checkpoint byte length mismatch",
        ));
    }
    for i in 0..count {
        let off = 16 + i * V1_ENTRY_STRIDE;
        let row = &body[off..off + V1_ENTRY_STRIDE];
        let id = InlineId::from_raw_bytes(row[0..24].try_into().unwrap())?;
        let loc_tag = row[28];
        let has_parent = row[29] != 0;
        let version = Version(u64::from_le_bytes(row[32..40].try_into().unwrap()));
        let ttl_expires_at_ns = u64::from_le_bytes(row[48..56].try_into().unwrap());
        let location = match loc_tag {
            0 => DiskLocation::Unflushed,
            1 => DiskLocation::DedicatedFile {
                parent_group: if has_parent {
                    Some(InlineId::from_raw_bytes(row[64..88].try_into().unwrap())?)
                } else {
                    None
                },
                ping_pong_stride: u32::from_le_bytes(row[56..60].try_into().unwrap()),
            },
            2 => DiskLocation::Arena(ArenaLocation {
                segment_id: u32::from_le_bytes(row[56..60].try_into().unwrap()),
                offset: u32::from_le_bytes(row[60..64].try_into().unwrap()),
                len: u32::from_le_bytes(row[64..68].try_into().unwrap()),
            }),
            _ => {
                return Err(StorageError::corrupted_header(
                    "invalid location tag in primary checkpoint",
                ))
            }
        };
        index.restore_entry(restored(id, version, ttl_expires_at_ns, location));
    }
    Ok(count)
}

#[inline]
fn restored(
    id: InlineId,
    version: Version,
    ttl_expires_at_ns: u64,
    location: DiskLocation,
) -> PrimaryEntry {
    PrimaryEntry {
        id,
        ordinal: RecordOrdinal::INVALID,
        version,
        ttl_expires_at_ns,
        location,
        // Bytes on disk are untrusted until re-validated by the engine.
        disk_verified: false,
        hot_dirty: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_v2_checkpoint_and_incremental_delta_roundtrip() {
        let dir = std::env::temp_dir().join(format!("stge_ckpt_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("primary.idx");

        let idx = PrimaryIndex::new();
        let n = 1000u32;
        for i in 0..n {
            let id = InlineId::new(&format!("key_{i:06}")).unwrap();
            let location = match i % 3 {
                0 => DiskLocation::Arena(ArenaLocation {
                    segment_id: 1,
                    offset: 64 * (i + 1),
                    len: 200,
                }),
                1 => DiskLocation::DedicatedFile {
                    parent_group: Some(InlineId::new("parent").unwrap()),
                    ping_pong_stride: 0,
                },
                _ => DiskLocation::Unflushed,
            };
            idx.restore_entry(restored(id, Version(u64::from(i) + 1), 0, location));
        }
        save_primary_checkpoint(&idx, &path).unwrap();
        let file_len = std::fs::metadata(&path).unwrap().len() as usize;
        // v1 would be 16 + 88 * n + 8 bytes.
        assert!(file_len < (16 + 88 * n as usize + 8) / 3, "file_len={file_len}");

        // Apply an incremental delta: update key_000001 and delete key_000002
        let k1 = InlineId::new("key_000001").unwrap();
        let k2 = InlineId::new("key_000002").unwrap();
        idx.restore_entry(restored(
            k1,
            Version(999),
            0,
            DiskLocation::Arena(ArenaLocation {
                segment_id: 2,
                offset: 128,
                len: 256,
            }),
        ));
        if let Some(removed) = idx.remove(k2) {
            idx.release_ordinal(removed.ordinal);
        }
        append_primary_checkpoint_delta(&idx, &path, &[k1], &[k2]).unwrap();

        let loaded = PrimaryIndex::new();
        assert_eq!(load_primary_checkpoint(&loaded, &path).unwrap(), (n - 1) as usize);
        assert_eq!(loaded.get(k1).unwrap().version, Version(999));
        assert!(loaded.get(k2).is_none());

        // Full checkpoint removes the `.delta` file
        save_primary_checkpoint(&idx, &path).unwrap();
        assert!(!delta_checkpoint_path(&path).exists());

        // Corrupt one byte -> checksum must reject.
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[20] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        assert!(load_primary_checkpoint(&PrimaryIndex::new(), &path).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_varint_rejects_overflow() {
        let buf = [0xFFu8; 11];
        let mut c = 0;
        assert!(read_varint(&buf, &mut c).is_err());
    }
}
