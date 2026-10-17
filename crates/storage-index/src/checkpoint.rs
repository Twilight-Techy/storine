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

use std::path::Path;
use storage_codec::checksum_payload;
use storage_mmap::{atomic_write_replace, ArenaLocation};
use storage_types::{InlineId, RecordOrdinal, Result, StorageError, Version, HEADER_SIZE};

use crate::primary::{DiskLocation, PrimaryEntry, PrimaryIndex};

const CHECKPOINT_MAGIC_V1: &[u8; 8] = b"STGEIDX1";
const CHECKPOINT_MAGIC_V2: &[u8; 8] = b"STGEIDX2";
const V1_ENTRY_STRIDE: usize = 88;
/// Smallest possible v2 entry: 2B id + 1B version + 1B ttl + 1B tag.
const V2_MIN_ENTRY_LEN: usize = 5;

const LOC_UNFLUSHED: u8 = 0;
const LOC_DEDICATED: u8 = 1;
const LOC_ARENA: u8 = 2;
const LOC_KIND_MASK: u8 = 0b011;
const LOC_HAS_PARENT: u8 = 0b100;

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

/// Saves a full atomic checkpoint of `index` to `path` (`primary.idx`) in compact v2 format.
/// Returns the number of entries written.
pub fn save_primary_checkpoint(index: &PrimaryIndex, path: &Path) -> Result<usize> {
    let entries = index.entries_snapshot();
    let mut buf = Vec::with_capacity(24 + entries.len() * 24);
    buf.extend_from_slice(CHECKPOINT_MAGIC_V2);
    buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());

    for entry in &entries {
        entry.id.write_compact_to(&mut buf);
        write_varint(&mut buf, entry.version.0);
        write_varint(&mut buf, entry.ttl_expires_at_ns);
        match entry.location {
            DiskLocation::Unflushed => buf.push(LOC_UNFLUSHED),
            DiskLocation::DedicatedFile {
                parent_group,
                ping_pong_stride,
            } => {
                let tag = LOC_DEDICATED | if parent_group.is_some() { LOC_HAS_PARENT } else { 0 };
                buf.push(tag);
                write_varint(&mut buf, u64::from(ping_pong_stride));
                if let Some(parent) = parent_group {
                    parent.write_compact_to(&mut buf);
                }
            }
            DiskLocation::Arena(loc) => {
                buf.push(LOC_ARENA);
                write_varint(&mut buf, u64::from(loc.segment_id));
                write_varint(&mut buf, u64::from(loc.offset) / HEADER_SIZE as u64);
                write_varint(&mut buf, u64::from(loc.len));
            }
        }
    }

    let sum = checksum_payload(&buf);
    buf.extend_from_slice(&sum.to_le_bytes());

    atomic_write_replace(path, &buf, true)?;
    Ok(entries.len())
}

/// Loads a `primary.idx` checkpoint file (v2 or legacy v1) into `index`.
pub fn load_primary_checkpoint(index: &PrimaryIndex, path: &Path) -> Result<usize> {
    if !path.exists() {
        return Ok(0);
    }
    let bytes =
        std::fs::read(path).map_err(|e| StorageError::io("read primary index checkpoint", e))?;
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
    match &body[0..8] {
        m if m == CHECKPOINT_MAGIC_V2 => load_v2(index, body, count),
        m if m == CHECKPOINT_MAGIC_V1 => load_v1(index, body, count),
        _ => Err(StorageError::corrupted_header(
            "invalid primary index checkpoint magic",
        )),
    }
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
        let id = InlineId::read_compact_from(body, &mut cursor)?;
        let version = Version(read_varint(body, &mut cursor)?);
        let ttl_expires_at_ns = read_varint(body, &mut cursor)?;
        let tag = read_u8(body, &mut cursor)?;
        let location = match tag & LOC_KIND_MASK {
            LOC_UNFLUSHED => DiskLocation::Unflushed,
            LOC_DEDICATED => {
                let ping_pong_stride = read_varint_u32(body, &mut cursor)?;
                let parent_group = if tag & LOC_HAS_PARENT != 0 {
                    Some(InlineId::read_compact_from(body, &mut cursor)?)
                } else {
                    None
                };
                DiskLocation::DedicatedFile {
                    parent_group,
                    ping_pong_stride,
                }
            }
            LOC_ARENA => {
                let segment_id = read_varint_u32(body, &mut cursor)?;
                let offset_units = read_varint_u32(body, &mut cursor)?;
                let offset = offset_units
                    .checked_mul(HEADER_SIZE as u32)
                    .ok_or_else(|| StorageError::corrupted_header("arena offset overflow"))?;
                let len = read_varint_u32(body, &mut cursor)?;
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
        index.restore_entry(restored(id, version, ttl_expires_at_ns, location));
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
    fn test_v2_checkpoint_roundtrip_and_size() {
        let dir = std::env::temp_dir().join(format!("stge_ckpt_{}", std::process::id()));
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

        let loaded = PrimaryIndex::new();
        assert_eq!(load_primary_checkpoint(&loaded, &path).unwrap(), n as usize);
        for i in 0..n {
            let id = InlineId::new(&format!("key_{i:06}")).unwrap();
            let a = idx.get(id).unwrap();
            let b = loaded.get(id).unwrap();
            assert_eq!(a.version, b.version);
            assert_eq!(a.location, b.location);
            assert!(!b.disk_verified);
        }

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
