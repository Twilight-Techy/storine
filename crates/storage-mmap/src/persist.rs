//! Atomic disk persistence: Temp+Rename replacement and Ping-Pong double-buffered record slots (`TASK-010`, `F-CONC-05`).

use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use storage_types::{RecordHeader, Result, StorageError, HEADER_SIZE};

/// Writes `frame` (`[RecordHeader | Payload]`) to `<target>.tmp.<tid>`, flushes to disk if `sync_data` is true,
/// and atomically replaces `target` (`MoveFileExW` on Windows, `rename` on POSIX).
pub fn atomic_write_replace(target: &Path, frame: &[u8], sync_data: bool) -> Result<()> {
    if let Some(parent) = target.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)
                .map_err(|e| StorageError::io("create record parent directory", e))?;
        }
    }

    let tmp_path = temp_path_for(target);
    {
        let mut tmp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(|e| StorageError::io("open temporary record file", e))?;

        tmp_file
            .write_all(frame)
            .map_err(|e| StorageError::io("write temporary record file", e))?;

        if sync_data {
            tmp_file
                .sync_data()
                .map_err(|e| StorageError::io("fsync temporary record file", e))?;
        }
    }

    platform_atomic_rename(&tmp_path, target, sync_data)
}

#[inline]
fn temp_path_for(target: &Path) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT_TMP_SEQ: AtomicU64 = AtomicU64::new(1);
    let seq = NEXT_TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let mut os = target.as_os_str().to_os_string();
    os.push(format!(".{seq}.tmp"));
    PathBuf::from(os)
}

#[cfg(windows)]
fn platform_atomic_rename(src: &Path, dst: &Path, write_through: bool) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x01;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x08;

    let src_w: Vec<u16> = src.as_os_str().encode_wide().chain(Some(0)).collect();
    let dst_w: Vec<u16> = dst.as_os_str().encode_wide().chain(Some(0)).collect();
    let flags = if write_through {
        MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH
    } else {
        MOVEFILE_REPLACE_EXISTING
    };

    // SAFETY: `src_w` and `dst_w` are valid null-terminated UTF-16 strings.
    let ok = unsafe { MoveFileExW(src_w.as_ptr(), dst_w.as_ptr(), flags) };
    if ok == 0 {
        let err = std::io::Error::last_os_error();
        let _ = fs::remove_file(src);
        return Err(StorageError::io("MoveFileExW atomic replace", err));
    }
    Ok(())
}

#[cfg(windows)]
#[link(name = "kernel32")]
extern "system" {
    fn MoveFileExW(
        lpExistingFileName: *const u16,
        lpNewFileName: *const u16,
        dwFlags: u32,
    ) -> i32;
}

#[cfg(unix)]
fn platform_atomic_rename(src: &Path, dst: &Path, _write_through: bool) -> Result<()> {
    if let Err(e) = fs::rename(src, dst) {
        let _ = fs::remove_file(src);
        return Err(StorageError::io("rename atomic replace", e));
    }
    Ok(())
}

/// Writes `frame` into a pre-allocated Ping-Pong double-buffered record file (`[Slot 0 | Slot 1]`).
///
/// Each slot has capacity `slot_stride` (a multiple of 64 bytes, e.g. `4096`).
/// The writer inspects the headers of Slot 0 (offset `0`) and Slot 1 (offset `slot_stride`),
/// selects the slot with the lower or invalid `record_version`, writes the new frame there,
/// and returns `(winning_slot_offset, frame.len())`.
///
/// Because the reader always picks the valid checksummed slot with the highest `record_version`,
/// a crash mid-write in the inactive slot leaves the previous active slot 100% intact without
/// requiring file creation or directory rename syscalls!
pub fn write_ping_pong_slot(
    path: &Path,
    frame: &[u8],
    slot_stride: usize,
    sync_data: bool,
) -> Result<(usize, usize)> {
    let stride = slot_stride.max(frame.len()).next_multiple_of(HEADER_SIZE);
    if frame.len() > stride {
        return Err(StorageError::corrupted_header(
            "frame exceeds ping-pong slot stride",
        ));
    }

    if let Some(parent) = path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)
                .map_err(|e| StorageError::io("create ping-pong parent directory", e))?;
        }
    }

    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)
        .map_err(|e| StorageError::io("open ping-pong record file", e))?;

    let file_len = file
        .metadata()
        .map_err(|e| StorageError::io("stat ping-pong record file", e))?
        .len() as usize;

    let required_total = stride * 2;
    if file_len < required_total {
        file.set_len(required_total as u64)
            .map_err(|e| StorageError::io("preallocate ping-pong slots", e))?;
    }

    // Inspect existing headers in Slot 0 and Slot 1
    let mut hdr_buf = [0u8; HEADER_SIZE];
    let v0 = file
        .seek(SeekFrom::Start(0))
        .ok()
        .and_then(|_| file.read_exact(&mut hdr_buf).ok())
        .and_then(|_| RecordHeader::from_slice_verified(&hdr_buf).ok())
        .map(|h| h.record_version)
        .unwrap_or(0);

    let v1 = file
        .seek(SeekFrom::Start(stride as u64))
        .ok()
        .and_then(|_| file.read_exact(&mut hdr_buf).ok())
        .and_then(|_| RecordHeader::from_slice_verified(&hdr_buf).ok())
        .map(|h| h.record_version)
        .unwrap_or(0);

    // Choose the older/inactive slot to overwrite
    let target_offset = if v0 <= v1 { 0 } else { stride };

    // Write payload first (after 64-byte header), then write the 64-byte header last so
    // `header_checksum` commits the slot atomically.
    file.seek(SeekFrom::Start((target_offset + HEADER_SIZE) as u64))
        .map_err(|e| StorageError::io("seek ping-pong payload", e))?;
    file.write_all(&frame[HEADER_SIZE..])
        .map_err(|e| StorageError::io("write ping-pong payload", e))?;

    file.seek(SeekFrom::Start(target_offset as u64))
        .map_err(|e| StorageError::io("seek ping-pong header", e))?;
    file.write_all(&frame[..HEADER_SIZE])
        .map_err(|e| StorageError::io("write ping-pong header", e))?;

    if sync_data {
        file.sync_data()
            .map_err(|e| StorageError::io("fsync ping-pong record file", e))?;
    }

    Ok((target_offset, frame.len()))
}

/// Inspects a mapped Ping-Pong double-slot buffer and returns the `(offset, frame_slice)`
/// of the valid slot with the highest `record_version`.
pub fn read_ping_pong_active(mapped: &[u8], slot_stride: usize) -> Result<(usize, &[u8])> {
    let slot0 = check_slot(mapped, 0);
    let slot1 = if mapped.len() >= slot_stride + HEADER_SIZE {
        check_slot(mapped, slot_stride)
    } else {
        None
    };

    match (slot0, slot1) {
        (Some((v0, s0)), Some((v1, s1))) => {
            if v1 >= v0 {
                Ok((slot_stride, s1))
            } else {
                Ok((0, s0))
            }
        }
        (Some((_, s0)), None) => Ok((0, s0)),
        (None, Some((_, s1))) => Ok((slot_stride, s1)),
        (None, None) => Err(StorageError::corrupted_header(
            "neither ping-pong slot contains a valid RecordHeader",
        )),
    }
}

#[inline]
fn check_slot(mapped: &[u8], offset: usize) -> Option<(u64, &[u8])> {
    let rem = mapped.get(offset..)?;
    let hdr = RecordHeader::from_slice_verified(rem).ok()?;
    let frame_len = HEADER_SIZE.checked_add(hdr.payload_len as usize)?;
    let slice = rem.get(..frame_len)?;
    if storage_codec::checksum_payload(&slice[HEADER_SIZE..]) == hdr.payload_checksum {
        Some((hdr.record_version, slice))
    } else {
        None
    }
}

/// Recursively deletes any orphaned `.tmp` files left behind by an interrupted crash before rename (`F-DURA-04`).
pub fn cleanup_temp_files(root: &Path) -> Result<usize> {
    if !root.exists() {
        return Ok(0);
    }
    let mut removed = 0usize;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(it) => it,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .extension()
                .and_then(|e| e.to_str())
                .map_or(false, |ext| ext == "tmp")
            {
                if fs::remove_file(&path).is_ok() {
                    removed += 1;
                }
            }
        }
    }
    Ok(removed)
}
