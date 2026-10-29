//! Atomic disk persistence: Temp+Rename replacement and Ping-Pong double-buffered record slots (`TASK-010`, `F-CONC-05`).

use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use storage_types::vfs::{self, OpenOpts, Vfs, VfsHandle};
use storage_types::{RecordHeader, Result, StorageError, HEADER_SIZE};

/// Writes `frame` (`[RecordHeader | Payload]`) to `<target>.tmp.<tid>`, flushes to disk if `sync_data` is true,
/// and atomically replaces `target` (`MoveFileExW` on Windows, `rename` on POSIX).
pub fn atomic_write_replace(target: &Path, frame: &[u8], sync_data: bool) -> Result<()> {
    let vfs = vfs::at(target);
    if let Some(parent) = target.parent() {
        if !vfs.exists(parent) {
            vfs.create_dir_all(parent)
                .map_err(|e| StorageError::io("create record parent directory", e))?;
        }
    }

    let tmp_path = temp_path_for(target);
    {
        let mut tmp_file = vfs
            .open(&tmp_path, OpenOpts::create_truncate())
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

    if vfs.is_simulated() {
        return vfs_rename(&*vfs, &tmp_path, target, sync_data);
    }
    platform_atomic_rename(&tmp_path, target, sync_data)
}

/// Rename plus (when `write_through`) a sync of the destination directory.
fn vfs_rename(vfs: &dyn Vfs, src: &Path, dst: &Path, write_through: bool) -> Result<()> {
    if let Err(e) = vfs.rename(src, dst) {
        let _ = vfs.remove_file(src);
        return Err(StorageError::io("rename atomic replace", e));
    }
    if write_through {
        // A rename is only durable once the containing directory entry is synced.
        if let Some(parent) = dst.parent() {
            vfs.sync_dir(parent)
                .map_err(|e| StorageError::io("fsync parent directory after rename", e))?;
        }
    }
    Ok(())
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
fn platform_atomic_rename(src: &Path, dst: &Path, write_through: bool) -> Result<()> {
    vfs_rename(&vfs::RealVfs, src, dst, write_through)
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

    let vfs = vfs::at(path);
    if let Some(parent) = path.parent() {
        if !vfs.exists(parent) {
            vfs.create_dir_all(parent)
                .map_err(|e| StorageError::io("create ping-pong parent directory", e))?;
        }
    }

    let created = !vfs.exists(path);
    let mut file = vfs
        .open(path, OpenOpts::create())
        .map_err(|e| StorageError::io("open ping-pong record file", e))?;

    let file_len = file
        .len()
        .map_err(|e| StorageError::io("stat ping-pong record file", e))? as usize;

    let required_total = stride * 2;
    if file_len < required_total {
        file.set_len(required_total as u64)
            .map_err(|e| StorageError::io("preallocate ping-pong slots", e))?;
    }

    // Inspect existing headers in Slot 0 and Slot 1
    let mut read_header = |offset: u64| -> ([u8; HEADER_SIZE], u64) {
        let mut buf = [0u8; HEADER_SIZE];
        let version = file
            .seek(SeekFrom::Start(offset))
            .ok()
            .and_then(|_| file.read_exact(&mut buf).ok())
            .and_then(|_| RecordHeader::from_slice_verified(&buf).ok())
            .map_or(0, |h| h.record_version);
        (buf, version)
    };
    let (_, v0) = read_header(0);
    let (hdr1, v1) = read_header(stride as u64);

    // Overwrite the older slot. On a version tie (a same-version rewrite: WAL replay, re-seal)
    // overwrite the slot readers pick (`read_ping_pong_active` prefers slot 1), unless it already
    // holds exactly this frame — then the other slot, so two identical rewrites always reach both
    // slots. Every overwrite leaves a same-or-newer valid version in the other slot, so a torn
    // write (which fails its checksum) never loses the record.
    let target_offset = if v0 == v1 {
        if frame.get(..HEADER_SIZE) == Some(&hdr1[..]) {
            0
        } else {
            stride
        }
    } else if v0 < v1 {
        0
    } else {
        stride
    };

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
        if created {
            // A new file's directory entry is not durable until its directory is synced.
            if let Some(parent) = path.parent() {
                vfs.sync_dir(parent)
                    .map_err(|e| StorageError::io("fsync ping-pong record directory", e))?;
            }
        }
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
    let vfs = vfs::at(root);
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
                .and_then(|e| e.to_str()) == Some("tmp")
                && vfs.remove_file(&path).is_ok() {
                    removed += 1;
                }
        }
    }
    Ok(removed)
}

/// Exclusive OS-level file lock (`LockFileEx` on Windows, `flock` on Unix) preventing
/// concurrent processes or duplicate `StorageEngine` instances from corrupting the same
/// database collection directory (`TASK-059`).
#[derive(Debug)]
pub struct DirectoryLock {
    file: VfsHandle,
    path: PathBuf,
}

impl DirectoryLock {
    /// Acquires an immediate non-blocking exclusive lock on `lock_path`.
    /// Returns `Err(StorageError::Io)` if another instance or OS process already holds the lock.
    pub fn try_acquire(lock_path: &Path) -> Result<Self> {
        let vfs = vfs::at(lock_path);
        if let Some(parent) = lock_path.parent() {
            if !vfs.exists(parent) {
                vfs.create_dir_all(parent)
                    .map_err(|e| StorageError::io("create lock parent directory", e))?;
            }
        }
        let file = vfs
            .open(lock_path, OpenOpts::create())
            .map_err(|e| StorageError::io("open database collection .lock file", e))?;

        platform_try_lock_exclusive(file.file())?;

        Ok(Self {
            file,
            path: lock_path.to_path_buf(),
        })
    }

    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for DirectoryLock {
    fn drop(&mut self) {
        platform_unlock(self.file.file());
    }
}

#[cfg(windows)]
#[repr(C)]
struct Overlapped {
    internal: usize,
    internal_high: usize,
    offset: u32,
    offset_high: u32,
    h_event: isize,
}

#[cfg(windows)]
#[link(name = "kernel32")]
extern "system" {
    fn LockFileEx(
        hFile: isize,
        dwFlags: u32,
        dwReserved: u32,
        nNumberOfBytesToLockLow: u32,
        nNumberOfBytesToLockHigh: u32,
        lpOverlapped: *mut Overlapped,
    ) -> i32;

    fn UnlockFileEx(
        hFile: isize,
        dwReserved: u32,
        nNumberOfBytesToUnlockLow: u32,
        nNumberOfBytesToUnlockHigh: u32,
        lpOverlapped: *mut Overlapped,
    ) -> i32;
}

#[cfg(windows)]
fn platform_try_lock_exclusive(file: &fs::File) -> Result<()> {
    use std::os::windows::io::AsRawHandle;

    const LOCKFILE_FAIL_IMMEDIATELY: u32 = 0x0000_0001;
    const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x0000_0002;

    let handle = file.as_raw_handle() as isize;
    let mut overlapped = Overlapped {
        internal: 0,
        internal_high: 0,
        offset: 0,
        offset_high: 0,
        h_event: 0,
    };
    // SAFETY: `handle` is a valid open file handle and `overlapped` is zero-initialized on the stack.
    let ok = unsafe {
        LockFileEx(
            handle,
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            1,
            0,
            &mut overlapped,
        )
    };
    if ok == 0 {
        let err = std::io::Error::last_os_error();
        return Err(StorageError::io(
            "acquire exclusive database lock (another process or engine instance already holds .lock)",
            err,
        ));
    }
    Ok(())
}

#[cfg(windows)]
fn platform_unlock(file: &fs::File) {
    use std::os::windows::io::AsRawHandle;

    let handle = file.as_raw_handle() as isize;
    let mut overlapped = Overlapped {
        internal: 0,
        internal_high: 0,
        offset: 0,
        offset_high: 0,
        h_event: 0,
    };
    // SAFETY: `handle` is valid and unlocks the exact 1-byte region locked in `platform_try_lock_exclusive`.
    let _ = unsafe { UnlockFileEx(handle, 0, 1, 0, &mut overlapped) };
}

#[cfg(unix)]
extern "C" {
    fn flock(fd: i32, operation: i32) -> i32;
}

#[cfg(unix)]
fn platform_try_lock_exclusive(file: &fs::File) -> Result<()> {
    use std::os::unix::io::AsRawFd;

    const LOCK_EX: i32 = 2;
    const LOCK_NB: i32 = 4;

    // SAFETY: `file.as_raw_fd()` is a valid open file descriptor.
    let rc = unsafe { flock(file.as_raw_fd(), LOCK_EX | LOCK_NB) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        return Err(StorageError::io(
            "acquire exclusive database lock (another process or engine instance already holds .lock)",
            err,
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn platform_unlock(file: &fs::File) {
    use std::os::unix::io::AsRawFd;
    const LOCK_UN: i32 = 8;
    // SAFETY: `file.as_raw_fd()` is a valid open file descriptor.
    let _ = unsafe { flock(file.as_raw_fd(), LOCK_UN) };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(version: u64, fill: u8) -> Vec<u8> {
        let payload = vec![fill; 128];
        let hdr = RecordHeader::new(
            1,
            storage_types::Version(version),
            1,
            storage_codec::checksum_payload(&payload),
            payload.len() as u32,
            payload.len() as u32,
            0,
            storage_types::RecordFlags::NONE,
            1,
            0,
        );
        let mut f = hdr.to_bytes().to_vec();
        f.extend_from_slice(&payload);
        f
    }

    #[test]
    fn test_ping_pong_same_version_rewrite_is_visible_and_reaches_both_slots() {
        let dir = std::env::temp_dir().join(format!("stge_pingpong_tie_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let path = dir.join("r.rec");
        let read = |path: &Path| {
            let bytes = fs::read(path).unwrap();
            let (_, slice) = read_ping_pong_active(&bytes, 4096).unwrap();
            slice[HEADER_SIZE]
        };
        write_ping_pong_slot(&path, &frame(1, 0xA1), 4096, false).unwrap();
        write_ping_pong_slot(&path, &frame(2, 0xA2), 4096, false).unwrap();
        assert_eq!(read(&path), 0xA2);
        // Same-version rewrite (replay / re-seal): readers must see the new bytes.
        write_ping_pong_slot(&path, &frame(2, 0xB2), 4096, false).unwrap();
        assert_eq!(read(&path), 0xB2);
        // A second same-version rewrite leaves no slot holding the old bytes.
        write_ping_pong_slot(&path, &frame(2, 0xB2), 4096, false).unwrap();
        let bytes = fs::read(&path).unwrap();
        assert_eq!(bytes[HEADER_SIZE], 0xB2, "slot 0 payload");
        assert_eq!(bytes[4096 + HEADER_SIZE], 0xB2, "slot 1 payload");
        // A newer version still goes to the inactive slot and wins.
        write_ping_pong_slot(&path, &frame(3, 0xC3), 4096, false).unwrap();
        assert_eq!(read(&path), 0xC3);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_directory_lock_prevents_concurrent_open_and_releases_on_drop() {
        let dir = std::env::temp_dir().join("stge_test_dir_lock");
        let _ = fs::remove_dir_all(&dir);
        let lock_path = dir.join("agent_object.lock");

        let lock1 = DirectoryLock::try_acquire(&lock_path).unwrap();
        assert!(
            DirectoryLock::try_acquire(&lock_path).is_err(),
            "second concurrent lock acquisition on the same collection must fail immediately"
        );
        drop(lock1);

        let lock2 = DirectoryLock::try_acquire(&lock_path).unwrap();
        drop(lock2);
        let _ = fs::remove_dir_all(&dir);
    }
}

