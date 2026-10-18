//! Cross-platform OS virtual memory mapping (`TASK-007`, `F-CORE-01`, `F-CORE-08`, `RULE-SAFE-01`, `RULE-SAFE-02`).

use std::fmt;
use std::fs::File;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};
use storage_types::{Result, StorageError, HEADER_SIZE};

/// Kernel page-cache access advice hints (`madvise` on Unix, `PrefetchVirtualMemory` on Windows).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Advice {
    Normal,
    Random,
    Sequential,
    WillNeed,
    DontNeed,
}

/// Read-only memory-mapped virtual memory region backed by an OS file mapping.
///
/// Guaranteed to remain mapped and valid for the entire lifetime of `MmapRegion`.
/// When wrapped in `Arc<MmapRegion>` inside `MmapGuard`, unmapping is deferred until
/// the last active reader guard drops (`RULE-SAFE-02`).
pub struct MmapRegion {
    ptr: NonNull<u8>,
    len: usize,
    verified: AtomicBool,
    #[cfg(windows)]
    mapping_handle: isize,
}

// SAFETY: `MmapRegion` represents an immutable OS virtual memory view (`PAGE_READONLY` / `PROT_READ`).
unsafe impl Send for MmapRegion {}
// SAFETY: Concurrent threads only read immutable bytes from the mapped page.
unsafe impl Sync for MmapRegion {}

impl MmapRegion {
    /// Maps the first `len` bytes of `file` into process virtual address space as read-only.
    pub fn map_read_only(file: &File, len: usize) -> Result<Self> {
        if len == 0 {
            return Err(StorageError::corrupted_header(
                "cannot memory-map an empty 0-byte file",
            ));
        }
        Self::platform_map_read_only(file, len)
    }

    #[inline]
    pub const fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns `true` if `RecordCodec::validate_frame` has verified this mapped region (`RULE-SAFE-04`).
    #[inline]
    pub fn is_verified(&self) -> bool {
        self.verified.load(Ordering::Acquire)
    }

    /// Marks this mapped region as verified so subsequent hot reads skip `bytecheck` & checksum verification.
    #[inline]
    pub fn mark_verified(&self) {
        self.verified.store(true, Ordering::Release);
    }

    /// Returns the full mapped byte slice.
    /// OS page mappings are page-aligned (4096 / 65536 bytes), which is a multiple of 64 bytes (`HEADER_SIZE`).
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: `self.ptr` points to a valid read-only OS virtual memory mapping of `self.len` bytes
        // that stays alive until `Drop::drop` runs.
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Returns a 64-byte-aligned sub-slice `offset..offset + len` within the mapped region.
    #[inline]
    pub fn subslice_aligned(&self, offset: usize, len: usize) -> Result<&[u8]> {
        if (offset & (HEADER_SIZE - 1)) != 0 {
            return Err(StorageError::corrupted_header(
                "mmap subslice offset is not 64-byte aligned",
            ));
        }
        let end = offset
            .checked_add(len)
            .ok_or_else(|| StorageError::corrupted_header("mmap subslice offset overflow"))?;
        self.as_slice()
            .get(offset..end)
            .ok_or_else(|| StorageError::corrupted_header("mmap subslice out of region bounds"))
    }

    /// Provides kernel page-cache hints (`WillNeed`, `Random`, `Sequential`, `DontNeed`) for `offset..offset+len`.
    #[inline]
    pub fn advise(&self, offset: usize, len: usize, advice: Advice) {
        if offset.saturating_add(len) > self.len || len == 0 {
            return;
        }
        self.platform_advise(offset, len, advice);
    }

    #[cfg(windows)]
    fn platform_map_read_only(file: &File, len: usize) -> Result<Self> {
        use std::os::windows::io::AsRawHandle;

        const PAGE_READONLY: u32 = 0x02;
        const FILE_MAP_READ: u32 = 0x04;

        let raw_file = file.as_raw_handle() as isize;
        let size_hi = ((len as u64) >> 32) as u32;
        let size_lo = (len as u64) as u32;

        // SAFETY: `raw_file` is a valid open Win32 file handle, and we pass null security attributes
        // and null name for an anonymous read-only file mapping object.
        let mapping_handle = unsafe {
            CreateFileMappingW(
                raw_file,
                core::ptr::null(),
                PAGE_READONLY,
                size_hi,
                size_lo,
                core::ptr::null(),
            )
        };
        if mapping_handle == 0 {
            return Err(StorageError::io(
                "CreateFileMappingW",
                std::io::Error::last_os_error(),
            ));
        }

        // SAFETY: `mapping_handle` was just created and validated non-zero; we map `len` bytes read-only.
        let view_ptr = unsafe { MapViewOfFile(mapping_handle, FILE_MAP_READ, 0, 0, len) };
        let Some(ptr) = NonNull::new(view_ptr.cast::<u8>()) else {
            let err = std::io::Error::last_os_error();
            // SAFETY: `mapping_handle` is a valid handle that must be closed on error.
            unsafe {
                CloseHandle(mapping_handle);
            }
            return Err(StorageError::io("MapViewOfFile", err));
        };

        // OS page mappings are 64KB/4KB aligned, which is always a multiple of 64 bytes.
        debug_assert!((ptr.as_ptr() as usize) & (HEADER_SIZE - 1) == 0);

        Ok(Self {
            ptr,
            len,
            verified: AtomicBool::new(false),
            mapping_handle,
        })
    }

    #[cfg(windows)]
    fn platform_advise(&self, offset: usize, len: usize, advice: Advice) {
        if advice == Advice::WillNeed {
            // SAFETY: `offset` was bounds-checked by caller; `self.ptr.as_ptr().add(offset)` stays within `0..self.len`.
            let va = unsafe { self.ptr.as_ptr().add(offset) as *const core::ffi::c_void };
            let entry = Win32MemoryRangeEntry {
                virtual_address: va,
                number_of_bytes: len,
            };
            // SAFETY: `GetCurrentProcess()` returns the pseudo-handle `-1`, and `entry` points to
            // a valid range within our mapped view. Failure is non-fatal (hint only).
            unsafe {
                let proc_handle = GetCurrentProcess();
                let _ = PrefetchVirtualMemory(proc_handle, 1, &entry, 0);
            }
        }
    }

    #[cfg(unix)]
    fn platform_map_read_only(file: &File, len: usize) -> Result<Self> {
        use std::os::unix::io::AsRawFd;

        let fd = file.as_raw_fd();
        // SAFETY: Calling POSIX `mmap` with `PROT_READ` and `MAP_SHARED` on a valid file descriptor.
        let raw = unsafe {
            libc_mmap(
                core::ptr::null_mut(),
                len,
                PROT_READ,
                MAP_SHARED,
                fd,
                0,
            )
        };
        if raw == MAP_FAILED {
            return Err(StorageError::io("mmap", std::io::Error::last_os_error()));
        }
        let Some(ptr) = NonNull::new(raw as *mut u8) else {
            return Err(StorageError::corrupted_header("mmap returned null pointer"));
        };
        Ok(Self {
            ptr,
            len,
            verified: AtomicBool::new(false),
        })
    }

    #[cfg(unix)]
    fn platform_advise(&self, offset: usize, len: usize, advice: Advice) {
        let page_mask = 4095usize;
        let aligned_off = offset & !page_mask;
        let aligned_len = len + (offset - aligned_off);
        let flag = match advice {
            Advice::Normal => MADV_NORMAL,
            Advice::Random => MADV_RANDOM,
            Advice::Sequential => MADV_SEQUENTIAL,
            Advice::WillNeed => MADV_WILLNEED,
            Advice::DontNeed => MADV_DONTNEED,
        };
        // SAFETY: `aligned_off` is page-aligned and within the mapped region.
        unsafe {
            let _ = libc_madvise(
                self.ptr.as_ptr().add(aligned_off) as *mut core::ffi::c_void,
                aligned_len,
                flag,
            );
        }
    }
}

impl Drop for MmapRegion {
    fn drop(&mut self) {
        #[cfg(windows)]
        {
            // SAFETY: `self.ptr` and `self.mapping_handle` were created by `MapViewOfFile` and
            // `CreateFileMappingW` in `platform_map_read_only` and are uniquely owned by `self`.
            unsafe {
                let _ = UnmapViewOfFile(self.ptr.as_ptr() as *const core::ffi::c_void);
                let _ = CloseHandle(self.mapping_handle);
            }
        }
        #[cfg(unix)]
        {
            // SAFETY: `self.ptr` and `self.len` were returned by `mmap` in `platform_map_read_only`.
            unsafe {
                let _ = libc_munmap(self.ptr.as_ptr() as *mut core::ffi::c_void, self.len);
            }
        }
    }
}

impl fmt::Debug for MmapRegion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MmapRegion")
            .field("addr", &self.ptr)
            .field("len", &self.len)
            .finish()
    }
}

#[cfg(windows)]
#[repr(C)]
struct Win32MemoryRangeEntry {
    virtual_address: *const core::ffi::c_void,
    number_of_bytes: usize,
}

#[cfg(windows)]
#[link(name = "kernel32")]
extern "system" {
    fn CreateFileMappingW(
        hFile: isize,
        lpFileMappingAttributes: *const core::ffi::c_void,
        flProtect: u32,
        dwMaximumSizeHigh: u32,
        dwMaximumSizeLow: u32,
        lpName: *const u16,
    ) -> isize;

    fn MapViewOfFile(
        hFileMappingObject: isize,
        dwDesiredAccess: u32,
        dwFileOffsetHigh: u32,
        dwFileOffsetLow: u32,
        dwNumberOfBytesToMap: usize,
    ) -> *mut core::ffi::c_void;

    fn UnmapViewOfFile(lpBaseAddress: *const core::ffi::c_void) -> i32;
    fn CloseHandle(hObject: isize) -> i32;
    fn GetCurrentProcess() -> isize;
    fn PrefetchVirtualMemory(
        hProcess: isize,
        NumberOfEntries: usize,
        VirtualAddresses: *const Win32MemoryRangeEntry,
        Flags: u32,
    ) -> i32;
}

#[cfg(unix)]
const PROT_READ: i32 = 1;
#[cfg(unix)]
const MAP_SHARED: i32 = 1;
#[cfg(unix)]
const MAP_FAILED: *mut core::ffi::c_void = !0usize as *mut core::ffi::c_void;
#[cfg(unix)]
const MADV_NORMAL: i32 = 0;
#[cfg(unix)]
const MADV_RANDOM: i32 = 1;
#[cfg(unix)]
const MADV_SEQUENTIAL: i32 = 2;
#[cfg(unix)]
const MADV_WILLNEED: i32 = 3;
#[cfg(unix)]
const MADV_DONTNEED: i32 = 4;

#[cfg(unix)]
extern "C" {
    #[link_name = "mmap"]
    fn libc_mmap(
        addr: *mut core::ffi::c_void,
        len: usize,
        prot: i32,
        flags: i32,
        fd: i32,
        offset: i64,
    ) -> *mut core::ffi::c_void;

    #[link_name = "munmap"]
    fn libc_munmap(addr: *mut core::ffi::c_void, len: usize) -> i32;

    #[link_name = "madvise"]
    fn libc_madvise(addr: *mut core::ffi::c_void, len: usize, advice: i32) -> i32;
}
