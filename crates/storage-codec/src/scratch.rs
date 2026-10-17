//! 64-byte cache-line-aligned scratch buffers and immutable `AlignedBytes` (`TASK-006`, `RULE-MEM-02`).

use std::alloc::{alloc, dealloc, realloc, Layout};
use std::cell::RefCell;
use std::fmt;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::Arc;

/// Cache-line alignment (64 bytes) required for zero-copy `RecordHeader` and SIMD payloads.
pub const BUFFER_ALIGNMENT: usize = 64;

/// An immutable, reference-counted byte buffer guaranteed to be 64-byte aligned in memory.
/// Used by `HotDirtyArc` so concurrent readers can dereference `&RecordHeader` and `&T::Archived`
/// directly from in-flight memory before background disk flush completes.
#[derive(Clone)]
pub struct AlignedBytes {
    inner: Arc<AlignedStorage>,
}

struct AlignedStorage {
    ptr: NonNull<u8>,
    len: usize,
    cap: usize,
}

// SAFETY: `AlignedStorage` owns an immutable heap allocation and never mutates it after construction.
unsafe impl Send for AlignedStorage {}
// SAFETY: Shared references to `AlignedStorage` only expose immutable `&[u8]` slices.
unsafe impl Sync for AlignedStorage {}

impl AlignedBytes {
    /// Copies `src` into a newly allocated 64-byte-aligned immutable buffer.
    #[inline]
    pub fn copy_from_slice(src: &[u8]) -> Self {
        let cap = src.len().max(BUFFER_ALIGNMENT).next_multiple_of(BUFFER_ALIGNMENT);
        let layout = Layout::from_size_align(cap, BUFFER_ALIGNMENT).expect("valid aligned layout");
        // SAFETY: `cap >= 64` is non-zero and a multiple of 64.
        let raw = unsafe { alloc(layout) };
        let Some(ptr) = NonNull::new(raw) else {
            std::alloc::handle_alloc_error(layout);
        };
        // SAFETY: `ptr` has at least `src.len()` valid writable bytes and does not overlap `src`.
        unsafe {
            core::ptr::copy_nonoverlapping(src.as_ptr(), ptr.as_ptr(), src.len());
            if cap > src.len() {
                core::ptr::write_bytes(ptr.as_ptr().add(src.len()), 0, cap - src.len());
            }
        }
        Self {
            inner: Arc::new(AlignedStorage {
                ptr,
                len: src.len(),
                cap,
            }),
        }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: `inner.ptr` is valid for `inner.len` initialized bytes for the lifetime of `self`.
        unsafe { core::slice::from_raw_parts(self.inner.ptr.as_ptr(), self.inner.len) }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.len == 0
    }
}

impl Drop for AlignedStorage {
    fn drop(&mut self) {
        let layout =
            Layout::from_size_align(self.cap, BUFFER_ALIGNMENT).expect("valid aligned layout");
        // SAFETY: `self.ptr` was allocated with `layout` in `AlignedBytes::copy_from_slice`.
        unsafe {
            dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

impl Deref for AlignedBytes {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl fmt::Debug for AlignedBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AlignedBytes(len={}, align=64)", self.len())
    }
}

/// Reusable 64-byte-aligned growable scratch buffer for zero-allocation record serialization.
pub struct AlignedScratchBuf {
    ptr: NonNull<u8>,
    len: usize,
    cap: usize,
}

// SAFETY: `AlignedScratchBuf` uniquely owns its allocation.
unsafe impl Send for AlignedScratchBuf {}

impl AlignedScratchBuf {
    pub fn with_capacity(min_cap: usize) -> Self {
        let cap = min_cap.max(4096).next_multiple_of(BUFFER_ALIGNMENT);
        let layout = Layout::from_size_align(cap, BUFFER_ALIGNMENT).expect("valid aligned layout");
        // SAFETY: `cap >= 4096` is non-zero and aligned to 64.
        let raw = unsafe { alloc(layout) };
        let Some(ptr) = NonNull::new(raw) else {
            std::alloc::handle_alloc_error(layout);
        };
        Self { ptr, len: 0, cap }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        let required = self.len.checked_add(additional).expect("buffer size overflow");
        if required <= self.cap {
            return;
        }
        self.grow_amortized(required);
    }

    #[cold]
    #[inline(never)]
    fn grow_amortized(&mut self, required: usize) {
        let new_cap = required
            .max(self.cap.saturating_mul(2))
            .next_multiple_of(BUFFER_ALIGNMENT);
        let old_layout =
            Layout::from_size_align(self.cap, BUFFER_ALIGNMENT).expect("valid old layout");
        let new_layout =
            Layout::from_size_align(new_cap, BUFFER_ALIGNMENT).expect("valid new layout");
        // SAFETY: `self.ptr` was allocated with `old_layout`, and `new_cap >= self.cap > 0`.
        let raw = unsafe { realloc(self.ptr.as_ptr(), old_layout, new_cap) };
        let Some(ptr) = NonNull::new(raw) else {
            std::alloc::handle_alloc_error(new_layout);
        };
        self.ptr = ptr;
        self.cap = new_cap;
    }

    /// Pads `self.len` with zeroes up to a multiple of `align` (must be power of two `<= 64`).
    #[inline]
    pub fn align_to(&mut self, align: usize) {
        debug_assert!(align.is_power_of_two() && align <= BUFFER_ALIGNMENT);
        let mask = align - 1;
        let aligned_len = (self.len + mask) & !mask;
        let pad = aligned_len - self.len;
        if pad > 0 {
            self.reserve(pad);
            // SAFETY: `reserve(pad)` guarantees `self.cap >= aligned_len`.
            unsafe {
                core::ptr::write_bytes(self.ptr.as_ptr().add(self.len), 0, pad);
            }
            self.len = aligned_len;
        }
    }

    /// Appends `bytes` and returns the starting byte offset within the buffer.
    #[inline]
    pub fn extend_from_slice(&mut self, bytes: &[u8]) -> usize {
        let start = self.len;
        self.reserve(bytes.len());
        // SAFETY: `reserve` ensures capacity for `bytes.len()`, and `bytes` cannot alias `self.ptr`.
        unsafe {
            core::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.ptr.as_ptr().add(self.len),
                bytes.len(),
            );
        }
        self.len += bytes.len();
        start
    }

    /// Reserves `count` zeroed bytes at the current end of the buffer and returns their start offset.
    #[inline]
    pub fn allocate_zeroed(&mut self, count: usize, align: usize) -> usize {
        self.align_to(align);
        let start = self.len;
        self.reserve(count);
        // SAFETY: `reserve` guarantees capacity for `count` bytes after `self.len`.
        unsafe {
            core::ptr::write_bytes(self.ptr.as_ptr().add(self.len), 0, count);
        }
        self.len += count;
        start
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: `0..self.len` has been initialized.
        unsafe { core::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: `0..self.len` has been initialized and `self` is uniquely borrowed.
        unsafe { core::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    #[inline]
    pub fn freeze_copy(&self) -> AlignedBytes {
        AlignedBytes::copy_from_slice(self.as_slice())
    }
}

impl Drop for AlignedScratchBuf {
    fn drop(&mut self) {
        let layout =
            Layout::from_size_align(self.cap, BUFFER_ALIGNMENT).expect("valid aligned layout");
        // SAFETY: `self.ptr` was allocated with `layout`.
        unsafe {
            dealloc(self.ptr.as_ptr(), layout);
        }
    }
}

impl fmt::Debug for AlignedScratchBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AlignedScratchBuf(len={}, cap={}, align=64)",
            self.len, self.cap
        )
    }
}

thread_local! {
    static TLS_SCRATCH: RefCell<AlignedScratchBuf> = RefCell::new(AlignedScratchBuf::with_capacity(16 * 1024));
}

/// Runs `f` with a reusable thread-local 64-byte-aligned scratch buffer.
/// If re-entered recursively on the same thread, falls back to a temporary buffer cleanly.
#[inline]
pub fn with_thread_scratch<R>(f: impl FnOnce(&mut AlignedScratchBuf) -> R) -> R {
    TLS_SCRATCH.with(|cell| {
        if let Ok(mut guard) = cell.try_borrow_mut() {
            guard.clear();
            f(&mut guard)
        } else {
            let mut fallback = AlignedScratchBuf::with_capacity(4096);
            f(&mut fallback)
        }
    })
}
