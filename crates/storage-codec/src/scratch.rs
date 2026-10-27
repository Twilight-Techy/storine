//! 64-byte cache-line-aligned scratch buffers and immutable `AlignedBytes` (`TASK-006`, `RULE-MEM-02`).

use std::alloc::{alloc, dealloc, realloc, Layout};
use std::cell::RefCell;
use std::fmt;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::Arc;

/// Cache-line alignment (64 bytes) required for zero-copy `RecordHeader` and SIMD payloads.
pub const BUFFER_ALIGNMENT: usize = 64;

/// Largest capacity a 64-byte-aligned `Layout` can describe (`isize::MAX` rounded down to 64).
const MAX_CAP: usize = (isize::MAX as usize) & !(BUFFER_ALIGNMENT - 1);

/// Rounds `min` up to a non-zero multiple of 64, or `None` if no allocation could hold it.
#[inline]
fn aligned_cap(min: usize) -> Option<usize> {
    let cap = min
        .max(BUFFER_ALIGNMENT)
        .checked_next_multiple_of(BUFFER_ALIGNMENT)?;
    (cap <= MAX_CAP).then_some(cap)
}

/// Layout of a buffer whose capacity came from [`aligned_cap`] (or is clamped to [`MAX_CAP`]).
#[inline]
fn layout_for(cap: usize) -> Layout {
    let cap = cap.clamp(BUFFER_ALIGNMENT, MAX_CAP);
    // SAFETY: `cap` is clamped to `64..=MAX_CAP` and alignment 64 is a power of two, so the size
    // rounded up to the alignment never exceeds `isize::MAX`. Every capacity stored in a buffer
    // comes from `aligned_cap`, so it is already a multiple of 64 inside that range.
    unsafe { Layout::from_size_align_unchecked(cap, BUFFER_ALIGNMENT) }
}

/// Reports an allocation that cannot be satisfied the same way the standard library does
/// (process abort via the allocation-error handler, never a panic).
#[cold]
#[inline(never)]
fn alloc_failed(requested: usize) -> ! {
    std::alloc::handle_alloc_error(layout_for(requested))
}

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
    ///
    /// For engine-encoded (trusted) data. Out of memory aborts like the standard collections;
    /// use [`Self::try_copy_from_slice`] for sizes derived from network, disk or decompression.
    #[inline]
    pub fn copy_from_slice(src: &[u8]) -> Self {
        match Self::try_copy_from_slice(src) {
            Ok(bytes) => bytes,
            Err(_) => alloc_failed(src.len()),
        }
    }

    /// Fallible [`Self::copy_from_slice`]: returns `ResourceExhausted` instead of aborting when
    /// the allocation fails (`RULE-ERR-01`).
    #[inline]
    pub fn try_copy_from_slice(src: &[u8]) -> storage_types::Result<Self> {
        let oom = || storage_types::StorageError::resource_exhausted("heap memory (aligned buffer)");
        let cap = aligned_cap(src.len()).ok_or_else(oom)?;
        // SAFETY: `layout_for(cap)` has a non-zero size (`cap >= 64`).
        let raw = unsafe { alloc(layout_for(cap)) };
        let ptr = NonNull::new(raw).ok_or_else(oom)?;
        // SAFETY: `ptr` has at least `src.len()` valid writable bytes and does not overlap `src`.
        unsafe {
            core::ptr::copy_nonoverlapping(src.as_ptr(), ptr.as_ptr(), src.len());
            if cap > src.len() {
                core::ptr::write_bytes(ptr.as_ptr().add(src.len()), 0, cap - src.len());
            }
        }
        Ok(Self {
            inner: Arc::new(AlignedStorage {
                ptr,
                len: src.len(),
                cap,
            }),
        })
    }

    /// Allocates a zero-filled aligned buffer of `len` bytes, lets `fill` write it, then freezes
    /// it. Frames are built directly in their final buffer, with no intermediate `Vec` and no
    /// second copy (`TASK-087`). The buffer is freed if `fill` fails.
    pub fn try_build(
        len: usize,
        fill: impl FnOnce(&mut [u8]) -> storage_types::Result<()>,
    ) -> storage_types::Result<Self> {
        let oom = || storage_types::StorageError::resource_exhausted("heap memory (aligned buffer)");
        let cap = aligned_cap(len).ok_or_else(oom)?;
        // SAFETY: `layout_for(cap)` has a non-zero size (`cap >= 64`).
        let raw = unsafe { alloc(layout_for(cap)) };
        let ptr = NonNull::new(raw).ok_or_else(oom)?;
        // SAFETY: `ptr` is valid for `cap` writable bytes; zeroing makes all of them initialized.
        unsafe { core::ptr::write_bytes(ptr.as_ptr(), 0, cap) };
        // Owns the allocation from here on, so an error in `fill` frees it (via `Drop`).
        let storage = AlignedStorage { ptr, len, cap };
        // SAFETY: `ptr` is valid for `len <= cap` initialized bytes and uniquely owned by `storage`;
        // the mutable slice ends before `storage` is moved into the `Arc`.
        fill(unsafe { core::slice::from_raw_parts_mut(ptr.as_ptr(), len) })?;
        Ok(Self { inner: Arc::new(storage) })
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
        // SAFETY: `self.ptr` was allocated with `layout_for(self.cap)` in `try_copy_from_slice`.
        unsafe {
            dealloc(self.ptr.as_ptr(), layout_for(self.cap));
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
        let Some(cap) = aligned_cap(min_cap.max(4096)) else {
            alloc_failed(min_cap);
        };
        // SAFETY: `layout_for(cap)` has a non-zero size (`cap >= 4096`).
        let raw = unsafe { alloc(layout_for(cap)) };
        let Some(ptr) = NonNull::new(raw) else {
            alloc_failed(cap);
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
        match self.len.checked_add(additional) {
            Some(required) if required <= self.cap => {}
            Some(required) => self.grow_amortized(required),
            None => alloc_failed(usize::MAX),
        }
    }

    #[cold]
    #[inline(never)]
    fn grow_amortized(&mut self, required: usize) {
        let Some(new_cap) = aligned_cap(required.max(self.cap.saturating_mul(2).min(MAX_CAP)))
        else {
            alloc_failed(required);
        };
        // SAFETY: `self.ptr` was allocated with `layout_for(self.cap)`, and
        // `new_cap >= required > self.cap > 0` is a valid size for that alignment.
        let raw = unsafe { realloc(self.ptr.as_ptr(), layout_for(self.cap), new_cap) };
        let Some(ptr) = NonNull::new(raw) else {
            alloc_failed(new_cap);
        };
        self.ptr = ptr;
        self.cap = new_cap;
    }

    /// Pads `self.len` with zeroes up to a multiple of `align` (must be power of two `<= 64`).
    #[inline]
    pub fn align_to(&mut self, align: usize) {
        // Out-of-contract alignments are clamped to the nearest valid power of two (`1..=64`).
        let align = align.clamp(1, BUFFER_ALIGNMENT).next_power_of_two();
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
        // SAFETY: `self.ptr` was allocated (or reallocated) with `layout_for(self.cap)`.
        unsafe {
            dealloc(self.ptr.as_ptr(), layout_for(self.cap));
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
