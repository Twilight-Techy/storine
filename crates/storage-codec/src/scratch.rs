//! 64-byte cache-line-aligned scratch buffers and immutable `AlignedBytes` (`TASK-006`, `RULE-MEM-02`).

use std::alloc::{alloc, dealloc, realloc, Layout};
use std::cell::RefCell;
use std::fmt;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicUsize, Ordering};

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
///
/// One allocation holds both the reference count and the bytes: a 64-byte [`BlockHeader`]
/// followed by the data (so the data stays 64-byte aligned). Every write allocates one of these,
/// so a separate `Arc` allocation would double the allocator work on the write path.
pub struct AlignedBytes {
    block: NonNull<BlockHeader>,
}

/// Start of an [`AlignedBytes`] block; the data begins [`BUFFER_ALIGNMENT`] bytes after it.
#[repr(C, align(64))]
struct BlockHeader {
    refs: AtomicUsize,
    len: usize,
    /// Data capacity (a multiple of 64); the block is `BUFFER_ALIGNMENT + cap` bytes.
    cap: usize,
}

const _: () = assert!(core::mem::size_of::<BlockHeader>() == BUFFER_ALIGNMENT);

// SAFETY: the data is immutable once built and the reference count is atomic, exactly as for
// `Arc<[u8]>`; nothing is ever mutated through a shared `AlignedBytes`.
unsafe impl Send for AlignedBytes {}
// SAFETY: shared references only expose `&[u8]`.
unsafe impl Sync for AlignedBytes {}

impl AlignedBytes {
    /// Allocates an uninitialized block for `cap` data bytes, with one reference: from the
    /// frame pool when its size class has one, otherwise from the allocator.
    #[inline]
    fn allocate(len: usize, cap: usize) -> Option<NonNull<BlockHeader>> {
        let total = cap.checked_add(BUFFER_ALIGNMENT)?;
        if total > MAX_CAP {
            return None;
        }
        let block = match frame_pool::take(cap) {
            Some(block) => block,
            None => {
                // SAFETY: `layout_for(total)` has a non-zero size (`total >= 128`).
                let raw = unsafe { alloc(layout_for(total)) }.cast::<BlockHeader>();
                NonNull::new(raw)?
            }
        };
        // SAFETY: `block` is valid for writes of a `BlockHeader` and suitably aligned (64); a
        // pooled block is owned by nobody else once taken.
        unsafe { block.as_ptr().write(BlockHeader { refs: AtomicUsize::new(1), len, cap }) };
        Some(block)
    }

    #[inline]
    fn data_ptr(block: NonNull<BlockHeader>) -> *mut u8 {
        // SAFETY: the data starts `BUFFER_ALIGNMENT` bytes into a block at least that long.
        unsafe { block.as_ptr().cast::<u8>().add(BUFFER_ALIGNMENT) }
    }

    #[inline]
    fn header(&self) -> &BlockHeader {
        // SAFETY: `block` points to a live, initialized header while any reference exists.
        unsafe { self.block.as_ref() }
    }

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
        let block = Self::allocate(src.len(), cap).ok_or_else(oom)?;
        let data = Self::data_ptr(block);
        // SAFETY: `data` has `cap >= src.len()` writable bytes and does not overlap `src`.
        unsafe {
            core::ptr::copy_nonoverlapping(src.as_ptr(), data, src.len());
            if cap > src.len() {
                core::ptr::write_bytes(data.add(src.len()), 0, cap - src.len());
            }
        }
        Ok(Self { block })
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
        let block = Self::allocate(len, cap).ok_or_else(oom)?;
        let data = Self::data_ptr(block);
        // SAFETY: `data` is valid for `cap` writable bytes; zeroing makes all of them initialized.
        unsafe { core::ptr::write_bytes(data, 0, cap) };
        // Owns the block from here on, so an error in `fill` frees it (via `Drop`).
        let bytes = Self { block };
        // SAFETY: `data` is valid for `len <= cap` initialized bytes and `bytes` is the only
        // reference; the mutable slice ends before `bytes` is returned or shared.
        fill(unsafe { core::slice::from_raw_parts_mut(data, len) })?;
        Ok(bytes)
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: the data is valid for `len` initialized bytes for the lifetime of `self`.
        unsafe { core::slice::from_raw_parts(Self::data_ptr(self.block), self.header().len) }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.header().len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.header().len == 0
    }
}

impl Clone for AlignedBytes {
    #[inline]
    fn clone(&self) -> Self {
        // Relaxed, as `Arc::clone`: a new reference can only come from an existing one.
        let old = self.header().refs.fetch_add(1, Ordering::Relaxed);
        if old > isize::MAX as usize {
            std::process::abort();
        }
        Self { block: self.block }
    }
}

impl Drop for AlignedBytes {
    #[inline]
    fn drop(&mut self) {
        if self.header().refs.fetch_sub(1, Ordering::Release) != 1 {
            return;
        }
        // Synchronizes with every earlier release, as `Arc::drop`, before the memory is reused.
        std::sync::atomic::fence(Ordering::Acquire);
        let cap = self.header().cap;
        // SAFETY: this was the last reference, so the block is no longer reachable.
        unsafe { frame_pool::give(self.block, cap) };
    }
}

/// Recycles [`AlignedBytes`] blocks of up to [`frame_pool::MAX_POOLED_CAP`] bytes.
///
/// Every write allocates a frame on the writing thread, and the background flusher frees it.
/// Through the allocator that is a cross-thread free into the writer's arena, and the two
/// threads then serialize on that arena's lock. Here each thread keeps a small cache per size
/// class, and caches trade blocks in groups through one shared store per class, so a lock is
/// taken once per [`frame_pool::GROUP`] blocks. The shared store is bounded
/// ([`frame_pool::MAX_SHARED_BYTES`] per class); blocks beyond it go back to the allocator.
mod frame_pool {
    use super::{dealloc, layout_for, BlockHeader, BUFFER_ALIGNMENT};
    use std::cell::RefCell;
    use std::ptr::NonNull;
    use std::sync::Mutex;

    /// Largest pooled data capacity (one class per 64 bytes up to this).
    pub(super) const MAX_POOLED_CAP: usize = 4096;
    const CLASSES: usize = MAX_POOLED_CAP / BUFFER_ALIGNMENT;
    /// Blocks moved between a thread cache and the shared store at a time.
    pub(super) const GROUP: usize = 64;
    /// A thread cache holds at most this many blocks per class before giving a group back.
    const LOCAL_MAX: usize = 2 * GROUP;
    /// Bytes the shared store keeps per class; the rest is freed.
    pub(super) const MAX_SHARED_BYTES: usize = 16 << 20;

    /// A free block (raw, uninitialized). Only ever owned by one cache or store at a time.
    struct FreeBlock(NonNull<BlockHeader>);
    // SAFETY: a free block is plain memory owned by whichever cache or store holds it.
    unsafe impl Send for FreeBlock {}

    static SHARED: [Mutex<Vec<FreeBlock>>; CLASSES] = [const { Mutex::new(Vec::new()) }; CLASSES];

    struct Local {
        classes: [Vec<FreeBlock>; CLASSES],
    }

    impl Drop for Local {
        fn drop(&mut self) {
            for (class, blocks) in self.classes.iter_mut().enumerate() {
                for b in blocks.drain(..) {
                    // SAFETY: pooled blocks of `class` hold `(class + 1) * 64` data bytes and
                    // were allocated with `layout_for(cap + BUFFER_ALIGNMENT)`.
                    unsafe { free_block(b.0, cap_of(class)) };
                }
            }
        }
    }

    thread_local! {
        static LOCAL: RefCell<Local> = const { RefCell::new(Local { classes: [const { Vec::new() }; CLASSES] }) };
    }

    #[inline]
    fn class_of(cap: usize) -> Option<usize> {
        ((BUFFER_ALIGNMENT..=MAX_POOLED_CAP).contains(&cap) && cap % BUFFER_ALIGNMENT == 0).then(|| cap / BUFFER_ALIGNMENT - 1)
    }

    #[inline]
    fn cap_of(class: usize) -> usize {
        (class + 1) * BUFFER_ALIGNMENT
    }

    /// # Safety
    /// `block` must be unreachable and allocated with `layout_for(cap + BUFFER_ALIGNMENT)`.
    unsafe fn free_block(block: NonNull<BlockHeader>, cap: usize) {
        // SAFETY: guaranteed by the caller.
        unsafe { dealloc(block.as_ptr().cast::<u8>(), layout_for(cap + BUFFER_ALIGNMENT)) };
    }

    /// A free block for data capacity `cap`, if the pool has one.
    #[inline]
    pub(super) fn take(cap: usize) -> Option<NonNull<BlockHeader>> {
        let class = class_of(cap)?;
        LOCAL
            .try_with(|l| {
                let mut l = l.try_borrow_mut().ok()?;
                let local = &mut l.classes[class];
                if local.is_empty() {
                    let mut shared = SHARED[class].lock().unwrap_or_else(|p| p.into_inner());
                    let n = shared.len().min(GROUP);
                    let from = shared.len() - n;
                    local.extend(shared.drain(from..));
                }
                local.pop().map(|b| b.0)
            })
            .ok()
            .flatten()
    }

    /// Returns an unreachable block to the pool (or frees it).
    ///
    /// # Safety
    /// `block` must be unreachable and allocated with `layout_for(cap + BUFFER_ALIGNMENT)`.
    #[inline]
    pub(super) unsafe fn give(block: NonNull<BlockHeader>, cap: usize) {
        let Some(class) = class_of(cap) else {
            // SAFETY: guaranteed by the caller.
            unsafe { free_block(block, cap) };
            return;
        };
        let kept = LOCAL
            .try_with(|l| {
                let Ok(mut l) = l.try_borrow_mut() else { return false };
                let local = &mut l.classes[class];
                local.push(FreeBlock(block));
                if local.len() > LOCAL_MAX {
                    let group: Vec<FreeBlock> = local.drain(local.len() - GROUP..).collect();
                    let mut shared = SHARED[class].lock().unwrap_or_else(|p| p.into_inner());
                    let room = (MAX_SHARED_BYTES / (cap + BUFFER_ALIGNMENT)).saturating_sub(shared.len());
                    let mut group = group.into_iter();
                    shared.extend(group.by_ref().take(room));
                    drop(shared);
                    for b in group {
                        // SAFETY: each pooled block of this class was allocated for `cap`.
                        unsafe { free_block(b.0, cap) };
                    }
                }
                true
            })
            .unwrap_or(false);
        if !kept {
            // Thread-local storage is gone (thread exiting) or busy: free it directly.
            // SAFETY: guaranteed by the caller.
            unsafe { free_block(block, cap) };
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
