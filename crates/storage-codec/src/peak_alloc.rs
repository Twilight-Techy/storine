//! An allocator for fuzz tests (`TASK-135`, `RULE-SAFE-01`): the system's, noting the largest single
//! request a thread makes while it measures, to catch a length field trusted before its bytes
//! arrived. Install it in a test binary with `#[global_allocator]`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub struct PeakAlloc;

thread_local! {
    static MEASURING: Cell<bool> = const { Cell::new(false) };
    static LARGEST: Cell<usize> = const { Cell::new(0) };
}

fn note(size: usize) {
    if MEASURING.try_with(Cell::get).unwrap_or(false) {
        let _ = LARGEST.try_with(|l| l.set(l.get().max(size)));
    }
}

// SAFETY: every call is forwarded unchanged to `System`; `note` only reads and writes
// const-initialized thread-locals, which never allocate.
unsafe impl GlobalAlloc for PeakAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        // SAFETY: the caller's contract is `System`'s.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        // SAFETY: the caller's contract is `System`'s.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract is `System`'s.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note(new_size);
        // SAFETY: the caller's contract is `System`'s.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// Runs `f`; its result and the largest single allocation this thread asked for meanwhile.
pub fn measure<T>(f: impl FnOnce() -> T) -> (T, usize) {
    LARGEST.with(|l| l.set(0));
    MEASURING.with(|m| m.set(true));
    let out = f();
    MEASURING.with(|m| m.set(false));
    (out, LARGEST.with(Cell::get))
}

/// The same across every thread, for a test whose hostile input reaches a server's threads.
#[derive(Debug)]
pub struct ProcessPeakAlloc;

static PROCESS_LARGEST: AtomicUsize = AtomicUsize::new(0);

/// The largest single allocation any thread has asked for of a `ProcessPeakAlloc`.
pub fn process_largest() -> usize {
    PROCESS_LARGEST.load(Ordering::Relaxed)
}

// SAFETY: every call is forwarded unchanged to `System`; the bookkeeping is one atomic.
unsafe impl GlobalAlloc for ProcessPeakAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        PROCESS_LARGEST.fetch_max(layout.size(), Ordering::Relaxed);
        // SAFETY: the caller's contract is `System`'s.
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        PROCESS_LARGEST.fetch_max(layout.size(), Ordering::Relaxed);
        // SAFETY: the caller's contract is `System`'s.
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract is `System`'s.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        PROCESS_LARGEST.fetch_max(new_size, Ordering::Relaxed);
        // SAFETY: the caller's contract is `System`'s.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}
