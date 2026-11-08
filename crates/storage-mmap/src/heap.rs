//! Returning freed heap memory to the operating system.
//!
//! glibc's allocator keeps memory a program has freed, in every thread's arena, for reuse. A
//! server whose threads allocate and free in bursts (request bodies, Raft batches, snapshots)
//! can then sit at gigabytes of resident memory while it holds a tenth of that: the range-mode
//! soak measured 30–110 MB allocated against 1.2–1.9 GB resident. `malloc_trim(0)` gives the
//! free pages of all arenas back.

/// Gives freed heap memory back to the operating system where the allocator supports it
/// (glibc); `true` if any was released. Elsewhere a no-op returning `false`.
///
/// It walks every arena under that arena's lock, so allocations wait for it briefly: call it
/// every few seconds from a background thread, not on a request path.
pub fn release_free_heap() -> bool {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        extern "C" {
            fn malloc_trim(pad: usize) -> i32;
        }
        // SAFETY: `malloc_trim` takes no pointers and only touches the allocator's own state,
        // under its own locks; any thread may call it at any time.
        unsafe { malloc_trim(0) != 0 }
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    {
        false
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn freed_memory_can_be_released() {
        // Free 200 MB of small blocks, then trim: it must not crash, and on glibc
        // it finds something to give back.
        let blocks: Vec<Vec<u8>> = (0..200_000).map(|i| vec![i as u8; 1024]).collect();
        drop(blocks);
        let released = super::release_free_heap();
        if cfg!(all(target_os = "linux", target_env = "gnu")) {
            assert!(released, "glibc released nothing after 200 MB were freed");
        }
    }
}
