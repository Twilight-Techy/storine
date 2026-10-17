//! Cross-platform memory-mapped I/O (`mmap` / `MapViewOfFile`), bounded `MmapPool` with
//! Clock/TinyLFU eviction, zero-allocation stack path resolution, Ping-Pong double-buffered
//! record slots, and 64 MB Slotted Arena Segments.

#![deny(unsafe_op_in_unsafe_fn)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

pub mod arena;
pub mod net_poll;
pub mod path;
pub mod persist;
pub mod pool;
pub mod raw;

pub use arena::{ArenaLocation, ArenaManager, ArenaSegment, DEFAULT_SEGMENT_CAPACITY};
pub use net_poll::SocketPoller;
pub use path::{PathResolver, StackRelPath};
pub use persist::{
    atomic_write_replace, cleanup_temp_files, read_ping_pong_active, write_ping_pong_slot,
    DirectoryLock,
};
pub use pool::{MmapGuard, MmapPool, MmapPoolConfig, MmapPoolStats};
pub use raw::{Advice, MmapRegion};
