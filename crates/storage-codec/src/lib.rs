//! Zero-copy relative-offset archive codec, SIMD-accelerated `XXH3-64` and `CRC32C`
//! checksums, 64-byte-aligned pooled buffers, and fast LZ4 block compression.

#![deny(unsafe_op_in_unsafe_fn)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

pub mod archive;
pub mod checksum;
pub mod compress;
pub mod scratch;

pub use archive::{
    ArchiveBuilder, ArchivedBytes, ArchivedOption, ArchivedSlice, ArchivedString, RecordCodec,
    RecordView, ZeroCopyRecord,
};
pub use checksum::{checksum_payload, crc32c_64, xxh3_64};
pub use compress::{compress_lz4, decompress_lz4};
pub use scratch::{AlignedBytes, AlignedScratchBuf, with_thread_scratch};
