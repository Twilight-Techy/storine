//! Primary 64-shard index, `primary.idx` checkpointing, differential `Indexable` trait,
//! B+Tree numeric/timestamp range index, Roaring-style compressed bitmap tag index,
//! and SIMD vector similarity scorer (`#![forbid(unsafe_code)]`).

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

pub mod bitmap;
pub mod btree;
pub mod checkpoint;
pub mod hnsw;
pub mod cold_primary;
pub mod paged_bitmap;
mod paged_files;
pub mod paged_btree;
pub mod primary;
pub mod quantize;
pub mod tiered;
pub mod traits;
pub mod vector;

#[cfg(test)]
mod hostile_tests;

pub use bitmap::{BitmapTagIndex, CompressedBitmap};
pub use btree::{
    sortable_f64_to_i64, sortable_u64_to_i64, KeysetCursor, NumericRangeIndex, SortOrder,
};
pub use checkpoint::{
    append_primary_checkpoint_delta, delta_checkpoint_path, load_primary_checkpoint,
    save_primary_checkpoint, save_primary_checkpoint_entries, load_primary_checkpoint_with_stamp,
};
pub use hnsw::{HnswConfig, HnswVectorIndex};
pub use cold_primary::{
    BloomCacheBlock, ColdPageSeparator, ColdPrimaryEntry, ColdPrimaryIndex, SplitBlockBloomFilter,
};
pub use paged_bitmap::{MappedChunkRef, PagedBitmapIndex, PendingBitmapCheckpoint};
pub use paged_btree::{
    PagedBTreeIndex, PendingBTreeCheckpoint, SeparatorEntry, BTREE_PAGE_SIZE, MAX_ENTRIES_PER_LEAF_PAGE,
};
pub use primary::{DiskLocation, PrimaryEntry, PrimaryIndex};
pub use quantize::{dot_u8_unrolled, sq8_cosine_similarity, sq8_dot_product, QuantizedVecSq8};
pub use tiered::{RangeIndex, TagIndex};
pub use traits::{IndexDelta, IndexSnapshot, Indexable};
pub use vector::{cosine_similarity_f32, dot_product_f32, VectorTopKHit};
