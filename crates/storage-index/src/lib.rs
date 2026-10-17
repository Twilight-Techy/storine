//! Primary 64-shard index, `primary.idx` checkpointing, differential `Indexable` trait,
//! B+Tree numeric/timestamp range index, Roaring-style compressed bitmap tag index,
//! and SIMD vector similarity scorer (`#![forbid(unsafe_code)]`).

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

pub mod bitmap;
pub mod btree;
pub mod checkpoint;
pub mod hnsw;
pub mod paged_bitmap;
pub mod paged_btree;
pub mod primary;
pub mod quantize;
pub mod traits;
pub mod vector;

pub use bitmap::{BitmapTagIndex, CompressedBitmap};
pub use btree::{sortable_f64_to_i64, sortable_u64_to_i64, NumericRangeIndex};
pub use checkpoint::{load_primary_checkpoint, save_primary_checkpoint};
pub use hnsw::{HnswConfig, HnswVectorIndex};
pub use paged_bitmap::{
    BloomCacheBlock, ColdPageSeparator, ColdPrimaryEntry, ColdPrimaryIndex, MappedChunkRef,
    PagedBitmapIndex, SplitBlockBloomFilter,
};
pub use paged_btree::{PagedBTreeIndex, SeparatorEntry, BTREE_PAGE_SIZE, MAX_ENTRIES_PER_LEAF_PAGE};
pub use primary::{DiskLocation, PrimaryEntry, PrimaryIndex};
pub use quantize::{dot_u8_unrolled, sq8_cosine_similarity, sq8_dot_product, QuantizedVecSq8};
pub use traits::{IndexDelta, IndexSnapshot, Indexable};
pub use vector::{cosine_similarity_f32, dot_product_f32, VectorTopKHit};
