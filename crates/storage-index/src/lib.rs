//! Primary 64-shard index, `primary.idx` checkpointing, differential `Indexable` trait,
//! B+Tree numeric/timestamp range index, Roaring-style compressed bitmap tag index,
//! and SIMD vector similarity scorer (`#![forbid(unsafe_code)]`).

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

pub mod bitmap;
pub mod btree;
pub mod checkpoint;
pub mod primary;
pub mod traits;
pub mod vector;

pub use bitmap::{BitmapTagIndex, CompressedBitmap};
pub use btree::NumericRangeIndex;
pub use checkpoint::{load_primary_checkpoint, save_primary_checkpoint};
pub use primary::{DiskLocation, PrimaryEntry, PrimaryIndex};
pub use traits::{IndexDelta, IndexSnapshot, Indexable};
pub use vector::{cosine_similarity_f32, dot_product_f32, VectorTopKHit};
