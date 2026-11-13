//! Vector similarity search results (`TASK-027`, `F-OPS-05`); the scoring kernels live in
//! `storage_codec::vector`.

use storage_types::RecordOrdinal;

/// Result hit from a SIMD top-$K$ vector similarity search.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VectorTopKHit {
    pub ordinal: RecordOrdinal,
    pub score: f32,
}

/// Dot product and cosine similarity: in `storage-codec`, which picks an AVX2 + FMA copy at
/// run time on x86-64 builds without FMA (`storage_codec::vector`).
pub use storage_codec::vector::{cosine_similarity_f32, dot_product_f32};
