//! Core zero-allocation data types, 24-byte stack-inlined `InlineId`,
//! 64-byte cache-line-aligned `RecordHeader`, and error definitions.

#![deny(unsafe_op_in_unsafe_fn)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

pub mod error;
pub mod header;
pub mod id;
pub mod padded;

pub use error::{Result, StorageError};
pub use header::{
    RecordFlags, RecordHeader, HEADER_SIZE, MAX_PAYLOAD_SIZE, RECORD_MAGIC, WIRE_FORMAT_VERSION,
};
pub use id::{InlineId, MAX_INLINE_ID_LEN};
pub use padded::CachePadded;

/// Dense 32-bit internal ordinal assigned to each record for compact secondary
/// index storage (B+Tree leaves and compressed Roaring-style bitmaps).
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct RecordOrdinal(pub u32);

impl RecordOrdinal {
    pub const INVALID: Self = Self(u32::MAX);

    #[inline]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    #[inline]
    pub const fn as_u32(self) -> u32 {
        self.0
    }

    #[inline]
    pub const fn as_usize(self) -> usize {
        self.0 as usize
    }

    #[inline]
    pub const fn is_valid(self) -> bool {
        self.0 != u32::MAX
    }
}

/// Monotonically increasing 64-bit Optimistic Concurrency Control (OCC) version.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Version(pub u64);

impl Version {
    pub const INITIAL: Self = Self(1);

    #[inline]
    pub const fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }

    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

/// Zero-overhead deterministic 64-bit pass-through/mixing `Hasher` for `InlineId`, `RecordOrdinal`, and `u16`/`u32` keys.
///
/// Eliminates `SipHash-1-3` overhead and random-state initialization across all hot hash tables.
#[derive(Clone, Copy, Debug, Default)]
pub struct FastHasher(u64);

impl core::hash::Hasher for FastHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut h = self.0 ^ 0x9E37_79B1_85EB_CA87;
        for &b in bytes {
            h = (h ^ u64::from(b)).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
        }
        self.0 = h ^ (h >> 29);
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        // Rotate by 13 bits so the bits of `InlineId::fast_hash()` used for 64-shard routing (`bits 0..6`)
        // are decorrelated from SwissTable's intra-shard `h1` bucket index and `h2` SIMD control tag.
        let mixed = i.rotate_right(13);
        self.0 = if self.0 == 0 {
            mixed
        } else {
            (self.0.rotate_left(21) ^ mixed).wrapping_mul(0x9E37_79B1_85EB_CA87)
        };
    }

    #[inline]
    fn write_u32(&mut self, i: u32) {
        let m = u64::from(i).wrapping_mul(0x9E37_79B1_85EB_CA87);
        self.0 = if self.0 == 0 {
            m ^ (m >> 29)
        } else {
            (self.0 ^ m).wrapping_mul(0xC2B2_AE3D_27D4_EB4F)
        };
    }

    #[inline]
    fn write_u16(&mut self, i: u16) {
        self.write_u32(u32::from(i));
    }
}

/// Zero-cost `BuildHasherDefault<FastHasher>` for internal hash tables.
pub type FastBuildHasher = core::hash::BuildHasherDefault<FastHasher>;

/// High-speed `HashMap` backed by `FastBuildHasher`.
pub type FastHashMap<K, V> = std::collections::HashMap<K, V, FastBuildHasher>;

/// High-speed `HashSet` backed by `FastBuildHasher`.
pub type FastHashSet<K> = std::collections::HashSet<K, FastBuildHasher>;

// Compile-time struct layout assertions (RULE-MEM-03)
const _: () = {
    assert!(core::mem::size_of::<InlineId>() == 24);
    assert!(core::mem::align_of::<InlineId>() == 8);
    assert!(core::mem::size_of::<RecordHeader>() == 64);
    assert!(core::mem::align_of::<RecordHeader>() == 64);
    assert!(core::mem::size_of::<RecordOrdinal>() == 4);
    assert!(core::mem::size_of::<Version>() == 8);
};

