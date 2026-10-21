//! Zero-copy archived relationship references (`TASK-020`, `F-REL-01`).
//!
//! These `#[repr(C)]` types are what record roots store on disk; the owned, mutable
//! `OneToOne` / `OneToMany` / `ManyToMany` containers live in `storage-graph`. They are defined
//! here so their `ArchivePod` impls stay inside the crate that is allowed to contain `unsafe`.

use core::fmt;
use core::marker::PhantomData;
use storage_types::{InlineId, Result, StorageError};

use crate::archive::{ArchivePod, ArchivedSlice};

/// Zero-copy archived `1:1` reference (`#[repr(C)]`, 24 bytes).
/// Uses `InlineId::EMPTY` (`len == 0`) as the `None` niche.
#[repr(C)]
pub struct ArchivedOneToOne<Target> {
    pub raw: InlineId,
    _marker: PhantomData<Target>,
}

impl<Target> Clone for ArchivedOneToOne<Target> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<Target> Copy for ArchivedOneToOne<Target> {}

// SAFETY: a single `InlineId` (24 bytes, no padding, every bit pattern valid) plus a ZST marker;
// the layout does not depend on `Target`.
unsafe impl<Target: Send + Sync + 'static> ArchivePod for ArchivedOneToOne<Target> {}

impl<Target> fmt::Debug for ArchivedOneToOne<Target> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArchivedOneToOne({:?})", self.get())
    }
}

impl<Target> ArchivedOneToOne<Target> {
    #[inline]
    pub const fn new(target: Option<InlineId>) -> Self {
        let raw = match target {
            Some(id) => id,
            None => InlineId::EMPTY,
        };
        Self {
            raw,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub const fn get(&self) -> Option<InlineId> {
        if self.raw.is_empty() {
            None
        } else {
            Some(self.raw)
        }
    }

    #[inline]
    pub fn validate(&self) -> Result<()> {
        if !self.raw.is_empty() {
            InlineId::from_raw_bytes(self.raw.to_raw_bytes())?;
        }
        Ok(())
    }
}

/// Zero-copy archived `1:N` / `M:N` id list (`#[repr(C)]`, 8 bytes), sorted and deduplicated so
/// membership checks are $O(\log K)$ binary searches directly over the mapped payload.
#[repr(C)]
pub struct ArchivedOneToMany<Target> {
    pub slice: ArchivedSlice<InlineId>,
    _marker: PhantomData<Target>,
}

/// Zero-copy archived `M:N` id list.
pub type ArchivedManyToMany<Target> = ArchivedOneToMany<Target>;

impl<Target> Clone for ArchivedOneToMany<Target> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<Target> Copy for ArchivedOneToMany<Target> {}

// SAFETY: a single `ArchivedSlice` (two `u32`, 8 bytes, no padding) plus a ZST marker; the
// layout does not depend on `Target`.
unsafe impl<Target: Send + Sync + 'static> ArchivePod for ArchivedOneToMany<Target> {}

impl<Target> fmt::Debug for ArchivedOneToMany<Target> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArchivedOneToMany(len={})", self.slice.len())
    }
}

impl<Target> ArchivedOneToMany<Target> {
    #[inline]
    pub const fn new(slice: ArchivedSlice<InlineId>) -> Self {
        Self {
            slice,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub const fn len(&self) -> usize {
        self.slice.len()
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.slice.is_empty()
    }

    /// Returns a zero-copy slice `&'a [InlineId]` directly from the `mmap` payload (`0` allocations).
    #[inline]
    pub fn as_slice<'a>(&self, payload: &'a [u8]) -> &'a [InlineId] {
        self.slice.as_slice(payload)
    }

    /// $O(\log K)$ binary search membership check directly over the zero-copy `mmap` slice (`0` allocations).
    #[inline]
    pub fn contains(&self, payload: &[u8], id: InlineId) -> bool {
        self.as_slice(payload).binary_search(&id).is_ok()
    }

    /// Validates bounds, every `InlineId`, and the strictly-ascending order that `contains`
    /// (binary search) and unarchiving rely on.
    #[inline]
    pub fn validate(&self, payload: &[u8]) -> Result<()> {
        self.slice.validate(payload)?;
        let ids = self.slice.as_slice(payload);
        for &id in ids {
            InlineId::from_raw_bytes(id.to_raw_bytes())?;
        }
        if ids.windows(2).any(|w| w[0] >= w[1]) {
            return Err(StorageError::invalid_archive(
                "relationship id list is not sorted and deduplicated",
            ));
        }
        Ok(())
    }
}
