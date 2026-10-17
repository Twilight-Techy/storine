//! Zero-copy relationship containers (`OneToOne`, `OneToMany`, `ManyToMany`) (`TASK-020`, `F-REL-01`).
//!
//! Relationships are stored explicitly inside parent and child records without join tables.
//! In their archived zero-copy form (`ArchivedOneToMany`, `ArchivedManyToMany`), IDs are stored
//! in sorted order inside the record's `mmap` payload so membership checks (`contains`) run via
//! $O(\log K)$ binary search with **zero heap allocations**.

use core::fmt;
use core::marker::PhantomData;
use storage_codec::{ArchiveBuilder, ArchivedSlice};
use storage_types::{InlineId, Result};

/// Owned `1:1` explicit relationship reference to a record of type `Target`.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct OneToOne<Target> {
    target: Option<InlineId>,
    _marker: PhantomData<Target>,
}

/// Zero-copy archived representation of `OneToOne<Target>` (`#[repr(C)]`, 24 bytes).
/// Uses `InlineId::EMPTY` (`len == 0`) as the `None` niche, saving 8 bytes over `ArchivedOption<InlineId>`.
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

impl<Target> fmt::Debug for OneToOne<Target> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OneToOne({:?})", self.target)
    }
}

impl<Target> fmt::Debug for ArchivedOneToOne<Target> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArchivedOneToOne({:?})", self.get())
    }
}

impl<Target> OneToOne<Target> {
    pub const NONE: Self = Self {
        target: None,
        _marker: PhantomData,
    };

    #[inline]
    pub const fn new(target: Option<InlineId>) -> Self {
        Self {
            target,
            _marker: PhantomData,
        }
    }

    #[inline]
    pub const fn some(id: InlineId) -> Self {
        Self::new(Some(id))
    }

    #[inline]
    pub const fn get(&self) -> Option<InlineId> {
        self.target
    }

    #[inline]
    pub fn set(&mut self, target: Option<InlineId>) {
        self.target = target;
    }

    #[inline]
    pub fn clear_if_matches(&mut self, id: InlineId) -> bool {
        if self.target == Some(id) {
            self.target = None;
            true
        } else {
            false
        }
    }

    #[inline]
    pub const fn archive(&self) -> ArchivedOneToOne<Target> {
        let raw = match self.target {
            Some(id) => id,
            None => InlineId::EMPTY,
        };
        ArchivedOneToOne {
            raw,
            _marker: PhantomData,
        }
    }
}

impl<Target> ArchivedOneToOne<Target> {
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

    #[inline]
    pub const fn unarchive(&self) -> OneToOne<Target> {
        OneToOne::new(self.get())
    }
}

/// Owned `1:N` explicit relationship list storing sorted, deduplicated child `InlineId`s.
#[derive(Clone, PartialEq, Eq)]
pub struct OneToMany<Target> {
    ids: Vec<InlineId>,
    _marker: PhantomData<Target>,
}

impl<Target> Default for OneToMany<Target> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl<Target> fmt::Debug for OneToMany<Target> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OneToMany({:?})", self.ids)
    }
}

/// Zero-copy archived representation of `OneToMany<Target>` (`#[repr(C)]`, 8 bytes).
#[repr(C)]
pub struct ArchivedOneToMany<Target> {
    pub slice: ArchivedSlice<InlineId>,
    _marker: PhantomData<Target>,
}

impl<Target> Clone for ArchivedOneToMany<Target> {
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}

impl<Target> Copy for ArchivedOneToMany<Target> {}

impl<Target> fmt::Debug for ArchivedOneToMany<Target> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArchivedOneToMany(len={})", self.slice.len())
    }
}

impl<Target> OneToMany<Target> {
    #[inline]
    pub const fn new() -> Self {
        Self {
            ids: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Creates a sorted, deduplicated `OneToMany` list from an iterator of `InlineId`s.
    pub fn from_iter_sorted(iter: impl IntoIterator<Item = InlineId>) -> Self {
        let mut ids: Vec<InlineId> = iter.into_iter().collect();
        ids.sort_unstable();
        ids.dedup();
        Self {
            ids,
            _marker: PhantomData,
        }
    }

    /// Inserts `id` in sorted order if not already present. Returns `true` if newly added.
    #[inline]
    pub fn link(&mut self, id: InlineId) -> bool {
        match self.ids.binary_search(&id) {
            Ok(_) => false,
            Err(pos) => {
                self.ids.insert(pos, id);
                true
            }
        }
    }

    /// Removes `id` in $O(\log K)$ binary search. Returns `true` if present.
    #[inline]
    pub fn unlink(&mut self, id: InlineId) -> bool {
        match self.ids.binary_search(&id) {
            Ok(pos) => {
                self.ids.remove(pos);
                true
            }
            Err(_) => false,
        }
    }

    #[inline]
    pub fn contains(&self, id: InlineId) -> bool {
        self.ids.binary_search(&id).is_ok()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    #[inline]
    pub fn as_slice(&self) -> &[InlineId] {
        &self.ids
    }

    #[inline]
    pub fn archive(&self, builder: &mut ArchiveBuilder<'_>) -> ArchivedOneToMany<Target> {
        ArchivedOneToMany {
            slice: builder.write_slice(&self.ids),
            _marker: PhantomData,
        }
    }
}

impl<Target> ArchivedOneToMany<Target> {
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

    #[inline]
    pub fn validate(&self, payload: &[u8]) -> Result<()> {
        self.slice.validate(payload)?;
        for &id in self.slice.as_slice(payload) {
            InlineId::from_raw_bytes(id.to_raw_bytes())?;
        }
        Ok(())
    }

    #[inline]
    pub fn unarchive(&self, payload: &[u8]) -> OneToMany<Target> {
        OneToMany {
            ids: self.as_slice(payload).to_vec(),
            _marker: PhantomData,
        }
    }
}

/// Owned `M:N` explicit relationship list (symmetric peer set).
pub type ManyToMany<Target> = OneToMany<Target>;

/// Zero-copy archived representation of `ManyToMany<Target>`.
pub type ArchivedManyToMany<Target> = ArchivedOneToMany<Target>;
