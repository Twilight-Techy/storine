//! Zero-copy relationship containers (`OneToOne`, `OneToMany`, `ManyToMany`) (`TASK-020`, `F-REL-01`).
//!
//! Relationships are stored explicitly inside parent and child records without join tables.
//! In their archived zero-copy form (`ArchivedOneToMany`, `ArchivedManyToMany`, defined in
//! `storage-codec`), IDs are stored in sorted order inside the record's `mmap` payload so
//! membership checks (`contains`) run via $O(\log K)$ binary search with **zero heap allocations**.

use core::fmt;
use core::marker::PhantomData;
use storage_codec::{ArchiveBuilder, ArchivedOneToMany, ArchivedOneToOne};
use storage_types::InlineId;

/// Owned `1:1` explicit relationship reference to a record of type `Target`.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct OneToOne<Target> {
    target: Option<InlineId>,
    _marker: PhantomData<Target>,
}

impl<Target> fmt::Debug for OneToOne<Target> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OneToOne({:?})", self.target)
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
        ArchivedOneToOne::new(self.target)
    }

    /// Reconstructs the owned reference from its archived form.
    #[inline]
    pub const fn from_archived(archived: &ArchivedOneToOne<Target>) -> Self {
        Self::new(archived.get())
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
        ArchivedOneToMany::new(builder.write_slice(&self.ids))
    }

    /// Reconstructs the owned list from its archived form. The ids are re-sorted and
    /// deduplicated, so the sorted-list invariant holds even for unvalidated input.
    #[inline]
    pub fn from_archived(archived: &ArchivedOneToMany<Target>, payload: &[u8]) -> Self {
        Self::from_iter_sorted(archived.as_slice(payload).iter().copied())
    }
}

/// Owned `M:N` explicit relationship list (symmetric peer set).
pub type ManyToMany<Target> = OneToMany<Target>;
