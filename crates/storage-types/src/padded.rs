//! Cache-line alignment wrapper (`RULE-CPU-01`) to eliminate false sharing.

use core::ops::{Deref, DerefMut};

/// Pads and aligns `T` to a 64-byte CPU cache line boundary so concurrent threads
/// mutating adjacent slots in an array or struct never invalidate each other's L1/L2 cache lines.
#[repr(C, align(64))]
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct CachePadded<T> {
    value: T,
}

impl<T> CachePadded<T> {
    #[inline]
    pub const fn new(value: T) -> Self {
        Self { value }
    }

    #[inline]
    pub fn into_inner(self) -> T {
        self.value
    }
}

impl<T> Deref for CachePadded<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> DerefMut for CachePadded<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
