//! Explicit `1:1`, `1:N`, and `M:N` zero-copy relationship containers, canonical deadlock-free
//! bidirectional link ordering, and `Restrict`/`Cascade`/`Unlink` referential integrity (`#![forbid(unsafe_code)]`).

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

pub mod containers;
pub mod integrity;

pub use containers::{ManyToMany, OneToMany, OneToOne};
pub use storage_codec::{ArchivedManyToMany, ArchivedOneToMany, ArchivedOneToOne};
pub use integrity::{canonical_lock_order, DanglingEdge, IntegrityPolicy, OnDelete};
