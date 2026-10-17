//! Core Zero-Copy Object Storage Engine (`storage-core`).
//!
//! Combines:
//! - Zero-copy `mmap` & `HotDirtyArc` reads (`RecordRef`, `with_ref`)
//! - 2048-way `CachePadded` striped per-record write queues (`StripedLockTable`)
//! - Coalesced background disk flushing (`DedicatedFile`, `PingPong`, and `Arena` modes)
//! - Group-Commit WAL durability & deterministic crash recovery
//! - Secondary B+Tree range, Roaring-style bitmap tag, and SIMD vector indexing
//! - Explicit `1:1`, `1:N`, `M:N` relationships with deadlock-free linking and `Restrict`/`Cascade`/`Unlink` integrity
//! - Multi-record OCC transactions, schema evolution, TTL expiration, and consistent snapshots

#![deny(unsafe_op_in_unsafe_fn)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

pub mod engine;
pub mod model;
pub mod query;
pub mod write_queue;

pub use engine::{
    EngineConfig, EngineStats, RecordRef, StorageEngine, StorageLayoutMode, TransactionBatch,
};
pub use model::{
    AgentObjectRecord, ArchivedAgentObjectRecord, FIELD_PRIMARY_TAG, FIELD_SCORE, FIELD_TIMESTAMP,
};
pub use query::QueryBuilder;
pub use write_queue::{MultiRecordLockGuard, RecordLockGuard, StripedLockTable};
