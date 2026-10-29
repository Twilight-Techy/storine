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

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

pub mod analytics;
pub mod backup;
pub mod catalog;
pub mod columnar;
mod decrypt_cache;
pub mod document;
pub mod dist;
pub mod document_wire;
pub mod engine;
pub mod linearizability;
pub mod migration;
pub mod model;
pub mod mvcc;
pub mod optimizer;
pub mod query;
pub mod raft;
pub mod raft_runtime;
pub mod raft_sim;
pub mod sharding;
pub mod telemetry;
pub mod write_queue;

pub use backup::{
    archive_wal_for_pitr, create_online_backup, restore_point_in_time, BackupManifest,
};
pub use catalog::{CatalogTransaction, CollectionMeta, DatabaseCatalog, ErasedCollectionEngine};
pub use document::{
    CollectionInfo, CommitSink, DocOp, DocPage, DocQuery, DocWrite, Direction, Document,
    DocumentRecord, DocumentStore, Filter, WriteSummary, ByteRange, MAX_BYTE_RANGE, MAX_DOCUMENT_INDEXES,
    MAX_PROJECTION_FIELDS,
};
pub use engine::{
    EncryptionConfig, IndexTier, PrimaryTier, ResealReport, Precondition, WriteKind, WriteOp, WriteResult,
    EngineConfig, EngineStats, RecordRef, StorageEngine, StorageLayoutMode, TransactionBatch,
};
pub use migration::SchemaRegistry;
pub use model::{
    AgentObjectRecord, ArchivedAgentObjectRecord, FIELD_PRIMARY_TAG, FIELD_SCORE, FIELD_TIMESTAMP,
};
pub use mvcc::{CommitTs, MvccManager, ReadSnapshot, VersionChainNode};
pub use optimizer::{AccessPathKind, PredicateCostEstimate, QueryExplainPlan, QueryOptimizer};
pub use query::{QueryBuilder, QueryPage};
pub use storage_index::{KeysetCursor, SortOrder};
pub use raft::{
    AppendEntriesReq, AppendEntriesResp, RaftLogEntry, RaftNode, RaftReplicationGroup, RaftRole,
    RequestVoteReq, RequestVoteResp,
};
pub use sharding::{
    vshard_for_id, ClusterMap, ClusterRouter, NodeId, ShardMigrationReceipt, VIRTUAL_SHARD_COUNT,
};
pub use telemetry::{LatencyHistogram, SlowOpEntry, SlowOperationLog};
pub use write_queue::{MultiRecordLockGuard, RecordLockGuard, StripedLockTable};
