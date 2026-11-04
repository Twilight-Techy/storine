//! Scale-out and multi-region (`F-GEO-*`, Stage 30).
//!
//! - [`hlc`]: hybrid logical clocks with a bounded offset (`TASK-139`).
//! - [`node`]: the multi-Raft driver of one node: many range groups over one engine and WAL,
//!   coalesced heartbeats, quiescent idle ranges (`TASK-137`), replica moves, merges and
//!   rebalancing (`TASK-138`).
//! - [`alloc`]: the allocator's planning rules (`TASK-138`).
//! - [`meta`]: range descriptors, the meta range and range caches (`TASK-138`).
//! - [`docs`]: the document API over distributed transactions (`TASK-143`).
//! - [`backup`]: cluster-wide backups at one timestamp, and restore (`TASK-144`).
//! - [`feed`]: resumable change data capture (`TASK-144`).
//! - [`runtime`]: each node's real-time event loop, transports and the routing gateway
//!   (`TASK-143`).
//! - [`wire`]: encodings of node batches and KV requests between processes (`TASK-143`).
//! - [`zone`]: localities, zone configurations, survival goals and closed-timestamp policies
//!   (`TASK-141`).

/// WAL policy of a range node's engine: no fsync per write. The Raft logs are the durable
/// record; the WAL is synced at checkpoints, and a crash replays the logs from the last one.
pub const fn range_wal_policy() -> storage_wal::SyncPolicy {
    storage_wal::SyncPolicy::AsyncMemoryFirst
}

pub mod alloc;
pub mod backup;
pub mod docs;
pub mod feed;
pub mod hlc;
pub mod meta;
pub mod mvcc;
pub mod node;
pub mod runtime;
pub mod server;
pub mod txn;
pub mod txn_sim;
pub mod wire;
pub mod sim;
pub mod zone;
