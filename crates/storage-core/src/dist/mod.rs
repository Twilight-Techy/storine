//! Scale-out and multi-region (`F-GEO-*`, Stage 30).
//!
//! - [`hlc`]: hybrid logical clocks with a bounded offset (`TASK-139`).
//! - [`node`]: the multi-Raft driver of one node: many range groups over one engine and WAL,
//!   coalesced heartbeats, quiescent idle ranges (`TASK-137`), replica moves, merges and
//!   rebalancing (`TASK-138`).
//! - [`alloc`]: the allocator's planning rules (`TASK-138`).
//! - [`meta`]: range descriptors, the meta range and range caches (`TASK-138`).

pub mod alloc;
pub mod hlc;
pub mod meta;
pub mod mvcc;
pub mod node;
pub mod server;
pub mod txn;
pub mod txn_sim;
pub mod sim;
