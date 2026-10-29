//! Scale-out and multi-region (`F-GEO-*`, Stage 30).
//!
//! - [`hlc`]: hybrid logical clocks with a bounded offset (`TASK-139`).
//! - [`node`]: the multi-Raft driver of one node: many range groups over one engine and WAL,
//!   coalesced heartbeats, quiescent idle ranges (`TASK-137`).

pub mod hlc;
pub mod node;
pub mod sim;
