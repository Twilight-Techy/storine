//! Rust client of the Storine database. No dependencies; blocking I/O (call it from
//! `spawn_blocking` in async code).
//!
//! ```no_run
//! use storine::{add, dec, doc, gt, Query};
//!
//! let db = storine::connect("storine://127.0.0.1:7878")?; // or "storine://token@h1:7878,h2:7878,h3:7878"
//! let accounts = db.collection("accounts");
//! accounts.insert("alice", doc! { "owner" => "Alice", "balance" => dec("100.00") })?;
//!
//! // Atomic, exact, server-side: no lost updates, no overdraft.
//! accounts.update("alice", [add("balance", dec("-25.00")).min(0)])?;
//!
//! let rich = accounts.find(Query::new().filter(gt("balance", dec("50"))).order_by("-balance").limit(10))?;
//! # Ok::<(), storine::Error>(())
//! ```
//!
//! Every write carries a request id that the client reuses on its own retries, so timeouts,
//! reconnects and cluster leader changes never apply a write twice.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

mod client;
mod error;
mod value;
mod wire;

pub use client::{connect, connect_with, Client, Collection, Options, Scan, Transaction};
pub use error::{Error, Result};
pub use value::{dec, decode, encode, Decimal, Doc, Value};
pub use wire::{
    add, and, append, between, contains, eq, exists, ge, gt, is_in, key_prefix, le, lt, matches, missing, ne, or,
    prefix, set, unset, ClusterInfo, CollectionInfo, Document, FieldOp, Filter, Query,
};

#[cfg(test)]
mod conformance;
