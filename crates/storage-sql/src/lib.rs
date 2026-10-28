//! Storine SQL gateway: lets applications and ORMs written for PostgreSQL or MySQL use Storine
//! unchanged, by speaking their wire protocols and SQL dialects over the document API.

pub mod ast;
pub mod catalog;
pub mod copy;
pub mod datum;
pub mod embedded;
pub mod error;
pub mod exec;
pub mod fmt;
pub mod hash;
pub mod lexer;
pub mod mysql;
pub mod parser;
pub mod pg;
pub mod store;

pub use error::{SqlError, SqlResult};
pub use lexer::Dialect;

/// Version reported to PostgreSQL clients (`server_version`).
pub const PG_VERSION: &str = "16.0";
pub const PG_VERSION_NUM: &str = "160000";
pub const PG_VERSION_STRING: &str =
    "PostgreSQL 16.0 (Storine SQL gateway) on x86_64, compiled by rustc, 64-bit";
/// Version reported to MySQL clients.
pub const MYSQL_VERSION: &str = "8.4.3-storine";

/// A random id of this gateway process (row-lock ownership).
pub fn instance_id() -> &'static str {
    static ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ID.get_or_init(|| hash::hex(&hash::random_bytes(8)))
}

/// Whether statements and errors are logged to stderr (`STORINE_SQL_LOG=1`).
pub fn log_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("STORINE_SQL_LOG").is_ok_and(|v| v != "0"))
}
