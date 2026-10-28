//! Storine SQL gateway: lets applications and ORMs written for PostgreSQL or MySQL use Storine
//! unchanged, by speaking their wire protocols and SQL dialects over the document API.

pub mod ast;
pub mod catalog;
pub mod datum;
pub mod embedded;
pub mod error;
pub mod exec;
pub mod fmt;
pub mod hash;
pub mod lexer;
pub mod parser;
pub mod store;

pub use error::{SqlError, SqlResult};
pub use lexer::Dialect;

/// Version reported to PostgreSQL clients (`server_version`).
pub const PG_VERSION: &str = "16.0";
pub const PG_VERSION_NUM: &str = "160000";
pub const PG_VERSION_STRING: &str = "PostgreSQL 16.0 (Storine SQL gateway) on x86_64, compiled by rustc, 64-bit";
/// Version reported to MySQL clients.
pub const MYSQL_VERSION: &str = "8.0.36-storine";

/// A random id of this gateway process (row-lock ownership).
pub fn instance_id() -> &'static str {
    static ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ID.get_or_init(|| hash::hex(&hash::random_bytes(8)))
}
