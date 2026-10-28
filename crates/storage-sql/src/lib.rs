//! Storine SQL gateway: lets applications and ORMs written for PostgreSQL or MySQL use Storine
//! unchanged, by speaking their wire protocols and SQL dialects over the document API.

pub mod ast;
pub mod datum;
pub mod error;
pub mod hash;
pub mod lexer;
pub mod parser;

pub use error::{SqlError, SqlResult};
pub use lexer::Dialect;
