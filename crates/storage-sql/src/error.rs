//! SQL errors with standard SQLSTATE codes (mapped to MySQL error numbers on that protocol).

use std::fmt;

/// An error reported to the SQL client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlError {
    /// Five-character SQLSTATE (`42601` syntax error, `23505` unique violation, `40001` serialization failure, ...).
    pub code: &'static str,
    pub message: String,
}

pub type SqlResult<T> = Result<T, SqlError>;

impl SqlError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        SqlError {
            code,
            message: message.into(),
        }
    }

    pub fn syntax(message: String) -> Self {
        SqlError::new("42601", message)
    }

    /// A feature this gateway does not implement; the message names it precisely.
    pub fn unsupported(what: impl fmt::Display) -> Self {
        SqlError::new("0A000", format!("not supported by Storine SQL: {what}"))
    }

    pub fn undefined_table(name: &str) -> Self {
        SqlError::new("42P01", format!("relation \"{name}\" does not exist"))
    }

    pub fn undefined_column(name: &str) -> Self {
        SqlError::new("42703", format!("column \"{name}\" does not exist"))
    }

    pub fn duplicate_table(name: &str) -> Self {
        SqlError::new("42P07", format!("relation \"{name}\" already exists"))
    }

    pub fn ambiguous_column(name: &str) -> Self {
        SqlError::new("42702", format!("column reference \"{name}\" is ambiguous"))
    }

    pub fn datatype_mismatch(message: impl Into<String>) -> Self {
        SqlError::new("42804", message)
    }

    pub fn invalid_text(message: impl Into<String>) -> Self {
        SqlError::new("22P02", message)
    }

    pub fn numeric_out_of_range(message: impl Into<String>) -> Self {
        SqlError::new("22003", message)
    }

    pub fn division_by_zero() -> Self {
        SqlError::new("22012", "division by zero")
    }

    pub fn not_null(table: &str, column: &str) -> Self {
        SqlError::new("23502", format!("null value in column \"{column}\" of relation \"{table}\" violates not-null constraint"))
    }

    pub fn unique(table: &str, constraint: &str) -> Self {
        SqlError::new(
            "23505",
            format!(
                "duplicate key value violates unique constraint \"{constraint}\" on \"{table}\""
            ),
        )
    }

    pub fn foreign_key(message: impl Into<String>) -> Self {
        SqlError::new("23503", message)
    }

    pub fn check(table: &str, constraint: &str) -> Self {
        SqlError::new(
            "23514",
            format!("new row for relation \"{table}\" violates check constraint \"{constraint}\""),
        )
    }

    /// A concurrent transaction changed data this one read or wrote: retry the transaction.
    pub fn serialization() -> Self {
        SqlError::new(
            "40001",
            "could not serialize access due to a concurrent update; retry the transaction",
        )
    }

    pub fn in_failed_transaction() -> Self {
        SqlError::new(
            "25P02",
            "current transaction is aborted, commands ignored until end of transaction block",
        )
    }

    pub fn lock_timeout(what: &str) -> Self {
        SqlError::new("55P03", format!("could not obtain lock on {what}"))
    }

    pub fn auth(message: impl Into<String>) -> Self {
        SqlError::new("28P01", message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        SqlError::new("XX000", message)
    }

    /// The storage layer is unreachable or overloaded right now.
    pub fn unavailable(message: impl Into<String>) -> Self {
        SqlError::new("08006", message)
    }

    /// The MySQL error number for this SQLSTATE.
    pub fn mysql_code(&self) -> u16 {
        match self.code {
            "42601" => 1064, // ER_PARSE_ERROR
            "42P01" => 1146, // ER_NO_SUCH_TABLE
            "42703" => 1054, // ER_BAD_FIELD_ERROR
            "42P07" => 1050, // ER_TABLE_EXISTS_ERROR
            "42702" => 1052, // ER_NON_UNIQ_ERROR
            "23505" => 1062, // ER_DUP_ENTRY
            "23502" => 1048, // ER_BAD_NULL_ERROR
            "23503" => 1452, // ER_NO_REFERENCED_ROW_2
            "23514" => 3819, // ER_CHECK_CONSTRAINT_VIOLATED
            "40001" => 1213, // ER_LOCK_DEADLOCK (clients retry on it)
            "55P03" => 1205, // ER_LOCK_WAIT_TIMEOUT
            "22012" => 1365, // ER_DIVISION_BY_ZERO
            "22003" => 1264, // ER_WARN_DATA_OUT_OF_RANGE
            "28P01" => 1045, // ER_ACCESS_DENIED_ERROR
            "0A000" => 1235, // ER_NOT_SUPPORTED_YET
            "25P02" => 1180,
            "3D000" => 1049, // ER_BAD_DB_ERROR
            "42P04" => 1007, // ER_DB_CREATE_EXISTS
            _ => 1105,       // ER_UNKNOWN_ERROR
        }
    }
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.message, self.code)
    }
}

impl std::error::Error for SqlError {}

impl From<storine::Error> for SqlError {
    fn from(e: storine::Error) -> Self {
        match e {
            storine::Error::Conflict { .. } => SqlError::serialization(),
            storine::Error::NotFound { .. } => SqlError::serialization(),
            storine::Error::Invalid(m) => SqlError::new("22023", m),
            storine::Error::Unauthorized(m) => SqlError::new("42501", m),
            storine::Error::Overloaded(m) | storine::Error::RateLimited(m) => {
                SqlError::new("53300", m)
            }
            storine::Error::Unavailable(m) => SqlError::unavailable(m),
            storine::Error::Server(m) => SqlError::internal(m),
        }
    }
}
