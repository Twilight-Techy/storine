use std::fmt;

/// Every error of this crate. Each says what happened and what to do about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The document does not exist (e.g. `update` of a missing key). `write_index`: the write, in a
    /// transaction, whose document was missing.
    NotFound { message: String, write_index: Option<u32> },
    /// A condition did not hold: the document already exists, does not exist, or changed. Re-read and
    /// decide again. `current_version` is `None` when the document does not exist.
    Conflict { message: String, write_index: Option<u32>, current_version: Option<u64> },
    /// The request was rejected as invalid (bad value, type mismatch, bound violated). Do not retry.
    Invalid(String),
    /// Authentication failed or the token's role does not allow this operation.
    Unauthorized(String),
    /// The server is temporarily overloaded. Retry later.
    Overloaded(String),
    /// This connection exceeded its rate limit. Retry later.
    RateLimited(String),
    /// No server (or no cluster leader) could be reached in time. Retry later.
    Unavailable(String),
    /// The server failed internally (I/O error...). The message has the details.
    Server(String),
}

impl Error {
    /// True for errors that may succeed when retried later (overload, rate limit, unavailability).
    pub fn is_retryable(&self) -> bool {
        matches!(self, Error::Overloaded(_) | Error::RateLimited(_) | Error::Unavailable(_))
    }

    /// The human-readable message.
    pub fn message(&self) -> &str {
        match self {
            Error::NotFound { message, .. } | Error::Conflict { message, .. } => message,
            Error::Invalid(m)
            | Error::Unauthorized(m)
            | Error::Overloaded(m)
            | Error::RateLimited(m)
            | Error::Unavailable(m)
            | Error::Server(m) => m,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Error::NotFound { .. } => "not found",
            Error::Conflict { .. } => "conflict",
            Error::Invalid(_) => "invalid",
            Error::Unauthorized(_) => "unauthorized",
            Error::Overloaded(_) => "overloaded",
            Error::RateLimited(_) => "rate limited",
            Error::Unavailable(_) => "unavailable",
            Error::Server(_) => "server error",
        };
        write!(f, "storine: {kind}: {}", self.message())
    }
}

impl std::error::Error for Error {}

/// `Result` with this crate's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}
