//! Zero-allocation error definitions with `#[cold]` constructors (`RULE-CPU-03`).

use core::fmt;
use crate::{InlineId, Version};

pub type Result<T> = core::result::Result<T, StorageError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// Primary key was empty, contained forbidden path characters, or exceeded limits.
    InvalidKey { reason: &'static str },
    /// Requested record ID does not exist or has been deleted/expired.
    RecordNotFound { id: InlineId },
    /// Insert attempted for a key that already exists.
    RecordAlreadyExists { id: InlineId },
    /// Optimistic Concurrency Control (OCC) version mismatch during conditional update.
    VersionConflict {
        id: InlineId,
        expected: Version,
        actual: Version,
    },
    /// Record header magic, version, bounds, or header checksum failed verification.
    CorruptedHeader { reason: &'static str },
    /// Payload XXH3-64 / CRC32C checksum did not match header checksum.
    ChecksumMismatch { expected: u64, actual: u64 },
    /// Zero-copy archive bytecheck validation failed (out-of-bounds relative pointer or alignment).
    InvalidArchiveLayout { reason: &'static str },
    /// Referential integrity policy (`OnDelete::Restrict`) prevented record deletion or linking.
    IntegrityViolation {
        source: InlineId,
        target: InlineId,
        reason: &'static str,
    },
    /// Schema version in record header could not be migrated to target version.
    SchemaVersionMismatch { found: u16, expected: u16 },
    /// Configured resource ceiling (`max_active_mmaps`, `max_hot_dirty_bytes`, etc.) reached and all slots are pinned.
    ResourceExhausted { resource: &'static str },
    /// Multi-record transaction aborted due to conflict or user closure error.
    TransactionAborted { reason: &'static str },
    /// A write precondition (`if absent`, `if exists`, `if version`) did not hold.
    /// `actual` is the record's current version (`None` = the record does not exist).
    ConditionFailed {
        id: InlineId,
        reason: &'static str,
        actual: Option<Version>,
    },
    /// A value was rejected: arithmetic overflow, a bound violation, a type mismatch, or input
    /// that does not parse (e.g. a malformed decimal).
    InvalidValue { reason: &'static str },
    /// This replica is not the Raft leader; writes (and linearizable reads) must go to
    /// `leader` (a node id) when it is known.
    NotLeader { leader: Option<u64> },
    /// A replicated write was proposed but its outcome is unknown (no quorum acknowledged it in
    /// time; it may still commit). Retry with the same request id: the retry is applied at most
    /// once.
    CommitUnknown { index: u64 },
    /// The engine is a Raft replica: it is written only through consensus, never directly.
    ReadOnlyReplica,
    /// A remote server rejected a request; `status` is the wire status code and `message` the
    /// server's explanation.
    Rejected { status: u8, message: String },
    /// Underlying OS I/O or virtual memory mapping error.
    Io {
        context: &'static str,
        raw_os_error: Option<i32>,
        message: String,
    },
}

impl StorageError {
    #[cold]
    #[inline(never)]
    pub fn invalid_key(reason: &'static str) -> Self {
        Self::InvalidKey { reason }
    }

    #[cold]
    #[inline(never)]
    pub fn not_found(id: InlineId) -> Self {
        Self::RecordNotFound { id }
    }

    #[cold]
    #[inline(never)]
    pub fn already_exists(id: InlineId) -> Self {
        Self::RecordAlreadyExists { id }
    }

    #[cold]
    #[inline(never)]
    pub fn version_conflict(id: InlineId, expected: Version, actual: Version) -> Self {
        Self::VersionConflict {
            id,
            expected,
            actual,
        }
    }

    #[cold]
    #[inline(never)]
    pub fn corrupted_header(reason: &'static str) -> Self {
        Self::CorruptedHeader { reason }
    }

    #[cold]
    #[inline(never)]
    pub fn checksum_mismatch(expected: u64, actual: u64) -> Self {
        Self::ChecksumMismatch { expected, actual }
    }

    #[cold]
    #[inline(never)]
    pub fn invalid_archive(reason: &'static str) -> Self {
        Self::InvalidArchiveLayout { reason }
    }

    #[cold]
    #[inline(never)]
    pub fn integrity_violation(source: InlineId, target: InlineId, reason: &'static str) -> Self {
        Self::IntegrityViolation {
            source,
            target,
            reason,
        }
    }

    #[cold]
    #[inline(never)]
    pub fn resource_exhausted(resource: &'static str) -> Self {
        Self::ResourceExhausted { resource }
    }

    #[cold]
    #[inline(never)]
    pub fn condition_failed(id: InlineId, reason: &'static str, actual: Option<Version>) -> Self {
        Self::ConditionFailed { id, reason, actual }
    }

    #[cold]
    #[inline(never)]
    pub fn invalid_value(reason: &'static str) -> Self {
        Self::InvalidValue { reason }
    }

    #[cold]
    #[inline(never)]
    pub fn io(context: &'static str, err: std::io::Error) -> Self {
        Self::Io {
            context,
            raw_os_error: err.raw_os_error(),
            message: err.to_string(),
        }
    }
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidKey { reason } => write!(f, "invalid primary key: {reason}"),
            Self::RecordNotFound { id } => write!(f, "record not found: '{id}'"),
            Self::RecordAlreadyExists { id } => write!(f, "record already exists: '{id}'"),
            Self::VersionConflict {
                id,
                expected,
                actual,
            } => write!(
                f,
                "version conflict on '{id}': expected v{}, actual v{}",
                expected.0, actual.0
            ),
            Self::CorruptedHeader { reason } => write!(f, "corrupted record header: {reason}"),
            Self::ChecksumMismatch { expected, actual } => write!(
                f,
                "payload checksum mismatch: expected {expected:#018x}, actual {actual:#018x}"
            ),
            Self::InvalidArchiveLayout { reason } => {
                write!(f, "invalid zero-copy archive layout: {reason}")
            }
            Self::IntegrityViolation {
                source,
                target,
                reason,
            } => write!(
                f,
                "referential integrity violation between '{source}' and '{target}': {reason}"
            ),
            Self::SchemaVersionMismatch { found, expected } => write!(
                f,
                "schema version mismatch: found v{found}, expected v{expected}"
            ),
            Self::ResourceExhausted { resource } => {
                write!(f, "engine resource limit exhausted: {resource}")
            }
            Self::TransactionAborted { reason } => write!(f, "transaction aborted: {reason}"),
            Self::ConditionFailed { id, reason, actual } => match actual {
                Some(v) => write!(f, "condition failed on '{id}': {reason} (current version v{})", v.0),
                None => write!(f, "condition failed on '{id}': {reason} (record does not exist)"),
            },
            Self::InvalidValue { reason } => write!(f, "invalid value: {reason}"),
            Self::NotLeader { leader: Some(l) } => {
                write!(f, "this node is not the leader; send writes to node {l}")
            }
            Self::NotLeader { leader: None } => {
                write!(f, "this node is not the leader and no leader is known yet; retry shortly")
            }
            Self::CommitUnknown { index } => write!(
                f,
                "write outcome unknown (raft index {index} not yet confirmed by a quorum); retry with the same request id"
            ),
            Self::Rejected { status, message } => {
                write!(f, "server rejected the request (status {status:#04x}): {message}")
            }
            Self::ReadOnlyReplica => write!(
                f,
                "this engine is a raft replica: write through the leader, not directly"
            ),
            Self::Io {
                context,
                raw_os_error,
                message,
            } => write!(
                f,
                "I/O error during {context} (os={raw_os_error:?}): {message}"
            ),
        }
    }
}

impl std::error::Error for StorageError {}
