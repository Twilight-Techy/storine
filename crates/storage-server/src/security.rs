//! Authentication, Role-Based Access Control (RBAC), and Per-Connection Token-Bucket Rate Limiting
//! (`TASK-044`, `F-SEC-01`, `F-SEC-03`, `F-SEC-04`).

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::time::Instant;

/// Role-Based Access Control (RBAC) permission level for a client session (`F-SEC-01`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RbacRole {
    /// Connection has not yet authenticated when `SecurityConfig::require_auth` is enabled.
    Unauthenticated = 0,
    /// Permitted to execute `GetRawFrame`, `BatchGet`, `QueryHybrid`, `Stats`, `Metrics`, `Health`.
    ReadOnly = 1,
    /// Permitted to execute all `ReadOnly` operations plus `PutRawFrame`, `CasPut`, `BatchPut`, and `Delete`.
    ReadWrite = 2,
    /// Full administrative privileges: backups, compaction, checkpoints, shard import and Raft RPCs.
    Admin = 3,
}

impl RbacRole {
    #[inline]
    pub const fn can_read(self) -> bool {
        (self as u8) >= (Self::ReadOnly as u8)
    }

    #[inline]
    pub const fn can_write(self) -> bool {
        (self as u8) >= (Self::ReadWrite as u8)
    }

    #[inline]
    pub const fn is_admin(self) -> bool {
        matches!(self, Self::Admin)
    }

    /// Parses `read` / `write` / `admin` (case-insensitive) as used by `STORAGE_AUTH_TOKENS`.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "read" | "readonly" | "ro" => Some(Self::ReadOnly),
            "write" | "readwrite" | "rw" => Some(Self::ReadWrite),
            "admin" => Some(Self::Admin),
            _ => None,
        }
    }
}

/// Server-wide authentication, RBAC, and rate-limiting policy (`TASK-044`).
#[derive(Clone, Debug, Default)]
pub struct SecurityConfig {
    /// When `true`, connections start as `RbacRole::Unauthenticated` and must send `OP_AUTH` first.
    /// When `false` (default), connections start as `RbacRole::Admin`; the server then refuses to
    /// bind anything but a loopback address, so an unauthenticated server is never network-reachable.
    pub require_auth: bool,
    /// Sustained requests-per-second limit per connection (`0` = unlimited).
    pub rate_limit_rps: u32,
    /// Burst token capacity per connection (`0` = unlimited).
    pub rate_limit_burst: u32,
    /// Directory under which `OP_ADMIN_BACKUP` may create backups (`None` = admin backups disabled).
    pub backup_root: Option<PathBuf>,
    tokens: HashMap<String, RbacRole>,
}

impl SecurityConfig {
    #[inline]
    pub fn new(require_auth: bool) -> Self {
        Self {
            require_auth,
            ..Self::default()
        }
    }

    /// Registers a bearer/API token mapped to `role`.
    pub fn with_token(mut self, token: impl Into<String>, role: RbacRole) -> Self {
        self.tokens.insert(token.into(), role);
        self
    }

    /// Configures per-connection token-bucket rate limiting (`rps` sustained rate, `burst` max tokens).
    pub const fn with_rate_limit(mut self, rps: u32, burst: u32) -> Self {
        self.rate_limit_rps = rps;
        self.rate_limit_burst = burst;
        self
    }

    /// Enables `OP_ADMIN_BACKUP`, confined to fresh sub-directories of `root`.
    pub fn with_backup_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.backup_root = Some(root.into());
        self
    }

    /// Number of registered tokens.
    #[inline]
    pub fn token_count(&self) -> usize {
        self.tokens.len()
    }

    /// Verifies `candidate_token` using constant-time byte comparison against every registered
    /// token (no early exit), preventing timing side-channel attacks (`F-SEC-01`).
    pub fn authenticate_token(&self, candidate_token: &str) -> Option<RbacRole> {
        let cand_bytes = candidate_token.as_bytes();
        let mut matched_role = None;
        for (known_tok, &role) in &self.tokens {
            let known_bytes = known_tok.as_bytes();
            let mut diff = (known_bytes.len() ^ cand_bytes.len()) as u8
                | u8::from(known_bytes.len() != cand_bytes.len());
            for (i, &kb) in known_bytes.iter().enumerate() {
                diff |= kb ^ cand_bytes.get(i).copied().unwrap_or(0);
            }
            if core::hint::black_box(diff) == 0 {
                matched_role = Some(role);
            }
        }
        matched_role
    }

    #[inline]
    pub const fn initial_role(&self) -> RbacRole {
        if self.require_auth {
            RbacRole::Unauthenticated
        } else {
            RbacRole::Admin
        }
    }

    /// Resolves a client-supplied backup name to `<backup_root>/<name>`.
    ///
    /// Only plain relative paths are accepted (no root, drive prefix, `..` or `.` components),
    /// so a backup can never be written (or anything deleted) outside `backup_root`.
    pub fn resolve_backup_path(&self, requested: &str) -> Option<PathBuf> {
        let root = self.backup_root.as_ref()?;
        let rel = Path::new(requested);
        if requested.is_empty()
            || requested.contains([':', '\\', '\0'])
            || !rel
                .components()
                .all(|c| matches!(c, Component::Normal(_)))
        {
            return None;
        }
        Some(root.join(rel))
    }
}

/// Zero-allocation per-connection Token-Bucket Rate Limiter (`F-SEC-03`, `RULE-MEM-01`).
#[derive(Clone, Debug)]
pub struct TokenBucket {
    tokens_milli: u64,
    burst_milli: u64,
    rate_per_sec: u64,
    last_refill: Instant,
}

impl TokenBucket {
    #[inline]
    pub fn new(rate_rps: u32, burst: u32) -> Self {
        let burst_milli = u64::from(burst).saturating_mul(1_000);
        Self {
            tokens_milli: burst_milli,
            burst_milli,
            rate_per_sec: u64::from(rate_rps),
            last_refill: Instant::now(),
        }
    }

    /// Attempts to consume `cost` tokens (typically `1` per RPC).
    /// Returns `true` if allowed, or `false` if rate-limited.
    #[inline]
    pub fn try_acquire(&mut self, cost: u32) -> bool {
        if self.rate_per_sec == 0 || self.burst_milli == 0 {
            return true;
        }
        let now = Instant::now();
        let elapsed_us = now.duration_since(self.last_refill).as_micros() as u64;
        if elapsed_us > 0 {
            let added_milli = (elapsed_us.saturating_mul(self.rate_per_sec)) / 1_000;
            if added_milli > 0 {
                self.tokens_milli = self.tokens_milli.saturating_add(added_milli).min(self.burst_milli);
                self.last_refill = now;
            }
        }
        let needed_milli = u64::from(cost).saturating_mul(1_000);
        if self.tokens_milli >= needed_milli {
            self.tokens_milli -= needed_milli;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backup_path_resolution_rejects_escapes() {
        let sec = SecurityConfig::new(true).with_backup_root("/srv/backups");
        assert!(sec.resolve_backup_path("nightly/2026-10-20").is_some());
        for bad in ["", "..", "../etc", "a/../../b", "/etc", "C:\\Windows", "./x"] {
            assert!(sec.resolve_backup_path(bad).is_none(), "{bad} must be rejected");
        }
        assert!(SecurityConfig::new(true).resolve_backup_path("x").is_none());
    }

    #[test]
    fn test_token_auth_and_role_parse() {
        let sec = SecurityConfig::new(true).with_token("secret", RbacRole::Admin);
        assert_eq!(sec.authenticate_token("secret"), Some(RbacRole::Admin));
        assert_eq!(sec.authenticate_token("secre"), None);
        assert_eq!(sec.authenticate_token("secret!"), None);
        assert_eq!(RbacRole::parse("RW"), Some(RbacRole::ReadWrite));
        assert_eq!(RbacRole::parse("root"), None);
    }
}
