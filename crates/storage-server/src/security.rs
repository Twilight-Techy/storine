//! Authentication, Role-Based Access Control (RBAC), and Per-Connection Token-Bucket Rate Limiting
//! (`TASK-044`, `F-SEC-01`, `F-SEC-03`, `F-SEC-04`).

use std::collections::HashMap;
use std::time::Instant;

/// Role-Based Access Control (RBAC) permission level for a client session (`F-SEC-01`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RbacRole {
    /// Connection has not yet authenticated when `SecurityConfig::require_auth` is enabled.
    Unauthenticated = 0,
    /// Permitted to execute `GetRawFrame`, `BatchGet`, `QueryHybrid`, and `Stats`.
    ReadOnly = 1,
    /// Permitted to execute all `ReadOnly` operations plus `PutRawFrame`, `CasPut`, `BatchPut`, and `Delete`.
    ReadWrite = 2,
    /// Full administrative privileges including `Metrics` scrape and cluster administration.
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
}

/// Server-wide authentication, RBAC, and rate-limiting policy (`TASK-044`).
#[derive(Clone, Debug)]
pub struct SecurityConfig {
    /// When `true`, connections start as `RbacRole::Unauthenticated` and must send `OP_AUTH` first.
    /// When `false` (default), connections start as `RbacRole::Admin` for zero-config compatibility.
    pub require_auth: bool,
    /// Sustained requests-per-second limit per connection (`0` = unlimited).
    pub rate_limit_rps: u32,
    /// Burst token capacity per connection (`0` = unlimited).
    pub rate_limit_burst: u32,
    tokens: HashMap<String, RbacRole>,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            require_auth: false,
            rate_limit_rps: 0,
            rate_limit_burst: 0,
            tokens: HashMap::new(),
        }
    }
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

    /// Verifies `candidate_token` using constant-time byte comparison against registered tokens
    /// to prevent timing side-channel attacks (`F-SEC-01`).
    pub fn authenticate_token(&self, candidate_token: &str) -> Option<RbacRole> {
        let cand_bytes = candidate_token.as_bytes();
        let mut matched_role = None;
        for (known_tok, &role) in &self.tokens {
            let known_bytes = known_tok.as_bytes();
            if known_bytes.len() == cand_bytes.len() {
                let mut diff = 0u8;
                for i in 0..known_bytes.len() {
                    diff |= known_bytes[i] ^ cand_bytes[i];
                }
                if diff == 0 {
                    matched_role = Some(role);
                }
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
        let burst_milli = (burst as u64).saturating_mul(1_000);
        Self {
            tokens_milli: burst_milli,
            burst_milli,
            rate_per_sec: rate_rps as u64,
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
                self.tokens_milli = (self.tokens_milli + added_milli).min(self.burst_milli);
                self.last_refill = now;
            }
        }
        let needed_milli = (cost as u64).saturating_mul(1_000);
        if self.tokens_milli >= needed_milli {
            self.tokens_milli -= needed_milli;
            true
        } else {
            false
        }
    }
}
