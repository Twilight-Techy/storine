//! Control of running work (`F-ENT-02`, `TASK-146`): cancellation, timeouts and per-query memory.
//!
//! Each session has a [`Control`] others can reach through the gateway's registry:
//! - **Cancel** (PostgreSQL `CancelRequest` and `pg_cancel_backend`, MySQL `KILL QUERY`, the TDS
//!   attention signal): the running statement stops at its next check, the session lives on.
//! - **Terminate** (`pg_terminate_backend`, `KILL [CONNECTION]`, T-SQL `KILL`,
//!   `idle_in_transaction_session_timeout`): a running statement stops the same way; an idle
//!   connection is woken by shutting down its socket's read side, which also works under TLS, and
//!   the connection loop reports why before closing.
//! - **Statement timeout** (`statement_timeout`, MySQL `max_execution_time`).
//! - **Memory** (`max_query_memory`): what a statement holds in rows is counted; a subquery's rows
//!   stop counting when it returns (its result stays counted).
//!
//! **Where checks happen**: expression evaluation (every row of every plan passes through it),
//! between documents of a storage scan, and in lock waits. A single storage request that blocks
//! is not interrupted; it ends within the client's own request timeout.

use std::collections::HashMap;
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crate::error::{SqlError, SqlResult};
use crate::exec::now_ns;

const RUNNING: u8 = 0;
const CANCELED: u8 = 1;
const TERMINATED: u8 = 2;
const IDLE_TIMEOUT: u8 = 3;

/// One session, as other sessions and the janitor see it.
#[derive(Debug)]
pub struct Control {
    pub id: u64,
    /// What a PostgreSQL `CancelRequest` must present.
    pub secret: u32,
    state: AtomicU8,
    /// Statement deadline (ns since the epoch; `0`: none).
    deadline: AtomicI64,
    /// In a transaction and idle since (ns; `0`: not), and for how long that may last (ms).
    idle_since: AtomicI64,
    idle_limit_ms: AtomicU64,
    socket: Mutex<Option<TcpStream>>,
    pub(crate) identity: Mutex<Identity>,
}

/// Who runs a session, and what it is doing (`pg_stat_activity`, `TASK-147`).
#[derive(Debug, Default, Clone)]
pub struct Identity {
    pub user: String,
    pub role: Option<String>,
    pub database: String,
    pub application: String,
    pub client: String,
    pub dialect: &'static str,
    /// Statement text, while running or as last run.
    pub query: String,
    pub running: bool,
    pub in_transaction: bool,
    pub failed_transaction: bool,
    pub backend_start_ns: i64,
    pub query_start_ns: i64,
    pub transaction_start_ns: i64,
    pub state_change_ns: i64,
    /// What it waits for, if anything.
    pub wait: Option<String>,
}

impl Control {
    fn new(id: u64) -> Arc<Control> {
        let mut s = [0u8; 4];
        s.copy_from_slice(&crate::hash::random_bytes(4));
        Arc::new(Control {
            id,
            secret: u32::from_le_bytes(s),
            state: AtomicU8::new(RUNNING),
            deadline: AtomicI64::new(0),
            idle_since: AtomicI64::new(0),
            idle_limit_ms: AtomicU64::new(0),
            socket: Mutex::new(None),
            identity: Mutex::new(Identity { backend_start_ns: now_ns(), state_change_ns: now_ns(), ..Identity::default() }),
        })
    }

    /// Gives the control the connection's socket, so a terminate reaches an idle connection.
    pub fn attach_socket(&self, stream: &TcpStream) {
        if let Ok(peer) = stream.peer_addr() {
            self.identity.lock().unwrap_or_else(std::sync::PoisonError::into_inner).client = peer.ip().to_string();
        }
        if let Ok(c) = stream.try_clone() {
            *self.socket.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(c);
        }
    }

    pub fn cancel(&self) {
        let _ = self.state.compare_exchange(RUNNING, CANCELED, Ordering::AcqRel, Ordering::Acquire);
    }

    pub fn terminate(&self) {
        self.state.store(TERMINATED, Ordering::Release);
        self.wake();
    }

    fn wake(&self) {
        if let Some(s) = self.socket.lock().unwrap_or_else(std::sync::PoisonError::into_inner).as_ref() {
            let _ = s.shutdown(Shutdown::Read);
        }
    }

    /// A statement starts: an earlier cancel no longer applies (it was for what already ended).
    pub(crate) fn begin_statement(&self, timeout: Option<Duration>) {
        let _ = self.state.compare_exchange(CANCELED, RUNNING, Ordering::AcqRel, Ordering::Acquire);
        self.idle_since.store(0, Ordering::Release);
        let deadline = timeout.map_or(0, |t| now_ns().saturating_add(t.as_nanos().min(i64::MAX as u128) as i64));
        self.deadline.store(deadline, Ordering::Release);
    }

    pub(crate) fn end_statement(&self, in_transaction: bool, idle_limit: Option<Duration>) {
        self.deadline.store(0, Ordering::Release);
        self.idle_limit_ms.store(idle_limit.map_or(0, |d| d.as_millis() as u64), Ordering::Release);
        self.idle_since.store(if in_transaction { now_ns() } else { 0 }, Ordering::Release);
    }

    /// Fails if this statement should stop.
    pub fn check(&self) -> SqlResult<()> {
        match self.state.load(Ordering::Acquire) {
            RUNNING => {}
            CANCELED => return Err(SqlError::new("57014", "canceling statement due to user request")),
            _ => return Err(self.fatal().unwrap_or_else(|| SqlError::new("57P01", "terminating connection"))),
        }
        let d = self.deadline.load(Ordering::Acquire);
        if d != 0 && now_ns() >= d {
            return Err(SqlError::new("57014", "canceling statement due to statement timeout"));
        }
        Ok(())
    }

    /// Why this connection must close, if it must.
    pub fn fatal(&self) -> Option<SqlError> {
        match self.state.load(Ordering::Acquire) {
            TERMINATED => Some(SqlError::new("57P01", "terminating connection due to administrator command")),
            IDLE_TIMEOUT => Some(SqlError::new("25P03", "terminating connection due to idle-in-transaction timeout")),
            _ => None,
        }
    }

    /// Whether a client is connected (internal sessions, such as a database switch's, are not
    /// shown), or it has run something.
    pub fn is_serving(&self) -> bool {
        self.socket.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_some() || !self.identity.lock().unwrap_or_else(std::sync::PoisonError::into_inner).user.is_empty()
    }

    /// In the middle of a statement or a transaction.
    pub fn busy(&self) -> bool {
        let id = self.identity.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        id.running || id.in_transaction
    }

    /// What this session is waiting for now.
    pub fn set_wait(&self, wait: Option<String>) {
        self.identity.lock().unwrap_or_else(std::sync::PoisonError::into_inner).wait = wait;
    }

    /// Who owns this session, for permission checks.
    pub fn user(&self) -> String {
        self.identity.lock().unwrap_or_else(std::sync::PoisonError::into_inner).user.clone()
    }
}

/// The sessions of a gateway.
#[derive(Debug, Default)]
pub struct Registry {
    sessions: Mutex<HashMap<u64, Weak<Control>>>,
    janitor: AtomicBool,
    /// Statistics of statements run (`exec/activity.rs`).
    pub statements: crate::exec::activity::Statements,
    /// `LISTEN` / `NOTIFY` (`exec/notify.rs`).
    pub(crate) listeners: crate::exec::notify::Listeners,
}

impl Registry {
    pub(crate) fn register(self: &Arc<Self>, id: u64) -> Arc<Control> {
        let c = Control::new(id);
        let mut m = self.sessions.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if m.len() >= 256 && m.len().is_power_of_two() {
            m.retain(|_, w| w.strong_count() > 0);
        }
        m.insert(id, Arc::downgrade(&c));
        drop(m);
        if !self.janitor.swap(true, Ordering::AcqRel) {
            let weak = Arc::downgrade(self);
            let _ = std::thread::Builder::new().name("sql-janitor".into()).spawn(move || janitor(weak));
        }
        c
    }

    pub fn get(&self, id: u64) -> Option<Arc<Control>> {
        self.sessions.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get(&id).and_then(Weak::upgrade)
    }

    /// Every live session.
    pub fn all(&self) -> Vec<Arc<Control>> {
        self.sessions.lock().unwrap_or_else(std::sync::PoisonError::into_inner).values().filter_map(Weak::upgrade).collect()
    }

    /// A PostgreSQL `CancelRequest`: nothing happens, and nothing is said, unless the key is right.
    pub fn cancel_request(&self, pid: i32, secret: u32) {
        for c in self.all() {
            if c.id as i32 == pid && crate::hash::ct_eq(&c.secret.to_le_bytes(), &secret.to_le_bytes()) {
                c.cancel();
            }
        }
    }
}

/// Ends idle transactions that outlived their limit.
fn janitor(registry: Weak<Registry>) {
    while let Some(r) = registry.upgrade() {
        let now = now_ns();
        for c in r.all() {
            let since = c.idle_since.load(Ordering::Acquire);
            let limit = c.idle_limit_ms.load(Ordering::Acquire);
            if since != 0 && limit != 0 && now.saturating_sub(since) / 1_000_000 >= limit as i64 {
                c.state.store(IDLE_TIMEOUT, Ordering::Release);
                c.idle_since.store(0, Ordering::Release);
                c.wake();
            }
        }
        drop(r);
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// A duration setting: a number in `default_ms`-sized units, or with a unit (`500ms`, `2s`, `1min`).
/// `None`: not set, zero or negative.
pub fn parse_duration(text: &str, unit_ms: f64) -> Option<Duration> {
    let v = text.trim().trim_matches('\'').to_ascii_lowercase();
    let split = v.find(|c: char| !c.is_ascii_digit() && c != '.' && c != '-').unwrap_or(v.len());
    let (num, unit) = v.split_at(split);
    let n: f64 = num.parse().ok()?;
    let ms = match unit.trim() {
        "" => n * unit_ms,
        "ms" => n,
        "s" => n * 1e3,
        "min" => n * 60e3,
        "h" => n * 3600e3,
        "d" => n * 86_400e3,
        "us" => n / 1e3,
        _ => return None,
    };
    (ms > 0.0 && ms.is_finite()).then(|| Duration::from_secs_f64(ms / 1e3))
}

/// A byte-size setting: bytes, or with a unit (`64kB`, `512MB`, `2GB`).
pub fn parse_bytes(text: &str) -> Option<u64> {
    let v = text.trim().trim_matches('\'').to_ascii_lowercase();
    let split = v.find(|c: char| !c.is_ascii_digit()).unwrap_or(v.len());
    let (num, unit) = v.split_at(split);
    let n: u64 = num.parse().ok()?;
    let scale: u64 = match unit.trim() {
        "" | "b" => 1,
        "k" | "kb" => 1 << 10,
        "m" | "mb" => 1 << 20,
        "g" | "gb" => 1 << 30,
        "t" | "tb" => 1 << 40,
        _ => return None,
    };
    n.checked_mul(scale)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_and_sizes_parse_as_their_servers_do() {
        assert_eq!(parse_duration("100", 1.0), Some(Duration::from_millis(100)));
        assert_eq!(parse_duration("2", 1000.0), Some(Duration::from_secs(2)));
        assert_eq!(parse_duration("'1.5s'", 1.0), Some(Duration::from_millis(1500)));
        assert_eq!(parse_duration("1min", 1.0), Some(Duration::from_secs(60)));
        for none in ["0", "-1", "", "abc", "5 parsecs"] {
            assert_eq!(parse_duration(none, 1.0), None, "{none}");
        }
        assert_eq!(parse_bytes("64kB"), Some(65_536));
        assert_eq!(parse_bytes("2GB"), Some(2 << 30));
        assert_eq!(parse_bytes("12"), Some(12));
        assert_eq!(parse_bytes("99999999999999999TB"), None, "overflow is refused");
        assert_eq!(parse_bytes("x"), None);
    }

    #[test]
    fn cancel_applies_to_the_running_statement_only() {
        let c = Control::new(1);
        c.begin_statement(None);
        assert!(c.check().is_ok());
        c.cancel();
        assert_eq!(c.check().unwrap_err().code, "57014");
        // An idle session's cancel is for what already ended.
        c.end_statement(false, None);
        c.begin_statement(None);
        assert!(c.check().is_ok());
    }

    #[test]
    fn terminate_outlives_statements_and_a_cancel_cannot_undo_it() {
        let c = Control::new(1);
        c.terminate();
        c.cancel();
        c.begin_statement(None);
        assert_eq!(c.check().unwrap_err().code, "57P01");
        assert!(c.fatal().is_some());
    }

    #[test]
    fn deadlines_expire_and_end_with_the_statement() {
        let c = Control::new(1);
        c.begin_statement(Some(Duration::from_nanos(1)));
        std::thread::sleep(Duration::from_millis(2));
        assert!(c.check().unwrap_err().message.contains("statement timeout"));
        c.end_statement(false, None);
        assert!(c.check().is_ok());
    }

    #[test]
    fn a_cancel_request_needs_the_secret() {
        let r = Arc::new(Registry::default());
        let c = r.register(7);
        c.begin_statement(None);
        r.cancel_request(7, c.secret.wrapping_add(1));
        assert!(c.check().is_ok());
        r.cancel_request(8, c.secret);
        assert!(c.check().is_ok());
        r.cancel_request(7, c.secret);
        assert!(c.check().is_err());
    }
}
