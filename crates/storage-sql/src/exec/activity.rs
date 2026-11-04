//! What is running and what has run (`F-ENT-03`, `TASK-147`): the data behind
//! `pg_stat_activity`, `pg_stat_statements`, `pg_locks`, MySQL's process list and digests, and
//! SQL Server's `sys.dm_exec_*`.
//!
//! Sessions publish their state in their [`crate::exec::control::Control`]. Statement statistics
//! are kept per gateway, by (user, database, text with literals replaced), up to
//! [`MAX_STATEMENTS`] entries; the least used entry makes room.
//!
//! As in the systems imitated, a role that is no superuser sees other sessions but not their
//! statements (PostgreSQL), or only its own sessions (MySQL without `PROCESS`, SQL Server without
//! `VIEW SERVER STATE`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::datum::Datum;
use crate::exec::control::{Control, Identity};
use crate::exec::eval::Exec;
use crate::exec::{now_ns, Outcome, Session};
use crate::lexer::{tokenize, Dialect, Token};

/// Statements remembered per gateway.
pub const MAX_STATEMENTS: usize = 5000;
/// Longest statement text kept.
const MAX_TEXT: usize = 4096;

/// Statistics of one normalized statement.
#[derive(Debug, Clone, Default)]
pub struct StatementStat {
    pub user: String,
    pub database: String,
    pub text: String,
    /// Stable id of the normalized text.
    pub id: i64,
    pub calls: u64,
    pub rows: u64,
    pub total_ns: u64,
    pub min_ns: u64,
    pub max_ns: u64,
}

#[derive(Debug, Default)]
pub struct Statements {
    map: Mutex<HashMap<(String, String, String), StatementStat>>,
}

impl Statements {
    fn record(&self, user: &str, database: &str, text: String, ns: u64, rows: u64) {
        let mut m = self.map.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let key = (user.to_owned(), database.to_owned(), text);
        if !m.contains_key(&key) && m.len() >= MAX_STATEMENTS {
            if let Some(k) = m.iter().min_by_key(|(_, s)| s.calls).map(|(k, _)| k.clone()) {
                m.remove(&k);
            }
        }
        let e = m.entry(key).or_insert_with_key(|k| {
            let digest = crate::hash::sha256(k.2.as_bytes());
            let mut id = [0u8; 8];
            id.copy_from_slice(&digest[..8]);
            StatementStat { user: k.0.clone(), database: k.1.clone(), text: k.2.clone(), id: i64::from_le_bytes(id), min_ns: u64::MAX, ..StatementStat::default() }
        });
        e.calls += 1;
        e.rows += rows;
        e.total_ns += ns;
        e.min_ns = e.min_ns.min(ns);
        e.max_ns = e.max_ns.max(ns);
    }

    pub fn all(&self) -> Vec<StatementStat> {
        self.map.lock().unwrap_or_else(std::sync::PoisonError::into_inner).values().cloned().collect()
    }

    pub fn reset(&self) {
        self.map.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clear();
    }
}

/// `sql` with its literals replaced by `$1`, `$2`, ...; `None` if it does not tokenize.
pub fn normalize(sql: &str, dialect: Dialect) -> Option<String> {
    let (tokens, _) = tokenize(sql, dialect).ok()?;
    let mut out = String::with_capacity(sql.len().min(MAX_TEXT));
    let mut n = 0;
    let mut literal = |out: &mut String| {
        n += 1;
        out.push('$');
        out.push_str(&n.to_string());
    };
    let mut after_join = true;
    for t in &tokens {
        if out.len() >= MAX_TEXT {
            break;
        }
        let glue = matches!(t, Token::Op("," | ")" | "." | ";")) || after_join;
        if !glue {
            out.push(' ');
        }
        after_join = matches!(t, Token::Op("(" | "."));
        match t {
            Token::Word { text, quoted: false } => out.push_str(text),
            Token::Word { text, quoted: true } => {
                out.push('"');
                out.push_str(text);
                out.push('"');
            }
            Token::Number(_) | Token::Str(_) | Token::Blob(_) | Token::Param(_) => literal(&mut out),
            Token::Variable { name, system } => {
                out.push_str(if *system { "@@" } else { "@" });
                out.push_str(name);
            }
            Token::Op(o) => out.push_str(o),
        }
    }
    Some(out)
}

impl Session {
    /// The text of the statements about to run (wire protocols call this).
    pub fn note_sql(&mut self, sql: &str) {
        let text: String = sql.chars().take(MAX_TEXT).collect();
        self.sql_text = Some(Arc::from(text));
    }

    pub(crate) fn activity_start(&self) {
        let now = now_ns();
        let in_transaction = self.in_transaction();
        let mut id = self.control.identity.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if id.user != self.user {
            id.user.clone_from(&self.user);
        }
        id.role.clone_from(&self.role);
        if id.database != self.database {
            id.database.clone_from(&self.database);
        }
        id.dialect = match self.dialect {
            Dialect::MySql => "mysql",
            Dialect::MsSql => "mssql",
            _ => "postgres",
        };
        if let Some(a) = self.vars.get("application_name") {
            if id.application != *a {
                id.application.clone_from(a);
            }
        }
        if let Some(sql) = &self.sql_text {
            id.query.clear();
            id.query.push_str(sql);
        }
        id.running = true;
        id.query_start_ns = now;
        id.state_change_ns = now;
        if in_transaction && id.transaction_start_ns == 0 {
            id.transaction_start_ns = now;
        }
    }

    pub(crate) fn activity_end(&self, out: &crate::error::SqlResult<Outcome>, started_ns: i64) {
        let now = now_ns();
        let in_transaction = self.in_transaction();
        let failed = self.transaction_failed();
        {
            let mut id = self.control.identity.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            id.running = false;
            id.state_change_ns = now;
            id.in_transaction = in_transaction;
            id.failed_transaction = failed;
            id.wait = None;
            if in_transaction {
                if id.transaction_start_ns == 0 {
                    id.transaction_start_ns = started_ns;
                }
            } else {
                id.transaction_start_ns = 0;
            }
        }
        // Successful statements only, as `pg_stat_statements`.
        let Ok(o) = out else { return };
        let Some(sql) = &self.sql_text else { return };
        let Some(text) = normalize(sql, self.dialect) else { return };
        let rows = match o {
            Outcome::Rows(rs) => rs.rows.len() as u64,
            Outcome::Command { affected, .. } => *affected,
        };
        self.engine.registry.statements.record(self.current_role(), &self.database, text, now.saturating_sub(started_ns).max(0) as u64, rows);
    }
}

/// One session as a catalog shows it.
#[derive(Debug)]
pub struct SessionView {
    pub id: u64,
    pub identity: Identity,
    /// Whether the viewer may read its statement.
    pub full: bool,
}

/// Every session, for this viewer.
pub fn sessions(ex: &Exec<'_>) -> crate::error::SqlResult<Vec<SessionView>> {
    let acl = ex.s.engine.acl()?;
    let me = ex.s.current_role();
    let all_seen = acl.is_superuser(me);
    let mine = crate::exec::acl::role_key(&ex.s.user);
    let snapshot = |c: &Arc<Control>| c.identity.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
    let mut out: Vec<SessionView> = ex
        .s
        .engine
        .registry
        .all()
        .iter()
        .filter(|c| c.id == ex.s.id || c.is_serving())
        .map(|c| {
            let identity = snapshot(c);
            let full = all_seen || c.id == ex.s.id || crate::exec::acl::role_key(&identity.user) == mine;
            SessionView { id: c.id, identity, full }
        })
        .collect();
    out.sort_by_key(|s| s.id);
    Ok(out)
}

pub const INSUFFICIENT: &str = "<insufficient privilege>";

pub fn state_name(i: &Identity) -> &'static str {
    match (i.running, i.in_transaction, i.failed_transaction) {
        (true, _, _) => "active",
        (false, true, true) => "idle in transaction (aborted)",
        (false, true, false) => "idle in transaction",
        _ => "idle",
    }
}

pub fn timestamp(ns: i64) -> Datum {
    if ns == 0 {
        Datum::Null
    } else {
        Datum::Timestamp(ns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizing_hides_literals_and_groups_calls() {
        let a = normalize("SELECT * FROM t WHERE id = 5 AND name = 'bob'", Dialect::Postgres).unwrap();
        let b = normalize("select * from t where id = 7 and name = 'secret value'", Dialect::Postgres).unwrap();
        assert_eq!(a.to_lowercase(), b.to_lowercase());
        assert!(a.contains("$1") && a.contains("$2") && !a.contains("bob"), "{a}");
        assert_ne!(a, normalize("SELECT * FROM u WHERE id = 5", Dialect::Postgres).unwrap());
        assert_eq!(normalize("SELECT $1", Dialect::Postgres).unwrap(), "SELECT $1");
    }

    #[test]
    fn statements_are_bounded() {
        let s = Statements::default();
        for i in 0..MAX_STATEMENTS + 50 {
            s.record("u", "d", format!("q{i}"), 1, 1);
            s.record("u", "d", "hot".into(), 5, 2);
        }
        let all = s.all();
        assert!(all.len() <= MAX_STATEMENTS);
        let hot = all.iter().find(|x| x.text == "hot").expect("the most used stays");
        assert_eq!((hot.calls, hot.total_ns, hot.min_ns, hot.max_ns), ((MAX_STATEMENTS + 50) as u64, (MAX_STATEMENTS as u64 + 50) * 5, 5, 5));
        s.reset();
        assert!(s.all().is_empty());
    }
}
