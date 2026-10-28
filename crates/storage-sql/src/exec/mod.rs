//! Statement execution: sessions, transactions and dispatch.
//!
//! **Transactions.** Every statement runs inside a transaction: an explicit one (`BEGIN` …
//! `COMMIT`) or an implicit one per statement (autocommit). Writes are buffered in an overlay
//! (reads see them) and committed as one atomic batch. Every written row carries the version
//! it had when this transaction read it (or "absent" for inserts), so a concurrent change makes
//! the commit fail instead of being overwritten: **no lost updates**. Autocommit statements
//! that lose such a race are re-run transparently; an explicit transaction reports SQLSTATE
//! `40001` (serialization failure), which clients and ORMs retry. Unique constraints, primary
//! keys and foreign-key parents are verified by preconditions in the same batch.

mod ddl;
mod dml;
mod eval;
mod func;
mod regex;
mod select;
pub mod system;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use storine::Doc;

use crate::ast::{DataType, Expr, Statement};
use crate::catalog::{Catalog, Table, TABLES};
use crate::datum::Datum;
use crate::error::{SqlError, SqlResult};
use crate::lexer::Dialect;
use crate::store::{Op, Pre, Store, Tag, Write};

pub use eval::walk as eval_walk;
pub use select::Rel;

/// Settings of a gateway.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Database name reported to clients (`current_database()`, `DATABASE()`).
    pub database: String,
    /// How long a statement waits for a row lock.
    pub lock_timeout: Duration,
    /// How many times an autocommit statement is re-run after losing a write race.
    pub max_retries: u32,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig { database: "storine".into(), lock_timeout: Duration::from_secs(10), max_retries: 64 }
    }
}

/// State shared by every session of a gateway.
#[derive(Debug)]
pub struct Engine {
    pub store: Store,
    pub catalog: Catalog,
    pub config: EngineConfig,
    next_session: AtomicU64,
}

impl Engine {
    pub fn new(store: Store, config: EngineConfig) -> Arc<Engine> {
        Arc::new(Engine { store, catalog: Catalog::default(), config, next_session: AtomicU64::new(1) })
    }

    /// Re-reads every table schema when the cache is stale.
    pub fn refresh_catalog(&self, force: bool) -> SqlResult<()> {
        if !force && self.catalog.is_fresh() {
            return Ok(());
        }
        let mut tables = Vec::new();
        for d in self.store.scan(TABLES, None)? {
            tables.push(Table::from_catalog_doc(&d.fields, d.version)?);
        }
        self.catalog.replace_all(tables);
        Ok(())
    }

    /// The schema of `name`, re-reading the catalog once if it is unknown.
    pub fn table(&self, name: &str) -> SqlResult<Arc<Table>> {
        self.refresh_catalog(false)?;
        if let Some(t) = self.catalog.get(name) {
            return Ok(t);
        }
        self.refresh_catalog(true)?;
        self.catalog.get(name).ok_or_else(|| SqlError::undefined_table(name))
    }

    pub fn find_table(&self, name: &str) -> SqlResult<Option<Arc<Table>>> {
        match self.table(name) {
            Ok(t) => Ok(Some(t)),
            Err(e) if e.code == "42P01" => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn new_session(self: &Arc<Self>, dialect: Dialect, user: &str, database: Option<&str>) -> Session {
        let id = self.next_session.fetch_add(1, Ordering::Relaxed);
        Session {
            engine: Arc::clone(self),
            dialect,
            user: user.to_owned(),
            database: database.filter(|d| !d.is_empty()).unwrap_or(&self.config.database).to_owned(),
            vars: default_vars(dialect),
            user_vars: HashMap::new(),
            txn: None,
            last_insert_id: 0,
            prepared: HashMap::new(),
            id,
            last_found_rows: 0,
        }
    }
}

fn default_vars(dialect: Dialect) -> HashMap<String, String> {
    let mut v = HashMap::new();
    for (k, val) in [
        ("timezone", "UTC"),
        ("time_zone", "+00:00"),
        ("client_encoding", "UTF8"),
        ("server_encoding", "UTF8"),
        ("datestyle", "ISO, MDY"),
        ("standard_conforming_strings", "on"),
        ("search_path", "\"$user\", public"),
        ("transaction_isolation", if dialect == Dialect::MySql { "REPEATABLE-READ" } else { "read committed" }),
        ("transaction_read_only", "off"),
        ("sql_mode", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"),
        ("autocommit", "1"),
        ("character_set_client", "utf8mb4"),
        ("character_set_connection", "utf8mb4"),
        ("character_set_results", "utf8mb4"),
        ("collation_connection", "utf8mb4_0900_ai_ci"),
        ("max_allowed_packet", "67108864"),
        ("lower_case_table_names", "0"),
        ("wait_timeout", "28800"),
        ("interactive_timeout", "28800"),
        ("lock_wait_timeout", "10"),
        ("innodb_lock_wait_timeout", "10"),
        ("integer_datetimes", "on"),
        ("intervalstyle", "postgres"),
        ("is_superuser", "on"),
        ("application_name", ""),
        ("extra_float_digits", "1"),
        ("max_identifier_length", "63"),
    ] {
        v.insert(k.to_owned(), val.to_owned());
    }
    v
}

/// Column metadata of a result.
#[derive(Debug, Clone, PartialEq)]
pub struct OutCol {
    pub name: String,
    pub ty: DataType,
    /// Source table and column (for drivers that report it).
    pub table: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ResultSet {
    pub columns: Vec<OutCol>,
    pub rows: Vec<Vec<Datum>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Rows(ResultSet),
    /// A command: its completion tag (`INSERT 0 3`, `UPDATE 2`, `BEGIN`), rows affected and the
    /// first auto-generated id (MySQL `LAST_INSERT_ID()`).
    Command { tag: String, affected: u64, last_insert_id: Option<i64> },
}

impl Outcome {
    pub fn command(tag: impl Into<String>) -> Outcome {
        Outcome::Command { tag: tag.into(), affected: 0, last_insert_id: None }
    }
}

/// Transaction state: buffered writes (with the version each was based on) and checks.
#[derive(Debug, Default)]
pub struct Txn {
    pub explicit: bool,
    /// A failed statement aborted the transaction (PostgreSQL semantics): only ROLLBACK works.
    pub failed: bool,
    overlay: HashMap<(String, String), Entry>,
    order: Vec<(String, String)>,
    checks: Vec<Write>,
    journal: Vec<Undo>,
    savepoints: Vec<(String, usize)>,
    /// Row locks held (lock-document keys), released at commit / rollback.
    pub(crate) locks: Vec<String>,
}

#[derive(Debug, Clone)]
struct Entry {
    /// What the document was when this transaction first touched it.
    base: Pre,
    doc: Option<Doc>,
    tag: Tag,
}

#[derive(Debug)]
enum Undo {
    Overlay { key: (String, String), prev: Option<Entry>, new_in_order: bool },
    Check,
}

impl Txn {
    /// The buffered state of a document: `Some(None)` deleted, `Some(Some(doc))` written,
    /// `None` untouched.
    pub fn get(&self, collection: &str, key: &str) -> Option<Option<&Doc>> {
        self.overlay.get(&(collection.to_owned(), key.to_owned())).map(|e| e.doc.as_ref())
    }

    /// Buffered documents of `collection` (key, doc or deletion).
    pub fn entries<'a>(&'a self, collection: &'a str) -> impl Iterator<Item = (&'a str, Option<&'a Doc>)> + 'a {
        self.order
            .iter()
            .filter(move |(c, _)| c == collection)
            .filter_map(move |k| self.overlay.get(k).map(|e| (k.1.as_str(), e.doc.as_ref())))
    }

    /// Buffers a write of `doc` (`None`: delete). `observed` is the version this transaction read
    /// (`None`: the document did not exist), used only the first time the key is touched.
    pub fn write(&mut self, collection: &str, key: &str, observed: Option<u64>, doc: Option<Doc>, tag: Tag) {
        let k = (collection.to_owned(), key.to_owned());
        let prev = self.overlay.get(&k).cloned();
        let new_in_order = prev.is_none();
        let base = match &prev {
            Some(e) => e.base,
            None => observed.map_or(Pre::Absent, Pre::Version),
        };
        self.overlay.insert(k.clone(), Entry { base, doc, tag });
        if new_in_order {
            self.order.push(k.clone());
        }
        self.journal.push(Undo::Overlay { key: k, prev, new_in_order });
    }

    /// Adds a commit-time check (e.g. a foreign-key parent must still exist).
    pub fn check(&mut self, w: Write) {
        self.checks.push(w);
        self.journal.push(Undo::Check);
    }

    pub fn mark(&self) -> usize {
        self.journal.len()
    }

    /// Undoes every change made after `mark` (failed statement, ROLLBACK TO SAVEPOINT).
    pub fn rollback_to(&mut self, mark: usize) {
        while self.journal.len() > mark {
            match self.journal.pop() {
                Some(Undo::Overlay { key, prev, new_in_order }) => {
                    match prev {
                        Some(e) => {
                            self.overlay.insert(key.clone(), e);
                        }
                        None => {
                            self.overlay.remove(&key);
                        }
                    }
                    if new_in_order {
                        if let Some(p) = self.order.iter().rposition(|k| *k == key) {
                            self.order.remove(p);
                        }
                    }
                }
                Some(Undo::Check) => {
                    self.checks.pop();
                }
                None => break,
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty() && self.checks.is_empty()
    }

    /// The atomic batch that commits this transaction.
    pub fn batch(&self) -> Vec<Write> {
        let mut out = Vec::with_capacity(self.order.len() + self.checks.len());
        for k in &self.order {
            let Some(e) = self.overlay.get(k) else { continue };
            let op = match (&e.doc, e.base) {
                (None, Pre::Absent) => continue, // created and deleted again
                (Some(d), _) => Op::Put(d.clone()),
                (None, _) => Op::Delete,
            };
            out.push(Write { collection: k.0.clone(), key: k.1.clone(), op, pre: e.base, tag: e.tag.clone() });
        }
        for c in &self.checks {
            // A check on a document this transaction also writes is covered by that write.
            if !self.overlay.contains_key(&(c.collection.clone(), c.key.clone())) {
                out.push(c.clone());
            }
        }
        out
    }
}

/// One client connection's state.
#[derive(Debug)]
pub struct Session {
    pub engine: Arc<Engine>,
    pub dialect: Dialect,
    pub user: String,
    pub database: String,
    pub vars: HashMap<String, String>,
    pub user_vars: HashMap<String, Datum>,
    pub txn: Option<Txn>,
    pub last_insert_id: i64,
    pub prepared: HashMap<String, (Statement, usize)>,
    pub id: u64,
    pub last_found_rows: u64,
}

/// Nanoseconds since the epoch, now.
pub fn now_ns() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as i64).unwrap_or(0)
}

impl Session {
    pub fn in_transaction(&self) -> bool {
        self.txn.as_ref().is_some_and(|t| t.explicit)
    }

    pub fn transaction_failed(&self) -> bool {
        self.txn.as_ref().is_some_and(|t| t.explicit && t.failed)
    }

    /// Runs one statement with bound parameters.
    pub fn execute(&mut self, stmt: &Statement, params: &[Datum]) -> SqlResult<Outcome> {
        // Transaction control first: it works even in a failed transaction.
        match stmt {
            Statement::Begin => {
                if self.in_transaction() {
                    // PostgreSQL warns and continues; MySQL commits the open transaction.
                    if self.dialect == Dialect::MySql {
                        self.commit()?;
                    } else {
                        return Ok(Outcome::command("BEGIN"));
                    }
                }
                self.txn = Some(Txn { explicit: true, ..Txn::default() });
                return Ok(Outcome::command("BEGIN"));
            }
            Statement::Commit => {
                if self.transaction_failed() {
                    self.rollback();
                    return Ok(Outcome::command("ROLLBACK"));
                }
                self.commit()?;
                return Ok(Outcome::command("COMMIT"));
            }
            Statement::Rollback => {
                self.rollback();
                return Ok(Outcome::command("ROLLBACK"));
            }
            Statement::Savepoint(name) => {
                let txn = self.txn.get_or_insert_with(|| Txn { explicit: true, ..Txn::default() });
                if txn.failed {
                    return Err(SqlError::in_failed_transaction());
                }
                let mark = txn.mark();
                txn.savepoints.push((name.to_ascii_lowercase(), mark));
                return Ok(Outcome::command("SAVEPOINT"));
            }
            Statement::ReleaseSavepoint(name) => {
                if let Some(txn) = self.txn.as_mut() {
                    let n = name.to_ascii_lowercase();
                    match txn.savepoints.iter().rposition(|(s, _)| *s == n) {
                        Some(p) => txn.savepoints.truncate(p),
                        None => return Err(SqlError::new("3B001", format!("savepoint \"{name}\" does not exist"))),
                    }
                }
                return Ok(Outcome::command("RELEASE"));
            }
            Statement::RollbackToSavepoint(name) => {
                let Some(txn) = self.txn.as_mut() else {
                    return Err(SqlError::new("3B001", format!("savepoint \"{name}\" does not exist")));
                };
                let n = name.to_ascii_lowercase();
                let Some(p) = txn.savepoints.iter().rposition(|(s, _)| *s == n) else {
                    return Err(SqlError::new("3B001", format!("savepoint \"{name}\" does not exist")));
                };
                let mark = txn.savepoints[p].1;
                txn.savepoints.truncate(p + 1);
                txn.rollback_to(mark);
                txn.failed = false;
                return Ok(Outcome::command("ROLLBACK"));
            }
            _ => {}
        }
        if self.transaction_failed() {
            return Err(SqlError::in_failed_transaction());
        }
        // MySQL `autocommit = 0`: every statement runs in an open transaction until COMMIT.
        if !self.in_transaction() && self.vars.get("autocommit").is_some_and(|v| v == "0" || v == "off") && !matches!(stmt, Statement::Set { .. }) {
            self.txn = Some(Txn { explicit: true, ..Txn::default() });
        }
        if self.in_transaction() {
            let mark = self.txn.as_ref().map_or(0, Txn::mark);
            let r = self.run(stmt, params);
            if let Err(e) = &r {
                if let Some(txn) = self.txn.as_mut() {
                    // The statement's own changes are undone either way; PostgreSQL also aborts
                    // the transaction, MySQL keeps it usable.
                    txn.rollback_to(mark);
                    if self.dialect != Dialect::MySql && e.code != "0A000" {
                        txn.failed = true;
                    }
                }
            }
            return r;
        }
        // Autocommit: an implicit transaction per statement, re-run if it loses a write race.
        let mut delay = Duration::from_millis(1);
        let mut attempt = 0;
        loop {
            self.txn = Some(Txn::default());
            let r = self.run(stmt, params);
            let outcome = match r {
                Ok(o) => o,
                Err(e) => {
                    self.release_locks();
                    self.txn = None;
                    return Err(e);
                }
            };
            if self.in_transaction() {
                // The statement opened an explicit transaction (SET autocommit = 0).
                return Ok(outcome);
            }
            match self.try_commit() {
                Ok(()) => {
                    self.txn = None;
                    return Ok(outcome);
                }
                Err((e, retryable)) => {
                    self.release_locks();
                    self.txn = None;
                    attempt += 1;
                    if !retryable || attempt > self.engine.config.max_retries || !is_retry_safe(stmt) {
                        return Err(e);
                    }
                    std::thread::sleep(delay);
                    delay = (delay * 2).min(Duration::from_millis(50));
                }
            }
        }
    }

    /// Commits the open transaction (no-op without one).
    pub fn commit(&mut self) -> SqlResult<()> {
        match self.try_commit() {
            Ok(()) => {
                self.txn = None;
                Ok(())
            }
            Err((e, _)) => {
                self.release_locks();
                self.txn = None;
                Err(e)
            }
        }
    }

    fn try_commit(&mut self) -> Result<(), (SqlError, bool)> {
        let Some(txn) = self.txn.as_ref() else { return Ok(()) };
        let mut batch = txn.batch();
        // Row locks are released in the same atomic batch.
        for k in &txn.locks {
            batch.push(Write { collection: crate::catalog::LOCKS.into(), key: k.clone(), op: Op::Delete, pre: Pre::None, tag: Tag::Lock });
        }
        match self.engine.store.commit(&batch) {
            Ok(_) => {
                if let Some(t) = self.txn.as_mut() {
                    t.locks.clear();
                }
                Ok(())
            }
            Err(e) => Err((e.to_sql(&batch), e.is_retryable(&batch))),
        }
    }

    pub fn rollback(&mut self) {
        self.release_locks();
        self.txn = None;
    }

    fn release_locks(&mut self) {
        let Some(txn) = self.txn.as_mut() else { return };
        if txn.locks.is_empty() {
            return;
        }
        let batch: Vec<Write> = txn
            .locks
            .drain(..)
            .map(|k| Write { collection: crate::catalog::LOCKS.into(), key: k, op: Op::Delete, pre: Pre::None, tag: Tag::Lock })
            .collect();
        // Best effort: an unreleased lock expires on its own.
        let _ = self.engine.store.commit(&batch);
    }

    /// The open transaction (every statement runs inside one).
    pub(crate) fn txn_mut(&mut self) -> &mut Txn {
        self.txn.get_or_insert_with(Txn::default)
    }

    fn run(&mut self, stmt: &Statement, params: &[Datum]) -> SqlResult<Outcome> {
        match stmt {
            Statement::Query(q) => {
                if q.lock.is_some() && self.in_transaction() {
                    // Lock first, so the query reads the rows' latest values.
                    dml::lock_query_rows(self, q, params)?;
                }
                let ex = eval::Exec::new(self, params);
                let rel = ex.query(q, None)?;
                let rs = ex.result_set(rel)?;
                drop(ex);
                self.last_found_rows = rs.rows.len() as u64;
                Ok(Outcome::Rows(rs))
            }
            Statement::Insert(i) => dml::insert(self, i, params),
            Statement::Update(u) => dml::update(self, u, params),
            Statement::Delete(d) => dml::delete(self, d, params),
            Statement::CreateTable(c) => ddl::create_table(self, c, params),
            Statement::CreateIndex(c) => ddl::create_index(self, c),
            Statement::DropTable { names, if_exists } => ddl::drop_tables(self, names, *if_exists),
            Statement::DropIndex { names, table, if_exists } => ddl::drop_indexes(self, names, table.as_deref(), *if_exists),
            Statement::AlterTable { table, ops } => ddl::alter_table(self, table, ops),
            Statement::Truncate { tables } => ddl::truncate(self, tables),
            Statement::Set { name, value } => self.set_var(name, value.as_ref(), params),
            Statement::Show(s) => system::show(self, s),
            Statement::Use(db) => {
                self.database = db.clone();
                Ok(Outcome::command("USE"))
            }
            Statement::Explain(inner) => {
                let text = format!("Storine SQL plan: {}", explain_text(inner));
                Ok(Outcome::Rows(ResultSet {
                    columns: vec![OutCol { name: "QUERY PLAN".into(), ty: DataType::Text, table: None }],
                    rows: vec![vec![Datum::Text(text)]],
                }))
            }
            Statement::CreateSchema { .. } => Ok(Outcome::command(if self.dialect == Dialect::MySql { "CREATE DATABASE" } else { "CREATE SCHEMA" })),
            Statement::DropSchema { .. } => Ok(Outcome::command("DROP SCHEMA")),
            Statement::Prepare { name, statement } => {
                self.prepared.insert(name.to_ascii_lowercase(), ((**statement).clone(), 0));
                Ok(Outcome::command("PREPARE"))
            }
            Statement::Execute { name, args } => {
                let (stmt, _) = self
                    .prepared
                    .get(&name.to_ascii_lowercase())
                    .cloned()
                    .ok_or_else(|| SqlError::new("26000", format!("prepared statement \"{name}\" does not exist")))?;
                let values = {
                    let ex = eval::Exec::new(self, params);
                    args.iter().map(|a| ex.eval_const(a)).collect::<SqlResult<Vec<_>>>()?
                };
                self.run(&stmt, &values)
            }
            Statement::Deallocate(name) => {
                match name {
                    Some(n) => {
                        self.prepared.remove(&n.to_ascii_lowercase());
                    }
                    None => self.prepared.clear(),
                }
                Ok(Outcome::command("DEALLOCATE"))
            }
            Statement::NoOp(tag) => Ok(Outcome::command(*tag)),
            Statement::Empty => Ok(Outcome::command("")),
            Statement::Begin
            | Statement::Commit
            | Statement::Rollback
            | Statement::Savepoint(_)
            | Statement::ReleaseSavepoint(_)
            | Statement::RollbackToSavepoint(_) => Err(SqlError::internal("transaction control in run()")),
        }
    }

    fn set_var(&mut self, name: &str, value: Option<&Expr>, params: &[Datum]) -> SqlResult<Outcome> {
        let v = match value {
            None => Datum::Null,
            Some(e) => eval::Exec::new(self, params).eval_const(e)?,
        };
        if let Some(var) = name.strip_prefix('@') {
            self.user_vars.insert(var.to_ascii_lowercase(), v);
            return Ok(Outcome::command("SET"));
        }
        let text = match &v {
            Datum::Null => default_vars(self.dialect).get(name).cloned().unwrap_or_default(),
            other => other.to_text(self.dialect == Dialect::MySql),
        };
        let key = match name {
            "time zone" => "timezone",
            "transaction_isolation" | "tx_isolation" => "transaction_isolation",
            other => other,
        };
        if key == "autocommit" && matches!(text.as_str(), "0" | "off" | "false") {
            // MySQL autocommit=0: an implicit transaction stays open until COMMIT.
            if !self.in_transaction() {
                self.txn = Some(Txn { explicit: true, ..Txn::default() });
            }
        } else if key == "autocommit" && self.in_transaction() {
            self.commit()?;
        }
        self.vars.insert(key.to_owned(), text);
        Ok(Outcome::command("SET"))
    }
}

/// Whether re-running a statement after a lost write race is safe (it has no external effect).
fn is_retry_safe(stmt: &Statement) -> bool {
    !matches!(stmt, Statement::CreateTable(_) | Statement::DropTable { .. } | Statement::AlterTable { .. })
}

fn explain_text(stmt: &Statement) -> String {
    match stmt {
        Statement::Query(_) => "scan with filter push-down, in-memory joins and sorting".into(),
        Statement::Insert(_) => "insert with primary-key and unique preconditions".into(),
        Statement::Update(_) | Statement::Delete(_) => "read matching rows, write with version preconditions".into(),
        _ => "utility statement".into(),
    }
}

/// Result columns of `RETURNING items` on `table` (for protocol-level Describe).
pub fn describe_returning(s: &Session, table: &str, items: &[crate::ast::SelectItem]) -> SqlResult<Vec<OutCol>> {
    dml::describe_returning(s, table, items)
}
