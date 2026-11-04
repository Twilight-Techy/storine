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

pub mod acl;
pub mod activity;
pub(crate) mod cursor;
pub mod notify;
pub(crate) mod routine;
pub(crate) mod temp;
pub(crate) mod advisory;
pub mod control;
pub mod explain;
pub(crate) mod optimizer;
pub mod stats;
pub(crate) mod ddl;
pub(crate) mod dml;
mod eval;
pub(crate) mod func;
mod merge;
pub mod mssql;
mod pushdown;
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
    /// The `--user` accounts, superusers by configuration; `None`: trust mode (`exec/acl.rs`).
    pub bootstrap_users: Option<std::collections::HashSet<String>>,
    /// Most bytes of rows one statement may hold (`max_query_memory`, `exec/control.rs`).
    pub max_query_memory: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            database: "storine".into(),
            lock_timeout: Duration::from_secs(10),
            max_retries: 64,
            bootstrap_users: None,
            max_query_memory: 4 << 30,
        }
    }
}

/// State shared by every session of one database of a gateway.
#[derive(Debug)]
pub struct Engine {
    pub store: Store,
    pub catalog: Catalog,
    pub config: EngineConfig,
    next_session: Arc<AtomicU64>,
    databases: Arc<Databases>,
    /// Roles and grants, as last read.
    pub(crate) acl_cache: std::sync::RwLock<Option<(std::time::Instant, Arc<acl::Acl>)>>,
    /// The gateway's sessions.
    pub registry: Arc<control::Registry>,
    /// Table statistics, as last read.
    #[allow(clippy::type_complexity)]
    pub(crate) stats_cache: std::sync::RwLock<Option<(std::time::Instant, Arc<HashMap<String, stats::TableStats>>)>>,
}

/// The databases of a gateway: the default one (`EngineConfig::database`) and those made by
/// `CREATE DATABASE`, each in its own collection namespace. Names never created share the
/// default database, so clients that connect with any database name keep working.
#[derive(Debug)]
struct Databases {
    root: Store,
    default_name: String,
    default: std::sync::Weak<Engine>,
    open: std::sync::Mutex<HashMap<String, std::sync::Weak<Engine>>>,
}

/// Registry of created databases (in the default namespace).
const DATABASES: &str = "_sql_databases";

/// Collection namespace of a created database.
fn database_prefix(key: &str) -> String {
    let clean: String = key
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .take(32)
        .collect();
    if clean == key {
        format!("{}{clean}.", crate::store::DATABASE_PREFIX)
    } else {
        let h = crate::hash::hex(&crate::hash::sha256(key.as_bytes()));
        format!("{}{clean}_{}.", crate::store::DATABASE_PREFIX, &h[..8])
    }
}

impl Engine {
    /// The gateway's default database over `store`.
    pub fn new(store: Store, config: EngineConfig) -> Arc<Engine> {
        Arc::new_cyclic(|me| Engine {
            databases: Arc::new(Databases {
                root: store.namespaced(""),
                default_name: config.database.clone(),
                default: me.clone(),
                open: std::sync::Mutex::new(HashMap::new()),
            }),
            store,
            catalog: Catalog::default(),
            config,
            next_session: Arc::new(AtomicU64::new(1)),
            acl_cache: std::sync::RwLock::new(None),
            registry: Arc::default(),
            stats_cache: std::sync::RwLock::new(None),
        })
    }

    /// The store of what is global to the gateway (roles, the database registry).
    pub(crate) fn root_store(&self) -> &Store {
        &self.databases.root
    }

    /// Whether `name` names this database.
    pub(crate) fn is_same_database(&self, name: &str) -> bool {
        name.eq_ignore_ascii_case(&self.config.database) || (self.is_default_name(name) && self.is_default_name(&self.config.database))
    }

    /// A view's owner: `None` no such view; `Some(None)`: made before roles existed.
    pub(crate) fn view_owner(&self, name: &str) -> SqlResult<Option<Option<String>>> {
        Ok(self.store.get(crate::catalog::VIEWS, &name.to_lowercase())?.map(|d| d.fields.get("owner").and_then(storine::Value::as_str).map(str::to_owned)))
    }

    fn default_engine(&self) -> SqlResult<Arc<Engine>> {
        self.databases
            .default
            .upgrade()
            .ok_or_else(|| SqlError::new("57P01", "the gateway is shutting down"))
    }

    fn is_default_name(&self, name: &str) -> bool {
        name.is_empty() || name.eq_ignore_ascii_case(&self.databases.default_name)
    }

    fn is_created(&self, name: &str) -> SqlResult<bool> {
        Ok(self
            .databases
            .root
            .get(DATABASES, &name.to_lowercase())?
            .is_some())
    }

    /// The engine serving database `name` (the default one for names never created).
    pub fn database(&self, name: &str) -> SqlResult<Arc<Engine>> {
        if self.is_default_name(name) || !self.is_created(name)? {
            return self.default_engine();
        }
        let key = name.to_lowercase();
        let mut open = self
            .databases
            .open
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(e) = open.get(&key).and_then(std::sync::Weak::upgrade) {
            return Ok(e);
        }
        let e = Arc::new(Engine {
            store: self.databases.root.namespaced(&database_prefix(&key)),
            catalog: Catalog::default(),
            config: EngineConfig {
                database: name.to_owned(),
                ..self.config.clone()
            },
            next_session: Arc::clone(&self.next_session),
            databases: Arc::clone(&self.databases),
            acl_cache: std::sync::RwLock::new(None),
            registry: Arc::clone(&self.registry),
            stats_cache: std::sync::RwLock::new(None),
        });
        open.insert(key, Arc::downgrade(&e));
        Ok(e)
    }

    /// The default database and every created one.
    pub fn database_names(&self) -> SqlResult<Vec<String>> {
        let mut names = vec![self.databases.default_name.clone()];
        for d in self.databases.root.scan(DATABASES, None)? {
            if let Some(n) = d.fields.get("name").and_then(storine::Value::as_str) {
                names.push(n.to_owned());
            }
        }
        Ok(names)
    }

    /// A view's definition: `(sql, dialect, column names)`.
    pub fn view(&self, name: &str) -> SqlResult<Option<(String, Dialect, Vec<String>)>> {
        let Some(d) = self
            .store
            .get(crate::catalog::VIEWS, &name.to_lowercase())?
        else {
            return Ok(None);
        };
        let text = |k: &str| {
            d.fields
                .get(k)
                .and_then(storine::Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        let dialect = if text("dialect") == "mysql" {
            Dialect::MySql
        } else {
            Dialect::Postgres
        };
        let columns = d
            .fields
            .get("columns")
            .and_then(storine::Value::as_list)
            .map(|l| {
                l.iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        Ok(Some((text("sql"), dialect, columns)))
    }

    /// Every view name.
    pub fn view_names(&self) -> SqlResult<Vec<String>> {
        Ok(self
            .store
            .scan(crate::catalog::VIEWS, None)?
            .into_iter()
            .filter_map(|d| {
                d.fields
                    .get("name")
                    .and_then(storine::Value::as_str)
                    .map(str::to_owned)
            })
            .collect())
    }

    /// `CREATE DATABASE`; false when it already existed.
    pub fn create_database(&self, name: &str) -> SqlResult<bool> {
        if self.is_default_name(name) {
            return Ok(false);
        }
        let mut doc = Doc::new();
        doc.insert("name".into(), storine::Value::Text(name.to_owned()));
        let w = Write {
            collection: DATABASES.into(),
            key: name.to_lowercase(),
            op: Op::Put(doc),
            pre: Pre::Absent,
            tag: Tag::Internal,
        };
        match self.databases.root.commit(std::slice::from_ref(&w)) {
            Ok(_) => Ok(true),
            Err(crate::store::CommitError::Condition { .. }) => Ok(false),
            Err(e) => Err(e.to_sql(std::slice::from_ref(&w))),
        }
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
        if let Some(t) = temp::lookup(name) {
            return Ok(t);
        }
        self.refresh_catalog(false)?;
        if let Some(t) = self.catalog.get(name) {
            return Ok(t);
        }
        self.refresh_catalog(true)?;
        self.catalog
            .get(name)
            .ok_or_else(|| SqlError::undefined_table(name))
    }

    pub fn find_table(&self, name: &str) -> SqlResult<Option<Arc<Table>>> {
        match self.table(name) {
            Ok(t) => Ok(Some(t)),
            Err(e) if e.code == "42P01" => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn new_session(
        self: &Arc<Self>,
        dialect: Dialect,
        user: &str,
        database: Option<&str>,
    ) -> Session {
        let id = self.next_session.fetch_add(1, Ordering::Relaxed);
        let engine = match database.filter(|d| !d.is_empty()) {
            Some(d) => self.database(d).unwrap_or_else(|_| Arc::clone(self)),
            None => Arc::clone(self),
        };
        Session {
            control: self.registry.register(id),
            depth: 0,
            sql_text: None,
            trace: None,
            temp: Arc::default(),
            cursors: HashMap::new(),
            frames: std::sync::Mutex::new(Vec::new()),
            listening: std::collections::HashSet::new(),
            pending_notifies: Vec::new(),
            engine,
            dialect,
            user: user.to_owned(),
            role: None,
            database: database
                .filter(|d| !d.is_empty())
                .unwrap_or(&self.config.database)
                .to_owned(),
            vars: default_vars(dialect),
            base_vars: default_vars(dialect),
            user_vars: HashMap::new(),
            table_vars: HashMap::new(),
            advisory: std::sync::Mutex::new(advisory::Held::default()),
            return_status: 0,
            txn: None,
            last_insert_id: 0,
            prepared: HashMap::new(),
            id,
            last_found_rows: 0,
            last_row_count: 0,
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
        (
            "transaction_isolation",
            if dialect == Dialect::MySql {
                "REPEATABLE-READ"
            } else {
                "read committed"
            },
        ),
        ("transaction_read_only", "off"),
        ("sql_mode", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"),
        ("autocommit", "1"),
        ("character_set_client", "utf8mb4"),
        ("character_set_connection", "utf8mb4"),
        ("character_set_results", "utf8mb4"),
        ("collation_connection", "utf8mb4_0900_ai_ci"),
        ("character_set_server", "utf8mb4"),
        ("socket", "/tmp/mysql.sock"),
        ("port", "3306"),
        ("hostname", "storine"),
        ("default_storage_engine", "InnoDB"),
        ("explicit_defaults_for_timestamp", "1"),
        ("sql_require_primary_key", "0"),
        ("character_set_database", "utf8mb4"),
        ("character_set_system", "utf8mb3"),
        ("collation_server", "utf8mb4_0900_ai_ci"),
        ("collation_database", "utf8mb4_0900_ai_ci"),
        ("auto_increment_increment", "1"),
        ("auto_increment_offset", "1"),
        ("init_connect", ""),
        ("license", "Apache-2.0"),
        ("net_write_timeout", "60"),
        ("performance_schema", "0"),
        ("system_time_zone", "UTC"),
        ("tx_isolation", "REPEATABLE-READ"),
        ("query_cache_size", "0"),
        ("query_cache_type", "OFF"),
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

/// A T-SQL table variable: typed columns and its rows, private to the session.
#[derive(Debug, Clone, Default)]
pub struct TableVar {
    pub columns: Vec<(String, DataType)>,
    pub rows: Vec<Vec<Datum>>,
}

impl Session {
    /// Appends rows to a table variable (`columns`: the target columns, empty for all).
    pub(crate) fn table_var_append(
        &mut self,
        name: &str,
        columns: &[String],
        rows: Vec<Vec<Datum>>,
    ) -> SqlResult<()> {
        let tv = self.table_vars.get_mut(name).ok_or_else(|| {
            SqlError::new(
                "42P01",
                format!("Must declare the table variable \"{name}\"."),
            )
        })?;
        let targets: Vec<usize> = if columns.is_empty() {
            (0..tv.columns.len()).collect()
        } else {
            columns
                .iter()
                .map(|c| {
                    tv.columns
                        .iter()
                        .position(|(n, _)| n.eq_ignore_ascii_case(c))
                        .ok_or_else(|| SqlError::undefined_column(c))
                })
                .collect::<SqlResult<_>>()?
        };
        for r in rows {
            if r.len() != targets.len() {
                return Err(SqlError::syntax(
                    "Column name or number of supplied values does not match table definition."
                        .into(),
                ));
            }
            let mut row = vec![Datum::Null; tv.columns.len()];
            for (v, &k) in r.into_iter().zip(&targets) {
                row[k] = crate::datum::cast(&v, &tv.columns[k].1)?;
            }
            tv.rows.push(row);
        }
        Ok(())
    }
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
    Command {
        tag: String,
        affected: u64,
        last_insert_id: Option<i64>,
    },
}

impl Outcome {
    pub fn command(tag: impl Into<String>) -> Outcome {
        Outcome::Command {
            tag: tag.into(),
            affected: 0,
            last_insert_id: None,
        }
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
    /// REPEATABLE READ / SERIALIZABLE: every row read is validated at commit.
    pub(crate) validate_reads: bool,
    /// Rows read (first version seen; `None`: looked up and absent), for `validate_reads`.
    pub(crate) reads: std::cell::RefCell<HashMap<(String, String), Option<u64>>>,
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
    Overlay {
        key: (String, String),
        prev: Option<Entry>,
        new_in_order: bool,
    },
    Check,
}

/// A session that ends (its client disconnected) with a transaction open rolls it back, so
/// its row locks are released at once instead of when they expire.
impl Drop for Session {
    fn drop(&mut self) {
        self.engine.registry.unlisten_all(self.id);
        if self.txn.is_some() {
            self.rollback();
        }
        self.drop_temp_tables();
        advisory::release_all(self, None, "");
    }
}

impl Txn {
    /// The buffered state of a document: `Some(None)` deleted, `Some(Some(doc))` written,
    /// `None` untouched.
    pub fn get(&self, collection: &str, key: &str) -> Option<Option<&Doc>> {
        self.overlay
            .get(&(collection.to_owned(), key.to_owned()))
            .map(|e| e.doc.as_ref())
    }

    /// Buffered documents of `collection` (key, doc or deletion).
    pub fn entries<'a>(
        &'a self,
        collection: &'a str,
    ) -> impl Iterator<Item = (&'a str, Option<&'a Doc>)> + 'a {
        self.order
            .iter()
            .filter(move |(c, _)| c == collection)
            .filter_map(move |k| self.overlay.get(k).map(|e| (k.1.as_str(), e.doc.as_ref())))
    }

    /// Buffers a write of `doc` (`None`: delete). `observed` is the version this transaction read
    /// (`None`: the document did not exist), used only the first time the key is touched.
    pub fn write(
        &mut self,
        collection: &str,
        key: &str,
        observed: Option<u64>,
        doc: Option<Doc>,
        tag: Tag,
    ) {
        let k = (collection.to_owned(), key.to_owned());
        let prev = self.overlay.get(&k).cloned();
        let new_in_order = prev.is_none();
        // A row read earlier in a validating transaction must still be the version first seen.
        let first_read = self.reads.borrow().get(&k).copied();
        let base = match (&prev, first_read) {
            (Some(e), _) => e.base,
            (None, Some(v)) => v.map_or(Pre::Absent, Pre::Version),
            (None, None) => observed.map_or(Pre::Absent, Pre::Version),
        };
        self.overlay.insert(k.clone(), Entry { base, doc, tag });
        if new_in_order {
            self.order.push(k.clone());
        }
        self.journal.push(Undo::Overlay {
            key: k,
            prev,
            new_in_order,
        });
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
                Some(Undo::Overlay {
                    key,
                    prev,
                    new_in_order,
                }) => {
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
            let Some(e) = self.overlay.get(k) else {
                continue;
            };
            let op = match (&e.doc, e.base) {
                (None, Pre::Absent) => continue, // created and deleted again
                (Some(d), _) => Op::Put(d.clone()),
                (None, _) => Op::Delete,
            };
            out.push(Write {
                collection: k.0.clone(),
                key: k.1.clone(),
                op,
                pre: e.base,
                tag: e.tag.clone(),
            });
        }
        for c in &self.checks {
            // A check on a document this transaction also writes is covered by that write.
            if !self
                .overlay
                .contains_key(&(c.collection.clone(), c.key.clone()))
            {
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
    /// `SET ROLE` / `EXECUTE AS`: the role privileges are checked for instead of `user`.
    pub role: Option<String>,
    /// How others reach this session (`exec/control.rs`).
    pub control: Arc<control::Control>,
    /// Nesting of `execute` (a statement's limits are set by the outermost).
    pub(crate) depth: u32,
    /// The text of what is running, as the wire protocol received it (`exec/activity.rs`).
    pub(crate) sql_text: Option<Arc<str>>,
    /// `EXPLAIN ANALYZE` is running.
    pub(crate) trace: Option<explain::Trace>,
    /// Temporary tables and cursors (`exec/temp.rs`, `exec/cursor.rs`).
    pub(crate) temp: temp::TempTables,
    pub(crate) cursors: cursor::Cursors,
    /// Variables of routines being run (`exec/routine.rs`).
    pub(crate) frames: routine::Frames,
    /// Channels this session listens on, and notifications waiting for its commit.
    pub(crate) listening: std::collections::HashSet<String>,
    pub(crate) pending_notifies: Vec<(String, String)>,
    pub database: String,
    pub vars: HashMap<String, String>,
    /// Session defaults (`RESET ALL` / `DISCARD ALL` return to them): the dialect's defaults
    /// plus what the client set at connection time (`application_name`).
    pub base_vars: HashMap<String, String>,
    pub user_vars: HashMap<String, Datum>,
    /// T-SQL table variables by name (with the `@`).
    pub table_vars: HashMap<String, TableVar>,
    /// Return status of the last T-SQL procedure (`EXEC @status = ...`).
    pub return_status: i32,
    /// Advisory / named / application locks this session holds (`exec/advisory.rs`).
    pub advisory: std::sync::Mutex<advisory::Held>,
    pub txn: Option<Txn>,
    pub last_insert_id: i64,
    pub prepared: HashMap<String, (Statement, usize)>,
    pub id: u64,
    pub last_found_rows: u64,
    /// MySQL `ROW_COUNT()`: rows changed by the last DML statement, -1 after a query.
    pub last_row_count: i64,
}

/// Nanoseconds since the epoch, now.
pub fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

impl Session {
    /// `CREATE MATERIALIZED VIEW`: a table holding the query's rows, and the query to refresh it.
    #[allow(clippy::too_many_arguments)]
    fn create_matview(&mut self, name: &str, columns: &[String], query: &crate::ast::Query, sql: &str, with_data: bool, if_not_exists: bool, params: &[Datum]) -> SqlResult<Outcome> {
        use crate::ast::{ColumnDef, ConstraintNames, CreateTable};
        self.require_database("CREATE", "create materialized views")?;
        if self.engine.find_table(name)?.is_some() {
            if if_not_exists {
                return Ok(Outcome::command("SELECT 0"));
            }
            return Err(SqlError::duplicate_table(name));
        }
        let rs = {
            let ex = eval::Exec::new(self, params);
            let rel = ex.query(query, None)?;
            ex.result_set(rel)?
        };
        if !columns.is_empty() && columns.len() != rs.columns.len() {
            return Err(SqlError::new("42601", "the column list does not match the query"));
        }
        let cols = rs
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| ColumnDef { name: columns.get(i).unwrap_or(&c.name).clone(), ty: c.ty.clone(), precision: None, not_null: false, default: None, primary_key: false, unique: false, auto_increment: false, references: None, check: None, generated: None, declared: None, names: ConstraintNames::default() })
            .collect();
        ddl::create_table(self, &CreateTable { name: name.to_owned(), if_not_exists: false, columns: cols, constraints: Vec::new(), as_query: None, temporary: None, columnar: false }, params)?;
        let mut doc = Doc::new();
        doc.insert("name".into(), storine::Value::Text(name.to_owned()));
        doc.insert("sql".into(), storine::Value::Text(sql.to_owned()));
        doc.insert("dialect".into(), storine::Value::Text(if self.dialect == Dialect::MySql { "mysql".into() } else { "postgres".into() }));
        let w = Write { collection: crate::catalog::MATVIEWS.into(), key: name.to_lowercase(), op: Op::Put(doc), pre: Pre::None, tag: Tag::Internal };
        self.engine.store.commit(std::slice::from_ref(&w)).map_err(|e| e.to_sql(std::slice::from_ref(&w)))?;
        let n = if with_data { self.replace_rows(name, rs.rows, params)? } else { 0 };
        Ok(Outcome::Command { tag: format!("SELECT {n}"), affected: n, last_insert_id: None })
    }

    /// Replaces a table's rows, atomically when no transaction is open.
    fn replace_rows(&mut self, name: &str, rows: Vec<Vec<Datum>>, params: &[Datum]) -> SqlResult<u64> {
        use crate::ast::{Delete, Insert, InsertSource};
        let n = rows.len() as u64;
        let own_txn = !self.in_explicit_transaction();
        if own_txn {
            self.txn = Some(Txn { explicit: true, ..Txn::default() });
        }
        let run = (|| -> SqlResult<()> {
            dml::delete(self, &Delete { table: name.to_owned(), alias: None, using: Vec::new(), where_: None, returning: Vec::new(), order_by: Vec::new(), limit: None }, params)?;
            for chunk in rows.chunks(500) {
                let values = chunk.iter().map(|r| r.iter().map(ddl::datum_expr).collect()).collect();
                dml::insert(self, &Insert { table: name.to_owned(), alias: None, columns: Vec::new(), source: InsertSource::Values(values), on_conflict: None, returning: Vec::new(), ignore: false, replace: false }, params)?;
            }
            Ok(())
        })();
        if own_txn {
            match run {
                Ok(()) => self.commit()?,
                Err(e) => {
                    self.rollback();
                    return Err(e);
                }
            }
        } else {
            run?;
        }
        Ok(n)
    }

    fn refresh_matview(&mut self, name: &str, with_data: bool, params: &[Datum]) -> SqlResult<Outcome> {
        let t = self.engine.table(name)?;
        self.require_owner(&t, "ALTER")?;
        let Some(d) = self.engine.store.get(crate::catalog::MATVIEWS, &name.to_lowercase())? else {
            return Err(SqlError::new("42809", format!("\"{name}\" is not a materialized view")));
        };
        let sql = d.fields.get("sql").and_then(storine::Value::as_str).unwrap_or_default().to_owned();
        let dialect = if d.fields.get("dialect").and_then(storine::Value::as_str) == Some("mysql") { Dialect::MySql } else { Dialect::Postgres };
        let (stmts, _) = crate::parser::parse(&sql, dialect)?;
        let Some(Statement::Query(q)) = stmts.into_iter().next() else {
            return Err(SqlError::internal("a materialized view's query did not parse"));
        };
        let rows = if with_data {
            let ex = eval::Exec::new(self, params);
            let rel = ex.query(&q, None)?;
            ex.result_set(rel)?.rows
        } else {
            Vec::new()
        };
        self.replace_rows(&t.name, rows, params)?;
        Ok(Outcome::command("REFRESH MATERIALIZED VIEW"))
    }

    fn drop_matview(&mut self, names: &[String], if_exists: bool) -> SqlResult<Outcome> {
        for n in names {
            if self.engine.store.get(crate::catalog::MATVIEWS, &n.to_lowercase())?.is_none() {
                if if_exists {
                    continue;
                }
                return Err(SqlError::new("42P01", format!("materialized view \"{n}\" does not exist")));
            }
            ddl::drop_tables(self, std::slice::from_ref(n), false, false)?;
            let w = Write { collection: crate::catalog::MATVIEWS.into(), key: n.to_lowercase(), op: Op::Delete, pre: Pre::None, tag: Tag::Internal };
            self.engine.store.commit(std::slice::from_ref(&w)).map_err(|e| e.to_sql(std::slice::from_ref(&w)))?;
        }
        Ok(Outcome::command("DROP MATERIALIZED VIEW"))
    }

    /// A statement of a routine body: inside the running statement's transaction.
    pub(crate) fn run_nested(&mut self, stmt: &Statement, params: &[Datum]) -> SqlResult<Outcome> {
        if matches!(stmt, Statement::Begin { .. } | Statement::Commit | Statement::Rollback | Statement::Savepoint(_) | Statement::ReleaseSavepoint(_) | Statement::RollbackToSavepoint(_)) {
            return Err(SqlError::new("2D000", "transaction control is not allowed inside a function"));
        }
        self.control.check()?;
        self.run(stmt, params)
    }

    /// Inside `BEGIN ... COMMIT` (not the implicit transaction of one statement).
    pub(crate) fn in_explicit_transaction(&self) -> bool {
        self.txn.as_ref().is_some_and(|t| t.explicit)
    }

    /// T-SQL `SET SHOWPLAN_TEXT | SHOWPLAN_ALL ON`: statements are planned, not run.
    fn showplan(&self) -> Option<crate::ast::ExplainOptions> {
        if self.dialect != Dialect::MsSql {
            return None;
        }
        let on = |n: &str| self.vars.get(n).is_some_and(|v| v.eq_ignore_ascii_case("on"));
        (on("showplan_text") || on("showplan_all")).then_some(crate::ast::ExplainOptions { analyze: false, format: crate::ast::ExplainFormat::ShowPlan, summary: false })
    }

    /// Runs a statement for `EXPLAIN ANALYZE`, in its own transaction if none is open.
    pub(crate) fn run_traced(&mut self, stmt: &Statement, params: &[Datum]) -> SqlResult<Outcome> {
        self.execute_one(stmt, params)
    }

    fn statement_timeout(&self) -> Option<Duration> {
        let (name, unit) = if self.dialect == Dialect::MySql { ("max_execution_time", 1.0) } else { ("statement_timeout", 1.0) };
        control::parse_duration(self.vars.get(name)?, unit)
    }

    fn idle_transaction_timeout(&self) -> Option<Duration> {
        control::parse_duration(self.vars.get("idle_in_transaction_session_timeout")?, 1.0)
    }

    /// How long a row lock is waited for: `Some(ZERO)` not at all, `None` until cancelled.
    pub(crate) fn lock_wait(&self) -> Option<Duration> {
        let default = Some(self.engine.config.lock_timeout);
        match self.dialect {
            // SET LOCK_TIMEOUT: milliseconds, -1 forever, 0 not at all.
            Dialect::MsSql => match self.vars.get("lock_timeout").map(|v| v.trim().parse::<i64>()) {
                Some(Ok(n)) if n < 0 => None,
                Some(Ok(n)) => Some(Duration::from_millis(n as u64)),
                _ => default,
            },
            Dialect::MySql => self.vars.get("innodb_lock_wait_timeout").and_then(|v| control::parse_duration(v, 1000.0)).or(default),
            // PostgreSQL: 0 means forever.
            _ => match self.vars.get("lock_timeout") {
                Some(v) => control::parse_duration(v, 1.0),
                None => default,
            },
        }
    }

    /// Most bytes of rows one statement may hold.
    pub(crate) fn memory_limit(&self) -> u64 {
        self.vars.get("max_query_memory").and_then(|v| control::parse_bytes(v)).unwrap_or(self.engine.config.max_query_memory)
    }

    /// `KILL`, `pg_cancel_backend`, `pg_terminate_backend`: a superuser's right, or the session's
    /// own user's. `false`: no such session.
    pub(crate) fn signal_session(&self, id: u64, terminate: bool) -> SqlResult<bool> {
        let Some(target) = self.engine.registry.get(id) else { return Ok(false) };
        let acl = self.engine.acl()?;
        let me = self.current_role();
        if !acl.is_superuser(me) && acl::role_key(&target.user()) != acl::role_key(me) {
            return Err(SqlError::new("42501", "must be a superuser or the session's own user to signal it"));
        }
        if terminate {
            target.terminate();
        } else {
            target.cancel();
        }
        Ok(true)
    }

    /// MySQL `SET FOREIGN_KEY_CHECKS = 0` turns foreign-key enforcement off for the session.
    pub fn foreign_key_checks(&self) -> bool {
        !self
            .vars
            .get("foreign_key_checks")
            .is_some_and(|v| v == "0" || v.eq_ignore_ascii_case("off"))
    }

    pub fn in_transaction(&self) -> bool {
        self.txn.as_ref().is_some_and(|t| t.explicit)
    }

    /// `USE db` / MySQL `COM_INIT_DB`: later statements run in database `name`.
    pub fn use_database(&mut self, name: &str) -> SqlResult<()> {
        let engine = self.engine.database(name)?;
        if !Arc::ptr_eq(&engine, &self.engine) && self.in_transaction() {
            return Err(SqlError::new(
                "25001",
                "cannot switch databases inside a transaction",
            ));
        }
        if !engine.acl()?.may_connect(&self.user) {
            return Err(SqlError::new("42501", format!("permission denied for database \"{name}\"")));
        }
        self.engine = engine;
        self.database = name.to_owned();
        Ok(())
    }

    /// `DROP DATABASE`: drops every table and sequence of the database. A created database
    /// is also unregistered; the default one stays (empty).
    fn drop_database(&mut self, name: &str, if_exists: bool) -> SqlResult<Outcome> {
        if !self.engine.acl()?.is_superuser(self.current_role()) {
            return Err(SqlError::new("42501", "must be a superuser to drop a database"));
        }
        let default = self.engine.is_default_name(name);
        if !default && !self.engine.is_created(name)? {
            if if_exists {
                return Ok(Outcome::command("DROP DATABASE"));
            }
            return Err(SqlError::new(
                "3D000",
                format!("database \"{name}\" does not exist"),
            ));
        }
        if self.in_transaction() {
            return Err(SqlError::new(
                "25001",
                "DROP DATABASE cannot run inside a transaction block",
            ));
        }
        let target = self.engine.database(name)?;
        let mut s = target.new_session(self.dialect, &self.user, None);
        target.refresh_catalog(true)?;
        let names: Vec<String> = target
            .catalog
            .all()
            .iter()
            .map(|t| t.name.clone())
            .collect();
        if !names.is_empty() {
            ddl::drop_tables(&mut s, &names, true, true)?;
        }
        for c in [
            crate::catalog::SEQUENCES,
            crate::catalog::LOCKS,
            crate::catalog::UNIQUE,
            crate::catalog::REFS,
            crate::catalog::VIEWS,
        ] {
            let _ = target.store.drop_collection(c);
        }
        if !default {
            let _ = target.store.drop_collection(TABLES);
            let key = name.to_lowercase();
            let w = Write {
                collection: DATABASES.into(),
                key: key.clone(),
                op: Op::Delete,
                pre: Pre::None,
                tag: Tag::Internal,
            };
            let root = &self.engine.databases.root;
            root.commit(std::slice::from_ref(&w))
                .map_err(|e| e.to_sql(std::slice::from_ref(&w)))?;
            self.engine
                .databases
                .open
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&key);
            if Arc::ptr_eq(&self.engine, &target) {
                self.engine = self.engine.default_engine()?;
            }
        }
        target.catalog.invalidate();
        Ok(Outcome::command("DROP DATABASE"))
    }

    /// Whether a transaction at isolation `level` (else the session's) validates its reads:
    /// PostgreSQL REPEATABLE READ and SERIALIZABLE, MySQL SERIALIZABLE (MySQL's REPEATABLE READ,
    /// its default, reads without validation like InnoDB).
    fn validates_reads(&self, level: Option<&str>) -> bool {
        let session = self
            .vars
            .get("transaction_isolation")
            .cloned()
            .unwrap_or_default();
        let l = level
            .unwrap_or(&session)
            .to_ascii_lowercase()
            .replace(['-', '_'], " ");
        match l.trim() {
            "serializable" => true,
            "repeatable read" => self.dialect != Dialect::MySql,
            _ => false,
        }
    }

    /// False after MySQL `SET autocommit = 0`.
    pub fn autocommit(&self) -> bool {
        !self.vars.get("autocommit").is_some_and(|v| {
            ["0", "off", "false"]
                .iter()
                .any(|f| v.eq_ignore_ascii_case(f))
        })
    }

    pub fn transaction_failed(&self) -> bool {
        self.txn.as_ref().is_some_and(|t| t.explicit && t.failed)
    }

    /// Runs one statement with bound parameters.
    pub fn execute(&mut self, stmt: &Statement, params: &[Datum]) -> SqlResult<Outcome> {
        let started = now_ns();
        temp::enter(&self.temp);
        let was_explicit = self.in_explicit_transaction();
        if self.depth == 0 {
            self.control.begin_statement(self.statement_timeout());
            self.activity_start();
        }
        self.depth += 1;
        let out = self.control.check().and_then(|()| match self.showplan() {
            Some(opts) if !matches!(stmt, Statement::Set { .. } | Statement::Explain(..)) => self.explain(stmt, &opts, params),
            _ => self.execute_one(stmt, params),
        });
        self.depth -= 1;
        if self.depth == 0 {
            // A transaction just ended (or a statement was one): what lives only that long goes.
            if !self.in_transaction() && (was_explicit || !self.temp.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_empty()) {
                self.cursors_after_transaction();
                self.temp_after_transaction()?;
            }
            // Notifications go out when their transaction commits; a failure drops them.
            if !self.in_transaction() {
                let ok = out.is_ok() && !matches!(stmt, Statement::Rollback);
                self.flush_notifies(ok);
            }
            self.control.end_statement(self.in_transaction(), self.idle_transaction_timeout());
            self.activity_end(&out, started);
        }
        // Transaction-scoped advisory locks end with the transaction (an autocommit statement
        // is a transaction of its own).
        if !self.in_transaction() {
            advisory::release_all(self, Some(advisory::Scope::Xact), "");
        }
        out
    }

    fn execute_one(&mut self, stmt: &Statement, params: &[Datum]) -> SqlResult<Outcome> {
        if let Statement::If {
            cond,
            then,
            otherwise,
        } = stmt
        {
            let yes = eval::Exec::new(self, params)
                .eval_const(cond)?
                .truthy()?
                .unwrap_or(false);
            return match (yes, otherwise) {
                (true, _) => self.execute(then, params),
                (false, Some(o)) => self.execute(o, params),
                (false, None) => Ok(Outcome::command("IF")),
            };
        }
        // Transaction control first: it works even in a failed transaction.
        match stmt {
            Statement::Begin { isolation } => {
                if self.in_transaction() {
                    // PostgreSQL warns and continues; MySQL commits the open transaction.
                    if self.dialect == Dialect::MySql {
                        self.commit()?;
                    } else {
                        return Ok(Outcome::command("BEGIN"));
                    }
                }
                let level = isolation
                    .clone()
                    .or_else(|| self.vars.remove("next_transaction_isolation"));
                self.txn = Some(Txn {
                    explicit: true,
                    validate_reads: self.validates_reads(level.as_deref()),
                    ..Txn::default()
                });
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
                let txn = self.txn.get_or_insert_with(|| Txn {
                    explicit: true,
                    ..Txn::default()
                });
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
                        None => {
                            return Err(SqlError::new(
                                "3B001",
                                format!("savepoint \"{name}\" does not exist"),
                            ))
                        }
                    }
                }
                return Ok(Outcome::command("RELEASE"));
            }
            Statement::RollbackToSavepoint(name) => {
                let Some(txn) = self.txn.as_mut() else {
                    return Err(SqlError::new(
                        "3B001",
                        format!("savepoint \"{name}\" does not exist"),
                    ));
                };
                let n = name.to_ascii_lowercase();
                let Some(p) = txn.savepoints.iter().rposition(|(s, _)| *s == n) else {
                    return Err(SqlError::new(
                        "3B001",
                        format!("savepoint \"{name}\" does not exist"),
                    ));
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
        if !self.in_transaction() && !self.autocommit() && !matches!(stmt, Statement::Set { .. }) {
            self.txn = Some(Txn {
                explicit: true,
                ..Txn::default()
            });
        }
        if self.in_transaction() {
            let mark = self.txn.as_ref().map_or(0, Txn::mark);
            let r = self.run(stmt, params);
            if let Err(e) = &r {
                if let Some(txn) = self.txn.as_mut() {
                    // The statement's own changes are undone either way; PostgreSQL also aborts
                    // the transaction, MySQL keeps it usable.
                    txn.rollback_to(mark);
                    // SQL Server (XACT_ABORT OFF) keeps it too.
                    if !matches!(self.dialect, Dialect::MySql | Dialect::MsSql) && e.code != "0A000"
                    {
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
                    if !retryable
                        || attempt > self.engine.config.max_retries
                        || !is_retry_safe(stmt)
                    {
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
        let Some(txn) = self.txn.as_ref() else {
            return Ok(());
        };
        let mut batch = txn.batch();
        // REPEATABLE READ / SERIALIZABLE: rows read but not written must be unchanged.
        if txn.validate_reads {
            for ((c, k), v) in txn.reads.borrow().iter() {
                if txn.get(c, k).is_none() {
                    batch.push(Write {
                        collection: c.clone(),
                        key: k.clone(),
                        op: Op::Check,
                        pre: v.map_or(Pre::Absent, Pre::Version),
                        tag: Tag::Internal,
                    });
                }
            }
        }
        // Row locks are released in the same atomic batch.
        for k in &txn.locks {
            batch.push(Write {
                collection: crate::catalog::LOCKS.into(),
                key: k.clone(),
                op: Op::Delete,
                pre: Pre::None,
                tag: Tag::Lock,
            });
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
            .map(|k| Write {
                collection: crate::catalog::LOCKS.into(),
                key: k,
                op: Op::Delete,
                pre: Pre::None,
                tag: Tag::Lock,
            })
            .collect();
        // Best effort: an unreleased lock expires on its own.
        let _ = self.engine.store.commit(&batch);
    }

    /// The open transaction (every statement runs inside one).
    pub(crate) fn txn_mut(&mut self) -> &mut Txn {
        self.txn.get_or_insert_with(Txn::default)
    }

    /// Records a DML statement's affected rows for `ROW_COUNT()`.
    fn count_changes(&mut self, out: SqlResult<Outcome>) -> SqlResult<Outcome> {
        self.last_row_count = match &out {
            Ok(Outcome::Command { affected, .. }) => *affected as i64,
            Ok(Outcome::Rows(rs)) => rs.rows.len() as i64, // RETURNING
            Err(_) => -1,
        };
        out
    }

    fn run(&mut self, stmt: &Statement, params: &[Datum]) -> SqlResult<Outcome> {
        match stmt {
            Statement::Query(q) => {
                if q.lock.is_some() && self.in_transaction() {
                    // Lock first, so the query reads the rows' latest values.
                    dml::lock_query_rows(self, q, params)?;
                }
                let ex = eval::Exec::new(self, params);
                let first = ex.query(q, None).and_then(|rel| ex.result_set(rel));
                drop(ex);
                // A function that writes, called on its own, runs with the session itself.
                let rs = match first {
                    Err(e) if routine::needs_session(&e) => match self.select_function_with_session(q, params) {
                        Some(out) => return out,
                        None => return Err(e),
                    },
                    other => other?,
                };
                self.last_found_rows = rs.rows.len() as u64;
                self.last_row_count = -1;
                Ok(Outcome::Rows(rs))
            }
            Statement::Insert(i) => {
                let out = dml::insert(self, i, params);
                self.count_changes(out)
            }
            Statement::Update(u) => {
                let out = dml::update(self, u, params);
                self.count_changes(out)
            }
            Statement::Delete(d) => {
                let out = dml::delete(self, d, params);
                self.count_changes(out)
            }
            Statement::CreateFunction(f) => self.create_function(f),
            Statement::DropFunction { names, if_exists } => self.drop_function(names, *if_exists),
            Statement::Call { name, args } => {
                let _ = self.call_routine(name, args, params)?;
                Ok(Outcome::command("CALL"))
            }
            Statement::CreateTrigger(c) => self.create_trigger(c),
            Statement::DropTrigger { name, table, if_exists } => self.drop_trigger(name, table, *if_exists),
            Statement::Listen(c) => self.listen(c),
            Statement::Unlisten(c) => self.unlisten(c.as_deref()),
            Statement::Notify { channel, payload } => self.notify(channel, payload),
            Statement::DeclareCursor { name, query, hold } => self.declare_cursor(name, query, *hold, params),
            Statement::OpenCursor(name) => self.open_cursor(name, params),
            Statement::Fetch { name, direction, move_only, into } => self.fetch_cursor(name, direction, *move_only, into),
            Statement::CloseCursor(name) => self.close_cursor(name.as_deref()),
            Statement::DeallocateCursor(name) => self.deallocate_cursor(name),
            Statement::CreateMatView { name, columns, query, sql, with_data, if_not_exists } => self.create_matview(name, columns, query, sql, *with_data, *if_not_exists, params),
            Statement::RefreshMatView { name, with_data } => self.refresh_matview(name, *with_data, params),
            Statement::DropMatView { names, if_exists } => self.drop_matview(names, *if_exists),
            Statement::Analyze(tables) => {
                stats::analyze(self, tables)?;
                Ok(Outcome::command("ANALYZE"))
            }
            Statement::Kill { id, query_only } => {
                if !self.signal_session(*id, !*query_only)? {
                    return Err(SqlError::new("42704", format!("Unknown thread id: {id}")));
                }
                Ok(Outcome::command("KILL"))
            }
            Statement::Role(r) => acl::role_statement(self, r),
            Statement::Grant(g) => acl::grant_statement(self, g),
            Statement::CreatePolicy(p) => ddl::create_policy(self, p),
            Statement::DropPolicy { name, table, if_exists } => ddl::drop_policy(self, name, table, *if_exists),
            Statement::AlterPolicy { name, table, rename, roles, using, check } => ddl::alter_policy(self, name, table, rename.as_deref(), roles.as_deref(), using.as_ref(), check.as_ref()),
            Statement::CreateTable(c) => ddl::create_table(self, c, params),
            Statement::CreateIndex(c) => ddl::create_index(self, c),
            Statement::DropTable {
                names,
                if_exists,
                cascade,
            } => ddl::drop_tables(self, names, *if_exists, *cascade),
            Statement::DropIndex {
                names,
                table,
                if_exists,
            } => ddl::drop_indexes(self, names, table.as_deref(), *if_exists),
            Statement::AlterTable {
                table,
                if_exists,
                ops,
            } => {
                if *if_exists && self.engine.find_table(table)?.is_none() {
                    return Ok(Outcome::command("ALTER TABLE"));
                }
                ddl::alter_table(self, table, ops)
            }
            Statement::Truncate { tables } => ddl::truncate(self, tables),
            Statement::CreateSequence {
                name,
                start,
                increment,
                if_not_exists,
            } => {
                self.require_database("CREATE", "create sequences")?;
                dml::create_sequence(self, name, *start, *increment, *if_not_exists)
            },
            Statement::DropSequence { names, if_exists } => {
                self.require_database("CREATE", "drop sequences")?;
                dml::drop_sequences(self, names, *if_exists)
            }
            Statement::Set { name, value } => self.set_var(name, value.as_ref(), params),
            Statement::Show(s) => system::show(self, s),
            Statement::Declare(vars) => {
                for (name, ty, value) in vars {
                    let v = match value {
                        Some(e) => crate::datum::cast(&eval::Exec::new(self, params).eval_const(e)?, ty)?,
                        None => Datum::Null,
                    };
                    self.user_vars.insert(name.to_ascii_lowercase(), v);
                }
                Ok(Outcome::command("DECLARE"))
            }
            Statement::If { .. } => unreachable!("IF is dispatched by execute"),
            Statement::Merge(m) => merge::merge(self, m, params),
            Statement::InsertExec {
                target,
                columns,
                exec,
            } => {
                let rows = match self.run(exec, params)? {
                    Outcome::Rows(rs) => rs.rows,
                    _ => Vec::new(),
                };
                let n = rows.len();
                if target.starts_with('@') {
                    self.table_var_append(target, columns, rows)?;
                } else {
                    for r in rows {
                        let ins = crate::ast::Insert {
                            table: target.clone(),
                            alias: None,
                            columns: columns.clone(),
                            source: crate::ast::InsertSource::Values(vec![(0..r.len())
                                .map(Expr::Param)
                                .collect()]),
                            on_conflict: None,
                            returning: Vec::new(),
                            ignore: false,
                            replace: false,
                        };
                        dml::insert(self, &ins, &r)?;
                    }
                }
                Ok(Outcome::Command {
                    tag: format!("INSERT 0 {n}"),
                    affected: n as u64,
                    last_insert_id: None,
                })
            }
            Statement::DeclareTable { name, columns } => {
                self.table_vars.insert(
                    name.clone(),
                    TableVar {
                        columns: columns.iter().map(|c| (c.name.clone(), c.ty.clone())).collect(),
                        rows: Vec::new(),
                    },
                );
                Ok(Outcome::command("DECLARE"))
            }
            Statement::OutputInto {
                target,
                columns,
                inner,
            } => {
                let out = self.run(inner, params)?;
                let Outcome::Rows(rs) = out else {
                    return Ok(out);
                };
                let n = rs.rows.len();
                self.table_var_append(target, columns, rs.rows)?;
                let verb = match inner.as_ref() {
                    Statement::Insert(_) => "INSERT 0",
                    Statement::Update(_) => "UPDATE",
                    _ => "DELETE",
                };
                Ok(Outcome::Command {
                    tag: format!("{verb} {n}"),
                    affected: n as u64,
                    last_insert_id: None,
                })
            }
            Statement::Use(db) => {
                self.use_database(db)?;
                Ok(Outcome::command("USE"))
            }
            Statement::Explain(inner, opts) => self.explain(inner, opts, params),
            Statement::CreateSchema {
                database: false, ..
            } => Ok(Outcome::command("CREATE SCHEMA")),
            Statement::CreateSchema {
                name,
                if_not_exists,
                database: true,
            } => {
                let acl = self.engine.acl()?;
                let me = self.current_role();
                if !acl.is_superuser(me) && !acl.role(me).is_some_and(|r| r.createdb) {
                    return Err(SqlError::new("42501", "permission denied to create database"));
                }
                if !self.engine.create_database(name)? && !if_not_exists {
                    return Err(SqlError::new(
                        "42P04",
                        format!("database \"{name}\" already exists"),
                    ));
                }
                Ok(Outcome::command("CREATE DATABASE"))
            }
            Statement::CreateView {
                name,
                columns,
                sql,
                or_replace,
                if_not_exists,
            } => {
                self.require_database("CREATE", "create views")?;
                if let Some(owner) = self.engine.view_owner(name)? {
                    let acl = self.engine.acl()?;
                    let me = self.current_role();
                    if *or_replace && !acl.is_superuser(me) && !acl.owns(me, owner.as_deref()) {
                        return Err(SqlError::new("42501", format!("must be owner of view {name}")));
                    }
                }
                if self.engine.find_table(name)?.is_some() {
                    return Err(SqlError::new(
                        "42P07",
                        format!("relation \"{name}\" already exists"),
                    ));
                }
                let mut doc = Doc::new();
                doc.insert("name".into(), storine::Value::Text(name.clone()));
                doc.insert("owner".into(), storine::Value::Text(self.current_role().to_owned()));
                doc.insert("sql".into(), storine::Value::Text(sql.clone()));
                doc.insert(
                    "dialect".into(),
                    storine::Value::Text(
                        if self.dialect == Dialect::MySql {
                            "mysql"
                        } else {
                            "postgres"
                        }
                        .into(),
                    ),
                );
                doc.insert(
                    "columns".into(),
                    storine::Value::List(
                        columns
                            .iter()
                            .map(|c| storine::Value::Text(c.clone()))
                            .collect(),
                    ),
                );
                let w = Write {
                    collection: crate::catalog::VIEWS.into(),
                    key: name.to_lowercase(),
                    op: Op::Put(doc),
                    pre: if *or_replace { Pre::None } else { Pre::Absent },
                    tag: Tag::Internal,
                };
                match self.engine.store.commit(std::slice::from_ref(&w)) {
                    Ok(_) => Ok(Outcome::command("CREATE VIEW")),
                    Err(crate::store::CommitError::Condition { .. }) if *if_not_exists => {
                        Ok(Outcome::command("CREATE VIEW"))
                    }
                    Err(crate::store::CommitError::Condition { .. }) => Err(SqlError::new(
                        "42P07",
                        format!("relation \"{name}\" already exists"),
                    )),
                    Err(e) => Err(e.to_sql(std::slice::from_ref(&w))),
                }
            }
            Statement::DropView { names, if_exists } => {
                for n in names {
                    if self.engine.view(n)?.is_none() {
                        if *if_exists {
                            continue;
                        }
                        return Err(SqlError::new(
                            "42P01",
                            format!("view \"{n}\" does not exist"),
                        ));
                    }
                    let acl = self.engine.acl()?;
                    let me = self.current_role();
                    if !acl.is_superuser(me) && !acl.owns(me, self.engine.view_owner(n)?.flatten().as_deref()) {
                        return Err(SqlError::new("42501", format!("must be owner of view {n}")));
                    }
                    self.drop_grants(&format!("table:{}", n.to_lowercase()))?;
                    let w = Write {
                        collection: crate::catalog::VIEWS.into(),
                        key: n.to_lowercase(),
                        op: Op::Delete,
                        pre: Pre::None,
                        tag: Tag::Internal,
                    };
                    self.engine
                        .store
                        .commit(std::slice::from_ref(&w))
                        .map_err(|e| e.to_sql(std::slice::from_ref(&w)))?;
                }
                Ok(Outcome::command("DROP VIEW"))
            }
            Statement::DropSchema {
                database: false, ..
            } => Ok(Outcome::command("DROP SCHEMA")),
            Statement::DropSchema {
                name,
                if_exists,
                database: true,
            } => self.drop_database(name, *if_exists),
            Statement::Prepare { name, statement } => {
                self.prepared
                    .insert(name.to_ascii_lowercase(), ((**statement).clone(), 0));
                Ok(Outcome::command("PREPARE"))
            }
            Statement::ExecProc { name, args, status } => {
                self.return_status = 0;
                let out = mssql::exec_proc(self, name, args, params)?;
                if let Some(var) = status {
                    self.user_vars.insert(var.clone(), Datum::Int(i64::from(self.return_status)));
                }
                Ok(out)
            }
            Statement::Execute { name, args } => {
                let (stmt, _) = self
                    .prepared
                    .get(&name.to_ascii_lowercase())
                    .cloned()
                    .ok_or_else(|| {
                        SqlError::new(
                            "26000",
                            format!("prepared statement \"{name}\" does not exist"),
                        )
                    })?;
                let values = {
                    let ex = eval::Exec::new(self, params);
                    args.iter()
                        .map(|a| ex.eval_const(a))
                        .collect::<SqlResult<Vec<_>>>()?
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
            Statement::Copy(_) => Err(SqlError::unsupported(
                "COPY here: it streams through the PostgreSQL protocol's simple query (psql \\copy, COPY ... FROM STDIN / TO STDOUT)",
            )),
            Statement::Discard(kind) => {
                if kind == "all" {
                    if self.in_transaction() {
                        return Err(SqlError::new(
                            "25001",
                            "DISCARD ALL cannot run inside a transaction block",
                        ));
                    }
                    self.prepared.clear();
                    advisory::release_all(self, None, "");
                    self.vars = self.base_vars.clone();
                    self.role = None;
                    self.cursors.clear();
                    self.drop_temp_tables();
                    self.listening.clear();
                    self.engine.registry.unlisten_all(self.id);
                    self.user_vars.clear();
                    self.table_vars.clear();
                }
                Ok(Outcome::command(format!("DISCARD {}", kind.to_ascii_uppercase())))
            }
            Statement::ResetAll => {
                self.vars = self.base_vars.clone();
                self.role = None;
                Ok(Outcome::command("RESET"))
            }
            Statement::NoOp(tag) => Ok(Outcome::command(*tag)),
            Statement::Empty => Ok(Outcome::command("")),
            Statement::Begin { .. }
            | Statement::Commit
            | Statement::Rollback
            | Statement::Savepoint(_)
            | Statement::ReleaseSavepoint(_)
            | Statement::RollbackToSavepoint(_) => {
                Err(SqlError::internal("transaction control in run()"))
            }
        }
    }

    fn set_var(
        &mut self,
        name: &str,
        value: Option<&Expr>,
        params: &[Datum],
    ) -> SqlResult<Outcome> {
        let v = match value {
            None => Datum::Null,
            // `SET search_path = "public"`: a (quoted) name is the setting's value.
            Some(Expr::Column(p)) if !name.starts_with('@') => Datum::Text(p.join(".")),
            Some(e) => eval::Exec::new(self, params).eval_const(e)?,
        };
        if let Some(var) = name.strip_prefix('@') {
            self.user_vars.insert(var.to_ascii_lowercase(), v);
            return Ok(Outcome::command("SET"));
        }
        let text = match &v {
            Datum::Null => default_vars(self.dialect)
                .get(name)
                .cloned()
                .unwrap_or_default(),
            other => other.to_text(self.dialect == Dialect::MySql),
        };
        let key = match name {
            "time zone" => "timezone",
            "transaction_isolation" | "tx_isolation" => "transaction_isolation",
            other => other,
        };
        if key == "implicit_transactions" {
            // T-SQL: implicit transactions are autocommit turned off, but switching never
            // commits or opens a transaction by itself (the next statement opens one).
            let off = if text.eq_ignore_ascii_case("on") {
                "0"
            } else {
                "1"
            };
            self.vars.insert("autocommit".into(), off.into());
            self.vars.insert(key.into(), text);
            return Ok(Outcome::command("SET"));
        }
        if key == "autocommit"
            && ["0", "off", "false"]
                .iter()
                .any(|f| text.eq_ignore_ascii_case(f))
        {
            // MySQL autocommit=0: an implicit transaction stays open until COMMIT.
            if !self.in_transaction() {
                self.txn = Some(Txn {
                    explicit: true,
                    ..Txn::default()
                });
            }
        } else if key == "autocommit" && self.in_transaction() {
            self.commit()?;
        }
        if key == "next_transaction_isolation" && self.in_transaction() {
            // PostgreSQL: SET TRANSACTION applies to the transaction already open.
            let v = self.validates_reads(Some(&text));
            if let Some(t) = self.txn.as_mut() {
                t.validate_reads = v;
            }
            return Ok(Outcome::command("SET"));
        }
        self.vars.insert(key.to_owned(), text);
        Ok(Outcome::command("SET"))
    }
}

/// Whether re-running a statement after a lost write race is safe (it has no external effect).
fn is_retry_safe(stmt: &Statement) -> bool {
    !matches!(
        stmt,
        Statement::CreateTable(_) | Statement::DropTable { .. } | Statement::AlterTable { .. }
    )
}

/// Result columns of `RETURNING items` on `table` (for protocol-level Describe).
pub fn describe_returning(
    s: &Session,
    table: &str,
    items: &[crate::ast::SelectItem],
) -> SqlResult<Vec<OutCol>> {
    dml::describe_returning(s, table, items)
}
