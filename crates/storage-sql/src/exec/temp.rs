//! Temporary tables (`F-ENT-05`, `TASK-149`): `CREATE TEMP TABLE`, MySQL `CREATE TEMPORARY TABLE`,
//! T-SQL `#name`.
//!
//! A temporary table's schema lives in its session only; its rows are an ordinary collection with a
//! name of its own, so constraints, indexes, joins and transactions all work as usual. It hides a
//! permanent table of the same name for that session, and no other session can reach it.
//!
//! It is dropped with its session or, by `ON COMMIT DROP`, its transaction. A gateway that dies
//! leaves its collections behind: gateways keep a lease, and whichever gateway next creates a
//! temporary table removes those of leases long expired.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use storine::Value;

use crate::ast::OnCommit;
use crate::catalog::Table;
use crate::error::{SqlError, SqlResult};
use crate::exec::{now_ns, Engine, Session};
use crate::store::{Op, Pre, Store, Tag, Write};

/// A session's temporary tables, by lower-case name.
pub type TempTables = Arc<Mutex<HashMap<String, Arc<Table>>>>;

const PREFIX: &str = "_sql_tmp_";
const LEASES: &str = "_sql_gateways";
/// A gateway silent for this long is gone (an idle session's tables go with it: documented limit).
const LEASE_NS: i64 = 3_600_000_000_000;

thread_local! {
    /// The temporary tables of the session running on this thread.
    static CURRENT: RefCell<Option<TempTables>> = const { RefCell::new(None) };
    /// Inside `hidden`: code an owner wrote is not to read what the caller named like its tables.
    static HIDDEN: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Runs `f` (a view's body, a row policy) seeing no temporary tables: a caller's `CREATE TEMP TABLE
/// memberships` must not stand in for the table a policy reads.
pub(crate) fn hidden<R>(f: impl FnOnce() -> R) -> R {
    HIDDEN.with(|h| h.set(h.get() + 1));
    let out = f();
    HIDDEN.with(|h| h.set(h.get() - 1));
    out
}

/// What this thread's name lookups see from now on.
pub(crate) fn enter(tables: &TempTables) {
    CURRENT.with(|c| *c.borrow_mut() = Some(Arc::clone(tables)));
}

/// The temporary table of that name, for the session on this thread.
pub(crate) fn lookup(name: &str) -> Option<Arc<Table>> {
    if HIDDEN.with(std::cell::Cell::get) > 0 {
        return None;
    }
    CURRENT.with(|c| {
        let c = c.borrow();
        let tables = c.as_ref()?.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        tables.get(&name.to_ascii_lowercase()).cloned()
    })
}

/// Records a temporary table's schema.
pub(crate) fn save(t: &Table) {
    CURRENT.with(|c| {
        if let Some(tables) = c.borrow().as_ref() {
            let mut next = t.clone();
            next.version += 1;
            tables.lock().unwrap_or_else(std::sync::PoisonError::into_inner).insert(t.name.to_ascii_lowercase(), Arc::new(next));
        }
    });
}

pub(crate) fn remove(name: &str) {
    CURRENT.with(|c| {
        if let Some(tables) = c.borrow().as_ref() {
            tables.lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(&name.to_ascii_lowercase());
        }
    });
}

/// The collection of a session's temporary table.
pub(crate) fn collection(session: u64, table: &str) -> String {
    let clean: String = table.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).take(64).collect();
    let h = crate::hash::hex(&crate::hash::sha256(table.as_bytes()));
    format!("{PREFIX}{}_{session}_{clean}_{}", crate::instance_id(), &h[..8])
}

impl Engine {
    /// Keeps this gateway's lease and removes temporary tables of gateways long gone.
    pub(crate) fn sweep_temp_tables(&self) -> SqlResult<()> {
        let store = self.root_store();
        let now = now_ns();
        let mut doc = storine::Doc::new();
        doc.insert("expires".into(), Value::Timestamp(now.saturating_add(LEASE_NS)));
        let w = Write { collection: LEASES.into(), key: crate::instance_id().to_owned(), op: Op::Put(doc), pre: Pre::None, tag: Tag::Internal };
        store.commit(std::slice::from_ref(&w)).map_err(|e| e.to_sql(std::slice::from_ref(&w)))?;
        let live: Vec<String> = store.scan(LEASES, None)?.into_iter().filter(|d| matches!(d.fields.get("expires"), Some(Value::Timestamp(t)) if *t >= now)).map(|d| d.key).collect();
        sweep(store, &live)?;
        sweep(&self.store, &live)
    }
}

fn sweep(store: &Store, live: &[String]) -> SqlResult<()> {
    for c in store.collections()? {
        let Some(rest) = c.strip_prefix(PREFIX) else { continue };
        if !live.iter().any(|g| rest.starts_with(&format!("{g}_"))) {
            store.drop_collection(&c)?;
        }
    }
    Ok(())
}

impl Session {
    /// Ends what `ON COMMIT` asked for, now that no transaction is open.
    pub(crate) fn temp_after_transaction(&mut self) -> SqlResult<()> {
        let tables: Vec<Arc<Table>> = self.temp.lock().unwrap_or_else(std::sync::PoisonError::into_inner).values().cloned().collect();
        for t in tables {
            match t.temp {
                Some(OnCommit::Drop) => {
                    self.engine.store.drop_collection(&t.collection)?;
                    remove(&t.name);
                }
                Some(OnCommit::DeleteRows) => {
                    self.engine.store.drop_collection(&t.collection)?;
                    crate::exec::ddl::delete_markers(self, &t.collection, None)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Drops every temporary table of this session (`Drop`, and `DISCARD ALL` / `TEMP`).
    pub(crate) fn drop_temp_tables(&self) {
        let tables: Vec<Arc<Table>> = self.temp.lock().unwrap_or_else(std::sync::PoisonError::into_inner).drain().map(|(_, t)| t).collect();
        for t in tables {
            // Best effort: the sweep removes what a failure leaves.
            let _ = self.engine.store.drop_collection(&t.collection);
            let _ = crate::exec::ddl::delete_markers(self, &t.collection, None);
        }
    }
}

/// A temporary table cannot take part in what outlives it.
pub(crate) fn check_create(t: &Table) -> SqlResult<()> {
    if !t.foreign_keys.is_empty() {
        return Err(SqlError::unsupported("foreign keys on temporary tables"));
    }
    Ok(())
}
