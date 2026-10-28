//! INSERT, UPDATE and DELETE: constraint checks, unique markers, foreign keys, sequences, row
//! locks and the atomic fast path for `SET x = x + n`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use storine::{Doc, Value};

use crate::ast::{
    BinaryOp, DataType, Delete, Expr, Insert, InsertSource, OnConflict, Query, RefAction,
    SelectItem, SetExpr, TableRef, Update,
};
use crate::catalog::{encode_key, Row, Table, LOCKS, SEQUENCES, UNIQUE};
use crate::datum::{cast, Datum};
use crate::error::{SqlError, SqlResult};
use crate::exec::eval::{Env, Exec};
use crate::exec::select::{conjuncts, table_cols, ColMeta, Rel, TableRow};
use crate::exec::{Engine, Outcome, Session};
use crate::lexer::Dialect;
use crate::store::{CommitError, Op, Pre, Tag, Write};

// ── sequences ──

/// Allocates the next value of sequence `name` (compare-and-swap on its document).
pub fn next_sequence(engine: &Engine, name: &str) -> SqlResult<i64> {
    let store = &engine.store;
    for attempt in 0..10_000u32 {
        let (pre, n, step) = match store.get(SEQUENCES, name)? {
            None => (Pre::Absent, 1, 1),
            Some(d) => (
                Pre::Version(d.version),
                d.fields.get("next").and_then(Value::as_i64).unwrap_or(1),
                d.fields
                    .get("increment")
                    .and_then(Value::as_i64)
                    .unwrap_or(1),
            ),
        };
        let mut doc = Doc::new();
        doc.insert(
            "next".into(),
            Value::Int(n.checked_add(step).ok_or_else(|| {
                SqlError::new("2200H", format!("sequence {name} reached its limit"))
            })?),
        );
        if step != 1 {
            doc.insert("increment".into(), Value::Int(step));
        }
        let w = Write {
            collection: SEQUENCES.into(),
            key: name.into(),
            op: Op::Put(doc),
            pre,
            tag: Tag::Internal,
        };
        match store.commit(std::slice::from_ref(&w)) {
            Ok(_) => return Ok(n),
            Err(CommitError::Condition { .. }) => backoff(attempt),
            Err(e) => return Err(e.to_sql(std::slice::from_ref(&w))),
        }
    }
    Err(SqlError::serialization())
}

/// Makes sure sequence `name` never hands out `at_least` or anything below it.
pub fn raise_sequence(engine: &Engine, name: &str, at_least: i64) -> SqlResult<()> {
    let store = &engine.store;
    for attempt in 0..10_000u32 {
        let (pre, next) = match store.get(SEQUENCES, name)? {
            None => (Pre::Absent, 1),
            Some(d) => (
                Pre::Version(d.version),
                d.fields.get("next").and_then(Value::as_i64).unwrap_or(1),
            ),
        };
        if at_least < next {
            return Ok(());
        }
        let mut doc = Doc::new();
        doc.insert("next".into(), Value::Int(at_least.saturating_add(1)));
        let w = Write {
            collection: SEQUENCES.into(),
            key: name.into(),
            op: Op::Put(doc),
            pre,
            tag: Tag::Internal,
        };
        match store.commit(std::slice::from_ref(&w)) {
            Ok(_) => return Ok(()),
            Err(CommitError::Condition { .. }) => backoff(attempt),
            Err(e) => return Err(e.to_sql(std::slice::from_ref(&w))),
        }
    }
    Err(SqlError::serialization())
}

fn backoff(attempt: u32) {
    if attempt > 2 {
        std::thread::sleep(Duration::from_micros(u64::from(attempt.min(200)) * 50));
    }
}

fn auto_sequence(table: &Table, col: usize) -> String {
    format!("auto:{}.{}", table.collection, table.columns[col].field)
}

fn rowid_sequence(table: &Table) -> String {
    format!("rowid:{}", table.collection)
}

// ── reads through the transaction ──

/// A document as this transaction sees it: `(observed version, doc)`; the version is `None`
/// when this transaction wrote it.
fn read(s: &Session, collection: &str, key: &str) -> SqlResult<Option<(Option<u64>, Doc)>> {
    if let Some(t) = s.txn.as_ref() {
        if let Some(buffered) = t.get(collection, key) {
            return Ok(buffered.map(|d| (None, d.clone())));
        }
    }
    Ok(s.engine
        .store
        .get(collection, key)?
        .map(|d| (Some(d.version), d.fields)))
}

/// The version to base a write on: the observed version, or the one the overlay already has.
fn observed(v: Option<u64>) -> Option<u64> {
    v
}

// ── constraint checks ──

fn check_row(ex: &Exec<'_>, table: &Table, row: &Row) -> SqlResult<()> {
    for (i, c) in table.columns.iter().enumerate() {
        if c.not_null && row[i].is_null() {
            return Err(SqlError::not_null(&table.name, &c.name));
        }
    }
    if !table.checks.is_empty() {
        let cols = table_cols(table);
        let env = Env::new(&cols, row);
        for ck in &table.checks {
            // A CHECK passes unless it is false (NULL passes).
            if ex.eval(&ck.expr, &env)?.truthy()? == Some(false) {
                return Err(SqlError::check(&table.name, &ck.name));
            }
        }
    }
    Ok(())
}

/// The parent document a foreign key of `row` must find: `(collection, key, constraint)`.
fn fk_parent(
    s: &Session,
    table: &Table,
    fk: &crate::catalog::FkDef,
    row: &Row,
) -> SqlResult<Option<(String, String)>> {
    if fk.columns.iter().any(|i| row[*i].is_null()) {
        return Ok(None);
    }
    let parent = s.engine.table(&fk.ref_table)?;
    let ref_cols: Vec<usize> = if fk.ref_columns.is_empty() {
        parent.primary_key.clone()
    } else {
        fk.ref_columns
            .iter()
            .map(|c| parent.column(c))
            .collect::<SqlResult<_>>()?
    };
    if ref_cols.len() != fk.columns.len() {
        return Err(SqlError::foreign_key(format!(
            "foreign key \"{}\" does not match the referenced key of \"{}\"",
            fk.name, parent.name
        )));
    }
    let values: Vec<Datum> = fk
        .columns
        .iter()
        .zip(&ref_cols)
        .map(|(c, p)| cast(&row[*c], &parent.columns[*p].ty))
        .collect::<SqlResult<_>>()?;
    if ref_cols == parent.primary_key {
        let parts: Vec<&Datum> = values.iter().collect();
        return Ok(Some((parent.collection.clone(), encode_key(&parts)?)));
    }
    // A unique key of the parent: its marker document.
    let uk = parent
        .uniques
        .iter()
        .find(|u| u.columns == ref_cols)
        .ok_or_else(|| {
            SqlError::foreign_key(format!(
                "there is no unique constraint matching given keys for referenced table \"{}\"",
                parent.name
            ))
        })?;
    let mut prow = vec![Datum::Null; parent.columns.len()];
    for (p, v) in ref_cols.iter().zip(values) {
        prow[*p] = v;
    }
    let _ = table;
    Ok(parent
        .unique_marker(uk, &prow)?
        .map(|m| (UNIQUE.to_owned(), m)))
}

fn marker_doc(row_key: &str) -> Doc {
    let mut d = Doc::new();
    d.insert("row".into(), Value::Text(row_key.to_owned()));
    d
}

/// Buffers an inserted row with its unique markers and foreign-key checks.
fn apply_insert(s: &mut Session, table: &Table, key: &str, row: &Row) -> SqlResult<()> {
    // Unique values must be free (a clear error now; the absent preconditions make it race-free).
    let mut markers = Vec::new();
    for uk in &table.uniques {
        if let Some(m) = table.unique_marker(uk, row)? {
            if read(s, UNIQUE, &m)?.is_some() {
                return Err(SqlError::unique(&table.name, &uk.name));
            }
            markers.push((m, uk.name.clone()));
        }
    }
    let mut parents = Vec::new();
    for fk in table.foreign_keys.iter().filter(|_| s.foreign_key_checks()) {
        if let Some((coll, pkey)) = fk_parent(s, table, fk, row)? {
            if read(s, &coll, &pkey)?.is_none() {
                return Err(SqlError::foreign_key(format!(
                    "insert or update on table \"{}\" violates foreign key constraint \"{}\": the referenced row in \"{}\" does not exist",
                    table.name, fk.name, fk.ref_table
                )));
            }
            parents.push((coll, pkey, fk.name.clone()));
        }
    }
    let doc = table.to_doc(row);
    let txn = s.txn_mut();
    txn.write(
        &table.collection,
        key,
        None,
        Some(doc),
        Tag::Row {
            table: table.name.clone(),
        },
    );
    for (m, name) in markers {
        txn.write(
            UNIQUE,
            &m,
            None,
            Some(marker_doc(key)),
            Tag::Unique {
                table: table.name.clone(),
                constraint: name,
            },
        );
    }
    for (coll, pkey, name) in parents {
        txn.check(Write {
            collection: coll,
            key: pkey,
            op: Op::Check,
            pre: Pre::Exists,
            tag: Tag::Parent {
                table: table.name.clone(),
                constraint: name,
            },
        });
    }
    Ok(())
}

/// Buffers a changed row (same key): marker moves and foreign-key checks for changed columns.
fn apply_update(
    s: &mut Session,
    table: &Table,
    key: &str,
    version: Option<u64>,
    old: &Row,
    new: &Row,
) -> SqlResult<()> {
    let new_key = table.row_key(new)?;
    if let Some(nk) = &new_key {
        if nk != key {
            // The primary key changed: the row moves to a new key.
            if s.engine
                .catalog
                .referencing(&table.name)
                .iter()
                .any(|_| true)
            {
                plan_and_apply_delete(s, table, key, version, old, true)?;
            } else {
                apply_delete(s, table, key, version, old)?;
            }
            if read(s, &table.collection, nk)?.is_some() {
                return Err(SqlError::unique(
                    &table.name,
                    &format!("{}_pkey", table.name),
                ));
            }
            return apply_insert(s, table, nk, new);
        }
    }
    // (old marker with its version, new marker, constraint)
    type Move = (Option<(String, Option<u64>)>, Option<String>, String);
    let mut moves: Vec<Move> = Vec::new();
    for uk in &table.uniques {
        let (om, nm) = (table.unique_marker(uk, old)?, table.unique_marker(uk, new)?);
        if om == nm {
            continue;
        }
        let old_entry = match om {
            Some(m) => {
                let v = read(s, UNIQUE, &m)?.map(|(v, _)| v).unwrap_or(None);
                Some((m, v))
            }
            None => None,
        };
        if let Some(m) = &nm {
            if read(s, UNIQUE, m)?.is_some() && old_entry.as_ref().map(|(o, _)| o) != Some(m) {
                return Err(SqlError::unique(&table.name, &uk.name));
            }
        }
        moves.push((old_entry, nm, uk.name.clone()));
    }
    let mut parents = Vec::new();
    for fk in table.foreign_keys.iter().filter(|_| s.foreign_key_checks()) {
        if fk
            .columns
            .iter()
            .all(|c| crate::datum::group_eq(&old[*c], &new[*c]))
        {
            continue;
        }
        if let Some((coll, pkey)) = fk_parent(s, table, fk, new)? {
            if read(s, &coll, &pkey)?.is_none() {
                return Err(SqlError::foreign_key(format!(
                    "insert or update on table \"{}\" violates foreign key constraint \"{}\"",
                    table.name, fk.name
                )));
            }
            parents.push((coll, pkey, fk.name.clone()));
        }
    }
    let doc = table.to_doc(new);
    let txn = s.txn_mut();
    txn.write(
        &table.collection,
        key,
        observed(version),
        Some(doc),
        Tag::Row {
            table: table.name.clone(),
        },
    );
    for (old_m, new_m, name) in moves {
        if let Some((m, v)) = old_m {
            txn.write(
                UNIQUE,
                &m,
                v,
                None,
                Tag::Unique {
                    table: table.name.clone(),
                    constraint: name.clone(),
                },
            );
        }
        if let Some(m) = new_m {
            txn.write(
                UNIQUE,
                &m,
                None,
                Some(marker_doc(key)),
                Tag::Unique {
                    table: table.name.clone(),
                    constraint: name,
                },
            );
        }
    }
    for (coll, pkey, name) in parents {
        txn.check(Write {
            collection: coll,
            key: pkey,
            op: Op::Check,
            pre: Pre::Exists,
            tag: Tag::Parent {
                table: table.name.clone(),
                constraint: name,
            },
        });
    }
    Ok(())
}

/// Buffers a deleted row and its markers (no foreign-key work: see `plan_and_apply_delete`).
fn apply_delete(
    s: &mut Session,
    table: &Table,
    key: &str,
    version: Option<u64>,
    old: &Row,
) -> SqlResult<()> {
    let mut markers = Vec::new();
    for uk in &table.uniques {
        if let Some(m) = table.unique_marker(uk, old)? {
            let v = read(s, UNIQUE, &m)?.map(|(v, _)| v).unwrap_or(None);
            markers.push((m, v, uk.name.clone()));
        }
    }
    let txn = s.txn_mut();
    txn.write(
        &table.collection,
        key,
        observed(version),
        None,
        Tag::Row {
            table: table.name.clone(),
        },
    );
    for (m, v, name) in markers {
        txn.write(
            UNIQUE,
            &m,
            v,
            None,
            Tag::Unique {
                table: table.name.clone(),
                constraint: name,
            },
        );
    }
    Ok(())
}

enum Action {
    Delete {
        table: Arc<Table>,
        key: String,
        version: Option<u64>,
        row: Row,
    },
    Update {
        table: Arc<Table>,
        key: String,
        version: Option<u64>,
        old: Row,
        new: Row,
    },
}

/// Deletes a row and applies the referential actions of the foreign keys that point at it.
fn plan_and_apply_delete(
    s: &mut Session,
    table: &Table,
    key: &str,
    version: Option<u64>,
    old: &Row,
    key_change: bool,
) -> SqlResult<()> {
    let mut plan = Vec::new();
    let mut seen = HashSet::new();
    let me = s.engine.table(&table.name)?;
    plan_delete(
        s, &me, key, version, old, &mut plan, &mut seen, 0, key_change,
    )?;
    for a in plan {
        match a {
            Action::Delete {
                table,
                key,
                version,
                row,
            } => apply_delete(s, &table, &key, version, &row)?,
            Action::Update {
                table,
                key,
                version,
                old,
                new,
            } => apply_update(s, &table, &key, version, &old, &new)?,
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn plan_delete(
    s: &Session,
    table: &Arc<Table>,
    key: &str,
    version: Option<u64>,
    old: &Row,
    plan: &mut Vec<Action>,
    seen: &mut HashSet<(String, String)>,
    depth: usize,
    key_change: bool,
) -> SqlResult<()> {
    if depth > 32 {
        return Err(SqlError::new(
            "54001",
            "foreign key cascades are nested too deeply",
        ));
    }
    if !seen.insert((table.collection.clone(), key.to_owned())) {
        return Ok(());
    }
    plan.push(Action::Delete {
        table: Arc::clone(table),
        key: key.to_owned(),
        version,
        row: old.clone(),
    });
    for (child, fk) in s.engine.catalog.referencing(&table.name) {
        let ref_cols: Vec<usize> = if fk.ref_columns.is_empty() {
            table.primary_key.clone()
        } else {
            fk.ref_columns
                .iter()
                .filter_map(|c| table.column_index(c))
                .collect()
        };
        if ref_cols.len() != fk.columns.len() || ref_cols.iter().any(|i| old[*i].is_null()) {
            continue;
        }
        // Children whose foreign-key columns equal the deleted row's referenced columns.
        let cond = fk
            .columns
            .iter()
            .zip(&ref_cols)
            .map(|(c, p)| {
                Expr::binary(
                    BinaryOp::Eq,
                    Expr::Column(vec![child.name.clone(), child.columns[*c].name.clone()]),
                    literal(&old[*p]),
                )
            })
            .reduce(|a, b| Expr::binary(BinaryOp::And, a, b));
        let Some(cond) = cond else { continue };
        let ex = Exec::new(s, &[]);
        let conjs = conjuncts(&cond);
        let cols = table_cols(&child);
        let mut kids = Vec::new();
        for tr in ex.scan_rows(&child, Some(&child.name), &conjs, Vec::new(), None)? {
            if ex.eval_bool(&cond, &Env::new(&cols, &tr.row))?
                && !seen.contains(&(child.collection.clone(), tr.key.clone()))
            {
                kids.push(tr);
            }
        }
        drop(ex);
        if kids.is_empty() {
            continue;
        }
        let action = if key_change {
            fk.on_update
        } else {
            fk.on_delete
        };
        match action {
            RefAction::Cascade if !key_change => {
                for k in kids {
                    plan_delete(s, &child, &k.key, k.version, &k.row, plan, seen, depth + 1, false)?;
                }
            }
            RefAction::SetNull | RefAction::SetDefault => {
                let ex = Exec::new(s, &[]);
                for k in kids {
                    let mut new = k.row.clone();
                    for c in &fk.columns {
                        new[*c] = match (&action, &child.columns[*c].default) {
                            (RefAction::SetDefault, Some(d)) => cast(&ex.eval_const(d)?, &child.columns[*c].ty)?,
                            _ => Datum::Null,
                        };
                    }
                    check_row(&ex, &child, &new)?;
                    seen.insert((child.collection.clone(), k.key.clone()));
                    plan.push(Action::Update { table: Arc::clone(&child), key: k.key, version: k.version, old: k.row, new });
                }
            }
            _ => {
                return Err(SqlError::foreign_key(format!(
                    "update or delete on table \"{}\" violates foreign key constraint \"{}\" on table \"{}\"",
                    table.name, fk.name, child.name
                )))
            }
        }
    }
    Ok(())
}

/// A literal expression for a value (used to build internal conditions).
fn literal(d: &Datum) -> Expr {
    match d {
        Datum::Null => Expr::Null,
        Datum::Bool(b) => Expr::Bool(*b),
        Datum::Int(i) => Expr::Number(i.to_string()),
        Datum::Numeric(x) => Expr::Number(x.to_string()),
        other => Expr::Str(other.as_text()),
    }
}

// ── row locks ──

fn lock_owner(s: &Session) -> String {
    format!("{}#{}", crate::instance_id(), s.id)
}

/// Locks rows of `table` for this transaction, waiting for other holders (`nowait`: fail at once).
pub fn lock_rows(s: &mut Session, table: &Table, keys: &[String], nowait: bool) -> SqlResult<()> {
    let owner = lock_owner(s);
    let timeout = s.engine.config.lock_timeout;
    for key in keys {
        let lock_key = format!("{}/{}", table.collection, key);
        if s.txn.as_ref().is_some_and(|t| t.locks.contains(&lock_key)) {
            continue;
        }
        let deadline = Instant::now() + timeout;
        let mut delay = Duration::from_millis(1);
        loop {
            let mut doc = Doc::new();
            doc.insert("owner".into(), Value::Text(owner.clone()));
            doc.insert(
                "expires".into(),
                Value::Timestamp(crate::exec::now_ns() + 120 * 1_000_000_000),
            );
            let w = Write {
                collection: LOCKS.into(),
                key: lock_key.clone(),
                op: Op::Put(doc),
                pre: Pre::Absent,
                tag: Tag::Lock,
            };
            match s.engine.store.commit(std::slice::from_ref(&w)) {
                Ok(_) => {
                    s.txn_mut().locks.push(lock_key.clone());
                    break;
                }
                Err(CommitError::Condition { .. }) => {
                    // Held by someone: break it if it expired (a crashed gateway), else wait.
                    if let Some(d) = s.engine.store.get(LOCKS, &lock_key)? {
                        let expired = matches!(d.fields.get("expires"), Some(Value::Timestamp(t)) if *t < crate::exec::now_ns());
                        if d.fields.get("owner").and_then(Value::as_str) == Some(owner.as_str()) {
                            s.txn_mut().locks.push(lock_key.clone());
                            break;
                        }
                        if expired {
                            let del = Write {
                                collection: LOCKS.into(),
                                key: lock_key.clone(),
                                op: Op::Delete,
                                pre: Pre::Version(d.version),
                                tag: Tag::Lock,
                            };
                            let _ = s.engine.store.commit(std::slice::from_ref(&del));
                            continue;
                        }
                    } else {
                        continue;
                    }
                    if nowait || Instant::now() >= deadline {
                        return Err(SqlError::lock_timeout(&format!(
                            "row in relation \"{}\"",
                            table.name
                        )));
                    }
                    std::thread::sleep(delay);
                    delay = (delay * 2).min(Duration::from_millis(25));
                }
                Err(e) => return Err(e.to_sql(std::slice::from_ref(&w))),
            }
        }
    }
    Ok(())
}

/// `SELECT ... FOR UPDATE` in a transaction: locks the rows of its single base table first, so
/// the query then reads their latest values.
pub fn lock_query_rows(s: &mut Session, q: &Query, params: &[Datum]) -> SqlResult<()> {
    let SetExpr::Select(sel) = &q.body else {
        return Ok(());
    };
    let [TableRef::Named {
        schema: None,
        name,
        alias,
    }] = sel.from.as_slice()
    else {
        return Ok(());
    };
    let nowait = matches!(
        q.lock,
        Some(crate::ast::LockMode::Update { nowait: true, .. })
    );
    let Some(table) = s.engine.find_table(name)? else {
        return Ok(());
    };
    let qual = alias.as_ref().map_or(name.clone(), |a| a.name.clone());
    let keys = {
        let ex = Exec::new(s, params);
        target_rows(&ex, &table, &qual, sel.where_.as_ref())?
            .into_iter()
            .map(|t| t.key)
            .collect::<Vec<_>>()
    };
    lock_rows(s, &table, &keys, nowait)
}

/// Rows of `table` matching `where_` (evaluated exactly).
fn target_rows(
    ex: &Exec<'_>,
    table: &Table,
    qual: &str,
    where_: Option<&Expr>,
) -> SqlResult<Vec<TableRow>> {
    let conjs: Vec<&Expr> = where_.map(conjuncts).unwrap_or_default();
    let mut cols = table_cols(table);
    for c in &mut cols {
        c.qual = Some(qual.to_owned());
    }
    let mut out = Vec::new();
    for tr in ex.scan_rows(table, Some(qual), &conjs, Vec::new(), None)? {
        let ok = match where_ {
            None => true,
            Some(w) => ex.eval_bool(w, &Env::new(&cols, &tr.row))?,
        };
        if ok {
            out.push(tr);
        }
    }
    Ok(out)
}

// ── INSERT ──

fn returning_cols(table: &Table, qual: &str) -> Vec<ColMeta> {
    let mut cols = table_cols(table);
    for c in &mut cols {
        c.qual = Some(qual.to_owned());
    }
    cols
}

fn project_returning(
    ex: &Exec<'_>,
    items: &[SelectItem],
    cols: &[ColMeta],
    rows: &[Row],
) -> SqlResult<Rel> {
    let mut out_cols = Vec::new();
    let mut exprs = Vec::new();
    for it in items {
        match it {
            SelectItem::Wildcard(_) => {
                for c in cols {
                    out_cols.push(ColMeta {
                        qual: None,
                        ..c.clone()
                    });
                    exprs.push(Expr::Column(vec![c.name.clone()]));
                }
            }
            SelectItem::Expr { expr, alias } => {
                let (ty, table) = ex.static_type(expr, cols);
                let name = alias.clone().unwrap_or_else(|| {
                    expr.column_name()
                        .map_or_else(|| "?column?".to_owned(), str::to_owned)
                });
                out_cols.push(ColMeta {
                    qual: None,
                    name,
                    ty,
                    table,
                    hidden: false,
                });
                exprs.push(expr.clone());
            }
        }
    }
    let mut out = Vec::new();
    for r in rows {
        let env = Env::new(cols, r);
        out.push(
            exprs
                .iter()
                .map(|e| ex.eval(e, &env))
                .collect::<SqlResult<Row>>()?,
        );
    }
    Ok(Rel {
        cols: out_cols,
        rows: out,
    })
}

/// Fills defaults, generated ids and casts for one inserted row.
fn build_row(
    ex: &Exec<'_>,
    table: &Table,
    target: &[usize],
    values: Vec<Option<Datum>>,
    generated: &mut Option<i64>,
) -> SqlResult<Row> {
    let n = table.columns.len();
    let mut row = vec![Datum::Null; n];
    let mut given = vec![false; n];
    for (ti, v) in target.iter().zip(values) {
        if let Some(v) = v {
            row[*ti] = cast(&v, &table.columns[*ti].ty).map_err(|e| {
                SqlError::new(
                    e.code,
                    format!("{} (column \"{}\")", e.message, table.columns[*ti].name),
                )
            })?;
            given[*ti] = true;
        }
    }
    let mysql = ex.dialect() == Dialect::MySql;
    for (i, c) in table.columns.iter().enumerate() {
        if c.auto_increment {
            let generate =
                !given[i] || (row[i].is_null() && mysql) || (mysql && row[i] == Datum::Int(0));
            if generate {
                let id = next_sequence(&ex.s.engine, &auto_sequence(table, i))?;
                row[i] = cast(&Datum::Int(id), &c.ty)?;
                if generated.is_none() {
                    *generated = Some(id);
                }
            } else if let Ok(v) = row[i].as_i64() {
                // An explicit id moves the sequence past it (no later collision).
                raise_sequence(&ex.s.engine, &auto_sequence(table, i), v)?;
            }
            continue;
        }
        if !given[i] {
            if let Some(d) = &c.default {
                row[i] = cast(&ex.eval_const(d)?, &c.ty)?;
            }
        }
    }
    check_row(ex, table, &row)?;
    Ok(row)
}

pub fn insert(s: &mut Session, ins: &Insert, params: &[Datum]) -> SqlResult<Outcome> {
    let table = s.engine.table(&ins.table)?;
    let target: Vec<usize> = if ins.columns.is_empty() {
        if matches!(ins.source, InsertSource::DefaultValues) {
            Vec::new()
        } else {
            (0..table.columns.len()).collect()
        }
    } else {
        let mut seen = HashSet::new();
        let mut v = Vec::new();
        for c in &ins.columns {
            let i = table.column(c)?;
            if !seen.insert(i) {
                return Err(SqlError::new(
                    "42701",
                    format!("column \"{c}\" specified more than once"),
                ));
            }
            v.push(i);
        }
        v
    };
    let qual = ins.alias.clone().unwrap_or_else(|| table.name.clone());
    // Evaluate every source row first (reads only).
    let mut built: Vec<(Row, Option<i64>)> = Vec::new();
    let mut first_generated: Option<i64> = None;
    {
        let ex = Exec::new(s, params);
        let source: Vec<Vec<Option<Datum>>> = match &ins.source {
            InsertSource::DefaultValues => vec![Vec::new()],
            InsertSource::Values(rows) => {
                let mut out = Vec::with_capacity(rows.len());
                for r in rows {
                    if r.len() != target.len() {
                        return Err(SqlError::syntax(format!(
                            "INSERT has {} expressions but {} target columns",
                            r.len(),
                            target.len()
                        )));
                    }
                    out.push(
                        r.iter()
                            .map(|e| {
                                if *e == Expr::Default {
                                    Ok(None)
                                } else {
                                    ex.eval_const(e).map(Some)
                                }
                            })
                            .collect::<SqlResult<_>>()?,
                    );
                }
                out
            }
            InsertSource::Query(q) => {
                let rel = ex.query(q, None)?;
                if !rel.rows.is_empty() && rel.cols.len() != target.len() {
                    return Err(SqlError::syntax(
                        "INSERT has more target columns than expressions".into(),
                    ));
                }
                rel.rows
                    .into_iter()
                    .map(|r| r.into_iter().map(Some).collect())
                    .collect()
            }
        };
        for vals in source {
            let mut gen = None;
            let row = build_row(&ex, &table, &target, vals, &mut gen)?;
            built.push((row, gen));
        }
    }
    let mut affected = 0u64;
    let mut returned: Vec<Row> = Vec::new();
    let mut statement_keys = HashSet::new();
    for (row, gen) in built {
        let key = match table.row_key(&row)? {
            Some(k) => k,
            None => format!("r{}", next_sequence(&s.engine, &rowid_sequence(&table))?),
        };
        // Find a conflicting row: same primary key or same value of a unique key.
        let mut conflict: Option<(String, Option<u64>, Row, String)> = None;
        if let Some((v, d)) = read(s, &table.collection, &key)? {
            conflict = Some((
                key.clone(),
                v,
                table.from_doc(&d),
                format!("{}_pkey", table.name),
            ));
        }
        if conflict.is_none() {
            for uk in &table.uniques {
                if let Some(m) = table.unique_marker(uk, &row)? {
                    if let Some((_, md)) = read(s, UNIQUE, &m)? {
                        let rk = md
                            .get("row")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_owned();
                        if let Some((v, d)) = read(s, &table.collection, &rk)? {
                            conflict = Some((rk, v, table.from_doc(&d), uk.name.clone()));
                            break;
                        }
                    }
                }
            }
        }
        match conflict {
            None => {
                apply_insert(s, &table, &key, &row)?;
                statement_keys.insert(key);
                affected += 1;
                if first_generated.is_none() {
                    first_generated = gen;
                }
                returned.push(row);
            }
            Some((ckey, cver, existing, constraint)) => {
                if statement_keys.contains(&ckey)
                    && matches!(ins.on_conflict, Some(OnConflict::DoUpdate { .. }))
                    && s.dialect != Dialect::MySql
                {
                    return Err(SqlError::new(
                        "21000",
                        "ON CONFLICT DO UPDATE command cannot affect row a second time",
                    ));
                }
                match &ins.on_conflict {
                    Some(OnConflict::DoNothing { .. }) => {}
                    Some(OnConflict::DoUpdate { sets, where_, .. }) => {
                        // `excluded.*` (PostgreSQL / SQLite), `VALUES(col)` or the row alias (MySQL).
                        let mut cols = returning_cols(&table, &table.name);
                        let mut env_row = existing.clone();
                        for c in table_cols(&table) {
                            cols.push(ColMeta {
                                qual: Some("excluded".into()),
                                hidden: true,
                                ..c.clone()
                            });
                        }
                        env_row.extend(row.iter().cloned());
                        if let Some(a) = &ins.alias {
                            for c in table_cols(&table) {
                                cols.push(ColMeta {
                                    qual: Some(a.clone()),
                                    hidden: true,
                                    ..c
                                });
                            }
                            env_row.extend(row.iter().cloned());
                        }
                        let new = {
                            let ex = Exec::new(s, params);
                            let env = Env::new(&cols, &env_row);
                            if let Some(w) = where_ {
                                if !ex.eval_bool(w, &env)? {
                                    continue;
                                }
                            }
                            let mut new = existing.clone();
                            for (c, e) in sets {
                                let i = table.column(c)?;
                                let v = if *e == Expr::Default {
                                    match &table.columns[i].default {
                                        Some(d) => ex.eval_const(d)?,
                                        None => Datum::Null,
                                    }
                                } else {
                                    ex.eval(e, &env)?
                                };
                                new[i] = cast(&v, &table.columns[i].ty)?;
                            }
                            check_row(&ex, &table, &new)?;
                            new
                        };
                        let changed = new != existing;
                        apply_update(s, &table, &ckey, cver, &existing, &new)?;
                        // MySQL reports 2 for an updated row, 0 when nothing changed.
                        affected += if s.dialect == Dialect::MySql {
                            if changed {
                                2
                            } else {
                                0
                            }
                        } else {
                            1
                        };
                        returned.push(new);
                    }
                    None if ins.replace => {
                        plan_and_apply_delete(s, &table, &ckey, cver, &existing, false)?;
                        // Other unique keys of the new row may hit further rows: remove them too.
                        for uk in &table.uniques {
                            if let Some(m) = table.unique_marker(uk, &row)? {
                                if let Some((_, md)) = read(s, UNIQUE, &m)? {
                                    let rk = md
                                        .get("row")
                                        .and_then(Value::as_str)
                                        .unwrap_or_default()
                                        .to_owned();
                                    if let Some((v, d)) = read(s, &table.collection, &rk)? {
                                        let r = table.from_doc(&d);
                                        plan_and_apply_delete(s, &table, &rk, v, &r, false)?;
                                        affected += 1;
                                    }
                                }
                            }
                        }
                        apply_insert(s, &table, &key, &row)?;
                        affected += 2;
                        returned.push(row);
                    }
                    None if ins.ignore => {}
                    None => return Err(SqlError::unique(&table.name, &constraint)),
                }
            }
        }
    }
    if let Some(id) = first_generated {
        s.last_insert_id = id;
    }
    if !ins.returning.is_empty() {
        let ex = Exec::new(s, params);
        let rel = project_returning(
            &ex,
            &ins.returning,
            &returning_cols(&table, &qual),
            &returned,
        )?;
        let rs = ex.result_set(rel)?;
        return Ok(Outcome::Rows(rs));
    }
    Ok(Outcome::Command {
        tag: format!("INSERT 0 {affected}"),
        affected,
        last_insert_id: first_generated,
    })
}

// ── UPDATE ──

/// Autocommit `UPDATE t SET n = n + 1 WHERE pk = ...`: one server-side atomic patch, no read and
/// no retry loop. Returns `None` when the statement does not have that shape.
fn try_atomic_update(
    s: &mut Session,
    upd: &Update,
    table: &Table,
    params: &[Datum],
) -> SqlResult<Option<Outcome>> {
    if s.in_transaction()
        || !upd.from.is_empty()
        || !upd.returning.is_empty()
        || !upd.order_by.is_empty()
        || upd.limit.is_some()
    {
        return Ok(None);
    }
    if table.primary_key.is_empty() {
        return Ok(None);
    }
    let Some(w) = &upd.where_ else {
        return Ok(None);
    };
    let qual = upd.alias.as_deref().unwrap_or(&table.name);
    let ex = Exec::new(s, params);
    // WHERE must be exactly `pk1 = const AND pk2 = const ...`.
    let mut pk_vals: Vec<Option<Datum>> = vec![None; table.primary_key.len()];
    for c in conjuncts(w) {
        let Expr::Binary {
            op: BinaryOp::Eq,
            left,
            right,
        } = c
        else {
            return Ok(None);
        };
        let (col, val) = match (column_of(table, qual, left), column_of(table, qual, right)) {
            (Some(ci), None) => (ci, right),
            (None, Some(ci)) => (ci, left),
            _ => return Ok(None),
        };
        let Some(slot) = table.primary_key.iter().position(|p| *p == col) else {
            return Ok(None);
        };
        let Ok(v) = ex.eval_const(val) else {
            return Ok(None);
        };
        pk_vals[slot] = Some(cast(&v, &table.columns[col].ty)?);
    }
    let Some(pk) = pk_vals.into_iter().collect::<Option<Vec<Datum>>>() else {
        return Ok(None);
    };
    let constrained: HashSet<usize> = table
        .primary_key
        .iter()
        .chain(table.uniques.iter().flat_map(|u| u.columns.iter()))
        .chain(table.foreign_keys.iter().flat_map(|f| f.columns.iter()))
        .copied()
        .collect();
    let mut ops = Vec::new();
    let mut touched = HashSet::new();
    for (c, e) in &upd.sets {
        let ci = table.column(c)?;
        if constrained.contains(&ci) || !touched.insert(ci) {
            return Ok(None);
        }
        let col = &table.columns[ci];
        let numeric = matches!(
            col.ty,
            DataType::SmallInt | DataType::Int | DataType::BigInt | DataType::Numeric(_)
        );
        match e {
            Expr::Binary {
                op: op @ (BinaryOp::Add | BinaryOp::Sub),
                left,
                right,
            } if numeric && column_of(table, qual, left) == Some(ci) => {
                if column_refs_any(right) {
                    return Ok(None);
                }
                let Ok(amount) = ex.eval_const(right) else {
                    return Ok(None);
                };
                let amount = match (&col.ty, amount) {
                    (_, Datum::Null) => return Ok(None),
                    (DataType::Numeric(spec), v) => {
                        let Ok(Datum::Numeric(d)) = cast(&v, &DataType::Numeric(None)) else {
                            return Ok(None);
                        };
                        if spec.is_some_and(|(_, sc)| d.scale() > sc) {
                            return Ok(None);
                        }
                        Datum::Numeric(if *op == BinaryOp::Sub {
                            d.checked_neg()
                                .map_err(|_| SqlError::numeric_out_of_range("out of range"))?
                        } else {
                            d
                        })
                    }
                    (_, v) => {
                        let Ok(i) = v.as_i64() else { return Ok(None) };
                        if matches!(v, Datum::Float(_) | Datum::Numeric(_))
                            && cast(&v, &DataType::BigInt).ok() != Some(Datum::Int(i))
                        {
                            return Ok(None);
                        }
                        Datum::Int(if *op == BinaryOp::Sub {
                            i.checked_neg()
                                .ok_or_else(|| SqlError::numeric_out_of_range("out of range"))?
                        } else {
                            i
                        })
                    }
                };
                let mut op_ = storine::add(&col.field, crate::datum::to_value(&amount, &col.ty));
                // Column range limits become bounds; a violated bound falls back to the general
                // path, which reports the exact error.
                let (lo, hi) = type_bounds(&col.ty);
                if let Some(lo) = lo {
                    op_ = op_.min(lo);
                }
                if let Some(hi) = hi {
                    op_ = op_.max(hi);
                }
                ops.push(op_);
            }
            _ => return Ok(None),
        }
    }
    // CHECK constraints must not mention the changed columns (they would need the new row).
    for ck in &table.checks {
        let mut refs = false;
        crate::exec::eval::walk(&ck.expr, &mut |x| {
            if let Expr::Column(p) = x {
                if let Some(i) = table.column_index(&p[p.len() - 1]) {
                    if touched.contains(&i) {
                        refs = true;
                    }
                }
            }
        });
        if refs {
            return Ok(None);
        }
    }
    drop(ex);
    let parts: Vec<&Datum> = pk.iter().collect();
    let key = encode_key(&parts)?;
    let w = Write {
        collection: table.collection.clone(),
        key,
        op: Op::Patch(ops),
        pre: Pre::None,
        tag: Tag::Row {
            table: table.name.clone(),
        },
    };
    match s.engine.store.commit(std::slice::from_ref(&w)) {
        Ok(_) => Ok(Some(Outcome::Command {
            tag: "UPDATE 1".into(),
            affected: 1,
            last_insert_id: None,
        })),
        Err(CommitError::Missing { .. }) => Ok(Some(Outcome::Command {
            tag: "UPDATE 0".into(),
            affected: 0,
            last_insert_id: None,
        })),
        // Bound violated or a stored value of another type: let the general path decide.
        Err(CommitError::Other(e)) if e.code == "22023" => Ok(None),
        Err(e) => Err(e.to_sql(std::slice::from_ref(&w))),
    }
}

/// Storage bounds that keep an integer / decimal column inside its declared range.
fn type_bounds(ty: &DataType) -> (Option<Value>, Option<Value>) {
    match ty {
        DataType::SmallInt => (
            Some(Value::Int(i64::from(i16::MIN))),
            Some(Value::Int(i64::from(i16::MAX))),
        ),
        DataType::Int => (
            Some(Value::Int(i64::from(i32::MIN))),
            Some(Value::Int(i64::from(i32::MAX))),
        ),
        DataType::Numeric(Some((p, s))) if *p > *s && *p <= 38 => {
            let digits = u32::from(*p);
            let m = 10i128.pow(digits.min(38)) - 1;
            match (storine::Decimal::new(m, *s), storine::Decimal::new(-m, *s)) {
                (Ok(hi), Ok(lo)) => (Some(Value::Decimal(lo)), Some(Value::Decimal(hi))),
                _ => (None, None),
            }
        }
        _ => (None, None),
    }
}

fn column_of(table: &Table, qual: &str, e: &Expr) -> Option<usize> {
    let Expr::Column(p) = e else { return None };
    if p.len() > 1
        && !p[p.len() - 2].eq_ignore_ascii_case(qual)
        && !p[p.len() - 2].eq_ignore_ascii_case(&table.name)
    {
        return None;
    }
    table.column_index(&p[p.len() - 1])
}

fn column_refs_any(e: &Expr) -> bool {
    let mut found = false;
    crate::exec::eval::walk(e, &mut |x| {
        if matches!(
            x,
            Expr::Column(_) | Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. }
        ) {
            found = true;
        }
    });
    found
}

/// Targets of UPDATE / DELETE, with the joined FROM / USING row when there is one.
/// Target rows with the joined FROM / USING row (empty without one).
type Targets = (Vec<ColMeta>, Vec<(TableRow, Row)>);

fn dml_targets(
    ex: &Exec<'_>,
    table: &Table,
    qual: &str,
    from: &[TableRef],
    where_: Option<&Expr>,
) -> SqlResult<Targets> {
    let mut cols = table_cols(table);
    for c in &mut cols {
        c.qual = Some(qual.to_owned());
    }
    if from.is_empty() {
        let rows = target_rows(ex, table, qual, where_)?;
        return Ok((cols, rows.into_iter().map(|t| (t, Vec::new())).collect()));
    }
    let other = ex.eval_from(from, &[], None)?;
    let mut all = cols.clone();
    all.extend(other.cols.iter().cloned());
    let conjs: Vec<&Expr> = where_.map(conjuncts).unwrap_or_default();
    let mut out = Vec::new();
    for tr in ex.scan_rows(table, Some(qual), &conjs, Vec::new(), None)? {
        for orow in &other.rows {
            let mut row = tr.row.clone();
            row.extend(orow.iter().cloned());
            let ok = match where_ {
                None => true,
                Some(w) => ex.eval_bool(w, &Env::new(&all, &row))?,
            };
            if ok {
                out.push((tr.clone(), orow.clone()));
                break;
            }
        }
    }
    Ok((all, out))
}

fn order_and_limit(
    ex: &Exec<'_>,
    cols: &[ColMeta],
    targets: &mut Vec<(TableRow, Row)>,
    order_by: &[crate::ast::OrderBy],
    limit: Option<&Expr>,
) -> SqlResult<()> {
    if !order_by.is_empty() {
        let mut keyed = Vec::new();
        for t in targets.drain(..) {
            let mut row = t.0.row.clone();
            row.extend(t.1.iter().cloned());
            let env = Env::new(cols, &row);
            let k = order_by
                .iter()
                .map(|o| ex.eval(&o.expr, &env))
                .collect::<SqlResult<Vec<_>>>()?;
            keyed.push((k, t));
        }
        keyed.sort_by(|a, b| {
            for (o, (x, y)) in order_by.iter().zip(a.0.iter().zip(&b.0)) {
                let c = crate::datum::compare(x, y);
                let c = if o.desc { c.reverse() } else { c };
                if c != std::cmp::Ordering::Equal {
                    return c;
                }
            }
            std::cmp::Ordering::Equal
        });
        targets.extend(keyed.into_iter().map(|(_, t)| t));
    }
    if let Some(l) = limit {
        let n = ex.eval_const(l)?.as_i64()?.max(0) as usize;
        targets.truncate(n);
    }
    Ok(())
}

pub fn update(s: &mut Session, upd: &Update, params: &[Datum]) -> SqlResult<Outcome> {
    let table = s.engine.table(&upd.table)?;
    if let Some(o) = try_atomic_update(s, upd, &table, params)? {
        return Ok(o);
    }
    let qual = upd.alias.clone().unwrap_or_else(|| table.name.clone());
    // In a transaction, lock the target rows first so their values are current.
    if s.in_transaction() {
        let keys = {
            let ex = Exec::new(s, params);
            let (_, t) = dml_targets(&ex, &table, &qual, &upd.from, upd.where_.as_ref())?;
            t.into_iter().map(|(t, _)| t.key).collect::<Vec<_>>()
        };
        lock_rows(s, &table, &keys, false)?;
    }
    let (changes, matched) = {
        let ex = Exec::new(s, params);
        let (cols, mut targets) = dml_targets(&ex, &table, &qual, &upd.from, upd.where_.as_ref())?;
        order_and_limit(&ex, &cols, &mut targets, &upd.order_by, upd.limit.as_ref())?;
        let matched = targets.len() as u64;
        let mut changes = Vec::with_capacity(targets.len());
        for (t, other) in targets {
            let mut env_row = t.row.clone();
            env_row.extend(other);
            let env = Env::new(&cols, &env_row);
            let mut new = t.row.clone();
            for (c, e) in &upd.sets {
                let i = table.column(c)?;
                let v = if *e == Expr::Default {
                    match &table.columns[i].default {
                        Some(d) => ex.eval_const(d)?,
                        None => Datum::Null,
                    }
                } else {
                    ex.eval(e, &env)?
                };
                new[i] = cast(&v, &table.columns[i].ty).map_err(|e| {
                    SqlError::new(
                        e.code,
                        format!("{} (column \"{}\")", e.message, table.columns[i].name),
                    )
                })?;
            }
            check_row(&ex, &table, &new)?;
            changes.push((t, new));
        }
        (changes, matched)
    };
    let mut changed = 0u64;
    let mut returned = Vec::new();
    for (t, new) in changes {
        if new != t.row {
            changed += 1;
            // Rows referenced by foreign keys keep their key (key changes cascade / restrict).
            apply_update(s, &table, &t.key, t.version, &t.row, &new)?;
        }
        returned.push(new);
    }
    if !upd.returning.is_empty() {
        let ex = Exec::new(s, params);
        let rel = project_returning(
            &ex,
            &upd.returning,
            &returning_cols(&table, &qual),
            &returned,
        )?;
        return Ok(Outcome::Rows(ex.result_set(rel)?));
    }
    // MySQL reports changed rows (matched rows with CLIENT_FOUND_ROWS: see the tag).
    let affected = if s.dialect == Dialect::MySql {
        changed
    } else {
        matched
    };
    Ok(Outcome::Command {
        tag: format!("UPDATE {matched}"),
        affected,
        last_insert_id: None,
    })
}

// ── DELETE ──

pub fn delete(s: &mut Session, del: &Delete, params: &[Datum]) -> SqlResult<Outcome> {
    let table = s.engine.table(&del.table)?;
    let qual = del.alias.clone().unwrap_or_else(|| table.name.clone());
    if s.in_transaction() {
        let keys = {
            let ex = Exec::new(s, params);
            let (_, t) = dml_targets(&ex, &table, &qual, &del.using, del.where_.as_ref())?;
            t.into_iter().map(|(t, _)| t.key).collect::<Vec<_>>()
        };
        lock_rows(s, &table, &keys, false)?;
    }
    let targets = {
        let ex = Exec::new(s, params);
        let (cols, mut targets) = dml_targets(&ex, &table, &qual, &del.using, del.where_.as_ref())?;
        order_and_limit(&ex, &cols, &mut targets, &del.order_by, del.limit.as_ref())?;
        targets
    };
    let referenced =
        s.foreign_key_checks() && !s.engine.catalog.referencing(&table.name).is_empty();
    let mut returned = Vec::new();
    let n = targets.len() as u64;
    for (t, _) in targets {
        if referenced {
            plan_and_apply_delete(s, &table, &t.key, t.version, &t.row, false)?;
        } else {
            apply_delete(s, &table, &t.key, t.version, &t.row)?;
        }
        returned.push(t.row);
    }
    if !del.returning.is_empty() {
        let ex = Exec::new(s, params);
        let rel = project_returning(
            &ex,
            &del.returning,
            &returning_cols(&table, &qual),
            &returned,
        )?;
        return Ok(Outcome::Rows(ex.result_set(rel)?));
    }
    Ok(Outcome::Command {
        tag: format!("DELETE {n}"),
        affected: n,
        last_insert_id: None,
    })
}

pub fn describe_returning(
    s: &Session,
    table: &str,
    items: &[SelectItem],
) -> SqlResult<Vec<crate::exec::OutCol>> {
    let t = s.engine.table(table)?;
    let ex = Exec::new(s, &[]);
    let rel = project_returning(&ex, items, &returning_cols(&t, &t.name), &[])?;
    Ok(ex.result_set(rel)?.columns)
}

fn sequence_key(name: &str) -> String {
    let bare = name.rsplit('.').next().unwrap_or(name).trim_matches('"');
    format!("seq:{bare}")
}

/// The stored sequence behind `name`: a `CREATE SEQUENCE` one, or the implicit
/// `<table>_<column>_seq` of a SERIAL / AUTO_INCREMENT column.
fn resolve_sequence(engine: &Engine, name: &str) -> SqlResult<String> {
    let key = sequence_key(name);
    if engine.store.get(SEQUENCES, &key)?.is_some() {
        return Ok(key);
    }
    let bare = &key["seq:".len()..];
    if let Some(stem) = bare.strip_suffix("_seq") {
        for (i, _) in stem.match_indices('_') {
            if let Ok(t) = engine.table(&stem[..i]) {
                if let Some(c) = t
                    .columns
                    .iter()
                    .position(|c| c.auto_increment && c.name.eq_ignore_ascii_case(&stem[i + 1..]))
                {
                    return Ok(auto_sequence(&t, c));
                }
            }
        }
    }
    Err(SqlError::new(
        "42P01",
        format!("relation \"{bare}\" does not exist"),
    ))
}

pub fn create_sequence(
    s: &mut Session,
    name: &str,
    start: i64,
    increment: i64,
    if_not_exists: bool,
) -> SqlResult<Outcome> {
    let mut doc = Doc::new();
    doc.insert("next".into(), Value::Int(start));
    doc.insert("increment".into(), Value::Int(increment));
    let w = Write {
        collection: SEQUENCES.into(),
        key: sequence_key(name),
        op: Op::Put(doc),
        pre: Pre::Absent,
        tag: Tag::Internal,
    };
    match s.engine.store.commit(std::slice::from_ref(&w)) {
        Ok(_) => Ok(Outcome::command("CREATE SEQUENCE")),
        Err(CommitError::Condition { .. }) if if_not_exists => {
            Ok(Outcome::command("CREATE SEQUENCE"))
        }
        Err(CommitError::Condition { .. }) => Err(SqlError::new(
            "42P07",
            format!("relation \"{name}\" already exists"),
        )),
        Err(e) => Err(e.to_sql(std::slice::from_ref(&w))),
    }
}

pub fn drop_sequences(s: &mut Session, names: &[String], if_exists: bool) -> SqlResult<Outcome> {
    for n in names {
        let key = sequence_key(n);
        if s.engine.store.get(SEQUENCES, &key)?.is_none() {
            if if_exists {
                continue;
            }
            return Err(SqlError::undefined_table(n));
        }
        let w = Write {
            collection: SEQUENCES.into(),
            key,
            op: Op::Delete,
            pre: Pre::None,
            tag: Tag::Internal,
        };
        s.engine
            .store
            .commit(std::slice::from_ref(&w))
            .map_err(|e| e.to_sql(std::slice::from_ref(&w)))?;
    }
    Ok(Outcome::command("DROP SEQUENCE"))
}

/// `nextval('name')`.
pub fn nextval(engine: &Engine, name: &str) -> SqlResult<i64> {
    next_sequence(engine, &resolve_sequence(engine, name)?)
}

/// `setval('name', v)`: the next value becomes `v + increment`.
pub fn setval(engine: &Engine, name: &str, v: i64) -> SqlResult<()> {
    let key = resolve_sequence(engine, name)?;
    let step = engine
        .store
        .get(SEQUENCES, &key)?
        .and_then(|d| d.fields.get("increment").and_then(Value::as_i64))
        .unwrap_or(1);
    let mut doc = Doc::new();
    doc.insert("next".into(), Value::Int(v.saturating_add(step)));
    doc.insert("increment".into(), Value::Int(step));
    let w = Write {
        collection: SEQUENCES.into(),
        key,
        op: Op::Put(doc),
        pre: Pre::None,
        tag: Tag::Internal,
    };
    engine
        .store
        .commit(std::slice::from_ref(&w))
        .map_err(|e| e.to_sql(std::slice::from_ref(&w)))?;
    Ok(())
}
