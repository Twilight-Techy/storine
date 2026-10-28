//! CREATE / DROP / ALTER TABLE, CREATE / DROP INDEX and TRUNCATE.
//!
//! DDL takes effect immediately (it is not part of the surrounding transaction, as in MySQL).
//! Schema documents are written with version preconditions, so concurrent DDL from several
//! gateways cannot lose a change.

use std::collections::HashSet;

use storine::Doc;

use crate::ast::{AlterColumn, AlterOp, ColumnDef, CreateIndex, CreateTable, DataType, Expr, TableConstraint};
use crate::catalog::{collection_name, field_name, CheckDef, Column, FkDef, IndexDef, KeyDef, Table, SEQUENCES, TABLES, UNIQUE};
use crate::datum::{cast, Datum};
use crate::error::{SqlError, SqlResult};
use crate::exec::eval::Exec;
use crate::exec::{Outcome, Session};
use crate::lexer::Dialect;
use crate::store::{CommitError, Op, Pre, Tag, Write};

/// Most single-field storage indexes a collection can have.
const MAX_STORAGE_INDEXES: usize = 4;

fn column_from_def(ex: &Exec<'_>, d: &ColumnDef) -> SqlResult<Column> {
    if let Some(def) = &d.default {
        // Validate the default now (a bad default is a DDL error, not an INSERT surprise).
        if !contains_column(def) {
            let v = ex.eval_const(def)?;
            cast(&v, &d.ty)?;
        }
    }
    Ok(Column {
        name: d.name.clone(),
        field: field_name(&d.name),
        ty: d.ty.clone(),
        not_null: d.not_null,
        default: d.default.clone(),
        auto_increment: d.auto_increment,
        missing: None,
    })
}

fn contains_column(e: &Expr) -> bool {
    let mut found = false;
    crate::exec::eval::walk(e, &mut |x| {
        if matches!(x, Expr::Column(_)) {
            found = true;
        }
    });
    found
}

fn positions(t: &Table, cols: &[String]) -> SqlResult<Vec<usize>> {
    cols.iter().map(|c| t.column(c)).collect()
}

fn add_constraint(t: &mut Table, c: &TableConstraint, s: &Session) -> SqlResult<()> {
    match c {
        TableConstraint::PrimaryKey { columns, .. } => {
            if !t.primary_key.is_empty() {
                return Err(SqlError::new("42P16", format!("multiple primary keys for table \"{}\" are not allowed", t.name)));
            }
            t.primary_key = positions(t, columns)?;
            for i in t.primary_key.clone() {
                t.columns[i].not_null = true;
            }
        }
        TableConstraint::Unique { name, columns } => {
            let cols = positions(t, columns)?;
            if cols != t.primary_key && !t.uniques.iter().any(|u| u.columns == cols) {
                let name = name.clone().unwrap_or_else(|| format!("{}_{}_key", t.name, columns.join("_")));
                t.uniques.push(KeyDef { name, columns: cols });
            }
        }
        TableConstraint::ForeignKey { name, fk } => {
            let cols = positions(t, &fk.columns)?;
            if !fk.table.eq_ignore_ascii_case(&t.name) && s.engine.find_table(&fk.table)?.is_none() {
                return Err(SqlError::undefined_table(&fk.table));
            }
            let name = name.clone().unwrap_or_else(|| format!("{}_{}_fkey", t.name, fk.columns.join("_")));
            t.foreign_keys.push(FkDef {
                name,
                columns: cols,
                ref_table: fk.table.clone(),
                ref_columns: fk.ref_columns.clone(),
                on_delete: fk.on_delete,
                on_update: fk.on_update,
            });
        }
        TableConstraint::Check { name, expr } => {
            let name = name.clone().unwrap_or_else(|| format!("{}_check{}", t.name, if t.checks.is_empty() { String::new() } else { (t.checks.len() + 1).to_string() }));
            t.checks.push(CheckDef { name, expr: expr.clone() });
        }
        TableConstraint::Index { name, columns } => {
            let cols = positions(t, columns)?;
            let name = name.clone().unwrap_or_else(|| format!("{}_{}_idx", t.name, columns.join("_")));
            t.indexes.push(IndexDef { name, columns: cols });
        }
    }
    Ok(())
}

/// Fields worth a storage index: explicit indexes, unique keys and foreign keys (first columns).
fn wanted_storage_indexes(t: &Table) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let firsts = t
        .indexes
        .iter()
        .map(|i| i.columns.first())
        .chain(t.uniques.iter().map(|u| u.columns.first()))
        .chain(t.foreign_keys.iter().map(|f| f.columns.first()));
    for c in firsts.flatten() {
        let f = t.columns[*c].field.clone();
        // A single-column primary key is the document key already.
        if t.primary_key.len() == 1 && t.primary_key[0] == *c {
            continue;
        }
        if !out.contains(&f) {
            out.push(f);
        }
    }
    out.truncate(MAX_STORAGE_INDEXES);
    out
}

fn sync_storage_indexes(s: &Session, t: &Table) -> SqlResult<()> {
    let want = wanted_storage_indexes(t);
    let have: Vec<String> = s.engine.store.indexed_fields(&t.collection).unwrap_or_default();
    for f in &have {
        if !want.contains(f) {
            let _ = s.engine.store.drop_index(&t.collection, f);
        }
    }
    for f in want {
        if !have.contains(&f) {
            // Index builds run in the background on the server; a full slot list is not fatal.
            if let Err(e) = s.engine.store.create_index(&t.collection, &f) {
                if e.code != "22023" {
                    return Err(e);
                }
            }
        }
    }
    Ok(())
}

/// Writes a schema document (`pre`: absent for a new table, its version otherwise).
fn save_schema(s: &Session, t: &Table, pre: Pre) -> SqlResult<()> {
    let w = Write { collection: TABLES.into(), key: t.name.clone(), op: Op::Put(t.to_catalog_doc()), pre, tag: Tag::Internal };
    let r = s.engine.store.commit(std::slice::from_ref(&w));
    s.engine.catalog.invalidate();
    match r {
        Ok(_) => Ok(()),
        Err(CommitError::Condition { .. }) if pre == Pre::Absent => Err(SqlError::duplicate_table(&t.name)),
        Err(CommitError::Condition { .. }) => Err(SqlError::new("40001", format!("table \"{}\" was changed concurrently; retry", t.name))),
        Err(e) => Err(e.to_sql(std::slice::from_ref(&w))),
    }
}

pub fn create_table(s: &mut Session, ct: &CreateTable, params: &[Datum]) -> SqlResult<Outcome> {
    if s.engine.find_table(&ct.name)?.is_some() {
        if ct.if_not_exists {
            return Ok(Outcome::command("CREATE TABLE"));
        }
        return Err(SqlError::duplicate_table(&ct.name));
    }
    // CREATE TABLE ... AS SELECT: columns from the query, then the rows.
    if let Some(q) = &ct.as_query {
        let rs = {
            let ex = Exec::new(s, params);
            let rel = ex.query(q, None)?;
            ex.result_set(rel)?
        };
        let defs: Vec<ColumnDef> = rs
            .columns
            .iter()
            .map(|c| ColumnDef {
                name: c.name.clone(),
                ty: c.ty.clone(),
                not_null: false,
                default: None,
                primary_key: false,
                unique: false,
                auto_increment: false,
                references: None,
                check: None,
            })
            .collect();
        let inner = CreateTable { name: ct.name.clone(), if_not_exists: false, columns: defs, constraints: Vec::new(), as_query: None };
        create_table(s, &inner, params)?;
        let n = rs.rows.len();
        let values = rs.rows.into_iter().map(|r| r.into_iter().map(|d| datum_expr(&d)).collect()).collect();
        let ins = crate::ast::Insert {
            table: ct.name.clone(),
            alias: None,
            columns: Vec::new(),
            source: crate::ast::InsertSource::Values(values),
            on_conflict: None,
            returning: Vec::new(),
            ignore: false,
            replace: false,
        };
        if n > 0 {
            crate::exec::dml::insert(s, &ins, &[])?;
        }
        return Ok(Outcome::Command { tag: format!("SELECT {n}"), affected: n as u64, last_insert_id: None });
    }
    let mut t = Table {
        name: ct.name.clone(),
        collection: collection_name(&ct.name),
        columns: Vec::new(),
        primary_key: Vec::new(),
        uniques: Vec::new(),
        indexes: Vec::new(),
        foreign_keys: Vec::new(),
        checks: Vec::new(),
        version: 0,
    };
    {
        let ex = Exec::new(s, params);
        let mut names = HashSet::new();
        for d in &ct.columns {
            if !names.insert(d.name.to_ascii_lowercase()) {
                return Err(SqlError::new("42701", format!("column \"{}\" specified more than once", d.name)));
            }
            t.columns.push(column_from_def(&ex, d)?);
        }
    }
    if t.columns.is_empty() {
        return Err(SqlError::syntax("a table needs at least one column".into()));
    }
    // Column-level constraints, then table-level ones.
    let mut constraints = Vec::new();
    for d in &ct.columns {
        if d.primary_key {
            constraints.push(TableConstraint::PrimaryKey { name: None, columns: vec![d.name.clone()] });
        }
        if d.unique {
            constraints.push(TableConstraint::Unique { name: None, columns: vec![d.name.clone()] });
        }
        if let Some(fk) = &d.references {
            constraints.push(TableConstraint::ForeignKey { name: None, fk: fk.clone() });
        }
        if let Some(c) = &d.check {
            constraints.push(TableConstraint::Check { name: Some(format!("{}_{}_check", ct.name, d.name)), expr: c.clone() });
        }
    }
    constraints.extend(ct.constraints.iter().cloned());
    for c in &constraints {
        add_constraint(&mut t, c, s)?;
    }
    // MySQL: an AUTO_INCREMENT column must be a key; make it the primary key when there is none.
    if t.primary_key.is_empty() {
        if let Some(i) = t.columns.iter().position(|c| c.auto_increment) {
            if s.dialect == Dialect::MySql || t.uniques.iter().all(|u| u.columns != vec![i]) {
                t.primary_key = vec![i];
                t.columns[i].not_null = true;
                t.uniques.retain(|u| u.columns != vec![i]);
            }
        }
    }
    save_schema(s, &t, Pre::Absent)?;
    sync_storage_indexes(s, &t)?;
    Ok(Outcome::command("CREATE TABLE"))
}

fn datum_expr(d: &Datum) -> Expr {
    match d {
        Datum::Null => Expr::Null,
        Datum::Bool(b) => Expr::Bool(*b),
        Datum::Int(i) => Expr::Number(i.to_string()),
        Datum::Numeric(x) => Expr::Number(x.to_string()),
        other => Expr::Cast { expr: Box::new(Expr::Str(other.as_text())), ty: other.natural_type() },
    }
}

/// Writes the unique markers of every existing row for a new unique key (fails on duplicates).
fn backfill_unique(s: &Session, t: &Table, uk: &KeyDef) -> SqlResult<()> {
    let mut batch = Vec::new();
    let mut seen = HashSet::new();
    for d in s.engine.store.scan(&t.collection, None)? {
        let row = t.from_doc(&d.fields);
        if let Some(m) = t.unique_marker(uk, &row)? {
            if !seen.insert(m.clone()) {
                return Err(SqlError::new("23505", format!("could not create unique index \"{}\": duplicate values exist", uk.name)));
            }
            let mut doc = Doc::new();
            doc.insert("row".into(), storine::Value::Text(d.key.clone()));
            batch.push(Write { collection: UNIQUE.into(), key: m, op: Op::Put(doc), pre: Pre::Absent, tag: Tag::Unique { table: t.name.clone(), constraint: uk.name.clone() } });
            if batch.len() >= 500 {
                s.engine.store.commit(&batch).map_err(|e| e.to_sql(&batch))?;
                batch.clear();
            }
        }
    }
    s.engine.store.commit(&batch).map_err(|e| e.to_sql(&batch))?;
    Ok(())
}

fn delete_markers(s: &Session, collection: &str, constraint: Option<&str>) -> SqlResult<()> {
    let prefix = match constraint {
        Some(c) => format!("{collection}/{c}/"),
        None => format!("{collection}/"),
    };
    let docs = s.engine.store.scan(UNIQUE, Some(storine::key_prefix(&prefix)))?;
    for chunk in docs.chunks(500) {
        let batch: Vec<Write> = chunk
            .iter()
            .map(|d| Write { collection: UNIQUE.into(), key: d.key.clone(), op: Op::Delete, pre: Pre::None, tag: Tag::Internal })
            .collect();
        s.engine.store.commit(&batch).map_err(|e| e.to_sql(&batch))?;
    }
    Ok(())
}

pub fn create_index(s: &mut Session, ci: &CreateIndex) -> SqlResult<Outcome> {
    let mut t = (*s.engine.table(&ci.table)?).clone();
    let cols = positions(&t, &ci.columns)?;
    let name = ci.name.clone().unwrap_or_else(|| format!("{}_{}_{}", t.name, ci.columns.join("_"), if ci.unique { "key" } else { "idx" }));
    let exists = t.indexes.iter().any(|i| i.name.eq_ignore_ascii_case(&name)) || t.uniques.iter().any(|u| u.name.eq_ignore_ascii_case(&name));
    if exists {
        if ci.if_not_exists {
            return Ok(Outcome::command("CREATE INDEX"));
        }
        return Err(SqlError::new("42P07", format!("relation \"{name}\" already exists")));
    }
    let version = t.version;
    if ci.unique {
        let uk = KeyDef { name, columns: cols };
        if uk.columns != t.primary_key {
            backfill_unique(s, &t, &uk)?;
            t.uniques.push(uk);
        }
    } else {
        t.indexes.push(IndexDef { name, columns: cols });
    }
    save_schema(s, &t, Pre::Version(version))?;
    sync_storage_indexes(s, &t)?;
    Ok(Outcome::command("CREATE INDEX"))
}

pub fn drop_tables(s: &mut Session, names: &[String], if_exists: bool) -> SqlResult<Outcome> {
    let mut tables = Vec::new();
    for n in names {
        match s.engine.find_table(n)? {
            Some(t) => tables.push(t),
            None if if_exists => {}
            None => return Err(SqlError::undefined_table(n)),
        }
    }
    let dropping: HashSet<String> = tables.iter().map(|t| t.name.to_ascii_lowercase()).collect();
    for t in &tables {
        for (child, fk) in s.engine.catalog.referencing(&t.name) {
            if !dropping.contains(&child.name.to_ascii_lowercase()) {
                return Err(SqlError::new(
                    "2BP01",
                    format!("cannot drop table {} because constraint {} on table {} depends on it", t.name, fk.name, child.name),
                ));
            }
        }
    }
    for t in &tables {
        let w = Write { collection: TABLES.into(), key: t.name.clone(), op: Op::Delete, pre: Pre::None, tag: Tag::Internal };
        s.engine.store.commit(std::slice::from_ref(&w)).map_err(|e| e.to_sql(std::slice::from_ref(&w)))?;
        s.engine.store.drop_collection(&t.collection)?;
        delete_markers(s, &t.collection, None)?;
        drop_sequences(s, t)?;
    }
    s.engine.catalog.invalidate();
    Ok(Outcome::command("DROP TABLE"))
}

fn drop_sequences(s: &Session, t: &Table) -> SqlResult<()> {
    let mut batch = Vec::new();
    for c in &t.columns {
        if c.auto_increment {
            batch.push(Write { collection: SEQUENCES.into(), key: format!("auto:{}.{}", t.collection, c.field), op: Op::Delete, pre: Pre::None, tag: Tag::Internal });
        }
    }
    batch.push(Write { collection: SEQUENCES.into(), key: format!("rowid:{}", t.collection), op: Op::Delete, pre: Pre::None, tag: Tag::Internal });
    s.engine.store.commit(&batch).map_err(|e| e.to_sql(&batch))?;
    Ok(())
}

pub fn drop_indexes(s: &mut Session, names: &[String], table: Option<&str>, if_exists: bool) -> SqlResult<Outcome> {
    s.engine.refresh_catalog(true)?;
    for n in names {
        let owner = match table {
            Some(t) => Some(s.engine.table(t)?),
            None => s
                .engine
                .catalog
                .all()
                .into_iter()
                .find(|t| t.indexes.iter().any(|i| i.name.eq_ignore_ascii_case(n)) || t.uniques.iter().any(|u| u.name.eq_ignore_ascii_case(n))),
        };
        let Some(owner) = owner else {
            if if_exists {
                continue;
            }
            return Err(SqlError::new("42704", format!("index \"{n}\" does not exist")));
        };
        let mut t = (*owner).clone();
        let version = t.version;
        let before = t.indexes.len() + t.uniques.len();
        t.indexes.retain(|i| !i.name.eq_ignore_ascii_case(n));
        let removed_unique: Vec<String> = t.uniques.iter().filter(|u| u.name.eq_ignore_ascii_case(n)).map(|u| u.name.clone()).collect();
        t.uniques.retain(|u| !u.name.eq_ignore_ascii_case(n));
        if t.indexes.len() + t.uniques.len() == before {
            if if_exists {
                continue;
            }
            return Err(SqlError::new("42704", format!("index \"{n}\" does not exist")));
        }
        save_schema(s, &t, Pre::Version(version))?;
        for u in removed_unique {
            delete_markers(s, &t.collection, Some(&u))?;
        }
        sync_storage_indexes(s, &t)?;
    }
    Ok(Outcome::command("DROP INDEX"))
}

pub fn alter_table(s: &mut Session, table: &str, ops: &[AlterOp]) -> SqlResult<Outcome> {
    for op in ops {
        // Each operation works on the latest schema.
        s.engine.refresh_catalog(true)?;
        let current = s.engine.table(table)?;
        let mut t = (*current).clone();
        let version = t.version;
        match op {
            AlterOp::AddColumn { column, if_not_exists } => {
                if t.column_index(&column.name).is_some_and(|i| t.columns[i].name.eq_ignore_ascii_case(&column.name)) {
                    if *if_not_exists {
                        continue;
                    }
                    return Err(SqlError::new("42701", format!("column \"{}\" of relation \"{}\" already exists", column.name, t.name)));
                }
                let mut c = {
                    let ex = Exec::new(s, &[]);
                    column_from_def(&ex, column)?
                };
                let rows = s.engine.store.count(&t.collection, None)?;
                if let Some(d) = &c.default {
                    // Existing rows read the default, like PostgreSQL's "missing value".
                    let ex = Exec::new(s, &[]);
                    c.missing = Some(cast(&ex.eval_const(d)?, &c.ty)?);
                } else if c.auto_increment && rows > 0 {
                    return Err(SqlError::unsupported("adding an auto-increment column to a non-empty table"));
                }
                if c.not_null && c.missing.is_none() && rows > 0 {
                    return Err(SqlError::not_null(&t.name, &c.name));
                }
                if column.primary_key {
                    return Err(SqlError::unsupported("adding a primary key column to an existing table"));
                }
                t.columns.push(c);
                let name = column.name.clone();
                if column.unique {
                    add_constraint(&mut t, &TableConstraint::Unique { name: None, columns: vec![name.clone()] }, s)?;
                }
                if let Some(fk) = &column.references {
                    let mut fk = fk.clone();
                    fk.columns = vec![name.clone()];
                    add_constraint(&mut t, &TableConstraint::ForeignKey { name: None, fk }, s)?;
                }
                if let Some(ck) = &column.check {
                    let cname = format!("{}_{}_check", t.name, name);
                    add_constraint(&mut t, &TableConstraint::Check { name: Some(cname), expr: ck.clone() }, s)?;
                }
            }
            AlterOp::DropColumn { name, if_exists } => {
                let Some(i) = t.column_index(name) else {
                    if *if_exists {
                        continue;
                    }
                    return Err(SqlError::undefined_column(name));
                };
                if t.primary_key.contains(&i) {
                    return Err(SqlError::unsupported("dropping a primary key column"));
                }
                if t.columns.len() == 1 {
                    return Err(SqlError::syntax("a table needs at least one column".into()));
                }
                let dropped_uniques: Vec<String> = t.uniques.iter().filter(|u| u.columns.contains(&i)).map(|u| u.name.clone()).collect();
                t.uniques.retain(|u| !u.columns.contains(&i));
                t.indexes.retain(|x| !x.columns.contains(&i));
                t.foreign_keys.retain(|f| !f.columns.contains(&i));
                let col_name = t.columns[i].name.clone();
                t.checks.retain(|c| {
                    let mut uses = false;
                    crate::exec::eval::walk(&c.expr, &mut |x| {
                        if let Expr::Column(p) = x {
                            if p.last().is_some_and(|n| n.eq_ignore_ascii_case(&col_name)) {
                                uses = true;
                            }
                        }
                    });
                    !uses
                });
                t.columns.remove(i);
                let shift = |v: &mut Vec<usize>| {
                    for x in v.iter_mut() {
                        if *x > i {
                            *x -= 1;
                        }
                    }
                };
                shift(&mut t.primary_key);
                for u in &mut t.uniques {
                    shift(&mut u.columns);
                }
                for x in &mut t.indexes {
                    shift(&mut x.columns);
                }
                for f in &mut t.foreign_keys {
                    shift(&mut f.columns);
                }
                save_schema(s, &t, Pre::Version(version))?;
                for u in dropped_uniques {
                    delete_markers(s, &t.collection, Some(&u))?;
                }
                sync_storage_indexes(s, &t)?;
                continue;
            }
            AlterOp::RenameColumn { from, to } => {
                let i = t.column(from)?;
                if t.column_index(to).is_some_and(|j| j != i) {
                    return Err(SqlError::new("42701", format!("column \"{to}\" of relation \"{}\" already exists", t.name)));
                }
                // The stored field keeps its name: no data is rewritten.
                t.columns[i].name = to.clone();
            }
            AlterOp::RenameTable(new) => {
                if s.engine.find_table(new)?.is_some() {
                    return Err(SqlError::duplicate_table(new));
                }
                let old = t.name.clone();
                t.name = new.clone();
                save_schema(s, &t, Pre::Absent)?;
                let w = Write { collection: TABLES.into(), key: old.clone(), op: Op::Delete, pre: Pre::Version(version), tag: Tag::Internal };
                s.engine.store.commit(std::slice::from_ref(&w)).map_err(|e| e.to_sql(std::slice::from_ref(&w)))?;
                // Foreign keys of other tables follow the rename.
                for (child, _) in s.engine.catalog.referencing(&old) {
                    let mut c = (*child).clone();
                    let cv = c.version;
                    for f in &mut c.foreign_keys {
                        if f.ref_table.eq_ignore_ascii_case(&old) {
                            f.ref_table = new.clone();
                        }
                    }
                    save_schema(s, &c, Pre::Version(cv))?;
                }
                s.engine.catalog.invalidate();
                continue;
            }
            AlterOp::AlterColumn { name, change } => {
                let i = t.column(name)?;
                match change {
                    AlterColumn::SetDefault(e) => t.columns[i].default = Some(e.clone()),
                    AlterColumn::DropDefault => t.columns[i].default = None,
                    AlterColumn::DropNotNull => {
                        if t.primary_key.contains(&i) {
                            return Err(SqlError::new("42P16", format!("column \"{name}\" is in a primary key")));
                        }
                        t.columns[i].not_null = false;
                    }
                    AlterColumn::SetNotNull => {
                        let field = t.columns[i].field.clone();
                        let has_null = t.columns[i].missing.is_none() && s.engine.store.count(&t.collection, Some(storine::missing(&field)))? > 0;
                        if has_null {
                            return Err(SqlError::new("23502", format!("column \"{name}\" of relation \"{}\" contains null values", t.name)));
                        }
                        t.columns[i].not_null = true;
                    }
                    AlterColumn::Type(ty) => change_type(s, &t, i, ty)?.into_iter().for_each(|c| t.columns[i] = c),
                }
            }
            AlterOp::AddConstraint(c) => {
                match c {
                    TableConstraint::PrimaryKey { .. } if !t.primary_key.is_empty() => {
                        return Err(SqlError::new("42P16", format!("multiple primary keys for table \"{}\" are not allowed", t.name)))
                    }
                    TableConstraint::PrimaryKey { .. } if s.engine.store.count(&t.collection, None)? > 0 => {
                        return Err(SqlError::unsupported("adding a primary key to a table that already has rows"))
                    }
                    _ => {}
                }
                let before_u = t.uniques.len();
                let before_c = t.checks.len();
                let before_f = t.foreign_keys.len();
                add_constraint(&mut t, c, s)?;
                if t.uniques.len() > before_u {
                    let uk = t.uniques[t.uniques.len() - 1].clone();
                    backfill_unique(s, &t, &uk)?;
                }
                if t.checks.len() > before_c || t.foreign_keys.len() > before_f {
                    validate_existing(s, &t, before_c, before_f)?;
                }
            }
            AlterOp::DropConstraint { name, if_exists } => {
                let n = name.as_str();
                let before = t.uniques.len() + t.foreign_keys.len() + t.checks.len() + t.indexes.len();
                let removed_unique: Vec<String> = t.uniques.iter().filter(|u| u.name.eq_ignore_ascii_case(n)).map(|u| u.name.clone()).collect();
                t.uniques.retain(|u| !u.name.eq_ignore_ascii_case(n));
                t.foreign_keys.retain(|f| !f.name.eq_ignore_ascii_case(n));
                t.checks.retain(|c| !c.name.eq_ignore_ascii_case(n));
                t.indexes.retain(|i| !i.name.eq_ignore_ascii_case(n));
                if t.uniques.len() + t.foreign_keys.len() + t.checks.len() + t.indexes.len() == before {
                    if *if_exists {
                        continue;
                    }
                    if n == "PRIMARY" || n.ends_with("_pkey") {
                        return Err(SqlError::unsupported("dropping a primary key"));
                    }
                    return Err(SqlError::new("42704", format!("constraint \"{n}\" of relation \"{}\" does not exist", t.name)));
                }
                save_schema(s, &t, Pre::Version(version))?;
                for u in removed_unique {
                    delete_markers(s, &t.collection, Some(&u))?;
                }
                sync_storage_indexes(s, &t)?;
                continue;
            }
        }
        save_schema(s, &t, Pre::Version(version))?;
        sync_storage_indexes(s, &t)?;
    }
    Ok(Outcome::command("ALTER TABLE"))
}

/// Checks existing rows against newly added CHECK / FOREIGN KEY constraints.
fn validate_existing(s: &Session, t: &Table, checks_from: usize, fks_from: usize) -> SqlResult<()> {
    let ex = Exec::new(s, &[]);
    let cols = crate::exec::select::table_cols(t);
    for d in s.engine.store.scan(&t.collection, None)? {
        let row = t.from_doc(&d.fields);
        let env = crate::exec::eval::Env::new(&cols, &row);
        for c in &t.checks[checks_from..] {
            if ex.eval(&c.expr, &env)?.truthy()? == Some(false) {
                return Err(SqlError::new("23514", format!("check constraint \"{}\" of relation \"{}\" is violated by some row", c.name, t.name)));
            }
        }
        for fk in &t.foreign_keys[fks_from..] {
            if fk.columns.iter().any(|i| row[*i].is_null()) {
                continue;
            }
            let parent = s.engine.table(&fk.ref_table)?;
            let ref_cols: Vec<usize> =
                if fk.ref_columns.is_empty() { parent.primary_key.clone() } else { fk.ref_columns.iter().map(|c| parent.column(c)).collect::<SqlResult<_>>()? };
            if ref_cols == parent.primary_key {
                let vals: Vec<Datum> = fk.columns.iter().zip(&ref_cols).map(|(c, p)| cast(&row[*c], &parent.columns[*p].ty)).collect::<SqlResult<_>>()?;
                let parts: Vec<&Datum> = vals.iter().collect();
                let key = crate::catalog::encode_key(&parts)?;
                if s.engine.store.get(&parent.collection, &key)?.is_none() {
                    return Err(SqlError::foreign_key(format!("insert or update on table \"{}\" violates foreign key constraint \"{}\"", t.name, fk.name)));
                }
            }
        }
    }
    Ok(())
}

/// `ALTER COLUMN ... TYPE`: verifies every stored value converts, then switches the type.
fn change_type(s: &Session, t: &Table, i: usize, ty: &DataType) -> SqlResult<Option<Column>> {
    let field = &t.columns[i].field;
    for d in s.engine.store.scan(&t.collection, Some(storine::exists(field)))? {
        let row = t.from_doc(&d.fields);
        cast(&row[i], ty).map_err(|e| SqlError::new(e.code, format!("column \"{}\" cannot be cast to {}: {}", t.columns[i].name, crate::datum::type_sql(ty), e.message)))?;
    }
    let mut c = t.columns[i].clone();
    c.ty = ty.clone();
    if let Some(m) = &c.missing {
        c.missing = Some(cast(m, ty)?);
    }
    Ok(Some(c))
}

pub fn truncate(s: &mut Session, tables: &[String]) -> SqlResult<Outcome> {
    for n in tables {
        let t = s.engine.table(n)?;
        s.engine.store.drop_collection(&t.collection)?;
        delete_markers(s, &t.collection, None)?;
        if s.dialect == Dialect::MySql {
            drop_sequences(s, &t)?;
        }
        sync_storage_indexes(s, &t)?;
    }
    Ok(Outcome::command("TRUNCATE TABLE"))
}
