//! Table schemas: how SQL tables map onto document collections.
//!
//! - A table is a collection; a row is a document whose fields are the columns (so the SDKs see
//!   the same data). The document key encodes the primary key (`5`, `alice`, `7|2024`), or a
//!   generated row id for tables without one.
//! - Schemas are documents in the `_sql_tables` collection (cached, refreshed every few seconds
//!   and after every DDL). Defaults and checks are stored as SQL text.
//! - Unique constraints are enforced with marker documents in `_sql_unique`
//!   (`<collection>/<constraint>/<values>` → the row key), written atomically with the row with an
//!   "absent" precondition, so concurrent inserts of the same value cannot both commit.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use storine::{Doc, Value};

use crate::ast::{DataType, Expr, RefAction};
use crate::datum::{self, Datum};
use crate::error::{SqlError, SqlResult};
use crate::hash;
use crate::lexer::Dialect;
use crate::parser::{parse_data_type, parse_expr};

pub const TABLES: &str = "_sql_tables";
pub const UNIQUE: &str = "_sql_unique";
pub const SEQUENCES: &str = "_sql_seq";
pub const LOCKS: &str = "_sql_locks";
/// Reference markers: touched by every write that makes a row point at a parent row, so a
/// concurrent parent delete that scanned before the child committed fails instead of
/// leaving an orphan.
pub const REFS: &str = "_sql_refs";
/// View definitions: `{name, sql, dialect, columns}` keyed by the lower-case name.
pub const VIEWS: &str = "_sql_views";

/// Collections the gateway keeps for itself (hidden from table listings).
pub fn is_internal(collection: &str) -> bool {
    collection.starts_with("_sql_") || collection.starts_with('$')
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
    /// Document field holding the column.
    pub field: String,
    pub ty: DataType,
    /// Fractional-second digits of a temporal column (`None`: the type's default).
    pub precision: Option<u8>,
    pub not_null: bool,
    pub default: Option<Expr>,
    pub auto_increment: bool,
    /// Value of rows written before the column was added (`ALTER TABLE ... ADD COLUMN ... DEFAULT`).
    pub missing: Option<Datum>,
}

impl Column {
    /// `atttypmod`: the declared precision of a temporal column, else the type's modifier.
    pub fn type_mod(&self) -> i32 {
        match self.precision {
            Some(p) => i32::from(p),
            None => crate::exec::system::type_mod(&self.ty),
        }
    }

    /// Fractional-second digits reported for a temporal column (PostgreSQL's default is 6).
    pub fn datetime_precision(&self) -> Option<i64> {
        matches!(
            self.ty,
            DataType::Timestamp | DataType::TimestampTz | DataType::Time
        )
        .then(|| i64::from(self.precision.unwrap_or(6)))
    }

    /// Rounds a stored temporal value to the column's precision, as PostgreSQL and MySQL do.
    pub fn fit(&self, d: Datum) -> Datum {
        let Some(p) = self.precision.filter(|p| *p < 9) else {
            return d;
        };
        let unit = 10i64.pow(9 - u32::from(p));
        let round = |ns: i64| (ns + unit.div_euclid(2)).div_euclid(unit) * unit;
        match d {
            Datum::Timestamp(ns) => Datum::Timestamp(round(ns)),
            Datum::Time(ns) => Datum::Time(round(ns)),
            other => other,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeyDef {
    pub name: String,
    pub columns: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexDef {
    pub name: String,
    pub columns: Vec<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FkDef {
    pub name: String,
    pub columns: Vec<usize>,
    pub ref_table: String,
    /// Referenced columns (empty: the referenced table's primary key).
    pub ref_columns: Vec<String>,
    pub on_delete: RefAction,
    pub on_update: RefAction,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CheckDef {
    pub name: String,
    pub expr: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    pub name: String,
    pub collection: String,
    pub columns: Vec<Column>,
    /// Primary key columns; empty: rows get a generated id.
    pub primary_key: Vec<usize>,
    pub uniques: Vec<KeyDef>,
    pub indexes: Vec<IndexDef>,
    pub foreign_keys: Vec<FkDef>,
    pub checks: Vec<CheckDef>,
    /// Version of the schema document.
    pub version: u64,
}

pub type Row = Vec<Datum>;

impl Table {
    /// Column position by name (exact, then case-insensitive).
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.name == name)
            .or_else(|| {
                let mut it = self
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.name.eq_ignore_ascii_case(name));
                match (it.next(), it.next()) {
                    (Some((i, _)), None) => Some(i),
                    _ => None,
                }
            })
    }

    pub fn column(&self, name: &str) -> SqlResult<usize> {
        self.column_index(name)
            .ok_or_else(|| SqlError::undefined_column(name))
    }

    /// The document key of `row`, or `None` when the table has no primary key.
    pub fn row_key(&self, row: &Row) -> SqlResult<Option<String>> {
        if self.primary_key.is_empty() {
            return Ok(None);
        }
        let parts: Vec<&Datum> = self.primary_key.iter().map(|i| &row[*i]).collect();
        if let Some(i) = self.primary_key.iter().find(|i| row[**i].is_null()) {
            return Err(SqlError::not_null(&self.name, &self.columns[*i].name));
        }
        Ok(Some(encode_key(&parts)?))
    }

    /// The stored document of `row` (NULL columns are omitted).
    pub fn to_doc(&self, row: &Row) -> Doc {
        let mut d = Doc::new();
        for (c, v) in self.columns.iter().zip(row) {
            if !v.is_null() {
                d.insert(c.field.clone(), datum::to_value(v, &c.ty));
            }
        }
        d
    }

    /// The row of a stored document (missing fields are NULL).
    pub fn from_doc(&self, doc: &Doc) -> Row {
        self.columns
            .iter()
            .map(|c| match doc.get(&c.field) {
                Some(v) => datum::from_value(v, Some(&c.ty)),
                None => c.missing.clone().unwrap_or(Datum::Null),
            })
            .collect()
    }

    /// The marker key of unique constraint `k` for `row` (`None` when a column is NULL).
    pub fn unique_marker(&self, k: &KeyDef, row: &Row) -> SqlResult<Option<String>> {
        if k.columns.iter().any(|i| row[*i].is_null()) {
            return Ok(None);
        }
        let parts: Vec<&Datum> = k.columns.iter().map(|i| &row[*i]).collect();
        let raw = encode_key_unbounded(&parts);
        let prefix = format!("{}/{}/", self.collection, k.name);
        let key = if prefix.len() + raw.len() > 1000 {
            format!("{prefix}#{}", hash::hex(&hash::sha256(raw.as_bytes())))
        } else {
            prefix + &raw
        };
        Ok(Some(key))
    }

    /// Every unique key: the primary key first (as a constraint named `<table>_pkey`).
    pub fn all_unique_keys(&self) -> Vec<KeyDef> {
        let mut out = Vec::new();
        if !self.primary_key.is_empty() {
            out.push(KeyDef {
                name: format!("{}_pkey", self.name),
                columns: self.primary_key.clone(),
            });
        }
        out.extend(self.uniques.iter().cloned());
        out
    }

    // ── persistence ──

    pub fn to_catalog_doc(&self) -> Doc {
        let names = |cols: &[usize]| {
            Value::List(
                cols.iter()
                    .map(|i| Value::Text(self.columns[*i].name.clone()))
                    .collect(),
            )
        };
        let mut d = Doc::new();
        d.insert("name".into(), Value::Text(self.name.clone()));
        d.insert("collection".into(), Value::Text(self.collection.clone()));
        d.insert(
            "columns".into(),
            Value::List(
                self.columns
                    .iter()
                    .map(|c| {
                        let mut m = Doc::new();
                        m.insert("name".into(), Value::Text(c.name.clone()));
                        m.insert("field".into(), Value::Text(c.field.clone()));
                        m.insert("type".into(), Value::Text(datum::type_sql(&c.ty)));
                        if let Some(p) = c.precision {
                            m.insert("precision".into(), Value::Int(i64::from(p)));
                        }
                        m.insert("not_null".into(), Value::Bool(c.not_null));
                        m.insert("auto_increment".into(), Value::Bool(c.auto_increment));
                        m.insert(
                            "default".into(),
                            c.default
                                .as_ref()
                                .map_or(Value::Null, |e| Value::Text(e.to_string())),
                        );
                        m.insert(
                            "missing".into(),
                            c.missing
                                .as_ref()
                                .map_or(Value::Null, |d| datum::to_value(d, &c.ty)),
                        );
                        Value::Map(m)
                    })
                    .collect(),
            ),
        );
        d.insert("primary_key".into(), names(&self.primary_key));
        let keys = |ks: Vec<(&String, &Vec<usize>)>| {
            Value::List(
                ks.into_iter()
                    .map(|(n, cols)| {
                        let mut m = Doc::new();
                        m.insert("name".into(), Value::Text(n.clone()));
                        m.insert("columns".into(), names(cols));
                        Value::Map(m)
                    })
                    .collect(),
            )
        };
        d.insert(
            "uniques".into(),
            keys(self.uniques.iter().map(|k| (&k.name, &k.columns)).collect()),
        );
        d.insert(
            "indexes".into(),
            keys(self.indexes.iter().map(|k| (&k.name, &k.columns)).collect()),
        );
        d.insert(
            "foreign_keys".into(),
            Value::List(
                self.foreign_keys
                    .iter()
                    .map(|f| {
                        let mut m = Doc::new();
                        m.insert("name".into(), Value::Text(f.name.clone()));
                        m.insert("columns".into(), names(&f.columns));
                        m.insert("ref_table".into(), Value::Text(f.ref_table.clone()));
                        m.insert(
                            "ref_columns".into(),
                            Value::List(
                                f.ref_columns
                                    .iter()
                                    .map(|c| Value::Text(c.clone()))
                                    .collect(),
                            ),
                        );
                        m.insert(
                            "on_delete".into(),
                            Value::Text(action_name(f.on_delete).into()),
                        );
                        m.insert(
                            "on_update".into(),
                            Value::Text(action_name(f.on_update).into()),
                        );
                        Value::Map(m)
                    })
                    .collect(),
            ),
        );
        d.insert(
            "checks".into(),
            Value::List(
                self.checks
                    .iter()
                    .map(|c| {
                        let mut m = Doc::new();
                        m.insert("name".into(), Value::Text(c.name.clone()));
                        m.insert("sql".into(), Value::Text(c.expr.to_string()));
                        Value::Map(m)
                    })
                    .collect(),
            ),
        );
        d
    }

    pub fn from_catalog_doc(doc: &Doc, version: u64) -> SqlResult<Table> {
        let bad = || SqlError::internal("corrupt table schema document");
        let text = |m: &Doc, k: &str| {
            m.get(k)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(bad)
        };
        let list = |m: &Doc, k: &str| -> Vec<Value> {
            m.get(k)
                .and_then(Value::as_list)
                .map(<[Value]>::to_vec)
                .unwrap_or_default()
        };
        let mut columns = Vec::new();
        for c in list(doc, "columns") {
            let m = c.as_map().ok_or_else(bad)?;
            let ty = parse_data_type(&text(m, "type")?)?;
            let missing = m
                .get("missing")
                .filter(|v| !v.is_null())
                .map(|v| datum::from_value(v, Some(&ty)));
            columns.push(Column {
                missing,
                name: text(m, "name")?,
                field: text(m, "field")?,
                ty,
                precision: m
                    .get("precision")
                    .and_then(Value::as_i64)
                    .and_then(|p| u8::try_from(p).ok()),
                not_null: m.get("not_null").and_then(Value::as_bool).unwrap_or(false),
                auto_increment: m
                    .get("auto_increment")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                default: match m.get("default").and_then(Value::as_str) {
                    Some(sql) => Some(parse_expr(sql, Dialect::Postgres)?),
                    None => None,
                },
            });
        }
        let mut t = Table {
            name: text(doc, "name")?,
            collection: text(doc, "collection")?,
            columns,
            primary_key: Vec::new(),
            uniques: Vec::new(),
            indexes: Vec::new(),
            foreign_keys: Vec::new(),
            checks: Vec::new(),
            version,
        };
        let positions = |t: &Table, v: &Value| -> SqlResult<Vec<usize>> {
            v.as_list()
                .unwrap_or(&[])
                .iter()
                .map(|n| t.column(n.as_str().unwrap_or_default()))
                .collect()
        };
        t.primary_key = positions(&t, doc.get("primary_key").unwrap_or(&Value::Null))?;
        for k in list(doc, "uniques") {
            let m = k.as_map().ok_or_else(bad)?;
            let columns = positions(&t, m.get("columns").unwrap_or(&Value::Null))?;
            t.uniques.push(KeyDef {
                name: text(m, "name")?,
                columns,
            });
        }
        for k in list(doc, "indexes") {
            let m = k.as_map().ok_or_else(bad)?;
            let columns = positions(&t, m.get("columns").unwrap_or(&Value::Null))?;
            t.indexes.push(IndexDef {
                name: text(m, "name")?,
                columns,
            });
        }
        for f in list(doc, "foreign_keys") {
            let m = f.as_map().ok_or_else(bad)?;
            let columns = positions(&t, m.get("columns").unwrap_or(&Value::Null))?;
            t.foreign_keys.push(FkDef {
                name: text(m, "name")?,
                columns,
                ref_table: text(m, "ref_table")?,
                ref_columns: list(m, "ref_columns")
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .collect(),
                on_delete: parse_action(&text(m, "on_delete")?),
                on_update: parse_action(&text(m, "on_update")?),
            });
        }
        for c in list(doc, "checks") {
            let m = c.as_map().ok_or_else(bad)?;
            t.checks.push(CheckDef {
                name: text(m, "name")?,
                expr: parse_expr(&text(m, "sql")?, Dialect::Postgres)?,
            });
        }
        Ok(t)
    }
}

pub fn action_name(a: RefAction) -> &'static str {
    match a {
        RefAction::NoAction => "NO ACTION",
        RefAction::Restrict => "RESTRICT",
        RefAction::Cascade => "CASCADE",
        RefAction::SetNull => "SET NULL",
        RefAction::SetDefault => "SET DEFAULT",
    }
}

fn parse_action(s: &str) -> RefAction {
    match s {
        "RESTRICT" => RefAction::Restrict,
        "CASCADE" => RefAction::Cascade,
        "SET NULL" => RefAction::SetNull,
        "SET DEFAULT" => RefAction::SetDefault,
        _ => RefAction::NoAction,
    }
}

/// The canonical text of a key part: equal values (5, 5.0, '5' cast to int) give equal text.
fn key_text(d: &Datum) -> String {
    match d {
        Datum::Numeric(x) => x.normalize().to_string(),
        Datum::Float(f) if f.fract() == 0.0 && f.abs() < 9e15 => (*f as i64).to_string(),
        Datum::Bool(b) => if *b { "t" } else { "f" }.into(),
        Datum::Bytes(b) => hash::hex(b),
        Datum::Timestamp(ns) => ns.to_string(),
        Datum::Date(d) => d.to_string(),
        other => other.as_text(),
    }
}

fn encode_key_unbounded(parts: &[&Datum]) -> String {
    let mut out = String::new();
    for (i, d) in parts.iter().enumerate() {
        if i > 0 {
            out.push('|');
        }
        let t = key_text(d);
        if t.is_empty() {
            out.push_str("\\e");
        }
        for c in t.chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '|' => out.push_str("\\|"),
                c if c.is_control() => out.push_str(&format!("\\x{:02x}", c as u32)),
                c => out.push(c),
            }
        }
    }
    out
}

/// The document key of a primary key value.
pub fn encode_key(parts: &[&Datum]) -> SqlResult<String> {
    let k = encode_key_unbounded(parts);
    if k.len() > 1000 {
        return Err(SqlError::new(
            "54000",
            "primary key value is too long (at most 1000 bytes)",
        ));
    }
    Ok(k)
}

/// A storage name for a table: the table name when it is a valid collection name.
pub fn collection_name(table: &str) -> String {
    let ok = !table.is_empty()
        && table.len() <= 200
        && table
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        && !table.starts_with("_sql_");
    if ok {
        return table.to_owned();
    }
    let clean: String = table
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .take(180)
        .collect();
    format!(
        "{}_{}",
        clean.trim_start_matches("_sql"),
        &hash::hex(&hash::sha256(table.as_bytes()))[..12]
    )
}

/// A document field name for a column (fields cannot contain `.`, which addresses nested maps).
pub fn field_name(column: &str) -> String {
    if !column.is_empty() && !column.contains('.') && !column.starts_with('$') {
        column.to_owned()
    } else {
        column.replace(['.', '$'], "_")
    }
}

/// Cached view of every table schema.
#[derive(Debug, Default)]
pub struct Catalog {
    inner: RwLock<CatalogState>,
}

#[derive(Debug, Default)]
struct CatalogState {
    tables: HashMap<String, Arc<Table>>,
    loaded: Option<Instant>,
}

/// How long a cached schema is trusted before it is re-read (other gateways may change it).
const CATALOG_TTL: Duration = Duration::from_secs(2);

impl Catalog {
    pub fn is_fresh(&self) -> bool {
        self.inner
            .read()
            .map(|s| s.loaded.is_some_and(|t| t.elapsed() < CATALOG_TTL))
            .unwrap_or(false)
    }

    pub fn replace_all(&self, tables: Vec<Table>) {
        if let Ok(mut s) = self.inner.write() {
            s.tables = tables
                .into_iter()
                .map(|t| (t.name.clone(), Arc::new(t)))
                .collect();
            s.loaded = Some(Instant::now());
        }
    }

    pub fn invalidate(&self) {
        if let Ok(mut s) = self.inner.write() {
            s.loaded = None;
        }
    }

    /// The table `name` (exact, then case-insensitive when unambiguous).
    pub fn get(&self, name: &str) -> Option<Arc<Table>> {
        let s = self.inner.read().ok()?;
        if let Some(t) = s.tables.get(name) {
            return Some(Arc::clone(t));
        }
        let mut it = s
            .tables
            .values()
            .filter(|t| t.name.eq_ignore_ascii_case(name));
        match (it.next(), it.next()) {
            (Some(t), None) => Some(Arc::clone(t)),
            _ => None,
        }
    }

    pub fn all(&self) -> Vec<Arc<Table>> {
        let mut v: Vec<Arc<Table>> = self
            .inner
            .read()
            .map(|s| s.tables.values().cloned().collect())
            .unwrap_or_default();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    /// Tables whose foreign keys reference `table`.
    pub fn referencing(&self, table: &str) -> Vec<(Arc<Table>, FkDef)> {
        let mut out = Vec::new();
        for t in self.all() {
            for fk in &t.foreign_keys {
                if fk.ref_table.eq_ignore_ascii_case(table) {
                    out.push((Arc::clone(&t), fk.clone()));
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage_types::Decimal;

    fn table() -> Table {
        let col = |name: &str, ty: DataType| Column {
            name: name.into(),
            field: field_name(name),
            ty,
            precision: None,
            not_null: false,
            default: None,
            auto_increment: false,
            missing: None,
        };
        Table {
            name: "Accounts".into(),
            collection: collection_name("Accounts"),
            columns: vec![
                Column {
                    auto_increment: true,
                    not_null: true,
                    ..col("id", DataType::BigInt)
                },
                Column {
                    default: Some(Expr::Number("0".into())),
                    missing: Some(Datum::Numeric("0.00".parse::<Decimal>().unwrap())),
                    ..col("balance", DataType::Numeric(Some((12, 2))))
                },
                col("email", DataType::Varchar(Some(255))),
                col("tags", DataType::Array(Box::new(DataType::Text))),
            ],
            primary_key: vec![0],
            uniques: vec![KeyDef {
                name: "accounts_email_key".into(),
                columns: vec![2],
            }],
            indexes: vec![IndexDef {
                name: "ix_balance".into(),
                columns: vec![1],
            }],
            foreign_keys: vec![FkDef {
                name: "fk".into(),
                columns: vec![0],
                ref_table: "users".into(),
                ref_columns: vec![],
                on_delete: RefAction::Cascade,
                on_update: RefAction::NoAction,
            }],
            checks: vec![CheckDef {
                name: "positive".into(),
                expr: parse_expr("balance >= 0", Dialect::Postgres).unwrap(),
            }],
            version: 3,
        }
    }

    #[test]
    fn schema_documents_round_trip() {
        let t = table();
        let back = Table::from_catalog_doc(&t.to_catalog_doc(), 3).unwrap();
        assert_eq!(back, t);
    }

    #[test]
    fn keys_rows_and_markers() {
        let t = table();
        let row = vec![
            Datum::Int(5),
            Datum::Numeric("10.50".parse::<Decimal>().unwrap()),
            Datum::Text("a|b".into()),
            Datum::Array(vec![Datum::Text("x".into())]),
        ];
        assert_eq!(t.row_key(&row).unwrap().as_deref(), Some("5"));
        assert_eq!(t.from_doc(&t.to_doc(&row)), row);
        assert_eq!(
            t.unique_marker(&t.uniques[0], &row).unwrap().as_deref(),
            Some("Accounts/accounts_email_key/a\\|b")
        );
        let mut nulls = row.clone();
        nulls[2] = Datum::Null;
        assert_eq!(t.unique_marker(&t.uniques[0], &nulls).unwrap(), None);
        nulls[0] = Datum::Null;
        assert!(t.row_key(&nulls).is_err());
        assert_eq!(
            encode_key(&[&Datum::Int(7), &Datum::Text("".into())]).unwrap(),
            "7|\\e"
        );
        assert_eq!(
            encode_key(&[&Datum::Numeric("5.00".parse::<Decimal>().unwrap())]).unwrap(),
            "5"
        );
        assert_eq!(collection_name("orders"), "orders");
        assert!(collection_name("my table").starts_with("my_table_"));
        assert_eq!(field_name("a.b"), "a_b");
    }
}
