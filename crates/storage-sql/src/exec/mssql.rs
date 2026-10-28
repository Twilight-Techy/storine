//! SQL Server flavour: the `sys.*` catalog views and the T-SQL built-in functions that
//! drivers and ORMs (SQLAlchemy, EF Core, Hibernate, Sequelize, GORM, Prisma) call.

use std::sync::Arc;

use crate::ast::{DataType, RefAction};
use crate::catalog::{Column, Table};
use crate::datum::{self, Datum};
use crate::error::{SqlError, SqlResult};
use crate::exec::eval::Exec;
use crate::exec::select::{ColMeta, Rel};

pub const PRODUCT_VERSION: &str = "16.0.1000.6";
pub const COLLATION: &str = "SQL_Latin1_General_CP1_CI_AS";
/// `schema_id` of `dbo`, the one schema every table lives in.
pub const DBO: i64 = 1;
const SYS_SCHEMA: i64 = 4;
const INFORMATION_SCHEMA: i64 = 3;

/// A stable `object_id` (positive `int`) for a table, view or constraint name.
pub fn object_id(kind: &str, name: &str) -> i64 {
    let mut h: u32 = 0x811c_9dc5;
    for b in kind
        .bytes()
        .chain([0])
        .chain(name.to_ascii_lowercase().bytes())
    {
        h ^= u32::from(b);
        h = h.wrapping_mul(0x0100_0193);
    }
    i64::from(h & 0x3fff_ffff) + 1_000
}

fn table_object(tb: &Table) -> i64 {
    object_id("U", &tb.name)
}

pub fn pk_name(tb: &Table) -> String {
    tb.pk_name
        .clone()
        .unwrap_or_else(|| format!("PK_{}", tb.name))
}

/// The name of a column's default constraint (given, or generated like SQL Server does).
pub fn default_name(table: &str, c: &Column) -> String {
    c.default_name
        .clone()
        .unwrap_or_else(|| format!("DF_{table}_{}", c.name))
}

/// SQL Server's name and `system_type_id` for a column type, with `max_length` (bytes, -1 for
/// MAX), precision and scale.
pub fn type_info(ty: &DataType, precision: Option<u8>) -> (&'static str, i64, i64, i64, i64) {
    match ty {
        DataType::Bool => ("bit", 104, 1, 1, 0),
        DataType::SmallInt => ("smallint", 52, 2, 5, 0),
        DataType::Int => ("int", 56, 4, 10, 0),
        DataType::BigInt => ("bigint", 127, 8, 19, 0),
        DataType::Numeric(p) => {
            let (p, s) = p.unwrap_or((38, 10));
            let len = match p {
                0..=9 => 5,
                10..=19 => 9,
                20..=28 => 13,
                _ => 17,
            };
            ("decimal", 106, len, i64::from(p), i64::from(s))
        }
        DataType::Real => ("real", 59, 4, 24, 0),
        DataType::Double => ("float", 62, 8, 53, 0),
        DataType::Varchar(Some(n)) if *n <= 4000 => ("nvarchar", 231, i64::from(*n) * 2, 0, 0),
        DataType::Char(Some(n)) if *n <= 4000 => ("nchar", 239, i64::from(*n) * 2, 0, 0),
        DataType::Bytes => ("varbinary", 165, -1, 0, 0),
        DataType::Timestamp => {
            let p = i64::from(precision.unwrap_or(7).min(7));
            (
                "datetime2",
                42,
                if p < 3 {
                    6
                } else if p < 5 {
                    7
                } else {
                    8
                },
                27,
                p,
            )
        }
        DataType::TimestampTz => {
            let p = i64::from(precision.unwrap_or(7).min(7));
            ("datetimeoffset", 43, 10, 34, p)
        }
        DataType::Date => ("date", 40, 3, 10, 0),
        DataType::Time => {
            let p = i64::from(precision.unwrap_or(7).min(7));
            ("time", 41, 5, 16, p)
        }
        DataType::Uuid => ("uniqueidentifier", 36, 16, 0, 0),
        _ => ("nvarchar", 231, -1, 0, 0),
    }
}

/// A column's SQL Server type: the declared one when the table was created over TDS, else
/// derived from the stored type. `(name, system_type_id, max_length, precision, scale)`.
pub fn column_type(c: &Column) -> (String, i64, i64, i64, i64) {
    if let Some(d) = &c.declared {
        if let Some(t) = declared_type(d) {
            return t;
        }
    }
    let (n, id, len, p, s) = type_info(&c.ty, c.precision);
    (n.to_owned(), id, len, p, s)
}

fn declared_type(decl: &str) -> Option<(String, i64, i64, i64, i64)> {
    let (base, args) = match decl.split_once('(') {
        Some((b, rest)) => (b.trim(), rest.trim_end_matches(')').trim()),
        None => (decl.trim(), ""),
    };
    let base = base
        .rsplit('.')
        .next()
        .unwrap_or(base)
        .trim_matches(|c| c == '[' || c == ']');
    let nums: Vec<&str> = args
        .split(',')
        .map(str::trim)
        .filter(|a| !a.is_empty())
        .collect();
    let n = |k: usize| nums.get(k).and_then(|v| v.parse::<i64>().ok());
    let max = nums.first().is_some_and(|v| v.eq_ignore_ascii_case("max"));
    let base = match base {
        "integer" => "int",
        "character" => "char",
        "rowversion" => "timestamp",
        "double" => "float",
        "dec" => "decimal",
        "national" => "nvarchar",
        b => b,
    };
    let &(name, id, fixed_len, fixed_p, fixed_s) = TYPES.iter().find(|t| t.0 == base)?;
    Some(match name {
        "char" | "varchar" | "binary" | "varbinary" => (
            name.into(),
            id,
            if max { -1 } else { n(0).unwrap_or(1) },
            0,
            0,
        ),
        "nchar" | "nvarchar" => (
            name.into(),
            id,
            if max { -1 } else { n(0).unwrap_or(1) * 2 },
            0,
            0,
        ),
        "decimal" | "numeric" => {
            let p = n(0).unwrap_or(18);
            let len = match p {
                0..=9 => 5,
                10..=19 => 9,
                20..=28 => 13,
                _ => 17,
            };
            (name.into(), id, len, p, n(1).unwrap_or(0))
        }
        "datetime2" | "datetimeoffset" | "time" => {
            let p = n(0).unwrap_or(7).clamp(0, 7);
            let len = match name {
                "datetimeoffset" => 10,
                "time" => {
                    if p < 3 {
                        3
                    } else if p < 5 {
                        4
                    } else {
                        5
                    }
                }
                _ => {
                    if p < 3 {
                        6
                    } else if p < 5 {
                        7
                    } else {
                        8
                    }
                }
            };
            let prec = match name {
                "time" => 8 + if p > 0 { p + 1 } else { 0 },
                "datetime2" => 19 + if p > 0 { p + 1 } else { 0 },
                _ => 26 + if p > 0 { p + 1 } else { 0 },
            };
            (name.into(), id, len, prec, p)
        }
        "float" if n(0).is_some_and(|b| b <= 24) => ("real".into(), 59, 4, 24, 0),
        "text" | "ntext" | "image" => (name.into(), id, 16, 0, 0),
        "xml" => (name.into(), id, -1, 0, 0),
        _ => (name.into(), id, fixed_len, fixed_p, fixed_s),
    })
}

/// The type as T-SQL spells it in DDL: `nvarchar(50)`, `decimal(12,2)`, `nvarchar(max)`.
pub fn type_sql(c: &Column) -> String {
    if let Some(d) = &c.declared {
        return d.clone();
    }
    let (name, _, len, p, s) = type_info(&c.ty, c.precision);
    match name {
        "nvarchar" | "nchar" if len < 0 => format!("{name}(max)"),
        "nvarchar" | "nchar" => format!("{name}({})", len / 2),
        "varbinary" => "varbinary(max)".into(),
        "decimal" => format!("decimal({p},{s})"),
        "datetime2" | "datetimeoffset" | "time" => format!("{name}({s})"),
        _ => name.into(),
    }
}

/// A column default as SQL Server reports it: wrapped in parentheses, numbers twice.
pub fn default_sql(e: &crate::ast::Expr) -> String {
    use crate::ast::Expr;
    // Functions read back in SQL Server's spelling (CURRENT_TIMESTAMP is stored as getdate()).
    if let Expr::Function { name, args, .. } = e {
        if args.is_empty() {
            let n = match name.as_str() {
                "now" | "current_timestamp" | "getdate" | "localtimestamp" => "getdate",
                "sysdatetime" => "sysdatetime",
                "getutcdate" | "utc_timestamp" => "getutcdate",
                "sysutcdatetime" => "sysutcdatetime",
                "newid" | "gen_random_uuid" | "uuid" => "newid",
                "newsequentialid" => "newsequentialid",
                other => other,
            };
            return format!("({n}())");
        }
    }
    let s = e.to_string();
    if s.parse::<f64>().is_ok() {
        format!("(({s}))")
    } else {
        format!("({s})")
    }
}

/// The `sys` views this module serves.
const SYS_VIEWS: &[&str] = &[
    "schemas",
    "objects",
    "all_objects",
    "tables",
    "views",
    "sql_modules",
    "all_sql_modules",
    "columns",
    "all_columns",
    "identity_columns",
    "computed_columns",
    "types",
    "indexes",
    "index_columns",
    "key_constraints",
    "foreign_keys",
    "foreign_key_columns",
    "default_constraints",
    "check_constraints",
    "databases",
    "database_principals",
    "dm_exec_sessions",
    "sequences",
    "extended_properties",
    "system_views",
    "all_views",
    "triggers",
    "synonyms",
    "procedures",
    "table_types",
    "partitions",
];

const TYPES: &[(&str, i64, i64, i64, i64)] = &[
    ("bit", 104, 1, 1, 0),
    ("tinyint", 48, 1, 3, 0),
    ("smallint", 52, 2, 5, 0),
    ("int", 56, 4, 10, 0),
    ("bigint", 127, 8, 19, 0),
    ("decimal", 106, 17, 38, 38),
    ("numeric", 108, 17, 38, 38),
    ("money", 60, 8, 19, 4),
    ("smallmoney", 122, 4, 10, 4),
    ("real", 59, 4, 24, 0),
    ("float", 62, 8, 53, 0),
    ("date", 40, 3, 10, 0),
    ("time", 41, 5, 16, 7),
    ("datetime2", 42, 8, 27, 7),
    ("datetimeoffset", 43, 10, 34, 7),
    ("datetime", 61, 8, 23, 3),
    ("smalldatetime", 58, 4, 16, 0),
    ("char", 175, 8000, 0, 0),
    ("varchar", 167, 8000, 0, 0),
    ("text", 35, 16, 0, 0),
    ("nchar", 239, 8000, 0, 0),
    ("nvarchar", 231, 8000, 0, 0),
    ("ntext", 99, 16, 0, 0),
    ("binary", 173, 8000, 0, 0),
    ("varbinary", 165, 8000, 0, 0),
    ("image", 34, 16, 0, 0),
    ("uniqueidentifier", 36, 16, 0, 0),
    ("xml", 241, -1, 0, 0),
    ("sql_variant", 98, 8016, 0, 0),
    ("timestamp", 189, 8, 0, 0),
];

struct Builder {
    cols: Vec<ColMeta>,
    rows: Vec<Vec<Datum>>,
}

impl Builder {
    fn new(spec: &[(&str, DataType)]) -> Builder {
        Builder {
            cols: spec
                .iter()
                .map(|(n, t)| ColMeta::new(*n, Some(t.clone())))
                .collect(),
            rows: Vec::new(),
        }
    }
    fn row(&mut self, r: Vec<Datum>) {
        debug_assert_eq!(r.len(), self.cols.len());
        self.rows.push(r);
    }
    fn done(self) -> Rel {
        Rel {
            cols: self.cols,
            rows: self.rows,
        }
    }
}

fn t(s: impl Into<String>) -> Datum {
    Datum::Text(s.into())
}
fn i(n: i64) -> Datum {
    Datum::Int(n)
}
fn b(v: bool) -> Datum {
    Datum::Bool(v)
}
const N: Datum = Datum::Null;
use DataType::{BigInt as I8, Bool as B, Int as I4, Text as T, Timestamp as TS};

const EPOCH_2024: i64 = 1_704_067_200_000_000_000;

fn action(a: RefAction) -> (i64, &'static str) {
    match a {
        RefAction::NoAction | RefAction::Restrict => (0, "NO_ACTION"),
        RefAction::Cascade => (1, "CASCADE"),
        RefAction::SetNull => (2, "SET_NULL"),
        RefAction::SetDefault => (3, "SET_DEFAULT"),
    }
}

/// Indexes of a table as SQL Server numbers them: 1 the (clustered) primary key, then unique
/// constraints and plain indexes. `(index_id, name, columns, unique, primary, constraint)`.
fn indexes(tb: &Table) -> Vec<(i64, String, Vec<usize>, bool, bool, bool)> {
    let mut out = Vec::new();
    if !tb.primary_key.is_empty() {
        out.push((1, pk_name(tb), tb.primary_key.clone(), true, true, true));
    }
    let mut id = 2;
    for u in &tb.uniques {
        out.push((id, u.name.clone(), u.columns.clone(), true, false, true));
        id += 1;
    }
    for x in &tb.indexes {
        out.push((id, x.name.clone(), x.columns.clone(), false, false, false));
        id += 1;
    }
    out
}

/// The referenced table and column positions of a foreign key.
fn fk_target(ex: &Exec<'_>, tables: &[Arc<Table>], fk: &crate::catalog::FkDef) -> (i64, Vec<i64>) {
    let target = tables
        .iter()
        .find(|t| t.name.eq_ignore_ascii_case(&fk.ref_table))
        .cloned()
        .or_else(|| ex.s.engine.catalog.get(&fk.ref_table));
    let Some(rt) = target else {
        return (object_id("U", &fk.ref_table), vec![1; fk.columns.len()]);
    };
    let cols = if fk.ref_columns.is_empty() {
        rt.primary_key.iter().map(|c| *c as i64 + 1).collect()
    } else {
        fk.ref_columns
            .iter()
            .map(|n| rt.column_index(n).map_or(0, |c| c as i64 + 1))
            .collect()
    };
    (table_object(&rt), cols)
}

/// `sys.<name>`.
pub fn sys_catalog(ex: &Exec<'_>, n: &str, tables: &[Arc<Table>]) -> SqlResult<Rel> {
    let views = ex.s.engine.view_names()?;
    let created = Datum::Timestamp(EPOCH_2024);
    let object_cols: &[(&str, DataType)] = &[
        ("name", T),
        ("object_id", I4),
        ("principal_id", I4),
        ("schema_id", I4),
        ("parent_object_id", I4),
        ("type", T),
        ("type_desc", T),
        ("create_date", TS),
        ("modify_date", TS),
        ("is_ms_shipped", B),
        ("is_published", B),
        ("is_schema_published", B),
    ];
    let object_row = |name: &str, id: i64, parent: i64, ty: &str, desc: &str| {
        vec![
            t(name),
            i(id),
            N,
            i(DBO),
            i(parent),
            t(ty),
            t(desc),
            created.clone(),
            created.clone(),
            b(false),
            b(false),
            b(false),
        ]
    };
    Ok(match n {
        "schemas" => {
            let mut bd = Builder::new(&[("name", T), ("schema_id", I4), ("principal_id", I4)]);
            for (name, id) in [
                ("dbo", DBO),
                ("guest", 2),
                ("INFORMATION_SCHEMA", INFORMATION_SCHEMA),
                ("sys", SYS_SCHEMA),
            ] {
                bd.row(vec![t(name), i(id), i(1)]);
            }
            bd.done()
        }
        "objects" | "all_objects" => {
            let mut bd = Builder::new(object_cols);
            for tb in tables {
                let id = table_object(tb);
                bd.row(object_row(&tb.name, id, 0, "U", "USER_TABLE"));
                if !tb.primary_key.is_empty() {
                    bd.row(object_row(
                        &pk_name(tb),
                        object_id("PK", &pk_name(tb)),
                        id,
                        "PK",
                        "PRIMARY_KEY_CONSTRAINT",
                    ));
                }
                for u in &tb.uniques {
                    bd.row(object_row(
                        &u.name,
                        object_id("UQ", &u.name),
                        id,
                        "UQ",
                        "UNIQUE_CONSTRAINT",
                    ));
                }
                for f in &tb.foreign_keys {
                    bd.row(object_row(
                        &f.name,
                        object_id("F", &f.name),
                        id,
                        "F",
                        "FOREIGN_KEY_CONSTRAINT",
                    ));
                }
                for c in &tb.checks {
                    bd.row(object_row(
                        &c.name,
                        object_id("C", &c.name),
                        id,
                        "C",
                        "CHECK_CONSTRAINT",
                    ));
                }
                for c in tb.columns.iter().filter(|c| c.default.is_some()) {
                    let dn = default_name(&tb.name, c);
                    bd.row(object_row(
                        &dn,
                        object_id("D", &dn),
                        id,
                        "D",
                        "DEFAULT_CONSTRAINT",
                    ));
                }
            }
            for v in &views {
                bd.row(object_row(v, object_id("V", v), 0, "V", "VIEW"));
            }
            bd.done()
        }
        "tables" => {
            let mut spec = object_cols.to_vec();
            spec.extend([
                ("lob_data_space_id", I4),
                ("is_memory_optimized", B),
                ("temporal_type", I4),
                ("temporal_type_desc", T),
                ("is_external", B),
                ("is_filetable", B),
                ("is_tracked_by_cdc", B),
                ("history_table_id", I4),
                ("is_node", B),
                ("is_edge", B),
                ("ledger_type", I4),
            ]);
            let mut bd = Builder::new(&spec);
            for tb in tables {
                let mut r = object_row(&tb.name, table_object(tb), 0, "U", "USER_TABLE");
                r.extend([
                    i(1),
                    b(false),
                    i(0),
                    t("NON_TEMPORAL_TABLE"),
                    b(false),
                    b(false),
                    b(false),
                    N,
                    b(false),
                    b(false),
                    i(0),
                ]);
                bd.row(r);
            }
            bd.done()
        }
        "views" => {
            let mut spec = object_cols.to_vec();
            spec.push(("is_replicated", B));
            let mut bd = Builder::new(&spec);
            for v in &views {
                let mut r = object_row(v, object_id("V", v), 0, "V", "VIEW");
                r.push(b(false));
                bd.row(r);
            }
            bd.done()
        }
        "sql_modules" | "all_sql_modules" => {
            let mut bd = Builder::new(&[("object_id", I4), ("definition", T)]);
            for v in &views {
                if let Some((sql, _, _)) = ex.s.engine.view(v)? {
                    bd.row(vec![
                        i(object_id("V", v)),
                        t(format!("CREATE VIEW [{v}] AS {sql}")),
                    ]);
                }
            }
            bd.done()
        }
        "columns" | "all_columns" => {
            let mut bd = Builder::new(&[
                ("object_id", I4),
                ("name", T),
                ("column_id", I4),
                ("system_type_id", I4),
                ("user_type_id", I4),
                ("max_length", I4),
                ("precision", I4),
                ("scale", I4),
                ("collation_name", T),
                ("is_nullable", B),
                ("is_ansi_padded", B),
                ("is_rowguidcol", B),
                ("is_identity", B),
                ("is_computed", B),
                ("is_filestream", B),
                ("is_replicated", B),
                ("is_sparse", B),
                ("is_column_set", B),
                ("generated_always_type", I4),
                ("generated_always_type_desc", T),
                ("default_object_id", I4),
                ("rule_object_id", I4),
                ("is_hidden", B),
                ("is_masked", B),
            ]);
            for tb in tables {
                for (k, c) in tb.columns.iter().enumerate() {
                    let (name, id, len, p, s) = column_type(c);
                    let text = matches!(
                        name.as_str(),
                        "nvarchar" | "nchar" | "varchar" | "char" | "text" | "ntext"
                    );
                    bd.row(vec![
                        i(table_object(tb)),
                        t(&c.name),
                        i(k as i64 + 1),
                        i(id),
                        i(id),
                        i(len),
                        i(p),
                        i(s),
                        if text { t(COLLATION) } else { N },
                        b(!c.not_null),
                        b(true),
                        b(false),
                        b(c.auto_increment),
                        b(false),
                        b(false),
                        b(false),
                        b(false),
                        b(false),
                        i(0),
                        t("NOT_APPLICABLE"),
                        i(if c.default.is_some() {
                            object_id("D", &default_name(&tb.name, c))
                        } else {
                            0
                        }),
                        i(0),
                        b(false),
                        b(false),
                    ]);
                }
            }
            bd.done()
        }
        "identity_columns" => {
            let mut bd = Builder::new(&[
                ("object_id", I4),
                ("name", T),
                ("column_id", I4),
                ("seed_value", I8),
                ("increment_value", I8),
                ("last_value", I8),
                ("is_not_for_replication", B),
                ("is_identity", B),
            ]);
            for tb in tables {
                for (k, c) in tb
                    .columns
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| c.auto_increment)
                {
                    let last = super::dml::identity_current(&ex.s.engine, tb, k)?;
                    bd.row(vec![
                        i(table_object(tb)),
                        t(&c.name),
                        i(k as i64 + 1),
                        i(1),
                        i(1),
                        last.map_or(N, i),
                        b(false),
                        b(true),
                    ]);
                }
            }
            bd.done()
        }
        "computed_columns" => Builder::new(&[
            ("object_id", I4),
            ("name", T),
            ("column_id", I4),
            ("definition", T),
            ("is_persisted", B),
        ])
        .done(),
        "types" => {
            let mut bd = Builder::new(&[
                ("name", T),
                ("system_type_id", I4),
                ("user_type_id", I4),
                ("schema_id", I4),
                ("principal_id", I4),
                ("max_length", I4),
                ("precision", I4),
                ("scale", I4),
                ("collation_name", T),
                ("is_nullable", B),
                ("is_user_defined", B),
                ("is_assembly_type", B),
                ("default_object_id", I4),
                ("rule_object_id", I4),
                ("is_table_type", B),
            ]);
            let mut add = |name: &str, sys: i64, user: i64, len: i64, p: i64, s: i64| {
                let text = matches!(
                    name,
                    "char" | "varchar" | "text" | "nchar" | "nvarchar" | "ntext" | "sysname"
                );
                bd.row(vec![
                    t(name),
                    i(sys),
                    i(user),
                    i(SYS_SCHEMA),
                    N,
                    i(len),
                    i(p),
                    i(s),
                    if text { t(COLLATION) } else { N },
                    b(true),
                    b(false),
                    b(false),
                    i(0),
                    i(0),
                    b(false),
                ]);
            };
            for (name, id, len, p, s) in TYPES {
                add(name, *id, *id, *len, *p, *s);
            }
            add("sysname", 231, 256, 256, 0, 0);
            bd.done()
        }
        "indexes" => {
            let mut bd = Builder::new(&[
                ("object_id", I4),
                ("name", T),
                ("index_id", I4),
                ("type", I4),
                ("type_desc", T),
                ("is_unique", B),
                ("data_space_id", I4),
                ("ignore_dup_key", B),
                ("is_primary_key", B),
                ("is_unique_constraint", B),
                ("fill_factor", I4),
                ("is_padded", B),
                ("is_disabled", B),
                ("is_hypothetical", B),
                ("allow_row_locks", B),
                ("allow_page_locks", B),
                ("has_filter", B),
                ("filter_definition", T),
                ("auto_created", B),
            ]);
            for tb in tables {
                let obj = table_object(tb);
                if tb.primary_key.is_empty() {
                    bd.row(vec![
                        i(obj),
                        N,
                        i(0),
                        i(0),
                        t("HEAP"),
                        b(false),
                        i(1),
                        b(false),
                        b(false),
                        b(false),
                        i(0),
                        b(false),
                        b(false),
                        b(false),
                        b(true),
                        b(true),
                        b(false),
                        N,
                        b(false),
                    ]);
                }
                for (id, name, _, unique, primary, constraint) in indexes(tb) {
                    let clustered = primary;
                    bd.row(vec![
                        i(obj),
                        t(name),
                        i(id),
                        i(if clustered { 1 } else { 2 }),
                        t(if clustered {
                            "CLUSTERED"
                        } else {
                            "NONCLUSTERED"
                        }),
                        b(unique),
                        i(1),
                        b(false),
                        b(primary),
                        b(constraint && !primary),
                        i(0),
                        b(false),
                        b(false),
                        b(false),
                        b(true),
                        b(true),
                        b(false),
                        N,
                        b(false),
                    ]);
                }
            }
            bd.done()
        }
        "index_columns" => {
            let mut bd = Builder::new(&[
                ("object_id", I4),
                ("index_id", I4),
                ("index_column_id", I4),
                ("column_id", I4),
                ("key_ordinal", I4),
                ("partition_ordinal", I4),
                ("is_descending_key", B),
                ("is_included_column", B),
                ("column_store_order_ordinal", I4),
            ]);
            for tb in tables {
                for (id, _, cols, ..) in indexes(tb) {
                    for (k, c) in cols.iter().enumerate() {
                        bd.row(vec![
                            i(table_object(tb)),
                            i(id),
                            i(k as i64 + 1),
                            i(*c as i64 + 1),
                            i(k as i64 + 1),
                            i(0),
                            b(false),
                            b(false),
                            i(0),
                        ]);
                    }
                }
            }
            bd.done()
        }
        "key_constraints" => {
            let mut spec = object_cols.to_vec();
            spec.extend([
                ("unique_index_id", I4),
                ("is_system_named", B),
                ("is_enforced", B),
            ]);
            let mut bd = Builder::new(&spec);
            for tb in tables {
                for (id, name, _, _, primary, constraint) in indexes(tb) {
                    if !constraint {
                        continue;
                    }
                    let (ty, desc) = if primary {
                        ("PK", "PRIMARY_KEY_CONSTRAINT")
                    } else {
                        ("UQ", "UNIQUE_CONSTRAINT")
                    };
                    let mut r = object_row(&name, object_id(ty, &name), table_object(tb), ty, desc);
                    r.extend([i(id), b(false), b(true)]);
                    bd.row(r);
                }
            }
            bd.done()
        }
        "foreign_keys" => {
            let mut spec = object_cols.to_vec();
            spec.extend([
                ("referenced_object_id", I4),
                ("key_index_id", I4),
                ("is_disabled", B),
                ("is_not_for_replication", B),
                ("is_not_trusted", B),
                ("delete_referential_action", I4),
                ("delete_referential_action_desc", T),
                ("update_referential_action", I4),
                ("update_referential_action_desc", T),
                ("is_system_named", B),
            ]);
            let mut bd = Builder::new(&spec);
            for tb in tables {
                for f in &tb.foreign_keys {
                    let (target, _) = fk_target(ex, tables, f);
                    let (d, dd) = action(f.on_delete);
                    let (u, ud) = action(f.on_update);
                    let mut r = object_row(
                        &f.name,
                        object_id("F", &f.name),
                        table_object(tb),
                        "F",
                        "FOREIGN_KEY_CONSTRAINT",
                    );
                    r.extend([
                        i(target),
                        i(1),
                        b(false),
                        b(false),
                        b(false),
                        i(d),
                        t(dd),
                        i(u),
                        t(ud),
                        b(false),
                    ]);
                    bd.row(r);
                }
            }
            bd.done()
        }
        "foreign_key_columns" => {
            let mut bd = Builder::new(&[
                ("constraint_object_id", I4),
                ("constraint_column_id", I4),
                ("parent_object_id", I4),
                ("parent_column_id", I4),
                ("referenced_object_id", I4),
                ("referenced_column_id", I4),
            ]);
            for tb in tables {
                for f in &tb.foreign_keys {
                    let (target, refs) = fk_target(ex, tables, f);
                    for (k, (c, r)) in f.columns.iter().zip(refs).enumerate() {
                        bd.row(vec![
                            i(object_id("F", &f.name)),
                            i(k as i64 + 1),
                            i(table_object(tb)),
                            i(*c as i64 + 1),
                            i(target),
                            i(r),
                        ]);
                    }
                }
            }
            bd.done()
        }
        "default_constraints" => {
            let mut spec = object_cols.to_vec();
            spec.extend([
                ("parent_column_id", I4),
                ("definition", T),
                ("is_system_named", B),
            ]);
            let mut bd = Builder::new(&spec);
            for tb in tables {
                for (k, c) in tb.columns.iter().enumerate() {
                    if let Some(d) = &c.default {
                        let dn = default_name(&tb.name, c);
                        let mut r = object_row(
                            &dn,
                            object_id("D", &dn),
                            table_object(tb),
                            "D",
                            "DEFAULT_CONSTRAINT",
                        );
                        r.extend([i(k as i64 + 1), t(default_sql(d)), b(false)]);
                        bd.row(r);
                    }
                }
            }
            bd.done()
        }
        "check_constraints" => {
            let mut spec = object_cols.to_vec();
            spec.extend([
                ("is_disabled", B),
                ("is_not_trusted", B),
                ("parent_column_id", I4),
                ("definition", T),
                ("is_system_named", B),
            ]);
            let mut bd = Builder::new(&spec);
            for tb in tables {
                for c in &tb.checks {
                    let mut r = object_row(
                        &c.name,
                        object_id("C", &c.name),
                        table_object(tb),
                        "C",
                        "CHECK_CONSTRAINT",
                    );
                    r.extend([
                        b(false),
                        b(false),
                        i(0),
                        t(format!("({})", c.expr)),
                        b(false),
                    ]);
                    bd.row(r);
                }
            }
            bd.done()
        }
        "databases" => {
            let mut bd = Builder::new(&[
                ("name", T),
                ("database_id", I4),
                ("owner_sid", T),
                ("create_date", TS),
                ("compatibility_level", I4),
                ("collation_name", T),
                ("user_access", I4),
                ("user_access_desc", T),
                ("is_read_only", B),
                ("state", I4),
                ("state_desc", T),
                ("snapshot_isolation_state", I4),
                ("snapshot_isolation_state_desc", T),
                ("is_read_committed_snapshot_on", B),
                ("recovery_model", I4),
                ("recovery_model_desc", T),
                ("is_memory_optimized_elevate_to_snapshot_on", B),
            ]);
            let mut names = vec![
                "master".to_owned(),
                "tempdb".to_owned(),
                "model".to_owned(),
                "msdb".to_owned(),
            ];
            for d in super::system::database_list(ex.s)? {
                if !names.contains(&d) {
                    names.push(d);
                }
            }
            for (k, name) in names.iter().enumerate() {
                bd.row(vec![
                    t(name),
                    i(k as i64 + 1),
                    N,
                    created.clone(),
                    i(160),
                    t(COLLATION),
                    i(0),
                    t("MULTI_USER"),
                    b(false),
                    i(0),
                    t("ONLINE"),
                    i(1),
                    t("ON"),
                    b(true),
                    i(1),
                    t("FULL"),
                    b(false),
                ]);
            }
            bd.done()
        }
        "database_principals" => {
            let mut bd = Builder::new(&[
                ("name", T),
                ("principal_id", I4),
                ("type", T),
                ("type_desc", T),
                ("default_schema_name", T),
                ("owning_principal_id", I4),
                ("is_fixed_role", B),
            ]);
            bd.row(vec![
                t("dbo"),
                i(1),
                t("S"),
                t("SQL_USER"),
                t("dbo"),
                N,
                b(false),
            ]);
            if ex.s.user != "dbo" {
                bd.row(vec![
                    t(&ex.s.user),
                    i(5),
                    t("S"),
                    t("SQL_USER"),
                    t("dbo"),
                    N,
                    b(false),
                ]);
            }
            bd.done()
        }
        "dm_exec_sessions" => {
            let mut bd = Builder::new(&[
                ("session_id", I4),
                ("login_name", T),
                ("transaction_isolation_level", I4),
                ("is_user_process", B),
                ("database_id", I4),
            ]);
            let level = match ex
                .s
                .vars
                .get("transaction_isolation")
                .map(|v| v.to_ascii_lowercase())
            {
                Some(v) if v.contains("uncommitted") => 1,
                Some(v) if v.contains("repeatable") => 3,
                Some(v) if v.contains("serializable") => 4,
                Some(v) if v.contains("snapshot") => 5,
                _ => 2,
            };
            bd.row(vec![
                i(ex.s.id as i64),
                t(&ex.s.user),
                i(level),
                b(true),
                i(5),
            ]);
            bd.done()
        }
        "sequences" => {
            let mut spec = object_cols.to_vec();
            spec.extend([
                ("start_value", I8),
                ("increment", I8),
                ("minimum_value", I8),
                ("maximum_value", I8),
                ("is_cycling", B),
                ("is_cached", B),
                ("current_value", I8),
                ("system_type_id", I4),
                ("user_type_id", I4),
            ]);
            let mut bd = Builder::new(&spec);
            for q in super::dml::user_sequences(&ex.s.engine)? {
                let mut r = object_row(
                    &q.name,
                    object_id("SO", &q.name),
                    0,
                    "SO",
                    "SEQUENCE_OBJECT",
                );
                r.extend([
                    i(q.start),
                    i(q.increment),
                    i(i64::MIN),
                    i(i64::MAX),
                    b(false),
                    b(false),
                    i(q.next - q.increment),
                    i(127),
                    i(127),
                ]);
                bd.row(r);
            }
            bd.done()
        }
        "system_views" | "all_views" => {
            let mut bd = Builder::new(&[
                ("name", T),
                ("object_id", I4),
                ("schema_id", I4),
                ("type", T),
            ]);
            for (k, v) in SYS_VIEWS.iter().enumerate() {
                bd.row(vec![t(*v), i(-100 - k as i64), i(SYS_SCHEMA), t("V")]);
            }
            if n == "all_views" {
                for v in &views {
                    bd.row(vec![t(v), i(object_id("V", v)), i(DBO), t("V")]);
                }
            }
            bd.done()
        }
        "extended_properties" => Builder::new(&[
            ("class", I4),
            ("class_desc", T),
            ("major_id", I4),
            ("minor_id", I4),
            ("name", T),
            ("value", T),
        ])
        .done(),
        "triggers"
        | "synonyms"
        | "procedures"
        | "table_types"
        | "partitions"
        | "assemblies"
        | "xml_schema_collections"
        | "fulltext_indexes"
        | "spatial_indexes"
        | "stats" => {
            let mut spec = object_cols.to_vec();
            spec.extend([("is_disabled", B), ("base_object_name", T), ("rows", I8)]);
            Builder::new(&spec).done()
        }
        other => {
            return Err(SqlError::new(
                "42P01",
                format!("Invalid object name 'sys.{other}' (not available in Storine SQL)"),
            ))
        }
    })
}

fn text(d: &Datum) -> String {
    d.as_text()
}

/// Strips `[dbo].[name]`, `"dbo"."name"`, `shop.dbo.name` down to the object name.
fn bare_name(s: &str) -> String {
    let last = s.rsplit('.').next().unwrap_or(s).trim();
    last.trim_matches(|c| c == '[' || c == ']' || c == '"' || c == '`')
        .to_owned()
}

fn find_object(ex: &Exec<'_>, raw: &str) -> SqlResult<Option<(i64, String, &'static str)>> {
    let name = bare_name(raw);
    if name.starts_with('#') {
        return Ok(None);
    }
    ex.s.engine.refresh_catalog(false)?;
    for tb in ex.s.engine.catalog.all() {
        if tb.name.eq_ignore_ascii_case(&name) {
            return Ok(Some((table_object(&tb), tb.name.clone(), "U")));
        }
        if !tb.primary_key.is_empty() && pk_name(&tb).eq_ignore_ascii_case(&name) {
            return Ok(Some((object_id("PK", &pk_name(&tb)), pk_name(&tb), "PK")));
        }
        for u in &tb.uniques {
            if u.name.eq_ignore_ascii_case(&name) {
                return Ok(Some((object_id("UQ", &u.name), u.name.clone(), "UQ")));
            }
        }
        for f in &tb.foreign_keys {
            if f.name.eq_ignore_ascii_case(&name) {
                return Ok(Some((object_id("F", &f.name), f.name.clone(), "F")));
            }
        }
        for c in &tb.checks {
            if c.name.eq_ignore_ascii_case(&name) {
                return Ok(Some((object_id("C", &c.name), c.name.clone(), "C")));
            }
        }
        for c in &tb.columns {
            let dn = default_name(&tb.name, c);
            if c.default.is_some() && dn.eq_ignore_ascii_case(&name) {
                return Ok(Some((object_id("D", &dn), dn, "D")));
            }
        }
        for x in &tb.indexes {
            if x.name.eq_ignore_ascii_case(&name) {
                return Ok(Some((table_object(&tb), tb.name.clone(), "U")));
            }
        }
    }
    for v in ex.s.engine.view_names()? {
        if v.eq_ignore_ascii_case(&name) {
            return Ok(Some((object_id("V", &v), v, "V")));
        }
    }
    Ok(None)
}

fn object_by_id(ex: &Exec<'_>, id: i64) -> SqlResult<Option<(String, Option<Arc<Table>>)>> {
    ex.s.engine.refresh_catalog(false)?;
    for tb in ex.s.engine.catalog.all() {
        if table_object(&tb) == id {
            return Ok(Some((tb.name.clone(), Some(tb))));
        }
    }
    for v in ex.s.engine.view_names()? {
        if object_id("V", &v) == id {
            return Ok(Some((v, None)));
        }
    }
    Ok(None)
}

/// The definition of the default or check constraint with this `object_id`.
fn constraint_definition(ex: &Exec<'_>, id: i64) -> SqlResult<Option<String>> {
    ex.s.engine.refresh_catalog(false)?;
    for tb in ex.s.engine.catalog.all() {
        for c in &tb.columns {
            if let Some(d) = &c.default {
                if object_id("D", &default_name(&tb.name, c)) == id {
                    return Ok(Some(default_sql(d)));
                }
            }
        }
        for c in &tb.checks {
            if object_id("C", &c.name) == id {
                return Ok(Some(format!("({})", c.expr)));
            }
        }
    }
    Ok(None)
}

/// The name of the constraint (key, foreign key, check, default) with this `object_id`.
fn constraint_by_id(ex: &Exec<'_>, id: i64) -> SqlResult<Option<String>> {
    ex.s.engine.refresh_catalog(false)?;
    for tb in ex.s.engine.catalog.all() {
        let mut names: Vec<(&str, String)> = Vec::new();
        if !tb.primary_key.is_empty() {
            names.push(("PK", pk_name(&tb)));
        }
        names.extend(tb.uniques.iter().map(|u| ("UQ", u.name.clone())));
        names.extend(tb.foreign_keys.iter().map(|f| ("F", f.name.clone())));
        names.extend(tb.checks.iter().map(|c| ("C", c.name.clone())));
        names.extend(
            tb.columns
                .iter()
                .filter(|c| c.default.is_some())
                .map(|c| ("D", default_name(&tb.name, c))),
        );
        if let Some((_, n)) = names.into_iter().find(|(k, n)| object_id(k, n) == id) {
            return Ok(Some(n));
        }
    }
    Ok(None)
}

fn table_named(ex: &Exec<'_>, raw: &str) -> SqlResult<Option<Arc<Table>>> {
    ex.s.engine.refresh_catalog(false)?;
    let name = bare_name(raw);
    Ok(ex
        .s
        .engine
        .catalog
        .all()
        .into_iter()
        .find(|t| t.name.eq_ignore_ascii_case(&name)))
}

/// Units of DATEADD / DATEDIFF / DATEPART, with their abbreviations.
fn date_unit(s: &str) -> Option<&'static str> {
    Some(match s.to_ascii_lowercase().as_str() {
        "year" | "yy" | "yyyy" => "year",
        "quarter" | "qq" | "q" => "quarter",
        "month" | "mm" | "m" => "month",
        "dayofyear" | "dy" | "y" => "doy",
        "day" | "dd" | "d" => "day",
        "week" | "wk" | "ww" => "week",
        "weekday" | "dw" => "dow",
        "hour" | "hh" => "hour",
        "minute" | "mi" | "n" => "minute",
        "second" | "ss" | "s" => "second",
        "millisecond" | "ms" => "millisecond",
        "microsecond" | "mcs" => "microsecond",
        "nanosecond" | "ns" => "nanosecond",
        _ => return None,
    })
}

const NS_PER_DAY: i64 = 86_400_000_000_000;

fn civil(days: i64) -> (i64, i64, i64) {
    // Days since 1970-01-01 -> (year, month, day), Howard Hinnant's algorithm.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn days_in_month(y: i64, m: i64) -> i64 {
    let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
    days_from_civil(ny, nm, 1) - days_from_civil(y, m, 1)
}

/// A temporal value as nanoseconds since the epoch, and whether it was a DATE.
fn instant(d: &Datum) -> SqlResult<(i64, bool)> {
    Ok(match d {
        Datum::Date(days) => (i64::from(*days) * NS_PER_DAY, true),
        Datum::Timestamp(ns) => (*ns, false),
        Datum::Time(ns) => (*ns - 25_567 * NS_PER_DAY, false), // TIME counts from 1900-01-01
        other => match datum::cast(other, &DataType::Timestamp)? {
            Datum::Timestamp(ns) => (ns, false),
            _ => return Err(SqlError::datatype_mismatch("expected a date or time")),
        },
    })
}

fn date_add(unit: &str, n: i64, d: &Datum) -> SqlResult<Datum> {
    let (ns, was_date) = instant(d)?;
    let add_months = |ns: i64, months: i64| {
        let days = ns.div_euclid(NS_PER_DAY);
        let tod = ns.rem_euclid(NS_PER_DAY);
        let (y, m, day) = civil(days);
        let total = y * 12 + (m - 1) + months;
        let (ny, nm) = (total.div_euclid(12), total.rem_euclid(12) + 1);
        let nd = day.min(days_in_month(ny, nm));
        days_from_civil(ny, nm, nd) * NS_PER_DAY + tod
    };
    let out = match unit {
        "year" => add_months(ns, n * 12),
        "quarter" => add_months(ns, n * 3),
        "month" => add_months(ns, n),
        "day" | "doy" | "dow" => ns + n * NS_PER_DAY,
        "week" => ns + n * 7 * NS_PER_DAY,
        "hour" => ns + n * 3_600_000_000_000,
        "minute" => ns + n * 60_000_000_000,
        "second" => ns + n * 1_000_000_000,
        "millisecond" => ns + n * 1_000_000,
        "microsecond" => ns + n * 1_000,
        _ => ns + n,
    };
    Ok(match d {
        Datum::Date(_)
            if was_date
                && matches!(
                    unit,
                    "year" | "quarter" | "month" | "day" | "doy" | "dow" | "week"
                ) =>
        {
            Datum::Date(out.div_euclid(NS_PER_DAY) as i32)
        }
        _ => Datum::Timestamp(out),
    })
}

/// DATEDIFF: the number of `unit` boundaries crossed between the two values.
fn date_diff(unit: &str, a: &Datum, b: &Datum) -> SqlResult<i64> {
    let (x, _) = instant(a)?;
    let (y, _) = instant(b)?;
    let months = |ns: i64| {
        let (yy, mm, _) = civil(ns.div_euclid(NS_PER_DAY));
        yy * 12 + mm - 1
    };
    let floor = |ns: i64, unit: i64| ns.div_euclid(unit);
    Ok(match unit {
        "year" => months(y).div_euclid(12) - months(x).div_euclid(12),
        "quarter" => months(y).div_euclid(3) - months(x).div_euclid(3),
        "month" => months(y) - months(x),
        "day" | "doy" | "dow" => floor(y, NS_PER_DAY) - floor(x, NS_PER_DAY),
        // Weeks start on Sunday (DATEFIRST 7); 1970-01-04 was a Sunday.
        "week" => {
            floor(y - 3 * NS_PER_DAY, 7 * NS_PER_DAY) - floor(x - 3 * NS_PER_DAY, 7 * NS_PER_DAY)
        }
        "hour" => floor(y, 3_600_000_000_000) - floor(x, 3_600_000_000_000),
        "minute" => floor(y, 60_000_000_000) - floor(x, 60_000_000_000),
        "second" => floor(y, 1_000_000_000) - floor(x, 1_000_000_000),
        "millisecond" => floor(y, 1_000_000) - floor(x, 1_000_000),
        "microsecond" => floor(y, 1_000) - floor(x, 1_000),
        _ => y - x,
    })
}

fn unit_arg(d: &Datum) -> SqlResult<&'static str> {
    let s = text(d);
    date_unit(&s).ok_or_else(|| {
        SqlError::new(
            "42883",
            format!("'{s}' is not a recognized datepart option"),
        )
    })
}

/// A T-SQL built-in, or `None` when `name` is not one (the shared functions handle it).
pub fn function(ex: &Exec<'_>, name: &str, args: &[Datum]) -> Option<SqlResult<Datum>> {
    call(ex, name, args).transpose()
}

fn call(ex: &Exec<'_>, name: &str, args: &[Datum]) -> SqlResult<Option<Datum>> {
    let a = |k: usize| args.get(k).cloned().unwrap_or(Datum::Null);
    let any_null = |n: usize| args.iter().take(n).any(Datum::is_null);
    Ok(Some(match name {
        "schema_name" => match args.first() {
            None => t("dbo"),
            Some(d) => match d.as_i64() {
                Ok(DBO) => t("dbo"),
                Ok(SYS_SCHEMA) => t("sys"),
                Ok(INFORMATION_SCHEMA) => t("INFORMATION_SCHEMA"),
                _ => N,
            },
        },
        "schema_id" => match args.first().map(text).map(|s| s.to_ascii_lowercase()) {
            None => i(DBO),
            Some(s) if s == "dbo" => i(DBO),
            Some(s) if s == "sys" => i(SYS_SCHEMA),
            Some(s) if s == "information_schema" => i(INFORMATION_SCHEMA),
            _ => N,
        },
        "db_name" => t(&ex.s.database),
        "db_id" => i(5),
        "object_id" => {
            if any_null(1) {
                return Ok(Some(N));
            }
            let want = args.get(1).map(|d| text(d).trim().to_ascii_uppercase());
            match find_object(ex, &text(&a(0)))? {
                Some((id, _, ty)) if want.as_deref().is_none_or(|w| w == ty) => i(id),
                _ => N,
            }
        }
        "object_name" => {
            let id = a(0).as_i64().unwrap_or(0);
            match object_by_id(ex, id)? {
                Some((n, _)) => t(n),
                None => constraint_by_id(ex, id)?.map_or(N, t),
            }
        }
        // Internal helper of the catalog views: the ODBC scale of a type.
        "odbcscale" => match a(0).as_i64().unwrap_or(0) {
            106 | 108 | 60 | 122 | 41 | 42 | 43 => a(1),
            48 | 52 | 56 | 127 | 104 => i(0),
            _ => N,
        },
        "object_definition" => {
            let id = a(0).as_i64().unwrap_or(0);
            let mut def = constraint_definition(ex, id)?.map_or(N, t);
            for v in ex.s.engine.view_names()? {
                if object_id("V", &v) == id {
                    if let Some((sql, _, _)) = ex.s.engine.view(&v)? {
                        def = t(format!("CREATE VIEW [{v}] AS {sql}"));
                    }
                }
            }
            def
        }
        "object_schema_name" => match object_by_id(ex, a(0).as_i64().unwrap_or(0))? {
            Some(_) => t("dbo"),
            None => N,
        },
        "objectproperty" | "objectpropertyex" => {
            let found = object_by_id(ex, a(0).as_i64().unwrap_or(0))?;
            let prop = text(&a(1)).to_ascii_lowercase();
            match (found, prop.as_str()) {
                (None, _) => N,
                (Some((_, tb)), "isusertable" | "istable") => i(i64::from(tb.is_some())),
                (Some((_, tb)), "isview") => i(i64::from(tb.is_none())),
                (Some(_), "ismsshipped" | "issystemtable") => i(0),
                (Some((_, Some(tb))), "tablehasidentity") => {
                    i(i64::from(tb.columns.iter().any(|c| c.auto_increment)))
                }
                (Some((_, Some(tb))), "tablehasprimarykey") => {
                    i(i64::from(!tb.primary_key.is_empty()))
                }
                _ => N,
            }
        }
        "col_name" => match object_by_id(ex, a(0).as_i64().unwrap_or(0))? {
            Some((_, Some(tb))) => {
                let k = a(1).as_i64().unwrap_or(0);
                tb.columns
                    .get((k - 1).max(0) as usize)
                    .filter(|_| k >= 1)
                    .map_or(N, |c| t(&c.name))
            }
            _ => N,
        },
        "col_length" => match table_named(ex, &text(&a(0)))? {
            Some(tb) => match tb.column_index(&text(&a(1))) {
                Some(k) => i(column_type(&tb.columns[k]).2),
                None => N,
            },
            None => N,
        },
        "columnproperty" => match object_by_id(ex, a(0).as_i64().unwrap_or(0))? {
            Some((_, Some(tb))) => match tb.column_index(&text(&a(1))) {
                Some(k) => {
                    let c = &tb.columns[k];
                    match text(&a(2)).to_ascii_lowercase().as_str() {
                        "isidentity" => i(i64::from(c.auto_increment)),
                        "allowsnull" => i(i64::from(!c.not_null)),
                        "iscomputed" | "isrowguidcol" | "isfulltextindexed" => i(0),
                        "precision" => i(column_type(c).3),
                        "scale" => i(column_type(c).4),
                        "columnid" | "ordinal" => i(k as i64 + 1),
                        "charmaxlen" => {
                            let (ty, _, len, _, _) = column_type(c);
                            match ty.as_str() {
                                _ if len < 0 => i(-1),
                                "nchar" | "nvarchar" => i(len / 2),
                                "char" | "varchar" | "binary" | "varbinary" => i(len),
                                _ => N,
                            }
                        }
                        _ => N,
                    }
                }
                None => N,
            },
            _ => N,
        },
        "type_name" => {
            let id = a(0).as_i64().unwrap_or(0);
            if id == 256 {
                t("sysname")
            } else {
                TYPES.iter().find(|x| x.1 == id).map_or(N, |x| t(x.0))
            }
        }
        "type_id" => {
            let n = bare_name(&text(&a(0))).to_ascii_lowercase();
            if n == "sysname" {
                i(256)
            } else {
                TYPES.iter().find(|x| x.0 == n).map_or(N, |x| i(x.1))
            }
        }
        "ident_current" => {
            // The last value handed out for the table's IDENTITY column.
            match table_named(ex, &text(&a(0)))? {
                Some(tb) => match tb.columns.iter().position(|c| c.auto_increment) {
                    Some(c) => super::dml::identity_current(&ex.s.engine, &tb, c)?.map_or(N, i),
                    None => N,
                },
                None => N,
            }
        }
        "ident_seed" | "ident_incr" => i(1),
        "serverproperty" => match text(&a(0)).to_ascii_lowercase().as_str() {
            "productversion" => t(PRODUCT_VERSION),
            "productmajorversion" => t("16"),
            "productminorversion" => t("0"),
            "productlevel" => t("RTM"),
            "edition" => t("Developer Edition (64-bit)"),
            "engineedition" => i(3),
            "editionid" => i(-2_117_995_310),
            "servername" | "machinename" | "computernamephysicalnetbios" => t("storine"),
            "collation" => t(COLLATION),
            "lcid" => i(1033),
            "isintegratedsecurityonly"
            | "ishadrenabled"
            | "isclustered"
            | "issingleuser"
            | "isfulltextinstalled"
            | "isxtpsupported"
            | "ispolybaseinstalled" => i(0),
            "sqlcharset" => i(1),
            "sqlsortorder" => i(52),
            _ => N,
        },
        "databasepropertyex" => match text(&a(1)).to_ascii_lowercase().as_str() {
            "collation" => t(COLLATION),
            "status" => t("ONLINE"),
            "updateability" => t("READ_WRITE"),
            "useraccess" => t("MULTI_USER"),
            "recovery" => t("FULL"),
            "version" => i(957),
            "edition" => t("Developer"),
            "serviceobjective" => t("Developer"),
            "isautoclose" | "isautoshrink" | "isfulltextenabled" => i(0),
            _ => N,
        },
        "user_name" | "suser_name" | "original_login" | "session_user" | "system_user" => {
            t(&ex.s.user)
        }
        "user_id" | "suser_id" => i(1),
        "host_name" => t("storine"),
        "app_name" => t(ex
            .s
            .vars
            .get("application_name")
            .cloned()
            .unwrap_or_default()),
        "is_member" | "is_srvrolemember" | "is_rolemember" | "has_dbaccess"
        | "has_perms_by_name" => i(1),
        "permissions" => i(-1),
        "@@microsoftversion" => i(0x1000_03e8),
        "sysutcdatetime" | "sysdatetimeoffset" | "getutcdate" => {
            Datum::Timestamp(ex.now / 100 * 100)
        }
        "dateadd" => {
            if any_null(3) {
                return Ok(Some(N));
            }
            date_add(unit_arg(&a(0))?, a(1).as_i64()?, &a(2))?
        }
        "datediff" | "datediff_big" => {
            if any_null(3) {
                return Ok(Some(N));
            }
            i(date_diff(unit_arg(&a(0))?, &a(1), &a(2))?)
        }
        "datepart" => {
            if any_null(2) {
                return Ok(Some(N));
            }
            let unit = unit_arg(&a(0))?;
            let (ns, _) = instant(&a(1))?;
            let days = ns.div_euclid(NS_PER_DAY);
            let tod = ns.rem_euclid(NS_PER_DAY);
            let (y, m, d) = civil(days);
            i(match unit {
                "year" => y,
                "quarter" => (m - 1) / 3 + 1,
                "month" => m,
                "doy" => days - days_from_civil(y, 1, 1) + 1,
                "day" => d,
                "week" => {
                    let jan1 = days_from_civil(y, 1, 1);
                    let jan1_dow = (jan1 + 4).rem_euclid(7); // 0 = Sunday
                    (days - jan1 + jan1_dow) / 7 + 1
                }
                "dow" => (days + 4).rem_euclid(7) + 1,
                "hour" => tod / 3_600_000_000_000,
                "minute" => tod / 60_000_000_000 % 60,
                "second" => tod / 1_000_000_000 % 60,
                "millisecond" => tod / 1_000_000 % 1000,
                "microsecond" => tod / 1_000 % 1_000_000,
                _ => tod % 1_000_000_000,
            })
        }
        "datename" => {
            if any_null(2) {
                return Ok(Some(N));
            }
            let unit = unit_arg(&a(0))?;
            let (ns, _) = instant(&a(1))?;
            let days = ns.div_euclid(NS_PER_DAY);
            let (_, m, _) = civil(days);
            const MONTHS: [&str; 12] = [
                "January",
                "February",
                "March",
                "April",
                "May",
                "June",
                "July",
                "August",
                "September",
                "October",
                "November",
                "December",
            ];
            const DAYS: [&str; 7] = [
                "Sunday",
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
            ];
            match unit {
                "month" => t(MONTHS[(m - 1) as usize]),
                "dow" => t(DAYS[(days + 4).rem_euclid(7) as usize]),
                _ => t(call(ex, "datepart", args)?.map_or(String::new(), |d| d.as_text())),
            }
        }
        "eomonth" => {
            if any_null(1) {
                return Ok(Some(N));
            }
            let (ns, _) = instant(&a(0))?;
            let (y, m, _) = civil(ns.div_euclid(NS_PER_DAY));
            let total = y * 12 + (m - 1) + args.get(1).map_or(Ok(0), Datum::as_i64)?;
            let (ny, nm) = (total.div_euclid(12), total.rem_euclid(12) + 1);
            Datum::Date(days_from_civil(ny, nm, days_in_month(ny, nm)) as i32)
        }
        "datefromparts" => {
            if any_null(3) {
                return Ok(Some(N));
            }
            Datum::Date(days_from_civil(a(0).as_i64()?, a(1).as_i64()?, a(2).as_i64()?) as i32)
        }
        "datetimefromparts" | "datetime2fromparts" => {
            if any_null(6) {
                return Ok(Some(N));
            }
            let days = days_from_civil(a(0).as_i64()?, a(1).as_i64()?, a(2).as_i64()?);
            let frac = match name {
                "datetimefromparts" => a(6).as_i64().unwrap_or(0) * 1_000_000,
                _ => {
                    let f = a(6).as_i64().unwrap_or(0);
                    let p = a(7).as_i64().unwrap_or(7).clamp(0, 7) as u32;
                    f * 10i64.pow(9 - p)
                }
            };
            Datum::Timestamp(
                days * NS_PER_DAY
                    + a(3).as_i64()? * 3_600_000_000_000
                    + a(4).as_i64()? * 60_000_000_000
                    + a(5).as_i64()? * 1_000_000_000
                    + frac,
            )
        }
        "isdate" => i(i64::from(
            !a(0).is_null() && datum::cast(&a(0), &DataType::Timestamp).is_ok(),
        )),
        "isnumeric" => i(i64::from(
            !a(0).is_null() && datum::to_decimal(&a(0)).is_some(),
        )),
        "charindex" => {
            if any_null(2) {
                return Ok(Some(N));
            }
            let needle = text(&a(0)).to_lowercase();
            let hay: Vec<char> = text(&a(1)).to_lowercase().chars().collect();
            let start = args.get(2).map_or(Ok(1), Datum::as_i64)?.max(1) as usize - 1;
            let n: Vec<char> = needle.chars().collect();
            if n.is_empty() {
                return Ok(Some(i(0)));
            }
            let pos = (start..hay.len().saturating_sub(n.len() - 1))
                .find(|&p| hay[p..p + n.len()] == n[..]);
            i(pos.map_or(0, |p| p as i64 + 1))
        }
        "patindex" => {
            if any_null(2) {
                return Ok(Some(N));
            }
            let pat = text(&a(0));
            let s = text(&a(1));
            let inner = pat.trim_start_matches('%').trim_end_matches('%');
            let chars: Vec<char> = s.chars().collect();
            let found = (0..=chars.len()).find(|&p| {
                let rest: String = chars[p..].iter().collect();
                super::eval::like(
                    &rest.to_lowercase(),
                    &format!("{inner}%").to_lowercase(),
                    None,
                )
            });
            i(found.map_or(0, |p| p as i64 + 1))
        }
        "datalength" => match a(0) {
            Datum::Null => N,
            Datum::Bytes(b) => i(b.len() as i64),
            Datum::Text(s) => i(s.encode_utf16().count() as i64 * 2),
            Datum::Int(_) | Datum::Float(_) => i(8),
            Datum::Bool(_) => i(1),
            other => i(other.as_text().len() as i64),
        },
        "len" => match a(0) {
            Datum::Null => N,
            d => i(d.as_text().trim_end_matches(' ').chars().count() as i64),
        },
        "replicate" => {
            if any_null(2) {
                return Ok(Some(N));
            }
            t(text(&a(0)).repeat(a(1).as_i64()?.max(0) as usize))
        }
        "space" => {
            if any_null(1) {
                return Ok(Some(N));
            }
            t(" ".repeat(a(0).as_i64()?.max(0) as usize))
        }
        "stuff" => {
            if any_null(4) {
                return Ok(Some(N));
            }
            let s: Vec<char> = text(&a(0)).chars().collect();
            let start = a(1).as_i64()?;
            let len = a(2).as_i64()?.max(0) as usize;
            if start < 1 || start as usize > s.len() {
                return Ok(Some(N));
            }
            let at = start as usize - 1;
            let mut out: String = s[..at].iter().collect();
            out.push_str(&text(&a(3)));
            out.extend(s[(at + len).min(s.len())..].iter());
            t(out)
        }
        "quotename" => {
            if any_null(1) {
                return Ok(Some(N));
            }
            let q = args.get(1).map(text).unwrap_or_else(|| "[".into());
            let s = text(&a(0));
            match q.as_str() {
                "'" => t(format!("'{}'", s.replace('\'', "''"))),
                "\"" => t(format!("\"{}\"", s.replace('"', "\"\""))),
                _ => t(format!("[{}]", s.replace(']', "]]"))),
            }
        }
        "unicode" => match text(&a(0)).chars().next() {
            Some(c) if !a(0).is_null() => i(c as i64),
            _ => N,
        },
        "nchar" | "char" => match a(0).as_i64() {
            Ok(n) => char::from_u32(n as u32).map_or(N, |c| t(c.to_string())),
            Err(_) => N,
        },
        "square" => match datum::to_f64(&a(0)) {
            Some(x) => Datum::Float(x * x),
            None => N,
        },
        "ceiling" => return Ok(super::func::scalar(ex, "ceil", args).ok()),
        "choose" => {
            let k = a(0).as_i64().unwrap_or(0);
            if k >= 1 {
                args.get(k as usize).cloned().unwrap_or(N)
            } else {
                N
            }
        }
        "newsequentialid" => return Ok(super::func::scalar(ex, "newid", &[]).ok()),
        "error_number" | "error_severity" | "error_state" | "error_line" | "@@error" => i(0),
        "error_message" | "error_procedure" => N,
        "xact_state" => i(i64::from(ex.s.in_transaction())),
        "isjson" => match a(0) {
            Datum::Null => N,
            d => i(i64::from(super::func::parse_json(&d.as_text()).is_ok())),
        },
        _ => return Ok(None),
    }))
}

/// Result types of the T-SQL functions above (`None`: not one of them).
pub fn return_type(name: &str) -> Option<DataType> {
    Some(match name {
        "schema_name" | "db_name" | "object_name" | "object_schema_name" | "col_name"
        | "object_definition" | "type_name" | "user_name" | "suser_name" | "original_login"
        | "host_name" | "app_name" | "datename" | "replicate" | "space" | "stuff" | "quotename"
        | "nchar" | "serverproperty" | "databasepropertyex" => DataType::Varchar(Some(4000)),
        "odbcscale" | "schema_id" | "db_id" | "object_id" | "objectproperty" | "col_length"
        | "columnproperty" | "type_id" | "datepart" | "charindex" | "patindex" | "datalength"
        | "unicode" | "isdate" | "isnumeric" | "is_member" | "is_srvrolemember" | "user_id"
        | "xact_state" | "error_number" | "isjson" | "len" => DataType::Int,
        "datediff_big" | "ident_current" | "count_big" => DataType::BigInt,
        // COUNT is `int` in SQL Server (COUNT_BIG is the `bigint` one).
        "count" | "datediff" => DataType::Int,
        "dateadd" | "datetimefromparts" | "datetime2fromparts" | "sysutcdatetime"
        | "getutcdate" => DataType::Timestamp,
        "sysdatetimeoffset" => DataType::TimestampTz,
        "eomonth" | "datefromparts" => DataType::Date,
        "square" => DataType::Double,
        "newsequentialid" => DataType::Uuid,
        _ => return None,
    })
}

// ── system stored procedures ──

fn rows(cols: &[(&str, DataType)], rows: Vec<Vec<Datum>>) -> super::Outcome {
    super::Outcome::Rows(super::ResultSet {
        columns: cols
            .iter()
            .map(|(n, ty)| super::OutCol {
                name: (*n).to_owned(),
                ty: ty.clone(),
                table: None,
            })
            .collect(),
        rows,
    })
}

/// Runs T-SQL text (EXEC (sql), sp_executesql) with `vars` bound as session variables;
/// returns the last result.
fn run_sql(
    s: &mut super::Session,
    sql: &str,
    vars: Vec<(String, Datum)>,
) -> SqlResult<super::Outcome> {
    let (stmts, _) = crate::parser::parse(sql, crate::lexer::Dialect::MsSql)?;
    let saved: Vec<(String, Option<Datum>)> = vars
        .into_iter()
        .map(|(k, v)| {
            let k = k.trim_start_matches('@').to_ascii_lowercase();
            let old = s.user_vars.insert(k.clone(), v);
            (k, old)
        })
        .collect();
    let mut last = Ok(super::Outcome::command("EXECUTE"));
    for st in &stmts {
        last = s.execute(st, &[]);
        if last.is_err() {
            break;
        }
    }
    for (k, old) in saved {
        match old {
            Some(v) => s.user_vars.insert(k, v),
            None => s.user_vars.remove(&k),
        };
    }
    last
}

/// `EXEC procedure args`: the system procedures drivers and ORMs call.
pub fn exec_proc(
    s: &mut super::Session,
    name: &str,
    args: &[(Option<String>, crate::ast::Expr)],
    params: &[Datum],
) -> SqlResult<super::Outcome> {
    let mut values = Vec::with_capacity(args.len());
    for (n, e) in args {
        let v = Exec::new(s, params).eval_const(e)?;
        values.push((n.as_ref().map(|n| n.to_ascii_lowercase()), v));
    }
    // An argument by name, else by position.
    let arg = |name: &str, pos: usize| -> Datum {
        values
            .iter()
            .find(|(n, _)| n.as_deref() == Some(name))
            .or_else(|| values.get(pos).filter(|(n, _)| n.is_none()))
            .map_or(N, |(_, v)| v.clone())
    };
    let ok = || Ok(super::Outcome::command("EXECUTE"));
    match name {
        "" => run_sql(s, &text(&arg("", 0)), Vec::new()),
        "sp_executesql" => {
            let sql = text(&arg("stmt", 0));
            let declared = crate::tds::declared_names(&text(&arg("params", 1)));
            let mut vars = Vec::new();
            let mut positional = declared.iter();
            for (n, v) in values.iter().skip(2) {
                match n {
                    Some(n) => vars.push((n.clone(), v.clone())),
                    None => {
                        if let Some(d) = positional.next() {
                            vars.push((d.clone(), v.clone()));
                        }
                    }
                }
            }
            run_sql(s, &sql, vars)
        }
        "sp_helpindex" => {
            let tb = {
                let ex = Exec::new(s, params);
                table_named(&ex, &text(&arg("objname", 0)))?
            };
            let Some(tb) = tb else {
                return Err(SqlError::new(
                    "42P01",
                    format!("The object '{}' does not exist", text(&arg("objname", 0))),
                ));
            };
            let out: Vec<Vec<Datum>> = indexes(&tb)
                .into_iter()
                .map(|(_, name, cols, unique, primary, _)| {
                    let mut desc = if primary { "clustered" } else { "nonclustered" }.to_owned();
                    if unique {
                        desc.push_str(", unique");
                    }
                    if primary {
                        desc.push_str(", primary key");
                    }
                    desc.push_str(" located on PRIMARY");
                    let keys = cols
                        .iter()
                        .map(|c| tb.columns[*c].name.clone())
                        .collect::<Vec<_>>()
                        .join(", ");
                    vec![t(name), t(desc), t(keys)]
                })
                .collect();
            Ok(rows(
                &[
                    ("index_name", T),
                    ("index_description", T),
                    ("index_keys", T),
                ],
                out,
            ))
        }
        "sp_rename" => {
            let obj = text(&arg("objname", 0));
            let new = bare_name(&text(&arg("newname", 1)));
            let kind = text(&arg("objtype", 2)).to_ascii_uppercase();
            let parts: Vec<String> = obj.split('.').map(bare_name).collect();
            let sql = match kind.as_str() {
                "COLUMN" if parts.len() >= 2 => format!(
                    "ALTER TABLE [{}] RENAME COLUMN [{}] TO [{new}]",
                    parts[parts.len() - 2],
                    parts[parts.len() - 1]
                ),
                // Index and constraint names are not part of how rows are stored.
                "INDEX" | "OBJECT" if parts.len() >= 2 => return ok(),
                _ => format!(
                    "ALTER TABLE [{}] RENAME TO [{new}]",
                    parts.last().cloned().unwrap_or_default()
                ),
            };
            run_sql(s, &sql, Vec::new())
        }
        "sp_tables" | "sp_columns" | "sp_columns_100" | "sp_pkeys" | "sp_fkeys"
        | "sp_statistics" | "sp_statistics_100" => {
            let ex = Exec::new(s, params);
            metadata(&ex, name, &|n: &str, pos: usize| arg(n, pos))
        }
        "sp_set_session_context"
        | "sp_reset_connection"
        | "sp_addextendedproperty"
        | "sp_updateextendedproperty"
        | "sp_dropextendedproperty"
        | "sp_tableoption"
        | "sp_releaseapplock"
        | "sp_refreshview"
        | "sp_updatestats"
        | "sp_recompile" => ok(),
        "sp_getapplock" => Ok(rows(&[("", I4)], vec![vec![i(0)]])),
        "sp_server_info" => Ok(rows(
            &[
                ("attribute_id", I4),
                ("attribute_name", T),
                ("attribute_value", T),
            ],
            vec![
                vec![i(1), t("DBMS_NAME"), t("Microsoft SQL Server")],
                vec![
                    i(2),
                    t("DBMS_VER"),
                    t(format!("Microsoft SQL Server 2022 - {PRODUCT_VERSION}")),
                ],
                vec![i(500), t("SYS_SPROC_VERSION"), t("16.00.1000")],
            ],
        )),
        other => Err(SqlError::new(
            "42883",
            format!("Could not find stored procedure '{other}' (not supported by Storine SQL)"),
        )),
    }
}

// ── JDBC / ODBC catalog procedures ──

/// ODBC 3 type codes, as `sp_columns` reports them.
fn odbc_type(name: &str) -> i64 {
    match name {
        "bit" => -7,
        "tinyint" => -6,
        "smallint" => 5,
        "int" => 4,
        "bigint" => -5,
        "decimal" | "money" | "smallmoney" => 3,
        "numeric" => 2,
        "real" => 7,
        "float" => 6,
        "char" => 1,
        "varchar" => 12,
        "text" => -1,
        "nchar" => -8,
        "nvarchar" => -9,
        "ntext" => -10,
        "binary" => -2,
        "varbinary" | "timestamp" => -3,
        "image" => -4,
        "uniqueidentifier" => -11,
        "date" => 91,
        "time" => -154,
        "datetime" | "datetime2" | "smalldatetime" => 93,
        "datetimeoffset" => -155,
        "xml" => -152,
        _ => 12,
    }
}

/// T-SQL LIKE (`%`, `_`, `[x]` escapes) as the catalog procedures use it; NULL matches all.
fn pattern_match(pattern: &Datum, s: &str) -> bool {
    let Datum::Text(p) = pattern else {
        return true;
    };
    let mut lit = String::new();
    let mut chars = p.chars();
    while let Some(c) = chars.next() {
        if c == '[' {
            let inner: String = chars.by_ref().take_while(|&x| x != ']').collect();
            lit.push('\\');
            lit.push_str(&inner);
        } else {
            lit.push(c);
        }
    }
    super::eval::like(&s.to_lowercase(), &lit.to_lowercase(), Some('\\'))
}

/// `sp_tables`, `sp_columns[_100]`, `sp_pkeys`, `sp_fkeys`, `sp_statistics[_100]`: what
/// mssql-jdbc's `DatabaseMetaData` (Hibernate schema validation, tools) reads.
fn metadata(
    ex: &Exec<'_>,
    proc_name: &str,
    arg: &dyn Fn(&str, usize) -> Datum,
) -> SqlResult<super::Outcome> {
    ex.s.engine.refresh_catalog(false)?;
    let tables = ex.s.engine.catalog.all();
    let db = ex.s.database.clone();
    let named = |pat: &Datum| -> Vec<Arc<Table>> {
        tables
            .iter()
            .filter(|t| pattern_match(pat, &t.name))
            .cloned()
            .collect()
    };
    const I2: DataType = DataType::SmallInt;
    const MAXLEN: i64 = 2_147_483_647;
    Ok(match proc_name {
        "sp_tables" => {
            let types = text(&arg("table_type", 3)).to_ascii_uppercase();
            let want = |kind: &str| types.is_empty() || types.contains(kind);
            let pat = arg("table_name", 0);
            let mut out = Vec::new();
            if want("TABLE") {
                for tb in named(&pat) {
                    out.push(vec![t(&db), t("dbo"), t(&tb.name), t("TABLE"), N]);
                }
            }
            if want("VIEW") {
                for v in ex.s.engine.view_names()? {
                    if pattern_match(&pat, &v) {
                        out.push(vec![t(&db), t("dbo"), t(v), t("VIEW"), N]);
                    }
                }
            }
            rows(
                &[
                    ("TABLE_QUALIFIER", T),
                    ("TABLE_OWNER", T),
                    ("TABLE_NAME", T),
                    ("TABLE_TYPE", T),
                    ("REMARKS", T),
                ],
                out,
            )
        }
        "sp_columns" | "sp_columns_100" => {
            let col_pat = arg("column_name", 3);
            let mut out = Vec::new();
            for tb in named(&arg("table_name", 0)) {
                for (k, c) in tb.columns.iter().enumerate() {
                    if !pattern_match(&col_pat, &c.name) {
                        continue;
                    }
                    let (ty, id, len, p, sc) = column_type(c);
                    let wide = matches!(ty.as_str(), "nchar" | "nvarchar" | "ntext");
                    let chars_ty = wide || matches!(ty.as_str(), "char" | "varchar" | "text");
                    let bytes_ty = matches!(ty.as_str(), "binary" | "varbinary" | "image");
                    let length = if len < 0 { MAXLEN } else { len };
                    let precision = if chars_ty {
                        if len < 0 {
                            MAXLEN / if wide { 2 } else { 1 }
                        } else if wide {
                            len / 2
                        } else {
                            len
                        }
                    } else if bytes_ty {
                        length
                    } else {
                        p
                    };
                    let numeric = !(chars_ty
                        || bytes_ty
                        || matches!(ty.as_str(), "uniqueidentifier" | "xml"));
                    let code = odbc_type(&ty);
                    let (sql_type, sub) = match code {
                        91 => (9, Some(1)),
                        93 => (9, Some(3)),
                        other => (other, None),
                    };
                    let type_name = if c.auto_increment {
                        format!("{ty} identity")
                    } else {
                        ty.clone()
                    };
                    out.push(vec![
                        t(&db),
                        t("dbo"),
                        t(&tb.name),
                        t(&c.name),
                        i(code),
                        t(type_name),
                        i(precision),
                        i(length),
                        if numeric { i(sc) } else { N },
                        if numeric && !matches!(code, 91 | 93 | -154 | -155) {
                            i(10)
                        } else {
                            N
                        },
                        i(i64::from(!c.not_null)),
                        N,
                        c.default.as_ref().map_or(N, |d| t(default_sql(d))),
                        i(sql_type),
                        sub.map_or(N, i),
                        if chars_ty || bytes_ty { i(length) } else { N },
                        i(k as i64 + 1),
                        t(if c.not_null { "NO" } else { "YES" }),
                        i(0),
                        i(0),
                        i(0),
                        i(i64::from(c.auto_increment)),
                        N,
                        N,
                        N,
                        N,
                        N,
                        N,
                        i(id),
                    ]);
                }
            }
            rows(
                &[
                    ("TABLE_QUALIFIER", T),
                    ("TABLE_OWNER", T),
                    ("TABLE_NAME", T),
                    ("COLUMN_NAME", T),
                    ("DATA_TYPE", I2),
                    ("TYPE_NAME", T),
                    ("PRECISION", I4),
                    ("LENGTH", I4),
                    ("SCALE", I2),
                    ("RADIX", I2),
                    ("NULLABLE", I2),
                    ("REMARKS", T),
                    ("COLUMN_DEF", T),
                    ("SQL_DATA_TYPE", I2),
                    ("SQL_DATETIME_SUB", I2),
                    ("CHAR_OCTET_LENGTH", I4),
                    ("ORDINAL_POSITION", I4),
                    ("IS_NULLABLE", T),
                    ("SS_IS_SPARSE", I2),
                    ("SS_IS_COLUMN_SET", I2),
                    ("SS_IS_COMPUTED", I2),
                    ("SS_IS_IDENTITY", I2),
                    ("SS_UDT_CATALOG_NAME", T),
                    ("SS_UDT_SCHEMA_NAME", T),
                    ("SS_UDT_ASSEMBLY_TYPE_NAME", T),
                    ("SS_XML_SCHEMACOLLECTION_CATALOG_NAME", T),
                    ("SS_XML_SCHEMACOLLECTION_SCHEMA_NAME", T),
                    ("SS_XML_SCHEMACOLLECTION_NAME", T),
                    ("SS_DATA_TYPE", I2),
                ],
                out,
            )
        }
        "sp_pkeys" => {
            let mut out = Vec::new();
            for tb in named(&arg("table_name", 0)) {
                for (k, c) in tb.primary_key.iter().enumerate() {
                    out.push(vec![
                        t(&db),
                        t("dbo"),
                        t(&tb.name),
                        t(&tb.columns[*c].name),
                        i(k as i64 + 1),
                        t(pk_name(&tb)),
                    ]);
                }
            }
            rows(
                &[
                    ("TABLE_QUALIFIER", T),
                    ("TABLE_OWNER", T),
                    ("TABLE_NAME", T),
                    ("COLUMN_NAME", T),
                    ("KEY_SEQ", I2),
                    ("PK_NAME", T),
                ],
                out,
            )
        }
        "sp_fkeys" => {
            // @pktable_name (referenced) and/or @fktable_name (referencing).
            let pk_pat = arg("pktable_name", 0);
            let fk_pat = arg("fktable_name", 3);
            let rule = |a: RefAction| match a {
                RefAction::Cascade => 0,
                RefAction::SetNull => 2,
                RefAction::SetDefault => 4,
                RefAction::NoAction | RefAction::Restrict => 1,
            };
            let mut out = Vec::new();
            for tb in tables.iter().filter(|x| pattern_match(&fk_pat, &x.name)) {
                for f in tb
                    .foreign_keys
                    .iter()
                    .filter(|f| pattern_match(&pk_pat, &f.ref_table))
                {
                    let target = tables
                        .iter()
                        .find(|x| x.name.eq_ignore_ascii_case(&f.ref_table));
                    let ref_cols: Vec<String> = match target {
                        Some(rt) if f.ref_columns.is_empty() => rt
                            .primary_key
                            .iter()
                            .map(|c| rt.columns[*c].name.clone())
                            .collect(),
                        _ => f.ref_columns.clone(),
                    };
                    let target_name = target.map_or(f.ref_table.clone(), |x| x.name.clone());
                    for (k, c) in f.columns.iter().enumerate() {
                        out.push(vec![
                            t(&db),
                            t("dbo"),
                            t(&target_name),
                            t(ref_cols.get(k).cloned().unwrap_or_default()),
                            t(&db),
                            t("dbo"),
                            t(&tb.name),
                            t(&tb.columns[*c].name),
                            i(k as i64 + 1),
                            i(rule(f.on_update)),
                            i(rule(f.on_delete)),
                            t(&f.name),
                            target.map_or(N, |x| t(pk_name(x))),
                            i(7),
                        ]);
                    }
                }
            }
            rows(
                &[
                    ("PKTABLE_QUALIFIER", T),
                    ("PKTABLE_OWNER", T),
                    ("PKTABLE_NAME", T),
                    ("PKCOLUMN_NAME", T),
                    ("FKTABLE_QUALIFIER", T),
                    ("FKTABLE_OWNER", T),
                    ("FKTABLE_NAME", T),
                    ("FKCOLUMN_NAME", T),
                    ("KEY_SEQ", I2),
                    ("UPDATE_RULE", I2),
                    ("DELETE_RULE", I2),
                    ("FK_NAME", T),
                    ("PK_NAME", T),
                    ("DEFERRABILITY", I2),
                ],
                out,
            )
        }
        _ => {
            // sp_statistics: one row per index column (TYPE 1 clustered, 3 other).
            let mut out = Vec::new();
            for tb in named(&arg("table_name", 0)) {
                for (_, name, cols, unique, primary, _) in indexes(&tb) {
                    for (k, c) in cols.iter().enumerate() {
                        out.push(vec![
                            t(&db),
                            t("dbo"),
                            t(&tb.name),
                            i(i64::from(!unique)),
                            t(&tb.name),
                            t(&name),
                            i(if primary { 1 } else { 3 }),
                            i(k as i64 + 1),
                            t(&tb.columns[*c].name),
                            t("A"),
                            N,
                            N,
                            N,
                        ]);
                    }
                }
            }
            rows(
                &[
                    ("TABLE_QUALIFIER", T),
                    ("TABLE_OWNER", T),
                    ("TABLE_NAME", T),
                    ("NON_UNIQUE", I2),
                    ("INDEX_QUALIFIER", T),
                    ("INDEX_NAME", T),
                    ("TYPE", I2),
                    ("SEQ_IN_INDEX", I2),
                    ("COLUMN_NAME", T),
                    ("COLLATION", T),
                    ("CARDINALITY", I4),
                    ("PAGES", I4),
                    ("FILTER_CONDITION", T),
                ],
                out,
            )
        }
    })
}
