//! System catalogs (`pg_catalog`, `information_schema`, MySQL's `SHOW`) generated from the
//! table schemas, so drivers, ORMs and admin tools can introspect the database.

use crate::ast::{DataType, Show};
use crate::catalog::{action_name, Column, Table};
use crate::datum::{type_sql, Datum};
use crate::error::{SqlError, SqlResult};
use crate::exec::eval::Exec;
use crate::exec::select::{ColMeta, Rel};
use crate::exec::{OutCol, Outcome, ResultSet, Session};
use crate::lexer::Dialect;

pub const NS_PG_CATALOG: i64 = 11;
pub const NS_PUBLIC: i64 = 2200;
pub const NS_INFORMATION_SCHEMA: i64 = 13_183;

/// PostgreSQL type OIDs.
pub fn type_oid(ty: &DataType) -> u32 {
    match ty {
        DataType::Vector(_) => crate::vector::VECTOR_OID,
        DataType::Array(inner) if matches!(**inner, DataType::Vector(_)) => {
            crate::vector::VECTOR_ARRAY_OID
        }
        DataType::Bool => 16,
        DataType::Bytes => 17,
        DataType::BigInt => 20,
        DataType::SmallInt => 21,
        DataType::Int => 23,
        DataType::Text | DataType::Enum(_) => 25,
        DataType::Name => 19,
        DataType::PgChar => 18,
        DataType::Json => 114,
        DataType::Jsonb => 3802,
        DataType::Real => 700,
        DataType::Double => 701,
        DataType::Char(_) => 1042,
        DataType::Varchar(_) => 1043,
        DataType::Date => 1082,
        DataType::Time => 1083,
        DataType::Timestamp => 1114,
        DataType::TimestampTz => 1184,
        DataType::Interval => 1186,
        DataType::Numeric(_) => 1700,
        DataType::Uuid => 2950,
        DataType::Array(inner) => match **inner {
            DataType::Bool => 1000,
            DataType::Bytes => 1001,
            DataType::SmallInt => 1005,
            DataType::Int => 1007,
            DataType::Text | DataType::Enum(_) => 1009,
            DataType::Name => 1003,
            DataType::PgChar => 1002,
            DataType::Varchar(_) => 1015,
            DataType::BigInt => 1016,
            DataType::Real => 1021,
            DataType::Double => 1022,
            DataType::Timestamp => 1115,
            DataType::Date => 1182,
            DataType::TimestampTz => 1185,
            DataType::Numeric(_) => 1231,
            DataType::Uuid => 2951,
            DataType::Json => 199,
            DataType::Jsonb => 3807,
            _ => 1009,
        },
    }
}

/// `atttypmod` of a column (`-1`: none).
pub fn type_mod(ty: &DataType) -> i32 {
    match ty {
        DataType::Varchar(Some(n)) | DataType::Char(Some(n)) => (*n as i32).saturating_add(4),
        DataType::Numeric(Some((p, s))) => ((i32::from(*p) << 16) | i32::from(*s)) + 4,
        _ => -1,
    }
}

/// `(oid, name, length, category, array oid, element oid)` of every type the catalogs list.
const PG_TYPES: &[(u32, &str, i32, &str, u32, u32)] = &[
    (16, "bool", 1, "B", 1000, 0),
    (17, "bytea", -1, "U", 1001, 0),
    (18, "char", 1, "S", 1002, 0),
    (19, "name", 64, "S", 1003, 0),
    (20, "int8", 8, "N", 1016, 0),
    (21, "int2", 2, "N", 1005, 0),
    (23, "int4", 4, "N", 1007, 0),
    (25, "text", -1, "S", 1009, 0),
    (26, "oid", 4, "N", 1028, 0),
    (114, "json", -1, "U", 199, 0),
    (700, "float4", 4, "N", 1021, 0),
    (701, "float8", 8, "N", 1022, 0),
    (705, "unknown", -2, "X", 0, 0),
    (1042, "bpchar", -1, "S", 1014, 0),
    (1043, "varchar", -1, "S", 1015, 0),
    (1082, "date", 4, "D", 1182, 0),
    (1083, "time", 8, "D", 1183, 0),
    (1114, "timestamp", 8, "D", 1115, 0),
    (1184, "timestamptz", 8, "D", 1185, 0),
    (1186, "interval", 16, "T", 1187, 0),
    (1700, "numeric", -1, "N", 1231, 0),
    (2950, "uuid", 16, "U", 2951, 0),
    (3802, "jsonb", -1, "U", 3807, 0),
    (2205, "regclass", 4, "N", 2210, 0),
    (2206, "regtype", 4, "N", 2211, 0),
    (1003, "_name", -1, "A", 0, 19),
    (1000, "_bool", -1, "A", 0, 16),
    (1001, "_bytea", -1, "A", 0, 17),
    (1005, "_int2", -1, "A", 0, 21),
    (1007, "_int4", -1, "A", 0, 23),
    (1009, "_text", -1, "A", 0, 25),
    (1014, "_bpchar", -1, "A", 0, 1042),
    (1015, "_varchar", -1, "A", 0, 1043),
    (1016, "_int8", -1, "A", 0, 20),
    (1021, "_float4", -1, "A", 0, 700),
    (1022, "_float8", -1, "A", 0, 701),
    (1115, "_timestamp", -1, "A", 0, 1114),
    (1182, "_date", -1, "A", 0, 1082),
    (1183, "_time", -1, "A", 0, 1083),
    (1185, "_timestamptz", -1, "A", 0, 1184),
    (1231, "_numeric", -1, "A", 0, 1700),
    (2951, "_uuid", -1, "A", 0, 2950),
    (199, "_json", -1, "A", 0, 114),
    (3807, "_jsonb", -1, "A", 0, 3802),
    (1560, "bit", -1, "V", 1561, 0),
    (1561, "_bit", -1, "A", 0, 1560),
    // pgvector (its OIDs are assigned by CREATE EXTENSION; clients look them up by name).
    (
        crate::vector::VECTOR_OID,
        "vector",
        -1,
        "U",
        crate::vector::VECTOR_ARRAY_OID,
        0,
    ),
    (
        crate::vector::VECTOR_ARRAY_OID,
        "_vector",
        -1,
        "A",
        0,
        crate::vector::VECTOR_OID,
    ),
];

/// OID of a catalog type by its `pg_type.typname` (`_vector`, `int4`, ...).
pub fn type_oid_by_name(name: &str) -> Option<u32> {
    let name = name.trim().trim_matches('"').to_ascii_lowercase();
    let name = name
        .strip_prefix("pg_catalog.")
        .or_else(|| name.strip_prefix("public."))
        .unwrap_or(&name);
    PG_TYPES.iter().find(|t| t.1 == name).map(|t| t.0)
}

fn fnv(s: &str) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for b in s.bytes() {
        h ^= u32::from(b);
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

/// A stable OID for a table (`16384` and up, like user objects in PostgreSQL).
pub fn table_oid(name: &str) -> i64 {
    16_384 + i64::from(fnv(&format!("t:{}", name.to_ascii_lowercase())) % 50_000_000)
}

fn object_oid(kind: &str, table: &str, name: &str) -> i64 {
    100_000_000
        + i64::from(
            fnv(&format!(
                "{kind}:{}:{}",
                table.to_ascii_lowercase(),
                name.to_ascii_lowercase()
            )) % 1_000_000_000,
        )
}

fn table_by_oid(ex: &Exec<'_>, oid: i64) -> Option<std::sync::Arc<Table>> {
    ex.s.engine
        .catalog
        .all()
        .into_iter()
        .find(|t| table_oid(&t.name) == oid)
}

/// True for names that address a system catalog.
pub fn is_system(schema: Option<&str>, name: &str) -> bool {
    match schema.map(str::to_ascii_lowercase).as_deref() {
        Some("information_schema" | "pg_catalog" | "mysql" | "performance_schema" | "sys") => true,
        Some(_) => false,
        None => {
            let n = name.to_ascii_lowercase();
            n.starts_with("pg_") || n == "dual" || n == "sqlite_master" || n == "sqlite_schema"
        }
    }
}

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

use DataType::{
    BigInt as I8, Bool as B, Int as I4, Name as NM, PgChar as CH, SmallInt as I2, Text as T,
};

fn int2vector(cols: &[usize]) -> Datum {
    t(cols
        .iter()
        .map(|c| (c + 1).to_string())
        .collect::<Vec<_>>()
        .join(" "))
}

fn int_array(cols: &[usize]) -> Datum {
    Datum::Array(cols.iter().map(|c| i(*c as i64 + 1)).collect())
}

/// A system table by name, or `None` if it is not one.
pub fn table(ex: &Exec<'_>, schema: Option<&str>, name: &str) -> SqlResult<Option<Rel>> {
    if !is_system(schema, name) {
        return Ok(None);
    }
    ex.s.engine.refresh_catalog(false)?;
    let n = name.to_ascii_lowercase();
    let sch = schema.map(str::to_ascii_lowercase);
    let tables = ex.s.engine.catalog.all();
    let mysql = ex.dialect() == Dialect::MySql;
    let db = ex.s.database.clone();
    let mssql = ex.dialect() == Dialect::MsSql;
    let schema_name = if mysql {
        db.clone()
    } else if mssql {
        "dbo".to_owned()
    } else {
        "public".to_owned()
    };
    let rel = match (sch.as_deref(), n.as_str()) {
        (None, "dual") => Rel {
            cols: Vec::new(),
            rows: vec![Vec::new()],
        },
        (None, "sqlite_master" | "sqlite_schema") => {
            let mut bd = Builder::new(&[
                ("type", T),
                ("name", T),
                ("tbl_name", T),
                ("rootpage", I8),
                ("sql", T),
            ]);
            for tb in &tables {
                bd.row(vec![
                    t("table"),
                    t(&tb.name),
                    t(&tb.name),
                    i(0),
                    t(create_table_sql(tb, Dialect::Sqlite)),
                ]);
            }
            bd.done()
        }
        (Some("information_schema"), _) => {
            information_schema(ex, &n, &tables, &schema_name, mysql)?
        }
        (Some("sys"), _) if mssql => super::mssql::sys_catalog(ex, &n, &tables)?,
        (Some("mysql" | "performance_schema" | "sys"), _) => Rel::default(),
        _ => pg_catalog(ex, &n, &tables)?,
    };
    Ok(Some(rel))
}

fn pg_catalog(ex: &Exec<'_>, n: &str, tables: &[std::sync::Arc<Table>]) -> SqlResult<Rel> {
    let user = ex.s.user.clone();
    Ok(match n {
        "pg_namespace" => {
            let mut bd = Builder::new(&[
                ("oid", I8),
                ("nspname", NM),
                ("nspowner", I8),
                ("nspacl", T),
            ]);
            for (oid, name) in [
                (NS_PG_CATALOG, "pg_catalog"),
                (NS_PUBLIC, "public"),
                (NS_INFORMATION_SCHEMA, "information_schema"),
            ] {
                bd.row(vec![i(oid), t(name), i(10), N]);
            }
            bd.done()
        }
        "pg_class" => {
            let mut bd = Builder::new(&[
                ("oid", I8),
                ("relname", NM),
                ("relnamespace", I8),
                ("reltype", I8),
                ("reloftype", I8),
                ("relowner", I8),
                ("relam", I8),
                ("relfilenode", I8),
                ("reltablespace", I8),
                ("relpages", I4),
                ("reltuples", DataType::Real),
                ("relallvisible", I4),
                ("reltoastrelid", I8),
                ("relhasindex", B),
                ("relisshared", B),
                ("relpersistence", CH),
                ("relkind", CH),
                ("relnatts", I2),
                ("relchecks", I2),
                ("relhasrules", B),
                ("relhastriggers", B),
                ("relhassubclass", B),
                ("relrowsecurity", B),
                ("relforcerowsecurity", B),
                ("relispopulated", B),
                ("relreplident", CH),
                ("relispartition", B),
                ("relacl", T),
                ("reloptions", T),
                ("relpartbound", T),
            ]);
            for tb in tables {
                let oid = table_oid(&tb.name);
                let has_index =
                    !tb.primary_key.is_empty() || !tb.uniques.is_empty() || !tb.indexes.is_empty();
                bd.row(vec![
                    i(oid),
                    t(&tb.name),
                    i(NS_PUBLIC),
                    i(oid + 1),
                    i(0),
                    i(10),
                    i(2),
                    i(oid),
                    i(0),
                    i(1),
                    Datum::Float(-1.0),
                    i(0),
                    i(0),
                    b(has_index),
                    b(false),
                    t("p"),
                    t("r"),
                    i(tb.columns.len() as i64),
                    i(tb.checks.len() as i64),
                    b(false),
                    b(false),
                    b(false),
                    b(false),
                    b(false),
                    b(true),
                    t("d"),
                    b(false),
                    N,
                    N,
                    N,
                ]);
                for (idx_name, cols) in index_list(tb) {
                    let ioid = object_oid("i", &tb.name, &idx_name);
                    bd.row(vec![
                        i(ioid),
                        t(idx_name),
                        i(NS_PUBLIC),
                        i(0),
                        i(0),
                        i(10),
                        i(403),
                        i(ioid),
                        i(0),
                        i(1),
                        Datum::Float(0.0),
                        i(0),
                        i(0),
                        b(false),
                        b(false),
                        t("p"),
                        t("i"),
                        i(cols.len() as i64),
                        i(0),
                        b(false),
                        b(false),
                        b(false),
                        b(false),
                        b(false),
                        b(true),
                        t("n"),
                        b(false),
                        N,
                        N,
                        N,
                    ]);
                }
            }
            // Views (relkind 'v').
            for v in ex.s.engine.view_names()? {
                let oid = table_oid(&v);
                let mut row = vec![
                    i(oid),
                    t(&v),
                    i(NS_PUBLIC),
                    i(oid + 1),
                    i(0),
                    i(10),
                    i(0),
                    i(0),
                    i(0),
                    i(0),
                    Datum::Float(-1.0),
                    i(0),
                    i(0),
                    b(false),
                    b(false),
                    t("p"),
                    t("v"),
                    i(0),
                    i(0),
                    b(true),
                    b(false),
                    b(false),
                    b(false),
                    b(false),
                    b(true),
                    t("n"),
                    b(false),
                    N,
                    N,
                    N,
                ];
                row.truncate(30);
                bd.row(row);
            }
            bd.done()
        }
        "pg_attribute" => {
            let mut bd = Builder::new(&[
                ("attrelid", I8),
                ("attname", NM),
                ("atttypid", I8),
                ("attstattarget", I4),
                ("attlen", I2),
                ("attnum", I2),
                ("attndims", I4),
                ("attcacheoff", I4),
                ("atttypmod", I4),
                ("attbyval", B),
                ("attstorage", T),
                ("attalign", T),
                ("attnotnull", B),
                ("atthasdef", B),
                ("atthasmissing", B),
                ("attidentity", CH),
                ("attgenerated", CH),
                ("attisdropped", B),
                ("attislocal", B),
                ("attinhcount", I4),
                ("attcollation", I8),
                ("attacl", T),
                ("attoptions", T),
                ("attfdwoptions", T),
                ("attmissingval", T),
            ]);
            for tb in tables {
                let oid = table_oid(&tb.name);
                for (k, c) in tb.columns.iter().enumerate() {
                    let len = match c.ty {
                        DataType::Bool => 1,
                        DataType::SmallInt => 2,
                        DataType::Int | DataType::Real | DataType::Date => 4,
                        DataType::BigInt
                        | DataType::Double
                        | DataType::Timestamp
                        | DataType::TimestampTz
                        | DataType::Time => 8,
                        DataType::Uuid | DataType::Interval => 16,
                        _ => -1,
                    };
                    bd.row(vec![
                        i(oid),
                        t(&c.name),
                        i(i64::from(type_oid(&c.ty))),
                        i(-1),
                        i(len),
                        i(k as i64 + 1),
                        i(i64::from(matches!(c.ty, DataType::Array(_)))),
                        i(-1),
                        i(i64::from(c.type_mod())),
                        b(len > 0 && len <= 8),
                        t(if len > 0 { "p" } else { "x" }),
                        t("i"),
                        b(c.not_null),
                        b(c.default.is_some() || c.auto_increment),
                        b(c.missing.is_some()),
                        t(if c.auto_increment { "d" } else { "" }),
                        t(""),
                        b(false),
                        b(true),
                        i(0),
                        i(
                            if matches!(
                                c.ty,
                                DataType::Text | DataType::Varchar(_) | DataType::Char(_)
                            ) {
                                100
                            } else {
                                0
                            },
                        ),
                        N,
                        N,
                        N,
                        N,
                    ]);
                }
            }
            bd.done()
        }
        "pg_type" => {
            let mut bd = Builder::new(&[
                ("oid", I8),
                ("typname", NM),
                ("typnamespace", I8),
                ("typowner", I8),
                ("typlen", I2),
                ("typbyval", B),
                ("typtype", CH),
                ("typcategory", CH),
                ("typispreferred", B),
                ("typisdefined", B),
                ("typdelim", CH),
                ("typrelid", I8),
                ("typsubscript", T),
                ("typelem", I8),
                ("typarray", I8),
                ("typinput", T),
                ("typoutput", T),
                ("typreceive", T),
                ("typsend", T),
                ("typmodin", T),
                ("typmodout", T),
                ("typanalyze", T),
                ("typalign", CH),
                ("typstorage", CH),
                ("typnotnull", B),
                ("typbasetype", I8),
                ("typtypmod", I4),
                ("typndims", I4),
                ("typcollation", I8),
                ("typdefaultbin", T),
                ("typdefault", T),
                ("typacl", T),
            ]);
            for (oid, name, len, cat, arr, elem) in PG_TYPES {
                let base = name.trim_start_matches('_');
                bd.row(vec![
                    i(i64::from(*oid)),
                    t(*name),
                    i(NS_PG_CATALOG),
                    i(10),
                    i(i64::from(*len)),
                    b(*len > 0 && *len <= 8),
                    t("b"),
                    t(*cat),
                    b(false),
                    b(true),
                    t(","),
                    i(0),
                    t("-"),
                    i(i64::from(*elem)),
                    i(i64::from(*arr)),
                    t(format!("{base}in")),
                    t(format!("{base}out")),
                    t(format!("{base}recv")),
                    t(format!("{base}send")),
                    t("-"),
                    t("-"),
                    t("-"),
                    t("i"),
                    t("p"),
                    b(false),
                    i(0),
                    i(-1),
                    i(0),
                    i(0),
                    N,
                    N,
                    N,
                ]);
            }
            // Each table has a composite row type.
            for tb in tables {
                let oid = table_oid(&tb.name);
                let mut row = vec![
                    i(oid + 1),
                    t(&tb.name),
                    i(NS_PUBLIC),
                    i(10),
                    i(-1),
                    b(false),
                    t("c"),
                    t("C"),
                    b(false),
                    b(true),
                    t(","),
                    i(oid),
                    t("-"),
                    i(0),
                    i(0),
                    t("record_in"),
                    t("record_out"),
                    t("record_recv"),
                    t("record_send"),
                    t("-"),
                    t("-"),
                    t("-"),
                    t("d"),
                    t("x"),
                    b(false),
                    i(0),
                    i(-1),
                    i(0),
                    i(0),
                    N,
                    N,
                    N,
                ];
                row.truncate(32);
                bd.row(row);
            }
            bd.done()
        }
        "pg_index" => {
            let mut bd = Builder::new(&[
                ("indexrelid", I8),
                ("indrelid", I8),
                ("indnatts", I2),
                ("indnkeyatts", I2),
                ("indisunique", B),
                ("indnullsnotdistinct", B),
                ("indisprimary", B),
                ("indisexclusion", B),
                ("indimmediate", B),
                ("indisclustered", B),
                ("indisvalid", B),
                ("indcheckxmin", B),
                ("indisready", B),
                ("indislive", B),
                ("indisreplident", B),
                ("indkey", T),
                ("indcollation", T),
                ("indclass", T),
                ("indoption", T),
                ("indexprs", T),
                ("indpred", T),
            ]);
            for tb in tables {
                for (k, (name, cols)) in index_list(tb).into_iter().enumerate() {
                    let primary = k == 0 && !tb.primary_key.is_empty();
                    let unique = primary || tb.uniques.iter().any(|u| u.name == name);
                    bd.row(vec![
                        i(object_oid("i", &tb.name, &name)),
                        i(table_oid(&tb.name)),
                        i(cols.len() as i64),
                        i(cols.len() as i64),
                        b(unique),
                        b(false),
                        b(primary),
                        b(false),
                        b(true),
                        b(false),
                        b(true),
                        b(false),
                        b(true),
                        b(true),
                        b(false),
                        int2vector(&cols),
                        t("0"),
                        t("0"),
                        t("0"),
                        N,
                        N,
                    ]);
                }
            }
            bd.done()
        }
        "pg_constraint" => {
            let mut bd = Builder::new(&[
                ("oid", I8),
                ("conname", NM),
                ("connamespace", I8),
                ("contype", CH),
                ("condeferrable", B),
                ("condeferred", B),
                ("convalidated", B),
                ("conrelid", I8),
                ("contypid", I8),
                ("conindid", I8),
                ("conparentid", I8),
                ("confrelid", I8),
                ("confupdtype", CH),
                ("confdeltype", CH),
                ("confmatchtype", CH),
                ("conislocal", B),
                ("coninhcount", I4),
                ("connoinherit", B),
                ("conkey", DataType::Array(Box::new(I2))),
                ("confkey", DataType::Array(Box::new(I2))),
                ("conbin", T),
            ]);
            let act = |a: crate::ast::RefAction| {
                t(match a {
                    crate::ast::RefAction::NoAction => "a",
                    crate::ast::RefAction::Restrict => "r",
                    crate::ast::RefAction::Cascade => "c",
                    crate::ast::RefAction::SetNull => "n",
                    crate::ast::RefAction::SetDefault => "d",
                })
            };
            for tb in tables {
                let toid = table_oid(&tb.name);
                if !tb.primary_key.is_empty() {
                    let name = format!("{}_pkey", tb.name);
                    bd.row(vec![
                        i(object_oid("c", &tb.name, &name)),
                        t(&name),
                        i(NS_PUBLIC),
                        t("p"),
                        b(false),
                        b(false),
                        b(true),
                        i(toid),
                        i(0),
                        i(object_oid("i", &tb.name, &name)),
                        i(0),
                        i(0),
                        t(" "),
                        t(" "),
                        t(" "),
                        b(true),
                        i(0),
                        b(true),
                        int_array(&tb.primary_key),
                        N,
                        N,
                    ]);
                }
                for u in &tb.uniques {
                    bd.row(vec![
                        i(object_oid("c", &tb.name, &u.name)),
                        t(&u.name),
                        i(NS_PUBLIC),
                        t("u"),
                        b(false),
                        b(false),
                        b(true),
                        i(toid),
                        i(0),
                        i(object_oid("i", &tb.name, &u.name)),
                        i(0),
                        i(0),
                        t(" "),
                        t(" "),
                        t(" "),
                        b(true),
                        i(0),
                        b(true),
                        int_array(&u.columns),
                        N,
                        N,
                    ]);
                }
                for f in &tb.foreign_keys {
                    let parent = ex.s.engine.catalog.get(&f.ref_table);
                    let fcols: Vec<usize> = match &parent {
                        Some(p) if f.ref_columns.is_empty() => p.primary_key.clone(),
                        Some(p) => f
                            .ref_columns
                            .iter()
                            .filter_map(|c| p.column_index(c))
                            .collect(),
                        None => Vec::new(),
                    };
                    bd.row(vec![
                        i(object_oid("c", &tb.name, &f.name)),
                        t(&f.name),
                        i(NS_PUBLIC),
                        t("f"),
                        b(false),
                        b(false),
                        b(true),
                        i(toid),
                        i(0),
                        i(0),
                        i(0),
                        i(table_oid(&f.ref_table)),
                        act(f.on_update),
                        act(f.on_delete),
                        t("s"),
                        b(true),
                        i(0),
                        b(true),
                        int_array(&f.columns),
                        int_array(&fcols),
                        N,
                    ]);
                }
                for c in &tb.checks {
                    bd.row(vec![
                        i(object_oid("c", &tb.name, &c.name)),
                        t(&c.name),
                        i(NS_PUBLIC),
                        t("c"),
                        b(false),
                        b(false),
                        b(true),
                        i(toid),
                        i(0),
                        i(0),
                        i(0),
                        i(0),
                        t(" "),
                        t(" "),
                        t(" "),
                        b(true),
                        i(0),
                        b(false),
                        Datum::Array(Vec::new()),
                        N,
                        t(c.expr.to_string()),
                    ]);
                }
            }
            bd.done()
        }
        "pg_attrdef" => {
            let mut bd = Builder::new(&[("oid", I8), ("adrelid", I8), ("adnum", I2), ("adbin", T)]);
            for tb in tables {
                for (k, c) in tb.columns.iter().enumerate() {
                    let def = match (&c.default, c.auto_increment) {
                        (Some(d), _) => Some(d.to_string()),
                        (None, true) => {
                            Some(format!("nextval('{}_{}_seq'::regclass)", tb.name, c.name))
                        }
                        _ => None,
                    };
                    if let Some(d) = def {
                        bd.row(vec![
                            i(object_oid("d", &tb.name, &c.name)),
                            i(table_oid(&tb.name)),
                            i(k as i64 + 1),
                            t(d),
                        ]);
                    }
                }
            }
            bd.done()
        }
        "pg_tables" => {
            let mut bd = Builder::new(&[
                ("schemaname", T),
                ("tablename", T),
                ("tableowner", T),
                ("tablespace", T),
                ("hasindexes", B),
                ("hasrules", B),
                ("hastriggers", B),
                ("rowsecurity", B),
            ]);
            for tb in tables {
                bd.row(vec![
                    t("public"),
                    t(&tb.name),
                    t(&user),
                    N,
                    b(true),
                    b(false),
                    b(false),
                    b(false),
                ]);
            }
            bd.done()
        }
        "pg_indexes" => {
            let mut bd = Builder::new(&[
                ("schemaname", T),
                ("tablename", T),
                ("indexname", T),
                ("tablespace", T),
                ("indexdef", T),
            ]);
            for tb in tables {
                for (name, cols) in index_list(tb) {
                    let unique =
                        name.ends_with("_pkey") || tb.uniques.iter().any(|u| u.name == name);
                    bd.row(vec![
                        t("public"),
                        t(&tb.name),
                        t(&name),
                        N,
                        t(index_sql(tb, &name, &cols, unique)),
                    ]);
                }
            }
            bd.done()
        }
        "pg_database" => {
            let mut bd = Builder::new(&[
                ("oid", I8),
                ("datname", T),
                ("datdba", I8),
                ("encoding", I4),
                ("datlocprovider", T),
                ("datistemplate", B),
                ("datallowconn", B),
                ("datconnlimit", I4),
                ("datfrozenxid", I8),
                ("datminmxid", I8),
                ("dattablespace", I8),
                ("datcollate", T),
                ("datctype", T),
                ("datacl", T),
            ]);
            for (k, name) in database_list(ex.s)?.iter().enumerate() {
                bd.row(vec![
                    i(16_384 + k as i64),
                    t(name),
                    i(10),
                    i(6),
                    t("c"),
                    b(false),
                    b(true),
                    i(-1),
                    i(0),
                    i(1),
                    i(1663),
                    t("C"),
                    t("C"),
                    N,
                ]);
            }
            bd.done()
        }
        "pg_roles" | "pg_user" | "pg_authid" | "pg_shadow" => {
            let mut bd = Builder::new(&[
                ("oid", I8),
                ("rolname", T),
                ("usename", T),
                ("rolsuper", B),
                ("usesuper", B),
                ("rolinherit", B),
                ("rolcreaterole", B),
                ("rolcreatedb", B),
                ("usecreatedb", B),
                ("rolcanlogin", B),
                ("rolreplication", B),
                ("rolconnlimit", I4),
                ("rolvaliduntil", T),
                ("rolbypassrls", B),
                ("rolconfig", T),
                ("usesysid", I8),
            ]);
            bd.row(vec![
                i(10),
                t(&user),
                t(&user),
                b(true),
                b(true),
                b(true),
                b(true),
                b(true),
                b(true),
                b(true),
                b(false),
                i(-1),
                N,
                b(false),
                N,
                i(10),
            ]);
            bd.done()
        }
        "pg_settings" => {
            let mut bd = Builder::new(&[
                ("name", T),
                ("setting", T),
                ("unit", T),
                ("category", T),
                ("short_desc", T),
                ("context", T),
                ("vartype", T),
                ("source", T),
                ("boot_val", T),
                ("reset_val", T),
            ]);
            let mut vars: Vec<(&String, &String)> = ex.s.vars.iter().collect();
            vars.sort();
            for (k, v) in vars {
                bd.row(vec![
                    t(k),
                    t(v),
                    N,
                    t("Storine"),
                    t(""),
                    t("user"),
                    t("string"),
                    t("session"),
                    t(v),
                    t(v),
                ]);
            }
            bd.row(vec![
                t("server_version"),
                t(crate::PG_VERSION),
                N,
                t("Storine"),
                t(""),
                t("internal"),
                t("string"),
                t("default"),
                t(crate::PG_VERSION),
                t(crate::PG_VERSION),
            ]);
            bd.row(vec![
                t("server_version_num"),
                t(crate::PG_VERSION_NUM),
                N,
                t("Storine"),
                t(""),
                t("internal"),
                t("integer"),
                t("default"),
                t(crate::PG_VERSION_NUM),
                t(crate::PG_VERSION_NUM),
            ]);
            bd.done()
        }
        "pg_am" => {
            let mut bd =
                Builder::new(&[("oid", I8), ("amname", T), ("amhandler", T), ("amtype", CH)]);
            bd.row(vec![i(2), t("heap"), t("heap_tableam_handler"), t("t")]);
            bd.row(vec![i(403), t("btree"), t("bthandler"), t("i")]);
            bd.done()
        }
        "pg_description" => Builder::new(&[
            ("objoid", I8),
            ("classoid", I8),
            ("objsubid", I4),
            ("description", T),
        ])
        .done(),
        "pg_proc" => Builder::new(&[
            ("oid", I8),
            ("proname", T),
            ("pronamespace", I8),
            ("prorettype", I8),
            ("proargtypes", T),
            ("prokind", CH),
        ])
        .done(),
        "pg_enum" => Builder::new(&[
            ("oid", I8),
            ("enumtypid", I8),
            ("enumsortorder", DataType::Real),
            ("enumlabel", T),
        ])
        .done(),
        "pg_extension" => Builder::new(&[
            ("oid", I8),
            ("extname", T),
            ("extowner", I8),
            ("extnamespace", I8),
            ("extrelocatable", B),
            ("extversion", T),
        ])
        .done(),
        "pg_inherits" => Builder::new(&[
            ("inhrelid", I8),
            ("inhparent", I8),
            ("inhseqno", I4),
            ("inhdetachpending", B),
        ])
        .done(),
        "pg_trigger" => Builder::new(&[
            ("oid", I8),
            ("tgrelid", I8),
            ("tgname", T),
            ("tgenabled", T),
            ("tgisinternal", B),
        ])
        .done(),
        "pg_views" => {
            let mut bd = Builder::new(&[
                ("schemaname", T),
                ("viewname", T),
                ("viewowner", T),
                ("definition", T),
            ]);
            for v in ex.s.engine.view_names()? {
                let sql = ex.s.engine.view(&v)?.map(|d| d.0).unwrap_or_default();
                bd.row(vec![t("public"), t(&v), t(&ex.s.user), t(sql)]);
            }
            bd.done()
        }
        "pg_matviews" => Builder::new(&[
            ("schemaname", T),
            ("matviewname", T),
            ("matviewowner", T),
            ("definition", T),
        ])
        .done(),
        "pg_sequence" | "pg_sequences" => Builder::new(&[
            ("seqrelid", I8),
            ("schemaname", T),
            ("sequencename", T),
            ("last_value", I8),
        ])
        .done(),
        "pg_collation" => {
            let mut bd = Builder::new(&[
                ("oid", I8),
                ("collname", T),
                ("collnamespace", I8),
                ("collprovider", CH),
                ("collencoding", I4),
                ("collcollate", T),
                ("collctype", T),
                ("collisdeterministic", B),
            ]);
            bd.row(vec![
                i(100),
                t("default"),
                i(NS_PG_CATALOG),
                t("d"),
                i(-1),
                N,
                N,
                b(true),
            ]);
            bd.row(vec![
                i(950),
                t("C"),
                i(NS_PG_CATALOG),
                t("c"),
                i(-1),
                t("C"),
                t("C"),
                b(true),
            ]);
            bd.done()
        }
        "pg_stat_activity" => {
            let mut bd = Builder::new(&[
                ("datid", I8),
                ("datname", T),
                ("pid", I8),
                ("usename", T),
                ("application_name", T),
                ("state", T),
                ("query", T),
            ]);
            bd.row(vec![
                i(16_384),
                t(&ex.s.database),
                i(ex.s.id as i64),
                t(&user),
                t(ex.s
                    .vars
                    .get("application_name")
                    .cloned()
                    .unwrap_or_default()),
                t("active"),
                t(""),
            ]);
            bd.done()
        }
        // Catalogs of features this gateway does not have: empty, with their real columns
        // so introspection queries that name them still run.
        "pg_locks" => Builder::new(&[
            ("locktype", T),
            ("database", I8),
            ("relation", I8),
            ("pid", I8),
            ("mode", T),
            ("granted", B),
        ])
        .done(),
        "pg_stat_user_tables" => Builder::new(&[
            ("relid", I8),
            ("schemaname", T),
            ("relname", T),
            ("seq_scan", I8),
            ("n_live_tup", I8),
            ("n_dead_tup", I8),
        ])
        .done(),
        "pg_statio_user_tables" => Builder::new(&[
            ("relid", I8),
            ("schemaname", T),
            ("relname", T),
            ("heap_blks_read", I8),
            ("heap_blks_hit", I8),
        ])
        .done(),
        "pg_stat_database" => Builder::new(&[
            ("datid", I8),
            ("datname", T),
            ("numbackends", I8),
            ("xact_commit", I8),
            ("xact_rollback", I8),
        ])
        .done(),
        "pg_publication" => Builder::new(&[
            ("oid", I8),
            ("pubname", T),
            ("pubowner", I8),
            ("puballtables", B),
        ])
        .done(),
        "pg_rewrite" => Builder::new(&[
            ("oid", I8),
            ("rulename", T),
            ("ev_class", I8),
            ("ev_type", CH),
            ("is_instead", B),
        ])
        .done(),
        "pg_policy" => Builder::new(&[
            ("oid", I8),
            ("polname", T),
            ("polrelid", I8),
            ("polcmd", CH),
            ("polpermissive", B),
        ])
        .done(),
        "pg_foreign_table" => {
            Builder::new(&[("ftrelid", I8), ("ftserver", I8), ("ftoptions", T)]).done()
        }
        "pg_partitioned_table" => Builder::new(&[
            ("partrelid", I8),
            ("partstrat", CH),
            ("partnatts", I8),
            ("partattrs", T),
        ])
        .done(),
        "pg_depend" => Builder::new(&[
            ("classid", I8),
            ("objid", I8),
            ("objsubid", I8),
            ("refclassid", I8),
            ("refobjid", I8),
            ("refobjsubid", I8),
            ("deptype", CH),
        ])
        .done(),
        "pg_shdescription" => {
            Builder::new(&[("objoid", I8), ("classoid", I8), ("description", T)]).done()
        }
        "pg_event_trigger" => Builder::new(&[
            ("oid", I8),
            ("evtname", T),
            ("evtevent", T),
            ("evtowner", I8),
            ("evtfoid", I8),
            ("evtenabled", CH),
        ])
        .done(),
        "pg_cast" => Builder::new(&[
            ("oid", I8),
            ("castsource", I8),
            ("casttarget", I8),
            ("castfunc", I8),
            ("castcontext", CH),
            ("castmethod", CH),
        ])
        .done(),
        "pg_operator" => Builder::new(&[
            ("oid", I8),
            ("oprname", T),
            ("oprnamespace", I8),
            ("oprkind", CH),
            ("oprleft", I8),
            ("oprright", I8),
            ("oprresult", I8),
            ("oprcode", I8),
        ])
        .done(),
        "pg_range" => Builder::new(&[
            ("rngtypid", I8),
            ("rngsubtype", I8),
            ("rngmultitypid", I8),
            ("rngcollation", I8),
            ("rngsubopc", I8),
            ("rngcanonical", I8),
            ("rngsubdiff", I8),
        ])
        .done(),
        "pg_tablespace" => Builder::new(&[
            ("oid", I8),
            ("spcname", T),
            ("spcowner", I8),
            ("spcoptions", T),
        ])
        .done(),
        "pg_seclabel" => Builder::new(&[
            ("objoid", I8),
            ("classoid", I8),
            ("objsubid", I8),
            ("provider", T),
            ("label", T),
        ])
        .done(),
        "pg_statistic_ext" => Builder::new(&[
            ("oid", I8),
            ("stxrelid", I8),
            ("stxname", T),
            ("stxnamespace", I8),
            ("stxkeys", T),
            ("stxkind", CH),
        ])
        .done(),
        "pg_opclass" => Builder::new(&[
            ("oid", I8),
            ("opcmethod", I8),
            ("opcname", T),
            ("opcnamespace", I8),
            ("opcfamily", I8),
            ("opcintype", I8),
            ("opcdefault", B),
        ])
        .done(),
        "pg_language" => Builder::new(&[
            ("oid", I8),
            ("lanname", T),
            ("lanowner", I8),
            ("lanispl", B),
            ("lanpltrusted", B),
        ])
        .done(),
        "pg_aggregate" => Builder::new(&[
            ("aggfnoid", I8),
            ("aggkind", CH),
            ("aggtransfn", I8),
            ("aggfinalfn", I8),
            ("aggtranstype", I8),
        ])
        .done(),
        // Other catalogs (publications, policies, statistics...) describe features this
        // gateway does not have: they are empty.
        _ => Builder::new(&[("oid", I8)]).done(),
    })
}

/// Index names with their columns: the primary key first, then unique keys, then indexes.
fn index_list(tb: &Table) -> Vec<(String, Vec<usize>)> {
    let mut out = Vec::new();
    if !tb.primary_key.is_empty() {
        out.push((format!("{}_pkey", tb.name), tb.primary_key.clone()));
    }
    for u in &tb.uniques {
        out.push((u.name.clone(), u.columns.clone()));
    }
    for x in &tb.indexes {
        out.push((x.name.clone(), x.columns.clone()));
    }
    out
}

fn index_sql(tb: &Table, name: &str, cols: &[usize], unique: bool) -> String {
    format!(
        "CREATE {}INDEX {} ON public.{} USING btree ({})",
        if unique { "UNIQUE " } else { "" },
        crate::fmt::quote_ident(name),
        crate::fmt::quote_ident(&tb.name),
        cols.iter()
            .map(|c| crate::fmt::quote_ident(&tb.columns[*c].name))
            .collect::<Vec<_>>()
            .join(", ")
    )
}

/// MySQL's spelling of a column's type, with its declared fractional-second precision.
pub fn mysql_column_type(c: &Column) -> (String, String) {
    let (dt, ct) = mysql_type(&c.ty);
    match (c.precision, &c.ty) {
        (Some(0), DataType::Timestamp | DataType::TimestampTz | DataType::Time) => (dt.clone(), dt),
        (Some(p), DataType::Timestamp | DataType::TimestampTz | DataType::Time) => {
            (dt.clone(), format!("{dt}({p})"))
        }
        _ => (dt, ct),
    }
}

/// MySQL's spelling of a column type.
pub fn mysql_type(ty: &DataType) -> (String, String) {
    // (DATA_TYPE, COLUMN_TYPE)
    match ty {
        DataType::Bool => ("tinyint".into(), "tinyint(1)".into()),
        DataType::SmallInt => ("smallint".into(), "smallint".into()),
        DataType::Int => ("int".into(), "int".into()),
        DataType::BigInt => ("bigint".into(), "bigint".into()),
        DataType::Numeric(Some((p, s))) => ("decimal".into(), format!("decimal({p},{s})")),
        DataType::Numeric(None) => ("decimal".into(), "decimal(65,30)".into()),
        DataType::Real => ("float".into(), "float".into()),
        DataType::Double => ("double".into(), "double".into()),
        DataType::Text | DataType::Name => ("text".into(), "text".into()),
        DataType::PgChar => ("char".into(), "char(1)".into()),
        DataType::Varchar(n) => ("varchar".into(), format!("varchar({})", n.unwrap_or(255))),
        DataType::Char(n) => ("char".into(), format!("char({})", n.unwrap_or(1))),
        DataType::Bytes => ("blob".into(), "blob".into()),
        DataType::Timestamp | DataType::TimestampTz => ("datetime".into(), "datetime(6)".into()),
        DataType::Date => ("date".into(), "date".into()),
        DataType::Time => ("time".into(), "time(6)".into()),
        DataType::Interval => ("varchar".into(), "varchar(64)".into()),
        DataType::Uuid => ("char".into(), "char(36)".into()),
        DataType::Json => ("json".into(), "json".into()),
        DataType::Jsonb => ("jsonb".into(), "jsonb".into()),
        DataType::Enum(v) => (
            "enum".into(),
            format!(
                "enum({})",
                v.iter()
                    .map(|x| format!("'{x}'"))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        ),
        DataType::Array(_) | DataType::Vector(_) => ("json".into(), "json".into()),
    }
}

fn pg_data_type(ty: &DataType) -> String {
    match ty {
        DataType::Array(_) => "ARRAY".into(),
        DataType::Enum(_) => "USER-DEFINED".into(),
        other => {
            let s = type_sql(other);
            s.split('(').next().unwrap_or(&s).to_owned()
        }
    }
}

fn information_schema(
    ex: &Exec<'_>,
    n: &str,
    tables: &[std::sync::Arc<Table>],
    schema: &str,
    mysql: bool,
) -> SqlResult<Rel> {
    let db = ex.s.database.clone();
    let mssql = ex.dialect() == Dialect::MsSql;
    // MySQL's and SQL Server's information_schema use upper-case column names; lookups are
    // case-insensitive.
    let cased = |name: &str| {
        if mysql || mssql {
            name.to_ascii_uppercase()
        } else {
            name.to_owned()
        }
    };
    let mk = |spec: &[(&str, DataType)]| -> Builder {
        // SQL Server's catalog numbers are `int` (drivers return `bigint` as strings).
        let owned: Vec<(String, DataType)> = spec
            .iter()
            .map(|(n, t)| {
                let t = if mssql && *t == DataType::BigInt {
                    DataType::Int
                } else {
                    t.clone()
                };
                (cased(n), t)
            })
            .collect();
        Builder {
            cols: owned
                .into_iter()
                .map(|(n, t)| ColMeta::new(n, Some(t)))
                .collect(),
            rows: Vec::new(),
        }
    };
    Ok(match n {
        "schemata" => {
            let mut bd = mk(&[
                ("catalog_name", T),
                ("schema_name", T),
                ("schema_owner", T),
                ("default_character_set_name", T),
                ("default_collation_name", T),
            ]);
            if mysql {
                for name in database_list(ex.s)? {
                    bd.row(vec![
                        t("def"),
                        t(&name),
                        N,
                        t("utf8mb4"),
                        t("utf8mb4_0900_ai_ci"),
                    ]);
                }
                bd.row(vec![
                    t("def"),
                    t("information_schema"),
                    N,
                    t("utf8mb3"),
                    t("utf8mb3_general_ci"),
                ]);
            } else {
                for s in ["public", "pg_catalog", "information_schema"] {
                    bd.row(vec![t(&db), t(s), t(&ex.s.user), N, N]);
                }
            }
            bd.done()
        }
        "tables" => {
            let mut bd = mk(&[
                ("table_catalog", T),
                ("table_schema", T),
                ("table_name", T),
                ("table_type", T),
                ("engine", T),
                ("table_rows", I8),
                ("auto_increment", I8),
                ("table_collation", T),
                ("table_comment", T),
                ("create_time", DataType::Timestamp),
                ("is_insertable_into", T),
                ("create_options", T),
                ("row_format", T),
                ("version", I8),
                ("update_time", DataType::Timestamp),
            ]);
            for tb in tables {
                bd.row(vec![
                    t(if mysql { "def" } else { &db }),
                    t(schema),
                    t(&tb.name),
                    t("BASE TABLE"),
                    t("InnoDB"),
                    N,
                    N,
                    t("utf8mb4_0900_ai_ci"),
                    t(""),
                    N,
                    t("YES"),
                    t(""),
                    t("Dynamic"),
                    i(10),
                    N,
                ]);
            }
            for v in ex.s.engine.view_names()? {
                bd.row(vec![
                    t(if mysql { "def" } else { &db }),
                    t(schema),
                    t(&v),
                    t("VIEW"),
                    N,
                    N,
                    N,
                    N,
                    t(if mysql { "VIEW" } else { "" }),
                    N,
                    t("NO"),
                    t(""),
                    t(""),
                    N,
                    N,
                ]);
            }
            bd.done()
        }
        "columns" => {
            let mut bd = mk(&[
                ("table_catalog", T),
                ("table_schema", T),
                ("table_name", T),
                ("column_name", T),
                ("ordinal_position", I8),
                ("column_default", T),
                ("is_nullable", T),
                ("data_type", T),
                ("character_maximum_length", I8),
                ("character_octet_length", I8),
                ("numeric_precision", I8),
                ("numeric_precision_radix", I8),
                ("numeric_scale", I8),
                ("datetime_precision", I8),
                ("character_set_name", T),
                ("collation_name", T),
                ("column_type", T),
                ("column_key", T),
                ("extra", T),
                ("privileges", T),
                ("column_comment", T),
                ("udt_catalog", T),
                ("udt_schema", T),
                ("udt_name", T),
                ("is_identity", T),
                ("identity_generation", T),
                ("is_generated", T),
                ("generation_expression", T),
                ("is_updatable", T),
                ("dtd_identifier", T),
            ]);
            for tb in tables {
                for (k, c) in tb.columns.iter().enumerate() {
                    let (my_dt, my_ct) = mysql_column_type(c);
                    let (ms_type, _, ms_len, ms_p, ms_s) = super::mssql::column_type(c);
                    let ms_wide = matches!(ms_type.as_str(), "nvarchar" | "nchar" | "ntext");
                    let ms_text =
                        ms_wide || matches!(ms_type.as_str(), "varchar" | "char" | "text");
                    let ms_numeric = matches!(
                        ms_type.as_str(),
                        "decimal"
                            | "numeric"
                            | "money"
                            | "smallmoney"
                            | "tinyint"
                            | "smallint"
                            | "int"
                            | "bigint"
                            | "float"
                            | "real"
                    );
                    let (char_len, num_prec, num_scale) = match &c.ty {
                        _ if mssql && ms_text => (
                            Some(if ms_len < 0 {
                                -1
                            } else if ms_wide {
                                ms_len / 2
                            } else {
                                ms_len
                            }),
                            None,
                            None,
                        ),
                        _ if mssql && ms_numeric => (None, Some(ms_p), Some(ms_s)),
                        _ if mssql => (None, None, None),
                        DataType::Varchar(Some(n)) | DataType::Char(Some(n)) => {
                            (Some(i64::from(*n)), None, None)
                        }
                        DataType::Numeric(Some((p, s))) => {
                            (None, Some(i64::from(*p)), Some(i64::from(*s)))
                        }
                        DataType::SmallInt => (None, Some(16), Some(0)),
                        DataType::Int => (None, Some(32), Some(0)),
                        DataType::BigInt => (None, Some(64), Some(0)),
                        DataType::Real => (None, Some(24), None),
                        DataType::Double => (None, Some(53), None),
                        _ => (None, None, None),
                    };
                    let key = if tb.primary_key.contains(&k) {
                        "PRI"
                    } else if tb.uniques.iter().any(|u| u.columns.first() == Some(&k)) {
                        "UNI"
                    } else if tb.indexes.iter().any(|x| x.columns.first() == Some(&k))
                        || tb
                            .foreign_keys
                            .iter()
                            .any(|f| f.columns.first() == Some(&k))
                    {
                        "MUL"
                    } else {
                        ""
                    };
                    let default = match (&c.default, c.auto_increment) {
                        (Some(d), _) if mssql => Some(super::mssql::default_sql(d)),
                        (None, true) if mssql => None,
                        (Some(d), _) => Some(if mysql {
                            mysql_default(d)
                        } else {
                            d.to_string()
                        }),
                        (None, true) if !mysql => {
                            Some(format!("nextval('{}_{}_seq'::regclass)", tb.name, c.name))
                        }
                        _ => None,
                    };
                    let udt = PG_TYPES
                        .iter()
                        .find(|p| p.0 == type_oid(&c.ty))
                        .map_or("text", |p| p.1);
                    bd.row(vec![
                        t(if mysql { "def" } else { &db }),
                        t(schema),
                        t(&tb.name),
                        t(&c.name),
                        i(k as i64 + 1),
                        default.map_or(N, t),
                        t(if c.not_null { "NO" } else { "YES" }),
                        t(if mysql {
                            my_dt.to_owned()
                        } else if mssql {
                            ms_type.to_owned()
                        } else {
                            pg_data_type(&c.ty)
                        }),
                        char_len.map_or(N, i),
                        char_len.map_or(N, |n| {
                            i(if n < 0 {
                                -1
                            } else if mssql {
                                ms_len
                            } else {
                                n * 4
                            })
                        }),
                        num_prec.map_or(N, i),
                        num_prec.map_or(N, |_| {
                            i(if matches!(c.ty, DataType::Numeric(_)) {
                                10
                            } else {
                                2
                            })
                        }),
                        num_scale.map_or(N, i),
                        c.datetime_precision().map_or(N, i),
                        // Character set and collation: MySQL names them; PostgreSQL leaves
                        // the default collation NULL.
                        if mssql && ms_text {
                            t("UNICODE")
                        } else if mysql
                            && matches!(
                                c.ty,
                                DataType::Text | DataType::Varchar(_) | DataType::Char(_)
                            )
                        {
                            t("utf8mb4")
                        } else {
                            N
                        },
                        if mssql && ms_text {
                            t(super::mssql::COLLATION)
                        } else if mysql
                            && matches!(
                                c.ty,
                                DataType::Text | DataType::Varchar(_) | DataType::Char(_)
                            )
                        {
                            t("utf8mb4_0900_ai_ci")
                        } else {
                            N
                        },
                        t(my_ct),
                        t(key),
                        t(if c.auto_increment {
                            "auto_increment"
                        } else {
                            ""
                        }),
                        t("select,insert,update,references"),
                        t(""),
                        t(&db),
                        t("pg_catalog"),
                        t(udt),
                        t(if c.auto_increment && !mysql {
                            "YES"
                        } else {
                            "NO"
                        }),
                        if c.auto_increment && !mysql {
                            t("BY DEFAULT")
                        } else {
                            N
                        },
                        t("NEVER"),
                        N,
                        t("YES"),
                        t((k + 1).to_string()),
                    ]);
                }
            }
            bd.done()
        }
        "table_constraints" => {
            let mut bd = mk(&[
                ("constraint_catalog", T),
                ("constraint_schema", T),
                ("constraint_name", T),
                ("table_catalog", T),
                ("table_schema", T),
                ("table_name", T),
                ("constraint_type", T),
                ("is_deferrable", T),
                ("initially_deferred", T),
                ("enforced", T),
            ]);
            for tb in tables {
                let mut push = |name: &str, kind: &str| {
                    bd.row(vec![
                        t(&db),
                        t(schema),
                        t(name),
                        t(&db),
                        t(schema),
                        t(&tb.name),
                        t(kind),
                        t("NO"),
                        t("NO"),
                        t("YES"),
                    ]);
                };
                if !tb.primary_key.is_empty() {
                    let pk = if mysql {
                        "PRIMARY".to_owned()
                    } else {
                        format!("{}_pkey", tb.name)
                    };
                    push(&pk, "PRIMARY KEY");
                }
                for u in &tb.uniques {
                    push(&u.name, "UNIQUE");
                }
                for f in &tb.foreign_keys {
                    push(&f.name, "FOREIGN KEY");
                }
                for c in &tb.checks {
                    push(&c.name, "CHECK");
                }
            }
            bd.done()
        }
        "key_column_usage" => {
            let mut bd = mk(&[
                ("constraint_catalog", T),
                ("constraint_schema", T),
                ("constraint_name", T),
                ("table_catalog", T),
                ("table_schema", T),
                ("table_name", T),
                ("column_name", T),
                ("ordinal_position", I8),
                ("position_in_unique_constraint", I8),
                ("referenced_table_schema", T),
                ("referenced_table_name", T),
                ("referenced_column_name", T),
            ]);
            for tb in tables {
                let mut push = |name: &str, cols: &[usize], refs: Option<(&str, Vec<String>)>| {
                    for (k, c) in cols.iter().enumerate() {
                        let (rt, rc) = match &refs {
                            Some((rt, rcs)) => (t(*rt), rcs.get(k).cloned().map_or(N, t)),
                            None => (N, N),
                        };
                        bd.row(vec![
                            t(&db),
                            t(schema),
                            t(name),
                            t(&db),
                            t(schema),
                            t(&tb.name),
                            t(&tb.columns[*c].name),
                            i(k as i64 + 1),
                            if refs.is_some() { i(k as i64 + 1) } else { N },
                            if refs.is_some() { t(schema) } else { N },
                            rt,
                            rc,
                        ]);
                    }
                };
                if !tb.primary_key.is_empty() {
                    push(
                        &if mysql {
                            "PRIMARY".to_owned()
                        } else {
                            format!("{}_pkey", tb.name)
                        },
                        &tb.primary_key,
                        None,
                    );
                }
                for u in &tb.uniques {
                    push(&u.name, &u.columns, None);
                }
                for f in &tb.foreign_keys {
                    let ref_cols = if f.ref_columns.is_empty() {
                        ex.s.engine
                            .catalog
                            .get(&f.ref_table)
                            .map(|p| {
                                p.primary_key
                                    .iter()
                                    .map(|i| p.columns[*i].name.clone())
                                    .collect()
                            })
                            .unwrap_or_default()
                    } else {
                        f.ref_columns.clone()
                    };
                    push(&f.name, &f.columns, Some((&f.ref_table, ref_cols)));
                }
            }
            bd.done()
        }
        "referential_constraints" => {
            let mut bd = mk(&[
                ("constraint_catalog", T),
                ("constraint_schema", T),
                ("constraint_name", T),
                ("unique_constraint_catalog", T),
                ("unique_constraint_schema", T),
                ("unique_constraint_name", T),
                ("match_option", T),
                ("update_rule", T),
                ("delete_rule", T),
                ("table_name", T),
                ("referenced_table_name", T),
            ]);
            for tb in tables {
                for f in &tb.foreign_keys {
                    bd.row(vec![
                        t(&db),
                        t(schema),
                        t(&f.name),
                        t(&db),
                        t(schema),
                        t(if mysql {
                            "PRIMARY".to_owned()
                        } else {
                            format!("{}_pkey", f.ref_table)
                        }),
                        t(if mysql { "NONE" } else { "SIMPLE" }),
                        t(action_name(f.on_update)),
                        t(action_name(f.on_delete)),
                        t(&tb.name),
                        t(&f.ref_table),
                    ]);
                }
            }
            bd.done()
        }
        "constraint_column_usage" => {
            let mut bd = mk(&[
                ("table_catalog", T),
                ("table_schema", T),
                ("table_name", T),
                ("column_name", T),
                ("constraint_catalog", T),
                ("constraint_schema", T),
                ("constraint_name", T),
            ]);
            for tb in tables {
                if !tb.primary_key.is_empty() {
                    for c in &tb.primary_key {
                        bd.row(vec![
                            t(&db),
                            t(schema),
                            t(&tb.name),
                            t(&tb.columns[*c].name),
                            t(&db),
                            t(schema),
                            t(format!("{}_pkey", tb.name)),
                        ]);
                    }
                }
                for u in &tb.uniques {
                    for c in &u.columns {
                        bd.row(vec![
                            t(&db),
                            t(schema),
                            t(&tb.name),
                            t(&tb.columns[*c].name),
                            t(&db),
                            t(schema),
                            t(&u.name),
                        ]);
                    }
                }
                for f in &tb.foreign_keys {
                    if let Some(p) = ex.s.engine.catalog.get(&f.ref_table) {
                        let cols: Vec<String> = if f.ref_columns.is_empty() {
                            p.primary_key
                                .iter()
                                .map(|i| p.columns[*i].name.clone())
                                .collect()
                        } else {
                            f.ref_columns.clone()
                        };
                        for c in cols {
                            bd.row(vec![
                                t(&db),
                                t(schema),
                                t(&p.name),
                                t(c),
                                t(&db),
                                t(schema),
                                t(&f.name),
                            ]);
                        }
                    }
                }
            }
            bd.done()
        }
        "check_constraints" => {
            let mut bd = mk(&[
                ("constraint_catalog", T),
                ("constraint_schema", T),
                ("constraint_name", T),
                ("check_clause", T),
            ]);
            for tb in tables {
                for c in &tb.checks {
                    bd.row(vec![t(&db), t(schema), t(&c.name), t(c.expr.to_string())]);
                }
            }
            bd.done()
        }
        "statistics" => {
            let mut bd = mk(&[
                ("table_catalog", T),
                ("table_schema", T),
                ("table_name", T),
                ("non_unique", I8),
                ("index_schema", T),
                ("index_name", T),
                ("seq_in_index", I8),
                ("column_name", T),
                ("collation", T),
                ("cardinality", I8),
                ("sub_part", I8),
                ("packed", T),
                ("nullable", T),
                ("index_type", T),
                ("comment", T),
                ("index_comment", T),
                ("is_visible", T),
                ("expression", T),
            ]);
            for tb in tables {
                for (name, cols) in index_list(tb) {
                    let unique =
                        name.ends_with("_pkey") || tb.uniques.iter().any(|u| u.name == name);
                    let shown = if name.ends_with("_pkey") && mysql {
                        "PRIMARY".to_owned()
                    } else {
                        name.clone()
                    };
                    for (k, c) in cols.iter().enumerate() {
                        bd.row(vec![
                            t("def"),
                            t(schema),
                            t(&tb.name),
                            i(i64::from(!unique)),
                            t(schema),
                            t(&shown),
                            i(k as i64 + 1),
                            t(&tb.columns[*c].name),
                            t("A"),
                            i(0),
                            N,
                            N,
                            t(if tb.columns[*c].not_null { "" } else { "YES" }),
                            t("BTREE"),
                            t(""),
                            t(""),
                            t("YES"),
                            N,
                        ]);
                    }
                }
            }
            bd.done()
        }
        "character_sets" => {
            let mut bd = mk(&[
                ("character_set_name", T),
                ("default_collate_name", T),
                ("description", T),
                ("maxlen", I8),
            ]);
            bd.row(vec![
                t("utf8mb4"),
                t("utf8mb4_0900_ai_ci"),
                t("UTF-8 Unicode"),
                i(4),
            ]);
            bd.done()
        }
        "collations" => {
            let mut bd = mk(&[
                ("collation_name", T),
                ("character_set_name", T),
                ("id", I8),
                ("is_default", T),
                ("is_compiled", T),
                ("sortlen", I8),
            ]);
            bd.row(vec![
                t("utf8mb4_0900_ai_ci"),
                t("utf8mb4"),
                i(255),
                t("Yes"),
                t("Yes"),
                i(0),
            ]);
            bd.row(vec![
                t("utf8mb4_bin"),
                t("utf8mb4"),
                i(46),
                t(""),
                t("Yes"),
                i(1),
            ]);
            bd.done()
        }
        "keywords" => {
            let mut bd = mk(&[("word", T), ("reserved", I8)]);
            for w in crate::exec::func::RESERVED_WORDS {
                bd.row(vec![t(*w), i(1)]);
            }
            bd.done()
        }
        "engines" => {
            let mut bd = mk(&[
                ("engine", T),
                ("support", T),
                ("comment", T),
                ("transactions", T),
                ("xa", T),
                ("savepoints", T),
            ]);
            bd.row(vec![
                t("InnoDB"),
                t("DEFAULT"),
                t("Storine"),
                t("YES"),
                t("NO"),
                t("YES"),
            ]);
            bd.done()
        }
        "views" => {
            let mut bd = mk(&[
                ("table_catalog", T),
                ("table_schema", T),
                ("table_name", T),
                ("view_definition", T),
                ("check_option", T),
                ("is_updatable", T),
                ("is_insertable_into", T),
            ]);
            for v in ex.s.engine.view_names()? {
                let sql = ex.s.engine.view(&v)?.map(|d| d.0).unwrap_or_default();
                bd.row(vec![
                    t(if mysql { "def" } else { &db }),
                    t(schema),
                    t(&v),
                    t(sql),
                    t("NONE"),
                    t("NO"),
                    t("NO"),
                ]);
            }
            bd.done()
        }
        "sequences" => {
            let mut bd = mk(&[
                ("sequence_catalog", T),
                ("sequence_schema", T),
                ("sequence_name", T),
                ("data_type", T),
                ("numeric_precision", I8),
                ("numeric_precision_radix", I8),
                ("numeric_scale", I8),
                ("start_value", T),
                ("minimum_value", T),
                ("maximum_value", T),
                ("increment", T),
                ("cycle_option", T),
            ]);
            for q in super::dml::user_sequences(&ex.s.engine)? {
                bd.row(vec![
                    t(&db),
                    t(schema),
                    t(&q.name),
                    t("bigint"),
                    i(64),
                    i(2),
                    i(0),
                    t(q.start.to_string()),
                    t("1"),
                    t(i64::MAX.to_string()),
                    t(q.increment.to_string()),
                    t("NO"),
                ]);
            }
            bd.done()
        }
        "routines" | "triggers" | "parameters" | "events" | "partitions" | "table_privileges"
        | "column_privileges" | "user_privileges" | "schema_privileges" | "processlist"
        | "innodb_trx" | "role_table_grants" | "domains" | "element_types" | "attributes"
        | "user_defined_types" | "column_statistics" | "plugins" => {
            mk(&[("table_schema", T), ("table_name", T)]).done()
        }
        other => {
            return Err(SqlError::undefined_table(&format!(
                "information_schema.{other}"
            )))
        }
    })
}

/// MySQL shows literal defaults without quotes (`0`, `abc`, `CURRENT_TIMESTAMP`).
fn mysql_default(e: &crate::ast::Expr) -> String {
    match e {
        crate::ast::Expr::Str(s) => s.clone(),
        crate::ast::Expr::Function { name, args, .. } if args.is_empty() => match name.as_str() {
            "now" | "current_timestamp" => "CURRENT_TIMESTAMP".into(),
            other => format!("{other}()"),
        },
        other => other.to_string(),
    }
}

// ── functions used by catalog queries ──

pub fn format_type(oid: &Datum, typmod: Option<&Datum>) -> Datum {
    let Ok(o) = oid.as_i64() else {
        return Datum::Null;
    };
    let m = typmod.and_then(|t| t.as_i64().ok()).unwrap_or(-1);
    let base = match o {
        16 => "boolean".to_owned(),
        17 => "bytea".into(),
        20 => "bigint".into(),
        21 => "smallint".into(),
        23 => "integer".into(),
        25 => "text".into(),
        114 => "json".into(),
        3802 => "jsonb".into(),
        700 => "real".into(),
        701 => "double precision".into(),
        1042 => {
            if m > 4 {
                format!("character({})", m - 4)
            } else {
                "character".into()
            }
        }
        1043 => {
            if m > 4 {
                format!("character varying({})", m - 4)
            } else {
                "character varying".into()
            }
        }
        1082 => "date".into(),
        1083 | 1114 | 1184 => {
            let (base, zone) = match o {
                1083 => ("time", "without"),
                1114 => ("timestamp", "without"),
                _ => ("timestamp", "with"),
            };
            if m >= 0 {
                format!("{base}({m}) {zone} time zone")
            } else {
                format!("{base} {zone} time zone")
            }
        }
        1186 => "interval".into(),
        1700 => {
            if m > 4 {
                let v = m - 4;
                format!("numeric({},{})", v >> 16, v & 0xffff)
            } else {
                "numeric".into()
            }
        }
        2950 => "uuid".into(),
        _ => match PG_TYPES.iter().find(|p| i64::from(p.0) == o) {
            Some(p) if p.1.starts_with('_') => format!(
                "{}[]",
                format_type(&Datum::Int(i64::from(p.5)), None).as_text()
            ),
            Some(p) => p.1.to_owned(),
            None => "unknown".into(),
        },
    };
    Datum::Text(base)
}

pub fn pg_get_expr(_ex: &Exec<'_>, expr: &Datum, _rel: Option<&Datum>) -> SqlResult<Datum> {
    // The catalogs store expressions as SQL text already.
    Ok(match expr {
        Datum::Null => Datum::Null,
        other => Datum::Text(other.as_text()),
    })
}

pub fn constraint_def(ex: &Exec<'_>, oid: &Datum) -> SqlResult<Datum> {
    let Ok(o) = oid.as_i64() else {
        return Ok(Datum::Null);
    };
    for tb in ex.s.engine.catalog.all() {
        let names = |cols: &[usize]| {
            cols.iter()
                .map(|c| crate::fmt::quote_ident(&tb.columns[*c].name))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let pk = format!("{}_pkey", tb.name);
        if !tb.primary_key.is_empty() && object_oid("c", &tb.name, &pk) == o {
            return Ok(Datum::Text(format!(
                "PRIMARY KEY ({})",
                names(&tb.primary_key)
            )));
        }
        for u in &tb.uniques {
            if object_oid("c", &tb.name, &u.name) == o {
                return Ok(Datum::Text(format!("UNIQUE ({})", names(&u.columns))));
            }
        }
        for f in &tb.foreign_keys {
            if object_oid("c", &tb.name, &f.name) == o {
                let refs = if f.ref_columns.is_empty() {
                    String::new()
                } else {
                    format!("({})", f.ref_columns.join(", "))
                };
                let mut s = format!(
                    "FOREIGN KEY ({}) REFERENCES {}{refs}",
                    names(&f.columns),
                    crate::fmt::quote_ident(&f.ref_table)
                );
                if f.on_delete != crate::ast::RefAction::NoAction {
                    s.push_str(&format!(" ON DELETE {}", action_name(f.on_delete)));
                }
                return Ok(Datum::Text(s));
            }
        }
        for c in &tb.checks {
            if object_oid("c", &tb.name, &c.name) == o {
                return Ok(Datum::Text(format!("CHECK ({})", c.expr)));
            }
        }
    }
    Ok(Datum::Null)
}

/// `pg_get_indexdef(oid)`, or with `column` > 0 the name of that index column.
pub fn index_def(ex: &Exec<'_>, oid: &Datum, column: i64) -> SqlResult<Datum> {
    let Ok(o) = oid.as_i64() else {
        return Ok(Datum::Null);
    };
    for tb in ex.s.engine.catalog.all() {
        for (name, cols) in index_list(&tb) {
            if object_oid("i", &tb.name, &name) == o {
                if column > 0 {
                    return Ok(usize::try_from(column - 1)
                        .ok()
                        .and_then(|k| cols.get(k))
                        .map_or(Datum::Null, |c| Datum::Text(tb.columns[*c].name.clone())));
                }
                let unique = name.ends_with("_pkey") || tb.uniques.iter().any(|u| u.name == name);
                return Ok(Datum::Text(index_sql(&tb, &name, &cols, unique)));
            }
        }
    }
    let _ = table_by_oid;
    Ok(Datum::Null)
}

/// `CREATE TABLE` text of a table (`SHOW CREATE TABLE`, `sqlite_master.sql`).
pub fn create_table_sql(tb: &Table, dialect: Dialect) -> String {
    let q = |s: &str| {
        if dialect == Dialect::MySql {
            format!("`{s}`")
        } else {
            crate::fmt::quote_ident(s)
        }
    };
    let mut lines = Vec::new();
    for c in &tb.columns {
        let ty = if dialect == Dialect::MySql {
            mysql_column_type(c).1
        } else {
            type_sql(&c.ty)
        };
        let mut l = format!("  {} {ty}", q(&c.name));
        if c.not_null {
            l.push_str(" NOT NULL");
        }
        if c.auto_increment {
            l.push_str(if dialect == Dialect::MySql {
                " AUTO_INCREMENT"
            } else {
                " GENERATED BY DEFAULT AS IDENTITY"
            });
        }
        if let Some(d) = &c.default {
            l.push_str(&format!(" DEFAULT {d}"));
        }
        lines.push(l);
    }
    let names = |cols: &[usize]| {
        cols.iter()
            .map(|c| q(&tb.columns[*c].name))
            .collect::<Vec<_>>()
            .join(", ")
    };
    if !tb.primary_key.is_empty() {
        lines.push(format!("  PRIMARY KEY ({})", names(&tb.primary_key)));
    }
    for u in &tb.uniques {
        if dialect == Dialect::MySql {
            lines.push(format!(
                "  UNIQUE KEY {} ({})",
                q(&u.name),
                names(&u.columns)
            ));
        } else {
            lines.push(format!(
                "  CONSTRAINT {} UNIQUE ({})",
                q(&u.name),
                names(&u.columns)
            ));
        }
    }
    for x in &tb.indexes {
        if dialect == Dialect::MySql {
            lines.push(format!("  KEY {} ({})", q(&x.name), names(&x.columns)));
        }
    }
    for f in &tb.foreign_keys {
        lines.push(format!(
            "  CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}{}{}",
            q(&f.name),
            names(&f.columns),
            q(&f.ref_table),
            if f.ref_columns.is_empty() {
                String::new()
            } else {
                format!(
                    " ({})",
                    f.ref_columns
                        .iter()
                        .map(|c| q(c))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            },
            if f.on_delete == crate::ast::RefAction::NoAction {
                String::new()
            } else {
                format!(" ON DELETE {}", action_name(f.on_delete))
            }
        ));
    }
    for c in &tb.checks {
        lines.push(format!("  CONSTRAINT {} CHECK ({})", q(&c.name), c.expr));
    }
    let mut out = format!("CREATE TABLE {} (\n{}\n)", q(&tb.name), lines.join(",\n"));
    if dialect == Dialect::MySql {
        out.push_str(" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
    }
    out
}

fn text_rs(cols: &[&str], rows: Vec<Vec<Datum>>) -> Outcome {
    Outcome::Rows(ResultSet {
        columns: cols
            .iter()
            .map(|c| OutCol {
                name: (*c).to_owned(),
                ty: DataType::Text,
                table: None,
            })
            .collect(),
        rows,
    })
}

fn like_filter(like: &Option<String>, s: &str) -> bool {
    like.as_ref()
        .is_none_or(|p| crate::exec::eval::like(&s.to_lowercase(), &p.to_lowercase(), Some('\\')))
}

/// `SHOW ...` (MySQL commands and PostgreSQL's `SHOW setting`).
pub fn show(s: &mut Session, what: &Show) -> SqlResult<Outcome> {
    s.engine.refresh_catalog(false)?;
    let tables = s.engine.catalog.all();
    let db = s.database.clone();
    Ok(match what {
        Show::Variable(name) => {
            let v = match name.as_str() {
                "server_version" => crate::PG_VERSION.to_owned(),
                "server_version_num" => crate::PG_VERSION_NUM.to_owned(),
                "all" => {
                    let mut rows: Vec<Vec<Datum>> = s
                        .vars
                        .iter()
                        .map(|(k, v)| vec![t(k), t(v), t("")])
                        .collect();
                    rows.sort_by_key(|r| r[0].as_text());
                    return Ok(text_rs(&["name", "setting", "description"], rows));
                }
                other => s.vars.get(other).cloned().ok_or_else(|| {
                    SqlError::new(
                        "42704",
                        format!("unrecognized configuration parameter \"{other}\""),
                    )
                })?,
            };
            text_rs(&[name.as_str()], vec![vec![t(v)]])
        }
        Show::Tables { full, like } => {
            let col = format!("Tables_in_{db}");
            let rows: Vec<Vec<Datum>> = tables
                .iter()
                .filter(|tb| like_filter(like, &tb.name))
                .map(|tb| {
                    if *full {
                        vec![t(&tb.name), t("BASE TABLE")]
                    } else {
                        vec![t(&tb.name)]
                    }
                })
                .chain(
                    s.engine
                        .view_names()?
                        .into_iter()
                        .filter(|v| like_filter(like, v))
                        .map(|v| {
                            if *full {
                                vec![t(&v), t("VIEW")]
                            } else {
                                vec![t(&v)]
                            }
                        }),
                )
                .collect();
            if *full {
                text_rs(&[&col, "Table_type"], rows)
            } else {
                text_rs(&[&col], rows)
            }
        }
        Show::Columns { table, full } => {
            let tb = s.engine.table(table)?;
            let rows = tb
                .columns
                .iter()
                .enumerate()
                .map(|(k, c)| {
                    let key = if tb.primary_key.contains(&k) {
                        "PRI"
                    } else if tb.uniques.iter().any(|u| u.columns.first() == Some(&k)) {
                        "UNI"
                    } else if tb.indexes.iter().any(|x| x.columns.first() == Some(&k)) {
                        "MUL"
                    } else {
                        ""
                    };
                    let mut r = vec![
                        t(&c.name),
                        t(mysql_column_type(c).1),
                        t(if c.not_null { "NO" } else { "YES" }),
                        t(key),
                        c.default.as_ref().map_or(N, |d| t(mysql_default(d))),
                        t(if c.auto_increment {
                            "auto_increment"
                        } else {
                            ""
                        }),
                    ];
                    if *full {
                        r.insert(
                            2,
                            if matches!(
                                c.ty,
                                DataType::Text | DataType::Varchar(_) | DataType::Char(_)
                            ) {
                                t("utf8mb4_0900_ai_ci")
                            } else {
                                N
                            },
                        );
                        r.push(t("select,insert,update,references"));
                        r.push(t(""));
                    }
                    r
                })
                .collect();
            if *full {
                text_rs(
                    &[
                        "Field",
                        "Type",
                        "Collation",
                        "Null",
                        "Key",
                        "Default",
                        "Extra",
                        "Privileges",
                        "Comment",
                    ],
                    rows,
                )
            } else {
                text_rs(&["Field", "Type", "Null", "Key", "Default", "Extra"], rows)
            }
        }
        Show::Index { table } => {
            let tb = s.engine.table(table)?;
            let mut rows = Vec::new();
            for (name, cols) in index_list(&tb) {
                let unique = name.ends_with("_pkey") || tb.uniques.iter().any(|u| u.name == name);
                let shown = if name.ends_with("_pkey") {
                    "PRIMARY".to_owned()
                } else {
                    name.clone()
                };
                for (k, c) in cols.iter().enumerate() {
                    rows.push(vec![
                        t(&tb.name),
                        i(i64::from(!unique)),
                        t(&shown),
                        i(k as i64 + 1),
                        t(&tb.columns[*c].name),
                        t("A"),
                        i(0),
                        N,
                        N,
                        t(if tb.columns[*c].not_null { "" } else { "YES" }),
                        t("BTREE"),
                        t(""),
                        t(""),
                        t("YES"),
                        N,
                    ]);
                }
            }
            text_rs(
                &[
                    "Table",
                    "Non_unique",
                    "Key_name",
                    "Seq_in_index",
                    "Column_name",
                    "Collation",
                    "Cardinality",
                    "Sub_part",
                    "Packed",
                    "Null",
                    "Index_type",
                    "Comment",
                    "Index_comment",
                    "Visible",
                    "Expression",
                ],
                rows,
            )
        }
        Show::Databases => {
            let mut rows = vec![vec![t("information_schema")]];
            rows.extend(database_list(s)?.iter().map(|n| vec![t(n)]));
            text_rs(&["Database"], rows)
        }
        Show::CreateTable(name) => {
            let tb = s.engine.table(name)?;
            text_rs(
                &["Table", "Create Table"],
                vec![vec![t(&tb.name), t(create_table_sql(&tb, s.dialect))]],
            )
        }
        Show::Variables { like, .. } => {
            let mut rows: Vec<Vec<Datum>> = s
                .vars
                .iter()
                .filter(|(k, _)| like_filter(like, k))
                .map(|(k, v)| vec![t(k), t(v)])
                .collect();
            for (k, v) in [
                ("version", crate::MYSQL_VERSION),
                ("version_comment", "Storine"),
                ("have_ssl", "DISABLED"),
            ] {
                if like_filter(like, k) {
                    rows.push(vec![t(k), t(v)]);
                }
            }
            rows.sort_by_key(|r| r[0].as_text());
            text_rs(&["Variable_name", "Value"], rows)
        }
        Show::Status => text_rs(
            &["Variable_name", "Value"],
            vec![
                vec![t("Uptime"), t("1")],
                vec![t("Threads_connected"), t("1")],
            ],
        ),
        Show::Warnings | Show::Errors => text_rs(&["Level", "Code", "Message"], Vec::new()),
        Show::Collation => text_rs(
            &[
                "Collation",
                "Charset",
                "Id",
                "Default",
                "Compiled",
                "Sortlen",
                "Pad_attribute",
            ],
            vec![vec![
                t("utf8mb4_0900_ai_ci"),
                t("utf8mb4"),
                i(255),
                t("Yes"),
                t("Yes"),
                i(0),
                t("NO PAD"),
            ]],
        ),
        Show::CharacterSet => text_rs(
            &["Charset", "Description", "Default collation", "Maxlen"],
            vec![vec![
                t("utf8mb4"),
                t("UTF-8 Unicode"),
                t("utf8mb4_0900_ai_ci"),
                i(4),
            ]],
        ),
        Show::Engines => text_rs(
            &[
                "Engine",
                "Support",
                "Comment",
                "Transactions",
                "XA",
                "Savepoints",
            ],
            vec![vec![
                t("InnoDB"),
                t("DEFAULT"),
                t("Storine"),
                t("YES"),
                t("NO"),
                t("YES"),
            ]],
        ),
        Show::ProcessList => text_rs(
            &[
                "Id", "User", "Host", "db", "Command", "Time", "State", "Info",
            ],
            vec![vec![
                i(s.id as i64),
                t(&s.user),
                t("localhost"),
                t(&db),
                t("Query"),
                i(0),
                t(""),
                t("SHOW PROCESSLIST"),
            ]],
        ),
        Show::TableStatus { like } => {
            let rows = tables
                .iter()
                .filter(|tb| like_filter(like, &tb.name))
                .map(|tb| {
                    vec![
                        t(&tb.name),
                        t("InnoDB"),
                        i(10),
                        t("Dynamic"),
                        N,
                        N,
                        N,
                        N,
                        N,
                        N,
                        N,
                        N,
                        N,
                        N,
                        t("utf8mb4_0900_ai_ci"),
                        N,
                        t(""),
                        t(""),
                    ]
                })
                .collect();
            text_rs(
                &[
                    "Name",
                    "Engine",
                    "Version",
                    "Row_format",
                    "Rows",
                    "Avg_row_length",
                    "Data_length",
                    "Max_data_length",
                    "Index_length",
                    "Data_free",
                    "Auto_increment",
                    "Create_time",
                    "Update_time",
                    "Check_time",
                    "Collation",
                    "Checksum",
                    "Create_options",
                    "Comment",
                ],
                rows,
            )
        }
    })
}

/// Databases to list: the default one, every created one, and the name this session
/// connected with (names never created are served by the default database).
pub(crate) fn database_list(s: &crate::exec::Session) -> SqlResult<Vec<String>> {
    let mut names = s.engine.database_names()?;
    if !names.iter().any(|n| n.eq_ignore_ascii_case(&s.database)) {
        names.push(s.database.clone());
    }
    Ok(names)
}
