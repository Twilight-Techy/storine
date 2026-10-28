//! The SQL syntax tree shared by every dialect.

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Query(Box<Query>),
    Insert(Box<Insert>),
    Update(Box<Update>),
    Delete(Box<Delete>),
    CreateTable(Box<CreateTable>),
    CreateIndex(Box<CreateIndex>),
    DropTable {
        names: Vec<String>,
        if_exists: bool,
        cascade: bool,
    },
    DropIndex {
        names: Vec<String>,
        table: Option<String>,
        if_exists: bool,
    },
    AlterTable {
        table: String,
        ops: Vec<AlterOp>,
    },
    Truncate {
        tables: Vec<String>,
    },
    CreateSequence {
        name: String,
        start: i64,
        increment: i64,
        if_not_exists: bool,
    },
    DropSequence {
        names: Vec<String>,
        if_exists: bool,
    },
    /// `BEGIN` / `START TRANSACTION`.
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    ReleaseSavepoint(String),
    RollbackToSavepoint(String),
    /// `SET name = value` (session settings; value kept as text).
    Set {
        name: String,
        value: Option<Expr>,
    },
    Show(Show),
    /// `USE db` / `\c` style database switch (accepted; there is one database).
    Use(String),
    Explain(Box<Statement>),
    CreateSchema {
        name: String,
        if_not_exists: bool,
    },
    DropSchema {
        name: String,
        if_exists: bool,
    },
    /// SQL-level `PREPARE name AS stmt` / `EXECUTE name(args)` / `DEALLOCATE name`.
    Prepare {
        name: String,
        statement: Box<Statement>,
    },
    Execute {
        name: String,
        args: Vec<Expr>,
    },
    Deallocate(Option<String>),
    /// Statements accepted and ignored because they have no effect here (the tag is reported back).
    NoOp(&'static str),
    Empty,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Show {
    Variable(String),
    Tables { full: bool, like: Option<String> },
    Columns { table: String, full: bool },
    Index { table: String },
    Databases,
    CreateTable(String),
    Variables { like: Option<String>, global: bool },
    Status,
    Warnings,
    Errors,
    Collation,
    CharacterSet,
    Engines,
    ProcessList,
    TableStatus { like: Option<String> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub ctes: Vec<Cte>,
    pub body: SetExpr,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
    pub lock: Option<LockMode>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cte {
    pub name: String,
    pub columns: Vec<String>,
    pub query: Query,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Update { nowait: bool, skip_locked: bool },
    Share,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetExpr {
    Select(Box<Select>),
    Values(Vec<Vec<Expr>>),
    SetOp {
        op: SetOp,
        all: bool,
        left: Box<SetExpr>,
        right: Box<SetExpr>,
    },
    Nested(Box<Query>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOp {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Select {
    pub distinct: bool,
    pub items: Vec<SelectItem>,
    pub from: Vec<TableRef>,
    pub where_: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    /// `*` or `t.*`.
    Wildcard(Option<String>),
    Expr {
        expr: Expr,
        alias: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableAlias {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
    Named {
        schema: Option<String>,
        name: String,
        alias: Option<TableAlias>,
    },
    Derived {
        query: Box<Query>,
        alias: Option<TableAlias>,
        lateral: bool,
    },
    Function {
        name: String,
        args: Vec<Expr>,
        alias: Option<TableAlias>,
    },
    Join {
        left: Box<TableRef>,
        right: Box<TableRef>,
        kind: JoinKind,
        constraint: JoinConstraint,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinConstraint {
    On(Expr),
    Using(Vec<String>),
    Natural,
    None,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub expr: Expr,
    pub desc: bool,
    pub nulls_first: Option<bool>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    pub table: String,
    pub alias: Option<String>,
    pub columns: Vec<String>,
    pub source: InsertSource,
    pub on_conflict: Option<OnConflict>,
    pub returning: Vec<SelectItem>,
    /// MySQL `INSERT IGNORE`.
    pub ignore: bool,
    /// MySQL / SQLite `REPLACE INTO` and SQLite `INSERT OR REPLACE`.
    pub replace: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InsertSource {
    Values(Vec<Vec<Expr>>),
    Query(Box<Query>),
    DefaultValues,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OnConflict {
    /// PostgreSQL / SQLite `ON CONFLICT [(cols)] DO NOTHING`.
    DoNothing { target: Vec<String> },
    /// PostgreSQL / SQLite `ON CONFLICT (cols) DO UPDATE SET ... [WHERE ...]`; MySQL
    /// `ON DUPLICATE KEY UPDATE ...` (empty target: any unique key).
    DoUpdate {
        target: Vec<String>,
        sets: Vec<(String, Expr)>,
        where_: Option<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Update {
    pub table: String,
    pub alias: Option<String>,
    pub sets: Vec<(String, Expr)>,
    pub from: Vec<TableRef>,
    pub where_: Option<Expr>,
    pub returning: Vec<SelectItem>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Delete {
    pub table: String,
    pub alias: Option<String>,
    pub using: Vec<TableRef>,
    pub where_: Option<Expr>,
    pub returning: Vec<SelectItem>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub as_query: Option<Box<Query>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub ty: DataType,
    /// Fractional-second digits of a TIMESTAMP(p) / TIME(p) / DATETIME(p) column.
    pub precision: Option<u8>,
    pub not_null: bool,
    pub default: Option<Expr>,
    pub primary_key: bool,
    pub unique: bool,
    pub auto_increment: bool,
    pub references: Option<ForeignKey>,
    pub check: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ForeignKey {
    pub columns: Vec<String>,
    pub table: String,
    pub ref_columns: Vec<String>,
    pub on_delete: RefAction,
    pub on_update: RefAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefAction {
    NoAction,
    Restrict,
    Cascade,
    SetNull,
    SetDefault,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
    ForeignKey {
        name: Option<String>,
        fk: ForeignKey,
    },
    Check {
        name: Option<String>,
        expr: Expr,
    },
    /// MySQL `KEY idx (cols)` / `INDEX idx (cols)` inside `CREATE TABLE`.
    Index {
        name: Option<String>,
        columns: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterOp {
    AddColumn {
        column: ColumnDef,
        if_not_exists: bool,
    },
    DropColumn {
        name: String,
        if_exists: bool,
    },
    RenameColumn {
        from: String,
        to: String,
    },
    RenameTable(String),
    AlterColumn {
        name: String,
        change: AlterColumn,
    },
    AddConstraint(TableConstraint),
    DropConstraint {
        name: String,
        if_exists: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterColumn {
    SetDefault(Expr),
    DropDefault,
    SetNotNull,
    DropNotNull,
    Type(DataType),
    /// MySQL `MODIFY` / `CHANGE`: the whole definition (type, nullability, default).
    Redefine(Box<ColumnDef>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndex {
    pub name: Option<String>,
    pub table: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub if_not_exists: bool,
}

/// SQL column types (every spelling of each dialect maps onto one of these).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Bool,
    SmallInt,
    Int,
    BigInt,
    /// `NUMERIC(precision, scale)`; `None`: unconstrained.
    Numeric(Option<(u8, u8)>),
    Real,
    Double,
    Text,
    /// PostgreSQL `name` (catalog identifiers; text everywhere but the type OID).
    Name,
    Varchar(Option<u32>),
    Char(Option<u32>),
    Bytes,
    Timestamp,
    TimestampTz,
    Date,
    Time,
    Interval,
    Uuid,
    Json,
    /// PostgreSQL `jsonb`: stored and compared like `json`, reported with its own type.
    Jsonb,
    /// PostgreSQL's one-byte `"char"` (catalog codes such as `pg_constraint.contype`).
    PgChar,
    /// MySQL `ENUM('a', 'b')`.
    Enum(Vec<String>),
    Array(Box<DataType>),
}

/// `OVER (PARTITION BY ... ORDER BY ...)` of a window function.
#[derive(Debug, Clone, PartialEq)]
pub struct Window {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderBy>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
    BitNot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    /// MySQL `DIV`.
    IntDiv,
    Mod,
    Pow,
    Concat,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    /// MySQL `<=>` (null-safe equality).
    NullSafeEq,
    And,
    Or,
    Xor,
    BitAnd,
    BitOr,
    BitXor,
    ShiftLeft,
    ShiftRight,
    /// `->` (JSON field).
    JsonGet,
    /// `->>` (JSON field as text).
    JsonGetText,
    /// `~`, `~*`, `!~`, `!~*`, `REGEXP`.
    Regex {
        case_insensitive: bool,
        negated: bool,
    },
    /// `@>` (JSON / array contains).
    Contains,
    ContainedBy,
    /// `&&` (array overlap).
    Overlap,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Null,
    Bool(bool),
    Number(String),
    Str(String),
    Blob(Vec<u8>),
    Param(usize),
    /// `DEFAULT` inside `VALUES` / `SET`.
    Default,
    /// `col`, `t.col` or `schema.t.col`.
    Column(Vec<String>),
    Variable {
        name: String,
        system: bool,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Function {
        name: String,
        args: Vec<Expr>,
        star: bool,
        distinct: bool,
        over: Option<Box<Window>>,
    },
    Case {
        operand: Option<Box<Expr>>,
        whens: Vec<(Expr, Expr)>,
        else_: Option<Box<Expr>>,
    },
    Cast {
        expr: Box<Expr>,
        ty: DataType,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    IsBool {
        expr: Box<Expr>,
        value: bool,
        negated: bool,
    },
    IsDistinctFrom {
        left: Box<Expr>,
        right: Box<Expr>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        query: Box<Query>,
        negated: bool,
    },
    /// `x = ANY(array or subquery)` / `x > ALL(...)`.
    AnyAll {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
        all: bool,
    },
    Exists {
        query: Box<Query>,
        negated: bool,
    },
    Subquery(Box<Query>),
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
        case_insensitive: bool,
        escape: Option<Box<Expr>>,
    },
    Tuple(Vec<Expr>),
    Array(Vec<Expr>),
    /// `expr[index]` (1-based, PostgreSQL arrays).
    Index {
        expr: Box<Expr>,
        index: Box<Expr>,
    },
    /// `INTERVAL '1 day'`.
    Interval(String),
    /// `EXTRACT(field FROM expr)`.
    Extract {
        field: String,
        expr: Box<Expr>,
    },
}

impl Expr {
    pub fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
        Expr::Binary {
            op,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// The column name when this is a bare or qualified column reference.
    pub fn column_name(&self) -> Option<&str> {
        match self {
            Expr::Column(parts) => parts.last().map(String::as_str),
            _ => None,
        }
    }
}
