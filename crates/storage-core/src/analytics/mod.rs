//! Analytic queries over columnar collections (`TASK-127`, `TASK-128`, `F-OLAP-03`, `F-OLAP-04`).
//!
//! An [`AggregateQuery`] filters a collection, groups it and computes aggregates. It runs over a
//! [`crate::columnar::ColumnarSnapshot`]: live segment rows in parallel (morsel-driven over row
//! groups), plus the delta documents, all as of one MVCC snapshot.
//!
//! - **Exact semantics**: filters use [`crate::document::Filter::matches_value`], the same
//!   leaf semantics as document queries, so both always select the same documents. Fast paths
//!   evaluate a leaf on each dictionary entry, or find the exact interval of matching encoded
//!   values by binary search with that same function, then run vectorized range kernels.
//! - **Exact numbers**: integer and decimal sums are exact (`i128`, fail on overflow, never
//!   wrap). Float sums are correctly rounded (exact partials, as in `math.fsum`), so results do
//!   not depend on thread count, data layout or CPU (`RULE-OLAP-03`).
//! - **Bounded memory**: a query has a memory budget for its group tables; going over it fails
//!   with a clear error instead of exhausting RAM (`RULE-OLAP-04`).

mod exec;
mod filter;
mod values;

pub use exec::{run, run_documents};
pub use values::{GroupValue, Hll};

use storage_codec::types::PropertyValue;

use crate::document::{Direction, Filter};

/// A grouping key.
#[derive(Clone, Debug, PartialEq)]
pub enum GroupKey {
    /// The value of a field (dotted path).
    Field(String),
    /// Numbers and timestamps bucketed: `offset + floor((v - offset) / width) * width`.
    Bucket {
        field: String,
        width: i64,
        offset: i64,
    },
}

/// An aggregate function.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggFunc {
    /// Rows (with an argument: rows where it is not null).
    Count,
    /// Distinct non-null values, exactly.
    CountDistinct,
    /// Distinct non-null values, estimated with HyperLogLog (about 0.8 % standard error).
    ApproxCountDistinct,
    /// Exact for integers and decimals; correctly rounded for floats.
    Sum,
    /// Mean of numbers (decimal for exact inputs, float otherwise).
    Avg,
    Min,
    Max,
}

/// An expression over fields: arithmetic on numbers (exact for integers and decimals).
#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Field(String),
    Const(PropertyValue),
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Neg(Box<Expr>),
}

/// One output aggregate.
#[derive(Clone, Debug, PartialEq)]
pub struct Aggregate {
    pub func: AggFunc,
    /// `None` only for `Count` (rows).
    pub arg: Option<Expr>,
}

/// An aggregation over one collection.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AggregateQuery {
    pub collection: String,
    pub filter: Option<Filter>,
    pub group_by: Vec<GroupKey>,
    pub aggregates: Vec<Aggregate>,
    /// Output columns (group keys first, then aggregates) to sort by. Without it, rows are in
    /// group-key order.
    pub order_by: Vec<(usize, Direction)>,
    pub limit: Option<usize>,
    /// Memory budget for group tables and distinct sets (`0`: the default, 256 MiB).
    pub memory_budget: usize,
}

/// Default per-query memory budget.
pub const DEFAULT_MEMORY_BUDGET: usize = 256 << 20;
/// Most group keys / aggregates per query.
pub const MAX_QUERY_ITEMS: usize = 64;

/// The result: one row per group (one row in total without grouping).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AggregateResult {
    pub rows: Vec<Vec<PropertyValue>>,
    /// Rows the filter selected (before grouping).
    pub matched: u64,
}
