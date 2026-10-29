//! Analytics: server-side aggregation (`OP_DOC_AGGREGATE`) and column scans (`OP_DOC_SCAN_COLUMNS`),
//! protocol/PROTOCOL.md §5.5-§5.6.

use std::ops::{Add, Mul, Neg, Sub};

use crate::client::Collection;
use crate::error::{invalid, Error, Result};
use crate::value::{encode_value, put_str, put_u32, Doc, Reader, Value, MAX_DEPTH};
use crate::wire::{self, Filter};

pub(crate) const OP_DOC_AGGREGATE: u8 = 0x29;
pub(crate) const OP_DOC_SCAN_COLUMNS: u8 = 0x2A;

const MAX_ITEMS: usize = 64;
const MAX_PAGE_SIZE: u32 = 100_000;

/// An aggregate argument, built with [`field`] and [`lit`] and combined with `+`, `-`, `*` and unary `-`:
/// `field("price") * field("qty")`, `field("qty") - lit(1)`. A `&str` always converts
/// into a field path, never a text constant (use `lit("...")` for text).
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// The value of a field (dotted path).
    Field(String),
    /// A constant.
    Lit(Value),
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Neg(Box<Expr>),
}

/// The value of the field at `path` (dotted for nested maps).
pub fn field(path: &str) -> Expr {
    Expr::Field(path.to_owned())
}

/// A constant: `lit(1)`, `lit(dec("2.5"))`.
pub fn lit(value: impl Into<Value>) -> Expr {
    Expr::Lit(value.into())
}

impl From<&str> for Expr {
    fn from(path: &str) -> Expr {
        field(path)
    }
}

impl From<String> for Expr {
    fn from(path: String) -> Expr {
        Expr::Field(path)
    }
}

impl Add for Expr {
    type Output = Expr;

    fn add(self, rhs: Expr) -> Expr {
        Expr::Add(Box::new(self), Box::new(rhs))
    }
}

impl Sub for Expr {
    type Output = Expr;

    fn sub(self, rhs: Expr) -> Expr {
        Expr::Sub(Box::new(self), Box::new(rhs))
    }
}

impl Mul for Expr {
    type Output = Expr;

    fn mul(self, rhs: Expr) -> Expr {
        Expr::Mul(Box::new(self), Box::new(rhs))
    }
}

impl Neg for Expr {
    type Output = Expr;

    fn neg(self) -> Expr {
        Expr::Neg(Box::new(self))
    }
}

impl Expr {
    fn encode(&self, out: &mut Vec<u8>, depth: usize) -> Result<()> {
        if depth > MAX_DEPTH {
            return Err(invalid("expression nesting is too deep (max 32)"));
        }
        match self {
            Expr::Field(p) => {
                out.push(1);
                put_str(out, p)?;
            }
            Expr::Lit(v) => {
                out.push(2);
                encode_value(v, out, 0)?;
            }
            Expr::Add(a, b) | Expr::Sub(a, b) | Expr::Mul(a, b) => {
                out.push(match self {
                    Expr::Add(..) => 3,
                    Expr::Sub(..) => 4,
                    _ => 5,
                });
                a.encode(out, depth + 1)?;
                b.encode(out, depth + 1)?;
            }
            Expr::Neg(a) => {
                out.push(6);
                a.encode(out, depth + 1)?;
            }
        }
        Ok(())
    }
}

/// One aggregate function, built with [`count`], [`count_of`], [`sum`], [`avg`], [`min`], [`max`],
/// [`count_distinct`] or [`approx_count_distinct`]: `sum("qty")`, `avg(field("price") * field("qty"))`.
#[derive(Debug, Clone, PartialEq)]
pub struct Agg {
    func: u8, // 1 count, 2 sum, 3 avg, 4 min, 5 max, 6 count_distinct, 7 approx_count_distinct
    arg: Option<Expr>,
}

fn agg(func: u8, arg: impl Into<Expr>) -> Agg {
    Agg { func, arg: Some(arg.into()) }
}

/// Number of matching rows (of each group).
pub fn count() -> Agg {
    Agg { func: 1, arg: None }
}

/// Number of non-null values of `arg`.
pub fn count_of(arg: impl Into<Expr>) -> Agg {
    agg(1, arg)
}

/// Exact sum of `arg` (integers and decimals never round; floats are correctly rounded), skipping non-numeric
/// values (`Value::Null` when there are none).
pub fn sum(arg: impl Into<Expr>) -> Agg {
    agg(2, arg)
}

/// Mean of `arg`: a decimal (6 more fractional digits) for exact inputs, else a float.
pub fn avg(arg: impl Into<Expr>) -> Agg {
    agg(3, arg)
}

/// Smallest value of `arg` in the total sort order.
pub fn min(arg: impl Into<Expr>) -> Agg {
    agg(4, arg)
}

/// Largest value of `arg` in the total sort order.
pub fn max(arg: impl Into<Expr>) -> Agg {
    agg(5, arg)
}

/// Exact number of distinct values of `arg`.
pub fn count_distinct(arg: impl Into<Expr>) -> Agg {
    agg(6, arg)
}

/// Estimated number of distinct values of `arg` (HyperLogLog, about 0.8 % error).
pub fn approx_count_distinct(arg: impl Into<Expr>) -> Agg {
    agg(7, arg)
}

/// A group key: a field's value (a `&str` converts into one) or a [`bucket`]. Its output column is named by its
/// field path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupKey {
    Field(String),
    /// `offset + floor((v - offset) / width) * width`.
    Bucket {
        field: String,
        width: i64,
        offset: i64,
    },
}

/// Groups by `offset + floor((v - offset) / width) * width` of the field at `path`.
pub fn bucket(path: &str, width: i64, offset: i64) -> GroupKey {
    GroupKey::Bucket { field: path.to_owned(), width, offset }
}

impl From<&str> for GroupKey {
    fn from(path: &str) -> GroupKey {
        GroupKey::Field(path.to_owned())
    }
}

impl From<String> for GroupKey {
    fn from(path: String) -> GroupKey {
        GroupKey::Field(path)
    }
}

impl GroupKey {
    fn name(&self) -> &str {
        match self {
            GroupKey::Field(p) | GroupKey::Bucket { field: p, .. } => p,
        }
    }
}

/// What [`Collection::aggregate`] computes:
/// `AggregateQuery::new().agg("orders", count()).agg("revenue", sum(field("price") * field("qty")))
/// .group_by("region").order_by("-revenue").limit(10)`.
///
/// Result columns are the group keys (named by their path) followed by the aggregates (named by `agg`), in order.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AggregateQuery {
    filter: Option<Filter>,
    group_by: Vec<GroupKey>,
    aggs: Vec<(String, Agg)>,
    order_by: Vec<(String, bool)>,
    limit: u32,
    memory_budget: u64,
    spill_budget: u64,
}

impl AggregateQuery {
    /// No aggregates, no grouping, every document.
    pub fn new() -> AggregateQuery {
        AggregateQuery::default()
    }

    /// Adds an aggregate whose output column is `name`.
    pub fn agg(mut self, name: &str, agg: Agg) -> AggregateQuery {
        self.aggs.push((name.to_owned(), agg));
        self
    }

    /// Only documents matching `filter`.
    pub fn filter(mut self, filter: Filter) -> AggregateQuery {
        self.filter = Some(filter);
        self
    }

    /// Adds a group key: a field path or a [`bucket`]. Without any, the result is one row of totals.
    pub fn group_by(mut self, key: impl Into<GroupKey>) -> AggregateQuery {
        self.group_by.push(key.into());
        self
    }

    /// Adds a sort column, by output column name: `"revenue"` ascending, `"-revenue"` descending.
    pub fn order_by(mut self, column: &str) -> AggregateQuery {
        match column.strip_prefix('-') {
            Some(c) => self.order_by.push((c.to_owned(), true)),
            None => self.order_by.push((column.to_owned(), false)),
        }
        self
    }

    /// At most `n` groups (default: every group).
    pub fn limit(mut self, n: u32) -> AggregateQuery {
        self.limit = n;
        self
    }

    /// Bytes of group tables kept in memory before spilling to disk (0: server default, 256 MiB).
    pub fn memory_budget(mut self, bytes: u64) -> AggregateQuery {
        self.memory_budget = bytes;
        self
    }

    /// Most bytes spilled to disk (0: server default, 16 GiB).
    pub fn spill_budget(mut self, bytes: u64) -> AggregateQuery {
        self.spill_budget = bytes;
        self
    }

    /// Output column names: group keys, then aggregates.
    pub fn columns(&self) -> Vec<String> {
        self.group_by.iter().map(|k| k.name().to_owned()).chain(self.aggs.iter().map(|(n, _)| n.clone())).collect()
    }
}

pub(crate) fn encode_aggregate(collection: &str, q: &AggregateQuery) -> Result<Vec<u8>> {
    if q.group_by.len() > MAX_ITEMS || q.aggs.len() > MAX_ITEMS || q.order_by.len() > MAX_ITEMS {
        return Err(invalid("at most 64 group keys, aggregates and order columns"));
    }
    let columns = q.columns();
    for (i, c) in columns.iter().enumerate() {
        if c.is_empty() {
            return Err(invalid("every aggregate needs a name"));
        }
        if columns[..i].contains(c) {
            return Err(invalid(format!("duplicate output column {c:?}")));
        }
    }
    let mut out = Vec::new();
    put_str(&mut out, collection)?;
    match &q.filter {
        None => out.push(0),
        Some(f) => {
            out.push(1);
            f.encode(&mut out, 0)?;
        }
    }
    put_u32(&mut out, q.group_by.len())?;
    for k in &q.group_by {
        match k {
            GroupKey::Field(p) => {
                out.push(1);
                put_str(&mut out, p)?;
            }
            GroupKey::Bucket { field, width, offset } => {
                if *width <= 0 {
                    return Err(invalid("bucket width must be positive"));
                }
                out.push(2);
                put_str(&mut out, field)?;
                out.extend_from_slice(&width.to_le_bytes());
                out.extend_from_slice(&offset.to_le_bytes());
            }
        }
    }
    put_u32(&mut out, q.aggs.len())?;
    for (_, a) in &q.aggs {
        out.push(a.func);
        match &a.arg {
            None => out.push(0),
            Some(e) => {
                out.push(1);
                e.encode(&mut out, 0)?;
            }
        }
    }
    put_u32(&mut out, q.order_by.len())?;
    for (name, desc) in &q.order_by {
        let Some(i) = columns.iter().position(|c| c == name) else {
            return Err(invalid(format!(
                "cannot order by {name:?}: not an output column (one of {})",
                columns.join(", ")
            )));
        };
        put_u32(&mut out, i)?;
        out.push(u8::from(*desc));
    }
    out.extend_from_slice(&q.limit.to_le_bytes());
    out.extend_from_slice(&q.memory_budget.to_le_bytes());
    out.extend_from_slice(&q.spill_budget.to_le_bytes());
    Ok(out)
}

/// The result of [`Collection::aggregate`]: one row per group, sorted by the order columns then the group keys.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregateResult {
    /// Group keys, then aggregates.
    pub columns: Vec<String>,
    /// One value per column in each row.
    pub rows: Vec<Vec<Value>>,
    /// Documents that matched the filter.
    pub matched: u64,
    /// Column row groups skipped by zone maps without reading them.
    pub pruned_row_groups: u64,
    /// Greater than 0 when the query outgrew its memory budget and spilled to disk.
    pub spilled_bytes: u64,
}

impl AggregateResult {
    /// Index of the column `name`.
    pub fn column(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c == name)
    }

    /// The value of column `name` in row `row`.
    pub fn get(&self, row: usize, name: &str) -> Option<&Value> {
        self.rows.get(row)?.get(self.column(name)?)
    }

    /// Each row as a column name -> value map.
    pub fn maps(&self) -> Vec<Doc> {
        self.rows.iter().map(|r| self.columns.iter().cloned().zip(r.iter().cloned()).collect()).collect()
    }
}

/// `u64 matched | u64 pruned | u64 spilled | u32 columns | u32 rows | rows x columns x value` (columns unnamed).
pub(crate) fn decode_aggregate_result(body: &[u8]) -> Result<AggregateResult> {
    let mut r = Reader::new(body);
    let (matched, pruned_row_groups, spilled_bytes) = (r.u64()?, r.u64()?, r.u64()?);
    let cols = r.u32()? as usize;
    let n = r.count(cols)?;
    let mut rows = Vec::with_capacity(n);
    for _ in 0..n {
        rows.push((0..cols).map(|_| r.value(0)).collect::<Result<Vec<_>>>()?);
    }
    if r.remaining() != 0 {
        return Err(invalid("trailing bytes after aggregate result"));
    }
    Ok(AggregateResult { columns: Vec::new(), rows, matched, pruned_row_groups, spilled_bytes })
}

pub(crate) fn encode_scan(
    collection: &str,
    filter: Option<&Filter>,
    fields: &[&str],
    after_key: Option<&str>,
    limit: u32,
) -> Result<Vec<u8>> {
    if fields.len() > MAX_ITEMS {
        return Err(invalid("a column scan reads at most 64 fields"));
    }
    if limit > MAX_PAGE_SIZE {
        return Err(invalid(format!("page size must be at most {MAX_PAGE_SIZE}")));
    }
    let mut out = Vec::new();
    put_str(&mut out, collection)?;
    match filter {
        None => out.push(0),
        Some(f) => {
            out.push(1);
            f.encode(&mut out, 0)?;
        }
    }
    put_u32(&mut out, fields.len())?;
    for f in fields {
        put_str(&mut out, f)?;
    }
    match after_key {
        None => out.push(0),
        Some(k) => {
            out.push(1);
            put_str(&mut out, k)?;
        }
    }
    out.extend_from_slice(&limit.to_le_bytes());
    Ok(out)
}

pub(crate) struct ScanPage {
    pub total: u64,
    pub next_key: Option<String>,
    pub fields: usize,
    pub rows: Vec<(String, Vec<Value>)>,
}

/// `u64 total | u8 has_next | str next? | u32 fields | u32 rows | rows x (str key | fields x value)`.
pub(crate) fn decode_scan_page(body: &[u8]) -> Result<ScanPage> {
    let mut r = Reader::new(body);
    let total = r.u64()?;
    let next_key = if r.u8()? != 0 { Some(r.str()?) } else { None };
    let fields = r.u32()? as usize;
    let n = r.count(4 + fields)?;
    let mut rows = Vec::with_capacity(n);
    for _ in 0..n {
        let key = r.str()?;
        rows.push((key, (0..fields).map(|_| r.value(0)).collect::<Result<Vec<_>>>()?));
    }
    if r.remaining() != 0 {
        return Err(invalid("trailing bytes after scan page"));
    }
    Ok(ScanPage { total, next_key, fields, rows })
}

/// One document read by [`Collection::scan_columns`].
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRow {
    pub key: String,
    /// One value per requested field, in order (`Value::Null` for a missing field).
    pub values: Vec<Value>,
    /// Requested field path -> value.
    pub fields: Doc,
}

impl ColumnRow {
    /// The value of the requested field `path`.
    pub fn get(&self, path: &str) -> Option<&Value> {
        self.fields.get(path)
    }
}

impl Collection<'_> {
    /// Filters, groups and aggregates the collection on the server, exactly and as of one snapshot (vectorized
    /// over column segments when columnar is enabled, else over the row store). An unknown `order_by` column is
    /// `Error::Invalid` before anything is sent; `Error::Overloaded` only once retries with backoff are exhausted.
    pub fn aggregate(&self, query: AggregateQuery) -> Result<AggregateResult> {
        let body = encode_aggregate(&self.name, &query)?;
        let mut res = decode_aggregate_result(&self.client.call(OP_DOC_AGGREGATE, &body)?.body)?;
        let columns = query.columns();
        if res.rows.first().is_some_and(|r| r.len() != columns.len()) {
            return Err(Error::Server("aggregate result has an unexpected number of columns".into()));
        }
        res.columns = columns;
        Ok(res)
    }

    /// Also keeps the collection as column segments (built in the background) so `aggregate` and
    /// `scan_columns` run vectorized over them. Results are the same either way.
    pub fn enable_columnar(&self) -> Result<()> {
        self.client.call(wire::OP_DOC_ADMIN, &wire::encode_admin(10, &self.name, None)?).map(|_| ())
    }

    /// Deletes the column segments; analytics then read the row store.
    pub fn disable_columnar(&self) -> Result<()> {
        self.client.call(wire::OP_DOC_ADMIN, &wire::encode_admin(11, &self.name, None)?).map(|_| ())
    }

    /// The `fields` (dotted paths; at most 64) of every document matching `filter` (`None`: all), in key order,
    /// fetched lazily `page_size` rows per request (0: 1,000; at most 100,000) without transferring whole
    /// documents.
    pub fn scan_columns(&self, fields: &[&str], filter: impl Into<Option<Filter>>, page_size: u32) -> ColumnScan<'_> {
        ColumnScan {
            collection: self,
            fields: fields.iter().map(|f| (*f).to_owned()).collect(),
            filter: filter.into(),
            page_size,
            page: Vec::new().into_iter(),
            after: None,
            done: false,
            total: None,
        }
    }
}

/// The iterator returned by [`Collection::scan_columns`].
#[derive(Debug)]
pub struct ColumnScan<'a> {
    collection: &'a Collection<'a>,
    fields: Vec<String>,
    filter: Option<Filter>,
    page_size: u32,
    page: std::vec::IntoIter<(String, Vec<Value>)>,
    after: Option<String>,
    done: bool,
    total: Option<u64>,
}

impl ColumnScan<'_> {
    /// Total number of matching documents, known once the first page has been fetched.
    pub fn total(&self) -> Option<u64> {
        self.total
    }

    fn fetch(&mut self) -> Result<()> {
        let fields: Vec<&str> = self.fields.iter().map(String::as_str).collect();
        let body =
            encode_scan(&self.collection.name, self.filter.as_ref(), &fields, self.after.as_deref(), self.page_size)?;
        let p = decode_scan_page(&self.collection.client.call(OP_DOC_SCAN_COLUMNS, &body)?.body)?;
        if p.fields != fields.len() {
            return Err(Error::Server("scan page has an unexpected number of fields".into()));
        }
        self.total = Some(p.total);
        self.done = p.next_key.is_none();
        self.after = p.next_key;
        self.page = p.rows.into_iter();
        Ok(())
    }
}

impl Iterator for ColumnScan<'_> {
    type Item = Result<ColumnRow>;

    fn next(&mut self) -> Option<Result<ColumnRow>> {
        loop {
            if let Some((key, values)) = self.page.next() {
                let fields = self.fields.iter().cloned().zip(values.iter().cloned()).collect();
                return Some(Ok(ColumnRow { key, values, fields }));
            }
            if self.done {
                return None;
            }
            if let Err(e) = self.fetch() {
                self.done = true;
                return Some(Err(e));
            }
        }
    }
}
