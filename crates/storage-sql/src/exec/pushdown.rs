//! Aggregate push-down (`TASK-130`): a single-table `SELECT ... GROUP BY` whose filter, keys and
//! aggregates the store computes exactly runs as one `OP_DOC_AGGREGATE` (vectorized and parallel
//! over column segments on a columnar table) instead of streaming every row to the gateway.
//!
//! Push-down is all or nothing and only where the store's result equals the gateway's own: the
//! WHERE clause must translate to an equivalent storage filter (not a wider one), keys and
//! arguments must be plain columns of types compared the same way on both sides, and the
//! aggregates are COUNT / COUNT(DISTINCT) / SUM / AVG / MIN / MAX over exact types. Anything
//! else (floats, defaults filled in for missing fields, an open transaction's own writes,
//! window functions, subqueries) runs locally as before. HAVING, the select list, ORDER BY,
//! DISTINCT and LIMIT then run over the groups as usual.

use storine::Filter;

use crate::ast::{BinaryOp, DataType, Expr, OrderBy, Select, SelectItem, TableRef, UnaryOp};
use crate::catalog::{Row, Table};
use crate::datum::{self, cast, sql_cmp, Datum};
use crate::error::SqlResult;
use crate::exec::eval::{find_column, has_aggregate, is_aggregate, walk, Env, Exec};
use crate::exec::func;
use crate::exec::select::{conjuncts, has_subquery, table_cols, ColMeta, Rel};

/// Per group: a representative row (group key columns set) and its aggregate values.
pub(crate) type Groups = Vec<(Row, Vec<(Expr, Datum)>)>;

/// How one SQL aggregate is read from the store's result columns.
#[derive(Debug, Clone)]
enum Out {
    /// `COUNT(*)`, `COUNT(x)`, `COUNT(DISTINCT x)`.
    Count(usize),
    /// `SUM(x)`: integers sum as numeric, as the gateway sums them.
    Sum(usize),
    /// `AVG(x)` from a sum and a count, with the dialect's scale rule.
    Avg(usize, usize),
    /// `MIN(x)` / `MAX(x)` of column `ci`.
    Value(usize, usize),
}

/// A pushed-down aggregation.
#[derive(Debug)]
pub(crate) struct Plan {
    table: std::sync::Arc<Table>,
    cols: Vec<ColMeta>,
    query: storine::AggregateQuery,
    /// Table columns of the group keys, in key order.
    keys: Vec<usize>,
    /// Each SQL aggregate expression with how to read it.
    aggs: Vec<(Expr, Out)>,
    filter_text: String,
}

/// Grouping and comparison agree with the store for these types.
fn key_type(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Bool
            | DataType::SmallInt
            | DataType::Int
            | DataType::BigInt
            | DataType::Numeric(Some(_))
            | DataType::Text
            | DataType::Varchar(_)
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Date
            | DataType::Uuid
    )
}

fn exact_number(t: &DataType) -> bool {
    matches!(
        t,
        DataType::SmallInt | DataType::Int | DataType::BigInt | DataType::Numeric(_)
    )
}

fn text_type(t: &DataType) -> bool {
    matches!(t, DataType::Text | DataType::Varchar(_))
}

impl Exec<'_> {
    /// Whether this session allows push-down (`SET storine_pushdown = off` disables it).
    pub(crate) fn pushdown_enabled(&self) -> bool {
        !self.s.vars.get("storine_pushdown").is_some_and(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "off" | "false" | "0"
            )
        })
    }

    /// A column of the table usable by push-down (no default filled in for missing fields).
    fn plain_column(&self, table: &Table, qual: Option<&str>, e: &Expr) -> Option<usize> {
        let ci = self.own_column(table, qual, e)?;
        table.columns[ci].missing.is_none().then_some(ci)
    }

    /// The constant `e` as a storage value of column `ci`'s type, only if the conversion is
    /// exact (so the store compares exactly what SQL compares).
    fn exact_value(
        &self,
        table: &Table,
        ci: usize,
        e: &Expr,
        outer: Option<&Env<'_>>,
    ) -> Option<storine::Value> {
        let ty = &table.columns[ci].ty;
        let v = self.constant(e, outer)?;
        if v.is_null() {
            return None;
        }
        let same_class = match ty {
            t if exact_number(t) => matches!(v, Datum::Int(_) | Datum::Numeric(_)),
            t if text_type(t) => matches!(v, Datum::Text(_)),
            DataType::Bool => matches!(v, Datum::Bool(_)),
            DataType::Timestamp | DataType::TimestampTz | DataType::Date => {
                matches!(v, Datum::Timestamp(_) | Datum::Date(_) | Datum::Text(_))
            }
            DataType::Uuid => matches!(v, Datum::Uuid(_) | Datum::Text(_)),
            _ => false,
        };
        if !same_class {
            return None;
        }
        let c = cast(&v, ty).ok()?;
        let exact = match &v {
            Datum::Text(t) if !text_type(ty) => {
                c.as_text() == *t || sql_cmp(&c, &cast(&v, ty).ok()?).is_some()
            }
            _ => sql_cmp(&c, &v) == Some(std::cmp::Ordering::Equal),
        };
        exact.then(|| datum::to_value(&c, ty))
    }

    /// A storage filter selecting exactly the rows for which `c` is true, or `None`.
    fn exact_filter(
        &self,
        table: &Table,
        qual: Option<&str>,
        c: &Expr,
        outer: Option<&Env<'_>>,
    ) -> Option<Filter> {
        let field = |ci: usize| table.columns[ci].field.clone();
        match c {
            Expr::Binary {
                op: BinaryOp::And,
                left,
                right,
            } => Some(storine::and(vec![
                self.exact_filter(table, qual, left, outer)?,
                self.exact_filter(table, qual, right, outer)?,
            ])),
            Expr::Binary {
                op: BinaryOp::Or,
                left,
                right,
            } => Some(storine::or(vec![
                self.exact_filter(table, qual, left, outer)?,
                self.exact_filter(table, qual, right, outer)?,
            ])),
            Expr::Binary { op, left, right }
                if matches!(
                    op,
                    BinaryOp::Eq
                        | BinaryOp::NotEq
                        | BinaryOp::Lt
                        | BinaryOp::LtEq
                        | BinaryOp::Gt
                        | BinaryOp::GtEq
                ) =>
            {
                let (ci, k, flipped) = match self.plain_column(table, qual, left) {
                    Some(ci) => (ci, right, false),
                    None => (self.plain_column(table, qual, right)?, left, true),
                };
                if !key_type(&table.columns[ci].ty) && !exact_number(&table.columns[ci].ty) {
                    return None;
                }
                let v = self.exact_value(table, ci, k, outer)?;
                let f = field(ci);
                Some(match (op, flipped) {
                    (BinaryOp::Eq, _) => storine::eq(&f, v),
                    // SQL: NULL <> x is not true; the store's "not equal" matches missing fields.
                    (BinaryOp::NotEq, _) => {
                        storine::and(vec![storine::ne(&f, v), storine::exists(&f)])
                    }
                    (BinaryOp::Lt, false) | (BinaryOp::Gt, true) => storine::lt(&f, v),
                    (BinaryOp::LtEq, false) | (BinaryOp::GtEq, true) => storine::le(&f, v),
                    (BinaryOp::Gt, false) | (BinaryOp::Lt, true) => storine::gt(&f, v),
                    _ => storine::ge(&f, v),
                })
            }
            Expr::Between {
                expr,
                low,
                high,
                negated: false,
            } => {
                let ci = self.plain_column(table, qual, expr)?;
                let lo = self.exact_value(table, ci, low, outer)?;
                let hi = self.exact_value(table, ci, high, outer)?;
                Some(storine::between(&field(ci), lo, hi))
            }
            Expr::InList {
                expr,
                list,
                negated: false,
            } => {
                let ci = self.plain_column(table, qual, expr)?;
                let mut vals = Vec::new();
                for x in list {
                    // NULL in the list never makes IN true.
                    if self.constant(x, outer)?.is_null() {
                        continue;
                    }
                    vals.push(self.exact_value(table, ci, x, outer)?);
                }
                Some(storine::is_in(&field(ci), vals))
            }
            Expr::IsNull { expr, negated } => {
                let ci = self.plain_column(table, qual, expr)?;
                Some(if *negated {
                    storine::exists(&field(ci))
                } else {
                    storine::missing(&field(ci))
                })
            }
            Expr::Like {
                expr,
                pattern,
                negated: false,
                case_insensitive: false,
                escape: None,
            } => {
                let ci = self.plain_column(table, qual, expr)?;
                if !text_type(&table.columns[ci].ty) {
                    return None;
                }
                // Exactly `literal%`: a prefix match.
                let p = self.constant(pattern, outer)?.as_text();
                let lit = p.strip_suffix('%')?;
                if lit.is_empty() || lit.contains(['%', '_', '\\']) {
                    return None;
                }
                Some(storine::prefix(&field(ci), lit))
            }
            _ => None,
        }
    }

    /// The store expression for a SUM argument: a numeric column, or `+ - *` / unary minus of
    /// columns and numeric constants where every operation has a NUMERIC operand (the gateway's
    /// result is then NUMERIC, exact and overflow-free, as the store's is; integer-only
    /// arithmetic can overflow in the gateway and stays local).
    fn sum_expr(
        &self,
        table: &Table,
        qual: Option<&str>,
        e: &Expr,
        outer: Option<&Env<'_>>,
    ) -> Option<storine::Expr> {
        let (x, _) = self.arith(table, qual, e, outer)?;
        Some(x)
    }

    /// `(store expression, is NUMERIC)` of an exact arithmetic expression.
    fn arith(
        &self,
        table: &Table,
        qual: Option<&str>,
        e: &Expr,
        outer: Option<&Env<'_>>,
    ) -> Option<(storine::Expr, bool)> {
        if let Some(ci) = self.plain_column(table, qual, e) {
            let ty = &table.columns[ci].ty;
            return exact_number(ty).then(|| {
                (
                    storine::field(&table.columns[ci].field),
                    matches!(ty, DataType::Numeric(_)),
                )
            });
        }
        match e {
            Expr::Binary {
                op: op @ (BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul),
                left,
                right,
            } => {
                let (a, na) = self.arith(table, qual, left, outer)?;
                let (b, nb) = self.arith(table, qual, right, outer)?;
                if !na && !nb {
                    return None;
                }
                Some((
                    match op {
                        BinaryOp::Add => a + b,
                        BinaryOp::Sub => a - b,
                        _ => a * b,
                    },
                    true,
                ))
            }
            Expr::Unary {
                op: UnaryOp::Neg,
                expr,
            } => {
                let (a, n) = self.arith(table, qual, expr, outer)?;
                // Negating an integer can overflow (i64::MIN) in the gateway.
                n.then(|| (-a, true))
            }
            other => match self.constant(other, outer)? {
                Datum::Numeric(d) => Some((
                    storine::lit(storine::Value::Decimal(
                        storine::Decimal::new(d.mantissa(), d.scale()).ok()?,
                    )),
                    true,
                )),
                Datum::Int(i) => Some((storine::lit(storine::Value::Int(i)), false)),
                _ => None,
            },
        }
    }

    /// A push-down plan for `sel`, or `None` when it must run locally.
    pub(crate) fn plan_pushdown(
        &self,
        sel: &Select,
        order_by: &[OrderBy],
        outer: Option<&Env<'_>>,
    ) -> SqlResult<Option<Plan>> {
        if !self.pushdown_enabled() {
            return Ok(None);
        }
        let [TableRef::Named {
            schema: None,
            name,
            alias,
        }] = sel.from.as_slice()
        else {
            return Ok(None);
        };
        if self.cte(name).is_some() || name.starts_with('@') {
            return Ok(None);
        }
        let Some(table) = self.s.engine.find_table(name)? else {
            return Ok(None);
        };
        // An open transaction's own writes (or validated reads) need row-level reads.
        if let Some(t) = self.s.txn.as_ref() {
            if t.validate_reads || t.entries(&table.collection).next().is_some() {
                return Ok(None);
            }
        }
        let qual = alias.as_ref().map_or(name.as_str(), |a| a.name.as_str());
        let cols = Rel {
            cols: table_cols(&table),
            rows: Vec::new(),
        }
        .requalify(alias, Some(&table.name))?
        .cols;
        // The select list (a wildcard means non-grouped columns: local).
        let mut items: Vec<&Expr> = Vec::new();
        for it in &sel.items {
            match it {
                SelectItem::Expr { expr, .. } => items.push(expr),
                SelectItem::Wildcard(_) => return Ok(None),
            }
        }
        let all: Vec<&Expr> = items
            .iter()
            .copied()
            .chain(sel.having.iter())
            .chain(order_by.iter().map(|o| &o.expr))
            .collect();
        if sel.group_by.is_empty() && !all.iter().any(|e| has_aggregate(e)) {
            return Ok(None);
        }
        let mut unsupported = false;
        for e in all
            .iter()
            .copied()
            .chain(sel.where_.iter())
            .chain(sel.group_by.iter())
        {
            if has_subquery(e) {
                unsupported = true;
            }
            walk(e, &mut |x| {
                if matches!(x, Expr::Function { over: Some(_), .. }) {
                    unsupported = true;
                }
            });
        }
        if unsupported {
            return Ok(None);
        }
        // WHERE: every conjunct exactly.
        let mut filters = Vec::new();
        if let Some(w) = &sel.where_ {
            for c in conjuncts(w) {
                match self.exact_filter(&table, Some(qual), c, outer) {
                    Some(f) => filters.push(f),
                    None => return Ok(None),
                }
            }
        }
        // GROUP BY: plain columns (ordinals and output aliases resolved as `select` does).
        let mut keys = Vec::new();
        for g in &sel.group_by {
            let g = match g {
                Expr::Number(n) => match n
                    .parse::<usize>()
                    .ok()
                    .and_then(|i| items.get(i.wrapping_sub(1)))
                {
                    Some(e) => *e,
                    None => return Ok(None),
                },
                Expr::Column(p)
                    if p.len() == 1 && find_column(&cols, None, &p[0]).ok().flatten().is_none() =>
                {
                    return Ok(None)
                }
                other => other,
            };
            match self.plain_column(&table, Some(qual), g) {
                Some(ci) if key_type(&table.columns[ci].ty) => keys.push(ci),
                _ => return Ok(None),
            }
        }
        // Outside aggregate calls only group columns may appear.
        let mut stray = false;
        let aliases: Vec<&str> = sel
            .items
            .iter()
            .filter_map(|it| match it {
                SelectItem::Expr { alias: Some(a), .. } => Some(a.as_str()),
                _ => None,
            })
            .collect();
        for e in &all {
            check_grouped(self, &table, qual, &aliases, e, &keys, &mut stray);
        }
        if stray {
            return Ok(None);
        }
        // The aggregates, collected the way `select` collects them.
        let mut agg_exprs: Vec<Expr> = Vec::new();
        for e in &all {
            walk(e, &mut |x| {
                if let Expr::Function {
                    name,
                    args,
                    over: None,
                    ..
                } = x
                {
                    if is_aggregate(name)
                        && !(matches!(name.as_str(), "min" | "max") && args.len() > 1)
                        && !agg_exprs.contains(x)
                    {
                        agg_exprs.push(x.clone());
                    }
                }
            });
        }
        let mut query = storine::AggregateQuery::new();
        if !filters.is_empty() {
            query = query.filter(if filters.len() == 1 {
                filters.remove(0)
            } else {
                storine::and(filters)
            });
        }
        for &ci in &keys {
            query = query.group_by(table.columns[ci].field.as_str());
        }
        let mut aggs = Vec::new();
        let mut n = keys.len();
        let mut next =
            |q: storine::AggregateQuery, a: storine::Agg| -> (storine::AggregateQuery, usize) {
                let i = n;
                n += 1;
                (q.agg(&format!("a{i}"), a), i)
            };
        for a in agg_exprs {
            let Expr::Function {
                name,
                args,
                star,
                distinct,
                ..
            } = &a
            else {
                return Ok(None);
            };
            let arg = args.first();
            let col = arg.and_then(|x| self.plain_column(&table, Some(qual), x));
            let out = match (name.as_str(), *star, *distinct, col) {
                ("count" | "count_big", true, _, _) => {
                    let (q, i) = next(query, storine::count());
                    query = q;
                    Out::Count(i)
                }
                ("count" | "count_big", false, false, Some(ci)) if args.len() == 1 => {
                    let (q, i) = next(
                        query,
                        storine::count_of(storine::field(&table.columns[ci].field)),
                    );
                    query = q;
                    Out::Count(i)
                }
                ("count" | "count_big", false, true, Some(ci))
                    if args.len() == 1 && key_type(&table.columns[ci].ty) =>
                {
                    let (q, i) = next(
                        query,
                        storine::count_distinct(storine::field(&table.columns[ci].field)),
                    );
                    query = q;
                    Out::Count(i)
                }
                ("sum", false, false, _) if args.len() == 1 => {
                    let Some(e) = self.sum_expr(&table, Some(qual), &args[0], outer) else {
                        return Ok(None);
                    };
                    let (q, i) = next(query, storine::sum(e));
                    query = q;
                    Out::Sum(i)
                }
                ("avg", false, false, Some(ci))
                    if args.len() == 1 && exact_number(&table.columns[ci].ty) =>
                {
                    let f = &table.columns[ci].field;
                    let (q, s) = next(query, storine::sum(storine::field(f)));
                    let (q, c) = next(q, storine::count_of(storine::field(f)));
                    query = q;
                    Out::Avg(s, c)
                }
                ("min" | "max", false, false, Some(ci))
                    if args.len() == 1 && key_type(&table.columns[ci].ty) =>
                {
                    let f = storine::field(&table.columns[ci].field);
                    let (q, i) = next(
                        query,
                        if name == "min" {
                            storine::min(f)
                        } else {
                            storine::max(f)
                        },
                    );
                    query = q;
                    Out::Value(i, ci)
                }
                _ => return Ok(None),
            };
            aggs.push((a, out));
        }
        let filter_text = sel
            .where_
            .as_ref()
            .map_or_else(|| "none".to_owned(), ToString::to_string);
        Ok(Some(Plan {
            table,
            cols,
            query,
            keys,
            aggs,
            filter_text,
        }))
    }

    /// Runs a plan: the table's columns and, per group, a representative row (group keys set)
    /// with its aggregate values. `None` when the store refused it (the query then runs
    /// locally, so errors keep the gateway's own semantics).
    pub(crate) fn run_pushdown(&self, plan: Plan) -> SqlResult<Option<(Vec<ColMeta>, Groups)>> {
        let Ok(res) = self
            .s
            .engine
            .store
            .aggregate(&plan.table.collection, plan.query)
        else {
            return Ok(None);
        };
        let t = &plan.table;
        let mut work = Vec::with_capacity(res.rows.len());
        for r in &res.rows {
            let mut rep: Row = vec![Datum::Null; t.columns.len()];
            for (k, &ci) in plan.keys.iter().enumerate() {
                rep[ci] = datum::from_value(&r[k], Some(&t.columns[ci].ty));
            }
            let mut aggs = Vec::with_capacity(plan.aggs.len());
            for (e, out) in &plan.aggs {
                let d = match *out {
                    Out::Count(i) => match &r[i] {
                        storine::Value::Int(c) => Datum::Int(*c),
                        _ => Datum::Int(0),
                    },
                    Out::Sum(i) => match datum::from_value(&r[i], None) {
                        Datum::Int(x) => Datum::Numeric(storage_types::Decimal::from_i64(x)),
                        other => other,
                    },
                    Out::Avg(s, c) => {
                        let n = match &r[c] {
                            storine::Value::Int(n) => *n,
                            _ => 0,
                        };
                        match datum::from_value(&r[s], None) {
                            _ if n == 0 => Datum::Null,
                            Datum::Int(x) => {
                                func::avg_exact(self, storage_types::Decimal::from_i64(x), 0, n)?
                            }
                            Datum::Numeric(d) => func::avg_exact(self, d, d.scale(), n)?,
                            _ => return Ok(None),
                        }
                    }
                    Out::Value(i, ci) => datum::from_value(&r[i], Some(&t.columns[ci].ty)),
                };
                aggs.push((e.clone(), d));
            }
            work.push((rep, aggs));
        }
        Ok(Some((plan.cols, work)))
    }

    /// `EXPLAIN` text of a pushed-down aggregation.
    pub(crate) fn explain_pushdown(plan: &Plan) -> String {
        let keys: Vec<&str> = plan
            .keys
            .iter()
            .map(|&ci| plan.table.columns[ci].name.as_str())
            .collect();
        format!(
            "aggregate push-down to Storine over {} of {}: filter {}; group by {}; {} aggregate(s); HAVING / ORDER BY / LIMIT in the gateway",
            if plan.table.columnar.is_some() { "column segments" } else { "the row store" },
            plan.table.name,
            plan.filter_text,
            if keys.is_empty() { "nothing".to_owned() } else { keys.join(", ") },
            plan.aggs.len()
        )
    }
}

/// Sets `stray` if `e` references, outside aggregate calls, a column that is not a group key.
fn check_grouped(
    ex: &Exec<'_>,
    table: &Table,
    qual: &str,
    aliases: &[&str],
    e: &Expr,
    keys: &[usize],
    stray: &mut bool,
) {
    match e {
        Expr::Function {
            name, over: None, ..
        } if is_aggregate(name) => {}
        // An output alias (ORDER BY s) names a select-list expression, checked on its own.
        Expr::Column(p)
            if p.len() == 1
                && ex.own_column(table, Some(qual), e).is_none()
                && aliases.iter().any(|a| a.eq_ignore_ascii_case(&p[0])) => {}
        Expr::Column(_) => {
            if !ex
                .own_column(table, Some(qual), e)
                .is_some_and(|ci| keys.contains(&ci))
            {
                *stray = true;
            }
        }
        other => match children(other) {
            Some(cs) => {
                for c in cs {
                    check_grouped(ex, table, qual, aliases, c, keys, stray);
                }
            }
            // An expression kind not listed: refuse if it reads any column at all.
            None => walk(other, &mut |x| {
                if matches!(x, Expr::Column(_)) {
                    *stray = true;
                }
            }),
        },
    }
}

/// Direct sub-expressions of `e` for the shapes a grouped select list uses; `None` for other
/// kinds (opaque to the grouping check).
fn children(e: &Expr) -> Option<Vec<&Expr>> {
    Some(match e {
        Expr::Binary { left, right, .. } => vec![left, right],
        Expr::Unary { expr, .. } | Expr::Cast { expr, .. } | Expr::IsNull { expr, .. } => {
            vec![expr]
        }
        Expr::Function { args, .. } => args.iter().collect(),
        Expr::Between {
            expr, low, high, ..
        } => vec![expr, low, high],
        Expr::InList { expr, list, .. } => std::iter::once(&**expr).chain(list).collect(),
        Expr::Case {
            operand,
            whens,
            else_,
        } => {
            let mut v: Vec<&Expr> = operand.iter().map(|b| &**b).collect();
            for (a, b) in whens {
                v.push(a);
                v.push(b);
            }
            v.extend(else_.iter().map(|b| &**b));
            v
        }
        Expr::Number(_) | Expr::Str(_) | Expr::Bool(_) | Expr::Null | Expr::Param(_) => Vec::new(),
        _ => return None,
    })
}
