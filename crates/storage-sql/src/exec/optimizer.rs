//! The cost-based optimizer (`F-ENT-04`, `TASK-148`).
//!
//! **Join order.** A chain of inner joins and comma-separated tables is flattened into its leaves
//! and predicates, and joined in the order with the fewest rows passing through: dynamic
//! programming over subsets up to [`MAX_DP_RELATIONS`] leaves, greedy beyond. Outer joins, `USING`
//! and `NATURAL` stay as they are written, as single leaves. Column positions in the result are
//! restored afterwards, so `SELECT *` does not change.
//!
//! **Estimates** come from `ANALYZE` (`exec/stats.rs`). A leaf without statistics means the written
//! order is kept: plans do not change on guesses alone.
//!
//! **Sub-queries.** A correlated `EXISTS` or scalar aggregate whose correlation is `inner = outer`
//! equalities runs once, as a hash set or group map, instead of once per outer row. Its results
//! are the same; only `NULL`-safe equality and non-correlated remainders qualify.
//!
//! `SET enable_optimizer = off` turns all of it off, which is also how the tests compare answers.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use crate::ast::*;
use crate::catalog::Row;
use crate::datum::{hash_key, key_class, Datum};
use crate::error::SqlResult;
use crate::exec::eval::{has_aggregate, Env, Exec};
use crate::exec::select::{column_refs, conjuncts, has_subquery, resolves, table_cols, ColMeta, Rel};
use crate::exec::stats::{ColumnStats, TableStats};

/// Most leaves ordered exhaustively (2^n subsets).
pub const MAX_DP_RELATIONS: usize = 10;
/// Assumed for a predicate statistics say nothing about.
const DEFAULT_SELECTIVITY: f64 = 0.25;
const DEFAULT_RANGE: f64 = 1.0 / 3.0;
const DEFAULT_LIKE: f64 = 0.1;
/// Rows assumed for a subquery leaf.
const DERIVED_ROWS: f64 = 1000.0;

/// A leaf of a flattened join.
#[derive(Debug)]
pub(crate) struct Leaf<'a> {
    /// Columns as the query names them.
    pub cols: Vec<ColMeta>,
    pub rows: f64,
    /// `true`: the estimate came from statistics.
    pub known: bool,
    pub stats: Option<&'a TableStats>,
}

/// A flattened chain of inner joins.
#[derive(Debug, Default)]
pub(crate) struct Flat<'a> {
    pub items: Vec<&'a TableRef>,
    /// `ON` conjuncts: they must hold.
    pub on: Vec<&'a Expr>,
}

/// Flattens `from`; `None` when something in it depends on what comes before it.
pub(crate) fn flatten<'a>(from: &'a [TableRef]) -> Option<Flat<'a>> {
    fn go<'a>(t: &'a TableRef, out: &mut Flat<'a>) -> Option<()> {
        match t {
            TableRef::Join { left, right, kind: JoinKind::Inner | JoinKind::Cross, constraint: JoinConstraint::On(_) | JoinConstraint::None } => {
                go(left, out)?;
                go(right, out)?;
                if let TableRef::Join { constraint: JoinConstraint::On(e), .. } = t {
                    out.on.extend(conjuncts(e));
                }
                Some(())
            }
            TableRef::Derived { lateral: true, .. } => None,
            TableRef::Function { args, .. } if args.iter().any(|a| !column_refs(a).is_empty()) => None,
            other => {
                out.items.push(other);
                Some(())
            }
        }
    }
    let mut flat = Flat::default();
    for t in from {
        go(t, &mut flat)?;
    }
    Some(flat)
}

/// The value of a constant expression, for estimates.
fn constant(ex: &Exec<'_>, e: &Expr) -> Option<Datum> {
    if !column_refs(e).is_empty() || has_subquery(e) || has_aggregate(e) {
        return None;
    }
    ex.eval_const(e).ok()
}

fn column_stats<'s>(stats: &'s TableStats, cols: &[ColMeta], p: &[String]) -> Option<&'s ColumnStats> {
    if !resolves(cols, p) {
        return None;
    }
    stats.columns.get(&p.last()?.to_ascii_lowercase())
}

fn column(x: &Expr) -> Option<&Vec<String>> {
    if let Expr::Column(p) = x {
        Some(p)
    } else {
        None
    }
}

/// Fraction of a table's rows that pass `e` (`e` reads only that table).
pub(crate) fn selectivity(ex: &Exec<'_>, e: &Expr, stats: &TableStats, cols: &[ColMeta]) -> f64 {
    let s = match e {
        Expr::Binary { op: BinaryOp::And, left, right } => selectivity(ex, left, stats, cols) * selectivity(ex, right, stats, cols),
        Expr::Binary { op: BinaryOp::Or, left, right } => {
            let (a, b) = (selectivity(ex, left, stats, cols), selectivity(ex, right, stats, cols));
            a + b - a * b
        }
        Expr::Binary { op, left, right } if matches!(op, BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq) => {
            // Either way round: the constant's side decides the direction.
            let (col, value, flipped) = match (column(left), column(right)) {
                (Some(p), None) => (p, constant(ex, right), false),
                (None, Some(p)) => (p, constant(ex, left), true),
                _ => return DEFAULT_SELECTIVITY,
            };
            let Some(c) = column_stats(stats, cols, col) else {
                return if *op == BinaryOp::Eq { DEFAULT_SELECTIVITY / 2.0 } else { DEFAULT_RANGE };
            };
            match op {
                BinaryOp::Eq => c.eq_selectivity(value.as_ref()),
                BinaryOp::NotEq => (1.0 - c.null_frac - c.eq_selectivity(value.as_ref())).max(0.0),
                _ => {
                    let below = matches!((op, flipped), (BinaryOp::Lt | BinaryOp::LtEq, false) | (BinaryOp::Gt | BinaryOp::GtEq, true));
                    value
                        .as_ref()
                        .and_then(|v| if below { c.range_selectivity(None, Some(v)) } else { c.range_selectivity(Some(v), None) })
                        .unwrap_or(DEFAULT_RANGE)
                }
            }
        }
        Expr::InList { expr, list, negated } => {
            let Some(c) = column(expr).and_then(|p| column_stats(stats, cols, p)) else { return DEFAULT_SELECTIVITY };
            let hit: f64 = list.iter().map(|v| c.eq_selectivity(constant(ex, v).as_ref())).sum::<f64>().min(1.0);
            if *negated {
                (1.0 - c.null_frac - hit).max(0.0)
            } else {
                hit
            }
        }
        Expr::IsNull { expr, negated } => {
            let Some(c) = column(expr).and_then(|p| column_stats(stats, cols, p)) else { return DEFAULT_SELECTIVITY };
            if *negated {
                1.0 - c.null_frac
            } else {
                c.null_frac
            }
        }
        Expr::Like { negated, .. } => {
            if *negated {
                1.0 - DEFAULT_LIKE
            } else {
                DEFAULT_LIKE
            }
        }
        _ => DEFAULT_SELECTIVITY,
    };
    s.clamp(0.0, 1.0)
}

/// Fraction of a pair of rows that pass join predicate `e`: `a = b` by the larger distinct count.
fn join_selectivity(e: &Expr, leaves: &[Leaf<'_>]) -> f64 {
    if let Expr::Binary { op: BinaryOp::Eq, left, right } = e {
        let distinct = |x: &Expr| -> Option<f64> {
            let Expr::Column(p) = x else { return None };
            leaves.iter().find_map(|l| column_stats(l.stats?, &l.cols, p).map(|c| c.distinct.max(1.0)))
        };
        if let (Some(a), Some(b)) = (distinct(left), distinct(right)) {
            return 1.0 / a.max(b);
        }
        return 0.1;
    }
    DEFAULT_RANGE
}

/// The best order of `n` leaves: left-deep, fewest rows passing through. `edges`: for each
/// predicate, the leaves it reads (bitmask) and its selectivity.
pub(crate) fn best_order(rows: &[f64], edges: &[(u32, f64)]) -> Vec<usize> {
    let n = rows.len();
    let applied = |set: u32, add: usize| -> f64 {
        // Predicates completed by adding `add` to `set`.
        let with = set | (1 << add);
        edges.iter().filter(|(m, _)| m & with == *m && m & set != *m).map(|(_, s)| *s).product()
    };
    if n > MAX_DP_RELATIONS {
        // Greedy: smallest first, then always the join with the fewest resulting rows.
        let mut order = vec![(0..n).min_by(|a, b| rows[*a].total_cmp(&rows[*b])).unwrap_or(0)];
        let mut set = 1u32 << order[0];
        let mut card = rows[order[0]];
        while order.len() < n {
            let next = (0..n).filter(|i| set & (1 << i) == 0).min_by(|a, b| (card * rows[*a] * applied(set, *a)).total_cmp(&(card * rows[*b] * applied(set, *b)))).unwrap_or(0);
            card *= rows[next] * applied(set, next);
            set |= 1 << next;
            order.push(next);
        }
        return order;
    }
    #[derive(Clone)]
    struct Entry {
        cost: f64,
        rows: f64,
        last: usize,
    }
    let full = (1u32 << n) - 1;
    let mut dp: Vec<Option<Entry>> = vec![None; (full as usize) + 1];
    for (i, r) in rows.iter().enumerate() {
        dp[1 << i] = Some(Entry { cost: *r, rows: *r, last: i });
    }
    for set in 1..=full {
        let Some(cur) = dp[set as usize].clone() else { continue };
        for (next, next_rows) in rows.iter().enumerate() {
            if set & (1 << next) != 0 {
                continue;
            }
            let out = cur.rows * next_rows * applied(set, next);
            // What a hash join touches: both inputs and what it produces.
            let cost = cur.cost + next_rows + out;
            let to = (set | (1 << next)) as usize;
            if dp[to].as_ref().is_none_or(|e| cost < e.cost) {
                dp[to] = Some(Entry { cost, rows: out, last: next });
            }
        }
    }
    let mut order = Vec::with_capacity(n);
    let mut set = full;
    while set != 0 {
        let Some(e) = dp[set as usize].as_ref() else { break };
        order.push(e.last);
        set &= !(1 << e.last);
    }
    order.reverse();
    order
}

impl Exec<'_> {
    pub(crate) fn optimizer_enabled(&self) -> bool {
        !self.s.vars.get("enable_optimizer").is_some_and(|v| matches!(v.to_ascii_lowercase().as_str(), "off" | "0" | "false"))
    }

    /// A leaf's columns and estimated rows; `None` when it cannot be reordered blind.
    fn leaf<'a>(&self, item: &'a TableRef, conjs: &[&Expr], on: &[&Expr], all: &'a HashMap<String, TableStats>) -> SqlResult<Option<Leaf<'a>>> {
        let requalified = |t: &crate::catalog::Table, alias: &Option<TableAlias>| -> SqlResult<Vec<ColMeta>> {
            Ok(Rel { cols: table_cols(t), rows: Vec::new() }.requalify(alias, Some(&t.name))?.cols)
        };
        Ok(match item {
            TableRef::Named { schema: None, name, alias } if self.cte(name).is_none() && !name.starts_with('@') => {
                let Some(t) = self.s.engine.find_table(name)? else { return Ok(None) };
                let cols = requalified(&t, alias)?;
                let stats = all.get(&t.name.to_ascii_lowercase());
                let (mut rows, known) = match stats {
                    Some(s) => (s.rows, true),
                    None => (DERIVED_ROWS, false),
                };
                if let Some(s) = stats {
                    for c in conjs.iter().chain(on) {
                        let refs = column_refs(c);
                        if !refs.is_empty() && !has_subquery(c) && refs.iter().all(|p| resolves(&cols, p)) {
                            rows *= selectivity(self, c, s, &cols);
                        }
                    }
                }
                Some(Leaf { cols, rows: rows.max(1.0), known, stats })
            }
            // Outer joins and the like run as written; their shape is unknown here.
            _ => None,
        })
    }

    /// The join order for `from`, or `None` to keep it as written. On success, gives the flattened
    /// leaves in planned order and their indexes in the written order.
    pub(crate) fn plan_join_order<'a>(&self, from: &'a [TableRef], pushable: &[&'a Expr]) -> SqlResult<Option<(Flat<'a>, Vec<usize>)>> {
        if !self.optimizer_enabled() {
            return Ok(None);
        }
        let Some(flat) = flatten(from) else { return Ok(None) };
        if flat.items.len() < 2 || flat.items.len() > 30 {
            return Ok(None);
        }
        let all = self.s.engine.stats()?;
        if all.is_empty() {
            return Ok(None);
        }
        let mut leaves = Vec::with_capacity(flat.items.len());
        for item in &flat.items {
            match self.leaf(item, pushable, &flat.on, &all)? {
                Some(l) if l.known => leaves.push(l),
                _ => return Ok(None),
            }
        }
        // A name two leaves share, unqualified, must still fail the way it does as written.
        let mut names = HashSet::new();
        for l in &leaves {
            let q = l.cols.first().and_then(|c| c.qual.clone()).unwrap_or_default().to_ascii_lowercase();
            if !names.insert(q) {
                return Ok(None);
            }
        }
        let mut edges = Vec::new();
        for c in flat.on.iter().chain(pushable) {
            let refs = column_refs(c);
            let mut mask = 0u32;
            for (i, l) in leaves.iter().enumerate() {
                if refs.iter().any(|p| resolves(&l.cols, p)) {
                    mask |= 1 << i;
                }
            }
            if mask.count_ones() >= 2 {
                edges.push((mask, join_selectivity(c, &leaves)));
            }
        }
        let rows: Vec<f64> = leaves.iter().map(|l| l.rows).collect();
        let order = best_order(&rows, &edges);
        Ok(Some((flat, order)))
    }

    /// Runs a planned join: leaves in `order`, each joined on the predicates it completes, then
    /// columns back in the written order.
    pub(crate) fn run_planned_join(&self, flat: &Flat<'_>, order: &[usize], pushable: &[&Expr], outer: Option<&Env<'_>>) -> SqlResult<Rel> {
        let mut acc: Option<Rel> = None;
        // Where each written leaf's columns ended up.
        let mut placed: Vec<(usize, usize, usize)> = Vec::with_capacity(order.len());
        let mut done_on: HashSet<usize> = HashSet::new();
        for &i in order {
            let item = flat.items[i];
            let linking = |l: &Rel, right_cols: &[ColMeta], done_on: &mut HashSet<usize>| -> Vec<Expr> {
                let mut both = l.cols.clone();
                both.extend(right_cols.iter().cloned());
                let mut out = Vec::new();
                for (k, c) in flat.on.iter().enumerate() {
                    let refs = column_refs(c);
                    if !done_on.contains(&k) && !has_subquery(c) && !refs.is_empty() && refs.iter().all(|p| resolves(&both, p)) {
                        done_on.insert(k);
                        out.push((*c).clone());
                    }
                }
                // WHERE conjuncts still get re-checked above: here they only make the join cheap.
                for c in pushable {
                    let refs = column_refs(c);
                    if !refs.is_empty() && refs.iter().all(|p| resolves(&both, p)) && refs.iter().any(|p| resolves(&l.cols, p) && !resolves(right_cols, p)) && refs.iter().any(|p| resolves(right_cols, p) && !resolves(&l.cols, p)) {
                        out.push((*c).clone());
                    }
                }
                out
            };
            let next = match acc.take() {
                None => {
                    let r = self.relation_of_filtered(item, pushable, outer)?;
                    placed.push((i, 0, r.cols.len()));
                    r
                }
                Some(l) => {
                    let left_width = l.cols.len();
                    let probe_cols = self.leaf_cols(item)?;
                    let on = linking(&l, &probe_cols, &mut done_on).into_iter().reduce(|a, b| Expr::binary(BinaryOp::And, a, b));
                    let r = self.right_of_join(item, pushable, on.as_ref(), &l, outer)?;
                    placed.push((i, left_width, r.cols.len()));
                    match on {
                        Some(on) => self.join(l, r, JoinKind::Inner, &JoinConstraint::On(on), outer)?,
                        None => self.join(l, r, JoinKind::Cross, &JoinConstraint::None, outer)?,
                    }
                }
            };
            acc = Some(next);
        }
        let mut rel = acc.unwrap_or(Rel { cols: Vec::new(), rows: vec![Vec::new()] });
        // `ON` predicates nothing completed so far (subqueries, outer references): they still hold.
        for (k, c) in flat.on.iter().enumerate() {
            if !done_on.contains(&k) {
                let cols = rel.cols.clone();
                let mut kept = Vec::with_capacity(rel.rows.len());
                for row in rel.rows {
                    let env = Env { cols: &cols, row: &row, outer, aggs: None };
                    if self.eval_bool(c, &env)? {
                        kept.push(row);
                    }
                }
                rel.rows = kept;
            }
        }
        // Columns in the written order.
        placed.sort_by_key(|p| p.0);
        let positions: Vec<usize> = placed.iter().flat_map(|&(_, start, len)| start..start + len).collect();
        if positions.iter().enumerate().any(|(a, b)| a != *b) {
            rel.cols = positions.iter().map(|&p| rel.cols[p].clone()).collect();
            rel.rows = rel.rows.into_iter().map(|row: Row| positions.iter().map(|&p| row[p].clone()).collect()).collect();
        }
        Ok(rel)
    }

    // ── sub-queries ──

    /// Splits a correlated `WHERE` into `inner = outer` equalities and what reads only the inner
    /// tables; `None` when it is not that simple (it then runs per row, as written).
    fn correlation<'q>(&self, sel: &'q Select) -> SqlResult<Option<Correlation<'q>>> {
        if !sel.group_by.is_empty() || sel.having.is_some() || !sel.distinct_on.is_empty() {
            return Ok(None);
        }
        let Some(where_) = &sel.where_ else { return Ok(None) };
        let mut cols = Vec::new();
        for t in &sel.from {
            let TableRef::Named { schema: None, name, alias } = t else { return Ok(None) };
            if self.cte(name).is_some() || name.starts_with('@') {
                return Ok(None);
            }
            let Some(table) = self.s.engine.find_table(name)? else { return Ok(None) };
            cols.extend(Rel { cols: table_cols(&table), rows: Vec::new() }.requalify(alias, Some(&table.name))?.cols);
        }
        if cols.is_empty() {
            return Ok(None);
        }
        let reads_inner = |e: &Expr| column_refs(e).iter().any(|p| resolves(&cols, p));
        let all_inner = |e: &Expr| column_refs(e).iter().all(|p| resolves(&cols, p));
        let (mut keys, mut rest) = (Vec::new(), Vec::new());
        for c in conjuncts(where_) {
            if has_subquery(c) || has_aggregate(c) {
                return Ok(None);
            }
            if all_inner(c) {
                rest.push(c);
                continue;
            }
            match c {
                Expr::Binary { op: BinaryOp::Eq, left, right } => {
                    if all_inner(left) && reads_inner(left) && !reads_inner(right) {
                        keys.push((&**left, &**right));
                    } else if all_inner(right) && reads_inner(right) && !reads_inner(left) {
                        keys.push((&**right, &**left));
                    } else {
                        return Ok(None);
                    }
                }
                _ => return Ok(None),
            }
        }
        if keys.is_empty() {
            return Ok(None);
        }
        Ok(Some(Correlation { keys, rest }))
    }

    /// `EXISTS (correlated)` as a set lookup; `None` to run it per row.
    pub(crate) fn exists_decorrelated(&self, q: &Query, env: &Env<'_>) -> SqlResult<Option<bool>> {
        if !self.optimizer_enabled() {
            return Ok(None);
        }
        let key = q as *const Query as usize;
        let cached = self.semi_joins.borrow().get(&key).cloned();
        let entry = match cached {
            Some(e) => e,
            None => {
                let e = self.build_semi_join(q)?.map(Rc::new);
                self.semi_joins.borrow_mut().insert(key, e.clone());
                e
            }
        };
        let Some(semi) = entry else { return Ok(None) };
        let mut k = String::new();
        for (i, e) in semi.outer_keys.iter().enumerate() {
            let v = self.eval(e, env)?;
            if v.is_null() {
                return Ok(Some(false));
            }
            // A value `=` may coerce: this row takes the long way.
            if semi.classes.get(i).is_some_and(|c| *c != key_class(&v)) {
                return Ok(None);
            }
            k.push_str(&hash_key(&v));
            k.push('\u{1}');
        }
        Ok(Some(semi.keys.contains(&k)))
    }

    fn build_semi_join(&self, q: &Query) -> SqlResult<Option<SemiJoin>> {
        if !q.ctes.is_empty() || q.limit.is_some() || q.offset.is_some() || q.lock.is_some() {
            return Ok(None);
        }
        let SetExpr::Select(sel) = &q.body else { return Ok(None) };
        let Some(c) = self.correlation(sel)? else { return Ok(None) };
        let mut inner = (**sel).clone();
        inner.distinct = false;
        inner.items = c.keys.iter().map(|(i, _)| SelectItem::Expr { expr: (*i).clone(), alias: None }).collect();
        inner.where_ = c.rest.iter().map(|e| (*e).clone()).reduce(|a, b| Expr::binary(BinaryOp::And, a, b));
        let rel = self.select(&inner, None, &[], None, 0)?;
        let mut keys = HashSet::with_capacity(rel.rows.len());
        let Some(classes) = classes_of(&rel.rows, c.keys.len()) else { return Ok(None) };
        'row: for row in &rel.rows {
            let mut k = String::new();
            for v in row {
                if v.is_null() {
                    continue 'row; // `=` with NULL is never true
                }
                k.push_str(&hash_key(v));
                k.push('\u{1}');
            }
            keys.insert(k);
        }
        Ok(Some(SemiJoin { keys, classes, outer_keys: c.keys.iter().map(|(_, o)| (*o).clone()).collect() }))
    }

    /// `(SELECT agg(x) FROM inner WHERE inner.k = outer.k)` as one grouped pass; `None` to run it
    /// per row.
    pub(crate) fn scalar_decorrelated(&self, q: &Query, env: &Env<'_>) -> SqlResult<Option<Datum>> {
        if !self.optimizer_enabled() {
            return Ok(None);
        }
        let key = q as *const Query as usize;
        let cached = self.group_maps.borrow().get(&key).cloned();
        let entry = match cached {
            Some(e) => e,
            None => {
                let e = self.build_group_map(q)?.map(Rc::new);
                self.group_maps.borrow_mut().insert(key, e.clone());
                e
            }
        };
        let Some(g) = entry else { return Ok(None) };
        let mut k = String::new();
        for (i, e) in g.outer_keys.iter().enumerate() {
            let v = self.eval(e, env)?;
            if v.is_null() {
                return Ok(Some(g.empty.clone()));
            }
            if g.classes.get(i).is_some_and(|c| *c != key_class(&v)) {
                return Ok(None);
            }
            k.push_str(&hash_key(&v));
            k.push('\u{1}');
        }
        Ok(Some(g.values.get(&k).cloned().unwrap_or_else(|| g.empty.clone())))
    }

    fn build_group_map(&self, q: &Query) -> SqlResult<Option<GroupMap>> {
        if !q.ctes.is_empty() || q.limit.is_some() || q.offset.is_some() || q.lock.is_some() || !q.order_by.is_empty() {
            return Ok(None);
        }
        let SetExpr::Select(sel) = &q.body else { return Ok(None) };
        if sel.distinct {
            return Ok(None);
        }
        // One aggregate call, nothing around it: what no rows give is then known.
        let [SelectItem::Expr { expr: agg @ Expr::Function { name, distinct: false, .. }, .. }] = sel.items.as_slice() else { return Ok(None) };
        if !has_aggregate(agg) {
            return Ok(None);
        }
        let lower = name.to_ascii_lowercase();
        let empty = match lower.as_str() {
            "count" | "count_big" => Datum::Int(0),
            "sum" | "avg" | "min" | "max" => Datum::Null,
            _ => return Ok(None),
        };
        let Some(c) = self.correlation(sel)? else { return Ok(None) };
        let mut inner = (**sel).clone();
        let inner_keys: Vec<Expr> = c.keys.iter().map(|(i, _)| (*i).clone()).collect();
        inner.items = inner_keys.iter().map(|e| SelectItem::Expr { expr: e.clone(), alias: None }).collect();
        inner.items.push(SelectItem::Expr { expr: agg.clone(), alias: None });
        inner.where_ = c.rest.iter().map(|e| (*e).clone()).reduce(|a, b| Expr::binary(BinaryOp::And, a, b));
        inner.group_by = inner_keys;
        let rel = self.select(&inner, None, &[], None, 0)?;
        let width = c.keys.len();
        let Some(classes) = classes_of(&rel.rows, width) else { return Ok(None) };
        let mut values = HashMap::with_capacity(rel.rows.len());
        'row: for row in rel.rows {
            let mut k = String::new();
            for v in &row[..width] {
                if v.is_null() {
                    continue 'row;
                }
                k.push_str(&hash_key(v));
                k.push('\u{1}');
            }
            values.insert(k, row[width].clone());
        }
        Ok(Some(GroupMap { values, classes, outer_keys: c.keys.iter().map(|(_, o)| (*o).clone()).collect(), empty }))
    }
}

/// The class of each of the first `width` columns; `None` if a column mixes classes. Empty when
/// there are no rows: nothing can match anyway.
fn classes_of(rows: &[Row], width: usize) -> Option<Vec<u8>> {
    let mut classes = vec![0u8; width];
    for row in rows {
        for (c, v) in classes.iter_mut().zip(row) {
            let k = key_class(v);
            if k == 0 {
                continue;
            }
            if *c != 0 && *c != k {
                return None;
            }
            *c = k;
        }
    }
    classes.retain(|c| *c != 0);
    Some(if classes.len() == width { classes } else { Vec::new() })
}

struct Correlation<'q> {
    /// (inner expression, outer expression).
    keys: Vec<(&'q Expr, &'q Expr)>,
    rest: Vec<&'q Expr>,
}

/// A decorrelated `EXISTS`.
#[derive(Debug)]
pub(crate) struct SemiJoin {
    keys: HashSet<String>,
    /// [`key_class`] of each key (empty: no rows).
    classes: Vec<u8>,
    outer_keys: Vec<Expr>,
}

/// A decorrelated scalar aggregate.
#[derive(Debug)]
pub(crate) struct GroupMap {
    values: HashMap<String, Datum>,
    classes: Vec<u8>,
    outer_keys: Vec<Expr>,
    /// What the aggregate gives over no rows.
    empty: Datum,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_starts_small_and_follows_the_predicates() {
        // big(1M) - mid(10k) - small(10): a chain joined on keys. Starting from `small` passes few
        // rows; the written order (big first) would pass many.
        let rows = [1_000_000.0, 10_000.0, 10.0];
        let edges = [(0b011u32, 1.0 / 1_000_000.0), (0b110, 1.0 / 10_000.0)];
        let o = best_order(&rows, &edges);
        assert_eq!(o[2], 0, "the big table comes last: {o:?}");
    }

    #[test]
    fn cross_products_are_avoided_when_a_chain_exists() {
        // a-b and b-c are connected, a-c is not: a then c would be a cross product.
        let rows = [100.0, 100.0, 100.0];
        let edges = [(0b011u32, 0.01), (0b110, 0.01)];
        let o = best_order(&rows, &edges);
        let first_two = (o[0].min(o[1]), o[0].max(o[1]));
        assert_ne!(first_two, (0, 2), "{o:?}");
    }

    #[test]
    fn greedy_beyond_the_exhaustive_limit_visits_everything_once() {
        let n = MAX_DP_RELATIONS + 3;
        let rows: Vec<f64> = (0..n).map(|i| (n - i) as f64 * 100.0).collect();
        let edges: Vec<(u32, f64)> = (0..n - 1).map(|i| (0b11u32 << i, 0.01)).collect();
        let mut o = best_order(&rows, &edges);
        assert_eq!(o[0], n - 1, "smallest first");
        o.sort_unstable();
        assert_eq!(o, (0..n).collect::<Vec<_>>());
    }

    #[test]
    fn nothing_to_choose() {
        assert_eq!(best_order(&[5.0], &[]), vec![0]);
    }
}
