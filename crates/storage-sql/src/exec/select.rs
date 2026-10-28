//! Queries: FROM (with filter push-down), joins, WHERE, grouping, projection, ordering, set
//! operations and common table expressions.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use storine::Filter;

use crate::ast::{
    BinaryOp, DataType, Expr, JoinConstraint, JoinKind, OrderBy, Query, Select, SelectItem,
    SetExpr, SetOp, TableAlias, TableRef,
};
use crate::catalog::{Row, Table};
use crate::datum::{self, cast, compare, hash_key, Datum};
use crate::error::{SqlError, SqlResult};
use crate::exec::eval::{find_column, has_aggregate, is_aggregate, walk, Env, Exec};
use crate::exec::{func, system};
use crate::lexer::Dialect;

/// One column of an intermediate result.
#[derive(Debug, Clone, PartialEq)]
pub struct ColMeta {
    /// The table name or alias that qualifies it (`t` in `t.col`).
    pub qual: Option<String>,
    pub name: String,
    pub ty: Option<DataType>,
    /// The base table it comes from (result metadata).
    pub table: Option<String>,
    /// Not visible to `*` or unqualified names (merged `USING` columns).
    pub hidden: bool,
}

impl ColMeta {
    pub fn new(name: impl Into<String>, ty: Option<DataType>) -> ColMeta {
        ColMeta {
            qual: None,
            name: name.into(),
            ty,
            table: None,
            hidden: false,
        }
    }
}

/// An intermediate or final relation.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Rel {
    pub cols: Vec<ColMeta>,
    pub rows: Vec<Row>,
}

impl Rel {
    fn requalify(mut self, alias: &Option<TableAlias>, default: Option<&str>) -> SqlResult<Rel> {
        let qual = alias
            .as_ref()
            .map(|a| a.name.clone())
            .or_else(|| default.map(str::to_owned));
        for c in &mut self.cols {
            c.qual = qual.clone();
        }
        if let Some(a) = alias {
            if !a.columns.is_empty() {
                if a.columns.len() > self.cols.len() {
                    return Err(SqlError::syntax(format!(
                        "table \"{}\" has {} columns available but {} columns specified",
                        a.name,
                        self.cols.len(),
                        a.columns.len()
                    )));
                }
                for (c, n) in self.cols.iter_mut().zip(&a.columns) {
                    c.name = n.clone();
                }
            }
        }
        Ok(self)
    }
}

/// A matching row of a table with its key and version (DML targets).
#[derive(Debug, Clone)]
pub struct TableRow {
    pub key: String,
    /// `None`: written by this transaction and not yet committed.
    pub version: Option<u64>,
    pub row: Row,
}

/// Splits `e` into its top-level AND conjuncts.
pub fn conjuncts(e: &Expr) -> Vec<&Expr> {
    match e {
        Expr::Binary {
            op: BinaryOp::And,
            left,
            right,
        } => {
            let mut v = conjuncts(left);
            v.extend(conjuncts(right));
            v
        }
        other => vec![other],
    }
}

/// Column references in `e` (not entering subqueries).
fn column_refs(e: &Expr) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    walk(e, &mut |x| {
        if let Expr::Column(p) = x {
            out.push(p.clone());
        }
    });
    out
}

fn has_subquery(e: &Expr) -> bool {
    let mut found = false;
    walk(e, &mut |x| {
        if matches!(
            x,
            Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. }
        ) {
            found = true;
        }
        if let Expr::AnyAll { right, .. } = x {
            if matches!(**right, Expr::Subquery(_)) {
                found = true;
            }
        }
    });
    found
}

impl Exec<'_> {
    pub fn query(&self, q: &Query, outer: Option<&Env<'_>>) -> SqlResult<Rel> {
        let pushed = !q.ctes.is_empty();
        if pushed {
            let mut scope = HashMap::new();
            self.ctes.borrow_mut().push(HashMap::new());
            for cte in &q.ctes {
                let rel = self.query(&cte.query, outer)?;
                let rel = rel.requalify(
                    &Some(TableAlias {
                        name: cte.name.clone(),
                        columns: cte.columns.clone(),
                    }),
                    None,
                )?;
                let rc = Rc::new(rel);
                scope.insert(cte.name.to_ascii_lowercase(), Rc::clone(&rc));
                if let Some(top) = self.ctes.borrow_mut().last_mut() {
                    top.insert(cte.name.to_ascii_lowercase(), rc);
                }
            }
            let _ = scope;
        }
        let r = self.query_body(q, outer);
        if pushed {
            self.ctes.borrow_mut().pop();
        }
        r
    }

    fn limit_offset(&self, q: &Query) -> SqlResult<(Option<usize>, usize)> {
        let lim = match &q.limit {
            None => None,
            Some(e) => match self.eval_const(e)? {
                Datum::Null => None,
                v => Some(
                    usize::try_from(v.as_i64()?)
                        .map_err(|_| SqlError::new("2201W", "LIMIT must not be negative"))?,
                ),
            },
        };
        let off = match &q.offset {
            None => 0,
            Some(e) => match self.eval_const(e)? {
                Datum::Null => 0,
                v => usize::try_from(v.as_i64()?)
                    .map_err(|_| SqlError::new("2201X", "OFFSET must not be negative"))?,
            },
        };
        Ok((lim, off))
    }

    fn query_body(&self, q: &Query, outer: Option<&Env<'_>>) -> SqlResult<Rel> {
        let (limit, offset) = self.limit_offset(q)?;
        if let SetExpr::Select(sel) = &q.body {
            return self.select(sel, outer, &q.order_by, limit, offset);
        }
        let mut rel = self.set_expr(&q.body, outer)?;
        if !q.order_by.is_empty() {
            let keys: Vec<Vec<Datum>> = rel
                .rows
                .iter()
                .map(|r| {
                    let env = Env {
                        cols: &rel.cols,
                        row: r,
                        outer,
                        aggs: None,
                    };
                    q.order_by
                        .iter()
                        .map(|o| self.output_order_key(o, &rel.cols, r, &env))
                        .collect::<SqlResult<Vec<_>>>()
                })
                .collect::<SqlResult<_>>()?;
            let mut idx: Vec<usize> = (0..rel.rows.len()).collect();
            idx.sort_by(|a, b| self.order_cmp(&q.order_by, &keys[*a], &keys[*b]));
            let mut rows: Vec<Option<Row>> = rel.rows.into_iter().map(Some).collect();
            rel.rows = idx.into_iter().filter_map(|i| rows[i].take()).collect();
        }
        apply_limit(&mut rel.rows, limit, offset);
        Ok(rel)
    }

    /// An ORDER BY key of a set operation / VALUES result: an ordinal, an output name or an
    /// expression over the output columns.
    fn output_order_key(
        &self,
        o: &OrderBy,
        cols: &[ColMeta],
        row: &[Datum],
        env: &Env<'_>,
    ) -> SqlResult<Datum> {
        if let Expr::Number(n) = &o.expr {
            let i: usize = n
                .parse()
                .map_err(|_| SqlError::syntax(format!("bad ORDER BY position {n}")))?;
            return row.get(i.wrapping_sub(1)).cloned().ok_or_else(|| {
                SqlError::syntax(format!("ORDER BY position {i} is not in select list"))
            });
        }
        if let Expr::Column(p) = &o.expr {
            if let Some(i) = find_column(
                cols,
                if p.len() > 1 {
                    Some(&p[p.len() - 2])
                } else {
                    None
                },
                &p[p.len() - 1],
            )? {
                return Ok(row[i].clone());
            }
        }
        self.eval(&o.expr, env)
    }

    fn order_cmp(&self, order: &[OrderBy], a: &[Datum], b: &[Datum]) -> Ordering {
        let mysql = self.dialect() == Dialect::MySql;
        for (o, (x, y)) in order.iter().zip(a.iter().zip(b)) {
            // PostgreSQL: NULLs sort as larger than any value; MySQL: as smaller.
            let nulls_first = o
                .nulls_first
                .unwrap_or(if mysql { !o.desc } else { o.desc });
            let ord = match (x.is_null(), y.is_null()) {
                (true, true) => Ordering::Equal,
                (true, false) => {
                    if nulls_first {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
                (false, true) => {
                    if nulls_first {
                        Ordering::Greater
                    } else {
                        Ordering::Less
                    }
                }
                _ => {
                    let c = compare(x, y);
                    if o.desc {
                        c.reverse()
                    } else {
                        c
                    }
                }
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    }

    fn set_expr(&self, e: &SetExpr, outer: Option<&Env<'_>>) -> SqlResult<Rel> {
        match e {
            SetExpr::Select(s) => self.select(s, outer, &[], None, 0),
            SetExpr::Nested(q) => self.query(q, outer),
            SetExpr::Values(rows) => {
                let env = outer.copied().unwrap_or(Env::EMPTY);
                let mut out = Rel::default();
                for (i, r) in rows.iter().enumerate() {
                    let vals = r
                        .iter()
                        .map(|x| self.eval(x, &env))
                        .collect::<SqlResult<Vec<_>>>()?;
                    if i == 0 {
                        out.cols = (1..=vals.len())
                            .map(|n| ColMeta::new(format!("column{n}"), None))
                            .collect();
                    } else if vals.len() != out.cols.len() {
                        return Err(SqlError::syntax(
                            "VALUES lists must all be the same length".into(),
                        ));
                    }
                    out.rows.push(vals);
                }
                Ok(out)
            }
            SetExpr::SetOp {
                op,
                all,
                left,
                right,
            } => {
                let l = self.set_expr(left, outer)?;
                let r = self.set_expr(right, outer)?;
                if !l.cols.is_empty() && !r.cols.is_empty() && l.cols.len() != r.cols.len() {
                    return Err(SqlError::syntax(
                        "each UNION query must have the same number of columns".into(),
                    ));
                }
                let key = |row: &Row| row.iter().map(hash_key).collect::<Vec<_>>().join("\u{1}");
                let mut cols = l.cols.clone();
                // A typed side wins over an untyped one (NULL literals).
                for (c, rc) in cols.iter_mut().zip(&r.cols) {
                    if c.ty.is_none() {
                        c.ty = rc.ty.clone();
                    }
                }
                let rows = match op {
                    SetOp::Union => {
                        let mut rows = l.rows;
                        rows.extend(r.rows);
                        if !*all {
                            let mut seen = HashSet::new();
                            rows.retain(|row| seen.insert(key(row)));
                        }
                        rows
                    }
                    SetOp::Intersect => {
                        let mut counts: HashMap<String, usize> = HashMap::new();
                        for row in &r.rows {
                            *counts.entry(key(row)).or_default() += 1;
                        }
                        let mut out = Vec::new();
                        let mut seen = HashSet::new();
                        for row in l.rows {
                            let k = key(&row);
                            if let Some(c) = counts.get_mut(&k) {
                                if *c > 0 && (*all || seen.insert(k.clone())) {
                                    if *all {
                                        *c -= 1;
                                    }
                                    out.push(row);
                                }
                            }
                        }
                        out
                    }
                    SetOp::Except => {
                        let mut counts: HashMap<String, usize> = HashMap::new();
                        for row in &r.rows {
                            *counts.entry(key(row)).or_default() += 1;
                        }
                        let mut out = Vec::new();
                        let mut seen = HashSet::new();
                        for row in l.rows {
                            let k = key(&row);
                            match counts.get_mut(&k) {
                                Some(c) if *c > 0 => {
                                    if *all {
                                        *c -= 1;
                                    }
                                }
                                _ => {
                                    if *all || seen.insert(k) {
                                        out.push(row);
                                    }
                                }
                            }
                        }
                        out
                    }
                };
                Ok(Rel { cols, rows })
            }
        }
    }

    // ── FROM ──

    fn cte(&self, name: &str) -> Option<Rc<Rel>> {
        let lower = name.to_ascii_lowercase();
        self.ctes
            .borrow()
            .iter()
            .rev()
            .find_map(|scope| scope.get(&lower).cloned())
    }

    /// Evaluates the FROM list; `pushable` are WHERE conjuncts that may narrow table scans.
    pub fn eval_from(
        &self,
        from: &[TableRef],
        pushable: &[&Expr],
        outer: Option<&Env<'_>>,
    ) -> SqlResult<Rel> {
        let mut acc: Option<Rel> = None;
        for item in from {
            let rel = self.eval_from_entry(item, pushable, outer, acc.as_ref())?;
            acc = Some(match acc {
                None => rel,
                Some(l) => self.join(l, rel, JoinKind::Cross, &JoinConstraint::None, outer)?,
            });
        }
        Ok(acc.unwrap_or(Rel {
            cols: Vec::new(),
            rows: vec![Vec::new()],
        }))
    }

    fn eval_from_entry(
        &self,
        item: &TableRef,
        pushable: &[&Expr],
        outer: Option<&Env<'_>>,
        lateral_left: Option<&Rel>,
    ) -> SqlResult<Rel> {
        match item {
            TableRef::Named {
                schema,
                name,
                alias,
            } => {
                if schema.is_none() {
                    if let Some(rel) = self.cte(name) {
                        return (*rel).clone().requalify(alias, Some(name));
                    }
                }
                if let Some(rel) = system::table(self, schema.as_deref(), name)? {
                    return rel.requalify(alias, Some(name));
                }
                let table = self.s.engine.table(name)?;
                let qual = alias.as_ref().map_or(name.as_str(), |a| a.name.as_str());
                let rows = self.scan_rows(&table, Some(qual), pushable, Vec::new(), outer)?;
                let rel = Rel {
                    cols: table_cols(&table),
                    rows: rows.into_iter().map(|r| r.row).collect(),
                };
                rel.requalify(alias, Some(&table.name))
            }
            TableRef::Derived {
                query,
                alias,
                lateral,
            } => {
                let rel = if *lateral {
                    // LATERAL: evaluated per left row is not supported beyond uncorrelated use.
                    let _ = lateral_left;
                    self.query(query, outer)?
                } else {
                    self.query(query, outer)?
                };
                rel.requalify(alias, None)
            }
            TableRef::Function { name, args, alias } => {
                let env = outer.copied().unwrap_or(Env::EMPTY);
                let vals = args
                    .iter()
                    .map(|a| self.eval(a, &env))
                    .collect::<SqlResult<Vec<_>>>()?;
                func::table_function(self, name, &vals)?.requalify(alias, Some(name))
            }
            TableRef::Join {
                left,
                right,
                kind,
                constraint,
            } => {
                // WHERE conjuncts may narrow the preserved side(s) only.
                let lp: &[&Expr] =
                    if matches!(kind, JoinKind::Inner | JoinKind::Cross | JoinKind::Left) {
                        pushable
                    } else {
                        &[]
                    };
                let rp: &[&Expr] =
                    if matches!(kind, JoinKind::Inner | JoinKind::Cross | JoinKind::Right) {
                        pushable
                    } else {
                        &[]
                    };
                let l = self.eval_from_entry(left, lp, outer, lateral_left)?;
                // Lookup join: fetch only the right rows whose join key appears on the left.
                let r = match (&**right, constraint) {
                    (
                        TableRef::Named {
                            schema: None,
                            name,
                            alias,
                        },
                        JoinConstraint::On(on),
                    ) if self.cte(name).is_none()
                        && !system::is_system(None, name)
                        && l.rows.len() <= 2000
                        && matches!(kind, JoinKind::Inner | JoinKind::Left) =>
                    {
                        let table = self.s.engine.table(name)?;
                        let qual = alias.as_ref().map_or(name.as_str(), |a| a.name.as_str());
                        let extra = self.lookup_filters(&table, qual, on, &l, outer)?;
                        let rows = self.scan_rows(&table, Some(qual), rp, extra, outer)?;
                        Rel {
                            cols: table_cols(&table),
                            rows: rows.into_iter().map(|r| r.row).collect(),
                        }
                        .requalify(alias, Some(&table.name))?
                    }
                    _ => self.eval_from_entry(right, rp, outer, Some(&l))?,
                };
                self.join(l, r, *kind, constraint, outer)
            }
        }
    }

    /// For `ON right.col = <left expr>`: an IN filter on `right.col` with the left side's values.
    fn lookup_filters(
        &self,
        table: &Table,
        qual: &str,
        on: &Expr,
        left: &Rel,
        outer: Option<&Env<'_>>,
    ) -> SqlResult<Vec<Filter>> {
        let mut out = Vec::new();
        for c in conjuncts(on) {
            let Expr::Binary {
                op: BinaryOp::Eq,
                left: a,
                right: b,
            } = c
            else {
                continue;
            };
            for (mine, other) in [(a, b), (b, a)] {
                let Expr::Column(p) = &**mine else { continue };
                let q = if p.len() > 1 {
                    Some(p[p.len() - 2].as_str())
                } else {
                    None
                };
                if q.is_some_and(|q| !q.eq_ignore_ascii_case(qual))
                    || (q.is_none()
                        && find_column(&left.cols, None, &p[p.len() - 1])
                            .ok()
                            .flatten()
                            .is_some())
                {
                    continue;
                }
                let Some(ci) = table.column_index(&p[p.len() - 1]) else {
                    continue;
                };
                // The other side must be computable from the left row alone.
                let refs = column_refs(other);
                if refs.is_empty() || has_subquery(other) {
                    continue;
                }
                if !refs.iter().all(|r| {
                    find_column(
                        &left.cols,
                        if r.len() > 1 {
                            Some(&r[r.len() - 2])
                        } else {
                            None
                        },
                        &r[r.len() - 1],
                    )
                    .ok()
                    .flatten()
                    .is_some()
                }) {
                    continue;
                }
                let col = &table.columns[ci];
                let mut seen = HashSet::new();
                let mut values = Vec::new();
                for row in &left.rows {
                    let env = Env {
                        cols: &left.cols,
                        row,
                        outer,
                        aggs: None,
                    };
                    let v = self.eval(other, &env)?;
                    if v.is_null() {
                        continue;
                    }
                    let Ok(cv) = cast(&v, &col.ty) else {
                        return Ok(Vec::new());
                    };
                    if seen.insert(hash_key(&cv)) {
                        values.push(datum::to_value(&cv, &col.ty));
                    }
                }
                if pushable_type(&col.ty) {
                    out.push(storine::is_in(&col.field, values));
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }

    fn join(
        &self,
        l: Rel,
        r: Rel,
        kind: JoinKind,
        constraint: &JoinConstraint,
        outer: Option<&Env<'_>>,
    ) -> SqlResult<Rel> {
        let mut cols = l.cols.clone();
        let mut rcols = r.cols.clone();
        // USING / NATURAL: equality on shared names; the right copies are hidden.
        let using: Vec<String> = match constraint {
            JoinConstraint::Using(names) => names.clone(),
            JoinConstraint::Natural => l
                .cols
                .iter()
                .filter(|c| {
                    !c.hidden
                        && r.cols
                            .iter()
                            .any(|rc| !rc.hidden && rc.name.eq_ignore_ascii_case(&c.name))
                })
                .map(|c| c.name.clone())
                .collect(),
            _ => Vec::new(),
        };
        let mut eq_pairs: Vec<(usize, usize)> = Vec::new();
        for n in &using {
            let li = find_column(&l.cols, None, n)?.ok_or_else(|| SqlError::undefined_column(n))?;
            let ri = find_column(&r.cols, None, n)?.ok_or_else(|| SqlError::undefined_column(n))?;
            rcols[ri].hidden = true;
            eq_pairs.push((li, ri));
        }
        cols.extend(rcols);
        let nl = l.cols.len();
        let on: Option<&Expr> = match constraint {
            JoinConstraint::On(e) => Some(e),
            _ => None,
        };
        // Hash keys from ON equalities between a left-only and a right-only expression.
        let mut hash_l: Vec<&Expr> = Vec::new();
        let mut hash_r: Vec<&Expr> = Vec::new();
        let mut residual: Vec<&Expr> = Vec::new();
        if let Some(on) = on {
            for c in conjuncts(on) {
                if let Expr::Binary {
                    op: BinaryOp::Eq,
                    left: a,
                    right: b,
                } = c
                {
                    let side = |e: &Expr| -> Option<bool> {
                        let refs = column_refs(e);
                        if refs.is_empty() || has_subquery(e) {
                            return None;
                        }
                        let all_l = refs.iter().all(|p| resolves(&l.cols, p));
                        let all_r = refs.iter().all(|p| resolves(&r.cols, p));
                        match (all_l, all_r) {
                            (true, false) => Some(true),
                            (false, true) => Some(false),
                            _ => None,
                        }
                    };
                    match (side(a), side(b)) {
                        (Some(true), Some(false)) => {
                            hash_l.push(a);
                            hash_r.push(b);
                            continue;
                        }
                        (Some(false), Some(true)) => {
                            hash_l.push(b);
                            hash_r.push(a);
                            continue;
                        }
                        _ => {}
                    }
                }
                residual.push(c);
            }
        }
        let lkey = |row: &Row| -> SqlResult<Option<String>> {
            let env = Env {
                cols: &l.cols,
                row,
                outer,
                aggs: None,
            };
            let mut k = String::new();
            for (li, _) in &eq_pairs {
                if row[*li].is_null() {
                    return Ok(None);
                }
                k.push_str(&hash_key(&row[*li]));
                k.push('\u{1}');
            }
            for e in &hash_l {
                let v = self.eval(e, &env)?;
                if v.is_null() {
                    return Ok(None);
                }
                k.push_str(&hash_key(&v));
                k.push('\u{1}');
            }
            Ok(Some(k))
        };
        let rkey = |row: &Row| -> SqlResult<Option<String>> {
            let env = Env {
                cols: &r.cols,
                row,
                outer,
                aggs: None,
            };
            let mut k = String::new();
            for (_, ri) in &eq_pairs {
                if row[*ri].is_null() {
                    return Ok(None);
                }
                k.push_str(&hash_key(&row[*ri]));
                k.push('\u{1}');
            }
            for e in &hash_r {
                let v = self.eval(e, &env)?;
                if v.is_null() {
                    return Ok(None);
                }
                k.push_str(&hash_key(&v));
                k.push('\u{1}');
            }
            Ok(Some(k))
        };
        let hashed = !eq_pairs.is_empty() || !hash_l.is_empty();
        let mut index: HashMap<String, Vec<usize>> = HashMap::new();
        if hashed {
            for (i, row) in r.rows.iter().enumerate() {
                if let Some(k) = rkey(row)? {
                    index.entry(k).or_default().push(i);
                }
            }
        }
        let all_r: Vec<usize> = (0..r.rows.len()).collect();
        let mut out = Vec::new();
        let mut r_matched = vec![false; r.rows.len()];
        let null_r = vec![Datum::Null; r.cols.len()];
        for lrow in &l.rows {
            let candidates: &[usize] = if hashed {
                match lkey(lrow)? {
                    Some(k) => index.get(&k).map_or(&[][..], Vec::as_slice),
                    None => &[],
                }
            } else {
                &all_r
            };
            let mut matched = false;
            for &ri in candidates {
                let mut row = lrow.clone();
                row.extend(r.rows[ri].iter().cloned());
                let env = Env {
                    cols: &cols,
                    row: &row,
                    outer,
                    aggs: None,
                };
                let mut ok = true;
                for c in &residual {
                    if !self.eval_bool(c, &env)? {
                        ok = false;
                        break;
                    }
                }
                if ok {
                    matched = true;
                    r_matched[ri] = true;
                    out.push(row);
                }
            }
            if !matched && matches!(kind, JoinKind::Left | JoinKind::Full) {
                let mut row = lrow.clone();
                row.extend(null_r.iter().cloned());
                out.push(row);
            }
        }
        if matches!(kind, JoinKind::Right | JoinKind::Full) {
            for (ri, m) in r_matched.iter().enumerate() {
                if !m {
                    let mut row = vec![Datum::Null; nl];
                    row.extend(r.rows[ri].iter().cloned());
                    // USING columns of a right-only row come from the right side.
                    for (li, rix) in &eq_pairs {
                        row[*li] = r.rows[ri][*rix].clone();
                    }
                    out.push(row);
                }
            }
        }
        Ok(Rel { cols, rows: out })
    }

    // ── table scans ──

    /// Rows of `table` that may satisfy `conjuncts` (filters pushed down where exact enough;
    /// callers re-check everything), merged with this transaction's own writes.
    pub fn scan_rows(
        &self,
        table: &Table,
        qual: Option<&str>,
        conjs: &[&Expr],
        mut extra: Vec<Filter>,
        outer: Option<&Env<'_>>,
    ) -> SqlResult<Vec<TableRow>> {
        let store = &self.s.engine.store;
        let txn = self.s.txn.as_ref();
        // Primary-key point lookups.
        if let Some(keys) = self.pk_lookup(table, qual, conjs, outer)? {
            let mut out = Vec::new();
            let mut seen = HashSet::new();
            for k in keys {
                if !seen.insert(k.clone()) {
                    continue;
                }
                match txn.and_then(|t| t.get(&table.collection, &k)) {
                    Some(Some(doc)) => out.push(TableRow {
                        key: k,
                        version: None,
                        row: table.from_doc(doc),
                    }),
                    Some(None) => {}
                    None => {
                        if let Some(d) = store.get(&table.collection, &k)? {
                            out.push(TableRow {
                                key: d.key,
                                version: Some(d.version),
                                row: table.from_doc(&d.fields),
                            });
                        }
                    }
                }
            }
            return Ok(out);
        }
        for c in conjs {
            if let Some(f) = self.to_filter(table, qual, c, outer) {
                extra.push(f);
            }
        }
        let filter = match extra.len() {
            0 => None,
            1 => extra.pop(),
            _ => Some(storine::and(extra)),
        };
        let mut out = Vec::new();
        let overlay: HashMap<&str, Option<&storine::Doc>> = txn
            .map(|t| t.entries(&table.collection).collect())
            .unwrap_or_default();
        for d in store.scan(&table.collection, filter)? {
            if overlay.contains_key(d.key.as_str()) {
                continue;
            }
            out.push(TableRow {
                row: table.from_doc(&d.fields),
                key: d.key,
                version: Some(d.version),
            });
        }
        for (k, doc) in overlay {
            if let Some(doc) = doc {
                out.push(TableRow {
                    key: k.to_owned(),
                    version: None,
                    row: table.from_doc(doc),
                });
            }
        }
        Ok(out)
    }

    /// Keys when the conjuncts pin the whole primary key to constants (`id = 5`, `id IN (1, 2)`).
    fn pk_lookup(
        &self,
        table: &Table,
        qual: Option<&str>,
        conjs: &[&Expr],
        outer: Option<&Env<'_>>,
    ) -> SqlResult<Option<Vec<String>>> {
        if table.primary_key.is_empty() {
            return Ok(None);
        }
        let mut choices: Vec<Option<Vec<Datum>>> = vec![None; table.primary_key.len()];
        for c in conjs {
            let (col, values) = match c {
                Expr::Binary {
                    op: BinaryOp::Eq,
                    left,
                    right,
                } => {
                    if let Some(ci) = self.own_column(table, qual, left) {
                        match self.constant(right, outer) {
                            Some(v) => (ci, vec![v]),
                            None => continue,
                        }
                    } else if let Some(ci) = self.own_column(table, qual, right) {
                        match self.constant(left, outer) {
                            Some(v) => (ci, vec![v]),
                            None => continue,
                        }
                    } else {
                        continue;
                    }
                }
                Expr::InList {
                    expr,
                    list,
                    negated: false,
                } => {
                    let Some(ci) = self.own_column(table, qual, expr) else {
                        continue;
                    };
                    let mut vals = Vec::new();
                    for x in list {
                        match self.constant(x, outer) {
                            Some(v) => vals.push(v),
                            None => {
                                vals.clear();
                                break;
                            }
                        }
                    }
                    if vals.is_empty() && !list.is_empty() {
                        continue;
                    }
                    (ci, vals)
                }
                _ => continue,
            };
            if let Some(slot) = table.primary_key.iter().position(|p| *p == col) {
                let ty = &table.columns[col].ty;
                let mut typed = Vec::new();
                for v in values {
                    if v.is_null() {
                        continue;
                    }
                    match cast(&v, ty) {
                        Ok(t) => typed.push(t),
                        Err(_) => return Ok(None),
                    }
                }
                choices[slot] = Some(typed);
            }
        }
        if choices.iter().any(Option::is_none) {
            return Ok(None);
        }
        // Cartesian product of the pinned values (bounded).
        let mut keys: Vec<Vec<Datum>> = vec![Vec::new()];
        for ch in choices.into_iter().flatten() {
            let mut next = Vec::new();
            for k in &keys {
                for v in &ch {
                    let mut k2 = k.clone();
                    k2.push(v.clone());
                    next.push(k2);
                }
            }
            keys = next;
            if keys.len() > 10_000 {
                return Ok(None);
            }
        }
        let mut out = Vec::new();
        for k in keys {
            let parts: Vec<&Datum> = k.iter().collect();
            out.push(crate::catalog::encode_key(&parts)?);
        }
        Ok(Some(out))
    }

    /// The table column `e` names, if it is a reference to one of `table`'s columns.
    fn own_column(&self, table: &Table, qual: Option<&str>, e: &Expr) -> Option<usize> {
        let Expr::Column(p) = e else { return None };
        if p.len() > 1 {
            let q = &p[p.len() - 2];
            let ok = qual.is_some_and(|x| x.eq_ignore_ascii_case(q))
                || q.eq_ignore_ascii_case(&table.name);
            if !ok {
                return None;
            }
        }
        table.column_index(&p[p.len() - 1])
    }

    /// The value of `e` if it does not depend on the current row.
    fn constant(&self, e: &Expr, outer: Option<&Env<'_>>) -> Option<Datum> {
        if has_subquery(e) || has_aggregate(e) {
            return None;
        }
        if column_refs(e).is_empty() {
            return self.eval_const(e).ok();
        }
        // Correlated with an enclosing query: constant for this scan.
        let env = outer?;
        self.eval(e, env).ok()
    }

    /// A storage filter equivalent to (or wider than) conjunct `c` for this table.
    fn to_filter(
        &self,
        table: &Table,
        qual: Option<&str>,
        c: &Expr,
        outer: Option<&Env<'_>>,
    ) -> Option<Filter> {
        let typed = |ci: usize, v: Datum| -> Option<storine::Value> {
            let col = &table.columns[ci];
            if !pushable_type(&col.ty) || v.is_null() {
                return None;
            }
            cast(&v, &col.ty).ok().map(|t| datum::to_value(&t, &col.ty))
        };
        match c {
            Expr::Binary { op, left, right }
                if matches!(
                    op,
                    BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq
                ) =>
            {
                let (ci, v, flipped) = if let Some(ci) = self.own_column(table, qual, left) {
                    (ci, self.constant(right, outer)?, false)
                } else {
                    let ci = self.own_column(table, qual, right)?;
                    (ci, self.constant(left, outer)?, true)
                };
                let field = table.columns[ci].field.as_str();
                let val = typed(ci, v)?;
                Some(match (op, flipped) {
                    (BinaryOp::Eq, _) => storine::eq(field, val),
                    (BinaryOp::Lt, false) | (BinaryOp::Gt, true) => storine::lt(field, val),
                    (BinaryOp::LtEq, false) | (BinaryOp::GtEq, true) => storine::le(field, val),
                    (BinaryOp::Gt, false) | (BinaryOp::Lt, true) => storine::gt(field, val),
                    _ => storine::ge(field, val),
                })
            }
            Expr::Between {
                expr,
                low,
                high,
                negated: false,
            } => {
                let ci = self.own_column(table, qual, expr)?;
                let lo = typed(ci, self.constant(low, outer)?)?;
                let hi = typed(ci, self.constant(high, outer)?)?;
                Some(storine::between(&table.columns[ci].field, lo, hi))
            }
            Expr::InList {
                expr,
                list,
                negated: false,
            } => {
                let ci = self.own_column(table, qual, expr)?;
                let mut vals = Vec::new();
                for x in list {
                    let v = self.constant(x, outer)?;
                    if v.is_null() {
                        continue;
                    }
                    vals.push(typed(ci, v)?);
                }
                Some(storine::is_in(&table.columns[ci].field, vals))
            }
            Expr::IsNull {
                expr,
                negated: true,
            } => {
                let ci = self.own_column(table, qual, expr)?;
                Some(storine::exists(&table.columns[ci].field))
            }
            Expr::Like {
                expr,
                pattern,
                negated: false,
                case_insensitive: false,
                escape: None,
            } => {
                let ci = self.own_column(table, qual, expr)?;
                if !matches!(
                    table.columns[ci].ty,
                    DataType::Text | DataType::Varchar(_) | DataType::Char(_)
                ) {
                    return None;
                }
                let p = self.constant(pattern, outer)?.as_text();
                let lit: String = p
                    .chars()
                    .take_while(|ch| !matches!(ch, '%' | '_' | '\\'))
                    .collect();
                if lit.is_empty() {
                    return None;
                }
                Some(storine::prefix(&table.columns[ci].field, &lit))
            }
            _ => None,
        }
    }

    // ── SELECT ──

    pub fn select(
        &self,
        sel: &Select,
        outer: Option<&Env<'_>>,
        order_by: &[OrderBy],
        limit: Option<usize>,
        offset: usize,
    ) -> SqlResult<Rel> {
        let where_conjs: Vec<&Expr> = sel.where_.as_ref().map(conjuncts).unwrap_or_default();
        let pushable: Vec<&Expr> = where_conjs
            .iter()
            .copied()
            .filter(|c| !has_subquery(c) && !has_aggregate(c))
            .collect();
        let src = self.eval_from(&sel.from, &pushable, outer)?;
        // WHERE.
        let mut rows = Vec::with_capacity(src.rows.len());
        for row in src.rows {
            let ok = match &sel.where_ {
                None => true,
                Some(w) => self.eval_bool(
                    w,
                    &Env {
                        cols: &src.cols,
                        row: &row,
                        outer,
                        aggs: None,
                    },
                )?,
            };
            if ok {
                rows.push(row);
            }
        }
        let cols = src.cols;
        // Expand the select list.
        let mut items: Vec<(Expr, String, Option<String>)> = Vec::new(); // expr, name, explicit alias
        for it in &sel.items {
            match it {
                SelectItem::Wildcard(q) => {
                    let mut any = false;
                    for c in &cols {
                        if c.hidden {
                            continue;
                        }
                        if let Some(q) = q {
                            if !c
                                .qual
                                .as_deref()
                                .is_some_and(|cq| cq.eq_ignore_ascii_case(q))
                            {
                                continue;
                            }
                        }
                        any = true;
                        let mut parts = Vec::new();
                        if let Some(cq) = &c.qual {
                            parts.push(cq.clone());
                        }
                        parts.push(c.name.clone());
                        items.push((Expr::Column(parts), c.name.clone(), None));
                    }
                    if !any {
                        if let Some(q) = q {
                            return Err(SqlError::new(
                                "42P01",
                                format!("missing FROM-clause entry for table \"{q}\""),
                            ));
                        }
                    }
                }
                SelectItem::Expr { expr, alias } => {
                    let name = alias.clone().unwrap_or_else(|| self.derive_name(expr));
                    items.push((expr.clone(), name, alias.clone()));
                }
            }
        }
        let aggregated = !sel.group_by.is_empty()
            || items.iter().any(|(e, _, _)| has_aggregate(e))
            || sel.having.as_ref().is_some_and(has_aggregate)
            || order_by.iter().any(|o| has_aggregate(&o.expr));
        let mut out_cols: Vec<ColMeta> = items
            .iter()
            .map(|(e, name, _)| {
                let (ty, table) = self.static_type(e, &cols);
                ColMeta {
                    qual: None,
                    name: name.clone(),
                    ty,
                    table,
                    hidden: false,
                }
            })
            .collect();
        // Each output row with its ORDER BY keys.
        let mut produced: Vec<(Row, Vec<Datum>)> = Vec::new();
        let order_key = |this: &Self, env: &Env<'_>, out: &Row| -> SqlResult<Vec<Datum>> {
            order_by
                .iter()
                .map(|o| {
                    if let Expr::Number(n) = &o.expr {
                        let i: usize = n
                            .parse()
                            .map_err(|_| SqlError::syntax(format!("bad ORDER BY position {n}")))?;
                        return out.get(i.wrapping_sub(1)).cloned().ok_or_else(|| {
                            SqlError::syntax(format!("ORDER BY position {i} is not in select list"))
                        });
                    }
                    // A bare name matching an output alias sorts by that output column.
                    if let Expr::Column(p) = &o.expr {
                        if p.len() == 1 {
                            if let Some(i) = items
                                .iter()
                                .position(|(_, n, a)| a.is_some() && n.eq_ignore_ascii_case(&p[0]))
                            {
                                return Ok(out[i].clone());
                            }
                            if find_column(env.cols, None, &p[0])?.is_none() {
                                if let Some(i) = items
                                    .iter()
                                    .position(|(_, n, _)| n.eq_ignore_ascii_case(&p[0]))
                                {
                                    return Ok(out[i].clone());
                                }
                            }
                        }
                    }
                    this.eval(&o.expr, env)
                })
                .collect()
        };
        // Rows to project, each with the aggregate (and window) values it sees.
        type Work = (Row, Vec<(Expr, Datum)>);
        let mut work: Vec<Work> = Vec::new();
        if aggregated {
            // Collect every aggregate call (window functions are computed separately).
            let mut agg_exprs: Vec<Expr> = Vec::new();
            let mut collect = |e: &Expr| {
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
                })
            };
            for (e, _, _) in &items {
                collect(e);
            }
            if let Some(h) = &sel.having {
                collect(h);
            }
            for o in order_by {
                collect(&o.expr);
            }
            // GROUP BY expressions (ordinals and output aliases allowed).
            let group_exprs: Vec<Expr> = sel
                .group_by
                .iter()
                .map(|g| match g {
                    Expr::Number(n) => n
                        .parse::<usize>()
                        .ok()
                        .and_then(|i| items.get(i.wrapping_sub(1)))
                        .map_or_else(|| g.clone(), |(e, _, _)| e.clone()),
                    Expr::Column(p)
                        if p.len() == 1
                            && find_column(&cols, None, &p[0]).ok().flatten().is_none() =>
                    {
                        items
                            .iter()
                            .find(|(_, n, _)| n.eq_ignore_ascii_case(&p[0]))
                            .map_or_else(|| g.clone(), |(e, _, _)| e.clone())
                    }
                    other => other.clone(),
                })
                .collect();
            let mut groups: Vec<Vec<usize>> = Vec::new();
            let mut by_key: HashMap<String, usize> = HashMap::new();
            for (i, row) in rows.iter().enumerate() {
                let env = Env {
                    cols: &cols,
                    row,
                    outer,
                    aggs: None,
                };
                let mut k = String::new();
                for g in &group_exprs {
                    k.push_str(&hash_key(&self.eval(g, &env)?));
                    k.push('\u{1}');
                }
                let gi = *by_key.entry(k).or_insert_with(|| {
                    groups.push(Vec::new());
                    groups.len() - 1
                });
                groups[gi].push(i);
            }
            if groups.is_empty() && group_exprs.is_empty() {
                groups.push(Vec::new());
            }
            let null_row: Row = vec![Datum::Null; cols.len()];
            for g in &groups {
                let mut aggs: Vec<(Expr, Datum)> = Vec::with_capacity(agg_exprs.len());
                for a in &agg_exprs {
                    let v = self.aggregate(a, g.iter().map(|i| &rows[*i]), &cols, outer)?;
                    aggs.push((a.clone(), v));
                }
                let rep = g.first().map_or(&null_row, |i| &rows[*i]);
                let env = Env {
                    cols: &cols,
                    row: rep,
                    outer,
                    aggs: Some(&aggs),
                };
                if let Some(h) = &sel.having {
                    if !self.eval_bool(h, &env)? {
                        continue;
                    }
                }
                work.push((rep.clone(), aggs));
            }
        } else {
            work = rows.into_iter().map(|r| (r, Vec::new())).collect();
        }
        // Window functions over the rows (or groups) that survived WHERE / HAVING.
        let mut wins: Vec<Expr> = Vec::new();
        for e in items
            .iter()
            .map(|(e, _, _)| e)
            .chain(order_by.iter().map(|o| &o.expr))
        {
            walk(e, &mut |x| {
                if matches!(x, Expr::Function { over: Some(_), .. }) && !wins.contains(x) {
                    wins.push(x.clone());
                }
            });
        }
        for w in &wins {
            self.window(w, &mut work, &cols, outer)?;
        }
        for (row, aggs) in &work {
            let env = Env {
                cols: &cols,
                row,
                outer,
                aggs: Some(aggs),
            };
            let out = items
                .iter()
                .map(|(e, _, _)| self.eval(e, &env))
                .collect::<SqlResult<Row>>()?;
            let keys = order_key(self, &env, &out)?;
            produced.push((out, keys));
        }
        if sel.distinct {
            let mut seen = HashSet::new();
            produced.retain(|(r, _)| {
                seen.insert(r.iter().map(hash_key).collect::<Vec<_>>().join("\u{1}"))
            });
        }
        if !order_by.is_empty() {
            produced.sort_by(|a, b| self.order_cmp(order_by, &a.1, &b.1));
        }
        let mut out_rows: Vec<Row> = produced.into_iter().map(|(r, _)| r).collect();
        apply_limit(&mut out_rows, limit, offset);
        // Untyped columns take the type of their first non-null value.
        for (i, c) in out_cols.iter_mut().enumerate() {
            if c.ty.is_none() {
                c.ty = out_rows
                    .iter()
                    .map(|r| &r[i])
                    .find(|d| !d.is_null())
                    .map(Datum::natural_type);
            }
        }
        Ok(Rel {
            cols: out_cols,
            rows: out_rows,
        })
    }

    /// Computes window function `w` for every row of `work` (appended to the row's values).
    fn window(
        &self,
        w: &Expr,
        work: &mut [(Row, Vec<(Expr, Datum)>)],
        cols: &[ColMeta],
        outer: Option<&Env<'_>>,
    ) -> SqlResult<()> {
        let Expr::Function {
            name,
            args,
            star,
            distinct,
            over: Some(spec),
        } = w
        else {
            return Ok(());
        };
        // Partitions in order of first appearance.
        let mut parts: Vec<Vec<usize>> = Vec::new();
        let mut by_key: HashMap<String, usize> = HashMap::new();
        let mut keys: Vec<Vec<Datum>> = Vec::with_capacity(work.len());
        for (i, (row, aggs)) in work.iter().enumerate() {
            let env = Env {
                cols,
                row,
                outer,
                aggs: Some(aggs),
            };
            let mut k = String::new();
            for p in &spec.partition_by {
                k.push_str(&hash_key(&self.eval(p, &env)?));
                k.push('\u{1}');
            }
            let pi = *by_key.entry(k).or_insert_with(|| {
                parts.push(Vec::new());
                parts.len() - 1
            });
            parts[pi].push(i);
            keys.push(
                spec.order_by
                    .iter()
                    .map(|o| self.eval(&o.expr, &env))
                    .collect::<SqlResult<Vec<_>>>()?,
            );
        }
        let mut values: Vec<Datum> = vec![Datum::Null; work.len()];
        let arg = |this: &Self, i: usize, k: usize| -> SqlResult<Datum> {
            match args.get(k) {
                Some(a) => this.eval(
                    a,
                    &Env {
                        cols,
                        row: &work[i].0,
                        outer,
                        aggs: Some(&work[i].1),
                    },
                ),
                None => Ok(Datum::Null),
            }
        };
        for part in &mut parts {
            part.sort_by(|a, b| self.order_cmp(&spec.order_by, &keys[*a], &keys[*b]));
            let n = part.len();
            // Peer groups: rows with equal ORDER BY keys.
            let mut peer_end = vec![0usize; n];
            let mut peer_start = vec![0usize; n];
            let mut start = 0;
            for pos in 0..n {
                if pos > 0
                    && self.order_cmp(&spec.order_by, &keys[part[pos]], &keys[part[pos - 1]])
                        != Ordering::Equal
                {
                    start = pos;
                }
                peer_start[pos] = start;
            }
            let mut end = n;
            for pos in (0..n).rev() {
                if pos + 1 < n
                    && self.order_cmp(&spec.order_by, &keys[part[pos]], &keys[part[pos + 1]])
                        != Ordering::Equal
                {
                    end = pos + 1;
                }
                peer_end[pos] = end;
            }
            let mut dense = 0i64;
            for pos in 0..n {
                let i = part[pos];
                if peer_start[pos] == pos {
                    dense += 1;
                }
                values[i] = match name.as_str() {
                    "row_number" => Datum::Int(pos as i64 + 1),
                    "rank" => Datum::Int(peer_start[pos] as i64 + 1),
                    "dense_rank" => Datum::Int(dense),
                    "percent_rank" => Datum::Float(if n > 1 {
                        peer_start[pos] as f64 / (n - 1) as f64
                    } else {
                        0.0
                    }),
                    "cume_dist" => Datum::Float(peer_end[pos] as f64 / n as f64),
                    "ntile" => {
                        let buckets = arg(self, i, 0)?.as_i64()?.max(1) as usize;
                        Datum::Int((pos * buckets / n) as i64 + 1)
                    }
                    "lag" | "lead" => {
                        let off = if args.len() > 1 {
                            arg(self, i, 1)?.as_i64()?
                        } else {
                            1
                        };
                        let target = if name == "lag" {
                            pos as i64 - off
                        } else {
                            pos as i64 + off
                        };
                        if target >= 0 && (target as usize) < n {
                            arg(self, part[target as usize], 0)?
                        } else {
                            arg(self, i, 2)?
                        }
                    }
                    "first_value" => arg(self, part[0], 0)?,
                    "last_value" => {
                        let last = if spec.order_by.is_empty() {
                            n - 1
                        } else {
                            peer_end[pos] - 1
                        };
                        arg(self, part[last], 0)?
                    }
                    "nth_value" => {
                        let k = arg(self, i, 1)?.as_i64()?.max(1) as usize;
                        let last = if spec.order_by.is_empty() {
                            n
                        } else {
                            peer_end[pos]
                        };
                        if k <= last {
                            arg(self, part[k - 1], 0)?
                        } else {
                            Datum::Null
                        }
                    }
                    agg if is_aggregate(agg) => {
                        // Default frame: the whole partition, or up to the current peers with ORDER BY.
                        let last = if spec.order_by.is_empty() {
                            n
                        } else {
                            peer_end[pos]
                        };
                        if pos > 0
                            && peer_end[pos] == peer_end[pos - 1]
                            && !spec.order_by.is_empty()
                            || (pos > 0 && spec.order_by.is_empty())
                        {
                            values[part[pos - 1]].clone()
                        } else {
                            let plain = Expr::Function {
                                name: name.clone(),
                                args: args.clone(),
                                star: *star,
                                distinct: *distinct,
                                over: None,
                            };
                            let rows: Vec<&Row> =
                                part[..last].iter().map(|j| &work[*j].0).collect();
                            self.aggregate(&plain, rows.into_iter(), cols, outer)?
                        }
                    }
                    other => {
                        return Err(SqlError::unsupported(format!("window function {other}()")))
                    }
                };
            }
        }
        for (i, v) in values.into_iter().enumerate() {
            work[i].1.push((w.clone(), v));
        }
        Ok(())
    }

    /// Computes one aggregate over a group's rows.
    fn aggregate<'r>(
        &self,
        e: &Expr,
        rows: impl Iterator<Item = &'r Row>,
        cols: &[ColMeta],
        outer: Option<&Env<'_>>,
    ) -> SqlResult<Datum> {
        let Expr::Function {
            name,
            args,
            star,
            distinct,
            ..
        } = e
        else {
            return Err(SqlError::internal("not an aggregate"));
        };
        let mut values: Vec<Datum> = Vec::new();
        let mut separator = None;
        let mut count = 0i64;
        for row in rows {
            let env = Env {
                cols,
                row,
                outer,
                aggs: None,
            };
            if *star {
                count += 1;
                continue;
            }
            let v = match args.first() {
                Some(a) => self.eval(a, &env)?,
                None => Datum::Null,
            };
            if separator.is_none()
                && matches!(name.as_str(), "string_agg" | "group_concat" | "listagg")
                && args.len() > 1
            {
                separator = Some(self.eval(&args[args.len() - 1], &env)?.as_text());
            }
            if !v.is_null() || matches!(name.as_str(), "array_agg" | "json_agg" | "jsonb_agg") {
                values.push(v);
            }
        }
        if *distinct {
            let mut seen = HashSet::new();
            values.retain(|v| seen.insert(hash_key(v)));
        }
        func::finish_aggregate(self, name, *star, count, values, separator)
    }

    /// The column name PostgreSQL / MySQL would report for an unaliased expression.
    fn derive_name(&self, e: &Expr) -> String {
        let mysql = self.dialect() == Dialect::MySql;
        match e {
            Expr::Column(p) => p.last().cloned().unwrap_or_default(),
            _ if mysql => e
                .to_string()
                .trim_matches(|c| c == '(' || c == ')')
                .to_owned(),
            Expr::Function { name, .. } => name.clone(),
            Expr::Cast { expr, ty } => match &**expr {
                Expr::Column(p) => p.last().cloned().unwrap_or_default(),
                _ => datum::type_sql(ty)
                    .split(['(', ' '])
                    .next()
                    .unwrap_or("?column?")
                    .to_owned(),
            },
            Expr::Case { .. } => "case".into(),
            Expr::Exists { .. } => "exists".into(),
            Expr::Subquery(q) => match &q.body {
                SetExpr::Select(s) => match s.items.first() {
                    Some(SelectItem::Expr { alias: Some(a), .. }) => a.clone(),
                    Some(SelectItem::Expr { expr, .. }) => self.derive_name(expr),
                    _ => "?column?".into(),
                },
                _ => "?column?".into(),
            },
            Expr::Variable { name, system } => {
                format!("{}{name}", if *system { "@@" } else { "@" })
            }
            _ => "?column?".into(),
        }
    }

    /// The type and source table of an output expression, when known without evaluating it.
    pub fn static_type(&self, e: &Expr, cols: &[ColMeta]) -> (Option<DataType>, Option<String>) {
        match e {
            Expr::Column(p) => {
                let q = if p.len() > 1 {
                    Some(p[p.len() - 2].as_str())
                } else {
                    None
                };
                match find_column(cols, q, &p[p.len() - 1]) {
                    Ok(Some(i)) => (cols[i].ty.clone(), cols[i].table.clone()),
                    _ => (None, None),
                }
            }
            Expr::Cast { ty, .. } => (Some(ty.clone()), None),
            Expr::Bool(_)
            | Expr::IsNull { .. }
            | Expr::IsBool { .. }
            | Expr::Exists { .. }
            | Expr::InList { .. }
            | Expr::InSubquery { .. }
            | Expr::Like { .. }
            | Expr::Between { .. } => (
                Some(if self.dialect() == Dialect::MySql {
                    DataType::BigInt
                } else {
                    DataType::Bool
                }),
                None,
            ),
            Expr::Binary { op, left, right } => match op {
                BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq
                | BinaryOp::And
                | BinaryOp::Or => (
                    Some(if self.dialect() == Dialect::MySql {
                        DataType::BigInt
                    } else {
                        DataType::Bool
                    }),
                    None,
                ),
                BinaryOp::Concat => (Some(DataType::Text), None),
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
                    let (a, _) = self.static_type(left, cols);
                    let (b, _) = self.static_type(right, cols);
                    (numeric_result(a, b), None)
                }
                _ => (None, None),
            },
            Expr::Function {
                name, args, star, ..
            } => (func::return_type(self, name, args, *star, cols), None),
            Expr::Str(_) => (Some(DataType::Text), None),
            _ => (None, None),
        }
    }
}

/// Result type of arithmetic between two typed operands.
fn numeric_result(a: Option<DataType>, b: Option<DataType>) -> Option<DataType> {
    use DataType::*;
    let rank = |t: &DataType| match t {
        SmallInt | Int | BigInt => 1,
        Numeric(_) => 2,
        Real | Double => 3,
        _ => 0,
    };
    match (a, b) {
        (Some(x), Some(y)) => {
            let (rx, ry) = (rank(&x), rank(&y));
            if rx == 0 || ry == 0 {
                return None;
            }
            Some(match rx.max(ry) {
                1 => BigInt,
                2 => Numeric(None),
                _ => Double,
            })
        }
        _ => None,
    }
}

fn resolves(cols: &[ColMeta], p: &[String]) -> bool {
    let q = if p.len() > 1 {
        Some(p[p.len() - 2].as_str())
    } else {
        None
    };
    matches!(find_column(cols, q, &p[p.len() - 1]), Ok(Some(_)))
}

/// Columns of a base table as an intermediate relation.
pub fn table_cols(t: &Table) -> Vec<ColMeta> {
    t.columns
        .iter()
        .map(|c| ColMeta {
            qual: Some(t.name.clone()),
            name: c.name.clone(),
            ty: Some(c.ty.clone()),
            table: Some(t.name.clone()),
            hidden: false,
        })
        .collect()
}

/// Types whose stored order and equality match SQL's, so filters can be pushed down.
fn pushable_type(t: &DataType) -> bool {
    matches!(
        t,
        DataType::Bool
            | DataType::SmallInt
            | DataType::Int
            | DataType::BigInt
            | DataType::Numeric(_)
            | DataType::Real
            | DataType::Double
            | DataType::Text
            | DataType::Varchar(_)
            | DataType::Char(_)
            | DataType::Enum(_)
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Date
            | DataType::Uuid
    )
}

fn apply_limit(rows: &mut Vec<Row>, limit: Option<usize>, offset: usize) {
    if offset > 0 {
        if offset >= rows.len() {
            rows.clear();
        } else {
            rows.drain(..offset);
        }
    }
    if let Some(l) = limit {
        rows.truncate(l);
    }
}
