//! `EXPLAIN [ANALYZE]` (`F-ENT-03`, `TASK-147`), and `SET SHOWPLAN_*` on the SQL Server endpoint.
//!
//! The plan is built from the executor's own decisions: the same primary-key, filter push-down,
//! aggregate push-down, vector index and join-key rules, called the same way. `ANALYZE` runs the
//! statement (its changes stay, as in PostgreSQL) with a trace of the actual rows and time at each
//! scan, join and select. Nodes the executor does not run separately (a sort inside a select) show
//! the actuals of the select that holds them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::ast::*;
use crate::datum::Datum;
use crate::error::SqlResult;
use crate::exec::eval::{has_aggregate, Exec};
use crate::exec::select::{conjuncts, has_subquery};
use crate::exec::system;
use crate::exec::{OutCol, Outcome, ResultSet, Session};
use crate::lexer::Dialect;

/// What actually happened at one plan node.
#[derive(Debug, Clone, Copy, Default)]
pub struct Actual {
    pub loops: u64,
    pub rows: u64,
    pub ns: u64,
}

/// The trace of an `EXPLAIN ANALYZE`, by address of the AST node.
pub type Trace = Arc<Mutex<HashMap<usize, Actual>>>;

/// One node of a plan.
#[derive(Debug, Default)]
pub struct Node {
    pub kind: String,
    pub table: Option<String>,
    pub alias: Option<String>,
    pub index: Option<String>,
    /// Lines of detail: (name, text).
    pub details: Vec<(String, String)>,
    /// The AST node the trace knows it by.
    pub key: Option<usize>,
    pub rows: Option<f64>,
    pub cost: Option<f64>,
    pub children: Vec<Node>,
}

impl Node {
    fn new(kind: &str) -> Node {
        Node { kind: kind.to_owned(), ..Node::default() }
    }

    fn with(mut self, name: &str, text: impl Into<String>) -> Node {
        self.details.push((name.to_owned(), text.into()));
        self
    }

    fn over(mut self, child: Node) -> Node {
        self.rows = self.rows.or(child.rows);
        self.cost = self.cost.or(child.cost);
        self.children.push(child);
        self
    }
}

fn list(es: &[Expr]) -> String {
    es.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
}

impl Exec<'_> {
    pub(crate) fn plan_statement(&self, st: &Statement) -> SqlResult<Node> {
        Ok(match st {
            Statement::Query(q) => self.plan_query(q)?,
            Statement::Insert(i) => {
                let mut n = Node::new("Insert");
                n.table = Some(i.table.clone());
                if let InsertSource::Query(q) = &i.source {
                    n = n.over(self.plan_query(q)?);
                } else if let InsertSource::Values(v) = &i.source {
                    let mut c = Node::new("Values Scan");
                    c.rows = Some(v.len() as f64);
                    n = n.over(c);
                }
                if i.on_conflict.is_some() {
                    n = n.with("Conflict Resolution", "yes");
                }
                n
            }
            Statement::Update(u) => {
                let mut n = Node::new("Update");
                n.table = Some(u.table.clone());
                n.over(self.plan_dml_scan(&u.table, u.alias.as_deref(), u.where_.as_ref())?)
            }
            Statement::Delete(d) => {
                let mut n = Node::new("Delete");
                n.table = Some(d.table.clone());
                n.over(self.plan_dml_scan(&d.table, d.alias.as_deref(), d.where_.as_ref())?)
            }
            Statement::Merge(m) => {
                let mut n = Node::new("Merge");
                n.table = Some(m.target.clone());
                n
            }
            other => Node::new("Utility").with("Statement", utility_name(other)),
        })
    }

    fn plan_dml_scan(&self, table: &str, alias: Option<&str>, where_: Option<&Expr>) -> SqlResult<Node> {
        let conjs: Vec<&Expr> = where_.map(conjuncts).unwrap_or_default();
        let t = self.s.engine.table(table)?;
        let mut n = self.plan_table_scan(&t, alias.unwrap_or(table), alias, &conjs, None)?;
        if let Some(w) = where_ {
            n = n.with("Filter", w.to_string());
        }
        Ok(n)
    }

    pub(crate) fn plan_query(&self, q: &Query) -> SqlResult<Node> {
        let mut top = match &q.body {
            SetExpr::Select(sel) => self.plan_select(sel, &q.order_by, q)?,
            other => {
                let mut b = self.plan_set_expr(other)?;
                if !q.order_by.is_empty() {
                    b = Node::new("Sort").with("Sort Key", order_keys(&q.order_by)).over(b);
                }
                b
            }
        };
        if q.limit.is_some() || q.offset.is_some() {
            top = Node::new("Limit").over(top);
        }
        for c in &q.ctes {
            top.children.push(self.plan_query(&c.query)?.with("CTE", c.name.clone()));
        }
        top.key = top.key.or(Some(q as *const Query as usize));
        Ok(top)
    }

    fn plan_set_expr(&self, e: &SetExpr) -> SqlResult<Node> {
        Ok(match e {
            SetExpr::Select(s) => self.plan_select(s, &[], &Query::empty())?,
            SetExpr::Nested(q) => self.plan_query(q)?,
            SetExpr::Values(rows) => {
                let mut n = Node::new("Values Scan");
                n.rows = Some(rows.len() as f64);
                n
            }
            SetExpr::SetOp { op, all, left, right } => {
                let kind = match (op, all) {
                    (SetOp::Union, true) => "Append",
                    (SetOp::Union, false) => "HashAggregate (UNION)",
                    (SetOp::Intersect, _) => "Intersect",
                    (SetOp::Except, _) => "Except",
                };
                let mut n = Node::new(kind);
                n.children.push(self.plan_set_expr(left)?);
                n.children.push(self.plan_set_expr(right)?);
                n
            }
        })
    }

    fn plan_select(&self, sel: &Select, order_by: &[OrderBy], q: &Query) -> SqlResult<Node> {
        let key = Some(sel as *const Select as usize);
        // Aggregations the store computes exactly: nothing above them reads rows.
        if let Some(plan) = self.plan_pushdown(sel, order_by, None)? {
            let mut n = Node::new("Aggregate Push-Down");
            n.details.push(("Plan".into(), Exec::explain_pushdown(&plan)));
            n.table = Some(plan.table.name.clone());
            n.key = key;
            return Ok(n);
        }
        let conjs: Vec<&Expr> = sel.where_.as_ref().map(conjuncts).unwrap_or_default();
        let pushable: Vec<&Expr> = conjs.iter().copied().filter(|c| !has_subquery(c) && !has_aggregate(c)).collect();
        let limit_known = q.limit.is_some();
        let vector = self.vector_hint_for(sel, order_by, if limit_known { Some(10) } else { None }, 0).ok().flatten();
        let mut from: Option<Node> = None;
        if let Some((flat, order)) = self.plan_join_order(&sel.from, &pushable)? {
            from = Some(self.plan_planned_join(&flat, &order, &pushable)?);
        }
        for item in if from.is_some() { &[][..] } else { &sel.from[..] } {
            let right = self.plan_from(item, &pushable, vector.is_some())?;
            from = Some(match from {
                None => right,
                Some(l) => {
                    let mut n = Node::new("Nested Loop").with("Join Type", "Cross");
                    n.children.push(l);
                    n.children.push(right);
                    n
                }
            });
        }
        let mut top = from.unwrap_or_else(|| Node::new("Result"));
        // WHERE is re-checked exactly above the scans, whatever was pushed down.
        if let Some(w) = &sel.where_ {
            if sel.from.len() != 1 || !matches!(sel.from[0], TableRef::Named { .. }) {
                top = Node::new("Filter").with("Filter", w.to_string()).over(top);
            } else {
                top = top.with("Filter", w.to_string());
            }
        }
        let aggregated = !sel.group_by.is_empty() || sel.having.is_some() || sel.items.iter().any(|i| matches!(i, SelectItem::Expr { expr, .. } if has_aggregate(expr)));
        if aggregated {
            let mut a = Node::new(if sel.group_by.is_empty() { "Aggregate" } else { "HashAggregate" });
            if !sel.group_by.is_empty() {
                a = a.with("Group Key", list(&sel.group_by));
            }
            if let Some(h) = &sel.having {
                a = a.with("Filter", h.to_string());
            }
            top = a.over(top);
        }
        if sel.distinct || !sel.distinct_on.is_empty() {
            top = Node::new("Unique").over(top);
        }
        if !order_by.is_empty() {
            top = Node::new("Sort").with("Sort Key", order_keys(order_by)).over(top);
        }
        top.key = key;
        Ok(top)
    }

    fn plan_from(&self, item: &TableRef, pushable: &[&Expr], vector: bool) -> SqlResult<Node> {
        let key = Some(item as *const TableRef as usize);
        let mut n = match item {
            TableRef::Named { schema, name, alias } => {
                let qual = alias.as_ref().map_or(name.as_str(), |a| a.name.as_str());
                let aname = alias.as_ref().map(|a| a.name.as_str());
                if self.cte(name).is_some() {
                    let mut c = Node::new("CTE Scan");
                    c.table = Some(name.clone());
                    c
                } else if name.starts_with('@') {
                    let mut c = Node::new("Table Variable Scan");
                    c.table = Some(name.clone());
                    c
                } else if system::is_system(schema.as_deref(), name) {
                    let mut c = Node::new("System View Scan");
                    c.table = Some(name.clone());
                    c
                } else if let Some(t) = self.s.engine.find_table(name)? {
                    self.plan_table_scan(&t, qual, aname, pushable, if vector { Some(()) } else { None })?
                } else if self.s.engine.view(name)?.is_some() {
                    let mut c = Node::new("View Scan");
                    c.table = Some(name.clone());
                    c
                } else {
                    return Err(crate::error::SqlError::undefined_table(name));
                }
            }
            TableRef::Derived { query, alias, .. } => {
                let mut c = Node::new("Subquery Scan").over(self.plan_query(query)?);
                c.alias = alias.as_ref().map(|a| a.name.clone());
                c
            }
            TableRef::Function { name, alias, .. } => {
                let mut c = Node::new("Function Scan");
                c.table = Some(name.clone());
                c.alias = alias.as_ref().map(|a| a.name.clone());
                c
            }
            TableRef::Join { left, right, kind, constraint } => {
                let lp: &[&Expr] = if matches!(kind, JoinKind::Inner | JoinKind::Cross | JoinKind::Left) { pushable } else { &[] };
                let rp: &[&Expr] = if matches!(kind, JoinKind::Inner | JoinKind::Cross | JoinKind::Right) { pushable } else { &[] };
                let l = self.plan_from(left, lp, false)?;
                let r = self.plan_from(right, rp, false)?;
                let type_name = match kind {
                    JoinKind::Inner => "Inner",
                    JoinKind::Left => "Left",
                    JoinKind::Right => "Right",
                    JoinKind::Full => "Full",
                    JoinKind::Cross => "Cross",
                };
                // As `join` decides: equalities between the two sides are hashed.
                let (algorithm, cond) = match constraint {
                    JoinConstraint::On(on) => {
                        let hashed = conjuncts(on).iter().any(|c| matches!(c, Expr::Binary { op: BinaryOp::Eq, left, right } if matches!(**left, Expr::Column(_)) && matches!(**right, Expr::Column(_))));
                        (if hashed { "Hash Join" } else { "Nested Loop" }, Some(on.to_string()))
                    }
                    JoinConstraint::Using(cols) => ("Hash Join", Some(format!("USING ({})", cols.join(", ")))),
                    JoinConstraint::Natural => ("Hash Join", Some("NATURAL".to_owned())),
                    JoinConstraint::None => ("Nested Loop", None),
                };
                let mut n = Node::new(algorithm).with("Join Type", type_name);
                if let Some(c) = cond {
                    n = n.with("Join Cond", c);
                }
                if algorithm == "Hash Join" && matches!(**right, TableRef::Named { .. }) && matches!(kind, JoinKind::Inner | JoinKind::Left) {
                    n = n.with("Lookup", "when the left side has at most 2000 rows, only right rows with its join keys are read");
                }
                n.rows = match (l.rows, r.rows) {
                    (Some(a), Some(b)) => Some(a.max(b)),
                    _ => None,
                };
                n.children.push(l);
                n.children.push(r);
                n
            }
        };
        n.key = key;
        Ok(n)
    }

    /// How `scan_rows` will read `table` for these conjuncts.
    fn plan_table_scan(&self, table: &crate::catalog::Table, qual: &str, alias: Option<&str>, conjs: &[&Expr], vector: Option<()>) -> SqlResult<Node> {
        let restriction = self.read_restriction(table)?;
        let used: &[&Expr] = if restriction.is_some() { &[] } else { conjs };
        let mut n = Node::new("Seq Scan");
        if vector.is_some() && restriction.is_none() {
            n = Node::new("Vector Index Scan");
        } else if self.pk_lookup(table, Some(qual), used, None)?.is_some() {
            n = Node::new("Primary Key Lookup");
        } else {
            let pushed: Vec<&Expr> = used.iter().copied().filter(|c| self.to_filter(table, Some(qual), c, None).is_some()).collect();
            if !pushed.is_empty() {
                n = n.with("Storage Filter", pushed.iter().map(ToString::to_string).collect::<Vec<_>>().join(" AND "));
                // An equality on an indexed field: the store answers from its index.
                let indexed = self.s.engine.store.indexed_fields(&table.collection).unwrap_or_default();
                let hit = pushed.iter().find_map(|c| match c {
                    Expr::Binary { op: BinaryOp::Eq, left, right } => {
                        let col = match (&**left, &**right) {
                            (Expr::Column(p), _) | (_, Expr::Column(p)) => p.last(),
                            _ => None,
                        }?;
                        let i = table.column_index(col)?;
                        indexed.contains(&table.columns[i].field).then(|| table.columns[i].name.clone())
                    }
                    _ => None,
                });
                if let Some(col) = hit {
                    n.kind = "Index Scan".into();
                    n.index = Some(col);
                }
            }
        }
        n.table = Some(table.name.clone());
        n.alias = alias.map(str::to_owned);
        if restriction.is_some() {
            n = n.with("Row Security", "applied above the scan; nothing pushed down");
        }
        let (rows, cost) = self.estimate_scan(table, used, &n);
        n.rows = rows;
        n.cost = cost;
        Ok(n)
    }
}

impl Exec<'_> {
    /// Estimated rows and cost of a scan (`None` without statistics).
    fn estimate_scan(&self, table: &crate::catalog::Table, conjs: &[&Expr], _node: &Node) -> (Option<f64>, Option<f64>) {
        let Ok(all) = self.s.engine.stats() else { return (None, None) };
        let Some(stats) = all.get(&table.name.to_ascii_lowercase()) else { return (None, None) };
        let cols = crate::exec::select::table_cols(table);
        let mut rows = stats.rows;
        for c in conjs {
            let refs = crate::exec::select::column_refs(c);
            if !refs.is_empty() && !has_subquery(c) && refs.iter().all(|p| crate::exec::select::resolves(&cols, p)) {
                rows *= crate::exec::optimizer::selectivity(self, c, stats, &cols);
            }
        }
        (Some(rows.max(1.0)), Some(stats.rows))
    }

    /// A planned join as a plan: left-deep, in the optimizer's order.
    fn plan_planned_join(&self, flat: &crate::exec::optimizer::Flat<'_>, order: &[usize], pushable: &[&Expr]) -> SqlResult<Node> {
        let mut top: Option<Node> = None;
        let mut seen: Vec<crate::exec::select::ColMeta> = Vec::new();
        for &i in order {
            let item = flat.items[i];
            let cols = self.leaf_cols(item)?;
            let scan = self.plan_from(item, pushable, false)?;
            top = Some(match top {
                None => scan,
                Some(l) => {
                    let mut both = seen.clone();
                    both.extend(cols.iter().cloned());
                    let linked: Vec<String> = flat
                        .on
                        .iter()
                        .chain(pushable)
                        .filter(|c| {
                            let refs = crate::exec::select::column_refs(c);
                            !refs.is_empty() && refs.iter().all(|p| crate::exec::select::resolves(&both, p)) && refs.iter().any(|p| crate::exec::select::resolves(&cols, p)) && refs.iter().any(|p| crate::exec::select::resolves(&seen, p))
                        })
                        .map(ToString::to_string)
                        .collect();
                    let mut n = if linked.is_empty() { Node::new("Nested Loop").with("Join Type", "Cross") } else { Node::new("Hash Join").with("Join Type", "Inner").with("Join Cond", linked.join(" AND ")) };
                    n.rows = match (l.rows, scan.rows) {
                        (Some(a), Some(b)) => Some(a.max(b)),
                        _ => None,
                    };
                    n.children.push(l);
                    n.children.push(scan);
                    n
                }
            });
            seen.extend(cols);
        }
        let mut n = top.unwrap_or_else(|| Node::new("Result"));
        n = n.with("Join Order", "chosen by the optimizer from statistics");
        Ok(n)
    }
}

fn utility_name(st: &Statement) -> &'static str {
    match st {
        Statement::CreateTable(_) => "CREATE TABLE",
        Statement::CreateIndex(_) => "CREATE INDEX",
        Statement::DropTable { .. } => "DROP TABLE",
        Statement::AlterTable { .. } => "ALTER TABLE",
        Statement::Truncate { .. } => "TRUNCATE",
        _ => "utility statement",
    }
}

fn order_keys(o: &[OrderBy]) -> String {
    o.iter().map(|k| format!("{}{}", k.expr, if k.desc { " DESC" } else { "" })).collect::<Vec<_>>().join(", ")
}

/// Text in PostgreSQL's form.
fn render_text(n: &Node, trace: Option<&HashMap<usize, Actual>>, depth: usize, out: &mut Vec<String>, analyze: bool) {
    let mut head = n.kind.clone();
    if let Some(i) = &n.index {
        head.push_str(&format!(" using {i}"));
    }
    if let Some(t) = &n.table {
        head.push_str(&format!(" on {t}"));
    }
    if let Some(a) = &n.alias {
        head.push_str(&format!(" {a}"));
    }
    if let (Some(c), Some(r)) = (n.cost, n.rows) {
        head.push_str(&format!("  (cost=0.00..{c:.2} rows={r:.0} width=0)"));
    }
    if analyze {
        match n.key.and_then(|k| trace?.get(&k)) {
            Some(a) => head.push_str(&format!(" (actual time=0.000..{:.3} rows={} loops={})", a.ns as f64 / 1e6, a.rows, a.loops)),
            None if n.key.is_some() => head.push_str(" (never executed)"),
            None => {}
        }
    }
    let pad = if depth == 0 { String::new() } else { " ".repeat(depth * 6 - 4) };
    out.push(if depth == 0 { head } else { format!("{pad}->  {head}") });
    let detail_pad = " ".repeat(if depth == 0 { 2 } else { depth * 6 + 2 });
    for (k, v) in &n.details {
        out.push(format!("{detail_pad}{k}: {v}"));
    }
    for c in &n.children {
        render_text(c, trace, depth + 1, out, analyze);
    }
}

fn json_str(s: &str) -> String {
    crate::datum::json_string(s)
}

/// JSON in PostgreSQL's form (what pgAdmin and plan visualizers read).
fn render_json(n: &Node, trace: Option<&HashMap<usize, Actual>>, analyze: bool) -> String {
    let mut f = vec![format!("\"Node Type\": {}", json_str(&n.kind))];
    if let Some(t) = &n.table {
        f.push(format!("\"Relation Name\": {}", json_str(t)));
    }
    if let Some(a) = &n.alias {
        f.push(format!("\"Alias\": {}", json_str(a)));
    }
    if let Some(i) = &n.index {
        f.push(format!("\"Index Name\": {}", json_str(i)));
    }
    f.push(format!("\"Startup Cost\": 0.00, \"Total Cost\": {:.2}, \"Plan Rows\": {:.0}, \"Plan Width\": 0", n.cost.unwrap_or(0.0), n.rows.unwrap_or(0.0)));
    if analyze {
        let a = n.key.and_then(|k| trace?.get(&k)).copied().unwrap_or_default();
        f.push(format!("\"Actual Startup Time\": 0.000, \"Actual Total Time\": {:.3}, \"Actual Rows\": {}, \"Actual Loops\": {}", a.ns as f64 / 1e6, a.rows, a.loops));
    }
    for (k, v) in &n.details {
        f.push(format!("{}: {}", json_str(k), json_str(v)));
    }
    if !n.children.is_empty() {
        let kids: Vec<String> = n.children.iter().map(|c| render_json(c, trace, analyze)).collect();
        f.push(format!("\"Plans\": [{}]", kids.join(", ")));
    }
    format!("{{{}}}", f.join(", "))
}

/// `|--` lines, as `SET SHOWPLAN_TEXT ON` prints them.
fn render_showplan(n: &Node, depth: usize, out: &mut Vec<String>) {
    let what = match n.kind.as_str() {
        "Seq Scan" => "Table Scan",
        "Index Scan" => "Index Seek",
        "Primary Key Lookup" => "Clustered Index Seek",
        "Hash Join" => "Hash Match",
        "Nested Loop" => "Nested Loops",
        other => other,
    };
    let object = n.table.as_ref().map_or(String::new(), |t| format!("(OBJECT:([{t}]))"));
    out.push(format!("{}|--{what}{object}", "   ".repeat(depth)));
    for c in &n.children {
        render_showplan(c, depth + 1, out);
    }
}

/// MySQL's traditional rows: one per table read.
fn mysql_rows(n: &Node, out: &mut Vec<Vec<Datum>>, select_type: &str) {
    let t = |s: &str| Datum::Text(s.to_owned());
    if let Some(table) = &n.table {
        let access = match n.kind.as_str() {
            "Primary Key Lookup" => "const",
            "Index Scan" => "ref",
            "Vector Index Scan" => "range",
            _ => "ALL",
        };
        let key = match n.kind.as_str() {
            "Primary Key Lookup" => Datum::Text("PRIMARY".into()),
            _ => n.index.as_deref().map_or(Datum::Null, t),
        };
        let filtered = n.details.iter().any(|(k, _)| k == "Filter" || k == "Storage Filter");
        let extra = n.details.iter().find(|(k, _)| k == "Plan").map(|(_, v)| v.as_str());
        out.push(vec![
            Datum::Int(out.len() as i64 + 1),
            t(select_type),
            n.alias.as_deref().map_or_else(|| t(table), t),
            Datum::Null,
            t(access),
            key.clone(),
            key.clone(),
            Datum::Null,
            Datum::Null,
            n.rows.map_or(Datum::Null, |r| Datum::Int(r as i64)),
            Datum::Float(if filtered { 10.0 } else { 100.0 }),
            match extra {
                Some(plan) => t(plan),
                None if filtered => t("Using where"),
                None => Datum::Null,
            },
        ]);
    }
    for c in &n.children {
        let kind = if c.kind == "Subquery Scan" { "DERIVED" } else { select_type };
        mysql_rows(c, out, kind);
    }
}

fn text_rows(column: &str, lines: Vec<String>) -> Outcome {
    Outcome::Rows(ResultSet {
        columns: vec![OutCol { name: column.into(), ty: DataType::Text, table: None }],
        rows: lines.into_iter().map(|l| vec![Datum::Text(l)]).collect(),
    })
}

impl Session {
    /// Runs `EXPLAIN`.
    pub(crate) fn explain(&mut self, inner: &Statement, opts: &ExplainOptions, params: &[Datum]) -> SqlResult<Outcome> {
        let planning = Instant::now();
        let plan = {
            let ex = Exec::new(self, params);
            ex.plan_statement(inner)?
        };
        let planning_ms = planning.elapsed().as_secs_f64() * 1e3;
        let mut trace = None;
        let mut exec_ms = 0.0;
        if opts.analyze {
            let t: Trace = Arc::default();
            self.trace = Some(Arc::clone(&t));
            let start = Instant::now();
            let r = self.run_traced(inner, params);
            exec_ms = start.elapsed().as_secs_f64() * 1e3;
            self.trace = None;
            r?;
            trace = Some(t.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone());
        }
        let footer = |out: &mut Vec<String>| {
            out.push(format!("Planning Time: {planning_ms:.3} ms"));
            if opts.analyze {
                out.push(format!("Execution Time: {exec_ms:.3} ms"));
            }
        };
        match opts.format {
            ExplainFormat::Json if self.dialect == Dialect::MySql => {
                let mut rows = Vec::new();
                mysql_rows(&plan, &mut rows, "SIMPLE");
                let tables: Vec<String> = rows
                    .iter()
                    .map(|r| format!("{{\"table\": {{\"table_name\": {}, \"access_type\": {}}}}}", json_str(&r[2].as_text()), json_str(&r[4].as_text())))
                    .collect();
                Ok(text_rows("EXPLAIN", vec![format!("{{\"query_block\": {{\"select_id\": 1, \"nested_loop\": [{}]}}}}", tables.join(", "))]))
            }
            ExplainFormat::Json => {
                let body = render_json(&plan, trace.as_ref(), opts.analyze);
                let mut extra = format!(", \"Planning Time\": {planning_ms:.3}");
                if opts.analyze {
                    extra.push_str(&format!(", \"Execution Time\": {exec_ms:.3}"));
                }
                Ok(text_rows("QUERY PLAN", vec![format!("[{{\"Plan\": {body}{extra}}}]")]))
            }
            ExplainFormat::Traditional => {
                let mut rows = Vec::new();
                mysql_rows(&plan, &mut rows, "SIMPLE");
                let names = ["id", "select_type", "table", "partitions", "type", "possible_keys", "key", "key_len", "ref", "rows", "filtered", "Extra"];
                Ok(Outcome::Rows(ResultSet {
                    columns: names
                        .iter()
                        .enumerate()
                        .map(|(i, n)| OutCol {
                            name: (*n).to_owned(),
                            ty: match i {
                                0 | 9 => DataType::BigInt,
                                10 => DataType::Double,
                                _ => DataType::Text,
                            },
                            table: None,
                        })
                        .collect(),
                    rows,
                }))
            }
            ExplainFormat::ShowPlan => {
                let mut out = Vec::new();
                render_showplan(&plan, 0, &mut out);
                Ok(text_rows("StmtText", out))
            }
            ExplainFormat::Text => {
                let mut out = Vec::new();
                render_text(&plan, trace.as_ref(), 0, &mut out, opts.analyze);
                if opts.summary {
                    footer(&mut out);
                }
                Ok(text_rows(if self.dialect == Dialect::MySql { "EXPLAIN" } else { "QUERY PLAN" }, out))
            }
        }
    }
}

/// What `EXPLAIN ANALYZE` records for `key`.
pub(crate) fn note(trace: &Trace, key: usize, rows: u64, started: Instant) {
    let ns = started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
    let mut t = trace.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let e = t.entry(key).or_default();
    e.loops += 1;
    e.rows += rows;
    e.ns += ns;
}
