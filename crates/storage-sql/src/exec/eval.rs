//! Expression evaluation (three-valued logic, subqueries, parameters).

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::rc::Rc;

use crate::ast::{BinaryOp, DataType, Expr, Query, UnaryOp};
use crate::datum::{self, arith, cast, sql_cmp, Datum};
use crate::error::{SqlError, SqlResult};
use crate::exec::select::{ColMeta, Rel};
use crate::exec::{func, now_ns, OutCol, ResultSet, Session};
use crate::lexer::Dialect;

/// Read-side execution context of one statement.
/// A parsed view: its query and declared column names.
pub(crate) type ViewDef = (crate::ast::Query, Vec<String>);

#[derive(Debug)]
pub struct Exec<'a> {
    pub s: &'a Session,
    pub params: &'a [Datum],
    /// Statement start time (`now()` is stable within a statement).
    pub now: i64,
    /// Common table expressions in scope (innermost last).
    pub ctes: RefCell<Vec<HashMap<String, Rc<Rel>>>>,
    /// Results of uncorrelated subqueries, by node address.
    subquery_cache: RefCell<HashMap<usize, Rc<Rel>>>,
    /// Subqueries known to be correlated (re-run per row).
    correlated: RefCell<std::collections::HashSet<usize>>,
    /// Parsed view definitions used by this statement (kept alive: the subquery cache is keyed
    /// by node address) and the current view nesting depth.
    pub(crate) views: RefCell<HashMap<String, Rc<ViewDef>>>,
    pub(crate) view_depth: std::cell::Cell<u32>,
}

/// The row an expression is evaluated against, with its enclosing rows (correlated subqueries).
#[derive(Debug, Clone, Copy)]
pub struct Env<'e> {
    pub cols: &'e [ColMeta],
    pub row: &'e [Datum],
    pub outer: Option<&'e Env<'e>>,
    /// Aggregate results of the current group.
    pub aggs: Option<&'e [(Expr, Datum)]>,
}

impl<'e> Env<'e> {
    pub const EMPTY: Env<'static> = Env {
        cols: &[],
        row: &[],
        outer: None,
        aggs: None,
    };

    pub fn new(cols: &'e [ColMeta], row: &'e [Datum]) -> Env<'e> {
        Env {
            cols,
            row,
            outer: None,
            aggs: None,
        }
    }
}

const AGGREGATES: &[&str] = &[
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "string_agg",
    "group_concat",
    "array_agg",
    "bool_and",
    "bool_or",
    "every",
    "json_agg",
    "jsonb_agg",
    "bit_and",
    "bit_or",
    "stddev",
    "stddev_samp",
    "stddev_pop",
    "variance",
    "var_samp",
    "var_pop",
    "listagg",
];

pub fn is_aggregate(name: &str) -> bool {
    AGGREGATES.contains(&name)
}

/// True if `e` contains an aggregate call outside a subquery.
pub fn has_aggregate(e: &Expr) -> bool {
    let mut found = false;
    walk(e, &mut |x| {
        if let Expr::Function {
            name, over: None, ..
        } = x
        {
            if is_aggregate(name) {
                found = true;
            }
        }
    });
    found
}

/// Visits `e` and its sub-expressions (not entering subqueries).
pub fn walk(e: &Expr, f: &mut dyn FnMut(&Expr)) {
    f(e);
    match e {
        Expr::Unary { expr, .. }
        | Expr::Cast { expr, .. }
        | Expr::IsNull { expr, .. }
        | Expr::IsBool { expr, .. }
        | Expr::Extract { expr, .. } => walk(expr, f),
        Expr::Binary { left, right, .. }
        | Expr::IsDistinctFrom { left, right, .. }
        | Expr::AnyAll { left, right, .. } => {
            walk(left, f);
            walk(right, f);
        }
        Expr::Function { args, .. } | Expr::Tuple(args) | Expr::Array(args) => {
            args.iter().for_each(|a| walk(a, f))
        }
        Expr::Case {
            operand,
            whens,
            else_,
        } => {
            if let Some(o) = operand {
                walk(o, f);
            }
            for (w, t) in whens {
                walk(w, f);
                walk(t, f);
            }
            if let Some(x) = else_ {
                walk(x, f);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            walk(expr, f);
            walk(low, f);
            walk(high, f);
        }
        Expr::InList { expr, list, .. } => {
            walk(expr, f);
            list.iter().for_each(|a| walk(a, f));
        }
        Expr::InSubquery { expr, .. } => walk(expr, f),
        Expr::Like {
            expr,
            pattern,
            escape,
            ..
        } => {
            walk(expr, f);
            walk(pattern, f);
            if let Some(x) = escape {
                walk(x, f);
            }
        }
        Expr::Index { expr, index } => {
            walk(expr, f);
            walk(index, f);
        }
        _ => {}
    }
}

impl<'a> Exec<'a> {
    pub fn new(s: &'a Session, params: &'a [Datum]) -> Exec<'a> {
        Exec {
            s,
            params,
            now: now_ns(),
            ctes: RefCell::new(Vec::new()),
            subquery_cache: RefCell::new(HashMap::new()),
            correlated: RefCell::new(Default::default()),
            views: RefCell::new(HashMap::new()),
            view_depth: std::cell::Cell::new(0),
        }
    }

    pub fn dialect(&self) -> Dialect {
        self.s.dialect
    }

    pub fn eval_const(&self, e: &Expr) -> SqlResult<Datum> {
        self.eval(e, &Env::EMPTY)
    }

    pub fn eval_bool(&self, e: &Expr, env: &Env<'_>) -> SqlResult<bool> {
        Ok(self.eval(e, env)?.truthy()? == Some(true))
    }

    /// A numeric literal: integers are bigint, decimals exact numeric (MySQL exponents: double).
    pub fn number(&self, n: &str) -> SqlResult<Datum> {
        if let Ok(i) = n.parse::<i64>() {
            return Ok(Datum::Int(i));
        }
        let has_exp = n.contains(['e', 'E']);
        if has_exp && self.dialect() == Dialect::MySql {
            return n
                .parse::<f64>()
                .map(Datum::Float)
                .map_err(|_| SqlError::syntax(format!("bad number {n}")));
        }
        match datum::parse_dec(n) {
            Ok(d) => Ok(Datum::Numeric(d)),
            Err(()) => n
                .parse::<f64>()
                .map(Datum::Float)
                .map_err(|_| SqlError::syntax(format!("bad number {n}"))),
        }
    }

    pub fn param(&self, i: usize) -> SqlResult<Datum> {
        self.params
            .get(i)
            .cloned()
            .ok_or_else(|| SqlError::new("08P01", format!("there is no parameter ${}", i + 1)))
    }

    /// Resolves a column reference in `env` (then enclosing rows).
    pub fn column(&self, parts: &[String], env: &Env<'_>) -> SqlResult<Datum> {
        let (qual, name) = match parts {
            [n] => (None, n.as_str()),
            [.., q, n] => (Some(q.as_str()), n.as_str()),
            [] => return Err(SqlError::internal("empty column reference")),
        };
        match find_column(env.cols, qual, name)? {
            Some(i) => Ok(env.row.get(i).cloned().unwrap_or(Datum::Null)),
            None => match env.outer {
                Some(o) => self.column(parts, o),
                None => {
                    // MySQL / SQL Server accept "string" in double quotes / [x] style in places;
                    // a bare unknown name is an error everywhere.
                    Err(SqlError::undefined_column(&parts.join(".")))
                }
            },
        }
    }

    pub fn eval(&self, e: &Expr, env: &Env<'_>) -> SqlResult<Datum> {
        Ok(match e {
            Expr::Null => Datum::Null,
            Expr::Bool(b) => Datum::Bool(*b),
            Expr::Number(n) => self.number(n)?,
            Expr::Str(s) => Datum::Text(s.clone()),
            Expr::Blob(b) => Datum::Bytes(b.clone()),
            Expr::Param(i) => self.param(*i)?,
            Expr::Default => {
                return Err(SqlError::syntax(
                    "DEFAULT is not allowed in this context".into(),
                ))
            }
            Expr::Column(parts) => self.column(parts, env)?,
            Expr::Variable { name, system } => self.variable(name, *system),
            Expr::Unary { op, expr } => {
                let v = self.eval(expr, env)?;
                match op {
                    UnaryOp::Neg => {
                        match v {
                            Datum::Null => Datum::Null,
                            Datum::Int(i) => Datum::Int(i.checked_neg().ok_or_else(|| {
                                SqlError::numeric_out_of_range("bigint out of range")
                            })?),
                            Datum::Float(f) => Datum::Float(-f),
                            Datum::Numeric(d) => Datum::Numeric(d.checked_neg().map_err(|_| {
                                SqlError::numeric_out_of_range("numeric out of range")
                            })?),
                            Datum::Interval { months, days, ns } => Datum::Interval {
                                months: -months,
                                days: -days,
                                ns: -ns,
                            },
                            other => arith('-', &Datum::Int(0), &other)?,
                        }
                    }
                    UnaryOp::Not => match v.truthy()? {
                        None => Datum::Null,
                        Some(b) => Datum::Bool(!b),
                    },
                    UnaryOp::BitNot => match v {
                        Datum::Null => Datum::Null,
                        other => Datum::Int(!other.as_i64()?),
                    },
                }
            }
            Expr::Binary { op, left, right } => self.binary(*op, left, right, env)?,
            Expr::Function {
                over: Some(_),
                name,
                ..
            } => {
                // Window values are computed before projection.
                if let Some(v) = env.aggs.and_then(|a| a.iter().find(|(x, _)| x == e)) {
                    return Ok(v.1.clone());
                }
                return Err(SqlError::new(
                    "42P20",
                    format!("window function {name}() is not allowed here"),
                ));
            }
            Expr::Function {
                name,
                args,
                star,
                distinct,
                ..
            } => {
                if is_aggregate(name) && !(name == "min" || name == "max")
                    || (matches!(name.as_str(), "min" | "max") && args.len() == 1)
                {
                    if let Some(aggs) = env.aggs {
                        if let Some((_, v)) = aggs.iter().find(|(x, _)| x == e) {
                            return Ok(v.clone());
                        }
                    }
                    let mut o = env.outer;
                    while let Some(oe) = o {
                        if let Some(aggs) = oe.aggs {
                            if let Some((_, v)) = aggs.iter().find(|(x, _)| x == e) {
                                return Ok(v.clone());
                            }
                        }
                        o = oe.outer;
                    }
                    return Err(SqlError::new(
                        "42803",
                        format!("aggregate function {name}() is not allowed here"),
                    ));
                }
                func::call(self, name, args, *star, *distinct, env)?
            }
            Expr::Case {
                operand,
                whens,
                else_,
            } => {
                let base = match operand {
                    Some(o) => Some(self.eval(o, env)?),
                    None => None,
                };
                for (w, t) in whens {
                    let hit = match &base {
                        Some(b) => sql_cmp(b, &self.eval(w, env)?) == Some(Ordering::Equal),
                        None => self.eval_bool(w, env)?,
                    };
                    if hit {
                        return self.eval(t, env);
                    }
                }
                match else_ {
                    Some(x) => self.eval(x, env)?,
                    None => Datum::Null,
                }
            }
            Expr::Cast { expr, ty } => {
                let v = self.eval(expr, env)?;
                if matches!(ty, DataType::Json | DataType::Jsonb) {
                    return Ok(match v {
                        Datum::Text(s) => Datum::Json(s),
                        other => cast(&other, ty)?,
                    });
                }
                cast(&v, ty)?
            }
            Expr::IsNull { expr, negated } => {
                Datum::Bool(self.eval(expr, env)?.is_null() != *negated)
            }
            Expr::IsBool {
                expr,
                value,
                negated,
            } => {
                let v = self.eval(expr, env)?.truthy()?;
                Datum::Bool((v == Some(*value)) != *negated)
            }
            Expr::IsDistinctFrom {
                left,
                right,
                negated,
            } => {
                let (a, b) = (self.eval(left, env)?, self.eval(right, env)?);
                let same = match (a.is_null(), b.is_null()) {
                    (true, true) => true,
                    (false, false) => sql_cmp(&a, &b) == Some(Ordering::Equal),
                    _ => false,
                };
                Datum::Bool(same == *negated)
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let v = self.eval(expr, env)?;
                let (lo, hi) = (self.eval(low, env)?, self.eval(high, env)?);
                let ge = sql_cmp(&v, &lo).map(|o| o != Ordering::Less);
                let le = sql_cmp(&v, &hi).map(|o| o != Ordering::Greater);
                let r = and3(ge, le);
                bool3(r.map(|b| b != *negated))
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let v = self.eval(expr, env)?;
                let mut saw_null = v.is_null();
                let mut hit = false;
                if !v.is_null() {
                    for item in list {
                        let x = self.eval(item, env)?;
                        match self.equals(&v, &x) {
                            Some(true) => {
                                hit = true;
                                break;
                            }
                            None => saw_null = true,
                            Some(false) => {}
                        }
                    }
                }
                in_result(hit, saw_null, *negated)
            }
            Expr::InSubquery {
                expr,
                query,
                negated,
            } => {
                let v = self.eval(expr, env)?;
                let rel = self.subquery(query, env)?;
                let mut saw_null = v.is_null();
                let mut hit = false;
                if !v.is_null() {
                    for r in &rel.rows {
                        let x = if r.len() == 1 {
                            r[0].clone()
                        } else {
                            Datum::Array(r.clone())
                        };
                        match self.equals(&v, &x) {
                            Some(true) => {
                                hit = true;
                                break;
                            }
                            None => saw_null = true,
                            Some(false) => {}
                        }
                    }
                }
                in_result(hit, saw_null, *negated)
            }
            Expr::AnyAll {
                left,
                op,
                right,
                all,
            } => {
                let v = self.eval(left, env)?;
                let items: Vec<Datum> = match &**right {
                    Expr::Subquery(q) => self
                        .subquery(q, env)?
                        .rows
                        .iter()
                        .map(|r| r.first().cloned().unwrap_or(Datum::Null))
                        .collect(),
                    other => match self.eval(other, env)? {
                        Datum::Array(a) => a,
                        Datum::Null => return Ok(Datum::Null),
                        Datum::Text(s) => datum::parse_array(&s)?,
                        x => vec![x],
                    },
                };
                let mut saw_null = false;
                let mut result = *all;
                for x in &items {
                    match self.compare_op(*op, &v, x)? {
                        None => saw_null = true,
                        Some(b) => {
                            if *all && !b {
                                result = false;
                                saw_null = false;
                                break;
                            }
                            if !*all && b {
                                result = true;
                                saw_null = false;
                                break;
                            }
                        }
                    }
                }
                if saw_null {
                    Datum::Null
                } else {
                    Datum::Bool(result)
                }
            }
            Expr::Exists { query, negated } => {
                let rel = self.subquery(query, env)?;
                Datum::Bool(rel.rows.is_empty() == *negated)
            }
            Expr::Subquery(q) => {
                let rel = self.subquery(q, env)?;
                match rel.rows.len() {
                    0 => Datum::Null,
                    1 => rel.rows[0].first().cloned().unwrap_or(Datum::Null),
                    _ => {
                        return Err(SqlError::new(
                            "21000",
                            "more than one row returned by a subquery used as an expression",
                        ))
                    }
                }
            }
            Expr::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
                escape,
            } => {
                let v = self.eval(expr, env)?;
                let p = self.eval(pattern, env)?;
                if v.is_null() || p.is_null() {
                    return Ok(Datum::Null);
                }
                let esc = match escape {
                    Some(x) => self.eval(x, env)?.as_text().chars().next(),
                    None => Some('\\'),
                };
                let (s, p) = if *case_insensitive {
                    (v.as_text().to_lowercase(), p.as_text().to_lowercase())
                } else {
                    (v.as_text(), p.as_text())
                };
                Datum::Bool(like(&s, &p, esc) != *negated)
            }
            Expr::Tuple(items) => Datum::Array(
                items
                    .iter()
                    .map(|x| self.eval(x, env))
                    .collect::<SqlResult<_>>()?,
            ),
            Expr::Array(items) => Datum::Array(
                items
                    .iter()
                    .map(|x| self.eval(x, env))
                    .collect::<SqlResult<_>>()?,
            ),
            Expr::Index { expr, index } => {
                let v = self.eval(expr, env)?;
                let i = self.eval(index, env)?;
                match (v, i) {
                    (Datum::Array(a), Datum::Int(n)) => usize::try_from(n - 1)
                        .ok()
                        .and_then(|k| a.get(k).cloned())
                        .unwrap_or(Datum::Null),
                    (Datum::Json(j), i) => func::json_get(&j, &i, false)?,
                    _ => Datum::Null,
                }
            }
            Expr::Interval(s) => datum::parse_interval(s)
                .ok_or_else(|| SqlError::invalid_text(format!("invalid interval \"{s}\"")))?,
            Expr::Extract { field, expr } => func::extract(field, &self.eval(expr, env)?)?,
        })
    }

    /// SQL equality (`None`: unknown because of NULL). Rows compare element by element.
    pub fn equals(&self, a: &Datum, b: &Datum) -> Option<bool> {
        match (a, b) {
            (Datum::Array(x), Datum::Array(y)) if x.len() == y.len() => {
                let mut unknown = false;
                for (p, q) in x.iter().zip(y) {
                    match self.equals(p, q) {
                        Some(false) => return Some(false),
                        None => unknown = true,
                        Some(true) => {}
                    }
                }
                if unknown {
                    None
                } else {
                    Some(true)
                }
            }
            _ => sql_cmp(a, b).map(|o| o == Ordering::Equal),
        }
    }

    fn compare_op(&self, op: BinaryOp, a: &Datum, b: &Datum) -> SqlResult<Option<bool>> {
        if matches!(op, BinaryOp::Eq) {
            return Ok(self.equals(a, b));
        }
        if matches!(op, BinaryOp::NotEq) {
            return Ok(self.equals(a, b).map(|x| !x));
        }
        let o = match sql_cmp(a, b) {
            Some(o) => o,
            None if a.is_null() || b.is_null() => return Ok(None),
            None => {
                return Err(SqlError::new(
                    "42883",
                    format!(
                        "operator does not exist: {} {} {}",
                        a.type_name(),
                        op_symbol(op),
                        b.type_name()
                    ),
                ))
            }
        };
        Ok(Some(match op {
            BinaryOp::Lt => o == Ordering::Less,
            BinaryOp::LtEq => o != Ordering::Greater,
            BinaryOp::Gt => o == Ordering::Greater,
            BinaryOp::GtEq => o != Ordering::Less,
            _ => return Err(SqlError::internal("not a comparison")),
        }))
    }

    fn binary(&self, op: BinaryOp, left: &Expr, right: &Expr, env: &Env<'_>) -> SqlResult<Datum> {
        use BinaryOp::*;
        match op {
            And => {
                let a = self.eval(left, env)?.truthy()?;
                if a == Some(false) {
                    return Ok(Datum::Bool(false));
                }
                let b = self.eval(right, env)?.truthy()?;
                return Ok(bool3(and3(a, b)));
            }
            Or => {
                let a = self.eval(left, env)?.truthy()?;
                if a == Some(true) {
                    return Ok(Datum::Bool(true));
                }
                let b = self.eval(right, env)?.truthy()?;
                return Ok(bool3(match (a, b) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                }));
            }
            _ => {}
        }
        let a = self.eval(left, env)?;
        let b = self.eval(right, env)?;
        Ok(match op {
            Eq | NotEq | Lt | LtEq | Gt | GtEq => bool3(self.compare_op(op, &a, &b)?),
            NullSafeEq => Datum::Bool(match (a.is_null(), b.is_null()) {
                (true, true) => true,
                (false, false) => self.equals(&a, &b) == Some(true),
                _ => false,
            }),
            Xor => match (a.truthy()?, b.truthy()?) {
                (Some(x), Some(y)) => Datum::Bool(x != y),
                _ => Datum::Null,
            },
            // T-SQL: `+` on two strings concatenates.
            Add if self.dialect() == Dialect::MsSql
                && matches!((&a, &b), (Datum::Text(_), Datum::Text(_))) =>
            {
                match (a, b) {
                    (Datum::Text(mut x), Datum::Text(y)) => {
                        x.push_str(&y);
                        Datum::Text(x)
                    }
                    _ => Datum::Null,
                }
            }
            Add => arith('+', &a, &b)?,
            Sub => arith('-', &a, &b)?,
            Mul => arith('*', &a, &b)?,
            Div => {
                // MySQL `/` on integers gives a decimal (7 / 2 = 3.5000).
                if self.dialect() == Dialect::MySql
                    && matches!((&a, &b), (Datum::Int(_), Datum::Int(_)))
                {
                    let x = datum::to_decimal(&a).unwrap_or_default();
                    let y = datum::to_decimal(&b).unwrap_or_default();
                    if y.is_zero() {
                        return Ok(Datum::Null);
                    }
                    Datum::Numeric(
                        x.checked_div(y, 4, storage_types::RoundingMode::HalfUp)
                            .map_err(|_| SqlError::numeric_out_of_range("out of range"))?,
                    )
                } else {
                    arith('/', &a, &b)?
                }
            }
            IntDiv => match arith('/', &a, &b)? {
                Datum::Null => Datum::Null,
                v => Datum::Int(match v {
                    Datum::Float(f) => f.trunc() as i64,
                    Datum::Numeric(d) => {
                        Datum::Numeric(d.rescale(0, storage_types::RoundingMode::Down).unwrap_or(d))
                            .as_i64()?
                    }
                    other => other.as_i64()?,
                }),
            },
            Mod => arith('%', &a, &b)?,
            Pow => match (datum::to_f64(&a), datum::to_f64(&b)) {
                _ if a.is_null() || b.is_null() => Datum::Null,
                (Some(x), Some(y)) => Datum::Float(x.powf(y)),
                _ => return Err(SqlError::datatype_mismatch("operator ^ needs numbers")),
            },
            Concat => {
                if a.is_null() || b.is_null() {
                    Datum::Null
                } else {
                    match (a, b) {
                        (Datum::Array(mut x), Datum::Array(y)) => {
                            x.extend(y);
                            Datum::Array(x)
                        }
                        (Datum::Array(mut x), y) => {
                            x.push(y);
                            Datum::Array(x)
                        }
                        (Datum::Json(x), Datum::Json(y)) => Datum::Json(func::json_concat(&x, &y)?),
                        (a, b) => Datum::Text(a.as_text() + &b.as_text()),
                    }
                }
            }
            BitAnd | BitOr | BitXor | ShiftLeft | ShiftRight => {
                if a.is_null() || b.is_null() {
                    Datum::Null
                } else {
                    let (x, y) = (a.as_i64()?, b.as_i64()?);
                    Datum::Int(match op {
                        BitAnd => x & y,
                        BitOr => x | y,
                        BitXor => x ^ y,
                        ShiftLeft => x.checked_shl(y as u32).unwrap_or(0),
                        _ => x.checked_shr(y as u32).unwrap_or(0),
                    })
                }
            }
            JsonGet | JsonGetText => match a {
                Datum::Null => Datum::Null,
                Datum::Json(j) | Datum::Text(j) => func::json_get(&j, &b, op == JsonGetText)?,
                _ => Datum::Null,
            },
            Regex {
                case_insensitive,
                negated,
            } => {
                if a.is_null() || b.is_null() {
                    Datum::Null
                } else {
                    let re = crate::exec::regex::Regex::new(&b.as_text(), case_insensitive)?;
                    Datum::Bool(re.is_match(&a.as_text()) != negated)
                }
            }
            Contains | ContainedBy => {
                let (big, small) = if op == Contains { (a, b) } else { (b, a) };
                match (big, small) {
                    (Datum::Null, _) | (_, Datum::Null) => Datum::Null,
                    (Datum::Array(x), Datum::Array(y)) => Datum::Bool(
                        y.iter()
                            .all(|i| x.iter().any(|j| self.equals(i, j) == Some(true))),
                    ),
                    (Datum::Json(x) | Datum::Text(x), Datum::Json(y) | Datum::Text(y)) => {
                        Datum::Bool(func::json_contains(&x, &y)?)
                    }
                    _ => return Err(SqlError::datatype_mismatch("@> needs arrays or JSON")),
                }
            }
            Overlap => match (a, b) {
                (Datum::Array(x), Datum::Array(y)) => Datum::Bool(
                    x.iter()
                        .any(|i| y.iter().any(|j| self.equals(i, j) == Some(true))),
                ),
                (Datum::Null, _) | (_, Datum::Null) => Datum::Null,
                _ => return Err(SqlError::datatype_mismatch("&& needs arrays")),
            },
            And | Or => unreachable!(),
        })
    }

    fn variable(&self, name: &str, system: bool) -> Datum {
        let lower = name.to_ascii_lowercase();
        if !system {
            return self.s.user_vars.get(&lower).cloned().unwrap_or(Datum::Null);
        }
        let n = lower
            .trim_start_matches("session.")
            .trim_start_matches("global.")
            .trim_start_matches("local.");
        match n {
            "version" if self.dialect() == Dialect::MsSql => {
                Datum::Text(crate::MSSQL_VERSION_STRING.into())
            }
            "version" => Datum::Text(crate::MYSQL_VERSION.into()),
            "version_comment" => Datum::Text("Storine".into()),
            "identity" | "last_insert_id" => Datum::Int(self.s.last_insert_id),
            "autocommit" => Datum::Int(i64::from(!self.s.in_transaction())),
            "tx_isolation" | "transaction_isolation" => Datum::Text("REPEATABLE-READ".into()),
            "tx_read_only" | "transaction_read_only" => Datum::Int(0),
            "max_allowed_packet" => Datum::Int(67_108_864),
            "lower_case_table_names" | "sql_auto_is_null" | "sql_safe_updates" => Datum::Int(0),
            "auto_increment_increment" | "auto_increment_offset" => Datum::Int(1),
            "net_write_timeout" | "net_read_timeout" => Datum::Int(60),
            "wait_timeout" | "interactive_timeout" => Datum::Int(28_800),
            "spid" => Datum::Int(self.s.id as i64),
            "servername" | "hostname" => Datum::Text("storine".into()),
            "trancount" => Datum::Int(i64::from(self.s.in_transaction())),
            "rowcount" if self.s.last_row_count >= 0 => Datum::Int(self.s.last_row_count),
            "rowcount" => Datum::Int(self.s.last_found_rows as i64),
            other => match self.s.vars.get(other) {
                Some(v) => match v.parse::<i64>() {
                    Ok(i) => Datum::Int(i),
                    Err(_) => Datum::Text(v.clone()),
                },
                None => Datum::Null,
            },
        }
    }

    /// Runs a subquery with `env` as its enclosing row. Uncorrelated subqueries run once.
    pub fn subquery(&self, q: &Query, env: &Env<'_>) -> SqlResult<Rc<Rel>> {
        let key = q as *const Query as usize;
        if let Some(r) = self.subquery_cache.borrow().get(&key) {
            return Ok(Rc::clone(r));
        }
        if !self.correlated.borrow().contains(&key) {
            match self.query(q, None) {
                Ok(rel) => {
                    let rel = Rc::new(rel);
                    self.subquery_cache
                        .borrow_mut()
                        .insert(key, Rc::clone(&rel));
                    return Ok(rel);
                }
                Err(e) if e.code == "42703" || e.code == "42P01" => {
                    self.correlated.borrow_mut().insert(key);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(Rc::new(self.query(q, Some(env))?))
    }

    /// Final result columns and rows (types inferred from metadata, then from values).
    pub fn result_set(&self, rel: Rel) -> SqlResult<ResultSet> {
        let columns = rel
            .cols
            .iter()
            .enumerate()
            .map(|(i, c)| OutCol {
                name: c.name.clone(),
                ty: c.ty.clone().unwrap_or_else(|| {
                    rel.rows
                        .iter()
                        .map(|r| &r[i])
                        .find(|d| !d.is_null())
                        .map_or(DataType::Text, Datum::natural_type)
                }),
                table: c.table.clone(),
            })
            .collect();
        Ok(ResultSet {
            columns,
            rows: rel.rows,
        })
    }
}

fn op_symbol(op: BinaryOp) -> &'static str {
    match op {
        BinaryOp::Lt => "<",
        BinaryOp::LtEq => "<=",
        BinaryOp::Gt => ">",
        BinaryOp::GtEq => ">=",
        BinaryOp::Eq => "=",
        _ => "<>",
    }
}

/// Finds a column; `Err` when the name is ambiguous.
pub fn find_column(cols: &[ColMeta], qual: Option<&str>, name: &str) -> SqlResult<Option<usize>> {
    let matches_q = |c: &ColMeta| match qual {
        None => true,
        Some(q) => c
            .qual
            .as_deref()
            .is_some_and(|cq| cq == q || cq.eq_ignore_ascii_case(q)),
    };
    let mut found: Option<usize> = None;
    for (i, c) in cols.iter().enumerate() {
        if c.hidden && qual.is_none() {
            continue;
        }
        if c.name == name && matches_q(c) {
            if found.is_some() {
                return Err(SqlError::ambiguous_column(name));
            }
            found = Some(i);
        }
    }
    if found.is_none() {
        for (i, c) in cols.iter().enumerate() {
            if c.hidden && qual.is_none() {
                continue;
            }
            if c.name.eq_ignore_ascii_case(name) && matches_q(c) {
                if found.is_some() {
                    return Err(SqlError::ambiguous_column(name));
                }
                found = Some(i);
            }
        }
    }
    if found.is_none() && qual.is_none() {
        // Hidden duplicates (the right side of USING joins) still resolve when qualified only.
        return Ok(None);
    }
    Ok(found)
}

fn and3(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

fn bool3(v: Option<bool>) -> Datum {
    v.map_or(Datum::Null, Datum::Bool)
}

fn in_result(hit: bool, saw_null: bool, negated: bool) -> Datum {
    if hit {
        Datum::Bool(!negated)
    } else if saw_null {
        Datum::Null
    } else {
        Datum::Bool(negated)
    }
}

/// SQL LIKE: `%` any run, `_` one character, `esc` escapes the next character.
pub fn like(s: &str, p: &str, esc: Option<char>) -> bool {
    let s: Vec<char> = s.chars().collect();
    let p: Vec<char> = p.chars().collect();
    // Iterative matcher with backtracking over the last `%`.
    let (mut si, mut pi) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    loop {
        if pi < p.len() {
            let c = p[pi];
            if Some(c) == esc && pi + 1 < p.len() {
                if si < s.len() && s[si] == p[pi + 1] {
                    si += 1;
                    pi += 2;
                    continue;
                }
            } else if c == '%' {
                star = Some((pi, si));
                pi += 1;
                continue;
            } else if si < s.len() && (c == '_' || c == s[si]) {
                si += 1;
                pi += 1;
                continue;
            }
        } else if si == s.len() {
            return true;
        }
        match star {
            Some((sp, ss)) if ss < s.len() => {
                star = Some((sp, ss + 1));
                pi = sp + 1;
                si = ss + 1;
            }
            _ => return false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::like;

    #[test]
    fn like_patterns() {
        assert!(like("hello", "h%o", Some('\\')));
        assert!(like("hello", "h_llo", Some('\\')));
        assert!(!like("hello", "h_lo", Some('\\')));
        assert!(like("50%", "50\\%", Some('\\')));
        assert!(!like("500", "50\\%", Some('\\')));
        assert!(like("", "%", Some('\\')));
        assert!(like("abcabc", "%bc", Some('\\')));
        assert!(like("a%b", "a%%b", Some('\\')));
        assert!(!like("abc", "", Some('\\')));
    }
}
