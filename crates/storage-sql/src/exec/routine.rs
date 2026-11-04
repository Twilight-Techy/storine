//! User functions, procedures and triggers (`F-ENT-05`, `TASK-149`): `LANGUAGE sql` and a PL/pgSQL
//! subset (`parser/plpgsql.rs`), PostgreSQL dialect.
//!
//! **Where a body may write.** Expressions are evaluated over a shared session, so a function called
//! from inside a larger query can only read; if its body writes, the statement fails with a precise
//! error. `SELECT f(...)` on its own, `CALL`, `PERFORM` in another routine, and triggers run with
//! the session itself and may write, in the caller's transaction.
//!
//! **Variables** are looked up when a name resolves to no column, so a column hides a variable of
//! the same name (PostgreSQL reports such a name as ambiguous).
//!
//! **Rights.** Routines run with the caller's rights (`SECURITY DEFINER` is refused). `EXECUTE` is
//! open to everyone until revoked from `public`. A trigger's writes run with the rights of whoever
//! fired it. Foreign-key cascades do not fire triggers.

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Mutex;

use storine::{Doc, Value};

use crate::ast::{CreateFunction, CreateTrigger, DataType, Expr, FuncReturn, Query, SetExpr, Statement};
use crate::catalog::{Row, Table, Trigger};
use crate::datum::{cast, Datum};
use crate::error::{SqlError, SqlResult};
use crate::exec::eval::Exec;
use crate::exec::select::{ColMeta, Rel};
use crate::exec::{OutCol, Outcome, ResultSet, Session};
use crate::lexer::Dialect;
use crate::parser::plpgsql::{parse_body, PlBlock, PlStmt};
use crate::store::{Op, Pre, Tag, Write};

pub const FUNCTIONS: &str = "_sql_functions";
/// Routine calls nested deeper than this are stopped (a runaway recursion or trigger cascade).
const MAX_DEPTH: usize = 32;
/// Marks the error raised when a body writes over a shared session.
const NEEDS_SESSION: &str = "a function called inside a query cannot write";

/// A stored routine.
#[derive(Debug)]
pub struct Routine {
    pub name: String,
    pub args: Vec<(String, DataType, Option<Expr>)>,
    pub returns: FuncReturn,
    pub owner: Option<String>,
    parsed: Parsed,
}

#[derive(Debug)]
enum Parsed {
    Sql(Vec<Statement>),
    Pl(PlBlock),
}

/// What runs a routine's statements.
trait Host {
    fn session(&self) -> &Session;
    fn run(&mut self, st: &Statement) -> SqlResult<Outcome>;
    /// Undoes what a failed block did (only a session that can write has anything to undo).
    fn mark(&self) -> Option<usize>;
    fn rollback_to(&mut self, mark: Option<usize>);
}

struct Shared<'a>(&'a Session);

impl Host for Shared<'_> {
    fn session(&self) -> &Session {
        self.0
    }

    fn run(&mut self, st: &Statement) -> SqlResult<Outcome> {
        let Statement::Query(q) = st else {
            return Err(SqlError::new("0A000", format!("{NEEDS_SESSION}: call it on its own (SELECT f(...), CALL)")));
        };
        let params = self.0.frame_params();
        let ex = Exec::new(self.0, &params);
        let rel = ex.query(q, None)?;
        Ok(Outcome::Rows(ex.result_set(rel)?))
    }

    fn mark(&self) -> Option<usize> {
        None
    }

    fn rollback_to(&mut self, _mark: Option<usize>) {}
}

struct Owned<'a>(&'a mut Session);

impl Host for Owned<'_> {
    fn session(&self) -> &Session {
        self.0
    }

    fn run(&mut self, st: &Statement) -> SqlResult<Outcome> {
        // In the caller's transaction: `execute` would start one of its own and lose this one.
        let params = self.0.frame_params();
        self.0.run_nested(st, &params)
    }

    fn mark(&self) -> Option<usize> {
        self.0.txn.as_ref().map(crate::exec::Txn::mark)
    }

    fn rollback_to(&mut self, mark: Option<usize>) {
        if let (Some(m), Some(t)) = (mark, self.0.txn.as_mut()) {
            t.rollback_to(m);
            t.failed = false;
        }
    }
}

enum Flow {
    Next,
    Exit,
    Continue,
    Return(Option<Datum>),
}

/// Variables of the routine calls in progress on a session: innermost last.
pub type Frames = Mutex<Vec<HashMap<String, Datum>>>;

impl Session {
    fn with_frame<R>(&self, f: impl FnOnce(&mut HashMap<String, Datum>) -> R) -> Option<R> {
        let mut frames = self.frames.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        frames.last_mut().map(f)
    }

    /// The value of a variable of the running routine.
    pub(crate) fn variable(&self, parts: &[String]) -> Option<Datum> {
        let frames = self.frames.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let top = frames.last()?;
        top.get(&parts.join(".").to_ascii_lowercase()).cloned()
    }

    fn set_variable(&self, name: &str, v: Datum) {
        self.with_frame(|f| f.insert(name.to_owned(), v));
    }

    /// `CREATE FUNCTION` / `CREATE PROCEDURE`.
    pub(crate) fn create_function(&mut self, f: &CreateFunction) -> SqlResult<Outcome> {
        self.require_database("CREATE", "create functions")?;
        let exists = self.engine.store.get(FUNCTIONS, &f.name)?;
        if let Some(d) = &exists {
            if !f.or_replace {
                return Err(SqlError::new("42723", format!("function \"{}\" already exists", f.name)));
            }
            let acl = self.engine.acl()?;
            let owner = d.fields.get("owner").and_then(Value::as_str);
            if !acl.is_superuser(self.current_role()) && !acl.owns(self.current_role(), owner) {
                return Err(SqlError::new("42501", format!("must be owner of function {}", f.name)));
            }
        }
        if f.is_procedure && !matches!(f.returns, FuncReturn::Void) {
            return Err(SqlError::new("42P13", "a procedure returns nothing"));
        }
        let mut d = Doc::new();
        d.insert("name".into(), Value::Text(f.name.clone()));
        d.insert("owner".into(), Value::Text(self.current_role().to_owned()));
        d.insert("language".into(), Value::Text(f.language.clone()));
        d.insert("body".into(), Value::Text(f.body.clone()));
        d.insert("procedure".into(), Value::Bool(f.is_procedure));
        d.insert("returns".into(), Value::Text(return_text(&f.returns)));
        d.insert("args".into(), Value::List(f.args.iter().map(|(n, ty, default)| {
            let mut m = Doc::new();
            m.insert("name".into(), Value::Text(n.clone()));
            m.insert("type".into(), Value::Text(crate::datum::type_sql(ty)));
            if let Some(e) = default {
                m.insert("default".into(), Value::Text(e.to_string()));
            }
            Value::Map(m)
        }).collect()));
        let w = Write { collection: FUNCTIONS.into(), key: f.name.clone(), op: Op::Put(d), pre: Pre::None, tag: Tag::Internal };
        self.engine.store.commit(std::slice::from_ref(&w)).map_err(|e| e.to_sql(std::slice::from_ref(&w)))?;
        Ok(Outcome::command(if f.is_procedure { "CREATE PROCEDURE" } else { "CREATE FUNCTION" }))
    }

    pub(crate) fn drop_function(&mut self, names: &[String], if_exists: bool) -> SqlResult<Outcome> {
        for n in names {
            let key = n.to_ascii_lowercase();
            let Some(d) = self.engine.store.get(FUNCTIONS, &key)? else {
                if if_exists {
                    continue;
                }
                return Err(SqlError::new("42883", format!("function {n} does not exist")));
            };
            let acl = self.engine.acl()?;
            if !acl.is_superuser(self.current_role()) && !acl.owns(self.current_role(), d.fields.get("owner").and_then(Value::as_str)) {
                return Err(SqlError::new("42501", format!("must be owner of function {n}")));
            }
            self.engine.refresh_catalog(true)?;
            if let Some(t) = self.engine.catalog.all().iter().find(|t| t.triggers.iter().any(|x| x.function == key)) {
                return Err(SqlError::new("2BP01", format!("cannot drop function {n} because a trigger on table {} depends on it", t.name)));
            }
            let w = Write { collection: FUNCTIONS.into(), key: key.clone(), op: Op::Delete, pre: Pre::None, tag: Tag::Internal };
            self.engine.store.commit(std::slice::from_ref(&w)).map_err(|e| e.to_sql(std::slice::from_ref(&w)))?;
            self.drop_grants(&format!("func:{key}"))?;
        }
        Ok(Outcome::command("DROP FUNCTION"))
    }

    pub(crate) fn create_trigger(&mut self, c: &CreateTrigger) -> SqlResult<Outcome> {
        let mut t = (*self.engine.table(&c.table)?).clone();
        self.require_table(&t, "TRIGGER", &[])?;
        if t.temp.is_some() {
            return Err(SqlError::unsupported("triggers on temporary tables"));
        }
        let Some(r) = load_routine(self, &c.function)? else {
            return Err(SqlError::new("42883", format!("function {}() does not exist", c.function)));
        };
        if !matches!(r.returns, FuncReturn::Trigger) {
            return Err(SqlError::new("42P17", format!("function {} must return type trigger", c.function)));
        }
        if c.when.is_some() && !c.for_each_row {
            return Err(SqlError::unsupported("WHEN on a statement trigger"));
        }
        if let Some(pos) = t.triggers.iter().position(|x| x.name.eq_ignore_ascii_case(&c.name)) {
            if !c.or_replace {
                return Err(SqlError::new("42710", format!("trigger \"{}\" for relation \"{}\" already exists", c.name, t.name)));
            }
            t.triggers.remove(pos);
        }
        t.triggers.push(Trigger { name: c.name.clone(), before: c.before, events: c.events.clone(), for_each_row: c.for_each_row, when: c.when.clone(), function: c.function.clone(), args: c.args.clone() });
        crate::exec::ddl::save_table(self, &t)?;
        Ok(Outcome::command("CREATE TRIGGER"))
    }

    pub(crate) fn drop_trigger(&mut self, name: &str, table: &str, if_exists: bool) -> SqlResult<Outcome> {
        let mut t = (*self.engine.table(table)?).clone();
        self.require_owner(&t, "ALTER")?;
        let before = t.triggers.len();
        t.triggers.retain(|x| !x.name.eq_ignore_ascii_case(name));
        if t.triggers.len() == before {
            if if_exists {
                return Ok(Outcome::command("DROP TRIGGER"));
            }
            return Err(SqlError::new("42704", format!("trigger \"{name}\" for table \"{table}\" does not exist")));
        }
        crate::exec::ddl::save_table(self, &t)?;
        Ok(Outcome::command("DROP TRIGGER"))
    }

    /// `CALL p(...)`, and `SELECT f(...)` on its own: the routine may write.
    pub(crate) fn call_routine(&mut self, name: &str, args: &[Expr], params: &[Datum]) -> SqlResult<(Rc<Routine>, Vec<Vec<Datum>>)> {
        let Some(r) = load_routine(self, name)? else {
            return Err(SqlError::new("42883", format!("function {name}() does not exist")));
        };
        let values = {
            let ex = Exec::new(self, params);
            args.iter().map(|a| ex.eval_const(a)).collect::<SqlResult<Vec<_>>>()?
        };
        let rows = run_routine(&mut Owned(self), &r, values, None)?;
        Ok((r, rows))
    }

    /// Runs `SELECT f(...)` with the session when a write made the shared attempt fail.
    pub(crate) fn select_function_with_session(&mut self, q: &Query, params: &[Datum]) -> Option<SqlResult<Outcome>> {
        let SetExpr::Select(sel) = &q.body else { return None };
        if !sel.from.is_empty() || sel.where_.is_some() || !sel.group_by.is_empty() || !q.ctes.is_empty() {
            return None;
        }
        let [crate::ast::SelectItem::Expr { expr: Expr::Function { name, args, .. }, alias }] = sel.items.as_slice() else { return None };
        let lower = name.to_ascii_lowercase();
        let (r, mut rows) = match self.call_routine(&lower, args, params) {
            Ok(x) => x,
            Err(e) => return Some(Err(e)),
        };
        let value = rows.drain(..).next().and_then(|mut x| if x.is_empty() { None } else { Some(x.remove(0)) }).unwrap_or(Datum::Null);
        let ty = match &r.returns {
            FuncReturn::Scalar(t) | FuncReturn::Set(t) => t.clone(),
            _ => DataType::Text,
        };
        Some(Ok(Outcome::Rows(ResultSet { columns: vec![OutCol { name: alias.clone().unwrap_or(lower), ty, table: None }], rows: vec![vec![value]] })))
    }

    /// Row triggers of `table` for this event. `Ok(None)`: a BEFORE trigger cancelled the row.
    /// `new` comes back as the triggers left it.
    pub(crate) fn fire_row_triggers(&mut self, table: &Table, before: bool, event: &str, old: Option<&Row>, new: Option<Row>) -> SqlResult<Option<Option<Row>>> {
        let mut new = new;
        for tr in table.triggers.iter().filter(|t| t.before == before && t.for_each_row && t.events.iter().any(|e| e == event)) {
            let Some(r) = load_routine(self, &tr.function)? else {
                return Err(SqlError::new("42883", format!("function {}() does not exist", tr.function)));
            };
            let mut vars = trigger_vars(table, tr, before, event, true);
            let bind = |vars: &mut HashMap<String, Datum>, prefix: &str, row: Option<&Row>| {
                for (i, c) in table.columns.iter().enumerate() {
                    let v = row.and_then(|x| x.get(i).cloned()).unwrap_or(Datum::Null);
                    vars.insert(format!("{prefix}.{}", c.name.to_ascii_lowercase()), v);
                }
            };
            bind(&mut vars, "new", new.as_ref());
            bind(&mut vars, "old", old);
            if let Some(w) = &tr.when {
                let frames = vars.clone();
                self.frames.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(frames);
                let ok = Exec::new(self, &[]).eval_const(w).and_then(|d| d.truthy());
                self.frames.lock().unwrap_or_else(std::sync::PoisonError::into_inner).pop();
                if ok? != Some(true) {
                    continue;
                }
            }
            let (ret, frame) = run_trigger(&mut Owned(self), &r, vars)?;
            if !before {
                continue;
            }
            // RETURN NULL skips the row; RETURN NEW / OLD give what the variables hold.
            let Some(which) = ret else { return Ok(None) };
            if which == "old" {
                new = old.cloned().or(new);
            } else if new.is_some() {
                new = Some(table.columns.iter().map(|c| frame.get(&format!("new.{}", c.name.to_ascii_lowercase())).cloned().unwrap_or(Datum::Null)).collect());
            }
        }
        Ok(Some(new))
    }

    pub(crate) fn fire_statement_triggers(&mut self, table: &Table, before: bool, event: &str) -> SqlResult<()> {
        if table.triggers.is_empty() {
            return Ok(());
        }
        for tr in table.triggers.iter().filter(|t| t.before == before && !t.for_each_row && t.events.iter().any(|e| e == event)) {
            let Some(r) = load_routine(self, &tr.function)? else {
                return Err(SqlError::new("42883", format!("function {}() does not exist", tr.function)));
            };
            let vars = trigger_vars(table, tr, before, event, false);
            run_trigger(&mut Owned(self), &r, vars)?;
        }
        Ok(())
    }
}

fn trigger_vars(table: &Table, tr: &Trigger, before: bool, event: &str, row: bool) -> HashMap<String, Datum> {
    let t = |s: &str| Datum::Text(s.to_owned());
    let mut vars = HashMap::new();
    vars.insert("tg_name".to_owned(), t(&tr.name));
    vars.insert("tg_when".to_owned(), t(if before { "BEFORE" } else { "AFTER" }));
    vars.insert("tg_level".to_owned(), t(if row { "ROW" } else { "STATEMENT" }));
    vars.insert("tg_op".to_owned(), t(&event.to_ascii_uppercase()));
    vars.insert("tg_table_name".to_owned(), t(&table.name));
    vars.insert("tg_relname".to_owned(), t(&table.name));
    vars.insert("tg_table_schema".to_owned(), t("public"));
    vars.insert("tg_nargs".to_owned(), Datum::Int(tr.args.len() as i64));
    vars
}

fn return_text(r: &FuncReturn) -> String {
    match r {
        FuncReturn::Void => "void".into(),
        FuncReturn::Trigger => "trigger".into(),
        FuncReturn::Scalar(t) => crate::datum::type_sql(t),
        FuncReturn::Set(t) => format!("setof {}", crate::datum::type_sql(t)),
        FuncReturn::Table(cols) => format!("table({})", cols.iter().map(|(n, t)| format!("{n} {}", crate::datum::type_sql(t))).collect::<Vec<_>>().join(", ")),
    }
}

fn parse_return(text: &str) -> SqlResult<FuncReturn> {
    let ty = crate::parser::parse_data_type;
    Ok(if text == "void" {
        FuncReturn::Void
    } else if text == "trigger" {
        FuncReturn::Trigger
    } else if let Some(t) = text.strip_prefix("setof ") {
        FuncReturn::Set(ty(t)?)
    } else if let Some(inner) = text.strip_prefix("table(").and_then(|x| x.strip_suffix(')')) {
        let mut cols = Vec::new();
        for c in split_top(inner) {
            let (n, t) = c.trim().split_once(' ').ok_or_else(|| SqlError::internal("a stored function is damaged"))?;
            cols.push((n.to_owned(), ty(t)?));
        }
        FuncReturn::Table(cols)
    } else {
        FuncReturn::Scalar(ty(text)?)
    })
}

/// Splits at commas outside brackets (`numeric(10,2), text`).
fn split_top(s: &str) -> Vec<&str> {
    let (mut out, mut depth, mut start) = (Vec::new(), 0, 0);
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                out.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    out.push(&s[start..]);
    out
}

/// A routine by name.
pub(crate) fn load_routine(s: &Session, name: &str) -> SqlResult<Option<Rc<Routine>>> {
    let key = name.rsplit('.').next().unwrap_or(name).to_ascii_lowercase();
    let Some(d) = s.engine.store.get(FUNCTIONS, &key)? else { return Ok(None) };
    let text = |k: &str| d.fields.get(k).and_then(Value::as_str).unwrap_or_default().to_owned();
    let mut args = Vec::new();
    if let Some(Value::List(l)) = d.fields.get("args") {
        for a in l {
            let Some(m) = a.as_map() else { continue };
            let t = |k: &str| m.get(k).and_then(Value::as_str).unwrap_or_default().to_owned();
            let default = match m.get("default").and_then(Value::as_str) {
                Some(sql) => Some(crate::parser::parse_expr(sql, Dialect::Postgres)?),
                None => None,
            };
            args.push((t("name"), crate::parser::parse_data_type(&t("type"))?, default));
        }
    }
    let language = text("language");
    let body = text("body");
    let parsed = if language == "plpgsql" { Parsed::Pl(parse_body(&body)?) } else { Parsed::Sql(crate::parser::parse(&body, Dialect::Postgres)?.0) };
    Ok(Some(Rc::new(Routine { name: key, args, returns: parse_return(&text("returns"))?, owner: d.fields.get("owner").and_then(Value::as_str).map(str::to_owned), parsed })))
}

/// Whether the caller may run it: `EXECUTE` is open until revoked from `public`.
fn check_execute(s: &Session, r: &Routine) -> SqlResult<()> {
    let acl = s.engine.acl()?;
    let object = format!("func:{}", r.name);
    let me = s.current_role();
    let closed = acl.grants().iter().any(|g| g.object == object && g.privilege == "NOEXECUTE");
    if !closed || acl.is_superuser(me) || acl.owns(me, r.owner.as_deref()) {
        return Ok(());
    }
    let roles = acl.closure(me);
    if acl.grants().iter().any(|g| g.object == object && g.privilege == "EXECUTE" && !g.deny && roles.contains(&g.grantee)) {
        return Ok(());
    }
    Err(SqlError::new("42501", format!("permission denied for function {}", r.name)))
}

/// Rows a routine returns: one row of one column for a scalar.
fn run_routine(host: &mut dyn Host, r: &Routine, args: Vec<Datum>, frame: Option<HashMap<String, Datum>>) -> SqlResult<Vec<Vec<Datum>>> {
    check_execute(host.session(), r)?;
    if args.len() > r.args.len() || r.args.iter().skip(args.len()).any(|a| a.2.is_none()) {
        return Err(SqlError::new("42883", format!("function {}() takes {} arguments, not {}", r.name, r.args.len(), args.len())));
    }
    let mut vars = frame.unwrap_or_default();
    let ex_session = host.session();
    for (i, (name, ty, default)) in r.args.iter().enumerate() {
        let v = match args.get(i) {
            Some(v) => cast(v, ty)?,
            None => match default {
                Some(e) => cast(&Exec::new(ex_session, &[]).eval_const(e)?, ty)?,
                None => Datum::Null,
            },
        };
        vars.insert(format!("${}", i + 1), v.clone());
        if !name.is_empty() {
            vars.insert(name.clone(), v);
        }
    }
    {
        let mut frames = host.session().frames.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if frames.len() >= MAX_DEPTH {
            return Err(SqlError::new("54001", "routine calls are nested too deeply"));
        }
        frames.push(vars);
    }
    let out = run_body(host, r);
    host.session().frames.lock().unwrap_or_else(std::sync::PoisonError::into_inner).pop();
    out
}

/// Runs a trigger function; gives what it returned (`new`, `old`, or `None` for NULL), and its
/// variables.
fn run_trigger(host: &mut dyn Host, r: &Routine, vars: HashMap<String, Datum>) -> SqlResult<(Option<String>, HashMap<String, Datum>)> {
    check_execute(host.session(), r)?;
    {
        let mut frames = host.session().frames.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if frames.len() >= MAX_DEPTH {
            return Err(SqlError::new("54001", "triggers are nested too deeply"));
        }
        frames.push(vars);
    }
    let Parsed::Pl(block) = &r.parsed else {
        host.session().frames.lock().unwrap_or_else(std::sync::PoisonError::into_inner).pop();
        return Err(SqlError::new("42P17", "a trigger function must be written in plpgsql"));
    };
    let mut rows = Vec::new();
    let flow = run_block(host, block, &mut rows, true);
    let frame = host.session().frames.lock().unwrap_or_else(std::sync::PoisonError::into_inner).pop().unwrap_or_default();
    let which = match flow? {
        Flow::Return(Some(Datum::Text(w))) if w == TRIGGER_NEW || w == TRIGGER_OLD => Some(w.trim_start_matches('\u{0}').to_owned()),
        Flow::Return(_) => None,
        _ => return Err(SqlError::new("2F005", "control reached end of trigger procedure without RETURN")),
    };
    Ok((which, frame))
}

const TRIGGER_NEW: &str = "\u{0}new";
const TRIGGER_OLD: &str = "\u{0}old";

fn run_body(host: &mut dyn Host, r: &Routine) -> SqlResult<Vec<Vec<Datum>>> {
    match &r.parsed {
        Parsed::Sql(stmts) => {
            let mut last = None;
            for st in stmts {
                last = Some(host.run(st)?);
            }
            match (&r.returns, last) {
                (FuncReturn::Void, _) | (_, None) => Ok(Vec::new()),
                (FuncReturn::Scalar(t), Some(Outcome::Rows(rs))) => Ok(match rs.rows.into_iter().next() {
                    Some(row) => vec![vec![cast(row.first().unwrap_or(&Datum::Null), t)?]],
                    None => vec![vec![Datum::Null]],
                }),
                (_, Some(Outcome::Rows(rs))) => Ok(rs.rows),
                (_, Some(_)) => Err(SqlError::new("42P13", "the function's last statement must return rows")),
            }
        }
        Parsed::Pl(block) => {
            let mut rows = Vec::new();
            match run_block(host, block, &mut rows, false)? {
                Flow::Return(Some(v)) => match &r.returns {
                    FuncReturn::Scalar(t) => Ok(vec![vec![cast(&v, t)?]]),
                    _ => Ok(rows),
                },
                Flow::Return(None) | Flow::Next | Flow::Exit | Flow::Continue => match &r.returns {
                    FuncReturn::Scalar(_) => Err(SqlError::new("2F005", format!("control reached end of function {} without RETURN", r.name))),
                    _ => Ok(rows),
                },
            }
        }
    }
}

fn eval(host: &dyn Host, e: &Expr) -> SqlResult<Datum> {
    Exec::new(host.session(), &host.session().frame_params()).eval_const(e)
}

fn truthy(host: &dyn Host, e: &Expr) -> SqlResult<bool> {
    Ok(eval(host, e)?.truthy()? == Some(true))
}

impl Session {
    /// `$1`, `$2`, ... of the running routine.
    fn frame_params(&self) -> Vec<Datum> {
        let frames = self.frames.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(top) = frames.last() else { return Vec::new() };
        let mut out = Vec::new();
        while let Some(v) = top.get(&format!("${}", out.len() + 1)) {
            out.push(v.clone());
        }
        out
    }
}

fn run_block(host: &mut dyn Host, b: &PlBlock, rows: &mut Vec<Vec<Datum>>, trigger: bool) -> SqlResult<Flow> {
    for (name, ty, init) in &b.declarations {
        let v = match init {
            Some(e) => cast(&eval(host, e)?, ty)?,
            None => Datum::Null,
        };
        host.session().set_variable(name, v);
    }
    let mark = host.mark();
    let out = run_list(host, &b.body, rows, trigger);
    let Err(e) = out else { return out };
    // Writes over a shared session are not application errors: they go up as they are.
    if e.message.starts_with(NEEDS_SESSION) || e.code == "57014" || e.code == "57P01" {
        return Err(e);
    }
    for (conditions, body) in &b.handlers {
        if conditions.iter().any(|c| condition_matches(c, e.code)) {
            host.rollback_to(mark);
            host.session().set_variable("sqlstate", Datum::Text(e.code.to_owned()));
            host.session().set_variable("sqlerrm", Datum::Text(e.message.clone()));
            return run_list(host, body, rows, trigger);
        }
    }
    Err(e)
}

fn condition_matches(condition: &str, code: &str) -> bool {
    match condition {
        "others" => true,
        "division_by_zero" => code == "22012",
        "unique_violation" => code == "23505",
        "foreign_key_violation" => code == "23503",
        "not_null_violation" => code == "23502",
        "check_violation" => code == "23514",
        "no_data_found" => code == "P0002",
        "too_many_rows" => code == "P0003",
        "raise_exception" => code == "P0001",
        "invalid_text_representation" => code == "22P02",
        "numeric_value_out_of_range" => code == "22003",
        "serialization_failure" => code == "40001",
        "lock_not_available" => code == "55P03",
        other => other.eq_ignore_ascii_case(code),
    }
}

fn run_list(host: &mut dyn Host, list: &[PlStmt], rows: &mut Vec<Vec<Datum>>, trigger: bool) -> SqlResult<Flow> {
    for st in list {
        match run_stmt(host, st, rows, trigger)? {
            Flow::Next => {}
            other => return Ok(other),
        }
    }
    Ok(Flow::Next)
}

fn run_loop_body(host: &mut dyn Host, body: &[PlStmt], rows: &mut Vec<Vec<Datum>>, trigger: bool) -> SqlResult<Option<Flow>> {
    host.session().control.check()?;
    Ok(match run_list(host, body, rows, trigger)? {
        Flow::Next | Flow::Continue => None,
        Flow::Exit => Some(Flow::Next),
        ret @ Flow::Return(_) => Some(ret),
    })
}

fn run_stmt(host: &mut dyn Host, st: &PlStmt, rows: &mut Vec<Vec<Datum>>, trigger: bool) -> SqlResult<Flow> {
    match st {
        PlStmt::Null => {}
        PlStmt::Assign(name, e) => {
            let v = eval(host, e)?;
            host.session().set_variable(name, v);
        }
        PlStmt::Block(b) => return run_block(host, b, rows, trigger),
        PlStmt::If(branches, otherwise) => {
            for (c, body) in branches {
                if truthy(host, c)? {
                    return run_list(host, body, rows, trigger);
                }
            }
            return run_list(host, otherwise, rows, trigger);
        }
        PlStmt::While(c, body) => {
            while truthy(host, c)? {
                if let Some(f) = run_loop_body(host, body, rows, trigger)? {
                    return Ok(f);
                }
            }
        }
        PlStmt::Loop(body) => loop {
            if let Some(f) = run_loop_body(host, body, rows, trigger)? {
                return Ok(f);
            }
        },
        PlStmt::ForRange { var, from, to, by, reverse, body } => {
            let (a, b) = (eval(host, from)?.as_i64()?, eval(host, to)?.as_i64()?);
            let step = match by {
                Some(e) => eval(host, e)?.as_i64()?,
                None => 1,
            };
            if step <= 0 {
                return Err(SqlError::new("22023", "BY value of FOR loop must be greater than zero"));
            }
            let mut i = a;
            while if *reverse { i >= b } else { i <= b } {
                host.session().set_variable(var, Datum::Int(i));
                if let Some(f) = run_loop_body(host, body, rows, trigger)? {
                    return Ok(f);
                }
                i = if *reverse { i.saturating_sub(step) } else { i.saturating_add(step) };
                if i == i64::MAX || i == i64::MIN {
                    break;
                }
            }
        }
        PlStmt::ForQuery { var, query, body } => {
            let rs = {
                let params = host.session().frame_params();
                let ex = Exec::new(host.session(), &params);
                let rel = ex.query(query, None)?;
                ex.result_set(rel)?
            };
            for row in rs.rows {
                for (c, v) in rs.columns.iter().zip(row) {
                    host.session().set_variable(&format!("{var}.{}", c.name.to_ascii_lowercase()), v);
                }
                if let Some(f) = run_loop_body(host, body, rows, trigger)? {
                    return Ok(f);
                }
            }
        }
        PlStmt::Exit(c) => {
            if c.as_ref().map_or(Ok(true), |e| truthy(host, e))? {
                return Ok(Flow::Exit);
            }
        }
        PlStmt::Continue(c) => {
            if c.as_ref().map_or(Ok(true), |e| truthy(host, e))? {
                return Ok(Flow::Continue);
            }
        }
        PlStmt::Return(e) => {
            if trigger {
                // Trigger functions return NEW, OLD or NULL.
                return Ok(Flow::Return(match e {
                    Some(Expr::Column(p)) if p.len() == 1 && p[0].eq_ignore_ascii_case("new") => Some(Datum::Text(TRIGGER_NEW.into())),
                    Some(Expr::Column(p)) if p.len() == 1 && p[0].eq_ignore_ascii_case("old") => Some(Datum::Text(TRIGGER_OLD.into())),
                    Some(Expr::Null) | None => None,
                    Some(_) => return Err(SqlError::unsupported("a trigger function returning anything but NEW, OLD or NULL")),
                }));
            }
            return Ok(Flow::Return(match e {
                Some(e) => Some(eval(host, e)?),
                None => None,
            }));
        }
        PlStmt::ReturnNext(e) => {
            let Some(e) = e else { return Err(SqlError::unsupported("RETURN NEXT without an expression")) };
            let v = eval(host, e)?;
            rows.push(vec![v]);
        }
        PlStmt::ReturnQuery(q) => {
            let params = host.session().frame_params();
            let ex = Exec::new(host.session(), &params);
            let rel = ex.query(q, None)?;
            rows.extend(rel.rows);
        }
        PlStmt::Perform(q) => {
            // A routine called here may write when this routine may.
            let st = Statement::Query(q.clone());
            match host.run(&st) {
                Err(e) if e.message.starts_with(NEEDS_SESSION) => return Err(e),
                other => {
                    other?;
                }
            }
        }
        PlStmt::Raise { exception, message, args, code } => {
            let mut text = String::with_capacity(message.len());
            let mut it = args.iter();
            let mut chars = message.chars().peekable();
            while let Some(c) = chars.next() {
                if c == '%' {
                    if chars.peek() == Some(&'%') {
                        chars.next();
                        text.push('%');
                        continue;
                    }
                    match it.next() {
                        Some(e) => {
                            let v = eval(host, e)?;
                            text.push_str(&if v.is_null() { "<NULL>".to_owned() } else { v.as_text() });
                        }
                        None => return Err(SqlError::new("42601", "too few parameters specified for RAISE")),
                    }
                } else {
                    text.push(c);
                }
            }
            if *exception {
                let code = code.as_deref().map_or("P0001", leak_code);
                return Err(SqlError::new(code, text));
            }
        }
        PlStmt::Sql(st, into) => {
            let out = host.run(st)?;
            if !into.is_empty() {
                let row = match &out {
                    Outcome::Rows(rs) => rs.rows.first().cloned(),
                    _ => None,
                };
                for (i, name) in into.iter().enumerate() {
                    let v = row.as_ref().and_then(|r| r.get(i).cloned()).unwrap_or(Datum::Null);
                    host.session().set_variable(name, v);
                }
            }
        }
    }
    Ok(Flow::Next)
}

/// An error code from `RAISE ... USING ERRCODE`: a known one, else the generic raise code.
/// (`SqlError` codes are static; five characters of a user's choosing are not remembered.)
fn leak_code(code: &str) -> &'static str {
    const KNOWN: &[&str] = &["22000", "22012", "23505", "23503", "23502", "23514", "40001", "42501", "P0001", "P0002", "P0003", "55P03", "22023", "42P01", "42703"];
    KNOWN.iter().copied().find(|k| k.eq_ignore_ascii_case(code)).unwrap_or("P0001")
}

impl Exec<'_> {
    /// A user function called from an expression.
    pub(crate) fn call_user_function(&self, name: &str, args: Vec<Datum>) -> SqlResult<Option<Datum>> {
        let Some(r) = load_routine(self.s, name)? else { return Ok(None) };
        if matches!(r.returns, FuncReturn::Trigger) {
            return Err(SqlError::new("0A000", "trigger functions can only be called as triggers"));
        }
        let rows = run_routine(&mut Shared(self.s), &r, args, None)?;
        Ok(Some(rows.into_iter().next().and_then(|mut row| if row.is_empty() { None } else { Some(row.remove(0)) }).unwrap_or(Datum::Null)))
    }

    /// A user function in `FROM`.
    pub(crate) fn user_table_function(&self, name: &str, args: &[Datum]) -> SqlResult<Option<Rel>> {
        let Some(r) = load_routine(self.s, name)? else { return Ok(None) };
        let rows = run_routine(&mut Shared(self.s), &r, args.to_vec(), None)?;
        let cols = match &r.returns {
            FuncReturn::Table(cols) => cols.iter().map(|(n, t)| ColMeta::new(n.clone(), Some(t.clone()))).collect(),
            FuncReturn::Scalar(t) | FuncReturn::Set(t) => vec![ColMeta::new(r.name.clone(), Some(t.clone()))],
            _ => return Err(SqlError::new("42809", format!("function {name} does not return rows"))),
        };
        Ok(Some(Rel { cols, rows }))
    }
}

/// Whether an error asks to run again with the session (see the module docs).
pub(crate) fn needs_session(e: &SqlError) -> bool {
    e.code == "0A000" && e.message.starts_with(NEEDS_SESSION)
}
