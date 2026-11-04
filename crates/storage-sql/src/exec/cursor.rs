//! Cursors (`F-ENT-05`, `TASK-149`): `DECLARE` / `FETCH` / `MOVE` / `CLOSE` (what psycopg's named
//! cursors send), T-SQL `DECLARE c CURSOR` / `OPEN` / `FETCH ... INTO` / `DEALLOCATE`.
//!
//! A cursor's rows are the query's at `DECLARE` (`OPEN` in T-SQL), held in the session and counted
//! against `max_query_memory`: every cursor is insensitive and scrollable. Without `WITH HOLD`, a
//! PostgreSQL cursor ends with its transaction.

use std::collections::HashMap;

use crate::ast::{FetchDirection, Query};
use crate::datum::Datum;
use crate::error::{SqlError, SqlResult};
use crate::exec::eval::Exec;
use crate::exec::{OutCol, Outcome, ResultSet, Session};
use crate::lexer::Dialect;

#[derive(Debug)]
pub struct Cursor {
    /// T-SQL: declared, not yet opened.
    pub query: Option<Query>,
    pub columns: Vec<OutCol>,
    pub rows: Vec<Vec<Datum>>,
    /// 0 before the first row, `rows.len() + 1` after the last.
    pub at: usize,
    pub hold: bool,
    pub open: bool,
}

pub type Cursors = HashMap<String, Cursor>;

fn missing(name: &str) -> SqlError {
    SqlError::new("34000", format!("cursor \"{name}\" does not exist"))
}

impl Session {
    pub(crate) fn declare_cursor(&mut self, name: &str, query: &Query, hold: bool, params: &[Datum]) -> SqlResult<Outcome> {
        let key = name.to_ascii_lowercase();
        if self.dialect == Dialect::MsSql {
            if self.cursors.get(&key).is_some_and(|c| c.open) {
                return Err(SqlError::new("42P03", format!("A cursor with the name '{name}' already exists.")));
            }
            self.cursors.insert(key, Cursor { query: Some(query.clone()), columns: Vec::new(), rows: Vec::new(), at: 0, hold: true, open: false });
            return Ok(Outcome::command("DECLARE CURSOR"));
        }
        if !hold && !self.in_explicit_transaction() {
            return Err(SqlError::new("25P01", "DECLARE CURSOR can only be used in transaction blocks"));
        }
        if self.cursors.contains_key(&key) {
            return Err(SqlError::new("42P03", format!("cursor \"{name}\" already exists")));
        }
        let (columns, rows) = self.cursor_rows(query, params)?;
        self.cursors.insert(key, Cursor { query: None, columns, rows, at: 0, hold, open: true });
        Ok(Outcome::command("DECLARE CURSOR"))
    }

    fn cursor_rows(&self, query: &Query, params: &[Datum]) -> SqlResult<(Vec<OutCol>, Vec<Vec<Datum>>)> {
        let ex = Exec::new(self, params);
        let rel = ex.query(query, None)?;
        let rs = ex.result_set(rel)?;
        Ok((rs.columns, rs.rows))
    }

    /// T-SQL `OPEN c`.
    pub(crate) fn open_cursor(&mut self, name: &str, params: &[Datum]) -> SqlResult<Outcome> {
        let key = name.to_ascii_lowercase();
        let query = match self.cursors.get(&key) {
            Some(c) if c.open => return Err(SqlError::new("42P03", "The cursor is already open.")),
            Some(c) => c.query.clone(),
            None => return Err(missing(name)),
        };
        let Some(query) = query else { return Err(missing(name)) };
        let (columns, rows) = self.cursor_rows(&query, params)?;
        if let Some(c) = self.cursors.get_mut(&key) {
            c.columns = columns;
            c.rows = rows;
            c.at = 0;
            c.open = true;
        }
        Ok(Outcome::command("OPEN"))
    }

    /// `FETCH` (`move_only`: `MOVE`). T-SQL `INTO @a, @b` assigns one row and sets `@@FETCH_STATUS`.
    pub(crate) fn fetch_cursor(&mut self, name: &str, dir: &FetchDirection, move_only: bool, into: &[String]) -> SqlResult<Outcome> {
        let key = name.to_ascii_lowercase();
        let c = self.cursors.get_mut(&key).ok_or_else(|| missing(name))?;
        if !c.open {
            return Err(SqlError::new("34000", format!("The cursor \"{name}\" is not open.")));
        }
        let n = c.rows.len();
        let mut out: Vec<Vec<Datum>> = Vec::new();
        let mut moved = 0u64;
        // Steps `count` rows forward or back, stopping beyond either end, as PostgreSQL does.
        let step = |c: &mut Cursor, count: i64, out: &mut Vec<Vec<Datum>>, moved: &mut u64| {
            let forward = count >= 0;
            for _ in 0..count.unsigned_abs() {
                if forward {
                    if c.at > n {
                        break;
                    }
                    c.at += 1;
                    if c.at > n {
                        break;
                    }
                } else {
                    if c.at == 0 {
                        break;
                    }
                    c.at -= 1;
                    if c.at == 0 {
                        break;
                    }
                }
                *moved += 1;
                if !move_only {
                    out.push(c.rows[c.at - 1].clone());
                }
            }
        };
        // Goes to one row (or past an end), returning at most that row.
        let jump = |c: &mut Cursor, to: i64, out: &mut Vec<Vec<Datum>>, moved: &mut u64| {
            let total = n as i64;
            let target = if to < 0 { total + 1 + to } else { to };
            c.at = target.clamp(0, total + 1) as usize;
            if (1..=total).contains(&target) {
                *moved = 1;
                if !move_only {
                    out.push(c.rows[c.at - 1].clone());
                }
            }
        };
        match dir {
            FetchDirection::Next => step(c, 1, &mut out, &mut moved),
            FetchDirection::Prior => step(c, -1, &mut out, &mut moved),
            FetchDirection::First => jump(c, 1, &mut out, &mut moved),
            FetchDirection::Last => jump(c, n as i64, &mut out, &mut moved),
            FetchDirection::Absolute(k) => jump(c, *k, &mut out, &mut moved),
            FetchDirection::Relative(k) => {
                let to = c.at as i64 + *k;
                if *k == 0 && (1..=n).contains(&c.at) {
                    moved = 1;
                    if !move_only {
                        out.push(c.rows[c.at - 1].clone());
                    }
                } else {
                    jump(c, to.max(0), &mut out, &mut moved);
                }
            }
            FetchDirection::Forward(k) => step(c, *k, &mut out, &mut moved),
            FetchDirection::ForwardAll => step(c, (n + 1) as i64, &mut out, &mut moved),
            FetchDirection::Backward(k) => step(c, -*k, &mut out, &mut moved),
            FetchDirection::BackwardAll => step(c, -((n + 1) as i64), &mut out, &mut moved),
        }
        let columns = c.columns.clone();
        if self.dialect == Dialect::MsSql {
            // 0: a row; -1: beyond the result set.
            self.user_vars.insert("@fetch_status".into(), Datum::Int(if moved > 0 { 0 } else { -1 }));
            if !into.is_empty() {
                if let Some(row) = out.first() {
                    if row.len() != into.len() {
                        return Err(SqlError::new("42601", "The number of variables declared in the INTO list must match that of selected columns."));
                    }
                    for (v, d) in into.iter().zip(row) {
                        self.user_vars.insert(v.to_ascii_lowercase(), d.clone());
                    }
                }
                return Ok(Outcome::command("FETCH"));
            }
        }
        if move_only {
            return Ok(Outcome::Command { tag: format!("MOVE {moved}"), affected: moved, last_insert_id: None });
        }
        Ok(Outcome::Rows(ResultSet { columns, rows: out }))
    }

    pub(crate) fn close_cursor(&mut self, name: Option<&str>) -> SqlResult<Outcome> {
        match name {
            None => self.cursors.clear(),
            Some(n) => {
                let key = n.to_ascii_lowercase();
                if self.dialect == Dialect::MsSql {
                    let c = self.cursors.get_mut(&key).ok_or_else(|| missing(n))?;
                    c.open = false;
                    c.rows = Vec::new();
                } else if self.cursors.remove(&key).is_none() {
                    return Err(missing(n));
                }
            }
        }
        Ok(Outcome::command("CLOSE CURSOR"))
    }

    pub(crate) fn deallocate_cursor(&mut self, name: &str) -> SqlResult<Outcome> {
        if self.cursors.remove(&name.to_ascii_lowercase()).is_none() {
            return Err(missing(name));
        }
        Ok(Outcome::command("DEALLOCATE"))
    }

    /// A transaction ended: cursors without `WITH HOLD` end with it.
    pub(crate) fn cursors_after_transaction(&mut self) {
        self.cursors.retain(|_, c| c.hold);
    }
}
