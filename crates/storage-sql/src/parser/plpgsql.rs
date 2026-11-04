//! The PL/pgSQL subset of `CREATE FUNCTION` bodies (`F-ENT-05`, `TASK-149`).
//!
//! Blocks, `DECLARE`, assignment, `IF`, `WHILE`, `LOOP`, `FOR` over integers and query rows, `EXIT`,
//! `CONTINUE`, `RETURN`, `RETURN NEXT`, `RETURN QUERY`, `PERFORM`, `RAISE`, `SELECT ... INTO`,
//! `... RETURNING ... INTO`, `EXCEPTION WHEN`, and any other SQL statement. Anything else is refused
//! when the function is created.

use super::Parser;
use crate::ast::*;
use crate::error::{SqlError, SqlResult};
use crate::lexer::{tokenize, Dialect, Token};

/// A statement of a body.
#[derive(Debug, Clone, PartialEq)]
pub enum PlStmt {
    Assign(String, Expr),
    /// SQL, and the variables its first row goes into.
    Sql(Statement, Vec<String>),
    If(Vec<(Expr, Vec<PlStmt>)>, Vec<PlStmt>),
    While(Expr, Vec<PlStmt>),
    Loop(Vec<PlStmt>),
    ForRange { var: String, from: Expr, to: Expr, by: Option<Expr>, reverse: bool, body: Vec<PlStmt> },
    ForQuery { var: String, query: Box<Query>, body: Vec<PlStmt> },
    Exit(Option<Expr>),
    Continue(Option<Expr>),
    Return(Option<Expr>),
    ReturnNext(Option<Expr>),
    ReturnQuery(Box<Query>),
    Perform(Box<Query>),
    Raise { exception: bool, message: String, args: Vec<Expr>, code: Option<String> },
    Block(Box<PlBlock>),
    Null,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct PlBlock {
    pub declarations: Vec<(String, DataType, Option<Expr>)>,
    pub body: Vec<PlStmt>,
    /// `WHEN conditions THEN statements`.
    pub handlers: Vec<(Vec<String>, Vec<PlStmt>)>,
}

/// Parses a PL/pgSQL body.
pub fn parse_body(body: &str) -> SqlResult<PlBlock> {
    // `1..10` must not read as the number `1.` and `.10`.
    let text = body.replace("..", " .. ");
    let (tokens, _) = tokenize(&text, Dialect::Postgres)?;
    let mut p = Parser::new(tokens, Dialect::Postgres);
    let block = p.pl_block()?;
    let _ = p.eat_op(";");
    if !p.at_end() {
        return Err(p.unexpected("the end of the function body"));
    }
    Ok(block)
}

impl Parser {
    /// `:=` (two tokens here) or `=`.
    fn eat_assign(&mut self) -> bool {
        if self.is_op(":") && self.is_op_at(1, "=") {
            self.pos += 2;
            return true;
        }
        self.eat_op("=")
    }

    /// The tokens from here up to `stop` at bracket depth 0, as a parser of their own.
    fn pl_slice(&mut self, stop: &[&str]) -> SqlResult<Parser> {
        let start = self.pos;
        let mut depth = 0i32;
        while let Some(t) = self.peek() {
            match t {
                Token::Op("(") => depth += 1,
                Token::Op(")") => depth -= 1,
                Token::Op(o) if depth == 0 && stop.contains(o) => break,
                other => {
                    if depth == 0 && other.keyword().is_some_and(|k| stop.contains(&k.as_str())) {
                        break;
                    }
                }
            }
            self.pos += 1;
        }
        Ok(Parser::new(self.tokens[start..self.pos].to_vec(), Dialect::Postgres))
    }

    fn pl_expr_until(&mut self, stop: &[&str]) -> SqlResult<Expr> {
        let mut p = self.pl_slice(stop)?;
        let e = p.expr()?;
        if !p.at_end() {
            return Err(p.unexpected("the end of the expression"));
        }
        Ok(e)
    }

    fn pl_query_until(&mut self, stop: &[&str]) -> SqlResult<Query> {
        let mut p = self.pl_slice(stop)?;
        let q = p.query()?;
        if !p.at_end() {
            return Err(p.unexpected("the end of the query"));
        }
        Ok(q)
    }

    /// A possibly qualified name (`NEW.x`), lower-cased as PL/pgSQL names are.
    fn pl_target(&mut self) -> SqlResult<String> {
        let mut n = self.ident()?.to_ascii_lowercase();
        while self.eat_op(".") {
            n.push('.');
            n.push_str(&self.ident()?.to_ascii_lowercase());
        }
        Ok(n)
    }

    fn pl_block(&mut self) -> SqlResult<PlBlock> {
        let mut block = PlBlock::default();
        if self.eat_kw("declare") {
            while !self.is_kw("begin") {
                if self.at_end() {
                    return Err(self.unexpected("BEGIN"));
                }
                let name = self.ident()?.to_ascii_lowercase();
                let _ = self.eat_kw("constant");
                let ty = if self.eat_kw("record") {
                    DataType::Text
                } else {
                    let (ty, _) = self.data_type()?;
                    ty
                };
                let _ = self.eat_kws(&["not", "null"]);
                let init = if self.eat_assign() || self.eat_kw("default") { Some(self.pl_expr_until(&[";"])?) } else { None };
                self.expect_op(";")?;
                block.declarations.push((name, ty, init));
            }
        }
        self.expect_kw("begin")?;
        block.body = self.pl_statements(&["end", "exception"])?;
        if self.eat_kw("exception") {
            while self.eat_kw("when") {
                let mut conditions = Vec::new();
                loop {
                    conditions.push(match self.peek().cloned() {
                        Some(Token::Word { text, .. }) => {
                            self.pos += 1;
                            if text.eq_ignore_ascii_case("sqlstate") {
                                match self.next() {
                                    Some(Token::Str(c)) => c,
                                    _ => return Err(self.unexpected("a SQLSTATE")),
                                }
                            } else {
                                text.to_ascii_lowercase()
                            }
                        }
                        _ => return Err(self.unexpected("a condition")),
                    });
                    if !self.eat_kw("or") {
                        break;
                    }
                }
                self.expect_kw("then")?;
                let body = self.pl_statements(&["when", "end"])?;
                block.handlers.push((conditions, body));
            }
        }
        self.expect_kw("end")?;
        Ok(block)
    }

    fn pl_statements(&mut self, until: &[&str]) -> SqlResult<Vec<PlStmt>> {
        let mut out = Vec::new();
        loop {
            if self.at_end() {
                return Err(self.unexpected("END"));
            }
            if self.peek().and_then(Token::keyword).is_some_and(|k| until.contains(&k.as_str())) {
                return Ok(out);
            }
            out.push(self.pl_statement()?);
        }
    }

    fn pl_end(&mut self) -> SqlResult<()> {
        self.expect_op(";")
    }

    fn pl_statement(&mut self) -> SqlResult<PlStmt> {
        let kw = self.peek().and_then(Token::keyword).unwrap_or_default();
        match kw.as_str() {
            "null" => {
                self.pos += 1;
                self.pl_end()?;
                return Ok(PlStmt::Null);
            }
            "begin" | "declare" => {
                let b = self.pl_block()?;
                self.pl_end()?;
                return Ok(PlStmt::Block(Box::new(b)));
            }
            "if" => {
                self.pos += 1;
                let mut branches = Vec::new();
                let cond = self.pl_expr_until(&["then"])?;
                self.expect_kw("then")?;
                branches.push((cond, self.pl_statements(&["elsif", "elseif", "else", "end"])?));
                while self.eat_kw("elsif") || self.eat_kw("elseif") {
                    let cond = self.pl_expr_until(&["then"])?;
                    self.expect_kw("then")?;
                    branches.push((cond, self.pl_statements(&["elsif", "elseif", "else", "end"])?));
                }
                let otherwise = if self.eat_kw("else") { self.pl_statements(&["end"])? } else { Vec::new() };
                self.expect_kw("end")?;
                self.expect_kw("if")?;
                self.pl_end()?;
                return Ok(PlStmt::If(branches, otherwise));
            }
            "while" => {
                self.pos += 1;
                let cond = self.pl_expr_until(&["loop"])?;
                let body = self.pl_loop_body()?;
                return Ok(PlStmt::While(cond, body));
            }
            "loop" => return Ok(PlStmt::Loop(self.pl_loop_body()?)),
            "for" => {
                self.pos += 1;
                let var = self.ident()?.to_ascii_lowercase();
                self.expect_kw("in")?;
                let reverse = self.eat_kw("reverse");
                // A range has `..` at depth 0 before `LOOP`; anything else is a query.
                let mut i = self.pos;
                let mut depth = 0i32;
                let mut range = false;
                while let Some(t) = self.tokens.get(i) {
                    match t {
                        Token::Op("(") => depth += 1,
                        Token::Op(")") => depth -= 1,
                        Token::Op(".") if depth == 0 && matches!(self.tokens.get(i + 1), Some(Token::Op("."))) => {
                            range = true;
                            break;
                        }
                        other if depth == 0 && other.keyword().as_deref() == Some("loop") => break,
                        _ => {}
                    }
                    i += 1;
                }
                if range {
                    let start = self.pos;
                    let mut from_parser = Parser::new(self.tokens[start..i].to_vec(), Dialect::Postgres);
                    let from = from_parser.expr()?;
                    self.pos = i + 2;
                    let to = self.pl_expr_until(&["by", "loop"])?;
                    let by = if self.eat_kw("by") { Some(self.pl_expr_until(&["loop"])?) } else { None };
                    let body = self.pl_loop_body()?;
                    return Ok(PlStmt::ForRange { var, from, to, by, reverse, body });
                }
                let query = Box::new(self.pl_query_until(&["loop"])?);
                let body = self.pl_loop_body()?;
                return Ok(PlStmt::ForQuery { var, query, body });
            }
            "exit" | "continue" => {
                self.pos += 1;
                let cond = if self.eat_kw("when") { Some(self.pl_expr_until(&[";"])?) } else { None };
                self.pl_end()?;
                return Ok(if kw == "exit" { PlStmt::Exit(cond) } else { PlStmt::Continue(cond) });
            }
            "return" => {
                self.pos += 1;
                if self.eat_kw("next") {
                    let e = if self.is_op(";") { None } else { Some(self.pl_expr_until(&[";"])?) };
                    self.pl_end()?;
                    return Ok(PlStmt::ReturnNext(e));
                }
                if self.eat_kw("query") {
                    let q = self.pl_query_until(&[";"])?;
                    self.pl_end()?;
                    return Ok(PlStmt::ReturnQuery(Box::new(q)));
                }
                let e = if self.is_op(";") { None } else { Some(self.pl_expr_until(&[";"])?) };
                self.pl_end()?;
                return Ok(PlStmt::Return(e));
            }
            "perform" => {
                // `PERFORM expr FROM ...` is `SELECT expr FROM ...` with the result thrown away.
                self.pos += 1;
                let mut tokens = vec![Token::Word { text: "SELECT".into(), quoted: false }];
                tokens.extend(self.pl_slice(&[";"])?.tokens);
                let mut p = Parser::new(tokens, Dialect::Postgres);
                let q = p.query()?;
                self.pl_end()?;
                return Ok(PlStmt::Perform(Box::new(q)));
            }
            "raise" => {
                self.pos += 1;
                let level = self.peek().and_then(Token::keyword).unwrap_or_default();
                let exception = match level.as_str() {
                    "exception" => {
                        self.pos += 1;
                        true
                    }
                    "notice" | "warning" | "info" | "log" | "debug" => {
                        self.pos += 1;
                        false
                    }
                    _ => true,
                };
                let message = match self.next() {
                    Some(Token::Str(m)) => m,
                    _ => return Err(SqlError::unsupported("RAISE without a message string")),
                };
                let mut args = Vec::new();
                while self.eat_op(",") {
                    args.push(self.pl_expr_until(&[",", ";", "using"])?);
                }
                let mut code = None;
                if self.eat_kw("using") {
                    loop {
                        let opt = self.ident()?.to_ascii_lowercase();
                        self.expect_op("=")?;
                        let Some(Token::Str(v)) = self.next() else { return Err(self.unexpected("a string")) };
                        match opt.as_str() {
                            "errcode" => code = Some(v),
                            "detail" | "hint" | "message" => {}
                            other => return Err(SqlError::unsupported(format!("RAISE option {other}"))),
                        }
                        if !self.eat_op(",") {
                            break;
                        }
                    }
                }
                self.pl_end()?;
                return Ok(PlStmt::Raise { exception, message, args, code });
            }
            "execute" | "goto" | "case" | "open" | "fetch" | "close" | "foreach" | "get" | "assert" | "call" => {
                return Err(SqlError::unsupported(format!("{} in a function body", kw.to_ascii_uppercase())));
            }
            _ => {}
        }
        // `name := expr;` (also `name = expr;`).
        let start = self.pos;
        if matches!(self.peek(), Some(Token::Word { .. })) {
            if let Ok(target) = self.pl_target() {
                if self.eat_assign() {
                    let e = self.pl_expr_until(&[";"])?;
                    self.pl_end()?;
                    return Ok(PlStmt::Assign(target, e));
                }
            }
            self.pos = start;
        }
        // Any other SQL, with its `INTO` lifted out.
        let mut slice = self.pl_slice(&[";"])?.tokens;
        self.pl_end()?;
        let into = take_into(&mut slice)?;
        let mut p = Parser::new(slice, Dialect::Postgres);
        let st = p.statement()?;
        if !p.at_end() {
            return Err(p.unexpected("the end of the statement"));
        }
        Ok(PlStmt::Sql(st, into))
    }

    fn pl_loop_body(&mut self) -> SqlResult<Vec<PlStmt>> {
        self.expect_kw("loop")?;
        let body = self.pl_statements(&["end"])?;
        self.expect_kw("end")?;
        self.expect_kw("loop")?;
        self.pl_end()?;
        Ok(body)
    }

    /// `CREATE [OR REPLACE] FUNCTION | PROCEDURE ...`, after `FUNCTION` / `PROCEDURE`.
    pub(super) fn create_function(&mut self, or_replace: bool, is_procedure: bool) -> SqlResult<Statement> {
        if self.dialect != Dialect::Postgres {
            return Err(SqlError::unsupported("user functions and procedures outside the PostgreSQL dialect"));
        }
        let name = self.table_name()?.to_ascii_lowercase();
        self.expect_op("(")?;
        let mut args = Vec::new();
        while !self.is_op(")") {
            let mode = self.peek().and_then(Token::keyword);
            if matches!(mode.as_deref(), Some("out" | "inout" | "variadic")) {
                return Err(SqlError::unsupported("OUT, INOUT and VARIADIC arguments"));
            }
            let _ = self.eat_kw("in");
            // `name type`, or just `type`.
            let start = self.pos;
            let mut arg_name = String::new();
            if let Some(Token::Word { text, .. }) = self.peek().cloned() {
                self.pos += 1;
                if self.is_op(",") || self.is_op(")") || self.is_kw("default") || self.is_op("=") {
                    self.pos = start;
                } else if self.data_type().is_ok() {
                    self.pos = start + 1;
                    arg_name = text.to_ascii_lowercase();
                } else {
                    self.pos = start;
                }
            }
            let (ty, _) = self.data_type()?;
            let default = if self.eat_kw("default") || self.eat_op("=") { Some(self.expr()?) } else { None };
            args.push((arg_name, ty, default));
            if !self.eat_op(",") {
                break;
            }
        }
        self.expect_op(")")?;
        let mut returns = FuncReturn::Void;
        if self.eat_kw("returns") {
            returns = if self.eat_kw("trigger") {
                FuncReturn::Trigger
            } else if self.eat_kw("void") {
                FuncReturn::Void
            } else if self.eat_kw("setof") {
                FuncReturn::Set(self.data_type()?.0)
            } else if self.eat_kw("table") {
                self.expect_op("(")?;
                let mut cols = Vec::new();
                loop {
                    let n = self.ident()?;
                    cols.push((n, self.data_type()?.0));
                    if !self.eat_op(",") {
                        break;
                    }
                }
                self.expect_op(")")?;
                FuncReturn::Table(cols)
            } else {
                FuncReturn::Scalar(self.data_type()?.0)
            };
        }
        let (mut language, mut body) = (None, None);
        while !self.statement_end() {
            if self.eat_kw("language") {
                language = Some(self.ident().or_else(|_| match self.next() {
                    Some(Token::Str(s)) => Ok(s),
                    _ => Err(self.unexpected("a language")),
                })?.to_ascii_lowercase());
            } else if self.eat_kw("as") {
                match self.next() {
                    Some(Token::Str(s)) => body = Some(s),
                    _ => return Err(self.unexpected("the function body as a string")),
                }
            } else if self.eat_kws(&["security", "definer"]) {
                return Err(SqlError::unsupported("SECURITY DEFINER functions: they would run with the rights of the caller here"));
            } else if self.eat_kws(&["security", "invoker"]) || self.eat_kw("immutable") || self.eat_kw("stable") || self.eat_kw("volatile") || self.eat_kw("strict") || self.eat_kw("leakproof") || self.eat_kws(&["called", "on", "null", "input"]) || self.eat_kws(&["returns", "null", "on", "null", "input"]) || self.eat_kws(&["parallel", "safe"]) || self.eat_kws(&["parallel", "unsafe"]) || self.eat_kws(&["parallel", "restricted"]) {
            } else if self.eat_kw("cost") || self.eat_kw("rows") {
                self.pos += 1;
            } else {
                return Err(self.unexpected("a function option"));
            }
        }
        let language = language.ok_or_else(|| SqlError::new("42P13", "no language specified"))?;
        if language != "sql" && language != "plpgsql" {
            return Err(SqlError::unsupported(format!("LANGUAGE {language}")));
        }
        let body = body.ok_or_else(|| SqlError::new("42P13", "no function body specified"))?;
        // Refused now rather than at the first call.
        if language == "plpgsql" {
            parse_body(&body)?;
        } else {
            super::parse(&body, Dialect::Postgres)?;
        }
        Ok(Statement::CreateFunction(Box::new(CreateFunction { name, or_replace, args, returns, language, body, is_procedure })))
    }

    /// Skips `(types)` after a routine's name.
    pub(super) fn pl_arg_types(&mut self) -> SqlResult<()> {
        let mut depth = 0;
        while let Some(t) = self.peek() {
            if matches!(t, Token::Op("(")) {
                depth += 1;
            } else if matches!(t, Token::Op(")")) {
                depth -= 1;
            }
            self.pos += 1;
            if depth == 0 {
                break;
            }
        }
        Ok(())
    }

    /// `CREATE TRIGGER`, after `TRIGGER`.
    pub(super) fn create_trigger(&mut self, or_replace: bool) -> SqlResult<Statement> {
        if self.dialect != Dialect::Postgres {
            return Err(SqlError::unsupported("triggers outside the PostgreSQL dialect"));
        }
        let name = self.ident()?;
        let before = if self.eat_kw("before") {
            true
        } else if self.eat_kw("after") {
            false
        } else if self.eat_kws(&["instead", "of"]) {
            return Err(SqlError::unsupported("INSTEAD OF triggers"));
        } else {
            return Err(self.unexpected("BEFORE or AFTER"));
        };
        let mut events = Vec::new();
        loop {
            let e = self.peek().and_then(Token::keyword).unwrap_or_default();
            match e.as_str() {
                "insert" | "update" | "delete" => {
                    self.pos += 1;
                    if e == "update" && self.is_kw("of") {
                        return Err(SqlError::unsupported("UPDATE OF columns in a trigger"));
                    }
                    events.push(e);
                }
                "truncate" => return Err(SqlError::unsupported("TRUNCATE triggers")),
                _ => return Err(self.unexpected("INSERT, UPDATE or DELETE")),
            }
            if !self.eat_kw("or") {
                break;
            }
        }
        self.expect_kw("on")?;
        let table = self.table_name()?;
        if self.is_kw("from") || self.is_kw("deferrable") || self.is_kw("referencing") || self.is_kw("initially") || self.is_kw("not") {
            return Err(SqlError::unsupported("constraint triggers and transition tables"));
        }
        let mut for_each_row = false;
        if self.eat_kw("for") {
            let _ = self.eat_kw("each");
            for_each_row = if self.eat_kw("row") {
                true
            } else {
                self.expect_kw("statement")?;
                false
            };
        }
        let when = if self.eat_kw("when") {
            self.expect_op("(")?;
            let e = self.expr()?;
            self.expect_op(")")?;
            Some(e)
        } else {
            None
        };
        self.expect_kw("execute")?;
        if !(self.eat_kw("function") || self.eat_kw("procedure")) {
            return Err(self.unexpected("FUNCTION"));
        }
        let function = self.table_name()?.to_ascii_lowercase();
        self.expect_op("(")?;
        let mut args = Vec::new();
        while !self.is_op(")") {
            match self.next() {
                Some(Token::Str(s)) => args.push(s),
                Some(Token::Number(n)) => args.push(n),
                Some(Token::Word { text, .. }) => args.push(text),
                _ => return Err(self.unexpected("a trigger argument")),
            }
            if !self.eat_op(",") {
                break;
            }
        }
        self.expect_op(")")?;
        Ok(Statement::CreateTrigger(Box::new(CreateTrigger { name, or_replace, table, before, events, for_each_row, when, function, args })))
    }
}

/// Takes `INTO a, b` out of a statement's tokens (a top-level word, after the select list or
/// `RETURNING`, not `INSERT INTO`).
fn take_into(tokens: &mut Vec<Token>) -> SqlResult<Vec<String>> {
    let first = tokens.first().and_then(Token::keyword).unwrap_or_default();
    let mut depth = 0i32;
    let mut at = None;
    let mut returning = first == "select" || first == "with";
    for (i, t) in tokens.iter().enumerate() {
        match t {
            Token::Op("(") => depth += 1,
            Token::Op(")") => depth -= 1,
            other if depth == 0 => match other.keyword().as_deref() {
                Some("returning") => returning = true,
                Some("into") if returning => {
                    at = Some(i);
                    break;
                }
                _ => {}
            },
            _ => {}
        }
    }
    let Some(i) = at else { return Ok(Vec::new()) };
    let mut j = i + 1;
    let mut out = Vec::new();
    if matches!(tokens.get(j).and_then(Token::keyword).as_deref(), Some("strict")) {
        j += 1;
    }
    loop {
        let mut name = match tokens.get(j) {
            Some(Token::Word { text, .. }) => text.to_ascii_lowercase(),
            _ => return Err(SqlError::syntax("INTO takes variables".into())),
        };
        j += 1;
        while matches!(tokens.get(j), Some(Token::Op("."))) {
            let Some(Token::Word { text, .. }) = tokens.get(j + 1) else { return Err(SqlError::syntax("INTO takes variables".into())) };
            name.push('.');
            name.push_str(&text.to_ascii_lowercase());
            j += 2;
        }
        out.push(name);
        if matches!(tokens.get(j), Some(Token::Op(","))) {
            j += 1;
        } else {
            break;
        }
    }
    tokens.drain(i..j);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bodies_parse() {
        let b = parse_body(
            "DECLARE total int := 0; r record; BEGIN
               FOR i IN 1..10 LOOP total := total + i; END LOOP;
               FOR r IN SELECT id FROM t WHERE id > 1 LOOP total := total + r.id; END LOOP;
               IF total > 5 THEN total := 1; ELSIF total < 0 THEN total := 2; ELSE NULL; END IF;
               WHILE total < 3 LOOP total := total + 1; EXIT WHEN total = 2; END LOOP;
               SELECT count(*) INTO total FROM t;
               INSERT INTO t VALUES (1) RETURNING id INTO total;
               PERFORM f(1);
               RAISE EXCEPTION 'bad % thing', total USING ERRCODE = '22000';
               RETURN total;
             EXCEPTION WHEN division_by_zero OR SQLSTATE '22000' THEN RETURN -1; END",
        )
        .unwrap();
        assert_eq!(b.declarations.len(), 2);
        assert_eq!(b.body.len(), 9);
        assert_eq!(b.handlers.len(), 1);
        assert!(matches!(&b.body[0], PlStmt::ForRange { .. }));
        assert!(matches!(&b.body[4], PlStmt::Sql(Statement::Query(_), into) if into == &["total".to_owned()]));
        assert!(matches!(&b.body[5], PlStmt::Sql(Statement::Insert(_), into) if into == &["total".to_owned()]));
    }

    #[test]
    fn unsupported_constructs_are_named() {
        for bad in ["BEGIN EXECUTE 'x'; END", "BEGIN CASE WHEN true THEN NULL; END CASE; END", "BEGIN FOREACH x IN ARRAY a LOOP NULL; END LOOP; END"] {
            let e = parse_body(bad).unwrap_err();
            assert_eq!(e.code, "0A000", "{bad}: {e}");
        }
        assert!(parse_body("BEGIN").is_err());
        assert!(parse_body("BEGIN IF true THEN NULL; END; END").is_err());
    }
}
