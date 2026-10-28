//! Recursive-descent SQL parser for the PostgreSQL, MySQL, SQLite and SQL Server dialects.

use crate::ast::*;
use crate::error::{SqlError, SqlResult};
use crate::lexer::{tokenize, Dialect, Token};

/// Words that end an expression or a table reference, so they cannot be bare aliases.
const RESERVED: &[&str] = &[
    "select", "from", "where", "group", "order", "having", "limit", "offset", "union", "intersect", "except", "join",
    "inner", "left", "right", "full", "cross", "natural", "on", "using", "and", "or", "not", "as", "into", "values", "set",
    "returning", "when", "then", "else", "end", "is", "in", "between", "like", "ilike", "case", "null", "true", "false",
    "for", "fetch", "window", "with", "lock", "outer", "default", "do", "collate", "escape", "asc", "desc", "nulls",
    "rlike", "regexp", "xor", "div", "mod", "straight_join", "lateral", "only", "top", "returning",
];

/// Parses a string that may hold several `;`-separated statements.
pub fn parse(sql: &str, dialect: Dialect) -> SqlResult<(Vec<Statement>, usize)> {
    let (tokens, params) = tokenize(sql, dialect)?;
    let mut p = Parser { tokens, pos: 0, dialect };
    let mut out = Vec::new();
    loop {
        while p.eat_op(";") {}
        if p.at_end() {
            break;
        }
        out.push(p.statement()?);
        if !p.at_end() && !p.eat_op(";") {
            return Err(p.unexpected("end of statement"));
        }
    }
    Ok((out, params.count))
}

/// Parses exactly one statement (an empty string gives `Statement::Empty`).
pub fn parse_one(sql: &str, dialect: Dialect) -> SqlResult<(Statement, usize)> {
    let (mut stmts, n) = parse(sql, dialect)?;
    match stmts.len() {
        0 => Ok((Statement::Empty, n)),
        1 => Ok((stmts.remove(0), n)),
        _ => Err(SqlError::syntax("cannot insert multiple commands into a prepared statement".into())),
    }
}

/// Parses one expression (stored defaults and checks).
pub fn parse_expr(sql: &str, dialect: Dialect) -> SqlResult<Expr> {
    let (tokens, _) = tokenize(sql, dialect)?;
    let mut p = Parser { tokens, pos: 0, dialect };
    let e = p.expr()?;
    if !p.at_end() {
        return Err(p.unexpected("end of expression"));
    }
    Ok(e)
}

/// Parses a column type name such as `numeric(12,2)` or `timestamp with time zone`.
pub fn parse_data_type(sql: &str) -> SqlResult<DataType> {
    let (tokens, _) = tokenize(sql, Dialect::Postgres)?;
    let mut p = Parser { tokens, pos: 0, dialect: Dialect::Postgres };
    let (ty, _) = p.data_type()?;
    if !p.at_end() {
        return Err(p.unexpected("end of type"));
    }
    Ok(ty)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    dialect: Dialect,
}

impl Parser {
    // ── token helpers ──

    fn at_end(&self) -> bool {
        self.pos >= self.tokens.len()
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn peek_at(&self, n: usize) -> Option<&Token> {
        self.tokens.get(self.pos + n)
    }

    fn next(&mut self) -> Option<Token> {
        let t = self.tokens.get(self.pos).cloned();
        self.pos += 1;
        t
    }

    fn unexpected(&self, wanted: &str) -> SqlError {
        let found = match self.peek() {
            None => "end of input".to_owned(),
            Some(Token::Word { text, .. }) => format!("\"{text}\""),
            Some(Token::Op(o)) => format!("\"{o}\""),
            Some(Token::Number(n)) => n.clone(),
            Some(Token::Str(s)) => format!("'{s}'"),
            Some(t) => format!("{t:?}"),
        };
        SqlError::syntax(format!("syntax error at or near {found}: expected {wanted}"))
    }

    fn is_kw(&self, kw: &str) -> bool {
        self.is_kw_at(0, kw)
    }

    fn is_kw_at(&self, n: usize, kw: &str) -> bool {
        matches!(self.peek_at(n), Some(Token::Word { text, quoted: false }) if text.eq_ignore_ascii_case(kw))
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        if self.is_kw(kw) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn eat_kws(&mut self, kws: &[&str]) -> bool {
        if kws.iter().enumerate().all(|(i, k)| self.is_kw_at(i, k)) {
            self.pos += kws.len();
            true
        } else {
            false
        }
    }

    fn expect_kw(&mut self, kw: &str) -> SqlResult<()> {
        if self.eat_kw(kw) {
            Ok(())
        } else {
            Err(self.unexpected(&kw.to_ascii_uppercase()))
        }
    }

    fn is_op(&self, op: &str) -> bool {
        matches!(self.peek(), Some(Token::Op(o)) if *o == op)
    }

    fn eat_op(&mut self, op: &str) -> bool {
        if self.is_op(op) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect_op(&mut self, op: &str) -> SqlResult<()> {
        if self.eat_op(op) {
            Ok(())
        } else {
            Err(self.unexpected(&format!("\"{op}\"")))
        }
    }

    /// Folds an unquoted identifier the way the dialect does (PostgreSQL: lower case).
    fn fold(&self, text: String, quoted: bool) -> String {
        if !quoted && self.dialect == Dialect::Postgres {
            text.to_ascii_lowercase()
        } else {
            text
        }
    }

    /// Any identifier, keywords included (used where the grammar expects a name).
    fn ident(&mut self) -> SqlResult<String> {
        match self.next() {
            Some(Token::Word { text, quoted }) => Ok(self.fold(text, quoted)),
            Some(Token::Str(s)) if self.dialect == Dialect::MySql => Ok(s),
            _ => {
                self.pos -= 1;
                Err(self.unexpected("a name"))
            }
        }
    }

    /// An identifier that may serve as a bare alias (not a reserved word).
    fn alias_ident(&mut self) -> Option<String> {
        match self.peek() {
            Some(Token::Word { text, quoted }) => {
                if !*quoted && RESERVED.contains(&text.to_ascii_lowercase().as_str()) {
                    return None;
                }
                let (t, q) = (text.clone(), *quoted);
                self.pos += 1;
                Some(self.fold(t, q))
            }
            Some(Token::Str(s)) if self.dialect == Dialect::MySql => {
                let s = s.clone();
                self.pos += 1;
                Some(s)
            }
            _ => None,
        }
    }

    /// `[AS] alias`.
    fn opt_alias(&mut self) -> SqlResult<Option<String>> {
        if self.eat_kw("as") {
            return Ok(Some(self.ident()?));
        }
        Ok(self.alias_ident())
    }

    /// A possibly qualified name `a.b.c`; returns `(schema, name)`.
    fn object_name(&mut self) -> SqlResult<(Option<String>, String)> {
        let mut parts = vec![self.ident()?];
        while self.eat_op(".") {
            parts.push(self.ident()?);
        }
        let name = parts.pop().unwrap_or_default();
        Ok((parts.pop(), name))
    }

    fn table_name(&mut self) -> SqlResult<String> {
        Ok(self.object_name()?.1)
    }

    fn ident_list(&mut self) -> SqlResult<Vec<String>> {
        self.expect_op("(")?;
        let mut out = Vec::new();
        loop {
            out.push(self.ident()?);
            // MySQL index prefix lengths `col(10)` and ASC/DESC in key definitions.
            if self.is_op("(") && matches!(self.peek_at(1), Some(Token::Number(_))) {
                self.pos += 3;
            }
            let _ = self.eat_kw("asc") || self.eat_kw("desc");
            if !self.eat_op(",") {
                break;
            }
        }
        self.expect_op(")")?;
        Ok(out)
    }

    /// Skips tokens up to the end of the statement (table options we ignore).
    fn skip_to_end(&mut self) {
        let mut depth = 0i32;
        while let Some(t) = self.peek() {
            match t {
                Token::Op("(") => depth += 1,
                Token::Op(")") => depth -= 1,
                Token::Op(";") if depth <= 0 => return,
                _ => {}
            }
            self.pos += 1;
        }
    }

    // ── statements ──

    fn statement(&mut self) -> SqlResult<Statement> {
        let kw = match self.peek() {
            Some(t) => t.keyword().unwrap_or_default(),
            None => return Ok(Statement::Empty),
        };
        match kw.as_str() {
            "select" | "with" | "values" | "table" => Ok(Statement::Query(Box::new(self.query()?))),
            "" if self.is_op("(") => Ok(Statement::Query(Box::new(self.query()?))),
            "insert" | "replace" => self.insert(),
            "update" => self.update(),
            "delete" => self.delete(),
            "create" => self.create(),
            "drop" => self.drop(),
            "alter" => self.alter(),
            "truncate" => {
                self.pos += 1;
                self.eat_kw("table");
                let mut tables = vec![self.table_name()?];
                while self.eat_op(",") {
                    tables.push(self.table_name()?);
                }
                self.skip_to_end();
                Ok(Statement::Truncate { tables })
            }
            "begin" | "start" => {
                self.pos += 1;
                if kw == "start" {
                    self.expect_kw("transaction")?;
                } else {
                    let _ = self.eat_kw("transaction") || self.eat_kw("work") || self.eat_kw("tran");
                }
                self.skip_to_end(); // isolation level / read write / deferrable
                Ok(Statement::Begin)
            }
            "commit" | "end" => {
                self.pos += 1;
                self.skip_to_end();
                Ok(Statement::Commit)
            }
            "rollback" | "abort" => {
                self.pos += 1;
                let _ = self.eat_kw("work") || self.eat_kw("transaction") || self.eat_kw("tran");
                if self.eat_kw("to") {
                    self.eat_kw("savepoint");
                    return Ok(Statement::RollbackToSavepoint(self.ident()?));
                }
                self.skip_to_end();
                Ok(Statement::Rollback)
            }
            "savepoint" => {
                self.pos += 1;
                Ok(Statement::Savepoint(self.ident()?))
            }
            "release" => {
                self.pos += 1;
                self.eat_kw("savepoint");
                Ok(Statement::ReleaseSavepoint(self.ident()?))
            }
            "set" => self.set(),
            "show" => self.show(),
            "use" => {
                self.pos += 1;
                Ok(Statement::Use(self.ident()?))
            }
            "explain" | "describe" | "desc" => {
                self.pos += 1;
                if (kw == "describe" || kw == "desc") && !self.is_kw("select") {
                    let table = self.table_name()?;
                    return Ok(Statement::Show(Show::Columns { table, full: false }));
                }
                let _ = self.eat_kw("analyze");
                let _ = self.eat_kw("verbose");
                Ok(Statement::Explain(Box::new(self.statement()?)))
            }
            "prepare" => {
                self.pos += 1;
                let name = self.ident()?;
                if self.is_op("(") {
                    let mut depth = 0;
                    while let Some(t) = self.next() {
                        match t {
                            Token::Op("(") => depth += 1,
                            Token::Op(")") => {
                                depth -= 1;
                                if depth == 0 {
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                if !self.eat_kw("as") {
                    self.expect_kw("from")?;
                }
                if let Some(Token::Str(s)) = self.peek().cloned() {
                    self.pos += 1;
                    let (stmt, _) = parse_one(&s, self.dialect)?;
                    return Ok(Statement::Prepare { name, statement: Box::new(stmt) });
                }
                Ok(Statement::Prepare { name, statement: Box::new(self.statement()?) })
            }
            "execute" | "exec" => {
                self.pos += 1;
                let name = self.ident()?;
                let mut args = Vec::new();
                if self.eat_op("(") {
                    loop {
                        args.push(self.expr()?);
                        if !self.eat_op(",") {
                            break;
                        }
                    }
                    self.expect_op(")")?;
                } else if self.eat_kw("using") {
                    loop {
                        args.push(self.expr()?);
                        if !self.eat_op(",") {
                            break;
                        }
                    }
                }
                Ok(Statement::Execute { name, args })
            }
            "deallocate" => {
                self.pos += 1;
                self.eat_kw("prepare");
                if self.eat_kw("all") {
                    return Ok(Statement::Deallocate(None));
                }
                Ok(Statement::Deallocate(Some(self.ident()?)))
            }
            "discard" | "listen" | "unlisten" | "notify" | "reset" | "lock" | "unlock" | "analyze" | "vacuum"
            | "checkpoint" | "flush" | "pragma" | "comment" | "grant" | "revoke" | "optimize" | "kill" | "load" => {
                self.pos += 1;
                self.skip_to_end();
                Ok(Statement::NoOp(static_tag(&kw)))
            }
            _ => Err(SqlError::unsupported(format!("statement starting with {}", match self.peek() {
                Some(Token::Word { text, .. }) => text.to_ascii_uppercase(),
                _ => "this token".to_owned(),
            }))),
        }
    }

    fn set(&mut self) -> SqlResult<Statement> {
        self.pos += 1; // SET
        if self.is_kw("transaction") || self.eat_kws(&["session", "characteristics"])
            || ((self.is_kw("session") || self.is_kw("global") || self.is_kw("local")) && self.is_kw_at(1, "transaction"))
        {
            self.skip_to_end();
            return Ok(Statement::NoOp("SET"));
        }
        if self.eat_kw("names") || self.eat_kws(&["character", "set"]) || self.eat_kw("charset") {
            let v = self.next();
            self.skip_to_end();
            let value = match v {
                Some(Token::Word { text, .. }) | Some(Token::Str(text)) => Some(Expr::Str(text)),
                _ => None,
            };
            return Ok(Statement::Set { name: "names".into(), value });
        }
        let _ = self.eat_kw("session") || self.eat_kw("local") || self.eat_kw("global") || self.eat_kw("persist");
        if self.eat_kws(&["time", "zone"]) {
            let value = if self.eat_kw("local") || self.eat_kw("default") { None } else { Some(self.expr()?) };
            return Ok(Statement::Set { name: "timezone".into(), value });
        }
        // SET a = 1, b = 2 (MySQL) — keep the first; SET x TO a, b (search_path).
        let name = match self.next() {
            Some(Token::Variable { name, system }) => {
                if system {
                    name.trim_start_matches("session.").trim_start_matches("global.").to_ascii_lowercase()
                } else {
                    format!("@{name}")
                }
            }
            Some(Token::Word { text, quoted }) => {
                let mut n = self.fold(text, quoted);
                while self.eat_op(".") {
                    n.push('.');
                    n.push_str(&self.ident()?);
                }
                n.to_ascii_lowercase()
            }
            _ => return Err(self.unexpected("a setting name")),
        };
        if !self.eat_op("=") && !self.eat_kw("to") {
            if self.dialect == Dialect::MsSql {
                // SET NOCOUNT ON, SET ANSI_NULLS ON ...
                self.skip_to_end();
                return Ok(Statement::NoOp("SET"));
            }
            return Err(self.unexpected("= or TO"));
        }
        let value = if self.eat_kw("default") {
            None
        } else {
            let first = self.set_value()?;
            if self.is_op(",") && name == "search_path" {
                let mut parts = vec![first];
                while self.eat_op(",") {
                    parts.push(self.set_value()?);
                }
                Some(Expr::Str(
                    parts.iter().map(|e| match e {
                        Expr::Str(s) => s.clone(),
                        Expr::Column(p) => p.join("."),
                        _ => String::new(),
                    }).collect::<Vec<_>>().join(", "),
                ))
            } else {
                Some(first)
            }
        };
        self.skip_to_end();
        Ok(Statement::Set { name, value })
    }

    /// A setting value: an expression, or a bare word such as `ON` / `utf8`.
    fn set_value(&mut self) -> SqlResult<Expr> {
        if let Some(Token::Word { text, quoted: false }) = self.peek() {
            let lower = text.to_ascii_lowercase();
            if matches!(self.peek_at(1), None | Some(Token::Op(",")) | Some(Token::Op(";"))) && lower != "null" {
                let t = text.clone();
                self.pos += 1;
                return Ok(match lower.as_str() {
                    "true" | "on" => Expr::Str(if lower == "on" { "on".into() } else { "true".into() }),
                    "false" | "off" => Expr::Str(lower),
                    _ => Expr::Str(t),
                });
            }
        }
        self.expr()
    }

    fn show(&mut self) -> SqlResult<Statement> {
        self.pos += 1;
        let full = self.eat_kw("full");
        let _ = self.eat_kw("global") || self.eat_kw("session");
        let like = |p: &mut Parser| -> SqlResult<Option<String>> {
            if p.eat_kw("like") {
                if let Some(Token::Str(s)) = p.next() {
                    return Ok(Some(s));
                }
                return Err(p.unexpected("a pattern"));
            }
            if p.eat_kw("where") {
                let _ = p.expr()?;
            }
            Ok(None)
        };
        let show = if self.eat_kw("tables") {
            if self.eat_kw("from") || self.eat_kw("in") {
                self.ident()?;
            }
            Show::Tables { full, like: like(self)? }
        } else if self.eat_kw("columns") || self.eat_kw("fields") {
            let _ = self.eat_kw("from") || self.eat_kw("in");
            let table = self.table_name()?;
            if self.eat_kw("from") || self.eat_kw("in") {
                self.ident()?;
            }
            like(self)?;
            Show::Columns { table, full }
        } else if self.eat_kw("index") || self.eat_kw("indexes") || self.eat_kw("keys") {
            let _ = self.eat_kw("from") || self.eat_kw("in");
            let table = self.table_name()?;
            self.skip_to_end();
            Show::Index { table }
        } else if self.eat_kw("databases") || self.eat_kw("schemas") {
            like(self)?;
            Show::Databases
        } else if self.eat_kws(&["create", "table"]) {
            Show::CreateTable(self.table_name()?)
        } else if self.eat_kw("variables") {
            Show::Variables { like: like(self)?, global: false }
        } else if self.eat_kw("status") {
            like(self)?;
            Show::Status
        } else if self.eat_kw("warnings") {
            self.skip_to_end();
            Show::Warnings
        } else if self.eat_kw("errors") {
            self.skip_to_end();
            Show::Errors
        } else if self.eat_kw("collation") {
            like(self)?;
            Show::Collation
        } else if self.eat_kws(&["character", "set"]) || self.eat_kw("charset") {
            like(self)?;
            Show::CharacterSet
        } else if self.eat_kw("engines") {
            Show::Engines
        } else if self.eat_kw("processlist") {
            Show::ProcessList
        } else if self.eat_kws(&["table", "status"]) {
            if self.eat_kw("from") || self.eat_kw("in") {
                self.ident()?;
            }
            Show::TableStatus { like: like(self)? }
        } else if self.eat_kws(&["transaction", "isolation", "level"]) {
            Show::Variable("transaction_isolation".into())
        } else {
            let mut name = self.ident()?.to_ascii_lowercase();
            while self.eat_op(".") {
                name.push('.');
                name.push_str(&self.ident()?.to_ascii_lowercase());
            }
            if name == "time" && self.eat_kw("zone") {
                name = "timezone".into();
            }
            Show::Variable(name)
        };
        Ok(Statement::Show(show))
    }

    fn create(&mut self) -> SqlResult<Statement> {
        self.pos += 1; // CREATE
        let _ = self.eat_kws(&["or", "replace"]);
        let _ = self.eat_kw("temporary") || self.eat_kw("temp") || self.eat_kw("unlogged");
        if self.eat_kw("table") {
            return self.create_table();
        }
        let unique = self.eat_kw("unique");
        let _ = self.eat_kw("clustered") || self.eat_kw("nonclustered");
        if self.eat_kw("index") {
            let _ = self.eat_kw("concurrently");
            let if_not_exists = self.eat_kws(&["if", "not", "exists"]);
            let name = if self.is_kw("on") { None } else { Some(self.ident()?) };
            self.expect_kw("on")?;
            let _ = self.eat_kw("only");
            let table = self.table_name()?;
            if self.eat_kw("using") {
                self.ident()?;
            }
            let mut columns = Vec::new();
            self.expect_op("(")?;
            loop {
                // Expression indexes (lower(email)) index the column they wrap.
                let e = self.expr()?;
                match e {
                    Expr::Column(p) => columns.push(p.last().cloned().unwrap_or_default()),
                    Expr::Function { args, .. } if matches!(args.first(), Some(Expr::Column(_))) => {
                        columns.push(args[0].column_name().unwrap_or_default().to_owned())
                    }
                    _ => return Err(SqlError::unsupported("index on an expression")),
                }
                let _ = self.eat_kw("asc") || self.eat_kw("desc");
                if self.eat_kw("nulls") {
                    let _ = self.eat_kw("first") || self.eat_kw("last");
                }
                if !self.eat_op(",") {
                    break;
                }
            }
            self.expect_op(")")?;
            self.skip_to_end(); // INCLUDE / WHERE (partial) / WITH options
            return Ok(Statement::CreateIndex(Box::new(CreateIndex { name, table, columns, unique, if_not_exists })));
        }
        if self.eat_kw("schema") || self.eat_kw("database") {
            let if_not_exists = self.eat_kws(&["if", "not", "exists"]);
            let name = self.ident()?;
            self.skip_to_end();
            return Ok(Statement::CreateSchema { name, if_not_exists });
        }
        if self.eat_kw("extension") || self.eat_kw("sequence") {
            self.skip_to_end();
            return Ok(Statement::NoOp("CREATE"));
        }
        Err(SqlError::unsupported(format!(
            "CREATE {}",
            match self.peek() {
                Some(Token::Word { text, .. }) => text.to_ascii_uppercase(),
                _ => "...".into(),
            }
        )))
    }

    fn create_table(&mut self) -> SqlResult<Statement> {
        let if_not_exists = self.eat_kws(&["if", "not", "exists"]);
        let name = self.table_name()?;
        let mut ct = CreateTable { name, if_not_exists, columns: Vec::new(), constraints: Vec::new(), as_query: None };
        if self.eat_kw("as") {
            ct.as_query = Some(Box::new(self.query()?));
            return Ok(Statement::CreateTable(Box::new(ct)));
        }
        self.expect_op("(")?;
        loop {
            if self.is_kw("constraint")
                || self.is_kw("primary")
                || self.is_kw("foreign")
                || self.is_kw("check")
                || (self.is_kw("unique") && matches!(self.peek_at(1), Some(Token::Op("(")) | Some(Token::Word { .. })) && !self.is_column_def_ahead())
                || ((self.is_kw("key") || self.is_kw("index") || self.is_kw("fulltext") || self.is_kw("spatial")) && !self.is_column_def_ahead())
            {
                ct.constraints.push(self.table_constraint()?);
            } else {
                ct.columns.push(self.column_def()?);
            }
            if !self.eat_op(",") {
                break;
            }
        }
        self.expect_op(")")?;
        self.skip_to_end(); // ENGINE=..., WITHOUT ROWID, WITH (...), INHERITS ...
        Ok(Statement::CreateTable(Box::new(ct)))
    }

    /// Whether the current word starts a column definition (`key text`) rather than a key clause.
    fn is_column_def_ahead(&self) -> bool {
        // A column definition is `name type ...`: the next token is a type word, not `(` or KEY.
        match self.peek_at(1) {
            Some(Token::Word { text, quoted: false }) => {
                let t = text.to_ascii_lowercase();
                !matches!(t.as_str(), "key" | "index") && type_word(&t)
            }
            Some(Token::Word { quoted: true, .. }) => false,
            _ => false,
        }
    }

    fn table_constraint(&mut self) -> SqlResult<TableConstraint> {
        let name = if self.eat_kw("constraint") { Some(self.ident()?) } else { None };
        if self.eat_kws(&["primary", "key"]) {
            let _ = self.eat_kw("clustered") || self.eat_kw("nonclustered");
            let columns = self.ident_list()?;
            self.constraint_tail();
            return Ok(TableConstraint::PrimaryKey { name, columns });
        }
        if self.eat_kw("unique") {
            let _ = self.eat_kw("key") || self.eat_kw("index");
            let name = if !self.is_op("(") { Some(self.ident()?) } else { name };
            let columns = self.ident_list()?;
            self.constraint_tail();
            return Ok(TableConstraint::Unique { name, columns });
        }
        if self.eat_kws(&["foreign", "key"]) {
            if !self.is_op("(") {
                self.ident()?;
            }
            let columns = self.ident_list()?;
            let mut fk = self.references()?;
            fk.columns = columns;
            return Ok(TableConstraint::ForeignKey { name, fk });
        }
        if self.eat_kw("check") {
            self.expect_op("(")?;
            let expr = self.expr()?;
            self.expect_op(")")?;
            return Ok(TableConstraint::Check { name, expr });
        }
        let _ = self.eat_kw("fulltext") || self.eat_kw("spatial");
        if self.eat_kw("key") || self.eat_kw("index") {
            let name = if !self.is_op("(") { Some(self.ident()?) } else { name };
            let columns = self.ident_list()?;
            self.constraint_tail();
            return Ok(TableConstraint::Index { name, columns });
        }
        Err(self.unexpected("a table constraint"))
    }

    /// Skips `USING BTREE`, `DEFERRABLE INITIALLY DEFERRED`, `COMMENT '..'` after a key.
    fn constraint_tail(&mut self) {
        loop {
            if self.eat_kw("using") || self.eat_kw("initially") || self.eat_kw("comment") {
                let _ = self.next();
            } else if !(self.eat_kw("deferrable") || self.eat_kws(&["not", "deferrable"])) {
                return;
            }
        }
    }

    fn references(&mut self) -> SqlResult<ForeignKey> {
        self.expect_kw("references")?;
        let table = self.table_name()?;
        let ref_columns = if self.is_op("(") { self.ident_list()? } else { Vec::new() };
        let mut fk = ForeignKey {
            columns: Vec::new(),
            table,
            ref_columns,
            on_delete: RefAction::NoAction,
            on_update: RefAction::NoAction,
        };
        loop {
            if self.eat_kw("match") {
                let _ = self.next();
            } else if self.eat_kw("on") {
                let delete = if self.eat_kw("delete") {
                    true
                } else {
                    self.expect_kw("update")?;
                    false
                };
                let action = if self.eat_kw("cascade") {
                    RefAction::Cascade
                } else if self.eat_kw("restrict") {
                    RefAction::Restrict
                } else if self.eat_kws(&["set", "null"]) {
                    RefAction::SetNull
                } else if self.eat_kws(&["set", "default"]) {
                    RefAction::SetDefault
                } else if self.eat_kws(&["no", "action"]) {
                    RefAction::NoAction
                } else {
                    return Err(self.unexpected("a referential action"));
                };
                if delete {
                    fk.on_delete = action;
                } else {
                    fk.on_update = action;
                }
            } else if self.eat_kw("deferrable") || self.eat_kws(&["not", "deferrable"]) {
            } else if self.eat_kw("initially") {
                let _ = self.next();
            } else {
                return Ok(fk);
            }
        }
    }

    fn column_def(&mut self) -> SqlResult<ColumnDef> {
        let name = self.ident()?;
        let (ty, serial) = self.data_type()?;
        let mut col = ColumnDef {
            name,
            ty,
            not_null: serial,
            default: None,
            primary_key: false,
            unique: false,
            auto_increment: serial,
            references: None,
            check: None,
        };
        loop {
            if self.eat_kw("constraint") {
                self.ident()?;
            } else if self.eat_kws(&["not", "null"]) {
                col.not_null = true;
            } else if self.eat_kw("null") {
                col.not_null = false;
            } else if self.eat_kw("default") {
                col.default = Some(self.unary()?);
            } else if self.eat_kws(&["primary", "key"]) {
                let _ = self.eat_kw("asc") || self.eat_kw("desc");
                let _ = self.eat_kw("clustered") || self.eat_kw("nonclustered");
                col.primary_key = true;
                col.not_null = true;
            } else if self.eat_kw("unique") {
                self.eat_kw("key");
                col.unique = true;
            } else if self.eat_kw("auto_increment") || self.eat_kw("autoincrement") {
                col.auto_increment = true;
            } else if self.eat_kw("identity") {
                if self.is_op("(") {
                    self.ident_or_numbers_paren()?;
                }
                col.auto_increment = true;
                col.not_null = true;
            } else if self.eat_kw("generated") {
                let _ = self.eat_kw("always") || self.eat_kws(&["by", "default"]);
                self.expect_kw("as")?;
                if self.eat_kw("identity") {
                    if self.is_op("(") {
                        self.ident_or_numbers_paren()?;
                    }
                    col.auto_increment = true;
                    col.not_null = true;
                } else {
                    return Err(SqlError::unsupported("generated (computed) columns"));
                }
            } else if self.is_kw("references") {
                let mut fk = self.references()?;
                fk.columns = vec![col.name.clone()];
                col.references = Some(fk);
            } else if self.eat_kw("check") {
                self.expect_op("(")?;
                col.check = Some(self.expr()?);
                self.expect_op(")")?;
            } else if self.eat_kw("collate") {
                self.ident()?;
            } else if self.eat_kw("comment") {
                let _ = self.next();
            } else if self.eat_kws(&["character", "set"]) || self.eat_kw("charset") {
                self.ident()?;
            } else if self.eat_kws(&["on", "update"]) {
                // MySQL `ON UPDATE CURRENT_TIMESTAMP`: accepted (ORMs set the value explicitly).
                self.unary()?;
            } else if self.eat_kw("unsigned") || self.eat_kw("signed") || self.eat_kw("zerofill") {
            } else {
                return Ok(col);
            }
        }
    }

    fn ident_or_numbers_paren(&mut self) -> SqlResult<()> {
        self.expect_op("(")?;
        let mut depth = 1;
        while depth > 0 {
            match self.next() {
                Some(Token::Op("(")) => depth += 1,
                Some(Token::Op(")")) => depth -= 1,
                None => return Err(self.unexpected(")")),
                _ => {}
            }
        }
        Ok(())
    }

    /// A column type; the flag is true for serial types (auto-increment).
    fn data_type(&mut self) -> SqlResult<(DataType, bool)> {
        let word = match self.next() {
            Some(Token::Word { text, .. }) => text.to_ascii_lowercase(),
            _ => {
                self.pos -= 1;
                return Err(self.unexpected("a type name"));
            }
        };
        let paren_numbers = |p: &mut Parser| -> SqlResult<Vec<u32>> {
            let mut out = Vec::new();
            if p.eat_op("(") {
                loop {
                    match p.next() {
                        Some(Token::Number(n)) => out.push(n.parse().map_err(|_| SqlError::syntax("bad type size".into()))?),
                        Some(Token::Word { text, .. }) if text.eq_ignore_ascii_case("max") => out.push(u32::MAX),
                        _ => return Err(p.unexpected("a size")),
                    }
                    if !p.eat_op(",") {
                        break;
                    }
                }
                p.expect_op(")")?;
            }
            Ok(out)
        };
        let mut serial = false;
        let ty = match word.as_str() {
            "bool" | "boolean" => DataType::Bool,
            "bit" => {
                let n = paren_numbers(self)?;
                if n.first().copied().unwrap_or(1) <= 1 {
                    DataType::Bool
                } else {
                    DataType::Bytes
                }
            }
            "tinyint" => {
                let n = paren_numbers(self)?;
                if self.dialect == Dialect::MySql && n.first() == Some(&1) {
                    DataType::Bool
                } else {
                    DataType::SmallInt
                }
            }
            "smallint" | "int2" | "year" => {
                paren_numbers(self)?;
                DataType::SmallInt
            }
            "mediumint" | "int" | "integer" | "int4" => {
                paren_numbers(self)?;
                DataType::Int
            }
            "bigint" | "int8" => {
                paren_numbers(self)?;
                DataType::BigInt
            }
            "smallserial" | "serial2" => {
                serial = true;
                DataType::SmallInt
            }
            "serial" | "serial4" => {
                serial = true;
                // MySQL SERIAL is BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE.
                if self.dialect == Dialect::MySql {
                    DataType::BigInt
                } else {
                    DataType::Int
                }
            }
            "bigserial" | "serial8" => {
                serial = true;
                DataType::BigInt
            }
            "decimal" | "numeric" | "dec" | "fixed" | "number" => {
                let n = paren_numbers(self)?;
                match n.as_slice() {
                    [] => DataType::Numeric(None),
                    [p] => DataType::Numeric(Some((clamp_u8(*p), 0))),
                    [p, s, ..] => {
                        if *s > 38 || *s > *p {
                            return Err(SqlError::syntax(format!("NUMERIC scale {s} must be between 0 and precision {p} (at most 38)")));
                        }
                        DataType::Numeric(Some((clamp_u8(*p), clamp_u8(*s))))
                    }
                }
            }
            "money" | "smallmoney" => DataType::Numeric(Some((19, if word == "money" && self.dialect == Dialect::MsSql { 4 } else { 2 }))),
            "real" | "float4" => DataType::Real,
            "float" => {
                let n = paren_numbers(self)?;
                if n.first().is_some_and(|p| *p <= 24) {
                    DataType::Real
                } else {
                    DataType::Double
                }
            }
            "double" => {
                self.eat_kw("precision");
                paren_numbers(self)?;
                DataType::Double
            }
            "float8" => DataType::Double,
            "text" | "tinytext" | "mediumtext" | "longtext" | "ntext" | "clob" | "citext" | "string" | "name" => DataType::Text,
            "varchar" | "nvarchar" | "varchar2" | "nvarchar2" => {
                let n = paren_numbers(self)?;
                DataType::Varchar(n.first().copied().filter(|n| *n != u32::MAX))
            }
            "character" | "char" | "nchar" => {
                if self.eat_kw("varying") {
                    let n = paren_numbers(self)?;
                    DataType::Varchar(n.first().copied())
                } else {
                    let n = paren_numbers(self)?;
                    DataType::Char(Some(n.first().copied().unwrap_or(1)))
                }
            }
            "bytea" | "blob" | "tinyblob" | "mediumblob" | "longblob" | "binary" | "varbinary" | "image" => {
                let _ = self.eat_kw("varying");
                paren_numbers(self)?;
                DataType::Bytes
            }
            "timestamp" | "datetime" | "datetime2" | "smalldatetime" => {
                paren_numbers(self)?;
                if self.eat_kws(&["with", "time", "zone"]) {
                    DataType::TimestampTz
                } else {
                    let _ = self.eat_kws(&["without", "time", "zone"]);
                    DataType::Timestamp
                }
            }
            "timestamptz" | "datetimeoffset" => {
                paren_numbers(self)?;
                DataType::TimestampTz
            }
            "date" => DataType::Date,
            "time" | "timetz" => {
                paren_numbers(self)?;
                let _ = self.eat_kws(&["with", "time", "zone"]) || self.eat_kws(&["without", "time", "zone"]);
                DataType::Time
            }
            "interval" => {
                // INTERVAL [fields] [(p)]
                while matches!(self.peek(), Some(Token::Word { text, quoted: false }) if matches!(text.to_ascii_lowercase().as_str(), "year" | "month" | "day" | "hour" | "minute" | "second" | "to"))
                {
                    self.pos += 1;
                }
                paren_numbers(self)?;
                DataType::Interval
            }
            "uuid" | "uniqueidentifier" => DataType::Uuid,
            "json" | "jsonb" => DataType::Json,
            "enum" => {
                self.expect_op("(")?;
                let mut values = Vec::new();
                loop {
                    match self.next() {
                        Some(Token::Str(s)) => values.push(s),
                        _ => return Err(self.unexpected("an ENUM value")),
                    }
                    if !self.eat_op(",") {
                        break;
                    }
                }
                self.expect_op(")")?;
                DataType::Enum(values)
            }
            "set" | "geometry" | "point" | "polygon" | "xml" | "tsvector" | "hstore" | "inet" | "cidr" | "macaddr" => {
                return Err(SqlError::unsupported(format!("column type {}", word.to_ascii_uppercase())))
            }
            other => return Err(SqlError::unsupported(format!("column type {}", other.to_ascii_uppercase()))),
        };
        // PostgreSQL arrays: text[] / integer ARRAY.
        if self.is_op("[") {
            while self.eat_op("[") {
                if let Some(Token::Number(_)) = self.peek() {
                    self.pos += 1;
                }
                self.expect_op("]")?;
            }
            return Ok((DataType::Array(Box::new(ty)), serial));
        }
        if self.eat_kw("array") {
            return Ok((DataType::Array(Box::new(ty)), serial));
        }
        let _ = self.eat_kw("unsigned") || self.eat_kw("signed");
        self.eat_kw("zerofill");
        Ok((ty, serial))
    }

    fn drop(&mut self) -> SqlResult<Statement> {
        self.pos += 1;
        let _ = self.eat_kw("temporary");
        if self.eat_kw("table") {
            let if_exists = self.eat_kws(&["if", "exists"]);
            let mut names = vec![self.table_name()?];
            while self.eat_op(",") {
                names.push(self.table_name()?);
            }
            self.skip_to_end(); // CASCADE / RESTRICT
            return Ok(Statement::DropTable { names, if_exists });
        }
        if self.eat_kw("index") {
            let _ = self.eat_kw("concurrently");
            let if_exists = self.eat_kws(&["if", "exists"]);
            let mut names = vec![self.table_name()?];
            while self.eat_op(",") {
                names.push(self.table_name()?);
            }
            let table = if self.eat_kw("on") { Some(self.table_name()?) } else { None };
            self.skip_to_end();
            return Ok(Statement::DropIndex { names, table, if_exists });
        }
        if self.eat_kw("schema") || self.eat_kw("database") {
            let if_exists = self.eat_kws(&["if", "exists"]);
            let name = self.ident()?;
            self.skip_to_end();
            return Ok(Statement::DropSchema { name, if_exists });
        }
        if self.eat_kw("extension") || self.eat_kw("sequence") {
            self.skip_to_end();
            return Ok(Statement::NoOp("DROP"));
        }
        Err(SqlError::unsupported("this DROP statement"))
    }

    fn alter(&mut self) -> SqlResult<Statement> {
        self.pos += 1;
        if !self.eat_kw("table") {
            if self.eat_kw("sequence") || self.eat_kw("database") || self.eat_kw("schema") {
                self.skip_to_end();
                return Ok(Statement::NoOp("ALTER"));
            }
            return Err(SqlError::unsupported("this ALTER statement"));
        }
        let _ = self.eat_kws(&["if", "exists"]);
        let _ = self.eat_kw("only");
        let table = self.table_name()?;
        let mut ops = Vec::new();
        loop {
            ops.push(self.alter_op()?);
            if !self.eat_op(",") {
                break;
            }
        }
        Ok(Statement::AlterTable { table, ops })
    }

    fn alter_op(&mut self) -> SqlResult<AlterOp> {
        if self.eat_kw("add") {
            if self.is_kw("constraint") || self.is_kw("primary") || self.is_kw("foreign") || self.is_kw("unique") || self.is_kw("check")
                || self.is_kw("index") || self.is_kw("key")
            {
                return Ok(AlterOp::AddConstraint(self.table_constraint()?));
            }
            self.eat_kw("column");
            let if_not_exists = self.eat_kws(&["if", "not", "exists"]);
            let column = self.column_def()?;
            let _ = self.eat_kw("first") || (self.eat_kw("after") && self.ident().is_ok());
            return Ok(AlterOp::AddColumn { column, if_not_exists });
        }
        if self.eat_kw("drop") {
            if self.eat_kw("constraint") || self.eat_kw("index") || self.eat_kw("key") || self.eat_kws(&["foreign", "key"]) {
                let if_exists = self.eat_kws(&["if", "exists"]);
                let name = self.ident()?;
                self.skip_alter_tail();
                return Ok(AlterOp::DropConstraint { name, if_exists });
            }
            if self.eat_kws(&["primary", "key"]) {
                return Ok(AlterOp::DropConstraint { name: "PRIMARY".into(), if_exists: false });
            }
            self.eat_kw("column");
            let if_exists = self.eat_kws(&["if", "exists"]);
            let name = self.ident()?;
            self.skip_alter_tail();
            return Ok(AlterOp::DropColumn { name, if_exists });
        }
        if self.eat_kw("rename") {
            if self.eat_kw("to") || self.eat_kw("as") {
                return Ok(AlterOp::RenameTable(self.table_name()?));
            }
            self.eat_kw("column");
            let from = self.ident()?;
            self.expect_kw("to")?;
            return Ok(AlterOp::RenameColumn { from, to: self.ident()? });
        }
        if self.eat_kw("alter") {
            self.eat_kw("column");
            let name = self.ident()?;
            let change = if self.eat_kws(&["set", "default"]) {
                AlterColumn::SetDefault(self.expr()?)
            } else if self.eat_kws(&["drop", "default"]) {
                AlterColumn::DropDefault
            } else if self.eat_kws(&["set", "not", "null"]) {
                AlterColumn::SetNotNull
            } else if self.eat_kws(&["drop", "not", "null"]) {
                AlterColumn::DropNotNull
            } else if self.eat_kws(&["set", "data", "type"]) || self.eat_kw("type") {
                let (ty, _) = self.data_type()?;
                if self.eat_kw("using") {
                    self.expr()?;
                }
                AlterColumn::Type(ty)
            } else {
                return Err(SqlError::unsupported("this ALTER COLUMN form"));
            };
            return Ok(AlterOp::AlterColumn { name, change });
        }
        if self.eat_kw("modify") || self.eat_kw("change") {
            let changing = self.tokens.get(self.pos - 1).and_then(Token::keyword).as_deref() == Some("change");
            self.eat_kw("column");
            let old = if changing { Some(self.ident()?) } else { None };
            let column = self.column_def()?;
            let _ = self.eat_kw("first") || (self.eat_kw("after") && self.ident().is_ok());
            if let Some(old) = old {
                if old != column.name {
                    return Err(SqlError::unsupported("CHANGE COLUMN with a rename; use RENAME COLUMN"));
                }
            }
            return Ok(AlterOp::AlterColumn { name: column.name.clone(), change: AlterColumn::Type(column.ty) });
        }
        Err(SqlError::unsupported("this ALTER TABLE operation"))
    }

    fn skip_alter_tail(&mut self) {
        let _ = self.eat_kw("cascade") || self.eat_kw("restrict");
    }

    fn insert(&mut self) -> SqlResult<Statement> {
        let replace_kw = self.is_kw("replace");
        self.pos += 1;
        let mut ins = Insert {
            table: String::new(),
            alias: None,
            columns: Vec::new(),
            source: InsertSource::DefaultValues,
            on_conflict: None,
            returning: Vec::new(),
            ignore: false,
            replace: replace_kw,
        };
        let _ = self.eat_kw("low_priority") || self.eat_kw("delayed") || self.eat_kw("high_priority");
        if self.eat_kw("ignore") {
            ins.ignore = true;
        }
        if self.eat_kw("or") {
            if self.eat_kw("replace") {
                ins.replace = true;
            } else if self.eat_kw("ignore") {
                ins.ignore = true;
            } else {
                return Err(SqlError::unsupported("this INSERT OR ... conflict clause"));
            }
        }
        self.eat_kw("into");
        ins.table = self.table_name()?;
        if self.eat_kw("as") {
            ins.alias = Some(self.ident()?);
        }
        if self.is_op("(") && !self.is_kw_at(1, "select") && !self.is_kw_at(1, "with") {
            ins.columns = self.ident_list()?;
        }
        // T-SQL OUTPUT INSERTED.* (as RETURNING).
        if self.eat_kw("output") {
            ins.returning = self.output_items()?;
        }
        if self.eat_kws(&["default", "values"]) {
            ins.source = InsertSource::DefaultValues;
        } else if self.eat_kw("values") || self.eat_kw("value") {
            ins.source = InsertSource::Values(self.values_rows()?);
        } else if self.eat_kw("set") {
            // MySQL INSERT ... SET a = 1, b = 2.
            let sets = self.assignments()?;
            ins.columns = sets.iter().map(|(c, _)| c.clone()).collect();
            ins.source = InsertSource::Values(vec![sets.into_iter().map(|(_, e)| e).collect()]);
        } else {
            ins.source = InsertSource::Query(Box::new(self.query()?));
        }
        if self.dialect == Dialect::MySql && self.eat_kw("as") {
            // INSERT ... VALUES (...) AS new ON DUPLICATE KEY UPDATE a = new.a
            ins.alias = Some(self.ident()?);
            if self.is_op("(") {
                self.ident_list()?;
            }
        }
        if self.eat_kws(&["on", "conflict"]) {
            let mut target = Vec::new();
            if self.is_op("(") {
                target = self.ident_list()?;
            } else if self.eat_kws(&["on", "constraint"]) {
                self.ident()?;
            }
            if self.eat_kw("where") {
                self.expr()?;
            }
            self.expect_kw("do")?;
            if self.eat_kw("nothing") {
                ins.on_conflict = Some(OnConflict::DoNothing { target });
            } else {
                self.expect_kw("update")?;
                self.expect_kw("set")?;
                let sets = self.assignments()?;
                let where_ = if self.eat_kw("where") { Some(self.expr()?) } else { None };
                ins.on_conflict = Some(OnConflict::DoUpdate { target, sets, where_ });
            }
        } else if self.eat_kws(&["on", "duplicate", "key", "update"]) {
            let sets = self.assignments()?;
            ins.on_conflict = Some(OnConflict::DoUpdate { target: Vec::new(), sets, where_: None });
        }
        if self.eat_kw("returning") {
            ins.returning = self.select_items()?;
        }
        Ok(Statement::Insert(Box::new(ins)))
    }

    /// T-SQL `OUTPUT INSERTED.a, INSERTED.*` → select items.
    fn output_items(&mut self) -> SqlResult<Vec<SelectItem>> {
        let items = self.select_items()?;
        Ok(items
            .into_iter()
            .map(|it| match it {
                SelectItem::Wildcard(Some(q)) if q.eq_ignore_ascii_case("inserted") || q.eq_ignore_ascii_case("deleted") => {
                    SelectItem::Wildcard(None)
                }
                SelectItem::Expr { expr: Expr::Column(p), alias } if p.len() == 2 => {
                    SelectItem::Expr { expr: Expr::Column(vec![p[1].clone()]), alias }
                }
                other => other,
            })
            .collect())
    }

    fn values_rows(&mut self) -> SqlResult<Vec<Vec<Expr>>> {
        let mut rows = Vec::new();
        loop {
            self.eat_kw("row");
            self.expect_op("(")?;
            let mut row = Vec::new();
            if !self.is_op(")") {
                loop {
                    row.push(if self.eat_kw("default") { Expr::Default } else { self.expr()? });
                    if !self.eat_op(",") {
                        break;
                    }
                }
            }
            self.expect_op(")")?;
            rows.push(row);
            if !self.eat_op(",") {
                break;
            }
        }
        Ok(rows)
    }

    fn assignments(&mut self) -> SqlResult<Vec<(String, Expr)>> {
        let mut sets = Vec::new();
        loop {
            let (_, col) = self.object_name()?;
            self.expect_op("=")?;
            let e = if self.eat_kw("default") { Expr::Default } else { self.expr()? };
            sets.push((col, e));
            if !self.eat_op(",") {
                break;
            }
        }
        Ok(sets)
    }

    fn update(&mut self) -> SqlResult<Statement> {
        self.pos += 1;
        let _ = self.eat_kw("low_priority") || self.eat_kw("ignore") || self.eat_kw("only");
        let table = self.table_name()?;
        let alias = self.opt_alias()?;
        self.expect_kw("set")?;
        let sets = self.assignments()?;
        let mut up = Update {
            table,
            alias,
            sets,
            from: Vec::new(),
            where_: None,
            returning: Vec::new(),
            order_by: Vec::new(),
            limit: None,
        };
        if self.eat_kw("output") {
            up.returning = self.output_items()?;
        }
        if self.eat_kw("from") {
            up.from = self.table_list()?;
        }
        if self.eat_kw("where") {
            up.where_ = Some(self.expr()?);
        }
        if self.eat_kws(&["order", "by"]) {
            up.order_by = self.order_list()?;
        }
        if self.eat_kw("limit") {
            up.limit = Some(self.expr()?);
        }
        if self.eat_kw("returning") {
            up.returning = self.select_items()?;
        }
        Ok(Statement::Update(Box::new(up)))
    }

    fn delete(&mut self) -> SqlResult<Statement> {
        self.pos += 1;
        let _ = self.eat_kw("low_priority") || self.eat_kw("quick") || self.eat_kw("ignore");
        self.eat_kw("from");
        let _ = self.eat_kw("only");
        let table = self.table_name()?;
        let alias = self.opt_alias()?;
        let mut del =
            Delete { table, alias, using: Vec::new(), where_: None, returning: Vec::new(), order_by: Vec::new(), limit: None };
        if self.eat_kw("output") {
            del.returning = self.output_items()?;
        }
        if self.eat_kw("using") {
            del.using = self.table_list()?;
        }
        if self.eat_kw("where") {
            del.where_ = Some(self.expr()?);
        }
        if self.eat_kws(&["order", "by"]) {
            del.order_by = self.order_list()?;
        }
        if self.eat_kw("limit") {
            del.limit = Some(self.expr()?);
        }
        if self.eat_kw("returning") {
            del.returning = self.select_items()?;
        }
        Ok(Statement::Delete(Box::new(del)))
    }

    // ── queries ──

    fn query(&mut self) -> SqlResult<Query> {
        let mut ctes = Vec::new();
        if self.eat_kw("with") {
            if self.eat_kw("recursive") {
                return Err(SqlError::unsupported("WITH RECURSIVE"));
            }
            loop {
                let name = self.ident()?;
                let columns = if self.is_op("(") { self.ident_list()? } else { Vec::new() };
                self.expect_kw("as")?;
                let _ = self.eat_kws(&["not", "materialized"]) || self.eat_kw("materialized");
                self.expect_op("(")?;
                let query = self.query()?;
                self.expect_op(")")?;
                ctes.push(Cte { name, columns, query });
                if !self.eat_op(",") {
                    break;
                }
            }
        }
        let (body, top) = self.set_expr()?;
        let mut q = Query { ctes, body, order_by: Vec::new(), limit: top, offset: None, lock: None };
        if self.eat_kws(&["order", "by"]) {
            q.order_by = self.order_list()?;
        }
        loop {
            if self.eat_kw("limit") {
                if self.eat_kw("all") {
                    continue;
                }
                let first = self.expr()?;
                if self.eat_op(",") {
                    // MySQL LIMIT offset, count
                    q.offset = Some(first);
                    q.limit = Some(self.expr()?);
                } else {
                    q.limit = Some(first);
                }
            } else if self.eat_kw("offset") {
                q.offset = Some(self.expr()?);
                let _ = self.eat_kw("rows") || self.eat_kw("row");
            } else if self.eat_kw("fetch") {
                let _ = self.eat_kw("first") || self.eat_kw("next");
                q.limit = Some(if self.is_kw("row") || self.is_kw("rows") { Expr::Number("1".into()) } else { self.expr()? });
                let _ = self.eat_kw("rows") || self.eat_kw("row");
                if !self.eat_kw("only") {
                    self.expect_kws_with_ties()?;
                }
            } else if self.eat_kw("for") {
                if self.eat_kw("update") {
                    if self.eat_kw("of") {
                        self.table_name()?;
                        while self.eat_op(",") {
                            self.table_name()?;
                        }
                    }
                    let nowait = self.eat_kw("nowait");
                    let skip_locked = self.eat_kws(&["skip", "locked"]);
                    q.lock = Some(LockMode::Update { nowait, skip_locked });
                } else if self.eat_kw("share") || self.eat_kws(&["no", "key", "update"]) || self.eat_kws(&["key", "share"]) {
                    if self.eat_kw("of") {
                        self.table_name()?;
                    }
                    let _ = self.eat_kw("nowait") || self.eat_kws(&["skip", "locked"]);
                    q.lock = Some(LockMode::Share);
                } else if self.eat_kw("json") || self.eat_kw("xml") {
                    return Err(SqlError::unsupported("FOR JSON / FOR XML"));
                } else {
                    return Err(self.unexpected("UPDATE or SHARE"));
                }
            } else if self.eat_kws(&["lock", "in", "share", "mode"]) {
                q.lock = Some(LockMode::Share);
            } else {
                break;
            }
        }
        Ok(q)
    }

    fn expect_kws_with_ties(&mut self) -> SqlResult<()> {
        if self.eat_kws(&["with", "ties"]) {
            return Err(SqlError::unsupported("FETCH ... WITH TIES"));
        }
        Err(self.unexpected("ONLY"))
    }

    /// Set operations; INTERSECT binds tighter than UNION / EXCEPT. Returns a T-SQL `TOP` limit too.
    fn set_expr(&mut self) -> SqlResult<(SetExpr, Option<Expr>)> {
        let (mut left, top) = self.set_term()?;
        loop {
            let op = if self.eat_kw("union") {
                SetOp::Union
            } else if self.eat_kw("except") || self.eat_kw("minus") {
                SetOp::Except
            } else {
                break;
            };
            let all = self.eat_kw("all");
            if !all {
                self.eat_kw("distinct");
            }
            let (right, _) = self.set_term()?;
            left = SetExpr::SetOp { op, all, left: Box::new(left), right: Box::new(right) };
        }
        Ok((left, top))
    }

    fn set_term(&mut self) -> SqlResult<(SetExpr, Option<Expr>)> {
        let (mut left, top) = self.set_primary()?;
        while self.eat_kw("intersect") {
            let all = self.eat_kw("all");
            if !all {
                self.eat_kw("distinct");
            }
            let (right, _) = self.set_primary()?;
            left = SetExpr::SetOp { op: SetOp::Intersect, all, left: Box::new(left), right: Box::new(right) };
        }
        Ok((left, top))
    }

    fn set_primary(&mut self) -> SqlResult<(SetExpr, Option<Expr>)> {
        if self.eat_op("(") {
            let q = self.query()?;
            self.expect_op(")")?;
            return Ok((SetExpr::Nested(Box::new(q)), None));
        }
        if self.eat_kw("values") {
            return Ok((SetExpr::Values(self.values_rows()?), None));
        }
        if self.eat_kw("table") {
            let (schema, name) = self.object_name()?;
            let sel = Select {
                items: vec![SelectItem::Wildcard(None)],
                from: vec![TableRef::Named { schema, name, alias: None }],
                ..Select::default()
            };
            return Ok((SetExpr::Select(Box::new(sel)), None));
        }
        self.expect_kw("select")?;
        let mut sel = Select::default();
        let mut top = None;
        if self.eat_kw("distinct") {
            if self.eat_kw("on") {
                return Err(SqlError::unsupported("SELECT DISTINCT ON"));
            }
            sel.distinct = true;
        } else {
            self.eat_kw("all");
        }
        let _ = self.eat_kw("sql_calc_found_rows") || self.eat_kw("straight_join") || self.eat_kw("sql_no_cache");
        if self.eat_kw("top") {
            let paren = self.eat_op("(");
            top = Some(self.primary()?);
            if paren {
                self.expect_op(")")?;
            }
            if self.eat_kw("percent") {
                return Err(SqlError::unsupported("TOP ... PERCENT"));
            }
        }
        sel.items = self.select_items()?;
        if self.eat_kw("into") {
            return Err(SqlError::unsupported("SELECT ... INTO"));
        }
        if self.eat_kw("from") {
            sel.from = self.table_list()?;
        }
        if self.eat_kw("where") {
            sel.where_ = Some(self.expr()?);
        }
        if self.eat_kws(&["group", "by"]) {
            loop {
                sel.group_by.push(self.expr()?);
                if !self.eat_op(",") {
                    break;
                }
            }
            if self.eat_kws(&["with", "rollup"]) {
                return Err(SqlError::unsupported("GROUP BY ... WITH ROLLUP"));
            }
        }
        if self.eat_kw("having") {
            sel.having = Some(self.expr()?);
        }
        if self.is_kw("window") {
            return Err(SqlError::unsupported("window functions"));
        }
        Ok((SetExpr::Select(Box::new(sel)), top))
    }

    fn select_items(&mut self) -> SqlResult<Vec<SelectItem>> {
        let mut items = Vec::new();
        loop {
            if self.eat_op("*") {
                items.push(SelectItem::Wildcard(None));
            } else if matches!(self.peek(), Some(Token::Word { .. }))
                && self.is_op_at(1, ".")
                && self.is_op_at(2, "*")
            {
                let q = self.ident()?;
                self.pos += 2;
                items.push(SelectItem::Wildcard(Some(q)));
            } else if matches!(self.peek(), Some(Token::Word { .. }))
                && self.is_op_at(1, ".")
                && matches!(self.peek_at(2), Some(Token::Word { .. }))
                && self.is_op_at(3, ".")
                && self.is_op_at(4, "*")
            {
                self.ident()?;
                self.pos += 1;
                let q = self.ident()?;
                self.pos += 2;
                items.push(SelectItem::Wildcard(Some(q)));
            } else {
                let expr = self.expr()?;
                let alias = self.opt_alias()?;
                items.push(SelectItem::Expr { expr, alias });
            }
            if !self.eat_op(",") {
                break;
            }
        }
        Ok(items)
    }

    fn is_op_at(&self, n: usize, op: &str) -> bool {
        matches!(self.peek_at(n), Some(Token::Op(o)) if *o == op)
    }

    fn order_list(&mut self) -> SqlResult<Vec<OrderBy>> {
        let mut out = Vec::new();
        loop {
            let expr = self.expr()?;
            let desc = if self.eat_kw("desc") {
                true
            } else {
                self.eat_kw("asc");
                false
            };
            let nulls_first = if self.eat_kw("nulls") {
                if self.eat_kw("first") {
                    Some(true)
                } else {
                    self.expect_kw("last")?;
                    Some(false)
                }
            } else {
                None
            };
            out.push(OrderBy { expr, desc, nulls_first });
            if !self.eat_op(",") {
                break;
            }
        }
        Ok(out)
    }

    fn table_list(&mut self) -> SqlResult<Vec<TableRef>> {
        let mut out = vec![self.table_ref()?];
        while self.eat_op(",") {
            out.push(self.table_ref()?);
        }
        Ok(out)
    }

    fn table_ref(&mut self) -> SqlResult<TableRef> {
        let mut left = self.table_factor()?;
        loop {
            let natural = self.eat_kw("natural");
            let kind = if self.eat_kw("cross") {
                self.expect_kw("join")?;
                JoinKind::Cross
            } else if self.eat_kw("inner") {
                self.expect_kw("join")?;
                JoinKind::Inner
            } else if self.eat_kw("join") || self.eat_kw("straight_join") {
                JoinKind::Inner
            } else if self.eat_kw("left") {
                self.eat_kw("outer");
                self.expect_kw("join")?;
                JoinKind::Left
            } else if self.eat_kw("right") {
                self.eat_kw("outer");
                self.expect_kw("join")?;
                JoinKind::Right
            } else if self.eat_kw("full") {
                self.eat_kw("outer");
                self.expect_kw("join")?;
                JoinKind::Full
            } else {
                if natural {
                    return Err(self.unexpected("JOIN"));
                }
                break;
            };
            let right = self.table_factor()?;
            let constraint = if natural {
                JoinConstraint::Natural
            } else if self.eat_kw("on") {
                JoinConstraint::On(self.expr()?)
            } else if self.eat_kw("using") {
                JoinConstraint::Using(self.ident_list()?)
            } else {
                JoinConstraint::None
            };
            left = TableRef::Join { left: Box::new(left), right: Box::new(right), kind, constraint };
        }
        Ok(left)
    }

    fn table_alias(&mut self) -> SqlResult<Option<TableAlias>> {
        let name = match self.opt_alias()? {
            Some(n) => n,
            None => return Ok(None),
        };
        let columns = if self.is_op("(") { self.ident_list()? } else { Vec::new() };
        Ok(Some(TableAlias { name, columns }))
    }

    fn table_factor(&mut self) -> SqlResult<TableRef> {
        let lateral = self.eat_kw("lateral");
        if self.eat_op("(") {
            if self.is_kw("select") || self.is_kw("with") || self.is_kw("values") || self.is_op("(") {
                let query = self.query()?;
                self.expect_op(")")?;
                let alias = self.table_alias()?;
                return Ok(TableRef::Derived { query: Box::new(query), alias, lateral });
            }
            let inner = self.table_ref()?;
            self.expect_op(")")?;
            return Ok(inner);
        }
        let (schema, name) = self.object_name()?;
        if self.eat_op("(") {
            let mut args = Vec::new();
            if !self.is_op(")") {
                loop {
                    args.push(self.expr()?);
                    if !self.eat_op(",") {
                        break;
                    }
                }
            }
            self.expect_op(")")?;
            let alias = self.table_alias()?;
            return Ok(TableRef::Function { name: name.to_ascii_lowercase(), args, alias });
        }
        let alias = self.table_alias()?;
        // MySQL index hints / T-SQL table hints.
        loop {
            if (self.is_kw("use") || self.is_kw("force") || self.is_kw("ignore"))
                && (self.is_kw_at(1, "index") || self.is_kw_at(1, "key"))
            {
                self.pos += 2;
                if self.eat_kw("for") {
                    let _ = self.eat_kw("join") || self.eat_kws(&["order", "by"]) || self.eat_kws(&["group", "by"]);
                }
                self.ident_or_numbers_paren()?;
            } else if self.is_kw("with") && self.is_op_at(1, "(") {
                self.pos += 1;
                self.ident_or_numbers_paren()?;
            } else {
                break;
            }
        }
        Ok(TableRef::Named { schema, name, alias })
    }

    // ── expressions ──

    pub(crate) fn expr(&mut self) -> SqlResult<Expr> {
        self.or_expr()
    }

    fn or_expr(&mut self) -> SqlResult<Expr> {
        let mut left = self.xor_expr()?;
        loop {
            if self.eat_kw("or") || (self.dialect == Dialect::MySql && self.eat_op("||")) {
                let right = self.xor_expr()?;
                left = Expr::binary(BinaryOp::Or, left, right);
            } else {
                return Ok(left);
            }
        }
    }

    fn xor_expr(&mut self) -> SqlResult<Expr> {
        let mut left = self.and_expr()?;
        while self.eat_kw("xor") {
            let right = self.and_expr()?;
            left = Expr::binary(BinaryOp::Xor, left, right);
        }
        Ok(left)
    }

    fn and_expr(&mut self) -> SqlResult<Expr> {
        let mut left = self.not_expr()?;
        loop {
            if self.eat_kw("and") || (self.dialect == Dialect::MySql && self.eat_op("&&")) {
                let right = self.not_expr()?;
                left = Expr::binary(BinaryOp::And, left, right);
            } else {
                return Ok(left);
            }
        }
    }

    fn not_expr(&mut self) -> SqlResult<Expr> {
        if self.eat_kw("not") {
            let e = self.not_expr()?;
            return Ok(Expr::Unary { op: UnaryOp::Not, expr: Box::new(e) });
        }
        if self.dialect == Dialect::MySql && self.is_op("!") {
            self.pos += 1;
            let e = self.not_expr()?;
            return Ok(Expr::Unary { op: UnaryOp::Not, expr: Box::new(e) });
        }
        self.predicate()
    }

    /// Comparisons and predicates: `=`, `<`, IS, BETWEEN, IN, LIKE, REGEXP.
    fn predicate(&mut self) -> SqlResult<Expr> {
        let mut left = self.bit_or()?;
        loop {
            if self.eat_kw("is") {
                let negated = self.eat_kw("not");
                left = if self.eat_kw("null") || self.eat_kw("unknown") {
                    Expr::IsNull { expr: Box::new(left), negated }
                } else if self.eat_kw("true") {
                    Expr::IsBool { expr: Box::new(left), value: true, negated }
                } else if self.eat_kw("false") {
                    Expr::IsBool { expr: Box::new(left), value: false, negated }
                } else if self.eat_kws(&["distinct", "from"]) {
                    let right = self.bit_or()?;
                    Expr::IsDistinctFrom { left: Box::new(left), right: Box::new(right), negated }
                } else {
                    return Err(self.unexpected("NULL, TRUE, FALSE or DISTINCT FROM"));
                };
                continue;
            }
            if self.eat_kw("isnull") {
                left = Expr::IsNull { expr: Box::new(left), negated: false };
                continue;
            }
            if self.eat_kw("notnull") {
                left = Expr::IsNull { expr: Box::new(left), negated: true };
                continue;
            }
            let negated = self.is_kw("not")
                && (self.is_kw_at(1, "between")
                    || self.is_kw_at(1, "in")
                    || self.is_kw_at(1, "like")
                    || self.is_kw_at(1, "ilike")
                    || self.is_kw_at(1, "regexp")
                    || self.is_kw_at(1, "rlike")
                    || self.is_kw_at(1, "similar"));
            if negated {
                self.pos += 1;
            }
            if self.eat_kw("between") {
                self.eat_kw("symmetric");
                let low = self.bit_or()?;
                self.expect_kw("and")?;
                let high = self.bit_or()?;
                left = Expr::Between { expr: Box::new(left), low: Box::new(low), high: Box::new(high), negated };
                continue;
            }
            if self.eat_kw("in") {
                self.expect_op("(")?;
                if self.is_kw("select") || self.is_kw("with") {
                    let q = self.query()?;
                    self.expect_op(")")?;
                    left = Expr::InSubquery { expr: Box::new(left), query: Box::new(q), negated };
                } else {
                    let mut list = Vec::new();
                    if !self.is_op(")") {
                        loop {
                            list.push(self.expr()?);
                            if !self.eat_op(",") {
                                break;
                            }
                        }
                    }
                    self.expect_op(")")?;
                    left = Expr::InList { expr: Box::new(left), list, negated };
                }
                continue;
            }
            let like = if self.eat_kw("like") {
                Some(false)
            } else if self.eat_kw("ilike") {
                Some(true)
            } else {
                None
            };
            if let Some(ci) = like {
                // MySQL LIKE is case-insensitive under its default collations.
                let ci = ci || self.dialect == Dialect::MySql;
                let pattern = self.bit_or()?;
                let escape = if self.eat_kw("escape") { Some(Box::new(self.primary()?)) } else { None };
                left = Expr::Like { expr: Box::new(left), pattern: Box::new(pattern), negated, case_insensitive: ci, escape };
                continue;
            }
            if self.eat_kw("regexp") || self.eat_kw("rlike") {
                let right = self.bit_or()?;
                left = Expr::binary(BinaryOp::Regex { case_insensitive: true, negated }, left, right);
                continue;
            }
            if self.is_kw("similar") {
                return Err(SqlError::unsupported("SIMILAR TO"));
            }
            if negated {
                return Err(self.unexpected("BETWEEN, IN or LIKE"));
            }
            let op = match self.peek() {
                Some(Token::Op("=")) => BinaryOp::Eq,
                Some(Token::Op("<>")) | Some(Token::Op("!=")) => BinaryOp::NotEq,
                Some(Token::Op("<")) => BinaryOp::Lt,
                Some(Token::Op("<=")) => BinaryOp::LtEq,
                Some(Token::Op(">")) => BinaryOp::Gt,
                Some(Token::Op(">=")) => BinaryOp::GtEq,
                Some(Token::Op("<=>")) => BinaryOp::NullSafeEq,
                Some(Token::Op("~")) if self.dialect == Dialect::Postgres => BinaryOp::Regex { case_insensitive: false, negated: false },
                Some(Token::Op("~*")) => BinaryOp::Regex { case_insensitive: true, negated: false },
                Some(Token::Op("!~")) => BinaryOp::Regex { case_insensitive: false, negated: true },
                Some(Token::Op("!~*")) => BinaryOp::Regex { case_insensitive: true, negated: true },
                _ => return Ok(left),
            };
            self.pos += 1;
            if self.is_kw("any") || self.is_kw("some") || self.is_kw("all") {
                let all = self.is_kw("all");
                self.pos += 1;
                self.expect_op("(")?;
                let right = if self.is_kw("select") || self.is_kw("with") {
                    Expr::Subquery(Box::new(self.query()?))
                } else {
                    self.expr()?
                };
                self.expect_op(")")?;
                left = Expr::AnyAll { left: Box::new(left), op, right: Box::new(right), all };
                continue;
            }
            let right = self.bit_or()?;
            left = Expr::binary(op, left, right);
        }
    }

    fn bit_or(&mut self) -> SqlResult<Expr> {
        let mut left = self.bit_and()?;
        loop {
            let op = if self.is_op("|") {
                BinaryOp::BitOr
            } else if self.is_op("#") || (self.is_op("^") && self.dialect == Dialect::MySql) {
                BinaryOp::BitXor
            } else {
                return Ok(left);
            };
            self.pos += 1;
            let right = self.bit_and()?;
            left = Expr::binary(op, left, right);
        }
    }

    fn bit_and(&mut self) -> SqlResult<Expr> {
        let mut left = self.shift()?;
        while self.is_op("&") {
            self.pos += 1;
            let right = self.shift()?;
            left = Expr::binary(BinaryOp::BitAnd, left, right);
        }
        Ok(left)
    }

    fn shift(&mut self) -> SqlResult<Expr> {
        let mut left = self.other_op()?;
        loop {
            let op = if self.is_op("<<") {
                BinaryOp::ShiftLeft
            } else if self.is_op(">>") {
                BinaryOp::ShiftRight
            } else {
                return Ok(left);
            };
            self.pos += 1;
            let right = self.other_op()?;
            left = Expr::binary(op, left, right);
        }
    }

    /// `||` (concatenation), JSON and containment operators.
    fn other_op(&mut self) -> SqlResult<Expr> {
        let mut left = self.additive()?;
        loop {
            let op = match self.peek() {
                Some(Token::Op("||")) if self.dialect != Dialect::MySql => BinaryOp::Concat,
                Some(Token::Op("->")) => BinaryOp::JsonGet,
                Some(Token::Op("->>")) => BinaryOp::JsonGetText,
                Some(Token::Op("@>")) => BinaryOp::Contains,
                Some(Token::Op("<@")) => BinaryOp::ContainedBy,
                Some(Token::Op("&&")) if self.dialect == Dialect::Postgres => BinaryOp::Overlap,
                _ => return Ok(left),
            };
            self.pos += 1;
            let right = self.additive()?;
            left = Expr::binary(op, left, right);
        }
    }

    fn additive(&mut self) -> SqlResult<Expr> {
        let mut left = self.multiplicative()?;
        loop {
            let op = if self.is_op("+") {
                BinaryOp::Add
            } else if self.is_op("-") {
                BinaryOp::Sub
            } else {
                return Ok(left);
            };
            self.pos += 1;
            let right = self.multiplicative()?;
            left = Expr::binary(op, left, right);
        }
    }

    fn multiplicative(&mut self) -> SqlResult<Expr> {
        let mut left = self.power()?;
        loop {
            let op = if self.is_op("*") {
                BinaryOp::Mul
            } else if self.is_op("/") {
                BinaryOp::Div
            } else if self.is_op("%") {
                BinaryOp::Mod
            } else if self.is_kw("div") && self.dialect == Dialect::MySql {
                BinaryOp::IntDiv
            } else if self.is_kw("mod") {
                BinaryOp::Mod
            } else {
                return Ok(left);
            };
            self.pos += 1;
            let right = self.power()?;
            left = Expr::binary(op, left, right);
        }
    }

    fn power(&mut self) -> SqlResult<Expr> {
        let left = self.unary()?;
        if self.is_op("^") && self.dialect != Dialect::MySql {
            self.pos += 1;
            let right = self.power()?;
            return Ok(Expr::binary(BinaryOp::Pow, left, right));
        }
        Ok(left)
    }

    fn unary(&mut self) -> SqlResult<Expr> {
        if self.eat_op("-") {
            let e = self.unary()?;
            return Ok(match e {
                Expr::Number(n) if !n.starts_with('-') => Expr::Number(format!("-{n}")),
                other => Expr::Unary { op: UnaryOp::Neg, expr: Box::new(other) },
            });
        }
        if self.eat_op("+") {
            return self.unary();
        }
        if self.eat_op("~") {
            let e = self.unary()?;
            return Ok(Expr::Unary { op: UnaryOp::BitNot, expr: Box::new(e) });
        }
        let mut e = self.primary()?;
        loop {
            if self.eat_op("::") {
                let (ty, _) = self.data_type()?;
                e = Expr::Cast { expr: Box::new(e), ty };
            } else if self.is_op("[") {
                self.pos += 1;
                let index = self.expr()?;
                if self.eat_op(":") {
                    return Err(SqlError::unsupported("array slices"));
                }
                self.expect_op("]")?;
                e = Expr::Index { expr: Box::new(e), index: Box::new(index) };
            } else if self.eat_kw("collate") {
                self.ident()?;
            } else if self.is_kw("at") && self.is_kw_at(1, "time") && self.is_kw_at(2, "zone") {
                self.pos += 3;
                let zone = self.primary()?;
                e = Expr::Function { name: "timezone".into(), args: vec![zone, e], star: false, distinct: false, over: false };
            } else {
                return Ok(e);
            }
        }
    }

    pub(crate) fn primary(&mut self) -> SqlResult<Expr> {
        let tok = match self.next() {
            Some(t) => t,
            None => return Err(self.unexpected("an expression")),
        };
        match tok {
            Token::Number(n) => Ok(Expr::Number(n)),
            Token::Str(s) => Ok(Expr::Str(s)),
            Token::Blob(b) => Ok(Expr::Blob(b)),
            Token::Param(i) => Ok(Expr::Param(i)),
            Token::Variable { name, system } => Ok(Expr::Variable { name, system }),
            Token::Op("(") => {
                if self.is_kw("select") || self.is_kw("with") || self.is_kw("values") {
                    let q = self.query()?;
                    self.expect_op(")")?;
                    return Ok(Expr::Subquery(Box::new(q)));
                }
                let first = self.expr()?;
                if self.eat_op(",") {
                    let mut items = vec![first];
                    loop {
                        items.push(self.expr()?);
                        if !self.eat_op(",") {
                            break;
                        }
                    }
                    self.expect_op(")")?;
                    return Ok(Expr::Tuple(items));
                }
                self.expect_op(")")?;
                Ok(first)
            }
            Token::Op("{") if self.dialect == Dialect::MySql => {
                // ODBC escapes {d '2020-01-01'} / {ts '...'}.
                self.ident()?;
                let e = self.primary()?;
                self.expect_op("}")?;
                Ok(e)
            }
            Token::Word { text, quoted } => self.word_expr(text, quoted),
            other => {
                self.pos -= 1;
                let _ = other;
                Err(self.unexpected("an expression"))
            }
        }
    }

    fn word_expr(&mut self, text: String, quoted: bool) -> SqlResult<Expr> {
        let lower = text.to_ascii_lowercase();
        if !quoted {
            match lower.as_str() {
                "null" => return Ok(Expr::Null),
                "true" => return Ok(Expr::Bool(true)),
                "false" => return Ok(Expr::Bool(false)),
                "default" => return Ok(Expr::Default),
                "case" => return self.case_expr(),
                "exists" => {
                    self.expect_op("(")?;
                    let q = self.query()?;
                    self.expect_op(")")?;
                    return Ok(Expr::Exists { query: Box::new(q), negated: false });
                }
                "cast" | "try_cast" | "safe_cast" => {
                    self.expect_op("(")?;
                    let e = self.expr()?;
                    self.expect_kw("as")?;
                    let ty = self.cast_type()?;
                    self.expect_op(")")?;
                    return Ok(Expr::Cast { expr: Box::new(e), ty });
                }
                "convert" if self.is_op("(") => {
                    self.pos += 1;
                    if self.dialect == Dialect::MsSql {
                        let ty = self.cast_type()?;
                        self.expect_op(",")?;
                        let e = self.expr()?;
                        if self.eat_op(",") {
                            self.expr()?;
                        }
                        self.expect_op(")")?;
                        return Ok(Expr::Cast { expr: Box::new(e), ty });
                    }
                    let e = self.expr()?;
                    if self.eat_kw("using") {
                        self.ident()?;
                        self.expect_op(")")?;
                        return Ok(e);
                    }
                    self.expect_op(",")?;
                    let ty = self.cast_type()?;
                    self.expect_op(")")?;
                    return Ok(Expr::Cast { expr: Box::new(e), ty });
                }
                "array" if self.is_op("[") => {
                    self.pos += 1;
                    let mut items = Vec::new();
                    if !self.is_op("]") {
                        loop {
                            items.push(self.expr()?);
                            if !self.eat_op(",") {
                                break;
                            }
                        }
                    }
                    self.expect_op("]")?;
                    return Ok(Expr::Array(items));
                }
                "extract" if self.is_op("(") => {
                    self.pos += 1;
                    let field = match self.next() {
                        Some(Token::Word { text, .. }) | Some(Token::Str(text)) => text.to_ascii_lowercase(),
                        _ => return Err(self.unexpected("a date field")),
                    };
                    self.expect_kw("from")?;
                    let e = self.expr()?;
                    self.expect_op(")")?;
                    return Ok(Expr::Extract { field, expr: Box::new(e) });
                }
                "interval" => {
                    let v = match self.next() {
                        Some(Token::Str(s)) => s,
                        Some(Token::Number(n)) => n,
                        _ => return Err(self.unexpected("an interval literal")),
                    };
                    // MySQL INTERVAL 1 DAY / PostgreSQL INTERVAL '1' DAY.
                    if let Some(Token::Word { text, quoted: false }) = self.peek() {
                        let unit = text.to_ascii_lowercase();
                        if is_interval_unit(&unit) {
                            self.pos += 1;
                            return Ok(Expr::Interval(format!("{v} {unit}")));
                        }
                    }
                    return Ok(Expr::Interval(v));
                }
                "date" | "timestamp" | "time" | "timestamptz" | "datetime" if matches!(self.peek(), Some(Token::Str(_))) => {
                    let Some(Token::Str(s)) = self.next() else { unreachable!() };
                    let ty = match lower.as_str() {
                        "date" => DataType::Date,
                        "time" => DataType::Time,
                        "timestamptz" => DataType::TimestampTz,
                        _ => DataType::Timestamp,
                    };
                    return Ok(Expr::Cast { expr: Box::new(Expr::Str(s)), ty });
                }
                "position" if self.is_op("(") => {
                    self.pos += 1;
                    let needle = self.bit_or()?;
                    if self.eat_kw("in") {
                        let hay = self.expr()?;
                        self.expect_op(")")?;
                        return Ok(func("position", vec![needle, hay]));
                    }
                    self.expect_op(",")?;
                    let hay = self.expr()?;
                    self.expect_op(")")?;
                    return Ok(func("position", vec![needle, hay]));
                }
                "substring" | "substr" if self.is_op("(") => {
                    self.pos += 1;
                    let s = self.expr()?;
                    let mut args = vec![s];
                    if self.eat_kw("from") {
                        args.push(self.expr()?);
                        if self.eat_kw("for") {
                            args.push(self.expr()?);
                        }
                    } else if self.eat_kw("for") {
                        args.push(Expr::Number("1".into()));
                        args.push(self.expr()?);
                    } else {
                        while self.eat_op(",") {
                            args.push(self.expr()?);
                        }
                    }
                    self.expect_op(")")?;
                    return Ok(func("substring", args));
                }
                "trim" if self.is_op("(") => {
                    self.pos += 1;
                    let mode = if self.eat_kw("leading") {
                        "ltrim"
                    } else if self.eat_kw("trailing") {
                        "rtrim"
                    } else {
                        self.eat_kw("both");
                        "btrim"
                    };
                    let mut chars = None;
                    let target;
                    if self.eat_kw("from") {
                        target = self.expr()?;
                    } else {
                        let first = self.expr()?;
                        if self.eat_kw("from") {
                            chars = Some(first);
                            target = self.expr()?;
                        } else if self.eat_op(",") {
                            chars = Some(self.expr()?);
                            target = first;
                        } else {
                            target = first;
                        }
                    }
                    self.expect_op(")")?;
                    let mut args = vec![target];
                    args.extend(chars);
                    return Ok(func(mode, args));
                }
                "current_timestamp" | "current_date" | "current_time" | "localtimestamp" | "localtime" | "current_user"
                | "session_user" | "current_schema" | "current_catalog" | "current_role" | "user" | "utc_timestamp"
                | "sysdate" | "sysdatetime" | "getdate" | "getutcdate"
                    if !self.is_op("(") || matches!(self.peek_at(1), Some(Token::Op(")")) | Some(Token::Number(_))) =>
                {
                    if self.eat_op("(") {
                        if let Some(Token::Number(_)) = self.peek() {
                            self.pos += 1;
                        }
                        self.expect_op(")")?;
                    }
                    return Ok(func(&lower, Vec::new()));
                }
                _ => {}
            }
        }
        // Function call?
        if self.is_op("(") {
            return self.call(lower);
        }
        // Column reference a, a.b, a.b.c (or a qualified function call pg_catalog.f(...)).
        let mut parts = vec![self.fold(text, quoted)];
        while self.is_op(".") && !self.is_op_at(1, "*") {
            self.pos += 1;
            parts.push(self.ident()?);
        }
        if self.is_op("(") && parts.len() > 1 {
            let name = parts.pop().unwrap_or_default().to_ascii_lowercase();
            return self.call(name);
        }
        Ok(Expr::Column(parts))
    }

    /// A function call; the name has been read and `(` is next.
    fn call(&mut self, name: String) -> SqlResult<Expr> {
        {
            self.pos += 1;
            let mut args = Vec::new();
            let mut star = false;
            let mut distinct = false;
            if self.eat_op("*") {
                star = true;
            } else if !self.is_op(")") {
                distinct = self.eat_kw("distinct");
                if !distinct {
                    self.eat_kw("all");
                }
                loop {
                    args.push(self.expr()?);
                    if !self.eat_op(",") {
                        break;
                    }
                }
                if self.eat_kws(&["order", "by"]) {
                    // string_agg(x, ',' ORDER BY y): ordering inside aggregates is ignored.
                    self.order_list()?;
                }
                if self.eat_kw("separator") {
                    args.push(self.primary()?);
                }
            }
            self.expect_op(")")?;
            if self.eat_kw("filter") {
                return Err(SqlError::unsupported("aggregate FILTER (WHERE ...)"));
            }
            if self.eat_kws(&["within", "group"]) {
                return Err(SqlError::unsupported("WITHIN GROUP"));
            }
            let over = self.is_kw("over");
            if over {
                return Err(SqlError::unsupported("window functions (OVER)"));
            }
            Ok(Expr::Function { name, args, star, distinct, over })
        }
    }

    /// The type of a CAST: also accepts MySQL `SIGNED [INTEGER]`, `UNSIGNED`, `CHAR`.
    fn cast_type(&mut self) -> SqlResult<DataType> {
        if self.eat_kw("signed") || self.eat_kw("unsigned") {
            self.eat_kw("integer");
            self.eat_kw("int");
            return Ok(DataType::BigInt);
        }
        Ok(self.data_type()?.0)
    }

    fn case_expr(&mut self) -> SqlResult<Expr> {
        let operand = if self.is_kw("when") { None } else { Some(Box::new(self.expr()?)) };
        let mut whens = Vec::new();
        while self.eat_kw("when") {
            let cond = self.expr()?;
            self.expect_kw("then")?;
            let value = self.expr()?;
            whens.push((cond, value));
        }
        if whens.is_empty() {
            return Err(self.unexpected("WHEN"));
        }
        let else_ = if self.eat_kw("else") { Some(Box::new(self.expr()?)) } else { None };
        self.expect_kw("end")?;
        Ok(Expr::Case { operand, whens, else_ })
    }
}

fn func(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function { name: name.to_owned(), args, star: false, distinct: false, over: false }
}

fn clamp_u8(n: u32) -> u8 {
    n.min(255) as u8
}

fn is_interval_unit(u: &str) -> bool {
    matches!(
        u.trim_end_matches('s'),
        "microsecond" | "millisecond" | "second" | "minute" | "hour" | "day" | "week" | "month" | "quarter" | "year"
    )
}

/// Words that begin a column type (to tell `key text` columns from `KEY idx (...)` clauses).
fn type_word(t: &str) -> bool {
    matches!(
        t,
        "bool" | "boolean" | "bit" | "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "int2" | "int4"
            | "int8" | "serial" | "bigserial" | "smallserial" | "decimal" | "numeric" | "real" | "float" | "double"
            | "float4" | "float8" | "text" | "varchar" | "char" | "character" | "nvarchar" | "nchar" | "bytea" | "blob"
            | "binary" | "varbinary" | "timestamp" | "timestamptz" | "datetime" | "date" | "time" | "uuid" | "json"
            | "jsonb" | "enum" | "longtext" | "mediumtext" | "tinytext" | "money" | "year" | "interval"
    )
}

fn static_tag(kw: &str) -> &'static str {
    match kw {
        "discard" => "DISCARD ALL",
        "listen" => "LISTEN",
        "unlisten" => "UNLISTEN",
        "notify" => "NOTIFY",
        "reset" => "RESET",
        "lock" => "LOCK TABLE",
        "unlock" => "UNLOCK TABLES",
        "analyze" => "ANALYZE",
        "vacuum" => "VACUUM",
        "checkpoint" => "CHECKPOINT",
        "flush" => "FLUSH",
        "pragma" => "PRAGMA",
        "comment" => "COMMENT",
        "grant" => "GRANT",
        "revoke" => "REVOKE",
        "optimize" => "OPTIMIZE",
        "kill" => "KILL",
        _ => "OK",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(sql: &str, d: Dialect) -> Statement {
        parse_one(sql, d).unwrap_or_else(|e| panic!("{sql}: {e}")).0
    }

    #[test]
    fn parses_orm_style_statements() {
        let pg = Dialect::Postgres;
        let my = Dialect::MySql;
        for (sql, d) in [
            ("SELECT users.id AS users_id, users.name FROM users WHERE users.id = $1", pg),
            ("SELECT * FROM \"User\" u LEFT OUTER JOIN posts p ON p.user_id = u.id WHERE u.age > 18 AND p.title ILIKE '%x%' ORDER BY u.id DESC NULLS LAST LIMIT 10 OFFSET 5", pg),
            ("INSERT INTO users (name, email) VALUES ($1, $2), ($3, $4) RETURNING users.id", pg),
            ("INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b + 1 WHERE t.b < 5", pg),
            ("INSERT INTO `t` (`a`, `b`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `b` = VALUES(`b`)", my),
            ("UPDATE accounts SET balance = balance - 25.00 WHERE id = 'alice' AND balance >= 25", pg),
            ("DELETE FROM sessions WHERE expires_at < NOW() - INTERVAL '1 day'", pg),
            ("SELECT count(*), sum(amount), max(created_at) FROM orders GROUP BY customer_id HAVING count(*) > 2", pg),
            ("SELECT a FROM t WHERE b IN (SELECT b FROM u WHERE c IS NOT NULL) AND NOT EXISTS (SELECT 1 FROM v)", pg),
            ("SELECT CASE WHEN a > 1 THEN 'x' ELSE 'y' END, CAST(b AS NUMERIC(10, 2)), c::text FROM t", pg),
            ("CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, email VARCHAR(255) NOT NULL UNIQUE, balance NUMERIC(12,2) DEFAULT 0 CHECK (balance >= 0), created_at TIMESTAMP WITH TIME ZONE DEFAULT now(), org_id INTEGER REFERENCES orgs(id) ON DELETE CASCADE)", pg),
            ("CREATE TABLE `users` (`id` int(11) NOT NULL AUTO_INCREMENT, `key` varchar(20), `active` tinyint(1) DEFAULT 1, PRIMARY KEY (`id`), UNIQUE KEY `uk` (`key`), KEY `idx` (`active`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", my),
            ("BEGIN ISOLATION LEVEL SERIALIZABLE", pg),
            ("START TRANSACTION READ WRITE", my),
            ("SELECT * FROM t FOR UPDATE SKIP LOCKED", pg),
            ("SELECT * FROM t LIMIT 5, 10", my),
            ("SELECT 1 UNION ALL SELECT 2 UNION SELECT 3", pg),
            ("SET NAMES utf8mb4", my),
            ("SET search_path TO public, other", pg),
            ("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED", my),
            ("SHOW FULL COLUMNS FROM `t`", my),
            ("WITH x AS (SELECT 1 AS a) SELECT a FROM x", pg),
            ("SELECT n.nspname, c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = ANY(ARRAY['r','p']) AND pg_catalog.pg_table_is_visible(c.oid)", pg),
            ("ALTER TABLE users ADD COLUMN age INTEGER NOT NULL DEFAULT 0, DROP COLUMN legacy", pg),
            ("CREATE UNIQUE INDEX IF NOT EXISTS ix_email ON users (lower(email))", pg),
            ("SELECT TOP 10 * FROM t ORDER BY a", Dialect::MsSql),
            ("SELECT a FROM t ORDER BY a OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY", Dialect::MsSql),
            ("INSERT INTO t (a) OUTPUT INSERTED.id VALUES (@p1)", Dialect::MsSql),
            ("SELECT @@version, DATABASE(), CONCAT('a', 'b')", my),
            ("SELECT EXTRACT(YEAR FROM created_at), TRIM(BOTH ' ' FROM name), SUBSTRING(name FROM 2 FOR 3) FROM t", pg),
        ] {
            one(sql, d);
        }
    }

    #[test]
    fn precedence_and_folding() {
        let Statement::Query(q) = one("SELECT A + b * 2 = 7 OR NOT c AND d FROM T", Dialect::Postgres) else { panic!() };
        let SetExpr::Select(s) = q.body else { panic!() };
        let SelectItem::Expr { expr, .. } = &s.items[0] else { panic!() };
        // OR at the top, AND below it, = below AND, + below =, * below +.
        let Expr::Binary { op: BinaryOp::Or, left, right } = expr else { panic!("{expr:?}") };
        assert!(matches!(**right, Expr::Binary { op: BinaryOp::And, .. }));
        let Expr::Binary { op: BinaryOp::Eq, left: sum, .. } = &**left else { panic!() };
        let Expr::Binary { op: BinaryOp::Add, left: a, right: prod } = &**sum else { panic!() };
        assert_eq!(**a, Expr::Column(vec!["a".into()]));
        assert!(matches!(**prod, Expr::Binary { op: BinaryOp::Mul, .. }));
        assert!(matches!(&s.from[0], TableRef::Named { name, .. } if name == "t"));
        // MySQL keeps identifier case.
        let Statement::Query(q) = one("SELECT A FROM T", Dialect::MySql) else { panic!() };
        let SetExpr::Select(s) = q.body else { panic!() };
        assert!(matches!(&s.from[0], TableRef::Named { name, .. } if name == "T"));
    }

    #[test]
    fn multiple_statements_and_errors() {
        let (stmts, n) = parse("BEGIN; UPDATE t SET a = $2 WHERE id = $1; COMMIT;", Dialect::Postgres).unwrap();
        assert_eq!((stmts.len(), n), (3, 2));
        assert!(parse_one("SELECT FROM WHERE", Dialect::Postgres).is_err());
        assert_eq!(parse_one("SELEC 1", Dialect::Postgres).unwrap_err().code, "0A000");
        assert_eq!(parse_one("SELECT a FROM t WHERE", Dialect::Postgres).unwrap_err().code, "42601");
        assert_eq!(parse_one("SELECT sum(a) OVER () FROM t", Dialect::Postgres).unwrap_err().code, "0A000");
        assert_eq!(one("", Dialect::Postgres), Statement::Empty);
    }
}
