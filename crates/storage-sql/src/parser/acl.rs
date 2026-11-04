//! Access-control statements (`TASK-145`): roles and users, `GRANT` / `REVOKE` / `DENY` and
//! row-level security policies, in PostgreSQL, MySQL and T-SQL syntax.

use super::Parser;
use crate::ast::*;
use crate::error::{SqlError, SqlResult};
use crate::lexer::{Dialect, Token};

impl Parser {
    fn string(&mut self) -> SqlResult<String> {
        match self.peek().cloned() {
            Some(Token::Str(s)) => {
                self.pos += 1;
                Ok(s)
            }
            _ => Err(self.unexpected("a string")),
        }
    }

    /// A role name: `name`, `'name'`, and in MySQL `'name'@'host'`. Host patterns other than
    /// `%` are refused: accounts are not tied to client addresses here, so honouring the name and
    /// ignoring the restriction would grant more than was asked.
    pub(super) fn role_name(&mut self) -> SqlResult<String> {
        let name = match self.peek().cloned() {
            Some(Token::Str(s)) => {
                self.pos += 1;
                s
            }
            _ => self.ident()?,
        };
        if self.dialect == Dialect::MySql {
            let host = match self.peek().cloned() {
                Some(Token::Op("@")) => {
                    self.pos += 1;
                    match self.peek().cloned() {
                        Some(Token::Str(h)) => {
                            self.pos += 1;
                            Some(h)
                        }
                        _ => Some(self.ident()?),
                    }
                }
                // The lexer reads `@host` (and `@'host'` in some forms) as a variable.
                Some(Token::Variable { name: h, system: false }) => {
                    self.pos += 1;
                    Some(h)
                }
                _ => None,
            };
            if let Some(h) = host {
                if h != "%" {
                    return Err(SqlError::unsupported(format!("accounts restricted to a host ('{name}'@'{h}'): use '{name}'@'%'")));
                }
            }
        }
        Ok(name)
    }

    fn role_names(&mut self) -> SqlResult<Vec<String>> {
        let mut out = vec![self.role_name()?];
        while self.eat_op(",") {
            out.push(self.role_name()?);
        }
        Ok(out)
    }

    /// PostgreSQL role options, after `CREATE ROLE name [WITH]` or `ALTER ROLE name [WITH]`.
    fn role_options(&mut self) -> SqlResult<Vec<RoleOpt>> {
        let _ = self.eat_kw("with");
        let mut opts = Vec::new();
        while !self.statement_end() {
            let Some(k) = self.peek().and_then(Token::keyword) else {
                return Err(self.unexpected("a role option"));
            };
            self.pos += 1;
            opts.push(match k.as_str() {
                "login" => RoleOpt::Login(true),
                "nologin" => RoleOpt::Login(false),
                "superuser" => RoleOpt::Superuser(true),
                "nosuperuser" => RoleOpt::Superuser(false),
                "createrole" => RoleOpt::CreateRole(true),
                "nocreaterole" => RoleOpt::CreateRole(false),
                "createdb" => RoleOpt::CreateDb(true),
                "nocreatedb" => RoleOpt::CreateDb(false),
                "bypassrls" => RoleOpt::BypassRls(true),
                "nobypassrls" => RoleOpt::BypassRls(false),
                // Accepted without effect: every role inherits, nothing replicates through roles.
                "inherit" | "noinherit" | "replication" | "noreplication" => continue,
                "connection" => {
                    self.expect_kw("limit")?;
                    self.pos += 1;
                    continue;
                }
                "valid" => {
                    self.expect_kw("until")?;
                    return Err(SqlError::unsupported("VALID UNTIL on roles"));
                }
                "encrypted" | "password" => {
                    if k == "encrypted" {
                        self.expect_kw("password")?;
                    }
                    if self.eat_kw("null") {
                        RoleOpt::Password(None)
                    } else {
                        RoleOpt::Password(Some(self.string()?))
                    }
                }
                "in" => {
                    self.expect_kw("role")?;
                    RoleOpt::InRole(self.role_names()?)
                }
                "role" | "user" => RoleOpt::Members(self.role_names()?),
                "admin" => RoleOpt::Admins(self.role_names()?),
                other => return Err(SqlError::syntax(format!("unknown role option {other}"))),
            });
        }
        Ok(opts)
    }

    /// After `CREATE`: `ROLE`, `USER`, `LOGIN`, `POLICY`; `None` if it is none of them.
    pub(super) fn create_access(&mut self) -> SqlResult<Option<Statement>> {
        if self.eat_kw("policy") {
            return self.create_policy().map(Some);
        }
        if self.eat_kws(&["security", "policy"]) {
            return Err(SqlError::unsupported("CREATE SECURITY POLICY: use CREATE POLICY name ON table [FOR command] [TO roles] USING (predicate) [WITH CHECK (predicate)]"));
        }
        let login = self.is_kw("login");
        if !(login || self.is_kw("role") || self.is_kw("user") || self.is_kw("group")) {
            return Ok(None);
        }
        let is_user = self.is_kw("user");
        self.pos += 1;
        // A T-SQL user is the login's other half: creating it after the login changes nothing.
        let if_not_exists = self.eat_kws(&["if", "not", "exists"]) || (is_user && self.dialect == Dialect::MsSql);
        let names = self.role_names()?;
        let name = names.first().cloned().unwrap_or_default();
        let mut opts = Vec::new();
        match self.dialect {
            Dialect::MsSql => {
                // `CREATE LOGIN n WITH PASSWORD = 'p' [, ...]`, `CREATE USER n [FOR LOGIN l |
                // WITHOUT LOGIN | WITH PASSWORD = 'p']`, `CREATE ROLE n [AUTHORIZATION o]`.
                if login || is_user {
                    opts.push(RoleOpt::Login(true));
                }
                if self.eat_kw("from") {
                    return Err(SqlError::unsupported("CREATE LOGIN ... FROM: logins take a password here"));
                }
                if self.eat_kws(&["for", "login"]) || self.eat_kws(&["from", "login"]) {
                    let l = self.ident()?;
                    if !l.eq_ignore_ascii_case(&name) {
                        return Err(SqlError::unsupported("a database user named differently from its login: a login and its user are one role here"));
                    }
                } else if self.eat_kws(&["without", "login"]) {
                    opts = vec![RoleOpt::Login(false)];
                } else if self.eat_kw("authorization") {
                    self.ident()?;
                }
                if self.eat_kw("with") {
                    loop {
                        if self.eat_kw("password") {
                            self.expect_op("=")?;
                            opts.push(RoleOpt::Password(Some(self.string()?)));
                            let _ = self.eat_kw("must_change");
                        } else {
                            // CHECK_POLICY, DEFAULT_DATABASE, ...: nothing here depends on them.
                            self.pos += 1;
                            if self.eat_op("=") {
                                self.pos += 1;
                            }
                        }
                        if !self.eat_op(",") {
                            break;
                        }
                    }
                }
            }
            Dialect::MySql => {
                if is_user {
                    opts.push(RoleOpt::Login(true));
                }
                if self.eat_kw("identified") {
                    opts.push(self.mysql_identified()?);
                }
                // REQUIRE, PASSWORD EXPIRE, ACCOUNT LOCK, ...: refused rather than ignored.
                if !self.statement_end() {
                    return Err(SqlError::unsupported("account options other than IDENTIFIED"));
                }
            }
            _ => {
                if is_user {
                    opts.push(RoleOpt::Login(true));
                }
                opts.extend(self.role_options()?);
            }
        }
        Ok(Some(Statement::Role(Box::new(RoleStmt::Create { name, if_not_exists, opts }))))
    }

    /// `[WITH plugin] BY 'password'` or `WITH plugin AS 'hash'`.
    fn mysql_identified(&mut self) -> SqlResult<RoleOpt> {
        if self.eat_kw("with") {
            let plugin = self.string().or_else(|_| self.ident())?;
            if !plugin.eq_ignore_ascii_case("mysql_native_password") && !plugin.eq_ignore_ascii_case("caching_sha2_password") {
                return Err(SqlError::unsupported(format!("authentication plugin {plugin}")));
            }
            if self.eat_kw("as") {
                let hash = self.string()?;
                if !plugin.eq_ignore_ascii_case("mysql_native_password") {
                    return Err(SqlError::unsupported("password hashes of plugins other than mysql_native_password"));
                }
                return Ok(RoleOpt::Password(Some(hash)));
            }
        }
        self.expect_kw("by")?;
        Ok(RoleOpt::Password(Some(self.string()?)))
    }

    /// After `DROP`: `ROLE`, `USER`, `LOGIN`, `GROUP`, `POLICY`, `OWNED`.
    pub(super) fn drop_access(&mut self) -> SqlResult<Option<Statement>> {
        if self.eat_kw("policy") {
            let if_exists = self.eat_kws(&["if", "exists"]);
            let name = self.ident()?;
            self.expect_kw("on")?;
            let table = self.table_name()?;
            return Ok(Some(Statement::DropPolicy { name, table, if_exists }));
        }
        if self.eat_kws(&["owned", "by"]) {
            let role = self.role_name()?;
            let _ = self.eat_kw("cascade") || self.eat_kw("restrict");
            return Ok(Some(Statement::Role(Box::new(RoleStmt::DropOwned { role }))));
        }
        if !(self.eat_kw("role") || self.eat_kw("user") || self.eat_kw("login") || self.eat_kw("group")) {
            return Ok(None);
        }
        let if_exists = self.eat_kws(&["if", "exists"]);
        let names = self.role_names()?;
        Ok(Some(Statement::Role(Box::new(RoleStmt::Drop { names, if_exists }))))
    }

    /// After `ALTER`: roles, default privileges and policies.
    pub(super) fn alter_access(&mut self) -> SqlResult<Option<Statement>> {
        if self.eat_kw("policy") {
            let name = self.ident()?;
            self.expect_kw("on")?;
            let table = self.table_name()?;
            if self.eat_kws(&["rename", "to"]) {
                let to = self.ident()?;
                return Ok(Some(Statement::AlterPolicy { name, table, rename: Some(to), roles: None, using: None, check: None }));
            }
            let roles = if self.eat_kw("to") { Some(self.role_names()?) } else { None };
            let (using, check) = self.policy_predicates()?;
            return Ok(Some(Statement::AlterPolicy { name, table, rename: None, roles, using, check }));
        }
        if self.eat_kws(&["default", "privileges"]) {
            let mut for_role = None;
            if self.eat_kw("for") {
                let _ = self.eat_kw("role") || self.eat_kw("user");
                for_role = Some(self.role_name()?);
            }
            if self.eat_kws(&["in", "schema"]) {
                let schema = self.ident()?;
                self.check_flat_schema(&schema)?;
            }
            let revoke = self.is_kw("revoke");
            if !(self.eat_kw("grant") || self.eat_kw("revoke")) {
                return Err(self.unexpected("GRANT or REVOKE"));
            }
            let mut g = self.grant_body(if revoke { GrantKind::Revoke } else { GrantKind::Grant })?;
            g.default_for = Some(for_role.unwrap_or_default());
            return Ok(Some(Statement::Grant(Box::new(g))));
        }
        if self.is_kw("server") && self.is_kw_at(1, "role") {
            self.pos += 2;
            let role = self.ident()?;
            return self.role_membership_change(role).map(Some);
        }
        if !(self.eat_kw("role") || self.eat_kw("user") || self.eat_kw("login") || self.eat_kw("group")) {
            return Ok(None);
        }
        let _ = self.eat_kws(&["if", "exists"]);
        let name = self.role_name()?;
        if self.is_kw("add") || self.is_kw("drop") {
            // PostgreSQL `ALTER GROUP g ADD USER u`, T-SQL `ALTER ROLE r ADD MEMBER u`.
            return self.role_membership_change(name).map(Some);
        }
        if self.eat_kws(&["rename", "to"]) {
            let to = self.role_name()?;
            return Ok(Some(Statement::Role(Box::new(RoleStmt::Rename { from: name, to }))));
        }
        let opts = match self.dialect {
            Dialect::MsSql => {
                if self.eat_kw("enable") {
                    vec![RoleOpt::Login(true)]
                } else if self.eat_kw("disable") {
                    vec![RoleOpt::Login(false)]
                } else {
                    self.expect_kw("with")?;
                    if self.eat_kw("name") {
                        self.expect_op("=")?;
                        let to = self.ident()?;
                        return Ok(Some(Statement::Role(Box::new(RoleStmt::Rename { from: name, to }))));
                    }
                    self.expect_kw("password")?;
                    self.expect_op("=")?;
                    let p = self.string()?;
                    self.skip_to_end();
                    vec![RoleOpt::Password(Some(p))]
                }
            }
            Dialect::MySql => {
                self.expect_kw("identified")?;
                let o = self.mysql_identified()?;
                if !self.statement_end() {
                    return Err(SqlError::unsupported("account options other than IDENTIFIED"));
                }
                vec![o]
            }
            _ => {
                if self.is_kw("set") || self.is_kw("reset") {
                    return Err(SqlError::unsupported("per-role settings (ALTER ROLE ... SET)"));
                }
                self.role_options()?
            }
        };
        Ok(Some(Statement::Role(Box::new(RoleStmt::Alter { name, opts }))))
    }

    fn role_membership_change(&mut self, role: String) -> SqlResult<Statement> {
        let add = self.is_kw("add");
        if !(self.eat_kw("add") || self.eat_kw("drop")) {
            return Err(self.unexpected("ADD or DROP"));
        }
        if !(self.eat_kw("member") || self.eat_kw("user")) {
            return Err(self.unexpected("MEMBER"));
        }
        let members = self.role_names()?;
        Ok(Statement::Role(Box::new(if add {
            RoleStmt::AddMembers { roles: vec![role], members, admin: false }
        } else {
            RoleStmt::DropMembers { roles: vec![role], members }
        })))
    }

    /// `SET ROLE`, `SET SESSION AUTHORIZATION`, `SET PASSWORD`, `SET DEFAULT ROLE`.
    pub(super) fn set_access(&mut self) -> SqlResult<Option<Statement>> {
        let _ = (self.is_kw("session") || self.is_kw("local")) && self.is_kw_at(1, "role") && self.eat_kw("session") | self.eat_kw("local");
        if self.eat_kw("role") {
            if self.dialect == Dialect::MySql && self.eat_kw("all") {
                // Every granted role is active here anyway.
                self.skip_to_end();
                return Ok(Some(Statement::NoOp("SET")));
            }
            let none = self.eat_kw("none") || self.eat_kw("default");
            let role = if none { None } else { Some(self.role_name()?) };
            return Ok(Some(Statement::Role(Box::new(RoleStmt::SetRole(role)))));
        }
        if self.eat_kws(&["session", "authorization"]) {
            if self.eat_kw("default") {
                return Ok(Some(Statement::Role(Box::new(RoleStmt::SetRole(None)))));
            }
            let role = self.role_name()?;
            return Ok(Some(Statement::Role(Box::new(RoleStmt::SetRole(Some(role))))));
        }
        if self.dialect == Dialect::MySql && self.eat_kws(&["default", "role"]) {
            self.skip_to_end();
            return Ok(Some(Statement::NoOp("SET")));
        }
        if self.dialect == Dialect::MySql && self.eat_kw("password") {
            let name = if self.eat_kw("for") { Some(self.role_name()?) } else { None };
            if !self.eat_op("=") && !self.eat_kw("to") {
                return Err(self.unexpected("="));
            }
            let password = self.string()?;
            return Ok(Some(Statement::Role(Box::new(RoleStmt::SetPassword { name, password }))));
        }
        Ok(None)
    }

    /// `REASSIGN OWNED BY a TO b`.
    pub(super) fn reassign(&mut self) -> SqlResult<Statement> {
        self.pos += 1;
        self.expect_kw("owned")?;
        self.expect_kw("by")?;
        let from = self.role_name()?;
        self.expect_kw("to")?;
        let to = self.role_name()?;
        Ok(Statement::Role(Box::new(RoleStmt::ReassignOwned { from, to })))
    }

    /// Schemas are not namespaces here (every table lives in one flat name space), so naming
    /// another schema in a grant would silently cover tables it was never meant to.
    fn check_flat_schema(&self, schema: &str) -> SqlResult<()> {
        if schema.eq_ignore_ascii_case("public") || schema.eq_ignore_ascii_case("dbo") {
            return Ok(());
        }
        Err(SqlError::unsupported(format!("privileges on schema \"{schema}\": schemas other than public / dbo do not separate tables here, so this would cover every table; grant on the tables")))
    }

    /// `GRANT`, `REVOKE` and `DENY`.
    pub(super) fn grant(&mut self) -> SqlResult<Statement> {
        let kind = match self.peek().and_then(Token::keyword).as_deref() {
            Some("grant") => GrantKind::Grant,
            Some("revoke") => GrantKind::Revoke,
            _ => GrantKind::Deny,
        };
        self.pos += 1;
        self.grant_body(kind).map(|g| Statement::Grant(Box::new(g)))
    }

    /// One privilege and its columns. Keywords of the privilege end at `(`, `,` or `ON`.
    fn privilege(&mut self) -> SqlResult<(String, Vec<String>)> {
        // MySQL role names may be quoted strings.
        if let Some(Token::Str(role)) = self.peek().cloned() {
            self.pos += 1;
            return Ok((role, Vec::new()));
        }
        let mut words = Vec::new();
        loop {
            match self.peek().cloned() {
                Some(Token::Word { text, .. }) if !text.eq_ignore_ascii_case("on") && !text.eq_ignore_ascii_case("to") && !text.eq_ignore_ascii_case("from") => {
                    words.push(text.to_ascii_uppercase());
                    self.pos += 1;
                }
                _ => break,
            }
        }
        if words.is_empty() {
            return Err(self.unexpected("a privilege"));
        }
        let mut name = words.join(" ");
        if name == "ALL PRIVILEGES" {
            name = "ALL".into();
        }
        let columns = if self.is_op("(") { self.ident_list()? } else { Vec::new() };
        Ok((name, columns))
    }

    fn grant_body(&mut self, kind: GrantKind) -> SqlResult<GrantStmt> {
        let option_only = kind == GrantKind::Revoke && self.eat_kws(&["grant", "option", "for"]);
        let admin_only = kind == GrantKind::Revoke && self.eat_kws(&["admin", "option", "for"]);
        let mut privileges = vec![self.privilege()?];
        while self.eat_op(",") {
            privileges.push(self.privilege()?);
        }
        let mut object = GrantObject::Roles;
        if self.eat_kw("on") {
            object = self.grant_object()?;
        }
        // T-SQL column list after the object: `GRANT SELECT ON t (a, b) TO u`.
        if self.is_op("(") {
            let cols = self.ident_list()?;
            for p in &mut privileges {
                p.1.extend(cols.iter().cloned());
            }
        }
        if !(self.eat_kw("to") || self.eat_kw("from")) {
            return Err(self.unexpected("TO or FROM"));
        }
        let mut grantees = Vec::new();
        loop {
            let _ = self.eat_kw("group");
            grantees.push(self.role_name()?);
            if !self.eat_op(",") {
                break;
            }
        }
        let grant_option = self.eat_kws(&["with", "grant", "option"]);
        let admin = self.eat_kws(&["with", "admin", "option"]) || admin_only;
        let cascade = self.eat_kw("cascade");
        let _ = self.eat_kw("restrict");
        // T-SQL `AS principal`, PostgreSQL `GRANTED BY role`: acting for someone else is refused.
        if self.is_kw("as") || self.is_kw("granted") {
            return Err(SqlError::unsupported("granting on behalf of another role (AS / GRANTED BY)"));
        }
        if !self.statement_end() {
            return Err(self.unexpected("the end of the statement"));
        }
        Ok(GrantStmt { kind, privileges, object, grantees, grant_option, option_only, admin, cascade, default_for: None })
    }

    fn grant_object(&mut self) -> SqlResult<GrantObject> {
        // MySQL: `*.*`, `db.*`, `*`, `db.t`, `t`.
        if self.dialect == Dialect::MySql {
            if self.eat_op("*") {
                if self.eat_op(".") {
                    self.expect_op("*")?;
                    return Ok(GrantObject::Server);
                }
                return Ok(GrantObject::Database(None));
            }
            let _ = self.eat_kw("table");
            let first = self.ident()?;
            if self.eat_op(".") {
                if self.eat_op("*") {
                    return Ok(GrantObject::Database(Some(first)));
                }
                let t = self.ident()?;
                return Ok(GrantObject::Tables(vec![t]));
            }
            return Ok(GrantObject::Tables(vec![first]));
        }
        // T-SQL: `SCHEMA::dbo`, `OBJECT::dbo.t`, `DATABASE::db`.
        if self.dialect == Dialect::MsSql && (self.is_kw("schema") || self.is_kw("object") || self.is_kw("database")) {
            let which = self.peek().and_then(Token::keyword).unwrap_or_default();
            self.pos += 1;
            if !self.eat_op("::") {
                return Err(self.unexpected("\"::\""));
            }
            let (_, name) = self.object_name()?;
            return match which.as_str() {
                "schema" => {
                    self.check_flat_schema(&name)?;
                    Ok(GrantObject::Database(None))
                }
                "database" => Ok(GrantObject::Database(Some(name))),
                _ => Ok(GrantObject::Tables(vec![name])),
            };
        }
        let list = |p: &mut Self| -> SqlResult<Vec<String>> {
            let mut v = vec![p.table_name()?];
            while p.eat_op(",") {
                v.push(p.table_name()?);
            }
            Ok(v)
        };
        if self.eat_kw("all") {
            // ALL TABLES / SEQUENCES / FUNCTIONS IN SCHEMA s.
            let what = self.peek().and_then(Token::keyword).unwrap_or_default();
            self.pos += 1;
            self.expect_kw("in")?;
            self.expect_kw("schema")?;
            let mut schema = self.ident()?;
            self.check_flat_schema(&schema)?;
            while self.eat_op(",") {
                schema = self.ident()?;
                self.check_flat_schema(&schema)?;
            }
            return match what.as_str() {
                "tables" => Ok(GrantObject::AllTables),
                "sequences" => Ok(GrantObject::AllSequences),
                "functions" | "procedures" | "routines" => Ok(GrantObject::AllFunctions),
                _ => Err(self.unexpected("TABLES, SEQUENCES or FUNCTIONS")),
            };
        }
        if self.eat_kw("tables") {
            return Ok(GrantObject::DefaultTables);
        }
        if self.eat_kw("sequences") {
            return Ok(GrantObject::DefaultSequences);
        }
        if self.eat_kw("functions") || self.eat_kw("routines") || self.eat_kw("types") || self.eat_kw("schemas") {
            return Ok(GrantObject::AllFunctions);
        }
        if self.eat_kw("sequence") {
            return list(self).map(GrantObject::Sequences);
        }
        if self.eat_kw("database") {
            let name = self.ident()?;
            return Ok(GrantObject::Database(Some(name)));
        }
        if self.eat_kw("schema") {
            let mut s = self.ident()?;
            self.check_flat_schema(&s)?;
            while self.eat_op(",") {
                s = self.ident()?;
                self.check_flat_schema(&s)?;
            }
            return Ok(GrantObject::Database(None));
        }
        if self.eat_kw("function") || self.eat_kw("procedure") || self.eat_kw("routine") {
            let (_, name) = self.object_name()?;
            if self.is_op("(") {
                // The argument types: names are unique per function here.
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
            }
            return Ok(GrantObject::Functions(vec![name]));
        }
        let _ = self.eat_kw("table");
        list(self).map(GrantObject::Tables)
    }

    fn policy_predicates(&mut self) -> SqlResult<(Option<Expr>, Option<Expr>)> {
        let mut using = None;
        let mut check = None;
        if self.eat_kw("using") {
            self.expect_op("(")?;
            using = Some(self.expr()?);
            self.expect_op(")")?;
        }
        if self.eat_kws(&["with", "check"]) {
            self.expect_op("(")?;
            check = Some(self.expr()?);
            self.expect_op(")")?;
        }
        Ok((using, check))
    }

    fn create_policy(&mut self) -> SqlResult<Statement> {
        let name = self.ident()?;
        self.expect_kw("on")?;
        let table = self.table_name()?;
        let mut permissive = true;
        if self.eat_kw("as") {
            if self.eat_kw("restrictive") {
                permissive = false;
            } else {
                self.expect_kw("permissive")?;
            }
        }
        let mut command = "all".to_owned();
        if self.eat_kw("for") {
            let c = self.peek().and_then(Token::keyword).unwrap_or_default();
            if !matches!(c.as_str(), "all" | "select" | "insert" | "update" | "delete") {
                return Err(self.unexpected("ALL, SELECT, INSERT, UPDATE or DELETE"));
            }
            self.pos += 1;
            command = c;
        }
        let roles = if self.eat_kw("to") { self.role_names()? } else { Vec::new() };
        let (using, check) = self.policy_predicates()?;
        if command == "insert" && using.is_some() {
            return Err(SqlError::syntax("only WITH CHECK expression allowed for INSERT".into()));
        }
        if matches!(command.as_str(), "select" | "delete") && check.is_some() {
            return Err(SqlError::syntax("WITH CHECK cannot be applied to SELECT or DELETE".into()));
        }
        Ok(Statement::CreatePolicy(Box::new(CreatePolicy { name, table, permissive, command, roles, using, check })))
    }

    /// `DECLARE name [BINARY] [INSENSITIVE | ASENSITIVE] [[NO] SCROLL] CURSOR [WITH[OUT] HOLD] FOR query`.
    pub(super) fn declare_cursor(&mut self) -> SqlResult<Statement> {
        self.pos += 1;
        let name = self.ident()?;
        while !self.eat_kw("cursor") {
            if !(self.eat_kw("binary") || self.eat_kw("insensitive") || self.eat_kw("asensitive") || self.eat_kw("scroll") || self.eat_kw("no")) {
                return Err(self.unexpected("CURSOR"));
            }
        }
        let mut hold = false;
        if self.eat_kws(&["with", "hold"]) {
            hold = true;
        } else {
            let _ = self.eat_kws(&["without", "hold"]);
        }
        self.expect_kw("for")?;
        let query = Box::new(self.query()?);
        Ok(Statement::DeclareCursor { name, query, hold })
    }

    /// `FETCH | MOVE [direction] [FROM | IN] name`, T-SQL `... INTO @a, @b`.
    pub(super) fn fetch_cursor(&mut self, move_only: bool) -> SqlResult<Statement> {
        self.pos += 1;
        let number = |p: &mut Self| -> Option<i64> {
            let neg = p.eat_op("-");
            match p.peek().cloned() {
                Some(Token::Number(n)) => {
                    p.pos += 1;
                    n.parse::<i64>().ok().map(|x| if neg { -x } else { x })
                }
                Some(Token::Variable { .. }) => None,
                _ => None,
            }
        };
        let direction = if self.eat_kw("next") {
            FetchDirection::Next
        } else if self.eat_kw("prior") {
            FetchDirection::Prior
        } else if self.eat_kw("first") {
            FetchDirection::First
        } else if self.eat_kw("last") {
            FetchDirection::Last
        } else if self.eat_kw("absolute") {
            FetchDirection::Absolute(number(self).ok_or_else(|| self.unexpected("a count"))?)
        } else if self.eat_kw("relative") {
            FetchDirection::Relative(number(self).ok_or_else(|| self.unexpected("a count"))?)
        } else if self.eat_kw("all") {
            FetchDirection::ForwardAll
        } else if self.eat_kw("forward") {
            if self.eat_kw("all") {
                FetchDirection::ForwardAll
            } else {
                FetchDirection::Forward(number(self).unwrap_or(1))
            }
        } else if self.eat_kw("backward") {
            if self.eat_kw("all") {
                FetchDirection::BackwardAll
            } else {
                FetchDirection::Backward(number(self).unwrap_or(1))
            }
        } else {
            match number(self) {
                Some(n) if n < 0 => FetchDirection::Backward(-n),
                Some(n) => FetchDirection::Forward(n),
                None => FetchDirection::Next,
            }
        };
        let _ = self.eat_kw("from") || self.eat_kw("in") || self.eat_kw("global");
        let _ = self.eat_kw("global");
        let name = self.ident()?;
        let mut into = Vec::new();
        if self.dialect == Dialect::MsSql && self.eat_kw("into") {
            loop {
                match self.next() {
                    Some(Token::Variable { name, system: false }) => into.push(name),
                    _ => return Err(self.unexpected("a variable")),
                }
                if !self.eat_op(",") {
                    break;
                }
            }
        }
        Ok(Statement::Fetch { name, direction, move_only, into })
    }

    /// After `CREATE MATERIALIZED VIEW`.
    pub(super) fn create_matview(&mut self) -> SqlResult<Statement> {
        let if_not_exists = self.eat_kws(&["if", "not", "exists"]);
        let name = self.table_name()?;
        let columns = if self.is_op("(") { self.ident_list()? } else { Vec::new() };
        self.expect_kw("as")?;
        let from = self.pos;
        let query = Box::new(self.query()?);
        let sql = super::tokens_sql(&self.tokens[from..self.pos], self.dialect)?;
        let mut with_data = true;
        if self.eat_kw("with") {
            with_data = !self.eat_kw("no");
            self.expect_kw("data")?;
        }
        Ok(Statement::CreateMatView { name, columns, query, sql, with_data, if_not_exists })
    }
}
