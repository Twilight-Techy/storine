//! Access control (`F-ENT-01`, `TASK-145`): roles, grants and row-level security, enforced on every
//! statement in all three dialects.
//!
//! **Roles** are global documents (`_sql_roles`) holding verifiers, never passwords: a SCRAM-SHA-256
//! verifier and the MySQL native hash. Who is a superuser:
//! - with `--user` accounts, exactly those bootstrap accounts and roles marked `SUPERUSER`;
//! - without any (trust mode, where a client may claim any name, so grants cannot bind it), every
//!   name that is not a role; a role keeps its own attributes, which lets a trusted deployment or
//!   test drop privileges with `CREATE ROLE`.
//!
//! **Grants** are per-database documents (`_sql_grants`), one per grantee, object, privilege and
//! column. A check takes the role's closure over memberships plus `public`:
//! superuser, then owner, then `DENY`, then grants on the table, its columns, the database
//! (MySQL `db.*`, T-SQL `SCHEMA::dbo`) and the gateway (MySQL `*.*`), then the fixed roles
//! (`pg_read_all_data`, `db_datareader`, ...).
//!
//! **Where it is enforced.** Not by walking statements (a missed node would be a hole) but where
//! rows are reached:
//! - every table scan in `select.rs` and the DML target scans ask [`Exec::read_restriction`], and
//!   every fast path that reaches storage directly backs off when a restriction applies;
//! - columns a role may not read are marked on their `ColMeta`, so resolving one fails
//!   (`find_column`), whichever clause it appears in;
//! - with a restriction no filter is pushed down: a pushed-down filter on a hidden column would
//!   answer "any rows?" about values the role may not see.
//!
//! **Row-level security** applies to everyone but superusers, `BYPASSRLS` roles and the table's
//! owner (unless `FORCE`), as in PostgreSQL: permissive policies are or-ed (none: no rows),
//! restrictive ones and-ed. A view runs with its owner's rights.
//!
//! **Across gateways**: the cache lives for [`ACL_TTL`], so a revoke made on one gateway binds
//! others after at most that long (immediately on its own gateway).

use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use storine::{Doc, Value};

use crate::ast::{Expr, GrantKind, GrantObject, GrantStmt, RoleOpt, RoleStmt};
use crate::catalog::{Policy, Row, Table};
use crate::error::{SqlError, SqlResult};
use crate::exec::eval::{Env, Exec};
use crate::exec::select::{table_cols, ColMeta};
use crate::exec::{Engine, Outcome, Session};
use crate::hash;
use crate::lexer::Dialect;
use crate::store::{Op, Pre, Tag, Write};

pub const ROLES: &str = "_sql_roles";
pub const GRANTS: &str = "_sql_grants";
/// How long one gateway may act on a role or grant another gateway has changed.
pub const ACL_TTL: Duration = Duration::from_secs(2);
const SCRAM_ITERATIONS: u32 = 4096;

const TABLE_PRIVS: [&str; 7] = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"];
/// What only a table's owner can do in PostgreSQL, and MySQL and T-SQL can grant.
const DDL_PRIVS: [&str; 4] = ["ALTER", "DROP", "INDEX", "CREATE"];

/// A SCRAM-SHA-256 verifier.
#[derive(Debug, Clone, PartialEq)]
pub struct Scram {
    pub salt: Vec<u8>,
    pub iterations: u32,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
}

impl Scram {
    pub fn from_password(password: &str) -> Scram {
        let salt = hash::random_bytes(16);
        Self::derive(password, salt, SCRAM_ITERATIONS)
    }

    /// The verifier of `password` under a known salt (also how a plaintext login is checked).
    pub fn derive(password: &str, salt: Vec<u8>, iterations: u32) -> Scram {
        let salted = hash::pbkdf2_sha256(password.as_bytes(), &salt, iterations);
        let stored_key = hash::sha256(&hash::hmac_sha256(&salted, b"Client Key")).to_vec();
        let server_key = hash::hmac_sha256(&salted, b"Server Key").to_vec();
        Scram { salt, iterations, stored_key, server_key }
    }

    /// `SCRAM-SHA-256$<iterations>:<salt>$<stored>:<server>`, as `pg_dump` writes it.
    pub fn parse(text: &str) -> Option<Scram> {
        let rest = text.strip_prefix("SCRAM-SHA-256$")?;
        let (head, keys) = rest.split_once('$')?;
        let (iters, salt) = head.split_once(':')?;
        let (stored, server) = keys.split_once(':')?;
        Some(Scram { iterations: iters.parse().ok().filter(|i| *i > 0)?, salt: hash::base64_decode(salt)?, stored_key: hash::base64_decode(stored)?, server_key: hash::base64_decode(server)? })
    }

    pub fn text(&self) -> String {
        format!("SCRAM-SHA-256${}:{}${}:{}", self.iterations, hash::base64_encode(&self.salt), hash::base64_encode(&self.stored_key), hash::base64_encode(&self.server_key))
    }

    /// Whether `password` is the one this verifier was made from.
    pub fn check(&self, password: &str) -> bool {
        let d = Self::derive(password, self.salt.clone(), self.iterations);
        hash::ct_eq(&d.stored_key, &self.stored_key)
    }
}

/// A role, user or login.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Role {
    pub name: String,
    pub login: bool,
    pub superuser: bool,
    pub createrole: bool,
    pub createdb: bool,
    pub bypassrls: bool,
    /// Roles this one is a member of, with the right to add others.
    pub member_of: Vec<(String, bool)>,
    pub scram: Option<Scram>,
    /// `SHA1(SHA1(password))`, the MySQL native verifier.
    pub native: Option<Vec<u8>>,
    /// Privileges on every database (MySQL `*.*`).
    pub global: Vec<String>,
}

impl Role {
    fn to_doc(&self) -> Doc {
        let mut d = Doc::new();
        d.insert("name".into(), Value::Text(self.name.clone()));
        for (k, v) in [("login", self.login), ("superuser", self.superuser), ("createrole", self.createrole), ("createdb", self.createdb), ("bypassrls", self.bypassrls)] {
            d.insert(k.into(), Value::Bool(v));
        }
        let mut members = Doc::new();
        for (r, admin) in &self.member_of {
            members.insert(r.clone(), Value::Bool(*admin));
        }
        d.insert("member_of".into(), Value::Map(members));
        if let Some(s) = &self.scram {
            d.insert("scram".into(), Value::Text(s.text()));
        }
        if let Some(n) = &self.native {
            d.insert("native".into(), Value::Text(hash::hex(n)));
        }
        d.insert("global".into(), Value::List(self.global.iter().map(|p| Value::Text(p.clone())).collect()));
        d
    }

    fn from_doc(d: &Doc) -> Role {
        let flag = |k: &str| matches!(d.get(k), Some(Value::Bool(true)));
        Role {
            name: d.get("name").and_then(Value::as_str).unwrap_or_default().to_owned(),
            login: flag("login"),
            superuser: flag("superuser"),
            createrole: flag("createrole"),
            createdb: flag("createdb"),
            bypassrls: flag("bypassrls"),
            member_of: d.get("member_of").and_then(Value::as_map).map(|m| m.iter().map(|(k, v)| (k.clone(), matches!(v, Value::Bool(true)))).collect()).unwrap_or_default(),
            scram: d.get("scram").and_then(Value::as_str).and_then(Scram::parse),
            native: d.get("native").and_then(Value::as_str).and_then(unhex),
            global: match d.get("global") {
                Some(Value::List(l)) => l.iter().filter_map(Value::as_str).map(str::to_owned).collect(),
                _ => Vec::new(),
            },
        }
    }

    /// The verifiers of a password, or of a verifier handed in as such.
    fn set_password(&mut self, password: Option<&str>) {
        self.scram = None;
        self.native = None;
        let Some(p) = password else { return };
        if let Some(s) = Scram::parse(p) {
            self.scram = Some(s);
            return;
        }
        // `*` + 40 hex digits: a MySQL native hash.
        if let Some(n) = p.strip_prefix('*').filter(|h| h.len() == 40).and_then(unhex) {
            self.native = Some(n);
            return;
        }
        self.scram = Some(Scram::from_password(p));
        self.native = Some(hash::sha1(&hash::sha1(p.as_bytes())).to_vec());
    }
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok()).collect()
}

/// The key of a role name: names compare without case (`Bob` and `bob` are one role).
pub fn role_key(name: &str) -> String {
    name.to_ascii_lowercase()
}

/// One granted (or denied) privilege.
#[derive(Debug, Clone, PartialEq)]
pub struct GrantRow {
    pub grantee: String,
    /// `db`, `table:<name>`, `seq:<name>`, `func:<name>`, or `default:<tables|sequences>:<owner>`.
    pub object: String,
    pub privilege: String,
    /// Empty: the whole object.
    pub column: String,
    pub grant_option: bool,
    pub deny: bool,
    pub grantor: String,
}

impl GrantRow {
    fn key(&self) -> String {
        let id = format!("{}\u{1}{}\u{1}{}\u{1}{}\u{1}{}", self.grantee, self.object, self.privilege, self.column, self.deny);
        hash::hex(&hash::sha256(id.as_bytes()))
    }

    fn to_doc(&self) -> Doc {
        let mut d = Doc::new();
        for (k, v) in [("grantee", &self.grantee), ("object", &self.object), ("privilege", &self.privilege), ("column", &self.column), ("grantor", &self.grantor)] {
            d.insert(k.into(), Value::Text(v.clone()));
        }
        d.insert("grant_option".into(), Value::Bool(self.grant_option));
        d.insert("deny".into(), Value::Bool(self.deny));
        d
    }

    fn from_doc(d: &Doc) -> GrantRow {
        let t = |k: &str| d.get(k).and_then(Value::as_str).unwrap_or_default().to_owned();
        GrantRow { grantee: t("grantee"), object: t("object"), privilege: t("privilege"), column: t("column"), grant_option: matches!(d.get("grant_option"), Some(Value::Bool(true))), deny: matches!(d.get("deny"), Some(Value::Bool(true))), grantor: t("grantor") }
    }
}

/// What a role may do with a table for one privilege.
#[derive(Debug, Clone, PartialEq)]
pub enum Access {
    All,
    /// These columns only (lower case).
    Columns(HashSet<String>),
    None,
}

/// A snapshot of the roles and grants of one database.
#[derive(Debug)]
pub struct Acl {
    roles: HashMap<String, Role>,
    grants: Vec<GrantRow>,
    /// `Some`: the bootstrap accounts, and authentication is on. `None`: trust mode.
    bootstrap: Option<HashSet<String>>,
}

impl Acl {
    pub fn role(&self, name: &str) -> Option<&Role> {
        self.roles.get(&role_key(name))
    }

    pub fn roles(&self) -> impl Iterator<Item = &Role> {
        self.roles.values()
    }

    pub fn grants(&self) -> &[GrantRow] {
        &self.grants
    }

    /// Whether a name can be granted to: a role, `public`, a fixed role or a bootstrap account;
    /// in trust mode any name (a client may use it at any time).
    fn known(&self, name: &str) -> bool {
        let k = role_key(name);
        k == "public" || is_fixed_role(&k) || self.roles.contains_key(&k) || self.bootstrap.as_ref().is_none_or(|b| b.contains(&k))
    }

    /// `user` and every role it reaches through memberships, plus `public`.
    pub fn closure(&self, user: &str) -> Vec<String> {
        let mut out = vec![role_key(user)];
        let mut i = 0;
        while i < out.len() {
            if let Some(r) = self.roles.get(&out[i]) {
                for (m, _) in &r.member_of {
                    let m = role_key(m);
                    if !out.contains(&m) {
                        out.push(m);
                    }
                }
            }
            i += 1;
        }
        out.push("public".into());
        out
    }

    pub fn is_superuser(&self, user: &str) -> bool {
        let k = role_key(user);
        if let Some(r) = self.roles.get(&k) {
            if r.superuser {
                return true;
            }
        } else if self.bootstrap.is_none() {
            return true; // trust mode: see the module docs
        }
        if self.bootstrap.as_ref().is_some_and(|b| b.contains(&k)) {
            return true;
        }
        self.closure(user).iter().any(|r| r == "sysadmin")
    }

    /// Whether `user` holds `privilege` on `object` (a whole-object grant, the database, or the
    /// gateway), not counting ownership.
    fn holds(&self, roles: &[String], object: &str, privilege: &str) -> bool {
        let wide = object.starts_with("table:") || object.starts_with("seq:") || object.starts_with("func:") || object == "db";
        self.grants.iter().any(|g| !g.deny && g.column.is_empty() && g.privilege == privilege && (g.object == object || (wide && g.object == "db")) && roles.contains(&g.grantee))
            || (wide && roles.iter().any(|r| self.roles.get(r).is_some_and(|x| x.global.iter().any(|p| p == privilege))))
    }

    fn denied(&self, roles: &[String], object: &str, privilege: &str) -> bool {
        roles.iter().any(|r| (r == "db_denydatareader" && privilege == "SELECT") || (r == "db_denydatawriter" && matches!(privilege, "INSERT" | "UPDATE" | "DELETE")))
            || self.grants.iter().any(|g| g.deny && g.column.is_empty() && g.privilege == privilege && (g.object == object || g.object == "db") && roles.contains(&g.grantee))
    }

    pub fn owns(&self, user: &str, owner: Option<&str>) -> bool {
        let Some(owner) = owner else { return false };
        let owner = role_key(owner);
        self.closure(user).contains(&owner)
    }

    /// The access `user` has to `table` for `privilege`.
    pub fn table_access(&self, user: &str, table: &Table, privilege: &str) -> Access {
        if self.is_superuser(user) || self.owns(user, table.owner.as_deref()) {
            return Access::All;
        }
        let roles = self.closure(user);
        if roles.iter().any(|r| r == "db_owner") {
            return Access::All;
        }
        let object = format!("table:{}", table.name);
        if self.denied(&roles, &object, privilege) {
            return Access::None;
        }
        let column_denies: HashSet<String> = self.grants.iter().filter(|g| g.deny && g.object == object && g.privilege == privilege && !g.column.is_empty() && roles.contains(&g.grantee)).map(|g| g.column.to_ascii_lowercase()).collect();
        let whole = self.holds(&roles, &object, privilege) || fixed_role_grants(&roles, privilege);
        if whole && column_denies.is_empty() {
            return Access::All;
        }
        let names: HashSet<String> = if whole {
            table.columns.iter().map(|c| c.name.to_ascii_lowercase()).collect()
        } else {
            self.grants.iter().filter(|g| !g.deny && g.object == object && g.privilege == privilege && !g.column.is_empty() && roles.contains(&g.grantee)).map(|g| g.column.to_ascii_lowercase()).collect()
        };
        let allowed: HashSet<String> = names.difference(&column_denies).cloned().collect();
        if allowed.is_empty() {
            Access::None
        } else {
            Access::Columns(allowed)
        }
    }

    /// Whether `user` holds a database-level privilege (`CREATE`, ...).
    pub fn database_allows(&self, user: &str, privilege: &str) -> bool {
        if self.is_superuser(user) {
            return true;
        }
        let roles = self.closure(user);
        if roles.iter().any(|r| r == "db_owner") || (privilege != "CONNECT" && roles.iter().any(|r| r == "db_ddladmin")) {
            return true;
        }
        !self.denied(&roles, "db", privilege) && self.holds(&roles, "db", privilege)
    }

    /// Whether `user` may connect: everyone, unless `CONNECT` was revoked from `public`.
    pub fn may_connect(&self, user: &str) -> bool {
        let closed = self.grants.iter().any(|g| g.object == "db" && g.privilege == "NOCONNECT");
        !closed || self.database_allows(user, "CONNECT")
    }

    /// Whether `user` may grant `privilege` on `object` with `owner`.
    fn may_grant(&self, user: &str, object: &str, owner: Option<&str>, privilege: &str) -> bool {
        if self.is_superuser(user) || self.owns(user, owner) {
            return true;
        }
        let roles = self.closure(user);
        self.grants.iter().any(|g| !g.deny && g.grant_option && g.privilege == privilege && (g.object == object || g.object == "db") && roles.contains(&g.grantee))
    }

    /// The policies that apply to `user` for `command`, or `None` when row security does not.
    fn policies(&self, user: &str, table: &Table, command: &str) -> Option<Vec<Policy>> {
        if !table.row_security || self.is_superuser(user) || self.roles.get(&role_key(user)).is_some_and(|r| r.bypassrls) {
            return None;
        }
        if !table.force_row_security && self.owns(user, table.owner.as_deref()) {
            return None;
        }
        let roles = self.closure(user);
        Some(table.policies.iter().filter(|p| (p.command == "all" || p.command == command) && (p.roles.is_empty() || p.roles.iter().any(|r| roles.contains(&role_key(r))))).cloned().collect())
    }
}

fn is_fixed_role(k: &str) -> bool {
    matches!(k, "pg_read_all_data" | "pg_write_all_data" | "db_owner" | "db_datareader" | "db_datawriter" | "db_ddladmin" | "db_denydatareader" | "db_denydatawriter" | "sysadmin")
}

fn fixed_role_grants(roles: &[String], privilege: &str) -> bool {
    roles.iter().any(|r| match r.as_str() {
        "pg_read_all_data" | "db_datareader" => privilege == "SELECT",
        "pg_write_all_data" | "db_datawriter" => matches!(privilege, "INSERT" | "UPDATE" | "DELETE"),
        "db_ddladmin" => matches!(privilege, "ALTER" | "DROP" | "INDEX" | "TRUNCATE" | "REFERENCES"),
        _ => false,
    })
}

fn denied(what: &str, name: &str) -> SqlError {
    SqlError::new("42501", format!("permission denied for {what} {name}"))
}

impl Engine {
    /// The current roles and grants (fails closed: no snapshot, no access).
    pub fn acl(&self) -> SqlResult<Arc<Acl>> {
        {
            let c = self.acl_cache.read().unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some((at, acl)) = c.as_ref() {
                if at.elapsed() < ACL_TTL {
                    return Ok(Arc::clone(acl));
                }
            }
        }
        let mut roles = HashMap::new();
        for d in self.root_store().scan(ROLES, None)? {
            let r = Role::from_doc(&d.fields);
            roles.insert(role_key(&r.name), r);
        }
        let grants = self.store.scan(GRANTS, None)?.iter().map(|d| GrantRow::from_doc(&d.fields)).collect();
        let bootstrap = self.config.bootstrap_users.as_ref().map(|b| b.iter().map(|u| role_key(u)).collect());
        let acl = Arc::new(Acl { roles, grants, bootstrap });
        *self.acl_cache.write().unwrap_or_else(std::sync::PoisonError::into_inner) = Some((Instant::now(), Arc::clone(&acl)));
        Ok(acl)
    }

    fn invalidate_acl(&self) {
        *self.acl_cache.write().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }

    /// A login's role, if it may log in.
    pub fn login_role(&self, user: &str) -> Option<Role> {
        self.acl().ok()?.role(user).filter(|r| r.login).cloned()
    }

    fn commit_internal(&self, store: &crate::store::Store, writes: &[Write]) -> SqlResult<()> {
        let r = store.commit(writes).map_err(|e| e.to_sql(writes)).map(|_| ());
        self.invalidate_acl();
        r
    }
}

fn put_role(s: &Session, r: &Role) -> SqlResult<()> {
    let w = Write { collection: ROLES.into(), key: role_key(&r.name), op: Op::Put(r.to_doc()), pre: Pre::None, tag: Tag::Internal };
    s.engine.commit_internal(s.engine.root_store(), std::slice::from_ref(&w))
}

fn put_grant(g: &GrantRow) -> Write {
    Write { collection: GRANTS.into(), key: g.key(), op: Op::Put(g.to_doc()), pre: Pre::None, tag: Tag::Internal }
}

fn del_grant(g: &GrantRow) -> Write {
    Write { collection: GRANTS.into(), key: g.key(), op: Op::Delete, pre: Pre::None, tag: Tag::Internal }
}

impl Session {
    /// The role privileges are checked for: `SET ROLE`'s, else the session's.
    pub fn current_role(&self) -> &str {
        self.role.as_deref().unwrap_or(&self.user)
    }

    /// Refuses `privilege` on `table` unless granted on all of `columns` (empty: any access will
    /// do, for privileges that are about rows).
    pub fn require_table(&self, table: &Table, privilege: &str, columns: &[&str]) -> SqlResult<()> {
        match self.engine.acl()?.table_access(self.current_role(), table, privilege) {
            Access::All => Ok(()),
            Access::None => Err(denied("table", &table.name)),
            Access::Columns(_) if columns.is_empty() && privilege != "DELETE" && privilege != "TRUNCATE" => Ok(()),
            Access::Columns(_) if columns.is_empty() => Err(denied("table", &table.name)),
            Access::Columns(allowed) => match columns.iter().find(|c| !allowed.contains(&c.to_ascii_lowercase())) {
                Some(c) => Err(denied("column", c)),
                None => Ok(()),
            },
        }
    }

    /// Refuses DDL on `table` unless its owner, a superuser, or (MySQL, T-SQL) holding `privilege`.
    pub fn require_owner(&self, table: &Table, privilege: &str) -> SqlResult<()> {
        let acl = self.engine.acl()?;
        if acl.table_access(self.current_role(), table, privilege) == Access::All {
            return Ok(());
        }
        Err(SqlError::new("42501", format!("must be owner of table {}", table.name)))
    }

    pub fn require_database(&self, privilege: &str, what: &str) -> SqlResult<()> {
        if self.engine.acl()?.database_allows(self.current_role(), privilege) {
            return Ok(());
        }
        Err(SqlError::new("42501", format!("permission denied to {what}")))
    }

    /// A sequence's use: `USAGE` or `UPDATE` on it (or the database, or a table owner's own).
    pub fn require_sequence(&self, name: &str) -> SqlResult<()> {
        let acl = self.engine.acl()?;
        let user = self.current_role();
        if acl.is_superuser(user) {
            return Ok(());
        }
        let last = name.rsplit('.').next().unwrap_or(name).trim_matches('"');
        let roles = acl.closure(user);
        if roles.iter().any(|r| r == "db_owner") {
            return Ok(());
        }
        let object = format!("seq:{last}");
        if ["USAGE", "UPDATE"].iter().any(|p| !acl.denied(&roles, &object, p) && acl.holds(&roles, &object, p)) {
            return Ok(());
        }
        // `serial`: the sequence `<table>_<column>_seq` belongs to its table's owner.
        self.engine.refresh_catalog(false)?;
        if self.engine.catalog.all().iter().any(|t| last.starts_with(&format!("{}_", t.name)) && acl.owns(user, t.owner.as_deref())) {
            return Ok(());
        }
        Err(denied("sequence", last))
    }

    pub fn may_connect(&self) -> SqlResult<()> {
        if self.engine.acl()?.may_connect(&self.user) {
            return Ok(());
        }
        Err(SqlError::new("42501", format!("permission denied for database \"{}\"", self.database)))
    }

    /// A newly created table's owner, and what default privileges give.
    pub fn apply_default_privileges(&self, object: &str, kind: &str) -> SqlResult<()> {
        let acl = self.engine.acl()?;
        let me = role_key(self.current_role());
        let prefix = format!("default:{kind}:{me}");
        let writes: Vec<Write> = acl
            .grants
            .iter()
            .filter(|g| g.object == prefix)
            .map(|g| put_grant(&GrantRow { object: object.to_owned(), ..g.clone() }))
            .collect();
        if writes.is_empty() {
            return Ok(());
        }
        self.engine.commit_internal(&self.engine.store, &writes)
    }

    /// Forgets the grants on a dropped object.
    pub fn drop_grants(&self, object: &str) -> SqlResult<()> {
        let acl = self.engine.acl()?;
        let writes: Vec<Write> = acl.grants.iter().filter(|g| g.object == object).map(del_grant).collect();
        if writes.is_empty() {
            return Ok(());
        }
        self.engine.commit_internal(&self.engine.store, &writes)
    }

    /// Moves the grants of a renamed object.
    pub fn rename_grants(&self, from: &str, to: &str) -> SqlResult<()> {
        let acl = self.engine.acl()?;
        let mut writes = Vec::new();
        for g in acl.grants.iter().filter(|g| g.object == from) {
            writes.push(del_grant(g));
            writes.push(put_grant(&GrantRow { object: to.to_owned(), ..g.clone() }));
        }
        if writes.is_empty() {
            return Ok(());
        }
        self.engine.commit_internal(&self.engine.store, &writes)
    }

    /// Refuses a role name the cluster does not know (trust mode knows every name).
    pub fn check_role_known(&self, name: &str) -> SqlResult<()> {
        if self.engine.acl()?.known(name) {
            return Ok(());
        }
        Err(SqlError::new("42704", format!("role \"{name}\" does not exist")))
    }

    fn may_manage_roles(&self, acl: &Acl) -> bool {
        let me = self.current_role();
        acl.is_superuser(me) || acl.role(me).is_some_and(|r| r.createrole)
    }
}

fn no_createrole() -> SqlError {
    SqlError::new("42501", "permission denied to manage roles")
}

/// Runs a role statement.
pub fn role_statement(s: &mut Session, st: &RoleStmt) -> SqlResult<Outcome> {
    let acl = s.engine.acl()?;
    let me = s.current_role().to_owned();
    match st {
        RoleStmt::Create { name, if_not_exists, opts } => {
            if !s.may_manage_roles(&acl) {
                return Err(no_createrole());
            }
            if role_key(name) == "public" || is_fixed_role(&role_key(name)) {
                return Err(SqlError::new("42939", format!("role name \"{name}\" is reserved")));
            }
            if acl.role(name).is_some() || acl.bootstrap.as_ref().is_some_and(|b| b.contains(&role_key(name))) {
                if *if_not_exists {
                    return Ok(Outcome::command("CREATE ROLE"));
                }
                return Err(SqlError::new("42710", format!("role \"{name}\" already exists")));
            }
            let mut r = Role { name: name.clone(), ..Role::default() };
            apply_role_options(s, &acl, &mut r, opts, true)?;
            put_role(s, &r)?;
            Ok(Outcome::command("CREATE ROLE"))
        }
        RoleStmt::Alter { name, opts } => {
            let Some(old) = acl.role(name).cloned() else {
                return Err(SqlError::new("42704", format!("role \"{name}\" does not exist")));
            };
            let own_password_only = role_key(name) == role_key(&me) && opts.iter().all(|o| matches!(o, RoleOpt::Password(_)));
            if !own_password_only && !s.may_manage_roles(&acl) {
                return Err(no_createrole());
            }
            if old.superuser && !acl.is_superuser(&me) {
                return Err(SqlError::new("42501", "only a superuser may alter a superuser"));
            }
            let mut r = old;
            apply_role_options(s, &acl, &mut r, opts, false)?;
            put_role(s, &r)?;
            Ok(Outcome::command("ALTER ROLE"))
        }
        RoleStmt::SetPassword { name, password } => {
            let target = name.clone().unwrap_or_else(|| me.clone());
            if role_key(&target) != role_key(&me) && !s.may_manage_roles(&acl) {
                return Err(no_createrole());
            }
            let Some(mut r) = acl.role(&target).cloned() else {
                return Err(SqlError::new("42704", format!("role \"{target}\" does not exist")));
            };
            r.set_password(Some(password));
            put_role(s, &r)?;
            Ok(Outcome::command("SET"))
        }
        RoleStmt::Rename { from, to } => {
            if !s.may_manage_roles(&acl) {
                return Err(no_createrole());
            }
            let Some(mut r) = acl.role(from).cloned() else {
                return Err(SqlError::new("42704", format!("role \"{from}\" does not exist")));
            };
            if acl.role(to).is_some() {
                return Err(SqlError::new("42710", format!("role \"{to}\" already exists")));
            }
            if r.superuser && !acl.is_superuser(&me) {
                return Err(SqlError::new("42501", "only a superuser may rename a superuser"));
            }
            r.name = to.clone();
            // A verifier does not depend on the name, but memberships and grants name the role.
            let mut writes = vec![
                Write { collection: ROLES.into(), key: role_key(from), op: Op::Delete, pre: Pre::None, tag: Tag::Internal },
                Write { collection: ROLES.into(), key: role_key(to), op: Op::Put(r.to_doc()), pre: Pre::None, tag: Tag::Internal },
            ];
            for other in acl.roles().filter(|o| o.member_of.iter().any(|(m, _)| role_key(m) == role_key(from))) {
                let mut o = other.clone();
                for m in &mut o.member_of {
                    if role_key(&m.0) == role_key(from) {
                        m.0 = to.clone();
                    }
                }
                writes.push(Write { collection: ROLES.into(), key: role_key(&o.name), op: Op::Put(o.to_doc()), pre: Pre::None, tag: Tag::Internal });
            }
            s.engine.commit_internal(s.engine.root_store(), &writes)?;
            rename_role_in_database(s, &acl, from, to)?;
            Ok(Outcome::command("ALTER ROLE"))
        }
        RoleStmt::Drop { names, if_exists } => {
            if !s.may_manage_roles(&acl) {
                return Err(no_createrole());
            }
            for n in names {
                let Some(r) = acl.role(n) else {
                    if *if_exists {
                        continue;
                    }
                    return Err(SqlError::new("42704", format!("role \"{n}\" does not exist")));
                };
                if r.superuser && !acl.is_superuser(&me) {
                    return Err(SqlError::new("42501", "only a superuser may drop a superuser"));
                }
                if role_key(n) == role_key(&me) {
                    return Err(SqlError::new("55006", "the current role cannot be dropped"));
                }
                s.engine.refresh_catalog(false)?;
                if let Some(t) = s.engine.catalog.all().iter().find(|t| t.owner.as_deref().is_some_and(|o| role_key(o) == role_key(n))) {
                    return Err(SqlError::new("2BP01", format!("role \"{n}\" cannot be dropped because it owns table {}", t.name)));
                }
                if acl.grants.iter().any(|g| role_key(&g.grantee) == role_key(n) && !g.deny) {
                    return Err(SqlError::new("2BP01", format!("role \"{n}\" cannot be dropped because it holds privileges: revoke them or DROP OWNED BY")));
                }
                let mut writes = vec![Write { collection: ROLES.into(), key: role_key(n), op: Op::Delete, pre: Pre::None, tag: Tag::Internal }];
                for other in acl.roles().filter(|o| o.member_of.iter().any(|(m, _)| role_key(m) == role_key(n))) {
                    let mut o = other.clone();
                    o.member_of.retain(|(m, _)| role_key(m) != role_key(n));
                    writes.push(Write { collection: ROLES.into(), key: role_key(&o.name), op: Op::Put(o.to_doc()), pre: Pre::None, tag: Tag::Internal });
                }
                s.engine.commit_internal(s.engine.root_store(), &writes)?;
            }
            Ok(Outcome::command("DROP ROLE"))
        }
        RoleStmt::AddMembers { roles, members, admin } => {
            change_members(s, &acl, roles, members, Some(*admin))?;
            Ok(Outcome::command("GRANT ROLE"))
        }
        RoleStmt::DropMembers { roles, members } => {
            change_members(s, &acl, roles, members, None)?;
            Ok(Outcome::command("REVOKE ROLE"))
        }
        RoleStmt::SetRole(None) => {
            s.role = None;
            Ok(Outcome::command("SET"))
        }
        RoleStmt::SetRole(Some(name)) => {
            // Against the session's own identity: membership decides, not the current role.
            if !acl.is_superuser(&s.user) && !acl.closure(&s.user).contains(&role_key(name)) {
                return Err(SqlError::new("42501", format!("permission denied to set role \"{name}\"")));
            }
            s.check_role_known(name)?;
            s.role = Some(name.clone());
            Ok(Outcome::command("SET"))
        }
        RoleStmt::ReassignOwned { from, to } => {
            if !acl.is_superuser(&me) && !(acl.closure(&me).contains(&role_key(from)) && acl.closure(&me).contains(&role_key(to))) {
                return Err(SqlError::new("42501", "permission denied to reassign objects"));
            }
            s.check_role_known(to)?;
            s.engine.refresh_catalog(true)?;
            for t in s.engine.catalog.all() {
                if t.owner.as_deref().is_some_and(|o| role_key(o) == role_key(from)) {
                    let mut n = (*t).clone();
                    n.owner = Some(to.clone());
                    crate::exec::ddl::save_table(s, &n)?;
                }
            }
            Ok(Outcome::command("REASSIGN OWNED"))
        }
        RoleStmt::DropOwned { role } => {
            if !acl.is_superuser(&me) && !acl.closure(&me).contains(&role_key(role)) {
                return Err(SqlError::new("42501", "permission denied to drop objects"));
            }
            s.engine.refresh_catalog(true)?;
            let owned: Vec<String> = s.engine.catalog.all().iter().filter(|t| t.owner.as_deref().is_some_and(|o| role_key(o) == role_key(role))).map(|t| t.name.clone()).collect();
            if !owned.is_empty() {
                crate::exec::ddl::drop_tables(s, &owned, true, false)?;
            }
            let writes: Vec<Write> = acl.grants.iter().filter(|g| role_key(&g.grantee) == role_key(role)).map(del_grant).collect();
            if !writes.is_empty() {
                s.engine.commit_internal(&s.engine.store, &writes)?;
            }
            Ok(Outcome::command("DROP OWNED"))
        }
    }
}

/// Grants, ownership and policies naming a renamed role, in this database (others keep the old
/// name until renamed there: documented limit of per-database grants).
fn rename_role_in_database(s: &Session, acl: &Acl, from: &str, to: &str) -> SqlResult<()> {
    let mut writes = Vec::new();
    for g in acl.grants.iter().filter(|g| role_key(&g.grantee) == role_key(from)) {
        writes.push(del_grant(g));
        writes.push(put_grant(&GrantRow { grantee: role_key(to), ..g.clone() }));
    }
    if !writes.is_empty() {
        s.engine.commit_internal(&s.engine.store, &writes)?;
    }
    s.engine.refresh_catalog(true)?;
    for t in s.engine.catalog.all() {
        let owner = t.owner.as_deref().is_some_and(|o| role_key(o) == role_key(from));
        let named = t.policies.iter().any(|p| p.roles.iter().any(|r| role_key(r) == role_key(from)));
        if owner || named {
            let mut n = (*t).clone();
            if owner {
                n.owner = Some(to.to_owned());
            }
            for p in &mut n.policies {
                for r in &mut p.roles {
                    if role_key(r) == role_key(from) {
                        *r = to.to_owned();
                    }
                }
            }
            crate::exec::ddl::save_table(s, &n)?;
        }
    }
    Ok(())
}

fn apply_role_options(s: &Session, acl: &Acl, r: &mut Role, opts: &[RoleOpt], creating: bool) -> SqlResult<()> {
    let me = s.current_role();
    let is_super = acl.is_superuser(me);
    let may_manage = s.may_manage_roles(acl);
    let need_super = |what: &str| -> SqlResult<()> {
        if is_super {
            Ok(())
        } else {
            Err(SqlError::new("42501", format!("only a superuser may change {what}")))
        }
    };
    let mut members = Vec::new();
    for o in opts {
        match o {
            RoleOpt::Login(v) => r.login = *v,
            RoleOpt::Superuser(v) => {
                need_super("SUPERUSER")?;
                r.superuser = *v;
            }
            RoleOpt::BypassRls(v) => {
                need_super("BYPASSRLS")?;
                r.bypassrls = *v;
            }
            RoleOpt::CreateRole(v) => {
                need_super("CREATEROLE")?;
                r.createrole = *v;
            }
            RoleOpt::CreateDb(v) => {
                need_super("CREATEDB")?;
                r.createdb = *v;
            }
            RoleOpt::Password(p) => r.set_password(p.as_deref()),
            RoleOpt::InRole(rs) => {
                if !may_manage {
                    return Err(no_createrole());
                }
                for x in rs {
                    s.check_role_known(x)?;
                    if !r.member_of.iter().any(|(m, _)| role_key(m) == role_key(x)) {
                        r.member_of.push((x.clone(), false));
                    }
                }
            }
            RoleOpt::Members(rs) => members.extend(rs.iter().map(|x| (x.clone(), false))),
            RoleOpt::Admins(rs) => members.extend(rs.iter().map(|x| (x.clone(), true))),
        }
    }
    if !members.is_empty() {
        if !creating {
            return Err(SqlError::syntax("ALTER ROLE cannot name members: use GRANT role TO member".into()));
        }
        for (m, admin) in members {
            let Some(mut other) = acl.role(&m).cloned() else {
                return Err(SqlError::new("42704", format!("role \"{m}\" does not exist")));
            };
            other.member_of.push((r.name.clone(), admin));
            put_role(s, &other)?;
        }
    }
    Ok(())
}

fn change_members(s: &Session, acl: &Acl, roles: &[String], members: &[String], add: Option<bool>) -> SqlResult<()> {
    let me = s.current_role();
    for role in roles {
        s.check_role_known(role)?;
        // A fixed role's power is not the role manager's to hand out.
        let powerful = role_key(role) == "sysadmin" || acl.role(role).is_some_and(|r| r.superuser);
        let allowed = acl.is_superuser(me) || (!powerful && !is_fixed_role(&role_key(role)) && s.may_manage_roles(acl)) || (role_key(role) != "sysadmin" && acl.role(me).is_some_and(|r| r.member_of.iter().any(|(m, adm)| *adm && role_key(m) == role_key(role))));
        if !allowed {
            return Err(SqlError::new("42501", format!("permission denied to change members of role \"{role}\"")));
        }
        for m in members {
            let k = role_key(m);
            if k == role_key(role) {
                return Err(SqlError::new("0LP01", format!("role \"{m}\" cannot be a member of itself")));
            }
            let mut member = match acl.role(m) {
                Some(r) => r.clone(),
                None if acl.known(m) => Role { name: m.clone(), ..Role::default() },
                None => return Err(SqlError::new("42704", format!("role \"{m}\" does not exist"))),
            };
            // No cycles: the role must not already reach the member.
            if add.is_some() && acl.closure(role).contains(&k) {
                return Err(SqlError::new("0LP01", format!("role \"{m}\" is a member of role \"{role}\"")));
            }
            member.member_of.retain(|(x, _)| role_key(x) != role_key(role));
            if let Some(admin) = add {
                member.member_of.push((role.clone(), admin));
            }
            put_role(s, &member)?;
        }
    }
    Ok(())
}

/// Expands `ALL` and checks a privilege against its object.
fn expand_privileges(s: &Session, privileges: &[(String, Vec<String>)], object: &GrantObject) -> SqlResult<Vec<(String, Vec<String>)>> {
    let mut out = Vec::new();
    let is_table = matches!(object, GrantObject::Tables(_) | GrantObject::AllTables | GrantObject::DefaultTables);
    let is_seq = matches!(object, GrantObject::Sequences(_) | GrantObject::AllSequences | GrantObject::DefaultSequences);
    let is_func = matches!(object, GrantObject::Functions(_) | GrantObject::AllFunctions);
    let wildcard = matches!(object, GrantObject::Database(_) | GrantObject::Server);
    let mysql = s.dialect == Dialect::MySql;
    let tsql = s.dialect == Dialect::MsSql;
    for (p, cols) in privileges {
        let p = p.as_str();
        if p == "ALL" || (tsql && p == "CONTROL") {
            let names: Vec<&str> = if is_table {
                let mut v: Vec<&str> = TABLE_PRIVS.to_vec();
                if mysql || tsql {
                    v.extend(DDL_PRIVS.iter().filter(|x| **x != "CREATE"));
                }
                v
            } else if is_seq {
                vec!["USAGE", "SELECT", "UPDATE"]
            } else if is_func {
                vec!["EXECUTE"]
            } else if mysql || tsql {
                let mut v: Vec<&str> = TABLE_PRIVS.to_vec();
                v.extend(DDL_PRIVS);
                v.push("CONNECT");
                v.push("USAGE");
                v.push("EXECUTE");
                v
            } else {
                vec!["CONNECT", "CREATE", "TEMPORARY", "USAGE"]
            };
            for n in names {
                if !cols.is_empty() && !matches!(n, "SELECT" | "INSERT" | "UPDATE" | "REFERENCES") {
                    continue;
                }
                out.push((n.to_owned(), cols.clone()));
            }
            continue;
        }
        let canonical = match p {
            "TEMP" => "TEMPORARY",
            "CREATE TABLE" | "CREATE VIEW" | "CREATE SEQUENCE" | "CREATE SCHEMA" | "CREATE PROCEDURE" | "CREATE FUNCTION" | "CREATE ROUTINE" | "CREATE TEMPORARY TABLES" => "CREATE",
            "EXEC" => "EXECUTE",
            "ALTER ANY SCHEMA" | "ALTER ANY TABLE" => "ALTER",
            other => other,
        };
        let ok = if is_table {
            TABLE_PRIVS.contains(&canonical) || ((mysql || tsql) && DDL_PRIVS.contains(&canonical))
        } else if is_seq {
            matches!(canonical, "USAGE" | "SELECT" | "UPDATE")
        } else if is_func {
            canonical == "EXECUTE"
        } else if wildcard && (mysql || tsql) {
            TABLE_PRIVS.contains(&canonical) || DDL_PRIVS.contains(&canonical) || matches!(canonical, "CONNECT" | "USAGE" | "EXECUTE" | "TEMPORARY")
        } else {
            matches!(canonical, "CONNECT" | "CREATE" | "TEMPORARY" | "USAGE")
        };
        // MySQL's USAGE means "no privileges": it creates nothing to enforce.
        if mysql && canonical == "USAGE" {
            continue;
        }
        if !ok {
            return Err(SqlError::new("0LP01", format!("invalid privilege type {p} for this object")));
        }
        if !cols.is_empty() && !is_table {
            return Err(SqlError::syntax("column privileges apply to tables".into()));
        }
        if !cols.is_empty() && !matches!(canonical, "SELECT" | "INSERT" | "UPDATE" | "REFERENCES") {
            return Err(SqlError::new("0LP01", format!("invalid privilege type {p} for column")));
        }
        out.push((canonical.to_owned(), cols.clone()));
    }
    Ok(out)
}

/// Runs `GRANT`, `REVOKE`, `DENY` and `ALTER DEFAULT PRIVILEGES`.
pub fn grant_statement(s: &mut Session, g: &GrantStmt) -> SqlResult<Outcome> {
    let acl = s.engine.acl()?;
    let me = s.current_role().to_owned();
    let tag = match g.kind {
        GrantKind::Grant => "GRANT",
        GrantKind::Revoke => "REVOKE",
        GrantKind::Deny => "DENY",
    };
    if matches!(g.object, GrantObject::Roles) {
        // T-SQL database permissions: `GRANT CREATE TABLE TO u`.
        if s.dialect == Dialect::MsSql && g.privileges.iter().any(|(p, _)| p.contains(' ') || TABLE_PRIVS.contains(&p.as_str()) || p == "CONTROL") {
            let mut db = g.clone();
            db.object = GrantObject::Database(None);
            return grant_statement(s, &db);
        }
        if g.kind == GrantKind::Deny {
            return Err(SqlError::syntax("DENY takes permissions, not roles".into()));
        }
        let roles: Vec<String> = g.privileges.iter().map(|(p, _)| p.clone()).collect();
        // Privilege names keep the case they were written in only upper-cased: names compare
        // without case, so that is the same role.
        change_members(s, &acl, &roles, &g.grantees, (g.kind == GrantKind::Grant).then_some(g.admin))?;
        return Ok(Outcome::command(tag));
    }
    for r in &g.grantees {
        s.check_role_known(r)?;
    }
    let privileges = expand_privileges(s, &g.privileges, &g.object)?;
    if matches!(g.object, GrantObject::Server) {
        if !acl.is_superuser(&me) {
            return Err(SqlError::new("42501", "only a superuser may grant on *.*"));
        }
        for r in &g.grantees {
            let mut role = acl.role(r).cloned().unwrap_or(Role { name: r.clone(), ..Role::default() });
            for (p, _) in &privileges {
                role.global.retain(|x| x != p);
                if g.kind == GrantKind::Grant {
                    role.global.push(p.clone());
                }
            }
            put_role(s, &role)?;
        }
        return Ok(Outcome::command(tag));
    }
    // The objects, each with the owner who may always grant on it.
    let mut objects: Vec<(String, Option<String>)> = Vec::new();
    s.engine.refresh_catalog(true)?;
    let view_owner = |name: &str| -> SqlResult<Option<Option<String>>> { s.engine.view_owner(name) };
    match &g.object {
        GrantObject::Tables(names) => {
            for n in names {
                match s.engine.find_table(n)? {
                    Some(t) => objects.push((format!("table:{}", t.name), t.owner.clone())),
                    None => match view_owner(n)? {
                        Some(o) => objects.push((format!("table:{}", n.to_lowercase()), o)),
                        None => return Err(SqlError::undefined_table(n)),
                    },
                }
            }
            for (_, cols) in &privileges {
                if cols.is_empty() {
                    continue;
                }
                for n in names {
                    let t = s.engine.table(n)?;
                    for c in cols {
                        t.column(c)?;
                    }
                }
            }
        }
        GrantObject::AllTables => {
            for t in s.engine.catalog.all() {
                objects.push((format!("table:{}", t.name), t.owner.clone()));
            }
        }
        GrantObject::Sequences(names) => objects.extend(names.iter().map(|n| (format!("seq:{}", n.rsplit('.').next().unwrap_or(n)), None))),
        GrantObject::AllSequences => {
            for name in crate::exec::dml::sequence_names(&s.engine)? {
                objects.push((format!("seq:{name}"), None));
            }
        }
        GrantObject::Functions(names) => objects.extend(names.iter().map(|n| (format!("func:{n}"), None))),
        GrantObject::AllFunctions => objects.push(("db".into(), None)),
        GrantObject::Database(name) => {
            if let Some(n) = name {
                if !n.eq_ignore_ascii_case(&s.database) && !s.engine.is_same_database(n) {
                    return Err(SqlError::unsupported(format!("privileges on another database ({n}): connect to it first")));
                }
            }
            objects.push(("db".into(), None));
        }
        GrantObject::DefaultTables | GrantObject::DefaultSequences => {
            let kind = if matches!(g.object, GrantObject::DefaultTables) { "tables" } else { "sequences" };
            let owner = g.default_for.clone().filter(|o| !o.is_empty()).unwrap_or_else(|| me.clone());
            if !acl.is_superuser(&me) && !acl.closure(&me).contains(&role_key(&owner)) {
                return Err(SqlError::new("42501", "permission denied to change default privileges"));
            }
            objects.push((format!("default:{kind}:{}", role_key(&owner)), Some(owner)));
        }
        GrantObject::Server | GrantObject::Roles => unreachable!("handled above"),
    }
    // `CONNECT` is open to `public` by default; closing it is a mark, opening it removes the mark.
    let mut writes = Vec::new();
    for (object, owner) in &objects {
        for (p, cols) in &privileges {
            if !acl.may_grant(&me, object, owner.as_deref(), p) {
                let what = object.split(':').next_back().unwrap_or(object);
                return Err(SqlError::new("42501", format!("permission denied to grant {p} on {what}")));
            }
            let columns: Vec<&str> = if cols.is_empty() { vec![""] } else { cols.iter().map(String::as_str).collect() };
            for col in columns {
                for grantee in &g.grantees {
                    let row = GrantRow { grantee: role_key(grantee), object: object.clone(), privilege: p.clone(), column: col.to_ascii_lowercase(), grant_option: g.grant_option, deny: g.kind == GrantKind::Deny, grantor: role_key(&me) };
                    if object == "db" && p == "CONNECT" && role_key(grantee) == "public" {
                        let mark = GrantRow { privilege: "NOCONNECT".into(), grant_option: false, deny: false, ..row.clone() };
                        writes.push(if g.kind == GrantKind::Revoke { put_grant(&mark) } else { del_grant(&mark) });
                        continue;
                    }
                    match g.kind {
                        GrantKind::Grant | GrantKind::Deny => {
                            // A GRANT lifts a DENY of the same thing only through REVOKE, as in
                            // SQL Server; DENY keeps winning.
                            writes.push(put_grant(&row));
                        }
                        GrantKind::Revoke if g.option_only => {
                            if let Some(held) = acl.grants.iter().find(|x| x.key() == row.key()) {
                                writes.push(put_grant(&GrantRow { grant_option: false, ..held.clone() }));
                            }
                        }
                        GrantKind::Revoke => {
                            writes.extend(revocations(&acl, &row, g.cascade)?.iter().map(del_grant));
                        }
                    }
                }
            }
        }
    }
    if !writes.is_empty() {
        s.engine.commit_internal(&s.engine.store, &writes)?;
    }
    Ok(Outcome::command(tag))
}

/// What revoking `row` removes: it, its denies (T-SQL `REVOKE` undoes a `DENY`), its column
/// grants when the whole object is revoked, and with `cascade` what the grantee passed on.
fn revocations(acl: &Acl, row: &GrantRow, cascade: bool) -> SqlResult<Vec<GrantRow>> {
    let mut out: Vec<GrantRow> = Vec::new();
    let mut work = vec![(row.grantee.clone(), row.column.clone())];
    while let Some((grantee, column)) = work.pop() {
        for g in acl.grants.iter().filter(|x| x.object == row.object && x.privilege == row.privilege && x.grantee == grantee && (column.is_empty() || x.column == column)) {
            if !out.contains(g) {
                out.push(g.clone());
            }
        }
        // Grants this grantee made depend on what is being taken away, unless it still holds it
        // another way (kept simple: from anyone else).
        let dependents: Vec<&GrantRow> = acl.grants.iter().filter(|x| x.object == row.object && x.privilege == row.privilege && x.grantor == grantee && x.grantee != grantee && (column.is_empty() || x.column == column)).collect();
        for d in dependents {
            if out.contains(d) {
                continue;
            }
            if !cascade {
                return Err(SqlError::new("2BP01", format!("dependent privileges exist: {} granted {} to {}; use CASCADE", grantee, d.privilege, d.grantee)));
            }
            work.push((d.grantee.clone(), d.column.clone()));
        }
    }
    Ok(out)
}

/// What a policy needs from a role, compiled for one statement.
#[derive(Debug, Default)]
pub struct RowPolicies {
    permissive: Vec<Expr>,
    restrictive: Vec<Expr>,
}

/// What restricts one role's reads of one table.
#[derive(Debug, Default)]
pub struct Restriction {
    /// Columns it may not read (lower case).
    pub denied_columns: HashSet<String>,
    pub rows: Option<RowPolicies>,
}

impl Exec<'_> {
    /// The role privileges are checked for right now: a view's owner while a view runs.
    pub(crate) fn effective_user(&self) -> String {
        match self.acting_as.borrow().as_ref() {
            Some(u) => u.clone(),
            None => self.s.current_role().to_owned(),
        }
    }

    /// What restricts reading `table`: `None` when nothing does. No access at all is refused.
    pub(crate) fn read_restriction(&self, table: &Table) -> SqlResult<Option<Rc<Restriction>>> {
        let r = self.restriction(table, "select")?;
        if let Some((_, none)) = &r {
            if *none {
                return Err(denied("table", &table.name));
            }
        }
        Ok(r.map(|x| x.0))
    }

    /// The restriction of `command` on `table` for DML: columns follow `SELECT` (a role that can
    /// only update may still not read), rows follow the command's policies and `SELECT`'s.
    pub(crate) fn dml_restriction(&self, table: &Table, command: &str) -> SqlResult<Option<Rc<Restriction>>> {
        let read = self.restriction(table, "select")?;
        let own = self.restriction(table, command)?;
        Ok(match (read, own) {
            (None, None) => None,
            (Some((r, _)), None) => Some(r),
            (None, Some((o, _))) => Some(Rc::new(Restriction { denied_columns: HashSet::new(), rows: o.rows.as_ref().map(clone_rows) })),
            (Some((r, _)), Some((o, _))) => {
                let mut rows = r.rows.as_ref().map(clone_rows);
                if let Some(c) = o.rows.as_ref() {
                    rows = Some(match rows {
                        None => clone_rows(c),
                        // Both must pass: the read's permissive set becomes one restrictive term.
                        Some(mut x) => {
                            x.restrictive.extend(c.restrictive.iter().cloned());
                            let any = or_all(&c.permissive);
                            x.restrictive.push(any);
                            x
                        }
                    });
                }
                Some(Rc::new(Restriction { denied_columns: r.denied_columns.clone(), rows }))
            }
        })
    }

    /// `None`: unrestricted. The flag says no access at all for this privilege.
    fn restriction(&self, table: &Table, command: &str) -> SqlResult<Option<(Rc<Restriction>, bool)>> {
        let user = self.effective_user();
        let key = (user.clone(), table.name.clone(), command.to_owned());
        if let Some(r) = self.restrictions.borrow().get(&key) {
            return Ok(r.clone());
        }
        let acl = self.s.engine.acl()?;
        let privilege = command.to_ascii_uppercase();
        let access = acl.table_access(&user, table, &privilege);
        let policies = acl.policies(&user, table, command);
        let out = if access == Access::All && policies.is_none() {
            None
        } else {
            let denied_columns: HashSet<String> = match &access {
                Access::All => HashSet::new(),
                Access::None => table.columns.iter().map(|c| c.name.to_ascii_lowercase()).collect(),
                Access::Columns(allowed) => table.columns.iter().map(|c| c.name.to_ascii_lowercase()).filter(|c| !allowed.contains(c)).collect(),
            };
            let rows = policies.map(|ps| {
                let mut r = RowPolicies::default();
                for p in ps {
                    // Reads and `USING` commands filter by `USING`; a policy for all commands
                    // with only `WITH CHECK` does not limit reads.
                    let e = p.using.clone().unwrap_or(Expr::Bool(true));
                    if p.permissive {
                        r.permissive.push(e);
                    } else {
                        r.restrictive.push(e);
                    }
                }
                r
            });
            Some((Rc::new(Restriction { denied_columns, rows }), access == Access::None))
        };
        self.restrictions.borrow_mut().insert(key, out.clone());
        Ok(out)
    }

    /// Whether row policies let `row` through.
    pub(crate) fn row_visible(&self, r: &Restriction, cols: &[ColMeta], row: &[crate::datum::Datum]) -> SqlResult<bool> {
        let Some(rows) = &r.rows else { return Ok(true) };
        self.guarded(|| {
            let env = Env::new(cols, row);
            for e in &rows.restrictive {
                if !self.eval_bool(e, &env)? {
                    return Ok(false);
                }
            }
            for e in &rows.permissive {
                if self.eval_bool(e, &env)? {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }

    /// Fails on policies that reach their own table without end.
    fn guarded<R>(&self, f: impl FnOnce() -> SqlResult<R>) -> SqlResult<R> {
        let d = self.rls_depth.get();
        if d >= 8 {
            return Err(SqlError::new("42P17", "infinite recursion detected in row-level security policy"));
        }
        self.rls_depth.set(d + 1);
        let out = f();
        self.rls_depth.set(d);
        out
    }

    /// Marks the columns of `cols` this restriction hides.
    pub(crate) fn mark_denied(cols: &mut [ColMeta], r: &Restriction) {
        for c in cols {
            if r.denied_columns.contains(&c.name.to_ascii_lowercase()) {
                c.denied = true;
            }
        }
    }

    /// `WITH CHECK`: a row written by `command` must pass (`USING` where a policy has none).
    pub(crate) fn check_new_row(&self, table: &Table, row: &Row, command: &str) -> SqlResult<()> {
        let acl = self.s.engine.acl()?;
        let Some(ps) = acl.policies(&self.effective_user(), table, command) else {
            return Ok(());
        };
        let cols = table_cols(table);
        let env = Env::new(&cols, row);
        let verdict = self.guarded(|| {
            let mut any = false;
            for p in &ps {
                let Some(e) = p.check.as_ref().or(p.using.as_ref()) else {
                    any |= p.permissive;
                    continue;
                };
                let ok = self.eval_bool(e, &env)?;
                if p.permissive {
                    any |= ok;
                } else if !ok {
                    return Ok(false);
                }
            }
            Ok(any)
        })?;
        if verdict {
            return Ok(());
        }
        Err(SqlError::new("42501", format!("new row violates row-level security policy for table \"{}\"", table.name)))
    }
}

fn clone_rows(r: &RowPolicies) -> RowPolicies {
    RowPolicies { permissive: r.permissive.clone(), restrictive: r.restrictive.clone() }
}

fn or_all(es: &[Expr]) -> Expr {
    let mut it = es.iter().cloned();
    let Some(first) = it.next() else { return Expr::Bool(false) };
    it.fold(first, |a, b| Expr::Binary { left: Box::new(a), op: crate::ast::BinaryOp::Or, right: Box::new(b) })
}

/// `SHOW GRANTS` rows for `user`, in MySQL form.
pub fn show_grants(s: &Session, user: &str) -> SqlResult<Vec<String>> {
    let acl = s.engine.acl()?;
    let me = s.current_role();
    if role_key(user) != role_key(me) && !acl.is_superuser(me) {
        return Err(SqlError::new("42501", "permission denied to show another account's grants"));
    }
    let who = format!("`{user}`@`%`");
    let mut out = Vec::new();
    if acl.is_superuser(user) {
        out.push(format!("GRANT ALL PRIVILEGES ON *.* TO {who} WITH GRANT OPTION"));
    } else {
        let global = acl.role(user).map(|r| r.global.join(", ")).filter(|g| !g.is_empty());
        out.push(format!("GRANT {} ON *.* TO {who}", global.unwrap_or_else(|| "USAGE".into())));
    }
    let k = role_key(user);
    let mut rows: Vec<&GrantRow> = acl.grants.iter().filter(|g| g.grantee == k && !g.deny && g.privilege != "NOCONNECT").collect();
    rows.sort_by(|a, b| (&a.object, &a.privilege, &a.column).cmp(&(&b.object, &b.privilege, &b.column)));
    for g in rows {
        let object = match g.object.split_once(':') {
            Some((_, name)) => format!("`{}`.`{name}`", s.database),
            None => format!("`{}`.*", s.database),
        };
        let what = if g.column.is_empty() { g.privilege.clone() } else { format!("{} (`{}`)", g.privilege, g.column) };
        out.push(format!("GRANT {what} ON {object} TO {who}{}", if g.grant_option { " WITH GRANT OPTION" } else { "" }));
    }
    if let Some(r) = acl.role(user) {
        for (m, admin) in &r.member_of {
            out.push(format!("GRANT `{m}`@`%` TO {who}{}", if *admin { " WITH ADMIN OPTION" } else { "" }));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scram_verifiers_check_and_round_trip() {
        let v = Scram::from_password("hunter2");
        assert!(v.check("hunter2"));
        assert!(!v.check("hunter3"));
        assert_eq!(Scram::parse(&v.text()), Some(v));
        for bad in ["", "SCRAM-SHA-256$", "SCRAM-SHA-256$x:y$z:w", "SCRAM-SHA-256$0:YQ==$YQ==:YQ==", "md5abc"] {
            assert_eq!(Scram::parse(bad), None, "{bad}");
        }
    }

    #[test]
    fn a_password_shaped_like_a_verifier_is_taken_as_one() {
        let mut r = Role::default();
        let v = Scram::from_password("x");
        r.set_password(Some(&v.text()));
        assert_eq!(r.scram, Some(v));
        assert_eq!(r.native, None);
        r.set_password(Some("*94BDCEBE19083CE2A1F959FD02F964C7AF4CFC29"));
        assert_eq!(r.native.as_ref().map(Vec::len), Some(20));
        assert_eq!(r.scram, None);
        r.set_password(None);
        assert_eq!((r.scram.is_none(), r.native.is_none()), (true, true));
    }

    #[test]
    fn role_documents_never_hold_a_password() {
        let mut r = Role { name: "a".into(), login: true, member_of: vec![("b".into(), true)], ..Role::default() };
        r.set_password(Some("correct horse"));
        let d = r.to_doc();
        assert!(!format!("{d:?}").contains("correct horse"));
        assert_eq!(Role::from_doc(&d), r);
    }

    fn acl(roles: Vec<Role>, grants: Vec<GrantRow>) -> Acl {
        Acl { roles: roles.into_iter().map(|r| (role_key(&r.name), r)).collect(), grants, bootstrap: Some(HashSet::from(["admin".to_owned()])) }
    }

    fn grant(grantee: &str, object: &str, privilege: &str, column: &str, deny: bool) -> GrantRow {
        GrantRow { grantee: grantee.into(), object: object.into(), privilege: privilege.into(), column: column.into(), grant_option: false, deny, grantor: "admin".into() }
    }

    #[test]
    fn membership_is_transitive_and_cycles_end() {
        let a = acl(vec![Role { name: "x".into(), member_of: vec![("y".into(), false)], ..Role::default() }, Role { name: "y".into(), member_of: vec![("z".into(), false), ("x".into(), false)], ..Role::default() }], Vec::new());
        let c = a.closure("X");
        assert!(c.contains(&"z".to_owned()) && c.contains(&"public".to_owned()));
        assert_eq!(c.len(), 4);
        assert!(!a.is_superuser("x"));
        assert!(a.is_superuser("admin"));
        assert!(!a.is_superuser("stranger"), "with authentication on, an unknown name holds nothing");
    }

    #[test]
    fn deny_beats_grant_and_public_counts() {
        let t = Table { name: "t".into(), ..Table::test_table() };
        let a = acl(vec![Role { name: "u".into(), ..Role::default() }], vec![grant("public", "table:t", "SELECT", "", false)]);
        assert_eq!(a.table_access("u", &t, "SELECT"), Access::All);
        assert_eq!(a.table_access("u", &t, "INSERT"), Access::None);
        let a = acl(vec![Role { name: "u".into(), ..Role::default() }], vec![grant("public", "table:t", "SELECT", "", false), grant("u", "table:t", "SELECT", "", true)]);
        assert_eq!(a.table_access("u", &t, "SELECT"), Access::None);
    }

    #[test]
    fn revoking_with_dependents_needs_cascade() {
        let mut g1 = grant("a", "table:t", "SELECT", "", false);
        g1.grant_option = true;
        let mut g2 = grant("b", "table:t", "SELECT", "", false);
        g2.grantor = "a".into();
        let a = acl(Vec::new(), vec![g1.clone(), g2.clone()]);
        assert_eq!(revocations(&a, &g1, false).unwrap_err().code, "2BP01");
        let out = revocations(&a, &g1, true).unwrap();
        assert!(out.contains(&g1) && out.contains(&g2));
        assert_eq!(revocations(&a, &g2, false).unwrap(), vec![g2]);
    }
}
