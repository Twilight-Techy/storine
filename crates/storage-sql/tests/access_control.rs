//! Access control (`TASK-145`): every statement of every dialect, as a user who should not get
//! through, with authentication on (a `--user` account as the superuser).

use std::collections::HashSet;
use std::sync::Arc;

use storage_sql::exec::{Engine, EngineConfig, Outcome, Session};
use storage_sql::parser::parse;
use storage_sql::{Dialect, SqlError, SqlResult};

fn engine(name: &str) -> (Arc<Engine>, storage_sql::embedded::Embedded, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("storine_acl_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let config = EngineConfig { bootstrap_users: Some(HashSet::from(["admin".to_owned()])), ..EngineConfig::default() };
    let e = storage_sql::embedded::start(&dir, config).unwrap();
    (Arc::clone(&e.engine), e, dir)
}

fn exec(s: &mut Session, sql: &str) -> SqlResult<Outcome> {
    let (stmts, _) = parse(sql, s.dialect)?;
    let mut last = Outcome::command("");
    for st in &stmts {
        last = s.execute(st, &[])?;
    }
    Ok(last)
}

fn ok(s: &mut Session, sql: &str) {
    exec(s, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn rows(s: &mut Session, sql: &str) -> Vec<Vec<String>> {
    match exec(s, sql).unwrap_or_else(|e| panic!("{sql}: {e}")) {
        Outcome::Rows(rs) => rs.rows.iter().map(|r| r.iter().map(|d| if d.is_null() { "NULL".into() } else { d.as_text() }).collect()).collect(),
        other => panic!("{sql}: expected rows, got {other:?}"),
    }
}

/// The statement is refused for lack of rights, with the error every client recognizes.
fn refused(s: &mut Session, sql: &str) -> SqlError {
    match exec(s, sql) {
        Err(e) => {
            assert_eq!(e.code, "42501", "{sql}: {e}");
            e
        }
        Ok(o) => panic!("{sql}: expected permission denied, got {o:?}"),
    }
}

fn setup(a: &mut Session) {
    ok(a, "CREATE TABLE t (id INT PRIMARY KEY, owner_name TEXT, secret TEXT, n INT)");
    ok(a, "INSERT INTO t VALUES (1,'ann','s1',10),(2,'bob','s2',20),(3,'ann','s3',30)");
    ok(a, "CREATE ROLE ann LOGIN PASSWORD 'pw-ann'");
    ok(a, "CREATE ROLE bob LOGIN PASSWORD 'pw-bob'");
}

#[test]
fn no_grant_no_access_in_any_statement() {
    let (e, _keep, _d) = engine("nothing");
    let mut a = e.new_session(Dialect::Postgres, "admin", None);
    setup(&mut a);
    ok(&mut a, "CREATE TABLE other (x INT PRIMARY KEY, id INT)");
    ok(&mut a, "CREATE VIEW v AS SELECT id, n FROM t");
    let mut u = e.new_session(Dialect::Postgres, "ann", None);
    for sql in [
        "SELECT * FROM t",
        "SELECT count(*) FROM t",
        "SELECT sum(n) FROM t",
        "SELECT 1 FROM t WHERE id = 1",
        "SELECT o.x FROM other o JOIN t ON t.id = o.id",
        "SELECT (SELECT max(n) FROM t)",
        "SELECT 1 WHERE EXISTS (SELECT 1 FROM t)",
        "WITH c AS (SELECT * FROM t) SELECT * FROM c",
        "SELECT * FROM (SELECT * FROM t) d",
        "SELECT * FROM v",
        "SELECT * FROM t FOR UPDATE",
        "BEGIN; SELECT * FROM t FOR UPDATE",
        "INSERT INTO t VALUES (9,'x','x',1)",
        "INSERT INTO other SELECT id, id FROM t",
        "UPDATE t SET n = 0",
        "UPDATE t SET n = n + 1 WHERE id = 1",
        "DELETE FROM t",
        "TRUNCATE t",
        "DROP TABLE t",
        "ALTER TABLE t ADD COLUMN z INT",
        "ALTER TABLE t ENABLE ROW LEVEL SECURITY",
        "ALTER TABLE t OWNER TO ann",
        "CREATE INDEX i ON t (n)",
        "CREATE TABLE mine (a INT)",
        "CREATE VIEW w AS SELECT 1",
        "DROP VIEW v",
        "CREATE POLICY p ON t USING (true)",
        "GRANT SELECT ON t TO ann",
        "GRANT SELECT ON t TO bob",
        "CREATE ROLE evil",
        "ALTER ROLE ann SUPERUSER",
        "ALTER ROLE bob PASSWORD 'stolen'",
        "DROP ROLE bob",
        "GRANT admin TO ann",
        "SET ROLE admin",
        "SET SESSION AUTHORIZATION admin",
        "CREATE DATABASE d2",
        "DROP DATABASE storine",
    ] {
        refused(&mut u, sql);
        let _ = exec(&mut u, "ROLLBACK");
    }
    // Still all there.
    assert_eq!(rows(&mut a, "SELECT count(*), sum(n) FROM t"), vec![vec!["3", "60"]]);
    // A name with no role at all holds nothing either.
    let mut stranger = e.new_session(Dialect::Postgres, "stranger", None);
    refused(&mut stranger, "SELECT * FROM t");
}

#[test]
fn grants_give_exactly_what_they_say() {
    let (e, _keep, _d) = engine("grants");
    let mut a = e.new_session(Dialect::Postgres, "admin", None);
    setup(&mut a);
    let mut u = e.new_session(Dialect::Postgres, "ann", None);

    ok(&mut a, "GRANT SELECT ON t TO ann");
    assert_eq!(rows(&mut u, "SELECT count(*) FROM t"), vec![vec!["3"]]);
    refused(&mut u, "UPDATE t SET n = 0");
    refused(&mut u, "DELETE FROM t");

    // Revoked at once on this gateway.
    ok(&mut a, "REVOKE SELECT ON t FROM ann");
    refused(&mut u, "SELECT count(*) FROM t");

    // Through a role, transitively, and PUBLIC.
    ok(&mut a, "CREATE ROLE readers");
    ok(&mut a, "CREATE ROLE staff IN ROLE readers");
    ok(&mut a, "GRANT staff TO ann");
    ok(&mut a, "GRANT SELECT ON t TO readers");
    assert_eq!(rows(&mut u, "SELECT count(*) FROM t"), vec![vec!["3"]]);
    ok(&mut a, "REVOKE staff FROM ann");
    refused(&mut u, "SELECT count(*) FROM t");
    ok(&mut a, "GRANT SELECT ON t TO PUBLIC");
    assert_eq!(rows(&mut u, "SELECT count(*) FROM t"), vec![vec!["3"]]);
    ok(&mut a, "REVOKE SELECT ON t FROM PUBLIC");
    refused(&mut u, "SELECT count(*) FROM t");

    // ALL TABLES covers tables existing now, not later ones; default privileges cover later ones.
    ok(&mut a, "GRANT SELECT ON ALL TABLES IN SCHEMA public TO ann");
    assert_eq!(rows(&mut u, "SELECT count(*) FROM t"), vec![vec!["3"]]);
    ok(&mut a, "CREATE TABLE later (x INT PRIMARY KEY)");
    refused(&mut u, "SELECT * FROM later");
    ok(&mut a, "ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO ann");
    ok(&mut a, "CREATE TABLE later2 (x INT PRIMARY KEY)");
    assert!(rows(&mut u, "SELECT * FROM later2").is_empty());

    // Grant options, and what depends on them.
    let mut b = e.new_session(Dialect::Postgres, "bob", None);
    refused(&mut u, "GRANT INSERT ON later TO bob");
    ok(&mut a, "GRANT INSERT ON later TO ann WITH GRANT OPTION");
    ok(&mut u, "GRANT INSERT ON later TO bob");
    ok(&mut b, "INSERT INTO later VALUES (1)");
    let e2 = exec(&mut a, "REVOKE INSERT ON later FROM ann").unwrap_err();
    assert_eq!(e2.code, "2BP01", "{e2}");
    ok(&mut a, "REVOKE INSERT ON later FROM ann CASCADE");
    refused(&mut b, "INSERT INTO later VALUES (2)");

    // A table's creator owns it; ownership is what lets it drop and grant.
    ok(&mut a, "GRANT CREATE ON SCHEMA public TO ann");
    ok(&mut u, "CREATE TABLE annt (x INT PRIMARY KEY)");
    ok(&mut u, "INSERT INTO annt VALUES (1)");
    ok(&mut u, "GRANT SELECT ON annt TO bob");
    assert_eq!(rows(&mut b, "SELECT x FROM annt"), vec![vec!["1"]]);
    refused(&mut b, "DROP TABLE annt");
    ok(&mut u, "DROP TABLE annt");

    // A foreign key is a probe of the parent's values: it takes REFERENCES.
    ok(&mut a, "CREATE TABLE parent (id INT PRIMARY KEY)");
    refused(&mut u, "CREATE TABLE child (id INT PRIMARY KEY, p INT REFERENCES parent(id))");
    ok(&mut a, "GRANT REFERENCES ON parent TO ann");
    ok(&mut u, "CREATE TABLE child (id INT PRIMARY KEY, p INT REFERENCES parent(id))");
    // Indexes, sequences and replaced views belong to their owners.
    ok(&mut a, "CREATE INDEX later_i ON later (x)");
    refused(&mut b, "DROP INDEX later_i");
    refused(&mut b, "CREATE SEQUENCE s1");
    ok(&mut a, "CREATE SEQUENCE s1");
    refused(&mut b, "SELECT nextval('s1')");
    ok(&mut a, "GRANT USAGE ON SEQUENCE s1 TO bob");
    assert_eq!(rows(&mut b, "SELECT nextval('s1')"), vec![vec!["1"]]);
    ok(&mut a, "CREATE VIEW av AS SELECT 1 AS one");
    ok(&mut a, "GRANT CREATE ON SCHEMA public TO bob");
    refused(&mut b, "CREATE OR REPLACE VIEW av AS SELECT 2 AS one");
    ok(&mut a, "REVOKE CREATE ON SCHEMA public FROM bob");

    // Grants follow renames and die with their table.
    ok(&mut a, "GRANT SELECT ON later TO bob");
    ok(&mut a, "ALTER TABLE later RENAME TO renamed");
    assert_eq!(rows(&mut b, "SELECT x FROM renamed"), vec![vec!["1"]]);
    ok(&mut a, "DROP TABLE renamed");
    ok(&mut a, "CREATE TABLE renamed (x INT PRIMARY KEY)");
    refused(&mut b, "SELECT x FROM renamed");

    // Refused precisely: names that do not exist, and schemas that would over-grant.
    assert_eq!(exec(&mut a, "GRANT SELECT ON t TO nobody").unwrap_err().code, "42704");
    assert_eq!(exec(&mut a, "GRANT SELECT ON nothere TO ann").unwrap_err().code, "42P01");
    assert_eq!(exec(&mut a, "GRANT SELECT ON ALL TABLES IN SCHEMA hr TO ann").unwrap_err().code, "0A000");
    assert_eq!(exec(&mut a, "GRANT BOGUS ON t TO ann").unwrap_err().code, "0LP01");
}

#[test]
fn column_privileges_hide_columns_in_every_clause() {
    let (e, _keep, _d) = engine("columns");
    let mut a = e.new_session(Dialect::Postgres, "admin", None);
    setup(&mut a);
    ok(&mut a, "GRANT SELECT (id, n), UPDATE (n) ON t TO ann");
    let mut u = e.new_session(Dialect::Postgres, "ann", None);
    assert_eq!(rows(&mut u, "SELECT id, n FROM t WHERE id = 1"), vec![vec!["1", "10"]]);
    assert_eq!(rows(&mut u, "SELECT count(*) FROM t"), vec![vec!["3"]]);
    for sql in [
        "SELECT * FROM t",
        "SELECT secret FROM t",
        "SELECT t.secret FROM t",
        "SELECT id FROM t WHERE secret = 's1'",
        "SELECT id FROM t WHERE secret = 'no such value'",
        "SELECT id FROM t ORDER BY secret",
        "SELECT secret, count(*) FROM t GROUP BY secret",
        "SELECT count(secret) FROM t",
        "SELECT id FROM t HAVING max(secret) > ''",
        "SELECT (SELECT secret FROM t LIMIT 1)",
        "SELECT d.s FROM (SELECT secret s FROM t) d",
        "UPDATE t SET n = 1 WHERE secret = 's1'",
        "UPDATE t SET n = 1 WHERE secret = 'no such value'",
        "UPDATE t SET n = length(secret)",
        "UPDATE t SET secret = 'x'",
        "UPDATE t SET n = 1 WHERE id = 1 RETURNING *",
        "UPDATE t SET n = 1 WHERE id = 1 RETURNING secret",
    ] {
        refused(&mut u, sql);
    }
    ok(&mut u, "UPDATE t SET n = n + 1 WHERE id = 1 RETURNING id, n");
    assert_eq!(rows(&mut a, "SELECT n FROM t WHERE id = 1"), vec![vec!["11"]]);

    // Update without reading: allowed, as in PostgreSQL.
    ok(&mut a, "REVOKE SELECT (id, n) ON t FROM ann");
    ok(&mut u, "UPDATE t SET n = 5");
    refused(&mut u, "UPDATE t SET n = n + 1");
    refused(&mut u, "UPDATE t SET n = 5 WHERE id = 1");

    // Insert into some columns only.
    ok(&mut a, "GRANT INSERT (id, n) ON t TO ann");
    ok(&mut u, "INSERT INTO t (id, n) VALUES (7, 7)");
    refused(&mut u, "INSERT INTO t (id, secret) VALUES (8, 'x')");
    refused(&mut u, "INSERT INTO t VALUES (9, 'x', 'x', 1)");
}

#[test]
fn row_level_security_filters_every_path() {
    let (e, _keep, _d) = engine("rls");
    let mut a = e.new_session(Dialect::Postgres, "admin", None);
    setup(&mut a);
    ok(&mut a, "GRANT SELECT, INSERT, UPDATE, DELETE ON t TO ann, bob");
    ok(&mut a, "ALTER TABLE t ENABLE ROW LEVEL SECURITY");
    let mut u = e.new_session(Dialect::Postgres, "ann", None);
    // Enabled with no policy: no rows (and no writes).
    assert_eq!(rows(&mut u, "SELECT count(*) FROM t"), vec![vec!["0"]]);
    assert_eq!(rows(&mut a, "SELECT count(*) FROM t"), vec![vec!["3"]], "a superuser bypasses it");
    assert_eq!(exec(&mut u, "INSERT INTO t VALUES (5,'ann','x',1)").unwrap_err().code, "42501");

    ok(&mut a, "CREATE POLICY mine ON t USING (owner_name = current_user)");
    // All the shapes that have a fast path.
    assert_eq!(rows(&mut u, "SELECT id FROM t ORDER BY id"), vec![vec!["1"], vec!["3"]]);
    assert_eq!(rows(&mut u, "SELECT count(*), sum(n) FROM t"), vec![vec!["2", "40"]]);
    assert!(rows(&mut u, "SELECT id FROM t WHERE id = 2").is_empty(), "primary-key lookup");
    assert!(rows(&mut u, "SELECT id FROM t WHERE secret = 's2'").is_empty(), "pushed-down filter");
    assert_eq!(rows(&mut u, "SELECT count(*) FROM t x JOIN t y ON x.id = y.id"), vec![vec!["2"]], "join");
    assert_eq!(rows(&mut u, "SELECT (SELECT count(*) FROM t)"), vec![vec!["2"]], "subquery");

    // Writes reach visible rows only: other rows look absent.
    assert!(matches!(exec(&mut u, "UPDATE t SET n = 99 WHERE id = 2").unwrap(), Outcome::Command { affected: 0, .. }));
    assert!(matches!(exec(&mut u, "UPDATE t SET n = n + 1 WHERE id = 2").unwrap(), Outcome::Command { affected: 0, .. }), "atomic update path");
    assert!(matches!(exec(&mut u, "DELETE FROM t WHERE id = 2").unwrap(), Outcome::Command { affected: 0, .. }));
    assert_eq!(rows(&mut a, "SELECT n FROM t WHERE id = 2"), vec![vec!["20"]]);
    ok(&mut u, "UPDATE t SET n = 11 WHERE id = 1");

    // WITH CHECK: a role cannot write rows it could not see, nor move a row out of its sight.
    assert_eq!(exec(&mut u, "INSERT INTO t VALUES (5,'bob','x',1)").unwrap_err().code, "42501");
    ok(&mut u, "INSERT INTO t VALUES (5,'ann','x',1)");
    assert_eq!(exec(&mut u, "UPDATE t SET owner_name = 'bob' WHERE id = 5").unwrap_err().code, "42501");
    assert_eq!(rows(&mut a, "SELECT owner_name FROM t WHERE id = 5"), vec![vec!["ann"]]);
    ok(&mut u, "DELETE FROM t WHERE id = 5");

    // Upsert: the conflicting row is someone else's; the policy still decides.
    assert_eq!(exec(&mut u, "INSERT INTO t VALUES (2,'ann','x',1) ON CONFLICT (id) DO UPDATE SET n = 1").map(|_| ()).unwrap_err().code, "42501");
    assert_eq!(rows(&mut a, "SELECT n FROM t WHERE id = 2"), vec![vec!["20"]]);

    // Combination: permissive policies add, restrictive ones cut.
    ok(&mut a, "CREATE POLICY extra ON t FOR SELECT USING (id = 2)");
    assert_eq!(rows(&mut u, "SELECT id FROM t ORDER BY id"), vec![vec!["1"], vec!["2"], vec!["3"]]);
    ok(&mut a, "CREATE POLICY cut ON t AS RESTRICTIVE USING (id <> 3)");
    assert_eq!(rows(&mut u, "SELECT id FROM t ORDER BY id"), vec![vec!["1"], vec!["2"]]);
    ok(&mut a, "DROP POLICY cut ON t");
    ok(&mut a, "DROP POLICY extra ON t");

    // Per role.
    ok(&mut a, "CREATE POLICY for_bob ON t TO bob USING (true)");
    let mut b = e.new_session(Dialect::Postgres, "bob", None);
    assert_eq!(rows(&mut b, "SELECT count(*) FROM t"), vec![vec!["3"]]);
    assert_eq!(rows(&mut u, "SELECT count(*) FROM t"), vec![vec!["2"]]);

    // Session settings as policy input, the multi-tenant pattern.
    ok(&mut a, "DROP POLICY for_bob ON t");
    ok(&mut a, "DROP POLICY mine ON t");
    ok(&mut a, "CREATE POLICY tenant ON t USING (owner_name = current_setting('app.tenant', true))");
    ok(&mut u, "SET app.tenant = 'bob'");
    assert_eq!(rows(&mut u, "SELECT id FROM t"), vec![vec!["2"]]);

    // A role that owns the table bypasses unless forced; BYPASSRLS bypasses.
    ok(&mut a, "ALTER TABLE t OWNER TO bob");
    ok(&mut b, "SET app.tenant = 'nobody'");
    assert_eq!(rows(&mut b, "SELECT count(*) FROM t"), vec![vec!["3"]]);
    ok(&mut b, "ALTER TABLE t FORCE ROW LEVEL SECURITY");
    assert_eq!(rows(&mut b, "SELECT count(*) FROM t"), vec![vec!["0"]]);
    ok(&mut b, "ALTER TABLE t NO FORCE ROW LEVEL SECURITY");
    refused(&mut u, "ALTER TABLE t DISABLE ROW LEVEL SECURITY");
    ok(&mut b, "ALTER TABLE t DISABLE ROW LEVEL SECURITY");
    assert_eq!(rows(&mut u, "SELECT count(*) FROM t"), vec![vec!["3"]]);
}

#[test]
fn a_view_runs_with_its_owners_rights() {
    let (e, _keep, _d) = engine("views");
    let mut a = e.new_session(Dialect::Postgres, "admin", None);
    setup(&mut a);
    ok(&mut a, "GRANT CREATE ON SCHEMA public TO bob");
    ok(&mut a, "GRANT SELECT (id, n) ON t TO bob");
    let mut b = e.new_session(Dialect::Postgres, "bob", None);
    ok(&mut b, "CREATE VIEW bobs AS SELECT id, n FROM t");
    let mut u = e.new_session(Dialect::Postgres, "ann", None);
    refused(&mut u, "SELECT * FROM bobs");
    ok(&mut b, "GRANT SELECT ON bobs TO ann");
    assert_eq!(rows(&mut u, "SELECT count(*) FROM bobs"), vec![vec!["3"]]);
    refused(&mut u, "SELECT * FROM t");
    // The owner's rights, not more: a view cannot launder what its owner may not read.
    ok(&mut b, "CREATE VIEW sneaky AS SELECT secret FROM t");
    ok(&mut b, "GRANT SELECT ON sneaky TO ann");
    refused(&mut u, "SELECT * FROM sneaky");
    // Nor do the caller's rights leak out once the view is done: same statement, `t` directly.
    refused(&mut u, "SELECT b.id FROM bobs b JOIN t ON t.id = b.id");
    ok(&mut a, "REVOKE SELECT (id, n) ON t FROM bob");
    refused(&mut u, "SELECT * FROM bobs");
}

#[test]
fn set_role_and_catalog_answers() {
    let (e, _keep, _d) = engine("setrole");
    let mut a = e.new_session(Dialect::Postgres, "admin", None);
    setup(&mut a);
    ok(&mut a, "CREATE ROLE readers");
    ok(&mut a, "GRANT SELECT ON t TO readers");
    ok(&mut a, "GRANT readers TO ann");
    let mut u = e.new_session(Dialect::Postgres, "ann", None);
    ok(&mut u, "SET ROLE readers");
    assert_eq!(rows(&mut u, "SELECT current_user, session_user"), vec![vec!["readers", "ann"]]);
    assert_eq!(rows(&mut u, "SELECT count(*) FROM t"), vec![vec!["3"]]);
    // Membership is checked against the session's identity, so nobody climbs through a role.
    refused(&mut u, "SET ROLE admin");
    ok(&mut u, "RESET ALL");
    assert_eq!(rows(&mut u, "SELECT current_user"), vec![vec!["ann"]]);
    ok(&mut u, "SET ROLE readers");
    ok(&mut u, "SET ROLE NONE");
    assert_eq!(rows(&mut u, "SELECT current_user"), vec![vec!["ann"]]);
    // Everything a connection pool sends between clients really ends impersonation.
    for reset in ["RESET ROLE", "RESET SESSION AUTHORIZATION", "DISCARD ALL"] {
        ok(&mut u, "SET ROLE readers");
        ok(&mut u, reset);
        assert_eq!(rows(&mut u, "SELECT current_user"), vec![vec!["ann"]], "{reset}");
    }

    let q = |s: &mut Session, sql: &str| rows(s, sql)[0][0].clone();
    assert_eq!(q(&mut u, "SELECT has_table_privilege('t', 'SELECT')"), "t");
    assert_eq!(q(&mut u, "SELECT has_table_privilege('t', 'DELETE')"), "f");
    assert_eq!(q(&mut u, "SELECT has_table_privilege('bob', 't', 'SELECT')"), "f");
    assert_eq!(q(&mut u, "SELECT pg_has_role('readers', 'MEMBER')"), "t");
    assert_eq!(q(&mut u, "SELECT pg_has_role('admin', 'MEMBER')"), "f");
    assert_eq!(q(&mut a, "SELECT rolsuper FROM pg_roles WHERE rolname = 'ann'"), "f");
    assert_eq!(q(&mut a, "SELECT rolcanlogin FROM pg_roles WHERE rolname = 'ann'"), "t");

    // Role lifecycle.
    assert_eq!(exec(&mut a, "DROP ROLE readers").unwrap_err().code, "2BP01");
    ok(&mut a, "REVOKE SELECT ON t FROM readers");
    ok(&mut a, "DROP ROLE readers");
    ok(&mut a, "ALTER TABLE t OWNER TO bob");
    assert_eq!(exec(&mut a, "DROP ROLE bob").unwrap_err().code, "2BP01");
    ok(&mut a, "REASSIGN OWNED BY bob TO ann");
    ok(&mut a, "DROP ROLE bob");
    ok(&mut a, "DROP ROLE IF EXISTS bob");
    assert_eq!(exec(&mut a, "CREATE ROLE public").unwrap_err().code, "42939");
    assert_eq!(exec(&mut a, "CREATE ROLE ann").unwrap_err().code, "42710");
    ok(&mut a, "CREATE ROLE IF NOT EXISTS ann");
    // A role manager cannot mint superusers or walk into one.
    ok(&mut a, "CREATE ROLE boss CREATEROLE");
    let mut boss = e.new_session(Dialect::Postgres, "boss", None);
    ok(&mut boss, "CREATE ROLE worker");
    refused(&mut boss, "CREATE ROLE god SUPERUSER");
    refused(&mut boss, "ALTER ROLE boss SUPERUSER");
    refused(&mut boss, "GRANT sysadmin TO boss");
    refused(&mut boss, "GRANT pg_read_all_data TO boss");
    // Fixed roles work.
    ok(&mut a, "GRANT pg_read_all_data TO worker");
    let mut w = e.new_session(Dialect::Postgres, "worker", None);
    assert_eq!(rows(&mut w, "SELECT count(*) FROM t"), vec![vec!["3"]]);
    refused(&mut w, "DELETE FROM t");
}

#[test]
fn mysql_syntax() {
    let (e, _keep, _d) = engine("mysql");
    let mut a = e.new_session(Dialect::MySql, "admin", None);
    ok(&mut a, "CREATE TABLE t (id INT PRIMARY KEY, secret TEXT, n INT)");
    ok(&mut a, "INSERT INTO t VALUES (1,'s',1)");
    ok(&mut a, "CREATE USER 'ann'@'%' IDENTIFIED BY 'pw'");
    ok(&mut a, "CREATE USER IF NOT EXISTS 'ann'@'%' IDENTIFIED BY 'pw'");
    assert_eq!(exec(&mut a, "CREATE USER 'x'@'10.0.0.1' IDENTIFIED BY 'pw'").unwrap_err().code, "0A000", "a host restriction is not silently dropped");
    let mut u = e.new_session(Dialect::MySql, "ann", None);
    refused(&mut u, "SELECT * FROM t");
    ok(&mut a, "GRANT SELECT ON storine.t TO 'ann'@'%'");
    assert_eq!(rows(&mut u, "SELECT id FROM t"), vec![vec!["1"]]);
    refused(&mut u, "INSERT INTO t VALUES (2,'s',1)");
    ok(&mut a, "GRANT INSERT, UPDATE ON storine.* TO 'ann'@'%'");
    ok(&mut u, "INSERT INTO t VALUES (2,'s',1)");
    refused(&mut u, "DELETE FROM t");
    refused(&mut u, "DROP TABLE t");
    ok(&mut a, "GRANT DROP ON t TO 'ann'@'%'");
    let g = rows(&mut a, "SHOW GRANTS FOR 'ann'@'%'");
    assert!(g.iter().any(|r| r[0].contains("GRANT SELECT ON `storine`.`t`")), "{g:?}");
    assert!(rows(&mut u, "SHOW GRANTS").len() >= 3);
    refused(&mut u, "SHOW GRANTS FOR 'admin'@'%'");
    ok(&mut a, "REVOKE SELECT ON storine.t FROM 'ann'@'%'");
    refused(&mut u, "SELECT * FROM t");
    ok(&mut a, "CREATE ROLE 'r1'");
    ok(&mut a, "GRANT SELECT ON t TO 'r1'");
    ok(&mut a, "GRANT 'r1' TO 'ann'@'%'");
    ok(&mut u, "SET ROLE ALL");
    assert_eq!(rows(&mut u, "SELECT count(*) FROM t"), vec![vec!["2"]]);
    ok(&mut a, "ALTER USER 'ann'@'%' IDENTIFIED BY 'new'");
    ok(&mut u, "SET PASSWORD = 'mine'");
    ok(&mut a, "GRANT ALL PRIVILEGES ON *.* TO 'ann'@'%'");
    ok(&mut u, "DELETE FROM t");
    ok(&mut a, "REVOKE ALL PRIVILEGES ON *.* FROM 'ann'@'%'");
    refused(&mut u, "DELETE FROM t");
}

#[test]
fn tsql_syntax() {
    let (e, _keep, _d) = engine("mssql");
    let mut a = e.new_session(Dialect::MsSql, "admin", None);
    ok(&mut a, "CREATE TABLE dbo.t (id INT PRIMARY KEY, secret NVARCHAR(50), n INT)");
    ok(&mut a, "INSERT INTO t VALUES (1,'s',1)");
    ok(&mut a, "CREATE LOGIN ann WITH PASSWORD = 'Str0ng!pw', CHECK_POLICY = OFF");
    ok(&mut a, "CREATE USER ann FOR LOGIN ann");
    let mut u = e.new_session(Dialect::MsSql, "ann", None);
    refused(&mut u, "SELECT * FROM t");
    ok(&mut a, "GRANT SELECT ON OBJECT::dbo.t TO ann");
    assert_eq!(rows(&mut u, "SELECT id FROM t"), vec![vec!["1"]]);
    // DENY wins over any grant, and only REVOKE lifts it.
    ok(&mut a, "DENY SELECT ON dbo.t TO ann");
    ok(&mut a, "GRANT SELECT ON dbo.t TO ann");
    refused(&mut u, "SELECT id FROM t");
    ok(&mut a, "REVOKE SELECT ON dbo.t FROM ann");
    refused(&mut u, "SELECT id FROM t");
    // Column list after the object.
    ok(&mut a, "GRANT SELECT ON dbo.t (id, n) TO ann");
    assert_eq!(rows(&mut u, "SELECT id FROM t"), vec![vec!["1"]]);
    refused(&mut u, "SELECT secret FROM t");
    ok(&mut a, "REVOKE SELECT ON dbo.t (id, n) FROM ann");
    // Schema-wide, database permissions and fixed roles.
    ok(&mut a, "GRANT SELECT ON SCHEMA::dbo TO ann");
    assert_eq!(rows(&mut u, "SELECT n FROM t"), vec![vec!["1"]]);
    ok(&mut a, "REVOKE SELECT ON SCHEMA::dbo FROM ann");
    refused(&mut u, "SELECT n FROM t");
    refused(&mut u, "CREATE TABLE u1 (x INT)");
    ok(&mut a, "GRANT CREATE TABLE TO ann");
    ok(&mut u, "CREATE TABLE u1 (x INT)");
    ok(&mut a, "ALTER ROLE db_datareader ADD MEMBER ann");
    assert_eq!(rows(&mut u, "SELECT IS_MEMBER('db_datareader')"), vec![vec!["1"]]);
    assert_eq!(rows(&mut u, "SELECT HAS_PERMS_BY_NAME('dbo.t', 'OBJECT', 'SELECT')"), vec![vec!["1"]]);
    assert_eq!(rows(&mut u, "SELECT HAS_PERMS_BY_NAME('dbo.t', 'OBJECT', 'DELETE')"), vec![vec!["0"]]);
    refused(&mut u, "DELETE FROM t");
    ok(&mut a, "ALTER ROLE db_datareader DROP MEMBER ann");
    refused(&mut u, "SELECT n FROM t");
    ok(&mut a, "ALTER ROLE db_denydatareader ADD MEMBER ann");
    ok(&mut a, "GRANT SELECT ON dbo.t TO ann");
    refused(&mut u, "SELECT n FROM t");
    ok(&mut a, "ALTER ROLE db_denydatareader DROP MEMBER ann");
    // Impersonation needs membership; REVERT ends it.
    refused(&mut u, "EXECUTE AS USER = 'admin'");
    ok(&mut a, "EXECUTE AS USER = 'ann'");
    assert_eq!(rows(&mut a, "SELECT USER_NAME()"), vec![vec!["ann"]]);
    refused(&mut a, "DELETE FROM t");
    ok(&mut a, "REVERT");
    ok(&mut a, "DELETE FROM t");
    assert_eq!(exec(&mut a, "CREATE SECURITY POLICY p ADD FILTER PREDICATE f(x) ON t").unwrap_err().code, "0A000");
    // A policy written from one dialect binds sessions of every dialect.
    ok(&mut a, "INSERT INTO t VALUES (1,'s',1),(2,'s',2)");
    ok(&mut a, "ALTER TABLE t ENABLE ROW LEVEL SECURITY");
    ok(&mut a, "CREATE POLICY one ON t USING (id = 1)");
    assert_eq!(rows(&mut u, "SELECT id FROM t"), vec![vec!["1"]]);
    let mut pg = e.new_session(Dialect::Postgres, "ann", None);
    assert_eq!(rows(&mut pg, "SELECT id FROM t"), vec![vec!["1"]]);
}

#[test]
fn connect_is_open_until_closed() {
    let (e, _keep, _d) = engine("connect");
    let mut a = e.new_session(Dialect::Postgres, "admin", None);
    ok(&mut a, "CREATE ROLE ann LOGIN");
    ok(&mut a, "CREATE ROLE bob LOGIN");
    let u = e.new_session(Dialect::Postgres, "ann", None);
    assert!(u.may_connect().is_ok());
    ok(&mut a, "REVOKE CONNECT ON DATABASE storine FROM PUBLIC");
    assert_eq!(u.may_connect().unwrap_err().code, "42501");
    ok(&mut a, "GRANT CONNECT ON DATABASE storine TO ann");
    assert!(u.may_connect().is_ok());
    let b = e.new_session(Dialect::Postgres, "bob", None);
    assert_eq!(b.may_connect().unwrap_err().code, "42501");
    assert!(a.may_connect().is_ok());
    ok(&mut a, "GRANT CONNECT ON DATABASE storine TO PUBLIC");
    assert!(b.may_connect().is_ok());
}

/// Trust mode: nothing changes for a deployment that has not asked for roles.
#[test]
fn trust_mode_keeps_working() {
    let dir = std::env::temp_dir().join(format!("storine_acl_trust_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let keep = storage_sql::embedded::start(&dir, EngineConfig::default()).unwrap();
    let e = Arc::clone(&keep.engine);
    let mut s = e.new_session(Dialect::Postgres, "anyone", None);
    ok(&mut s, "CREATE TABLE t (id INT PRIMARY KEY)");
    ok(&mut s, "GRANT SELECT ON t TO some_app_role");
    ok(&mut s, "ALTER TABLE t OWNER TO some_app_role");
    assert!(rows(&mut s, "SELECT * FROM t").is_empty());
    // Dropping privileges works even here: a role has only what it was given.
    ok(&mut s, "CREATE ROLE limited");
    let mut l = e.new_session(Dialect::Postgres, "limited", None);
    refused(&mut l, "SELECT * FROM t");
}

/// Audit findings (`TASK-152`): ways a role could become more than it was given.
#[test]
fn audit_no_way_up() {
    let (e, _keep, dir) = engine("audit_up");
    let mut a = e.new_session(Dialect::Postgres, "admin", None);
    ok(&mut a, "CREATE ROLE member LOGIN IN ROLE sysadmin");
    ok(&mut a, "CREATE ROLE boss CREATEROLE LOGIN");
    ok(&mut a, "CREATE ROLE low LOGIN");
    ok(&mut a, "CREATE TABLE t (a INT PRIMARY KEY, secret TEXT, owner_name TEXT)");
    ok(&mut a, "INSERT INTO t VALUES (1, 's', 'low')");
    // CREATEROLE does not reach a superuser role, directly or through membership.
    let mut boss = e.new_session(Dialect::Postgres, "boss", None);
    refused(&mut boss, "ALTER ROLE boss IN ROLE sysadmin");
    refused(&mut boss, "CREATE ROLE evil LOGIN PASSWORD 'x' IN ROLE sysadmin");
    refused(&mut boss, "ALTER ROLE member PASSWORD 'pwned'");
    let mut my = e.new_session(Dialect::MySql, "boss", None);
    refused(&mut my, "SET PASSWORD FOR member = 'pwned'");
    // A grant option on a column gives that column.
    ok(&mut a, "GRANT SELECT (a) ON t TO low WITH GRANT OPTION");
    let mut low = e.new_session(Dialect::Postgres, "low", None);
    refused(&mut low, "GRANT SELECT ON t TO low");
    refused(&mut low, "GRANT SELECT (secret) ON t TO low");
    ok(&mut low, "GRANT SELECT (a) ON t TO boss");
    refused(&mut low, "SELECT secret FROM t");
    // A temporary table's name is no way to a grant on the real one.
    let mut low2 = e.new_session(Dialect::Postgres, "low", None);
    ok(&mut low2, "CREATE TEMP TABLE t (x INT)");
    assert_eq!(exec(&mut low2, "GRANT ALL ON t TO low").map(|_| ()).unwrap_err().code, "0A000");
    ok(&mut low2, "DROP TABLE t");
    refused(&mut low2, "SELECT secret FROM t");
    // Nor is one read beside a view of the real one: their answers are not shared.
    ok(&mut a, "CREATE ROLE maker LOGIN");
    ok(&mut a, "GRANT CREATE ON DATABASE storine TO maker");
    let mut m = e.new_session(Dialect::Postgres, "maker", None);
    ok(&mut m, "CREATE VIEW v AS SELECT * FROM t");
    ok(&mut m, "CREATE TEMP TABLE t (x INT)");
    refused(&mut m, "SELECT * FROM t s LEFT JOIN v ON true");
    // Membership in a superuser role makes a role off limits to a role manager, whichever way.
    ok(&mut a, "CREATE ROLE deep LOGIN IN ROLE member");
    refused(&mut boss, "GRANT member TO boss");
    refused(&mut boss, "GRANT deep TO boss");
    refused(&mut boss, "DROP ROLE member");
    refused(&mut boss, "ALTER ROLE member RENAME TO other");
    drop((a, boss, my, low, low2, m));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn audit_row_security_holds() {
    let (e, _keep, dir) = engine("audit_rls");
    let mut a = e.new_session(Dialect::Postgres, "admin", None);
    ok(&mut a, "CREATE ROLE low LOGIN");
    ok(&mut a, "CREATE TABLE memberships (usr TEXT PRIMARY KEY, tenant INT)");
    ok(&mut a, "INSERT INTO memberships VALUES ('low', 1)");
    ok(&mut a, "CREATE TABLE docs (id INT PRIMARY KEY, tenant INT, ssn TEXT)");
    ok(&mut a, "INSERT INTO docs VALUES (1, 1, 'mine'), (2, 2, 'yours')");
    ok(&mut a, "GRANT SELECT ON docs TO low");
    ok(&mut a, "GRANT SELECT ON memberships TO low");
    ok(&mut a, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY");
    ok(&mut a, "CREATE POLICY p ON docs USING (tenant IN (SELECT tenant FROM memberships WHERE usr = current_user))");
    // The policy reads its own table, not the caller's temporary one.
    let mut low = e.new_session(Dialect::Postgres, "low", None);
    assert_eq!(rows(&mut low, "SELECT id FROM docs"), vec![vec!["1"]]);
    ok(&mut low, "CREATE TEMP TABLE memberships (usr TEXT, tenant INT)");
    ok(&mut low, "INSERT INTO memberships VALUES ('low', 2)");
    assert_eq!(rows(&mut low, "SELECT id FROM docs"), vec![vec!["1"]]);
    // Statistics are over every row: row security hides them.
    ok(&mut a, "ANALYZE docs");
    assert!(rows(&mut low, "SELECT attname FROM pg_stats WHERE tablename = 'docs'").is_empty());
    assert!(!rows(&mut a, "SELECT attname FROM pg_stats WHERE tablename = 'docs'").is_empty());
    // WITH CHECK sees what a BEFORE trigger made of the row.
    ok(&mut a, "CREATE TABLE owned (id INT PRIMARY KEY, who TEXT)");
    ok(&mut a, "GRANT SELECT, INSERT, UPDATE ON owned TO low");
    ok(&mut a, "ALTER TABLE owned ENABLE ROW LEVEL SECURITY");
    ok(&mut a, "CREATE POLICY mine ON owned USING (who = current_user) WITH CHECK (who = current_user)");
    ok(&mut a, "CREATE FUNCTION steal() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.who := 'victim'; RETURN NEW; END $$");
    ok(&mut a, "CREATE TRIGGER x BEFORE INSERT ON owned FOR EACH ROW EXECUTE FUNCTION steal()");
    refused(&mut low, "INSERT INTO owned VALUES (1, 'low')");
    drop((a, low));
    let _ = std::fs::remove_dir_all(dir);
}
