//! SQL used by tools and ad-hoc users (`TASK-149`).

use std::sync::Arc;

use storage_sql::exec::{Engine, EngineConfig, Outcome, Session};
use storage_sql::parser::parse;
use storage_sql::{Dialect, SqlResult};

fn engine(name: &str) -> (Arc<Engine>, storage_sql::embedded::Embedded) {
    let dir = std::env::temp_dir().join(format!("storine_surface_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let e = storage_sql::embedded::start(&dir, EngineConfig::default()).unwrap();
    (Arc::clone(&e.engine), e)
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

fn col(s: &mut Session, sql: &str) -> Vec<String> {
    rows(s, sql).into_iter().map(|mut r| r.remove(0)).collect()
}

fn code(s: &mut Session, sql: &str) -> String {
    exec(s, sql).map(|_| panic!("{sql}: expected an error")).unwrap_or_else(|e| e.code.to_owned())
}

#[test]
fn generated_columns() {
    let (e, _k) = engine("gen");
    let mut s = e.new_session(Dialect::Postgres, "app", None);
    ok(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, price INT, qty INT, total INT GENERATED ALWAYS AS (price * qty) STORED, big BOOLEAN GENERATED ALWAYS AS (price * qty > 100) STORED)");
    ok(&mut s, "INSERT INTO t (id, price, qty) VALUES (1, 5, 3)");
    assert_eq!(rows(&mut s, "SELECT total, big FROM t"), vec![vec!["15", "f"]]);
    ok(&mut s, "UPDATE t SET qty = 30 WHERE id = 1");
    assert_eq!(rows(&mut s, "SELECT total, big FROM t"), vec![vec!["150", "t"]]);
    ok(&mut s, "UPDATE t SET qty = qty + 1 WHERE id = 1");
    assert_eq!(col(&mut s, "SELECT total FROM t"), vec!["155"], "the atomic update path too");
    ok(&mut s, "INSERT INTO t VALUES (1, 2, 2) ON CONFLICT (id) DO UPDATE SET price = 2");
    assert_eq!(col(&mut s, "SELECT total FROM t"), vec!["62"], "upsert");
    assert_eq!(code(&mut s, "UPDATE t SET total = 1"), "428C9");
    assert_eq!(code(&mut s, "INSERT INTO t (id, price, qty, total) VALUES (2, 1, 1, 99)"), "428C9");
    ok(&mut s, "INSERT INTO t (id, price, qty, total) VALUES (2, 1, 1, DEFAULT)");
    assert_eq!(col(&mut s, "SELECT id FROM t WHERE total = 1"), vec!["2"]);

    let mut m = e.new_session(Dialect::MySql, "app", None);
    ok(&mut m, "CREATE TABLE my (id INT PRIMARY KEY, a INT, b INT AS (a + 1), c INT GENERATED ALWAYS AS (a * 2) VIRTUAL)");
    ok(&mut m, "INSERT INTO my (id, a) VALUES (1, 10)");
    assert_eq!(rows(&mut m, "SELECT b, c FROM my"), vec![vec!["11", "20"]]);
    let mut t = e.new_session(Dialect::MsSql, "app", None);
    ok(&mut t, "CREATE TABLE ms (id INT PRIMARY KEY, a INT, b AS (a + 1) PERSISTED)");
    ok(&mut t, "INSERT INTO ms (id, a) VALUES (1, 10)");
    assert_eq!(col(&mut t, "SELECT b FROM ms"), vec!["11"]);
    // The expression survives being read back from the catalog by another gateway's refresh.
    e.refresh_catalog(true).unwrap();
    ok(&mut s, "INSERT INTO t (id, price, qty) VALUES (3, 3, 3)");
    assert_eq!(col(&mut s, "SELECT total FROM t WHERE id = 3"), vec!["9"]);
}

#[test]
fn temporary_tables_are_private_and_end() {
    let (e, _k) = engine("temp");
    let mut a = e.new_session(Dialect::Postgres, "app", None);
    let mut b = e.new_session(Dialect::Postgres, "app", None);
    ok(&mut a, "CREATE TEMP TABLE t (id INT PRIMARY KEY, v TEXT UNIQUE)");
    ok(&mut a, "INSERT INTO t VALUES (1, 'x'), (2, 'y')");
    assert_eq!(col(&mut a, "SELECT count(*) FROM t"), vec!["2"]);
    assert_eq!(code(&mut b, "SELECT * FROM t"), "42P01", "no other session reaches it");
    assert_eq!(code(&mut a, "INSERT INTO t VALUES (3, 'x')"), "23505", "constraints work");
    assert_eq!(code(&mut a, "CREATE TEMP TABLE t (x INT)"), "42P07");
    ok(&mut a, "CREATE TEMP TABLE IF NOT EXISTS t (x INT)");
    // Both sessions may use one name.
    ok(&mut b, "CREATE TEMPORARY TABLE t (id INT PRIMARY KEY)");
    ok(&mut b, "INSERT INTO t VALUES (9)");
    assert_eq!(col(&mut b, "SELECT id FROM t"), vec!["9"]);
    assert_eq!(col(&mut a, "SELECT count(*) FROM t"), vec!["2"]);
    // It hides a permanent table, and DROP takes the temporary one first.
    ok(&mut a, "CREATE TABLE perm (id INT PRIMARY KEY)");
    ok(&mut a, "INSERT INTO perm VALUES (1)");
    ok(&mut a, "CREATE TEMP TABLE perm (id INT PRIMARY KEY)");
    assert!(rows(&mut a, "SELECT * FROM perm").is_empty());
    ok(&mut a, "DROP TABLE perm");
    assert_eq!(col(&mut a, "SELECT id FROM perm"), vec!["1"]);
    // Joins, CTAS, ALTER, indexes, transactions.
    ok(&mut a, "CREATE TEMP TABLE s AS SELECT id FROM t WHERE id < 2");
    assert_eq!(col(&mut a, "SELECT t.v FROM t JOIN s ON s.id = t.id"), vec!["x"]);
    ok(&mut a, "ALTER TABLE t ADD COLUMN w INT DEFAULT 7");
    assert_eq!(col(&mut a, "SELECT w FROM t WHERE id = 1"), vec!["7"]);
    ok(&mut a, "CREATE INDEX i ON t (w)");
    ok(&mut a, "BEGIN");
    ok(&mut a, "INSERT INTO t VALUES (5, 'z', 1)");
    ok(&mut a, "ROLLBACK");
    assert_eq!(col(&mut a, "SELECT count(*) FROM t"), vec!["2"]);
    assert_eq!(code(&mut a, "ALTER TABLE t RENAME TO u"), "0A000");
    assert_eq!(code(&mut a, "CREATE TEMP TABLE f (x INT REFERENCES perm(id))"), "0A000");
    // ON COMMIT.
    ok(&mut a, "BEGIN");
    ok(&mut a, "CREATE TEMP TABLE od (x INT) ON COMMIT DROP");
    ok(&mut a, "INSERT INTO od VALUES (1)");
    assert_eq!(col(&mut a, "SELECT count(*) FROM od"), vec!["1"]);
    ok(&mut a, "COMMIT");
    assert_eq!(code(&mut a, "SELECT * FROM od"), "42P01");
    ok(&mut a, "BEGIN");
    ok(&mut a, "CREATE TEMP TABLE dr (x INT) ON COMMIT DELETE ROWS");
    ok(&mut a, "INSERT INTO dr VALUES (1)");
    ok(&mut a, "COMMIT");
    assert_eq!(col(&mut a, "SELECT count(*) FROM dr"), vec!["0"]);
    // DISCARD ALL drops them (connection pools), as does the session's end.
    let collections_before = e.store.collections().unwrap().iter().filter(|c| c.starts_with("_sql_tmp_")).count();
    assert!(collections_before > 0);
    ok(&mut a, "DISCARD ALL");
    assert_eq!(code(&mut a, "SELECT * FROM t"), "42P01");
    drop(b);
    assert_eq!(e.store.collections().unwrap().iter().filter(|c| c.starts_with("_sql_tmp_")).count(), 0, "every temporary collection is gone");
    // Hidden from the catalogs of others, and from the table list.
    let mut c = e.new_session(Dialect::Postgres, "app", None);
    ok(&mut c, "CREATE TEMP TABLE hidden (x INT)");
    let mut d = e.new_session(Dialect::Postgres, "app", None);
    assert!(rows(&mut d, "SELECT table_name FROM information_schema.tables WHERE table_name = 'hidden'").is_empty());

    // The other dialects.
    let mut m = e.new_session(Dialect::MySql, "app", None);
    ok(&mut m, "CREATE TEMPORARY TABLE mt (id INT PRIMARY KEY)");
    ok(&mut m, "INSERT INTO mt VALUES (1)");
    assert_eq!(col(&mut m, "SELECT id FROM mt"), vec!["1"]);
    ok(&mut m, "DROP TEMPORARY TABLE mt");
    let mut t = e.new_session(Dialect::MsSql, "app", None);
    ok(&mut t, "CREATE TABLE #work (id INT PRIMARY KEY)");
    ok(&mut t, "INSERT INTO #work VALUES (1)");
    assert_eq!(col(&mut t, "SELECT id FROM #work"), vec!["1"]);
    let mut t2 = e.new_session(Dialect::MsSql, "app", None);
    assert_eq!(code(&mut t2, "SELECT id FROM #work"), "42P01");
}

#[test]
fn cursors() {
    let (e, _k) = engine("cursor");
    let mut s = e.new_session(Dialect::Postgres, "app", None);
    ok(&mut s, "CREATE TABLE t (id INT PRIMARY KEY)");
    ok(&mut s, "INSERT INTO t VALUES (1),(2),(3),(4),(5)");
    assert_eq!(code(&mut s, "DECLARE c CURSOR FOR SELECT id FROM t ORDER BY id"), "25P01", "needs a transaction");
    ok(&mut s, "BEGIN");
    ok(&mut s, "DECLARE c SCROLL CURSOR WITHOUT HOLD FOR SELECT id FROM t ORDER BY id");
    assert_eq!(col(&mut s, "FETCH c"), vec!["1"]);
    assert_eq!(col(&mut s, "FETCH 2 FROM c"), vec!["2", "3"]);
    assert_eq!(col(&mut s, "FETCH FORWARD 1 IN c"), vec!["4"]);
    assert_eq!(col(&mut s, "FETCH PRIOR FROM c"), vec!["3"]);
    assert_eq!(col(&mut s, "FETCH BACKWARD 2 FROM c"), vec!["2", "1"]);
    assert!(col(&mut s, "FETCH BACKWARD 1 FROM c").is_empty(), "before the first");
    assert_eq!(col(&mut s, "FETCH NEXT FROM c"), vec!["1"]);
    assert_eq!(col(&mut s, "FETCH LAST FROM c"), vec!["5"]);
    assert_eq!(col(&mut s, "FETCH FIRST FROM c"), vec!["1"]);
    assert_eq!(col(&mut s, "FETCH ABSOLUTE 3 FROM c"), vec!["3"]);
    assert_eq!(col(&mut s, "FETCH ABSOLUTE -1 FROM c"), vec!["5"]);
    assert_eq!(col(&mut s, "FETCH RELATIVE -2 FROM c"), vec!["3"]);
    assert_eq!(col(&mut s, "FETCH RELATIVE 0 FROM c"), vec!["3"], "the current row again");
    assert!(col(&mut s, "FETCH ABSOLUTE 99 FROM c").is_empty());
    assert_eq!(col(&mut s, "FETCH BACKWARD ALL FROM c"), vec!["5", "4", "3", "2", "1"]);
    assert_eq!(col(&mut s, "FETCH ALL FROM c"), vec!["1", "2", "3", "4", "5"]);
    assert!(matches!(exec(&mut s, "MOVE BACKWARD 2 FROM c").unwrap(), Outcome::Command { affected: 2, .. }));
    assert_eq!(col(&mut s, "FETCH c"), vec!["5"]);
    // A cursor sees the rows as they were.
    ok(&mut s, "DELETE FROM t WHERE id = 5");
    assert!(matches!(exec(&mut s, "MOVE ABSOLUTE 5 FROM c").unwrap(), Outcome::Command { affected: 1, .. }));
    assert_eq!(code(&mut s, "DECLARE c CURSOR FOR SELECT 1"), "42P03");
    ok(&mut s, "ROLLBACK");
    assert_eq!(code(&mut s, "FETCH c"), "34000", "it ended with its transaction");
    assert_eq!(col(&mut s, "SELECT count(*) FROM t"), vec!["5"]);
    ok(&mut s, "BEGIN");
    ok(&mut s, "DECLARE c CURSOR FOR SELECT 1");
    ok(&mut s, "CLOSE c");
    assert_eq!(code(&mut s, "FETCH c"), "34000");
    ok(&mut s, "ROLLBACK");
    assert_eq!(code(&mut s, "CLOSE c"), "34000");
    ok(&mut s, "BEGIN");
    ok(&mut s, "DECLARE w CURSOR WITH HOLD FOR SELECT id FROM t ORDER BY id");
    ok(&mut s, "DECLARE x CURSOR FOR SELECT id FROM t");
    ok(&mut s, "COMMIT");
    assert_eq!(col(&mut s, "FETCH 1 FROM w"), vec!["1"], "WITH HOLD outlives its transaction");
    assert_eq!(code(&mut s, "FETCH x"), "34000", "the other does not");
    ok(&mut s, "CLOSE ALL");
    ok(&mut s, "BEGIN");
    ok(&mut s, "DECLARE y CURSOR FOR SELECT 1");
    ok(&mut s, "ROLLBACK");
    assert_eq!(code(&mut s, "FETCH y"), "34000");

    let mut t = e.new_session(Dialect::MsSql, "app", None);
    ok(&mut t, "DECLARE c CURSOR FOR SELECT id FROM t ORDER BY id");
    assert!(exec(&mut t, "FETCH NEXT FROM c").is_err(), "not open");
    ok(&mut t, "OPEN c");
    ok(&mut t, "DECLARE @id INT");
    ok(&mut t, "FETCH NEXT FROM c INTO @id");
    assert_eq!(rows(&mut t, "SELECT @id, @@FETCH_STATUS"), vec![vec!["1", "0"]]);
    ok(&mut t, "FETCH LAST FROM c INTO @id");
    assert_eq!(col(&mut t, "SELECT @id"), vec!["5"]);
    ok(&mut t, "FETCH NEXT FROM c INTO @id");
    assert_eq!(col(&mut t, "SELECT @@FETCH_STATUS"), vec!["-1"]);
    ok(&mut t, "CLOSE c");
    ok(&mut t, "OPEN c");
    ok(&mut t, "CLOSE c");
    ok(&mut t, "DEALLOCATE c");
    assert!(exec(&mut t, "OPEN c").is_err());
}

#[test]
fn materialized_views() {
    let (e, _k) = engine("mat");
    let mut s = e.new_session(Dialect::Postgres, "app", None);
    ok(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, g INT)");
    ok(&mut s, "INSERT INTO t VALUES (1,1),(2,1),(3,2)");
    ok(&mut s, "CREATE MATERIALIZED VIEW m AS SELECT g, count(*) AS n FROM t GROUP BY g");
    assert_eq!(rows(&mut s, "SELECT g, n FROM m ORDER BY g"), vec![vec!["1", "2"], vec!["2", "1"]]);
    ok(&mut s, "INSERT INTO t VALUES (4,2),(5,3)");
    assert_eq!(col(&mut s, "SELECT count(*) FROM m"), vec!["2"], "it is a snapshot");
    ok(&mut s, "REFRESH MATERIALIZED VIEW m");
    assert_eq!(rows(&mut s, "SELECT g, n FROM m ORDER BY g"), vec![vec!["1", "2"], vec!["2", "2"], vec!["3", "1"]]);
    ok(&mut s, "REFRESH MATERIALIZED VIEW CONCURRENTLY m");
    ok(&mut s, "CREATE INDEX mi ON m (g)");
    assert_eq!(col(&mut s, "SELECT matviewname FROM pg_matviews"), vec!["m"]);
    ok(&mut s, "CREATE MATERIALIZED VIEW e (a) AS SELECT id FROM t WITH NO DATA");
    assert_eq!(col(&mut s, "SELECT count(*) FROM e"), vec!["0"]);
    ok(&mut s, "REFRESH MATERIALIZED VIEW e");
    assert_eq!(col(&mut s, "SELECT count(a) FROM e"), vec!["5"]);
    assert_eq!(code(&mut s, "REFRESH MATERIALIZED VIEW t"), "42809");
    assert_eq!(code(&mut s, "CREATE MATERIALIZED VIEW m AS SELECT 1"), "42P07");
    ok(&mut s, "CREATE MATERIALIZED VIEW IF NOT EXISTS m AS SELECT 1");
    // A failed refresh leaves the old rows.
    ok(&mut s, "ALTER TABLE t ADD COLUMN z INT");
    ok(&mut s, "DROP TABLE t");
    assert!(exec(&mut s, "REFRESH MATERIALIZED VIEW m").is_err());
    assert_eq!(col(&mut s, "SELECT count(*) FROM m"), vec!["3"]);
    ok(&mut s, "DROP MATERIALIZED VIEW m, e");
    assert_eq!(code(&mut s, "SELECT * FROM m"), "42P01");
    ok(&mut s, "DROP MATERIALIZED VIEW IF EXISTS m");
    assert!(col(&mut s, "SELECT matviewname FROM pg_matviews").is_empty());
}
