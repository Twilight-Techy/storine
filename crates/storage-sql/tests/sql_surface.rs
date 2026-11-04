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

#[test]
fn functions() {
    let (e, _k) = engine("func");
    let mut s = e.new_session(Dialect::Postgres, "app", None);
    ok(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    ok(&mut s, "INSERT INTO t VALUES (1,10),(2,20),(3,30)");
    // SQL functions.
    ok(&mut s, "CREATE FUNCTION add1(a INT) RETURNS INT LANGUAGE sql AS 'SELECT a + 1'");
    assert_eq!(col(&mut s, "SELECT add1(41)"), vec!["42"]);
    ok(&mut s, "CREATE FUNCTION pos(int) RETURNS int AS $$ SELECT $1 * 2 $$ LANGUAGE sql");
    assert_eq!(col(&mut s, "SELECT pos(4)"), vec!["8"]);
    assert_eq!(col(&mut s, "SELECT add1(v) FROM t WHERE id = 2"), vec!["21"], "per row");
    ok(&mut s, "CREATE FUNCTION big(lim INT) RETURNS SETOF INT LANGUAGE sql AS 'SELECT v FROM t WHERE v > lim ORDER BY v'");
    assert_eq!(col(&mut s, "SELECT * FROM big(15)"), vec!["20", "30"]);
    ok(&mut s, "CREATE FUNCTION rows_of(lim INT) RETURNS TABLE(a INT, b INT) LANGUAGE sql AS 'SELECT id, v FROM t WHERE id <= lim ORDER BY id'");
    assert_eq!(rows(&mut s, "SELECT b, a FROM rows_of(2)"), vec![vec!["10", "1"], vec!["20", "2"]]);
    ok(&mut s, "CREATE FUNCTION dflt(a INT, b INT DEFAULT 5) RETURNS INT LANGUAGE sql AS 'SELECT a + b'");
    assert_eq!(rows(&mut s, "SELECT dflt(1), dflt(1, 2)"), vec![vec!["6", "3"]]);
    assert_eq!(code(&mut s, "SELECT add1(1, 2)"), "42883");
    assert_eq!(code(&mut s, "SELECT nothing(1)"), "42883");
    assert_eq!(code(&mut s, "CREATE FUNCTION add1(a INT) RETURNS INT LANGUAGE sql AS 'SELECT 1'"), "42723");
    ok(&mut s, "CREATE OR REPLACE FUNCTION add1(a INT) RETURNS INT LANGUAGE sql AS 'SELECT a + 100'");
    assert_eq!(col(&mut s, "SELECT add1(1)"), vec!["101"]);

    // PL/pgSQL: variables, control flow, loops over ranges and rows.
    ok(&mut s, "CREATE FUNCTION sum_to(n INT) RETURNS INT LANGUAGE plpgsql AS $$
        DECLARE total INT := 0;
        BEGIN
            FOR i IN 1..n LOOP
                IF i % 2 = 0 THEN CONTINUE; END IF;
                total := total + i;
                EXIT WHEN total > 100;
            END LOOP;
            RETURN total;
        END $$");
    assert_eq!(col(&mut s, "SELECT sum_to(5)"), vec!["9"]);
    assert_eq!(col(&mut s, "SELECT sum_to(1000)"), vec!["121"]);
    ok(&mut s, "CREATE FUNCTION total_v() RETURNS INT LANGUAGE plpgsql AS $$
        DECLARE r RECORD; n INT := 0; m INT;
        BEGIN
            FOR r IN SELECT id, v FROM t ORDER BY id LOOP n := n + r.v; END LOOP;
            SELECT count(*) INTO m FROM t;
            RETURN n + m;
        END $$");
    assert_eq!(col(&mut s, "SELECT total_v()"), vec!["63"]);
    ok(&mut s, "CREATE FUNCTION evens(n INT) RETURNS SETOF INT LANGUAGE plpgsql AS $$
        DECLARE i INT := 0;
        BEGIN
            WHILE i < n LOOP
                i := i + 1;
                IF i % 2 = 0 THEN RETURN NEXT i; END IF;
            END LOOP;
            RETURN QUERY SELECT 100;
        END $$");
    assert_eq!(col(&mut s, "SELECT * FROM evens(5)"), vec!["2", "4", "100"]);
    // Errors and handlers.
    ok(&mut s, "CREATE FUNCTION safe_div(a INT, b INT) RETURNS INT LANGUAGE plpgsql AS $$
        BEGIN RETURN a / b;
        EXCEPTION WHEN division_by_zero THEN RETURN -1; END $$");
    assert_eq!(rows(&mut s, "SELECT safe_div(6, 2), safe_div(1, 0)"), vec![vec!["3", "-1"]]);
    ok(&mut s, "CREATE FUNCTION boom(x INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'bad value %', x USING ERRCODE = '22023'; END $$");
    let err = exec(&mut s, "SELECT boom(7)").unwrap_err();
    assert_eq!((err.code, err.message.as_str()), ("22023", "bad value 7"));
    ok(&mut s, "CREATE FUNCTION no_return() RETURNS INT LANGUAGE plpgsql AS $$ BEGIN NULL; END $$");
    assert_eq!(code(&mut s, "SELECT no_return()"), "2F005");
    assert_eq!(code(&mut s, "CREATE FUNCTION bad() RETURNS INT LANGUAGE plpgsql AS $$ BEGIN EXECUTE 'x'; END $$"), "0A000");
    assert_eq!(code(&mut s, "CREATE FUNCTION py() RETURNS INT LANGUAGE plpython3u AS 'x'"), "0A000");
    assert_eq!(code(&mut s, "CREATE FUNCTION d() RETURNS INT LANGUAGE sql SECURITY DEFINER AS 'SELECT 1'"), "0A000");
    // Runaway recursion stops.
    ok(&mut s, "CREATE FUNCTION rec(n INT) RETURNS INT LANGUAGE plpgsql AS $$ BEGIN RETURN rec(n + 1); END $$");
    assert_eq!(code(&mut s, "SELECT rec(1)"), "54001");

    // Writing: on its own and through CALL in the caller's transaction; never inside a query.
    ok(&mut s, "CREATE FUNCTION bump(k INT) RETURNS INT LANGUAGE plpgsql AS $$
        DECLARE out_v INT;
        BEGIN
            UPDATE t SET v = v + 1 WHERE id = k RETURNING v INTO out_v;
            RETURN out_v;
        END $$");
    assert_eq!(col(&mut s, "SELECT bump(1)"), vec!["11"]);
    assert_eq!(col(&mut s, "SELECT v FROM t WHERE id = 1"), vec!["11"]);
    ok(&mut s, "BEGIN");
    assert_eq!(col(&mut s, "SELECT bump(1)"), vec!["12"]);
    ok(&mut s, "ROLLBACK");
    assert_eq!(col(&mut s, "SELECT v FROM t WHERE id = 1"), vec!["11"], "it was in the transaction");
    assert_eq!(code(&mut s, "SELECT bump(id) FROM t"), "0A000");
    assert_eq!(col(&mut s, "SELECT v FROM t WHERE id = 1"), vec!["11"]);
    ok(&mut s, "CREATE PROCEDURE reset_v() LANGUAGE plpgsql AS $$ BEGIN UPDATE t SET v = 0; END $$");
    ok(&mut s, "CALL reset_v()");
    assert_eq!(col(&mut s, "SELECT sum(v) FROM t"), vec!["0"]);
    // A failed block undoes its own writes.
    ok(&mut s, "CREATE FUNCTION undo() RETURNS INT LANGUAGE plpgsql AS $$
        BEGIN
            BEGIN
                UPDATE t SET v = 99;
                RAISE EXCEPTION 'no';
            EXCEPTION WHEN others THEN NULL;
            END;
            RETURN (SELECT sum(v) FROM t);
        END $$");
    assert_eq!(col(&mut s, "SELECT undo()"), vec!["0"]);

    // Catalog, rights, drops.
    assert!(col(&mut s, "SELECT proname FROM pg_proc WHERE proname = 'bump'") == vec!["bump"]);
    ok(&mut s, "CREATE ROLE r");
    ok(&mut s, "REVOKE EXECUTE ON FUNCTION pos(int) FROM PUBLIC");
    let mut r = e.new_session(Dialect::Postgres, "r", None);
    assert_eq!(code(&mut r, "SELECT pos(1)"), "42501");
    ok(&mut s, "GRANT EXECUTE ON FUNCTION pos(int) TO r");
    assert_eq!(col(&mut r, "SELECT pos(1)"), vec!["2"]);
    assert_eq!(code(&mut r, "DROP FUNCTION pos"), "42501");
    assert_eq!(code(&mut r, "SELECT bump(1)"), "42501", "a function runs with the caller's rights");
    ok(&mut s, "DROP FUNCTION pos(int)");
    assert_eq!(code(&mut s, "SELECT pos(1)"), "42883");
    ok(&mut s, "DROP FUNCTION IF EXISTS pos");
    // A built-in keeps its name, and no other dialect pretends to have this.
    ok(&mut s, "CREATE FUNCTION lower(a INT) RETURNS INT LANGUAGE sql AS 'SELECT 1'");
    assert_eq!(col(&mut s, "SELECT lower('ABC')"), vec!["abc"]);
    let mut m = e.new_session(Dialect::MySql, "app", None);
    assert_eq!(code(&mut m, "CREATE FUNCTION f() RETURNS INT DETERMINISTIC RETURN 1"), "0A000");
}

#[test]
fn triggers() {
    let (e, _k) = engine("trig");
    let mut s = e.new_session(Dialect::Postgres, "app", None);
    ok(&mut s, "CREATE TABLE t (id INT PRIMARY KEY, v INT CHECK (v >= 0), updated INT DEFAULT 0, note TEXT)");
    ok(&mut s, "CREATE TABLE log (id SERIAL PRIMARY KEY, what TEXT, old_v INT, new_v INT)");
    // BEFORE changes the row, including what constraints then see.
    ok(&mut s, "CREATE FUNCTION touch() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            NEW.updated := COALESCE(OLD.updated, 0) + 1;
            IF NEW.v IS NULL THEN NEW.v := 7; END IF;
            RETURN NEW;
        END $$");
    ok(&mut s, "CREATE TRIGGER t_touch BEFORE INSERT OR UPDATE ON t FOR EACH ROW EXECUTE FUNCTION touch()");
    ok(&mut s, "INSERT INTO t (id, v) VALUES (1, 5)");
    assert_eq!(rows(&mut s, "SELECT v, updated FROM t"), vec![vec!["5", "1"]]);
    ok(&mut s, "INSERT INTO t (id) VALUES (2)");
    assert_eq!(rows(&mut s, "SELECT v FROM t WHERE id = 2"), vec![vec!["7"]]);
    ok(&mut s, "UPDATE t SET v = v + 1 WHERE id = 1");
    assert_eq!(rows(&mut s, "SELECT v, updated FROM t WHERE id = 1"), vec![vec!["6", "2"]], "the atomic path steps aside");
    // AFTER sees the final row, writes, and all of it is one transaction.
    ok(&mut s, "CREATE FUNCTION audit() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                INSERT INTO log (what, old_v) VALUES (TG_OP, OLD.v);
                RETURN OLD;
            END IF;
            INSERT INTO log (what, old_v, new_v) VALUES (TG_OP, OLD.v, NEW.v);
            RETURN NEW;
        END $$");
    ok(&mut s, "CREATE TRIGGER t_audit AFTER INSERT OR UPDATE OR DELETE ON t FOR EACH ROW EXECUTE PROCEDURE audit()");
    ok(&mut s, "INSERT INTO t (id, v) VALUES (3, 1)");
    ok(&mut s, "UPDATE t SET v = 2 WHERE id = 3");
    ok(&mut s, "DELETE FROM t WHERE id = 3");
    assert_eq!(rows(&mut s, "SELECT what, old_v, new_v FROM log ORDER BY id"), vec![vec!["INSERT", "NULL", "1"], vec!["UPDATE", "1", "2"], vec!["DELETE", "2", "NULL"]]);
    ok(&mut s, "BEGIN");
    ok(&mut s, "INSERT INTO t (id, v) VALUES (4, 1)");
    ok(&mut s, "ROLLBACK");
    assert_eq!(col(&mut s, "SELECT count(*) FROM log"), vec!["3"], "the trigger's write rolled back too");
    // A failing trigger fails the statement; a constraint sees what BEFORE produced.
    ok(&mut s, "CREATE FUNCTION no_neg() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.v := -1; RETURN NEW; END $$");
    ok(&mut s, "CREATE TRIGGER t_neg BEFORE UPDATE ON t FOR EACH ROW WHEN (NEW.v = 1000) EXECUTE FUNCTION no_neg()");
    assert_eq!(code(&mut s, "UPDATE t SET v = 1000 WHERE id = 1"), "23514");
    assert_eq!(col(&mut s, "SELECT v FROM t WHERE id = 1"), vec!["6"]);
    ok(&mut s, "UPDATE t SET v = 8 WHERE id = 1");
    ok(&mut s, "DROP TRIGGER t_neg ON t");
    // Returning NULL from BEFORE skips the row.
    ok(&mut s, "CREATE FUNCTION skip_it() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$");
    ok(&mut s, "CREATE TRIGGER t_skip BEFORE DELETE ON t FOR EACH ROW EXECUTE FUNCTION skip_it()");
    ok(&mut s, "DELETE FROM t");
    assert_eq!(col(&mut s, "SELECT count(*) FROM t"), vec!["2"]);
    ok(&mut s, "DROP TRIGGER t_skip ON t");
    // Statement triggers fire once.
    ok(&mut s, "CREATE TABLE counts (n INT)");
    ok(&mut s, "INSERT INTO counts VALUES (0)");
    ok(&mut s, "CREATE FUNCTION once() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE counts SET n = n + 1; RETURN NULL; END $$");
    ok(&mut s, "CREATE TRIGGER t_once AFTER UPDATE ON t FOR EACH STATEMENT EXECUTE FUNCTION once()");
    ok(&mut s, "UPDATE t SET note = 'x'");
    assert_eq!(col(&mut s, "SELECT n FROM counts"), vec!["1"]);
    // Refusals, catalog, dependents, cascades that cannot end.
    assert_eq!(code(&mut s, "CREATE TRIGGER x BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION missing()"), "42883");
    ok(&mut s, "CREATE FUNCTION plain() RETURNS INT LANGUAGE sql AS 'SELECT 1'");
    assert_eq!(code(&mut s, "CREATE TRIGGER x BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION plain()"), "42P17");
    assert_eq!(code(&mut s, "CREATE TRIGGER x INSTEAD OF UPDATE ON t FOR EACH ROW EXECUTE FUNCTION touch()"), "0A000");
    assert_eq!(code(&mut s, "SELECT touch()"), "0A000");
    assert_eq!(code(&mut s, "DROP FUNCTION touch"), "2BP01");
    assert_eq!(col(&mut s, "SELECT count(*) FROM pg_trigger WHERE tgname = 't_audit'"), vec!["1"]);
    ok(&mut s, "CREATE FUNCTION loopy() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE counts SET n = n + 1; RETURN NEW; END $$");
    ok(&mut s, "CREATE TRIGGER c_loop AFTER UPDATE ON counts FOR EACH ROW EXECUTE FUNCTION loopy()");
    assert_eq!(code(&mut s, "UPDATE counts SET n = 0"), "54001");
    // Rights: creating one takes TRIGGER; firing one needs nothing more of the writer.
    ok(&mut s, "CREATE ROLE r");
    ok(&mut s, "GRANT UPDATE ON t TO r");
    let mut r = e.new_session(Dialect::Postgres, "r", None);
    assert_eq!(code(&mut r, "CREATE TRIGGER y BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION touch()"), "42501");
    assert_eq!(code(&mut r, "UPDATE t SET note = 'r'"), "42501", "its trigger runs with the writer's rights");
}

#[test]
fn listen_and_notify() {
    let (e, _k) = engine("notify");
    let mut a = e.new_session(Dialect::Postgres, "app", None);
    let mut b = e.new_session(Dialect::Postgres, "app", None);
    ok(&mut a, "LISTEN jobs");
    assert!(a.take_notifications().is_empty());
    ok(&mut b, "NOTIFY other, 'x'");
    ok(&mut b, "NOTIFY jobs, 'one'");
    let got = a.take_notifications();
    assert_eq!(got.len(), 1);
    assert_eq!((got[0].channel.as_str(), got[0].payload.as_str(), got[0].pid), ("jobs", "one", b.id as i32));
    // On commit only; collapsed; a rollback drops them.
    ok(&mut b, "BEGIN");
    ok(&mut b, "NOTIFY jobs, 'two'");
    ok(&mut b, "NOTIFY jobs, 'two'");
    ok(&mut b, "NOTIFY jobs, 'three'");
    assert!(a.take_notifications().is_empty(), "not before the commit");
    ok(&mut b, "COMMIT");
    assert_eq!(a.take_notifications().iter().map(|n| n.payload.clone()).collect::<Vec<_>>(), vec!["two", "three"]);
    ok(&mut b, "BEGIN");
    ok(&mut b, "NOTIFY jobs, 'gone'");
    ok(&mut b, "ROLLBACK");
    assert!(a.take_notifications().is_empty());
    ok(&mut b, "SELECT pg_notify('jobs', 'fn')");
    assert_eq!(a.take_notifications()[0].payload, "fn");
    ok(&mut a, "UNLISTEN jobs");
    ok(&mut b, "NOTIFY jobs, 'late'");
    assert!(a.take_notifications().is_empty());
    ok(&mut a, "LISTEN x");
    ok(&mut a, "UNLISTEN *");
    ok(&mut b, "NOTIFY x, 'y'");
    assert!(a.take_notifications().is_empty());
}

#[test]
fn network_and_range_types() {
    let (e, _k) = engine("types");
    let mut s = e.new_session(Dialect::Postgres, "app", None);
    ok(&mut s, "CREATE TABLE net (id INT PRIMARY KEY, a INET, n CIDR, m MACADDR)");
    ok(&mut s, "INSERT INTO net VALUES (1, '192.168.1.5/24', '10.0.0.0/8', '08-00-2B-01-02-03')");
    assert_eq!(rows(&mut s, "SELECT a, n, m FROM net"), vec![vec!["192.168.1.5/24", "10.0.0.0/8", "08:00:2b:01:02:03"]]);
    assert_eq!(code(&mut s, "INSERT INTO net (id, a) VALUES (2, 'not an address')"), "22P02");
    assert_eq!(code(&mut s, "INSERT INTO net (id, n) VALUES (2, '10.0.0.1/8')"), "22P02");
    assert_eq!(code(&mut s, "SELECT 'zz'::macaddr"), "22P02");
    assert_eq!(rows(&mut s, "SELECT host(a), masklen(a), family(a), network(a), netmask(a) FROM net"), vec![vec!["192.168.1.5", "24", "4", "192.168.1.0/24", "255.255.255.0"]]);
    assert_eq!(col(&mut s, "SELECT '10.1.2.3'::inet << '10.0.0.0/8'::cidr"), vec!["t"]);
    assert_eq!(col(&mut s, "SELECT '10.0.0.0/8'::cidr >> '11.0.0.1'::inet"), vec!["f"]);
    assert_eq!(col(&mut s, "SELECT id FROM net WHERE a << '192.168.0.0/16'"), vec!["1"]);
    // Integers still shift.
    assert_eq!(col(&mut s, "SELECT 1 << 3"), vec!["8"]);

    ok(&mut s, "CREATE TABLE booking (id INT PRIMARY KEY, nights INT4RANGE, days DATERANGE, money NUMRANGE)");
    ok(&mut s, "INSERT INTO booking VALUES (1, '[1,5]', '[2024-01-01,2024-01-05)', '[1.5,2.5]'), (2, '(,3)', 'empty', NULL)");
    assert_eq!(rows(&mut s, "SELECT nights, days, money FROM booking WHERE id = 1"), vec![vec!["[1,6)", "[2024-01-01,2024-01-05)", "[1.5,2.5]"]]);
    assert_eq!(col(&mut s, "SELECT id FROM booking WHERE nights @> 2 ORDER BY id"), vec!["1", "2"]);
    assert_eq!(col(&mut s, "SELECT id FROM booking WHERE nights @> 5"), vec!["1"]);
    assert_eq!(col(&mut s, "SELECT id FROM booking WHERE nights && '[5,9)'"), vec!["1"]);
    assert_eq!(col(&mut s, "SELECT int4range(1, 5)"), vec!["[1,5)"]);
    assert_eq!(col(&mut s, "SELECT int4range(1, 5, '[]')"), vec!["[1,6)"]);
    assert_eq!(col(&mut s, "SELECT numrange(1.5, NULL)"), vec!["[1.5,)"]);
    assert_eq!(col(&mut s, "SELECT isempty(int4range(3, 3))"), vec!["t"]);
    assert_eq!(rows(&mut s, "SELECT lower_inc(nights), upper_inc(nights), upper_inf(nights), range_lower(nights), range_upper(nights) FROM booking WHERE id = 1"), vec![vec!["t", "f", "f", "1", "6"]]);
    assert_eq!(code(&mut s, "SELECT int4range(5, 1)"), "22000");
    assert_eq!(code(&mut s, "INSERT INTO booking (id, nights) VALUES (3, 'nonsense')"), "22P02");
    // JSON keeps these operators.
    assert_eq!(col(&mut s, "SELECT '[1,2,3]'::jsonb @> '[2]'::jsonb"), vec!["t"]);
    let mut m = e.new_session(Dialect::MySql, "app", None);
    assert_eq!(code(&mut m, "CREATE TABLE x (a INET)"), "0A000");
}
