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

/// What pgjdbc's `DatabaseMetaData` sends (`TASK-150`), verbatim from its 42.7 sources.
#[test]
fn pgjdbc_metadata_queries() {
    let (e, _g) = engine("pgjdbc");
    let mut s = e.new_session(Dialect::Postgres, "app", None);
    ok(&mut s, "CREATE TABLE p (a INT, b INT, label TEXT, PRIMARY KEY (a, b))");
    ok(&mut s, "CREATE TABLE c (id INT PRIMARY KEY, pa INT, pb INT, FOREIGN KEY (pa, pb) REFERENCES p (a, b))");
    ok(&mut s, "CREATE INDEX c_pa ON c (pa, pb)");
    ok(&mut s, "CREATE VIEW v AS SELECT a FROM p");

    assert_eq!(col(&mut s, "SELECT setting FROM pg_catalog.pg_settings WHERE name='max_index_keys'"), ["32"]);

    let pk = rows(&mut s, "SELECT result.TABLE_CAT, result.TABLE_SCHEM, result.TABLE_NAME, result.COLUMN_NAME, result.KEY_SEQ, result.PK_NAME FROM (SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, ct.relname AS TABLE_NAME, a.attname AS COLUMN_NAME, (information_schema._pg_expandarray(i.indkey)).n AS KEY_SEQ, ci.relname AS PK_NAME, information_schema._pg_expandarray(i.indkey) AS KEYS, a.attnum AS A_ATTNUM, i.indnkeyatts AS KEY_COUNT FROM pg_catalog.pg_class ct JOIN pg_catalog.pg_attribute a ON (ct.oid = a.attrelid) JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) JOIN pg_catalog.pg_index i ON ( a.attrelid = i.indrelid) JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) WHERE true AND ct.relname = E'p' AND i.indisprimary ) result where  result.A_ATTNUM = (result.KEYS).x  ORDER BY result.table_name, result.pk_name, result.key_seq");
    let pk: Vec<(&str, &str)> = pk.iter().map(|r| (r[3].as_str(), r[4].as_str())).collect();
    assert_eq!(pk, [("a", "1"), ("b", "2")]);

    let idx = rows(&mut s, "SELECT tmp.TABLE_CAT, tmp.TABLE_SCHEM, tmp.TABLE_NAME, tmp.NON_UNIQUE, tmp.INDEX_QUALIFIER, tmp.INDEX_NAME, tmp.TYPE, tmp.ORDINAL_POSITION, trim(both '\"' from pg_catalog.pg_get_indexdef(tmp.CI_OID, tmp.ORDINAL_POSITION, false)) AS COLUMN_NAME, CASE tmp.AM_NAME WHEN 'btree' THEN CASE tmp.I_INDOPTION[tmp.ORDINAL_POSITION - 1] & 1::smallint WHEN 1 THEN 'D' ELSE 'A' END ELSE NULL END AS ASC_OR_DESC, tmp.CARDINALITY, tmp.PAGES, tmp.FILTER_CONDITION FROM (SELECT NULL AS TABLE_CAT, n.nspname AS TABLE_SCHEM, ct.relname AS TABLE_NAME, NOT i.indisunique AS NON_UNIQUE, NULL AS INDEX_QUALIFIER, ci.relname AS INDEX_NAME, CASE i.indisclustered WHEN true THEN 1 ELSE CASE am.amname WHEN 'hash' THEN 2 ELSE 3 END END AS TYPE, (information_schema._pg_expandarray(i.indkey)).n AS ORDINAL_POSITION, ci.reltuples AS CARDINALITY, ci.relpages AS PAGES, pg_catalog.pg_get_expr(i.indpred, i.indrelid) AS FILTER_CONDITION, ci.oid AS CI_OID, i.indoption AS I_INDOPTION, am.amname AS AM_NAME FROM pg_catalog.pg_class ct JOIN pg_catalog.pg_namespace n ON (ct.relnamespace = n.oid) JOIN pg_catalog.pg_index i ON (ct.oid = i.indrelid) JOIN pg_catalog.pg_class ci ON (ci.oid = i.indexrelid) JOIN pg_catalog.pg_am am ON (ci.relam = am.oid) WHERE true  AND n.nspname = E'public' AND ct.relname = E'c') AS tmp ORDER BY NON_UNIQUE, TYPE, INDEX_NAME, ORDINAL_POSITION");
    let idx: Vec<(&str, &str, &str, &str)> = idx.iter().map(|r| (r[5].as_str(), r[7].as_str(), r[8].as_str(), r[9].as_str())).collect();
    assert_eq!(idx, [("c_pkey", "1", "id", "A"), ("c_pa", "1", "pa", "A"), ("c_pa", "2", "pb", "A")]);

    let fk = rows(&mut s, "SELECT NULL::text AS PKTABLE_CAT, pkn.nspname AS PKTABLE_SCHEM, pkc.relname AS PKTABLE_NAME, pka.attname AS PKCOLUMN_NAME, NULL::text AS FKTABLE_CAT, fkn.nspname AS FKTABLE_SCHEM, fkc.relname AS FKTABLE_NAME, fka.attname AS FKCOLUMN_NAME, pos.n AS KEY_SEQ, CASE con.confupdtype  WHEN 'c' THEN 0 WHEN 'n' THEN 2 WHEN 'd' THEN 4 WHEN 'r' THEN 1 WHEN 'p' THEN 1 WHEN 'a' THEN 3 ELSE NULL END AS UPDATE_RULE, CASE con.confdeltype  WHEN 'c' THEN 0 WHEN 'n' THEN 2 WHEN 'd' THEN 4 WHEN 'r' THEN 1 WHEN 'p' THEN 1 WHEN 'a' THEN 3 ELSE NULL END AS DELETE_RULE, con.conname AS FK_NAME, pkic.relname AS PK_NAME, CASE  WHEN con.condeferrable AND con.condeferred THEN 5 WHEN con.condeferrable THEN 6 ELSE 7 END AS DEFERRABILITY FROM  pg_catalog.pg_namespace pkn, pg_catalog.pg_class pkc, pg_catalog.pg_attribute pka,  pg_catalog.pg_namespace fkn, pg_catalog.pg_class fkc, pg_catalog.pg_attribute fka,  pg_catalog.pg_constraint con,  pg_catalog.generate_series(1, 32) pos(n),  pg_catalog.pg_class pkic WHERE pkn.oid = pkc.relnamespace AND pkc.oid = pka.attrelid AND pka.attnum = con.confkey[pos.n] AND con.confrelid = pkc.oid  AND fkn.oid = fkc.relnamespace AND fkc.oid = fka.attrelid AND fka.attnum = con.conkey[pos.n] AND con.conrelid = fkc.oid  AND con.contype = 'f' AND (pkic.relkind = 'i' OR pkic.relkind = 'I') AND pkic.oid = con.conindid  AND fkn.nspname = E'public' AND fkc.relname = E'c' ORDER BY pkn.nspname,pkc.relname, con.conname,pos.n");
    let fk: Vec<(&str, &str, &str, &str)> = fk.iter().map(|r| (r[3].as_str(), r[7].as_str(), r[8].as_str(), r[12].as_str())).collect();
    assert_eq!(fk, [("a", "pa", "1", "p_pkey"), ("b", "pb", "2", "p_pkey")]);

    // pgAdmin and SQLAlchemy read view definitions; a view is a relation to `regclass`.
    assert_eq!(col(&mut s, "SELECT pg_catalog.pg_get_viewdef('v'::regclass, true)").len(), 1);
    assert!(col(&mut s, "SELECT pg_get_viewdef('v'::regclass, true)")[0].to_lowercase().contains("select"));
    assert!(col(&mut s, "SELECT pg_get_viewdef(c.oid, true) FROM pg_class c WHERE c.relname = 'v'")[0].to_lowercase().contains("select"));
    assert_eq!(col(&mut s, "SELECT pg_get_viewdef('missing')"), ["NULL"]);

    // Composite fields; a value that is not one is refused, not read as NULL.
    assert_eq!(col(&mut s, "SELECT (information_schema._pg_expandarray(ARRAY[7,8])).x"), ["7", "8"]);
    assert_eq!(code(&mut s, "SELECT (1).x"), "42809");
    assert_eq!(code(&mut s, "SELECT (information_schema._pg_expandarray(ARRAY[7])).nope"), "42703");
    assert_eq!(col(&mut s, "SELECT indkey[1] FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'c_pa'"), ["3"]);
}

/// psqlODBC nests its joins in parentheses, around tables and around subqueries.
#[test]
fn parenthesized_joins() {
    let (e, _g) = engine("parens");
    let mut s = e.new_session(Dialect::Postgres, "app", None);
    ok(&mut s, "CREATE TABLE a (id INT PRIMARY KEY)");
    ok(&mut s, "CREATE TABLE b (id INT PRIMARY KEY, a INT)");
    ok(&mut s, "CREATE TABLE c (id INT PRIMARY KEY, b INT)");
    ok(&mut s, "INSERT INTO a VALUES (1), (2)");
    ok(&mut s, "INSERT INTO b VALUES (10, 1), (20, 2)");
    ok(&mut s, "INSERT INTO c VALUES (100, 10)");
    assert_eq!(col(&mut s, "SELECT c.id FROM ((a INNER JOIN b ON b.a = a.id) INNER JOIN c ON c.b = b.id)"), ["100"]);
    assert_eq!(col(&mut s, "SELECT count(*) FROM ((a INNER JOIN b ON b.a = a.id)) LEFT OUTER JOIN c ON c.b = b.id"), ["2"]);
    assert_eq!(col(&mut s, "SELECT r.id FROM ((((SELECT id, a FROM b) r INNER JOIN a ON a.id = r.a)) INNER JOIN c ON c.b = r.id)"), ["10"]);
    // What follows a query in its parentheses, and aliases that look like it.
    assert_eq!(col(&mut s, "SELECT id FROM ((SELECT id FROM a ORDER BY id DESC) LIMIT 1) x"), ["2"]);
    assert_eq!(col(&mut s, "SELECT minus.id FROM ((SELECT id FROM a) minus JOIN b ON b.a = minus.id) ORDER BY 1"), ["1", "2"]);
    // Still a query where it is one, and a syntax error where it is neither.
    assert_eq!(col(&mut s, "SELECT count(*) FROM ((SELECT id FROM a) UNION ALL (SELECT id FROM b)) u"), ["4"]);
    assert_eq!(col(&mut s, "SELECT x.id FROM ((SELECT id FROM a WHERE id = 2)) x"), ["2"]);
    assert_eq!(code(&mut s, "SELECT 1 FROM ((a JOIN b ON true)"), "42601");
    assert_eq!(code(&mut s, "SELECT 1 FROM (SELEC 1) x"), "42601");
}

/// What mssql-jdbc's `DatabaseMetaData` calls (`TASK-150`).
#[test]
fn tsql_metadata_procedures() {
    let (e, _g) = engine("tsqlmeta");
    let mut s = e.new_session(Dialect::MsSql, "app", None);
    ok(&mut s, "CREATE TABLE t1 (id INT PRIMARY KEY, name NVARCHAR(10))");
    ok(&mut s, "CREATE TABLE t2 (a INT UNIQUE, b INT)");
    let mut pg = e.new_session(Dialect::Postgres, "app", None);
    ok(&mut pg, "CREATE PROCEDURE pr1() LANGUAGE plpgsql AS $$ BEGIN NULL; END $$");
    // Named without EXEC, as the first statement of a batch.
    assert!(col(&mut s, "sp_datatype_info_100 @ODBCVer=3").contains(&"nvarchar".to_owned()));
    assert_eq!(col(&mut s, "EXEC sp_datatype_info @data_type = 4"), ["int"]);
    assert_eq!(code(&mut s, "sp_no_such_proc"), "42883");
    assert_eq!(rows(&mut s, "EXEC sp_special_columns_100 't1', NULL, NULL, 'R'")[0][1], "id");
    assert_eq!(rows(&mut s, "EXEC sp_special_columns_100 't2'")[0][1], "a");
    assert!(rows(&mut s, "EXEC sp_special_columns_100 't1', NULL, NULL, 'V'").is_empty());
    assert_eq!(rows(&mut s, "EXEC sp_stored_procedures 'pr1'")[0][2], "pr1;1");
    assert!(rows(&mut s, "EXEC sp_stored_procedures 'nope'").is_empty());
    ok(&mut s, "CREATE ROLE reader");
    ok(&mut s, "GRANT SELECT ON t1 TO reader");
    let g = rows(&mut s, "EXEC sp_table_privileges 't1'");
    assert_eq!((g[0][4].as_str(), g[0][5].as_str(), g[0][6].as_str()), ("reader", "SELECT", "NO"));
    assert_eq!(rows(&mut s, "EXEC sp_column_privileges 't1'").len(), 2);
    assert_eq!(rows(&mut s, "EXEC sp_column_privileges 't1', NULL, NULL, 'name'").len(), 1);
    assert!(rows(&mut s, "EXEC sp_table_privileges 't2'").is_empty());
    // Undeclared parameters, in order, once each; declared ones are not asked about.
    let p = rows(&mut s, "EXEC sp_describe_undeclared_parameters N'SELECT * FROM t1 WHERE id = @P0 AND name = @P1 OR id = @P0'");
    assert_eq!(p.iter().map(|r| r[1].as_str()).collect::<Vec<_>>(), ["@P0", "@P1"]);
    assert_eq!(p[0][3], "nvarchar(4000)");
    let p = rows(&mut s, "EXEC sp_describe_undeclared_parameters N'SELECT @a, @b', N'@a int'");
    assert_eq!(p.iter().map(|r| r[1].as_str()).collect::<Vec<_>>(), ["@b"]);
}

/// `information_schema` shows real grants and routines, in every dialect's letter case.
#[test]
fn information_schema_grants_and_routines() {
    let (e, _g) = engine("infoschema");
    let mut s = e.new_session(Dialect::Postgres, "app", None);
    ok(&mut s, "CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT)");
    ok(&mut s, "CREATE ROLE reader");
    ok(&mut s, "GRANT SELECT ON t1 TO reader");
    ok(&mut s, "GRANT UPDATE (name) ON t1 TO reader");
    ok(&mut s, "CREATE FUNCTION f1() RETURNS INT LANGUAGE plpgsql AS $$ BEGIN RETURN 1; END $$");
    assert_eq!(
        rows(&mut s, "SELECT grantee, table_name, privilege_type, is_grantable FROM information_schema.table_privileges"),
        [["reader", "t1", "SELECT", "NO"]]
    );
    assert_eq!(
        rows(&mut s, "SELECT column_name, privilege_type FROM information_schema.column_privileges ORDER BY 2, 1"),
        [["id", "SELECT"], ["name", "SELECT"], ["name", "UPDATE"]]
    );
    assert_eq!(
        rows(&mut s, "SELECT routine_name, routine_type, external_language FROM information_schema.routines"),
        [["f1", "FUNCTION", "plpgsql"]]
    );
    let mut t = e.new_session(Dialect::MsSql, "app", None);
    assert_eq!(col(&mut t, "SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES"), ["f1"]);
    assert_eq!(col(&mut t, "SELECT GRANTEE FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES"), ["reader"]);
}

/// HAVING without GROUP BY filters, and MySQL's may name aliases; Connector/J lists tables by one.
#[test]
fn having_filters_and_mysql_aliases() {
    let (e, _g) = engine("having");
    let mut s = e.new_session(Dialect::MySql, "app", None);
    ok(&mut s, "CREATE TABLE t (a INT PRIMARY KEY, g INT)");
    ok(&mut s, "CREATE TABLE u (a INT PRIMARY KEY)");
    ok(&mut s, "CREATE VIEW v AS SELECT a FROM t");
    ok(&mut s, "INSERT INTO t VALUES (1, 1), (2, 1), (3, 2)");
    assert_eq!(col(&mut s, "SELECT a FROM t HAVING a > 1 ORDER BY a"), ["2", "3"]);
    assert_eq!(col(&mut s, "SELECT a * 10 AS x FROM t HAVING x > 10 ORDER BY x"), ["20", "30"]);
    assert_eq!(rows(&mut s, "SELECT g, count(*) AS c FROM t GROUP BY g HAVING c > 1"), [["1", "2"]]);
    assert_eq!(col(&mut s, "SELECT g FROM t GROUP BY g HAVING sum(a) > 2 AND g < 2"), ["1"]);
    // An alias over a column of the same name wins, as the driver relies on.
    assert_eq!(col(&mut s, "SELECT g + 1 AS a FROM t HAVING a = 3"), ["3"]);
    assert_eq!(col(&mut s, "SELECT t.a AS b FROM t HAVING t.a = 2"), ["2"]);
    let list = |kind: &str| format!("SELECT TABLE_SCHEMA AS TABLE_CAT, TABLE_NAME, CASE WHEN TABLE_TYPE='BASE TABLE' THEN CASE WHEN TABLE_SCHEMA = 'mysql' OR TABLE_SCHEMA = 'performance_schema' THEN 'SYSTEM TABLE' ELSE 'TABLE' END WHEN TABLE_TYPE='TEMPORARY' THEN 'LOCAL_TEMPORARY' ELSE TABLE_TYPE END AS TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'storine' AND TABLE_NAME LIKE '%' HAVING TABLE_TYPE IN ('{kind}',null,null,null,null) ORDER BY TABLE_TYPE, TABLE_SCHEMA, TABLE_NAME");
    assert_eq!(rows(&mut s, &list("TABLE")).iter().map(|r| r[1].clone()).collect::<Vec<_>>(), ["t", "u"]);
    assert_eq!(rows(&mut s, &list("VIEW")).iter().map(|r| r[1].clone()).collect::<Vec<_>>(), ["v"]);
    // Metabase's time zone probe.
    assert_eq!(col(&mut s, "SELECT time_format(timediff(now(), convert_tz(now(), @@GLOBAL.time_zone, '+00:00')), '%H:%i') AS 'offset'"), ["00:00"]);
    assert_eq!(col(&mut s, "SELECT time_format(timediff('2024-01-01 10:00:00', '2024-01-01 12:30:00'), '%H:%i')"), ["-02:30"]);
}

/// pg_attribute has index columns: psqlODBC's SQLPrimaryKeys joins them.
#[test]
fn psqlodbc_catalog_queries() {
    let (e, _g) = engine("psqlodbc");
    let mut s = e.new_session(Dialect::Postgres, "app", None);
    ok(&mut s, "CREATE TABLE p (a INT, b INT, PRIMARY KEY (b, a))");
    ok(&mut s, "CREATE TABLE c (id INT PRIMARY KEY, pa INT, pb INT, FOREIGN KEY (pb, pa) REFERENCES p (b, a))");
    let pk = col(&mut s, "select ta.attname, ia.attnum, ic.relname, n.nspname, tc.relname from pg_catalog.pg_attribute ta, pg_catalog.pg_attribute ia, pg_catalog.pg_class tc, pg_catalog.pg_index i, pg_catalog.pg_namespace n, pg_catalog.pg_class ic where tc.relname = 'p' AND n.nspname = 'public' AND tc.oid = i.indrelid AND n.oid = tc.relnamespace AND i.indisprimary = 't' AND ia.attrelid = i.indexrelid AND ta.attrelid = i.indrelid AND ta.attnum = i.indkey[ia.attnum-1] AND (NOT ta.attisdropped) AND (NOT ia.attisdropped) AND ic.oid = i.indexrelid order by ia.attnum");
    assert_eq!(pk, ["b", "a"]);
    let fk = rows(&mut s, "select ref.oid, a1.attname, a2.attname, ref.i from ((((((( (select cn.oid, conrelid, conkey, confrelid, confkey, generate_series(array_lower(conkey, 1), array_upper(conkey, 1)) i, confupdtype, confdeltype, conname, condeferrable, condeferred from pg_catalog.pg_constraint cn, pg_catalog.pg_class c, pg_catalog.pg_namespace n where contype = 'f' and conrelid = c.oid and relname = 'c' and n.oid = c.relnamespace and n.nspname = 'public') ref inner join pg_catalog.pg_class c1 on c1.oid = ref.conrelid) inner join pg_catalog.pg_namespace n1 on n1.oid = c1.relnamespace) inner join pg_catalog.pg_attribute a1 on a1.attrelid = c1.oid and a1.attnum = conkey[i]) inner join pg_catalog.pg_class c2 on c2.oid = ref.confrelid) inner join pg_catalog.pg_namespace n2 on n2.oid = c2.relnamespace) inner join pg_catalog.pg_attribute a2 on a2.attrelid = c2.oid and a2.attnum = confkey[i]) left outer join pg_catalog.pg_constraint cn on cn.conrelid = ref.confrelid and cn.contype = 'p') order by ref.oid, ref.i");
    let fk: Vec<(&str, &str)> = fk.iter().map(|r| (r[1].as_str(), r[2].as_str())).collect();
    assert_eq!(fk, [("pb", "b"), ("pa", "a")]);
    // Metabase's table listing reaches views too, and column-wise grants.
    ok(&mut s, "CREATE VIEW w AS SELECT a FROM p");
    assert_eq!(col(&mut s, "SELECT pg_catalog.has_any_column_privilege(current_user, '\"public\".\"p\"', 'update')"), ["t"]);
    assert_eq!(col(&mut s, "SELECT has_table_privilege(current_user, '\"public\".\"w\"', 'select')"), ["t"]);
    assert_eq!(col(&mut s, "SELECT has_table_privilege(current_user, 'nothing', 'select')"), ["f"]);
}

/// Audit findings over SQL (`TASK-152`): each of these was a way past a check or a wrong answer.
#[test]
fn audit_temp_tables_cannot_stand_in_for_real_ones() {
    let (e, _g) = engine("audit_temp");
    let mut admin = e.new_session(Dialect::Postgres, "app", None);
    ok(&mut admin, "CREATE TABLE payroll (id INT PRIMARY KEY, pay INT)");
    ok(&mut admin, "CREATE TABLE helper (id INT PRIMARY KEY)");
    ok(&mut admin, "INSERT INTO helper VALUES (1)");
    ok(&mut admin, "INSERT INTO payroll VALUES (1, 100), (2, 200)");
    ok(&mut admin, "CREATE VIEW v AS SELECT id FROM helper");
    // A grant kept by name must not outlive the temporary table it was made on.
    ok(&mut admin, "CREATE TEMP TABLE payroll2 (x INT)");
    assert_eq!(code(&mut admin, "GRANT ALL ON payroll2 TO PUBLIC"), "0A000");
    // A view reads its own table, not a temporary one named like it.
    ok(&mut admin, "CREATE TEMP TABLE helper (id INT PRIMARY KEY)");
    ok(&mut admin, "INSERT INTO helper VALUES (7), (8)");
    assert_eq!(col(&mut admin, "SELECT id FROM v"), ["1"]);
    assert_eq!(col(&mut admin, "SELECT count(*) FROM helper"), ["2"]);
}

#[test]
fn audit_optimizer_keeps_results() {
    let (e, _g) = engine("audit_opt");
    let mut s = e.new_session(Dialect::Postgres, "app", None);
    ok(&mut s, "CREATE TABLE o (k INT PRIMARY KEY, w INT)");
    ok(&mut s, "CREATE TABLE i (id INT PRIMARY KEY, k INT, v INT)");
    ok(&mut s, "INSERT INTO o VALUES (1, 10), (2, 20)");
    ok(&mut s, "INSERT INTO i VALUES (1, 1, 5)");
    // An aggregate always gives a row, so EXISTS is true for every outer row.
    assert_eq!(col(&mut s, "SELECT k FROM o WHERE EXISTS (SELECT count(*) FROM i WHERE i.k = o.k) ORDER BY k"), ["1", "2"]);
    assert!(col(&mut s, "SELECT k FROM o WHERE NOT EXISTS (SELECT max(v) FROM i WHERE i.k = o.k)").is_empty());
    // The aggregate's argument names the outer row.
    assert_eq!(col(&mut s, "SELECT (SELECT sum(i.v + o.w) FROM i WHERE i.k = o.k) FROM o ORDER BY k"), ["15", "NULL"]);
    // Cached subqueries of two arms of a statement are different queries.
    assert_eq!(col(&mut s, "SELECT (SELECT max(k) FROM o) UNION ALL SELECT (SELECT min(k) FROM o)"), ["2", "1"]);
    // Equal values join whatever their numeric type.
    assert_eq!(col(&mut s, "SELECT count(*) FROM (SELECT 1.5::float8 AS a) x JOIN (SELECT 1.5::numeric AS b) y ON a = b"), ["1"]);
    // An unqualified ON name stays on the table it resolved to.
    ok(&mut s, "CREATE TABLE a (k INT, x INT)");
    ok(&mut s, "CREATE TABLE b (k INT, j INT)");
    ok(&mut s, "CREATE TABLE c (j INT, x INT)");
    ok(&mut s, "INSERT INTO a SELECT g % 10, g % 2 FROM generate_series(1, 2000) g");
    ok(&mut s, "INSERT INTO b SELECT g, g FROM generate_series(0, 9) g");
    ok(&mut s, "INSERT INTO c SELECT g, 0 FROM generate_series(0, 9) g");
    ok(&mut s, "ANALYZE");
    assert_eq!(col(&mut s, "SELECT count(*) FROM a JOIN b ON a.k = b.k AND x = 1 JOIN c ON b.j = c.j"), ["1000"]);
}

#[test]
fn audit_limits_hold() {
    let (e, _g) = engine("audit_limits");
    let mut s = e.new_session(Dialect::Postgres, "app", None);
    // A setting past what a duration holds is refused; it used to abort the process later.
    let _ = exec(&mut s, "SET statement_timeout = '100000000000000000000000000'");
    assert_eq!(col(&mut s, "SELECT 1"), ["1"]);
    // Function bodies nested in the parser.
    let deep = format!("CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $$ {} $$", "BEGIN ".repeat(200_000));
    assert_eq!(code(&mut s, &deep), "54001");
    // `..` inside a string is the string's.
    ok(&mut s, "CREATE FUNCTION dots() RETURNS text LANGUAGE plpgsql AS $$ BEGIN RETURN 'a..b'; END $$");
    assert_eq!(col(&mut s, "SELECT dots()"), ["a..b"]);
    ok(&mut s, "CREATE FUNCTION loops() RETURNS int LANGUAGE plpgsql AS $$ DECLARE t int := 0; BEGIN FOR i IN 1..10 LOOP t := t + i; END LOOP; RETURN t; END $$");
    assert_eq!(col(&mut s, "SELECT loops()"), ["55"]);
    // A routine's returned rows count against the statement's memory.
    ok(&mut s, "SET max_query_memory = '1MB'");
    ok(&mut s, "CREATE FUNCTION boom() RETURNS SETOF int LANGUAGE plpgsql AS $$ BEGIN LOOP RETURN NEXT 1; END LOOP; END $$");
    assert_eq!(code(&mut s, "SELECT * FROM boom()"), "53200");
    // More columns than a wire format can say.
    let wide = format!("SELECT {}", vec!["1"; 5000].join(","));
    assert_eq!(code(&mut s, &wide), "54011");
    // Bad cursor arithmetic stops at the ends.
    ok(&mut s, "BEGIN");
    ok(&mut s, "DECLARE c SCROLL CURSOR FOR SELECT 1");
    ok(&mut s, "FETCH 1 FROM c");
    assert!(col(&mut s, "FETCH RELATIVE 9223372036854775807 FROM c").is_empty());
    ok(&mut s, "COMMIT");
    // Parentheses that open both readings cost no exponential time.
    let t = std::time::Instant::now();
    let nested = format!("SELECT * FROM {}", "((SELECT * FROM ".repeat(70));
    assert_eq!(code(&mut s, &nested), "42601");
    assert!(t.elapsed() < std::time::Duration::from_secs(5), "{:?}", t.elapsed());
}

#[test]
fn audit_expressions_that_could_not_be_kept_are_refused() {
    let (e, _g) = engine("audit_ddl");
    let mut s = e.new_session(Dialect::Postgres, "app", None);
    assert_eq!(code(&mut s, "CREATE TABLE t (a INT DEFAULT (SELECT 1))"), "0A000");
    assert_eq!(code(&mut s, "CREATE TABLE t (a INT, CHECK (a IN (SELECT 1)))"), "0A000");
    ok(&mut s, "CREATE TABLE t (a INT)");
    assert_eq!(code(&mut s, "ALTER TABLE t ALTER COLUMN a SET DEFAULT (SELECT 1)"), "0A000");
}
