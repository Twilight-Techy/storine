//! SQL executor end to end over an embedded document server.

use std::sync::Arc;

use storage_sql::datum::Datum;
use storage_sql::embedded::{self, Embedded};
use storage_sql::exec::{EngineConfig, Outcome, Session};
use storage_sql::parser::parse;
use storage_sql::{Dialect, SqlResult};

fn setup(tag: &str) -> Embedded {
    let dir = std::env::temp_dir().join(format!("storine_sql_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    embedded::start(&dir, EngineConfig::default()).unwrap()
}

fn exec(s: &mut Session, sql: &str) -> SqlResult<Outcome> {
    let (stmts, _) = parse(sql, s.dialect)?;
    let mut last = Outcome::command("");
    for st in &stmts {
        last = s.execute(st, &[])?;
    }
    Ok(last)
}

fn exec_p(s: &mut Session, sql: &str, params: &[Datum]) -> SqlResult<Outcome> {
    let (stmts, _) = parse(sql, s.dialect)?;
    s.execute(&stmts[0], params)
}

/// Rows of a query as text (NULL as "NULL").
fn rows(s: &mut Session, sql: &str) -> Vec<Vec<String>> {
    match exec(s, sql).unwrap_or_else(|e| panic!("{sql}: {e}")) {
        Outcome::Rows(rs) => rs
            .rows
            .iter()
            .map(|r| {
                r.iter()
                    .map(|d| {
                        if d.is_null() {
                            "NULL".into()
                        } else {
                            d.as_text()
                        }
                    })
                    .collect()
            })
            .collect(),
        other => panic!("{sql}: expected rows, got {other:?}"),
    }
}

fn one(s: &mut Session, sql: &str) -> String {
    rows(s, sql)[0][0].clone()
}

fn ok(s: &mut Session, sql: &str) -> Outcome {
    exec(s, sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

fn err(s: &mut Session, sql: &str) -> String {
    match exec(s, sql) {
        Ok(o) => panic!("{sql}: expected an error, got {o:?}"),
        Err(e) => e.code.to_owned(),
    }
}

#[test]
fn crud_constraints_and_queries() {
    let db = setup("crud");
    let mut s = db.engine.new_session(Dialect::Postgres, "app", None);
    ok(&mut s, "CREATE TABLE users (id SERIAL PRIMARY KEY, email VARCHAR(100) NOT NULL UNIQUE, name TEXT, age INT CHECK (age >= 0), created TIMESTAMPTZ DEFAULT now())");
    ok(&mut s, "CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE, title TEXT, score NUMERIC(10,2) DEFAULT 0)");
    let r = rows(&mut s, "INSERT INTO users (email, name, age) VALUES ('a@x', 'Ann', 30), ('b@x', 'Bob', 25) RETURNING id, email");
    assert_eq!(
        r,
        vec![
            vec!["1".to_owned(), "a@x".into()],
            vec!["2".into(), "b@x".into()]
        ]
    );
    assert_eq!(
        err(
            &mut s,
            "INSERT INTO users (email, name) VALUES ('a@x', 'dup')"
        ),
        "23505"
    );
    assert_eq!(
        err(&mut s, "INSERT INTO users (name) VALUES ('no email')"),
        "23502"
    );
    assert_eq!(
        err(&mut s, "INSERT INTO users (email, age) VALUES ('c@x', -1)"),
        "23514"
    );
    assert_eq!(
        err(
            &mut s,
            "INSERT INTO posts (user_id, title) VALUES (99, 'orphan')"
        ),
        "23503"
    );
    ok(&mut s, "INSERT INTO posts (user_id, title, score) VALUES (1, 'p1', 1.5), (1, 'p2', 2.25), (2, 'p3', 10)");
    // Joins, grouping, ordering.
    let r = rows(&mut s, "SELECT u.name, count(p.id) AS n, sum(p.score) FROM users u LEFT JOIN posts p ON p.user_id = u.id GROUP BY u.name ORDER BY n DESC, u.name");
    assert_eq!(
        r,
        vec![
            vec!["Ann".to_owned(), "2".into(), "3.75".into()],
            vec!["Bob".into(), "1".into(), "10.00".into()]
        ]
    );
    assert_eq!(one(&mut s, "SELECT title FROM posts WHERE user_id IN (SELECT id FROM users WHERE age > 26) ORDER BY title DESC LIMIT 1"), "p2");
    assert_eq!(one(&mut s, "SELECT count(*) FROM users WHERE EXISTS (SELECT 1 FROM posts WHERE posts.user_id = users.id AND score > 5)"), "1");
    assert_eq!(
        one(
            &mut s,
            "SELECT COALESCE(NULL, upper(name) || '!') FROM users WHERE id = 1"
        ),
        "ANN!"
    );
    assert_eq!(
        one(
            &mut s,
            "SELECT CASE WHEN age > 26 THEN 'old' ELSE 'young' END FROM users WHERE email = 'b@x'"
        ),
        "young"
    );
    // Unique constraint follows updates.
    assert_eq!(
        err(&mut s, "UPDATE users SET email = 'a@x' WHERE id = 2"),
        "23505"
    );
    ok(&mut s, "UPDATE users SET email = 'bob@x' WHERE id = 2");
    ok(
        &mut s,
        "INSERT INTO users (email, name) VALUES ('b@x', 'Bea')",
    );
    // Upsert.
    ok(&mut s, "INSERT INTO users (email, name) VALUES ('a@x', 'Ann2') ON CONFLICT (email) DO UPDATE SET name = excluded.name");
    assert_eq!(
        one(&mut s, "SELECT name FROM users WHERE email = 'a@x'"),
        "Ann2"
    );
    ok(
        &mut s,
        "INSERT INTO users (email, name) VALUES ('a@x', 'X') ON CONFLICT DO NOTHING",
    );
    assert_eq!(one(&mut s, "SELECT name FROM users WHERE id = 1"), "Ann2");
    // Cascade delete.
    match ok(&mut s, "DELETE FROM users WHERE id = 1") {
        Outcome::Command { affected, .. } => assert_eq!(affected, 1),
        o => panic!("{o:?}"),
    }
    assert_eq!(one(&mut s, "SELECT count(*) FROM posts"), "1");
    // Catalog introspection.
    assert_eq!(
        one(
            &mut s,
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'"
        ),
        "2"
    );
    assert_eq!(one(&mut s, "SELECT data_type FROM information_schema.columns WHERE table_name = 'posts' AND column_name = 'score'"), "numeric");
    assert_eq!(
        one(&mut s, "SELECT c.relname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname ~ '^(us)' ORDER BY 1"),
        "users"
    );
    // ALTER TABLE ADD COLUMN with default reads back for old rows.
    ok(
        &mut s,
        "ALTER TABLE users ADD COLUMN active BOOLEAN NOT NULL DEFAULT true",
    );
    assert_eq!(one(&mut s, "SELECT count(*) FROM users WHERE active"), "2");
    ok(&mut s, "ALTER TABLE users RENAME COLUMN name TO full_name");
    assert_eq!(
        one(&mut s, "SELECT full_name FROM users WHERE email = 'bob@x'"),
        "Bob"
    );
    ok(&mut s, "DROP TABLE posts");
    ok(&mut s, "DROP TABLE users");
    assert_eq!(err(&mut s, "SELECT * FROM users"), "42P01");
}

#[test]
fn transactions_are_atomic_and_never_lose_updates() {
    let db = setup("txn");
    let mut a = db.engine.new_session(Dialect::Postgres, "a", None);
    ok(&mut a, "CREATE TABLE accounts (id TEXT PRIMARY KEY, balance NUMERIC(12,2) NOT NULL CHECK (balance >= 0))");
    ok(
        &mut a,
        "INSERT INTO accounts VALUES ('alice', 100.00), ('bob', 0)",
    );
    // A failed transfer leaves nothing behind.
    ok(&mut a, "BEGIN");
    ok(
        &mut a,
        "UPDATE accounts SET balance = balance + 50 WHERE id = 'bob'",
    );
    assert_eq!(
        one(&mut a, "SELECT balance FROM accounts WHERE id = 'bob'"),
        "50.00"
    );
    assert_eq!(
        err(
            &mut a,
            "UPDATE accounts SET balance = balance - 150 WHERE id = 'alice'"
        ),
        "23514"
    );
    assert_eq!(err(&mut a, "SELECT 1"), "25P02");
    ok(&mut a, "ROLLBACK");
    assert_eq!(
        one(&mut a, "SELECT balance FROM accounts WHERE id = 'bob'"),
        "0.00"
    );
    // Concurrent autocommit deposits: exact and none lost.
    let engine = Arc::clone(&db.engine);
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let e = Arc::clone(&engine);
            std::thread::spawn(move || {
                let mut s = e.new_session(Dialect::Postgres, "w", None);
                for _ in 0..50 {
                    exec(&mut s, "UPDATE accounts SET balance = balance + 0.01 WHERE id = 'bob'").unwrap();
                }
                for _ in 0..25 {
                    // The general (read, check, conditional write, retry) path.
                    exec(&mut s, "UPDATE accounts SET balance = balance - 0.01 WHERE id = 'alice' AND balance > 0").unwrap();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(
        one(&mut a, "SELECT balance FROM accounts WHERE id = 'bob'"),
        "4.00"
    );
    assert_eq!(
        one(&mut a, "SELECT balance FROM accounts WHERE id = 'alice'"),
        "98.00"
    );
    // Two transactions that read-modify-write the same row: the second waits for the first's
    // row lock (SELECT ... FOR UPDATE) and sees its result.
    let mut b = db.engine.new_session(Dialect::Postgres, "b", None);
    ok(&mut a, "BEGIN");
    let bal = one(
        &mut a,
        "SELECT balance FROM accounts WHERE id = 'alice' FOR UPDATE",
    );
    assert_eq!(bal, "98.00");
    let e2 = Arc::clone(&db.engine);
    let waiter = std::thread::spawn(move || {
        let mut b2 = e2.new_session(Dialect::Postgres, "b2", None);
        exec(&mut b2, "BEGIN").unwrap();
        let seen = match exec(
            &mut b2,
            "SELECT balance FROM accounts WHERE id = 'alice' FOR UPDATE",
        )
        .unwrap()
        {
            Outcome::Rows(rs) => rs.rows[0][0].as_text(),
            _ => String::new(),
        };
        exec(
            &mut b2,
            "UPDATE accounts SET balance = balance - 1 WHERE id = 'alice'",
        )
        .unwrap();
        exec(&mut b2, "COMMIT").unwrap();
        seen
    });
    std::thread::sleep(std::time::Duration::from_millis(200));
    ok(
        &mut a,
        "UPDATE accounts SET balance = 90.00 WHERE id = 'alice'",
    );
    ok(&mut a, "COMMIT");
    assert_eq!(waiter.join().unwrap(), "90.00");
    assert_eq!(
        one(&mut b, "SELECT balance FROM accounts WHERE id = 'alice'"),
        "89.00"
    );
    // Optimistic conflict without locks: the later commit fails with 40001.
    ok(&mut a, "BEGIN");
    ok(&mut a, "INSERT INTO accounts VALUES ('carol', 1)");
    ok(&mut b, "INSERT INTO accounts VALUES ('carol', 2)");
    assert_eq!(err(&mut a, "COMMIT"), "23505");
    assert_eq!(
        one(&mut a, "SELECT balance FROM accounts WHERE id = 'carol'"),
        "2.00"
    );
    // Savepoints.
    ok(&mut a, "BEGIN; INSERT INTO accounts VALUES ('dan', 1); SAVEPOINT s1; INSERT INTO accounts VALUES ('eve', 1); ROLLBACK TO SAVEPOINT s1; COMMIT");
    assert_eq!(
        one(
            &mut a,
            "SELECT count(*) FROM accounts WHERE id IN ('dan', 'eve')"
        ),
        "1"
    );
}

#[test]
fn mysql_dialect_and_parameters() {
    let db = setup("mysql");
    let mut s = db.engine.new_session(Dialect::MySql, "root", Some("shop"));
    ok(&mut s, "CREATE TABLE `items` (`id` int NOT NULL AUTO_INCREMENT, `sku` varchar(20) NOT NULL, `qty` int DEFAULT 0, `active` tinyint(1) DEFAULT 1, PRIMARY KEY (`id`), UNIQUE KEY `uk_sku` (`sku`)) ENGINE=InnoDB");
    match exec_p(
        &mut s,
        "INSERT INTO items (sku, qty) VALUES (?, ?)",
        &[Datum::Text("A1".into()), Datum::Int(5)],
    )
    .unwrap()
    {
        Outcome::Command { last_insert_id, .. } => assert_eq!(last_insert_id, Some(1)),
        o => panic!("{o:?}"),
    }
    // ROW_COUNT() is the last DML's change count; FOUND_ROWS() the last query's rows.
    ok(&mut s, "UPDATE items SET qty = qty + 1 WHERE sku = 'A1'");
    assert_eq!(one(&mut s, "SELECT ROW_COUNT()"), "1");
    assert_eq!(one(&mut s, "SELECT ROW_COUNT()"), "-1");
    ok(&mut s, "UPDATE items SET qty = 0 WHERE sku = 'none'");
    assert_eq!(one(&mut s, "SELECT ROW_COUNT()"), "0");
    rows(&mut s, "SELECT * FROM items");
    assert_eq!(one(&mut s, "SELECT FOUND_ROWS()"), "1");
    ok(&mut s, "UPDATE items SET qty = 5 WHERE sku = 'A1'");
    ok(&mut s, "INSERT INTO items (sku, qty) VALUES ('B2', 1)");
    ok(&mut s, "INSERT INTO items (sku, qty) VALUES ('A1', 3) ON DUPLICATE KEY UPDATE qty = qty + VALUES(qty)");
    assert_eq!(one(&mut s, "SELECT qty FROM items WHERE sku = \"A1\""), "8");
    ok(&mut s, "INSERT IGNORE INTO items (sku) VALUES ('A1')");
    assert_eq!(one(&mut s, "SELECT COUNT(*) FROM items"), "2");
    assert_eq!(one(&mut s, "SELECT LAST_INSERT_ID()"), "2");
    assert_eq!(
        rows(&mut s, "SELECT sku FROM items ORDER BY id LIMIT 1, 1"),
        vec![vec!["B2".to_owned()]]
    );
    assert_eq!(one(&mut s, "SELECT IFNULL(NULL, CONCAT('x', 'y'))"), "xy");
    assert_eq!(one(&mut s, "SELECT DATABASE()"), "shop");
    let r = rows(&mut s, "SHOW COLUMNS FROM items");
    assert_eq!(r[0][0], "id");
    assert_eq!(r[0][3], "PRI");
    assert_eq!(rows(&mut s, "SHOW TABLES"), vec![vec!["items".to_owned()]]);
    assert!(one(&mut s, "SELECT @@version").contains("storine"));
    ok(&mut s, "SET NAMES utf8mb4");
    ok(&mut s, "SET autocommit = 0");
    ok(&mut s, "UPDATE items SET qty = 100 WHERE sku = 'B2'");
    ok(&mut s, "ROLLBACK");
    ok(&mut s, "SET autocommit = 1");
    assert_eq!(one(&mut s, "SELECT qty FROM items WHERE sku = 'B2'"), "1");
}

#[test]
fn window_functions_and_sequences() {
    let db = setup("window");
    let mut s = db.engine.new_session(Dialect::Postgres, "app", None);
    ok(
        &mut s,
        "CREATE TABLE sales (region TEXT, amount NUMERIC(10,2))",
    );
    ok(&mut s, "INSERT INTO sales VALUES ('east', 10), ('east', 20), ('east', 20), ('west', 5), ('west', 7.5)");
    let r = rows(
        &mut s,
        "SELECT region, amount, row_number() OVER (PARTITION BY region ORDER BY amount), rank() OVER (PARTITION BY region ORDER BY amount), \
         dense_rank() OVER (PARTITION BY region ORDER BY amount DESC), sum(amount) OVER (PARTITION BY region ORDER BY amount), \
         count(*) OVER (), lag(amount) OVER (PARTITION BY region ORDER BY amount), lead(amount, 1, 0) OVER (PARTITION BY region ORDER BY amount) \
         FROM sales ORDER BY region, amount",
    );
    let want = [
        [
            "east", "10.00", "1", "1", "2", "10.00", "5", "NULL", "20.00",
        ],
        [
            "east", "20.00", "2", "2", "1", "50.00", "5", "10.00", "20.00",
        ],
        ["east", "20.00", "3", "2", "1", "50.00", "5", "20.00", "0"],
        ["west", "5.00", "1", "1", "2", "5.00", "5", "NULL", "7.50"],
        ["west", "7.50", "2", "2", "1", "12.50", "5", "5.00", "0"],
    ];
    assert_eq!(
        r,
        want.iter()
            .map(|w| w.iter().map(|x| x.to_string()).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    );
    // Windows over grouped rows, and ordering by a window value.
    let r = rows(&mut s, "SELECT region, sum(amount) AS t, rank() OVER (ORDER BY sum(amount) DESC) FROM sales GROUP BY region ORDER BY 3");
    assert_eq!(
        r,
        vec![vec!["east", "50.00", "1"], vec!["west", "12.50", "2"]]
    );
    // Top-N per group through a derived table.
    let r = rows(&mut s, "SELECT region, amount FROM (SELECT region, amount, row_number() OVER (PARTITION BY region ORDER BY amount DESC) AS rn FROM sales) t WHERE rn = 1 ORDER BY region");
    assert_eq!(r, vec![vec!["east", "20.00"], vec!["west", "7.50"]]);
    assert_eq!(
        err(
            &mut s,
            "SELECT region FROM sales WHERE row_number() OVER () = 1"
        ),
        "42P20"
    );

    // Comma joins use the WHERE equalities (hash join), with the same results as JOIN ... ON.
    ok(
        &mut s,
        "CREATE TABLE regions (name TEXT PRIMARY KEY, manager TEXT)",
    );
    ok(
        &mut s,
        "INSERT INTO regions VALUES ('east', 'Ann'), ('west', 'Bo')",
    );
    let r = rows(
        &mut s,
        "SELECT g.manager, count(*), sum(s.amount) FROM sales s, regions g \
         WHERE s.region = g.name AND s.amount > 6 GROUP BY g.manager ORDER BY 1",
    );
    assert_eq!(r, vec![vec!["Ann", "3", "50.00"], vec!["Bo", "1", "7.50"]]);
    let r = rows(
        &mut s,
        "SELECT count(*) FROM sales a, sales b, regions g WHERE a.region = b.region AND b.region = g.name",
    );
    assert_eq!(r, vec![vec!["13"]]);

    ok(
        &mut s,
        "CREATE SEQUENCE order_no START WITH 100 INCREMENT BY 10",
    );
    assert_eq!(one(&mut s, "SELECT nextval('order_no')"), "100");
    assert_eq!(one(&mut s, "SELECT nextval('order_no')"), "110");
    ok(&mut s, "SELECT setval('order_no', 500)");
    assert_eq!(one(&mut s, "SELECT nextval('order_no')"), "510");
    ok(&mut s, "DROP SEQUENCE order_no");
    assert_eq!(err(&mut s, "SELECT nextval('order_no')"), "42P01");
    // The implicit sequence of a SERIAL column (Django and others reset it after bulk loads).
    ok(
        &mut s,
        "CREATE TABLE tickets (id SERIAL PRIMARY KEY, note TEXT)",
    );
    ok(&mut s, "INSERT INTO tickets (note) VALUES ('a')");
    ok(&mut s, "SELECT setval('public.tickets_id_seq', 41)");
    assert_eq!(
        one(
            &mut s,
            "INSERT INTO tickets (note) VALUES ('b') RETURNING id"
        ),
        "42"
    );
    assert_eq!(one(&mut s, "SELECT nextval('tickets_id_seq')"), "43");
}

#[test]
fn databases_are_separate_namespaces() {
    let db = setup("databases");
    let mut main = db
        .engine
        .new_session(Dialect::Postgres, "app", Some("shop"));
    ok(
        &mut main,
        "CREATE TABLE notes (id SERIAL PRIMARY KEY, body TEXT)",
    );
    ok(&mut main, "INSERT INTO notes (body) VALUES ('main')");
    ok(&mut main, "CREATE DATABASE test_shop");
    assert_eq!(err(&mut main, "CREATE DATABASE test_shop"), "42P04");
    ok(&mut main, "CREATE DATABASE IF NOT EXISTS test_shop");

    // A session on the new database sees none of the default database's tables.
    let mut t = db
        .engine
        .new_session(Dialect::Postgres, "app", Some("test_shop"));
    assert_eq!(one(&mut t, "SELECT current_database()"), "test_shop");
    assert_eq!(err(&mut t, "SELECT * FROM notes"), "42P01");
    ok(
        &mut t,
        "CREATE TABLE notes (id SERIAL PRIMARY KEY, body TEXT UNIQUE)",
    );
    ok(
        &mut t,
        "INSERT INTO notes (body) VALUES ('test'), ('test2')",
    );
    assert_eq!(one(&mut t, "SELECT max(id) FROM notes"), "2");
    assert_eq!(
        rows(&mut main, "SELECT id, body FROM notes"),
        vec![vec!["1", "main"]]
    );
    // Sequences are per database too.
    ok(&mut main, "INSERT INTO notes (body) VALUES ('main2')");
    assert_eq!(one(&mut main, "SELECT max(id) FROM notes"), "2");

    // MySQL USE switches namespaces; listings show every database.
    let mut m = db.engine.new_session(Dialect::MySql, "root", Some("shop"));
    ok(&mut m, "USE test_shop");
    assert_eq!(one(&mut m, "SELECT count(*) FROM notes"), "2");
    ok(&mut m, "USE shop");
    assert_eq!(one(&mut m, "SELECT count(*) FROM notes"), "2");
    let dbs = rows(&mut m, "SHOW DATABASES");
    assert!(
        dbs.iter().any(|r| r[0] == "test_shop") && dbs.iter().any(|r| r[0] == "shop"),
        "{dbs:?}"
    );
    let pg_dbs = rows(&mut main, "SELECT datname FROM pg_database ORDER BY 1");
    assert!(pg_dbs.contains(&vec!["test_shop".to_owned()]), "{pg_dbs:?}");

    // DROP DATABASE removes it and its tables; the name then falls back to the default.
    ok(&mut main, "DROP DATABASE test_shop");
    assert_eq!(err(&mut main, "DROP DATABASE test_shop"), "3D000");
    ok(&mut main, "DROP DATABASE IF EXISTS test_shop");
    let mut again = db
        .engine
        .new_session(Dialect::Postgres, "app", Some("test_shop"));
    assert_eq!(one(&mut again, "SELECT count(*) FROM notes"), "2");
    ok(&mut main, "CREATE DATABASE test_shop");
    let mut fresh = db
        .engine
        .new_session(Dialect::Postgres, "app", Some("test_shop"));
    assert_eq!(err(&mut fresh, "SELECT * FROM notes"), "42P01");
}
