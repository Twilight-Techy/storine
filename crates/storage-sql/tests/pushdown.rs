//! Aggregate push-down (`TASK-130`): every query returns exactly the same rows and column types
//! with push-down on and off, in all three dialects, over the row store, column segments and a
//! delta; EXPLAIN reports the push-down; the dialects' columnar DDL works.

use storage_sql::embedded::{self, Embedded};
use storage_sql::exec::{EngineConfig, Outcome, ResultSet, Session};
use storage_sql::parser::parse;
use storage_sql::{Dialect, SqlResult};

fn setup(tag: &str) -> Embedded {
    let dir = std::env::temp_dir().join(format!("storine_sql_push_{tag}_{}", std::process::id()));
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

fn ok(s: &mut Session, sql: &str) {
    exec(s, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn rows(s: &mut Session, sql: &str) -> ResultSet {
    match exec(s, sql).unwrap_or_else(|e| panic!("{sql}: {e}")) {
        Outcome::Rows(rs) => rs,
        other => panic!("{sql}: {other:?}"),
    }
}

fn set_pushdown(s: &mut Session, on: bool) {
    s.vars.insert(
        "storine_pushdown".into(),
        if on { "on" } else { "off" }.into(),
    );
}

/// The same query with push-down and without: identical values (including decimal scales),
/// column names and types.
fn same(s: &mut Session, sql: &str) {
    set_pushdown(s, true);
    let pushed = rows(s, sql);
    set_pushdown(s, false);
    let local = rows(s, sql);
    set_pushdown(s, true);
    assert_eq!(pushed.columns, local.columns, "columns of {sql}");
    assert_eq!(pushed.rows.len(), local.rows.len(), "row count of {sql}");
    for (a, b) in pushed.rows.iter().zip(&local.rows) {
        for (x, y) in a.iter().zip(b) {
            let (tx, ty) = (x.as_text(), y.as_text());
            assert!(
                tx == ty && std::mem::discriminant(x) == std::mem::discriminant(y),
                "{sql}: pushed {x:?} vs local {y:?}"
            );
        }
    }
}

fn plan(s: &mut Session, sql: &str) -> String {
    rows(s, &format!("EXPLAIN {sql}")).rows[0][0].as_text()
}

fn load(s: &mut Session, table: &str, from: i64, to: i64) {
    let mut values = Vec::new();
    for i in from..to {
        let region = ["eu", "us", "apac", "latam"][(i % 4) as usize];
        let note = if i % 5 == 0 {
            "NULL".to_owned()
        } else {
            format!("'n{}'", i % 37)
        };
        values.push(format!(
            "({i}, '{region}', {}, {}.{:02}, {}.{}, '2024-01-{:02}', {}, {note}, {})",
            i % 50,
            10 + i % 90,
            i % 100,
            i % 3,
            i % 7,
            1 + i % 28,
            if i % 3 == 0 { "true" } else { "false" },
            (i as f64) / 7.0
        ));
        if values.len() == 400 {
            ok(
                s,
                &format!("INSERT INTO {table} VALUES {}", values.join(", ")),
            );
            values.clear();
        }
    }
    if !values.is_empty() {
        ok(
            s,
            &format!("INSERT INTO {table} VALUES {}", values.join(", ")),
        );
    }
}

const QUERIES: &[&str] = &[
    "SELECT region, count(*), sum(qty), avg(qty), min(price), max(price), sum(price * qty) FROM sales GROUP BY region ORDER BY region",
    "SELECT count(*), count(note), count(DISTINCT region), sum(price), avg(price), avg(disc), sum(disc) FROM sales WHERE day >= '2024-01-10' AND qty < 30",
    "SELECT region, flag, count(*) FROM sales WHERE region <> 'eu' OR qty BETWEEN 3 AND 7 GROUP BY region, flag HAVING count(*) > 10 ORDER BY 3 DESC, 1, 2 LIMIT 4",
    "SELECT day, sum(qty) FROM sales WHERE note IS NULL GROUP BY day ORDER BY day",
    "SELECT region, max(day), min(note), count(DISTINCT note) FROM sales WHERE note LIKE 'n1%' GROUP BY region ORDER BY 1",
    "SELECT sum(qty) + 1, avg(disc) FROM sales WHERE region IN ('eu', 'us', NULL)",
    "SELECT region, sum(qty) AS s FROM sales GROUP BY region HAVING sum(qty) > 100 ORDER BY s DESC, region",
    "SELECT region, count(*) FROM sales WHERE qty > 1000 GROUP BY region",
    "SELECT count(*), sum(qty), avg(price), min(day) FROM sales WHERE qty > 1000",
    "SELECT sum(-price + 2.5), sum(price - disc) FROM sales WHERE flag = true AND note IS NOT NULL",
    "SELECT qty, count(*) FROM sales WHERE qty >= 45 GROUP BY qty ORDER BY qty",
    "SELECT CASE WHEN region = 'eu' THEN 'E' ELSE 'X' END AS r, count(*) FROM sales GROUP BY region ORDER BY 1, 2",
    // Not pushed down (a constant that does not convert exactly, floats, an expression key):
    // still equal.
    "SELECT count(*) FROM sales WHERE price = 10.005",
    "SELECT avg(score), sum(score) FROM sales",
    "SELECT region, count(*) FROM sales GROUP BY region, qty % 2 ORDER BY 1, 2",
];

#[test]
fn pushed_down_aggregates_equal_local_execution_in_every_dialect() {
    let db = setup("pg");
    let mut s = db.engine.new_session(Dialect::Postgres, "app", None);
    ok(&mut s, "CREATE TABLE sales (id INT PRIMARY KEY, region VARCHAR(10), qty INT, price NUMERIC(10,2), disc NUMERIC, day DATE, flag BOOLEAN, note TEXT, score DOUBLE PRECISION)");
    load(&mut s, "sales", 0, 3000);
    for q in QUERIES {
        same(&mut s, q);
    }
    let p = plan(&mut s, QUERIES[0]);
    assert!(
        p.contains("aggregate push-down") && p.contains("row store"),
        "{p}"
    );
    let pushed: Vec<&str> = QUERIES
        .iter()
        .copied()
        .filter(|q| plan(&mut s, q).contains("aggregate push-down"))
        .collect();
    eprintln!("pushed down: {} of {}", pushed.len(), QUERIES.len());
    let missing: Vec<&&str> = QUERIES[..12]
        .iter()
        .filter(|q| !pushed.contains(q))
        .collect();
    assert!(missing.is_empty(), "not pushed down: {missing:#?}");
    for q in &QUERIES[12..] {
        assert!(
            !plan(&mut s, q).contains("aggregate push-down"),
            "pushed down: {q}"
        );
    }
    // Columnar storage, then a delta on top of the segments.
    ok(&mut s, "ALTER TABLE sales SET (columnar = on)");
    assert!(plan(&mut s, QUERIES[0]).contains("column segments"));
    for q in QUERIES {
        same(&mut s, q);
    }
    load(&mut s, "sales", 3000, 3300);
    ok(&mut s, "UPDATE sales SET qty = qty + 1 WHERE id % 13 = 0");
    ok(&mut s, "DELETE FROM sales WHERE id % 17 = 0");
    for q in QUERIES {
        same(&mut s, q);
    }
    // Inside a transaction that wrote to the table, the transaction's own rows count: local.
    ok(&mut s, "BEGIN");
    ok(
        &mut s,
        "INSERT INTO sales VALUES (99999, 'eu', 1, 1.00, 1, '2024-01-01', true, 'x', 1.0)",
    );
    let in_txn = rows(&mut s, "SELECT count(*) FROM sales WHERE region = 'eu'");
    ok(&mut s, "ROLLBACK");
    let after = rows(&mut s, "SELECT count(*) FROM sales WHERE region = 'eu'");
    assert_eq!(
        in_txn.rows[0][0].as_text().parse::<i64>().unwrap(),
        after.rows[0][0].as_text().parse::<i64>().unwrap() + 1
    );
    ok(&mut s, "ALTER TABLE sales SET (columnar = off)");
    assert!(plan(&mut s, QUERIES[0]).contains("row store"));
    same(&mut s, QUERIES[0]);

    // MySQL: ENGINE = COLUMNAR.
    let mut m = db.engine.new_session(Dialect::MySql, "root", None);
    ok(&mut m, "CREATE TABLE my_sales (id INT PRIMARY KEY, region VARCHAR(10), qty INT, price DECIMAL(10,2), disc DECIMAL(10,1), day DATE, flag BOOLEAN, note TEXT, score DOUBLE) ENGINE=COLUMNAR");
    load(&mut m, "my_sales", 0, 1200);
    let my = |q: &str| q.replace("FROM sales", "FROM my_sales");
    assert!(plan(&mut m, &my(QUERIES[0])).contains("column segments"));
    for q in QUERIES {
        same(&mut m, &my(q));
    }
    ok(&mut m, "ALTER TABLE my_sales ENGINE=InnoDB");
    assert!(plan(&mut m, &my(QUERIES[0])).contains("row store"));

    // SQL Server: a clustered columnstore index.
    let mut t = db.engine.new_session(Dialect::MsSql, "sa", None);
    ok(&mut t, "CREATE TABLE ms_sales (id INT PRIMARY KEY, region VARCHAR(10), qty INT, price DECIMAL(10,2), disc DECIMAL(10,1), day DATE, flag BIT, note VARCHAR(100), score FLOAT)");
    let mut values = Vec::new();
    for i in 0..800i64 {
        values.push(format!(
            "({i}, '{}', {}, {}.{:02}, {}.{}, '2024-01-{:02}', {}, {}, {})",
            ["eu", "us"][(i % 2) as usize],
            i % 20,
            10 + i % 9,
            i % 100,
            i % 3,
            i % 7,
            1 + i % 28,
            i % 2,
            if i % 4 == 0 {
                "NULL".into()
            } else {
                format!("'n{}'", i % 11)
            },
            i
        ));
    }
    ok(
        &mut t,
        &format!("INSERT INTO ms_sales VALUES {}", values.join(", ")),
    );
    ok(
        &mut t,
        "CREATE CLUSTERED COLUMNSTORE INDEX cci_sales ON ms_sales",
    );
    let mq = [
        "SELECT region, count(*), sum(qty), avg(qty), min(price), max(price) FROM ms_sales GROUP BY region ORDER BY region",
        "SELECT count(*), sum(price), avg(price) FROM ms_sales WHERE qty < 10 AND note IS NOT NULL",
        "SELECT TOP 3 qty, count(*) AS c FROM ms_sales GROUP BY qty ORDER BY c DESC, qty",
    ];
    assert!(plan(&mut t, mq[0]).contains("column segments"));
    for q in mq {
        same(&mut t, q);
    }
    ok(&mut t, "DROP INDEX cci_sales ON ms_sales");
    assert!(plan(&mut t, mq[0]).contains("row store"));
    for q in mq {
        same(&mut t, q);
    }
}

#[test]
fn pushdown_reads_no_rows_into_the_gateway() {
    let db = setup("speed");
    let mut s = db.engine.new_session(Dialect::Postgres, "app", None);
    ok(&mut s, "CREATE TABLE sales (id INT PRIMARY KEY, region VARCHAR(10), qty INT, price NUMERIC(10,2), disc NUMERIC, day DATE, flag BOOLEAN, note TEXT, score DOUBLE PRECISION) WITH (columnar = true)");
    load(&mut s, "sales", 0, 20_000);
    let q = "SELECT region, count(*), sum(price * qty), avg(qty) FROM sales WHERE qty < 40 GROUP BY region ORDER BY region";
    // The background merger builds the column segments within a few seconds.
    std::thread::sleep(std::time::Duration::from_secs(4));
    let time = |s: &mut Session, on: bool| {
        set_pushdown(s, on);
        let t = std::time::Instant::now();
        for _ in 0..3 {
            rows(s, q);
        }
        t.elapsed()
    };
    let (_, pushed, local) = (time(&mut s, true), time(&mut s, true), time(&mut s, false));
    eprintln!("20k rows: pushed {pushed:?}, local {local:?} (3 runs each)");
    assert!(
        pushed * 3 < local,
        "push-down should be much faster: {pushed:?} vs {local:?}"
    );
}
