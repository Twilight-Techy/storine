//! The optimizer (`TASK-148`) changes plans, never answers: every query here is run with it off and
//! on, over data with NULLs, skew and empty matches, and must give the same rows and columns.

use std::sync::Arc;

use storage_sql::exec::{Engine, EngineConfig, Outcome, Session};
use storage_sql::parser::parse;
use storage_sql::Dialect;
use storage_types::testing::SweepRng;

fn engine(name: &str) -> (Arc<Engine>, storage_sql::embedded::Embedded) {
    let dir = std::env::temp_dir().join(format!("storine_opt_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let e = storage_sql::embedded::start(&dir, EngineConfig::default()).unwrap();
    (Arc::clone(&e.engine), e)
}

fn exec(s: &mut Session, sql: &str) -> Result<Outcome, String> {
    let (stmts, _) = parse(sql, s.dialect).map_err(|e| format!("{}: {}", e.code, e.message))?;
    let mut last = Outcome::command("");
    for st in &stmts {
        last = s.execute(st, &[]).map_err(|e| format!("{}: {}", e.code, e.message))?;
    }
    Ok(last)
}

fn ok(s: &mut Session, sql: &str) {
    exec(s, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Column names and sorted rows.
fn answer(s: &mut Session, sql: &str) -> Result<(Vec<String>, Vec<Vec<String>>), String> {
    match exec(s, sql)? {
        Outcome::Rows(rs) => {
            let mut rows: Vec<Vec<String>> = rs.rows.iter().map(|r| r.iter().map(|d| if d.is_null() { "NULL".into() } else { d.as_text() }).collect()).collect();
            rows.sort();
            Ok((rs.columns.iter().map(|c| c.name.clone()).collect(), rows))
        }
        other => panic!("{sql}: expected rows, got {other:?}"),
    }
}

fn plan(s: &mut Session, sql: &str) -> String {
    answer_unsorted(s, &format!("EXPLAIN {sql}")).join("\n")
}

fn answer_unsorted(s: &mut Session, sql: &str) -> Vec<String> {
    match exec(s, sql).unwrap_or_else(|e| panic!("{sql}: {e}")) {
        Outcome::Rows(rs) => rs.rows.iter().map(|r| r.iter().map(|d| d.as_text()).collect::<Vec<_>>().join(" | ")).collect(),
        other => panic!("{sql}: {other:?}"),
    }
}

/// A small star: `f` (facts) to `a`, `b`, `c` by key, with NULL keys, keys with no match, and skew.
fn schema(s: &mut Session) {
    sized_schema(s, 1000);
}

fn sized_schema(s: &mut Session, facts: usize) {
    ok(s, "CREATE TABLE a (id INT PRIMARY KEY, name TEXT, grp INT)");
    ok(s, "CREATE TABLE b (id INT PRIMARY KEY, a_id INT, tag TEXT)");
    ok(s, "CREATE TABLE c (id INT PRIMARY KEY, b_id INT, v INT)");
    ok(s, "CREATE TABLE f (id INT PRIMARY KEY, a_id INT, b_id INT, c_id INT, amt INT, note TEXT)");
    ok(s, "CREATE TABLE tiny (id INT PRIMARY KEY, label TEXT)");
    ok(s, "CREATE TABLE empty_t (id INT PRIMARY KEY, a_id INT)");
    let mut rng = SweepRng::new(7);
    let insert = |s: &mut Session, table: &str, rows: Vec<String>| {
        for chunk in rows.chunks(200) {
            ok(s, &format!("INSERT INTO {table} VALUES {}", chunk.join(",")));
        }
    };
    insert(s, "a", (0..50).map(|i| format!("({i},'n{}',{})", i % 7, i % 5)).collect());
    insert(s, "b", (0..200).map(|i| format!("({i},{},'{}')", if i % 11 == 0 { "NULL".to_owned() } else { (i % 60).to_string() }, ["x", "y", "z"][i % 3])).collect());
    insert(s, "c", (0..100).map(|i| format!("({i},{},{i})", i * 3)).collect());
    let facts: Vec<String> = (0..facts)
        .map(|i| {
            // Skewed: most facts point at a few rows.
            let a = if rng.below(10) < 7 { rng.below(3) } else { rng.below(60) };
            let b = if rng.below(20) == 0 { "NULL".to_owned() } else { rng.below(220).to_string() };
            format!("({i},{a},{b},{},{},'{}')", rng.below(110), rng.below(100), if i % 2 == 0 { "even" } else { "odd" })
        })
        .collect();
    insert(s, "f", facts);
    insert(s, "tiny", (0..3).map(|i| format!("({i},'t{i}')")).collect());
}

/// Same answer with the optimizer off and on; `what` for the message.
fn same(s: &mut Session, sql: &str) {
    ok(s, "SET enable_optimizer = off");
    let off = answer(s, sql);
    ok(s, "SET enable_optimizer = on");
    let on = answer(s, sql);
    assert_eq!(off, on, "{sql}");
}

const QUERIES: &[&str] = &[
    // Join orders, written badly.
    "SELECT * FROM f JOIN b ON f.b_id = b.id JOIN c ON c.b_id = b.id",
    "SELECT * FROM f, b, a WHERE f.b_id = b.id AND b.a_id = a.id AND a.grp = 1",
    "SELECT f.id, a.name FROM f INNER JOIN a ON f.a_id = a.id WHERE a.id < 3",
    "SELECT f.id, t.label FROM f, tiny t WHERE f.a_id = t.id AND f.amt > 50",
    "SELECT * FROM f CROSS JOIN tiny WHERE f.id < 4",
    "SELECT a.id, b.id, c.id, f.id, t.id FROM f JOIN a ON f.a_id = a.id JOIN b ON f.b_id = b.id JOIN c ON c.id = f.c_id JOIN tiny t ON t.id = a.grp WHERE c.v % 2 = 0",
    "SELECT f.id FROM f JOIN a ON f.a_id = a.id AND a.grp = 2 JOIN tiny ON tiny.id = 1",
    "SELECT f.id FROM f JOIN empty_t e ON e.a_id = f.a_id",
    "SELECT count(*), sum(f.amt) FROM f JOIN a ON f.a_id = a.id JOIN b ON b.id = f.b_id",
    "SELECT a.grp, count(*) FROM f, a WHERE f.a_id = a.id GROUP BY a.grp",
    "SELECT x.id, y.id FROM a x JOIN a y ON x.grp = y.grp WHERE x.id < 8",
    "SELECT * FROM a, tiny",
    // Non-equalities and expressions across tables.
    "SELECT f.id, c.id FROM f JOIN c ON f.c_id < c.id WHERE f.id < 5 AND c.id < 5",
    "SELECT f.id FROM f JOIN a ON f.a_id + 1 = a.id + 1 WHERE a.id < 2",
    "SELECT f.id, a.id FROM f JOIN a ON f.a_id = a.id OR f.b_id = a.id WHERE f.id < 8",
    // Outer joins stay where they are, and mix with inner ones.
    "SELECT f.id, a.id FROM f LEFT JOIN a ON f.a_id = a.id WHERE f.id < 20",
    "SELECT a.id, f.id FROM a RIGHT JOIN f ON f.a_id = a.id WHERE f.id < 20",
    "SELECT f.id, a.id, b.id FROM f LEFT JOIN a ON f.a_id = a.id JOIN b ON f.b_id = b.id WHERE f.id < 40",
    "SELECT * FROM a JOIN b USING (id) WHERE a.id < 5",
    "SELECT * FROM a NATURAL JOIN b WHERE a.id < 5",
    // Correlated EXISTS, with NULLs on both sides and empty sets.
    "SELECT id FROM b WHERE EXISTS (SELECT 1 FROM a WHERE a.id = b.a_id)",
    "SELECT id FROM b WHERE NOT EXISTS (SELECT 1 FROM a WHERE a.id = b.a_id)",
    "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.a_id = a.id AND b.tag = 'x')",
    "SELECT id FROM a WHERE NOT EXISTS (SELECT 1 FROM empty_t e WHERE e.a_id = a.id)",
    "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM f, b WHERE f.b_id = b.id AND b.a_id = a.id AND f.amt > 90)",
    "SELECT id FROM a WHERE EXISTS (SELECT * FROM b WHERE b.a_id = a.id AND b.id % 2 = a.id % 2)",
    "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.a_id = a.id LIMIT 1)",
    "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE a_id = a.id)",
    "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.a_id = a.id AND a.grp = 1)",
    "SELECT id, (EXISTS (SELECT 1 FROM b WHERE b.a_id = a.id)) FROM a",
    // Same column name inside and out: the inner one wins.
    "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE id = a.id)",
    "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE id = id AND b.a_id = a.id)",
    // Correlated scalar aggregates: count over nothing is 0, the others NULL.
    "SELECT id, (SELECT count(*) FROM b WHERE b.a_id = a.id) FROM a",
    "SELECT id, (SELECT count(b.tag) FROM b WHERE b.a_id = a.id AND b.tag = 'x') FROM a",
    "SELECT id, (SELECT sum(c.v) FROM c WHERE c.b_id = b.id) FROM b",
    "SELECT id, (SELECT max(amt) FROM f WHERE f.a_id = a.id) FROM a WHERE id < 20",
    "SELECT id, (SELECT min(amt) FROM f WHERE f.a_id = a.id AND f.note = 'odd') FROM a",
    "SELECT id, (SELECT avg(v) FROM c WHERE c.b_id = b.a_id) FROM b",
    "SELECT id FROM a WHERE (SELECT count(*) FROM f WHERE f.a_id = a.id) > 5",
    "SELECT id, (SELECT count(DISTINCT f.note) FROM f WHERE f.a_id = a.id) FROM a",
    "SELECT id, (SELECT coalesce(sum(v), -1) FROM c WHERE c.b_id = b.id) FROM b",
    "SELECT id, (SELECT v FROM c WHERE c.id = a.id) FROM a",
    // With a NULL on the outer side.
    "SELECT id, (SELECT count(*) FROM a WHERE a.id = b.a_id) FROM b",
    "SELECT id, (SELECT sum(a.grp) FROM a WHERE a.id = b.a_id) FROM b",
    // Nested.
    "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.a_id = a.id AND EXISTS (SELECT 1 FROM c WHERE c.b_id = b.id))",
    "SELECT d.id FROM (SELECT a.id FROM a JOIN b ON b.a_id = a.id) d JOIN tiny ON tiny.id = d.id",
    "WITH w AS (SELECT a_id, count(*) n FROM f GROUP BY a_id) SELECT a.id, w.n FROM a JOIN w ON w.a_id = a.id",
];

#[test]
fn answers_do_not_change() {
    let (e, _keep) = engine("same");
    let mut s = e.new_session(Dialect::Postgres, "app", None);
    schema(&mut s);
    // Without statistics, and with.
    for q in QUERIES {
        same(&mut s, q);
    }
    ok(&mut s, "ANALYZE");
    for q in QUERIES {
        same(&mut s, q);
    }
    // In a transaction with its own uncommitted rows, and after statistics went stale.
    ok(&mut s, "BEGIN");
    ok(&mut s, "INSERT INTO a VALUES (1000, 'new', 1)");
    ok(&mut s, "INSERT INTO b VALUES (1000, 1000, 'x')");
    ok(&mut s, "DELETE FROM f WHERE id < 500");
    for q in QUERIES {
        same(&mut s, q);
    }
    ok(&mut s, "ROLLBACK");
    // The other dialects, whose equality and NULL rules differ.
    let mut m = e.new_session(Dialect::MySql, "app", None);
    let mut t = e.new_session(Dialect::MsSql, "app", None);
    for q in QUERIES {
        for s in [&mut m, &mut t] {
            ok(s, "SET enable_optimizer = off");
            let off = answer(s, q);
            ok(s, "SET enable_optimizer = on");
            assert_eq!(off, answer(s, q), "{:?}: {q}", s.dialect);
        }
    }
}

#[test]
fn text_and_number_keys_compare_as_the_dialect_does() {
    let (e, _keep) = engine("types");
    let mut m = e.new_session(Dialect::MySql, "app", None);
    ok(&mut m, "CREATE TABLE n (id INT PRIMARY KEY, k INT)");
    ok(&mut m, "CREATE TABLE t (id INT PRIMARY KEY, k VARCHAR(10))");
    ok(&mut m, "CREATE TABLE d (id INT PRIMARY KEY, k DECIMAL(10,2))");
    ok(&mut m, "CREATE TABLE ci (id INT PRIMARY KEY, k VARCHAR(10))");
    ok(&mut m, "INSERT INTO n VALUES (1,1),(2,2),(3,NULL)");
    ok(&mut m, "INSERT INTO t VALUES (1,'1'),(2,'01'),(3,'x'),(4,'1.0')");
    ok(&mut m, "INSERT INTO d VALUES (1,1.00),(2,1.50),(3,2)");
    ok(&mut m, "INSERT INTO ci VALUES (1,'ABC'),(2,'abc'),(3,'def')");
    ok(&mut m, "ANALYZE TABLE n, t, d, ci");
    for q in [
        "SELECT id FROM n WHERE EXISTS (SELECT 1 FROM t WHERE t.k = n.k)",
        "SELECT id, (SELECT count(*) FROM t WHERE t.k = n.k) FROM n",
        "SELECT id FROM n WHERE EXISTS (SELECT 1 FROM d WHERE d.k = n.k)",
        "SELECT id, (SELECT count(*) FROM d WHERE d.k = n.k) FROM n",
        "SELECT id, (SELECT count(*) FROM ci x WHERE x.k = ci.k) FROM ci",
        "SELECT n.id, t.id FROM n JOIN t ON n.k = t.k",
        "SELECT n.id, d.id FROM n JOIN d ON n.k = d.k",
    ] {
        ok(&mut m, "SET enable_optimizer = off");
        let off = answer(&mut m, q);
        ok(&mut m, "SET enable_optimizer = on");
        assert_eq!(off, answer(&mut m, q), "{q}");
    }
}

/// Random conjunctive joins over random data (`RULE-TEST-03`: fixed seeds).
#[test]
fn random_queries_agree() {
    let (e, _keep) = engine("random");
    let mut s = e.new_session(Dialect::Postgres, "app", None);
    // Few enough rows that a badly chosen random join stays small.
    sized_schema(&mut s, 120);
    ok(&mut s, "ANALYZE");
    let tables = ["a", "b", "c", "f", "tiny"];
    let cols: &[&[&str]] = &[&["id", "grp"], &["id", "a_id"], &["id", "b_id", "v"], &["id", "a_id", "b_id", "c_id", "amt"], &["id"]];
    let n = std::env::var("STORINE_OPT_QUERIES").ok().and_then(|v| v.parse().ok()).unwrap_or(150);
    let mut rng = SweepRng::new(42);
    for _ in 0..n {
        let picks: Vec<usize> = (0..2 + rng.below(2)).map(|_| rng.below(tables.len())).collect();
        let from: Vec<String> = picks.iter().enumerate().map(|(i, t)| format!("{} t{i}", tables[*t])).collect();
        let col = |rng: &mut SweepRng, i: usize| format!("t{i}.{}", cols[picks[i]][rng.below(cols[picks[i]].len())]);
        let mut preds = Vec::new();
        for i in 1..picks.len() {
            let j = rng.below(i);
            let (a, b) = (col(&mut rng, i), col(&mut rng, j));
            preds.push(format!("{a} = {b}"));
        }
        for _ in 0..rng.below(3) {
            let i = rng.below(picks.len());
            let c = col(&mut rng, i);
            preds.push(match rng.below(5) {
                0 => format!("{c} = {}", rng.below(20)),
                1 => format!("{c} < {}", rng.below(60)),
                2 => format!("{c} IS NOT NULL"),
                3 => format!("{c} IN ({}, {})", rng.below(10), rng.below(10)),
                _ => format!("{c} % 2 = 0"),
            });
        }
        let q = format!("SELECT * FROM {} WHERE {} LIMIT 100000", from.join(", "), preds.join(" AND "));
        same(&mut s, &q);
    }
}

#[test]
fn the_plan_follows_the_statistics() {
    let (e, _keep) = engine("plans");
    let mut s = e.new_session(Dialect::Postgres, "app", None);
    schema(&mut s);
    // No statistics: as written.
    let q = "SELECT * FROM f JOIN b ON f.b_id = b.id JOIN tiny ON tiny.id = b.id";
    let before = plan(&mut s, q);
    assert!(!before.contains("Join Order"), "{before}");
    ok(&mut s, "ANALYZE");
    let after = plan(&mut s, q);
    assert!(after.contains("chosen by the optimizer"), "{after}");
    let pos = |t: &str| after.find(t).unwrap_or_else(|| panic!("{t}: {after}"));
    assert!(pos("on tiny") < pos("on f"), "the smallest input is the deepest left: {after}");
    // Estimates against what is true.
    let rows = |p: &str| -> f64 { p.split("rows=").nth(1).unwrap().split(' ').next().unwrap().parse().unwrap() };
    let est = plan(&mut s, "SELECT * FROM f WHERE note = 'even'");
    assert!((rows(&est) - 500.0).abs() < 60.0, "common value: {est}");
    let est = plan(&mut s, "SELECT * FROM f WHERE amt < 25");
    assert!((rows(&est) - 250.0).abs() < 60.0, "histogram: {est}");
    let est = plan(&mut s, "SELECT * FROM b WHERE a_id IS NULL");
    assert!((rows(&est) - 19.0).abs() < 3.0, "null fraction: {est}");
    let est = plan(&mut s, "SELECT * FROM a WHERE id = 10");
    assert!(rows(&est) < 3.0, "a key: {est}");
    // The catalog view.
    let pg = answer_unsorted(&mut s, "SELECT n_distinct, null_frac FROM pg_stats WHERE tablename = 'a' AND attname = 'grp'");
    assert_eq!(pg, vec!["5 | 0".to_owned()]);
    // Other spellings, and refusals.
    let mut m = e.new_session(Dialect::MySql, "app", None);
    ok(&mut m, "ANALYZE TABLE a, b");
    let mut t = e.new_session(Dialect::MsSql, "app", None);
    ok(&mut t, "UPDATE STATISTICS a");
    assert!(exec(&mut s, "ANALYZE nothere").is_err());
    // Reading statistics needs the right to read the table.
    ok(&mut s, "CREATE ROLE r");
    let mut r = e.new_session(Dialect::Postgres, "r", None);
    assert_eq!(exec(&mut r, "ANALYZE a").unwrap_err().split(':').next().unwrap(), "42501");
    assert!(answer_unsorted(&mut r, "SELECT attname FROM pg_stats").is_empty());
}

/// Row security and column privileges are applied whatever the plan.
#[test]
fn restrictions_hold_under_any_order() {
    let (e, _keep) = engine("restrict");
    let mut s = e.new_session(Dialect::Postgres, "app", None);
    schema(&mut s);
    ok(&mut s, "ANALYZE");
    ok(&mut s, "CREATE ROLE r");
    ok(&mut s, "GRANT SELECT ON a, tiny TO r");
    ok(&mut s, "GRANT SELECT (id, a_id) ON f TO r");
    ok(&mut s, "ALTER TABLE a ENABLE ROW LEVEL SECURITY");
    ok(&mut s, "CREATE POLICY few ON a USING (id < 2)");
    let mut r = e.new_session(Dialect::Postgres, "r", None);
    let (_, rows) = answer(&mut r, "SELECT a.id FROM f JOIN a ON f.a_id = a.id JOIN tiny ON tiny.id = a.id").unwrap();
    assert!(!rows.is_empty() && rows.iter().all(|row| row[0] == "0" || row[0] == "1"), "{rows:?}");
    assert!(exec(&mut r, "SELECT f.amt FROM f JOIN tiny ON tiny.id = f.a_id").unwrap_err().starts_with("42501"));
    assert!(exec(&mut r, "SELECT f.id FROM f JOIN tiny ON tiny.id = f.amt").unwrap_err().starts_with("42501"));
    // The decorrelated forms too.
    let (_, rows) = answer(&mut r, "SELECT id FROM tiny WHERE EXISTS (SELECT 1 FROM a WHERE a.id = tiny.id)").unwrap();
    assert_eq!(rows, vec![vec!["0".to_owned()], vec!["1".to_owned()]]);
    let (_, rows) = answer(&mut r, "SELECT id, (SELECT count(*) FROM a WHERE a.id = tiny.id) FROM tiny").unwrap();
    assert_eq!(rows[2], vec!["2".to_owned(), "0".to_owned()]);
    assert!(exec(&mut r, "SELECT id FROM tiny WHERE EXISTS (SELECT 1 FROM f WHERE f.amt = tiny.id)").unwrap_err().starts_with("42501"));
}

/// What the optimizer buys, off and on in turn (A/B/A/B): `cargo test --release -p storage-sql
/// --test optimizer bench -- --ignored --nocapture`.
#[test]
#[ignore]
fn bench_off_and_on() {
    let (e, _keep) = engine("bench");
    let mut s = e.new_session(Dialect::Postgres, "app", None);
    ok(&mut s, "CREATE TABLE big1 (id INT PRIMARY KEY, k INT, v INT)");
    ok(&mut s, "CREATE TABLE big2 (id INT PRIMARY KEY, k INT, v INT)");
    ok(&mut s, "CREATE TABLE big3 (id INT PRIMARY KEY, k INT, v INT)");
    ok(&mut s, "CREATE TABLE small (id INT PRIMARY KEY, k INT)");
    for t in ["big1", "big2", "big3"] {
        for chunk in (0..20_000).collect::<Vec<i32>>().chunks(1000) {
            let v: Vec<String> = chunk.iter().map(|i| format!("({i},{},{i})", i % 2000)).collect();
            ok(&mut s, &format!("INSERT INTO {t} VALUES {}", v.join(",")));
        }
    }
    ok(&mut s, "INSERT INTO small VALUES (1,1),(2,2),(3,3),(4,4),(5,5)");
    ok(&mut s, "ANALYZE");
    let queries = [
        ("join: big, big, big, small (written worst first)", "SELECT count(*) FROM big1 a JOIN big2 b ON a.id = b.id JOIN big3 c ON b.id = c.id JOIN small s ON s.id = c.id"),
        ("join: comma form", "SELECT count(*) FROM big1 a, big2 b, big3 c, small s WHERE a.id = b.id AND b.id = c.id AND c.id = s.id"),
        ("join: written best first (the optimizer can only cost time here)", "SELECT count(*) FROM small s JOIN big3 c ON s.id = c.id JOIN big2 b ON b.id = c.id JOIN big1 a ON a.id = b.id"),
        ("point join", "SELECT count(*) FROM big1 a JOIN big2 b ON a.id = b.id WHERE a.id = 5"),
        ("correlated EXISTS over 20k x 20k", "SELECT count(*) FROM big1 a WHERE EXISTS (SELECT 1 FROM big2 b WHERE b.k = a.k AND b.v = a.v)"),
        ("correlated NOT EXISTS", "SELECT count(*) FROM big1 a WHERE NOT EXISTS (SELECT 1 FROM small s WHERE s.id = a.id)"),
        ("correlated scalar aggregate", "SELECT count(*), sum(x) FROM (SELECT (SELECT count(*) FROM small s WHERE s.k = a.k) x FROM big1 a) t"),
    ];
    for (name, q) in queries {
        let mut lines = Vec::new();
        for setting in ["off", "on", "off", "on"] {
            ok(&mut s, &format!("SET enable_optimizer = {setting}"));
            ok(&mut s, "SET statement_timeout = '60s'");
            let t0 = std::time::Instant::now();
            let r = exec(&mut s, q);
            let took = t0.elapsed();
            lines.push(format!("{setting} {}", if r.is_ok() { format!("{:.1} ms", took.as_secs_f64() * 1e3) } else { format!(">{:.0} s (stopped)", took.as_secs_f64()) }));
        }
        eprintln!("bench: {name}: {}", lines.join(" | "));
    }
}
