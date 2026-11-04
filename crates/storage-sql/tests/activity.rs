//! Activity, statement statistics and `EXPLAIN` (`TASK-147`).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use storage_sql::exec::{Engine, EngineConfig, Outcome, Session};
use storage_sql::parser::parse;
use storage_sql::{Dialect, SqlResult};

fn engine(name: &str, config: EngineConfig) -> (Arc<Engine>, storage_sql::embedded::Embedded) {
    let dir = std::env::temp_dir().join(format!("storine_act_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let e = storage_sql::embedded::start(&dir, config).unwrap();
    (Arc::clone(&e.engine), e)
}

/// As a wire protocol runs text: it notes the text, then executes.
fn exec(s: &mut Session, sql: &str) -> SqlResult<Outcome> {
    s.note_sql(sql);
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

fn plan(s: &mut Session, sql: &str) -> String {
    rows(s, sql).into_iter().map(|r| r.join(" | ")).collect::<Vec<_>>().join("\n")
}

fn fill(s: &mut Session) {
    ok(s, "CREATE TABLE t (id INT PRIMARY KEY, grp INT, name TEXT)");
    ok(s, "CREATE TABLE u (id INT PRIMARY KEY, t_id INT)");
    ok(s, "CREATE INDEX t_grp ON t (grp)");
    let v: Vec<String> = (0..300).map(|i| format!("({i},{},'n{i}')", i % 10)).collect();
    ok(s, &format!("INSERT INTO t VALUES {}", v.join(",")));
    let v: Vec<String> = (0..50).map(|i| format!("({i},{})", i * 2)).collect();
    ok(s, &format!("INSERT INTO u VALUES {}", v.join(",")));
}

#[test]
fn pg_stat_activity_shows_what_runs() {
    let (e, _keep) = engine("activity", EngineConfig::default());
    let mut a = e.new_session(Dialect::Postgres, "alice", None);
    fill(&mut a);
    let mut b = e.new_session(Dialect::Postgres, "bob", None);
    ok(&mut b, "BEGIN");
    ok(&mut b, "SELECT count(*) FROM u");
    let me = plan(&mut a, &format!("SELECT usename, state, query FROM pg_stat_activity WHERE pid = {}", b.id));
    assert_eq!(me, "bob | idle in transaction | SELECT count(*) FROM u");
    let mine = rows(&mut a, "SELECT state, query FROM pg_stat_activity WHERE pid = pg_backend_pid()");
    assert_eq!(mine[0][0], "active");
    assert!(mine[0][1].contains("pg_stat_activity"), "{mine:?}");
    assert!(exec(&mut b, "SELECT 1/0").is_err());
    assert_eq!(plan(&mut a, &format!("SELECT state FROM pg_stat_activity WHERE pid = {}", b.id)), "idle in transaction (aborted)");
    ok(&mut b, "ROLLBACK");
    let idle = rows(&mut a, &format!("SELECT state, xact_start FROM pg_stat_activity WHERE pid = {}", b.id));
    assert_eq!(idle, vec![vec!["idle".to_owned(), "NULL".to_owned()]]);

    // A statement running in another thread is seen running, with its text; a lock wait is too.
    let mut h = e.new_session(Dialect::Postgres, "holder", None);
    ok(&mut h, "BEGIN");
    ok(&mut h, "UPDATE t SET name = 'x' WHERE id = 1");
    let mut w = e.new_session(Dialect::Postgres, "waiter", None);
    let wid = w.id;
    let t = std::thread::spawn(move || {
        ok(&mut w, "SET lock_timeout = '2s'");
        ok(&mut w, "BEGIN");
        let _ = exec(&mut w, "UPDATE t SET name = 'y' WHERE id = 1");
    });
    std::thread::sleep(Duration::from_millis(300));
    let seen = plan(&mut a, &format!("SELECT state, wait_event_type, query FROM pg_stat_activity WHERE pid = {wid}"));
    assert!(seen.starts_with("active | Lock | UPDATE t SET name = 'y'"), "{seen}");
    let locks = plan(&mut a, &format!("SELECT locktype, granted FROM pg_locks WHERE pid = {wid}"));
    assert_eq!(locks, "tuple | f", "the waiter's lock is not granted");
    let held = plan(&mut a, &format!("SELECT locktype, granted FROM pg_locks WHERE pid = {}", h.id));
    assert_eq!(held, "tuple | t");
    ok(&mut h, "ROLLBACK");
    t.join().unwrap();

    // Advisory locks too.
    ok(&mut h, "SELECT pg_advisory_lock(42)");
    assert_eq!(plan(&mut a, &format!("SELECT locktype, mode FROM pg_locks WHERE pid = {}", h.id)), "advisory | ExclusiveLock");
}

#[test]
fn statement_text_is_private_without_rights() {
    let cfg = EngineConfig { bootstrap_users: Some(HashSet::from(["admin".to_owned()])), ..EngineConfig::default() };
    let (e, _keep) = engine("private", cfg);
    let mut a = e.new_session(Dialect::Postgres, "admin", None);
    ok(&mut a, "CREATE ROLE ann LOGIN");
    ok(&mut a, "CREATE ROLE bob LOGIN");
    let mut ann = e.new_session(Dialect::Postgres, "ann", None);
    ok(&mut ann, "SELECT 'ann secret'");
    let mut bob = e.new_session(Dialect::Postgres, "bob", None);
    let seen = plan(&mut bob, &format!("SELECT query, state FROM pg_stat_activity WHERE pid = {}", ann.id));
    assert_eq!(seen, "<insufficient privilege> | NULL");
    let seen = plan(&mut a, &format!("SELECT query FROM pg_stat_activity WHERE pid = {}", ann.id));
    assert_eq!(seen, "SELECT 'ann secret'");
    // Statements keep no literals, and others' are hidden.
    let stats = plan(&mut bob, "SELECT query FROM pg_stat_statements WHERE username = 'ann'");
    assert!(stats.lines().all(|l| l == "<insufficient privilege>"), "{stats}");
    assert_eq!(exec(&mut bob, "SELECT pg_stat_statements_reset()").unwrap_err().code, "42501");
    // MySQL hides other sessions altogether without PROCESS.
    let mut mb = e.new_session(Dialect::MySql, "bob", None);
    let list = rows(&mut mb, "SHOW PROCESSLIST");
    assert!(list.iter().all(|r| r[1] == "bob"), "{list:?}");
    assert!(rows(&mut mb, "SELECT ID FROM information_schema.processlist").len() < rows(&mut a, "SELECT pid FROM pg_stat_activity").len());
}

#[test]
fn pg_stat_statements_counts() {
    let (e, _keep) = engine("stats", EngineConfig::default());
    let mut a = e.new_session(Dialect::Postgres, "alice", None);
    fill(&mut a);
    ok(&mut a, "SELECT pg_stat_statements_reset()");
    for i in 0..5 {
        ok(&mut a, &format!("SELECT name FROM t WHERE id = {i}"));
    }
    let _ = exec(&mut a, "SELECT 1/0");
    ok(&mut a, "UPDATE t SET name = 'z' WHERE grp = 1");
    let r = rows(&mut a, "SELECT query, calls, rows FROM pg_stat_statements WHERE query LIKE '%FROM t WHERE id%'");
    assert_eq!(r, vec![vec!["SELECT name FROM t WHERE id = $1".to_owned(), "5".to_owned(), "5".to_owned()]]);
    let r = rows(&mut a, "SELECT rows FROM pg_stat_statements WHERE query LIKE 'UPDATE%'");
    assert_eq!(r, vec![vec!["30".to_owned()]]);
    assert!(rows(&mut a, "SELECT query FROM pg_stat_statements WHERE query LIKE '%1/%'").is_empty(), "failures do not count");
    let times = rows(&mut a, "SELECT total_exec_time > 0, max_exec_time >= min_exec_time FROM pg_stat_statements WHERE query LIKE '%FROM t WHERE id%'");
    assert_eq!(times, vec![vec!["t".to_owned(), "t".to_owned()]]);
    // The other dialects read the same counters.
    let mut m = e.new_session(Dialect::MySql, "alice", None);
    let r = rows(&mut m, "SELECT COUNT_STAR, DIGEST_TEXT FROM performance_schema.events_statements_summary_by_digest WHERE DIGEST_TEXT LIKE '%FROM t WHERE id%'");
    assert_eq!(r[0][0], "5");
    let mut s = e.new_session(Dialect::MsSql, "alice", None);
    let r = rows(&mut s, "SELECT q.execution_count, x.text FROM sys.dm_exec_query_stats q CROSS APPLY sys.dm_exec_sql_text(q.sql_handle) x WHERE x.text LIKE '%FROM t WHERE id%'");
    assert_eq!(r[0][0], "5");
}

#[test]
fn explain_tells_what_the_executor_does() {
    let (e, _keep) = engine("explain", EngineConfig::default());
    let mut a = e.new_session(Dialect::Postgres, "alice", None);
    fill(&mut a);
    let p = plan(&mut a, "EXPLAIN SELECT * FROM t WHERE id = 5");
    assert!(p.contains("Primary Key Lookup on t"), "{p}");
    let p = plan(&mut a, "EXPLAIN SELECT * FROM t WHERE grp = 5");
    assert!(p.contains("Index Scan using grp on t") || p.contains("Storage Filter"), "{p}");
    let p = plan(&mut a, "EXPLAIN SELECT * FROM t WHERE name LIKE '%x'");
    assert!(p.contains("Seq Scan on t"), "{p}");
    let p = plan(&mut a, "EXPLAIN SELECT t.id FROM t JOIN u ON u.t_id = t.id WHERE t.grp = 1 ORDER BY t.id LIMIT 3");
    for want in ["Limit", "Sort", "Sort Key: t.id", "Hash Join", "Join Cond", "on t", "on u"] {
        assert!(p.contains(want), "{want}: {p}");
    }
    let p = plan(&mut a, "EXPLAIN SELECT grp, count(*) FROM t WHERE name <> 'x' || id::text GROUP BY grp");
    assert!(p.contains("HashAggregate") || p.contains("Aggregate Push-Down"), "{p}");
    let p = plan(&mut a, "EXPLAIN UPDATE t SET name = 'a' WHERE id = 1");
    assert!(p.contains("Update on t") && p.contains("Primary Key Lookup"), "{p}");
    assert_eq!(rows(&mut a, "SELECT name FROM t WHERE id = 1"), vec![vec!["n1".to_owned()]], "plain EXPLAIN does not run it");
    assert!(exec(&mut a, "EXPLAIN SELECT * FROM missing").is_err());

    // ANALYZE runs it, and reports what happened.
    let p = plan(&mut a, "EXPLAIN ANALYZE SELECT t.id FROM t JOIN u ON u.t_id = t.id WHERE t.grp = 0");
    assert!(p.contains("actual time=") && p.contains("Execution Time:") && p.contains("Planning Time:"), "{p}");
    assert!(p.lines().next().unwrap().contains("rows=10"), "multiples of 10 below 100: {p}");
    assert!(p.lines().any(|l| l.contains("on u") && l.contains("rows=10")), "the lookup read only matching rows: {p}");
    plan(&mut a, "EXPLAIN ANALYZE UPDATE t SET name = 'a' WHERE id = 1");
    assert_eq!(rows(&mut a, "SELECT name FROM t WHERE id = 1"), vec![vec!["a".to_owned()]], "ANALYZE really runs it");

    // JSON for tools.
    let j = plan(&mut a, "EXPLAIN (ANALYZE, FORMAT JSON) SELECT id FROM t WHERE grp = 3 ORDER BY id");
    assert!(j.starts_with("[{\"Plan\": {\"Node Type\": \"Sort\""), "{j}");
    for want in ["\"Plans\": [", "\"Relation Name\": \"t\"", "\"Actual Rows\": 30", "\"Execution Time\":"] {
        assert!(j.contains(want), "{want}: {j}");
    }
    assert!(exec(&mut a, "EXPLAIN (FORMAT YAML) SELECT 1").is_err());
    assert!(exec(&mut a, "EXPLAIN (NONSENSE) SELECT 1").is_err());

    // Row security changes the plan, and says so.
    ok(&mut a, "CREATE ROLE r");
    ok(&mut a, "GRANT SELECT ON t TO r");
    ok(&mut a, "ALTER TABLE t ENABLE ROW LEVEL SECURITY");
    let mut r = e.new_session(Dialect::Postgres, "r", None);
    let p = plan(&mut r, "EXPLAIN SELECT * FROM t WHERE id = 1");
    assert!(p.contains("Seq Scan") && p.contains("Row Security"), "{p}");
}

#[test]
fn explain_in_mysql_and_tsql() {
    let (e, _keep) = engine("explain2", EngineConfig::default());
    let mut m = e.new_session(Dialect::MySql, "alice", None);
    fill(&mut m);
    let r = rows(&mut m, "EXPLAIN SELECT * FROM t WHERE id = 5");
    assert_eq!(r[0][2], "t");
    assert_eq!(r[0][4], "const");
    assert_eq!(r[0][6], "PRIMARY");
    let r = rows(&mut m, "EXPLAIN SELECT * FROM t WHERE name = 'x'");
    assert_eq!(r[0][11], "Using where");
    let j = plan(&mut m, "EXPLAIN FORMAT=JSON SELECT * FROM t WHERE id = 5");
    assert!(j.contains("\"query_block\"") && j.contains("\"table_name\": \"t\""), "{j}");
    let tree = plan(&mut m, "EXPLAIN ANALYZE SELECT count(*) FROM t WHERE name = 'n1' OR id = 2");
    assert!(tree.contains("actual"), "{tree}");

    let mut s = e.new_session(Dialect::MsSql, "alice", None);
    ok(&mut s, "SET SHOWPLAN_TEXT ON");
    let p = plan(&mut s, "SELECT * FROM t WHERE id = 5");
    assert!(p.contains("|--Clustered Index Seek(OBJECT:([t]))"), "{p}");
    ok(&mut s, "SET SHOWPLAN_TEXT OFF");
    assert_eq!(rows(&mut s, "SELECT name FROM t WHERE id = 5"), vec![vec!["n5".to_owned()]]);
    let w = rows(&mut s, "EXEC sp_who");
    assert!(!w.is_empty());
    assert!(rows(&mut s, "SELECT session_id, status FROM sys.dm_exec_sessions").iter().any(|r| r[0] == s.id.to_string()));
    assert!(rows(&mut s, "SELECT session_id FROM sys.dm_exec_requests").iter().any(|r| r[0] == s.id.to_string()), "itself, running");
}
