//! Control of running work (`TASK-146`): cancellation, timeouts and memory limits, through the
//! executor and over a real PostgreSQL connection (`CancelRequest` is a wire-level thing).

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use storage_sql::exec::{Engine, EngineConfig, Outcome, Session};
use storage_sql::parser::parse;
use storage_sql::{Dialect, SqlResult};

/// Enough work to be running when a signal arrives (a triple cross join of `ROWS` rows).
const ROWS: i64 = 400;
const SLOW: &str = "SELECT count(*) FROM t a, t b, t c WHERE a.id + b.id + c.id = -1";

fn engine(name: &str, config: EngineConfig) -> (Arc<Engine>, storage_sql::embedded::Embedded) {
    let dir = std::env::temp_dir().join(format!("storine_ctl_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let e = storage_sql::embedded::start(&dir, config).unwrap();
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

fn fill(s: &mut Session) {
    ok(s, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let values: Vec<String> = (0..ROWS).map(|i| format!("({i},{i})")).collect();
    ok(s, &format!("INSERT INTO t VALUES {}", values.join(",")));
}

/// Runs `SLOW` on its own thread and gives back its error once `signal` has been sent.
fn signalled(e: &Arc<Engine>, dialect: Dialect, signal: impl FnOnce(u64)) -> (String, Duration, Session) {
    let mut s = e.new_session(dialect, "app", None);
    let id = s.id;
    let t = std::thread::spawn(move || {
        let start = Instant::now();
        let r = exec(&mut s, SLOW);
        (r.map(|_| ()), start.elapsed(), s)
    });
    while !e.registry.get(id).is_some_and(|c| c.busy()) { std::thread::sleep(Duration::from_millis(10)); }
    std::thread::sleep(Duration::from_millis(100));
    assert!(!t.is_finished(), "the slow query should still be running");
    signal(id);
    let (r, took, s) = t.join().unwrap();
    (r.unwrap_err().code.to_owned(), took, s)
}

#[test]
fn statements_stop_when_cancelled_and_the_session_lives_on() {
    let (e, _keep) = engine("cancel", EngineConfig::default());
    let mut a = e.new_session(Dialect::Postgres, "admin", None);
    fill(&mut a);

    // PostgreSQL: a function from another session.
    let e2 = Arc::clone(&e);
    let (code, took, mut s) = signalled(&e, Dialect::Postgres, move |id| {
        let mut other = e2.new_session(Dialect::Postgres, "admin", None);
        ok(&mut other, &format!("SELECT pg_cancel_backend({id})"));
    });
    assert_eq!(code, "57014");
    assert!(took < Duration::from_secs(10), "{took:?}");
    ok(&mut s, "SELECT 1");

    // In a transaction, the statement fails and the transaction with it (PostgreSQL rules).
    ok(&mut s, "BEGIN");
    s.control.cancel();
    // A cancel for nothing running stays nothing: the next statement is unaffected.
    ok(&mut s, "SELECT 1");
    ok(&mut s, "ROLLBACK");

    // MySQL: KILL QUERY keeps the connection, plain KILL does not.
    let e2 = Arc::clone(&e);
    let (code, _, mut s) = signalled(&e, Dialect::MySql, move |id| {
        let mut other = e2.new_session(Dialect::MySql, "admin", None);
        ok(&mut other, &format!("KILL QUERY {id}"));
    });
    assert_eq!(code, "57014");
    ok(&mut s, "SELECT 1");
    let e2 = Arc::clone(&e);
    let (code, _, mut s) = signalled(&e, Dialect::MySql, move |id| {
        let mut other = e2.new_session(Dialect::MySql, "admin", None);
        ok(&mut other, &format!("KILL {id}"));
    });
    assert_eq!(code, "57P01");
    assert_eq!(exec(&mut s, "SELECT 1").unwrap_err().code, "57P01", "terminated stays terminated");
    assert!(s.control.fatal().is_some());

    // T-SQL.
    let e2 = Arc::clone(&e);
    let (code, _, _) = signalled(&e, Dialect::MsSql, move |id| {
        let mut other = e2.new_session(Dialect::MsSql, "admin", None);
        ok(&mut other, &format!("KILL {id}"));
    });
    assert_eq!(code, "57P01");

    // Unknown sessions.
    let mut o = e.new_session(Dialect::Postgres, "admin", None);
    assert!(matches!(exec(&mut o, "SELECT pg_cancel_backend(99999)"), Ok(Outcome::Rows(r)) if r.rows[0][0].as_text() == "f"));
    let mut m = e.new_session(Dialect::MySql, "admin", None);
    assert_eq!(exec(&mut m, "KILL 99999").unwrap_err().mysql_code(), 1094);
}

#[test]
fn only_a_superuser_or_the_same_user_may_signal() {
    let cfg = EngineConfig { bootstrap_users: Some(HashSet::from(["admin".to_owned()])), ..EngineConfig::default() };
    let (e, _keep) = engine("perm", cfg);
    let mut a = e.new_session(Dialect::Postgres, "admin", None);
    ok(&mut a, "CREATE ROLE ann LOGIN");
    ok(&mut a, "CREATE ROLE bob LOGIN");
    let mut ann = e.new_session(Dialect::Postgres, "ann", None);
    let mut bob = e.new_session(Dialect::Postgres, "bob", None);
    ok(&mut ann, "SELECT 1");
    ok(&mut a, "SELECT 1");
    assert_eq!(exec(&mut bob, &format!("SELECT pg_terminate_backend({})", ann.id)).unwrap_err().code, "42501");
    assert_eq!(exec(&mut bob, &format!("SELECT pg_cancel_backend({})", a.id)).unwrap_err().code, "42501");
    assert!(ann.control.fatal().is_none());
    let mut ann2 = e.new_session(Dialect::Postgres, "ann", None);
    ok(&mut ann2, &format!("SELECT pg_terminate_backend({})", ann.id));
    assert!(ann.control.fatal().is_some());
}

#[test]
fn timeouts() {
    let (e, _keep) = engine("timeouts", EngineConfig::default());
    let mut a = e.new_session(Dialect::Postgres, "admin", None);
    fill(&mut a);
    // Statement timeout, with units.
    ok(&mut a, "SET statement_timeout = '100ms'");
    let start = Instant::now();
    let err = exec(&mut a, SLOW).unwrap_err();
    assert_eq!(err.code, "57014");
    assert!(err.message.contains("statement timeout"), "{err}");
    assert!(start.elapsed() < Duration::from_secs(5), "{:?}", start.elapsed());
    ok(&mut a, "SELECT 1");
    ok(&mut a, "SET statement_timeout = 0");

    let mut m = e.new_session(Dialect::MySql, "admin", None);
    ok(&mut m, "SET SESSION max_execution_time = 100");
    let err = exec(&mut m, SLOW).unwrap_err();
    assert_eq!(err.mysql_code(), 3024, "{err}");

    // Lock waits, per dialect, stop at the session's limit and when cancelled.
    let mut holder = e.new_session(Dialect::Postgres, "h", None);
    ok(&mut holder, "BEGIN");
    ok(&mut holder, "UPDATE t SET v = v WHERE id = 1");
    let mut w = e.new_session(Dialect::Postgres, "w", None);
    ok(&mut w, "SET lock_timeout = '50ms'");
    ok(&mut w, "BEGIN");
    let start = Instant::now();
    assert_eq!(exec(&mut w, "UPDATE t SET v = 0 WHERE id = 1").unwrap_err().code, "55P03");
    assert!(start.elapsed() < Duration::from_secs(3), "{:?}", start.elapsed());
    ok(&mut w, "ROLLBACK");
    let mut ms = e.new_session(Dialect::MsSql, "w2", None);
    ok(&mut ms, "SET LOCK_TIMEOUT 50");
    ok(&mut ms, "BEGIN TRANSACTION");
    assert_eq!(exec(&mut ms, "UPDATE t SET v = 0 WHERE id = 1").unwrap_err().code, "55P03");
    ok(&mut ms, "ROLLBACK");
    let mut my = e.new_session(Dialect::MySql, "w3", None);
    ok(&mut my, "SET innodb_lock_wait_timeout = 1");
    ok(&mut my, "BEGIN");
    let start = Instant::now();
    assert_eq!(exec(&mut my, "UPDATE t SET v = 0 WHERE id = 1").unwrap_err().mysql_code(), 1205);
    assert!(start.elapsed() >= Duration::from_millis(900));
    ok(&mut my, "ROLLBACK");
    // Waiting forever still answers a cancel.
    let mut f = e.new_session(Dialect::Postgres, "f", None);
    ok(&mut f, "SET lock_timeout = 0");
    let ctl = Arc::clone(&f.control);
    let t = std::thread::spawn(move || {
        ok(&mut f, "BEGIN");
        exec(&mut f, "UPDATE t SET v = 0 WHERE id = 1").map(|_| ()).unwrap_err().code
    });
    std::thread::sleep(Duration::from_millis(300));
    assert!(!t.is_finished());
    ctl.cancel();
    assert_eq!(t.join().unwrap(), "57014");
    ok(&mut holder, "ROLLBACK");
}

#[test]
fn memory_is_limited_per_statement() {
    let (e, _keep) = engine("memory", EngineConfig::default());
    let mut a = e.new_session(Dialect::Postgres, "admin", None);
    ok(&mut a, "CREATE TABLE big (id INT PRIMARY KEY, body TEXT)");
    let blob = "x".repeat(4096);
    for chunk in 0..10 {
        let values: Vec<String> = (0..100).map(|i| format!("({}, '{blob}')", chunk * 100 + i)).collect();
        ok(&mut a, &format!("INSERT INTO big VALUES {}", values.join(",")));
    }
    ok(&mut a, "SET max_query_memory = '1MB'");
    let e1 = exec(&mut a, "SELECT * FROM big").unwrap_err();
    assert_eq!(e1.code, "53200", "{e1}");
    let e2 = exec(&mut a, "SELECT a.body FROM big a JOIN big b ON a.id = b.id").unwrap_err();
    assert_eq!(e2.code, "53200", "{e2}");
    // A join that explodes stops while it explodes, not after.
    ok(&mut a, "SET max_query_memory = '16MB'");
    let e3 = exec(&mut a, "SELECT count(*) FROM big a, big b, big c").unwrap_err();
    assert_eq!(e3.code, "53200", "{e3}");
    ok(&mut a, "SET max_query_memory = '1MB'");
    // Small queries, one after another, are fine: each statement has its own budget, and a
    // subquery's rows stop counting when it returns.
    for _ in 0..3 {
        ok(&mut a, "SELECT id FROM big WHERE id = 1");
    }
    ok(&mut a, "SET max_query_memory = '64MB'");
    assert!(matches!(exec(&mut a, "SELECT count(*) FROM big WHERE id IN (SELECT id FROM big WHERE id < 5)"), Ok(Outcome::Rows(_))));
    ok(&mut a, "SELECT * FROM big");
}

// ── over a real PostgreSQL connection ──

struct Pg {
    s: TcpStream,
    pid: i32,
    secret: i32,
}

fn msg(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut o = vec![tag];
    o.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
    o.extend_from_slice(body);
    o
}

impl Pg {
    fn connect(addr: std::net::SocketAddr) -> Pg {
        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
        let mut body = 196_608i32.to_be_bytes().to_vec();
        body.extend_from_slice(b"user\0admin\0database\0storine\0\0");
        let mut start = (body.len() as i32 + 4).to_be_bytes().to_vec();
        start.extend_from_slice(&body);
        s.write_all(&start).unwrap();
        let mut c = Pg { s, pid: 0, secret: 0 };
        loop {
            let (t, b) = c.read();
            match t {
                b'K' => {
                    c.pid = i32::from_be_bytes([b[0], b[1], b[2], b[3]]);
                    c.secret = i32::from_be_bytes([b[4], b[5], b[6], b[7]]);
                }
                b'Z' => return c,
                _ => {}
            }
        }
    }

    fn read(&mut self) -> (u8, Vec<u8>) {
        let mut h = [0u8; 5];
        self.s.read_exact(&mut h).expect("a message");
        let n = i32::from_be_bytes([h[1], h[2], h[3], h[4]]) as usize - 4;
        let mut b = vec![0u8; n];
        self.s.read_exact(&mut b).expect("a message body");
        (h[0], b)
    }

    fn send(&mut self, sql: &str) {
        let mut q = sql.as_bytes().to_vec();
        q.push(0);
        self.s.write_all(&msg(b'Q', &q)).unwrap();
    }

    /// Runs `sql`; the SQLSTATE of its error, if any.
    fn run(&mut self, sql: &str) -> Option<String> {
        self.send(sql);
        self.finish()
    }

    fn finish(&mut self) -> Option<String> {
        let mut err = None;
        loop {
            let (t, b) = self.read();
            match t {
                b'E' => err = Some(sqlstate(&b)),
                b'Z' => return err,
                _ => {}
            }
        }
    }
}

fn sqlstate(b: &[u8]) -> String {
    let mut i = 0;
    while i < b.len() && b[i] != 0 {
        let end = i + 1 + b[i + 1..].iter().position(|x| *x == 0).unwrap_or(0);
        if b[i] == b'C' {
            return String::from_utf8_lossy(&b[i + 1..end]).into_owned();
        }
        i = end + 1;
    }
    String::new()
}

fn serve(e: &Arc<Engine>) -> std::net::SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap();
    let (e, auth) = (Arc::clone(e), Arc::new(storage_sql::pg::Auth::default()));
    std::thread::spawn(move || storage_sql::pg::serve(e, l, auth));
    addr
}

fn cancel_request(addr: std::net::SocketAddr, pid: i32, secret: i32) {
    let mut s = TcpStream::connect(addr).unwrap();
    let mut b = 16i32.to_be_bytes().to_vec();
    b.extend_from_slice(&80_877_102i32.to_be_bytes());
    b.extend_from_slice(&pid.to_be_bytes());
    b.extend_from_slice(&secret.to_be_bytes());
    s.write_all(&b).unwrap();
    // Answered by closing.
    let mut sink = Vec::new();
    let _ = s.read_to_end(&mut sink);
    assert!(sink.is_empty());
}

#[test]
fn over_the_postgres_wire() {
    let (e, _keep) = engine("wire", EngineConfig::default());
    let addr = serve(&e);
    let mut c = Pg::connect(addr);
    assert_eq!(c.run("CREATE TABLE t (id INT PRIMARY KEY, v INT)"), None);
    let values: Vec<String> = (0..ROWS).map(|i| format!("({i},{i})")).collect();
    assert_eq!(c.run(&format!("INSERT INTO t VALUES {}", values.join(","))), None);

    // The wrong secret does nothing, and the right one stops the query.
    c.send(SLOW);
    while !e.registry.get(c.pid as u64).is_some_and(|s| s.busy()) { std::thread::sleep(Duration::from_millis(10)); }
    std::thread::sleep(Duration::from_millis(100));
    cancel_request(addr, c.pid, c.secret.wrapping_add(1));
    cancel_request(addr, c.pid + 1, c.secret);
    std::thread::sleep(Duration::from_millis(300));
    assert!(e.registry.get(c.pid as u64).is_some_and(|s| s.check().is_ok()), "a wrong key must not cancel");
    let start = Instant::now();
    cancel_request(addr, c.pid, c.secret);
    assert_eq!(c.finish().as_deref(), Some("57014"));
    assert!(start.elapsed() < Duration::from_secs(10), "{:?}", start.elapsed());
    assert_eq!(c.run("SELECT 1"), None);

    // The keys are per connection.
    let d = Pg::connect(addr);
    assert_ne!(d.secret, c.secret);
    assert_ne!(d.pid, c.pid);

    // An idle transaction outliving its limit: the connection says why and closes.
    assert_eq!(c.run("SET idle_in_transaction_session_timeout = '100ms'"), None);
    assert_eq!(c.run("BEGIN"), None);
    let (t, b) = c.read();
    assert_eq!((t, sqlstate(&b).as_str()), (b'E', "25P03"));
    let mut rest = Vec::new();
    assert_eq!(c.s.read_to_end(&mut rest).unwrap_or(0), 0, "closed");
    // Outside a transaction, idle is fine.
    let mut f = Pg::connect(addr);
    assert_eq!(f.run("SET idle_in_transaction_session_timeout = '50ms'"), None);
    std::thread::sleep(Duration::from_millis(400));
    assert_eq!(f.run("SELECT 1"), None);

    // Terminating an idle connection reaches it through its socket.
    let mut g = Pg::connect(addr);
    assert_eq!(f.run(&format!("SELECT pg_terminate_backend({})", g.pid)), None);
    let (t, b) = g.read();
    assert_eq!((t, sqlstate(&b).as_str()), (b'E', "57P01"));
}

/// What drivers read of a SQL cursor over the wire (`TASK-150`): psqlODBC finds its rows by the
/// `FETCH` tag; psycopg describes and closes the cursor as a portal.
#[test]
fn cursors_over_the_postgres_wire() {
    let (e, _g) = engine("wirecursor", EngineConfig::default());
    let addr = serve(&e);
    let mut c = Pg::connect(addr);
    assert_eq!(c.run("CREATE TABLE t (a INT PRIMARY KEY, b TEXT)"), None);
    assert_eq!(c.run("INSERT INTO t VALUES (1, 'x'), (2, 'y')"), None);
    let messages = |c: &mut Pg| {
        let mut out = Vec::new();
        loop {
            let (tag, body) = c.read();
            if tag == b'Z' {
                return out;
            }
            out.push((tag, body));
        }
    };
    let text = |b: &[u8]| String::from_utf8_lossy(b).trim_end_matches('\0').to_owned();

    // As psqlODBC sends it: three statements, the rows tagged by the last.
    c.send("BEGIN;declare \"SQL_CUR\" cursor with hold for SELECT a FROM t ORDER BY a;fetch 100 in \"SQL_CUR\"");
    let tags: Vec<String> = messages(&mut c).iter().filter(|m| m.0 == b'C').map(|m| text(&m.1)).collect();
    assert_eq!(tags, ["BEGIN", "DECLARE CURSOR", "FETCH 2"]);

    // Describe ('P') of the portal a DECLARE made: its columns, without moving it.
    c.s.write_all(&msg(b'D', b"PSQL_CUR\0")).unwrap();
    c.s.write_all(&msg(b'S', b"")).unwrap();
    let got = messages(&mut c);
    assert_eq!(got[0].0, b'T', "{got:?}");
    assert!(String::from_utf8_lossy(&got[0].1).contains('a'));

    // Close ('P') ends the cursor; closing what does not exist is no error.
    for _ in 0..2 {
        c.s.write_all(&msg(b'C', b"PSQL_CUR\0")).unwrap();
        c.s.write_all(&msg(b'S', b"")).unwrap();
        assert_eq!(messages(&mut c)[0].0, b'3');
    }
    assert_eq!(c.run("FETCH 1 IN \"SQL_CUR\"").as_deref(), Some("34000"));
    assert_eq!(c.run("COMMIT"), None);

    // A portal that is nothing is still an error.
    c.s.write_all(&msg(b'D', b"Pnothing\0")).unwrap();
    c.s.write_all(&msg(b'S', b"")).unwrap();
    assert_eq!(messages(&mut c)[0].0, b'E');

    // Plain queries keep their tag after a fetch.
    c.send("SELECT 1");
    let tags: Vec<String> = messages(&mut c).iter().filter(|m| m.0 == b'C').map(|m| text(&m.1)).collect();
    assert_eq!(tags, ["SELECT 1"]);
}

/// Audit findings over the wire (`TASK-152`): what the parser accepts, the connection's stack holds.
#[test]
fn deep_expressions_run_over_the_wire() {
    let (e, _g) = engine("wiredeep", EngineConfig::default());
    let addr = serve(&e);
    let mut c = Pg::connect(addr);
    let sql = format!("SELECT 1{}", " + 1".repeat(990));
    assert_eq!(c.run(&sql), None);
    assert_eq!(c.run("SELECT 1"), None);
}
