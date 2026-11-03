//! SQL over a range cluster (`TASK-143`): three range nodes in this process, the SQL engine
//! connected to all of them. Transactions span ranges and are serializable; constraints hold.

use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;

use storage_server::ranges::{start_range_server, RangeServerConfig};
use storage_server::SecurityConfig;
use storage_sql::exec::{EngineConfig, Engine, Outcome, Session};
use storage_sql::parser::parse;
use storage_sql::store::Store;
use storage_sql::{Dialect, SqlResult};

fn exec(s: &mut Session, sql: &str) -> SqlResult<Outcome> {
    let (stmts, _) = parse(sql, s.dialect)?;
    let mut last = Outcome::command("");
    for st in &stmts {
        last = s.execute(st, &[])?;
    }
    Ok(last)
}

fn rows(s: &mut Session, sql: &str) -> Vec<Vec<String>> {
    match exec(s, sql).unwrap_or_else(|e| panic!("{sql}: {e}")) {
        Outcome::Rows(rs) => rs.rows.iter().map(|r| r.iter().map(|d| if d.is_null() { "NULL".into() } else { d.as_text() }).collect()).collect(),
        other => panic!("{sql}: expected rows, got {other:?}"),
    }
}

fn ok(s: &mut Session, sql: &str) {
    exec(s, sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
}

fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

#[test]
fn sql_on_a_range_cluster() {
    let root = std::env::temp_dir().join(format!("storine_sql_ranges_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let addrs: Vec<SocketAddr> = (0..3).map(|_| free_addr()).collect();
    let peers: HashMap<u16, SocketAddr> = addrs.iter().enumerate().map(|(i, a)| ((i + 1) as u16, *a)).collect();
    let _servers: Vec<_> = (0..3)
        .map(|i| {
            start_range_server(RangeServerConfig {
                data_dir: root.join(format!("n{}", i + 1)),
                bind_addr: addrs[i].to_string(),
                node_id: (i + 1) as u16,
                peers: peers.clone(),
                token: None,
                security: SecurityConfig::default(),
                max_connections: 1024,
                max_offset: Duration::from_millis(500),
            })
            .unwrap()
        })
        .collect();
    let url = format!("storine://{}", addrs.iter().map(SocketAddr::to_string).collect::<Vec<_>>().join(","));
    let store = Store::connect(&url, Duration::from_secs(30)).unwrap();
    let engine = Engine::new(store, EngineConfig::default());
    let mut s = engine.new_session(Dialect::Postgres, "app", None);

    ok(&mut s, "CREATE TABLE accounts (id INT PRIMARY KEY, owner TEXT UNIQUE, balance NUMERIC(12,2) NOT NULL CHECK (balance >= 0))");
    ok(&mut s, "CREATE TABLE orders (id INT PRIMARY KEY, account_id INT REFERENCES accounts(id), amount NUMERIC(12,2))");
    ok(&mut s, "INSERT INTO accounts VALUES (1,'ada',100.00),(2,'bob',50.00),(3,'cy',0)");
    ok(&mut s, "INSERT INTO orders VALUES (10,1,5.50),(11,1,4.50),(12,2,20)");
    // A transfer across rows (and ranges) in one transaction; a rolled-back one changes nothing.
    ok(&mut s, "BEGIN; UPDATE accounts SET balance = balance - 30 WHERE id = 1; UPDATE accounts SET balance = balance + 30 WHERE id = 2; COMMIT");
    ok(&mut s, "BEGIN; UPDATE accounts SET balance = 0 WHERE id = 2; ROLLBACK");
    assert_eq!(rows(&mut s, "SELECT id, balance FROM accounts ORDER BY id"), vec![vec!["1", "70.00"], vec!["2", "80.00"], vec!["3", "0.00"]]);
    // Aggregates, joins and subqueries.
    assert_eq!(rows(&mut s, "SELECT sum(balance), count(*) FROM accounts WHERE balance > 10"), vec![vec!["150.00", "2"]]);
    assert_eq!(
        rows(&mut s, "SELECT a.owner, count(o.id), sum(o.amount) FROM accounts a JOIN orders o ON o.account_id = a.id GROUP BY a.owner ORDER BY a.owner"),
        vec![vec!["ada", "2", "10.00"], vec!["bob", "1", "20.00"]]
    );
    assert_eq!(rows(&mut s, "SELECT owner FROM accounts WHERE id IN (SELECT account_id FROM orders WHERE amount > 5) ORDER BY owner"), vec![vec!["ada"], vec!["bob"]]);
    // Constraints.
    assert!(exec(&mut s, "INSERT INTO accounts VALUES (4,'ada',1)").is_err(), "unique");
    assert!(exec(&mut s, "UPDATE accounts SET balance = balance - 1000 WHERE id = 3").is_err(), "check");
    assert!(exec(&mut s, "INSERT INTO orders VALUES (13, 99, 1)").is_err(), "foreign key");
    // Serializable: concurrent read-modify-write sessions never lose an update.
    let threads: Vec<_> = (0..3)
        .map(|_| {
            let engine = engine.clone();
            std::thread::spawn(move || {
                let mut s = engine.new_session(Dialect::Postgres, "app", None);
                let mut done = 0;
                while done < 3 {
                    let r = exec(&mut s, "BEGIN ISOLATION LEVEL SERIALIZABLE; UPDATE accounts SET balance = balance + 1 WHERE id = 3; COMMIT");
                    if r.is_ok() {
                        done += 1;
                    } else {
                        let _ = exec(&mut s, "ROLLBACK");
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(rows(&mut s, "SELECT balance FROM accounts WHERE id = 3"), vec![vec!["9.00"]]);
    let _ = std::fs::remove_dir_all(&root);
}
