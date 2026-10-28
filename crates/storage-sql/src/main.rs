//! `storine-sql`: serves PostgreSQL and MySQL clients over a Storine database.
//!
//! ```text
//! storine-sql --data ./db                         # embedded database, PostgreSQL on :5432, MySQL on :3306
//! storine-sql --storine storine://h1,h2,h3        # a running server or cluster
//!             [--pg 0.0.0.0:5432] [--mysql 0.0.0.0:3306] [--no-pg] [--no-mysql]
//!             [--user name:password]... [--database name]
//! ```

use std::collections::HashMap;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use storage_sql::exec::{Engine, EngineConfig};
use storage_sql::store::Store;

const USAGE: &str = "usage: storine-sql (--data DIR | --storine URL) [--pg ADDR] [--mysql ADDR] [--no-pg] [--no-mysql] [--user NAME:PASSWORD]... [--database NAME]";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("storine-sql: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let mut data: Option<PathBuf> = None;
    let mut url: Option<String> = None;
    let mut pg = Some("127.0.0.1:5432".to_owned());
    let mut mysql = Some("127.0.0.1:3306".to_owned());
    let mut users = HashMap::new();
    let mut config = EngineConfig::default();
    while let Some(a) = args.next() {
        let mut value = |name: &str| {
            args.next()
                .ok_or_else(|| format!("{name} needs a value\n{USAGE}"))
        };
        match a.as_str() {
            "--data" => data = Some(PathBuf::from(value("--data")?)),
            "--storine" => url = Some(value("--storine")?),
            "--pg" => pg = Some(value("--pg")?),
            "--mysql" => mysql = Some(value("--mysql")?),
            "--no-pg" => pg = None,
            "--no-mysql" => mysql = None,
            "--database" => config.database = value("--database")?,
            "--user" => {
                let v = value("--user")?;
                let (u, p) = v.split_once(':').ok_or("--user takes NAME:PASSWORD")?;
                users.insert(u.to_owned(), p.to_owned());
            }
            "-h" | "--help" => {
                println!("{USAGE}");
                return Ok(());
            }
            other => return Err(format!("unknown argument {other}\n{USAGE}")),
        }
    }
    if let Ok(v) = std::env::var("STORINE_SQL_USERS") {
        // name:password,name2:password2
        for pair in v.split(',').filter(|p| !p.is_empty()) {
            if let Some((u, p)) = pair.split_once(':') {
                users.insert(u.to_owned(), p.to_owned());
            }
        }
    }
    let (engine, _embedded) = match (data, url) {
        (Some(dir), None) => {
            let e = storage_sql::embedded::start(&dir, config).map_err(|e| e.to_string())?;
            (Arc::clone(&e.engine), Some(e))
        }
        (None, Some(u)) => {
            let store = Store::connect(&u, Duration::from_secs(5)).map_err(|e| e.to_string())?;
            (Engine::new(store, config), None)
        }
        _ => return Err(format!("give exactly one of --data and --storine\n{USAGE}")),
    };
    engine
        .refresh_catalog(true)
        .map_err(|e| format!("cannot reach the database: {e}"))?;
    let mut threads = Vec::new();
    if let Some(addr) = pg {
        let l = TcpListener::bind(&addr).map_err(|e| format!("cannot listen on {addr}: {e}"))?;
        eprintln!("storine-sql: PostgreSQL protocol on {addr}");
        let (e, auth) = (
            Arc::clone(&engine),
            Arc::new(storage_sql::pg::Auth {
                users: users.clone(),
            }),
        );
        threads.push(std::thread::spawn(move || {
            storage_sql::pg::serve(e, l, auth)
        }));
    }
    if let Some(addr) = mysql {
        let l = TcpListener::bind(&addr).map_err(|e| format!("cannot listen on {addr}: {e}"))?;
        eprintln!("storine-sql: MySQL protocol on {addr}");
        let (e, auth) = (
            Arc::clone(&engine),
            Arc::new(storage_sql::mysql::Auth {
                users: users.clone(),
            }),
        );
        threads.push(std::thread::spawn(move || {
            storage_sql::mysql::serve(e, l, auth)
        }));
    }
    if threads.is_empty() {
        return Err("nothing to serve (--no-pg and --no-mysql)".into());
    }
    if users.is_empty() {
        eprintln!("storine-sql: no --user given: every client is trusted (bind to localhost or add users)");
    }
    for t in threads {
        let _ = t.join();
    }
    Ok(())
}
