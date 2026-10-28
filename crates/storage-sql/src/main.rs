//! `storine-sql`: serves PostgreSQL and MySQL clients over a Storine database.
//!
//! ```text
//! storine-sql --data ./db                         # embedded database, PostgreSQL on :5432, MySQL on :3306
//! storine-sql --storine storine://h1,h2,h3        # a running server or cluster
//!             [--pg 0.0.0.0:5432] [--mysql 0.0.0.0:3306] [--no-pg] [--no-mysql]
//!             [--user name:password]... [--database name]
//!             [--tls-cert chain.pem --tls-key key.pem | --no-tls] [--require-tls]
//! ```
//!
//! TLS is on by default: with `--data`, a self-signed Ed25519 certificate is created once in
//! `DIR/tls/` (clients using `sslmode=require` / `ssl-mode=REQUIRED` accept it; pin it with
//! `sslrootcert=DIR/tls/server.crt` for `verify-full`).

use std::collections::HashMap;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use storage_sql::exec::{Engine, EngineConfig};
use storage_sql::store::Store;

const USAGE: &str = "usage: storine-sql (--data DIR | --storine URL) [--pg ADDR] [--mysql ADDR] [--no-pg] [--no-mysql] [--user NAME:PASSWORD]... [--database NAME] [--tls-cert FILE --tls-key FILE | --no-tls] [--require-tls]";

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
    let (mut tls_cert, mut tls_key, mut no_tls, mut require_tls) =
        (None::<PathBuf>, None::<PathBuf>, false, false);
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
            "--tls-cert" => tls_cert = Some(PathBuf::from(value("--tls-cert")?)),
            "--tls-key" => tls_key = Some(PathBuf::from(value("--tls-key")?)),
            "--no-tls" => no_tls = true,
            "--require-tls" => require_tls = true,
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
    let tls = match (no_tls, &tls_cert, &tls_key) {
        (true, None, None) if require_tls => {
            return Err("--require-tls needs TLS (drop --no-tls)".into())
        }
        (true, None, None) => None,
        (true, _, _) => {
            return Err("--no-tls cannot be combined with --tls-cert / --tls-key".into())
        }
        (false, Some(c), Some(k)) => {
            let read = |p: &PathBuf| {
                std::fs::read_to_string(p).map_err(|e| format!("cannot read {}: {e}", p.display()))
            };
            Some(
                storage_sql::tls::TlsConfig::from_pem(&read(c)?, &read(k)?)
                    .map_err(|e| e.to_string())?,
            )
        }
        (false, None, None) => Some(match &data {
            Some(dir) => {
                storage_sql::tls::TlsConfig::load_or_create(&dir.join("tls")).map_err(|e| {
                    format!(
                        "cannot set up the TLS certificate in {}: {e}",
                        dir.join("tls").display()
                    )
                })?
            }
            None => storage_sql::tls::TlsConfig::self_signed(&["localhost", "127.0.0.1", "::1"])
                .map_err(|e| e.to_string())?,
        }),
        _ => return Err("--tls-cert and --tls-key go together".into()),
    }
    .map(Arc::new);
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
                tls: tls.clone(),
                require_tls,
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
                tls: tls.clone(),
                require_tls,
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
