//! `storine-console`: a web console for a Storine server or cluster, in the browser.
//!
//! ```text
//! storine-console [--listen 127.0.0.1:7880] [--allow-host NAME:PORT]... [storine://host:7878]
//! ```
//!
//! It serves a single-page app and a JSON API (`api.rs`) that talks to Storine with the Rust
//! client, so the server itself carries no web code. It listens on loopback by default and
//! prints a link with a key generated for this run: only a browser given that link can use it.
//!
//! Environment (as `storage-cli`): `STORAGE_TOKEN` (the token to authenticate with),
//! `STORAGE_TLS_CA` and `STORAGE_TLS_SERVER_NAME` (TLS), `STORINE_CONSOLE_KEY` (a fixed key
//! instead of a random one).

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

mod api;
mod http;
mod json;
#[cfg(test)]
mod tests;

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::ExitCode;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use http::Response;

const INDEX_HTML: &str = include_str!("../web/index.html");
const APP_JS: &str = include_str!("../web/app.js");
const APP_CSS: &str = include_str!("../web/app.css");
const ICON_SVG: &str = include_str!("../web/icon.svg");

/// Connections served at once; more wait in the listen queue.
const MAX_CONNECTIONS: usize = 64;

const USAGE: &str = "\
Usage: storine-console [options] [storine://[token@]host:port[,host:port...]]

Opens a web console for a Storine server or cluster. It prints a link to open in a browser.

Options:
  --listen ADDR        address to serve on (default 127.0.0.1:7880). Anything but loopback
                       serves plain HTTP on the network: prefer an SSH tunnel.
  --allow-host H:P     also answer to this Host header (when reached through another name)
  -h, --help           show this help

Environment:
  STORAGE_TOKEN             token to authenticate to Storine with
  STORAGE_TLS_CA            PEM file of certificates to trust (turns TLS on)
  STORAGE_TLS_SERVER_NAME   name the server's certificate must carry
  STORINE_CONSOLE_KEY       use this console key instead of a random one
";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("storine-console: {e}");
            ExitCode::FAILURE
        }
    }
}

fn random_key() -> Result<String, String> {
    let bytes: [u8; 24] = storage_codec::rand::os_random_array().map_err(|e| format!("no OS randomness for the console key: {e}"))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

fn run() -> Result<(), String> {
    let mut listen = "127.0.0.1:7880".to_owned();
    let mut extra_hosts = Vec::new();
    let mut url = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "-h" | "--help" => {
                print!("{USAGE}");
                return Ok(());
            }
            "--listen" => listen = args.next().ok_or("--listen needs an address")?,
            "--allow-host" => extra_hosts.push(args.next().ok_or("--allow-host needs a host:port")?),
            s if s.starts_with('-') => return Err(format!("unknown option {s}\n\n{USAGE}")),
            s => url = Some(s.to_owned()),
        }
    }
    let addr: SocketAddr = listen.parse().map_err(|_| format!("--listen: {listen} is not an address like 127.0.0.1:7880"))?;
    let listener = TcpListener::bind(addr).map_err(|e| format!("cannot listen on {addr}: {e}"))?;
    let addr = listener.local_addr().map_err(|e| e.to_string())?;
    let port = addr.port();

    let mut hosts = vec![addr.to_string(), format!("localhost:{port}"), format!("127.0.0.1:{port}"), format!("[::1]:{port}")];
    if port == 80 {
        hosts.extend(["localhost".to_owned(), "127.0.0.1".to_owned(), addr.ip().to_string()]);
    }
    hosts.extend(extra_hosts);
    let key = match std::env::var("STORINE_CONSOLE_KEY") {
        Ok(k) if k.len() >= 16 && k.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') => k,
        Ok(_) => return Err("STORINE_CONSOLE_KEY must be at least 16 letters, digits, '-' or '_'".into()),
        Err(_) => random_key()?,
    };
    let console = Arc::new(api::Console::new(key, hosts));

    if let Some(u) = url {
        let options = storine::Options {
            token: std::env::var("STORAGE_TOKEN").ok().filter(|t| !t.is_empty()),
            tls: std::env::var_os("STORAGE_TLS_CA").is_some(),
            tls_ca: std::env::var("STORAGE_TLS_CA").ok(),
            tls_server_name: std::env::var("STORAGE_TLS_SERVER_NAME").ok(),
            ..storine::Options::default()
        };
        match console.connect(&u, options) {
            Ok(shown) => eprintln!("storine-console: connected to {shown}"),
            Err(e) => eprintln!("storine-console: could not connect to {}: {e:?}; connect from the browser instead", api::redact(&u)),
        }
    }
    let shown_host = if addr.ip().is_unspecified() { format!("localhost:{port}") } else { addr.to_string() };
    if !addr.ip().is_loopback() {
        eprintln!("storine-console: warning: serving plain HTTP on {addr}; anyone who sees the link can use the console");
    }
    println!("storine-console: open http://{shown_host}/#key={}", console.key());

    serve_forever(&listener, &console);
    Ok(())
}

/// Accepts connections until the listener fails, each on its own thread.
fn serve_forever(listener: &TcpListener, console: &Arc<api::Console>) {
    let active = Arc::new(AtomicUsize::new(0));
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        if active.load(Ordering::Acquire) >= MAX_CONNECTIONS {
            let _ = http::write_response(&stream, &Response::new(503, "text/plain; charset=utf-8", "busy"));
            continue;
        }
        active.fetch_add(1, Ordering::AcqRel);
        let (console, in_flight) = (Arc::clone(console), Arc::clone(&active));
        let spawned = std::thread::Builder::new().name("console-http".into()).spawn(move || {
            serve(&stream, &console);
            in_flight.fetch_sub(1, Ordering::AcqRel);
        });
        if spawned.is_err() {
            active.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

fn serve(stream: &TcpStream, console: &api::Console) {
    let resp = match http::read_request(stream) {
        Err(r) => r,
        Ok(req) if !console.host_allowed(&req) => Response::new(421, "text/plain; charset=utf-8", "unknown Host: open the console by the address it printed (or pass --allow-host)"),
        Ok(req) if req.path.starts_with("/api/") => console.handle(&req),
        Ok(req) if req.method != "GET" => Response::new(405, "text/plain; charset=utf-8", "method not allowed"),
        Ok(req) => match req.path.as_str() {
            "/" | "/index.html" => Response::new(200, "text/html; charset=utf-8", INDEX_HTML),
            "/app.js" => Response::new(200, "text/javascript; charset=utf-8", APP_JS),
            "/app.css" => Response::new(200, "text/css; charset=utf-8", APP_CSS),
            "/icon.svg" | "/favicon.ico" => Response::new(200, "image/svg+xml", ICON_SVG),
            _ => Response::new(404, "text/plain; charset=utf-8", "not found"),
        },
    };
    let _ = http::write_response(stream, &resp);
}
