//! End to end: the console's HTTP API against a real `storage-server`.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::api::Console;
use crate::json::{self, Json};

const KEY: &str = "test-key-0123456789";

/// The `storage-server` binary built with this test (`$STORINE_SERVER` overrides).
fn server_binary() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os("STORINE_SERVER") {
        return Some(PathBuf::from(p));
    }
    let deps = std::env::current_exe().ok()?.parent()?.to_path_buf();
    let exe = deps.parent()?.join(format!("storage-server{}", std::env::consts::EXE_SUFFIX));
    exe.exists().then_some(exe)
}

struct Server(Child, PathBuf);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
        let _ = std::fs::remove_dir_all(&self.1);
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn start_server() -> Option<(Server, u16)> {
    let bin = server_binary()?;
    let port = free_port();
    let dir = std::env::temp_dir().join(format!("storine_console_e2e_{}_{port}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let child = Command::new(bin).arg(&dir).arg(format!("127.0.0.1:{port}")).stdout(Stdio::null()).stderr(Stdio::null()).spawn().ok()?;
    let server = Server(child, dir);
    let deadline = Instant::now() + Duration::from_secs(20);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "storage-server did not start");
        std::thread::sleep(Duration::from_millis(50));
    }
    Some((server, port))
}

fn start_console() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let console = Arc::new(Console::new(KEY.into(), vec![format!("127.0.0.1:{port}")]));
    std::thread::spawn(move || crate::serve_forever(&listener, &console));
    port
}

/// Sends one request; returns the status and body.
fn call(port: u16, method: &str, path: &str, key: Option<&str>, body: &str) -> (u16, String) {
    let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
    let key = key.map(|k| format!("X-Console-Key: {k}\r\n")).unwrap_or_default();
    write!(s, "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n{key}Content-Length: {}\r\n\r\n{body}", body.len()).unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    let status = out[9..12].parse().unwrap();
    let body = out.split_once("\r\n\r\n").map(|(_, b)| b.to_owned()).unwrap_or_default();
    (status, body)
}

fn api(port: u16, method: &str, path: &str, body: &str) -> (u16, Json) {
    let (status, text) = call(port, method, &format!("/api/{path}"), Some(KEY), body);
    (status, json::parse(&text).unwrap_or_else(|e| panic!("{method} {path}: {e}: {text}")))
}

#[test]
fn the_console_browses_edits_and_aggregates_a_real_server() {
    let Some((_server, db)) = start_server() else {
        eprintln!("skipped: no storage-server binary (cargo build -p storage-server, or set STORINE_SERVER)");
        return;
    };
    let port = start_console();

    // Access checks come first.
    assert_eq!(call(port, "GET", "/api/session", None, "").0, 401);
    assert_eq!(call(port, "GET", "/api/session", Some("wrong-key-0123456789"), "").0, 401);
    let (status, _) = {
        let mut s = TcpStream::connect(("127.0.0.1", port)).unwrap();
        write!(s, "GET / HTTP/1.1\r\nHost: attacker.example\r\n\r\n").unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).unwrap();
        (out[9..12].parse::<u16>().unwrap(), out)
    };
    assert_eq!(status, 421, "a foreign Host is refused (DNS rebinding)");
    let (status, page) = call(port, "GET", "/", None, "");
    assert_eq!(status, 200);
    assert!(page.contains("Storine Console"));

    // Not connected yet.
    assert_eq!(api(port, "GET", "overview", "").0, 409);
    let (status, s) = api(port, "POST", "connect", &format!(r#"{{"url":"127.0.0.1:{db}"}}"#));
    assert_eq!(status, 200, "{s:?}");
    assert_eq!(s.get("connected"), Some(&Json::Bool(true)));

    // Create keeps every type; a second create conflicts.
    let doc = r#"{"n": 1, "f": 1.0, "d": {"$decimal": "2.50"}, "t": {"$time": "2026-01-02T03:04:05Z"}, "big": {"$int": "18446744073709551615"}, "tags": ["a"]}"#;
    let (status, created) = api(port, "PUT", "collections/c/docs/k%2F1?mode=create", doc);
    assert_eq!(status, 200, "{created:?}");
    let text = created.get("text").and_then(Json::as_str).unwrap();
    assert!(text.contains("\"f\": 1.0") && text.contains("\"n\": 1,"), "{text}");
    assert_eq!(api(port, "PUT", "collections/c/docs/k%2F1?mode=create", "{}").0, 409);
    assert_eq!(api(port, "PUT", "collections/c/docs/x?mode=create", "[1]").0, 422);
    assert_eq!(api(port, "PUT", "collections/c/docs/x?mode=create", "{\"a\":").0, 400);
    for i in 0..5 {
        assert_eq!(api(port, "PUT", &format!("collections/c/docs/k{i}?mode=put"), &format!(r#"{{"n": {i}, "g": "{}"}}"#, i % 2)).0, 200);
    }

    // Query: filter, sort, keyset pages.
    let (status, page) = api(port, "POST", "collections/c/query", r#"{"filter":{"op":"ge","path":"n","value":3},"sort":["-n"]}"#);
    assert_eq!(status, 200, "{page:?}");
    let keys: Vec<&str> = page.get("docs").and_then(Json::as_array).unwrap().iter().map(|d| d.get("key").and_then(Json::as_str).unwrap()).collect();
    assert_eq!(keys, ["k4", "k3"]);
    let (_, first) = api(port, "POST", "collections/c/query", r#"{"limit":2}"#);
    assert_eq!(first.get("total").and_then(Json::as_u64), Some(6));
    let next = first.get("next").and_then(Json::as_str).unwrap().to_owned();
    let (_, second) = api(port, "POST", "collections/c/query", &format!(r#"{{"limit":2,"after":"{next}"}}"#));
    assert_ne!(second.get("docs"), first.get("docs"));
    assert_eq!(api(port, "POST", "collections/c/query", r#"{"filter":{"op":"nope"}}"#).0, 400);

    // Replace with a version check, then a stale version conflicts.
    let (status, v2) = api(port, "PUT", "collections/c/docs/k0?mode=replace&version=1", r#"{"n": 10}"#);
    assert_eq!(status, 200, "{v2:?}");
    assert_eq!(api(port, "PUT", "collections/c/docs/k0?mode=replace&version=1", r#"{"n": 11}"#).0, 409);

    // Aggregate, index, overview.
    let (status, agg) = api(port, "POST", "collections/c/aggregate", r#"{"group_by":["g"],"aggs":[{"fn":"count"},{"fn":"sum","field":"n"}],"order_by":["g"]}"#);
    assert_eq!(status, 200, "{agg:?}");
    assert_eq!(agg.get("columns"), Some(&json::parse(r#"["g","count","sum(n)"]"#).unwrap()));
    let (status, info) = api(port, "POST", "collections/c/indexes", r#"{"field":"n"}"#);
    assert_eq!(status, 200, "{info:?}");
    assert_eq!(info.get("indexes"), Some(&json::parse(r#"["n"]"#).unwrap()));
    let (_, o) = api(port, "GET", "overview", "");
    assert_eq!(o.get("health").and_then(|h| h.get("status")).and_then(Json::as_str), Some("ok"));

    // Delete with a stale version conflicts; without one it deletes.
    assert_eq!(api(port, "DELETE", "collections/c/docs/k1?version=99", "").0, 409);
    assert_eq!(api(port, "DELETE", "collections/c/docs/k1", "").0, 200);
    assert_eq!(api(port, "GET", "collections/c/docs/k1", "").0, 404);
    let (status, dropped) = api(port, "DELETE", "collections/c", "");
    assert_eq!(status, 200, "{dropped:?}");
    assert_eq!(dropped.get("deleted").and_then(Json::as_u64), Some(5));
    assert_eq!(api(port, "GET", "nothing", "").0, 404);
}
