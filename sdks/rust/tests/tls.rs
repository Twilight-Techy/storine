//! TLS on the native port (`--features tls`), with certificates from `storage-server gen-ca` / `gen-cert`.
#![cfg(feature = "tls")]

use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use storine::{connect, connect_with, Doc, Options, Value};

#[test]
fn tls_round_trip_and_untrusted_server() {
    let bin = std::env::var_os("STORINE_SERVER")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/release/storage-server"));
    if !bin.exists() {
        return;
    }
    let d = std::env::temp_dir().join(format!("storine_rs_tls_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    let p = |s: &str| d.join(s).to_string_lossy().into_owned();
    for args in [
        vec!["gen-ca", &p("ca")],
        vec!["gen-cert", &p("ca"), &p("node"), "127.0.0.1,localhost"],
        vec!["gen-ca", &p("other")],
    ] {
        assert!(Command::new(&bin).args(&args).stdout(Stdio::null()).status().unwrap().success());
    }
    let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let mut server = Command::new(&bin)
        .arg(p("data"))
        .arg(format!("127.0.0.1:{port}"))
        .env("STORAGE_TLS_CERT", p("node/node.crt"))
        .env("STORAGE_TLS_KEY", p("node/node.key"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "server did not start");
        std::thread::sleep(Duration::from_millis(50));
    }

    let big = "x".repeat(100_000);
    let db = connect_with(
        &format!("storines://127.0.0.1:{port}"),
        Options { tls_ca: Some(p("ca/ca.crt")), ..Options::default() },
    )
    .unwrap();
    let mut doc = Doc::new();
    doc.insert("v".into(), Value::Text(big.clone()));
    db.collection("t").put("k", doc).unwrap();
    assert_eq!(db.collection("t").get("k").unwrap().unwrap().fields.get("v"), Some(&Value::Text(big)));

    let named =
        connect(&format!("storines://127.0.0.1:{port}?tls_ca={}&tls_server_name=localhost", p("ca/ca.crt"))).unwrap();
    assert!(named.collection("t").get("k").unwrap().is_some());

    let other = connect_with(
        &format!("storines://127.0.0.1:{port}"),
        Options { tls_ca: Some(p("other/ca.crt")), timeout: Duration::from_secs(1), ..Options::default() },
    )
    .unwrap();
    assert!(other.collection("t").get("k").is_err());

    let _ = server.kill();
    let _ = server.wait();
    let _ = std::fs::remove_dir_all(&d);
}
