//! The accept loop (`TASK-152`): what a client that never logs in, or too many clients, can hold.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

use storage_sql::exec::EngineConfig;

/// Reads until the server closes; `false` if it has not within `limit`.
fn closes_within(s: &mut TcpStream, limit: Duration) -> bool {
    // macOS refuses socket options on a connection the peer has already reset (EINVAL).
    if s.set_read_timeout(Some(limit)).is_err() {
        return true;
    }
    let mut b = [0u8; 4096];
    let start = Instant::now();
    loop {
        match s.read(&mut b) {
            Ok(0) => return true,
            Ok(_) => {}
            Err(e) if matches!(e.kind(), std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe) => return true,
            Err(_) => return false,
        }
        if start.elapsed() > limit {
            return false;
        }
    }
}

// One test: the limits are per process.
#[test]
fn connections_are_limited_and_logins_have_a_deadline() {
    let dir = std::env::temp_dir().join(format!("storine_listener_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let e = storage_sql::embedded::start(&dir, EngineConfig::default()).unwrap();
    let (pg, mysql, tds) = (TcpListener::bind("127.0.0.1:0").unwrap(), TcpListener::bind("127.0.0.1:0").unwrap(), TcpListener::bind("127.0.0.1:0").unwrap());
    let (pg_addr, mysql_addr, tds_addr) = (pg.local_addr().unwrap(), mysql.local_addr().unwrap(), tds.local_addr().unwrap());
    let engine = Arc::clone(&e.engine);
    std::thread::spawn(move || storage_sql::pg::serve(engine, pg, Arc::default()));
    let engine = Arc::clone(&e.engine);
    std::thread::spawn(move || storage_sql::mysql::serve(engine, mysql, Arc::default()));
    let engine = Arc::clone(&e.engine);
    let tds_auth = storage_sql::tds::Auth { users: Default::default(), tls: None, require_tls: false, oidc: None, kerberos: None };
    std::thread::spawn(move || storage_sql::tds::serve(engine, tds, Arc::new(tds_auth)));

    // Saying nothing, on any protocol, ends at the deadline.
    storage_sql::listener::set_login_deadline(Duration::from_millis(300));
    for addr in [pg_addr, mysql_addr, tds_addr] {
        let mut idle = TcpStream::connect(addr).unwrap();
        assert!(closes_within(&mut idle, Duration::from_secs(5)), "{addr} kept an idle client");
    }
    // A 16 MiB header with no bytes behind it: the same, at no cost.
    let mut c = TcpStream::connect(mysql_addr).unwrap();
    c.write_all(&[0xff, 0xff, 0xff, 0]).unwrap();
    assert!(closes_within(&mut c, Duration::from_secs(5)));
    // A login packet beyond the limit is refused without waiting for the deadline.
    storage_sql::listener::set_login_deadline(Duration::from_secs(60));
    let mut c = TcpStream::connect(mysql_addr).unwrap();
    c.write_all(&[0xff, 0xff, 0x1f, 0]).unwrap(); // 2 MiB
    let filler = vec![0u8; 2 << 20];
    let _ = c.write_all(&filler);
    assert!(closes_within(&mut c, Duration::from_secs(10)));

    // Beyond the limit a connection is closed at once; a place freed is used again.
    storage_sql::listener::set_max_connections(2);
    std::thread::sleep(Duration::from_millis(100)); // the closed ones above give back their places
    let (a, _b) = (TcpStream::connect(pg_addr).unwrap(), TcpStream::connect(pg_addr).unwrap());
    std::thread::sleep(Duration::from_millis(100));
    let mut over = TcpStream::connect(pg_addr).unwrap();
    assert!(closes_within(&mut over, Duration::from_secs(5)), "a third connection stayed open");
    a.shutdown(std::net::Shutdown::Both).unwrap();
    drop(a);
    std::thread::sleep(Duration::from_millis(200));
    let mut again = TcpStream::connect(pg_addr).unwrap();
    assert!(!closes_within(&mut again, Duration::from_millis(300)), "a freed place was not used again");
    drop(e);
    let _ = std::fs::remove_dir_all(&dir);
}
