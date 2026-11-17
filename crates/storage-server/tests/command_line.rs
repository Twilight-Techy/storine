//! The command line: `--help` and `--version` print and exit, and an unknown option is an error.
//! None of them may start a server or create a data directory.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU32, Ordering};

/// An empty directory of its own, removed when dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn tempdir() -> TempDir {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("stge_command_line_{}_{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    TempDir(dir)
}

fn run_in(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_storage-server"))
        .args(args)
        .current_dir(dir)
        .output()
        .expect("run storage-server")
}

fn assert_nothing_created(dir: &Path) {
    let left: Vec<_> = std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().file_name()).collect();
    assert!(left.is_empty(), "the command created {left:?}");
}

#[test]
fn help_prints_usage_and_starts_nothing() {
    for flag in ["--help", "-h", "help"] {
        let dir = tempdir();
        let out = run_in(dir.path(), &[flag]);
        assert!(out.status.success(), "{flag}: {out:?}");
        let text = String::from_utf8(out.stdout).unwrap();
        assert!(text.starts_with("usage: storage-server"), "{flag}: {text}");
        assert!(text.contains("--max-connections"), "{text}");
        assert!(!text.contains("listening"), "{text}");
        assert_nothing_created(dir.path());
    }
}

#[test]
fn help_wins_wherever_it_is() {
    let dir = tempdir();
    let out = run_in(dir.path(), &["data", "127.0.0.1:0", "--max-connections", "4", "--help"]);
    assert!(out.status.success(), "{out:?}");
    assert!(String::from_utf8(out.stdout).unwrap().starts_with("usage:"));
    assert_nothing_created(dir.path());
}

#[test]
fn version_prints_the_crate_version() {
    for flag in ["--version", "-V"] {
        let dir = tempdir();
        let out = run_in(dir.path(), &[flag]);
        assert!(out.status.success(), "{flag}: {out:?}");
        assert_eq!(String::from_utf8(out.stdout).unwrap(), format!("storage-server {}\n", env!("CARGO_PKG_VERSION")));
        assert_nothing_created(dir.path());
    }
}

#[test]
fn unknown_option_is_an_error() {
    for args in [&["--hlep"][..], &["-x"], &["data", "--max-conections", "4"]] {
        let dir = tempdir();
        let out = run_in(dir.path(), args);
        assert_eq!(out.status.code(), Some(1), "{args:?}: {out:?}");
        let err = String::from_utf8(out.stderr).unwrap();
        assert!(err.contains("unknown option"), "{args:?}: {err}");
        assert_nothing_created(dir.path());
    }
}

#[test]
fn a_value_flag_without_its_value_is_an_error() {
    let dir = tempdir();
    let out = run_in(dir.path(), &["--max-connections"]);
    assert_eq!(out.status.code(), Some(1), "{out:?}");
    assert!(String::from_utf8(out.stderr).unwrap().contains("--max-connections needs a value"));
    assert_nothing_created(dir.path());
}

#[test]
fn known_flags_still_start_the_server() {
    let dir = tempdir();
    let out = run_in(dir.path(), &["data", "127.0.0.1:0", "--max-connections", "4", "--once"]);
    assert!(out.status.success(), "{out:?}");
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(text.contains("listening on 127.0.0.1:"), "{text}");
    assert!(text.contains("max_connections: 4"), "{text}");
    assert!(dir.path().join("data").is_dir());
}

/// `STORAGE_EXPECTED_DOCUMENTS` (`TASK-163`): the server starts with its index sized for that
/// many documents (seen in the health report), and refuses a value that is not a count.
#[test]
fn expected_documents_sizes_the_index_at_start() {
    let dir = tempdir();
    let bad = Command::new(env!("CARGO_BIN_EXE_storage-server"))
        .args(["data", "127.0.0.1:0", "--once"])
        .env("STORAGE_EXPECTED_DOCUMENTS", "lots")
        .current_dir(dir.path())
        .output()
        .unwrap();
    assert!(!bad.status.success());
    assert!(String::from_utf8_lossy(&bad.stderr).contains("STORAGE_EXPECTED_DOCUMENTS"), "{bad:?}");

    let addr = storage_types::testing::free_local_addr();
    let mut child = Command::new(env!("CARGO_BIN_EXE_storage-server"))
        .args(["data", &addr.to_string()])
        .env("STORAGE_EXPECTED_DOCUMENTS", "300_000")
        .current_dir(dir.path())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut health = None;
    for _ in 0..100 {
        if let Ok(mut c) = storage_server::StorageClient::connect(addr) {
            health = c.health().ok();
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    let _ = child.kill();
    let _ = child.wait();
    let health = health.expect("the server answered");
    let at = health.find("\"index_capacity\":").expect("index_capacity") + 17;
    let capacity: u64 = health[at..].chars().take_while(char::is_ascii_digit).collect::<String>().parse().unwrap();
    assert!(capacity >= 300_000, "index sized for {capacity}");
}
