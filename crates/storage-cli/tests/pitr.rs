//! Point-in-time recovery of a server's data (`TASK-156`): a server archives its WAL to another
//! directory, an online backup is taken, writes continue, the server crashes, and
//! `storage-cli restore-pitr` rebuilds the database as of a chosen moment (from the backup, the
//! archive and the WAL the crash left), which a server then serves. Encryption at rest is on
//! throughout, so archived WAL is sealed.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use storage_codec::PropertyValue;
use storage_core::{DocOp, DocWrite, Precondition};
use storage_server::StorageClient;

/// The `storage-server` binary built alongside (`$STORINE_SERVER` overrides).
fn server_binary() -> Option<PathBuf> {
    if let Some(p) = std::env::var_os("STORINE_SERVER") {
        return Some(PathBuf::from(p));
    }
    let deps = std::env::current_exe().ok()?.parent()?.to_path_buf();
    let exe = deps.parent()?.join(format!("storage-server{}", std::env::consts::EXE_SUFFIX));
    exe.exists().then_some(exe)
}

struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn(bin: &Path, data: &Path, addr: SocketAddr, env: &[(&str, &Path)]) -> Server {
    let mut cmd = Command::new(bin);
    cmd.arg(data).arg(addr.to_string()).stdout(Stdio::null()).stderr(Stdio::null());
    for (k, v) in env {
        cmd.env(k, v);
    }
    Server(cmd.spawn().unwrap())
}

fn client(addr: SocketAddr) -> StorageClient {
    for _ in 0..150 {
        if let Ok(c) = StorageClient::connect_timeout(addr, Duration::from_secs(1)) {
            return c;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("server never listened");
}

fn write(c: &mut StorageClient, key: &str) {
    let mut f = BTreeMap::new();
    f.insert("v".to_string(), PropertyValue::Text(key.to_string()));
    c.doc_write(&[DocWrite { collection: "t".into(), key: key.into(), op: DocOp::Put(f), precondition: Precondition::None }], None).unwrap();
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

fn cli(env: &[(&str, &Path)], args: &[&str]) -> String {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_storage-cli"));
    cmd.args(args);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.output().unwrap();
    assert!(out.status.success(), "storage-cli {args:?}: {}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Which of `keys` a server on `data` serves.
fn present(bin: &Path, data: &Path, env: &[(&str, &Path)], keys: &[String]) -> Vec<String> {
    let addr = storage_types::testing::free_local_addr();
    let _s = spawn(bin, data, addr, env);
    let mut c = client(addr);
    keys.iter().filter(|k| c.doc_get("t", k).unwrap().is_some()).cloned().collect()
}

#[test]
fn a_server_is_restored_to_a_point_in_time_from_its_archive() {
    let Some(bin) = server_binary() else {
        eprintln!("skipped: no storage-server binary (cargo build -p storage-server, or set STORINE_SERVER)");
        return;
    };
    let root = std::env::temp_dir().join(format!("stge_pitr_cli_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let (data, archive, backups, keys) = (root.join("data"), root.join("archive"), root.join("backups"), root.join("keys"));
    storage_server::at_rest::add_key(&keys).unwrap();
    let env: [(&str, &Path); 3] = [("STORAGE_WAL_ARCHIVE", &archive), ("STORAGE_BACKUP_ROOT", &backups), ("STORAGE_ENCRYPTION_KEYS", &keys)];
    let addr = storage_types::testing::free_local_addr();
    let s = spawn(&bin, &data, addr, &env);
    let mut c = client(addr);
    assert!(c.health().unwrap().contains("\"wal_archive\""));

    let batch = |p: &str| (0..10).map(|i| format!("{p}{i}")).collect::<Vec<_>>();
    for k in batch("a") {
        write(&mut c, &k);
    }
    // Online backup, then more writes on both sides of the moment to restore to. Standalone
    // servers checkpoint (and so archive) every 10 s while written.
    c.admin_backup("base").unwrap();
    for k in batch("b") {
        write(&mut c, &k);
    }
    std::thread::sleep(Duration::from_secs(12));
    std::thread::sleep(Duration::from_millis(50));
    let moment = now_ns();
    std::thread::sleep(Duration::from_millis(50));
    for k in batch("c") {
        write(&mut c, &k);
    }
    std::thread::sleep(Duration::from_secs(12));
    // Written after the last checkpoint: only in the active WAL when the server crashes.
    for k in batch("d") {
        write(&mut c, &k);
    }
    drop(c);
    drop(s); // killed: a crash

    let all: Vec<String> = ["a", "b", "c", "d"].iter().flat_map(|p| batch(p)).collect();
    let backup = backups.join("base");
    let archived = archive.join("documents");
    assert!(std::fs::read_dir(&archived).unwrap().count() > 0, "nothing archived");

    // As of `moment`: a and b, not c or d. The time may be given in RFC 3339 or nanoseconds.
    let r1 = root.join("restore1");
    let out = cli(&[("STORAGE_ENCRYPTION_KEYS", &keys)], &["restore-pitr", backup.to_str().unwrap(), r1.to_str().unwrap(), &moment.to_string(), "--archive", archive.join("documents").to_str().unwrap()]);
    assert!(out.contains("storage-server"), "{out}");
    let got = present(&bin, &r1, &[("STORAGE_ENCRYPTION_KEYS", &keys)], &all);
    let want: Vec<String> = ["a", "b"].iter().flat_map(|p| batch(p)).collect();
    assert_eq!(got, want, "restored as of the moment");

    // Up to the crash: the archive plus the WAL the crash left.
    let r2 = root.join("restore2");
    cli(
        &[("STORAGE_ENCRYPTION_KEYS", &keys)],
        &["restore-pitr", backup.to_str().unwrap(), r2.to_str().unwrap(), "2500-01-01T00:00:00Z", "--archive", archived.to_str().unwrap(), "--wal", data.join("wal").join("documents.wal").to_str().unwrap()],
    );
    assert_eq!(present(&bin, &r2, &[("STORAGE_ENCRYPTION_KEYS", &keys)], &all), all, "restored up to the crash");
    let _ = std::fs::remove_dir_all(&root);
}
