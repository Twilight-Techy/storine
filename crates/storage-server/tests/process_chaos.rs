//! Process-level chaos against real `storage-server` binaries (`F-SIM-03`, `TASK-134`).
//!
//! A 3-node cluster runs as 3 OS processes. Every directed Raft link goes through a fault
//! proxy that can pass, drop (refuse and cut connections), black-hole (accept and swallow:
//! a half-open socket) or delay traffic. A nemesis also `SIGKILL`s nodes (restarted from their
//! data directories) and `SIGSTOP`s / `SIGCONT`s them (a paused leader must not serve stale
//! reads when it wakes). Clients write and read a few keys the whole time, and the history is
//! checked for linearizability at the end.
//!
//! `STORINE_CHAOS_SECS` sets the length (default 20 s).
#![cfg(unix)]

use std::collections::BTreeMap;
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use storage_codec::PropertyValue;
use storage_core::linearizability::{check_keyed, CheckError, Event, RegisterOp};
use storage_core::{DocOp, DocWrite, Precondition};
use storage_server::StorageClient;
use storage_types::testing::SweepRng;
use storage_types::StorageError;

const PASS: u8 = 0;
const DROP: u8 = 1;
const BLACKHOLE: u8 = 2;
const DELAY: u8 = 3;

/// A TCP forwarder for one directed link, with switchable faults.
struct Proxy {
    addr: SocketAddr,
    mode: Arc<AtomicU8>,
    /// Bumped to cut every open connection.
    epoch: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
}

impl Proxy {
    fn start(target: SocketAddr) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let (mode, epoch, stop) = (Arc::new(AtomicU8::new(PASS)), Arc::new(AtomicU64::new(0)), Arc::new(AtomicBool::new(false)));
        let (m, e, s) = (Arc::clone(&mode), Arc::clone(&epoch), Arc::clone(&stop));
        thread::spawn(move || {
            while !s.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((inbound, _)) => {
                        if m.load(Ordering::Relaxed) == DROP {
                            drop(inbound);
                            continue;
                        }
                        let Ok(outbound) = TcpStream::connect_timeout(&target, Duration::from_millis(300)) else { continue };
                        let my_epoch = e.load(Ordering::Relaxed);
                        for (from, to) in [(inbound.try_clone().unwrap(), outbound.try_clone().unwrap()), (outbound, inbound)] {
                            let (m, e) = (Arc::clone(&m), Arc::clone(&e));
                            thread::spawn(move || pipe(from, to, &m, &e, my_epoch));
                        }
                    }
                    Err(ref err) if err.kind() == ErrorKind::WouldBlock => thread::sleep(Duration::from_millis(5)),
                    Err(_) => thread::sleep(Duration::from_millis(5)),
                }
            }
        });
        Proxy { addr, mode, epoch, stop }
    }

    fn set(&self, mode: u8) {
        self.mode.store(mode, Ordering::Relaxed);
        if mode == DROP {
            self.epoch.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.epoch.fetch_add(1, Ordering::Relaxed);
    }
}

fn pipe(mut from: TcpStream, mut to: TcpStream, mode: &AtomicU8, epoch: &AtomicU64, my_epoch: u64) {
    let _ = from.set_read_timeout(Some(Duration::from_millis(50)));
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        if epoch.load(Ordering::Relaxed) != my_epoch {
            break;
        }
        match from.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => match mode.load(Ordering::Relaxed) {
                // Half-open: the bytes vanish, the connection stays up.
                BLACKHOLE => {}
                DROP => break,
                m => {
                    if m == DELAY {
                        thread::sleep(Duration::from_millis(40));
                    }
                    if to.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
            },
            Err(ref e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {}
            Err(_) => break,
        }
    }
    let _ = from.shutdown(std::net::Shutdown::Both);
    let _ = to.shutdown(std::net::Shutdown::Both);
}

fn free_addr() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

struct Cluster {
    dir: PathBuf,
    addrs: Vec<SocketAddr>,
    /// `proxies[i][j]`: link from node `i` to node `j`.
    proxies: Vec<Vec<Option<Proxy>>>,
    procs: Vec<Option<Child>>,
}

impl Cluster {
    fn start(dir: &Path) -> Cluster {
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let n = 3;
        let addrs: Vec<SocketAddr> = (0..n).map(|_| free_addr()).collect();
        let proxies = (0..n).map(|i| (0..n).map(|j| (i != j).then(|| Proxy::start(addrs[j]))).collect()).collect();
        let mut c = Cluster { dir: dir.to_path_buf(), addrs, proxies, procs: (0..n).map(|_| None).collect() };
        for i in 0..n {
            c.spawn(i);
        }
        c
    }

    fn spawn(&mut self, i: usize) {
        // Node `i` reaches peer `j` through the proxy for link i -> j.
        let peers: Vec<String> = (0..self.addrs.len())
            .map(|j| {
                let a = if i == j { self.addrs[j] } else { self.proxies[i][j].as_ref().unwrap().addr };
                format!("{}={a}", j + 1)
            })
            .collect();
        let child = Command::new(env!("CARGO_BIN_EXE_storage-server"))
            .arg(self.dir.join(format!("n{}", i + 1)))
            .arg(self.addrs[i].to_string())
            .args(["--node-id", &(i + 1).to_string(), "--peers", &peers.join(",")])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        self.procs[i] = Some(child);
    }

    fn kill(&mut self, i: usize) {
        if let Some(mut c) = self.procs[i].take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    fn signal(&self, i: usize, sig: &str) {
        if let Some(c) = &self.procs[i] {
            let _ = Command::new("kill").args([sig, &c.id().to_string()]).status();
        }
    }

    fn isolate(&self, i: usize, mode: u8) {
        for j in 0..self.addrs.len() {
            if let Some(p) = &self.proxies[i][j] {
                p.set(mode);
            }
            if let Some(p) = &self.proxies[j][i] {
                p.set(mode);
            }
        }
    }

    fn heal(&self) {
        for row in &self.proxies {
            for p in row.iter().flatten() {
                p.set(PASS);
            }
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for i in 0..self.procs.len() {
            self.signal(i, "-CONT");
            self.kill(i);
        }
    }
}

fn put(key: &str, v: i64) -> DocWrite {
    let mut f = BTreeMap::new();
    f.insert("v".to_string(), PropertyValue::Int64(v));
    DocWrite { collection: "kv".into(), key: key.into(), op: DocOp::Put(f), precondition: Precondition::None }
}

/// A definite refusal: the write was never proposed.
fn definitely_not_applied(e: &StorageError) -> bool {
    matches!(e, StorageError::NotLeader { .. } | StorageError::ReadOnlyReplica)
}

#[test]
fn linearizable_under_kills_pauses_and_partitions() {
    let secs: u64 = std::env::var("STORINE_CHAOS_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(20);
    let dir = std::env::temp_dir().join(format!("storine_process_chaos_{}", std::process::id()));
    let cluster = Arc::new(Mutex::new(Cluster::start(&dir)));
    let addrs = cluster.lock().unwrap().addrs.clone();
    let t0 = Instant::now();
    let now = move || t0.elapsed().as_micros() as u64;
    let history: Arc<Mutex<Vec<(usize, Event<RegisterOp>)>>> = Arc::new(Mutex::new(Vec::new()));
    let next_value = Arc::new(AtomicU64::new(1));
    let stop = Arc::new(AtomicBool::new(false));
    let counts = Arc::new(Mutex::new([0usize; 4])); // writes ok, writes unknown, reads ok, refusals
    let keys = 3usize;

    // Wait for a first leader.
    let deadline = Instant::now() + Duration::from_secs(20);
    'ready: loop {
        for a in &addrs {
            if let Ok(mut c) = StorageClient::connect_timeout(*a, Duration::from_millis(200)) {
                if c.doc_write(&[put("warmup", 0)], None).is_ok() {
                    break 'ready;
                }
            }
        }
        assert!(Instant::now() < deadline, "cluster never elected a leader");
        thread::sleep(Duration::from_millis(100));
    }

    let mut workers = Vec::new();
    for w in 0..4u64 {
        let (addrs, history, next_value, stop, counts) = (addrs.clone(), Arc::clone(&history), Arc::clone(&next_value), Arc::clone(&stop), Arc::clone(&counts));
        workers.push(thread::spawn(move || {
            let mut rng = SweepRng::new(w + 11);
            let mut target = w as usize % addrs.len();
            let mut client: Option<StorageClient> = None;
            while !stop.load(Ordering::Relaxed) {
                if client.is_none() {
                    client = StorageClient::connect_timeout(addrs[target], Duration::from_millis(300)).ok();
                    if client.is_none() {
                        target = (target + 1) % addrs.len();
                        thread::sleep(Duration::from_millis(20));
                        continue;
                    }
                }
                let c = client.as_mut().unwrap();
                let key = rng.below(keys);
                let kname = format!("k{key}");
                let call = now();
                if rng.below(2) == 0 {
                    let v = next_value.fetch_add(1, Ordering::Relaxed) as i64;
                    match c.doc_write(&[put(&kname, v)], None) {
                        Ok(_) => {
                            history.lock().unwrap().push((key, Event { call, ret: Some(now()), op: RegisterOp::Write(v) }));
                            counts.lock().unwrap()[0] += 1;
                        }
                        Err(e) if definitely_not_applied(&e) => {
                            counts.lock().unwrap()[3] += 1;
                            target = (target + 1) % addrs.len();
                            client = None;
                        }
                        Err(_) => {
                            // Timeout, CommitUnknown, lost connection: may or may not apply.
                            history.lock().unwrap().push((key, Event { call, ret: None, op: RegisterOp::Write(v) }));
                            counts.lock().unwrap()[1] += 1;
                            target = (target + 1) % addrs.len();
                            client = None;
                        }
                    }
                } else {
                    match c.doc_get("kv", &kname) {
                        Ok(doc) => {
                            let v = doc.and_then(|d| match d.fields.get("v") {
                                Some(PropertyValue::Int64(v)) => Some(*v),
                                _ => None,
                            });
                            history.lock().unwrap().push((key, Event { call, ret: Some(now()), op: RegisterOp::Read(v) }));
                            counts.lock().unwrap()[2] += 1;
                        }
                        Err(_) => {
                            target = (target + 1) % addrs.len();
                            client = None;
                        }
                    }
                }
            }
        }));
    }

    // Nemesis.
    let mut rng = SweepRng::new(0xC4A05);
    let mut faults = BTreeMap::<&str, usize>::new();
    let end = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < end {
        thread::sleep(Duration::from_millis(400 + rng.below(800) as u64));
        let node = rng.below(3);
        let mut c = cluster.lock().unwrap();
        match rng.below(6) {
            0 => {
                *faults.entry("sigkill").or_default() += 1;
                c.kill(node);
                drop(c);
                thread::sleep(Duration::from_millis(300 + rng.below(1200) as u64));
                cluster.lock().unwrap().spawn(node);
            }
            1 => {
                *faults.entry("sigstop").or_default() += 1;
                c.signal(node, "-STOP");
                drop(c);
                thread::sleep(Duration::from_millis(500 + rng.below(2000) as u64));
                cluster.lock().unwrap().signal(node, "-CONT");
            }
            2 => {
                *faults.entry("partition").or_default() += 1;
                c.isolate(node, DROP);
                drop(c);
                thread::sleep(Duration::from_millis(500 + rng.below(1500) as u64));
                cluster.lock().unwrap().heal();
            }
            3 => {
                *faults.entry("half-open").or_default() += 1;
                c.isolate(node, BLACKHOLE);
                drop(c);
                thread::sleep(Duration::from_millis(500 + rng.below(1500) as u64));
                cluster.lock().unwrap().heal();
            }
            4 => {
                *faults.entry("delay").or_default() += 1;
                c.isolate(node, DELAY);
                drop(c);
                thread::sleep(Duration::from_millis(500 + rng.below(1500) as u64));
                cluster.lock().unwrap().heal();
            }
            _ => {}
        }
    }
    cluster.lock().unwrap().heal();
    thread::sleep(Duration::from_secs(2));
    stop.store(true, Ordering::Relaxed);
    for w in workers {
        let _ = w.join();
    }
    let counts = *counts.lock().unwrap();
    let history = history.lock().unwrap().clone();
    eprintln!(
        "process chaos: {secs}s, faults {faults:?}, {} writes ok, {} unknown, {} reads, {} refusals",
        counts[0], counts[1], counts[2], counts[3]
    );
    assert!(counts[0] > 20 && counts[2] > 20, "too little progress under chaos: {counts:?}");
    match check_keyed(&history, 50_000_000) {
        Ok(()) => {}
        Err((k, CheckError::Inconclusive)) => panic!("linearizability check inconclusive for key {k}"),
        Err((k, e)) => {
            let ops: Vec<_> = history.iter().filter(|(kk, _)| *kk == k).map(|(_, e)| e.clone()).collect();
            panic!("key {k} not linearizable: {e:?}\n{ops:#?}");
        }
    }
    drop(cluster);
    let _ = std::fs::remove_dir_all(&dir);
}
