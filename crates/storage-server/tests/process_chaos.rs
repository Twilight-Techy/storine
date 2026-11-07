//! Process-level chaos against real `storage-server` binaries (`F-SIM-03`, `TASK-134`).
//!
//! A 3-node cluster runs as 3 OS processes. Every directed Raft link goes through a fault
//! proxy that can pass, drop (refuse and cut connections), black-hole (accept and swallow:
//! a half-open socket) or delay traffic. A nemesis also `SIGKILL`s nodes (restarted from their
//! data directories) and `SIGSTOP`s / `SIGCONT`s them (a paused leader must not serve stale
//! reads when it wakes). Clients write and read a few keys the whole time, and the history is
//! checked for linearizability at the end.
//!
//! The same nemesis runs against a range cluster (`--ranges`, `TASK-142` / `TASK-143`), where
//! every node coordinates distributed transactions: there, concurrent transfers between
//! accounts must also conserve the total.
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

/// Client operations per key index.
type History = Vec<(usize, Event<RegisterOp>)>;

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
    ranges: bool,
}

impl Cluster {
    fn start(dir: &Path, ranges: bool) -> Cluster {
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let n = 3;
        let addrs: Vec<SocketAddr> = (0..n).map(|_| free_addr()).collect();
        let proxies = (0..n).map(|i| (0..n).map(|j| (i != j).then(|| Proxy::start(addrs[j]))).collect()).collect();
        let mut c = Cluster { dir: dir.to_path_buf(), addrs, proxies, procs: (0..n).map(|_| None).collect(), ranges };
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
            .args(if self.ranges { &["--ranges"][..] } else { &[][..] })
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
    chaos(false);
}

/// A range cluster of 3 processes under the same nemesis (`TASK-142`).
#[test]
fn range_cluster_under_kills_pauses_and_partitions() {
    chaos(true);
}

/// A client that gives up on a request hangs up; the server must not keep that connection until the
/// request answers. Clients that time out and reconnect while nothing can commit would otherwise
/// pile up descriptors at the rate they retry (found by `TASK-151`).
#[test]
fn abandoned_requests_do_not_hold_descriptors() {
    for (name, mode) in [("delay", Some(DELAY)), ("half-open", Some(BLACKHOLE)), ("paused followers", None)] {
        abandoned_requests(name, mode);
    }
}

fn abandoned_requests(name: &str, mode: Option<u8>) {
    const CLIENTS: u64 = 64;
    let dir = std::env::temp_dir().join(format!("storine_abandoned_{}_{}", std::process::id(), mode.unwrap_or(0)));
    let cluster = Cluster::start(&dir, false);
    let leader = (0..600)
        .find_map(|_| {
            let found = cluster.addrs.iter().position(|a| {
                StorageClient::connect_timeout(*a, Duration::from_secs(2)).and_then(|mut c| c.doc_write(&[put("warmup", 0)], None)).is_ok()
            });
            if found.is_none() {
                thread::sleep(Duration::from_millis(100));
            }
            found
        })
        .expect("no leader");
    let (pid, port, addr) = (cluster.procs[leader].as_ref().unwrap().id(), cluster.addrs[leader].port(), cluster.addrs[leader]);
    let baseline = process_usage(pid).unwrap().1;
    match mode {
        Some(mode) => cluster.isolate(leader, mode),
        None => (0..cluster.addrs.len()).filter(|i| *i != leader).for_each(|i| cluster.signal(i, "-STOP")),
    }
    let stop = Arc::new(AtomicBool::new(false));
    let abandoned = Arc::new(AtomicU64::new(0));
    let clients: Vec<_> = (0..CLIENTS)
        .map(|c| {
            let (stop, abandoned) = (Arc::clone(&stop), Arc::clone(&abandoned));
            thread::spawn(move || {
                let mut n = 0;
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(mut client) = StorageClient::connect_timeout(addr, Duration::from_millis(300)) {
                        n += 1;
                        if client.doc_write(&[put(&format!("c{c}"), n)], None).is_err() {
                            abandoned.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    thread::sleep(Duration::from_millis(10));
                }
            })
        })
        .collect();
    let (mut peak, mut detail) = (0, String::new());
    let end = Instant::now() + Duration::from_secs(20);
    while Instant::now() < end {
        thread::sleep(Duration::from_millis(250));
        let fds = process_usage(pid).unwrap().1;
        if fds > peak {
            peak = fds;
            detail = describe_fds(pid, port);
        }
    }
    stop.store(true, Ordering::Relaxed);
    for c in clients {
        c.join().unwrap();
    }
    eprintln!("soak probe {name}: baseline {baseline}, peak {peak} ({detail}), abandoned {}", abandoned.load(Ordering::Relaxed));
    drop(cluster);
    let _ = std::fs::remove_dir_all(&dir);
    assert!(peak <= baseline + 2 * CLIENTS, "{name}: {peak} descriptors for {CLIENTS} clients, from {baseline}: {detail}");
}

const ACCOUNTS: usize = 6;

/// Connect and request timeout. A range-cluster write is a distributed transaction (several
/// consensus rounds), so it gets longer than a single-group write.
fn client_timeout(ranges: bool) -> Duration {
    Duration::from_millis(if ranges { 5_000 } else { 300 })
}

fn add(key: &str, delta: i64) -> DocWrite {
    DocWrite {
        collection: "bank".into(),
        key: key.into(),
        op: DocOp::Patch(vec![storage_codec::field_ops::FieldOp::Add { path: "bal".into(), delta: PropertyValue::Int64(delta), min: Some(PropertyValue::Int64(0)), max: None }]),
        precondition: Precondition::None,
    }
}

/// One fault on a random node, held for a moment, then undone.
fn one_fault(cluster: &Mutex<Cluster>, rng: &mut SweepRng, faults: &mut BTreeMap<&'static str, usize>) {
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

/// Prints where a key's history stops being linearizable: histories run to tens of thousands of
/// operations.
fn report_unlinearized(history: &History, k: usize, e: &CheckError) {
    // In the checker's own order: `partial` indexes these.
    let ops: Vec<_> = history.iter().filter(|(kk, e)| *kk == k && !(matches!(e.op, RegisterOp::Read(_)) && e.ret.is_none())).map(|(_, e)| e.clone()).collect();
    let linearized: std::collections::BTreeSet<usize> = match e {
        CheckError::NotLinearizable { partial } => partial.iter().copied().collect(),
        CheckError::Inconclusive => Default::default(),
    };
    eprintln!("key {k}: {} operations, {} linearized", ops.len(), linearized.len());
    let stuck = ops.iter().enumerate().filter(|(i, _)| !linearized.contains(i)).map(|(_, o)| o.call).min().unwrap_or(0);
    let mut near: Vec<_> = ops.iter().filter(|o| o.call + 1_000_000 >= stuck && o.call <= stuck + 500_000).collect();
    near.sort_by_key(|o| o.call);
    for o in near {
        eprintln!("  call {:>10} ret {:>10} {:?}", o.call, o.ret.map_or("none".to_owned(), |r| r.to_string()), o.op);
    }
    eprintln!("first operation left out began at {stuck}");
}

fn chaos(ranges: bool) {
    let secs: u64 = std::env::var("STORINE_CHAOS_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(20);
    let dir = std::env::temp_dir().join(format!("storine_process_chaos_{}_{ranges}", std::process::id()));
    let cluster = Arc::new(Mutex::new(Cluster::start(&dir, ranges)));
    let addrs = cluster.lock().unwrap().addrs.clone();
    let t0 = Instant::now();
    let now = move || t0.elapsed().as_micros() as u64;
    let history: Arc<Mutex<History>> = Arc::new(Mutex::new(Vec::new()));
    let next_value = Arc::new(AtomicU64::new(1));
    let stop = Arc::new(AtomicBool::new(false));
    let counts = Arc::new(Mutex::new([0usize; 4])); // writes ok, writes unknown, reads ok, refusals
    let keys = 3usize;

    // Wait for a first leader.
    let deadline = Instant::now() + Duration::from_secs(20);
    'ready: loop {
        for a in &addrs {
            if let Ok(mut c) = StorageClient::connect_timeout(*a, client_timeout(ranges)) {
                if c.doc_write(&[put("warmup", 0)], None).is_ok() {
                    break 'ready;
                }
            }
        }
        assert!(Instant::now() < deadline, "cluster never elected a leader");
        thread::sleep(Duration::from_millis(100));
    }
    // Range clusters: accounts for transfers (each one atomic across ranges).
    let transfers = Arc::new(AtomicU64::new(0));
    if ranges {
        let seed: Vec<DocWrite> = (0..ACCOUNTS)
            .map(|i| {
                let mut f = BTreeMap::new();
                f.insert("bal".to_string(), PropertyValue::Int64(100));
                DocWrite { collection: "bank".into(), key: format!("a{i}"), op: DocOp::Put(f), precondition: Precondition::None }
            })
            .collect();
        let deadline = Instant::now() + Duration::from_secs(20);
        'seeded: loop {
            for a in &addrs {
                if let Ok(mut c) = StorageClient::connect_timeout(*a, client_timeout(ranges)) {
                    if c.doc_write(&seed, Some([0xA5; 16])).is_ok() {
                        break 'seeded;
                    }
                }
            }
            assert!(Instant::now() < deadline, "accounts never written");
            thread::sleep(Duration::from_millis(100));
        }
    }

    let mut workers = Vec::new();
    for w in 0..(if ranges { 2u64 } else { 0 }) {
        let (addrs, stop, transfers) = (addrs.clone(), Arc::clone(&stop), Arc::clone(&transfers));
        workers.push(thread::spawn(move || {
            let mut rng = SweepRng::new(w + 101);
            let mut target = w as usize % addrs.len();
            let mut client: Option<StorageClient> = None;
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if client.is_none() {
                    client = StorageClient::connect_timeout(addrs[target], client_timeout(ranges)).ok();
                    if client.is_none() {
                        target = (target + 1) % addrs.len();
                        thread::sleep(Duration::from_millis(20));
                        continue;
                    }
                }
                let (a, b) = (rng.below(ACCOUNTS), rng.below(ACCOUNTS));
                if a == b {
                    continue;
                }
                n += 1;
                // Retried with the same request id until it settles: applied at most once.
                let mut rid = [0u8; 16];
                rid[..8].copy_from_slice(&(w * 1_000_000 + n).to_le_bytes());
                let batch = [add(&format!("a{a}"), -1), add(&format!("a{b}"), 1)];
                for _ in 0..50 {
                    let Some(c) = client.as_mut() else { break };
                    match c.doc_write(&batch, Some(rid)) {
                        Ok(_) => {
                            transfers.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        // Insufficient funds (a definite refusal).
                        Err(StorageError::Rejected { status, .. }) if status == storage_server::STATUS_CONFLICT || status == storage_server::STATUS_INVALID => break,
                        Err(_) => {
                            target = (target + 1) % addrs.len();
                            client = StorageClient::connect_timeout(addrs[target], client_timeout(ranges)).ok();
                            thread::sleep(Duration::from_millis(50));
                        }
                    }
                }
            }
        }));
    }
    for w in 0..4u64 {
        let (addrs, history, next_value, stop, counts) = (addrs.clone(), Arc::clone(&history), Arc::clone(&next_value), Arc::clone(&stop), Arc::clone(&counts));
        workers.push(thread::spawn(move || {
            let mut rng = SweepRng::new(w + 11);
            let mut target = w as usize % addrs.len();
            let mut client: Option<StorageClient> = None;
            while !stop.load(Ordering::Relaxed) {
                if client.is_none() {
                    client = StorageClient::connect_timeout(addrs[target], client_timeout(ranges)).ok();
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
        one_fault(&cluster, &mut rng, &mut faults);
    }
    cluster.lock().unwrap().heal();
    thread::sleep(Duration::from_secs(2));
    stop.store(true, Ordering::Relaxed);
    for w in workers {
        let _ = w.join();
    }
    let counts = *counts.lock().unwrap();
    let history = history.lock().unwrap().clone();
    if ranges {
        // Every transfer applied atomically (or not at all): the total is conserved.
        let deadline = Instant::now() + Duration::from_secs(30);
        let total = loop {
            let read = (|| -> Option<i64> {
                let mut c = StorageClient::connect_timeout(addrs[0], client_timeout(ranges)).ok()?;
                let mut t = 0;
                for i in 0..ACCOUNTS {
                    let d = c.doc_get("bank", &format!("a{i}")).ok()??;
                    if let Some(PropertyValue::Int64(v)) = d.fields.get("bal") {
                        t += v;
                    }
                }
                Some(t)
            })();
            match read {
                Some(t) => break t,
                None if Instant::now() < deadline => thread::sleep(Duration::from_millis(200)),
                None => panic!("accounts unreadable after the chaos"),
            }
        };
        eprintln!("range cluster: {} transfers committed", transfers.load(Ordering::Relaxed));
        assert_eq!(total, 100 * ACCOUNTS as i64, "money created or destroyed");
        assert!(transfers.load(Ordering::Relaxed) > 0, "no transfer committed");
    }
    eprintln!(
        "process chaos: {secs}s, faults {faults:?}, {} writes ok, {} unknown, {} reads, {} refusals",
        counts[0], counts[1], counts[2], counts[3]
    );
    assert!(counts[0] > 20 && counts[2] > 20, "too little progress under chaos: {counts:?}");
    match check_keyed(&history, 50_000_000) {
        Ok(()) => {}
        Err((k, CheckError::Inconclusive)) => panic!("linearizability check inconclusive for key {k}"),
        Err((k, e)) => {
            report_unlinearized(&history, k, &e);
            panic!("key {k} not linearizable");
        }
    }
    drop(cluster);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Most memory one node may hold, and most descriptors it may have open, through a soak.
const SOAK_RSS_CEILING_KB: u64 = 6 << 20;
const SOAK_FD_CEILING: u64 = 4096;
/// Rows the analytics read, overwritten in a cycle so the data stays one size.
const SOAK_EVENTS: u64 = 20_000;

/// (resident kB, open descriptors) of a process, if it is there.
fn process_usage(pid: u32) -> Option<(u64, u64)> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let rss = status.lines().find(|l| l.starts_with("VmRSS:"))?.split_whitespace().nth(1)?.parse().ok()?;
    let fds = std::fs::read_dir(format!("/proc/{pid}/fd")).ok()?.count() as u64;
    Some((rss, fds))
}

/// What a node's descriptors are: sockets by TCP state on its client port, and files by extension.
/// A number alone cannot tell a leak from a burst, so the soak prints this at each window's peak.
fn describe_fds(pid: u32, port: u16) -> String {
    let (mut sockets, mut files) = (0u64, std::collections::BTreeMap::<String, u64>::new());
    for link in std::fs::read_dir(format!("/proc/{pid}/fd")).into_iter().flatten().flatten().filter_map(|e| std::fs::read_link(e.path()).ok()) {
        if link.to_string_lossy().starts_with("socket:") {
            sockets += 1;
        } else {
            let kind = link.extension().map_or_else(|| "other".to_string(), |e| e.to_string_lossy().into_owned());
            *files.entry(kind).or_default() += 1;
        }
    }
    let mut states = std::collections::BTreeMap::<&str, u64>::new();
    let tcp = std::fs::read_to_string(format!("/proc/{pid}/net/tcp")).unwrap_or_default();
    for fields in tcp.lines().skip(1).map(|l| l.split_whitespace().collect::<Vec<_>>()) {
        let local = fields.get(1).and_then(|a| a.rsplit(':').next()).and_then(|p| u16::from_str_radix(p, 16).ok());
        if local == Some(port) {
            let state = match fields.get(3).copied() {
                Some("01") => "established",
                Some("08") => "close-wait",
                Some("0A") => "listen",
                _ => "other",
            };
            *states.entry(state).or_default() += 1;
        }
    }
    format!("sockets {sockets} {states:?}, files {files:?}")
}

/// Hours of mixed OLTP and analytics under faults (`TASK-151`): linearizable registers (fresh keys
/// each window, checked at its end), atomic transfers (conserved), inserts and overwrites read by
/// aggregates and scans, while memory and descriptors stay under their ceilings and do not climb.
/// `STORINE_SOAK_SECS` (default 30 minutes), `STORINE_SOAK_WINDOW_SECS` (default 5 minutes).
fn soak(ranges: bool) {
    use storage_core::analytics::{AggFunc, Aggregate, AggregateQuery, Expr as AExpr, GroupKey};
    let env_secs = |k: &str, d: u64| std::env::var(k).ok().and_then(|s| s.parse().ok()).unwrap_or(d);
    let (secs, window_secs) = (env_secs("STORINE_SOAK_SECS", 1800), env_secs("STORINE_SOAK_WINDOW_SECS", 300));
    let dir = std::env::temp_dir().join(format!("storine_soak_{}_{ranges}", std::process::id()));
    let cluster = Arc::new(Mutex::new(Cluster::start(&dir, ranges)));
    let addrs = cluster.lock().unwrap().addrs.clone();
    let t0 = Instant::now();
    let now = move || t0.elapsed().as_micros() as u64;
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut seeded = false;
    let seed: Vec<DocWrite> = (0..ACCOUNTS)
        .map(|i| {
            let mut f = BTreeMap::new();
            f.insert("bal".to_string(), PropertyValue::Int64(100));
            DocWrite { collection: "bank".into(), key: format!("a{i}"), op: DocOp::Put(f), precondition: Precondition::None }
        })
        .collect();
    while !seeded {
        assert!(Instant::now() < deadline, "the cluster never took its first write");
        for a in &addrs {
            if let Ok(mut c) = StorageClient::connect_timeout(*a, client_timeout(ranges)) {
                if c.doc_write(&seed, Some([0xA6; 16])).is_ok() {
                    seeded = true;
                    break;
                }
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    let stop = Arc::new(AtomicBool::new(false));
    let window = Arc::new(AtomicU64::new(0));
    let history: Arc<Mutex<History>> = Arc::new(Mutex::new(Vec::new()));
    let next_value = Arc::new(AtomicU64::new(1));
    // writes, reads, transfers, event writes, aggregates, scans, failures of any kind
    let counts = Arc::new(Mutex::new([0u64; 7]));
    // The window each register worker's current operation belongs to.
    let in_window: Arc<[AtomicU64; 3]> = Arc::new(std::array::from_fn(|_| AtomicU64::new(0)));
    let mut workers = Vec::new();
    let client_of = move |addrs: &[SocketAddr], target: &mut usize| loop {
        match StorageClient::connect_timeout(addrs[*target % addrs.len()], client_timeout(ranges)) {
            Ok(c) => return c,
            Err(_) => {
                *target += 1;
                thread::sleep(Duration::from_millis(50));
            }
        }
    };
    // Registers: the same check as the chaos test, on keys of this window.
    for w in 0..3u64 {
        let (addrs, stop, window, history, next_value, counts, in_window) = (addrs.clone(), Arc::clone(&stop), Arc::clone(&window), Arc::clone(&history), Arc::clone(&next_value), Arc::clone(&counts), Arc::clone(&in_window));
        workers.push(thread::spawn(move || {
            let mut rng = SweepRng::new(w + 500);
            let mut target = w as usize;
            let mut c = None;
            while !stop.load(Ordering::Relaxed) {
                let client = c.get_or_insert_with(|| client_of(&addrs, &mut target));
                let current = window.load(Ordering::Relaxed);
                in_window[w as usize].store(current, Ordering::Relaxed);
                let key = current as usize * 3 + rng.below(3);
                let name = format!("k{key}");
                let call = now();
                if rng.below(2) == 0 {
                    let v = next_value.fetch_add(1, Ordering::Relaxed) as i64;
                    match client.doc_write(&[put(&name, v)], None) {
                        Ok(_) => {
                            history.lock().unwrap().push((key, Event { call, ret: Some(now()), op: RegisterOp::Write(v) }));
                            counts.lock().unwrap()[0] += 1;
                        }
                        Err(e) => {
                            if !definitely_not_applied(&e) {
                                history.lock().unwrap().push((key, Event { call, ret: None, op: RegisterOp::Write(v) }));
                            }
                            counts.lock().unwrap()[6] += 1;
                            target += 1;
                            c = None;
                        }
                    }
                } else {
                    match client.doc_get("kv", &name) {
                        Ok(doc) => {
                            let v = doc.and_then(|d| match d.fields.get("v") {
                                Some(PropertyValue::Int64(v)) => Some(*v),
                                _ => None,
                            });
                            history.lock().unwrap().push((key, Event { call, ret: Some(now()), op: RegisterOp::Read(v) }));
                            counts.lock().unwrap()[1] += 1;
                        }
                        Err(_) => {
                            counts.lock().unwrap()[6] += 1;
                            target += 1;
                            c = None;
                        }
                    }
                }
            }
        }));
    }
    // Transfers, each retried with one request id until it settles.
    for w in 0..2u64 {
        let (addrs, stop, counts) = (addrs.clone(), Arc::clone(&stop), Arc::clone(&counts));
        workers.push(thread::spawn(move || {
            let mut rng = SweepRng::new(w + 600);
            let mut target = w as usize;
            let mut c = None;
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let (a, b) = (rng.below(ACCOUNTS), rng.below(ACCOUNTS));
                if a == b {
                    continue;
                }
                n += 1;
                let mut rid = [0u8; 16];
                rid[..8].copy_from_slice(&(w * 1_000_000_000 + n).to_le_bytes());
                let batch = [add(&format!("a{a}"), -1), add(&format!("a{b}"), 1)];
                for _ in 0..100 {
                    let client = c.get_or_insert_with(|| client_of(&addrs, &mut target));
                    match client.doc_write(&batch, Some(rid)) {
                        Ok(_) => {
                            counts.lock().unwrap()[2] += 1;
                            break;
                        }
                        Err(StorageError::Rejected { status, .. }) if status == storage_server::STATUS_CONFLICT => break,
                        Err(_) => {
                            counts.lock().unwrap()[6] += 1;
                            target += 1;
                            c = None;
                            thread::sleep(Duration::from_millis(50));
                        }
                    }
                    if stop.load(Ordering::Relaxed) {
                        // Stopped mid-retry: the transfer is atomic whichever way it went.
                        break;
                    }
                }
            }
        }));
    }
    // Rows for the analytics: overwritten in a cycle, in batches.
    {
        let (addrs, stop, counts) = (addrs.clone(), Arc::clone(&stop), Arc::clone(&counts));
        workers.push(thread::spawn(move || {
            let mut target = 0usize;
            let mut c = None;
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let batch: Vec<DocWrite> = (0..50)
                    .map(|k| {
                        let id = (i + k) % SOAK_EVENTS;
                        let mut f = BTreeMap::new();
                        f.insert("region".to_string(), PropertyValue::Int64((id % 7) as i64));
                        f.insert("amount".to_string(), PropertyValue::Int64((id * 3 % 1000) as i64));
                        f.insert("note".to_string(), PropertyValue::Text("x".repeat(100)));
                        DocWrite { collection: "events".into(), key: format!("e{id}"), op: DocOp::Put(f), precondition: Precondition::None }
                    })
                    .collect();
                let client = c.get_or_insert_with(|| client_of(&addrs, &mut target));
                match client.doc_write(&batch, None) {
                    Ok(_) => {
                        i += 50;
                        counts.lock().unwrap()[3] += 1;
                    }
                    Err(_) => {
                        counts.lock().unwrap()[6] += 1;
                        target += 1;
                        c = None;
                        thread::sleep(Duration::from_millis(50));
                    }
                }
            }
        }));
    }
    // Analytics: aggregates and filtered scans over those rows.
    {
        let (addrs, stop, counts) = (addrs.clone(), Arc::clone(&stop), Arc::clone(&counts));
        workers.push(thread::spawn(move || {
            let mut target = 2usize;
            let mut c = None;
            let mut rng = SweepRng::new(700);
            while !stop.load(Ordering::Relaxed) {
                let client = c.get_or_insert_with(|| client_of(&addrs, &mut target));
                let q = AggregateQuery {
                    collection: "events".into(),
                    filter: None,
                    group_by: vec![GroupKey::Field("region".into())],
                    aggregates: vec![Aggregate { func: AggFunc::Count, arg: None }, Aggregate { func: AggFunc::Sum, arg: Some(AExpr::Field("amount".into())) }],
                    order_by: Vec::new(),
                    limit: None,
                    memory_budget: 64 << 20,
                    ..AggregateQuery::default()
                };
                let ok = if rng.below(2) == 0 {
                    let r = client.doc_aggregate(&q).is_ok();
                    counts.lock().unwrap()[4] += u64::from(r);
                    r
                } else {
                    let q = storage_core::DocQuery { collection: "events".into(), limit: Some(100), ..storage_core::DocQuery::default() };
                    let r = client.doc_query(&q).is_ok();
                    counts.lock().unwrap()[5] += u64::from(r);
                    r
                };
                if !ok {
                    counts.lock().unwrap()[6] += 1;
                    target += 1;
                    c = None;
                    thread::sleep(Duration::from_millis(100));
                }
                thread::sleep(Duration::from_millis(20));
            }
        }));
    }
    // Faults, a fifth of the time: hours of constant failure would measure only recovery.
    {
        let (cluster, stop) = (Arc::clone(&cluster), Arc::clone(&stop));
        workers.push(thread::spawn(move || {
            let mut rng = SweepRng::new(0x50AC);
            let mut faults = BTreeMap::new();
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(4000 + rng.below(8000) as u64));
                one_fault(&cluster, &mut rng, &mut faults);
            }
            cluster.lock().unwrap().heal();
            eprintln!("soak faults: {faults:?}");
        }));
    }
    let end = Instant::now() + Duration::from_secs(secs);
    let mut windows: Vec<(u64, u64)> = Vec::new();
    let mut n = 0u64;
    while Instant::now() < end {
        let window_end = Instant::now() + Duration::from_secs(window_secs);
        let (mut rss_max, mut fds_max) = (0u64, 0u64);
        // The lowest the busiest node fell to: a burst drains, a leak does not.
        let (mut fds_floor, mut peak) = (u64::MAX, String::new());
        while Instant::now() < window_end && Instant::now() < end {
            thread::sleep(Duration::from_secs(5));
            let pids: Vec<(u32, u16)> = {
                let c = cluster.lock().unwrap();
                c.procs.iter().zip(&c.addrs).filter_map(|(p, a)| Some((p.as_ref()?.id(), a.port()))).collect()
            };
            let mut busiest = 0;
            for (pid, port) in pids {
                if let Some((rss, fds)) = process_usage(pid) {
                    rss_max = rss_max.max(rss);
                    busiest = busiest.max(fds);
                    if fds > fds_max {
                        fds_max = fds;
                        peak = describe_fds(pid, port);
                    }
                    assert!(rss <= SOAK_RSS_CEILING_KB, "a node holds {rss} kB, beyond the ceiling");
                    assert!(fds <= SOAK_FD_CEILING, "a node has {fds} descriptors open, beyond the ceiling");
                }
            }
            fds_floor = fds_floor.min(busiest);
        }
        n += 1;
        window.store(n, Ordering::Relaxed);
        // Workers move to fresh keys. A key is checked once no operation on it can still return:
        // one that came back after the check would be judged alone, against no writes.
        let settled = Instant::now() + Duration::from_secs(300);
        while in_window.iter().any(|w| w.load(Ordering::Relaxed) < n) {
            assert!(Instant::now() < settled, "soak: a register worker is stuck in an operation of window {}", n - 1);
            thread::sleep(Duration::from_millis(50));
        }
        let done: History = std::mem::take(&mut *history.lock().unwrap());
        let closed: History = done.iter().filter(|(k, _)| *k < n as usize * 3).cloned().collect();
        history.lock().unwrap().extend(done.into_iter().filter(|(k, _)| *k >= n as usize * 3));
        match check_keyed(&closed, 50_000_000) {
            Ok(()) => {}
            Err((_, CheckError::Inconclusive)) => eprintln!("soak window {n}: a key was too concurrent to decide"),
            Err((k, e)) => {
                report_unlinearized(&closed, k, &e);
                panic!("soak window {n}: key {k} not linearizable");
            }
        }
        let counted = *counts.lock().unwrap();
        eprintln!("soak: window {n} at {}s: rss max {} MiB, fds max {fds_max} floor {fds_floor} ({peak}); writes {} reads {} transfers {} event batches {} aggregates {} scans {} failures {}", t0.elapsed().as_secs(), rss_max / 1024, counted[0], counted[1], counted[2], counted[3], counted[4], counted[5], counted[6]);
        windows.push((rss_max, fds_max));
    }
    stop.store(true, Ordering::Relaxed);
    for w in workers {
        let _ = w.join();
    }
    cluster.lock().unwrap().heal();
    thread::sleep(Duration::from_secs(3));
    // Money is conserved, whatever was in flight.
    let deadline = Instant::now() + Duration::from_secs(60);
    let total = loop {
        // Only the leader answers in a single group, and any node may be it.
        let read = addrs.iter().find_map(|a| {
            let mut c = StorageClient::connect_timeout(*a, client_timeout(ranges)).ok()?;
            let mut t = 0;
            for i in 0..ACCOUNTS {
                let d = c.doc_get("bank", &format!("a{i}")).ok()??;
                if let Some(PropertyValue::Int64(v)) = d.fields.get("bal") {
                    t += v;
                }
            }
            Some(t)
        });
        match read {
            Some(t) => break t,
            None if Instant::now() < deadline => thread::sleep(Duration::from_millis(500)),
            None => panic!("soak: accounts unreadable 60 s after healing"),
        }
    };
    assert_eq!(total, 100 * ACCOUNTS as i64, "money created or destroyed");
    eprintln!("soak: finished (ranges: {ranges}), balances conserved at {total}");
    // Nothing climbs without end: the last windows hold about what the first full ones did.
    if windows.len() >= 6 {
        let early = windows[1..3].iter().map(|w| w.0).max().unwrap_or(0);
        let late = windows[windows.len() - 2..].iter().map(|w| w.0).max().unwrap_or(0);
        assert!(late <= early * 3 / 2 + (256 << 10), "memory climbed: {early} kB early, {late} kB late: {windows:?}");
        let (early_fds, late_fds) = (windows[1..3].iter().map(|w| w.1).max().unwrap_or(0), windows[windows.len() - 2..].iter().map(|w| w.1).max().unwrap_or(0));
        assert!(late_fds <= early_fds * 2 + 64, "descriptors climbed: {early_fds} early, {late_fds} late");
    }
    let counted = *counts.lock().unwrap();
    assert!(counted[0] > 0 && counted[1] > 0 && counted[2] > 0 && counted[3] > 0 && counted[4] + counted[5] > 0, "a part of the work never ran: {counted:?}");
    drop(cluster);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore]
fn soak_range_cluster() {
    soak(true);
}

#[test]
#[ignore]
fn soak_single_group() {
    soak(false);
}
