//! Load against a running Storine server or single-group cluster (`TASK-157`): `threads`
//! clients write and read documents for `secs` seconds; prints throughput and latency
//! percentiles of each.
//!
//!     server-load <addr>[,<addr>...] [threads=4] [secs=20] [value_bytes=200]
//!
//! Writes go to whichever node accepts them (the leader); each thread keeps its connection.
//! Reads follow, on the same node, of keys this thread wrote.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use storage_codec::PropertyValue;
use storage_core::{DocOp, DocWrite, Precondition};
use storage_server::StorageClient;

fn pct(v: &mut [u64], p: f64) -> u64 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    v[((v.len() as f64 * p) as usize).min(v.len() - 1)]
}

fn connect_writable(addrs: &[SocketAddr], probe: &DocWrite) -> StorageClient {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        for a in addrs {
            if let Ok(mut c) = StorageClient::connect_timeout(*a, Duration::from_secs(1)) {
                if c.doc_write(std::slice::from_ref(probe), None).is_ok() {
                    return c;
                }
            }
        }
        assert!(Instant::now() < deadline, "no node accepted writes");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let addrs: Vec<SocketAddr> = args.get(1).expect("usage: server-load <addr>[,<addr>...] [threads] [secs] [value_bytes]").split(',').map(|a| a.parse().expect("address")).collect();
    let threads: usize = args.get(2).and_then(|a| a.parse().ok()).unwrap_or(4);
    let secs: u64 = args.get(3).and_then(|a| a.parse().ok()).unwrap_or(20);
    let value: usize = args.get(4).and_then(|a| a.parse().ok()).unwrap_or(200);
    let put = |key: String| {
        let mut f = BTreeMap::new();
        f.insert("v".to_string(), PropertyValue::Text("x".repeat(value)));
        DocWrite { collection: "load".into(), key, op: DocOp::Put(f), precondition: Precondition::None }
    };
    let results: Vec<(Vec<u64>, Vec<u64>)> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let addrs = &addrs;
                let put = &put;
                s.spawn(move || {
                    let mut c = connect_writable(addrs, &put(format!("t{t}-probe")));
                    let (mut w, mut r) = (Vec::new(), Vec::new());
                    let end = Instant::now() + Duration::from_secs(secs);
                    let mut i = 0u64;
                    while Instant::now() < end {
                        let key = format!("t{t}-{}", i % 10_000);
                        let start = Instant::now();
                        if c.doc_write(&[put(key.clone())], None).is_err() {
                            c = connect_writable(addrs, &put(key.clone()));
                            continue;
                        }
                        w.push(start.elapsed().as_nanos() as u64);
                        let start = Instant::now();
                        if c.doc_get("load", &key).is_ok() {
                            r.push(start.elapsed().as_nanos() as u64);
                        }
                        i += 1;
                    }
                    (w, r)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut w: Vec<u64> = results.iter().flat_map(|(w, _)| w.iter().copied()).collect();
    let mut r: Vec<u64> = results.iter().flat_map(|(_, r)| r.iter().copied()).collect();
    let line = |name: &str, v: &mut Vec<u64>| {
        let n = v.len();
        println!(
            "server-load {name:<6} {:>10.0} ops/s   p50 {:>9} ns   p99 {:>10} ns   p99.9 {:>10} ns",
            n as f64 / secs as f64,
            pct(v, 0.50),
            pct(v, 0.99),
            pct(v, 0.999)
        );
    };
    line("write", &mut w);
    line("read", &mut r);
}
