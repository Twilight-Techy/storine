//! Reactor scaling benchmark (`TASK-090`, `TASK-102`): request latency on one active connection
//! while `N` idle connections are open on the same server. Run once per reactor and compare:
//!
//! ```text
//! cargo run --release -p storage-benches --bin reactor-scale
//! STORINE_REACTOR=poll cargo run --release -p storage-benches --bin reactor-scale
//! ```

use std::net::TcpStream;
use std::sync::Arc;
use std::time::Instant;

use storage_core::{AgentObjectRecord, EngineConfig, StorageEngine};
use storage_server::{StorageClient, StorageServer};
use storage_types::InlineId;

fn main() -> storage_types::Result<()> {
    let dir = std::env::temp_dir().join(format!("stge_reactor_scale_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Arc::new(StorageEngine::<AgentObjectRecord>::open(&dir, EngineConfig::default())?);
    let id = InlineId::new("k")?;
    engine.put(id, &AgentObjectRecord::new(id, "v", 1, 1))?;
    let mut server = StorageServer::start("127.0.0.1:0", Arc::clone(&engine))?;
    let addr = server.local_addr();
    let mode = std::env::var("STORINE_REACTOR").unwrap_or_else(|_| "default (epoll / AFD)".into());
    println!("reactor: {mode}");
    println!("{:>8} | {:>10} | {:>9} | {:>9}", "idle", "ops/s", "p50 ns", "p99 ns");
    let mut idle: Vec<TcpStream> = Vec::new();
    for target in [0usize, 1_000, 4_000, 8_000] {
        while idle.len() < target {
            match TcpStream::connect(addr) {
                Ok(s) => idle.push(s),
                Err(_) => break, // descriptor limit reached
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
        let mut c = StorageClient::connect(addr)?;
        for _ in 0..500 {
            c.get_frame(id)?;
        }
        let n = 5_000;
        let mut lat = Vec::with_capacity(n);
        let start = Instant::now();
        for _ in 0..n {
            let t = Instant::now();
            c.get_frame(id)?;
            lat.push(t.elapsed().as_nanos() as u64);
        }
        let secs = start.elapsed().as_secs_f64();
        lat.sort_unstable();
        println!(
            "{:>8} | {:>10.0} | {:>9} | {:>9}",
            idle.len(),
            n as f64 / secs,
            lat[n / 2],
            lat[n * 99 / 100]
        );
    }
    drop(idle);
    server.stop();
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}
