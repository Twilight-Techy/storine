//! Standalone binary entrypoint for `storage-server` (`TASK-029`).

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

use std::env;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use storage_core::{AgentObjectRecord, EngineConfig, StorageEngine};
use storage_server::{StorageServer, DEFAULT_MAX_CONNECTIONS};

fn main() -> storage_types::Result<()> {
    let args: Vec<String> = env::args().collect();
    let data_dir = args.get(1).map(|s| s.as_str()).unwrap_or("./server_data");
    let bind_addr = args.get(2).map(|s| s.as_str()).unwrap_or("127.0.0.1:7878");

    let mut max_connections = env::var("STORAGE_MAX_CONNECTIONS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_CONNECTIONS);

    for i in 1..args.len() {
        if args[i] == "--max-connections" {
            if let Some(val) = args.get(i + 1).and_then(|v| v.parse::<usize>().ok()) {
                max_connections = val;
            }
        }
    }

    let engine = Arc::new(StorageEngine::<AgentObjectRecord>::open(
        data_dir,
        EngineConfig::default(),
    )?);
    let server = StorageServer::start_with_max_connections(bind_addr, engine, max_connections)?;
    println!(
        "storage-server listening on {} (data_dir: {}, max_connections: {})",
        server.local_addr(),
        data_dir,
        server.max_connections()
    );

    if args.iter().any(|s| s == "--once") {
        return Ok(());
    }

    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}
