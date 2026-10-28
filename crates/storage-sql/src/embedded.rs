//! Embedded mode: a document server in this process, so one binary serves SQL with no separate
//! `storage-server` to run.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use storage_core::{DocumentStore, EngineConfig as StoreConfig};
use storage_server::{SecurityConfig, StorageServer};

use crate::error::{SqlError, SqlResult};
use crate::exec::{Engine, EngineConfig};
use crate::store::Store;

/// A running embedded document server and the SQL engine connected to it.
#[derive(Debug)]
pub struct Embedded {
    pub server: StorageServer<storage_core::DocumentRecord>,
    pub engine: Arc<Engine>,
}

/// Opens (or creates) the database in `dir` and starts a loopback document server for it.
pub fn start(dir: &Path, config: EngineConfig) -> SqlResult<Embedded> {
    let store = Arc::new(
        DocumentStore::open(dir, StoreConfig::default())
            .map_err(|e| SqlError::internal(e.to_string()))?,
    );
    let server =
        StorageServer::start_documents("127.0.0.1:0", store, 4096, SecurityConfig::default(), None)
            .map_err(|e| SqlError::internal(e.to_string()))?;
    let url = format!("storine://{}", server.local_addr());
    let store = Store::connect(&url, Duration::from_secs(10))?;
    Ok(Embedded {
        server,
        engine: Engine::new(store, config),
    })
}
