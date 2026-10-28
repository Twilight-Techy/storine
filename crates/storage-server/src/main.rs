//! Standalone binary entrypoint for `storage-server` (`TASK-029`).
//!
//! Usage: `storage-server [data_dir] [bind_addr] [--max-connections N] [--records agent]
//!         [--node-id N (--peers 1=host:port,2=host:port,3=host:port | --join)] [--once]`
//!
//! Cluster mode (`--node-id` + `--peers`, every member listed including this one): the node joins
//! a Raft group; writes go through consensus on the leader, followers redirect clients to it.
//! Peers talk Raft over the same port. `STORAGE_CLUSTER_TOKEN` is the Admin token nodes use to
//! authenticate to each other (it is registered automatically; required with authentication).
//! `--peers` only bootstraps a new cluster: once the cluster changes its membership
//! (`storage-cli cluster ...`, `OP_CLUSTER_CHANGE`), the configuration in the Raft log wins.
//!
//! Growing a cluster (`TASK-121`): start the new node with `--node-id N --join` (no `--peers`;
//! it waits, without campaigning, for the leader to add it), then ask the leader to add it:
//! `storage-cli cluster <leader> add N host:port` (it joins as a learner and is promoted to
//! voter once it has caught up).
//!
//! Security configuration (environment):
//! - `STORAGE_AUTH_TOKENS`: comma-separated `token:role` pairs (`role` = `read` | `write` | `admin`).
//!   When set, every connection must authenticate (`OP_AUTH`). When unset, the server only
//!   accepts a loopback bind address.
//! - `STORAGE_BACKUP_ROOT`: directory under which remote `OP_ADMIN_BACKUP` may create backups
//!   (admin backups are disabled when unset).
//! - `STORAGE_RATE_LIMIT`: per-connection `rps:burst` token-bucket limit.
//! - `STORAGE_MAX_CONNECTIONS`: connection ceiling (overridden by `--max-connections`).

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

use std::env;
use std::process::ExitCode;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use std::collections::HashMap;

use storage_core::raft::{Member, Membership};
use storage_core::raft_runtime::{RaftConfig, RaftRuntime};
use storage_core::{AgentObjectRecord, DocumentStore, EngineConfig, NodeId, RaftNode, StorageEngine};
use storage_server::{cluster, RbacRole, SecurityConfig, StorageServer, DEFAULT_MAX_CONNECTIONS};
use storage_types::{Result, StorageError};

fn security_from_env() -> Result<SecurityConfig> {
    let mut sec = SecurityConfig::default();
    if let Ok(tokens) = env::var("STORAGE_AUTH_TOKENS") {
        sec = SecurityConfig::new(true);
        for pair in tokens.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let (token, role) = pair.rsplit_once(':').ok_or_else(|| {
                StorageError::invalid_key("STORAGE_AUTH_TOKENS entries must be `token:role`")
            })?;
            let role = RbacRole::parse(role).ok_or_else(|| {
                StorageError::invalid_key("STORAGE_AUTH_TOKENS role must be read, write or admin")
            })?;
            if token.len() < 16 {
                return Err(StorageError::invalid_key(
                    "STORAGE_AUTH_TOKENS tokens must be at least 16 characters",
                ));
            }
            sec = sec.with_token(token, role);
        }
    }
    if let Ok(root) = env::var("STORAGE_BACKUP_ROOT") {
        sec = sec.with_backup_root(root);
    }
    if let Ok(limit) = env::var("STORAGE_RATE_LIMIT") {
        let (rps, burst) = limit
            .split_once(':')
            .and_then(|(r, b)| Some((r.trim().parse().ok()?, b.trim().parse().ok()?)))
            .ok_or_else(|| StorageError::invalid_key("STORAGE_RATE_LIMIT must be `rps:burst`"))?;
        sec = sec.with_rate_limit(rps, burst);
    }
    Ok(sec)
}

fn run() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let positional: Vec<&String> = {
        let mut out = Vec::new();
        let mut skip_next = false;
        for a in args.iter().skip(1) {
            if skip_next {
                skip_next = false;
                continue;
            }
            if a == "--max-connections" || a == "--records" || a == "--node-id" || a == "--peers" {
                skip_next = true;
                continue;
            }
            if !a.starts_with("--") {
                out.push(a);
            }
        }
        out
    };
    let data_dir = positional.first().map_or("./server_data", |s| s.as_str());
    let bind_addr = positional.get(1).map_or("127.0.0.1:7878", |s| s.as_str());

    let mut max_connections = env::var("STORAGE_MAX_CONNECTIONS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_CONNECTIONS);
    if let Some(pos) = args.iter().position(|a| a == "--max-connections") {
        max_connections = args
            .get(pos + 1)
            .and_then(|v| v.parse::<usize>().ok())
            .ok_or_else(|| StorageError::invalid_key("--max-connections needs a number"))?;
    }

    let security = security_from_env()?;
    let auth_enabled = security.require_auth;
    // `--records agent` serves the legacy typed `AgentObjectRecord` collection; the default is
    // the document API used by every SDK and the SQL adapter.
    let legacy_agent = args
        .windows(2)
        .any(|w| w[0] == "--records" && w[1] == "agent");
    let once = args.iter().any(|s| s == "--once");
    if legacy_agent {
        let engine = Arc::new(StorageEngine::<AgentObjectRecord>::open(
            data_dir,
            EngineConfig::default(),
        )?);
        let server = StorageServer::start_with_config(bind_addr, engine, max_connections, security)?;
        report((server.local_addr(), server.max_connections()), data_dir, auth_enabled, "agent records");
        return park(server, once);
    }
    let store = Arc::new(DocumentStore::open(data_dir, EngineConfig::default())?);
    let flag = |name: &str| args.iter().position(|a| a == name).and_then(|i| args.get(i + 1));
    let join = args.iter().any(|a| a == "--join");
    if let (Some(node_id), true) = (flag("--node-id"), flag("--peers").is_some() || join) {
        let node_id: NodeId = node_id
            .parse()
            .map_err(|_| StorageError::invalid_key("--node-id must be a number (1-65535)"))?;
        let members = match flag("--peers") {
            Some(peers) => parse_peers(peers)?,
            None => HashMap::new(),
        };
        if !join && !members.contains_key(&node_id) {
            return Err(StorageError::invalid_key("--peers must list this node's own --node-id"));
        }
        let token = env::var("STORAGE_CLUSTER_TOKEN").ok();
        let mut security = security;
        if let Some(ref t) = token {
            if t.len() < 16 {
                return Err(StorageError::invalid_key("STORAGE_CLUSTER_TOKEN must be at least 16 characters"));
            }
            if security.require_auth {
                security = security.with_token(t, RbacRole::Admin);
            }
        } else if security.require_auth {
            return Err(StorageError::invalid_key(
                "cluster mode with authentication needs STORAGE_CLUSTER_TOKEN (peers authenticate with it)",
            ));
        }
        let mut raft_peers = HashMap::new();
        for (&id, addr) in &members {
            if id != node_id {
                raft_peers.insert(id, cluster::resolve(addr)?);
            }
        }
        let node = Arc::new(RaftNode::new(node_id, Arc::clone(store.engine()))?);
        let transport = Arc::new(cluster::TcpRaftTransport::new(raft_peers, token, Duration::from_millis(500)));
        // A joining node starts without a configuration (it never campaigns) and learns the
        // cluster's from the leader.
        let bootstrap = if join {
            Membership::default()
        } else {
            Membership::new(
                members.iter().map(|(&id, addr)| Member { id, addr: addr.clone(), voter: true }).collect(),
            )?
        };
        let runtime = RaftRuntime::start_with_membership(node, bootstrap, transport, RaftConfig::default())?;
        let server = StorageServer::start_documents_cluster(
            bind_addr,
            store,
            max_connections,
            security,
            cluster::ClusterOptions { runtime, client_addrs: members.clone() },
        )?;
        let mode = if join {
            format!("documents, raft node {node_id} waiting to be added to a cluster")
        } else {
            format!("documents, raft node {node_id} of {}", members.len())
        };
        report((server.local_addr(), server.max_connections()), data_dir, auth_enabled, &mode);
        return park(server, once);
    }
    let server = StorageServer::start_documents(bind_addr, store, max_connections, security, None)?;
    report((server.local_addr(), server.max_connections()), data_dir, auth_enabled, "documents");
    park(server, once)
}

/// Parses `1=host:port,2=host:port,...`.
fn parse_peers(spec: &str) -> Result<HashMap<NodeId, String>> {
    let mut out = HashMap::new();
    for item in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (id, addr) = item
            .split_once('=')
            .ok_or_else(|| StorageError::invalid_key("--peers entries must be `id=host:port`"))?;
        let id: NodeId = id
            .trim()
            .parse()
            .map_err(|_| StorageError::invalid_key("--peers ids must be numbers (1-65535)"))?;
        if id == 0 || out.insert(id, addr.trim().to_owned()).is_some() {
            return Err(StorageError::invalid_key("--peers ids must be unique and non-zero"));
        }
    }
    if out.is_empty() {
        return Err(StorageError::invalid_key("--peers is empty"));
    }
    Ok(out)
}

/// Keeps `server` running until the process is stopped (returns at once with `--once`).
fn park<S>(server: S, once: bool) -> Result<()> {
    if once {
        return Ok(());
    }
    let _keep = server;
    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}

fn report(
    (addr, max_conns): (std::net::SocketAddr, usize),
    data_dir: &str,
    auth: bool,
    mode: &str,
) {
    println!(
        "storage-server listening on {addr} (data_dir: {data_dir}, mode: {mode}, max_connections: {max_conns}, auth: {})",
        if auth { "required" } else { "disabled (loopback only)" }
    );
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(1)
        }
    }
}
