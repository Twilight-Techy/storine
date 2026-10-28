//! Clustered serving (`TASK-101`, `F-DIST-05`): the TCP Raft transport, the bounded worker pool
//! that keeps consensus waits off the reactor threads, and the per-server cluster context.

use std::collections::{HashMap, VecDeque};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use storage_codec::ZeroCopyRecord;
use storage_core::raft::{
    AppendEntriesReq, AppendEntriesResp, InstallSnapshotReq, InstallSnapshotResp, Membership,
    RequestVoteReq, RequestVoteResp,
};
use storage_core::raft_runtime::{RaftRuntime, RaftTransport};
use storage_core::NodeId;
use storage_index::Indexable;
use storage_types::{Result, StorageError};

use crate::{StorageClient, STATUS_OK};

/// A peer's cached connection (`None` until first used or after a failure).
type PeerConn = Arc<Mutex<Option<StorageClient>>>;

/// Sends Raft RPCs to peers over TCP (`OP_RAFT_*`, Admin role), one cached connection per peer,
/// reconnecting on failure. Every call is bounded by `timeout`.
pub struct TcpRaftTransport {
    /// Raft address and cached connection of every known peer. Members added to the cluster
    /// later are learned from the configuration ([`RaftTransport::update_members`]).
    peers: Mutex<HashMap<NodeId, (SocketAddr, PeerConn)>>,
    token: Option<String>,
    timeout: Duration,
}

impl core::fmt::Debug for TcpRaftTransport {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TcpRaftTransport").field("peers", &self.peers).finish_non_exhaustive()
    }
}

impl TcpRaftTransport {
    /// `peers`: node id -> Raft address of every other node; `token`: an Admin token accepted by
    /// every peer (required when peers enforce authentication).
    pub fn new(peers: HashMap<NodeId, SocketAddr>, token: Option<String>, timeout: Duration) -> Self {
        let peers = peers
            .into_iter()
            .map(|(id, addr)| (id, (addr, Arc::new(Mutex::new(None)))))
            .collect();
        Self { peers: Mutex::new(peers), token, timeout }
    }

    fn call(&self, peer: NodeId, f: impl Fn(&mut StorageClient) -> Result<(u8, u64, Vec<u8>)>) -> Result<Vec<u8>> {
        let (addr, slot) = self
            .peers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&peer)
            .map(|(a, c)| (*a, Arc::clone(c)))
            .ok_or_else(|| StorageError::invalid_key("unknown raft peer"))?;
        let mut guard = slot.lock().unwrap_or_else(|p| p.into_inner());
        for attempt in 0..2 {
            if guard.is_none() {
                let mut c = StorageClient::connect_timeout(addr, self.timeout)?;
                if let Some(ref t) = self.token {
                    c.authenticate(t)?;
                }
                *guard = Some(c);
            }
            let Some(client) = guard.as_mut() else { continue };
            match f(client) {
                Ok((STATUS_OK, _, body)) => return Ok(body),
                Ok((status, _, body)) => {
                    *guard = None;
                    return Err(StorageError::Rejected {
                        status,
                        message: String::from_utf8_lossy(&body).into_owned(),
                    });
                }
                // A stale cached connection: reconnect once.
                Err(e) => {
                    *guard = None;
                    if attempt == 1 {
                        return Err(e);
                    }
                }
            }
        }
        Err(StorageError::resource_exhausted("raft peer unreachable"))
    }
}

impl RaftTransport for TcpRaftTransport {
    fn request_vote(&self, peer: NodeId, req: &RequestVoteReq) -> Result<RequestVoteResp> {
        let body = self.call(peer, |c| c.raft_vote(req))?;
        RequestVoteResp::from_wire_bytes(&body)
    }

    fn append_entries(&self, peer: NodeId, req: &AppendEntriesReq) -> Result<AppendEntriesResp> {
        let body = self.call(peer, |c| c.raft_append(req))?;
        AppendEntriesResp::from_wire_bytes(&body)
    }

    fn install_snapshot(&self, peer: NodeId, req: &InstallSnapshotReq) -> Result<InstallSnapshotResp> {
        let body = self.call(peer, |c| c.raft_install_snapshot(req))?;
        InstallSnapshotResp::from_wire_bytes(&body)
    }

    /// Learns the address of members added to the configuration; a changed address drops the
    /// cached connection. Members without an address keep the one given at start-up.
    fn update_members(&self, members: &Membership) {
        let mut peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
        for m in members.members().iter().filter(|m| !m.addr.is_empty()) {
            let Ok(addr) = resolve(&m.addr) else { continue };
            if peers.get(&m.id).is_none_or(|(a, _)| *a != addr) {
                peers.insert(m.id, (addr, Arc::new(Mutex::new(None))));
            }
        }
    }
}

/// Resolves `host:port` to one socket address.
pub fn resolve(addr: &str) -> Result<SocketAddr> {
    addr.to_socket_addrs()
        .map_err(|e| StorageError::io("resolve cluster address", e))?
        .next()
        .ok_or_else(|| StorageError::invalid_key("cluster address did not resolve"))
}

/// Response of a pooled job: `(status, version, body)`.
pub(crate) type JobResponse = (u8, u64, Vec<u8>);
type Job = Box<dyn FnOnce() -> JobResponse + Send + 'static>;
/// Wakes the reactor shard that owns the connection waiting for a job.
type Waker = Arc<dyn Fn() + Send + Sync>;
type QueuedJob = (Job, mpsc::Sender<JobResponse>, Waker);

/// Fixed-size worker pool for requests that block on consensus (replicated writes, read-index
/// reads). Bounded queue: when full, requests are shed with `Overloaded` (`RULE-SCALE-01`).
pub(crate) struct WorkerPool {
    queue: Mutex<VecDeque<QueuedJob>>,
    cv: Condvar,
    capacity: usize,
    shutdown: AtomicBool,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl WorkerPool {
    pub(crate) fn new(workers: usize, capacity: usize) -> Arc<Self> {
        let pool = Arc::new(Self {
            queue: Mutex::new(VecDeque::new()),
            cv: Condvar::new(),
            capacity,
            shutdown: AtomicBool::new(false),
            threads: Mutex::new(Vec::new()),
        });
        let mut threads = Vec::with_capacity(workers);
        for i in 0..workers.max(1) {
            let p = Arc::clone(&pool);
            if let Ok(h) = thread::Builder::new()
                .name(format!("storine-worker-{i}"))
                .spawn(move || p.run())
            {
                threads.push(h);
            }
        }
        *pool.threads.lock().unwrap_or_else(|p| p.into_inner()) = threads;
        pool
    }

    fn run(&self) {
        loop {
            let item = {
                let mut q = self.queue.lock().unwrap_or_else(|p| p.into_inner());
                loop {
                    if let Some(item) = q.pop_front() {
                        break Some(item);
                    }
                    if self.shutdown.load(Ordering::Acquire) {
                        break None;
                    }
                    q = self.cv.wait(q).unwrap_or_else(|p| p.into_inner());
                }
            };
            let Some((job, tx, wake)) = item else { return };
            let resp = job();
            let _ = tx.send(resp);
            wake();
        }
    }

    /// Queues `job`; `wake` is called after its response is sent (wakes the reactor shard).
    pub(crate) fn submit(
        &self,
        job: Job,
        wake: Arc<dyn Fn() + Send + Sync>,
    ) -> core::result::Result<Receiver<JobResponse>, ()> {
        let (tx, rx) = mpsc::channel();
        let mut q = self.queue.lock().unwrap_or_else(|p| p.into_inner());
        if q.len() >= self.capacity || self.shutdown.load(Ordering::Acquire) {
            return Err(());
        }
        q.push_back((job, tx, wake));
        drop(q);
        self.cv.notify_one();
        Ok(rx)
    }

    pub(crate) fn stop(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.cv.notify_all();
        let threads = core::mem::take(&mut *self.threads.lock().unwrap_or_else(|p| p.into_inner()));
        for t in threads {
            let _ = t.join();
        }
    }
}

/// What a clustered server needs: the runtime, client-facing addresses for redirects, and the
/// worker pool.
pub struct ClusterOptions<T: ZeroCopyRecord + Indexable> {
    pub runtime: Arc<RaftRuntime<T>>,
    /// Client-facing address (`host:port`) of the nodes known at start-up, used in `NotLeader`
    /// redirects; members added later are found through the cluster configuration.
    pub client_addrs: HashMap<NodeId, String>,
}

impl<T: ZeroCopyRecord + Indexable> core::fmt::Debug for ClusterOptions<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ClusterOptions").field("client_addrs", &self.client_addrs).finish_non_exhaustive()
    }
}

pub(crate) struct ClusterContext<T: ZeroCopyRecord + Indexable> {
    pub(crate) runtime: Arc<RaftRuntime<T>>,
    pub(crate) client_addrs: HashMap<NodeId, String>,
    pub(crate) pool: Arc<WorkerPool>,
}

impl<T: ZeroCopyRecord + Indexable> ClusterContext<T> {
    /// Client address of node `id`: from the cluster configuration, else the start-up list.
    pub(crate) fn addr_of(&self, id: NodeId) -> String {
        self.runtime
            .membership()
            .get(id)
            .map(|m| m.addr.clone())
            .filter(|a| !a.is_empty())
            .or_else(|| self.client_addrs.get(&id).cloned())
            .unwrap_or_default()
    }

    /// `NotLeader` reply: version = leader node id (`0` unknown), body = leader client address.
    pub(crate) fn not_leader(&self) -> JobResponse {
        let leader = self.runtime.node().leader_id();
        let addr = leader.map(|l| self.addr_of(l)).unwrap_or_default();
        (crate::STATUS_NOT_LEADER, leader.map_or(0, u64::from), addr.into_bytes())
    }
}

/// Opens a plain TCP connection with connect / read / write timeouts.
pub(crate) fn connect_with_timeout(addr: SocketAddr, timeout: Duration) -> Result<TcpStream> {
    let stream = TcpStream::connect_timeout(&addr, timeout)
        .map_err(|e| StorageError::io("connect to cluster peer", e))?;
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));
    let _ = stream.set_nodelay(true);
    Ok(stream)
}
