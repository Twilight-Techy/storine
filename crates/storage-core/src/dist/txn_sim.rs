//! Deterministic simulation of distributed transactions (`F-GEO-04`, `F-GEO-06`,
//! `TASK-140`, `TASK-142`).
//!
//! Nodes run [`NodeServer`]s (multi-range Raft plus the transactional request layer) on
//! simulated disks and a simulated network, as in [`crate::dist::sim`].
//!
//! **Clients.** Each client is a real coordinator ([`crate::dist::txn`]) on its own thread.
//! Only the thread holding the *baton* runs, and the scheduler hands it out at `send`
//! boundaries in seeded order. Transactions really interleave (conflicts, pushes, wound-wait,
//! staging recovery), and a seed still replays exactly.
//!
//! **Checks.**
//! - Snapshot audits always see the bank's conserved total.
//! - Every committed transaction, replayed in commit-timestamp order, read exactly what the
//!   transactions before it wrote (serializability).
//! - Outcomes unknown to their coordinators are settled from the transaction records.
//! - Every intent is resolvable at the end.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};

use storage_codec::PropertyValue;
use storage_types::testing::SweepRng;
use storage_types::vfs::{CutMode, MountGuard, SimVfs};
use storage_types::StorageError;

use crate::dist::hlc::{Hlc, ManualClock, Timestamp};
use crate::dist::mvcc::{MvccKey, TxnId, TxnRecord, TxnStatus, Value, TXN_COLLECTION};
use crate::dist::meta::RangeIndex;
use crate::dist::node::{Batch, NodeConfig, NodeDriver, RangeId};
use crate::dist::server::{Key, KvRequest, KvResponse, MetaOp, NodeServer};
use crate::dist::txn::{self, KvSender, TxnError};
use crate::document::{document_id, DocumentRecord};
use crate::engine::{EngineConfig, StorageEngine};
use crate::raft::KeySpan;
use crate::sharding::NodeId;

#[derive(Clone, Debug)]
pub struct TxnSimConfig {
    pub nodes: usize,
    pub regions: Vec<u8>,
    pub local_latency: (u64, u64),
    pub remote_latency: (u64, u64),
    pub drop_ppm: u64,
    pub ranges: usize,
    pub accounts: usize,
    pub clients: usize,
    pub txns_per_client: usize,
    /// One audit (read every account in one snapshot) per this many transfers.
    pub audit_every: usize,
    pub initial_balance: i64,
    pub crashes: bool,
    pub partitions: bool,
    pub fault_every: u64,
    pub tick_us: u64,
    /// A request without a reply after this long fails (the coordinator handles it).
    pub request_timeout: u64,
    /// Maximum clock offset between nodes (and the HLC bound).
    pub max_offset_ns: u64,
    pub max_events: u64,
    pub node: NodeConfig,
    /// Clients route through their own range caches, filled by meta-range lookups
    /// (`TASK-138`), instead of the simulator's global view.
    pub meta_routing: bool,
    /// A read's first attempt goes to a random replica of its range, not the leaseholder:
    /// replicas serve it from their closed timestamps or redirect (`TASK-141`).
    pub nearest_reads: bool,
    /// Audits read `AS OF` this long ago (follower-readable); `0`: present-time audits.
    pub audit_staleness_ns: u64,
    /// Audits read every account with one scan of the accounts' span across ranges
    /// (`TASK-143`) instead of a read per account.
    pub scan_audits: bool,
    /// Clients use the document API (`TASK-143`) instead of raw transactions: atomic transfers
    /// between documents with conservation audits, and per-client counters retried with one
    /// request id (acknowledged <= final value <= attempted: applied at most once).
    pub doc_workload: bool,
    /// One client takes cluster backups and follows a changefeed at its audit points
    /// (`TASK-144`): every backup and every resolved feed state must equal the serial order at
    /// its timestamp, and a backup must hold every commit acknowledged before it began.
    pub backup_audits: bool,
}

impl Default for TxnSimConfig {
    fn default() -> Self {
        Self {
            nodes: 3,
            regions: vec![0, 0, 0],
            local_latency: (200, 2_000),
            remote_latency: (20_000, 60_000),
            drop_ppm: 10_000,
            ranges: 3,
            accounts: 6,
            clients: 4,
            txns_per_client: 12,
            audit_every: 4,
            initial_balance: 100,
            crashes: true,
            partitions: true,
            fault_every: 700_000,
            tick_us: 10_000,
            request_timeout: 1_500_000,
            max_offset_ns: 50_000_000,
            max_events: 5_000_000,
            node: NodeConfig { election_min_us: 300_000, election_max_us: 600_000, heartbeat_us: 50_000, snapshot_chunk: 2048, checkpoint_us: 300_000, log_retain: 32, ..NodeConfig::default() },
            meta_routing: false,
            nearest_reads: false,
            audit_staleness_ns: 0,
            scan_audits: false,
            doc_workload: false,
            backup_audits: false,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct TxnSimReport {
    pub seed: u64,
    pub committed: usize,
    pub retried: usize,
    pub unknown_then_settled: usize,
    pub unavailable: usize,
    pub audits: usize,
    pub crashes: usize,
    pub partitions: usize,
    pub events: u64,
    pub end_time: u64,
    /// The first error a coordinator reported (diagnostics).
    pub first_error: Option<String>,
    /// Clock updates refused for exceeding the maximum offset.
    pub offset_refusals: usize,
    /// Meta routing: lookups sent, and requests that reached a stale leaseholder or range.
    pub meta_lookups: usize,
    pub misroutes: usize,
    /// Reads replicas served below their closed timestamps.
    pub follower_reads: u64,
    /// Document writes committed in one phase.
    pub one_phase: usize,
    /// Backups finished; backups failed by faults; changefeed passes resolved; feeds restarted
    /// from a backup after losing history.
    pub backups: usize,
    pub backups_failed: usize,
    pub feed_passes: usize,
    pub feeds_lost: usize,
}

/// A transaction with an unknown outcome: id, reads, writes.
type Unknown = (TxnId, Vec<(Key, Value)>, Vec<(Key, Value)>);

/// A committed transaction, for the serializability check.
#[derive(Clone, Debug)]
struct Committed {
    ts: Timestamp,
    reads: Vec<(Key, Value)>,
    writes: Vec<(Key, Value)>,
}

#[derive(Debug)]
enum Ev {
    Deliver { to: usize, batch: Batch },
    Request { to: usize, id: u64, client: usize, req: KvRequest },
    Reply { client: usize, id: u64, resp: KvResponse },
    Tick { node: usize },
    Fault,
    Restart { node: usize },
    Heal { epoch: u64 },
    #[allow(dead_code)]
    Wake { client: usize },
}

struct SimNode {
    dir: PathBuf,
    sim: SimVfs,
    _mount: MountGuard,
    server: Option<NodeServer>,
    clock: ManualClock,
    skew_ns: i64,
}

#[derive(Default)]
struct ClientSlot {
    /// Outstanding request ids -> (index in batch, request, attempt).
    outstanding: BTreeMap<u64, (usize, KvRequest, u32)>,
    replies: Vec<Option<Result<KvResponse, String>>>,
    deadline: u64,
    sleeping_until: Option<u64>,
    done: bool,
    started: bool,
    /// Meta routing: this client's range cache, its outstanding lookups (the request ids
    /// in `outstanding` that are lookups for the request stored there), and where the meta
    /// range's leaseholder was last seen.
    cache: RangeIndex,
    lookups: std::collections::BTreeSet<u64>,
    meta_node: Option<usize>,
    /// Meta lookups sent, and requests that went to a stale leaseholder or range.
    pub_lookups: usize,
    /// How far this client's clock leads (nanoseconds, within the maximum offset).
    skew_ns: u64,
    misroutes: usize,
}

struct World {
    /// Debugging (`STORINE_SIM_WATCH`): last seen state of watched records per node.
    watched: BTreeMap<String, String>,
    cfg: TxnSimConfig,
    rng: SweepRng,
    now: u64,
    seq: u64,
    queue: BinaryHeap<Reverse<(u64, u64)>>,
    events: BTreeMap<u64, Ev>,
    nodes: Vec<SimNode>,
    cut: Vec<Vec<bool>>,
    epoch: u64,
    faults_on: bool,
    slots: Vec<ClientSlot>,
    /// Which client holds the baton.
    turn: Option<usize>,
    next_req: u64,
    leader_hint: BTreeMap<RangeId, usize>,
    committed: Vec<Committed>,
    /// Transactions whose outcome their coordinator could not learn: (id, reads, writes).
    unknown: Vec<Unknown>,
    audits: Vec<(Timestamp, i64)>,
    /// Account states read by backups and changefeeds, at their timestamps.
    snapshots: Vec<(&'static str, Timestamp, BTreeMap<Key, Value>)>,
    report: TxnSimReport,
    failure: Option<String>,
    processed: u64,
    trace: Vec<String>,
}

type Shared = Arc<(Mutex<World>, Condvar)>;

fn account(i: usize) -> Key {
    ("acct".to_string(), format!("a{i:02}"))
}

fn balance(v: &Value) -> i64 {
    match v.as_ref().and_then(|m| m.get("bal")) {
        Some(PropertyValue::Int64(b)) => *b,
        _ => 0,
    }
}

fn bal(b: i64) -> BTreeMap<String, PropertyValue> {
    let mut m = BTreeMap::new();
    m.insert("bal".to_string(), PropertyValue::Int64(b));
    m
}

fn engine_config() -> EngineConfig {
    EngineConfig { background_flush: false, collection_name: "node".into(), wal_sync_policy: storage_wal::SyncPolicy::AsyncMemoryFirst, ..EngineConfig::default() }
}

impl World {
    fn schedule(&mut self, delay: u64, ev: Ev) {
        self.seq += 1;
        self.queue.push(Reverse((self.now + delay, self.seq)));
        self.events.insert(self.seq, ev);
    }

    fn latency(&mut self, a: usize, b: usize) -> u64 {
        let same = self.cfg.regions.get(a) == self.cfg.regions.get(b);
        let (lo, hi) = if same { self.cfg.local_latency } else { self.cfg.remote_latency };
        lo + self.rng.below((hi - lo).max(1) as usize) as u64
    }

    fn node_clock(&mut self, node: usize) {
        let n = &self.nodes[node];
        n.clock.set((self.now as i64 * 1_000 + n.skew_ns).max(0) as u64);
    }

    fn flush(&mut self, node: usize) {
        let now = self.now;
        if let Some(srv) = self.nodes[node].server.as_mut() {
            for line in srv.take_trace() {
                self.trace.push(format!("t={now} {line}"));
            }
        }
        let Some(srv) = self.nodes[node].server.as_mut() else { return };
        let ts = srv.hlc().now();
        srv.driver.set_clock(ts);
        let out = srv.driver.drain_outbox();
        let replies = srv.take_replies();
        for (to, batch) in out {
            let to = usize::from(to) - 1;
            if self.cut[node][to] || (self.rng.below(1_000_000) as u64) < self.cfg.drop_ppm {
                continue;
            }
            let d = self.latency(node, to);
            self.schedule(d, Ev::Deliver { to, batch });
        }
        for r in replies {
            let client = r.client as usize;
            // Clients sit "near" node 1's region; the reply crosses the network.
            let d = self.latency(node, 0);
            self.schedule(d, Ev::Reply { client, id: r.id, resp: r.resp });
        }
    }

    /// The range holding `key` and the node to ask.
    fn route(&mut self, at: &[u8]) -> (Option<RangeId>, usize) {
        let mut best: Option<(RangeId, u64, Option<NodeId>)> = None;
        for n in self.nodes.iter().filter_map(|n| n.server.as_ref()) {
            for r in n.driver.ranges() {
                let Some(rep) = n.driver.replica(r) else { continue };
                if rep.span().contains(at) && best.as_ref().is_none_or(|b| rep.current_term() > b.1) {
                    best = Some((r, rep.current_term(), n.driver.leader_of(r)));
                }
            }
        }
        match best {
            Some((r, _, l)) => {
                let node = self.leader_hint.get(&r).copied().or(l.map(|l| usize::from(l) - 1)).unwrap_or_else(|| self.rng.below(self.cfg.nodes));
                (Some(r), node)
            }
            None => (None, self.rng.below(self.cfg.nodes)),
        }
    }

    /// Debugging: prints every change of a watched transaction record on any node.
    fn watch(&mut self, prefix: &str) {
        for i in 0..self.nodes.len() {
            let Some(srv) = self.nodes[i].server.as_ref() else { continue };
            let mut seen = String::new();
            for id in srv.driver.engine().ids_in_span(b"_txn/", Some(b"_txn0"), usize::MAX) {
                if let Ok((v, rec)) = srv.driver.engine().get_owned(id) {
                    if rec.key.starts_with(prefix) {
                        let st = rec.fields.get("s").map(|x| format!("{x:?}")).unwrap_or_default();
                        let owner = srv.driver.ranges().into_iter().filter(|&r| srv.driver.replica(r).is_some_and(|x| x.span().contains_id(id))).map(|r| format!("r{r}{}", if srv.driver.is_leader(r) { "*" } else { "" })).collect::<Vec<_>>().join(",");
                        seen = format!("v{} s={st} in {owner}", v.0);
                    }
                }
            }
            let key = format!("{prefix}@{i}");
            if self.watched.get(&key) != Some(&seen) {
                eprintln!("watch t={} n{}: {seen}", self.now, i + 1);
                self.watched.insert(key, seen);
            }
        }
    }

    /// A random live node hosting a replica of `key`'s range.
    fn random_host(&mut self, at: &[u8]) -> Option<usize> {
        let hosts: Vec<usize> = (0..self.nodes.len())
            .filter(|&i| self.nodes[i].server.as_ref().is_some_and(|s| s.driver.ranges().into_iter().any(|r| s.driver.replica(r).is_some_and(|x| !x.is_frozen() && x.span().contains(at)))))
            .collect();
        (!hosts.is_empty()).then(|| hosts[self.rng.below(hosts.len())])
    }

    /// Meta routing: the node to send `key`'s request to, from the client's cache.
    fn cached_route(&mut self, client: usize, at: &[u8]) -> Option<usize> {
        let d = self.slots[client].cache.lookup(at)?;
        let n = *d.candidates().first()?;
        Some(usize::from(n) - 1)
    }

    /// Meta routing: resolve `key` with a lookup (the reply re-submits request `idx`).
    fn lookup(&mut self, client: usize, idx: usize, req: KvRequest, attempt: u32, delay: u64) {
        self.next_req += 1;
        let id = self.next_req;
        let key = req.anchor();
        self.slots[client].outstanding.insert(id, (idx, req, attempt));
        self.slots[client].lookups.insert(id);
        self.slots[client].pub_lookups += 1;
        let to = match self.slots[client].meta_node {
            Some(n) => n,
            None => self.rng.below(self.cfg.nodes),
        };
        let d = self.latency(0, to) + delay;
        self.schedule(d, Ev::Request { to, id, client, req: KvRequest::Meta(MetaOp::Lookup { key, limit: 8 }) });
    }

    fn submit(&mut self, client: usize, idx: usize, req: KvRequest, attempt: u32) {
        if self.cfg.meta_routing {
            let Some(node) = self.cached_route(client, &req.anchor()) else {
                return self.lookup(client, idx, req, attempt, 0);
            };
            self.next_req += 1;
            let id = self.next_req;
            self.slots[client].outstanding.insert(id, (idx, req.clone(), attempt));
            let d = self.latency(0, node);
            return self.schedule(d, Ev::Request { to: node, id, client, req });
        }
        self.next_req += 1;
        let id = self.next_req;
        let (_, mut node) = self.route(&req.anchor());
        if self.cfg.nearest_reads && attempt == 0 && matches!(req, KvRequest::Get { .. }) {
            if let Some(n) = self.random_host(&req.anchor()) {
                node = n;
            }
        }
        self.slots[client].outstanding.insert(id, (idx, req.clone(), attempt));
        let d = self.latency(0, node);
        self.schedule(d, Ev::Request { to: node, id, client, req });
    }

    fn handle(&mut self, ev: Ev) {
        match ev {
            Ev::Deliver { to, batch } => {
                self.node_clock(to);
                let now = self.now;
                if let Some(srv) = self.nodes[to].server.as_mut() {
                    if srv.hlc().update(batch.ts).is_err() {
                        self.report.offset_refusals += 1;
                    }
                    if let Err(e) = srv.driver.step(batch, now) {
                        self.failure = Some(format!("n{} step: {e}", to + 1));
                        return;
                    }
                    srv.poll(now * 1_000);
                }
                self.flush(to);
            }
            Ev::Request { to, id, client, req } => {
                self.node_clock(to);
                let now = self.now;
                // A request to a node that is down times out.
                if let Some(srv) = self.nodes[to].server.as_mut() {
                    srv.submit(id, client as u64, req);
                    srv.poll(now * 1_000);
                    self.flush(to);
                }
            }
            Ev::Reply { client, id, resp } => {
                let Some((idx, req, attempt)) = self.slots[client].outstanding.remove(&id) else { return };
                if self.cfg.meta_routing {
                    return self.meta_reply(client, id, idx, req, attempt, resp);
                }
                match resp {
                    KvResponse::NotLeader { leader } if self.now < self.slots[client].deadline => {
                        if let (Some(r), Some(l)) = (self.route(&req.anchor()).0, leader) {
                            self.leader_hint.insert(r, usize::from(l) - 1);
                        } else if let Some(r) = self.route(&req.anchor()).0 {
                            let n = self.rng.below(self.cfg.nodes);
                            self.leader_hint.insert(r, n);
                        }
                        let backoff = 5_000 + self.rng.below(20_000) as u64;
                        // Resend after a short backoff through the outstanding map.
                        self.next_req += 1;
                        let nid = self.next_req;
                        self.slots[client].outstanding.insert(nid, (idx, req.clone(), attempt + 1));
                        let (_, node) = self.route(&req.anchor());
                        self.schedule(backoff, Ev::Request { to: node, id: nid, client, req });
                    }
                    KvResponse::WrongRange if self.now < self.slots[client].deadline => {
                        if let Some(r) = self.route(&req.anchor()).0 {
                            self.leader_hint.remove(&r);
                        }
                        self.next_req += 1;
                        let nid = self.next_req;
                        self.slots[client].outstanding.insert(nid, (idx, req.clone(), attempt + 1));
                        let (_, node) = self.route(&req.anchor());
                        self.schedule(10_000, Ev::Request { to: node, id: nid, client, req });
                    }
                    KvResponse::NotLeader { .. } | KvResponse::WrongRange => self.slots[client].replies[idx] = Some(Err("no leaseholder found".into())),
                    KvResponse::Error(e) => self.slots[client].replies[idx] = Some(Err(e)),
                    resp => self.slots[client].replies[idx] = Some(Ok(resp)),
                }
            }
            Ev::Tick { node } => {
                self.node_clock(node);
                let now = self.now;
                if let Some(srv) = self.nodes[node].server.as_mut() {
                    if let Err(e) = srv.driver.tick(now) {
                        self.failure = Some(format!("n{} tick: {e}", node + 1));
                        return;
                    }
                    srv.poll(now * 1_000);
                    self.flush(node);
                    let t = self.cfg.tick_us;
                    self.schedule(t, Ev::Tick { node });
                }
            }
            Ev::Fault => {
                if self.faults_on {
                    self.fault();
                    let d = self.cfg.fault_every / 2 + self.rng.below(self.cfg.fault_every as usize) as u64;
                    self.schedule(d, Ev::Fault);
                }
            }
            Ev::Restart { node } => self.restart(node),
            Ev::Heal { epoch } => {
                if epoch == self.epoch {
                    self.cut.iter_mut().for_each(|r| r.iter_mut().for_each(|c| *c = false));
                }
            }
            Ev::Wake { .. } => {}
        }
    }

    /// Meta routing: a reply to a lookup, or to a request routed by the cache.
    fn meta_reply(&mut self, client: usize, id: u64, idx: usize, req: KvRequest, attempt: u32, resp: KvResponse) {
        let late = self.now >= self.slots[client].deadline;
        let backoff = 5_000 + self.rng.below(20_000) as u64;
        if self.slots[client].lookups.remove(&id) {
            match resp {
                KvResponse::Ranges(ds) => {
                    for d in ds {
                        self.slots[client].cache.learn(d);
                    }
                    if self.cached_route(client, &req.anchor()).is_some() {
                        self.submit(client, idx, req, attempt);
                    } else if late {
                        self.slots[client].replies[idx] = Some(Err("no route".into()));
                    } else {
                        // Not published yet (a split just happened): ask again shortly.
                        self.lookup(client, idx, req, attempt + 1, 50_000);
                    }
                }
                _ if late => self.slots[client].replies[idx] = Some(Err("meta range unavailable".into())),
                KvResponse::NotLeader { leader } => {
                    self.slots[client].meta_node = Some(match leader {
                        Some(l) => usize::from(l) - 1,
                        None => self.rng.below(self.cfg.nodes),
                    });
                    self.lookup(client, idx, req, attempt + 1, backoff);
                }
                _ => {
                    self.slots[client].meta_node = None;
                    self.lookup(client, idx, req, attempt + 1, backoff);
                }
            }
            return;
        }
        let range = self.slots[client].cache.lookup(&req.anchor()).map(|d| d.range);
        match resp {
            KvResponse::NotLeader { leader } if !late => {
                self.slots[client].misroutes += 1;
                match (range, leader) {
                    (Some(r), Some(l)) => self.slots[client].cache.set_lease(r, l),
                    // No leader known there: try the range's next replica.
                    (Some(r), None) => {
                        let next = self.slots[client].cache.get(r).map(|d| d.candidates()).and_then(|c| c.get((1 + attempt as usize) % c.len().max(1)).copied());
                        if let Some(n) = next {
                            self.slots[client].cache.set_lease(r, n);
                        }
                    }
                    _ => {}
                }
                self.next_req += 1;
                let nid = self.next_req;
                self.slots[client].outstanding.insert(nid, (idx, req.clone(), attempt + 1));
                let node = self.cached_route(client, &req.anchor()).unwrap_or_else(|| self.rng.below(self.cfg.nodes));
                self.schedule(backoff, Ev::Request { to: node, id: nid, client, req });
            }
            KvResponse::WrongRange if !late => {
                // The range split, merged or moved: forget it and look it up again.
                self.slots[client].misroutes += 1;
                if let Some(r) = range {
                    self.slots[client].cache.evict(r);
                }
                self.lookup(client, idx, req, attempt + 1, 10_000);
            }
            KvResponse::NotLeader { .. } | KvResponse::WrongRange => self.slots[client].replies[idx] = Some(Err("no leaseholder found".into())),
            KvResponse::Error(e) => self.slots[client].replies[idx] = Some(Err(e)),
            resp => self.slots[client].replies[idx] = Some(Ok(resp)),
        }
    }

    fn fault(&mut self) {
        let n = self.nodes.len();
        match self.rng.below(6) {
            0..=1 if self.cfg.partitions => {
                self.epoch += 1;
                let side = self.rng.below(n);
                for a in 0..n {
                    for b in 0..n {
                        self.cut[a][b] = (a == side) != (b == side);
                    }
                }
                self.report.partitions += 1;
                let epoch = self.epoch;
                let d = 300_000 + self.rng.below(1_500_000) as u64;
                self.schedule(d, Ev::Heal { epoch });
            }
            2 if self.cfg.crashes => {
                let node = self.rng.below(n);
                if self.nodes.iter().all(|x| x.server.is_some()) {
                    self.report.crashes += 1;
                    self.nodes[node].sim.freeze();
                    if let Some(srv) = self.nodes[node].server.take() {
                        self.report.follower_reads += srv.follower_reads();
                    }
                    let mode = if self.rng.below(2) == 0 { CutMode::Random } else { CutMode::KeepAll };
                    self.nodes[node].sim.set_mode(mode);
                    if let Err(e) = self.nodes[node].sim.power_cut() {
                        self.failure = Some(format!("power cut: {e}"));
                    }
                    let d = 200_000 + self.rng.below(800_000) as u64;
                    self.schedule(d, Ev::Restart { node });
                }
            }
            3 => {
                // Clock skew changes, within the maximum offset.
                let node = self.rng.below(n);
                let half = (self.cfg.max_offset_ns / 2) as i64;
                self.nodes[node].skew_ns = self.rng.below((2 * half).max(1) as usize) as i64 - half;
            }
            _ => {}
        }
    }

    fn open_server(&mut self, node: usize, fresh: Option<&[KeySpan]>) -> Result<NodeServer, StorageError> {
        let dir = self.nodes[node].dir.clone();
        let engine = Arc::new(StorageEngine::<DocumentRecord>::open(&dir, engine_config())?);
        let members: Vec<NodeId> = (1..=self.cfg.nodes as NodeId).collect();
        let cfg = NodeConfig { seed: self.rng.next_u64(), incarnation: self.now + 1, trace: std::env::var_os("STORINE_SIM_TRACE").is_some(), ..self.cfg.node.clone() };
        let mut d = NodeDriver::new((node + 1) as NodeId, engine, cfg);
        match fresh {
            Some(spans) => {
                for (r, span) in spans.iter().enumerate() {
                    d.open_range(r as RangeId + 1, span.clone(), &members)?;
                }
            }
            None => {
                d.reopen_ranges()?;
            }
        }
        let hlc = Arc::new(Hlc::new(Arc::new(self.nodes[node].clock.clone()), std::time::Duration::from_nanos(self.cfg.max_offset_ns)).with_self_fencing(false));
        let mut srv = NodeServer::new(d, hlc);
        if std::env::var("STORINE_SIM_DEBUG").is_ok() {
            srv.enable_trace();
        }
        Ok(srv)
    }

    fn restart(&mut self, node: usize) {
        if self.nodes[node].server.is_some() {
            return;
        }
        self.node_clock(node);
        match self.open_server(node, None) {
            Ok(s) => {
                self.nodes[node].server = Some(s);
                let t = self.cfg.tick_us;
                self.schedule(t, Ev::Tick { node });
            }
            Err(e) => self.failure = Some(format!("n{} recovery: {e}", node + 1)),
        }
    }

    /// The client whose turn it can be (replies complete, timed out, or done sleeping).
    fn runnable(&self) -> Vec<usize> {
        self.slots
            .iter()
            .enumerate()
            .filter(|(_, s)| !s.done && s.started && self.now >= 1_500_000)
            .filter(|(_, s)| match s.sleeping_until {
                Some(t) => self.now >= t,
                None => s.outstanding.is_empty() || self.now >= s.deadline,
            })
            .map(|(i, _)| i)
            .collect()
    }
}

/// A client's view: implements [`KvSender`] by handing the baton back to the scheduler.
struct ClientHandle {
    i: usize,
    shared: Shared,
    hlc: Hlc,
    clock: ManualClock,
    rng: SweepRng,
    /// Collections known to the document layer ([`KvSender::collection_is_plain`]).
    plain: std::collections::BTreeSet<String>,
    one_phase: usize,
}

/// Hands the baton back to the scheduler and waits for client `i`'s next turn.
fn yield_and_wait<'a>(shared: &'a Shared, i: usize, clock: &ManualClock, w: std::sync::MutexGuard<'a, World>) -> std::sync::MutexGuard<'a, World> {
    let (_, cv) = &**shared;
    let mut w = w;
    w.turn = None;
    cv.notify_all();
    while w.turn != Some(i) {
        w = cv.wait(w).unwrap_or_else(|p| p.into_inner());
    }
    clock.set(w.now * 1_000 + w.slots[i].skew_ns);
    w
}

impl KvSender for ClientHandle {
    fn send(&mut self, reqs: Vec<KvRequest>) -> Vec<Result<KvResponse, String>> {
        let shared = Arc::clone(&self.shared);
        let mut w = shared.0.lock().unwrap_or_else(|p| p.into_inner());
        let n = reqs.len();
        w.slots[self.i].replies = vec![None; n];
        w.slots[self.i].outstanding.clear();
        let deadline = w.now + w.cfg.request_timeout;
        w.slots[self.i].deadline = deadline;
        for (idx, mut r) in reqs.into_iter().enumerate() {
            // Requests carry the coordinator's clock where they name a timestamp.
            if let KvRequest::Record { op: crate::dist::server::RecordOp::Push { now, .. }, .. } = &mut r {
                *now = (*now).max(self.hlc.peek());
            }
            w.submit(self.i, idx, r, 0);
        }
        let mut w = yield_and_wait(&shared, self.i, &self.clock, w);
        let out: Vec<Result<KvResponse, String>> = std::mem::take(&mut w.slots[self.i].replies).into_iter().map(|r| r.unwrap_or_else(|| Err("timeout".into()))).collect();
        w.slots[self.i].outstanding.clear();
        drop(w);
        for r in out.iter().flatten() {
            if matches!(r, KvResponse::Atomic { .. }) {
                self.one_phase += 1;
            }
            let ts = match r {
                KvResponse::Value { ts: Some(t), .. } | KvResponse::Written { ts: t } | KvResponse::Uncertain { ts: t } => Some(*t),
                KvResponse::Record(Some(rec)) => Some(rec.ts),
                _ => None,
            };
            if let Some(t) = ts {
                let _ = self.hlc.update(t);
            }
        }
        out
    }

    fn now(&mut self) -> Timestamp {
        self.hlc.now()
    }

    fn max_offset_ns(&self) -> u64 {
        self.hlc.max_offset_ns()
    }

    fn sleep(&mut self, us: u64) {
        let shared = Arc::clone(&self.shared);
        let mut w = shared.0.lock().unwrap_or_else(|p| p.into_inner());
        let until = w.now + us;
        w.slots[self.i].sleeping_until = Some(until);
        w.schedule(us, Ev::Wake { client: self.i });
        let mut w = yield_and_wait(&shared, self.i, &self.clock, w);
        w.slots[self.i].sleeping_until = None;
    }

    fn protect(&mut self, id: u64, ts: Timestamp, expires: u64) {
        let shared = Arc::clone(&self.shared);
        let mut w = shared.0.lock().unwrap_or_else(|p| p.into_inner());
        for n in &mut w.nodes {
            if let Some(srv) = n.server.as_mut() {
                srv.protect(id, ts, expires);
            }
        }
    }

    fn collection_is_plain(&mut self, collection: &str) -> bool {
        self.plain.contains(collection)
    }

    fn note_collection(&mut self, collection: &str, plain: bool) {
        if plain {
            self.plain.insert(collection.to_string());
        } else {
            self.plain.remove(collection);
        }
    }

    fn new_id(&mut self) -> TxnId {
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&self.rng.next_u64().to_le_bytes());
        id[8..].copy_from_slice(&self.rng.next_u64().to_le_bytes());
        id
    }
}

/// The document workload (see [`TxnSimConfig::doc_workload`]).
fn doc_client(h: &mut ClientHandle, cfg: &TxnSimConfig, rng: &mut SweepRng) {
    use crate::dist::docs;
    use crate::document::{DocOp, DocQuery, DocWrite};
    use crate::engine::Precondition;
    use storage_codec::field_ops::FieldOp;
    let add = |c: &str, k: String, d: i64, min: bool| DocWrite {
        collection: c.into(),
        key: k,
        op: DocOp::Patch(vec![FieldOp::Add { path: "n".into(), delta: PropertyValue::Int64(d), min: min.then_some(PropertyValue::Int64(0)), max: None }]),
        precondition: Precondition::None,
    };
    let put = |c: &str, k: String, n: i64| DocWrite { collection: c.into(), key: k, op: DocOp::Put(BTreeMap::from([("n".to_string(), PropertyValue::Int64(n))])), precondition: Precondition::None };
    let fail = |h: &ClientHandle, msg: String| {
        let shared = Arc::clone(&h.shared);
        let mut w = shared.0.lock().unwrap_or_else(|p| p.into_inner());
        w.failure.get_or_insert(msg);
    };
    let me = h.i;
    let counter = format!("c{me}");
    // One request id per logical write, reused by its retries.
    let mut seq = 0u64;
    let mut send = |h: &mut ClientHandle, writes: &[DocWrite], rng: &mut SweepRng| -> bool {
        seq += 1;
        let mut rid = [0u8; 16];
        rid[..8].copy_from_slice(&(me as u64).to_le_bytes());
        rid[8..].copy_from_slice(&seq.to_le_bytes());
        for _ in 0..4 {
            if docs::write(h, writes.to_vec(), Some(rid)).is_ok() {
                return true;
            }
            h.sleep(50_000 + rng.below(100_000) as u64);
        }
        false
    };
    if me == 0 {
        let seed: Vec<DocWrite> = (0..cfg.accounts).map(|i| put("bank", format!("a{i}"), cfg.initial_balance)).collect();
        while !send(h, &seed, rng) {}
    }
    if !send(h, &[put("ctr", counter.clone(), 0)], rng) {
        return;
    }
    let (mut attempted, mut acked) = (0i64, 0i64);
    for n in 0..cfg.txns_per_client * 2 {
        match n % 4 {
            0 | 1 => {
                attempted += 1;
                if send(h, &[add("ctr", counter.clone(), 1, false)], rng) {
                    acked += 1;
                }
            }
            2 => {
                let (a, b) = (rng.below(cfg.accounts), rng.below(cfg.accounts));
                if a != b {
                    let amount = 1 + rng.below(10) as i64;
                    send(h, &[add("bank", format!("a{a}"), -amount, true), add("bank", format!("a{b}"), amount, false)], rng);
                }
            }
            _ => {
                let q = DocQuery { collection: "bank".into(), ..DocQuery::default() };
                if let Ok(page) = docs::query(h, &q) {
                    let total: i64 = page.documents.iter().map(|d| if let Some(PropertyValue::Int64(v)) = d.fields.get("n") { *v } else { 0 }).sum();
                    if page.documents.len() == cfg.accounts && total != cfg.initial_balance * cfg.accounts as i64 {
                        fail(h, format!("document audit saw total {total}: {:?}", page.documents.iter().map(|d| (d.key.clone(), d.fields.get("n").cloned())).collect::<Vec<_>>()));
                        return;
                    }
                    let shared = Arc::clone(&h.shared);
                    shared.0.lock().unwrap_or_else(|p| p.into_inner()).report.audits += 1;
                }
            }
        }
    }
    for _ in 0..40 {
        match docs::get(h, "ctr", &counter) {
            Ok(Some(d)) => {
                let v = if let Some(PropertyValue::Int64(v)) = d.fields.get("n") { *v } else { -1 };
                if v < acked || v > attempted {
                    fail(h, format!("client {me}: counter {v} outside acknowledged {acked} .. attempted {attempted}"));
                }
                let shared = Arc::clone(&h.shared);
                shared.0.lock().unwrap_or_else(|p| p.into_inner()).report.committed += usize::try_from(acked).unwrap_or(0);
                return;
            }
            _ => h.sleep(100_000),
        }
    }
}

/// A changefeed consumer's state: its cursor (kept encoded, as a consumer would) and everything
/// delivered so far.
#[derive(Default)]
struct FeedConsumer {
    cursor: Option<Vec<u8>>,
    log: BTreeMap<(Key, Timestamp), Value>,
}

impl FeedConsumer {
    /// The state at `at`: each key's newest change at or below it.
    fn state(&self, at: Timestamp) -> BTreeMap<Key, Value> {
        let mut out = BTreeMap::new();
        for ((k, t), v) in &self.log {
            if *t <= at {
                out.insert(k.clone(), v.clone());
            }
        }
        out
    }
}

/// One backup, then the changefeed up to its next resolved timestamp.
fn backup_and_feed(h: &mut ClientHandle, feed: &mut FeedConsumer, rng: &mut SweepRng) {
    use crate::dist::{backup, feed as cdc};
    let shared = Arc::clone(&h.shared);
    let acked = shared.0.lock().unwrap_or_else(|p| p.into_inner()).committed.iter().map(|c| c.ts).max().unwrap_or(Timestamp::ZERO);
    let mut store = backup::MemoryStore::default();
    let taken = backup::backup(h, &mut store).and_then(|_| backup::verify(&store));
    let mut base = None;
    {
        let mut w = shared.0.lock().unwrap_or_else(|p| p.into_inner());
        match taken {
            Ok(m) => {
                if m.ts < acked {
                    w.failure.get_or_insert(format!("backup at {} misses a commit acknowledged before it began, at {acked}", m.ts));
                }
                let mut rows = BTreeMap::new();
                for i in 0..m.segments.len() {
                    let seg = store.segments.get(&(i as u64)).cloned().unwrap_or_default();
                    match backup::decode_segment(&seg) {
                        Ok(r) => rows.extend(r.into_iter().map(|(k, v)| (k, Some(v)))),
                        Err(e) => {
                            w.failure.get_or_insert(format!("backup segment: {e}"));
                        }
                    }
                }
                w.report.backups += 1;
                w.snapshots.push(("backup", m.ts, rows.clone()));
                base = Some((m.ts, rows));
            }
            Err(_) => w.report.backups_failed += 1,
        }
    }
    if feed.cursor.is_none() {
        // First the whole history, later from a backup after losing it.
        let start = if feed.log.is_empty() { Timestamp::ZERO } else { match &base { Some((t, _)) => *t, None => return } };
        feed.cursor = Some(cdc::Cursor::at(h, start).encode());
    }
    for _ in 0..64 {
        let Some(cursor) = feed.cursor.as_ref().and_then(|b| cdc::Cursor::decode(b).ok()) else { return };
        match cdc::poll(h, &cursor, Some("acct"), 2) {
            Ok(page) => {
                let page = match cdc::FeedPage::decode(&page.encode()) {
                    Ok(p) => p,
                    Err(e) => {
                        shared.0.lock().unwrap_or_else(|p| p.into_inner()).failure.get_or_insert(format!("feed page: {e}"));
                        return;
                    }
                };
                let mut w = shared.0.lock().unwrap_or_else(|p| p.into_inner());
                for c in &page.changes {
                    let k = ((c.collection.clone(), c.key.clone()), c.ts);
                    // Delivered again after a lost cursor: it must be the same change.
                    if let Some(prev) = feed.log.insert(k.clone(), c.value.clone()) {
                        if prev != c.value {
                            w.failure.get_or_insert(format!("feed delivered two values of {k:?}: {prev:?} and {:?}", c.value));
                        }
                    }
                }
                if rng.below(5) == 0 {
                    continue; // "crashed" before saving the cursor: this page comes again
                }
                feed.cursor = Some(page.cursor.encode());
                if let Some(at) = page.resolved {
                    w.report.feed_passes += 1;
                    w.snapshots.push(("changefeed", at, feed.state(at)));
                    return;
                }
            }
            Err(TxnError::HistoryLost(_)) => {
                // Start over from the backup just taken, as a consumer would.
                let mut w = shared.0.lock().unwrap_or_else(|p| p.into_inner());
                w.report.feeds_lost += 1;
                feed.cursor = None;
                feed.log.clear();
                if let Some((t, rows)) = base {
                    for (k, v) in rows {
                        feed.log.insert((k, t), v);
                    }
                    drop(w);
                    feed.cursor = Some(cdc::Cursor::at(h, t).encode());
                }
                return;
            }
            Err(TxnError::Invalid(e)) => {
                shared.0.lock().unwrap_or_else(|p| p.into_inner()).failure.get_or_insert(format!("feed: {e}"));
                return;
            }
            Err(_) => return, // faults: the cursor is still good, try at the next audit point
        }
    }
}

/// What a client thread does.
fn client_main(mut h: ClientHandle, cfg: TxnSimConfig) {
    // Wait for the first turn.
    {
        let shared = Arc::clone(&h.shared);
        let mut w = shared.0.lock().unwrap_or_else(|p| p.into_inner());
        w.slots[h.i].started = true;
        drop(yield_and_wait(&shared, h.i, &h.clock, w));
    }
    let mut rng = SweepRng::new(h.rng.next_u64());
    if cfg.doc_workload {
        doc_client(&mut h, &cfg, &mut rng);
        let shared = Arc::clone(&h.shared);
        let mut w = shared.0.lock().unwrap_or_else(|p| p.into_inner());
        w.report.one_phase += h.one_phase;
        w.slots[h.i].done = true;
        w.turn = None;
        shared.1.notify_all();
        return;
    }
    let mut feed = FeedConsumer::default();
    for n in 0..cfg.txns_per_client {
        if cfg.backup_audits && h.i == 0 && n % cfg.audit_every == cfg.audit_every - 1 {
            backup_and_feed(&mut h, &mut feed, &mut rng);
            continue;
        }
        if n % cfg.audit_every == cfg.audit_every - 1 {
            // Audit: every account in one snapshot.
            let read_all = |t: &mut txn::Txn, s: &mut dyn KvSender| -> Result<(i64, Vec<(Key, Value)>), TxnError> {
                let mut reads = Vec::new();
                let mut total = 0;
                if cfg.scan_audits {
                    let rows = t.scan(s, &crate::dist::zone::collection_span("acct"), 0)?;
                    let mut found: BTreeMap<Key, Value> = rows.into_iter().map(|(k, v)| (k, Some(v))).collect();
                    for i in 0..cfg.accounts {
                        let v = found.remove(&account(i)).flatten();
                        total += balance(&v);
                        reads.push((account(i), v));
                    }
                    if let Some((k, _)) = found.into_iter().next() {
                        return Err(TxnError::Invalid(format!("scan returned a key outside the accounts: {k:?}")));
                    }
                    return Ok((total, reads));
                }
                for i in 0..cfg.accounts {
                    let v = t.get(s, &account(i))?;
                    total += balance(&v);
                    reads.push((account(i), v));
                }
                Ok((total, reads))
            };
            let res = if cfg.audit_staleness_ns > 0 {
                // A historical snapshot: replicas serve it once it is closed.
                let at = h.now();
                let at = Timestamp { wall: at.wall.saturating_sub(cfg.audit_staleness_ns).max(1), logical: 0 };
                let mut t = txn::Txn::begin_as_of(&mut h, at);
                read_all(&mut t, &mut h).and_then(|out| t.commit(&mut h).map(|ts| (out, ts)))
            } else {
                txn::run(&mut h, read_all)
            };
            let shared = Arc::clone(&h.shared);
            let mut w = shared.0.lock().unwrap_or_else(|p| p.into_inner());
            match res {
                Ok(((total, reads), ts)) => {
                    w.audits.push((ts, total));
                    w.committed.push(Committed { ts, reads, writes: Vec::new() });
                    w.report.audits += 1;
                }
                Err(TxnError::Retry(_)) => w.report.retried += 1,
                Err(_) => w.report.unavailable += 1,
            }
            continue;
        }
        let a = rng.below(cfg.accounts);
        let b = (a + 1 + rng.below(cfg.accounts - 1)) % cfg.accounts;
        let amount = 1 + rng.below(10) as i64;
        let mut last_reads = Vec::new();
        let mut last_writes = Vec::new();
        let mut last_id = [0u8; 16];
        let res = txn::run(&mut h, |t, s| {
            last_id = t.id();
            let va = t.get(s, &account(a))?;
            let vb = t.get(s, &account(b))?;
            let (ba, bb) = (balance(&va), balance(&vb));
            last_reads = vec![(account(a), va), (account(b), vb)];
            if ba >= amount {
                t.put(account(a), bal(ba - amount));
                t.put(account(b), bal(bb + amount));
                last_writes = vec![(account(a), Some(bal(ba - amount))), (account(b), Some(bal(bb + amount)))];
            } else {
                last_writes = Vec::new();
            }
            Ok(())
        });
        let shared = Arc::clone(&h.shared);
        let mut w = shared.0.lock().unwrap_or_else(|p| p.into_inner());
        match res {
            Ok(((), ts)) => {
                w.report.committed += 1;
                w.committed.push(Committed { ts, reads: last_reads, writes: last_writes });
            }
            Err(TxnError::CommitUnknown(_)) => w.unknown.push((last_id, last_reads, last_writes)),
            Err(TxnError::Retry(e)) => {
                w.report.retried += 1;
                w.report.first_error.get_or_insert(e);
            }
            Err(TxnError::Unavailable(e)) => {
                w.report.unavailable += 1;
                w.report.first_error.get_or_insert(e);
            }
            Err(TxnError::Invalid(e) | TxnError::HistoryLost(e)) => w.failure = Some(format!("coordinator: {e}")),
        }
    }
    let shared = Arc::clone(&h.shared);
    let mut w = shared.0.lock().unwrap_or_else(|p| p.into_inner());
    w.slots[h.i].done = true;
    w.turn = None;
    shared.1.notify_all();
}

/// Runs one simulation; `Err` describes the first violation.
pub fn run(root: &Path, seed: u64, cfg: &TxnSimConfig) -> Result<TxnSimReport, String> {
    let _ = std::fs::remove_dir_all(root);
    let mut rng = SweepRng::new(seed.wrapping_mul(0xD6E8_FEB8_6659_FD93) | 1);
    let mut nodes = Vec::new();
    for i in 0..cfg.nodes {
        let dir = root.join(format!("n{}", i + 1));
        let sim = SimVfs::new(&dir, rng.next_u64()).map_err(|e| e.to_string())?;
        let mount = sim.mount();
        nodes.push(SimNode { dir, sim, _mount: mount, server: None, clock: ManualClock::new(0), skew_ns: 0 });
    }
    let n = cfg.nodes;
    let mut world = World {
        cfg: cfg.clone(),
        rng,
        now: 0,
        seq: 0,
        queue: BinaryHeap::new(),
        events: BTreeMap::new(),
        nodes,
        cut: vec![vec![false; n]; n],
        epoch: 0,
        faults_on: true,
        slots: (0..cfg.clients).map(|_| ClientSlot::default()).collect(),
        turn: None,
        next_req: 0,
        leader_hint: BTreeMap::new(),
        committed: Vec::new(),
        unknown: Vec::new(),
        audits: Vec::new(),
        snapshots: Vec::new(),
        watched: BTreeMap::new(),
        report: TxnSimReport { seed, ..TxnSimReport::default() },
        failure: None,
        processed: 0,
        trace: Vec::new(),
    };
    // Ranges over the account keys.
    let mut bounds: Vec<Vec<u8>> = (1..cfg.ranges).map(|i| document_id("acct", &format!("a{:02}", i * cfg.accounts / cfg.ranges)).unwrap_or_default().as_bytes().to_vec()).collect();
    bounds.sort();
    let mut spans = Vec::new();
    let mut start = Vec::new();
    for b in bounds {
        spans.push(KeySpan::new(start.clone(), Some(b.clone())));
        start = b;
    }
    spans.push(KeySpan::new(start, None));
    for i in 0..n {
        let s = world.open_server(i, Some(&spans)).map_err(|e| e.to_string())?;
        world.nodes[i].server = Some(s);
        let d = world.rng.below(cfg.tick_us as usize) as u64;
        world.schedule(d, Ev::Tick { node: i });
    }
    // Initial balances: written directly on every replica before the clients start (as if
    // loaded by a bulk import at timestamp 1).
    for node in &world.nodes {
        let srv = node.server.as_ref().ok_or("no server")?;
        for i in 0..cfg.accounts {
            let k = account(i);
            let m = MvccKey { versions: vec![(Timestamp::new(1, 0), Some(bal(cfg.initial_balance)))], intent: None, committed_by: Vec::new(), trimmed_below: Timestamp::ZERO };
            let id = document_id(&k.0, &k.1).map_err(|e| e.to_string())?;
            srv.driver.engine().install_exact(id, &m.to_record(&k.0, &k.1), storage_types::Version(1), 0).map_err(|e| e.to_string())?;
        }
        // Not in any log, so nothing would replay it after a power cut.
        srv.driver.engine().sync_wal().map_err(|e| e.to_string())?;
    }
    world.schedule(cfg.fault_every, Ev::Fault);

    if cfg.backup_audits {
        // Writers' clocks lead the backup client's: their commits are ahead of what it can see.
        for slot in world.slots.iter_mut().skip(1) {
            slot.skew_ns = cfg.max_offset_ns / 2;
        }
    }
    let shared: Shared = Arc::new((Mutex::new(world), Condvar::new()));
    let mut threads = Vec::new();
    for i in 0..cfg.clients {
        let clock = ManualClock::new(0);
        let h = ClientHandle {
            i,
            shared: Arc::clone(&shared),
            hlc: Hlc::new(Arc::new(clock.clone()), std::time::Duration::from_nanos(cfg.max_offset_ns)).with_self_fencing(false),
            clock,
            rng: SweepRng::new(seed ^ ((i as u64 + 1) * 0x9E37)),
            plain: std::collections::BTreeSet::new(),
            one_phase: 0,
        };
        let c = cfg.clone();
        threads.push(std::thread::spawn(move || client_main(h, c)));
    }
    // Let every client reach its first wait.
    {
        let (m, cv) = &*shared;
        let mut w = m.lock().unwrap_or_else(|p| p.into_inner());
        while w.slots.iter().any(|s| !s.started) {
            w = cv.wait(w).unwrap_or_else(|p| p.into_inner());
        }
    }
    // Scheduler.
    let result = (|| -> Result<(), String> {
        let (m, cv) = &*shared;
        let mut wind_down: Option<u64> = None;
        loop {
            let mut w = m.lock().unwrap_or_else(|p| p.into_inner());
            while w.turn.is_some() {
                w = cv.wait(w).unwrap_or_else(|p| p.into_inner());
            }
            if let Some(f) = w.failure.take() {
                return Err(f);
            }
            if w.slots.iter().all(|s| s.done) {
                if wind_down.is_none() {
                    wind_down = Some(w.now);
                    w.faults_on = false;
                    w.cut.iter_mut().for_each(|r| r.iter_mut().for_each(|c| *c = false));
                    for i in 0..w.nodes.len() {
                        if w.nodes[i].server.is_none() {
                            w.schedule(1_000, Ev::Restart { node: i });
                        }
                    }
                }
                if w.now - wind_down.unwrap_or(0) > 3_000_000 {
                    return Ok(());
                }
            }
            let runnable = w.runnable();
            if !runnable.is_empty() && w.turn.is_none() {
                let pick = runnable[w.rng.below(runnable.len())];
                w.turn = Some(pick);
                cv.notify_all();
                continue;
            }
            let Some(Reverse((t, seq))) = w.queue.pop() else { return Err("event queue ran dry".into()) };
            let Some(ev) = w.events.remove(&seq) else { continue };
            w.now = t;
            w.processed += 1;
            if w.processed > w.cfg.max_events {
                return Err("event budget exhausted (livelock?)".into());
            }
            w.handle(ev);
            if let Ok(prefix) = std::env::var("STORINE_SIM_WATCH") {
                w.watch(&prefix);
            }
        }
    })();
    {
        // Release every client (they finish on their own once their last call returns).
        let (m, cv) = &*shared;
        let mut w = m.lock().unwrap_or_else(|p| p.into_inner());
        if result.is_err() {
            // Wake waiting clients with failures so their threads end.
            for s in &mut w.slots {
                s.done = true;
            }
        }
        cv.notify_all();
        drop(w);
    }
    if result.is_ok() {
        for t in threads {
            let _ = t.join();
        }
    } else {
        // Clients may be parked forever on a failed run; detach them.
        drop(threads);
    }
    let mut w = shared.0.lock().unwrap_or_else(|p| p.into_inner());
    let out = result.and_then(|()| check(&mut w));
    for n in &mut w.nodes {
        n.server = None;
    }
    let report = w.report.clone();
    drop(w);
    let _ = std::fs::remove_dir_all(root);
    out.map(|()| report)
}

/// Settles unknown outcomes, then checks audits and serializability.
fn check(w: &mut World) -> Result<(), String> {
    w.report.events = w.processed;
    w.report.meta_lookups = w.slots.iter().map(|s| s.pub_lookups).sum();
    w.report.misroutes = w.slots.iter().map(|s| s.misroutes).sum();
    w.report.end_time = w.now;
    w.report.follower_reads += w.nodes.iter().filter_map(|n| n.server.as_ref()).map(NodeServer::follower_reads).sum::<u64>();
    w.nodes.iter().find_map(|n| n.server.as_ref()).ok_or("no live node")?;
    // Each key is read from the leaseholder of its range (replicas are not on every node).
    let nodes: Vec<&NodeServer> = w.nodes.iter().filter_map(|n| n.server.as_ref()).collect();
    let get = |id: storage_types::InlineId| -> Option<DocumentRecord> {
        let hosts = |lead: bool| {
            nodes.iter().find(|s| s.driver.ranges().into_iter().any(|r| s.driver.replica(r).is_some_and(|x| !x.is_frozen() && x.span().contains_id(id)) && (!lead || s.driver.is_leader(r))))
        };
        let srv = hosts(true).or_else(|| hosts(false))?;
        srv.driver.engine().get_owned(id).ok().map(|(_, r)| r)
    };
    // Unknown commits: the record decides (staging records are decided by their intents).
    let unknown = std::mem::take(&mut w.unknown);
    for (id, reads, writes) in unknown {
        let rid = document_id(TXN_COLLECTION, &crate::dist::mvcc::record_key(&id)).map_err(|e| e.to_string())?;
        let rec = get(rid).map(|r| TxnRecord::from_record(id, &r)).transpose().map_err(|e| e.to_string())?;
        let committed_at = match rec {
            Some(r) if r.status == TxnStatus::Committed => Some(r.ts),
            Some(TxnRecord { status: TxnStatus::Staging { in_flight }, ts, .. }) => {
                // Committed iff every in-flight key holds its intent at or below the staged
                // timestamp (or the intent was already resolved into a version at it).
                let all = in_flight.iter().all(|k| {
                    let kid = document_id(&k.0, &k.1).unwrap_or_default();
                    let mk = get(kid).and_then(|r| MvccKey::from_record(Some(&r)).ok()).unwrap_or_default();
                    mk.intent.as_ref().is_some_and(|i| i.txn.id == id && i.ts <= ts) || mk.versions.iter().any(|(t, _)| *t == ts)
                });
                all.then_some(ts)
            }
            _ => None,
        };
        if let Some(ts) = committed_at {
            w.report.unknown_then_settled += 1;
            w.committed.push(Committed { ts, reads, writes });
        }
    }
    // State-machine safety: replicas of a range that applied the same index hold the same
    // records in its span (byte for byte, versions aside: those are node-local).
    {
        let mut by_range: BTreeMap<RangeId, Vec<(usize, u64, KeySpan)>> = BTreeMap::new();
        for (i, n) in w.nodes.iter().enumerate() {
            let Some(srv) = n.server.as_ref() else { continue };
            for r in srv.driver.ranges() {
                if let Some(x) = srv.driver.replica(r) {
                    by_range.entry(r).or_default().push((i, x.last_applied(), x.span()));
                }
            }
        }
        let state = |i: usize, span: &KeySpan| -> Vec<(String, String, BTreeMap<String, PropertyValue>)> {
            let Some(srv) = w.nodes[i].server.as_ref() else { return Vec::new() };
            let e = srv.driver.engine();
            e.ids_in_span(&span.start, span.end.as_deref(), usize::MAX).into_iter().filter_map(|id| e.get_owned(id).ok()).map(|(_, r)| (r.collection, r.key, r.fields)).collect()
        };
        for (r, reps) in &by_range {
            for (a, b) in reps.iter().zip(reps.iter().skip(1)) {
                if a.1 == b.1 && a.2 == b.2 && state(a.0, &a.2) != state(b.0, &b.2) {
                    return Err(format!("state-machine safety: r{r} differs between n{} and n{} at applied index {}", a.0 + 1, b.0 + 1, a.1));
                }
            }
        }
    }
    // Audits: the total is conserved in every snapshot.
    let total = w.cfg.initial_balance * w.cfg.accounts as i64;
    for (ts, t) in &w.audits {
        if *t != total {
            let mut detail = String::new();
            if std::env::var("STORINE_SIM_DEBUG").is_ok() {
                let mut txns = w.committed.clone();
                txns.sort_by_key(|c| c.ts);
                for c in &txns {
                    let fmt = |kv: &[(Key, Value)]| kv.iter().map(|(k, v)| format!("{}={}", k.1, balance(v))).collect::<Vec<_>>().join(" ");
                    detail.push_str(&format!("\n  {} reads[{}] writes[{}]", c.ts, fmt(&c.reads), fmt(&c.writes)));
                }
            }
            if let Ok(k) = std::env::var("STORINE_SIM_KEY") {
                for line in w.trace.iter().filter(|l| l.contains(&k)) {
                    detail.push_str(&format!("\n# {line}"));
                }
            }
            return Err(format!("audit at {ts} saw total {t}, expected {total}{detail}"));
        }
    }
    // Serializability: replay in commit-timestamp order. Transactions at one timestamp go in
    // any order their reads allow: a read at `ts` sees writes at `ts` (a later write would have
    // been pushed above the read), so a writer precedes the readers of its keys.
    let mut model: BTreeMap<Key, Value> = (0..w.cfg.accounts).map(|i| (account(i), Some(bal(w.cfg.initial_balance)))).collect();
    let mut txns = w.committed.clone();
    txns.sort_by_key(|c| c.ts);
    let mut start = 0;
    while start < txns.len() {
        let end = start + txns[start..].iter().take_while(|c| c.ts == txns[start].ts).count();
        let mut done = start;
        while done < end {
            let fits = |c: &Committed| c.reads.iter().all(|(k, v)| &model.get(k).cloned().flatten() == v);
            let Some(next) = (done..end).find(|&i| fits(&txns[i])) else { break };
            txns.swap(done, next);
            for (k, v) in &txns[done].writes {
                model.insert(k.clone(), v.clone());
            }
            done += 1;
        }
        if done == end {
            start = end;
            continue;
        }
        let c = &txns[done];
        for (k, v) in &c.reads {
            let have = model.get(k).cloned().flatten();
            if &have != v {
                let mut detail = String::new();
                if std::env::var("STORINE_SIM_DEBUG").is_ok() {
                    for c in &txns {
                        let fmt = |kv: &[(Key, Value)]| kv.iter().map(|(k, v)| format!("{}={}", k.1, balance(v))).collect::<Vec<_>>().join(" ");
                        detail.push_str(&format!("\n  {} reads[{}] writes[{}]", c.ts, fmt(&c.reads), fmt(&c.writes)));
                    }
                    if let Ok(k) = std::env::var("STORINE_SIM_KEY") {
                        for line in w.trace.iter().filter(|l| l.contains(&k)) {
                            detail.push_str(&format!("\n# {line}"));
                        }
                    }
                }
                return Err(format!("serializability: a transaction committed at {} read {k:?} = {v:?}, but the serial order gives {have:?}{detail}", c.ts));
            }
        }
        return Err(format!("serializability: no serial order of the transactions committed at {}", c.ts));
    }
    // Every backup and resolved feed state equals the serial order at its timestamp.
    for (what, at, rows) in &w.snapshots {
        let mut at_ts: BTreeMap<Key, Value> = (0..w.cfg.accounts).map(|i| (account(i), Some(bal(w.cfg.initial_balance)))).collect();
        for c in txns.iter().filter(|c| c.ts <= *at) {
            for (k, v) in &c.writes {
                at_ts.insert(k.clone(), v.clone());
            }
        }
        if rows != &at_ts {
            let fmt = |m: &BTreeMap<Key, Value>| m.iter().map(|(k, v)| format!("{}={}", k.1, balance(v))).collect::<Vec<_>>().join(" ");
            return Err(format!("{what} at {at} holds [{}], the serial order gives [{}]", fmt(rows), fmt(&at_ts)));
        }
    }
    // The final state equals the model.
    for i in 0..w.cfg.accounts {
        let k = account(i);
        let id = document_id(&k.0, &k.1).map_err(|e| e.to_string())?;
        let mk = get(id).and_then(|r| MvccKey::from_record(Some(&r)).ok()).unwrap_or_default();
        let committed = mk.versions.first().and_then(|(_, v)| v.clone());
        // An unresolved intent of a committed transaction counts as its value.
        let want = model.get(&k).cloned().flatten();
        if committed != want && mk.intent.is_none() {
            if std::env::var("STORINE_SIM_DEBUG").is_ok() {
                for s in &nodes {
                    for r in s.driver.ranges() {
                        let Some(x) = s.driver.replica(r) else { continue };
                        if x.span().contains_id(id) {
                            eprintln!(
                                "n{} r{r} leader {} frozen {} term {} applied {} has {}",
                                s.driver.id(),
                                s.driver.is_leader(r),
                                x.is_frozen(),
                                x.current_term(),
                                x.last_applied(),
                                s.driver.engine().get_owned(id).is_ok()
                            );
                        }
                    }
                }
            }
            return Err(format!("final state: {k:?} holds {committed:?}, serial order gives {want:?}"));
        }
    }
    Ok(())
}

