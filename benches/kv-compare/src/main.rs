//! Storine against LMDB (`heed`) and RocksDB on the same key-value workloads.
//!
//! ```text
//! cargo run --release -- <dir> [records] [threads]
//! ```
//!
//! Every system stores the same keys (16-byte strings) and the same value bytes: LMDB and
//! RocksDB get Storine's own encoded frame for the record, prebuilt, so they skip
//! serialization entirely (their best case); Storine encodes inside `put`, as its API does.
//! Reads fetch the value and read one 8-byte field from it without copying (`with_ref`,
//! LMDB's `&[u8]`, RocksDB's pinned slice).
//!
//! Systems are compared within a durability tier only:
//! - **no fsync**: Storine `AsyncMemoryFirst` (a process crash can lose the last flush
//!   interval), RocksDB without its WAL (the same guarantee), RocksDB with its WAL (survives a
//!   process crash) and LMDB `NO_SYNC` (survives a process crash).
//! - **fsync per commit**: Storine `GroupCommit`, RocksDB `sync = true`, LMDB's default.
//!
//! Each timed operation is timed on its own (`Instant`, the same overhead for every system);
//! throughput is operations over the whole run's wall time.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use storage_codec::{ArchiveBuilder, ArchivedBytes, RecordView, ZeroCopyRecord};
use storage_core::{AgentObjectRecord, EngineConfig, StorageEngine, StorageLayoutMode};
use storage_types::{InlineId, RecordFlags, Result as SResult, Version};
use storage_wal::SyncPolicy;

const VALUE_BODY: usize = 100;
/// `KvRecord` value bytes: sized so its frame matches `AgentObjectRecord`'s (288 bytes).
const KV_VALUE: usize = 180;

/// A plain key-value record: what LMDB and RocksDB store (no secondary indexes, no embedding).
#[derive(Clone, Debug)]
pub struct KvRecord {
    id: InlineId,
    field: u64,
    value: Vec<u8>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct ArchivedKvRecord {
    id: InlineId,
    field: u64,
    value: ArchivedBytes,
}

storage_codec::impl_archive_pod!(ArchivedKvRecord { id: InlineId, field: u64, value: ArchivedBytes });

impl ZeroCopyRecord for KvRecord {
    type Archived = ArchivedKvRecord;
    fn archive(&self, b: &mut ArchiveBuilder<'_>) -> ArchivedKvRecord {
        let value = b.write_bytes(&self.value);
        ArchivedKvRecord { id: self.id, field: self.field, value }
    }
    fn validate_archived(root: &ArchivedKvRecord, payload: &[u8]) -> SResult<()> {
        InlineId::from_raw_bytes(root.id.to_raw_bytes())?;
        root.value.validate(payload)
    }
    fn unarchive(root: &ArchivedKvRecord, payload: &[u8]) -> SResult<Self> {
        Ok(Self { id: root.id, field: root.field, value: root.value.as_slice(payload).to_vec() })
    }
}

impl storage_index::Indexable for KvRecord {
    const INDEXED: bool = false;
    fn extract_archived_snapshot(_root: &ArchivedKvRecord, _payload: &[u8]) -> Option<storage_index::IndexSnapshot> {
        Some(storage_index::IndexSnapshot::new())
    }
}

/// A record type the Storine side of the benchmark can build and read one field from.
trait BenchRecord: ZeroCopyRecord + storage_index::Indexable + Clone + Send + Sync + 'static {
    fn make(i: usize, salt: i64) -> Self;
    /// `self` (record `i` as loaded) with its field set from `salt`: a clone and one store,
    /// the same work the LMDB and RocksDB puts do on their prebuilt value.
    fn salted(&self, i: usize, salt: i64) -> Self;
    fn read(view: RecordView<'_, Self>) -> u64;
}

impl BenchRecord for KvRecord {
    fn make(i: usize, salt: i64) -> Self {
        Self { id: InlineId::new(&key_str(i)).expect("key"), field: (i as i64 + salt) as u64, value: vec![(i % 251) as u8; KV_VALUE] }
    }
    fn salted(&self, i: usize, salt: i64) -> Self {
        let mut r = self.clone();
        r.field = (i as i64 + salt) as u64;
        r
    }
    fn read(view: RecordView<'_, Self>) -> u64 {
        view.field
    }
}

impl BenchRecord for AgentObjectRecord {
    fn make(i: usize, salt: i64) -> Self {
        record(i, salt)
    }
    fn salted(&self, i: usize, salt: i64) -> Self {
        let mut r = self.clone();
        r.score = i as i64 + salt;
        r
    }
    fn read(view: RecordView<'_, Self>) -> u64 {
        view.score as u64
    }
}

fn key_str(i: usize) -> String {
    format!("k{i:015}")
}

fn record(i: usize, salt: i64) -> AgentObjectRecord {
    let id = InlineId::new(&key_str(i)).expect("key");
    let mut r = AgentObjectRecord::new(id, "title", 1_700_000_000 + i as i64, i as i64 + salt);
    r.body = vec![(i % 251) as u8; VALUE_BODY];
    r
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// One system under test. `get` returns a field read from the stored value (so the read is not
/// optimized away).
trait Kv: Sync {
    fn name(&self) -> String;
    fn put(&self, i: usize, salt: i64);
    fn get(&self, i: usize) -> u64;
    /// After a bulk load: make the data the read benchmarks see the steady state (flush,
    /// compact).
    fn settle(&self);
}

// ---------- Storine ----------

struct Storine<T: BenchRecord> {
    engine: StorageEngine<T>,
    ids: Vec<InlineId>,
    records: Vec<T>,
    label: &'static str,
}

impl<T: BenchRecord> Storine<T> {
    fn open(dir: &Path, n: usize, policy: SyncPolicy, label: &'static str) -> Self {
        let cfg = EngineConfig {
            layout_mode: StorageLayoutMode::AdaptiveArena { arena_threshold_bytes: 64 * 1024, segment_capacity: 64 * 1024 * 1024 },
            wal_sync_policy: policy,
            // `KV_HOT_DIRTY_MB`: the unflushed-data cap (experiments only; default 64 MiB).
            max_hot_dirty_bytes: std::env::var("KV_HOT_DIRTY_MB").ok().and_then(|v| v.parse::<usize>().ok()).map_or(64 << 20, |m| m << 20),
            ..EngineConfig::default()
        };
        let engine = StorageEngine::open(dir, cfg).expect("open storine");
        Self { engine, ids: (0..n).map(|i| InlineId::new(&key_str(i)).unwrap()).collect(), records: (0..n).map(|i| T::make(i, 0)).collect(), label }
    }
}

impl<T: BenchRecord> Kv for Storine<T> {
    fn name(&self) -> String {
        self.label.into()
    }
    fn put(&self, i: usize, salt: i64) {
        if salt == 0 {
            self.engine.put(self.ids[i], &self.records[i]).expect("put");
        } else {
            self.engine.put(self.ids[i], &self.records[i].salted(i, salt)).expect("put");
        }
    }
    fn get(&self, i: usize) -> u64 {
        self.engine.with_ref(self.ids[i], T::read).expect("get")
    }
    fn settle(&self) {
        self.engine.flush_all().expect("flush");
    }
}

// ---------- shared value bytes for LMDB / RocksDB ----------

struct Values {
    keys: Vec<[u8; 16]>,
    vals: Vec<Vec<u8>>,
}

impl Values {
    fn new(n: usize) -> Self {
        let keys = (0..n).map(|i| key_str(i).as_bytes().try_into().unwrap()).collect();
        let vals = (0..n)
            .map(|i| {
                storage_codec::RecordCodec::encode_aligned(&KvRecord::make(i, 0), Version(1), 0, RecordFlags::NONE, 0).as_slice().to_vec()
            })
            .collect();
        Self { keys, vals }
    }
}

/// `KvRecord::field` inside the frame: 64-byte header, then the 24-byte id.
const FIELD_AT: usize = 64 + 24;

fn field(v: &[u8]) -> u64 {
    u64::from_le_bytes(v[FIELD_AT..FIELD_AT + 8].try_into().unwrap())
}

// ---------- LMDB ----------

struct Lmdb {
    env: heed::Env,
    db: heed::Database<heed::types::Bytes, heed::types::Bytes>,
    v: Arc<Values>,
    label: &'static str,
}

impl Lmdb {
    fn open(dir: &Path, v: Arc<Values>, nosync: bool, label: &'static str) -> Self {
        std::fs::create_dir_all(dir).unwrap();
        let mut opts = heed::EnvOpenOptions::new();
        opts.map_size(16 << 30).max_dbs(1);
        if nosync {
            // SAFETY: flags only change durability (no fsync); the benchmark owns the directory.
            unsafe { opts.flags(heed::EnvFlags::NO_SYNC | heed::EnvFlags::NO_META_SYNC) };
        }
        // SAFETY: one environment per directory, opened once.
        let env = unsafe { opts.open(dir) }.expect("open lmdb");
        let mut w = env.write_txn().unwrap();
        let db = env.create_database(&mut w, None).unwrap();
        w.commit().unwrap();
        Self { env, db, v, label }
    }
}

impl Lmdb {
    /// Loads every record in transactions of 10,000 (an environment cannot be reopened with
    /// other flags in the same process, so the durable one is loaded this way).
    fn bulk_load(&self) {
        for chunk in (0..self.v.keys.len()).collect::<Vec<_>>().chunks(10_000) {
            let mut w = self.env.write_txn().unwrap();
            for &i in chunk {
                self.db.put(&mut w, &self.v.keys[i], &self.v.vals[i]).unwrap();
            }
            w.commit().unwrap();
        }
    }
}

impl Kv for Lmdb {
    fn name(&self) -> String {
        self.label.into()
    }
    fn put(&self, i: usize, salt: i64) {
        let mut w = self.env.write_txn().unwrap();
        if salt == 0 {
            self.db.put(&mut w, &self.v.keys[i], &self.v.vals[i]).unwrap();
        } else {
            let mut val = self.v.vals[i].clone();
            val[FIELD_AT..FIELD_AT + 8].copy_from_slice(&((i as i64 + salt) as u64).to_le_bytes());
            self.db.put(&mut w, &self.v.keys[i], &val).unwrap();
        }
        w.commit().unwrap();
    }
    fn get(&self, i: usize) -> u64 {
        // One read transaction per get, as a point lookup service would do.
        let r = self.env.read_txn().unwrap();
        field(self.db.get(&r, &self.v.keys[i]).unwrap().expect("present"))
    }
    fn settle(&self) {
        self.env.force_sync().unwrap();
    }
}

// ---------- RocksDB ----------

#[cfg(feature = "rocks")]
struct Rocks {
    db: rocksdb::DB,
    wo: rocksdb::WriteOptions,
    v: Arc<Values>,
    label: &'static str,
}

#[cfg(feature = "rocks")]
impl Rocks {
    fn open(dir: &Path, v: Arc<Values>, wal: bool, sync: bool, label: &'static str) -> Self {
        Self::open_with(dir, v, wal, sync, false, label)
    }

    /// `recycle`: reuse old WAL files in place (`recycle_log_file_num`), RocksDB's own way to
    /// keep `fdatasync` from committing file growth, comparable to Storine's preallocation.
    fn open_with(dir: &Path, v: Arc<Values>, wal: bool, sync: bool, recycle: bool, label: &'static str) -> Self {
        let mut o = rocksdb::Options::default();
        o.create_if_missing(true);
        if recycle {
            o.set_recycle_log_file_num(4);
        }
        o.set_compression_type(rocksdb::DBCompressionType::None);
        o.increase_parallelism(std::thread::available_parallelism().map_or(4, |n| n.get() as i32));
        let mut b = rocksdb::BlockBasedOptions::default();
        let cache = rocksdb::Cache::new_lru_cache(2 << 30);
        b.set_block_cache(&cache);
        b.set_bloom_filter(10.0, false);
        b.set_cache_index_and_filter_blocks(true);
        o.set_block_based_table_factory(&b);
        let db = rocksdb::DB::open(&o, dir).expect("open rocksdb");
        let mut wo = rocksdb::WriteOptions::default();
        wo.disable_wal(!wal);
        wo.set_sync(sync);
        Self { db, wo, v, label }
    }
}

#[cfg(feature = "rocks")]
impl Kv for Rocks {
    fn name(&self) -> String {
        self.label.into()
    }
    fn put(&self, i: usize, salt: i64) {
        if salt == 0 {
            self.db.put_opt(self.v.keys[i], &self.v.vals[i], &self.wo).unwrap();
        } else {
            let mut val = self.v.vals[i].clone();
            val[FIELD_AT..FIELD_AT + 8].copy_from_slice(&((i as i64 + salt) as u64).to_le_bytes());
            self.db.put_opt(self.v.keys[i], &val, &self.wo).unwrap();
        }
    }
    fn get(&self, i: usize) -> u64 {
        field(&self.db.get_pinned(self.v.keys[i]).unwrap().expect("present"))
    }
    fn settle(&self) {
        self.db.flush().unwrap();
        self.db.compact_range::<&[u8], &[u8]>(None, None);
    }
}

// ---------- measurement ----------

struct Result {
    ops: usize,
    wall: Duration,
    lat: Vec<u32>,
}

impl Result {
    fn line(&self, system: &str, bench: &str) -> String {
        let mut l = self.lat.clone();
        l.sort_unstable();
        let q = |p: f64| l.get(((l.len() as f64 * p) as usize).min(l.len().saturating_sub(1))).copied().unwrap_or(0);
        format!(
            "{bench:<22} {system:<24} {:>12.0} ops/s   p50 {:>8} ns   p99 {:>9} ns   p99.9 {:>10} ns",
            self.ops as f64 / self.wall.as_secs_f64(),
            q(0.50),
            q(0.99),
            q(0.999)
        )
    }
}

/// Runs `per_thread` operations on each of `threads` threads; `op(thread, i, rng)`.
fn run(threads: usize, per_thread: usize, op: &(dyn Fn(usize, &mut Rng) + Sync)) -> Result {
    let start = Instant::now();
    let lats: Vec<Vec<u32>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..threads)
            .map(|t| {
                s.spawn(move || {
                    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ ((t as u64 + 1) * 0x1234_5678_9ABC));
                    let mut lat = Vec::with_capacity(per_thread);
                    for _ in 0..per_thread {
                        let t0 = Instant::now();
                        op(t, &mut rng);
                        lat.push(t0.elapsed().as_nanos().min(u128::from(u32::MAX)) as u32);
                    }
                    lat
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    Result { ops: threads * per_thread, wall: start.elapsed(), lat: lats.concat() }
}

fn bench_fast(kv: &dyn Kv, n: usize, threads: usize, out: &mut Vec<String>) {
    let name = kv.name();
    // fillrandom: every key once, in a random order.
    let mut order: Vec<usize> = (0..n).collect();
    let mut rng = Rng(42);
    for i in (1..n).rev() {
        order.swap(i, (rng.next() % (i as u64 + 1)) as usize);
    }
    let order = &order;
    let next = std::sync::atomic::AtomicUsize::new(0);
    let r = run(1, n, &|_, _| {
        let k = order[next.fetch_add(1, std::sync::atomic::Ordering::Relaxed)];
        kv.put(k, 0);
    });
    out.push(r.line(&name, "fillrandom"));
    let t = Instant::now();
    kv.settle();
    out.push(format!("{:<22} {name:<24} {:>12.3} s", "settle (flush)", t.elapsed().as_secs_f64()));

    // `KV_WRITES_ONLY=1` skips the reads (for profiling the write path).
    if std::env::var_os("KV_WRITES_ONLY").is_none() {
        let r = run(1, 2 * n, &|_, rng| {
            std::hint::black_box(kv.get((rng.next() % n as u64) as usize));
        });
        out.push(r.line(&name, "readrandom 1 thread"));
        let r = run(threads, 2 * n / threads.max(1), &|_, rng| {
            std::hint::black_box(kv.get((rng.next() % n as u64) as usize));
        });
        out.push(r.line(&name, &format!("readrandom {threads} threads")));
    }
    let r = run(1, n / 2, &|_, rng| {
        kv.put((rng.next() % n as u64) as usize, 7);
    });
    out.push(r.line(&name, "overwrite 1 thread"));
    let r = run(threads, n / 2 / threads.max(1), &|_, rng| {
        kv.put((rng.next() % n as u64) as usize, 9);
    });
    out.push(r.line(&name, &format!("overwrite {threads} threads")));
}

fn bench_durable(kv: &dyn Kv, n: usize, threads: usize, ops: usize, out: &mut Vec<String>) {
    let name = kv.name();
    let r = run(1, ops, &|_, rng| kv.put((rng.next() % n as u64) as usize, 11));
    out.push(r.line(&name, "durable put 1 thread"));
    let r = run(threads, ops, &|_, rng| kv.put((rng.next() % n as u64) as usize, 13));
    out.push(r.line(&name, &format!("durable put {threads} thr")));
}

fn fresh(root: &Path, name: &str) -> PathBuf {
    let d = root.join(name);
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn main() {
    let mut args = std::env::args().skip(1);
    let root = PathBuf::from(args.next().unwrap_or_else(|| "/tmp/kvcmp".into()));
    let n: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(1_000_000);
    let threads: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(4);
    let only = args.next().unwrap_or_default();
    let durable_ops = 2_000;
    let values = Arc::new(Values::new(n));
    println!("records {n}, value {} bytes, threads {threads}", values.vals[0].len());
    let mut out = Vec::new();
    let want = |s: &str| only.is_empty() || only.split(',').any(|o| s.starts_with(o));

    // No-fsync tier.
    if want("storine") {
        let s = Storine::<KvRecord>::open(&fresh(&root, "storine-mem"), n, SyncPolicy::AsyncMemoryFirst, "storine memory-first");
        bench_fast(&s, n, threads, &mut out);
    }
    // The same with a record type that keeps two secondary indexes (timestamp range, tags),
    // work the others never do: shown apart, not compared.
    if want("indexed") {
        let s = Storine::<AgentObjectRecord>::open(&fresh(&root, "storine-indexed"), n, SyncPolicy::AsyncMemoryFirst, "storine indexed");
        bench_fast(&s, n, threads, &mut out);
    }
    #[cfg(feature = "rocks")]
    if want("rocks") {
        let r = Rocks::open(&fresh(&root, "rocks-nowal"), Arc::clone(&values), false, false, "rocksdb no WAL");
        bench_fast(&r, n, threads, &mut out);
        drop(r);
        let r = Rocks::open(&fresh(&root, "rocks-wal"), Arc::clone(&values), true, false, "rocksdb WAL no sync");
        bench_fast(&r, n, threads, &mut out);
    }
    if want("lmdb") {
        let l = Lmdb::open(&fresh(&root, "lmdb-nosync"), Arc::clone(&values), true, "lmdb NO_SYNC");
        bench_fast(&l, n, threads, &mut out);
    }
    for l in &out {
        println!("{l}");
    }
    out.clear();

    // fsync-per-commit tier: load without fsync, close, reopen with fsync per commit on the same
    // data, then time durable puts.
    let load = |kv: &dyn Kv| {
        for i in 0..n {
            kv.put(i, 0);
        }
        kv.settle();
    };
    if want("storine") {
        let d = fresh(&root, "storine-durable");
        load(&Storine::<KvRecord>::open(&d, n, SyncPolicy::AsyncMemoryFirst, "load"));
        let s = Storine::<KvRecord>::open(&d, n, SyncPolicy::GroupCommit { max_batch_entries: 64 }, "storine group commit");
        bench_durable(&s, n, threads, durable_ops, &mut out);
    }
    #[cfg(feature = "rocks")]
    if want("rocks") {
        let d = fresh(&root, "rocks-sync");
        load(&Rocks::open(&d, Arc::clone(&values), true, false, "load"));
        let r = Rocks::open(&d, Arc::clone(&values), true, true, "rocksdb sync");
        bench_durable(&r, n, threads, durable_ops, &mut out);
        drop(r);
        let d = fresh(&root, "rocks-sync-recycle");
        load(&Rocks::open_with(&d, Arc::clone(&values), true, false, true, "load"));
        let r = Rocks::open_with(&d, Arc::clone(&values), true, true, true, "rocksdb sync recycle");
        bench_durable(&r, n, threads, durable_ops, &mut out);
    }
    if want("lmdb") {
        let l = Lmdb::open(&fresh(&root, "lmdb-sync"), Arc::clone(&values), false, "lmdb sync");
        l.bulk_load();
        bench_durable(&l, n, threads, durable_ops, &mut out);
    }
    for l in &out {
        println!("{l}");
    }
}
