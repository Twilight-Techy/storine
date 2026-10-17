//! Core `StorageEngine<T>` implementation (`TASK-013`..`TASK-027`).

use std::collections::VecDeque;
use std::fs;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use storage_codec::{
    AlignedBytes, RecordCodec, RecordView, ZeroCopyRecord,
};
use storage_graph::{DanglingEdge, OnDelete};
use storage_index::{
    append_primary_checkpoint_delta, cosine_similarity_f32, delta_checkpoint_path,
    load_primary_checkpoint, save_primary_checkpoint, BitmapTagIndex, CompressedBitmap,
    DiskLocation, HnswConfig, HnswVectorIndex, IndexDelta, IndexSnapshot, Indexable,
    NumericRangeIndex, PrimaryEntry, PrimaryIndex, VectorTopKHit,
};
use storage_mmap::{
    atomic_write_replace, cleanup_temp_files, read_ping_pong_active, write_ping_pong_slot,
    ArenaManager, DirectoryLock, MmapGuard, MmapPool, MmapPoolConfig, MmapRegion, PathResolver,
    DEFAULT_SEGMENT_CAPACITY,
};
use storage_types::{
    CachePadded, FastHashMap, FastHashSet, InlineId, RecordFlags, RecordHeader, RecordOrdinal,
    Result, StorageError, Version, HEADER_SIZE,
};
use storage_wal::{GroupCommitWal, SyncPolicy, WalOpKind};

use crate::migration::SchemaRegistry;
use crate::mvcc::{MvccManager, ReadSnapshot};
use crate::query::QueryBuilder;
use crate::write_queue::{MultiRecordLockGuard, StripedLockTable};
use storage_wal::WalEntry;

/// Physical on-disk storage layout strategy (`F-CORE-04`, `F-CORE-05`, `F-CORE-06`, `F-CONC-05`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageLayoutMode {
    /// Each record is stored in its own sharded `.rec` file (with optional parent folder grouping)
    /// and updated via atomic `.tmp` + rename replacement.
    DedicatedFiles,
    /// Each record file pre-allocates 2 slots (`[Slot 0 | Slot 1]`) of `slot_stride` bytes and alternates
    /// between them without temporary files or directory rename syscalls.
    PingPongFiles { slot_stride: u32 },
    /// Small records (`<= arena_threshold_bytes`) pack into 64-byte-aligned Slotted Arena Segments
    /// (`seg_XXXXX.dat`), while larger records use dedicated sharded `.rec` files.
    AdaptiveArena {
        arena_threshold_bytes: usize,
        segment_capacity: usize,
    },
}

impl Default for StorageLayoutMode {
    fn default() -> Self {
        Self::DedicatedFiles
    }
}

/// Configuration for `StorageEngine<T>`.
#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub collection_name: String,
    pub layout_mode: StorageLayoutMode,
    pub mmap_pool: MmapPoolConfig,
    pub wal_sync_policy: SyncPolicy,
    pub enable_wal: bool,
    /// When `true`, mutations return as soon as `HotDirtyArc` and WAL are updated, and a
    /// background worker coalesces and flushes dirty records to `.rec`/`.seg` files asynchronously.
    pub background_flush: bool,
    /// Interval at which the background flusher wakes to drain dirty records if not notified early.
    pub flush_interval_ms: u64,
    /// Hard memory budget for unflushed `HotDirtyArc` buffers in RAM (default: 64 MiB).
    /// When exceeded under heavy multi-user write bursts, writers apply synchronous flush
    /// backpressure so in-flight dirty memory never bloats unboundedly.
    pub max_hot_dirty_bytes: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            collection_name: "default".to_string(),
            layout_mode: StorageLayoutMode::DedicatedFiles,
            mmap_pool: MmapPoolConfig::default(),
            wal_sync_policy: SyncPolicy::GroupCommit {
                max_batch_entries: 64,
            },
            enable_wal: true,
            background_flush: true,
            flush_interval_ms: 10,
            max_hot_dirty_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Real-time telemetry and performance statistics (`F-OPS-06`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EngineStats {
    pub live_records: usize,
    pub hot_dirty_bytes: usize,
    pub total_reads: u64,
    pub total_writes: u64,
    pub coalesced_flushes: u64,
    pub wal_batches: u64,
    pub wal_frames: u64,
    pub active_mmaps: usize,
    pub mapped_bytes: usize,
    pub mmap_cache_hits: u64,
    pub mmap_cache_misses: u64,
}

enum BackingSource {
    HotDirty(AlignedBytes),
    PooledMmap {
        guard: MmapGuard,
        offset: usize,
        len: usize,
    },
    ArenaMmap {
        region: Arc<MmapRegion>,
        offset: usize,
        len: usize,
    },
}

/// Lifetime-safe zero-copy read guard pointing directly to `T::Archived` in an `Mmap` page
/// or `HotDirtyArc` buffer (`F-CORE-01`, `RULE-MEM-01`, `RULE-SAFE-02`).
pub struct RecordRef<T: ZeroCopyRecord> {
    backing: BackingSource,
    _marker: core::marker::PhantomData<T>,
}

impl<T: ZeroCopyRecord> core::fmt::Debug for RecordRef<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.view().fmt(f)
    }
}

impl<T: ZeroCopyRecord> RecordRef<T> {
    #[inline]
    pub fn raw_frame(&self) -> &[u8] {
        match &self.backing {
            BackingSource::HotDirty(bytes) => bytes.as_slice(),
            BackingSource::PooledMmap { guard, offset, len } => {
                &guard.as_slice()[*offset..*offset + *len]
            }
            BackingSource::ArenaMmap {
                region,
                offset,
                len,
            } => &region.as_slice()[*offset..*offset + *len],
        }
    }

    /// Returns the zero-copy `RecordView<'_, T>` for this record in 1 CPU cycle (`RULE-SAFE-04`).
    #[inline]
    pub fn view(&self) -> RecordView<'_, T> {
        let frame = self.raw_frame();
        // SAFETY: Every `BackingSource` inside `RecordRef` is validated before construction
        // (`HotDirty` is freshly encoded by `RecordCodec::encode_aligned`; `PooledMmap` and
        // `ArenaMmap` are validated via `RecordCodec::validate_frame`).
        unsafe { RecordCodec::decode_unchecked::<T>(frame) }
    }

    #[inline]
    pub fn header(&self) -> &RecordHeader {
        self.view().header()
    }

    #[inline]
    pub fn version(&self) -> Version {
        self.view().version()
    }

    #[inline]
    pub fn archived(&self) -> &T::Archived {
        self.view().archived()
    }

    #[inline]
    pub fn get_str<'a>(&'a self, field: &storage_codec::ArchivedString) -> &'a str {
        self.view().get_str(field)
    }

    #[inline]
    pub fn get_slice<'a, A: Copy>(&'a self, field: &storage_codec::ArchivedSlice<A>) -> &'a [A] {
        self.view().get_slice(field)
    }

    #[inline]
    pub fn to_owned_record(&self) -> Result<T> {
        self.view().to_owned_record()
    }
}

impl<T: ZeroCopyRecord> Deref for RecordRef<T> {
    type Target = T::Archived;

    #[inline]
    fn deref(&self) -> &Self::Target {
        let frame = self.raw_frame();
        // SAFETY: `frame` is verified and 64-byte aligned.
        unsafe { RecordCodec::decode_unchecked::<T>(frame).archived() }
    }
}

/// Deduplicated dirty-record queue for Write Coalescing (`TASK-015`, `F-CONC-04`).
#[derive(Default)]
struct CoalescingQueue {
    order: VecDeque<InlineId>,
    in_queue: FastHashSet<InlineId>,
}

impl CoalescingQueue {
    #[inline]
    fn enqueue(&mut self, id: InlineId) -> bool {
        if self.in_queue.insert(id) {
            self.order.push_back(id);
            true
        } else {
            false
        }
    }

    #[inline]
    fn drain_batch(&mut self, max_items: usize) -> Vec<InlineId> {
        let count = self.order.len().min(max_items);
        let mut batch = Vec::with_capacity(count);
        for _ in 0..count {
            if let Some(id) = self.order.pop_front() {
                self.in_queue.remove(&id);
                batch.push(id);
            }
        }
        batch
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.order.is_empty()
    }
}

/// Schema migration function signature transforming raw older payload bytes to current `T`.
pub type MigrationFn<T> = Box<dyn Fn(u16, &[u8]) -> Result<T> + Send + Sync>;

struct SharedEngineInner<T: ZeroCopyRecord + Indexable> {
    _dir_lock: DirectoryLock,
    config: EngineConfig,
    paths: PathResolver,
    locks: StripedLockTable,
    primary: PrimaryIndex,
    mmap_pool: MmapPool,
    arena: Option<ArenaManager>,
    wal: Option<Arc<GroupCommitWal>>,
    shared_wal_mode: bool,
    range_index: NumericRangeIndex,
    tag_index: BitmapTagIndex,
    vector_index: HnswVectorIndex,
    mvcc: MvccManager,
    migrations: RwLock<SchemaRegistry<T>>,
    flush_queue: Mutex<CoalescingQueue>,
    flush_cv: Condvar,
    shutdown: AtomicBool,
    total_reads: CachePadded<AtomicU64>,
    total_writes: CachePadded<AtomicU64>,
    coalesced_flushes: CachePadded<AtomicU64>,
}

/// High-Performance Zero-Copy Memory-Mapped Object Storage Engine.
pub struct StorageEngine<T: ZeroCopyRecord + Indexable> {
    inner: Arc<SharedEngineInner<T>>,
    flusher_handle: Mutex<Option<JoinHandle<()>>>,
}

impl<T: ZeroCopyRecord + Indexable> core::fmt::Debug for StorageEngine<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StorageEngine")
            .field("collection", &self.inner.config.collection_name)
            .field("stats", &self.stats())
            .finish()
    }
}

impl<T: ZeroCopyRecord + Indexable> StorageEngine<T> {
    /// Opens or creates a `StorageEngine<T>` rooted at `base_dir`.
    /// Automatically cleans up any orphaned `.tmp` files, loads `primary.idx` checkpoint,
    /// replays committed WAL frames, and rebuilds secondary indexes.
    pub fn open(base_dir: impl Into<PathBuf>, config: EngineConfig) -> Result<Self> {
        Self::open_with_shared_wal(base_dir, config, None)
    }

    /// Opens or creates a `StorageEngine<T>` sharing an optional catalog-wide `Arc<GroupCommitWal>` (`TASK-031`).
    pub fn open_with_shared_wal(
        base_dir: impl Into<PathBuf>,
        config: EngineConfig,
        shared_wal: Option<Arc<GroupCommitWal>>,
    ) -> Result<Self> {
        let base_dir = base_dir.into();
        if !base_dir.exists() {
            fs::create_dir_all(&base_dir)
                .map_err(|e| StorageError::io("create engine base directory", e))?;
        }

        // Acquire OS-level exclusive lock on `<base_dir>/<collection_name>.lock` (`TASK-059`)
        let lock_path = base_dir.join(format!("{}.lock", config.collection_name));
        let dir_lock = DirectoryLock::try_acquire(&lock_path)?;

        // Clean up any orphaned `.tmp` files left from a prior power loss (`F-DURA-04`)
        let _ = cleanup_temp_files(&base_dir)?;

        let paths = PathResolver::new(&base_dir);
        let primary = PrimaryIndex::new();
        let mmap_pool = MmapPool::new(config.mmap_pool);

        let arena = match config.layout_mode {
            StorageLayoutMode::AdaptiveArena {
                segment_capacity, ..
            } => Some(ArenaManager::open(
                &base_dir,
                segment_capacity.max(DEFAULT_SEGMENT_CAPACITY),
            )?),
            _ => None,
        };

        // Load primary index checkpoint if present
        let idx_path = base_dir
            .join("indexes")
            .join(format!("{}_primary.idx", config.collection_name));
        let _ = load_primary_checkpoint(&primary, &idx_path)?;

        let wal_path = base_dir
            .join("wal")
            .join(format!("{}.wal", config.collection_name));
        let shared_wal_mode = shared_wal.is_some();

        let inner = Arc::new(SharedEngineInner {
            _dir_lock: dir_lock,
            config: config.clone(),
            paths,
            locks: StripedLockTable::new(),
            primary,
            mmap_pool,
            arena,
            wal: None,
            shared_wal_mode,
            range_index: NumericRangeIndex::new(),
            tag_index: BitmapTagIndex::new(),
            vector_index: HnswVectorIndex::new(HnswConfig::default()),
            mvcc: MvccManager::new(),
            migrations: RwLock::new(SchemaRegistry::new()),
            flush_queue: Mutex::new(CoalescingQueue::default()),
            flush_cv: Condvar::new(),
            shutdown: AtomicBool::new(false),
            total_reads: CachePadded::new(AtomicU64::new(0)),
            total_writes: CachePadded::new(AtomicU64::new(0)),
            coalesced_flushes: CachePadded::new(AtomicU64::new(0)),
        });

        // Replay committed WAL frames if dedicated WAL exists (`F-DURA-04`)
        if !shared_wal_mode {
            let recovered = GroupCommitWal::recover_committed(&wal_path)?;
            if !recovered.is_empty() {
                for entry in &recovered {
                    if entry.collection_id != T::COLLECTION_ID && entry.collection_id != 0 {
                        continue;
                    }
                    inner.replay_wal_entry(entry)?;
                }
                save_primary_checkpoint(&inner.primary, &idx_path)?;
            }
        }

        // Open fresh WAL for active writes (or attach shared catalog WAL)
        let wal = if let Some(shared) = shared_wal {
            Some(shared)
        } else if config.enable_wal {
            let w = GroupCommitWal::open(&wal_path, config.wal_sync_policy)?;
            w.checkpoint_truncate()?;
            Some(Arc::new(w))
        } else {
            None
        };

        // Unsafe-free reconstruction with active WAL
        let inner = {
            let mut inner_mut = Arc::try_unwrap(inner)
                .unwrap_or_else(|_| unreachable!("unique Arc during open"));
            inner_mut.wal = wal;
            Arc::new(inner_mut)
        };

        // Rebuild in-memory secondary indexes from live checkpointed records
        inner.rebuild_secondary_indexes()?;

        let flusher_handle = if config.background_flush {
            let bg_inner = Arc::clone(&inner);
            let interval = Duration::from_millis(config.flush_interval_ms.max(1));
            Some(thread::spawn(move || {
                bg_inner.background_flush_loop(interval);
            }))
        } else {
            None
        };

        Ok(Self {
            inner,
            flusher_handle: Mutex::new(flusher_handle),
        })
    }

    #[inline]
    pub fn base_dir(&self) -> &Path {
        self.inner.paths.base_dir()
    }

    #[inline]
    pub fn collection_name(&self) -> &str {
        &self.inner.config.collection_name
    }

    #[inline]
    pub fn primary_index(&self) -> &PrimaryIndex {
        &self.inner.primary
    }

    #[inline]
    pub fn range_index(&self) -> &NumericRangeIndex {
        &self.inner.range_index
    }

    #[inline]
    pub fn tag_index(&self) -> &BitmapTagIndex {
        &self.inner.tag_index
    }

    #[inline]
    pub fn vector_index(&self) -> &HnswVectorIndex {
        &self.inner.vector_index
    }

    #[inline]
    pub fn mvcc(&self) -> &MvccManager {
        &self.inner.mvcc
    }

    /// Opens a point-in-time Snapshot Isolation read guard (`TASK-039`, `F-CONC-08`).
    /// Concurrent mutations committed after this call are invisible to the returned `ReadSnapshot`.
    pub fn begin_read_snapshot(&self) -> ReadSnapshot<'_, T> {
        let ts = self.inner.mvcc.register_snapshot();
        ReadSnapshot::new(self, ts)
    }

    /// Sweeps and reclaims obsolete MVCC historical version chains older than the minimum active
    /// snapshot watermark (`TASK-039`, `RULE-MEM-04`).
    pub fn gc_mvcc(&self) -> usize {
        self.inner.mvcc.gc_obsolete_versions()
    }

    /// Returns the latest committed WAL Log Sequence Number (`LSN`).
    #[inline]
    pub fn current_wal_lsn(&self) -> u64 {
        self.inner.wal.as_ref().map_or(0, |w| w.current_lsn())
    }

    /// Rotates the active WAL file into an immutable `wal_XXXXXX.log` segment for PITR archival (`TASK-040`).
    pub fn rotate_wal_segment(&self) -> Result<Option<PathBuf>> {
        if let Some(ref wal) = self.inner.wal {
            Ok(Some(wal.rotate_segment()?))
        } else {
            Ok(None)
        }
    }

    /// Registers a direct schema migration hook for upgrading records with `old_schema_version` (`TASK-026`, `TASK-033`, `F-OPS-01`).
    pub fn register_migration(
        &self,
        old_schema_version: u16,
        migrator: impl Fn(u16, &[u8]) -> Result<T> + Send + Sync + 'static,
    ) {
        self.inner
            .migrations
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .register_direct(old_schema_version, migrator);
    }

    /// Registers a step-by-step payload upcaster (`from_version -> from_version + 1`) (`TASK-033`, `F-CAT-03`).
    pub fn register_step_upcaster(
        &self,
        from_version: u16,
        step_fn: impl Fn(&[u8]) -> Result<Vec<u8>> + Send + Sync + 'static,
    ) {
        self.inner
            .migrations
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .register_step(from_version, step_fn);
    }

    /// Sets the final payload decoder used when a multi-step upcaster chain reaches `T::SCHEMA_VERSION` (`TASK-033`).
    pub fn set_step_upcaster_decoder(
        &self,
        decoder: impl Fn(u16, &[u8]) -> Result<T> + Send + Sync + 'static,
    ) {
        self.inner
            .migrations
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .set_final_decoder(decoder);
    }

    /// Background/online batch schema upgrader (`TASK-033`, `F-CAT-03`):
    /// Scans all live records and re-encodes any record with `hdr.schema_version != T::SCHEMA_VERSION`
    /// into the latest `T::SCHEMA_VERSION` frame on disk.
    pub fn upgrade_legacy_schemas(&self) -> Result<usize> {
        let entries = self.inner.primary.entries_snapshot();
        let mut upgraded_count = 0usize;
        for entry in entries {
            let _lock = self.inner.locks.lock_one(entry.id);
            let Some(cur) = self.inner.primary.get(entry.id) else {
                continue;
            };
            let needs_upgrade = if let Some(ref dirty) = cur.hot_dirty {
                let hdr = RecordHeader::from_slice_verified(dirty.as_slice())?;
                hdr.schema_version != T::SCHEMA_VERSION
            } else {
                match cur.location {
                    DiskLocation::Unflushed => false,
                    DiskLocation::DedicatedFile {
                        parent_group,
                        ping_pong_stride,
                    } => {
                        let path = self.inner.paths.full_record_path(
                            &self.inner.config.collection_name,
                            parent_group,
                            cur.id,
                        )?;
                        let guard = self.inner.mmap_pool.get_or_map(cur.id, &path, false)?;
                        let slice = if ping_pong_stride > 0 {
                            read_ping_pong_active(guard.as_slice(), ping_pong_stride as usize)?.1
                        } else {
                            guard.as_slice()
                        };
                        let hdr = RecordHeader::from_slice_verified(slice)?;
                        hdr.schema_version != T::SCHEMA_VERSION
                    }
                    DiskLocation::Arena(loc) => {
                        if let Some(ref arena) = self.inner.arena {
                            let region = arena.get_region(loc.segment_id)?;
                            let slice =
                                region.subslice_aligned(loc.offset as usize, loc.len as usize)?;
                            let hdr = RecordHeader::from_slice_verified(slice)?;
                            hdr.schema_version != T::SCHEMA_VERSION
                        } else {
                            false
                        }
                    }
                }
            };

            if needs_upgrade {
                let (ver, upgraded_rec, pg, ttl_ns, flags) =
                    self.inner.load_mutable_draft_locked(cur.id)?;
                let now_ns = current_time_ns();
                self.inner.write_record_locked(
                    cur.id,
                    pg,
                    &upgraded_rec,
                    ver.next(),
                    now_ns,
                    ttl_ns,
                    flags,
                )?;
                upgraded_count += 1;
            }
        }
        if upgraded_count > 0 {
            self.flush_all()?;
        }
        Ok(upgraded_count)
    }

    /// Zero-allocation hot read closure (`RULE-MEM-01`).
    ///
    /// When the record is in `HotDirtyArc` or already warm in `MmapPool`, executes `f(view)`
    /// with **zero heap allocations (`alloc_count == 0`)**!
    #[inline]
    pub fn with_ref<R>(
        &self,
        id: InlineId,
        f: impl FnOnce(RecordView<'_, T>) -> R,
    ) -> Result<R> {
        self.inner.total_reads.fetch_add(1, Ordering::Relaxed);
        let mut f_opt = Some(f);

        // Fast path: check `hot_dirty`, `Arena`, or warm `MmapPool` inside shard read lock (0 allocs, 0 refcount ops!)
        let fast_hit = self.inner.primary.with_entry(id, |entry| {
            if entry.ttl_expires_at_ns > 0 && current_time_ns() >= entry.ttl_expires_at_ns {
                return Some(Err(StorageError::not_found(id)));
            }
            if let Some(ref dirty) = entry.hot_dirty {
                let slice = dirty.as_slice();
                // SAFETY: `dirty` was produced by `RecordCodec::encode_aligned` and is 64B-aligned.
                let hdr = unsafe { RecordHeader::from_aligned_unchecked(slice) };
                if !hdr.flags_typed().contains(RecordFlags::COMPRESSED_LZ4) {
                    let view = unsafe { RecordCodec::decode_unchecked::<T>(slice) };
                    let cb = f_opt.take().expect("f_opt consumed once");
                    return Some(Ok(cb(view)));
                }
            }
            match entry.location {
                DiskLocation::Arena(loc) if entry.disk_verified => {
                    if let Some(ref arena) = self.inner.arena {
                        let cb = f_opt.take().expect("f_opt consumed once");
                        let res = arena.with_slice(loc, |slice| {
                            // SAFETY: `entry.disk_verified` is true, proving `validate_frame` succeeded
                            // and the frame is uncompressed and 64B-aligned.
                            let view = unsafe { RecordCodec::decode_unchecked::<T>(slice) };
                            Ok(cb(view))
                        });
                        return Some(res);
                    }
                }
                DiskLocation::DedicatedFile {
                    ping_pong_stride: 0,
                    ..
                } => {
                    if let Some(res) = self.inner.mmap_pool.with_verified_slice(id, |slice| {
                        // SAFETY: `with_verified_slice` only invokes the closure when `is_verified()` is true.
                        let view = unsafe { RecordCodec::decode_unchecked::<T>(slice) };
                        let cb = f_opt.take().expect("f_opt consumed once");
                        Ok(cb(view))
                    }) {
                        return Some(res);
                    }
                }
                _ => {}
            }
            None
        });

        match fast_hit {
            Some(Some(res)) => res,
            None => Err(StorageError::not_found(id)),
            Some(None) => {
                // Cold path: map from disk, validate once, and invoke `f`
                let rec_ref = self.get_ref(id)?;
                let cb = f_opt.take().expect("f_opt not consumed on cold path");
                Ok(cb(rec_ref.view()))
            }
        }
    }

    /// Returns a zero-copy `RecordRef<T>` guard for `id` (`F-CORE-01`).
    pub fn get_ref(&self, id: InlineId) -> Result<RecordRef<T>> {
        self.inner.total_reads.fetch_add(1, Ordering::Relaxed);
        self.inner.get_ref_internal(id)
    }

    /// Materializes an owned copy of `T` (automatically running schema migration or cold LZ4 decompression if needed).
    pub fn get_owned(&self, id: InlineId) -> Result<(Version, T)> {
        self.inner.total_reads.fetch_add(1, Ordering::Relaxed);
        self.inner.get_owned_internal(id)
    }

    /// Inserts a new record (`Err(RecordAlreadyExists)` if `id` already exists).
    pub fn insert(&self, id: InlineId, record: &T) -> Result<Version> {
        let _lock = self.inner.locks.lock_one(id);
        let now_ns = current_time_ns();
        if let Some(existing) = self.inner.primary.get(id) {
            if existing.ttl_expires_at_ns == 0 || now_ns < existing.ttl_expires_at_ns {
                return Err(StorageError::already_exists(id));
            }
        }
        self.inner
            .write_record_locked(id, None, record, Version::INITIAL, now_ns, 0, RecordFlags::NONE)
    }

    /// Inserts or overwrites a record under `id` (optionally grouped under `parent_group` folder).
    pub fn put_grouped(
        &self,
        id: InlineId,
        parent_group: Option<InlineId>,
        record: &T,
    ) -> Result<Version> {
        let _lock = self.inner.locks.lock_one(id);
        let now_ns = current_time_ns();
        let next_ver = self
            .inner
            .primary
            .get(id)
            .map_or(Version::INITIAL, |e| e.version.next());
        self.inner
            .write_record_locked(id, parent_group, record, next_ver, now_ns, 0, RecordFlags::NONE)
    }

    /// Inserts or overwrites a record under `id`.
    #[inline]
    pub fn put(&self, id: InlineId, record: &T) -> Result<Version> {
        self.put_grouped(id, None, record)
    }

    /// Inserts or overwrites a record with a Time-To-Live (`ttl`) duration (`TASK-027`, `F-OPS-02`).
    pub fn put_with_ttl(&self, id: InlineId, record: &T, ttl: Duration) -> Result<Version> {
        let _lock = self.inner.locks.lock_one(id);
        let now_ns = current_time_ns();
        let expires_ns = now_ns.saturating_add(ttl.as_nanos() as u64);
        let next_ver = self
            .inner
            .primary
            .get(id)
            .map_or(Version::INITIAL, |e| e.version.next());
        self.inner.write_record_locked(
            id,
            None,
            record,
            next_ver,
            now_ns,
            expires_ns,
            RecordFlags::HAS_TTL,
        )
    }

    /// Controlled transactional mutation closure (`F-CONC-01`, `RULE-SAFE-03`).
    ///
    /// Serializes writes to `id` via its per-record write queue without blocking any other record,
    /// increments `Version`, diffs secondary indexes, publishes `HotDirtyArc` for immediate
    /// reader visibility, and enqueues a coalesced background disk flush.
    pub fn update<F>(&self, id: InlineId, mutate: F) -> Result<Version>
    where
        F: FnOnce(&mut T) -> Result<()>,
    {
        let _lock = self.inner.locks.lock_one(id);
        let (cur_ver, mut draft, parent_group, ttl_ns, flags) =
            self.inner.load_mutable_draft_locked(id)?;
        mutate(&mut draft)?;
        let next_ver = cur_ver.next();
        let now_ns = current_time_ns();
        self.inner
            .write_record_locked(id, parent_group, &draft, next_ver, now_ns, ttl_ns, flags)
    }

    /// Optimistic Concurrency Control (OCC) Compare-And-Swap overwrite (`F-CONC-06`, `F-NET-02`).
    /// Fails with `StorageError::VersionConflict` if the current record version is not `expected_version`.
    pub fn put_cas(
        &self,
        id: InlineId,
        expected_version: Version,
        record: &T,
    ) -> Result<Version> {
        let _lock = self.inner.locks.lock_one(id);
        let now_ns = current_time_ns();
        let cur_ver = self
            .inner
            .primary
            .get(id)
            .map_or(Version(0), |e| e.version);
        if cur_ver != expected_version {
            return Err(StorageError::version_conflict(id, expected_version, cur_ver));
        }
        let next_ver = if cur_ver.0 == 0 {
            Version::INITIAL
        } else {
            cur_ver.next()
        };
        self.inner.write_record_locked(
            id,
            None,
            record,
            next_ver,
            now_ns,
            0,
            RecordFlags::NONE,
        )
    }

    /// Optimistic Concurrency Control (OCC) conditional update (`F-CONC-06`).
    /// Fails with `StorageError::VersionConflict` if the record's version is not `expected_version`.
    pub fn update_if_version<F>(
        &self,
        id: InlineId,
        expected_version: Version,
        mutate: F,
    ) -> Result<Version>
    where
        F: FnOnce(&mut T) -> Result<()>,
    {
        let _lock = self.inner.locks.lock_one(id);
        let (cur_ver, mut draft, parent_group, ttl_ns, flags) =
            self.inner.load_mutable_draft_locked(id)?;
        if cur_ver != expected_version {
            return Err(StorageError::version_conflict(id, expected_version, cur_ver));
        }
        mutate(&mut draft)?;
        let next_ver = cur_ver.next();
        let now_ns = current_time_ns();
        self.inner
            .write_record_locked(id, parent_group, &draft, next_ver, now_ns, ttl_ns, flags)
    }

    /// Compresses a record using cold LZ4 block compression (`TASK-027`, `F-OPS-03`).
    pub fn compress_cold_record(&self, id: InlineId) -> Result<Version> {
        let _lock = self.inner.locks.lock_one(id);
        let (cur_ver, draft, parent_group, ttl_ns, flags) =
            self.inner.load_mutable_draft_locked(id)?;
        let new_flags = flags.with(RecordFlags::COMPRESSED_LZ4);
        let now_ns = current_time_ns();
        let ver = self.inner.write_record_locked(
            id,
            parent_group,
            &draft,
            cur_ver.next(),
            now_ns,
            ttl_ns,
            new_flags,
        )?;
        self.inner.flush_single_record_to_disk_locked(id, true)?;
        Ok(ver)
    }

    /// Deletes `id` from the storage engine and removes all secondary index postings.
    pub fn delete(&self, id: InlineId) -> Result<bool> {
        let _lock = self.inner.locks.lock_one(id);
        self.inner.delete_record_locked(id)
    }

    /// Atomically links or updates two related records `id_a` and `id_b` in canonical lock order
    /// (`TASK-021`, `F-REL-02`, `RULE-CONC-02`).
    ///
    /// Guarantees zero deadlocks even when concurrent threads link `(A, B)` and `(B, A)` simultaneously!
    pub fn update_pair_atomic<FA, FB>(
        &self,
        id_a: InlineId,
        id_b: InlineId,
        mutate_a: FA,
        mutate_b: FB,
    ) -> Result<(Version, Version)>
    where
        FA: FnOnce(&mut T) -> Result<()>,
        FB: FnOnce(&mut T) -> Result<()>,
    {
        if id_a == id_b {
            return Err(StorageError::invalid_key(
                "cannot call update_pair_atomic with identical IDs",
            ));
        }
        let _guards = self.inner.locks.lock_many(&[id_a, id_b]);

        let (ver_a, mut draft_a, pg_a, ttl_a, flags_a) =
            self.inner.load_mutable_draft_locked(id_a)?;
        let (ver_b, mut draft_b, pg_b, ttl_b, flags_b) =
            self.inner.load_mutable_draft_locked(id_b)?;

        mutate_a(&mut draft_a)?;
        mutate_b(&mut draft_b)?;

        let next_a = ver_a.next();
        let next_b = ver_b.next();
        let now_ns = current_time_ns();

        let frame_a = RecordCodec::encode_aligned(&draft_a, next_a, now_ns, flags_a, ttl_a);
        let frame_b = RecordCodec::encode_aligned(&draft_b, next_b, now_ns, flags_b, ttl_b);

        if let Some(ref wal) = self.inner.wal {
            let txn_id = wal.allocate_txn_id();
            wal.append_transaction(
                txn_id,
                vec![
                    (
                        T::COLLECTION_ID,
                        WalOpKind::Upsert,
                        id_a,
                        next_a,
                        frame_a.as_slice().to_vec(),
                    ),
                    (
                        T::COLLECTION_ID,
                        WalOpKind::Upsert,
                        id_b,
                        next_b,
                        frame_b.as_slice().to_vec(),
                    ),
                ],
            )?;
        }

        self.inner
            .apply_encoded_upsert_locked(id_a, pg_a, &draft_a, next_a, now_ns, ttl_a, frame_a)?;
        self.inner
            .apply_encoded_upsert_locked(id_b, pg_b, &draft_b, next_b, now_ns, ttl_b, frame_b)?;

        Ok((next_a, next_b))
    }

    /// Deletes `id` while enforcing the specified Referential Integrity policy (`TASK-022`, `F-REL-03`):
    /// - `OnDelete::Restrict`: Aborts with `StorageError::IntegrityViolation` if `extract_related(&record)` is non-empty.
    /// - `OnDelete::Cascade`: Recursively deletes `id` and all reachable related records.
    /// - `OnDelete::Unlink`: Removes `id` from every peer record returned by `extract_related(&record)` via `unlink_from_peer`.
    pub fn delete_with_policy<FRel, FUnlink>(
        &self,
        id: InlineId,
        policy: OnDelete,
        extract_related: FRel,
        mut unlink_from_peer: FUnlink,
    ) -> Result<usize>
    where
        FRel: Fn(&T) -> Vec<InlineId>,
        FUnlink: FnMut(&mut T, InlineId),
    {
        let (_, target_owned) = self.get_owned(id)?;
        let related_ids = extract_related(&target_owned);

        match policy {
            OnDelete::Restrict => {
                for peer_id in related_ids {
                    if self.inner.primary.get(peer_id).is_some() {
                        return Err(StorageError::integrity_violation(
                            id,
                            peer_id,
                            "cannot delete record while related records exist (OnDelete::Restrict)",
                        ));
                    }
                }
                let deleted = self.delete(id)?;
                Ok(usize::from(deleted))
            }
            OnDelete::Unlink => {
                let mut all_ids = Vec::with_capacity(related_ids.len() + 1);
                all_ids.push(id);
                all_ids.extend(related_ids.iter().copied());
                let _guards = self.inner.locks.lock_many(&all_ids);

                let now_ns = current_time_ns();
                for peer_id in related_ids {
                    if peer_id == id {
                        continue;
                    }
                    if let Ok((peer_ver, mut peer_draft, pg, ttl, flags)) =
                        self.inner.load_mutable_draft_locked(peer_id)
                    {
                        unlink_from_peer(&mut peer_draft, id);
                        let _ = self.inner.write_record_locked(
                            peer_id,
                            pg,
                            &peer_draft,
                            peer_ver.next(),
                            now_ns,
                            ttl,
                            flags,
                        )?;
                    }
                }
                let deleted = self.inner.delete_record_locked(id)?;
                Ok(usize::from(deleted))
            }
            OnDelete::Cascade => {
                let mut visited = FastHashSet::default();
                let mut queue = VecDeque::new();
                queue.push_back(id);
                visited.insert(id);

                while let Some(cur) = queue.pop_front() {
                    if let Ok((_, rec)) = self.get_owned(cur) {
                        for child in extract_related(&rec) {
                            if visited.insert(child) {
                                queue.push_back(child);
                            }
                        }
                    }
                }

                let all_to_delete: Vec<InlineId> = visited.into_iter().collect();
                let _guards = self.inner.locks.lock_many(&all_to_delete);
                let mut count = 0usize;
                for del_id in all_to_delete {
                    if self.inner.delete_record_locked(del_id)? {
                        count += 1;
                    }
                }
                Ok(count)
            }
        }
    }

    /// Batch zero-copy traversal of related `InlineId`s (`TASK-023`, `F-REL-04`).
    pub fn get_related_refs(&self, ids: &[InlineId]) -> Vec<(InlineId, RecordRef<T>)> {
        let mut out = Vec::with_capacity(ids.len());
        for &id in ids {
            if let Ok(r) = self.get_ref(id) {
                out.push((id, r));
            }
        }
        out
    }

    /// Scans all records and reports any dangling relationship edges (`TASK-022`, `F-REL-05`).
    pub fn verify_relationships<FRel>(&self, extract_related: FRel) -> Vec<DanglingEdge>
    where
        FRel: Fn(&T) -> Vec<InlineId>,
    {
        let entries = self.inner.primary.entries_snapshot();
        let valid_ids: FastHashSet<InlineId> = entries.iter().map(|e| e.id).collect();
        let mut dangling = Vec::new();

        for entry in entries {
            if let Ok((_, rec)) = self.get_owned(entry.id) {
                for target_id in extract_related(&rec) {
                    if !valid_ids.contains(&target_id) {
                        dangling.push(DanglingEdge {
                            source_collection: T::COLLECTION_ID,
                            source_id: entry.id,
                            missing_target_id: target_id,
                        });
                    }
                }
            }
        }
        dangling
    }

    /// Executes a multi-record atomic transaction with Optimistic Concurrency Control (OCC)
    /// and canonical deadlock-free lock acquisition (`TASK-024`, `F-CONC-07`, `RULE-CONC-02`).
    pub fn transaction<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut TransactionBatch<'_, T>) -> Result<R>,
    {
        let mut batch = TransactionBatch::new(self);
        let output = f(&mut batch)?;
        batch.commit()?;
        Ok(output)
    }

    /// Starts a fluent secondary index + vector query (`TASK-019`, `F-INDX-03`, `F-INDX-04`).
    #[inline]
    pub fn query(&self) -> QueryBuilder<'_, T> {
        QueryBuilder::new(self)
    }

    /// Sweeps and deletes all records whose TTL has expired as of `now_ns` (`TASK-027`, `F-OPS-02`).
    pub fn sweep_expired(&self) -> Result<usize> {
        let now_ns = current_time_ns();
        let entries = self.inner.primary.entries_snapshot();
        let mut expired_count = 0usize;
        for entry in entries {
            if entry.ttl_expires_at_ns > 0 && now_ns >= entry.ttl_expires_at_ns {
                if self.delete(entry.id)? {
                    expired_count += 1;
                }
            }
        }
        Ok(expired_count)
    }

    /// Flushes all pending coalesced dirty records to disk and writes an updated `primary.idx` checkpoint.
    pub fn flush_all(&self) -> Result<usize> {
        let flushed = self.inner.drain_and_flush_all()?;
        let idx_path = self
            .inner
            .paths
            .base_dir()
            .join("indexes")
            .join(format!("{}_primary.idx", self.inner.config.collection_name));
        save_primary_checkpoint(&self.inner.primary, &idx_path)?;
        if !self.inner.vector_index.is_empty() {
            let hnsw_path = self
                .inner
                .paths
                .base_dir()
                .join("indexes")
                .join(format!("{}_hnsw.idx", self.inner.config.collection_name));
            let _ = self.inner.vector_index.save_to_disk(&hnsw_path);
        }
        if !self.inner.shared_wal_mode {
            if let Some(ref wal) = self.inner.wal {
                wal.checkpoint_truncate()?;
            }
        }
        Ok(flushed)
    }

    /// Appends an incremental $O(\Delta)$ delta frame (`STGEDLT2`) of modified `upserted_ids` and
    /// `deleted_ids` to `<collection>_primary.idx.delta` without rewriting the full `primary.idx` (`TASK-054`).
    /// Automatically compacts the `.delta` log into the base `primary.idx` once `.delta` exceeds `1 MiB`.
    pub fn checkpoint_incremental(
        &self,
        upserted_ids: &[InlineId],
        deleted_ids: &[InlineId],
    ) -> Result<usize> {
        for &id in upserted_ids {
            let _ = self.inner.flush_single_record_to_disk(id, false)?;
        }
        let idx_path = self
            .inner
            .paths
            .base_dir()
            .join("indexes")
            .join(format!("{}_primary.idx", self.inner.config.collection_name));
        let written = append_primary_checkpoint_delta(
            &self.inner.primary,
            &idx_path,
            upserted_ids,
            deleted_ids,
        )?;
        let delta_path = delta_checkpoint_path(&idx_path);
        if let Ok(meta) = fs::metadata(&delta_path) {
            if meta.len() >= 1024 * 1024 {
                save_primary_checkpoint(&self.inner.primary, &idx_path)?;
            }
        }
        Ok(written)
    }

    /// Reclaims fragmented dead slots across Slotted Arena Segments (`seg_XXXXX.dat`) whose
    /// dead-byte ratio meets or exceeds `min_fragmentation_ratio` (`0.0..=1.0`), evacuating live
    /// records into a clean segment and deleting the old `.dat` files from disk (`F-OPS-03`).
    /// Also automatically upgrades any legacy-schema records to `T::SCHEMA_VERSION` during evacuation (`TASK-033`).
    pub fn compact_arenas(&self, min_fragmentation_ratio: f64) -> Result<usize> {
        self.flush_all()?;
        let Some(ref arena) = self.inner.arena else {
            return Ok(0);
        };
        let frag_ids = arena.fragmented_segment_ids(min_fragmentation_ratio);
        if frag_ids.is_empty() {
            return Ok(0);
        }
        let frag_set: FastHashSet<u32> = frag_ids.iter().copied().collect();

        // Roll active segment so evacuated records are packed into a fresh segment.
        let _ = arena.roll_active_segment()?;

        let entries = self.inner.primary.entries_snapshot();
        for snap_entry in entries {
            if let DiskLocation::Arena(loc) = snap_entry.location {
                if frag_set.contains(&loc.segment_id) {
                    let _lock = self.inner.locks.lock_one(snap_entry.id);
                    let Some(cur) = self.inner.primary.get(snap_entry.id) else {
                        continue;
                    };
                    if cur.hot_dirty.is_some() {
                        let _ = self.inner.flush_single_record_to_disk_locked(cur.id, false)?;
                        continue;
                    }
                    let DiskLocation::Arena(cur_loc) = cur.location else {
                        continue;
                    };
                    if !frag_set.contains(&cur_loc.segment_id) {
                        continue;
                    }
                    let region = arena.get_region(cur_loc.segment_id)?;
                    let slice =
                        region.subslice_aligned(cur_loc.offset as usize, cur_loc.len as usize)?;
                    let raw_hdr = RecordHeader::from_slice_verified(slice)?;
                    if raw_hdr.schema_version != T::SCHEMA_VERSION {
                        let (_, upgraded) = self.inner.decode_or_migrate_frame(slice)?;
                        let flags = raw_hdr.flags_typed().without(RecordFlags::COMPRESSED_LZ4);
                        let reencoded = RecordCodec::encode_aligned(
                            &upgraded,
                            cur.version,
                            current_time_ns(),
                            flags,
                            cur.ttl_expires_at_ns,
                        );
                        let new_loc = arena.write_record(reencoded.as_slice(), false)?;
                        self.inner.primary.upsert(PrimaryEntry {
                            id: cur.id,
                            ordinal: cur.ordinal,
                            version: cur.version,
                            ttl_expires_at_ns: cur.ttl_expires_at_ns,
                            location: DiskLocation::Arena(new_loc),
                            disk_verified: true,
                            hot_dirty: None,
                        });
                    } else {
                        let hdr = RecordCodec::validate_frame::<T>(slice)?;
                        let new_loc = arena.write_record(slice, false)?;
                        let verified = !hdr.is_tombstone()
                            && !hdr.flags_typed().contains(RecordFlags::COMPRESSED_LZ4);
                        self.inner.primary.upsert(PrimaryEntry {
                            id: cur.id,
                            ordinal: cur.ordinal,
                            version: cur.version,
                            ttl_expires_at_ns: cur.ttl_expires_at_ns,
                            location: DiskLocation::Arena(new_loc),
                            disk_verified: verified,
                            hot_dirty: None,
                        });
                    }
                }
            }
        }

        let mut still_referenced: FastHashSet<u32> = FastHashSet::default();
        for entry in self.inner.primary.entries_snapshot() {
            if let DiskLocation::Arena(loc) = entry.location {
                if frag_set.contains(&loc.segment_id) {
                    still_referenced.insert(loc.segment_id);
                }
            }
        }

        let mut removed_segments = 0usize;
        for seg_id in frag_ids {
            if !still_referenced.contains(&seg_id) && arena.remove_segment(seg_id) {
                removed_segments += 1;
            }
        }

        let idx_path = self
            .inner
            .paths
            .base_dir()
            .join("indexes")
            .join(format!("{}_primary.idx", self.inner.config.collection_name));
        save_primary_checkpoint(&self.inner.primary, &idx_path)?;
        Ok(removed_segments)
    }

    /// Creates a consistent point-in-time snapshot of the entire database at `dest_dir` (`TASK-027`, `F-OPS-04`).
    pub fn create_snapshot(&self, dest_dir: &Path) -> Result<usize> {
        self.flush_all()?;
        if dest_dir.exists() {
            fs::remove_dir_all(dest_dir)
                .map_err(|e| StorageError::io("clear snapshot destination directory", e))?;
        }
        copy_dir_recursive(self.inner.paths.base_dir(), dest_dir)
    }

    /// Returns a snapshot of engine telemetry and performance counters (`F-OPS-06`).
    pub fn stats(&self) -> EngineStats {
        let pool_stats = self.inner.mmap_pool.stats();
        let (wal_batches, wal_frames) = self
            .inner
            .wal
            .as_ref()
            .map_or((0, 0), |w| w.stats());

        EngineStats {
            live_records: self.inner.primary.len(),
            hot_dirty_bytes: self.inner.primary.hot_dirty_bytes(),
            total_reads: self.inner.total_reads.load(Ordering::Relaxed),
            total_writes: self.inner.total_writes.load(Ordering::Relaxed),
            coalesced_flushes: self.inner.coalesced_flushes.load(Ordering::Relaxed),
            wal_batches,
            wal_frames,
            active_mmaps: pool_stats.active_mmaps,
            mapped_bytes: pool_stats.mapped_bytes,
            mmap_cache_hits: pool_stats.cache_hits,
            mmap_cache_misses: pool_stats.cache_misses,
        }
    }

    #[inline]
    pub(crate) fn lock_many_records(&self, ids: &[InlineId]) -> MultiRecordLockGuard<'_> {
        self.inner.locks.lock_many(ids)
    }

    #[inline]
    pub(crate) fn current_version(&self, id: InlineId) -> Option<Version> {
        self.inner.primary.get(id).map(|e| e.version)
    }

    #[inline]
    pub(crate) fn apply_tx_upsert_locked(
        &self,
        id: InlineId,
        record: &T,
        version: Version,
        now_ns: u64,
        frame: AlignedBytes,
    ) -> Result<()> {
        self.inner
            .apply_encoded_upsert_locked(id, None, record, version, now_ns, 0, frame)
    }

    pub(crate) fn apply_tx_delete_locked(&self, id: InlineId) -> Result<bool> {
        if let Some(removed) = self.inner.primary.remove(id) {
            self.inner.remove_from_secondary_indexes(&removed);
            self.inner.primary.release_ordinal(removed.ordinal);
            self.inner.mmap_pool.invalidate(id);
            match removed.location {
                DiskLocation::DedicatedFile { parent_group, .. } => {
                    if let Ok(path) = self.inner.paths.full_record_path(
                        &self.inner.config.collection_name,
                        parent_group,
                        id,
                    ) {
                        let _ = fs::remove_file(path);
                    }
                }
                DiskLocation::Arena(loc) => {
                    if let Some(ref arena) = self.inner.arena {
                        arena.mark_dead(loc);
                    }
                }
                DiskLocation::Unflushed => {}
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub(crate) fn replay_wal_entry(&self, entry: &WalEntry) -> Result<()> {
        self.inner.replay_wal_entry(entry)?;
        let idx_path = self
            .inner
            .paths
            .base_dir()
            .join("indexes")
            .join(format!("{}_primary.idx", self.inner.config.collection_name));
        save_primary_checkpoint(&self.inner.primary, &idx_path)?;
        self.inner.rebuild_secondary_indexes()?;
        Ok(())
    }

    pub(crate) fn score_embeddings(
        &self,
        query: &[f32],
        candidates: Option<&[RecordOrdinal]>,
        k: usize,
    ) -> Vec<VectorTopKHit> {
        if k == 0 || query.is_empty() {
            return Vec::new();
        }
        let mut hits: Vec<VectorTopKHit> = Vec::new();
        let score_id = |id: InlineId, ord: RecordOrdinal, hits: &mut Vec<VectorTopKHit>| {
            if let Ok(Some(score)) = self.with_ref(id, |view| {
                T::archived_embedding_slice(view.archived(), view.payload())
                    .map(|vec| cosine_similarity_f32(query, vec))
            }) {
                hits.push(VectorTopKHit { ordinal: ord, score });
            } else if let Ok((_, owned)) = self.get_owned(id) {
                if let Some(vec) = owned.embedding_slice() {
                    let score = cosine_similarity_f32(query, vec);
                    hits.push(VectorTopKHit { ordinal: ord, score });
                }
            }
        };

        // Adaptive Filter-Aware HNSW Execution + Exact Top-2K f32 Re-Ranking (`TASK-038`, `F-INDX-07`):
        // - If the filtered candidate set is small (`<= 256` records) or the total index has `<= 256` vectors,
        //   direct SIMD `f32` cosine scoring over the candidate ordinals is `< 5 µs`.
        // - Otherwise, traverse the multi-layer `SQ8` HNSW graph in `O(log N)` to retrieve `max(2K, 32)`
        //   candidates and re-rank them against their zero-copy `f32` `mmap` slices!
        let use_hnsw = self.inner.vector_index.len() > 256
            && candidates.is_none_or(|ords| ords.len() > 256);

        if use_hnsw {
            let filter_bm = candidates.map(|ords| {
                let mut bm = CompressedBitmap::new();
                for &ord in ords {
                    bm.insert(ord);
                }
                bm
            });
            let oversample_k = (k * 2).max(32);
            let sq8_candidates = self.inner.vector_index.search_sq8_candidates(
                query,
                oversample_k,
                filter_bm.as_ref(),
            );
            for cand in sq8_candidates {
                if let Some(id) = self.inner.primary.resolve_ordinal(cand.ordinal) {
                    score_id(id, cand.ordinal, &mut hits);
                }
            }
        } else {
            match candidates {
                Some(ords) => {
                    for &ord in ords {
                        if let Some(id) = self.inner.primary.resolve_ordinal(ord) {
                            score_id(id, ord, &mut hits);
                        }
                    }
                }
                None => {
                    for entry in self.inner.primary.entries_snapshot() {
                        score_id(entry.id, entry.ordinal, &mut hits);
                    }
                }
            }
        }

        if hits.len() > k {
            // $O(N)$ Quickselect partitioning followed by $O(K \log K)$ sort on top $K$ only!
            hits.select_nth_unstable_by(k - 1, |a, b| b.score.total_cmp(&a.score));
            hits.truncate(k);
        }
        hits.sort_unstable_by(|a, b| b.score.total_cmp(&a.score));
        hits
    }
}

impl<T: ZeroCopyRecord + Indexable> Drop for StorageEngine<T> {
    fn drop(&mut self) {
        self.inner.shutdown.store(true, Ordering::SeqCst);
        self.inner.flush_cv.notify_all();
        if let Ok(mut handle_opt) = self.flusher_handle.lock() {
            if let Some(handle) = handle_opt.take() {
                let _ = handle.join();
            }
        }
        let _ = self.flush_all();
    }
}

impl<T: ZeroCopyRecord + Indexable> SharedEngineInner<T> {
    fn get_ref_internal(&self, id: InlineId) -> Result<RecordRef<T>> {
        let entry = self
            .primary
            .get(id)
            .ok_or_else(|| StorageError::not_found(id))?;

        if entry.ttl_expires_at_ns > 0 && current_time_ns() >= entry.ttl_expires_at_ns {
            return Err(StorageError::not_found(id));
        }

        if let Some(dirty_bytes) = entry.hot_dirty {
            // SAFETY: `dirty_bytes` was encoded by `RecordCodec::encode_aligned` and is 64B-aligned.
            let hdr = unsafe { RecordHeader::from_aligned_unchecked(dirty_bytes.as_slice()) };
            if hdr.is_tombstone() || hdr.flags_typed().contains(RecordFlags::COMPRESSED_LZ4) {
                return Err(StorageError::invalid_archive(
                    "compressed or tombstone record must be read via get_owned",
                ));
            }
            return Ok(RecordRef {
                backing: BackingSource::HotDirty(dirty_bytes),
                _marker: core::marker::PhantomData,
            });
        }

        match entry.location {
            DiskLocation::Unflushed => Err(StorageError::not_found(id)),
            DiskLocation::DedicatedFile {
                parent_group,
                ping_pong_stride,
            } => {
                let path = self.paths.full_record_path(
                    &self.config.collection_name,
                    parent_group,
                    id,
                )?;
                let guard = self.mmap_pool.get_or_map(id, &path, false)?;
                if ping_pong_stride > 0 {
                    let (slot_off, slice) =
                        read_ping_pong_active(guard.as_slice(), ping_pong_stride as usize)?;
                    if !guard.is_verified() {
                        let hdr = RecordCodec::validate_frame::<T>(slice)?;
                        if hdr.is_tombstone()
                            || hdr.flags_typed().contains(RecordFlags::COMPRESSED_LZ4)
                        {
                            return Err(StorageError::invalid_archive(
                                "compressed or tombstone record must be read via get_owned",
                            ));
                        }
                        guard.mark_verified();
                    }
                    let len = slice.len();
                    Ok(RecordRef {
                        backing: BackingSource::PooledMmap {
                            guard,
                            offset: slot_off,
                            len,
                        },
                        _marker: core::marker::PhantomData,
                    })
                } else {
                    if !guard.is_verified() {
                        let hdr = RecordCodec::validate_frame::<T>(guard.as_slice())?;
                        if hdr.is_tombstone()
                            || hdr.flags_typed().contains(RecordFlags::COMPRESSED_LZ4)
                        {
                            return Err(StorageError::invalid_archive(
                                "compressed or tombstone record must be read via get_owned",
                            ));
                        }
                        guard.mark_verified();
                    }
                    let len = guard.as_slice().len();
                    Ok(RecordRef {
                        backing: BackingSource::PooledMmap {
                            guard,
                            offset: 0,
                            len,
                        },
                        _marker: core::marker::PhantomData,
                    })
                }
            }
            DiskLocation::Arena(loc) => {
                let arena = self
                    .arena
                    .as_ref()
                    .ok_or_else(|| StorageError::corrupted_header("arena manager not enabled"))?;
                let region = arena.get_region(loc.segment_id)?;
                let slice = region.subslice_aligned(loc.offset as usize, loc.len as usize)?;
                if !entry.disk_verified {
                    let hdr = RecordCodec::validate_frame::<T>(slice)?;
                    if hdr.is_tombstone()
                        || hdr.flags_typed().contains(RecordFlags::COMPRESSED_LZ4)
                    {
                        return Err(StorageError::invalid_archive(
                            "compressed or tombstone record must be read via get_owned",
                        ));
                    }
                    self.primary.mark_disk_verified(id, DiskLocation::Arena(loc));
                }
                Ok(RecordRef {
                    backing: BackingSource::ArenaMmap {
                        region,
                        offset: loc.offset as usize,
                        len: loc.len as usize,
                    },
                    _marker: core::marker::PhantomData,
                })
            }
        }
    }

    fn get_owned_internal(&self, id: InlineId) -> Result<(Version, T)> {
        let entry = self
            .primary
            .get(id)
            .ok_or_else(|| StorageError::not_found(id))?;
        let now_ns = current_time_ns();
        if entry.ttl_expires_at_ns > 0 && now_ns >= entry.ttl_expires_at_ns {
            return Err(StorageError::not_found(id));
        }

        if let Some(ref dirty) = entry.hot_dirty {
            return self.decode_or_migrate_frame(dirty.as_slice());
        }

        match entry.location {
            DiskLocation::Unflushed => Err(StorageError::not_found(id)),
            DiskLocation::DedicatedFile {
                parent_group,
                ping_pong_stride,
            } => {
                let path = self.paths.full_record_path(
                    &self.config.collection_name,
                    parent_group,
                    id,
                )?;
                let guard = self.mmap_pool.get_or_map(id, &path, false)?;
                let slice = if ping_pong_stride > 0 {
                    read_ping_pong_active(guard.as_slice(), ping_pong_stride as usize)?.1
                } else {
                    guard.as_slice()
                };
                self.decode_or_migrate_frame(slice)
            }
            DiskLocation::Arena(loc) => {
                let arena = self
                    .arena
                    .as_ref()
                    .ok_or_else(|| StorageError::corrupted_header("arena manager not enabled"))?;
                let region = arena.get_region(loc.segment_id)?;
                let slice = region.subslice_aligned(loc.offset as usize, loc.len as usize)?;
                self.decode_or_migrate_frame(slice)
            }
        }
    }

    fn replay_wal_entry(&self, entry: &WalEntry) -> Result<()> {
        match entry.kind {
            WalOpKind::Upsert => {
                let aligned = AlignedBytes::copy_from_slice(&entry.frame_bytes);
                let hdr = RecordHeader::from_slice_verified(&aligned)?;
                let ord = self
                    .primary
                    .get(entry.record_id)
                    .map(|e| e.ordinal)
                    .unwrap_or_else(|| self.primary.allocate_ordinal(entry.record_id));
                let prev_loc = self
                    .primary
                    .get(entry.record_id)
                    .map(|e| e.location)
                    .unwrap_or(DiskLocation::Unflushed);
                self.primary.upsert(PrimaryEntry {
                    id: entry.record_id,
                    ordinal: ord,
                    version: entry.version,
                    ttl_expires_at_ns: hdr.ttl_expires_at_ns,
                    location: prev_loc,
                    disk_verified: false,
                    hot_dirty: Some(aligned),
                });
                self.flush_single_record_to_disk(entry.record_id, true)?;
            }
            WalOpKind::Delete => {
                if let Some(removed) = self.primary.remove(entry.record_id) {
                    self.primary.release_ordinal(removed.ordinal);
                    self.mmap_pool.invalidate(entry.record_id);
                    if let DiskLocation::DedicatedFile { parent_group, .. } = removed.location {
                        if let Ok(p) = self.paths.full_record_path(
                            &self.config.collection_name,
                            parent_group,
                            entry.record_id,
                        ) {
                            let _ = fs::remove_file(p);
                        }
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn decode_or_migrate_frame(&self, frame: &[u8]) -> Result<(Version, T)> {
        let hdr = RecordHeader::from_slice_verified(frame)?;
        if hdr.schema_version != T::SCHEMA_VERSION {
            let total_needed = HEADER_SIZE
                .checked_add(hdr.payload_len as usize)
                .ok_or_else(|| StorageError::corrupted_header("frame length overflow"))?;
            let raw_payload = frame.get(HEADER_SIZE..total_needed).ok_or_else(|| {
                StorageError::corrupted_header("frame truncated before payload_len")
            })?;
            let actual_checksum = storage_codec::checksum_payload(raw_payload);
            if actual_checksum != hdr.payload_checksum {
                return Err(StorageError::checksum_mismatch(
                    hdr.payload_checksum,
                    actual_checksum,
                ));
            }
            let migs = self
                .migrations
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if hdr.flags_typed().contains(RecordFlags::COMPRESSED_LZ4) {
                let decompressed =
                    storage_codec::decompress_lz4(raw_payload, hdr.uncompressed_len as usize)?;
                let upgraded = migs.migrate_payload(hdr.schema_version, &decompressed)?;
                return Ok((hdr.version(), upgraded));
            }
            let upgraded = migs.migrate_payload(hdr.schema_version, raw_payload)?;
            return Ok((hdr.version(), upgraded));
        }
        let (hdr, owned) = RecordCodec::decode_owned::<T>(frame)?;
        Ok((hdr.version(), owned))
    }

    /// Reads the raw 64B-aligned record frame for `entry` (`O(1)` Arc clone if `hot_dirty`,
    /// or copied from `mmap` only when an active MVCC snapshot requires copy-on-write retention).
    fn read_raw_frame_for_entry(&self, entry: &PrimaryEntry) -> Option<AlignedBytes> {
        if let Some(ref dirty) = entry.hot_dirty {
            return Some(dirty.clone());
        }
        match entry.location {
            DiskLocation::Unflushed => None,
            DiskLocation::Arena(loc) => {
                let arena = self.arena.as_ref()?;
                arena
                    .with_slice(loc, |slice| Ok(AlignedBytes::copy_from_slice(slice)))
                    .ok()
            }
            DiskLocation::DedicatedFile {
                parent_group,
                ping_pong_stride,
            } => {
                let path = self
                    .paths
                    .full_record_path(&self.config.collection_name, parent_group, entry.id)
                    .ok()?;
                let guard = self.mmap_pool.get_or_map(entry.id, &path, false).ok()?;
                let slice = if ping_pong_stride > 0 {
                    read_ping_pong_active(guard.as_slice(), ping_pong_stride as usize)
                        .ok()?
                        .1
                } else {
                    guard.as_slice()
                };
                Some(AlignedBytes::copy_from_slice(slice))
            }
        }
    }

    /// Extracts an `IndexSnapshot` on-the-fly directly from an existing record's `hot_dirty` or
    /// `mmap` frame with zero persistent RAM overhead.
    fn extract_snapshot_for_entry(&self, entry: &PrimaryEntry) -> Option<IndexSnapshot> {
        if let Some(ref dirty) = entry.hot_dirty {
            return self.extract_snapshot_from_frame(dirty.as_slice(), true);
        }
        match entry.location {
            DiskLocation::Unflushed => None,
            DiskLocation::Arena(loc) => {
                let arena = self.arena.as_ref()?;
                arena
                    .with_slice(loc, |slice| {
                        Ok(self.extract_snapshot_from_frame(slice, entry.disk_verified))
                    })
                    .ok()
                    .flatten()
            }
            DiskLocation::DedicatedFile {
                parent_group,
                ping_pong_stride,
            } => {
                let path = self
                    .paths
                    .full_record_path(&self.config.collection_name, parent_group, entry.id)
                    .ok()?;
                let guard = self.mmap_pool.get_or_map(entry.id, &path, false).ok()?;
                let slice = if ping_pong_stride > 0 {
                    read_ping_pong_active(guard.as_slice(), ping_pong_stride as usize)
                        .ok()?
                        .1
                } else {
                    guard.as_slice()
                };
                self.extract_snapshot_from_frame(slice, guard.is_verified())
            }
        }
    }

    #[inline]
    fn extract_snapshot_from_frame(
        &self,
        frame: &[u8],
        already_verified: bool,
    ) -> Option<IndexSnapshot> {
        if already_verified {
            // SAFETY: `frame` is 64B-aligned and already validated for `T`.
            let hdr = unsafe { RecordHeader::from_aligned_unchecked(frame) };
            if !hdr.is_tombstone() && !hdr.flags_typed().contains(RecordFlags::COMPRESSED_LZ4) {
                let view = unsafe { RecordCodec::decode_unchecked::<T>(frame) };
                return T::extract_archived_snapshot(view.archived(), view.payload());
            }
        }
        if let Ok(view) = RecordCodec::decode_verified::<T>(frame) {
            return T::extract_archived_snapshot(view.archived(), view.payload());
        }
        self.decode_or_migrate_frame(frame)
            .ok()
            .map(|(_, owned)| owned.extract_index_snapshot())
    }

    fn load_mutable_draft_locked(
        &self,
        id: InlineId,
    ) -> Result<(Version, T, Option<InlineId>, u64, RecordFlags)> {
        let entry = self
            .primary
            .get(id)
            .ok_or_else(|| StorageError::not_found(id))?;
        if entry.ttl_expires_at_ns > 0 && current_time_ns() >= entry.ttl_expires_at_ns {
            return Err(StorageError::not_found(id));
        }

        let parent_group = match entry.location {
            DiskLocation::DedicatedFile { parent_group, .. } => parent_group,
            _ => None,
        };
        let (ver, owned) = if let Some(ref dirty) = entry.hot_dirty {
            let slice = dirty.as_slice();
            let hdr = unsafe { RecordHeader::from_aligned_unchecked(slice) };
            if !hdr.flags_typed().contains(RecordFlags::COMPRESSED_LZ4) {
                // Fast path: `dirty` was encoded in memory at `T::SCHEMA_VERSION` and is 64B-aligned
                let view = unsafe { RecordCodec::decode_unchecked::<T>(slice) };
                (entry.version, view.to_owned_record()?)
            } else {
                self.decode_or_migrate_frame(slice)?
            }
        } else {
            self.get_owned_internal(id)?
        };
        let flags = if entry.ttl_expires_at_ns > 0 {
            RecordFlags::HAS_TTL
        } else {
            RecordFlags::NONE
        };
        Ok((ver, owned, parent_group, entry.ttl_expires_at_ns, flags))
    }

    #[allow(clippy::too_many_arguments)]
    fn write_record_locked(
        &self,
        id: InlineId,
        parent_group: Option<InlineId>,
        record: &T,
        version: Version,
        now_ns: u64,
        ttl_expires_at_ns: u64,
        flags: RecordFlags,
    ) -> Result<Version> {
        let frame = RecordCodec::encode_aligned(record, version, now_ns, flags, ttl_expires_at_ns);

        if let Some(ref wal) = self.wal {
            wal.append_single(
                T::COLLECTION_ID,
                WalOpKind::Upsert,
                id,
                version,
                frame.as_slice(),
            )?;
        }

        self.apply_encoded_upsert_locked(
            id,
            parent_group,
            record,
            version,
            now_ns,
            ttl_expires_at_ns,
            frame,
        )?;
        Ok(version)
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_encoded_upsert_locked(
        &self,
        id: InlineId,
        parent_group: Option<InlineId>,
        record: &T,
        version: Version,
        _now_ns: u64,
        ttl_expires_at_ns: u64,
        frame: AlignedBytes,
    ) -> Result<()> {
        let existing = self.primary.get(id);
        if self.mvcc.has_active_snapshots() {
            if let Some(ref old_entry) = existing {
                let old_frame = self.read_raw_frame_for_entry(old_entry);
                self.mvcc
                    .seed_baseline_if_absent(id, old_entry.version, old_frame);
            } else {
                self.mvcc
                    .seed_baseline_if_absent(id, Version::INITIAL, None);
            }
            self.mvcc.record_version(id, version, Some(frame.clone()));
        }

        let ordinal = match &existing {
            Some(e) => e.ordinal,
            None => self.primary.allocate_ordinal(id),
        };
        let old_snap = existing
            .as_ref()
            .and_then(|e| self.extract_snapshot_for_entry(e))
            .unwrap_or_default();

        let location = match existing.map(|e| e.location) {
            Some(DiskLocation::DedicatedFile {
                parent_group: old_pg,
                ping_pong_stride,
            }) => DiskLocation::DedicatedFile {
                parent_group: parent_group.or(old_pg),
                ping_pong_stride,
            },
            Some(other) => other,
            None => match self.config.layout_mode {
                StorageLayoutMode::PingPongFiles { slot_stride } => DiskLocation::DedicatedFile {
                    parent_group,
                    ping_pong_stride: slot_stride,
                },
                StorageLayoutMode::DedicatedFiles => DiskLocation::DedicatedFile {
                    parent_group,
                    ping_pong_stride: 0,
                },
                StorageLayoutMode::AdaptiveArena { .. } => {
                    if parent_group.is_some() {
                        DiskLocation::DedicatedFile {
                            parent_group,
                            ping_pong_stride: 0,
                        }
                    } else {
                        DiskLocation::Unflushed
                    }
                }
            },
        };

        // Differential secondary index update without any global IndexSnapshot map (`TASK-016`, `F-INDX-05`, `RULE-INDX-01`)
        let new_snap = record.extract_index_snapshot();
        self.apply_index_snapshot_diff(ordinal, &old_snap, &new_snap);
        if old_snap.has_embedding != new_snap.has_embedding
            || old_snap.embedding_hash != new_snap.embedding_hash
        {
            if let Some(emb) = record.embedding_slice() {
                if !emb.is_empty() {
                    self.vector_index.insert(ordinal, emb);
                } else if old_snap.has_embedding {
                    self.vector_index.remove(ordinal);
                }
            } else if old_snap.has_embedding {
                self.vector_index.remove(ordinal);
            }
        }

        self.primary.upsert(PrimaryEntry {
            id,
            ordinal,
            version,
            ttl_expires_at_ns,
            location,
            disk_verified: false,
            hot_dirty: Some(frame),
        });

        self.total_writes.fetch_add(1, Ordering::Relaxed);

        if self.config.background_flush {
            // Backpressure guard: if unflushed dirty RAM exceeds `max_hot_dirty_bytes`, flush synchronously!
            if self.primary.hot_dirty_bytes() >= self.config.max_hot_dirty_bytes {
                self.flush_cv.notify_one();
                self.flush_single_record_to_disk_locked(id, false)?;
            } else {
                let mut q = self
                    .flush_queue
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let was_empty = q.is_empty();
                if !q.enqueue(id) {
                    // `id` was already in the flush queue! This update coalesced in memory (`F-CONC-04`)!
                    self.coalesced_flushes.fetch_add(1, Ordering::Relaxed);
                }
                if was_empty || q.order.len() == 256 {
                    self.flush_cv.notify_one();
                }
            }
        } else {
            self.flush_single_record_to_disk_locked(id, false)?;
        }

        Ok(())
    }

    #[inline]
    fn apply_index_snapshot_diff(
        &self,
        ordinal: RecordOrdinal,
        old_snap: &IndexSnapshot,
        new_snap: &IndexSnapshot,
    ) {
        let delta = IndexDelta::diff(old_snap, new_snap);
        if !delta.is_empty() {
            for &(field_id, val) in &delta.removed_numerics[..delta.removed_numerics_len] {
                self.range_index.remove(field_id, val, ordinal);
            }
            for &(field_id, val) in &delta.added_numerics[..delta.added_numerics_len] {
                self.range_index.insert(field_id, val, ordinal);
            }
            for &(field_id, tag) in &delta.removed_tags[..delta.removed_tags_len] {
                self.tag_index.remove(field_id, tag, ordinal);
            }
            for &(field_id, tag) in &delta.added_tags[..delta.added_tags_len] {
                self.tag_index.insert(field_id, tag, ordinal);
            }
        }
    }

    #[inline]
    fn remove_from_secondary_indexes(&self, removed: &PrimaryEntry) {
        if let Some(snap) = self.extract_snapshot_for_entry(removed) {
            for (field_id, val) in snap.numerics() {
                self.range_index.remove(field_id, val, removed.ordinal);
            }
            for (field_id, tag) in snap.tags() {
                self.tag_index.remove(field_id, tag, removed.ordinal);
            }
            if snap.has_embedding {
                self.vector_index.remove(removed.ordinal);
            }
        }
    }

    fn delete_record_locked(&self, id: InlineId) -> Result<bool> {
        let Some(removed) = self.primary.remove(id) else {
            return Ok(false);
        };

        if self.mvcc.has_active_snapshots() {
            let old_frame = self.read_raw_frame_for_entry(&removed);
            self.mvcc
                .seed_baseline_if_absent(id, removed.version, old_frame);
            self.mvcc.record_version(id, removed.version.next(), None);
        }

        if let Some(ref wal) = self.wal {
            wal.append_single(
                T::COLLECTION_ID,
                WalOpKind::Delete,
                id,
                removed.version.next(),
                &[],
            )?;
        }

        self.remove_from_secondary_indexes(&removed);
        self.primary.release_ordinal(removed.ordinal);
        self.mmap_pool.invalidate(id);

        match removed.location {
            DiskLocation::DedicatedFile { parent_group, .. } => {
                if let Ok(path) =
                    self.paths
                        .full_record_path(&self.config.collection_name, parent_group, id)
                {
                    let _ = fs::remove_file(path);
                }
            }
            DiskLocation::Arena(loc) => {
                if let Some(ref arena) = self.arena {
                    if self.mvcc.has_active_snapshots() {
                        arena.mark_dead(loc);
                    } else {
                        arena.reclaim_dead_slot(loc);
                    }
                }
            }
            DiskLocation::Unflushed => {}
        }
        Ok(true)
    }

    #[inline]
    fn flush_single_record_to_disk(&self, id: InlineId, sync_disk: bool) -> Result<bool> {
        self.flush_single_record_impl(id, sync_disk, false)
    }

    #[inline]
    fn flush_single_record_to_disk_locked(&self, id: InlineId, sync_disk: bool) -> Result<bool> {
        self.flush_single_record_impl(id, sync_disk, true)
    }

    fn flush_single_record_impl(
        &self,
        id: InlineId,
        sync_disk: bool,
        already_locked: bool,
    ) -> Result<bool> {
        let Some(entry) = self.primary.get(id) else {
            return Ok(false);
        };
        let Some(dirty_frame) = entry.hot_dirty else {
            return Ok(false);
        };

        let flushed_ver = entry.version;
        let frame_slice = dirty_frame.as_slice();
        // SAFETY: `dirty_frame` is 64B-aligned and produced by `RecordCodec::encode_aligned`.
        let hdr = unsafe { RecordHeader::from_aligned_unchecked(frame_slice) };
        let disk_verified =
            !hdr.is_tombstone() && !hdr.flags_typed().contains(RecordFlags::COMPRESSED_LZ4);

        // Determine target disk location based on layout mode
        let new_location = match self.config.layout_mode {
            StorageLayoutMode::AdaptiveArena {
                arena_threshold_bytes,
                ..
            } if frame_slice.len() <= arena_threshold_bytes
                && !matches!(
                    entry.location,
                    DiskLocation::DedicatedFile {
                        parent_group: Some(_),
                        ..
                    }
                ) =>
            {
                let arena = self
                    .arena
                    .as_ref()
                    .ok_or_else(|| StorageError::corrupted_header("arena manager not enabled"))?;
                let loc = arena.write_record(frame_slice, sync_disk)?;
                if let DiskLocation::Arena(old_loc) = entry.location {
                    if self.mvcc.has_active_snapshots() {
                        arena.mark_dead(old_loc);
                    } else {
                        arena.reclaim_dead_slot(old_loc);
                    }
                }
                DiskLocation::Arena(loc)
            }
            StorageLayoutMode::PingPongFiles { slot_stride } => {
                let _guard = if already_locked {
                    None
                } else {
                    Some(self.locks.lock_one(id))
                };
                let parent_group = match entry.location {
                    DiskLocation::DedicatedFile { parent_group, .. } => parent_group,
                    _ => None,
                };
                let path = self.paths.full_record_path(
                    &self.config.collection_name,
                    parent_group,
                    id,
                )?;
                // Unmap any old view before writing on Windows
                self.mmap_pool.invalidate(id);
                let actual_stride = (slot_stride as usize)
                    .max(frame_slice.len())
                    .next_multiple_of(HEADER_SIZE);
                write_ping_pong_slot(&path, frame_slice, actual_stride, sync_disk)?;
                DiskLocation::DedicatedFile {
                    parent_group,
                    ping_pong_stride: actual_stride as u32,
                }
            }
            _ => {
                let _guard = if already_locked {
                    None
                } else {
                    Some(self.locks.lock_one(id))
                };
                let parent_group = match entry.location {
                    DiskLocation::DedicatedFile { parent_group, .. } => parent_group,
                    _ => None,
                };
                let path = self.paths.full_record_path(
                    &self.config.collection_name,
                    parent_group,
                    id,
                )?;
                // Invalidate active mapping so Windows allows atomic rename over target
                self.mmap_pool.invalidate(id);
                atomic_write_replace(&path, frame_slice, sync_disk)?;
                DiskLocation::DedicatedFile {
                    parent_group,
                    ping_pong_stride: 0,
                }
            }
        };

        self.primary
            .complete_background_flush(id, flushed_ver, new_location, disk_verified);
        Ok(true)
    }

    fn drain_and_flush_all(&self) -> Result<usize> {
        let mut total = 0usize;
        loop {
            let batch = {
                let mut q = self
                    .flush_queue
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                q.drain_batch(256)
            };
            if batch.is_empty() {
                break;
            }
            for id in batch {
                if self.flush_single_record_to_disk(id, false)? {
                    total += 1;
                }
            }
        }
        // Also flush any entries that were marked dirty outside the queue
        for entry in self.primary.entries_snapshot() {
            if entry.hot_dirty.is_some() && self.flush_single_record_to_disk(entry.id, false)? {
                total += 1;
            }
        }
        Ok(total)
    }

    fn background_flush_loop(&self, interval: Duration) {
        while !self.shutdown.load(Ordering::Relaxed) {
            let batch = {
                let mut q = self
                    .flush_queue
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                // Sleep indefinitely (`0.0%` CPU, zero timer wakeups) when queue is empty
                while q.is_empty() && !self.shutdown.load(Ordering::Relaxed) {
                    q = self
                        .flush_cv
                        .wait(q)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                }
                if self.shutdown.load(Ordering::Relaxed) {
                    break;
                }
                // Coalesce rapid updates over `interval` unless batch threshold (`256`) is already reached
                if q.order.len() < 256 && !self.shutdown.load(Ordering::Relaxed) {
                    let (q2, _) = self
                        .flush_cv
                        .wait_timeout(q, interval)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    q = q2;
                }
                q.drain_batch(128)
            };

            for id in batch {
                if self.flush_single_record_to_disk(id, false).is_err() {
                    // Retain `hot_dirty` in RAM and re-enqueue `id` for retry on next flush tick
                    let mut q = self
                        .flush_queue
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    q.enqueue(id);
                    self.flush_cv.notify_one();
                }
            }
        }
    }

    fn rebuild_secondary_indexes(&self) -> Result<()> {
        let empty = IndexSnapshot::new();
        let entries = self.primary.entries_snapshot();
        for entry in entries {
            if let Some(snap) = self.extract_snapshot_for_entry(&entry) {
                self.apply_index_snapshot_diff(entry.ordinal, &empty, &snap);
                if snap.has_embedding {
                    if let Ok((_, owned)) = self.get_owned_internal(entry.id) {
                        if let Some(emb) = owned.embedding_slice() {
                            self.vector_index.insert(entry.ordinal, emb);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// Multi-record Optimistic Concurrency Control (OCC) Transaction Batch (`TASK-024`, `F-CONC-07`).
pub struct TransactionBatch<'a, T: ZeroCopyRecord + Indexable> {
    engine: &'a StorageEngine<T>,
    read_versions: FastHashMap<InlineId, Option<Version>>,
    staged_upserts: FastHashMap<InlineId, (Option<InlineId>, T)>,
    staged_deletes: FastHashSet<InlineId>,
}

impl<'a, T: ZeroCopyRecord + Indexable> core::fmt::Debug for TransactionBatch<'a, T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("TransactionBatch")
            .field("reads", &self.read_versions.len())
            .field("staged_upserts", &self.staged_upserts.len())
            .field("staged_deletes", &self.staged_deletes.len())
            .finish()
    }
}

impl<'a, T: ZeroCopyRecord + Indexable> TransactionBatch<'a, T> {
    fn new(engine: &'a StorageEngine<T>) -> Self {
        Self {
            engine,
            read_versions: FastHashMap::default(),
            staged_upserts: FastHashMap::default(),
            staged_deletes: FastHashSet::default(),
        }
    }

    /// Reads a record inside the transaction, recording its OCC `Version` in the read-set.
    pub fn get(&mut self, id: InlineId) -> Result<T> {
        if self.staged_deletes.contains(&id) {
            return Err(StorageError::not_found(id));
        }
        if let Some((_, staged)) = self.staged_upserts.get(&id) {
            return Ok(staged.clone());
        }
        match self.engine.get_owned(id) {
            Ok((ver, rec)) => {
                self.read_versions.entry(id).or_insert(Some(ver));
                Ok(rec)
            }
            Err(StorageError::RecordNotFound { .. }) => {
                self.read_versions.entry(id).or_insert(None);
                Err(StorageError::not_found(id))
            }
            Err(e) => Err(e),
        }
    }

    /// Stages an insert or update inside the transaction.
    pub fn put(&mut self, id: InlineId, record: T) {
        self.staged_deletes.remove(&id);
        self.staged_upserts.insert(id, (None, record));
    }

    /// Stages a deletion inside the transaction.
    pub fn delete(&mut self, id: InlineId) {
        self.staged_upserts.remove(&id);
        self.staged_deletes.insert(id);
    }

    fn commit(self) -> Result<()> {
        if self.staged_upserts.is_empty() && self.staged_deletes.is_empty() {
            return Ok(());
        }

        // Collect all touched keys (reads + writes + deletes) and lock in canonical ascending order (`RULE-CONC-02`)
        let mut all_keys: Vec<InlineId> = self
            .read_versions
            .keys()
            .chain(self.staged_upserts.keys())
            .chain(self.staged_deletes.iter())
            .copied()
            .collect();
        all_keys.sort_unstable();
        all_keys.dedup();

        let _guards = self.engine.inner.locks.lock_many(&all_keys);

        // Verify OCC read-set versions have not changed since read
        for (&id, &expected_opt) in &self.read_versions {
            let actual_opt = self.engine.inner.primary.get(id).map(|e| e.version);
            if actual_opt != expected_opt {
                return Err(StorageError::TransactionAborted {
                    reason: "OCC read-set version conflict detected at commit",
                });
            }
        }

        let now_ns = current_time_ns();
        let mut encoded_upserts: Vec<(InlineId, Option<InlineId>, T, Version, AlignedBytes)> =
            Vec::with_capacity(self.staged_upserts.len());
        let mut wal_ops = Vec::with_capacity(self.staged_upserts.len() + self.staged_deletes.len());

        for (id, (pg, rec)) in self.staged_upserts {
            let next_ver = self
                .engine
                .inner
                .primary
                .get(id)
                .map_or(Version::INITIAL, |e| e.version.next());
            let frame =
                RecordCodec::encode_aligned(&rec, next_ver, now_ns, RecordFlags::NONE, 0);
            wal_ops.push((
                T::COLLECTION_ID,
                WalOpKind::Upsert,
                id,
                next_ver,
                frame.as_slice().to_vec(),
            ));
            encoded_upserts.push((id, pg, rec, next_ver, frame));
        }

        for &id in &self.staged_deletes {
            let next_ver = self
                .engine
                .inner
                .primary
                .get(id)
                .map_or(Version::INITIAL, |e| e.version.next());
            wal_ops.push((
                T::COLLECTION_ID,
                WalOpKind::Delete,
                id,
                next_ver,
                Vec::new(),
            ));
        }

        // Write atomic multi-record commit batch to WAL before mutating in-memory state
        if let Some(ref wal) = self.engine.inner.wal {
            let txn_id = wal.allocate_txn_id();
            wal.append_transaction(txn_id, wal_ops)?;
        }

        // Apply all mutations atomically while still holding canonical stripe locks
        for (id, pg, rec, next_ver, frame) in encoded_upserts {
            self.engine
                .inner
                .apply_encoded_upsert_locked(id, pg, &rec, next_ver, now_ns, 0, frame)?;
        }
        for id in self.staged_deletes {
            if let Some(removed) = self.engine.inner.primary.remove(id) {
                if self.engine.inner.mvcc.has_active_snapshots() {
                    let old_frame = self.engine.inner.read_raw_frame_for_entry(&removed);
                    self.engine.inner.mvcc.seed_baseline_if_absent(
                        id,
                        removed.version,
                        old_frame,
                    );
                    self.engine
                        .inner
                        .mvcc
                        .record_version(id, removed.version.next(), None);
                }
                self.engine
                    .inner
                    .remove_from_secondary_indexes(&removed);
                self.engine.inner.primary.release_ordinal(removed.ordinal);
                self.engine.inner.mmap_pool.invalidate(id);
                match removed.location {
                    DiskLocation::DedicatedFile { parent_group, .. } => {
                        if let Ok(path) = self.engine.inner.paths.full_record_path(
                            &self.engine.inner.config.collection_name,
                            parent_group,
                            id,
                        ) {
                            let _ = fs::remove_file(path);
                        }
                    }
                    DiskLocation::Arena(loc) => {
                        if let Some(ref arena) = self.engine.inner.arena {
                            arena.mark_dead(loc);
                        }
                    }
                    DiskLocation::Unflushed => {}
                }
            }
        }

        Ok(())
    }
}

#[inline]
fn current_time_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<usize> {
    fs::create_dir_all(dst).map_err(|e| StorageError::io("create snapshot dir", e))?;
    let mut files_copied = 0usize;
    for entry in fs::read_dir(src).map_err(|e| StorageError::io("read source dir", e))? {
        let entry = entry.map_err(|e| StorageError::io("read dir entry", e))?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "lock") {
            continue;
        }
        let dest_path = dst.join(entry.file_name());
        if path.is_dir() {
            files_copied += copy_dir_recursive(&path, &dest_path)?;
        } else {
            fs::copy(&path, &dest_path).map_err(|e| StorageError::io("copy snapshot file", e))?;
            files_copied += 1;
        }
    }
    Ok(files_copied)
}
