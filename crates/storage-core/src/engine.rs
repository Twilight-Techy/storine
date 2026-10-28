//! Core `StorageEngine<T>` implementation (`TASK-013`..`TASK-027`).

use std::collections::VecDeque;
use std::fs;
use std::marker::PhantomData;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use storage_codec::{
    AlignedBytes, EncryptionKey, FrameProtection, KeyRing, RecordCodec, RecordView, ZeroCopyRecord,
};
use storage_codec::FileSeal;
use storage_index::RecordProtection;

use crate::decrypt_cache::DecryptedCache;
use storage_graph::{DanglingEdge, OnDelete};
use storage_index::{
    append_primary_checkpoint_delta_sealed, cosine_similarity_f32, delta_checkpoint_path, load_primary_checkpoint_with_stamp_sealed, save_primary_checkpoint_entries_sealed, BitmapTagIndex, CompressedBitmap, DiskLocation, ColdCheckpoint, HnswConfig, HnswVectorIndex, IndexDelta, IndexSnapshot, Indexable, NumericRangeIndex, PagedBTreeIndex, PagedBitmapIndex, PrimaryEntry, PrimaryIndex, RangeIndex, TagIndex, VectorTopKHit,
};
use storage_mmap::{
    atomic_write_replace, cleanup_temp_files, read_ping_pong_active, write_ping_pong_slot,
    ArenaLocation, ArenaManager, DirectoryLock, MmapGuard, MmapPool, MmapPoolConfig, MmapRegion,
    PathResolver, StackRelPath, DEFAULT_SEGMENT_CAPACITY,
};
use storage_types::{
    CachePadded, FastHashMap, FastHashSet, InlineId, RecordFlags, RecordHeader, RecordOrdinal,
    Result, StorageError, StripedCounter, Version, HEADER_SIZE,
};
use storage_wal::{GroupCommitWal, SyncPolicy, WalOpKind};

use crate::migration::SchemaRegistry;
use crate::mvcc::{MvccManager, ReadSnapshot};
use crate::query::QueryBuilder;
use crate::write_queue::{MultiRecordLockGuard, StripedLockTable};
use storage_wal::WalEntry;

/// Physical on-disk storage layout strategy (`F-CORE-04`, `F-CORE-05`, `F-CORE-06`, `F-CONC-05`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StorageLayoutMode {
    /// Each record is stored in its own sharded `.rec` file (with optional parent folder grouping)
    /// and updated via atomic `.tmp` + rename replacement.
    #[default]
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


/// Where the secondary (tag and numeric range) indexes live (`F-PIDX-01`, `F-PIDX-02`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IndexTier {
    /// In RAM, rebuilt from the records at every open. Fastest; RAM grows with the postings.
    #[default]
    Memory,
    /// Memory-mapped paged files (`indexes/<stem>_range.idx`, `indexes/<stem>_tags.idx`): RAM is
    /// bounded by the page directories plus the delta since the last checkpoint. Persisted at
    /// checkpoints and reused at open after a clean shutdown; rebuilt from the records after a
    /// crash (any WAL replay) or whenever they cannot be proven current.
    Paged,
}

/// Where the primary index (id -> location, ordinal table) lives (`F-PIDX-03`, `TASK-092`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PrimaryTier {
    /// Every entry and ordinal in RAM (`~100 B` per record), loaded from `primary.idx` at open.
    #[default]
    Memory,
    /// The state as of the last checkpoint lives in an immutable, page-structured generation file
    /// (`indexes/<stem>_primary.cold`); RAM holds only entries written, deleted or read-verified
    /// since then, plus the tier's page separators (`8 B` per 4 KiB page) and Bloom filter
    /// (~`10 bits` per record). Open maps the tier instead of loading every entry. Lookups that
    /// miss RAM read one page (decrypted on access when encryption is on).
    Cold,
}

/// Encryption at rest settings (`F-SEC-02`).
///
/// - **Encrypted (strict)**: `active_key: Some(id)`, `accept_unencrypted: false`. Every frame must be
///   sealed with a key in `keys`; plaintext frames are rejected (no downgrade).
/// - **Key rotation**: set the new `active_key`, keep the old key in `keys`, run
///   [`StorageEngine::reseal_all`], then drop the old key.
/// - **Turning encryption on for existing data**: `active_key: Some(id)` with
///   `accept_unencrypted: true`, run `reseal_all`, then switch to strict.
/// - **Turning it off**: `active_key: None`, `accept_unencrypted: true`, keep the old keys, run
///   `reseal_all` (frames are rewritten in plaintext), then remove the configuration.
///
/// `accept_unencrypted: true` is a migration-only state: while it is set, plaintext frames are
/// accepted unauthenticated.
#[derive(Clone, Debug)]
pub struct EncryptionConfig {
    pub keys: KeyRing,
    /// Key sealing new writes (`None`: new writes are plaintext).
    pub active_key: Option<u32>,
    /// Accept plaintext record frames (migration only).
    pub accept_unencrypted: bool,
}

/// Index-file protection derived from the encryption settings (`TASK-088`): index files are
/// sealed with the active key and, like record frames, plaintext ones are rejected unless
/// `accept_unencrypted` (no downgrade).
fn file_seal(enc: Option<&EncryptionConfig>) -> FileSeal<'_> {
    enc.map_or(FileSeal::PLAIN, |e| FileSeal {
        keys: Some(&e.keys),
        active_key: e.active_key,
        accept_plain: e.accept_unencrypted,
    })
}

impl EncryptionConfig {
    /// Strict encryption with a single key.
    pub fn strict(key_id: u32, key: EncryptionKey) -> Self {
        Self {
            keys: KeyRing::new().with_key(key_id, key),
            active_key: Some(key_id),
            accept_unencrypted: false,
        }
    }

    fn validate(&self) -> Result<()> {
        if let Some(id) = self.active_key {
            if self.keys.get(id).is_none() {
                return Err(StorageError::invalid_key("active encryption key id is not in the key ring"));
            }
        } else if !self.accept_unencrypted {
            return Err(StorageError::invalid_key(
                "encryption without an active key must accept unencrypted frames (decryption-only migration)",
            ));
        }
        Ok(())
    }
}

/// Outcome of [`StorageEngine::reseal_all`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ResealReport {
    /// Frames rewritten under the active key (or in plaintext when there is none).
    pub resealed: usize,
    /// Frames already in the target form.
    pub unchanged: usize,
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
    /// When `true`, WAL frames removed by checkpoints are archived under
    /// `wal/archive/<collection>/` instead of discarded. Required for Point-in-Time Recovery
    /// (`archive_wal_for_pitr`). Archived segments grow until pruned
    /// (`GroupCommitWal::prune_archived_segments_before_lsn`).
    pub wal_archive: bool,
    /// Storage tier of the secondary indexes (default [`IndexTier::Memory`]).
    pub index_tier: IndexTier,
    /// Storage tier of the primary index (default [`PrimaryTier::Memory`]). Switching tiers is
    /// supported in both directions; the next checkpoint converts the files.
    pub primary_tier: PrimaryTier,
    /// Encryption at rest (`F-SEC-02`, see [`EncryptionConfig`]): record frames written to arena
    /// segments, record files and the WAL are sealed with ChaCha20-Poly1305 under the active key,
    /// bound to their record id, key id and header metadata. Changeable at runtime with
    /// [`StorageEngine::set_encryption`] plus [`StorageEngine::reseal_all`].
    ///
    /// Index files are sealed with the active key too (`TASK-088`): `primary.idx` and its delta
    /// frames, `hnsw.idx`, and the paged secondary-index generations. Still visible on disk: file
    /// names and sizes, WAL frame headers (record ids, versions), and record file names in the
    /// dedicated-file layouts (derived from record ids).
    pub encryption: Option<EncryptionConfig>,
    /// Byte budget of the decrypted-frame cache used when `encryption` is set
    /// (default 256 MiB). Warm reads of cached records allocate nothing.
    pub decrypted_cache_bytes: usize,
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
            wal_archive: false,
            index_tier: IndexTier::Memory,
            primary_tier: PrimaryTier::Memory,
            encryption: None,
            decrypted_cache_bytes: 256 * 1024 * 1024,
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
    _record: PhantomData<fn() -> T>,
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

    /// Returns the zero-copy `RecordView<'_, T>` for this record with O(1) safe bounds checks
    /// (`RULE-SAFE-04`): the frame was fully validated when this `RecordRef` was created.
    #[inline]
    pub fn view(&self) -> RecordView<'_, T> {
        RecordCodec::decode_fast_or::<T>(self.raw_frame())
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
    pub fn get_slice<'a, A: storage_codec::ArchivePod>(
        &'a self,
        field: &storage_codec::ArchivedSlice<A>,
    ) -> &'a [A] {
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
        self.view().archived()
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
    range_index: RangeIndex,
    tag_index: TagIndex,
    /// Serializes full checkpoints (`save_checkpoint_files`).
    checkpoint_serial: Mutex<()>,
    /// `true` if paged secondary indexes were reused at open instead of rebuilt.
    indexes_reused: AtomicBool,
    /// Decrypted, validated frames of encrypted records (`F-SEC-02`).
    decrypted: DecryptedCache,
    /// Current encryption settings (swappable at runtime).
    crypto: RwLock<Option<Arc<EncryptionConfig>>>,
    /// Fast-path flag: `true` while any encryption configuration is installed.
    crypto_on: AtomicBool,
    vector_index: HnswVectorIndex,
    mvcc: MvccManager,
    migrations: RwLock<SchemaRegistry<T>>,
    flush_queue: Mutex<CoalescingQueue>,
    flush_cv: Condvar,
    shutdown: AtomicBool,
    total_reads: StripedCounter,
    total_writes: StripedCounter,
    coalesced_flushes: CachePadded<AtomicU64>,
    /// Ordinals of records carrying a TTL, so queries can exclude expired records cheaply.
    ttl_ordinals: RwLock<CompressedBitmap>,
    /// `<base>/indexes/<collection>_primary.idx`.
    idx_path: PathBuf,
    /// Manifest of the cold primary tier (`indexes/<stem>_primary.cold`, `TASK-092`).
    cold_manifest: PathBuf,
    /// Persisted HNSW graph (`indexes/<stem>_hnsw.idx`, `F-HNSW-02`), saved at checkpoints.
    hnsw_path: PathBuf,
    /// `HnswVectorIndex::generation` at the last save (`u64::MAX` = never saved).
    hnsw_saved_generation: AtomicU64,
    /// Archive directory for WAL frames removed by checkpoints (`EngineConfig::wal_archive`).
    wal_archive_dir: Option<PathBuf>,
    /// `(id, path)` of deleted record files whose removal failed (e.g. still mapped on Windows).
    pending_file_removals: Mutex<Vec<(InlineId, PathBuf)>>,
    /// Highest version ever deleted in this collection. A re-created record starts above it, so
    /// a version number is never reused for a key (no OCC/CAS ABA after delete + re-insert).
    version_floor: CachePadded<AtomicU64>,
    /// `<base>/<stem>.vfloor` (a single number, not secret), saved with every checkpoint.
    floor_path: PathBuf,
    /// Set while the engine is a Raft replica: direct writes are refused (`ReadOnlyReplica`);
    /// only the consensus apply path writes.
    replica: AtomicBool,
}

/// Percent-encodes `name` into a single safe path component (no separators, `..` or `:`).
pub(crate) fn collection_file_stem(name: &str) -> Result<String> {
    if name.is_empty() {
        return Err(StorageError::invalid_key("collection_name cannot be empty"));
    }
    let mut buf = StackRelPath::new();
    buf.push_safe_component(name)?;
    Ok(buf.as_str().to_string())
}

/// Moves legacy `segments/seg_*.dat` files (pre per-collection layout) into `seg_dir`.
fn migrate_legacy_segments(legacy_dir: &Path, seg_dir: &Path) -> Result<()> {
    if seg_dir.exists() || !legacy_dir.exists() {
        return Ok(());
    }
    let mut legacy = Vec::new();
    if let Ok(entries) = fs::read_dir(legacy_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let is_seg = path.is_file()
                && path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("seg_") && n.ends_with(".dat"));
            if is_seg {
                legacy.push(path);
            }
        }
    }
    if legacy.is_empty() {
        return Ok(());
    }
    fs::create_dir_all(seg_dir).map_err(|e| StorageError::io("create arena segment dir", e))?;
    for path in legacy {
        if let Some(name) = path.file_name() {
            fs::rename(&path, seg_dir.join(name))
                .map_err(|e| StorageError::io("migrate legacy arena segment", e))?;
        }
    }
    Ok(())
}

/// Rejects snapshot/restore destinations that are non-empty or located inside `base_dir`, so
/// a backup can never delete or overwrite existing data or recurse into itself.
pub(crate) fn ensure_fresh_destination(dest: &Path, base_dir: Option<&Path>) -> Result<()> {
    if dest.exists() {
        if !dest.is_dir() {
            return Err(StorageError::invalid_key(
                "destination exists and is not a directory",
            ));
        }
        let mut entries =
            fs::read_dir(dest).map_err(|e| StorageError::io("read destination directory", e))?;
        if entries.next().is_some() {
            return Err(StorageError::invalid_key(
                "destination directory must be empty (refusing to overwrite existing data)",
            ));
        }
    }
    if let Some(base) = base_dir {
        let canon_base = fs::canonicalize(base).unwrap_or_else(|_| base.to_path_buf());
        // Canonicalize the deepest existing ancestor of `dest`.
        let mut probe = dest.to_path_buf();
        let mut suffix = Vec::new();
        while !probe.exists() {
            match (probe.file_name().map(|n| n.to_os_string()), probe.parent()) {
                (Some(name), Some(parent)) => {
                    suffix.push(name);
                    probe = parent.to_path_buf();
                }
                _ => break,
            }
        }
        let mut canon_dest = fs::canonicalize(&probe).unwrap_or(probe);
        for name in suffix.into_iter().rev() {
            canon_dest.push(name);
        }
        if canon_dest.starts_with(&canon_base) {
            return Err(StorageError::invalid_key(
                "destination must not be inside the database directory",
            ));
        }
    }
    Ok(())
}

/// Hot-read source resolved under the primary-index shard lock and used after releasing it.
enum FastRead {
    Dirty(AlignedBytes),
    Arena(Arc<MmapRegion>, ArenaLocation),
    Mapped(MmapGuard),
    Cold,
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
        let stem = collection_file_stem(&config.collection_name)?;
        if let Some(ref enc) = config.encryption {
            enc.validate()?;
        }

        // Acquire OS-level exclusive lock on `<base_dir>/<collection>.lock` (`TASK-059`)
        let lock_path = base_dir.join(format!("{stem}.lock"));
        let dir_lock = DirectoryLock::try_acquire(&lock_path)?;
        let shared_wal_mode = shared_wal.is_some();

        // Clean up orphaned `.tmp` files left from a prior power loss (`F-DURA-04`). In a shared
        // catalog directory other collections may be writing right now, so the catalog performs
        // this cleanup once before any collection opens.
        if !shared_wal_mode {
            let _ = cleanup_temp_files(&base_dir)?;
        }

        let paths = PathResolver::new(&base_dir);
        let primary = PrimaryIndex::new();
        let mmap_pool = MmapPool::new(config.mmap_pool);

        let arena = match config.layout_mode {
            StorageLayoutMode::AdaptiveArena {
                segment_capacity, ..
            } => {
                let seg_root = base_dir.join("segments");
                let seg_dir = seg_root.join(&stem);
                migrate_legacy_segments(&seg_root, &seg_dir)?;
                Some(ArenaManager::open(
                    seg_dir,
                    segment_capacity.max(DEFAULT_SEGMENT_CAPACITY),
                )?)
            }
            _ => None,
        };

        // Load primary index checkpoint if present
        let idx_path = base_dir.join("indexes").join(format!("{stem}_primary.idx"));
        let cold_manifest = base_dir.join("indexes").join(format!("{stem}_primary.cold"));
        let open_seal = file_seal(config.encryption.as_ref());
        let primary_stamp = load_primary_state(&primary, &idx_path, &cold_manifest, config.primary_tier, &open_seal)?;
        let floor_path = base_dir.join(format!("{stem}.vfloor"));
        let version_floor = load_version_floor(&floor_path)?;

        // Secondary indexes. The paged tier may reuse its files only if they were written by the
        // same checkpoint as the primary index (equal stamps) and nothing happened afterwards (no
        // WAL frames to replay, checked below); otherwise they are rebuilt from the records.
        let (range_index, tag_index, paged_stamp_matches) = match config.index_tier {
            IndexTier::Memory => (
                RangeIndex::Memory(NumericRangeIndex::new()),
                TagIndex::Memory(BitmapTagIndex::new()),
                false,
            ),
            IndexTier::Paged => {
                let range_path = base_dir.join("indexes").join(format!("{stem}_range.idx"));
                let tags_path = base_dir.join("indexes").join(format!("{stem}_tags.idx"));
                let range = PagedBTreeIndex::open_sealed(&range_path, &open_seal).ok();
                let tags = PagedBitmapIndex::open_sealed(&tags_path, &open_seal).ok();
                let matches = match (&range, &tags, primary_stamp) {
                    (Some(r), Some(t), Some(stamp)) => {
                        stamp != 0 && r.stamp() == stamp && t.stamp() == stamp
                    }
                    _ => false,
                };
                let (range, tags) = match (range, tags) {
                    (Some(r), Some(t)) if matches => (r, t),
                    _ => (
                        fresh_paged_btree(&range_path, &open_seal)?,
                        fresh_paged_bitmap(&tags_path, &open_seal)?,
                    ),
                };
                (RangeIndex::Paged(range), TagIndex::Paged(tags), matches)
            }
        };

        // Load the persisted HNSW graph (`F-HNSW-02`). It is reconciled against the records in
        // `rebuild_secondary_indexes`, so a missing, stale or corrupted file only costs rebuild work.
        let hnsw_path = base_dir.join("indexes").join(format!("{stem}_hnsw.idx"));
        let (vector_index, hnsw_saved_generation) =
            match HnswVectorIndex::load_from_disk_sealed(&hnsw_path, HnswConfig::default(), &open_seal) {
                Ok(loaded) if hnsw_path.exists() => {
                    let generation = loaded.generation();
                    (loaded, generation)
                }
                _ => (HnswVectorIndex::new(HnswConfig::default()), u64::MAX),
            };

        let wal_dir = base_dir.join("wal");
        let wal_path = wal_dir.join(format!("{stem}.wal"));
        let wal_archive_dir = config
            .wal_archive
            .then(|| wal_dir.join("archive").join(&stem));

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
            range_index,
            tag_index,
            checkpoint_serial: Mutex::new(()),
            indexes_reused: AtomicBool::new(false),
            decrypted: DecryptedCache::new(config.decrypted_cache_bytes),
            crypto: RwLock::new(config.encryption.clone().map(Arc::new)),
            crypto_on: AtomicBool::new(config.encryption.is_some()),
            vector_index,
            mvcc: MvccManager::new(),
            migrations: RwLock::new(SchemaRegistry::new()),
            flush_queue: Mutex::new(CoalescingQueue::default()),
            flush_cv: Condvar::new(),
            shutdown: AtomicBool::new(false),
            total_reads: StripedCounter::new(),
            total_writes: StripedCounter::new(),
            coalesced_flushes: CachePadded::new(AtomicU64::new(0)),
            ttl_ordinals: RwLock::new(CompressedBitmap::new()),
            idx_path: idx_path.clone(),
            cold_manifest,
            hnsw_path,
            hnsw_saved_generation: AtomicU64::new(hnsw_saved_generation),
            wal_archive_dir,
            pending_file_removals: Mutex::new(Vec::new()),
            version_floor: CachePadded::new(AtomicU64::new(version_floor)),
            floor_path,
            replica: AtomicBool::new(false),
        });

        // Replay committed WAL frames of a dedicated WAL (`F-DURA-04`). Every replayed record is
        // flushed with `fsync` and checkpointed before the WAL is truncated below.
        let mut replayed = false;
        // Paged indexes are current only after a clean shutdown: a shared catalog WAL is replayed
        // later by the catalog, and any recovered frame means a crash.
        let mut paged_indexes_current = paged_stamp_matches && !shared_wal_mode;
        if !shared_wal_mode {
            let recovered = GroupCommitWal::recover_committed(&wal_path)?;
            if !recovered.is_empty() {
                paged_indexes_current = false;
                if inner.range_index.as_paged().is_some() {
                    inner.reset_paged_indexes()?;
                }
                inner.replay_wal_entries(&recovered, false)?;
                if let Some(ref arena) = inner.arena {
                    arena.sync_all()?;
                }
                let cut = inner.primary_cut();
                inner.save_primary_files(cut, 0)?;
                replayed = true;
            }
        }

        // Open the WAL for active writes (or attach the shared catalog WAL)
        let wal = if let Some(shared) = shared_wal {
            Some(shared)
        } else if config.enable_wal || replayed {
            let w = GroupCommitWal::open(&wal_path, config.wal_sync_policy)?;
            if replayed {
                w.checkpoint_truncate_before(
                    w.current_lsn(),
                    &|_| false,
                    inner.wal_archive_dir.as_deref(),
                )?;
            }
            config.enable_wal.then(|| Arc::new(w))
        } else {
            None
        };

        let inner = {
            let mut inner_mut = Arc::try_unwrap(inner).map_err(|_| {
                StorageError::resource_exhausted("engine state unexpectedly shared during open")
            })?;
            inner_mut.wal = wal;
            Arc::new(inner_mut)
        };

        // Arena segments referenced by no record (left over from compaction whose deletion
        // failed, or superseded by replay) are reclaimed.
        inner.remove_unreferenced_segments();

        // Rebuild the secondary indexes from the records unless reusable paged indexes were
        // loaded (TTL bitmap and vector graph are always reconciled).
        let rebuild_postings = !paged_indexes_current;
        inner
            .indexes_reused
            .store(paged_indexes_current, Ordering::Relaxed);
        if rebuild_postings && inner.range_index.as_paged().is_some() && !replayed {
            inner.reset_paged_indexes()?;
        }
        inner.rebuild_secondary_indexes(rebuild_postings)?;
        if rebuild_postings && inner.range_index.as_paged().is_some() {
            // Persist the rebuilt indexes now (no writers yet), so the next clean open reuses them.
            inner.save_checkpoint_files()?;
        }

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
    /// WAL body for an upsert of `id` (sealed when encryption at rest is enabled).
    pub(crate) fn wal_frame_bytes(&self, id: InlineId, frame: &[u8]) -> Result<Vec<u8>> {
        self.inner.wal_frame_bytes(id, frame)
    }

    /// Bytes currently held by the decrypted-frame cache (`0` without encryption).
    pub fn decrypted_cache_bytes(&self) -> usize {
        self.inner.decrypted.bytes()
    }

    /// Installs new encryption settings at runtime (`None` removes encryption entirely; only do
    /// that once [`Self::reseal_all`] has rewritten every frame in plaintext). Takes effect for
    /// every subsequent read and write; existing frames are converted by [`Self::reseal_all`].
    pub fn set_encryption(&self, config: Option<EncryptionConfig>) -> Result<()> {
        if let Some(ref c) = config {
            c.validate()?;
        }
        let on = config.is_some();
        // Paged index generations written from now on follow the new settings (`TASK-088`).
        let seal = file_seal(config.as_ref());
        if let Some(range) = self.inner.range_index.as_paged() {
            range.set_file_seal(&seal);
        }
        if let Some(tags) = self.inner.tag_index.as_paged() {
            tags.set_file_seal(&seal);
        }
        *self
            .inner
            .crypto
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = config.map(Arc::new);
        self.inner.crypto_on.store(on, Ordering::Release);
        Ok(())
    }

    /// Rewrites every on-disk record frame into the form the current settings require: sealed
    /// with the active key, or plaintext when there is none (key rotation, turning encryption on
    /// or off). Runs online: each record is converted under its own stripe lock, logged to the WAL
    /// (same version: the content is unchanged, so readers and OCC versions are unaffected), and
    /// flushed; a final checkpoint drops WAL frames sealed with the old keys.
    ///
    /// WAL segments archived for PITR keep their original keys: retain those keys while the
    /// archive is needed.
    pub fn reseal_all(&self) -> Result<ResealReport> {
        let mut report = ResealReport::default();
        let Some(enc) = self.inner.crypto() else {
            return Ok(report);
        };
        let target = enc.active_key;
        for entry in self.inner.primary.entries_snapshot() {
            // `target` / policy decide each record's form (see `desired_protection`).
            let _lock = self.inner.locks.lock_one(entry.id);
            let Some(cur) = self.inner.primary.get(entry.id) else {
                continue;
            };
            if cur.hot_dirty.is_some() {
                // Will be sealed with the active key when flushed.
                report.unchanged += 1;
                continue;
            }
            let Some(raw) = self.inner.read_disk_bytes_for_entry(&cur) else {
                continue;
            };
            let current = RecordCodec::frame_protection(raw.as_slice());
            let plain = RecordCodec::unprotect_frame(&enc.keys, cur.id, raw.as_slice())?;
            if current == self.inner.desired_protection(target, plain.as_slice()) {
                report.unchanged += 1;
                continue;
            }
            if let Some(ref wal) = self.inner.wal {
                let sealed = self.inner.seal_for_disk(cur.id, plain.as_slice())?;
                let body = sealed.as_ref().map_or(plain.as_slice(), |b| b.as_slice());
                wal.append_single(T::COLLECTION_ID, WalOpKind::Upsert, cur.id, cur.version, body)?;
            }
            let ping_pong = matches!(
                cur.location,
                DiskLocation::DedicatedFile { ping_pong_stride, .. } if ping_pong_stride > 0
            );
            let sync = self.inner.data_sync();
            // A ping-pong file keeps the previous version in its other slot: write twice so both
            // slots hold the new form.
            for _ in 0..if ping_pong { 2 } else { 1 } {
                self.inner.primary.upsert(PrimaryEntry {
                    hot_dirty: Some(plain.clone()),
                    ..cur.clone()
                });
                self.inner.flush_single_record_impl(cur.id, sync, true, true)?;
            }
            report.resealed += 1;
        }
        // The final checkpoint rewrites the index files in the new form (`TASK-088`); force the
        // vector graph to be rewritten even if it did not change.
        self.inner.hnsw_saved_generation.store(u64::MAX, Ordering::Release);
        self.flush_all()?;
        // Superseded frames stay in dead arena slots until reclaimed: evacuate every segment so
        // no copy in the old form (plaintext or an old key) remains in any segment file. Segment
        // dead-byte counters do not survive restarts, so every segment is rewritten.
        if report.resealed > 0 {
            if let Some(ref arena) = self.inner.arena {
                self.evacuate_segments(arena.segment_ids())?;
            }
        }
        Ok(report)
    }

    /// Ids of every live record (each shard read at its own instant).
    pub(crate) fn live_ids(&self) -> Vec<InlineId> {
        self.inner.primary.ids_snapshot()
    }

    /// `true` if this open reused persisted paged secondary indexes (`IndexTier::Paged` after a
    /// clean shutdown) instead of rebuilding them from the records.
    pub fn secondary_indexes_reused(&self) -> bool {
        self.inner.indexes_reused.load(Ordering::Relaxed)
    }

    pub fn range_index(&self) -> &RangeIndex {
        &self.inner.range_index
    }

    #[inline]
    pub fn tag_index(&self) -> &TagIndex {
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

    /// Directory receiving WAL frames removed by checkpoints, when `EngineConfig::wal_archive` is set.
    #[inline]
    pub fn wal_archive_dir(&self) -> Option<&Path> {
        self.inner.wal_archive_dir.as_deref()
    }

    /// Copies the current active WAL (frames not yet checkpointed) to `dest`.
    pub fn copy_active_wal(&self, dest: &Path) -> Result<bool> {
        if self.inner.shared_wal_mode {
            return Ok(false);
        }
        match self.inner.wal {
            Some(ref wal) => {
                wal.copy_active_to(dest)?;
                Ok(true)
            }
            None => Ok(false),
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
        self.inner.ensure_writable()?;
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
        self.inner.total_reads.add(1);

        // Resolve the backing bytes under the shard read lock, but run `f` only after releasing
        // it (Arc bumps only, 0 heap allocations), so `f` may freely call back into the engine.
        let resolved = self.inner.primary.with_entry(id, |entry| -> Result<FastRead> {
            if entry.ttl_expires_at_ns > 0 && current_time_ns() >= entry.ttl_expires_at_ns {
                return Err(StorageError::not_found(id));
            }
            if let Some(ref dirty) = entry.hot_dirty {
                let compressed = RecordCodec::header_ref(dirty.as_slice())
                    .map_or(true, |h| h.flags_typed().contains(RecordFlags::COMPRESSED_LZ4));
                if !compressed {
                    return Ok(FastRead::Dirty(dirty.clone()));
                }
            }
            if self.inner.crypto_on.load(Ordering::Acquire) {
                // Ciphertext is never read in place: serve the cached plaintext or go cold.
                return Ok(match self.inner.decrypted.get(id, entry.version) {
                    Some(plain) => FastRead::Dirty(plain),
                    None => FastRead::Cold,
                });
            }
            match entry.location {
                DiskLocation::Arena(loc) if entry.disk_verified => {
                    if let Some(ref arena) = self.inner.arena {
                        return Ok(FastRead::Arena(arena.get_region(loc.segment_id)?, loc));
                    }
                }
                DiskLocation::DedicatedFile {
                    ping_pong_stride: 0,
                    ..
                } => {
                    if let Some(guard) = self.inner.mmap_pool.get_if_mapped(id) {
                        if guard.is_verified() {
                            return Ok(FastRead::Mapped(guard));
                        }
                    }
                }
                _ => {}
            }
            Ok(FastRead::Cold)
        });

        match resolved.ok_or_else(|| StorageError::not_found(id))?? {
            // O(1) safe decode: the frame was fully validated once (encode / `validate_frame`);
            // holding `region` / `guard` keeps the bytes mapped and blocks in-place slot reuse.
            FastRead::Dirty(bytes) => Ok(f(RecordCodec::decode_fast::<T>(bytes.as_slice())?)),
            FastRead::Arena(region, loc) => {
                let slice = region.subslice_aligned(loc.offset as usize, loc.len as usize)?;
                Ok(f(RecordCodec::decode_fast::<T>(slice)?))
            }
            FastRead::Mapped(guard) => Ok(f(RecordCodec::decode_fast::<T>(guard.as_slice())?)),
            FastRead::Cold => {
                // Cold path: map from disk, validate once, and invoke `f`
                let rec_ref = self.inner.get_ref_internal(id)?;
                Ok(f(rec_ref.view()))
            }
        }
    }

    /// Returns a zero-copy `RecordRef<T>` guard for `id` (`F-CORE-01`).
    pub fn get_ref(&self, id: InlineId) -> Result<RecordRef<T>> {
        self.inner.total_reads.add(1);
        self.inner.get_ref_internal(id)
    }

    /// Materializes an owned copy of `T` (automatically running schema migration or cold LZ4 decompression if needed).
    pub fn get_owned(&self, id: InlineId) -> Result<(Version, T)> {
        self.inner.total_reads.add(1);
        self.inner.get_owned_internal(id)
    }

    /// Marks the engine as a Raft replica (`true`): every direct write API then fails with
    /// [`StorageError::ReadOnlyReplica`] and only the consensus apply path can change data.
    pub fn set_replica_mode(&self, replica: bool) {
        self.inner.replica.store(replica, Ordering::Release);
    }

    /// `true` while the engine only accepts writes from Raft consensus.
    pub fn is_replica(&self) -> bool {
        self.inner.replica.load(Ordering::Acquire)
    }

    /// Version the next upsert of `id` receives (monotonic across delete + re-create).
    #[inline]
    pub(crate) fn next_write_version(&self, id: InlineId) -> Version {
        self.inner.next_version(self.inner.primary.version_of(id))
    }

    /// Inserts a new record (`Err(RecordAlreadyExists)` if `id` already exists).
    pub fn insert(&self, id: InlineId, record: &T) -> Result<Version> {
        self.inner.ensure_writable()?;
        let _lock = self.inner.locks.lock_one(id);
        let now_ns = current_time_ns();
        let mut version = self.inner.next_version(None);
        if let Some(existing) = self.inner.primary.get(id) {
            if existing.ttl_expires_at_ns == 0 || now_ns < existing.ttl_expires_at_ns {
                return Err(StorageError::already_exists(id));
            }
            // Replacing an expired record keeps the version monotonic (no OCC ABA).
            version = existing.version.next();
        }
        self.inner
            .write_record_locked(id, None, record, version, now_ns, 0, RecordFlags::NONE)
    }

    /// Inserts or overwrites a record under `id` (optionally grouped under `parent_group` folder).
    pub fn put_grouped(
        &self,
        id: InlineId,
        parent_group: Option<InlineId>,
        record: &T,
    ) -> Result<Version> {
        self.inner.ensure_writable()?;
        let _lock = self.inner.locks.lock_one(id);
        let now_ns = current_time_ns();
        let next_ver = self.inner.next_version(self.inner.primary.version_of(id));
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
        self.inner.ensure_writable()?;
        let _lock = self.inner.locks.lock_one(id);
        let now_ns = current_time_ns();
        let expires_ns = now_ns.saturating_add(ttl.as_nanos() as u64);
        let next_ver = self.inner.next_version(self.inner.primary.version_of(id));
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
        self.inner.ensure_writable()?;
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
        self.inner.ensure_writable()?;
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
        let next_ver = self
            .inner
            .next_version((cur_ver.0 != 0).then_some(cur_ver));
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
        self.inner.ensure_writable()?;
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
        self.inner.ensure_writable()?;
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
        self.inner.ensure_writable()?;
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
        self.inner.ensure_writable()?;
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
                        self.inner.wal_frame_bytes(id_a, frame_a.as_slice())?,
                    ),
                    (
                        T::COLLECTION_ID,
                        WalOpKind::Upsert,
                        id_b,
                        next_b,
                        self.inner.wal_frame_bytes(id_b, frame_b.as_slice())?,
                    ),
                ],
            )?;
        }

        let _mvcc_gate = self.inner.mvcc.writer_guard();
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
        self.inner.ensure_writable()?;
        // The set of records to lock depends on record contents, which may change until locked:
        // compute it, lock it, recompute it under the locks, and retry if it grew.
        let compute_set = |policy: OnDelete| -> Result<Vec<InlineId>> {
            let (_, target) = self.get_owned(id)?;
            let mut set = vec![id];
            match policy {
                OnDelete::Restrict | OnDelete::Unlink => set.extend(extract_related(&target)),
                OnDelete::Cascade => {
                    let mut visited = FastHashSet::default();
                    visited.insert(id);
                    let mut queue = VecDeque::new();
                    queue.push_back(target);
                    while let Some(rec) = queue.pop_front() {
                        for child in extract_related(&rec) {
                            if visited.insert(child) {
                                set.push(child);
                                if let Ok((_, child_rec)) = self.get_owned(child) {
                                    queue.push_back(child_rec);
                                }
                            }
                        }
                    }
                }
            }
            set.sort_unstable();
            set.dedup();
            Ok(set)
        };

        let mut locked_set = compute_set(policy)?;
        let mut attempts = 0usize;
        let (_guards, related) = loop {
            let guards = self.inner.locks.lock_many(&locked_set);
            let current = compute_set(policy)?;
            if current.iter().all(|k| locked_set.binary_search(k).is_ok()) {
                break (guards, current);
            }
            drop(guards);
            attempts += 1;
            if attempts >= 16 {
                return Err(StorageError::TransactionAborted {
                    reason: "relationship set kept changing while acquiring locks",
                });
            }
            locked_set.extend(current);
            locked_set.sort_unstable();
            locked_set.dedup();
        };
        let peers: Vec<InlineId> = related.into_iter().filter(|&k| k != id).collect();

        match policy {
            OnDelete::Restrict => {
                for &peer_id in &peers {
                    if self.inner.live_version(peer_id).is_some() {
                        return Err(StorageError::integrity_violation(
                            id,
                            peer_id,
                            "cannot delete record while related records exist (OnDelete::Restrict)",
                        ));
                    }
                }
                Ok(usize::from(self.inner.delete_record_locked(id)?))
            }
            OnDelete::Unlink => {
                // Unlink every live peer and delete `id` in ONE atomic WAL transaction.
                let now_ns = current_time_ns();
                let mut staged = Vec::with_capacity(peers.len());
                for &peer_id in &peers {
                    if let Ok((peer_ver, mut draft, pg, ttl, flags)) =
                        self.inner.load_mutable_draft_locked(peer_id)
                    {
                        unlink_from_peer(&mut draft, id);
                        let next = peer_ver.next();
                        let frame = RecordCodec::encode_aligned(&draft, next, now_ns, flags, ttl);
                        staged.push((peer_id, pg, draft, next, ttl, frame));
                    }
                }
                let Some(target_ver) = self.inner.live_version(id) else {
                    return Ok(0);
                };
                if let Some(ref wal) = self.inner.wal {
                    let mut ops = Vec::with_capacity(staged.len() + 1);
                    for (peer_id, _, _, next, _, frame) in &staged {
                        ops.push((
                            T::COLLECTION_ID,
                            WalOpKind::Upsert,
                            *peer_id,
                            *next,
                            self.inner.wal_frame_bytes(*peer_id, frame.as_slice())?,
                        ));
                    }
                    ops.push((
                        T::COLLECTION_ID,
                        WalOpKind::Delete,
                        id,
                        target_ver.next(),
                        now_ns.to_le_bytes().to_vec(),
                    ));
                    wal.append_transaction(wal.allocate_txn_id(), ops)?;
                }
                let _mvcc_gate = self.inner.mvcc.writer_guard();
                for (peer_id, pg, draft, next, ttl, frame) in staged {
                    self.inner
                        .apply_encoded_upsert_locked(peer_id, pg, &draft, next, now_ns, ttl, frame)?;
                }
                Ok(usize::from(self.inner.remove_record_after_wal_locked(id)))
            }
            OnDelete::Cascade => {
                let mut all: Vec<(InlineId, Version)> = Vec::with_capacity(peers.len() + 1);
                for k in core::iter::once(id).chain(peers.iter().copied()) {
                    if let Some(v) = self.inner.live_version(k) {
                        all.push((k, v));
                    }
                }
                if all.is_empty() {
                    return Ok(0);
                }
                if let Some(ref wal) = self.inner.wal {
                    let now_ns = current_time_ns();
                    let ops = all
                        .iter()
                        .map(|&(k, v)| {
                            (
                                T::COLLECTION_ID,
                                WalOpKind::Delete,
                                k,
                                v.next(),
                                now_ns.to_le_bytes().to_vec(),
                            )
                        })
                        .collect();
                    wal.append_transaction(wal.allocate_txn_id(), ops)?;
                }
                let _mvcc_gate = self.inner.mvcc.writer_guard();
                let mut count = 0usize;
                for (k, _) in all {
                    if self.inner.remove_record_after_wal_locked(k) {
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
    ///
    /// `O(#TTL records)`: walks the TTL ordinal bitmap instead of the whole primary index. Expiry
    /// is re-checked under each record's stripe lock, so a key rewritten without a TTL after the
    /// scan is never deleted.
    pub fn sweep_expired(&self) -> Result<usize> {
        self.inner.ensure_writable()?;
        let Some(expired) = self.inner.expired_ordinals() else {
            return Ok(0);
        };
        let mut expired_count = 0usize;
        for ord in expired.to_vec() {
            let Some(id) = self.inner.primary.resolve_ordinal(ord) else {
                continue;
            };
            let _lock = self.inner.locks.lock_one(id);
            if self.inner.live_version(id).is_none() && self.inner.delete_record_locked(id)? {
                expired_count += 1;
            }
        }
        Ok(expired_count)
    }

    /// Flushes all pending coalesced dirty records to disk and writes an updated `primary.idx` checkpoint.
    pub fn flush_all(&self) -> Result<usize> {
        // Pick the WAL cutoff first, then wait out in-flight writes: every write whose WAL frame
        // has `lsn <= cutoff` holds its stripe lock until applied, so after the barrier all of
        // them are in `hot_dirty` and will be drained below. Later writes keep their WAL frames.
        let cutoff = self.inner.wal.as_ref().map_or(0, |w| w.current_lsn());
        self.inner.locks.barrier();
        let flushed = self.inner.drain_and_flush_all(false)?;
        self.inner.checkpoint_and_truncate(cutoff)?;
        Ok(flushed)
    }

    /// Makes every acknowledged write durable on stable storage regardless of the configured
    /// `SyncPolicy` (`fsync` of the WAL; a full [`Self::flush_all`] when the engine has no WAL).
    pub fn sync_wal(&self) -> Result<()> {
        match self.inner.wal {
            Some(ref wal) => wal.sync_to_disk(),
            None => self.flush_all().map(|_| ()),
        }
    }

    /// Full durable checkpoint (alias of [`Self::flush_all`]).
    #[inline]
    pub fn checkpoint(&self) -> Result<usize> {
        self.flush_all()
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
            let _ = self.inner.flush_single_record_to_disk(id, true)?;
        }
        if let Some(ref arena) = self.inner.arena {
            arena.sync_all()?;
        }
        let idx_path = self.inner.idx_path.clone();
        let enc = self.inner.crypto();
        self.inner.persist_version_floor()?;
        let written = append_primary_checkpoint_delta_sealed(
            &self.inner.primary,
            &idx_path,
            upserted_ids,
            deleted_ids,
            &file_seal(enc.as_deref()),
        )?;
        let delta_path = delta_checkpoint_path(&idx_path);
        if let Ok(meta) = fs::metadata(&delta_path) {
            if meta.len() >= 1024 * 1024 {
                self.inner.save_checkpoint_files()?;
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
        self.evacuate_segments(frag_ids)
    }

    /// Moves every live record out of `frag_ids` into a fresh segment and deletes those segment
    /// files (also used by [`Self::reseal_all`] to scrub superseded frames).
    fn evacuate_segments(&self, frag_ids: Vec<u32>) -> Result<usize> {
        let Some(ref arena) = self.inner.arena else {
            return Ok(0);
        };
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
                    // Encrypted frames: authenticate (decrypt) before trusting them; migrations
                    // decode the plaintext and re-seal the upgraded frame.
                    let plain = match self.inner.crypto() {
                        Some(ref enc)
                            if RecordCodec::frame_protection(slice) != FrameProtection::Plain =>
                        {
                            Some(RecordCodec::unprotect_frame(&enc.keys, cur.id, slice)?)
                        }
                        Some(ref enc) if !enc.accept_unencrypted => {
                            return Err(StorageError::invalid_archive(
                                "plaintext record frame found in an encrypted database",
                            ));
                        }
                        _ => None,
                    };
                    let plain_slice = plain.as_ref().map_or(slice, |p| p.as_slice());
                    if raw_hdr.schema_version != T::SCHEMA_VERSION {
                        let (_, upgraded) = self.inner.decode_or_migrate_frame(plain_slice)?;
                        let flags = raw_hdr
                            .flags_typed()
                            .without(RecordFlags::COMPRESSED_LZ4)
                            .without(RecordFlags::ENCRYPTED)
                            .without(RecordFlags::AUTHENTICATED);
                        let reencoded = RecordCodec::encode_aligned(
                            &upgraded,
                            cur.version,
                            current_time_ns(),
                            flags,
                            cur.ttl_expires_at_ns,
                        );
                        let sealed = self.inner.seal_for_disk(cur.id, reencoded.as_slice())?;
                        let to_write = sealed.as_ref().map_or(reencoded.as_slice(), |s| s.as_slice());
                        let new_loc = arena.write_record(to_write, false)?;
                        self.inner.decrypted.remove(cur.id);
                        self.inner.primary.upsert(PrimaryEntry {
                            id: cur.id,
                            ordinal: cur.ordinal,
                            version: cur.version,
                            ttl_expires_at_ns: cur.ttl_expires_at_ns,
                            location: DiskLocation::Arena(new_loc),
                            disk_verified: sealed.is_none(),
                            hot_dirty: None,
                        });
                    } else {
                        let hdr = RecordCodec::validate_frame::<T>(plain_slice)?;
                        // Ciphertext is copied unchanged: it is bound to the record id, not to
                        // its location.
                        let new_loc = arena.write_record(slice, false)?;
                        let verified = plain.is_none()
                            && !hdr.is_tombstone()
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

        // Evacuated frames must be durable before the checkpoint stops referencing old segments.
        arena.sync_all()?;
        self.inner.save_checkpoint_files()?;
        Ok(removed_segments)
    }

    /// Creates a consistent point-in-time snapshot of the entire database at `dest_dir` (`TASK-027`, `F-OPS-04`).
    pub fn create_snapshot(&self, dest_dir: &Path) -> Result<usize> {
        Ok(self.create_snapshot_with_lsn(dest_dir)?.0)
    }

    /// Creates a consistent snapshot copy of the database directory at `dest_dir` and returns
    /// `(files_copied, wal_lsn)`, where every operation with `lsn <= wal_lsn` is in the copy.
    ///
    /// Writers are paused (all stripe locks held) only while dirty records are flushed and the
    /// files are copied; readers continue. `dest_dir` must be empty or absent and must not be
    /// inside the database directory (existing data is never deleted).
    pub fn create_snapshot_with_lsn(&self, dest_dir: &Path) -> Result<(usize, u64)> {
        ensure_fresh_destination(dest_dir, Some(self.inner.paths.base_dir()))?;
        let _all = self.inner.locks.lock_all();
        let lsn = self.inner.wal.as_ref().map_or(0, |w| w.current_lsn());
        self.inner.drain_and_flush_all(true)?;
        self.inner.checkpoint_and_truncate(lsn)?;
        let copied = copy_dir_recursive(self.inner.paths.base_dir(), dest_dir)?;
        Ok((copied, lsn))
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
            total_reads: self.inner.total_reads.sum(),
            total_writes: self.inner.total_writes.sum(),
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

    /// Current version of a live (present and not expired) record.
    #[inline]
    pub(crate) fn current_version(&self, id: InlineId) -> Option<Version> {
        self.inner.live_version(id)
    }

    /// Shared MVCC writer gate; hold it across the apply phase of a multi-record commit.
    #[inline]
    pub(crate) fn mvcc_writer_guard(&self) -> std::sync::RwLockReadGuard<'_, ()> {
        self.inner.mvcc.writer_guard()
    }

    /// Ordinals of records whose TTL has expired (`None` when there are none).
    pub(crate) fn expired_ordinals(&self) -> Option<CompressedBitmap> {
        self.inner.expired_ordinals()
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
        Ok(self.inner.remove_record_after_wal_locked(id))
    }

    /// Replays recovered WAL entries (secondary indexes maintained), then writes one checkpoint.
    pub(crate) fn replay_wal_entries(&self, entries: &[WalEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.inner.replay_wal_entries(entries, true)?;
        if let Some(ref arena) = self.inner.arena {
            arena.sync_all()?;
        }
        self.inner.save_checkpoint_files()?;
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
                    // Only records that have an embedding can match: enumerate the (small)
                    // vector index, not the whole primary index.
                    for ord in self.inner.vector_index.live_ordinals() {
                        if let Some(id) = self.inner.primary.resolve_ordinal(ord) {
                            score_id(id, ord, &mut hits);
                        }
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
            let hdr = RecordCodec::header_ref(dirty_bytes.as_slice())?;
            if hdr.is_tombstone() || hdr.flags_typed().contains(RecordFlags::COMPRESSED_LZ4) {
                return Err(StorageError::invalid_archive(
                    "compressed or tombstone record must be read via get_owned",
                ));
            }
            return Ok(RecordRef {
                backing: BackingSource::HotDirty(dirty_bytes),
                _record: PhantomData,
            });
        }

        if self.crypto_on.load(Ordering::Acquire) {
            let (plain, current) = self.decrypted_disk_frame(&entry)?;
            if !current {
                // Older schema: callers fall back to `get_owned` (which migrates).
                RecordCodec::validate_frame::<T>(plain.as_slice())?;
            }
            let hdr = RecordCodec::header_ref(plain.as_slice())?;
            if hdr.is_tombstone() || hdr.flags_typed().contains(RecordFlags::COMPRESSED_LZ4) {
                return Err(StorageError::invalid_archive(
                    "compressed or tombstone record must be read via get_owned",
                ));
            }
            return Ok(RecordRef {
                backing: BackingSource::HotDirty(plain),
                _record: PhantomData,
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
                        _record: PhantomData,
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
                        _record: PhantomData,
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
                    _record: PhantomData,
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
        if self.crypto_on.load(Ordering::Acquire) {
            let (plain, _) = self.decrypted_disk_frame(&entry)?;
            return self.decode_or_migrate_frame(plain.as_slice());
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

    /// Replays committed WAL entries. Each upsert is flushed to disk with `fsync`.
    /// When `maintain_indexes` is set, secondary indexes are diffed per entry (used when the
    /// engine is already live); at open they are rebuilt from scratch afterwards instead.
    fn replay_wal_entries(&self, entries: &[WalEntry], maintain_indexes: bool) -> Result<()> {
        for entry in entries {
            if entry.collection_id != T::COLLECTION_ID && entry.collection_id != 0 {
                continue;
            }
            let _lock = self.locks.lock_one(entry.record_id);
            match entry.kind {
                WalOpKind::Upsert => {
                    let mut aligned = AlignedBytes::try_copy_from_slice(&entry.frame_bytes)?;
                    if let Some(enc) = self.crypto() {
                        if RecordCodec::frame_protection(aligned.as_slice()) != FrameProtection::Plain {
                            aligned = RecordCodec::unprotect_frame(
                                &enc.keys,
                                entry.record_id,
                                aligned.as_slice(),
                            )?;
                        } else if !enc.accept_unencrypted {
                            return Err(StorageError::invalid_archive(
                                "plaintext WAL frame found in an encrypted database",
                            ));
                        }
                    }
                    // WAL bytes are untrusted until validated for `T` (`RULE-SAFE-04`); frames of
                    // an older schema are migrated and re-encoded at the current schema.
                    match RecordCodec::validate_frame::<T>(&aligned) {
                        Ok(_) => {}
                        Err(StorageError::SchemaVersionMismatch { .. }) => {
                            let (ver, upgraded) = self.decode_or_migrate_frame(&aligned)?;
                            let old_hdr = RecordHeader::from_slice_verified(&aligned)?;
                            aligned = RecordCodec::encode_aligned(
                                &upgraded,
                                ver,
                                old_hdr.updated_at_ns,
                                old_hdr.flags_typed().without(RecordFlags::COMPRESSED_LZ4),
                                old_hdr.ttl_expires_at_ns,
                            );
                        }
                        Err(e) => return Err(e),
                    }
                    let hdr = RecordHeader::from_slice_verified(&aligned)?;
                    let existing = self.primary.get(entry.record_id);
                    let old_snap = if maintain_indexes {
                        existing
                            .as_ref()
                            .and_then(|e| self.extract_snapshot_for_entry(e))
                            .unwrap_or_default()
                    } else {
                        IndexSnapshot::new()
                    };
                    let ord = existing
                        .as_ref()
                        .map(|e| e.ordinal)
                        .unwrap_or_else(|| self.primary.allocate_ordinal(entry.record_id));
                    let prev_loc = existing
                        .as_ref()
                        .map(|e| e.location)
                        .unwrap_or(DiskLocation::Unflushed);
                    {
                        let _mvcc_gate = self.mvcc.writer_guard();
                        self.mvcc.forget(entry.record_id);
                        self.primary.upsert(PrimaryEntry {
                            id: entry.record_id,
                            ordinal: ord,
                            version: entry.version,
                            ttl_expires_at_ns: hdr.ttl_expires_at_ns,
                            location: prev_loc,
                            disk_verified: false,
                            hot_dirty: Some(aligned.clone()),
                        });
                    }
                    self.set_ttl_ordinal(ord, hdr.ttl_expires_at_ns > 0);
                    self.flush_single_record_impl(entry.record_id, true, true, false)?;
                    if maintain_indexes {
                        let new_snap = self
                            .extract_snapshot_from_frame(aligned.as_slice(), false)
                            .unwrap_or_default();
                        self.apply_index_snapshot_diff(ord, &old_snap, &new_snap);
                        if old_snap.embedding_hash != new_snap.embedding_hash
                            || old_snap.has_embedding != new_snap.has_embedding
                        {
                            match self.decode_or_migrate_frame(aligned.as_slice()) {
                                Ok((_, owned)) => match owned.embedding_slice() {
                                    Some(emb) if !emb.is_empty() => {
                                        self.vector_index.insert(ord, emb);
                                    }
                                    _ => self.vector_index.remove(ord),
                                },
                                Err(_) => self.vector_index.remove(ord),
                            }
                        }
                    }
                }
                WalOpKind::Delete => {
                    // The record may already be gone (checkpointed deletion): still remember
                    // its version so a re-created record never reuses it.
                    self.raise_version_floor(entry.version);
                    let _mvcc_gate = self.mvcc.writer_guard();
                    let _ = self.remove_record_after_wal_locked(entry.record_id);
                }
                _ => {}
            }
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
        if self.crypto_on.load(Ordering::Acquire) {
            // MVCC history keeps plaintext in RAM, like `hot_dirty`.
            return self.decrypted_disk_frame(entry).ok().map(|(f, _)| f);
        }
        self.read_disk_bytes_for_entry(entry)
    }

    /// Copies the frame bytes exactly as stored on disk for `entry` (ciphertext if encrypted).
    fn read_disk_bytes_for_entry(&self, entry: &PrimaryEntry) -> Option<AlignedBytes> {
        match entry.location {
            DiskLocation::Unflushed => None,
            DiskLocation::Arena(loc) => {
                let arena = self.arena.as_ref()?;
                arena
                    .with_slice(loc, AlignedBytes::try_copy_from_slice)
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
                AlignedBytes::try_copy_from_slice(slice).ok()
            }
        }
    }

    /// The plaintext frame of an on-disk encrypted record and whether it validated as the current
    /// schema (`true` for every cached frame). Decrypts and authenticates on a cache miss.
    fn decrypted_disk_frame(&self, entry: &PrimaryEntry) -> Result<(AlignedBytes, bool)> {
        let Some(enc) = self.crypto() else {
            return Err(StorageError::invalid_key("encryption is not enabled"));
        };
        if let Some(plain) = self.decrypted.get(entry.id, entry.version) {
            return Ok((plain, true));
        }
        let raw = self
            .read_disk_bytes_for_entry(entry)
            .ok_or_else(|| StorageError::not_found(entry.id))?;
        let plain = if RecordCodec::frame_protection(raw.as_slice()) != FrameProtection::Plain {
            RecordCodec::unprotect_frame(&enc.keys, entry.id, raw.as_slice())?
        } else if enc.accept_unencrypted {
            raw
        } else {
            return Err(StorageError::invalid_archive(
                "plaintext record frame found in an encrypted database",
            ));
        };
        match RecordCodec::validate_frame::<T>(plain.as_slice()) {
            Ok(_) => {
                self.decrypted.insert(entry.id, entry.version, plain.clone());
                Ok((plain, true))
            }
            Err(StorageError::SchemaVersionMismatch { .. }) => Ok((plain, false)),
            Err(e) => Err(e),
        }
    }

    /// The bytes to store on disk or in the WAL for `frame`: sealed when encryption is enabled.
    fn seal_for_disk(&self, id: InlineId, frame: &[u8]) -> Result<Option<AlignedBytes>> {
        if !self.crypto_on.load(Ordering::Acquire) {
            return Ok(None);
        }
        let Some(enc) = self.crypto() else {
            return Ok(None);
        };
        let Some((kid, key)) = enc.active_key.and_then(|kid| enc.keys.get(kid).map(|k| (kid, k)))
        else {
            return Ok(None);
        };
        match self.desired_protection(Some(kid), frame) {
            FrameProtection::Authenticated(_) => {
                RecordCodec::authenticate_frame(key, kid, id, frame).map(Some)
            }
            _ => RecordCodec::encrypt_frame(key, kid, id, frame).map(Some),
        }
    }

    /// The protection a plain `frame` must have on disk under active key `active` and the record
    /// type's [`Indexable::protection`] policy. Frames the policy cannot inspect (compressed,
    /// tombstones, other schemas) are encrypted.
    fn desired_protection(&self, active: Option<u32>, frame: &[u8]) -> FrameProtection {
        let Some(kid) = active else {
            return FrameProtection::Plain;
        };
        match RecordCodec::decode_fast::<T>(frame) {
            Ok(view) if T::protection(view.archived(), view.payload()) == RecordProtection::Authenticate => {
                FrameProtection::Authenticated(kid)
            }
            _ => FrameProtection::Encrypted(kid),
        }
    }

    /// The installed encryption configuration, if any.
    #[inline]
    fn crypto(&self) -> Option<Arc<EncryptionConfig>> {
        self.crypto
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// WAL body for an upsert of `id` with `frame` (sealed when encryption is enabled).
    pub(crate) fn wal_frame_bytes(&self, id: InlineId, frame: &[u8]) -> Result<Vec<u8>> {
        Ok(match self.seal_for_disk(id, frame)? {
            Some(sealed) => sealed.as_slice().to_vec(),
            None => frame.to_vec(),
        })
    }

    /// Extracts an `IndexSnapshot` on-the-fly directly from an existing record's `hot_dirty` or
    /// `mmap` frame with zero persistent RAM overhead.
    fn extract_snapshot_for_entry(&self, entry: &PrimaryEntry) -> Option<IndexSnapshot> {
        if let Some(ref dirty) = entry.hot_dirty {
            return self.extract_snapshot_from_frame(dirty.as_slice(), true);
        }
        if self.crypto_on.load(Ordering::Acquire) {
            let (plain, current) = self.decrypted_disk_frame(entry).ok()?;
            return self.extract_snapshot_from_frame(plain.as_slice(), current);
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
            if let Ok(view) = RecordCodec::decode_fast::<T>(frame) {
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
            match RecordCodec::decode_fast::<T>(slice) {
                Ok(view) => (entry.version, view.to_owned_record()?),
                Err(_) => self.decode_or_migrate_frame(slice)?,
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
            // Borrow the plaintext frame unless it must be sealed (no copy on the plain path).
            let sealed = self.seal_for_disk(id, frame.as_slice())?;
            let body = sealed.as_ref().map_or(frame.as_slice(), |s| s.as_slice());
            wal.append_single(T::COLLECTION_ID, WalOpKind::Upsert, id, version, body)?;
        }

        let _mvcc_gate = self.mvcc.writer_guard();
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

    /// Publishes an already-WAL-logged upsert in memory.
    ///
    /// Caller must hold the record's stripe lock and the MVCC writer guard
    /// (`self.mvcc.writer_guard()`), which keeps snapshot registration atomic with the write.
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
        // History must be recorded BEFORE the new version becomes visible (snapshot readers
        // re-check the chain after reading the current state).
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
        } else {
            self.mvcc.forget(id);
        }

        let ordinal = match &existing {
            Some(e) => e.ordinal,
            None => self.primary.allocate_ordinal(id),
        };
        let old_snap = existing
            .as_ref()
            .and_then(|e| self.extract_snapshot_for_entry(e))
            .unwrap_or_default();

        // A record moved to a different parent group leaves its old file behind; it is removed
        // once the new location has been written.
        let mut orphaned_parent: Option<Option<InlineId>> = None;
        let location = match existing.as_ref().map(|e| e.location) {
            Some(DiskLocation::DedicatedFile {
                parent_group: old_pg,
                ping_pong_stride,
            }) => {
                let new_pg = parent_group.or(old_pg);
                if new_pg != old_pg {
                    orphaned_parent = Some(old_pg);
                }
                DiskLocation::DedicatedFile {
                    parent_group: new_pg,
                    ping_pong_stride,
                }
            }
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

        // Differential secondary index update without any global IndexSnapshot map (`TASK-016`, `F-INDX-05`, `RULE-MEM-02`)
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
        self.set_ttl_ordinal(ordinal, ttl_expires_at_ns > 0);

        self.total_writes.add(1);

        let flush_now = orphaned_parent.is_some()
            || !self.config.background_flush
            || self.primary.hot_dirty_bytes() >= self.config.max_hot_dirty_bytes;
        if flush_now {
            if self.config.background_flush {
                self.flush_cv.notify_one();
            }
            if let Err(e) = self.flush_single_record_to_disk_locked(id, self.data_sync()) {
                // The write is already durable in the WAL and visible in memory: keep it dirty
                // and retry on the next flush instead of reporting a failed write. Without a WAL
                // the data would only live in RAM, so the error must surface.
                if self.wal.is_none() {
                    return Err(e);
                }
                self.enqueue_flush(id);
            } else if let Some(old_pg) = orphaned_parent {
                if let Ok(path) =
                    self.paths
                        .full_record_path(&self.config.collection_name, old_pg, id)
                {
                    self.remove_record_file(id, path);
                }
            }
        } else {
            self.enqueue_flush(id);
        }

        Ok(())
    }

    /// Adds `id` to the coalescing flush queue and wakes the background flusher when useful.
    fn enqueue_flush(&self, id: InlineId) {
        let mut q = self
            .flush_queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let was_empty = q.is_empty();
        if !q.enqueue(id) {
            // `id` was already in the flush queue! This update coalesced in memory (`F-CONC-04`)!
            self.coalesced_flushes.fetch_add(1, Ordering::Relaxed);
        }
        if self.config.background_flush && (was_empty || q.order.len() == 256) {
            self.flush_cv.notify_one();
        }
    }

    /// Whether data-file writes must be `fsync`ed (mirrors the WAL durability policy).
    #[inline]
    fn data_sync(&self) -> bool {
        !matches!(self.config.wal_sync_policy, SyncPolicy::AsyncMemoryFirst)
    }

    #[inline]
    fn set_ttl_ordinal(&self, ord: RecordOrdinal, has_ttl: bool) {
        let present = self
            .ttl_ordinals
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(ord);
        if present != has_ttl {
            let mut bm = self
                .ttl_ordinals
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if has_ttl {
                bm.insert(ord);
            } else {
                bm.remove(ord);
            }
        }
    }

    /// Version for the next write of a record whose current version is `current` (`None` when
    /// the record does not exist). A created record starts above every version ever deleted in
    /// this collection, so versions are never reused for a key (`RULE-SAFE-03`, no ABA).
    #[inline]
    pub(crate) fn next_version(&self, current: Option<Version>) -> Version {
        match current {
            Some(v) => v.next(),
            None => {
                let floor = self.version_floor.load(Ordering::Acquire);
                Version(floor.saturating_add(1).max(Version::INITIAL.0))
            }
        }
    }

    /// Refuses direct writes while the engine is a Raft replica.
    #[inline]
    pub(crate) fn ensure_writable(&self) -> Result<()> {
        if self.replica.load(Ordering::Acquire) {
            return Err(StorageError::ReadOnlyReplica);
        }
        Ok(())
    }

    /// Raises the version floor to `version` (called for every deletion, including replay).
    #[inline]
    pub(crate) fn raise_version_floor(&self, version: Version) {
        self.version_floor.fetch_max(version.0, Ordering::AcqRel);
    }

    /// Persists the version floor next to the primary checkpoint.
    fn persist_version_floor(&self) -> Result<()> {
        save_version_floor(&self.floor_path, self.version_floor.load(Ordering::Acquire))
    }

    /// Current version of `id` if it exists and has not expired.
    #[inline]
    fn live_version(&self, id: InlineId) -> Option<Version> {
        self.primary
            .with_entry(id, |e| {
                if e.ttl_expires_at_ns > 0 && current_time_ns() >= e.ttl_expires_at_ns {
                    None
                } else {
                    Some(e.version)
                }
            })
            .flatten()
    }

    /// Ordinals of TTL records that have expired (`None` when there are none). `O(#TTL records)`.
    fn expired_ordinals(&self) -> Option<CompressedBitmap> {
        let ttl = self
            .ttl_ordinals
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if ttl.is_empty() {
            return None;
        }
        let now = current_time_ns();
        let mut expired = CompressedBitmap::new();
        for ord in ttl.to_vec() {
            let is_expired = self
                .primary
                .resolve_ordinal(ord)
                .and_then(|id| {
                    self.primary
                        .with_entry(id, |e| e.ttl_expires_at_ns > 0 && now >= e.ttl_expires_at_ns)
                })
                .unwrap_or(false);
            if is_expired {
                expired.insert(ord);
            }
        }
        (!expired.is_empty()).then_some(expired)
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

    /// Deletes `id`: WAL first (so a failed log write changes nothing), then memory and disk.
    /// Caller must hold the record's stripe lock.
    fn delete_record_locked(&self, id: InlineId) -> Result<bool> {
        let Some(existing_version) = self.primary.version_of(id) else {
            return Ok(false);
        };
        if let Some(ref wal) = self.wal {
            wal.append_single(
                T::COLLECTION_ID,
                WalOpKind::Delete,
                id,
                existing_version.next(),
                &current_time_ns().to_le_bytes(),
            )?;
        }
        let _mvcc_gate = self.mvcc.writer_guard();
        Ok(self.remove_record_after_wal_locked(id))
    }

    /// Removes an already-WAL-logged deletion from memory and disk. Caller must hold the
    /// record's stripe lock and the MVCC writer guard.
    fn remove_record_after_wal_locked(&self, id: InlineId) -> bool {
        let Some(existing) = self.primary.get(id) else {
            return false;
        };
        self.decrypted.remove(id);
        // Record the tombstone in MVCC history before the deletion becomes visible.
        let snapshots_active = self.mvcc.has_active_snapshots();
        if snapshots_active {
            let old_frame = self.read_raw_frame_for_entry(&existing);
            self.mvcc
                .seed_baseline_if_absent(id, existing.version, old_frame);
            self.mvcc.record_version(id, existing.version.next(), None);
        } else {
            self.mvcc.forget(id);
        }

        let Some(removed) = self.primary.remove(id) else {
            return false;
        };
        self.raise_version_floor(removed.version.next());
        self.remove_from_secondary_indexes(&removed);
        self.set_ttl_ordinal(removed.ordinal, false);
        self.primary.release_ordinal(removed.ordinal);
        self.mmap_pool.invalidate(id);

        match removed.location {
            DiskLocation::DedicatedFile { parent_group, .. } => {
                if let Ok(path) =
                    self.paths
                        .full_record_path(&self.config.collection_name, parent_group, id)
                {
                    self.remove_record_file(id, path);
                }
            }
            DiskLocation::Arena(loc) => {
                if let Some(ref arena) = self.arena {
                    if snapshots_active {
                        arena.mark_dead(loc);
                    } else {
                        arena.reclaim_dead_slot(loc);
                    }
                }
            }
            DiskLocation::Unflushed => {}
        }
        true
    }

    /// Removes a deleted record's file, queueing a retry if it is still in use (Windows).
    fn remove_record_file(&self, id: InlineId, path: PathBuf) {
        if fs::remove_file(&path).is_err() && path.exists() {
            self.pending_file_removals
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((id, path));
        }
    }

    /// Retries queued file removals, skipping files whose record has since been re-created.
    fn retry_pending_removals(&self) {
        let mut pending = self
            .pending_file_removals
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pending.retain(|(id, path)| {
            if self.primary.version_of(*id).is_some() {
                return false; // re-created: the file now belongs to the live record
            }
            fs::remove_file(path).is_err() && path.exists()
        });
    }

    /// `fsync`s data files, saves the primary checkpoint and truncates WAL frames `<= cutoff`.
    /// Caller guarantees every operation with `lsn <= cutoff` is applied and flushed.
    fn checkpoint_and_truncate(&self, cutoff: u64) -> Result<()> {
        if let Some(ref arena) = self.arena {
            if self.data_sync() {
                arena.sync_all()?;
            }
        }
        self.save_checkpoint_files()?;
        // Persist the vector graph only when it changed since the last save.
        let generation = self.vector_index.generation();
        if generation != self.hnsw_saved_generation.load(Ordering::Acquire) {
            let enc = self.crypto();
            self.vector_index.save_to_disk_sealed(&self.hnsw_path, &file_seal(enc.as_deref()))?;
            self.hnsw_saved_generation.store(generation, Ordering::Release);
        }
        if !self.shared_wal_mode {
            if let Some(ref wal) = self.wal {
                wal.checkpoint_truncate_before(cutoff, &|_| false, self.wal_archive_dir.as_deref())?;
            }
        }
        self.retry_pending_removals();
        Ok(())
    }

    /// Deletes arena segment files that no record references (except the active segment).
    fn remove_unreferenced_segments(&self) {
        let Some(ref arena) = self.arena else {
            return;
        };
        let mut referenced: FastHashSet<u32> = FastHashSet::default();
        for entry in self.primary.entries_snapshot() {
            if let DiskLocation::Arena(loc) = entry.location {
                referenced.insert(loc.segment_id);
            }
        }
        let active = arena.active_segment_id();
        for seg_id in arena.segment_ids() {
            if seg_id != active && !referenced.contains(&seg_id) {
                arena.remove_segment(seg_id);
            }
        }
    }

    #[inline]
    fn flush_single_record_to_disk(&self, id: InlineId, sync_disk: bool) -> Result<bool> {
        self.flush_single_record_impl(id, sync_disk, false, true)
    }

    #[inline]
    fn flush_single_record_to_disk_locked(&self, id: InlineId, sync_disk: bool) -> Result<bool> {
        self.flush_single_record_impl(id, sync_disk, true, true)
    }

    /// Flushes the `hot_dirty` frame of `id` to its on-disk location.
    ///
    /// Holds the record's stripe lock for the whole read-write-publish sequence (unless the
    /// caller already does), so a concurrent writer or second flusher can never interleave and
    /// publish a stale location. Arena appends are not individually `fsync`ed when
    /// `defer_arena_sync` is set; `checkpoint_and_truncate` syncs all segments instead.
    fn flush_single_record_impl(
        &self,
        id: InlineId,
        sync_disk: bool,
        already_locked: bool,
        defer_arena_sync: bool,
    ) -> Result<bool> {
        let _guard = if already_locked {
            None
        } else {
            Some(self.locks.lock_one(id))
        };
        let Some(entry) = self.primary.get(id) else {
            return Ok(false);
        };
        let Some(dirty_frame) = entry.hot_dirty else {
            return Ok(false);
        };

        let flushed_ver = entry.version;
        let hdr = RecordCodec::header_ref(dirty_frame.as_slice())?;
        let zero_copy_frame =
            !hdr.is_tombstone() && !hdr.flags_typed().contains(RecordFlags::COMPRESSED_LZ4);
        let sealed = self.seal_for_disk(id, dirty_frame.as_slice())?;
        // Encrypted frames are never zero-copy readable on disk; the plaintext (already
        // validated: freshly encoded) goes to the decrypted cache instead.
        let disk_verified = zero_copy_frame && sealed.is_none();
        if sealed.is_some() && zero_copy_frame {
            self.decrypted.insert(id, flushed_ver, dirty_frame.clone());
        }
        let frame_slice = sealed.as_ref().map_or(dirty_frame.as_slice(), |s| s.as_slice());

        match self.config.layout_mode {
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
                let loc = arena.write_record(frame_slice, sync_disk && !defer_arena_sync)?;
                if self.primary.complete_background_flush(
                    id,
                    flushed_ver,
                    DiskLocation::Arena(loc),
                    disk_verified,
                ) {
                    if let DiskLocation::Arena(old_loc) = entry.location {
                        if self.mvcc.has_active_snapshots() {
                            arena.mark_dead(old_loc);
                        } else {
                            arena.reclaim_dead_slot(old_loc);
                        }
                    }
                } else {
                    arena.mark_dead(loc);
                }
            }
            StorageLayoutMode::PingPongFiles { slot_stride } => {
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
                self.primary.complete_background_flush(
                    id,
                    flushed_ver,
                    DiskLocation::DedicatedFile {
                        parent_group,
                        ping_pong_stride: actual_stride as u32,
                    },
                    disk_verified,
                );
            }
            _ => {
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
                self.primary.complete_background_flush(
                    id,
                    flushed_ver,
                    DiskLocation::DedicatedFile {
                        parent_group,
                        ping_pong_stride: 0,
                    },
                    disk_verified,
                );
            }
        }
        Ok(true)
    }

    /// Flushes every dirty record. `already_locked` is set when the caller holds all stripes.
    fn drain_and_flush_all(&self, already_locked: bool) -> Result<usize> {
        let sync = self.data_sync();
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
            for (i, &id) in batch.iter().enumerate() {
                match self.flush_single_record_impl(id, sync, already_locked, true) {
                    Ok(true) => total += 1,
                    Ok(false) => {}
                    Err(e) => {
                        // Keep the failed and not-yet-attempted keys queued for retry.
                        for &rest in &batch[i..] {
                            self.enqueue_flush(rest);
                        }
                        return Err(e);
                    }
                }
            }
        }
        // Also flush any entries that were marked dirty outside the queue
        for entry in self.primary.entries_snapshot() {
            if entry.hot_dirty.is_some()
                && self.flush_single_record_impl(entry.id, sync, already_locked, true)?
            {
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

            // Memory-first WAL frames reach the OS at least once per flush interval.
            if let Some(ref wal) = self.wal {
                let _ = wal.flush_buffered();
            }
            let sync = self.data_sync();
            for id in batch {
                if self.flush_single_record_impl(id, sync, false, true).is_err() {
                    // Retain `hot_dirty` in RAM and re-enqueue `id` for retry after a short
                    // back-off (no busy loop on persistent errors such as a full disk).
                    self.enqueue_flush(id);
                    thread::sleep(interval);
                }
            }
        }
    }

    /// Writes the primary checkpoint and, for the paged index tier, the secondary indexes, all
    /// tagged with one fresh stamp.
    ///
    /// Writers keep running: the primary snapshot is taken first and the index deltas frozen
    /// after it, and the WAL is synced before the index generations are published, so every write
    /// either made it into both files or is still in the WAL — and a non-empty WAL at open forces
    /// a rebuild. Index files are written before the primary checkpoint, so a crash in between
    /// leaves mismatched stamps (also a rebuild), never stale indexes that look current.
    fn save_checkpoint_files(&self) -> Result<()> {
        let _serial = self
            .checkpoint_serial
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (Some(range), Some(tags)) = (self.range_index.as_paged(), self.tag_index.as_paged())
        else {
            let cut = self.primary_cut();
            return self.save_primary_files(cut, 0);
        };
        let stamp = next_checkpoint_stamp();
        let cut = self.primary_cut();
        let pending_range = range.begin_checkpoint();
        let pending_tags = tags.begin_checkpoint();
        if let Some(ref wal) = self.wal {
            wal.sync_to_disk()?;
        }
        range.finish_checkpoint(pending_range, stamp)?;
        tags.finish_checkpoint(pending_tags, stamp)?;
        self.save_primary_files(cut, stamp)
    }

    /// A consistent cut of the primary index for the next primary checkpoint file.
    fn primary_cut(&self) -> PrimaryCut {
        match self.config.primary_tier {
            PrimaryTier::Memory => PrimaryCut::Entries(self.primary.entries_snapshot()),
            PrimaryTier::Cold => PrimaryCut::Cold(self.primary.begin_cold_checkpoint()),
        }
    }

    /// Persists `cut` in the configured primary tier's format (removing the other tier's files
    /// and any delta log it supersedes), sealed per the current encryption settings.
    fn save_primary_files(&self, cut: PrimaryCut, stamp: u64) -> Result<()> {
        // The floor is written first: it only ever grows, so an older checkpoint paired with a
        // newer floor is still safe, while the reverse could let a version be reused.
        self.persist_version_floor()?;
        let enc = self.crypto();
        let seal = file_seal(enc.as_deref());
        match cut {
            PrimaryCut::Entries(entries) => {
                save_primary_checkpoint_entries_sealed(&entries, &self.idx_path, stamp, &seal)?;
                if self.cold_manifest.exists() {
                    PrimaryIndex::remove_cold_files(&self.cold_manifest);
                }
            }
            PrimaryCut::Cold(cut) => {
                self.primary.write_cold_checkpoint(cut, &self.cold_manifest, stamp, &seal)?;
                let _ = fs::remove_file(delta_checkpoint_path(&self.idx_path));
                let _ = fs::remove_file(&self.idx_path);
            }
        }
        Ok(())
    }

    /// Empties the paged index files (they are about to be rebuilt from the records).
    fn reset_paged_indexes(&self) -> Result<()> {
        // A checkpoint stamped `0` never matches a primary checkpoint, so a crash during the
        // rebuild can never make partial indexes look current.
        if let Some(range) = self.range_index.as_paged() {
            range.clear_with_stamp(0)?;
        }
        if let Some(tags) = self.tag_index.as_paged() {
            tags.clear_with_stamp(0)?;
        }
        Ok(())
    }

    /// Rebuilds the in-memory secondary indexes from the records and reconciles the (possibly
    /// persisted) HNSW graph with them: unchanged vectors are kept, changed or missing ones are
    /// (re)inserted, and nodes of records that no longer have a vector are removed. With a current
    /// `hnsw.idx` this costs one `O(D)` comparison per vector instead of a graph insertion.
    fn rebuild_secondary_indexes(&self, rebuild_postings: bool) -> Result<()> {
        /// Paged-tier rebuilds spill their delta to disk this often to bound RAM.
        const SPILL_EVERY: usize = 1_000_000;
        let empty = IndexSnapshot::new();
        let entries = self.primary.entries_snapshot();
        let mut with_vectors = CompressedBitmap::new();
        for (i, entry) in entries.into_iter().enumerate() {
            self.set_ttl_ordinal(entry.ordinal, entry.ttl_expires_at_ns > 0);
            if let Some(snap) = self.extract_snapshot_for_entry(&entry) {
                if rebuild_postings {
                    self.apply_index_snapshot_diff(entry.ordinal, &empty, &snap);
                }
                if snap.has_embedding && self.reconcile_vector(&entry) {
                    with_vectors.insert(entry.ordinal);
                }
            }
            if rebuild_postings && i > 0 && i % SPILL_EVERY == 0 {
                if let Some(range) = self.range_index.as_paged() {
                    range.checkpoint_with_stamp(0)?;
                }
                if let Some(tags) = self.tag_index.as_paged() {
                    tags.checkpoint_with_stamp(0)?;
                }
            }
        }
        for ord in self.vector_index.live_ordinals() {
            if !with_vectors.contains(ord) {
                self.vector_index.remove(ord);
            }
        }
        Ok(())
    }

    /// Makes the vector index hold `entry`'s current embedding (zero-copy read; owned decode only
    /// for compressed or older-schema frames). Returns `false` if the record has no embedding.
    fn reconcile_vector(&self, entry: &PrimaryEntry) -> bool {
        let sync = |emb: &[f32]| {
            if !self.vector_index.vector_matches(entry.ordinal, emb) {
                self.vector_index.insert(entry.ordinal, emb);
            }
        };
        if let Ok(rec) = self.get_ref_internal(entry.id) {
            let view = rec.view();
            if let Some(emb) = T::archived_embedding_slice(view.archived(), view.payload()) {
                sync(emb);
                return true;
            }
        }
        match self.get_owned_internal(entry.id) {
            Ok((_, owned)) => match owned.embedding_slice() {
                Some(emb) => {
                    sync(emb);
                    true
                }
                None => false,
            },
            Err(_) => false,
        }
    }
}

/// Unique, never-zero checkpoint stamp (wall clock, forced strictly increasing in-process).
fn next_checkpoint_stamp() -> u64 {
    static LAST: AtomicU64 = AtomicU64::new(0);
    let now = current_time_ns().max(1);
    let mut prev = LAST.load(Ordering::Relaxed);
    loop {
        let next = now.max(prev + 1);
        match LAST.compare_exchange_weak(prev, next, Ordering::AcqRel, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(actual) => prev = actual,
        }
    }
}

/// Primary index state captured for a checkpoint (see `PrimaryTier`).
enum PrimaryCut {
    Entries(Vec<PrimaryEntry>),
    Cold(ColdCheckpoint),
}

/// Loads the persisted primary index into `primary` and returns the checkpoint stamp (`None`
/// when delta frames were applied or no stamped checkpoint exists).
///
/// - A cold tier is attached as is (`PrimaryTier::Cold`), or materialized into RAM when the
///   engine now runs the memory tier (the next checkpoint writes `primary.idx` again).
/// - Otherwise `primary.idx` is loaded (also in the cold tier, which converts it at the next
///   checkpoint). A `primary.idx` left next to a cold manifest predates it and is ignored.
/// - Delta frames (`primary.idx.delta`) are replayed on top in every case.
fn load_primary_state(
    primary: &PrimaryIndex,
    idx_path: &Path,
    cold_manifest: &Path,
    tier: PrimaryTier,
    seal: &FileSeal<'_>,
) -> Result<Option<u64>> {
    let cold_stamp = if cold_manifest.exists() {
        let stamp = match tier {
            PrimaryTier::Cold => primary.attach_cold_manifest(cold_manifest, seal)?,
            PrimaryTier::Memory => {
                let staging = PrimaryIndex::new();
                let stamp = staging.attach_cold_manifest(cold_manifest, seal)?;
                for entry in staging.entries_snapshot() {
                    primary.restore_entry_at(entry)?;
                }
                stamp
            }
        };
        let _ = fs::remove_file(idx_path);
        stamp
    } else {
        None
    };
    let had_delta = delta_checkpoint_path(idx_path).exists();
    let (_, idx_stamp) = load_primary_checkpoint_with_stamp_sealed(primary, idx_path, seal)?;
    if had_delta {
        return Ok(None);
    }
    Ok(cold_stamp.or(idx_stamp))
}

/// Opens an empty paged range index at `path`, discarding any previous files.
fn fresh_paged_btree(path: &Path, seal: &FileSeal<'_>) -> Result<PagedBTreeIndex> {
    let _ = fs::remove_file(path);
    PagedBTreeIndex::open_sealed(path, seal)
}

/// Opens an empty paged tag index at `path`, discarding any previous files.
fn fresh_paged_bitmap(path: &Path, seal: &FileSeal<'_>) -> Result<PagedBitmapIndex> {
    let _ = fs::remove_file(path);
    PagedBitmapIndex::open_sealed(path, seal)
}

/// Precondition checked atomically, under the record's lock, before a [`WriteOp`] is applied
/// (`TASK-096`, `F-CONC-09`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Precondition {
    /// Always apply.
    #[default]
    None,
    /// The record must not exist (insert / create-only).
    Absent,
    /// The record must exist.
    Exists,
    /// The record must exist at exactly this version (compare-and-swap).
    Version(Version),
}

/// What a [`WriteOp`] does to its record.
#[derive(Clone, Debug)]
pub enum WriteKind<T> {
    /// Insert or replace the record (clears any TTL).
    Put(T),
    /// Insert or replace the record with a time-to-live.
    PutWithTtl(T, Duration),
    /// Delete the record (a no-op if it does not exist).
    Delete,
    /// Apply atomic field operations to the existing record (`Indexable::apply_field_ops`).
    Patch(Vec<storage_codec::field_ops::FieldOp>),
    /// Change nothing; only the precondition is checked (OCC read-set validation).
    Check,
}

/// One operation of an atomic [`StorageEngine::commit`].
#[derive(Clone, Debug)]
pub struct WriteOp<T> {
    pub id: InlineId,
    pub kind: WriteKind<T>,
    pub precondition: Precondition,
}

impl<T> WriteOp<T> {
    #[inline]
    pub fn put(id: InlineId, record: T) -> Self {
        Self { id, kind: WriteKind::Put(record), precondition: Precondition::None }
    }

    #[inline]
    pub fn delete(id: InlineId) -> Self {
        Self { id, kind: WriteKind::Delete, precondition: Precondition::None }
    }

    #[inline]
    pub fn patch(id: InlineId, ops: Vec<storage_codec::field_ops::FieldOp>) -> Self {
        Self { id, kind: WriteKind::Patch(ops), precondition: Precondition::Exists }
    }

    #[inline]
    pub fn check(id: InlineId, precondition: Precondition) -> Self {
        Self { id, kind: WriteKind::Check, precondition }
    }

    /// Adds a precondition.
    #[inline]
    #[must_use]
    pub fn when(mut self, precondition: Precondition) -> Self {
        self.precondition = precondition;
        self
    }
}

/// Result of one [`WriteOp`] after a successful commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteResult {
    pub id: InlineId,
    /// Version of the record after the commit (`None` if it does not exist afterwards).
    pub version: Option<Version>,
    /// Whether the record existed before this operation.
    pub existed: bool,
}

/// Staged state of one record inside a commit.
struct StagedRecord<T> {
    original: Option<Version>,
    state: Option<(T, u64, RecordFlags)>,
    loaded: bool,
    changed: bool,
}

impl<T: ZeroCopyRecord + Indexable> StorageEngine<T> {
    /// Applies `ops` atomically (all or nothing) with every precondition checked under the
    /// records' locks (`TASK-096`, `RULE-SAFE-03`, `RULE-CONC-01`):
    ///
    /// - keys are locked in canonical order (deadlock-free), preconditions are evaluated
    ///   against the state left by the earlier operations of the same commit;
    /// - the whole change is one WAL transaction, written before anything becomes visible;
    /// - each touched record gets exactly one new version, monotonic across delete + re-create.
    ///
    /// A failed precondition returns [`StorageError::ConditionFailed`] (or `VersionConflict`
    /// for [`Precondition::Version`]) and changes nothing.
    pub fn commit(&self, ops: Vec<WriteOp<T>>) -> Result<Vec<WriteResult>> {
        self.inner.ensure_writable()?;
        self.commit_with_min_version(ops, None)
    }

    /// [`Self::commit`] where every new version is at least `min_version` (Raft replicas use
    /// the log index, so all replicas assign identical versions). Bypasses the replica guard.
    pub(crate) fn commit_with_min_version(
        &self,
        ops: Vec<WriteOp<T>>,
        min_version: Option<Version>,
    ) -> Result<Vec<WriteResult>> {
        if ops.is_empty() {
            return Ok(Vec::new());
        }
        let mut keys: Vec<InlineId> = ops.iter().map(|o| o.id).collect();
        keys.sort_unstable();
        keys.dedup();
        let _guards = self.inner.locks.lock_many(&keys);
        let now_ns = current_time_ns();

        // Stage every operation in order against the current state.
        let mut order: Vec<InlineId> = Vec::with_capacity(keys.len());
        let mut staged: FastHashMap<InlineId, StagedRecord<T>> = FastHashMap::default();
        let mut results: Vec<(InlineId, bool)> = Vec::with_capacity(ops.len());
        for op in ops {
            let rec = match staged.entry(op.id) {
                std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                std::collections::hash_map::Entry::Vacant(v) => {
                    order.push(op.id);
                    let original = self.inner.live_version(op.id);
                    v.insert(StagedRecord { original, state: None, loaded: false, changed: false })
                }
            };
            // Lazily load the current record when an operation needs its contents.
            let needs_body = matches!(op.kind, WriteKind::Patch(_));
            if needs_body && !rec.loaded && !rec.changed && rec.original.is_some() {
                let (_, draft, _pg, ttl, flags) = self.inner.load_mutable_draft_locked(op.id)?;
                rec.state = Some((draft, ttl, flags));
                rec.loaded = true;
            }
            let exists_now = if rec.changed || rec.loaded { rec.state.is_some() } else { rec.original.is_some() };
            let current_version = if rec.changed { None } else { rec.original };
            match op.precondition {
                Precondition::None => {}
                Precondition::Absent if exists_now => {
                    return Err(StorageError::condition_failed(op.id, "record already exists", current_version.or(rec.original)));
                }
                Precondition::Exists if !exists_now => {
                    return Err(StorageError::condition_failed(op.id, "record does not exist", None));
                }
                Precondition::Version(v) => {
                    if rec.changed {
                        return Err(StorageError::condition_failed(
                            op.id,
                            "version precondition after an earlier change in the same commit",
                            None,
                        ));
                    }
                    let actual = rec.original.unwrap_or(Version(0));
                    if actual != v {
                        return Err(StorageError::version_conflict(op.id, v, actual));
                    }
                }
                Precondition::Absent | Precondition::Exists => {}
            }
            let existed = exists_now;
            match op.kind {
                WriteKind::Put(record) => {
                    rec.state = Some((record, 0, RecordFlags::NONE));
                    rec.changed = true;
                }
                WriteKind::PutWithTtl(record, ttl) => {
                    let expires = now_ns.saturating_add(ttl.as_nanos().min(u128::from(u64::MAX)) as u64);
                    rec.state = Some((record, expires, RecordFlags::HAS_TTL));
                    rec.changed = true;
                }
                WriteKind::Delete => {
                    if exists_now {
                        rec.state = None;
                        rec.changed = true;
                        rec.loaded = true;
                    }
                }
                WriteKind::Patch(field_ops) => {
                    let Some((ref mut draft, _, _)) = rec.state else {
                        return Err(StorageError::not_found(op.id));
                    };
                    // Patch a copy so a failing operation leaves the staged record untouched.
                    let mut copy = draft.clone();
                    copy.apply_field_ops(&field_ops)?;
                    *draft = copy;
                    rec.changed = true;
                }
                WriteKind::Check => {}
            }
            results.push((op.id, existed));
        }

        // Encode new frames and the WAL transaction.
        let mut wal_ops = Vec::with_capacity(order.len());
        let mut upserts: Vec<(InlineId, T, Version, u64, AlignedBytes)> = Vec::new();
        let mut deletes: Vec<InlineId> = Vec::new();
        let mut final_versions: FastHashMap<InlineId, Option<Version>> = FastHashMap::default();
        for id in &order {
            let Some(rec) = staged.remove(id) else { continue };
            if !rec.changed {
                final_versions.insert(*id, rec.original);
                continue;
            }
            let mut version = self.inner.next_version(rec.original);
            if let Some(min) = min_version {
                version = version.max(min);
            }
            match rec.state {
                Some((record, ttl, flags)) => {
                    let frame = RecordCodec::encode_aligned(&record, version, now_ns, flags, ttl);
                    wal_ops.push((
                        T::COLLECTION_ID,
                        WalOpKind::Upsert,
                        *id,
                        version,
                        self.inner.wal_frame_bytes(*id, frame.as_slice())?,
                    ));
                    upserts.push((*id, record, version, ttl, frame));
                    final_versions.insert(*id, Some(version));
                }
                None => {
                    if rec.original.is_some() {
                        wal_ops.push((
                            T::COLLECTION_ID,
                            WalOpKind::Delete,
                            *id,
                            version,
                            now_ns.to_le_bytes().to_vec(),
                        ));
                        deletes.push(*id);
                    }
                    final_versions.insert(*id, None);
                }
            }
        }

        if !wal_ops.is_empty() {
            if let Some(ref wal) = self.inner.wal {
                let txn_id = wal.allocate_txn_id();
                wal.append_transaction(txn_id, wal_ops)?;
            }
            let _mvcc_gate = self.inner.mvcc.writer_guard();
            for (id, record, version, ttl, frame) in upserts {
                self.inner
                    .apply_encoded_upsert_locked(id, None, &record, version, now_ns, ttl, frame)?;
            }
            for id in deletes {
                let _ = self.inner.remove_record_after_wal_locked(id);
            }
        }

        Ok(results
            .into_iter()
            .map(|(id, existed)| WriteResult {
                id,
                version: final_versions.get(&id).copied().flatten(),
                existed,
            })
            .collect())
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
        // Read-set validation and writes in one atomic commit (`RULE-CONC-02`).
        let mut ops: Vec<WriteOp<T>> = Vec::with_capacity(
            self.read_versions.len() + self.staged_upserts.len() + self.staged_deletes.len(),
        );
        for (&id, &expected) in &self.read_versions {
            ops.push(WriteOp::check(
                id,
                expected.map_or(Precondition::Absent, Precondition::Version),
            ));
        }
        for (id, (_pg, rec)) in self.staged_upserts {
            ops.push(WriteOp::put(id, rec));
        }
        for id in self.staged_deletes {
            ops.push(WriteOp::delete(id));
        }
        match self.engine.commit(ops) {
            Ok(_) => Ok(()),
            Err(StorageError::ConditionFailed { .. } | StorageError::VersionConflict { .. }) => {
                Err(StorageError::TransactionAborted {
                    reason: "OCC read-set version conflict detected at commit",
                })
            }
            Err(e) => Err(e),
        }
    }
}

const VERSION_FLOOR_MAGIC: u32 = 0x5346_4C52; // "SFLR"

/// Reads the persisted version floor (`0` if the file does not exist yet).
fn load_version_floor(path: &Path) -> Result<u64> {
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(StorageError::io("read version floor", e)),
    };
    if bytes.len() != 20 || storage_types::wire::read_u32_le(&bytes, 0) != VERSION_FLOOR_MAGIC {
        return Err(StorageError::corrupted_header("version floor file is malformed"));
    }
    let floor = storage_types::wire::read_u64_le(&bytes, 4);
    if storage_types::wire::read_u64_le(&bytes, 12) != storage_codec::checksum_payload(&bytes[..12]) {
        return Err(StorageError::corrupted_header("version floor checksum mismatch"));
    }
    Ok(floor)
}

/// Atomically persists the version floor (`[magic u32 | floor u64 | checksum u64]`).
fn save_version_floor(path: &Path, floor: u64) -> Result<()> {
    let mut bytes = [0u8; 20];
    bytes[0..4].copy_from_slice(&VERSION_FLOOR_MAGIC.to_le_bytes());
    bytes[4..12].copy_from_slice(&floor.to_le_bytes());
    let check = storage_codec::checksum_payload(&bytes[..12]);
    bytes[12..20].copy_from_slice(&check.to_le_bytes());
    atomic_write_replace(path, &bytes, true)
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
