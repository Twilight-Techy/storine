//! Multi-Collection Database Catalog & Cross-Collection ACID Transactions (`TASK-031`, `TASK-032`, `F-CAT-01`, `F-CAT-02`).
//!
//! Coordinates multiple named, typed `StorageEngine<T>` collections under a single database
//! root directory (`base_dir`):
//! - Persists `catalog.meta` with collection IDs, names, schema versions, and CRC32C integrity.
//! - Shares a single catalog-wide `Arc<GroupCommitWal>` (`wal/catalog.wal`) across all collections
//!   so cross-collection transactions commit in a single `fsync` batch.
//! - Provides `CatalogTransaction` for deadlock-free, multi-collection ACID transactions using
//!   canonical `(collection_id, stripe_index)` lock ordering (`RULE-CONC-02`).
//! - Enforces cross-collection referential integrity (`OnDelete::Restrict`, `OnDelete::Cascade`,
//!   `OnDelete::Unlink`) across distinct collection types (`T -> P`).

use std::any::Any;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use storage_codec::{checksum_payload, AlignedBytes, RecordCodec, ZeroCopyRecord};
use storage_graph::OnDelete;
use storage_index::Indexable;
use storage_mmap::{atomic_write_replace, cleanup_temp_files};
use storage_types::{FastHashMap, FastHashSet, InlineId, RecordFlags, Result, StorageError, Version};
use storage_wal::{GroupCommitWal, SyncPolicy, WalEntry, WalOpKind};

use crate::engine::{EngineConfig, EngineStats, StorageEngine};
use crate::write_queue::MultiRecordLockGuard;

const CATALOG_MAGIC: u32 = 0x5354_4341; // "STCA"
const CATALOG_VERSION: u16 = 1;

/// Metadata describing a registered collection inside `DatabaseCatalog` (`F-CAT-01`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectionMeta {
    pub collection_id: u16,
    pub schema_version: u16,
    pub name: String,
}

/// Type-erased collection interface allowing `DatabaseCatalog` and `CatalogTransaction` to
/// coordinate heterogeneous `StorageEngine<T>` instances without dynamic dispatch on hot reads.
pub trait ErasedCollectionEngine: Send + Sync {
    fn collection_id(&self) -> u16;
    fn collection_name(&self) -> &str;
    fn schema_version(&self) -> u16;
    fn as_any(&self) -> &dyn Any;
    fn current_version(&self, id: InlineId) -> Option<Version>;
    /// Version the next upsert of `id` receives (monotonic across delete + re-create).
    fn next_version(&self, id: InlineId) -> Version;
    fn lock_many<'a>(&'a self, ids: &[InlineId]) -> MultiRecordLockGuard<'a>;
    fn apply_delete_locked(&self, id: InlineId) -> Result<bool>;
    fn flush_collection(&self) -> Result<usize>;
    fn replay_entries(&self, entries: &[WalEntry]) -> Result<()>;
    fn stats(&self) -> EngineStats;
    /// Shared MVCC writer gate held across a commit's apply phase.
    fn mvcc_guard(&self) -> RwLockReadGuard<'_, ()>;
}

impl<T: ZeroCopyRecord + Indexable + 'static> ErasedCollectionEngine for StorageEngine<T> {
    #[inline]
    fn collection_id(&self) -> u16 {
        T::COLLECTION_ID
    }

    #[inline]
    fn collection_name(&self) -> &str {
        StorageEngine::collection_name(self)
    }

    #[inline]
    fn schema_version(&self) -> u16 {
        T::SCHEMA_VERSION
    }

    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn current_version(&self, id: InlineId) -> Option<Version> {
        StorageEngine::current_version(self, id)
    }

    #[inline]
    fn next_version(&self, id: InlineId) -> Version {
        self.next_write_version(id)
    }

    #[inline]
    fn lock_many<'a>(&'a self, ids: &[InlineId]) -> MultiRecordLockGuard<'a> {
        self.lock_many_records(ids)
    }

    #[inline]
    fn apply_delete_locked(&self, id: InlineId) -> Result<bool> {
        self.apply_tx_delete_locked(id)
    }

    #[inline]
    fn flush_collection(&self) -> Result<usize> {
        self.flush_all()
    }

    #[inline]
    fn replay_entries(&self, entries: &[WalEntry]) -> Result<()> {
        self.replay_wal_entries(entries)
    }

    #[inline]
    fn mvcc_guard(&self) -> RwLockReadGuard<'_, ()> {
        self.mvcc_writer_guard()
    }

    #[inline]
    fn stats(&self) -> EngineStats {
        StorageEngine::stats(self)
    }
}

struct RegisteredCollection {
    typed_any: Arc<dyn Any + Send + Sync>,
}

/// Multi-Collection Database Catalog (`TASK-031`, `F-CAT-01`).
pub struct DatabaseCatalog {
    base_dir: PathBuf,
    shared_wal: Arc<GroupCommitWal>,
    pending_wal_recovery: RwLock<Vec<WalEntry>>,
    persisted_meta: RwLock<FastHashMap<String, CollectionMeta>>,
    collections_by_name: RwLock<FastHashMap<String, RegisteredCollection>>,
    collections_by_id: RwLock<FastHashMap<u16, Arc<dyn ErasedCollectionEngine>>>,
}

impl core::fmt::Debug for DatabaseCatalog {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("DatabaseCatalog")
            .field("base_dir", &self.base_dir)
            .field("collections", &self.list_collections())
            .finish()
    }
}

impl DatabaseCatalog {
    /// Opens or initializes a `DatabaseCatalog` rooted at `base_dir` with a shared catalog WAL.
    pub fn open(base_dir: impl Into<PathBuf>, wal_sync_policy: SyncPolicy) -> Result<Self> {
        let base_dir = base_dir.into();
        if !base_dir.exists() {
            storage_types::vfs::at(&base_dir).create_dir_all(&base_dir)
                .map_err(|e| StorageError::io("create catalog base_dir", e))?;
        }

        // Collections opened in shared mode skip temp-file cleanup (others may be writing),
        // so it happens exactly once here, before any collection is open.
        let _ = cleanup_temp_files(&base_dir)?;

        let meta_path = base_dir.join("catalog.meta");
        let persisted = load_catalog_meta(&meta_path)?;
        let mut meta_map = FastHashMap::default();
        for m in persisted {
            meta_map.insert(m.name.clone(), m);
        }

        let wal_path = base_dir.join("wal").join("catalog.wal");
        let recovered = GroupCommitWal::recover_committed(&wal_path)?;
        let shared_wal = Arc::new(GroupCommitWal::open(&wal_path, wal_sync_policy)?);

        Ok(Self {
            base_dir,
            shared_wal,
            pending_wal_recovery: RwLock::new(recovered),
            persisted_meta: RwLock::new(meta_map),
            collections_by_name: RwLock::new(FastHashMap::default()),
            collections_by_id: RwLock::new(FastHashMap::default()),
        })
    }

    /// Registers and opens a typed collection `StorageEngine<T>` inside the catalog.
    /// Automatically replays any committed frames for `T::COLLECTION_ID` from the shared catalog WAL
    /// and updates `catalog.meta`.
    pub fn register_collection<T: ZeroCopyRecord + Indexable + 'static>(
        &self,
        mut config: EngineConfig,
    ) -> Result<Arc<StorageEngine<T>>> {
        if config.collection_name.is_empty() {
            return Err(StorageError::invalid_key("collection_name cannot be empty"));
        }

        // Check if already open with the same name or conflicting collection_id
        {
            let by_name = self
                .collections_by_name
                .read()
                .unwrap_or_else(|p| p.into_inner());
            if let Some(existing) = by_name.get(&config.collection_name) {
                if let Ok(typed) = Arc::clone(&existing.typed_any).downcast::<StorageEngine<T>>() {
                    return Ok(typed);
                }
                return Err(StorageError::invalid_key(
                    "collection already registered with a different record type",
                ));
            }
            let by_id = self
                .collections_by_id
                .read()
                .unwrap_or_else(|p| p.into_inner());
            if by_id.contains_key(&T::COLLECTION_ID) {
                return Err(StorageError::invalid_key(
                    "COLLECTION_ID conflict: another collection is already registered with this ID",
                ));
            }
        }

        config.enable_wal = true;
        let engine = Arc::new(StorageEngine::<T>::open_with_shared_wal(
            &self.base_dir,
            config.clone(),
            Some(Arc::clone(&self.shared_wal)),
        )?);

        // Replay any pending committed catalog WAL entries belonging to `T::COLLECTION_ID`
        {
            let mut pending = self
                .pending_wal_recovery
                .write()
                .unwrap_or_else(|p| p.into_inner());
            if !pending.is_empty() {
                let (mine, remaining): (Vec<WalEntry>, Vec<WalEntry>) = pending
                    .drain(..)
                    .partition(|e| e.collection_id == T::COLLECTION_ID);
                *pending = remaining;
                // One batched replay (one checkpoint, incremental index maintenance).
                engine.replay_wal_entries(&mine)?;
            }
        }

        let meta = CollectionMeta {
            collection_id: T::COLLECTION_ID,
            schema_version: T::SCHEMA_VERSION,
            name: config.collection_name.clone(),
        };

        let erased: Arc<dyn ErasedCollectionEngine> = Arc::clone(&engine) as _;
        let typed_any: Arc<dyn Any + Send + Sync> = Arc::clone(&engine) as _;

        {
            let mut by_name = self
                .collections_by_name
                .write()
                .unwrap_or_else(|p| p.into_inner());
            let mut by_id = self
                .collections_by_id
                .write()
                .unwrap_or_else(|p| p.into_inner());
            let mut persisted = self
                .persisted_meta
                .write()
                .unwrap_or_else(|p| p.into_inner());

            by_id.insert(T::COLLECTION_ID, erased);
            by_name.insert(
                config.collection_name.clone(),
                RegisteredCollection { typed_any },
            );
            persisted.insert(meta.name.clone(), meta);

            let all_meta: Vec<CollectionMeta> = persisted.values().cloned().collect();
            save_catalog_meta(&self.base_dir.join("catalog.meta"), &all_meta)?;
        }

        Ok(engine)
    }

    /// Retrieves a previously registered typed `StorageEngine<T>` by collection name.
    pub fn collection<T: ZeroCopyRecord + Indexable + 'static>(
        &self,
        name: &str,
    ) -> Result<Arc<StorageEngine<T>>> {
        let by_name = self
            .collections_by_name
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let reg = by_name
            .get(name)
            .ok_or_else(|| StorageError::invalid_key("collection not found in catalog"))?;
        Arc::clone(&reg.typed_any)
            .downcast::<StorageEngine<T>>()
            .map_err(|_| StorageError::invalid_key("collection type mismatch"))
    }

    /// Lists metadata for all registered collections in `catalog.meta`.
    pub fn list_collections(&self) -> Vec<CollectionMeta> {
        let persisted = self
            .persisted_meta
            .read()
            .unwrap_or_else(|p| p.into_inner());
        let mut out: Vec<CollectionMeta> = persisted.values().cloned().collect();
        out.sort_by_key(|m| m.collection_id);
        out
    }

    /// Flushes and checkpoints all registered collections, then truncates the shared WAL.
    ///
    /// The cutoff LSN is taken before the collections flush; each collection waits out its
    /// in-flight writers (stripe barrier) before draining, so every operation `<= cutoff` is
    /// durable when the WAL is truncated. Frames of collections that have not been registered
    /// (and therefore not replayed) yet are always kept.
    pub fn flush_all(&self) -> Result<usize> {
        let cutoff = self.shared_wal.current_lsn();
        let engines: Vec<Arc<dyn ErasedCollectionEngine>> = {
            let by_id = self
                .collections_by_id
                .read()
                .unwrap_or_else(|p| p.into_inner());
            let mut v: Vec<_> = by_id.values().cloned().collect();
            v.sort_by_key(|e| e.collection_id());
            v
        };
        let mut total = 0usize;
        for eng in &engines {
            total += eng.flush_collection()?;
        }
        let unreplayed: FastHashSet<u16> = self
            .pending_wal_recovery
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|e| e.collection_id)
            .collect();
        let registered: FastHashSet<u16> = engines.iter().map(|e| e.collection_id()).collect();
        self.shared_wal.checkpoint_truncate_before(
            cutoff,
            &|cid| unreplayed.contains(&cid) || (cid != 0 && !registered.contains(&cid)),
            None,
        )?;
        Ok(total)
    }

    /// Executes a cross-collection ACID transaction across any number of registered collections
    /// (`TASK-032`, `F-CAT-02`, `RULE-CONC-02`).
    pub fn transaction<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut CatalogTransaction<'_>) -> Result<R>,
    {
        let mut tx = CatalogTransaction::new(self);
        let result = f(&mut tx)?;
        tx.commit()?;
        Ok(result)
    }

    /// Deletes a record `id` in `source_engine` while enforcing a cross-collection referential
    /// integrity policy against `peer_engine` (`TASK-032`, `F-CAT-02`):
    /// - `OnDelete::Restrict`: Aborts with `StorageError::IntegrityViolation` if any referenced peer exists in `peer_engine`.
    /// - `OnDelete::Cascade`: Atomically deletes `id` from `source_engine` and all referenced peer records from `peer_engine`.
    /// - `OnDelete::Unlink`: Atomically unlinks `id` from all referenced peer records in `peer_engine` and deletes `id` from `source_engine`.
    pub fn delete_with_cross_collection_policy<T, P, FRel, FUnlink>(
        &self,
        source_engine: &StorageEngine<T>,
        id: InlineId,
        peer_engine: &StorageEngine<P>,
        policy: OnDelete,
        extract_peers: FRel,
        mut unlink_from_peer: FUnlink,
    ) -> Result<usize>
    where
        T: ZeroCopyRecord + Indexable + 'static,
        P: ZeroCopyRecord + Indexable + 'static,
        FRel: Fn(&T) -> Vec<InlineId>,
        FUnlink: FnMut(&mut P, InlineId),
    {
        let (_, src_rec) = source_engine.get_owned(id)?;
        let peer_ids = extract_peers(&src_rec);

        match policy {
            OnDelete::Restrict => {
                for &peer_id in &peer_ids {
                    if peer_engine.current_version(peer_id).is_some() {
                        return Err(StorageError::integrity_violation(
                            id,
                            peer_id,
                            "cannot delete record while cross-collection related records exist (OnDelete::Restrict)",
                        ));
                    }
                }
                let deleted = source_engine.delete(id)?;
                Ok(usize::from(deleted))
            }
            OnDelete::Cascade => {
                let mut deleted_count = 0usize;
                self.transaction(|tx| {
                    tx.delete(source_engine, id);
                    deleted_count += 1;
                    for &peer_id in &peer_ids {
                        if peer_engine.current_version(peer_id).is_some() {
                            tx.delete(peer_engine, peer_id);
                            deleted_count += 1;
                        }
                    }
                    Ok(())
                })?;
                Ok(deleted_count)
            }
            OnDelete::Unlink => {
                let mut affected = 0usize;
                self.transaction(|tx| {
                    for &peer_id in &peer_ids {
                        if let Ok(mut peer_rec) = tx.get(peer_engine, peer_id) {
                            unlink_from_peer(&mut peer_rec, id);
                            tx.put(peer_engine, peer_id, peer_rec);
                            affected += 1;
                        }
                    }
                    tx.delete(source_engine, id);
                    affected += 1;
                    Ok(())
                })?;
                Ok(affected)
            }
        }
    }
}

struct PreparedUpsert {
    version: Version,
    frame: AlignedBytes,
    /// WAL body for `frame` (sealed when the collection encrypts at rest).
    wal_bytes: Result<Vec<u8>>,
    apply_fn: Box<dyn FnOnce(Version, u64, AlignedBytes) -> Result<()> + Send>,
}

struct StagedCollectionUpsert {
    record_any: Box<dyn Any + Send + Sync>,
    prepare_fn: Box<dyn FnOnce(u64) -> PreparedUpsert + Send>,
}

/// Cross-Collection ACID Transaction Batch (`TASK-032`, `F-CAT-02`).
///
/// Acquires locks across multiple collections in canonical `(collection_id, stripe_index)` ascending
/// order, verifies OCC read-set versions across all collections, writes a single atomic WAL
/// transaction batch, and applies all mutations atomically.
pub struct CatalogTransaction<'a> {
    catalog: &'a DatabaseCatalog,
    /// First staging error (e.g. an unregistered collection); reported by `commit`.
    deferred_error: Option<StorageError>,
    touched_engines: FastHashMap<u16, Arc<dyn ErasedCollectionEngine>>,
    read_versions: FastHashMap<(u16, InlineId), Option<Version>>,
    staged_upserts: FastHashMap<(u16, InlineId), StagedCollectionUpsert>,
    staged_deletes: FastHashSet<(u16, InlineId)>,
}

impl<'a> core::fmt::Debug for CatalogTransaction<'a> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CatalogTransaction")
            .field("reads", &self.read_versions.len())
            .field("staged_upserts", &self.staged_upserts.len())
            .field("staged_deletes", &self.staged_deletes.len())
            .finish()
    }
}

impl<'a> CatalogTransaction<'a> {
    fn new(catalog: &'a DatabaseCatalog) -> Self {
        Self {
            catalog,
            deferred_error: None,
            touched_engines: FastHashMap::default(),
            read_versions: FastHashMap::default(),
            staged_upserts: FastHashMap::default(),
            staged_deletes: FastHashSet::default(),
        }
    }

    fn ensure_engine_tracked<T: ZeroCopyRecord + Indexable + 'static>(
        &mut self,
        engine: &StorageEngine<T>,
    ) -> Result<()> {
        let cid = T::COLLECTION_ID;
        if let std::collections::hash_map::Entry::Vacant(e) = self.touched_engines.entry(cid) {
            let by_id = self
                .catalog
                .collections_by_id
                .read()
                .unwrap_or_else(|p| p.into_inner());
            let erased = by_id.get(&cid).ok_or_else(|| {
                StorageError::invalid_key(
                    "collection engine is not registered in this DatabaseCatalog",
                )
            })?;
            let _ = engine;
            e.insert(Arc::clone(erased));
        }
        Ok(())
    }

    /// Reads `id` from `engine`, recording its `(T::COLLECTION_ID, id)` OCC version in the read-set.
    pub fn get<T: ZeroCopyRecord + Indexable + 'static>(
        &mut self,
        engine: &StorageEngine<T>,
        id: InlineId,
    ) -> Result<T> {
        self.ensure_engine_tracked(engine)?;
        let key = (T::COLLECTION_ID, id);
        if self.staged_deletes.contains(&key) {
            return Err(StorageError::not_found(id));
        }
        if let Some(staged) = self.staged_upserts.get(&key) {
            if let Some(rec) = staged.record_any.downcast_ref::<T>() {
                return Ok(rec.clone());
            }
        }
        match engine.get_owned(id) {
            Ok((ver, rec)) => {
                self.read_versions.entry(key).or_insert(Some(ver));
                Ok(rec)
            }
            Err(StorageError::RecordNotFound { .. }) => {
                self.read_versions.entry(key).or_insert(None);
                Err(StorageError::not_found(id))
            }
            Err(e) => Err(e),
        }
    }

    /// Stages an insert or update for `(T::COLLECTION_ID, id)` inside the cross-collection transaction.
    pub fn put<T: ZeroCopyRecord + Indexable + 'static>(
        &mut self,
        engine: &StorageEngine<T>,
        id: InlineId,
        record: T,
    ) {
        if let Err(e) = self.ensure_engine_tracked(engine) {
            self.deferred_error.get_or_insert(e);
            return;
        }
        let key = (T::COLLECTION_ID, id);
        self.staged_deletes.remove(&key);

        let Some(erased) = self.touched_engines.get(&T::COLLECTION_ID).cloned() else {
            self.deferred_error.get_or_insert(StorageError::invalid_key(
                "collection engine is not registered in this DatabaseCatalog",
            ));
            return;
        };
        let rec_clone = record.clone();

        let prepare_fn = Box::new(move |now_ns: u64| -> PreparedUpsert {
            let next_ver = erased.next_version(id);
            let frame = RecordCodec::encode_aligned(&rec_clone, next_ver, now_ns, RecordFlags::NONE, 0);
            let wal_bytes = erased
                .as_any()
                .downcast_ref::<StorageEngine<T>>()
                .ok_or_else(|| StorageError::invalid_key("collection type mismatch"))
                .and_then(|typed| typed.wal_frame_bytes(id, frame.as_slice()));
            let apply_erased = erased;
            let apply_rec = rec_clone;
            PreparedUpsert {
                version: next_ver,
                frame,
                wal_bytes,
                apply_fn: Box::new(move |ver, ts, aligned_frame| {
                    let typed_eng = apply_erased
                        .as_any()
                        .downcast_ref::<StorageEngine<T>>()
                        .ok_or_else(|| StorageError::invalid_key("collection type mismatch"))?;
                    typed_eng.apply_tx_upsert_locked(id, &apply_rec, ver, ts, aligned_frame)
                }),
            }
        });

        self.staged_upserts.insert(
            key,
            StagedCollectionUpsert {
                record_any: Box::new(record),
                prepare_fn,
            },
        );
    }

    /// Stages a delete for `(T::COLLECTION_ID, id)` inside the cross-collection transaction.
    pub fn delete<T: ZeroCopyRecord + Indexable + 'static>(
        &mut self,
        engine: &StorageEngine<T>,
        id: InlineId,
    ) {
        if let Err(e) = self.ensure_engine_tracked(engine) {
            self.deferred_error.get_or_insert(e);
            return;
        }
        let key = (T::COLLECTION_ID, id);
        self.staged_upserts.remove(&key);
        self.staged_deletes.insert(key);
    }

    fn commit(self) -> Result<()> {
        if let Some(e) = self.deferred_error {
            return Err(e);
        }
        if self.staged_upserts.is_empty() && self.staged_deletes.is_empty() {
            return Ok(());
        }

        // Group all touched `(collection_id, InlineId)` keys by `collection_id` in strictly ascending order (`RULE-CONC-02`)
        let mut keys_by_collection: std::collections::BTreeMap<u16, Vec<InlineId>> =
            std::collections::BTreeMap::new();
        for &(cid, id) in self
            .read_versions
            .keys()
            .chain(self.staged_upserts.keys())
            .chain(self.staged_deletes.iter())
        {
            keys_by_collection.entry(cid).or_default().push(id);
        }

        // Acquire per-collection MultiRecordLockGuards in strictly ascending `collection_id` order
        let mut guards: Vec<MultiRecordLockGuard<'_>> =
            Vec::with_capacity(keys_by_collection.len());
        for (&cid, ids) in &keys_by_collection {
            let eng = self.touched_engines.get(&cid).ok_or_else(|| {
                StorageError::invalid_key("touched collection missing from transaction map")
            })?;
            guards.push(eng.lock_many(ids));
        }

        // Verify OCC read-set versions across all collections while holding locks
        for (&(cid, id), &expected_ver) in &self.read_versions {
            let eng = self.touched_engines.get(&cid).ok_or_else(|| {
                StorageError::invalid_key("read collection missing from transaction map")
            })?;
            let actual_ver = eng.current_version(id);
            if actual_ver != expected_ver {
                return Err(StorageError::TransactionAborted {
                    reason: "cross-collection OCC read-set version conflict detected at commit",
                });
            }
        }

        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let mut wal_ops = Vec::with_capacity(self.staged_upserts.len() + self.staged_deletes.len());
        let mut prepared_upserts = Vec::with_capacity(self.staged_upserts.len());

        // Sort staged upserts deterministically by `(collection_id, InlineId)`
        let mut upsert_entries: Vec<((u16, InlineId), StagedCollectionUpsert)> =
            self.staged_upserts.into_iter().collect();
        upsert_entries.sort_by_key(|(k, _)| *k);

        for ((cid, id), staged) in upsert_entries {
            let mut prep = (staged.prepare_fn)(now_ns);
            let body = core::mem::replace(&mut prep.wal_bytes, Ok(Vec::new()))?;
            wal_ops.push((cid, WalOpKind::Upsert, id, prep.version, body));
            prepared_upserts.push(prep);
        }

        let mut delete_keys: Vec<(u16, InlineId)> = self.staged_deletes.into_iter().collect();
        delete_keys.sort_unstable();

        for &(cid, id) in &delete_keys {
            let eng = self.touched_engines.get(&cid).ok_or_else(|| {
                StorageError::invalid_key("delete collection missing from transaction map")
            })?;
            let next_ver = eng
                .current_version(id)
                .map_or(Version::INITIAL, |v| v.next());
            wal_ops.push((cid, WalOpKind::Delete, id, next_ver, now_ns.to_le_bytes().to_vec()));
        }

        // Write single atomic multi-collection commit batch to the shared catalog WAL!
        let txn_id = self.catalog.shared_wal.allocate_txn_id();
        self.catalog.shared_wal.append_transaction(txn_id, wal_ops)?;

        // Apply all upserts and deletes in memory while still holding canonical locks and every
        // touched collection's MVCC writer gate (snapshots see all or none of the transaction).
        let mut mvcc_guards = Vec::with_capacity(self.touched_engines.len());
        for cid in keys_by_collection.keys() {
            if let Some(eng) = self.touched_engines.get(cid) {
                mvcc_guards.push(eng.mvcc_guard());
            }
        }
        for prep in prepared_upserts {
            (prep.apply_fn)(prep.version, now_ns, prep.frame)?;
        }
        for (cid, id) in delete_keys {
            if let Some(eng) = self.touched_engines.get(&cid) {
                eng.apply_delete_locked(id)?;
            }
        }

        drop(mvcc_guards);
        drop(guards);
        Ok(())
    }
}

fn save_catalog_meta(path: &Path, collections: &[CollectionMeta]) -> Result<()> {
    let mut payload = Vec::with_capacity(collections.len() * 32);
    for col in collections {
        payload.extend_from_slice(&col.collection_id.to_le_bytes());
        payload.extend_from_slice(&col.schema_version.to_le_bytes());
        let name_bytes = col.name.as_bytes();
        let name_len = u16::try_from(name_bytes.len())
            .map_err(|_| StorageError::invalid_key("collection name exceeds u16::MAX bytes"))?;
        payload.extend_from_slice(&name_len.to_le_bytes());
        payload.extend_from_slice(name_bytes);
    }
    let crc = checksum_payload(&payload);
    let mut buf = Vec::with_capacity(18 + payload.len());
    buf.extend_from_slice(&CATALOG_MAGIC.to_le_bytes());
    buf.extend_from_slice(&CATALOG_VERSION.to_le_bytes());
    buf.extend_from_slice(&(collections.len() as u32).to_le_bytes());
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(&payload);
    atomic_write_replace(path, &buf, true)
}

fn load_catalog_meta(path: &Path) -> Result<Vec<CollectionMeta>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let bytes = fs::read(path).map_err(|e| StorageError::io("read catalog.meta", e))?;
    if bytes.len() < 18 {
        return Err(StorageError::corrupted_header("catalog.meta truncated"));
    }
    let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    if magic != CATALOG_MAGIC {
        return Err(StorageError::corrupted_header("catalog.meta invalid magic"));
    }
    let _version = u16::from_le_bytes([bytes[4], bytes[5]]);
    let count = u32::from_le_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]) as usize;
    let expected_crc = u64::from_le_bytes([
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15], bytes[16], bytes[17],
    ]);
    let payload = &bytes[18..];
    let actual_crc = checksum_payload(payload);
    if actual_crc != expected_crc {
        return Err(StorageError::checksum_mismatch(expected_crc, actual_crc));
    }

    let mut out = Vec::with_capacity(count.min(1024));
    let mut off = 0usize;
    for _ in 0..count {
        if off + 6 > payload.len() {
            return Err(StorageError::corrupted_header("catalog.meta entry truncated"));
        }
        let collection_id = u16::from_le_bytes([payload[off], payload[off + 1]]);
        let schema_version = u16::from_le_bytes([payload[off + 2], payload[off + 3]]);
        let name_len = u16::from_le_bytes([payload[off + 4], payload[off + 5]]) as usize;
        off += 6;
        if off + name_len > payload.len() {
            return Err(StorageError::corrupted_header("catalog.meta name truncated"));
        }
        let name = core::str::from_utf8(&payload[off..off + name_len])
            .map_err(|_| StorageError::corrupted_header("catalog.meta invalid UTF-8 name"))?
            .to_string();
        off += name_len;
        out.push(CollectionMeta {
            collection_id,
            schema_version,
            name,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::StorageLayoutMode;
    use crate::model::AgentObjectRecord;
    use storage_codec::{impl_archive_pod, ArchiveBuilder};
    use storage_index::IndexSnapshot;

    /// Second collection record type (`COLLECTION_ID = 2`) for cross-collection transaction tests.
    #[derive(Clone, Debug, PartialEq)]
    struct OrderRecord {
        pub order_id: InlineId,
        pub user_id: InlineId,
        pub amount_cents: i64,
    }

    #[repr(C)]
    #[derive(Clone, Copy, Debug)]
    struct ArchivedOrderRecord {
        pub order_id: InlineId,
        pub user_id: InlineId,
        pub amount_cents: i64,
    }

    impl_archive_pod!(ArchivedOrderRecord {
        order_id: InlineId,
        user_id: InlineId,
        amount_cents: i64,
    });

    impl ZeroCopyRecord for OrderRecord {
        type Archived = ArchivedOrderRecord;
        const SCHEMA_VERSION: u16 = 1;
        const COLLECTION_ID: u16 = 2;

        fn archive(&self, _builder: &mut ArchiveBuilder<'_>) -> Self::Archived {
            ArchivedOrderRecord {
                order_id: self.order_id,
                user_id: self.user_id,
                amount_cents: self.amount_cents,
            }
        }

        fn validate_archived(root: &Self::Archived, _payload: &[u8]) -> Result<()> {
            InlineId::from_raw_bytes(root.order_id.to_raw_bytes())?;
            InlineId::from_raw_bytes(root.user_id.to_raw_bytes())?;
            Ok(())
        }

        fn unarchive(root: &Self::Archived, _payload: &[u8]) -> Result<Self> {
            Ok(Self {
                order_id: root.order_id,
                user_id: root.user_id,
                amount_cents: root.amount_cents,
            })
        }
    }

    impl Indexable for OrderRecord {
        fn extract_index_snapshot(&self) -> IndexSnapshot {
            let mut snap = IndexSnapshot::new();
            snap.push_numeric(1, self.amount_cents);
            snap.push_tag(1, self.user_id);
            snap
        }
    }

    /// Evolved record (`SCHEMA_VERSION = 3`) for testing `v1 -> v2 -> v3` step upcasting and online schema upgrade.
    #[derive(Clone, Debug, PartialEq)]
    struct EvolvedRecordV3 {
        pub val: i64,
        pub bonus: i64,
        pub tier: i64,
    }

    #[repr(C)]
    #[derive(Clone, Copy, Debug)]
    struct ArchivedEvolvedRecordV3 {
        pub val: i64,
        pub bonus: i64,
        pub tier: i64,
    }

    impl_archive_pod!(ArchivedEvolvedRecordV3 {
        val: i64,
        bonus: i64,
        tier: i64,
    });

    impl ZeroCopyRecord for EvolvedRecordV3 {
        type Archived = ArchivedEvolvedRecordV3;
        const SCHEMA_VERSION: u16 = 3;
        const COLLECTION_ID: u16 = 3;

        fn archive(&self, _builder: &mut ArchiveBuilder<'_>) -> Self::Archived {
            ArchivedEvolvedRecordV3 {
                val: self.val,
                bonus: self.bonus,
                tier: self.tier,
            }
        }

        fn validate_archived(_root: &Self::Archived, _payload: &[u8]) -> Result<()> {
            Ok(())
        }

        fn unarchive(root: &Self::Archived, _payload: &[u8]) -> Result<Self> {
            Ok(Self {
                val: root.val,
                bonus: root.bonus,
                tier: root.tier,
            })
        }
    }

    impl Indexable for EvolvedRecordV3 {
        fn extract_index_snapshot(&self) -> IndexSnapshot {
            IndexSnapshot::new()
        }
    }

    #[derive(Clone, Debug)]
    struct LegacyRecordV1 {
        pub val: i64,
    }

    #[repr(C)]
    #[derive(Clone, Copy, Debug)]
    struct ArchivedLegacyRecordV1 {
        pub val: i64,
    }

    impl_archive_pod!(ArchivedLegacyRecordV1 { val: i64 });

    impl ZeroCopyRecord for LegacyRecordV1 {
        type Archived = ArchivedLegacyRecordV1;
        const SCHEMA_VERSION: u16 = 1;
        const COLLECTION_ID: u16 = 3;

        fn archive(&self, _builder: &mut ArchiveBuilder<'_>) -> Self::Archived {
            ArchivedLegacyRecordV1 { val: self.val }
        }

        fn validate_archived(_root: &Self::Archived, _payload: &[u8]) -> Result<()> {
            Ok(())
        }

        fn unarchive(root: &Self::Archived, _payload: &[u8]) -> Result<Self> {
            Ok(Self { val: root.val })
        }
    }

    impl Indexable for LegacyRecordV1 {
        fn extract_index_snapshot(&self) -> IndexSnapshot {
            IndexSnapshot::new()
        }
    }

    #[test]
    fn test_catalog_multi_collection_and_cross_collection_acid_tx() -> Result<()> {
        let dir = std::env::temp_dir().join("storine_test_catalog_acid");
        let _ = fs::remove_dir_all(&dir);

        let catalog = DatabaseCatalog::open(&dir, SyncPolicy::Immediate)?;
        let users = catalog.register_collection::<AgentObjectRecord>(EngineConfig {
            collection_name: "users".to_string(),
            background_flush: false,
            ..EngineConfig::default()
        })?;
        let orders = catalog.register_collection::<OrderRecord>(EngineConfig {
            collection_name: "orders".to_string(),
            background_flush: false,
            ..EngineConfig::default()
        })?;

        assert_eq!(catalog.list_collections().len(), 2);

        let user_id = InlineId::new("user_01")?;
        let order_1 = InlineId::new("order_01")?;
        let order_2 = InlineId::new("order_02")?;

        // Cross-collection atomic transaction creating a user and two linked orders
        catalog.transaction(|tx| {
            let mut u = AgentObjectRecord::new(user_id, "Alice", 1_000, 100);
            u.children.link(order_1);
            u.children.link(order_2);
            tx.put(&users, user_id, u);
            tx.put(
                &orders,
                order_1,
                OrderRecord {
                    order_id: order_1,
                    user_id,
                    amount_cents: 2500,
                },
            );
            tx.put(
                &orders,
                order_2,
                OrderRecord {
                    order_id: order_2,
                    user_id,
                    amount_cents: 7500,
                },
            );
            Ok(())
        })?;

        assert_eq!(users.stats().live_records, 1);
        assert_eq!(orders.stats().live_records, 2);

        // Cross-collection Restrict policy prevents deleting `user_01` while `order_01` exists
        let restrict_res = catalog.delete_with_cross_collection_policy(
            &users,
            user_id,
            &orders,
            OnDelete::Restrict,
            |u| u.children.as_slice().to_vec(),
            |_, _| {},
        );
        assert!(matches!(
            restrict_res,
            Err(StorageError::IntegrityViolation { .. })
        ));

        // Cross-collection Cascade deletes `user_01` AND both orders atomically
        let cascaded = catalog.delete_with_cross_collection_policy(
            &users,
            user_id,
            &orders,
            OnDelete::Cascade,
            |u| u.children.as_slice().to_vec(),
            |_, _| {},
        )?;
        assert_eq!(cascaded, 3);
        assert_eq!(users.stats().live_records, 0);
        assert_eq!(orders.stats().live_records, 0);

        let _ = fs::remove_dir_all(&dir);
        Ok(())
    }

    #[test]
    fn test_multi_step_upcaster_and_online_schema_upgrade() -> Result<()> {
        let dir = std::env::temp_dir().join("storine_test_step_upcaster");
        let _ = fs::remove_dir_all(&dir);

        let id = InlineId::new("rec_v1")?;
        {
            let engine_v1 = StorageEngine::<LegacyRecordV1>::open(
                &dir,
                EngineConfig {
                    collection_name: "evolved".to_string(),
                    layout_mode: StorageLayoutMode::AdaptiveArena {
                        arena_threshold_bytes: 4096,
                        segment_capacity: 64 * 1024,
                    },
                    background_flush: false,
                    ..EngineConfig::default()
                },
            )?;
            engine_v1.put(id, &LegacyRecordV1 { val: 42 })?;
            engine_v1.flush_all()?;
        }

        // Re-open with V3 schema and register chained step upcasters `v1 -> v2 -> v3`
        let engine_v3 = StorageEngine::<EvolvedRecordV3>::open(
            &dir,
            EngineConfig {
                collection_name: "evolved".to_string(),
                layout_mode: StorageLayoutMode::AdaptiveArena {
                    arena_threshold_bytes: 4096,
                    segment_capacity: 64 * 1024,
                },
                background_flush: false,
                ..EngineConfig::default()
            },
        )?;

        // Step 1 -> 2: append `bonus = 10`
        engine_v3.register_step_upcaster(1, |bytes_v1| {
            let mut out = bytes_v1[..8].to_vec();
            out.extend_from_slice(&10i64.to_le_bytes());
            Ok(out)
        });
        // Step 2 -> 3: append `tier = 3`
        engine_v3.register_step_upcaster(2, |bytes_v2| {
            let mut out = bytes_v2[..16].to_vec();
            out.extend_from_slice(&3i64.to_le_bytes());
            Ok(out)
        });
        engine_v3.set_step_upcaster_decoder(|_ver, bytes_v3| {
            let val = i64::from_le_bytes(bytes_v3[0..8].try_into().unwrap_or([0; 8]));
            let bonus = i64::from_le_bytes(bytes_v3[8..16].try_into().unwrap_or([0; 8]));
            let tier = i64::from_le_bytes(bytes_v3[16..24].try_into().unwrap_or([0; 8]));
            Ok(EvolvedRecordV3 { val, bonus, tier })
        });

        let (_, migrated) = engine_v3.get_owned(id)?;
        assert_eq!(
            migrated,
            EvolvedRecordV3 {
                val: 42,
                bonus: 10,
                tier: 3,
            }
        );

        // Online batch schema upgrade rewrites the record to native V3 on disk so zero-copy `with_ref` works!
        let upgraded = engine_v3.upgrade_legacy_schemas()?;
        assert_eq!(upgraded, 1);

        let val_plus_tier = engine_v3.with_ref(id, |view| view.archived().val + view.archived().tier)?;
        assert_eq!(val_plus_tier, 45);

        let _ = fs::remove_dir_all(&dir);
        Ok(())
    }
}
