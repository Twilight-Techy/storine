//! Columnar HTAP projection of document collections (`TASK-126`, `F-OLAP-02`).
//!
//! A columnar collection keeps its documents twice: as row records (the source of truth, every
//! OLTP path unchanged) and as immutable column segments (`storage-columnar`) for analytics.
//!
//! - **Commit cost**: none here. The engine records the ids of changed documents in a
//!   [`TagWatch`] under the MVCC writer guard (one relaxed atomic load when nothing is watched),
//!   on every write path: local commits, Raft apply, live WAL replay, snapshot install.
//! - **Delta**: ids drained from the watch, each with the sequence number of its last drain and
//!   the segment rows it occupies. A delta document's segment rows are hidden and the document
//!   is read from the row store instead.
//! - **Snapshot** ([`ColumnarCollection::snapshot`]): registers an MVCC snapshot `S` *then*
//!   drains the watch, so every write visible at `S` is in the delta. Live segment rows plus the
//!   delta documents as of `S` are exactly the collection as of `S` (`RULE-OLAP-02`).
//! - **Merge** ([`ColumnarCollection::merge`]): drains the watch, then reads the drained
//!   documents' current state into a new segment and their old rows into delete bitmaps; only
//!   entries drained later stay in the delta. A document read here may already be newer than its
//!   drained change; that newer change is in the watch, so it re-enters the delta and its row
//!   stays hidden: no snapshot is held during the build (no MVCC history pinned in RAM). Small
//!   or mostly-dead segments are rewritten from their own live rows. Files are published
//!   through an atomically replaced manifest; files it does not name are swept at open.
//! - **Open**: segments and delete bitmaps are loaded, then reconciled with the row store:
//!   every document whose current version is not a live segment row joins the delta, and every
//!   live row that is no longer current is deleted. A crash at any point therefore loses
//!   nothing: the manifest only ever names complete files.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use storage_codec::sealed_file::FileSeal;
use storage_codec::types::PropertyValue;
use storage_codec::{checksum_payload, crc32c_64, xxh3_64};
use storage_columnar::{flatten_owned, Segment, SegmentRow, SegmentWriter, Val, ROW_GROUP_ROWS};
use storage_mmap::atomic_write_replace;
use storage_types::{FastHashMap, InlineId, Result, StorageError};

use crate::document::{collection_tag, document_id, Document, DocumentRecord, COLLECTION_FIELD_ID};
use crate::engine::{file_seal, EncryptionConfig, StorageEngine, TagWatch};
use crate::mvcc::ReadSnapshot;

/// Directory (under the engine's base directory) holding every columnar collection.
pub const COLUMNAR_DIR: &str = "columnar";
const MANIFEST: &str = "MANIFEST";
const MANIFEST_MAGIC: &[u8; 8] = b"STGECMF1";
const DELETED_MAGIC: &[u8; 8] = b"STGECDL1";
/// Rows per segment file at most (larger merges write several files).
const SEGMENT_MAX_ROWS: usize = 4 * 1024 * 1024;
/// Merging segments beyond this count rewrites them into fewer.
const MAX_SEGMENTS: usize = 8;
/// A segment whose hidden fraction exceeds this is rewritten from its live rows.
const REWRITE_DEAD_FRACTION: f64 = 0.25;

/// The two independent hashes identifying a document id in segment id indexes.
fn fingerprint(id: InlineId) -> (u64, u32) {
    let raw = id.to_raw_bytes();
    (xxh3_64(&raw), crc32c_64(&raw) as u32)
}

/// A fixed-size bit set (one bit per segment row).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Bits {
    words: Vec<u64>,
    len: usize,
}

impl Bits {
    pub fn new(len: usize) -> Self {
        Self { words: vec![0; len.div_ceil(64)], len }
    }

    #[inline]
    pub fn get(&self, i: usize) -> bool {
        self.words.get(i / 64).is_some_and(|w| w >> (i % 64) & 1 == 1)
    }

    #[inline]
    pub fn set(&mut self, i: usize) {
        if let Some(w) = self.words.get_mut(i / 64) {
            *w |= 1 << (i % 64);
        }
    }

    pub fn count(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn words(&self) -> &[u64] {
        &self.words
    }
}

/// One published segment and its deleted rows.
#[derive(Clone, Debug)]
pub struct ColumnarSegment {
    pub gen: u64,
    pub segment: Arc<Segment>,
    pub deleted: Arc<Bits>,
    del_gen: u64,
}

#[derive(Clone, Debug)]
struct DeltaEntry {
    seq: u64,
    /// `(segment gen, row)` of the document's live rows when it was drained.
    rows: Vec<(u64, u64)>,
}

/// Merge statistics (for tests and operations).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MergeReport {
    pub rows_written: u64,
    pub segments_before: usize,
    pub segments_after: usize,
    pub delta_after: usize,
}

/// The columnar projection of one collection.
pub struct ColumnarCollection {
    name: String,
    dir: PathBuf,
    engine: Arc<StorageEngine<DocumentRecord>>,
    watch: Arc<TagWatch>,
    segments: RwLock<Arc<Vec<ColumnarSegment>>>,
    delta: Mutex<FastHashMap<InlineId, DeltaEntry>>,
    seq: AtomicU64,
    next_gen: AtomicU64,
    merge_lock: Mutex<()>,
    /// `true` once a manifest exists (before that every document is read from the row store).
    built: AtomicBool,
    last_merge: Mutex<Instant>,
    last_error: Mutex<Option<String>>,
}

impl core::fmt::Debug for ColumnarCollection {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ColumnarCollection").field("name", &self.name).finish_non_exhaustive()
    }
}

fn io(what: &'static str) -> impl Fn(std::io::Error) -> StorageError {
    move |e| StorageError::io(what, e)
}

fn segment_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("seg_{gen:016x}.col"))
}

fn deleted_path(dir: &Path, gen: u64, del_gen: u64) -> PathBuf {
    dir.join(format!("seg_{gen:016x}.del{del_gen}"))
}

/// Directory of a collection's segments (named by a hash: collection names are arbitrary).
pub fn collection_dir(base: &Path, collection: &str) -> PathBuf {
    base.join(COLUMNAR_DIR).join(format!("{:016x}", xxh3_64(collection.as_bytes())))
}

impl ColumnarCollection {
    /// Opens (or starts) the projection of `name`. The watch is registered first, so nothing
    /// written while segments load and reconcile is missed.
    pub fn open(engine: Arc<StorageEngine<DocumentRecord>>, name: &str) -> Result<Arc<Self>> {
        let watch = engine.watch_tag(COLLECTION_FIELD_ID, collection_tag(name));
        let dir = collection_dir(engine.base_dir(), name);
        fs::create_dir_all(&dir).map_err(io("create columnar directory"))?;
        let c = Arc::new(Self {
            name: name.to_owned(),
            dir,
            engine,
            watch,
            segments: RwLock::new(Arc::new(Vec::new())),
            delta: Mutex::new(FastHashMap::default()),
            seq: AtomicU64::new(0),
            next_gen: AtomicU64::new(1),
            merge_lock: Mutex::new(()),
            built: AtomicBool::new(false),
            last_merge: Mutex::new(Instant::now()),
            last_error: Mutex::new(None),
        });
        if let Err(e) = c.load() {
            // Unreadable projection: start over from the row store (it is only derived data).
            c.set_error(&e);
            *c.segments.write().unwrap_or_else(|p| p.into_inner()) = Arc::new(Vec::new());
            c.built.store(false, Ordering::Release);
        }
        Ok(c)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    fn encryption(&self) -> Option<Arc<EncryptionConfig>> {
        self.engine.encryption_config()
    }

    fn context(&self, gen: u64) -> Vec<u8> {
        format!("col:{}:{gen}", self.name).into_bytes()
    }

    fn set_error(&self, e: &StorageError) {
        *self.last_error.lock().unwrap_or_else(|p| p.into_inner()) = Some(e.to_string());
    }

    /// The last error a background merge or load hit (`None` when healthy).
    pub fn last_error(&self) -> Option<String> {
        self.last_error.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// Stops recording changes (the projection is being dropped).
    pub fn close(&self) {
        self.engine.unwatch_tag(&self.watch);
    }

    /// Deletes every file of the projection (after [`Self::close`]).
    pub fn remove_files(&self) {
        let _ = fs::remove_dir_all(&self.dir);
    }

    // ── manifest ──

    /// Deletes files the manifest does not name (a crash between writing a segment and
    /// publishing it, or a file a reader still had mapped when it became obsolete).
    fn sweep(&self, keep: &[ColumnarSegment]) {
        let Ok(entries) = fs::read_dir(&self.dir) else { return };
        let mut wanted: Vec<PathBuf> = vec![self.dir.join(MANIFEST)];
        for s in keep {
            wanted.push(segment_path(&self.dir, s.gen));
            if s.del_gen > 0 {
                wanted.push(deleted_path(&self.dir, s.gen, s.del_gen));
            }
        }
        for e in entries.flatten() {
            let p = e.path();
            if !wanted.contains(&p) {
                let _ = fs::remove_file(&p);
            }
        }
    }

    fn load(&self) -> Result<()> {
        let path = self.dir.join(MANIFEST);
        if !path.exists() {
            self.sweep(&[]);
            return Ok(());
        }
        let m = fs::read(&path).map_err(io("read columnar manifest"))?;
        if m.len() < 32 || &m[0..8] != MANIFEST_MAGIC {
            return Err(StorageError::corrupted_header("columnar manifest"));
        }
        let body = &m[..m.len() - 8];
        let sum = u64::from_le_bytes(m[m.len() - 8..].try_into().map_err(|_| StorageError::corrupted_header("manifest"))?);
        if checksum_payload(body) != sum {
            return Err(StorageError::corrupted_header("columnar manifest checksum"));
        }
        let u64_at = |o: usize| -> Result<u64> {
            body.get(o..o + 8)
                .and_then(|b| b.try_into().ok())
                .map(u64::from_le_bytes)
                .ok_or_else(|| StorageError::corrupted_header("columnar manifest"))
        };
        let next_gen = u64_at(8)?;
        let n = u64_at(16)? as usize;
        if body.len() != 24 + n * 16 {
            return Err(StorageError::corrupted_header("columnar manifest length"));
        }
        let enc = self.encryption();
        let seal = file_seal(enc.as_deref());
        let mut segs = Vec::with_capacity(n);
        for i in 0..n {
            let gen = u64_at(24 + i * 16)?;
            let del_gen = u64_at(32 + i * 16)?;
            let segment = Arc::new(Segment::open(&segment_path(&self.dir, gen), &seal, &self.context(gen))?);
            let rows = usize::try_from(segment.rows()).map_err(|_| StorageError::corrupted_header("rows"))?;
            let deleted = if del_gen == 0 { Bits::new(rows) } else { self.read_deleted(gen, del_gen, rows, &seal)? };
            segs.push(ColumnarSegment { gen, segment, deleted: Arc::new(deleted), del_gen });
        }
        self.next_gen.store(next_gen, Ordering::Release);
        self.sweep(&segs);
        *self.segments.write().unwrap_or_else(|p| p.into_inner()) = Arc::new(segs);
        self.built.store(true, Ordering::Release);
        self.reconcile()
    }

    fn write_manifest(&self, segs: &[ColumnarSegment]) -> Result<()> {
        let mut m = Vec::with_capacity(32 + segs.len() * 16);
        m.extend_from_slice(MANIFEST_MAGIC);
        m.extend_from_slice(&self.next_gen.load(Ordering::Acquire).to_le_bytes());
        m.extend_from_slice(&(segs.len() as u64).to_le_bytes());
        for s in segs {
            m.extend_from_slice(&s.gen.to_le_bytes());
            m.extend_from_slice(&s.del_gen.to_le_bytes());
        }
        let sum = checksum_payload(&m);
        m.extend_from_slice(&sum.to_le_bytes());
        atomic_write_replace(&self.dir.join(MANIFEST), &m, true)
    }

    fn write_deleted(&self, gen: u64, del_gen: u64, bits: &Bits, seal: &FileSeal<'_>) -> Result<()> {
        let mut buf = vec![0u8; seal.header_len()];
        buf.extend_from_slice(DELETED_MAGIC);
        buf.extend_from_slice(&(bits.len() as u64).to_le_bytes());
        for w in bits.words() {
            buf.extend_from_slice(&w.to_le_bytes());
        }
        let sum = checksum_payload(&buf[seal.header_len()..]);
        buf.extend_from_slice(&sum.to_le_bytes());
        seal.seal_in_place(&self.context(gen), del_gen, &mut buf)?;
        atomic_write_replace(&deleted_path(&self.dir, gen, del_gen), &buf, true)
    }

    fn read_deleted(&self, gen: u64, del_gen: u64, rows: usize, seal: &FileSeal<'_>) -> Result<Bits> {
        let mut buf = fs::read(deleted_path(&self.dir, gen, del_gen)).map_err(io("read delete bitmap"))?;
        let bad = || StorageError::corrupted_header("delete bitmap");
        let range = if storage_codec::is_sealed(&buf) {
            seal.open_in_place(&self.context(gen), del_gen, &mut buf)?
        } else if seal.accept_plain {
            0..buf.len()
        } else {
            return Err(StorageError::invalid_value("plaintext delete bitmap refused (encryption is required)"));
        };
        let b = &buf[range];
        if b.len() < 24 || &b[0..8] != DELETED_MAGIC {
            return Err(bad());
        }
        let (body, sum) = b.split_at(b.len() - 8);
        if checksum_payload(body) != u64::from_le_bytes(sum.try_into().map_err(|_| bad())?) {
            return Err(bad());
        }
        let len = u64::from_le_bytes(body[8..16].try_into().map_err(|_| bad())?) as usize;
        if len != rows || body.len() != 16 + rows.div_ceil(64) * 8 {
            return Err(bad());
        }
        let words = body[16..].chunks_exact(8).map(|c| u64::from_le_bytes([c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7]])).collect();
        Ok(Bits { words, len })
    }

    // ── delta ──

    fn locate(segs: &[ColumnarSegment], id: InlineId, seal: &FileSeal<'_>) -> Vec<(u64, u64)> {
        let (h, c) = fingerprint(id);
        let mut out = Vec::new();
        for s in segs {
            if let Ok(rows) = s.segment.rows_with_hash(h, c, seal) {
                out.extend(rows.into_iter().filter(|&r| !s.deleted.get(r as usize)).map(|r| (s.gen, r)));
            }
        }
        out
    }

    /// Moves drained ids into the delta. Caller holds the delta lock.
    fn absorb_locked(&self, delta: &mut FastHashMap<InlineId, DeltaEntry>) {
        let ids = self.watch.drain();
        if ids.is_empty() {
            return;
        }
        let seq = self.seq.fetch_add(1, Ordering::AcqRel) + 1;
        let segs = Arc::clone(&self.segments.read().unwrap_or_else(|p| p.into_inner()));
        let enc = self.encryption();
        let seal = file_seal(enc.as_deref());
        for id in ids {
            let rows = Self::locate(&segs, id, &seal);
            delta.insert(id, DeltaEntry { seq, rows });
        }
    }

    /// Documents changed since their segment rows were written (drained or not).
    pub fn pending(&self) -> usize {
        self.delta.lock().unwrap_or_else(|p| p.into_inner()).len() + self.watch.pending()
    }

    /// Live segment rows.
    pub fn segment_rows(&self) -> u64 {
        self.segments
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .map(|s| s.segment.rows() - s.deleted.count() as u64)
            .sum()
    }

    /// Whether a background merge is due.
    pub fn needs_merge(&self) -> bool {
        if !self.built.load(Ordering::Acquire) {
            return true;
        }
        let pending = self.pending();
        if pending == 0 {
            return false;
        }
        let threshold = (self.segment_rows() as usize / 50).max(4096);
        let idle = self.last_merge.lock().unwrap_or_else(|p| p.into_inner()).elapsed() > Duration::from_secs(10);
        pending >= threshold || idle
    }

    /// Ordinals of the collection's documents (current state).
    fn collection_ids(&self) -> Vec<InlineId> {
        let bm = self.engine.tag_index().get_bitmap(COLLECTION_FIELD_ID, collection_tag(&self.name));
        let primary = self.engine.primary_index();
        bm.to_vec().into_iter().filter_map(|ord| primary.resolve_ordinal(ord)).collect()
    }

    /// The current state of `id` if it belongs to this collection.
    fn doc_now(&self, id: InlineId) -> Result<Option<Document>> {
        match self.engine.get_owned(id) {
            Ok((version, rec)) => Ok((rec.collection == self.name).then_some(Document { key: rec.key, version, fields: rec.fields })),
            Err(StorageError::RecordNotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn doc_at(&self, read: &ReadSnapshot<'_, DocumentRecord>, id: InlineId) -> Result<Option<Document>> {
        Ok(read.get(id)?.and_then(|(version, rec)| {
            (rec.collection == self.name).then_some(Document { key: rec.key, version, fields: rec.fields })
        }))
    }

    // ── reads ──

    /// A consistent analytic view of the collection: live segment rows plus the delta
    /// documents, both as of one MVCC snapshot.
    pub fn snapshot(&self) -> Result<ColumnarSnapshot<'_>> {
        // Snapshot first, then drain: every write visible at the snapshot is in the delta.
        let read = self.engine.begin_read_snapshot();
        let built = self.built.load(Ordering::Acquire);
        let (segs, ids, hidden) = {
            let mut delta = self.delta.lock().unwrap_or_else(|p| p.into_inner());
            self.absorb_locked(&mut delta);
            let segs = Arc::clone(&self.segments.read().unwrap_or_else(|p| p.into_inner()));
            let index_of: FastHashMap<u64, usize> = segs.iter().enumerate().map(|(i, s)| (s.gen, i)).collect();
            let mut hidden: Vec<Vec<u32>> = vec![Vec::new(); segs.len()];
            for e in delta.values() {
                for &(gen, row) in &e.rows {
                    if let Some(&i) = index_of.get(&gen) {
                        hidden[i].push(row as u32);
                    }
                }
            }
            let ids: Vec<InlineId> = if built { delta.keys().copied().collect() } else { Vec::new() };
            (segs, ids, hidden)
        };
        let mut hidden = hidden;
        for h in &mut hidden {
            h.sort_unstable();
            h.dedup();
        }
        // Before the first build, every document comes from the row store.
        let ids = if built { ids } else { self.collection_ids() };
        let mut docs = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(d) = self.doc_at(&read, id)? {
                docs.push(d);
            }
        }
        docs.sort_by(|a, b| a.key.cmp(&b.key));
        let segs = if built { segs } else { Arc::new(Vec::new()) };
        let hidden = if built { hidden } else { Vec::new() };
        Ok(ColumnarSnapshot { read, segments: segs, hidden, delta: docs, enc: self.encryption() })
    }

    // ── maintenance ──

    /// Folds the delta into segments (and rewrites small or mostly dead ones). Runs in the
    /// background; callable directly. One merge at a time.
    pub fn merge(&self) -> Result<MergeReport> {
        let result = self.merge_inner();
        *self.last_merge.lock().unwrap_or_else(|p| p.into_inner()) = Instant::now();
        match &result {
            Ok(_) => *self.last_error.lock().unwrap_or_else(|p| p.into_inner()) = None,
            Err(e) => self.set_error(e),
        }
        result
    }

    fn merge_inner(&self) -> Result<MergeReport> {
        let _merging = self.merge_lock.lock().unwrap_or_else(|p| p.into_inner());
        let initial = !self.built.load(Ordering::Acquire);
        // Drain, then snapshot: every entry drained so far describes changes before `S_b`.
        let (seq_b, absorbed, segs_now) = {
            let mut delta = self.delta.lock().unwrap_or_else(|p| p.into_inner());
            self.absorb_locked(&mut delta);
            let absorbed: Vec<(InlineId, Vec<(u64, u64)>)> = delta.iter().map(|(id, e)| (*id, e.rows.clone())).collect();
            (self.seq.load(Ordering::Acquire), absorbed, Arc::clone(&self.segments.read().unwrap_or_else(|p| p.into_inner())))
        };
        let enc = self.encryption();
        let seal = file_seal(enc.as_deref());

        // Rows hidden in each current segment once the absorbed changes are applied.
        let mut dead: Vec<Bits> = segs_now.iter().map(|s| (*s.deleted).clone()).collect();
        let index_of: FastHashMap<u64, usize> = segs_now.iter().enumerate().map(|(i, s)| (s.gen, i)).collect();
        for (_, rows) in &absorbed {
            for &(gen, row) in rows {
                if let Some(&i) = index_of.get(&gen) {
                    dead[i].set(row as usize);
                }
            }
        }
        // Which segments to rewrite from their own live rows.
        let rewrite: Vec<bool> = if segs_now.len() + 1 > MAX_SEGMENTS {
            vec![true; segs_now.len()]
        } else {
            segs_now
                .iter()
                .zip(&dead)
                .map(|(s, d)| s.segment.rows() == 0 || d.count() as f64 > s.segment.rows() as f64 * REWRITE_DEAD_FRACTION)
                .collect()
        };

        // Rows of the new segment(s): changed documents as of `S_b` (every document on the
        // first build) plus the live rows of rewritten segments.
        // (key, version, id, cells)
        type NewRow = (String, u64, InlineId, Vec<(String, Val)>);
        let mut rows: Vec<NewRow> = Vec::new();
        let ids: Vec<InlineId> = if initial { self.collection_ids() } else { absorbed.iter().map(|(id, _)| *id).collect() };
        for id in ids {
            if let Some(d) = self.doc_now(id)? {
                let mut cells = Vec::new();
                flatten_owned(&d.fields, &mut cells);
                rows.push((d.key, d.version.0, id, cells));
            }
        }
        for (i, s) in segs_now.iter().enumerate() {
            if !rewrite[i] {
                continue;
            }
            for (g, group) in s.segment.row_groups().iter().enumerate() {
                let decoded = s.segment.decode_rows(g, &seal)?;
                for (r, (key, version, cells)) in decoded.into_iter().enumerate() {
                    if dead[i].get(group.first_row as usize + r) {
                        continue;
                    }
                    let id = document_id(&self.name, &key)?;
                    rows.push((key, version, id, cells));
                }
            }
        }
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        let rows_written = rows.len() as u64;

        // Write the new segments, then the changed delete bitmaps, then the manifest.
        let mut next: Vec<ColumnarSegment> = Vec::new();
        let mut obsolete: Vec<PathBuf> = Vec::new();
        for (i, s) in segs_now.iter().enumerate() {
            if rewrite[i] {
                obsolete.push(segment_path(&self.dir, s.gen));
                if s.del_gen > 0 {
                    obsolete.push(deleted_path(&self.dir, s.gen, s.del_gen));
                }
                continue;
            }
            let mut kept = s.clone();
            if dead[i] != *s.deleted {
                kept.del_gen = s.del_gen + 1;
                self.write_deleted(s.gen, kept.del_gen, &dead[i], &seal)?;
                if s.del_gen > 0 {
                    obsolete.push(deleted_path(&self.dir, s.gen, s.del_gen));
                }
                kept.deleted = Arc::new(dead[i].clone());
            }
            next.push(kept);
        }
        for part in rows.chunks(SEGMENT_MAX_ROWS) {
            let gen = self.next_gen.fetch_add(1, Ordering::AcqRel);
            let path = segment_path(&self.dir, gen);
            let mut w = SegmentWriter::create(&path, seal, &self.context(gen), ROW_GROUP_ROWS, seq_b)?;
            for (key, version, id, cells) in part {
                let (h, c) = fingerprint(*id);
                w.push(SegmentRow { id_hash: h, id_check: c, key, version: *version, cells })?;
            }
            w.finish()?;
            let segment = Arc::new(Segment::open(&path, &seal, &self.context(gen))?);
            let n = segment.rows() as usize;
            next.push(ColumnarSegment { gen, segment, deleted: Arc::new(Bits::new(n)), del_gen: 0 });
        }
        self.write_manifest(&next)?;

        // Publish: entries drained after `S_b` stay, relocated in the new segments.
        let delta_after = {
            let mut delta = self.delta.lock().unwrap_or_else(|p| p.into_inner());
            *self.segments.write().unwrap_or_else(|p| p.into_inner()) = Arc::new(next.clone());
            delta.retain(|_, e| e.seq > seq_b);
            for (id, e) in delta.iter_mut() {
                e.rows = Self::locate(&next, *id, &seal);
            }
            self.built.store(true, Ordering::Release);
            delta.len()
        };
        for p in obsolete {
            // A file still mapped by a reader cannot be removed on Windows; open sweeps it later.
            let _ = fs::remove_file(p);
        }
        Ok(MergeReport { rows_written, segments_before: segs_now.len(), segments_after: next.len(), delta_after })
    }

    /// After loading: documents whose current version is not a live segment row join the
    /// delta; live rows that are no longer current are deleted.
    fn reconcile(&self) -> Result<()> {
        let enc = self.encryption();
        let seal = file_seal(enc.as_deref());
        let segs = Arc::clone(&self.segments.read().unwrap_or_else(|p| p.into_inner()));
        let mut versions: Vec<Vec<u64>> = Vec::with_capacity(segs.len());
        for s in segs.iter() {
            let mut v = Vec::with_capacity(s.segment.rows() as usize);
            for g in 0..s.segment.row_groups().len() {
                v.extend(s.segment.versions(g, &seal)?);
            }
            versions.push(v);
        }
        let index_of: FastHashMap<u64, usize> = segs.iter().enumerate().map(|(i, s)| (s.gen, i)).collect();
        let mut confirmed: Vec<Bits> = segs.iter().map(|s| Bits::new(s.segment.rows() as usize)).collect();
        let primary = self.engine.primary_index();
        let mut delta = self.delta.lock().unwrap_or_else(|p| p.into_inner());
        let seq = self.seq.fetch_add(1, Ordering::AcqRel) + 1;
        for id in self.collection_ids() {
            let Some(current) = primary.version_of(id) else { continue };
            let rows = Self::locate(&segs, id, &seal);
            let hit = rows.iter().find(|(gen, row)| {
                index_of.get(gen).and_then(|&i| versions[i].get(*row as usize)) == Some(&current.0)
            });
            match (hit, rows.len()) {
                (Some(&(gen, row)), 1) => {
                    if let Some(&i) = index_of.get(&gen) {
                        confirmed[i].set(row as usize);
                    }
                }
                _ => {
                    delta.insert(id, DeltaEntry { seq, rows });
                }
            }
        }
        // Live rows neither confirmed nor hidden by the delta belong to deleted documents.
        let mut hidden: Vec<Bits> = segs.iter().map(|s| Bits::new(s.segment.rows() as usize)).collect();
        for e in delta.values() {
            for &(gen, row) in &e.rows {
                if let Some(&i) = index_of.get(&gen) {
                    hidden[i].set(row as usize);
                }
            }
        }
        let mut next: Vec<ColumnarSegment> = segs.as_ref().clone();
        for (i, s) in next.iter_mut().enumerate() {
            let mut dead = (*s.deleted).clone();
            for r in 0..dead.len() {
                if !dead.get(r) && !confirmed[i].get(r) && !hidden[i].get(r) {
                    dead.set(r);
                }
            }
            if dead != *s.deleted {
                s.deleted = Arc::new(dead);
            }
        }
        *self.segments.write().unwrap_or_else(|p| p.into_inner()) = Arc::new(next);
        Ok(())
    }
}

/// A consistent analytic view: live segment rows and delta documents as of one snapshot.
pub struct ColumnarSnapshot<'e> {
    read: ReadSnapshot<'e, DocumentRecord>,
    pub segments: Arc<Vec<ColumnarSegment>>,
    /// Rows hidden by the delta, per segment (sorted).
    hidden: Vec<Vec<u32>>,
    /// Documents read from the row store as of the snapshot (in key order).
    pub delta: Vec<Document>,
    enc: Option<Arc<EncryptionConfig>>,
}

impl core::fmt::Debug for ColumnarSnapshot<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ColumnarSnapshot")
            .field("segments", &self.segments.len())
            .field("delta", &self.delta.len())
            .finish_non_exhaustive()
    }
}

impl ColumnarSnapshot<'_> {
    /// How segment files are opened (encryption at rest).
    pub fn seal(&self) -> FileSeal<'_> {
        file_seal(self.enc.as_deref())
    }

    /// The MVCC read timestamp of the view.
    pub fn read_ts(&self) -> u64 {
        self.read.read_ts().0
    }

    /// Whether row `row` of segment `seg` is part of the view.
    #[inline]
    pub fn is_live(&self, seg: usize, row: u64) -> bool {
        let Some(s) = self.segments.get(seg) else { return false };
        !s.deleted.get(row as usize) && self.hidden.get(seg).is_none_or(|h| h.binary_search(&(row as u32)).is_err())
    }

    /// Live-row bits of row group `group` of segment `seg` (bit `i` = row `first_row + i`).
    pub fn live_mask(&self, seg: usize, group: usize) -> Vec<u64> {
        let Some(s) = self.segments.get(seg) else { return Vec::new() };
        let Some(g) = s.segment.row_groups().get(group) else { return Vec::new() };
        let n = g.rows as usize;
        let mut mask = vec![u64::MAX; n.div_ceil(64)];
        if n % 64 != 0 {
            if let Some(last) = mask.last_mut() {
                *last = (1u64 << (n % 64)) - 1;
            }
        }
        let first = g.first_row as usize;
        for i in 0..n {
            if s.deleted.get(first + i) {
                mask[i / 64] &= !(1 << (i % 64));
            }
        }
        if let Some(h) = self.hidden.get(seg) {
            let lo = h.partition_point(|&r| (r as usize) < first);
            for &r in &h[lo..] {
                let r = r as usize;
                if r >= first + n {
                    break;
                }
                mask[(r - first) / 64] &= !(1 << ((r - first) % 64));
            }
        }
        mask
    }

    /// Every document of the view rebuilt from columns and delta (slow path: tests, fallback).
    pub fn documents(&self) -> Result<Vec<Document>> {
        let seal = self.seal();
        let mut out = Vec::new();
        for (si, s) in self.segments.iter().enumerate() {
            for (g, group) in s.segment.row_groups().iter().enumerate() {
                for (r, (key, version, cells)) in s.segment.decode_rows(g, &seal)?.into_iter().enumerate() {
                    if self.is_live(si, group.first_row + r as u64) {
                        out.push(Document { key, version: storage_types::Version(version), fields: unflatten(cells) });
                    }
                }
            }
        }
        out.extend(self.delta.iter().cloned());
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }
}

/// Rebuilds nested maps from dotted column paths.
pub fn unflatten(cells: Vec<(String, Val)>) -> BTreeMap<String, PropertyValue> {
    fn insert(map: &mut BTreeMap<String, PropertyValue>, path: &str, v: PropertyValue) {
        match path.split_once('.') {
            None => {
                map.insert(path.to_owned(), v);
            }
            Some((head, rest)) => {
                let slot = map.entry(head.to_owned()).or_insert_with(|| PropertyValue::Map(BTreeMap::new()));
                if !matches!(slot, PropertyValue::Map(_)) {
                    *slot = PropertyValue::Map(BTreeMap::new());
                }
                if let PropertyValue::Map(m) = slot {
                    insert(m, rest, v);
                }
            }
        }
    }
    let mut m = BTreeMap::new();
    for (path, v) in cells {
        insert(&mut m, &path, v.to_property());
    }
    m
}

/// The projections of one document store and the background thread that merges them.
#[derive(Debug, Default)]
pub struct ColumnarRegistry {
    map: RwLock<FastHashMap<String, Arc<ColumnarCollection>>>,
    stop: AtomicBool,
    wake: (Mutex<bool>, std::sync::Condvar),
    thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

/// How often the merger looks for work while idle (it parks in between, `RULE-CPU-03`).
const MERGER_TICK: Duration = Duration::from_secs(2);

impl ColumnarRegistry {
    pub fn get(&self, name: &str) -> Option<Arc<ColumnarCollection>> {
        self.map.read().unwrap_or_else(|p| p.into_inner()).get(name).cloned()
    }

    /// Opens the projection of `name` if needed and makes sure the merger runs.
    pub fn ensure(self: &Arc<Self>, engine: &Arc<StorageEngine<DocumentRecord>>, name: &str) -> Result<Arc<ColumnarCollection>> {
        if let Some(c) = self.get(name) {
            return Ok(c);
        }
        let mut map = self.map.write().unwrap_or_else(|p| p.into_inner());
        if let Some(c) = map.get(name) {
            return Ok(Arc::clone(c));
        }
        let c = ColumnarCollection::open(Arc::clone(engine), name)?;
        map.insert(name.to_owned(), Arc::clone(&c));
        drop(map);
        self.start_merger();
        self.notify();
        Ok(c)
    }

    /// Closes and deletes the projection of `name`.
    pub fn remove(&self, name: &str) {
        let removed = self.map.write().unwrap_or_else(|p| p.into_inner()).remove(name);
        if let Some(c) = removed {
            c.close();
            // Wait out a merge in progress before deleting its files.
            let _merging = c.merge_lock.lock().unwrap_or_else(|p| p.into_inner());
            c.remove_files();
        }
    }

    fn notify(&self) {
        let (m, cv) = &self.wake;
        *m.lock().unwrap_or_else(|p| p.into_inner()) = true;
        cv.notify_all();
    }

    fn start_merger(self: &Arc<Self>) {
        let mut t = self.thread.lock().unwrap_or_else(|p| p.into_inner());
        if t.is_some() || self.stop.load(Ordering::Acquire) {
            return;
        }
        let me = Arc::clone(self);
        *t = std::thread::Builder::new()
            .name("columnar-merger".into())
            .spawn(move || me.run())
            .ok();
    }

    fn run(&self) {
        while !self.stop.load(Ordering::Acquire) {
            let due: Vec<Arc<ColumnarCollection>> = self
                .map
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .values()
                .filter(|c| c.needs_merge())
                .cloned()
                .collect();
            for c in due {
                if self.stop.load(Ordering::Acquire) {
                    return;
                }
                // Errors are kept on the projection (`last_error`); queries stay exact anyway.
                let _ = c.merge();
            }
            let (m, cv) = &self.wake;
            let mut woken = m.lock().unwrap_or_else(|p| p.into_inner());
            if !*woken && !self.stop.load(Ordering::Acquire) {
                woken = cv.wait_timeout(woken, MERGER_TICK).map_or_else(|p| p.into_inner().0, |r| r.0);
            }
            *woken = false;
        }
    }

    /// Stops the merger (projections stay on disk).
    pub fn shutdown(&self) {
        self.stop.store(true, Ordering::Release);
        self.notify();
        let handle = self.thread.lock().unwrap_or_else(|p| p.into_inner()).take();
        if let Some(h) = handle {
            let _ = h.join();
        }
        for c in self.map.write().unwrap_or_else(|p| p.into_inner()).drain().map(|(_, c)| c) {
            c.close();
        }
    }
}

