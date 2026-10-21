//! Group-Commit Write-Ahead Log (WAL) with batched `fsync`, transactional commit barriers,
//! and deterministic crash-recovery replay (`TASK-014`, `F-DURA-01`, `F-DURA-02`, `F-DURA-04`, `RULE-SAFE-01`).
//!
//! Guarantees:
//! - Log Sequence Numbers (LSNs) are assigned under the WAL lock, so file order == LSN order.
//! - LSNs and transaction ids are monotonic across restarts (recovered from the file on `open`).
//! - An append returns `Ok` only after *its own* frames were written (and `fsync`ed under the
//!   `Immediate` / `GroupCommit` policies).
//! - Any I/O failure is fail-stop: the WAL refuses further appends, because a partially written
//!   or un-synced log cannot be trusted (the same policy PostgreSQL uses for `fsync` failures).
//! - Checkpoint truncation is crash-safe: frames that must survive are written to a new file
//!   which atomically replaces the old one, and removed frames can be archived for PITR.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};

use storage_codec::{checksum_payload, compress_lz4, decompress_lz4};
use storage_types::{CachePadded, InlineId, Result, StorageError, Version, MAX_PAYLOAD_SIZE};

const WAL_MAGIC: u32 = 0x5741_4C31; // "WAL1"
const WAL_FRAME_HEADER_LEN: usize = 72;
const WAL_FLAG_LZ4: u8 = 0x01;
/// Minimum WAL frame payload size (`1 KiB`) before transparent LZ4 frame compression is applied (`TASK-053`).
const WAL_LZ4_THRESHOLD: usize = 1024;
/// Record id carried by transaction-commit marker frames (validated at compile time).
const TXN_COMMIT_ID: InlineId = InlineId::from_static("txn_commit");
/// Record id carried by checkpoint marker frames (validated at compile time).
const CHECKPOINT_ID: InlineId = InlineId::from_static("checkpoint");

/// Durability policy controlling when WAL appends call `sync_data()` (`F-DURA-02`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncPolicy {
    /// Every write or group-commit batch flushes and `fsync`s to stable storage before returning.
    Immediate,
    /// Coalesces concurrent thread writes into a single sequential write + `fsync` batch.
    GroupCommit { max_batch_entries: usize },
    /// Memory-first: frames are buffered in RAM and pushed to the OS by
    /// [`GroupCommitWal::flush_buffered`] (the engine's background flusher calls it every
    /// `flush_interval_ms`); never `fsync`ed. A process crash can lose the last flush interval of
    /// acknowledged writes; an OS crash or power loss can lose everything not yet written back.
    AsyncMemoryFirst,
}

impl Default for SyncPolicy {
    fn default() -> Self {
        Self::GroupCommit {
            max_batch_entries: 64,
        }
    }
}

impl SyncPolicy {
    #[inline]
    const fn syncs(self) -> bool {
        matches!(self, Self::Immediate | Self::GroupCommit { .. })
    }
}

/// Type of operation recorded in a WAL frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum WalOpKind {
    Upsert = 1,
    Delete = 2,
    TxnCommit = 3,
    /// Internal marker written at the head of a truncated WAL so LSN / txn-id counters stay
    /// monotonic across restarts. Never returned by recovery.
    Checkpoint = 4,
}

impl WalOpKind {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::Upsert),
            2 => Some(Self::Delete),
            3 => Some(Self::TxnCommit),
            4 => Some(Self::Checkpoint),
            _ => None,
        }
    }
}

/// Decoded WAL entry replayed during crash recovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalEntry {
    pub lsn: u64,
    pub txn_id: u64,
    pub collection_id: u16,
    pub kind: WalOpKind,
    pub record_id: InlineId,
    pub version: Version,
    /// Full 64-byte-aligned record frame for `Upsert`; for `Delete` an optional 8-byte
    /// little-endian deletion timestamp (nanoseconds since the UNIX epoch).
    pub frame_bytes: Vec<u8>,
}

impl WalEntry {
    /// Returns the deletion timestamp carried by a `Delete` frame, if present.
    #[inline]
    pub fn delete_timestamp_ns(&self) -> Option<u64> {
        if self.kind == WalOpKind::Delete && self.frame_bytes.len() == 8 {
            Some(storage_types::wire::read_u64_le(&self.frame_bytes, 0))
        } else {
            None
        }
    }
}

/// One operation of a multi-record transaction (`append_transaction`).
pub type WalTxnOp = (u16, WalOpKind, InlineId, Version, Vec<u8>);

struct PendingItem {
    collection_id: u16,
    txn_id: u64,
    kind: WalOpKind,
    record_id: InlineId,
    version: Version,
    frame_bytes: Vec<u8>,
    lsn: u64,
}

struct GroupCommitState {
    queue: Vec<PendingItem>,
    /// Last LSN handed out (LSNs are assigned under this lock).
    last_lsn: u64,
    /// Monotonic submission ticket counter.
    enqueued_seq: u64,
    /// All submissions with `seq <= completed_seq` are durably written.
    completed_seq: u64,
    flushing: bool,
    /// Set after any I/O failure; the WAL is fail-stop from then on.
    broken: Option<String>,
}

/// High-throughput Group-Commit Write-Ahead Log (`RULE-CONC-03`).
///
/// Concurrent writers enqueue their frames; one leader writes the whole batch with a single
/// `write` + `fsync` while the others wait. File I/O happens outside the queue lock so new
/// writers can keep enqueueing into the next batch while the current one is being synced.
pub struct GroupCommitWal {
    path: PathBuf,
    policy: SyncPolicy,
    current_lsn: CachePadded<AtomicU64>,
    next_txn_id: CachePadded<AtomicU64>,
    state: Mutex<GroupCommitState>,
    cv: Condvar,
    io: Mutex<BufWriter<File>>,
    total_batches: CachePadded<AtomicU64>,
    total_frames: CachePadded<AtomicU64>,
}

impl core::fmt::Debug for GroupCommitWal {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("GroupCommitWal")
            .field("path", &self.path)
            .field("policy", &self.policy)
            .field("current_lsn", &self.current_lsn.load(Ordering::Relaxed))
            .finish()
    }
}

/// Parsed frame plus its raw byte range inside the file.
struct RawFrame {
    entry: WalEntry,
    start: usize,
    end: usize,
}

/// Parses all valid frames from `raw`, stopping at the first torn/corrupt frame.
/// Returns the frames and the byte offset just past the last valid frame.
fn parse_frames(raw: &[u8]) -> (Vec<RawFrame>, usize) {
    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos + WAL_FRAME_HEADER_LEN <= raw.len() {
        let Some(h) = decode_wal_frame_header(&raw[pos..pos + WAL_FRAME_HEADER_LEN]) else {
            break;
        };
        if h.body_len > MAX_PAYLOAD_SIZE as usize || h.uncompressed_len > MAX_PAYLOAD_SIZE as usize {
            break;
        }
        let frame_end = match (pos + WAL_FRAME_HEADER_LEN).checked_add(h.body_len) {
            Some(end) if end <= raw.len() => end,
            _ => break,
        };
        let body = &raw[pos + WAL_FRAME_HEADER_LEN..frame_end];
        let actual_sum = compute_wal_checksum(
            h.lsn,
            h.txn_id,
            h.collection_id,
            h.kind,
            h.flags,
            h.record_id,
            h.version,
            body,
            h.uncompressed_len as u32,
        );
        if actual_sum != h.checksum {
            break;
        }
        let frame_bytes = if (h.flags & WAL_FLAG_LZ4) != 0 {
            let Ok(decompressed) = decompress_lz4(body, h.uncompressed_len) else {
                break;
            };
            decompressed
        } else {
            body.to_vec()
        };
        out.push(RawFrame {
            entry: WalEntry {
                lsn: h.lsn,
                txn_id: h.txn_id,
                collection_id: h.collection_id,
                kind: h.kind,
                record_id: h.record_id,
                version: h.version,
                frame_bytes,
            },
            start: pos,
            end: frame_end,
        });
        pos = frame_end;
    }
    (out, pos)
}

/// Resolves committed entries from parsed frames (transactions only when their commit barrier
/// is present with the matching operation count). `Checkpoint` markers are skipped.
fn committed_from_frames(frames: Vec<RawFrame>) -> Vec<WalEntry> {
    let mut committed = Vec::new();
    let mut pending_txns: HashMap<u64, Vec<WalEntry>> = HashMap::new();
    for f in frames {
        let entry = f.entry;
        match entry.kind {
            WalOpKind::Checkpoint => {}
            WalOpKind::TxnCommit => {
                if let Some(ops) = pending_txns.remove(&entry.txn_id) {
                    if ops.len() as u64 == entry.version.0 {
                        committed.extend(ops);
                    }
                }
            }
            _ if entry.txn_id == 0 => committed.push(entry),
            _ => pending_txns.entry(entry.txn_id).or_default().push(entry),
        }
    }
    committed
}

fn read_file_if_exists(path: &Path) -> Result<Vec<u8>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut file = File::open(path).map_err(|e| StorageError::io("open WAL for reading", e))?;
    let mut raw = Vec::new();
    file.read_to_end(&mut raw)
        .map_err(|e| StorageError::io("read WAL", e))?;
    Ok(raw)
}

#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        let dir = File::open(parent).map_err(|e| StorageError::io("open WAL dir for fsync", e))?;
        dir.sync_all()
            .map_err(|e| StorageError::io("fsync WAL directory", e))?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_dir(_path: &Path) -> Result<()> {
    Ok(())
}

impl GroupCommitWal {
    /// Opens (or creates) the WAL at `path`.
    ///
    /// Any torn trailing frame left by a crash is truncated, and the LSN / transaction-id
    /// counters resume after the highest values found in the file.
    pub fn open(path: impl Into<PathBuf>, policy: SyncPolicy) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)
                    .map_err(|e| StorageError::io("create WAL directory", e))?;
            }
        }

        let raw = read_file_if_exists(&path)?;
        let (frames, valid_len) = parse_frames(&raw);
        let mut max_lsn = 0u64;
        let mut max_txn = 0u64;
        for f in &frames {
            max_lsn = max_lsn.max(f.entry.lsn);
            max_txn = max_txn.max(f.entry.txn_id);
            if f.entry.kind == WalOpKind::Checkpoint {
                // Checkpoint markers carry `next_txn_id - 1` in `version`.
                max_txn = max_txn.max(f.entry.version.0);
            }
        }

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| StorageError::io("open WAL file", e))?;
        if valid_len < raw.len() {
            file.set_len(valid_len as u64)
                .map_err(|e| StorageError::io("truncate torn WAL tail", e))?;
            file.sync_data()
                .map_err(|e| StorageError::io("fsync truncated WAL tail", e))?;
        }
        file.seek(SeekFrom::End(0))
            .map_err(|e| StorageError::io("seek WAL end", e))?;

        Ok(Self {
            path,
            policy,
            current_lsn: CachePadded::new(AtomicU64::new(max_lsn)),
            next_txn_id: CachePadded::new(AtomicU64::new(max_txn + 1)),
            state: Mutex::new(GroupCommitState {
                queue: Vec::with_capacity(64),
                last_lsn: max_lsn,
                enqueued_seq: 0,
                completed_seq: 0,
                flushing: false,
                broken: None,
            }),
            cv: Condvar::new(),
            io: Mutex::new(BufWriter::with_capacity(256 * 1024, file)),
            total_batches: CachePadded::new(AtomicU64::new(0)),
            total_frames: CachePadded::new(AtomicU64::new(0)),
        })
    }

    #[inline]
    pub fn allocate_txn_id(&self) -> u64 {
        self.next_txn_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Appends a standalone (`txn_id == 0`) upsert or delete operation to the WAL and returns its LSN.
    pub fn append_single(
        &self,
        collection_id: u16,
        kind: WalOpKind,
        record_id: InlineId,
        version: Version,
        frame_bytes: &[u8],
    ) -> Result<u64> {
        let mut guard = self.lock_state();
        if let Some(ref msg) = guard.broken {
            return Err(Self::broken_error(msg));
        }
        if !guard.flushing && guard.queue.is_empty() {
            // Uncontended fast path: become leader for a single frame without allocating a batch.
            guard.flushing = true;
            guard.last_lsn += 1;
            let lsn = guard.last_lsn;
            self.current_lsn.store(lsn, Ordering::Release);
            guard.enqueued_seq += 1;
            let my_seq = guard.enqueued_seq;
            drop(guard);

            let res = {
                let mut io = self.lock_io();
                Self::write_frame(&mut *io, lsn, 0, collection_id, kind, record_id, version, frame_bytes)
                    .and_then(|()| self.finish_batch_io(&mut io))
            };
            self.total_frames.fetch_add(1, Ordering::Relaxed);
            self.complete_batch(my_seq, res)?;
            if kind != WalOpKind::Upsert {
                // Only the hot upsert path is left buffered under `AsyncMemoryFirst`.
                self.flush_buffered()?;
            }
            return Ok(lsn);
        }

        guard.last_lsn += 1;
        let lsn = guard.last_lsn;
        self.current_lsn.store(lsn, Ordering::Release);
        guard.queue.push(PendingItem {
            collection_id,
            txn_id: 0,
            kind,
            record_id,
            version,
            frame_bytes: frame_bytes.to_vec(),
            lsn,
        });
        guard.enqueued_seq += 1;
        let my_seq = guard.enqueued_seq;
        self.total_frames.fetch_add(1, Ordering::Relaxed);
        self.wait_for(guard, my_seq)?;
        if kind != WalOpKind::Upsert {
            self.flush_buffered()?;
        }
        Ok(lsn)
    }

    /// Atomically appends a multi-record transaction (`ops` followed by a `TxnCommit` barrier)
    /// in a single group-commit batch (`TASK-024`). Returns the commit barrier's LSN.
    pub fn append_transaction(&self, txn_id: u64, ops: Vec<WalTxnOp>) -> Result<u64> {
        if ops.is_empty() {
            return Ok(0);
        }
        let count = ops.len() as u64;
        let mut guard = self.lock_state();
        if let Some(ref msg) = guard.broken {
            return Err(Self::broken_error(msg));
        }
        for (collection_id, kind, record_id, version, frame_bytes) in ops {
            guard.last_lsn += 1;
            let lsn = guard.last_lsn;
            guard.queue.push(PendingItem {
                collection_id,
                txn_id,
                kind,
                record_id,
                version,
                frame_bytes,
                lsn,
            });
        }
        guard.last_lsn += 1;
        let commit_lsn = guard.last_lsn;
        guard.queue.push(PendingItem {
            collection_id: 0,
            txn_id,
            kind: WalOpKind::TxnCommit,
            record_id: TXN_COMMIT_ID,
            version: Version(count),
            frame_bytes: Vec::new(),
            lsn: commit_lsn,
        });
        self.current_lsn.store(commit_lsn, Ordering::Release);
        guard.enqueued_seq += 1;
        let my_seq = guard.enqueued_seq;
        self.total_frames.fetch_add(count + 1, Ordering::Relaxed);
        self.wait_for(guard, my_seq)?;
        self.flush_buffered()?;
        Ok(commit_lsn)
    }

    #[inline]
    fn lock_state(&self) -> std::sync::MutexGuard<'_, GroupCommitState> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[inline]
    fn lock_io(&self) -> std::sync::MutexGuard<'_, BufWriter<File>> {
        self.io.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[cold]
    fn broken_error(msg: &str) -> StorageError {
        StorageError::Io {
            context: "WAL is fail-stopped after an earlier I/O error",
            raw_os_error: None,
            message: msg.to_string(),
        }
    }

    /// Waits until submission `my_seq` is durable, becoming the group-commit leader when needed.
    fn wait_for<'a>(
        &'a self,
        mut guard: std::sync::MutexGuard<'a, GroupCommitState>,
        my_seq: u64,
    ) -> Result<()> {
        loop {
            if guard.completed_seq >= my_seq {
                return Ok(());
            }
            if let Some(ref msg) = guard.broken {
                return Err(Self::broken_error(msg));
            }
            if !guard.flushing {
                guard.flushing = true;
                let drained = std::mem::take(&mut guard.queue);
                let batch_seq = guard.enqueued_seq;
                drop(guard);

                let res = {
                    let mut io = self.lock_io();
                    let mut r = Ok(());
                    for item in &drained {
                        r = Self::write_frame(
                            &mut *io,
                            item.lsn,
                            item.txn_id,
                            item.collection_id,
                            item.kind,
                            item.record_id,
                            item.version,
                            &item.frame_bytes,
                        );
                        if r.is_err() {
                            break;
                        }
                    }
                    r.and_then(|()| self.finish_batch_io(&mut io))
                };
                // Reuse the drained queue's allocation for the next batch.
                let mut drained = drained;
                drained.clear();
                let res = self.complete_batch(batch_seq, res);
                guard = self.lock_state();
                if guard.queue.is_empty() && guard.queue.capacity() < drained.capacity() {
                    guard.queue = drained;
                }
                res?;
            } else {
                guard = self
                    .cv
                    .wait(guard)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }
    }

    /// Completes a batch: under `Immediate` / `GroupCommit` flushes to the OS and `fsync`s;
    /// under `AsyncMemoryFirst` leaves the frames buffered for [`Self::flush_buffered`].
    fn finish_batch_io(&self, io: &mut BufWriter<File>) -> Result<()> {
        if self.policy.syncs() {
            io.flush()
                .map_err(|e| StorageError::io("flush WAL buffer", e))?;
            io.get_ref()
                .sync_data()
                .map_err(|e| StorageError::io("fsync WAL file", e))?;
        }
        Ok(())
    }

    /// Forces every frame appended so far to stable storage (`write` + `fsync`), whatever the
    /// [`SyncPolicy`]. Used before discarding state that the WAL is the only durable copy of
    /// (e.g. Raft log compaction). An I/O error fail-stops the WAL.
    pub fn sync_to_disk(&self) -> Result<()> {
        let res = {
            let mut io = self.lock_io();
            io.flush()
                .map_err(|e| StorageError::io("flush WAL buffer", e))
                .and_then(|()| {
                    io.get_ref()
                        .sync_data()
                        .map_err(|e| StorageError::io("fsync WAL file", e))
                })
        };
        if let Err(ref e) = res {
            let mut guard = self.lock_state();
            guard.broken = Some(e.to_string());
            self.cv.notify_all();
        }
        res
    }

    /// Pushes frames buffered under `AsyncMemoryFirst` to the OS page cache (no `fsync`).
    /// Cheap no-op when nothing is buffered; a write error fail-stops the WAL.
    pub fn flush_buffered(&self) -> Result<()> {
        if self.policy.syncs() {
            return Ok(());
        }
        let res = {
            let mut io = self.lock_io();
            if io.buffer().is_empty() {
                return Ok(());
            }
            io.flush()
                .map_err(|e| StorageError::io("flush buffered WAL frames", e))
        };
        if let Err(ref e) = res {
            let mut guard = self.lock_state();
            guard.broken = Some(e.to_string());
            self.cv.notify_all();
        }
        res
    }

    /// Publishes the outcome of a batch covering all submissions `<= batch_seq`.
    fn complete_batch(&self, batch_seq: u64, res: Result<()>) -> Result<()> {
        let mut guard = self.lock_state();
        guard.flushing = false;
        match res {
            Ok(()) => {
                guard.completed_seq = guard.completed_seq.max(batch_seq);
                self.total_batches.fetch_add(1, Ordering::Relaxed);
                self.cv.notify_all();
                Ok(())
            }
            Err(e) => {
                guard.broken = Some(e.to_string());
                self.cv.notify_all();
                Err(e)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[inline]
    fn write_frame(
        writer: &mut impl Write,
        lsn: u64,
        txn_id: u64,
        collection_id: u16,
        kind: WalOpKind,
        record_id: InlineId,
        version: Version,
        frame_bytes: &[u8],
    ) -> Result<()> {
        if frame_bytes.len() >= WAL_LZ4_THRESHOLD {
            let compressed = compress_lz4(frame_bytes);
            if compressed.len() < frame_bytes.len() {
                let hdr_bytes = encode_wal_frame_header(
                    lsn,
                    txn_id,
                    collection_id,
                    kind,
                    WAL_FLAG_LZ4,
                    record_id,
                    version,
                    &compressed,
                    frame_bytes.len() as u32,
                );
                writer
                    .write_all(&hdr_bytes)
                    .map_err(|e| StorageError::io("write compressed WAL frame header", e))?;
                writer
                    .write_all(&compressed)
                    .map_err(|e| StorageError::io("write compressed WAL frame body", e))?;
                return Ok(());
            }
        }

        let hdr_bytes = encode_wal_frame_header(
            lsn,
            txn_id,
            collection_id,
            kind,
            0,
            record_id,
            version,
            frame_bytes,
            0,
        );
        writer
            .write_all(&hdr_bytes)
            .map_err(|e| StorageError::io("write WAL frame header", e))?;
        if !frame_bytes.is_empty() {
            writer
                .write_all(frame_bytes)
                .map_err(|e| StorageError::io("write WAL frame body", e))?;
        }
        Ok(())
    }

    /// Blocks new batch I/O, waits for any in-flight batch, and runs `f` with exclusive file access.
    /// `f` sets its `poison` flag when it fails in a way that leaves the active WAL handle unusable.
    fn with_exclusive_io<R>(
        &self,
        f: impl FnOnce(&mut BufWriter<File>, &mut bool) -> Result<R>,
    ) -> Result<R> {
        let mut guard = self.lock_state();
        while guard.flushing {
            guard = self
                .cv
                .wait(guard)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        if let Some(ref msg) = guard.broken {
            return Err(Self::broken_error(msg));
        }
        // Mark as flushing so writers queue up (and wait) instead of touching the file.
        guard.flushing = true;
        drop(guard);

        let mut poison = false;
        let res = {
            let mut io = self.lock_io();
            f(&mut io, &mut poison)
        };

        let mut guard = self.lock_state();
        guard.flushing = false;
        if let (Err(ref e), true) = (&res, poison) {
            guard.broken = Some(e.to_string());
        }
        self.cv.notify_all();
        res
    }

    /// Removes every frame with `lsn <= cutoff_lsn` from the active WAL, except frames of
    /// collections for which `keep_collection` returns `true` (and the commit barriers of any
    /// transaction that still has a kept operation).
    ///
    /// The surviving frames are written to `<wal>.next`, `fsync`ed and atomically renamed over
    /// the WAL, so a crash at any point leaves either the old or the new log intact. When
    /// `archive_dir` is set, the removed frames are first written to
    /// `archive_dir/wal_<first_lsn>.log` for Point-in-Time Recovery.
    ///
    /// Callers must guarantee that every committed operation with `lsn <= cutoff_lsn` has been
    /// applied and durably checkpointed into the data files.
    pub fn checkpoint_truncate_before(
        &self,
        cutoff_lsn: u64,
        keep_collection: &dyn Fn(u16) -> bool,
        archive_dir: Option<&Path>,
    ) -> Result<()> {
        self.with_exclusive_io(|io, poison| {
            io.flush()
                .map_err(|e| StorageError::io("flush WAL before checkpoint truncate", e))?;
            let raw = read_file_if_exists(&self.path)?;
            let (frames, _) = parse_frames(&raw);

            let mut kept_txns: HashSet<u64> = HashSet::new();
            let keep_op = |f: &RawFrame| {
                f.entry.kind != WalOpKind::Checkpoint
                    && f.entry.kind != WalOpKind::TxnCommit
                    && (f.entry.lsn > cutoff_lsn || keep_collection(f.entry.collection_id))
            };
            for f in &frames {
                if keep_op(f) && f.entry.txn_id != 0 {
                    kept_txns.insert(f.entry.txn_id);
                }
            }
            let mut kept = Vec::with_capacity(raw.len().min(1 << 20));
            let mut removed = Vec::new();
            let mut first_removed_lsn = None;
            for f in &frames {
                let keep = keep_op(f)
                    || (f.entry.kind == WalOpKind::TxnCommit
                        && (f.entry.lsn > cutoff_lsn || kept_txns.contains(&f.entry.txn_id)));
                if keep {
                    kept.extend_from_slice(&raw[f.start..f.end]);
                } else if f.entry.kind != WalOpKind::Checkpoint {
                    first_removed_lsn.get_or_insert(f.entry.lsn);
                    removed.extend_from_slice(&raw[f.start..f.end]);
                }
            }

            if let (Some(dir), Some(first_lsn)) = (archive_dir, first_removed_lsn) {
                fs::create_dir_all(dir)
                    .map_err(|e| StorageError::io("create WAL archive directory", e))?;
                let seg_path = dir.join(format!("wal_{first_lsn:020}.log"));
                let mut seg = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&seg_path)
                    .map_err(|e| StorageError::io("create archived WAL segment", e))?;
                seg.write_all(&removed)
                    .map_err(|e| StorageError::io("write archived WAL segment", e))?;
                seg.sync_all()
                    .map_err(|e| StorageError::io("fsync archived WAL segment", e))?;
                sync_parent_dir(&seg_path)?;
            }

            // Head marker preserving the LSN / txn-id high-water marks across restarts.
            let mut new_contents = Vec::with_capacity(WAL_FRAME_HEADER_LEN + kept.len());
            let next_txn = self.next_txn_id.load(Ordering::Acquire);
            Self::write_frame(
                &mut new_contents,
                self.current_lsn.load(Ordering::Acquire),
                0,
                0,
                WalOpKind::Checkpoint,
                CHECKPOINT_ID,
                Version(next_txn.saturating_sub(1)),
                &[],
            )?;
            new_contents.extend_from_slice(&kept);

            let mut next_path = self.path.as_os_str().to_os_string();
            next_path.push(".next");
            let next_path = PathBuf::from(next_path);
            let mut next = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(&next_path)
                .map_err(|e| StorageError::io("create replacement WAL file", e))?;
            next.write_all(&new_contents)
                .map_err(|e| StorageError::io("write replacement WAL file", e))?;
            next.sync_all()
                .map_err(|e| StorageError::io("fsync replacement WAL file", e))?;

            // Close the old handle before replacing the file (required on Windows).
            let placeholder = BufWriter::with_capacity(
                256 * 1024,
                next.try_clone()
                    .map_err(|e| StorageError::io("clone replacement WAL handle", e))?,
            );
            let old = std::mem::replace(io, placeholder);
            drop(old.into_parts().0);
            if let Err(e) = fs::rename(&next_path, &self.path) {
                // The old handle is closed and the new file is not in place: stop accepting writes.
                *poison = true;
                return Err(StorageError::io("atomically replace WAL file", e));
            }
            if let Err(e) = sync_parent_dir(&self.path) {
                *poison = true;
                return Err(e);
            }
            io.get_mut()
                .seek(SeekFrom::End(0))
                .map_err(|e| StorageError::io("seek replacement WAL end", e))?;
            Ok(())
        })
    }

    /// Removes every frame up to the current LSN (all operations are known to be checkpointed).
    pub fn checkpoint_truncate(&self) -> Result<()> {
        self.checkpoint_truncate_before(self.current_lsn(), &|_| false, None)
    }

    /// Reads all committed operations from `path` without modifying the file (`F-DURA-04`).
    ///
    /// Transactions are returned only when their commit barrier is present; a torn tail is
    /// ignored (it is truncated when the WAL is next opened for writing).
    pub fn recover_committed(path: &Path) -> Result<Vec<WalEntry>> {
        let raw = read_file_if_exists(path)?;
        let (frames, _) = parse_frames(&raw);
        Ok(committed_from_frames(frames))
    }

    /// Returns the LSN of the most recently appended frame.
    #[inline]
    pub fn current_lsn(&self) -> u64 {
        self.current_lsn.load(Ordering::Acquire)
    }

    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[inline]
    pub fn policy(&self) -> SyncPolicy {
        self.policy
    }

    /// Copies the current contents of the active WAL into `dest` (without truncating it).
    pub fn copy_active_to(&self, dest: &Path) -> Result<()> {
        self.with_exclusive_io(|io, _poison| {
            io.flush()
                .map_err(|e| StorageError::io("flush WAL before copy", e))?;
            fs::copy(&self.path, dest).map_err(|e| StorageError::io("copy active WAL", e))?;
            Ok(())
        })
    }

    /// Lists all archived WAL segment files (`wal_XXXX.log`) in `wal_dir` in LSN order.
    pub fn list_archived_segments(wal_dir: &Path) -> Result<Vec<PathBuf>> {
        if !wal_dir.exists() {
            return Ok(Vec::new());
        }
        let mut segments = Vec::new();
        for entry in fs::read_dir(wal_dir).map_err(|e| StorageError::io("read wal_dir", e))? {
            let entry = entry.map_err(|e| StorageError::io("read wal_dir entry", e))?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("wal_") && name.ends_with(".log") {
                    segments.push(path);
                }
            }
        }
        // Segment names embed zero-padded LSNs, so lexical order is LSN order.
        segments.sort();
        Ok(segments)
    }

    /// Recovers all committed WAL entries across all archived segments in `wal_dir` followed by
    /// `active_wal_path`, in LSN order (`TASK-040`, `F-DURA-05`).
    pub fn recover_all_segments(wal_dir: &Path, active_wal_path: &Path) -> Result<Vec<WalEntry>> {
        let mut all = Vec::new();
        for seg in Self::list_archived_segments(wal_dir)? {
            all.extend(Self::recover_committed(&seg)?);
        }
        all.extend(Self::recover_committed(active_wal_path)?);
        all.sort_by_key(|e| e.lsn);
        Ok(all)
    }

    /// Prunes archived segments in `wal_dir` whose maximum LSN is strictly less than
    /// `min_keep_lsn`, bounding archive disk usage (`TASK-040`, `RULE-MEM-04`).
    pub fn prune_archived_segments_before_lsn(wal_dir: &Path, min_keep_lsn: u64) -> Result<usize> {
        let mut pruned = 0usize;
        for seg in Self::list_archived_segments(wal_dir)? {
            let raw = read_file_if_exists(&seg)?;
            let (frames, _) = parse_frames(&raw);
            let max_lsn = frames.iter().map(|f| f.entry.lsn).max().unwrap_or(0);
            if max_lsn < min_keep_lsn && fs::remove_file(&seg).is_ok() {
                pruned += 1;
            }
        }
        Ok(pruned)
    }

    #[inline]
    pub fn stats(&self) -> (u64, u64) {
        (
            self.total_batches.load(Ordering::Relaxed),
            self.total_frames.load(Ordering::Relaxed),
        )
    }
}

impl Drop for GroupCommitWal {
    fn drop(&mut self) {
        let mut io = self.io.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let _ = io.flush();
        if self.policy.syncs() {
            let _ = io.get_ref().sync_data();
        }
    }
}

#[allow(clippy::too_many_arguments)]
#[inline]
fn compute_wal_checksum(
    lsn: u64,
    txn_id: u64,
    collection_id: u16,
    kind: WalOpKind,
    flags: u8,
    record_id: InlineId,
    version: Version,
    body: &[u8],
    uncompressed_len: u32,
) -> u64 {
    let mut meta = [0u8; 64];
    meta[0..4].copy_from_slice(&WAL_MAGIC.to_le_bytes());
    meta[4..6].copy_from_slice(&collection_id.to_le_bytes());
    meta[6] = kind as u8;
    meta[7] = flags;
    meta[8..16].copy_from_slice(&lsn.to_le_bytes());
    meta[16..24].copy_from_slice(&txn_id.to_le_bytes());
    meta[24..32].copy_from_slice(&version.0.to_le_bytes());
    meta[32..56].copy_from_slice(&record_id.to_raw_bytes());
    meta[56..60].copy_from_slice(&(body.len() as u32).to_le_bytes());
    meta[60..64].copy_from_slice(&uncompressed_len.to_le_bytes());
    let h1 = checksum_payload(&meta);
    let h2 = checksum_payload(body);
    h1 ^ h2.rotate_left(29)
}

#[allow(clippy::too_many_arguments)]
#[inline]
fn encode_wal_frame_header(
    lsn: u64,
    txn_id: u64,
    collection_id: u16,
    kind: WalOpKind,
    flags: u8,
    record_id: InlineId,
    version: Version,
    body: &[u8],
    uncompressed_len: u32,
) -> [u8; WAL_FRAME_HEADER_LEN] {
    let sum = compute_wal_checksum(
        lsn,
        txn_id,
        collection_id,
        kind,
        flags,
        record_id,
        version,
        body,
        uncompressed_len,
    );
    let mut out = [0u8; WAL_FRAME_HEADER_LEN];
    out[0..4].copy_from_slice(&WAL_MAGIC.to_le_bytes());
    out[4..6].copy_from_slice(&collection_id.to_le_bytes());
    out[6] = kind as u8;
    out[7] = flags;
    out[8..16].copy_from_slice(&lsn.to_le_bytes());
    out[16..24].copy_from_slice(&txn_id.to_le_bytes());
    out[24..32].copy_from_slice(&version.0.to_le_bytes());
    out[32..56].copy_from_slice(&record_id.to_raw_bytes());
    out[56..60].copy_from_slice(&(body.len() as u32).to_le_bytes());
    out[60..64].copy_from_slice(&uncompressed_len.to_le_bytes());
    out[64..72].copy_from_slice(&sum.to_le_bytes());
    out
}

struct DecodedHeader {
    lsn: u64,
    txn_id: u64,
    collection_id: u16,
    kind: WalOpKind,
    flags: u8,
    record_id: InlineId,
    version: Version,
    body_len: usize,
    uncompressed_len: usize,
    checksum: u64,
}

#[inline]
fn decode_wal_frame_header(hdr: &[u8]) -> Option<DecodedHeader> {
    use storage_types::wire::{read_array_24, read_u16_le, read_u32_le, read_u64_le};
    if hdr.len() < WAL_FRAME_HEADER_LEN {
        return None;
    }
    if read_u32_le(hdr, 0) != WAL_MAGIC {
        return None;
    }
    Some(DecodedHeader {
        collection_id: read_u16_le(hdr, 4),
        kind: WalOpKind::from_u8(hdr[6])?,
        flags: hdr[7],
        lsn: read_u64_le(hdr, 8),
        txn_id: read_u64_le(hdr, 16),
        version: Version(read_u64_le(hdr, 24)),
        record_id: InlineId::from_raw_bytes(read_array_24(hdr, 32)).ok()?,
        body_len: read_u32_le(hdr, 56) as usize,
        uncompressed_len: read_u32_le(hdr, 60) as usize,
        checksum: read_u64_le(hdr, 64),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn test_hostile_wal_files_and_frames_never_panic() {
        use storage_types::testing::SweepRng;
        let dir = temp_dir("storage_wal_hostile");
        let wal_path = dir.join("engine.wal");
        {
            let wal = GroupCommitWal::open(&wal_path, SyncPolicy::Immediate).unwrap();
            let id = InlineId::new("rec").unwrap();
            wal.append_single(1, WalOpKind::Upsert, id, Version(1), &[0xAA; 64]).unwrap();
            wal.append_single(1, WalOpKind::Upsert, id, Version(2), &vec![0x5A; 4096]).unwrap();
            let txn = wal.allocate_txn_id();
            wal.append_transaction(
                txn,
                vec![
                    (1, WalOpKind::Upsert, id, Version(3), vec![1; 32]),
                    (1, WalOpKind::Delete, id, Version(4), Vec::new()),
                ],
            )
            .unwrap();
        }
        let good = fs::read(&wal_path).unwrap();
        let mut rng = SweepRng::new(11);

        // 1. On-disk corruption: recovery and reopen must fail cleanly or truncate, never panic.
        let probe = dir.join("probe.wal");
        for round in 0..150 {
            let bytes = if round < 30 {
                good[..(good.len() * round / 30)].to_vec()
            } else {
                rng.hostile_copy(&good)
            };
            fs::write(&probe, &bytes).unwrap();
            let _ = GroupCommitWal::recover_committed(&probe);
            if let Ok(wal) = GroupCommitWal::open(&probe, SyncPolicy::Immediate) {
                let _ = wal.append_single(1, WalOpKind::Upsert, InlineId::new("x").unwrap(), Version(1), &[2; 8]);
            }
        }

        // 2. Forged frames with valid checksums and hostile fields (flags, lengths, kinds).
        let kinds = [WalOpKind::Upsert, WalOpKind::Delete, WalOpKind::TxnCommit, WalOpKind::Checkpoint];
        for _ in 0..5_000 {
            let mut buf = Vec::new();
            for _ in 0..=rng.below(3) {
                let body_len = rng.below(300);
                let mut body: Vec<u8> = (0..body_len).map(|_| rng.next_u64() as u8).collect();
                if rng.below(2) == 0 {
                    body = compress_lz4(&body);
                }
                let flags = rng.below(4) as u8;
                let uncompressed = rng.edge_u32(body_len as u32);
                let hdr = encode_wal_frame_header(
                    rng.edge_u64(5),
                    rng.edge_u64(0),
                    rng.below(3) as u16,
                    kinds[rng.below(kinds.len())],
                    flags,
                    InlineId::new("f").unwrap(),
                    Version(rng.edge_u64(1)),
                    &body,
                    uncompressed,
                );
                buf.extend_from_slice(&hdr);
                buf.extend_from_slice(&body);
            }
            let (frames, _) = parse_frames(&buf);
            let _ = committed_from_frames(frames);
        }
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_wal_group_commit_lz4_compression_and_torn_frame_recovery() {
        let dir = temp_dir("storage_wal_test_1");
        let wal_path = dir.join("engine.wal");

        let large_payload = vec![0x5A_u8; 8192]; // 8 KiB compressible frame
        {
            let wal = GroupCommitWal::open(&wal_path, SyncPolicy::Immediate).unwrap();
            let id1 = InlineId::new("rec_1").unwrap();
            let id2 = InlineId::new("rec_2").unwrap();
            let id3 = InlineId::new("rec_large").unwrap();
            wal.append_single(1, WalOpKind::Upsert, id1, Version(1), &[0xAA; 64])
                .unwrap();
            wal.append_single(1, WalOpKind::Upsert, id3, Version(1), &large_payload)
                .unwrap();

            let txn_id = wal.allocate_txn_id();
            wal.append_transaction(
                txn_id,
                vec![
                    (1, WalOpKind::Upsert, id1, Version(2), vec![0xBB; 64]),
                    (1, WalOpKind::Upsert, id2, Version(1), vec![0xCC; 64]),
                ],
            )
            .unwrap();
        }

        let wal_disk_bytes = fs::metadata(&wal_path).unwrap().len();
        assert!(
            wal_disk_bytes < 2048,
            "expected LZ4-compressed WAL (<2KB), got {wal_disk_bytes} bytes"
        );

        // Append simulated torn tail bytes (uncommitted crash mid-write)
        {
            let mut f = OpenOptions::new().append(true).open(&wal_path).unwrap();
            f.write_all(&[0x57, 0x41, 0x4C, 0x31, 0x00, 0x99]).unwrap();
        }

        let entries = GroupCommitWal::recover_committed(&wal_path).unwrap();
        assert_eq!(entries.len(), 4);
        assert_eq!(entries[0].record_id.as_str(), "rec_1");
        assert_eq!(entries[0].version, Version(1));
        assert_eq!(entries[1].record_id.as_str(), "rec_large");
        assert_eq!(entries[1].frame_bytes, large_payload);
        assert_eq!(entries[2].version, Version(2));
        assert_eq!(entries[3].record_id.as_str(), "rec_2");

        // Reopening truncates the torn tail and resumes LSN / txn counters monotonically.
        let reopened = GroupCommitWal::open(&wal_path, SyncPolicy::Immediate).unwrap();
        assert_eq!(reopened.current_lsn(), 5);
        assert!(reopened.allocate_txn_id() >= 2);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_incomplete_txn_never_merges_with_new_txn_after_restart() {
        let dir = temp_dir("storage_wal_txn_restart");
        let wal_path = dir.join("engine.wal");
        let id = InlineId::new("k").unwrap();
        {
            let wal = GroupCommitWal::open(&wal_path, SyncPolicy::Immediate).unwrap();
            let t = wal.allocate_txn_id();
            wal.append_transaction(t, vec![(1, WalOpKind::Upsert, id, Version(1), vec![1; 8])])
                .unwrap();
        }
        // Chop off the commit barrier: the transaction's single op remains, uncommitted.
        let len = fs::metadata(&wal_path).unwrap().len();
        let f = OpenOptions::new().write(true).open(&wal_path).unwrap();
        f.set_len(len - WAL_FRAME_HEADER_LEN as u64).unwrap();
        drop(f);
        {
            let wal = GroupCommitWal::open(&wal_path, SyncPolicy::Immediate).unwrap();
            let t = wal.allocate_txn_id();
            wal.append_transaction(
                t,
                vec![
                    (1, WalOpKind::Upsert, id, Version(2), vec![2; 8]),
                    (1, WalOpKind::Upsert, id, Version(3), vec![3; 8]),
                ],
            )
            .unwrap();
        }
        let entries = GroupCommitWal::recover_committed(&wal_path).unwrap();
        assert_eq!(entries.len(), 2, "the new committed txn must survive intact");
        assert_eq!(entries[1].version, Version(3));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_checkpoint_truncate_keeps_later_frames_and_archives_removed() {
        let dir = temp_dir("storage_wal_ckpt");
        let wal_path = dir.join("engine.wal");
        let archive = dir.join("archive");
        let id = InlineId::new("k").unwrap();
        let wal = GroupCommitWal::open(&wal_path, SyncPolicy::Immediate).unwrap();
        for v in 1..=3 {
            wal.append_single(1, WalOpKind::Upsert, id, Version(v), &[v as u8; 16])
                .unwrap();
        }
        let cutoff = wal.current_lsn();
        wal.append_single(1, WalOpKind::Upsert, id, Version(4), &[4; 16])
            .unwrap();
        wal.checkpoint_truncate_before(cutoff, &|_| false, Some(&archive))
            .unwrap();
        wal.append_single(1, WalOpKind::Upsert, id, Version(5), &[5; 16])
            .unwrap();

        let live = GroupCommitWal::recover_committed(&wal_path).unwrap();
        assert_eq!(
            live.iter().map(|e| e.version.0).collect::<Vec<_>>(),
            vec![4, 5]
        );
        let archived = GroupCommitWal::list_archived_segments(&archive).unwrap();
        assert_eq!(archived.len(), 1);
        let all = GroupCommitWal::recover_all_segments(&archive, &wal_path).unwrap();
        assert_eq!(
            all.iter().map(|e| e.version.0).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        drop(wal);
        let reopened = GroupCommitWal::open(&wal_path, SyncPolicy::Immediate).unwrap();
        assert_eq!(reopened.current_lsn(), 5, "LSNs stay monotonic after truncation");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_async_memory_first_buffers_until_flushed() {
        let dir = temp_dir("storage_wal_async");
        let wal_path = dir.join("engine.wal");
        let wal = GroupCommitWal::open(&wal_path, SyncPolicy::AsyncMemoryFirst).unwrap();
        let id = InlineId::new("k").unwrap();
        wal.append_single(1, WalOpKind::Upsert, id, Version(1), &[1; 32]).unwrap();
        assert!(GroupCommitWal::recover_committed(&wal_path).unwrap().is_empty());
        wal.flush_buffered().unwrap();
        assert_eq!(GroupCommitWal::recover_committed(&wal_path).unwrap().len(), 1);
        // Deletes are pushed to the OS immediately.
        wal.append_single(1, WalOpKind::Delete, id, Version(2), &[]).unwrap();
        assert_eq!(GroupCommitWal::recover_committed(&wal_path).unwrap().len(), 2);
        drop(wal);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_concurrent_group_commit_every_ack_is_durable() {
        let dir = temp_dir("storage_wal_concurrent");
        let wal_path = dir.join("engine.wal");
        let wal = Arc::new(GroupCommitWal::open(&wal_path, SyncPolicy::GroupCommit {
            max_batch_entries: 64,
        })
        .unwrap());
        let mut handles = Vec::new();
        for t in 0..8u64 {
            let w = Arc::clone(&wal);
            handles.push(std::thread::spawn(move || {
                let id = InlineId::new(&format!("t{t}")).unwrap();
                for v in 0..50u64 {
                    w.append_single(1, WalOpKind::Upsert, id, Version(v), &[t as u8; 32])
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let entries = GroupCommitWal::recover_committed(&wal_path).unwrap();
        assert_eq!(entries.len(), 400);
        assert!(
            entries.windows(2).all(|w| w[0].lsn < w[1].lsn),
            "file order must equal LSN order"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
