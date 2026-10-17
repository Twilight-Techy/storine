//! Group-Commit Write-Ahead Log (WAL) with batched `fsync`, transactional commit barriers,
//! and deterministic crash-recovery replay (`TASK-014`, `F-DURA-01`, `F-DURA-02`, `F-DURA-04`, `RULE-SAFE-01`).

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations, rust_2018_idioms)]

use std::collections::HashMap;
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

/// Durability policy controlling when WAL appends call `sync_data()` (`F-DURA-02`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncPolicy {
    /// Every write or group-commit batch flushes and `fsync`s to stable storage before returning.
    Immediate,
    /// Coalesces concurrent thread writes into a single sequential write + `fsync` batch.
    GroupCommit { max_batch_entries: usize },
    /// Appends to the OS page cache immediately and flushes asynchronously in the background.
    AsyncMemoryFirst,
}

impl Default for SyncPolicy {
    fn default() -> Self {
        Self::GroupCommit {
            max_batch_entries: 64,
        }
    }
}

/// Type of operation recorded in a WAL frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum WalOpKind {
    Upsert = 1,
    Delete = 2,
    TxnCommit = 3,
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
    /// Full 64-byte-aligned record frame (`[RecordHeader | Payload]`) for `Upsert`, or empty for `Delete`.
    pub frame_bytes: Vec<u8>,
}

struct PendingBatchItem {
    collection_id: u16,
    txn_id: u64,
    kind: WalOpKind,
    record_id: InlineId,
    version: Version,
    frame_bytes: Vec<u8>,
    assigned_lsn: u64,
}

struct GroupCommitState {
    writer: BufWriter<File>,
    queue: Vec<PendingBatchItem>,
    flushed_lsn: u64,
    flushing: bool,
}

/// High-throughput Group-Commit Write-Ahead Log (`RULE-CONC-03`).
///
/// Multiple concurrent threads enqueue their frames into the group-commit buffer;
/// a single leader thread drains the entire batch in one sequential write and executes
/// a single `sync_data()` (`fsync`) syscall on behalf of all waiting writers.
pub struct GroupCommitWal {
    path: PathBuf,
    policy: SyncPolicy,
    next_lsn: CachePadded<AtomicU64>,
    next_txn_id: CachePadded<AtomicU64>,
    state: Mutex<GroupCommitState>,
    cv: Condvar,
    total_batches: CachePadded<AtomicU64>,
    total_frames: CachePadded<AtomicU64>,
}

impl core::fmt::Debug for GroupCommitWal {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("GroupCommitWal")
            .field("path", &self.path)
            .field("policy", &self.policy)
            .field("next_lsn", &self.next_lsn.load(Ordering::Relaxed))
            .finish()
    }
}

impl GroupCommitWal {
    pub fn open(path: impl Into<PathBuf>, policy: SyncPolicy) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)
                    .map_err(|e| StorageError::io("create WAL directory", e))?;
            }
        }

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| StorageError::io("open WAL file", e))?;

        file.seek(SeekFrom::End(0))
            .map_err(|e| StorageError::io("seek WAL end", e))?;

        Ok(Self {
            path,
            policy,
            next_lsn: CachePadded::new(AtomicU64::new(1)),
            next_txn_id: CachePadded::new(AtomicU64::new(1)),
            state: Mutex::new(GroupCommitState {
                writer: BufWriter::with_capacity(256 * 1024, file),
                queue: Vec::with_capacity(64),
                flushed_lsn: 0,
                flushing: false,
            }),
            cv: Condvar::new(),
            total_batches: CachePadded::new(AtomicU64::new(0)),
            total_frames: CachePadded::new(AtomicU64::new(0)),
        })
    }

    #[inline]
    pub fn allocate_txn_id(&self) -> u64 {
        self.next_txn_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Appends a standalone (`txn_id == 0`) upsert or delete operation to the WAL.
    pub fn append_single(
        &self,
        collection_id: u16,
        kind: WalOpKind,
        record_id: InlineId,
        version: Version,
        frame_bytes: &[u8],
    ) -> Result<u64> {
        let lsn = self.next_lsn.fetch_add(1, Ordering::Relaxed);
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !guard.flushing && guard.queue.is_empty() {
            // Zero-allocation uncontended leader fast path (with transparent LZ4 compression for >= 1KB frames)!
            self.total_frames.fetch_add(1, Ordering::Relaxed);
            Self::write_single_frame_locked(
                &mut guard.writer,
                lsn,
                0,
                collection_id,
                kind,
                record_id,
                version,
                frame_bytes,
            )?;
            let sync_disk = matches!(
                self.policy,
                SyncPolicy::Immediate | SyncPolicy::GroupCommit { .. }
            );
            if sync_disk {
                guard
                    .writer
                    .flush()
                    .map_err(|e| StorageError::io("flush WAL buffer", e))?;
                guard
                    .writer
                    .get_ref()
                    .sync_data()
                    .map_err(|e| StorageError::io("fsync WAL file", e))?;
            }
            guard.flushed_lsn = guard.flushed_lsn.max(lsn);
            self.total_batches.fetch_add(1, Ordering::Relaxed);
            return Ok(lsn);
        }
        drop(guard);

        let item = PendingBatchItem {
            collection_id,
            txn_id: 0,
            kind,
            record_id,
            version,
            frame_bytes: frame_bytes.to_vec(),
            assigned_lsn: lsn,
        };
        self.submit_and_wait(vec![item], lsn)?;
        Ok(lsn)
    }

    /// Atomically appends a multi-record transaction (`items` followed by a `TxnCommit` barrier)
    /// in a single group-commit batch (`TASK-024`).
    pub fn append_transaction(
        &self,
        txn_id: u64,
        ops: Vec<(u16, WalOpKind, InlineId, Version, Vec<u8>)>,
    ) -> Result<u64> {
        if ops.is_empty() {
            return Ok(0);
        }
        let count = ops.len() as u64;
        let mut batch = Vec::with_capacity(ops.len() + 1);
        for (collection_id, kind, record_id, version, frame_bytes) in ops {
            let lsn = self.next_lsn.fetch_add(1, Ordering::Relaxed);
            batch.push(PendingBatchItem {
                collection_id,
                txn_id,
                kind,
                record_id,
                version,
                frame_bytes,
                assigned_lsn: lsn,
            });
        }
        let commit_lsn = self.next_lsn.fetch_add(1, Ordering::Relaxed);
        batch.push(PendingBatchItem {
            collection_id: 0,
            txn_id,
            kind: WalOpKind::TxnCommit,
            record_id: InlineId::from_static("txn_commit"),
            version: Version(count),
            frame_bytes: Vec::new(),
            assigned_lsn: commit_lsn,
        });

        self.submit_and_wait(batch, commit_lsn)?;
        Ok(commit_lsn)
    }

    fn submit_and_wait(&self, items: Vec<PendingBatchItem>, target_lsn: u64) -> Result<()> {
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let item_count = items.len() as u64;
        guard.queue.extend(items);
        self.total_frames.fetch_add(item_count, Ordering::Relaxed);

        loop {
            if guard.flushed_lsn >= target_lsn {
                return Ok(());
            }
            if !guard.flushing {
                // Become the Group-Commit Leader for all currently queued writers
                guard.flushing = true;
                let drained: Vec<PendingBatchItem> = guard.queue.drain(..).collect();
                let max_lsn = drained
                    .iter()
                    .map(|it| it.assigned_lsn)
                    .max()
                    .unwrap_or(target_lsn);

                let sync_disk = matches!(
                    self.policy,
                    SyncPolicy::Immediate | SyncPolicy::GroupCommit { .. }
                );
                let write_res = Self::write_batch_locked(&mut guard.writer, &drained, sync_disk);
                guard.flushing = false;
                match write_res {
                    Ok(()) => {
                        guard.flushed_lsn = guard.flushed_lsn.max(max_lsn);
                        self.total_batches.fetch_add(1, Ordering::Relaxed);
                        self.cv.notify_all();
                    }
                    Err(e) => {
                        self.cv.notify_all();
                        return Err(e);
                    }
                }
            } else {
                guard = self
                    .cv
                    .wait(guard)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }
    }

    #[inline]
    fn write_single_frame_locked(
        writer: &mut BufWriter<File>,
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

    fn write_batch_locked(
        writer: &mut BufWriter<File>,
        items: &[PendingBatchItem],
        sync_disk: bool,
    ) -> Result<()> {
        for item in items {
            Self::write_single_frame_locked(
                writer,
                item.assigned_lsn,
                item.txn_id,
                item.collection_id,
                item.kind,
                item.record_id,
                item.version,
                &item.frame_bytes,
            )?;
        }
        writer
            .flush()
            .map_err(|e| StorageError::io("flush WAL buffer", e))?;
        if sync_disk {
            writer
                .get_ref()
                .sync_data()
                .map_err(|e| StorageError::io("fsync WAL file", e))?;
        }
        Ok(())
    }

    /// Truncates the WAL after all dirty records have been safely flushed and checkpointed to disk.
    pub fn checkpoint_truncate(&self) -> Result<()> {
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while guard.flushing {
            guard = self
                .cv
                .wait(guard)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        guard
            .writer
            .flush()
            .map_err(|e| StorageError::io("flush WAL before truncate", e))?;
        let file = guard.writer.get_mut();
        file.set_len(0)
            .map_err(|e| StorageError::io("truncate WAL file", e))?;
        file.seek(SeekFrom::Start(0))
            .map_err(|e| StorageError::io("seek WAL start", e))?;
        file.sync_data()
            .map_err(|e| StorageError::io("fsync truncated WAL", e))?;
        Ok(())
    }

    /// Recovers all committed WAL operations from `path` (`F-DURA-04`).
    pub fn recover_committed(path: &Path) -> Result<Vec<WalEntry>> {
        if !path.exists() {
            return Ok(Vec::new());
        }
        let mut file =
            File::open(path).map_err(|e| StorageError::io("open WAL for recovery", e))?;
        let mut raw = Vec::new();
        file.read_to_end(&mut raw)
            .map_err(|e| StorageError::io("read WAL for recovery", e))?;

        let mut committed = Vec::new();
        let mut pending_txns: HashMap<u64, Vec<WalEntry>> = HashMap::new();
        let mut pos = 0usize;
        let mut last_valid_pos = 0usize;

        while pos + WAL_FRAME_HEADER_LEN <= raw.len() {
            let hdr_slice = &raw[pos..pos + WAL_FRAME_HEADER_LEN];
            let Some((
                lsn,
                txn_id,
                collection_id,
                kind,
                flags,
                record_id,
                version,
                body_len,
                uncompressed_len,
                expected_sum,
            )) = decode_wal_frame_header(hdr_slice)
            else {
                break;
            };

            if body_len > MAX_PAYLOAD_SIZE as usize {
                break;
            }
            let frame_end = match (pos + WAL_FRAME_HEADER_LEN).checked_add(body_len) {
                Some(end) if end <= raw.len() => end,
                _ => break,
            };

            let body = &raw[pos + WAL_FRAME_HEADER_LEN..frame_end];
            let actual_sum = compute_wal_checksum(
                lsn,
                txn_id,
                collection_id,
                kind,
                flags,
                record_id,
                version,
                body,
                uncompressed_len as u32,
            );
            if actual_sum != expected_sum {
                break;
            }

            let decoded_frame = if (flags & WAL_FLAG_LZ4) != 0 {
                if uncompressed_len > MAX_PAYLOAD_SIZE as usize {
                    break;
                }
                let Ok(decompressed) = decompress_lz4(body, uncompressed_len) else {
                    break;
                };
                decompressed
            } else {
                body.to_vec()
            };

            let entry = WalEntry {
                lsn,
                txn_id,
                collection_id,
                kind,
                record_id,
                version,
                frame_bytes: decoded_frame,
            };

            if txn_id == 0 {
                committed.push(entry);
            } else if kind == WalOpKind::TxnCommit {
                if let Some(ops) = pending_txns.remove(&txn_id) {
                    if ops.len() as u64 == version.0 {
                        committed.extend(ops);
                    }
                }
            } else {
                pending_txns.entry(txn_id).or_default().push(entry);
            }

            pos = frame_end;
            last_valid_pos = pos;
        }

        if last_valid_pos < raw.len() {
            if let Ok(f) = OpenOptions::new().write(true).open(path) {
                let _ = f.set_len(last_valid_pos as u64);
            }
        }

        Ok(committed)
    }

    #[inline]
    pub fn current_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::Acquire).saturating_sub(1)
    }

    #[inline]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Rotates the active WAL into an immutable numbered segment file (`wal_XXXXXX.log`)
    /// for Point-in-Time Recovery (PITR) archival and resets the active WAL buffer (`TASK-040`, `F-DURA-05`).
    pub fn rotate_segment(&self) -> Result<PathBuf> {
        let mut guard = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while guard.flushing {
            guard = self
                .cv
                .wait(guard)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        guard
            .writer
            .flush()
            .map_err(|e| StorageError::io("flush WAL before rotate", e))?;
        guard
            .writer
            .get_ref()
            .sync_data()
            .map_err(|e| StorageError::io("fsync WAL before rotate", e))?;

        let parent = self
            .path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let existing_segs = Self::list_archived_segments(&parent)?;
        let next_seq = existing_segs.len() + 1;
        let seg_path = parent.join(format!("wal_{:06}.log", next_seq));

        fs::copy(&self.path, &seg_path)
            .map_err(|e| StorageError::io("archive rotated WAL segment", e))?;

        let file = guard.writer.get_mut();
        file.set_len(0)
            .map_err(|e| StorageError::io("truncate rotated WAL file", e))?;
        file.seek(SeekFrom::Start(0))
            .map_err(|e| StorageError::io("seek rotated WAL start", e))?;
        file.sync_data()
            .map_err(|e| StorageError::io("fsync rotated WAL", e))?;

        Ok(seg_path)
    }

    /// Lists all archived WAL segment files (`wal_XXXXXX.log`) in `wal_dir` sorted chronologically.
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
        segments.sort();
        Ok(segments)
    }

    /// Recovers all committed WAL entries across all archived `wal_XXXXXX.log` segments in `wal_dir`
    /// followed by `active_wal_path` in monotonically increasing LSN order (`TASK-040`, `F-DURA-05`).
    pub fn recover_all_segments(wal_dir: &Path, active_wal_path: &Path) -> Result<Vec<WalEntry>> {
        let mut all = Vec::new();
        for seg in Self::list_archived_segments(wal_dir)? {
            all.extend(Self::recover_committed(&seg)?);
        }
        if active_wal_path.exists() {
            all.extend(Self::recover_committed(active_wal_path)?);
        }
        Ok(all)
    }

    /// Prunes archived `wal_XXXXXX.log` segments in `wal_dir` whose maximum LSN is strictly less
    /// than `min_keep_lsn`, preventing unbounded WAL disk consumption (`TASK-040`, `RULE-MEM-04`).
    pub fn prune_archived_segments_before_lsn(
        wal_dir: &Path,
        min_keep_lsn: u64,
    ) -> Result<usize> {
        let mut pruned = 0usize;
        for seg in Self::list_archived_segments(wal_dir)? {
            let entries = Self::recover_committed(&seg)?;
            let max_lsn = entries.iter().map(|e| e.lsn).max().unwrap_or(0);
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
        if let Ok(mut guard) = self.state.lock() {
            let _ = guard.writer.flush();
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

#[allow(clippy::type_complexity)]
#[inline]
fn decode_wal_frame_header(
    hdr: &[u8],
) -> Option<(u64, u64, u16, WalOpKind, u8, InlineId, Version, usize, usize, u64)> {
    if hdr.len() < WAL_FRAME_HEADER_LEN {
        return None;
    }
    let magic = u32::from_le_bytes(hdr[0..4].try_into().ok()?);
    if magic != WAL_MAGIC {
        return None;
    }
    let collection_id = u16::from_le_bytes(hdr[4..6].try_into().ok()?);
    let kind = WalOpKind::from_u8(hdr[6])?;
    let flags = hdr[7];
    let lsn = u64::from_le_bytes(hdr[8..16].try_into().ok()?);
    let txn_id = u64::from_le_bytes(hdr[16..24].try_into().ok()?);
    let version = Version(u64::from_le_bytes(hdr[24..32].try_into().ok()?));
    let raw_id: [u8; 24] = hdr[32..56].try_into().ok()?;
    let record_id = InlineId::from_raw_bytes(raw_id).ok()?;
    let body_len = u32::from_le_bytes(hdr[56..60].try_into().ok()?) as usize;
    let uncompressed_len = u32::from_le_bytes(hdr[60..64].try_into().ok()?) as usize;
    let checksum = u64::from_le_bytes(hdr[64..72].try_into().ok()?);
    Some((
        lsn,
        txn_id,
        collection_id,
        kind,
        flags,
        record_id,
        version,
        body_len,
        uncompressed_len,
        checksum,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_group_commit_lz4_compression_and_torn_frame_recovery() {
        let dir = std::env::temp_dir().join("storage_wal_test_1");
        let _ = fs::remove_dir_all(&dir);
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

        // Verify the WAL file on disk is smaller than the raw 8 KiB payload thanks to LZ4 compression!
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

        let _ = fs::remove_dir_all(&dir);
    }
}
