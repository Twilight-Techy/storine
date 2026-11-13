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
use std::fs;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use storage_codec::{checksum_payload, compress_lz4, decompress_lz4};
use storage_types::vfs::{OpenOpts, VfsHandle, VfsRef};
use storage_types::{CachePadded, InlineId, Result, StorageError, Version, MAX_PAYLOAD_SIZE};

/// Upper bound on how long a group-commit leader waits for concurrent writers before an
/// `fsync` (only after a batch of more than one frame; a lone writer never waits).
const GROUP_COMMIT_LINGER_MAX: std::time::Duration = std::time::Duration::from_micros(250);
/// The wait is this fraction of the measured `fsync` time: about 225 µs on a network disk
/// (2.7 ms `fsync`), a few microseconds on local NVMe.
const GROUP_COMMIT_LINGER_DIVISOR: u64 = 12;

/// Bytes of zeros written ahead of the log at a time (`fsync`ing policies).
const WAL_PREALLOC_CHUNK: u64 = 1 << 20;
/// A new chunk is written once less than this much preallocated space is left.
const WAL_PREALLOC_LOW_WATER: u64 = 256 << 10;
static WAL_ZEROS: [u8; 64 << 10] = [0; 64 << 10];


/// Staged (`AsyncMemoryFirst`) bytes past which an appender pushes them to the OS itself.
///
/// The engine's flusher drains the buffer before every batch it flushes, so under load it is
/// written every few hundred records and stays far below this. The limit is only the bound
/// for when the flusher falls behind: an appender that hits it makes a multi-megabyte `write`
/// in the middle of its `put`. At 256 KiB appenders hit it about once in 2,000 puts, which was
/// the 4-writer p99.9 (90–105 µs against 25–30 µs at 4 MiB on a `c7i.xlarge`).
const STAGED_FLUSH_BYTES: usize = 4 << 20;
/// Capacity of one staging chunk. Frames are appended to chunks of this size, and a full
/// chunk is sealed rather than grown, so appending never copies what is already staged (a
/// growing `Vec` copies megabytes under the lock when the flusher falls behind).
const STAGED_CHUNK_BYTES: usize = 256 << 10;
/// Emptied chunks kept for reuse after a drain (1 MiB): reused chunks are already resident,
/// so appenders take no page faults on them.
const STAGED_POOL_CHUNKS: usize = 4;

/// Batches whose `fsync` may be in flight at once. The `fsync` runs on a second handle without
/// holding `io`, so a value of 2 lets the next batch be written and synced while one is still
/// syncing; batches are always written in LSN order and acknowledged in that order. Measured
/// on ext4 over gp3 EBS (`BENCHMARK_REPORT.md` section 8.2), 2 cost 34% of 4-writer durable
/// throughput and raised p99 from 2.5 ms to 4.4 ms: concurrent `fdatasync`s of one file do not
/// overlap there, and batches only got smaller. So one at a time.
const MAX_SYNCS_IN_FLIGHT: u32 = 1;

/// Unlocked catch-up copies a checkpoint truncation makes before its locked final copy.
const TRUNCATE_CATCH_UP_ROUNDS: usize = 16;
/// A tail this short is left for the locked final copy (writers wait while it is copied).
const TRUNCATE_LOCKED_TAIL_BYTES: u64 = 1 << 20;

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
    /// Threads blocked on `cv` (counted under this lock): a completed batch wakes them only if
    /// there are any, so the uncontended path makes no futex wake syscall.
    waiters: u32,
    /// Frames in the last batch written. Above one, writers are concurrent, and the next leader
    /// of an `fsync`ing policy waits [`GROUP_COMMIT_LINGER`] for them before syncing.
    last_batch: usize,
    /// Set after any I/O failure; the WAL is fail-stop from then on.
    broken: Option<String>,
    /// Batches written whose `fsync` has not been published yet (at most
    /// [`MAX_SYNCS_IN_FLIGHT`]).
    syncing: u32,
    /// Order tickets of `fsync`ing batches: the next to hand out, and the next to publish.
    next_ticket: u64,
    done_ticket: u64,
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
    io: Mutex<BufWriter<VfsHandle>>,
    /// A second handle on the WAL file, `fsync`ed without holding `io` so the next batch can be
    /// written meanwhile (`fdatasync` covers the file, whichever handle wrote). Replaced when a
    /// checkpoint swaps the file.
    sync_file: Mutex<Arc<VfsHandle>>,
    /// `AsyncMemoryFirst` only: frames appended since the last flush, in LSN order. Appenders
    /// copy into it under this short lock; [`Self::flush_buffered`] swaps it out and writes it
    /// holding only `io`, so an append never waits for a `write` syscall.
    staged: Mutex<StagedLog>,
    /// Emptied staging chunks for reuse, at most [`STAGED_POOL_CHUNKS`]. Lock order: `staged`
    /// before this.
    staged_pool: Mutex<Vec<Vec<u8>>>,
    vfs: VfsRef,
    total_batches: CachePadded<AtomicU64>,
    total_frames: CachePadded<AtomicU64>,
    /// Moving average of a batch `fsync`, in nanoseconds (sizes the group-commit wait).
    fsync_ns: AtomicU64,
    /// End of the frames written to the file (a lower bound while a write is in flight); the
    /// file can extend past it with preallocated zeros.
    logical_end: AtomicU64,
    /// File length including preallocated zeros (`fsync`ing policies; changed under `io`).
    alloc_end: AtomicU64,
    /// One checkpoint truncation at a time: between its two phases the file only grows.
    truncating: Mutex<()>,
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





/// Reads the next valid frame from `r` into `buf` (header and body, as on disk) and returns its
/// header. Tells a clean end (end of file, a torn or corrupt frame: `Ok(None)`) from a failed
/// read (`Err`). Recovery must never take an I/O
/// error for the end of the log: opening would cut the file there.
fn next_frame(r: &mut impl Read, buf: &mut Vec<u8>) -> Result<Option<DecodedHeader>> {
    fn fill(r: &mut impl Read, buf: &mut [u8]) -> Result<bool> {
        match r.read_exact(buf) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
            Err(e) => Err(StorageError::io("read WAL", e)),
        }
    }
    buf.clear();
    buf.resize(WAL_FRAME_HEADER_LEN, 0);
    if !fill(r, buf)? {
        return Ok(None);
    }
    let Some(h) = decode_wal_frame_header(buf) else { return Ok(None) };
    if h.body_len > MAX_PAYLOAD_SIZE as usize || h.uncompressed_len > MAX_PAYLOAD_SIZE as usize {
        return Ok(None);
    }
    buf.resize(WAL_FRAME_HEADER_LEN + h.body_len, 0);
    if !fill(r, &mut buf[WAL_FRAME_HEADER_LEN..])? {
        return Ok(None);
    }
    let sum = compute_wal_checksum(
        h.lsn,
        h.txn_id,
        h.collection_id,
        h.kind,
        h.flags,
        h.record_id,
        h.version,
        &buf[WAL_FRAME_HEADER_LEN..],
        h.uncompressed_len as u32,
    );
    Ok((sum == h.checksum).then_some(h))
}

/// Streams the valid frames of the WAL at `path`, one at a time, calling `f(header, body)` for
/// each; stops at the first torn or corrupt frame, or where `f` returns `false` (that frame is
/// not counted). Returns the offset just past the last valid frame. Memory stays at one frame
/// however long the log is (reading it whole took a recovering node to 2–4× its data).
fn scan_frames(
    vfs: &VfsRef,
    path: &Path,
    f: impl FnMut(&DecodedHeader, &[u8]) -> Result<bool>,
) -> Result<u64> {
    if !vfs.exists(path) {
        return Ok(0);
    }
    let file = vfs
        .open(path, OpenOpts::read())
        .map_err(|e| StorageError::io("open WAL for recovery", e))?;
    scan_reader(BufReader::with_capacity(1 << 20, file), f)
}

/// [`scan_frames`] over any reader.
fn scan_reader(mut r: impl Read, mut f: impl FnMut(&DecodedHeader, &[u8]) -> Result<bool>) -> Result<u64> {
    let mut buf = Vec::new();
    let mut end = 0u64;
    while let Some(h) = next_frame(&mut r, &mut buf)? {
        if !f(&h, &buf[WAL_FRAME_HEADER_LEN..])? {
            break;
        }
        end += buf.len() as u64;
    }
    Ok(end)
}

/// The entry a frame holds, decompressed; `None` if its LZ4 body does not decompress (treated
/// as the end of the log, as a torn frame is).
fn entry_of(h: &DecodedHeader, body: &[u8]) -> Option<WalEntry> {
    let frame_bytes = if (h.flags & WAL_FLAG_LZ4) != 0 {
        decompress_lz4(body, h.uncompressed_len).ok()?
    } else {
        body.to_vec()
    };
    Some(WalEntry {
        lsn: h.lsn,
        txn_id: h.txn_id,
        collection_id: h.collection_id,
        kind: h.kind,
        record_id: h.record_id,
        version: h.version,
        frame_bytes,
    })
}


fn sync_parent_dir(vfs: &VfsRef, path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        vfs.sync_dir(parent)
            .map_err(|e| StorageError::io("fsync WAL directory", e))?;
    }
    Ok(())
}

impl GroupCommitWal {
    /// Opens (or creates) the WAL at `path`.
    ///
    /// Any torn trailing frame left by a crash is truncated, and the LSN / transaction-id
    /// counters resume after the highest values found in the file.
    pub fn open(path: impl Into<PathBuf>, policy: SyncPolicy) -> Result<Self> {
        let path = path.into();
        let vfs = storage_types::vfs::at(&path);
        Self::open_with(path, policy, vfs)
    }

    /// [`Self::open`] on the given file system (`F-SIM-01`).
    pub fn open_with(path: impl Into<PathBuf>, policy: SyncPolicy, vfs: VfsRef) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            if !vfs.exists(parent) {
                vfs.create_dir_all(parent)
                    .map_err(|e| StorageError::io("create WAL directory", e))?;
            }
        }

        let existed = vfs.exists(&path);
        // One frame at a time: only the counters and the valid length are needed here.
        let mut max_lsn = 0u64;
        let mut max_txn = 0u64;
        let valid_len = scan_frames(&vfs, &path, |h, body| {
            if (h.flags & WAL_FLAG_LZ4) != 0 && decompress_lz4(body, h.uncompressed_len).is_err() {
                // The log ends here, as at a torn frame.
                return Ok(false);
            }
            max_lsn = max_lsn.max(h.lsn);
            max_txn = max_txn.max(h.txn_id);
            if h.kind == WalOpKind::Checkpoint {
                // Checkpoint markers carry `next_txn_id - 1` in `version`.
                max_txn = max_txn.max(h.version.0);
            }
            Ok(true)
        })?;

        let mut file = vfs
            .open(&path, OpenOpts::create())
            .map_err(|e| StorageError::io("open WAL file", e))?;
        if !existed {
            // A new WAL's directory entry must be durable before any commit relies on it.
            file.sync_all()
                .map_err(|e| StorageError::io("fsync new WAL file", e))?;
            sync_parent_dir(&vfs, &path)?;
        }
        let file_len = file.len().map_err(|e| StorageError::io("size WAL file", e))?;
        if valid_len < file_len {
            file.set_len(valid_len)
                .map_err(|e| StorageError::io("truncate torn WAL tail", e))?;
            file.sync_data()
                .map_err(|e| StorageError::io("fsync truncated WAL tail", e))?;
        }
        let end = file.seek(SeekFrom::End(0))
            .map_err(|e| StorageError::io("seek WAL end", e))?;
        // Read-write: Windows flushes only handles with write access (Linux and macOS sync
        // any descriptor).
        let sync_file = vfs
            .open(&path, OpenOpts::read_write())
            .map_err(|e| StorageError::io("open WAL sync handle", e))?;

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
                waiters: 0,
                last_batch: 0,
                broken: None,
                syncing: 0,
                next_ticket: 0,
                done_ticket: 0,
            }),
            cv: Condvar::new(),
            io: Mutex::new(BufWriter::with_capacity(256 * 1024, file)),
            sync_file: Mutex::new(Arc::new(sync_file)),
            staged: Mutex::new(StagedLog::default()),
            staged_pool: Mutex::new(Vec::new()),
            vfs,
            total_batches: CachePadded::new(AtomicU64::new(0)),
            total_frames: CachePadded::new(AtomicU64::new(0)),
            fsync_ns: AtomicU64::new(0),
            logical_end: AtomicU64::new(end),
            alloc_end: AtomicU64::new(end),
            truncating: Mutex::new(()),
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
        // Hashed (and compressed) before the lock, which then covers only the header and copy.
        let prepared = (!self.policy.syncs()).then(|| PreparedBody::new(frame_bytes));
        let mut guard = self.lock_state();
        if let Some(ref msg) = guard.broken {
            return Err(Self::broken_error(msg));
        }
        if let (Some(prepared), false, true) = (prepared.as_ref(), guard.flushing, guard.queue.is_empty()) {
            // `AsyncMemoryFirst` with nothing pending: there is no `fsync` to share, so the frame
            // is staged right here, with its LSN, under the state lock: no batch can be staged
            // in between (file order stays LSN order) and no writer waits on the condvar.
            guard.last_lsn += 1;
            let lsn = guard.last_lsn;
            self.current_lsn.store(lsn, Ordering::Release);
            let full = {
                let mut staged = storage_types::lock_spinning(&self.staged);
                let need = WAL_FRAME_HEADER_LEN + prepared.bytes.len();
                staged.append(need, &self.staged_pool, |buf| {
                    prepared.write(buf, lsn, 0, collection_id, kind, record_id, version);
                });
                staged.len() >= STAGED_FLUSH_BYTES
            };
            guard.enqueued_seq += 1;
            guard.completed_seq = guard.enqueued_seq;
            drop(guard);
            self.total_frames.fetch_add(1, Ordering::Relaxed);
            self.total_batches.fetch_add(1, Ordering::Relaxed);
            if full {
                let mut io = self.lock_io();
                if let Err(e) = self.drain_staged(&mut io) {
                    drop(io);
                    self.lock_state().broken = Some(e.to_string());
                    return Err(e);
                }
            }
            if kind != WalOpKind::Upsert {
                // Only the hot upsert path is left buffered under `AsyncMemoryFirst`.
                self.flush_buffered()?;
            }
            return Ok(lsn);
        }
        let concurrent = self.policy.syncs() && guard.last_batch > 1;
        if !guard.flushing && guard.syncing < MAX_SYNCS_IN_FLIGHT && guard.queue.is_empty() && !concurrent {
            // Uncontended fast path: become leader for a single frame without allocating a batch.
            guard.flushing = true;
            guard.last_batch = 1;
            guard.last_lsn += 1;
            let lsn = guard.last_lsn;
            self.current_lsn.store(lsn, Ordering::Release);
            guard.enqueued_seq += 1;
            let my_seq = guard.enqueued_seq;
            let ticket = guard.next_ticket;
            if self.policy.syncs() {
                guard.next_ticket += 1;
            }
            drop(guard);

            self.total_frames.fetch_add(1, Ordering::Relaxed);
            if self.policy.syncs() {
                let written = {
                    let mut io = self.lock_io();
                    Self::write_frame(&mut *io, lsn, 0, collection_id, kind, record_id, version, frame_bytes)
                        .and_then(|()| self.prepare_sync(&mut io))
                };
                self.sync_and_publish(ticket, my_seq, written)?;
            } else {
                let res = self.stage(|buf| Self::write_frame(buf, lsn, 0, collection_id, kind, record_id, version, frame_bytes));
                self.complete_batch(my_seq, res)?;
            }
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
        storage_types::lock_spinning(&self.state)
    }

    #[inline]
    /// Appends frames to the staged buffer (`AsyncMemoryFirst`); past [`STAGED_FLUSH_BYTES`]
    /// the appender pushes them to the OS itself. A failed write leaves nothing staged.
    fn stage(&self, write: impl FnOnce(&mut Vec<u8>) -> Result<()>) -> Result<()> {
        let full = {
            let mut staged = storage_types::lock_spinning(&self.staged);
            staged.try_append(&self.staged_pool, write)?;
            staged.len() >= STAGED_FLUSH_BYTES
        };
        if full {
            let mut io = self.lock_io();
            self.drain_staged(&mut io)?;
        }
        Ok(())
    }

    /// Moves the staged frames into `io` (whose lock the caller holds, which orders drains).
    fn drain_staged(&self, io: &mut BufWriter<VfsHandle>) -> Result<()> {
        // Keep a chunk ready, allocated outside `staged`'s lock, for the appender that opens
        // the next one. Syncing policies never stage, so they never allocate one.
        if storage_types::lock_spinning(&self.staged).is_empty() {
            return Ok(());
        }
        {
            let mut pool = self.staged_pool.lock().unwrap_or_else(|p| p.into_inner());
            if pool.is_empty() {
                pool.push(Vec::with_capacity(STAGED_CHUNK_BYTES));
            }
        }
        let taken = std::mem::take(&mut *storage_types::lock_spinning(&self.staged));
        let mut written = Vec::with_capacity(taken.sealed.len() + 1);
        for chunk in taken.sealed.into_iter().chain(std::iter::once(taken.open)) {
            io.write_all(&chunk)
                .map_err(|e| StorageError::io("write staged WAL frames", e))?;
            written.push(chunk);
        }
        let mut pool = self.staged_pool.lock().unwrap_or_else(|p| p.into_inner());
        for mut chunk in written {
            // A chunk one huge frame grew is freed, not pooled.
            if pool.len() < STAGED_POOL_CHUNKS && chunk.capacity() == STAGED_CHUNK_BYTES {
                chunk.clear();
                pool.push(chunk);
            }
        }
        Ok(())
    }

    fn lock_io(&self) -> std::sync::MutexGuard<'_, BufWriter<VfsHandle>> {
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
            if !guard.flushing && guard.syncing < MAX_SYNCS_IN_FLIGHT {
                guard.flushing = true;
                if self.policy.syncs() && guard.last_batch > 1 && guard.queue.len() < guard.last_batch {
                    // Writers are concurrent: the ones just served are about to queue again.
                    // Waiting for them makes one `fsync` cover all instead of alternating
                    // small and large batches (each costing a full `fsync`).
                    drop(guard);
                    std::thread::sleep(self.group_commit_linger());
                    guard = self.lock_state();
                }
                let drained = std::mem::take(&mut guard.queue);
                guard.last_batch = drained.len();
                let batch_seq = guard.enqueued_seq;
                let ticket = guard.next_ticket;
                if self.policy.syncs() {
                    guard.next_ticket += 1;
                }
                drop(guard);

                let res = if !self.policy.syncs() {
                    self.stage(|buf| {
                        for item in &drained {
                            Self::write_frame(
                                buf,
                                item.lsn,
                                item.txn_id,
                                item.collection_id,
                                item.kind,
                                item.record_id,
                                item.version,
                                &item.frame_bytes,
                            )?;
                        }
                        Ok(())
                    })
                } else {
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
                    r.and_then(|()| self.prepare_sync(&mut io))
                };
                // Reuse the drained queue's allocation for the next batch.
                let mut drained = drained;
                drained.clear();
                let res = if self.policy.syncs() {
                    self.sync_and_publish(ticket, batch_seq, res)
                } else {
                    self.complete_batch(batch_seq, res)
                };
                guard = self.lock_state();
                if guard.queue.is_empty() && guard.queue.capacity() < drained.capacity() {
                    guard.queue = drained;
                }
                res?;
            } else {
                guard.waiters += 1;
                guard = self
                    .cv
                    .wait(guard)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                guard.waiters -= 1;
            }
        }
    }

    /// Readies a written batch of an `fsync`ing policy for [`Self::sync_and_publish`]: pushes
    /// it to the OS and preallocates ahead of it.
    fn prepare_sync(&self, io: &mut BufWriter<VfsHandle>) -> Result<()> {
        {
            io.flush()
                .map_err(|e| StorageError::io("flush WAL buffer", e))?;
            let pos = io
                .stream_position()
                .map_err(|e| StorageError::io("locate WAL end", e))?;
            self.preallocate_after(io, pos)?;
            self.logical_end.store(pos, Ordering::Release);
        }
        Ok(())
    }

    /// Keeps zeros ahead of the log once a batch ending at `pos` is written (under `io`).
    ///
    /// Later appends then overwrite blocks the file already has, so their `fdatasync` commits
    /// no size or extent change (about 3x faster on ext4: 0.9 ms against 2.8 ms on EBS). The
    /// chunk becomes durable with this batch's `fdatasync`; recovery stops at the zeros like at
    /// any torn tail. Writing the chunk from a background job with direct I/O instead (tried
    /// 2026-11-12) cost 4-writer durable puts 16 % of their throughput and raised their p99.9
    /// from 6.2 to 11 ms on gp3: its writes and `fdatasync` competed with every commit's.
    fn preallocate_after(&self, io: &mut BufWriter<VfsHandle>, pos: u64) -> Result<()> {
        let alloc = self.alloc_end.load(Ordering::Relaxed);
        if pos.saturating_add(WAL_PREALLOC_LOW_WATER) <= alloc {
            return Ok(());
        }
        let start = alloc.max(pos);
        io.seek(SeekFrom::Start(start))
            .map_err(|e| StorageError::io("seek WAL preallocation", e))?;
        for _ in 0..WAL_PREALLOC_CHUNK / WAL_ZEROS.len() as u64 {
            io.write_all(&WAL_ZEROS)
                .map_err(|e| StorageError::io("preallocate WAL", e))?;
        }
        io.flush()
            .map_err(|e| StorageError::io("preallocate WAL", e))?;
        io.seek(SeekFrom::Start(pos))
            .map_err(|e| StorageError::io("seek WAL end", e))?;
        self.alloc_end.store(start + WAL_PREALLOC_CHUNK, Ordering::Relaxed);
        Ok(())
    }

    /// `fsync`s the WAL without holding `io`, then publishes the batch: `completed_seq` moves
    /// only in ticket order, so a later batch never acknowledges its writers before an earlier
    /// batch's `fsync` has succeeded (or reported the failure that stops the WAL).
    fn sync_and_publish(&self, ticket: u64, batch_seq: u64, written: Result<()>) -> Result<()> {
        {
            let mut guard = self.lock_state();
            // The next leader may write its batch while this one syncs.
            guard.flushing = false;
            guard.syncing += 1;
            if guard.waiters > 0 {
                self.cv.notify_all();
            }
        }
        let res = written.and_then(|()| {
            let file = Arc::clone(&self.sync_file.lock().unwrap_or_else(|p| p.into_inner()));
            let t = std::time::Instant::now();
            file.sync_data()
                .map_err(|e| StorageError::io("fsync WAL file", e))?;
            let took = t.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
            let avg = self.fsync_ns.load(Ordering::Relaxed);
            let next = if avg == 0 { took } else { avg - avg / 8 + took / 8 };
            self.fsync_ns.store(next, Ordering::Relaxed);
            Ok(())
        });
        let mut guard = self.lock_state();
        while guard.done_ticket != ticket && guard.broken.is_none() {
            guard.waiters += 1;
            guard = self.cv.wait(guard).unwrap_or_else(|p| p.into_inner());
            guard.waiters -= 1;
        }
        guard.syncing -= 1;
        guard.done_ticket = guard.done_ticket.max(ticket + 1);
        let out = match (res, guard.broken.clone()) {
            (Err(e), _) => {
                guard.broken = Some(e.to_string());
                Err(e)
            }
            (Ok(()), Some(msg)) => Err(Self::broken_error(&msg)),
            (Ok(()), None) => {
                guard.completed_seq = guard.completed_seq.max(batch_seq);
                self.total_batches.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        };
        if guard.waiters > 0 || out.is_err() {
            self.cv.notify_all();
        }
        out
    }


    /// How long the next group-commit leader waits for concurrent writers.
    fn group_commit_linger(&self) -> std::time::Duration {
        let ns = self.fsync_ns.load(Ordering::Relaxed) / GROUP_COMMIT_LINGER_DIVISOR;
        std::time::Duration::from_nanos(ns).min(GROUP_COMMIT_LINGER_MAX)
    }

    /// Forces every frame appended so far to stable storage (`write` + `fsync`), whatever the
    /// [`SyncPolicy`]. Used before discarding state that the WAL is the only durable copy of
    /// (e.g. Raft log compaction). An I/O error fail-stops the WAL.
    pub fn sync_to_disk(&self) -> Result<()> {
        let res = {
            let mut io = self.lock_io();
            self.drain_staged(&mut io)
                .and_then(|()| io.flush().map_err(|e| StorageError::io("flush WAL buffer", e)))
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
            self.drain_staged(&mut io).and_then(|()| {
                if io.buffer().is_empty() {
                    return Ok(());
                }
                io.flush()
                    .map_err(|e| StorageError::io("flush buffered WAL frames", e))?;
                let pos = io
                    .stream_position()
                    .map_err(|e| StorageError::io("locate WAL end", e))?;
                self.logical_end.store(pos, Ordering::Release);
                Ok(())
            })
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
                if guard.waiters > 0 {
                    self.cv.notify_all();
                }
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
        f: impl FnOnce(&mut BufWriter<VfsHandle>, &mut bool) -> Result<R>,
    ) -> Result<R> {
        let mut guard = self.lock_state();
        while guard.flushing {
            guard.waiters += 1;
            guard = self
                .cv
                .wait(guard)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.waiters -= 1;
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
            // Staged frames come first: `f` must see every frame appended before it.
            match self.drain_staged(&mut io) {
                Ok(()) => f(&mut io, &mut poison),
                Err(e) => Err(e),
            }
        };

        let mut guard = self.lock_state();
        guard.flushing = false;
        if let (Err(ref e), true) = (&res, poison) {
            guard.broken = Some(e.to_string());
        }
        if guard.waiters > 0 || guard.broken.is_some() {
            self.cv.notify_all();
        }
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
    /// Writers are held up only for the short second phase. The first phase notes how far the
    /// WAL reaches and, without blocking appends, reads, filters and syncs that prefix, then
    /// copies what was appended meanwhile (up to [`TRUNCATE_CATCH_UP_ROUNDS`] times, until less
    /// than [`TRUNCATE_LOCKED_TAIL_BYTES`] is left). The second copies the rest, unfiltered, and
    /// swaps the files. Those frames were
    /// all logged after the caller chose `cutoff_lsn` (or are still to be applied), so the old
    /// single-phase rewrite kept them too.
    ///
    /// Callers must guarantee that every committed operation with `lsn <= cutoff_lsn` has been
    /// applied and durably checkpointed into the data files.
    pub fn checkpoint_truncate_before(
        &self,
        cutoff_lsn: u64,
        keep_collection: &dyn Fn(u16) -> bool,
        archive_dir: Option<&Path>,
    ) -> Result<()> {
        let _one = self.truncating.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        // Phase 1: how far the WAL reaches now. Only appends can follow until phase 2.
        // The log's end, not the file's: the file may extend past it with preallocated zeros.
        let head_len = self.with_exclusive_io(|io, _| {
            io.flush()
                .map_err(|e| StorageError::io("flush WAL before checkpoint truncate", e))?;
            io.stream_position()
                .map_err(|e| StorageError::io("locate WAL end before checkpoint truncate", e))
        })?;
        let mut next_path = self.path.as_os_str().to_os_string();
        next_path.push(".next");
        let next_path = PathBuf::from(next_path);
        let mut next = self
            .vfs
            .open(&next_path, OpenOpts::create_truncate())
            .map_err(|e| StorageError::io("create replacement WAL file", e))?;
        {
            // Head marker preserving the LSN / txn-id high-water marks across restarts (frames
            // appended later carry higher ones, which recovery takes from the frames themselves).
            let mut out = BufWriter::with_capacity(1 << 20, &mut next);
            let next_txn = self.next_txn_id.load(Ordering::Acquire);
            Self::write_frame(
                &mut out,
                self.current_lsn.load(Ordering::Acquire),
                0,
                0,
                WalOpKind::Checkpoint,
                CHECKPOINT_ID,
                Version(next_txn.saturating_sub(1)),
                &[],
            )?;
            // One pass over the prefix, one frame in memory at a time. A transaction's commit
            // barrier follows its operations, so whether it has a kept one is known by then.
            let src = self
                .vfs
                .open(&self.path, OpenOpts::read())
                .map_err(|e| StorageError::io("open WAL for checkpoint truncate", e))?;
            let mut src = BufReader::with_capacity(1 << 20, src.take(head_len));
            let mut kept_txns: HashSet<u64> = HashSet::new();
            let mut archive: Option<(PathBuf, BufWriter<VfsHandle>)> = None;
            let mut frame = Vec::new();
            // A read error aborts the truncation before the swap: ending the copy early would
            // drop every frame after it.
            while let Some(h) = next_frame(&mut src, &mut frame)? {
                let keep = match h.kind {
                    WalOpKind::Checkpoint => continue,
                    WalOpKind::TxnCommit => h.lsn > cutoff_lsn || kept_txns.contains(&h.txn_id),
                    _ => {
                        let keep = h.lsn > cutoff_lsn || keep_collection(h.collection_id);
                        if keep && h.txn_id != 0 {
                            kept_txns.insert(h.txn_id);
                        }
                        keep
                    }
                };
                if keep {
                    out.write_all(&frame)
                        .map_err(|e| StorageError::io("write replacement WAL file", e))?;
                } else if let Some(dir) = archive_dir {
                    if archive.is_none() {
                        self.vfs
                            .create_dir_all(dir)
                            .map_err(|e| StorageError::io("create WAL archive directory", e))?;
                        let seg_path = dir.join(format!("wal_{:020}.log", h.lsn));
                        let seg = self
                            .vfs
                            .open(&seg_path, OpenOpts::create_truncate())
                            .map_err(|e| StorageError::io("create archived WAL segment", e))?;
                        archive = Some((seg_path, BufWriter::with_capacity(1 << 20, seg)));
                    }
                    if let Some((_, seg)) = archive.as_mut() {
                        seg.write_all(&frame)
                            .map_err(|e| StorageError::io("write archived WAL segment", e))?;
                    }
                }
            }
            if let Some((seg_path, seg)) = archive {
                let seg = seg
                    .into_inner()
                    .map_err(|e| StorageError::io("write archived WAL segment", e.into_error()))?;
                seg.sync_all()
                    .map_err(|e| StorageError::io("fsync archived WAL segment", e))?;
                sync_parent_dir(&self.vfs, &seg_path)?;
            }
            out.flush()
                .map_err(|e| StorageError::io("write replacement WAL file", e))?;
        }
        next.sync_data()
            .map_err(|e| StorageError::io("fsync replacement WAL file", e))?;

        // Catch up without the lock while appends keep coming: the file only grows, so its
        // bytes can be copied as they are (a frame cut in two is finished by the next copy).
        let mut copied = head_len;
        for _ in 0..TRUNCATE_CATCH_UP_ROUNDS {
            // Frames known to be in the file (a lower bound; the locked copy takes the rest).
            let len = self.logical_end.load(Ordering::Acquire);
            if len.saturating_sub(copied) < TRUNCATE_LOCKED_TAIL_BYTES {
                break;
            }
            copied = self.copy_wal_bytes(copied, Some(len), &mut next)?;
            // Synced now, so the locked final sync only has the last copy to write out.
            next.sync_data()
                .map_err(|e| StorageError::io("fsync replacement WAL file", e))?;
        }

        // Phase 2: copy the rest, appended since the last copy, and swap the files.
        self.with_exclusive_io(|io, poison| {
            io.flush()
                .map_err(|e| StorageError::io("flush WAL before checkpoint swap", e))?;
            let end = io
                .stream_position()
                .map_err(|e| StorageError::io("locate WAL end before checkpoint swap", e))?;
            self.copy_wal_bytes(copied, Some(end), &mut next)?;
            next.sync_all()
                .map_err(|e| StorageError::io("fsync replacement WAL file", e))?;

            // Close the old handle before replacing the file (required on Windows).
            drop(next);
            let reopened = self
                .vfs
                .open(&next_path, OpenOpts::read_write())
                .map_err(|e| StorageError::io("reopen replacement WAL file", e))?;
            let placeholder = BufWriter::with_capacity(256 * 1024, reopened);
            let old = std::mem::replace(io, placeholder);
            drop(old.into_parts().0);
            // Freeing the old file's blocks can take a while for a large WAL: keep it open, so
            // that happens when this handle is dropped, after writers are let go.
            let old_file = self.vfs.open(&self.path, OpenOpts::read()).ok();
            if let Err(e) = self.vfs.rename(&next_path, &self.path) {
                // The old handle is closed and the new file is not in place: stop accepting writes.
                *poison = true;
                return Err(StorageError::io("atomically replace WAL file", e));
            }
            if let Err(e) = sync_parent_dir(&self.vfs, &self.path) {
                *poison = true;
                return Err(e);
            }
            match self.vfs.open(&self.path, OpenOpts::read_write()) {
                Ok(h) => *self.sync_file.lock().unwrap_or_else(|p| p.into_inner()) = Arc::new(h),
                Err(e) => {
                    // Batches would be `fsync`ed on the old file: stop accepting writes.
                    *poison = true;
                    return Err(StorageError::io("open replacement WAL sync handle", e));
                }
            }
            let end = io
                .get_mut()
                .seek(SeekFrom::End(0))
                .map_err(|e| StorageError::io("seek replacement WAL end", e))?;
            // The replacement holds no preallocated zeros yet.
            self.logical_end.store(end, Ordering::Release);
            self.alloc_end.store(end, Ordering::Relaxed);
            Ok(old_file)
        })
        .map(drop)
    }

    /// Appends the active WAL's bytes `[from, to)` (to its end when `to` is `None`) to `dst`;
    /// returns the offset copied up to.
    fn copy_wal_bytes(&self, from: u64, to: Option<u64>, dst: &mut VfsHandle) -> Result<u64> {
        let mut src = self
            .vfs
            .open(&self.path, OpenOpts::read())
            .map_err(|e| StorageError::io("open WAL tail", e))?;
        src.seek(SeekFrom::Start(from))
            .map_err(|e| StorageError::io("seek WAL tail", e))?;
        let mut src = src.take(to.map_or(u64::MAX, |t| t.saturating_sub(from)));
        let n = std::io::copy(&mut src, dst).map_err(|e| StorageError::io("copy WAL tail to replacement", e))?;
        Ok(from + n)
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
        Self::recover_committed_with(&storage_types::vfs::at(path), path)
    }

    /// [`Self::recover_committed`] on the given file system.
    pub fn recover_committed_with(vfs: &VfsRef, path: &Path) -> Result<Vec<WalEntry>> {
        let mut out = Vec::new();
        Self::for_each_committed(vfs, path, |e| {
            out.push(e);
            Ok(())
        })?;
        Ok(out)
    }

    /// Streams the committed operations of the WAL at `path` to `f`, in the order
    /// [`Self::recover_committed`] returns them, holding one frame at a time (plus the
    /// operations of transactions whose commit barrier has not been read yet). Recovery uses
    /// this, so a node replaying a large log does not hold it all in memory.
    pub fn for_each_committed(vfs: &VfsRef, path: &Path, f: impl FnMut(WalEntry) -> Result<()>) -> Result<()> {
        if !vfs.exists(path) {
            return Ok(());
        }
        let file = vfs
            .open(path, OpenOpts::read())
            .map_err(|e| StorageError::io("open WAL for recovery", e))?;
        committed_from_reader(BufReader::with_capacity(1 << 20, file), f)
    }
}

/// The committed operations of the frames `r` yields, streamed to `f` (see
/// [`GroupCommitWal::for_each_committed`]).
fn committed_from_reader(r: impl Read, mut f: impl FnMut(WalEntry) -> Result<()>) -> Result<()> {
    let mut pending_txns: HashMap<u64, Vec<WalEntry>> = HashMap::new();
    scan_reader(r, |h, body| {
        let Some(entry) = entry_of(h, body) else { return Ok(false) };
        match entry.kind {
            WalOpKind::Checkpoint => {}
            WalOpKind::TxnCommit => {
                if let Some(ops) = pending_txns.remove(&entry.txn_id) {
                    if ops.len() as u64 == entry.version.0 {
                        for op in ops {
                            f(op)?;
                        }
                    }
                }
            }
            _ if entry.txn_id == 0 => f(entry)?,
            _ => pending_txns.entry(entry.txn_id).or_default().push(entry),
        }
        Ok(true)
    })?;
    Ok(())
}

impl GroupCommitWal {
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
            self.vfs
                .copy(&self.path, dest)
                .map_err(|e| StorageError::io("copy active WAL", e))?;
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
            let vfs = storage_types::vfs::at(&seg);
            let mut max_lsn = 0u64;
            scan_frames(&vfs, &seg, |h, _| {
                max_lsn = max_lsn.max(h.lsn);
                Ok(true)
            })?;
            if max_lsn < min_keep_lsn && vfs.remove_file(&seg).is_ok() {
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

/// Frames staged under `AsyncMemoryFirst`, in LSN order: sealed chunks, oldest first, then the
/// open one. Appending seals a full chunk and opens another instead of growing one buffer.
#[derive(Default)]
struct StagedLog {
    sealed: Vec<Vec<u8>>,
    open: Vec<u8>,
    bytes: usize,
}

impl StagedLog {
    fn is_empty(&self) -> bool {
        self.bytes == 0
    }

    fn len(&self) -> usize {
        self.bytes
    }

    /// The chunk to append about `need` bytes to: the open one while it has room, otherwise a
    /// fresh one (from `pool` when it has one) with the full chunk sealed.
    fn room(&mut self, need: usize, pool: &Mutex<Vec<Vec<u8>>>) -> &mut Vec<u8> {
        let fits = self.open.len() + need <= self.open.capacity();
        if (!fits && !self.open.is_empty()) || self.open.len() >= STAGED_CHUNK_BYTES {
            let next = pool.lock().unwrap_or_else(|p| p.into_inner()).pop();
            let next = next.unwrap_or_else(|| Vec::with_capacity(STAGED_CHUNK_BYTES));
            self.sealed.push(std::mem::replace(&mut self.open, next));
        } else if self.open.capacity() == 0 {
            if let Some(chunk) = pool.lock().unwrap_or_else(|p| p.into_inner()).pop() {
                self.open = chunk;
            } else {
                self.open.reserve_exact(STAGED_CHUNK_BYTES);
            }
        }
        // A frame larger than a chunk still fits: the fresh chunk grows once, copying nothing.
        &mut self.open
    }

    fn append(&mut self, need: usize, pool: &Mutex<Vec<Vec<u8>>>, write: impl FnOnce(&mut Vec<u8>)) {
        let buf = self.room(need, pool);
        let before = buf.len();
        write(buf);
        self.bytes += buf.len() - before;
    }

    /// Like [`Self::append`] for a write of unknown size that can fail; on failure nothing it
    /// wrote is kept.
    fn try_append(&mut self, pool: &Mutex<Vec<Vec<u8>>>, write: impl FnOnce(&mut Vec<u8>) -> Result<()>) -> Result<()> {
        let buf = self.room(0, pool);
        let before = buf.len();
        if let Err(e) = write(buf) {
            buf.truncate(before);
            return Err(e);
        }
        self.bytes += buf.len() - before;
        Ok(())
    }
}

impl Drop for GroupCommitWal {
    fn drop(&mut self) {
        let mut io = self.io.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let staged = std::mem::take(&mut *storage_types::lock_spinning(&self.staged));
        for chunk in staged.sealed.iter().chain(std::iter::once(&staged.open)) {
            let _ = io.write_all(chunk);
        }
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
    wal_checksum_from(&meta, checksum_payload(body))
}

/// The frame checksum from the 64-byte metadata block and the body's own hash (computed apart
/// so it can be taken before the WAL's lock; the result is identical).
#[inline]
fn wal_checksum_from(meta: &[u8; 64], body_hash: u64) -> u64 {
    checksum_payload(meta) ^ body_hash.rotate_left(29)
}

/// A frame body ready to be appended: compressed if worth it, hashed. Built outside the WAL's
/// lock; only the LSN-dependent header is computed under it.
struct PreparedBody<'a> {
    flags: u8,
    bytes: std::borrow::Cow<'a, [u8]>,
    uncompressed_len: u32,
    hash: u64,
}

impl<'a> PreparedBody<'a> {
    fn new(frame_bytes: &'a [u8]) -> Self {
        if frame_bytes.len() >= WAL_LZ4_THRESHOLD {
            let compressed = compress_lz4(frame_bytes);
            if compressed.len() < frame_bytes.len() {
                let hash = checksum_payload(&compressed);
                return Self { flags: WAL_FLAG_LZ4, bytes: compressed.into(), uncompressed_len: frame_bytes.len() as u32, hash };
            }
        }
        Self { flags: 0, bytes: frame_bytes.into(), uncompressed_len: 0, hash: checksum_payload(frame_bytes) }
    }

    /// Appends the frame (header for `lsn`, then the body) to `out`.
    #[allow(clippy::too_many_arguments)]
    fn write(&self, out: &mut Vec<u8>, lsn: u64, txn_id: u64, collection_id: u16, kind: WalOpKind, record_id: InlineId, version: Version) {
        let mut hdr = [0u8; WAL_FRAME_HEADER_LEN];
        hdr[0..4].copy_from_slice(&WAL_MAGIC.to_le_bytes());
        hdr[4..6].copy_from_slice(&collection_id.to_le_bytes());
        hdr[6] = kind as u8;
        hdr[7] = self.flags;
        hdr[8..16].copy_from_slice(&lsn.to_le_bytes());
        hdr[16..24].copy_from_slice(&txn_id.to_le_bytes());
        hdr[24..32].copy_from_slice(&version.0.to_le_bytes());
        hdr[32..56].copy_from_slice(&record_id.to_raw_bytes());
        hdr[56..60].copy_from_slice(&(self.bytes.len() as u32).to_le_bytes());
        hdr[60..64].copy_from_slice(&self.uncompressed_len.to_le_bytes());
        // The checksum's metadata block is these first 64 header bytes.
        let mut meta = [0u8; 64];
        meta.copy_from_slice(&hdr[0..64]);
        let sum = wal_checksum_from(&meta, self.hash);
        hdr[64..72].copy_from_slice(&sum.to_le_bytes());
        out.extend_from_slice(&hdr);
        out.extend_from_slice(&self.bytes);
    }
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
    use std::fs::OpenOptions;
    use std::sync::Arc;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    /// Pipelined group commit under power loss: four writers, two `fsync`s in flight, and an
    /// I/O failure injected at a different operation for each seed. Every frame a writer was
    /// told is durable is recovered, per writer in order; a failed earlier `fsync` never lets a
    /// later batch acknowledge its frames.
    #[test]
    fn power_loss_keeps_every_frame_acknowledged_by_concurrent_writers() {
        use storage_types::vfs::{CutMode, SimVfs};
        let dir = temp_dir("storage_wal_power_loss_concurrent");
        let wal_path = dir.join("engine.wal");
        for seed in 1..=60u64 {
            let _ = fs::remove_dir_all(&dir);
            let sim = SimVfs::new(&dir, seed).unwrap();
            if seed % 7 == 0 {
                sim.set_mode(CutMode::LoseAll);
            }
            let wal = Arc::new(GroupCommitWal::open_with(&wal_path, SyncPolicy::GroupCommit { max_batch_entries: 64 }, sim.as_ref()).unwrap());
            sim.fail_at(20 + (seed * 13) % 200);
            let writers: Vec<_> = (0..4u64)
                .map(|t| {
                    let w = Arc::clone(&wal);
                    std::thread::spawn(move || {
                        let id = InlineId::new(&format!("w{t}")).unwrap();
                        let mut acked = Vec::new();
                        for v in 1..=400u64 {
                            let body = vec![(v % 251) as u8; 64 + (v as usize * 31) % 900];
                            match w.append_single(1, WalOpKind::Upsert, id, Version(v), &body) {
                                Ok(_) => acked.push(v),
                                Err(_) => break,
                            }
                        }
                        acked
                    })
                })
                .collect();
            let acked: Vec<Vec<u64>> = writers.into_iter().map(|h| h.join().unwrap()).collect();
            drop(wal);
            sim.power_cut().unwrap();
            let got = GroupCommitWal::recover_committed(&wal_path).unwrap();
            assert!(got.windows(2).all(|w| w[0].lsn < w[1].lsn), "seed {seed}: frames out of LSN order");
            for (t, acked) in acked.iter().enumerate() {
                let id = InlineId::new(&format!("w{t}")).unwrap();
                let versions: Vec<u64> = got.iter().filter(|e| e.record_id == id).map(|e| e.version.0).collect();
                assert!(versions.windows(2).all(|w| w[0] < w[1]), "seed {seed} writer {t}: out of order {versions:?}");
                for v in acked {
                    assert!(versions.contains(v), "seed {seed} writer {t}: acknowledged v{v} lost; recovered {versions:?}");
                }
            }
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// `F-SIM-01`: under simulated power loss and crash points, every acknowledged frame is
    /// recovered, in order, and nothing that was never appended appears; checkpoints
    /// (replacement file + rename) included.
    #[test]
    fn power_loss_never_loses_an_acknowledged_frame() {
        use storage_types::vfs::{CutMode, SimVfs};
        let dir = temp_dir("storage_wal_power_loss");
        let wal_path = dir.join("engine.wal");
        for seed in 1..=120u64 {
            let _ = fs::remove_dir_all(&dir);
            let sim = SimVfs::new(&dir, seed).unwrap();
            if seed % 10 == 0 {
                sim.set_mode(CutMode::LoseAll);
            }
            let mut acked: Vec<u64> = Vec::new(); // versions acknowledged
            let mut checkpointed = 0u64;
            let mut version = 0u64;
            for life in 0..3 {
                let wal = match GroupCommitWal::open_with(&wal_path, SyncPolicy::Immediate, sim.as_ref()) {
                    Ok(w) => w,
                    Err(e) => panic!("seed {seed} life {life}: reopen failed: {e}"),
                };
                sim.fail_at(5 + (seed * 7 + life) % 40);
                let id = InlineId::new("rec").unwrap();
                loop {
                    version += 1;
                    let body = vec![(version % 251) as u8; 100 + (version as usize * 37) % 1500];
                    match wal.append_single(1, WalOpKind::Upsert, id, Version(version), &body) {
                        Ok(_) => acked.push(version),
                        Err(_) => break,
                    }
                    if version % 9 == 0 {
                        // A checkpoint that fails after its rename may still take effect.
                        checkpointed = version;
                        if wal.checkpoint_truncate().is_err() {
                            break;
                        }
                    }
                }
                drop(wal);
                sim.power_cut().unwrap();
                let got: Vec<u64> = GroupCommitWal::recover_committed(&wal_path)
                    .unwrap()
                    .iter()
                    .map(|e| e.version.0)
                    .collect();
                // Acknowledged and not checkpointed away: must be present, in order.
                let must: Vec<u64> = acked.iter().copied().filter(|v| *v > checkpointed).collect();
                let mut it = got.iter();
                for v in &must {
                    assert!(it.any(|g| g == v), "seed {seed} life {life}: acknowledged v{v} lost; got {got:?}");
                }
                for g in &got {
                    assert!(*g <= version && g > &0, "seed {seed}: invented frame v{g}");
                }
                for w in got.windows(2) {
                    assert!(w[0] < w[1], "seed {seed}: frames out of order {got:?}");
                }
                // A frame that was attempted but not acknowledged may survive; treat it as acked
                // from now on (recovery made it durable).
                for g in got {
                    if !acked.contains(&g) {
                        acked.push(g);
                    }
                }
                acked.sort_unstable();
            }
        }
        let _ = fs::remove_dir_all(&dir);
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
        for _ in 0..5_000 * storage_types::testing::fuzz_scale() {
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
            let _ = committed_from_reader(std::io::Cursor::new(&buf), |_| Ok(()));
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

        let wal_disk_bytes = log_bytes(&wal_path);
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

    /// Bytes of valid frames at the start of the WAL file: the log itself, without the zeros
    /// preallocated after it.
    fn log_bytes(path: &Path) -> u64 {
        scan_frames(&storage_types::vfs::at(path), path, |_, _| Ok(true)).unwrap()
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
        let len = log_bytes(&wal_path);
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
    fn test_staged_log_seals_chunks_keeps_order_and_drops_failed_writes() {
        let pool = Mutex::new(Vec::new());
        let mut log = StagedLog::default();
        let mut expect = Vec::new();
        // Frames of 1 KiB to 600 KiB: some fill chunks, one is larger than a chunk.
        for (n, len) in [1024usize, 200 << 10, 100 << 10, 600 << 10, 3000].into_iter().enumerate() {
            let bytes = vec![n as u8; len];
            log.append(len, &pool, |b| b.extend_from_slice(&bytes));
            expect.extend_from_slice(&bytes);
        }
        let before = log.len();
        let err = log.try_append(&pool, |b| {
            b.extend_from_slice(&[9; 100]);
            Err(StorageError::io("test", std::io::Error::other("fail")))
        });
        assert!(err.is_err());
        assert_eq!(log.len(), before, "a failed write must leave nothing staged");
        assert!(!log.sealed.is_empty(), "full chunks are sealed, not grown");
        let all: Vec<u8> = log.sealed.iter().chain(std::iter::once(&log.open)).flatten().copied().collect();
        assert_eq!(all.len(), log.len());
        assert!(all == expect, "staged bytes must be the frames in order");
    }

    /// Recovery tells a failed read from the end of the log: an I/O error part-way through a
    /// frame is an error, never a torn tail (opening would truncate the WAL there), while a
    /// genuinely short tail ends the log cleanly.
    #[test]
    fn test_recovery_reports_read_errors_instead_of_ending_the_log() {
        let mut frames = Vec::new();
        for v in 1..=3u64 {
            GroupCommitWal::write_frame(&mut frames, v, 0, 1, WalOpKind::Upsert, InlineId::new("r").unwrap(), Version(v), &[v as u8; 40]).unwrap();
        }
        struct FailAfter<'a>(&'a [u8], usize);
        impl Read for FailAfter<'_> {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if self.1 == 0 {
                    return Err(std::io::Error::other("disk read failed"));
                }
                let n = buf.len().min(self.1).min(self.0.len());
                buf[..n].copy_from_slice(&self.0[..n]);
                self.0 = &self.0[n..];
                self.1 -= n;
                Ok(n)
            }
        }
        // Fails in the middle of the second frame.
        let cut = frames.len() / 3 + 10;
        let mut seen = 0;
        let r = committed_from_reader(FailAfter(&frames, cut), |_| {
            seen += 1;
            Ok(())
        });
        assert!(r.is_err(), "a read error must not end the log quietly");
        assert_eq!(seen, 1);
        // A short tail (a torn write) ends the log after the whole frames.
        let mut seen = 0;
        committed_from_reader(std::io::Cursor::new(&frames[..cut]), |_| {
            seen += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(seen, 1);
    }

    /// Memory-first frames from concurrent writers survive many staging-buffer swaps (the spare
    /// buffer, the appenders' own drains past `STAGED_FLUSH_BYTES`, and a flusher draining
    /// meanwhile), each exactly once and in LSN order.
    #[test]
    fn test_memory_first_staging_swaps_keep_every_frame_in_order() {
        let dir = temp_dir("storage_wal_staging_swaps");
        let wal_path = dir.join("engine.wal");
        let wal = Arc::new(GroupCommitWal::open(&wal_path, SyncPolicy::AsyncMemoryFirst).unwrap());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flusher = {
            let (w, stop) = (Arc::clone(&wal), Arc::clone(&stop));
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    w.flush_buffered().unwrap();
                    std::thread::yield_now();
                }
            })
        };
        // 4 writers × 6,000 frames of about 900 bytes: over 20 MiB, past the threshold several
        // times; some frames over 1 KiB are LZ4-compressed.
        let writers: Vec<_> = (0..4u64)
            .map(|t| {
                let w = Arc::clone(&wal);
                std::thread::spawn(move || {
                    let id = InlineId::new(&format!("w{t}")).unwrap();
                    for v in 0..6_000u64 {
                        let len = 700 + (v as usize * 37) % 500;
                        w.append_single(1, WalOpKind::Upsert, id, Version(v), &vec![(v % 251) as u8; len])
                            .unwrap();
                    }
                })
            })
            .collect();
        for h in writers {
            h.join().unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        flusher.join().unwrap();
        drop(wal);
        let entries = GroupCommitWal::recover_committed(&wal_path).unwrap();
        assert_eq!(entries.len(), 24_000);
        assert!(entries.windows(2).all(|w| w[0].lsn < w[1].lsn), "file order must equal LSN order");
        for t in 0..4u64 {
            let id = InlineId::new(&format!("w{t}")).unwrap();
            let versions: Vec<u64> = entries.iter().filter(|e| e.record_id == id).map(|e| e.version.0).collect();
            assert_eq!(versions, (0..6_000).collect::<Vec<_>>(), "writer {t}");
        }
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

    /// Truncation copies the frames appended while it runs (`TASK-151`): writers keep appending
    /// through repeated truncations, and afterwards the log holds every frame above the last
    /// cutoff, in order, and at most one late frame per writer at or below it. Frames of 3 KiB are cut in two by the 256 KiB write buffer, so unlocked
    /// catch-up copies end mid-frame and the next copy finishes them.
    #[test]
    fn test_truncation_keeps_every_frame_appended_while_it_runs() {
        let dir = temp_dir("storage_wal_truncate_concurrent");
        let wal_path = dir.join("engine.wal");
        let wal = Arc::new(GroupCommitWal::open(&wal_path, SyncPolicy::AsyncMemoryFirst).unwrap());
        let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut handles = Vec::new();
        for t in 0..4u64 {
            let (w, done) = (Arc::clone(&wal), Arc::clone(&done));
            handles.push(std::thread::spawn(move || {
                let id = InlineId::new(&format!("t{t}")).unwrap();
                let lsns: Vec<u64> = (0..4_000u64)
                    .map(|v| w.append_single(1, WalOpKind::Upsert, id, Version(v), &[t as u8; 3 * 1024]).unwrap())
                    .collect();
                done.fetch_add(1, Ordering::Relaxed);
                lsns
            }));
        }
        let (mut last_cutoff, mut truncations) = (0, 0);
        while done.load(Ordering::Relaxed) < 4 {
            last_cutoff = wal.current_lsn();
            wal.checkpoint_truncate_before(last_cutoff, &|_| false, None).unwrap();
            truncations += 1;
        }
        assert!(truncations >= 2, "the writers finished before truncations overlapped them");
        let mut expected: Vec<u64> = handles.into_iter().flat_map(|h| h.join().unwrap()).filter(|&l| l > last_cutoff).collect();
        expected.sort_unstable();
        wal.flush_buffered().unwrap();
        let got: Vec<u64> = GroupCommitWal::recover_committed(&wal_path).unwrap().iter().map(|e| e.lsn).collect();
        assert!(got.windows(2).all(|w| w[0] < w[1]), "file order must equal LSN order");
        let missing: Vec<u64> = expected.iter().copied().filter(|l| got.binary_search(l).is_err()).collect();
        assert!(missing.is_empty(), "frames above the last cutoff were lost: {missing:?}");
        // A frame whose LSN was handed out before the cutoff but reached the file only after the
        // truncation noted its length stays (the single-phase rewrite kept those too); nothing
        // else at or below the cutoff may.
        let extra: Vec<u64> = got.iter().copied().filter(|l| expected.binary_search(l).is_err()).collect();
        assert!(extra.iter().all(|&l| l <= last_cutoff) && extra.len() <= 4, "frames the truncation should have removed: {extra:?}");
        drop(wal);
        let _ = fs::remove_dir_all(&dir);
    }

    /// Preallocated zeros never cost a frame (`TASK-151`): the file extends past the log, and a
    /// checkpoint truncation, a restart and later appends must all work from the log's end, not
    /// the file's. Were any of them to start at the file's end, frames written after the zeros
    /// would be unreachable at recovery, which stops at the first zero header.
    #[test]
    fn test_preallocated_wal_keeps_every_frame_through_truncation_and_restart() {
        let dir = temp_dir("storage_wal_prealloc");
        let wal_path = dir.join("engine.wal");
        let id = InlineId::new("k").unwrap();
        let cutoff;
        {
            let wal = GroupCommitWal::open(&wal_path, SyncPolicy::Immediate).unwrap();
            for v in 1..=50 {
                wal.append_single(1, WalOpKind::Upsert, id, Version(v), &[v as u8; 100]).unwrap();
            }
            assert!(fs::metadata(&wal_path).unwrap().len() > log_bytes(&wal_path), "zeros were written ahead of the log");
            cutoff = wal.current_lsn() - 10;
            wal.checkpoint_truncate_before(cutoff, &|_| false, None).unwrap();
            for v in 51..=60 {
                wal.append_single(1, WalOpKind::Upsert, id, Version(v), &[v as u8; 100]).unwrap();
            }
            assert!(fs::metadata(&wal_path).unwrap().len() > log_bytes(&wal_path), "the replacement is preallocated too");
        }
        {
            let wal = GroupCommitWal::open(&wal_path, SyncPolicy::Immediate).unwrap();
            for v in 61..=70 {
                wal.append_single(1, WalOpKind::Upsert, id, Version(v), &[v as u8; 100]).unwrap();
            }
            wal.checkpoint_truncate_before(cutoff, &|_| false, None).unwrap();
            wal.append_single(1, WalOpKind::Upsert, id, Version(71), &[71; 100]).unwrap();
        }
        let got: Vec<u64> = GroupCommitWal::recover_committed(&wal_path).unwrap().iter().map(|e| e.version.0).collect();
        assert_eq!(got, (41..=71).collect::<Vec<u64>>(), "every frame above the cutoff, in order");
        let _ = fs::remove_dir_all(&dir);
    }
}
