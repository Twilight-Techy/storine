//! Cluster-wide backups at one timestamp, and restore (`F-GEO-08`, `TASK-144`).
//!
//! **Backup.** One historical read of the whole key space at a timestamp `T`, written out in
//! checksummed segments, with a manifest stored last: a backup without a manifest does not exist.
//! - `T` is the coordinator's clock plus the maximum offset, and the read starts once the clock
//!   has passed it. A write acknowledged before the backup began has a timestamp no clock can
//!   have been past by more than the offset, so it is at or below `T`: **a backup holds every
//!   write acknowledged before it began**.
//! - The read is an ordinary one at `T`: an intent at or below `T` is resolved first (pushing
//!   its transaction), and later writes land above `T` through the timestamp caches. So every
//!   range is read at the same point of the serial order, whatever moves, splits or fails
//!   meanwhile.
//! - **History protection** asks every node to keep what `T` needs while the backup runs. It is
//!   advisory. If a key's history below `T` is trimmed anyway (a restarted node, a key beyond its
//!   version bounds), the read refuses it and the backup starts over at a new `T`: it never
//!   writes out a wrong value.
//!
//! **Contents.** Every user document with its stored layout, so versions and expiry survive
//! (`RULE-SAFE-08`), including the tombstones that hold the version floors of deleted documents;
//! and collection metadata. Not the range layer's own records (`_txn`, `_meta`), recent request
//! ids (`$req`, and `"r"` in values) or MVCC history: a backup is one version of each key.
//!
//! **Restore.** Into a cluster holding no user data, through ordinary transactions:
//! - Everything is verified before the first write (a damaged backup changes nothing).
//! - A marker document records which backup is being restored: a restore that failed part way
//!   runs again with the same backup and overwrites what it wrote; any other backup is refused.
//! - Restored values get new commit timestamps. Writes by others during a restore are not
//!   prevented: keep clients away until it reports done.

use std::collections::BTreeMap;

use storage_codec::checksum::checksum_payload;
use storage_codec::value_wire::{encode_map_body, SveReader};
use storage_codec::PropertyValue;
use storage_types::{Result, StorageError};

use crate::dist::hlc::Timestamp;
use crate::dist::mvcc::{fields_size, TXN_COLLECTION};
use crate::dist::server::Key;
use crate::dist::txn::{self, commit_wait, KvSender, Row, Txn, TxnError};
use crate::dist::wire::{put_key, put_ts, put_u32, put_u64};
use crate::dist::zone::collection_span;
use crate::raft::KeySpan;

const MANIFEST_MAGIC: &[u8; 8] = b"STRNBKM1";
const SEGMENT_MAGIC: &[u8; 8] = b"STRNBKS1";

/// A segment is closed once it holds about this many bytes.
const SEGMENT_BYTES: usize = 8 * 1024 * 1024;
/// Keys per request while reading.
const PAGE_KEYS: u32 = 1024;
/// How long a protection lasts unless renewed, and how often it is renewed (nanoseconds).
pub(crate) const PROTECT_TTL_NS: u64 = 120_000_000_000;
pub(crate) const PROTECT_RENEW_NS: u64 = 20_000_000_000;
/// Consecutive failures of one step before giving up (the cluster may be electing).
const STEP_ATTEMPTS: u32 = 64;
/// Fresh timestamps tried when history keeps being trimmed underneath a backup.
const BACKUP_ATTEMPTS: u32 = 4;
/// Rows and bytes per restore transaction.
const RESTORE_BATCH_ROWS: usize = 128;
const RESTORE_BATCH_BYTES: usize = 4 * 1024 * 1024;

const REQUESTS: &str = "$req";
/// Collection of the restore marker.
const RESTORE_COLLECTION: &str = "$restore";
const RESTORE_KEY: &str = "state";

/// Where a backup's bytes go. Each write is durable when it returns; the manifest is written
/// last and atomically.
pub trait BackupStore {
    fn put_segment(&mut self, index: u64, bytes: &[u8]) -> Result<()>;
    fn put_manifest(&mut self, bytes: &[u8]) -> Result<()>;
    fn segment(&self, index: u64) -> Result<Vec<u8>>;
    fn manifest(&self) -> Result<Vec<u8>>;
}

/// What a backup holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    /// The timestamp every row was read at: a changefeed can continue from here.
    pub ts: Timestamp,
    pub rows: u64,
    /// Each segment's length and checksum.
    pub segments: Vec<(u64, u64)>,
}

impl Manifest {
    pub fn encode(&self) -> Vec<u8> {
        let mut o = Vec::with_capacity(64 + self.segments.len() * 16);
        o.extend_from_slice(MANIFEST_MAGIC);
        put_ts(&mut o, self.ts);
        put_u64(&mut o, self.rows);
        put_u32(&mut o, self.segments.len() as u32);
        for (len, sum) in &self.segments {
            put_u64(&mut o, *len);
            put_u64(&mut o, *sum);
        }
        let sum = checksum_payload(&o);
        put_u64(&mut o, sum);
        o
    }

    /// Decodes a manifest from untrusted bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let body = sealed(bytes, MANIFEST_MAGIC, "backup manifest")?;
        let mut r = SveReader::new(body);
        let ts = crate::dist::wire::ts(&mut r)?;
        let rows = r.u64()?;
        let n = r.count(16)?;
        let mut segments = Vec::with_capacity(n);
        for _ in 0..n {
            segments.push((r.u64()?, r.u64()?));
        }
        crate::dist::wire::finish(&r, Self { ts, rows, segments })
    }

    /// Identifies this backup among others.
    pub fn id(&self) -> u64 {
        checksum_payload(&self.encode())
    }
}

/// The body of a sealed file (magic, body, checksum over both), checked.
fn sealed<'a>(bytes: &'a [u8], magic: &[u8; 8], what: &'static str) -> Result<&'a [u8]> {
    if bytes.len() < 16 || &bytes[..8] != magic {
        return Err(StorageError::corrupted_header(what));
    }
    let (head, tail) = bytes.split_at(bytes.len() - 8);
    let mut sum = [0u8; 8];
    sum.copy_from_slice(tail);
    if checksum_payload(head) != u64::from_le_bytes(sum) {
        return Err(StorageError::corrupted_header(what));
    }
    Ok(&head[8..])
}

fn encode_segment(rows: &[u8], count: u32) -> Vec<u8> {
    let mut o = Vec::with_capacity(rows.len() + 24);
    o.extend_from_slice(SEGMENT_MAGIC);
    put_u32(&mut o, count);
    o.extend_from_slice(rows);
    let sum = checksum_payload(&o);
    put_u64(&mut o, sum);
    o
}

/// Rows of a segment (key, stored value), from untrusted bytes.
pub fn decode_segment(bytes: &[u8]) -> Result<Vec<Row>> {
    let body = sealed(bytes, SEGMENT_MAGIC, "backup segment")?;
    let mut r = SveReader::new(body);
    let n = r.count(12)?;
    let mut rows = Vec::with_capacity(n);
    for _ in 0..n {
        let k = crate::dist::wire::key(&mut r)?;
        rows.push((k, r.map_body()?));
    }
    crate::dist::wire::finish(&r, rows)
}

/// Whether `collection` is the range layer's own, or only means something to the running cluster.
pub(crate) fn is_internal(collection: &str) -> bool {
    collection == TXN_COLLECTION || collection == crate::dist::meta::META_COLLECTION || collection == REQUESTS || collection == RESTORE_COLLECTION
}

/// The key space without the records no reader of user data wants (there can be very many
/// transaction records). Rows are still filtered by collection: see [`is_internal`].
pub(crate) fn data_spans() -> Vec<KeySpan> {
    let mut out = Vec::new();
    let mut start = Vec::new();
    for gap in [collection_span(REQUESTS), collection_span(crate::dist::meta::META_COLLECTION), collection_span(TXN_COLLECTION)] {
        out.push(KeySpan::new(start, Some(gap.start)));
        start = gap.end.unwrap_or_default();
    }
    out.push(KeySpan::new(start, None));
    out
}

/// A value without what only the running cluster needs.
pub(crate) fn strip(mut v: BTreeMap<String, PropertyValue>) -> BTreeMap<String, PropertyValue> {
    v.remove("r");
    v
}

/// A random id for a protection.
pub(crate) fn new_protection_id(s: &mut dyn KvSender) -> u64 {
    let id = s.new_id();
    let mut b = [0u8; 8];
    b.copy_from_slice(&id[..8]);
    u64::from_le_bytes(b)
}

fn io_err(e: TxnError) -> StorageError {
    match e {
        TxnError::Unavailable(_) | TxnError::CommitUnknown(_) | TxnError::Retry(_) => StorageError::NotLeader { leader: None },
        TxnError::Invalid(_) | TxnError::HistoryLost(_) => StorageError::invalid_value("the backup could not be read"),
    }
}

/// Runs `f`, retrying what can pass: anything but lost history and invalid requests.
pub(crate) fn step<R>(s: &mut dyn KvSender, mut f: impl FnMut(&mut dyn KvSender) -> std::result::Result<R, TxnError>) -> std::result::Result<R, TxnError> {
    let mut last = TxnError::Unavailable("no attempt".into());
    for _ in 0..STEP_ATTEMPTS {
        match f(s) {
            Ok(r) => return Ok(r),
            Err(e @ (TxnError::HistoryLost(_) | TxnError::Invalid(_))) => return Err(e),
            Err(e) => last = e,
        }
        s.sleep(100_000);
    }
    Err(last)
}

/// Takes a backup into `store` and returns its manifest. Starts over at a newer timestamp if
/// history it needed was trimmed.
pub fn backup(s: &mut dyn KvSender, store: &mut dyn BackupStore) -> Result<Manifest> {
    let id = new_protection_id(s);
    let mut result = Err(Attempt::Lost);
    for _ in 0..BACKUP_ATTEMPTS {
        result = attempt(s, store, id);
        if !matches!(&result, Err(Attempt::Lost)) {
            break;
        }
    }
    s.protect(id, Timestamp::ZERO, 0);
    match result {
        Ok(m) => Ok(m),
        Err(Attempt::Lost) => Err(StorageError::resource_exhausted("the backup kept losing history to trimming: writes outran it; retry when the cluster is quieter")),
        Err(Attempt::Failed(e)) => Err(e),
    }
}

enum Attempt {
    /// History needed was trimmed: a newer timestamp may work.
    Lost,
    Failed(StorageError),
}

impl From<StorageError> for Attempt {
    fn from(e: StorageError) -> Self {
        Self::Failed(e)
    }
}

fn attempt(s: &mut dyn KvSender, store: &mut dyn BackupStore, id: u64) -> std::result::Result<Manifest, Attempt> {
    // See the module docs: no write acknowledged so far can be above this.
    let target = s.now().add_nanos(s.max_offset_ns());
    s.protect(id, target, target.wall.saturating_add(PROTECT_TTL_NS));
    let mut renewed = target.wall;
    commit_wait(s, target);
    let mut t = Txn::begin_as_of(s, target);
    if t.read_ts() != target {
        // The clock never got there (a stopped clock, or a wait cut short).
        return Err(StorageError::NotLeader { leader: None }.into());
    }
    let mut buf = Vec::new();
    let mut in_segment = 0u32;
    let mut manifest = Manifest { ts: target, rows: 0, segments: Vec::new() };
    let flush = |store: &mut dyn BackupStore, buf: &mut Vec<u8>, n: &mut u32, m: &mut Manifest| -> Result<()> {
        let bytes = encode_segment(buf, *n);
        store.put_segment(m.segments.len() as u64, &bytes)?;
        m.segments.push((bytes.len() as u64, checksum_payload(&bytes)));
        buf.clear();
        *n = 0;
        Ok(())
    };
    for span in data_spans() {
        let mut start = span.start.clone();
        loop {
            let page_span = KeySpan::new(start.clone(), span.end.clone());
            let (rows, resume) = match step(s, |s| t.snapshot_page(s, &page_span, PAGE_KEYS)) {
                Ok(p) => p,
                Err(TxnError::HistoryLost(_)) => return Err(Attempt::Lost),
                Err(e) => return Err(io_err(e).into()),
            };
            for (k, _, v) in rows {
                if is_internal(&k.0) {
                    continue;
                }
                put_key(&mut buf, &k);
                encode_map_body(&strip(v), &mut buf);
                in_segment += 1;
                manifest.rows += 1;
                if buf.len() >= SEGMENT_BYTES {
                    flush(store, &mut buf, &mut in_segment, &mut manifest)?;
                }
            }
            let now = s.now().wall;
            if now.saturating_sub(renewed) > PROTECT_RENEW_NS {
                s.protect(id, target, now.saturating_add(PROTECT_TTL_NS));
                renewed = now;
            }
            match resume {
                Some(r) => start = r,
                None => break,
            }
        }
    }
    if in_segment > 0 {
        flush(store, &mut buf, &mut in_segment, &mut manifest)?;
    }
    store.put_manifest(&manifest.encode())?;
    Ok(manifest)
}

/// Reads and checks a whole backup: its manifest, and every segment against it.
pub fn verify(store: &dyn BackupStore) -> Result<Manifest> {
    let m = Manifest::decode(&store.manifest()?)?;
    let mut rows = 0u64;
    for (i, (len, sum)) in m.segments.iter().enumerate() {
        let bytes = store.segment(i as u64)?;
        if bytes.len() as u64 != *len || checksum_payload(&bytes) != *sum {
            return Err(StorageError::corrupted_header("backup segment does not match its manifest"));
        }
        rows += decode_segment(&bytes)?.len() as u64;
    }
    if rows != m.rows {
        return Err(StorageError::corrupted_header("backup row count does not match its manifest"));
    }
    Ok(m)
}

/// The marker's state for backup `id`.
fn marker(id: u64, done: bool) -> BTreeMap<String, PropertyValue> {
    BTreeMap::from([("id".to_string(), PropertyValue::UInt64(id)), ("done".to_string(), PropertyValue::Bool(done))])
}

/// Restores a verified backup into this cluster; returns its manifest.
pub fn restore(s: &mut dyn KvSender, store: &dyn BackupStore) -> Result<Manifest> {
    let m = verify(store)?;
    let id = m.id();
    let marker_key: Key = (RESTORE_COLLECTION.to_string(), RESTORE_KEY.to_string());
    let (admitted, _) = txn::run(s, |t, s| {
        match t.get(s, &marker_key)? {
            Some(state) => {
                if state.get("id") != Some(&PropertyValue::UInt64(id)) {
                    return Ok(Err(StorageError::invalid_value("another backup was restored here (or is being restored): restore into a new cluster")));
                }
                if state.get("done") == Some(&PropertyValue::Bool(true)) {
                    return Ok(Err(StorageError::invalid_value("this backup was already restored here")));
                }
            }
            None => {
                for span in data_spans() {
                    if !t.scan(s, &span, 1)?.is_empty() {
                        return Ok(Err(StorageError::invalid_value("the cluster already holds data: restore into a new cluster")));
                    }
                }
                t.put(marker_key.clone(), marker(id, false));
            }
        }
        Ok(Ok(()))
    })
    .map_err(io_err)?;
    admitted?;
    for i in 0..m.segments.len() {
        let rows = decode_segment(&store.segment(i as u64)?)?;
        let mut batch: Vec<Row> = Vec::new();
        let mut bytes = 0usize;
        for (k, v) in rows {
            bytes = bytes.saturating_add(fields_size(&v));
            batch.push((k, v));
            if batch.len() >= RESTORE_BATCH_ROWS || bytes >= RESTORE_BATCH_BYTES {
                write_batch(s, &mut batch)?;
                bytes = 0;
            }
        }
        write_batch(s, &mut batch)?;
    }
    step(s, |s| {
        txn::run(s, |t, _| {
            t.put(marker_key.clone(), marker(id, true));
            Ok(())
        })
    })
    .map_err(io_err)?;
    Ok(m)
}

/// Writes one batch of restored rows (overwriting is safe: a retried commit writes the same).
fn write_batch(s: &mut dyn KvSender, batch: &mut Vec<Row>) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let rows = std::mem::take(batch);
    step(s, |s| {
        txn::run(s, |t, _| {
            for (k, v) in &rows {
                t.put(k.clone(), v.clone());
            }
            Ok(())
        })
    })
    .map_err(io_err)?;
    Ok(())
}

/// A backup held in memory (tests).
#[derive(Debug, Default, Clone)]
pub struct MemoryStore {
    pub segments: BTreeMap<u64, Vec<u8>>,
    pub manifest: Option<Vec<u8>>,
}

impl BackupStore for MemoryStore {
    fn put_segment(&mut self, index: u64, bytes: &[u8]) -> Result<()> {
        self.segments.insert(index, bytes.to_vec());
        Ok(())
    }

    fn put_manifest(&mut self, bytes: &[u8]) -> Result<()> {
        self.manifest = Some(bytes.to_vec());
        Ok(())
    }

    fn segment(&self, index: u64) -> Result<Vec<u8>> {
        self.segments.get(&index).cloned().ok_or_else(|| StorageError::corrupted_header("backup segment missing"))
    }

    fn manifest(&self) -> Result<Vec<u8>> {
        self.manifest.clone().ok_or_else(|| StorageError::corrupted_header("backup has no manifest"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_rows(rows: &[Row]) -> Vec<u8> {
        let mut buf = Vec::new();
        for (k, v) in rows {
            put_key(&mut buf, k);
            encode_map_body(v, &mut buf);
        }
        encode_segment(&buf, rows.len() as u32)
    }

    fn sample() -> Manifest {
        Manifest { ts: Timestamp::new(1_700_000_000_000_000_000, 3), rows: 2, segments: vec![(100, 7), (200, 9)] }
    }

    #[test]
    fn manifest_round_trips_and_refuses_damage() {
        let m = sample();
        let bytes = m.encode();
        assert_eq!(Manifest::decode(&bytes).unwrap(), m);
        for cut in 0..bytes.len() {
            assert!(Manifest::decode(&bytes[..cut]).is_err(), "cut at {cut}");
        }
        for i in 0..bytes.len() {
            let mut bad = bytes.clone();
            bad[i] ^= 0x41;
            assert!(Manifest::decode(&bad).is_err(), "flip at {i}");
        }
    }

    #[test]
    fn manifest_format_is_pinned() {
        // Golden (`RULE-SAFE-05`): backups outlive binaries.
        let b = sample().encode();
        assert_eq!(&b[..8], b"STRNBKM1");
        assert_eq!(b.len(), 8 + 12 + 8 + 4 + 32 + 8);
        assert_eq!(&b[32..40], &100u64.to_le_bytes());
    }

    #[test]
    fn resealed_hostile_manifest_never_panics() {
        // A huge count with a valid checksum.
        let mut o = Vec::new();
        o.extend_from_slice(MANIFEST_MAGIC);
        put_ts(&mut o, Timestamp::new(1, 0));
        put_u64(&mut o, 0);
        put_u32(&mut o, u32::MAX);
        let sum = checksum_payload(&o);
        put_u64(&mut o, sum);
        assert!(Manifest::decode(&o).is_err());
    }

    #[test]
    fn segments_round_trip_and_refuse_damage() {
        let rows = vec![(("c".to_string(), "k".to_string()), BTreeMap::from([("v".to_string(), PropertyValue::Int64(1))]))];
        let bytes = encode_rows(&rows);
        assert_eq!(decode_segment(&bytes).unwrap(), rows);
        for cut in 0..bytes.len() {
            assert!(decode_segment(&bytes[..cut]).is_err());
        }
        for i in 0..bytes.len() {
            let mut bad = bytes.clone();
            bad[i] ^= 0x41;
            assert!(decode_segment(&bad).is_err());
        }
    }

    #[test]
    fn verify_catches_a_swapped_segment() {
        let rows = |n: i64| vec![(("c".to_string(), format!("k{n}")), BTreeMap::from([("v".to_string(), PropertyValue::Int64(n))]))];
        let (a, b) = (encode_rows(&rows(1)), encode_rows(&rows(2)));
        let m = Manifest { ts: Timestamp::new(1, 0), rows: 2, segments: vec![(a.len() as u64, checksum_payload(&a)), (b.len() as u64, checksum_payload(&b))] };
        let mut store = MemoryStore { segments: BTreeMap::from([(0, a.clone()), (1, b.clone())]), manifest: Some(m.encode()) };
        assert_eq!(verify(&store).unwrap(), m);
        store.segments.insert(0, b);
        store.segments.insert(1, a);
        assert!(verify(&store).is_err());
        store.segments.remove(&1);
        assert!(verify(&store).is_err());
    }

    #[test]
    fn data_spans_skip_the_range_layers_records() {
        let spans = data_spans();
        let has = |id: &[u8]| spans.iter().any(|s| s.contains(id));
        assert!(has(b"$coll/users"));
        assert!(has(b"users/a"));
        assert!(has(b"zzz"));
        assert!(has(b"_tables/a"));
        assert!(!has(b"_txn/abc"));
        assert!(!has(b"_meta/abc"));
        assert!(!has(b"$req/abc"));
    }
}
