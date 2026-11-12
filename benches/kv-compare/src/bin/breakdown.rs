//! Where a Storine `put` spends its time: each step timed alone over the same records.
//!
//! ```text
//! cargo run --release --no-default-features --bin breakdown -- <dir> [records]
//! ```

use std::time::Instant;

use storage_codec::{ArchiveBuilder, ArchivedBytes, RecordCodec, ZeroCopyRecord};
use storage_core::{EngineConfig, StorageEngine, StorageLayoutMode};
use storage_index::{DiskLocation, PrimaryEntry, PrimaryIndex};
use storage_types::{InlineId, RecordFlags, Result as SResult, Version};
use storage_wal::{GroupCommitWal, SyncPolicy, WalOpKind};

#[derive(Clone, Debug)]
struct KvRecord {
    id: InlineId,
    field: u64,
    value: Vec<u8>,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct ArchivedKvRecord {
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
    fn extract_archived_snapshot(_: &ArchivedKvRecord, _: &[u8]) -> Option<storage_index::IndexSnapshot> {
        Some(storage_index::IndexSnapshot::new())
    }
}

fn per_op(label: &str, n: usize, mut f: impl FnMut(usize)) {
    for i in 0..n.min(10_000) {
        f(i);
    }
    let t = Instant::now();
    for i in 0..n {
        f(i);
    }
    println!("{label:<34} {:>8.0} ns/op", t.elapsed().as_nanos() as f64 / n as f64);
}

fn main() {
    let mut args = std::env::args().skip(1);
    let root = std::path::PathBuf::from(args.next().unwrap_or_else(|| "/tmp/kvbd".into()));
    let n: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(1_000_000);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let ids: Vec<InlineId> = (0..n).map(|i| InlineId::new(&format!("k{i:015}")).unwrap()).collect();
    let recs: Vec<KvRecord> = (0..n).map(|i| KvRecord { id: ids[i], field: i as u64, value: vec![(i % 251) as u8; 180] }).collect();
    let mut rng = 0x9E37_79B9_7F4A_7C15u64;
    let mut pick = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        (rng % n as u64) as usize
    };
    let order: Vec<usize> = (0..n).map(|_| pick()).collect();

    per_op("encode_aligned", n, |i| {
        std::hint::black_box(RecordCodec::encode_aligned(&recs[order[i]], Version(2), 0, RecordFlags::NONE, 0));
    });
    let frame = RecordCodec::encode_aligned(&recs[0], Version(1), 0, RecordFlags::NONE, 0);
    println!("frame bytes {}", frame.as_slice().len());
    per_op("AlignedBytes copy + drop", n, |_| {
        std::hint::black_box(storage_codec::AlignedBytes::copy_from_slice(std::hint::black_box(frame.as_slice())));
    });
    per_op("Vec<u8> copy + drop", n, |_| {
        std::hint::black_box(std::hint::black_box(frame.as_slice()).to_vec());
    });
    let mut keep: Vec<storage_codec::AlignedBytes> = Vec::with_capacity(n);
    per_op("AlignedBytes copy, kept (heap grows)", n, |_| {
        keep.push(storage_codec::AlignedBytes::copy_from_slice(frame.as_slice()));
    });
    drop(keep);
    per_op("checksum_payload(frame)", n, |_| {
        std::hint::black_box(storage_codec::checksum_payload(std::hint::black_box(frame.as_slice())));
    });
    let wal = GroupCommitWal::open(root.join("w.wal"), SyncPolicy::AsyncMemoryFirst).unwrap();
    per_op("wal append (memory-first)", n, |i| {
        wal.append_single(1, WalOpKind::Upsert, ids[order[i]], Version(i as u64 + 1), frame.as_slice()).unwrap();
    });
    drop(wal);
    let primary = PrimaryIndex::new();
    for (i, id) in ids.iter().enumerate() {
        let ord = primary.allocate_ordinal(*id);
        primary.upsert(PrimaryEntry { id: *id, ordinal: ord, version: Version(1), ttl_expires_at_ns: 0, location: DiskLocation::Unflushed, disk_verified: false, hot_dirty: None });
        let _ = i;
    }
    per_op("primary get", n, |i| {
        std::hint::black_box(primary.get(ids[order[i]]));
    });
    per_op("primary get + upsert (w/ frame)", n, |i| {
        let id = ids[order[i]];
        let e = primary.get(id).unwrap();
        primary.upsert(PrimaryEntry { version: Version(e.version.0 + 1), hot_dirty: Some(frame.clone()), ..e });
    });
    let cfg = EngineConfig {
        layout_mode: StorageLayoutMode::AdaptiveArena { arena_threshold_bytes: 64 * 1024, segment_capacity: 64 << 20 },
        wal_sync_policy: SyncPolicy::AsyncMemoryFirst,
        ..EngineConfig::default()
    };
    let engine = StorageEngine::<KvRecord>::open(root.join("e"), cfg).unwrap();
    for i in 0..n {
        engine.put(ids[i], &recs[i]).unwrap();
    }
    engine.flush_all().unwrap();
    per_op("engine put (overwrite)", n, |i| {
        let k = order[i];
        engine.put(ids[k], &recs[k]).unwrap();
    });
    drop(engine);
    let _ = std::fs::remove_dir_all(&root);
}
