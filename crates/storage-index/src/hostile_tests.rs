//! Hostile-input sweeps for every on-disk index format (`RULE-TEST-01`, `RULE-ERR-01`).
//!
//! Each format is written once with valid data, then reopened after every kind of damage:
//! truncation, raw corruption (normally stopped by checksums) and *re-sealed* corruption, where
//! the checksums are recomputed so the structural validators themselves are exercised. Loading
//! and querying must return `Ok` or `Err` — never panic and never over-allocate.

use std::fs;
use std::path::{Path, PathBuf};

use storage_codec::checksum_payload;
use storage_types::testing::SweepRng;
use storage_types::wire::read_u32_le;
use storage_types::{InlineId, RecordOrdinal, Version};

use crate::checkpoint::{
    append_primary_checkpoint_delta, delta_checkpoint_path, load_primary_checkpoint,
    save_primary_checkpoint,
};
use crate::hnsw::{HnswConfig, HnswVectorIndex};
use crate::paged_bitmap::PagedBitmapIndex;
use crate::paged_btree::{page_crc, PagedBTreeIndex, BTREE_PAGE_SIZE};
use crate::primary::{DiskLocation, PrimaryEntry, PrimaryIndex};
use storage_mmap::ArenaLocation;

const ROUNDS: usize = 250;

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("storage_index_{name}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// Writes every damaged variant of `good` to `path` and calls `probe` on it.
fn sweep(path: &Path, good: &[u8], seed: u64, reseal: impl Fn(&mut Vec<u8>), probe: impl Fn(&Path)) {
    let mut rng = SweepRng::new(seed);
    for round in 0..ROUNDS {
        let bytes = match round % 3 {
            _ if round < 25 => good[..good.len() * round / 25].to_vec(),
            0 => rng.hostile_copy(good),
            _ => {
                let mut b = rng.hostile_copy(good);
                reseal(&mut b);
                b
            }
        };
        fs::write(path, &bytes).unwrap();
        probe(path);
    }
}

fn put_u64(buf: &mut [u8], at: usize, v: u64) {
    if let Some(dst) = buf.get_mut(at..at + 8) {
        dst.copy_from_slice(&v.to_le_bytes());
    }
}

#[test]
fn test_hostile_primary_checkpoints_never_panic() {
    let dir = temp_dir("ckpt");
    let path = dir.join("primary.idx");
    let index = PrimaryIndex::new();
    let mut ids = Vec::new();
    for i in 0..40u32 {
        let id = InlineId::new(&format!("rec_{i}")).unwrap();
        let location = match i % 3 {
            0 => DiskLocation::Unflushed,
            1 => DiskLocation::DedicatedFile { parent_group: None, ping_pong_stride: 128 },
            _ => DiskLocation::Arena(ArenaLocation { segment_id: i, offset: i * 64, len: 192 }),
        };
        index.restore_entry(PrimaryEntry {
            id,
            ordinal: RecordOrdinal(0),
            version: Version(u64::from(i) + 1),
            ttl_expires_at_ns: u64::from(i % 2) * 99,
            location,
            disk_verified: false,
            hot_dirty: None,
        });
        ids.push(id);
    }
    save_primary_checkpoint(&index, &path).unwrap();
    let good = fs::read(&path).unwrap();
    // Base checkpoint: body followed by an 8-byte checksum trailer.
    let reseal_base = |b: &mut Vec<u8>| {
        if b.len() >= 8 {
            let body_len = b.len() - 8;
            let sum = checksum_payload(&b[..body_len]);
            put_u64(b, body_len, sum);
        }
    };
    sweep(&path, &good, 1, reseal_base, |p| {
        let _ = load_primary_checkpoint(&PrimaryIndex::new(), p);
    });

    // Delta frames: `magic(8) | len(4) | sum(8) | payload`, appended to `<path>.delta`.
    save_primary_checkpoint(&index, &path).unwrap();
    append_primary_checkpoint_delta(&index, &path, &ids[..10], &ids[30..]).unwrap();
    append_primary_checkpoint_delta(&index, &path, &ids[10..20], &[]).unwrap();
    let delta_path = delta_checkpoint_path(&path);
    let good_delta = fs::read(&delta_path).unwrap();
    let reseal_delta = |b: &mut Vec<u8>| {
        let mut pos = 0usize;
        while pos + 20 <= b.len() {
            let len = read_u32_le(b, pos + 8) as usize;
            let Some(end) = (pos + 20).checked_add(len).filter(|&e| e <= b.len()) else {
                break;
            };
            let sum = checksum_payload(&b[pos + 20..end]);
            put_u64(b, pos + 12, sum);
            pos = end;
        }
    };
    sweep(&delta_path, &good_delta, 2, reseal_delta, |_| {
        let _ = load_primary_checkpoint(&PrimaryIndex::new(), &path);
    });
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_hostile_hnsw_files_never_panic() {
    let dir = temp_dir("hnsw");
    let path = dir.join("hnsw.idx");
    let index = HnswVectorIndex::new(HnswConfig::default());
    for i in 0..120u32 {
        let v: Vec<f32> = (0..16).map(|d| ((i * 7 + d) % 13) as f32 * 0.1).collect();
        index.insert(RecordOrdinal(i), &v);
    }
    index.remove(RecordOrdinal(5));
    index.save_to_disk(&path).unwrap();
    let good = fs::read(&path).unwrap();
    // `magic(4) | checksum(8) | payload`.
    let reseal = |b: &mut Vec<u8>| {
        if b.len() >= 12 {
            let sum = checksum_payload(&b[12..]);
            put_u64(b, 4, sum);
        }
    };
    let query = vec![0.3f32; 16];
    sweep(&path, &good, 3, reseal, |p| {
        if let Ok(loaded) = HnswVectorIndex::load_from_disk(p, HnswConfig::default()) {
            let _ = loaded.search_sq8_candidates(&query, 10, None);
            loaded.insert(RecordOrdinal(500), &query);
            let _ = loaded.search_sq8_candidates(&query, 5, None);
        }
    });
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_hostile_paged_btree_files_never_panic() {
    let dir = temp_dir("btree");
    let path = dir.join("btree.idx");
    {
        let index = PagedBTreeIndex::open(&path).unwrap();
        for i in 0..1_500u32 {
            index.insert(1 + (i % 3) as u16, i64::from(i) * 17 - 9_000, RecordOrdinal(i));
        }
        index.checkpoint_to_disk().unwrap();
    }
    // `path` is the manifest; the pages live in the generation file it names.
    let good_manifest = fs::read(&path).unwrap();
    let data = fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.file_name().is_some_and(|n| n.to_string_lossy().starts_with("btree.idx.")))
        .expect("generation file");
    let good = fs::read(&data).unwrap();
    // File header checksum at `[8..16]` over `[0..8]`; every leaf page carries `page_crc`.
    let reseal = |b: &mut Vec<u8>| {
        if b.len() >= 16 {
            let sum = checksum_payload(&b[0..8]);
            put_u64(b, 8, sum);
        }
        let mut off = BTREE_PAGE_SIZE;
        while off + BTREE_PAGE_SIZE <= b.len() {
            let crc = page_crc(&b[off..off + BTREE_PAGE_SIZE]);
            put_u64(b, off + 24, crc);
            off += BTREE_PAGE_SIZE;
        }
    };
    let rounds = std::cell::Cell::new(0u32);
    let probe = |_: &Path| {
        // A previous probe's checkpoint advanced the generation: point back at the swept file.
        fs::write(&path, &good_manifest).unwrap();
        if let Ok(index) = PagedBTreeIndex::open(&path) {
            for field in 0..4u16 {
                let _ = index.range_bitmap(field, ..).len();
                let _ = index.range_bitmap(field, -100..=5_000).len();
                let _ = index.count_in_range_zero_alloc(field, i64::MIN, i64::MAX);
                let _ = index.range_collect_paginated(field, -500, 500, crate::btree::SortOrder::Desc, None, 3, Some(9), |_| true);
            }
            index.insert(1, 7, RecordOrdinal(9_999));
            if rounds.get() % 8 == 0 {
                let _ = index.checkpoint_to_disk();
            }
            rounds.set(rounds.get() + 1);
        }
    };
    sweep(&data, &good, 4, reseal, probe);
    // The manifest itself (32 bytes, checksummed).
    sweep(&path, &good_manifest, 6, |_| {}, |p| {
        let _ = PagedBTreeIndex::open(p).map(|i| i.count_in_range(1, ..));
    });
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_hostile_paged_bitmap_files_never_panic() {
    let dir = temp_dir("bitmap");
    let path = dir.join("bitmap.idx");
    let tags: Vec<InlineId> = (0..6).map(|i| InlineId::new(&format!("tag_{i}")).unwrap()).collect();
    {
        let index = PagedBitmapIndex::open(&path).unwrap();
        for i in 0..20_000u32 {
            index.insert(1, tags[(i % 6) as usize], RecordOrdinal(i * 3));
        }
        index.checkpoint_to_disk().unwrap();
    }
    let good_manifest = fs::read(&path).unwrap();
    let data = fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.file_name().is_some_and(|n| n.to_string_lossy().starts_with("bitmap.idx.")))
        .expect("generation file");
    let good = fs::read(&data).unwrap();
    // v3 header: `magic | blocks @4 | dir_offset @8 | dir_len @16 | dir_crc @24 | blocks_crc @32
    // | header_crc @40 (over [0..40])`; blocks start at 4096.
    let reseal = |b: &mut Vec<u8>| {
        if b.len() < 48 {
            return;
        }
        let blocks = read_u32_le(b, 4) as usize;
        let dir_offset = storage_types::wire::read_u64_le(b, 8) as usize;
        let dir_len = read_u32_le(b, 16) as usize;
        if let Some(dir) = b.get(dir_offset..dir_offset.saturating_add(dir_len)) {
            let sum = checksum_payload(dir);
            put_u64(b, 24, sum);
        }
        if let Some(region) = b.get(4096..4096usize.saturating_add(blocks.saturating_mul(512))) {
            let sum = checksum_payload(region);
            put_u64(b, 32, sum);
        }
        let sum = checksum_payload(&b[0..40]);
        put_u64(b, 40, sum);
    };
    let rounds = std::cell::Cell::new(0u32);
    let probe = |_: &Path| {
        fs::write(&path, &good_manifest).unwrap();
        if let Ok(index) = PagedBitmapIndex::open(&path) {
            for &tag in &tags {
                let _ = index.get_bitmap(1, tag).len();
                let _ = index.cardinality(1, tag);
            }
            let _ = index.intersect_all(&[(1, tags[0]), (1, tags[3])]).len();
            let _ = index.union_tag_prefix(1, "tag").len();
            index.insert(1, tags[1], RecordOrdinal(1));
            if rounds.get() % 8 == 0 {
                let _ = index.checkpoint_to_disk();
            }
            rounds.set(rounds.get() + 1);
        }
    };
    sweep(&data, &good, 5, reseal, probe);
    sweep(&path, &good_manifest, 7, |_| {}, |p| {
        let _ = PagedBitmapIndex::open(p).map(|i| i.mapped_tag_count());
    });
    let _ = fs::remove_dir_all(&dir);
}
