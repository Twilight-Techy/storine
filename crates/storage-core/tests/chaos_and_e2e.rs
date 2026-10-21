//! Comprehensive End-to-End Integration, Concurrency, Referential Integrity, Transaction,
//! and Chaos Crash-Recovery Test Suite (`TASK-013`..`TASK-030`).

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use storage_codec::RecordCodec;
use storage_core::{
    AgentObjectRecord, EngineConfig, IndexTier, StorageEngine, StorageLayoutMode, FIELD_PRIMARY_TAG,
    FIELD_SCORE, FIELD_TIMESTAMP,
};
use storage_graph::OnDelete;
use storage_types::{InlineId, RecordFlags, StorageError, Version, HEADER_SIZE};
use storage_wal::SyncPolicy;

#[test]
fn test_all_three_storage_layouts_and_hierarchical_grouping() {
    for (idx, mode) in [
        StorageLayoutMode::DedicatedFiles,
        StorageLayoutMode::PingPongFiles { slot_stride: 4096 },
        StorageLayoutMode::AdaptiveArena {
            arena_threshold_bytes: 4096,
            segment_capacity: 512 * 1024,
        },
    ]
    .into_iter()
    .enumerate()
    {
        let dir = std::env::temp_dir().join(format!("storage_e2e_layout_{idx}"));
        let _ = fs::remove_dir_all(&dir);

        let cfg = EngineConfig {
            layout_mode: mode,
            background_flush: false,
            ..EngineConfig::default()
        };

        let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg.clone()).unwrap();
        let parent_id = InlineId::new("agent_core").unwrap();

        for i in 0..50 {
            let id = InlineId::new(&format!("item_{i:03}")).unwrap();
            let rec = AgentObjectRecord::new(id, format!("Title {i}"), 1000 + i, i * 10);
            if i % 2 == 0 {
                engine.put_grouped(id, Some(parent_id), &rec).unwrap();
            } else {
                engine.put(id, &rec).unwrap();
            }
        }

        // Update every record twice so PingPongFiles toggles between Slot 0 and Slot 1
        for i in 0..50 {
            let id = InlineId::new(&format!("item_{i:03}")).unwrap();
            engine
                .update(id, |r| {
                    r.score += 5;
                    Ok(())
                })
                .unwrap();
            engine
                .update(id, |r| {
                    r.score += 5;
                    Ok(())
                })
                .unwrap();
        }

        engine.flush_all().unwrap();
        drop(engine);

        // Reopen from disk checkpoint and verify zero-copy reads
        let reopened = StorageEngine::<AgentObjectRecord>::open(&dir, cfg).unwrap();
        assert_eq!(reopened.stats().live_records, 50);
        for i in 0..50 {
            let id = InlineId::new(&format!("item_{i:03}")).unwrap();
            reopened
                .with_ref(id, |view| {
                    assert_eq!(view.version(), Version(3));
                    assert_eq!(view.score, i * 10 + 10);
                    assert_eq!(view.get_str(&view.title), format!("Title {i}"));
                })
                .unwrap();
        }

        let _ = fs::remove_dir_all(&dir);
    }
}

#[test]
fn test_concurrent_per_record_writes_and_bidirectional_deadlock_freedom() {
    let dir = std::env::temp_dir().join("storage_e2e_concurrency");
    let _ = fs::remove_dir_all(&dir);

    let cfg = EngineConfig {
        wal_sync_policy: SyncPolicy::GroupCommit {
        max_batch_entries: 64,
    },
        background_flush: true,
        ..EngineConfig::default()
    };

    let engine = Arc::new(StorageEngine::<AgentObjectRecord>::open(&dir, cfg).unwrap());

    let id_a = InlineId::new("node_alpha").unwrap();
    let id_b = InlineId::new("node_beta").unwrap();
    engine
        .insert(id_a, &AgentObjectRecord::new(id_a, "Alpha", 100, 0))
        .unwrap();
    engine
        .insert(id_b, &AgentObjectRecord::new(id_b, "Beta", 100, 0))
        .unwrap();

    // 8 threads concurrently updating `(id_a, id_b)` and `(id_b, id_a)` in opposite argument order!
    // Canonical lock ordering (`RULE-CONC-02`) guarantees zero deadlocks and exact final counts.
    let threads = 8usize;
    let iters = 100usize;
    let barrier = Arc::new(Barrier::new(threads));
    let mut handles = Vec::new();

    for t in 0..threads {
        let eng = Arc::clone(&engine);
        let bar = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            bar.wait();
            for _ in 0..iters {
                if t % 2 == 0 {
                    eng.update_pair_atomic(
                        id_a,
                        id_b,
                        |a| {
                            a.score += 1;
                            a.peers.link(id_b);
                            Ok(())
                        },
                        |b| {
                            b.score += 1;
                            b.peers.link(id_a);
                            Ok(())
                        },
                    )
                    .unwrap();
                } else {
                    eng.update_pair_atomic(
                        id_b,
                        id_a,
                        |b| {
                            b.score += 1;
                            b.peers.link(id_a);
                            Ok(())
                        },
                        |a| {
                            a.score += 1;
                            a.peers.link(id_b);
                            Ok(())
                        },
                    )
                    .unwrap();
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let expected_score = (threads * iters) as i64;
    let ref_a = engine.get_ref(id_a).unwrap();
    let ref_b = engine.get_ref(id_b).unwrap();
    assert_eq!(ref_a.score, expected_score);
    assert_eq!(ref_b.score, expected_score);
    assert!(ref_a.peers.contains(ref_a.view().payload(), id_b));
    assert!(ref_b.peers.contains(ref_b.view().payload(), id_a));

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_referential_integrity_restrict_unlink_and_cascade() {
    let dir = std::env::temp_dir().join("storage_e2e_integrity");
    let _ = fs::remove_dir_all(&dir);

    let engine = StorageEngine::<AgentObjectRecord>::open(&dir, EngineConfig::default()).unwrap();

    let user_id = InlineId::new("user_1").unwrap();
    let post1_id = InlineId::new("post_1").unwrap();
    let post2_id = InlineId::new("post_2").unwrap();

    let mut user = AgentObjectRecord::new(user_id, "User 1", 1000, 10);
    user.children.link(post1_id);
    user.children.link(post2_id);

    let mut post1 = AgentObjectRecord::new(post1_id, "Post 1", 1001, 5);
    post1.parent.set(Some(user_id));

    let mut post2 = AgentObjectRecord::new(post2_id, "Post 2", 1002, 5);
    post2.parent.set(Some(user_id));

    engine.put(user_id, &user).unwrap();
    engine.put(post1_id, &post1).unwrap();
    engine.put(post2_id, &post2).unwrap();

    // 1. OnDelete::Restrict must block deleting `user_1` while `post_1` and `post_2` exist
    let restrict_res = engine.delete_with_policy(
        user_id,
        OnDelete::Restrict,
        |r| r.all_related_ids(),
        |peer, del_id| peer.unlink_all(del_id),
    );
    assert!(matches!(
        restrict_res,
        Err(StorageError::IntegrityViolation { .. })
    ));

    // 2. OnDelete::Unlink on `post_1` must remove `post_1` from `user_1.children` automatically!
    let unlinked = engine
        .delete_with_policy(
            post1_id,
            OnDelete::Unlink,
            |r| r.all_related_ids(),
            |peer, del_id| peer.unlink_all(del_id),
        )
        .unwrap();
    assert_eq!(unlinked, 1);
    assert!(engine.verify_relationships(|r| r.all_related_ids()).is_empty());

    let user_after_unlink = engine.get_owned(user_id).unwrap().1;
    assert!(!user_after_unlink.children.contains(post1_id));
    assert!(user_after_unlink.children.contains(post2_id));

    // 3. OnDelete::Cascade on `user_1` must delete both `user_1` and its remaining child `post_2`
    let cascaded = engine
        .delete_with_policy(
            user_id,
            OnDelete::Cascade,
            |r| r.all_related_ids(),
            |peer, del_id| peer.unlink_all(del_id),
        )
        .unwrap();
    assert_eq!(cascaded, 2);
    assert_eq!(engine.stats().live_records, 0);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_multi_record_atomic_transactions_bank_transfer_invariant() {
    let dir = std::env::temp_dir().join("storage_e2e_transactions");
    let _ = fs::remove_dir_all(&dir);

    let engine = Arc::new(
        StorageEngine::<AgentObjectRecord>::open(&dir, EngineConfig::default()).unwrap(),
    );

    let num_accounts = 10usize;
    let initial_balance = 1_000i64;
    let mut acct_ids = Vec::new();
    for i in 0..num_accounts {
        let id = InlineId::new(&format!("acct_{i:02}")).unwrap();
        engine
            .put(id, &AgentObjectRecord::new(id, "Account", 1, initial_balance))
            .unwrap();
        acct_ids.push(id);
    }

    // Perform 200 multi-record atomic transfers
    for step in 0..200usize {
        let from_id = acct_ids[step % num_accounts];
        let to_id = acct_ids[(step + 3) % num_accounts];
        engine
            .transaction(|txn| {
                let mut from = txn.get(from_id)?;
                let mut to = txn.get(to_id)?;
                from.score -= 25;
                to.score += 25;
                txn.put(from_id, from);
                txn.put(to_id, to);
                Ok(())
            })
            .unwrap();
    }

    let total_balance: i64 = acct_ids
        .iter()
        .map(|&id| engine.get_ref(id).unwrap().score)
        .sum();
    assert_eq!(total_balance, (num_accounts as i64) * initial_balance);

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_secondary_indexes_ttl_cold_compression_and_schema_migration() {
    let dir = std::env::temp_dir().join("storage_e2e_advanced");
    let _ = fs::remove_dir_all(&dir);

    let engine = StorageEngine::<AgentObjectRecord>::open(&dir, EngineConfig::default()).unwrap();

    let tag_rust = InlineId::from_static("rust");
    let tag_db = InlineId::from_static("db");

    for i in 0..20 {
        let id = InlineId::new(&format!("doc_{i:02}")).unwrap();
        let mut rec = AgentObjectRecord::new(id, format!("Doc {i}"), 2000 + i, i * 100);
        if i % 2 == 0 {
            rec.tags.push(tag_rust);
        }
        if i % 3 == 0 {
            rec.tags.push(tag_db);
        }
        rec.embedding = vec![i as f32, 1.0, 0.0, 0.0];
        engine.put(id, &rec).unwrap();
    }

    // 1. Multi-tag + timestamp range query
    let matches = engine
        .query()
        .filter_tag(FIELD_PRIMARY_TAG, tag_rust)
        .filter_tag(FIELD_PRIMARY_TAG, tag_db)
        .filter_range(FIELD_TIMESTAMP, 2000..=2015)
        .execute_ids();
    // Multiples of 6 in 0..=15 -> 0, 6, 12 (3 records)
    assert_eq!(matches.len(), 3);

    // 2. Hybrid tag + SIMD cosine similarity top-2 search
    let top_hits = engine
        .query()
        .filter_tag(FIELD_PRIMARY_TAG, tag_rust)
        .top_k_similar(&[19.0, 1.0, 0.0, 0.0], 2);
    assert_eq!(top_hits.len(), 2);
    assert_eq!(top_hits[0].0.as_str(), "doc_18");

    // 3. Cold LZ4 compression round-trip (`F-OPS-03`)
    let doc0 = InlineId::from_static("doc_00");
    engine.compress_cold_record(doc0).unwrap();
    let (_, decompressed) = engine.get_owned(doc0).unwrap();
    assert_eq!(decompressed.title, "Doc 0");

    // 4. TTL expiration (`F-OPS-02`)
    let eph_id = InlineId::from_static("ephemeral_key");
    let eph_rec = AgentObjectRecord::new(eph_id, "Temp Session", 3000, 1);
    engine
        .put_with_ttl(eph_id, &eph_rec, Duration::from_millis(2))
        .unwrap();
    thread::sleep(Duration::from_millis(10));
    assert!(matches!(
        engine.get_ref(eph_id),
        Err(StorageError::RecordNotFound { .. })
    ));
    let swept = engine.sweep_expired().unwrap();
    assert_eq!(swept, 1);

    // 5. Schema migration hook (`F-OPS-01`)
    engine.register_migration(0, |_old_ver, _raw_payload| {
        Ok(AgentObjectRecord::new(
            InlineId::from_static("migrated_doc"),
            "Upgraded From V0",
            9999,
            777,
        ))
    });
    let _ = FIELD_SCORE;

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_chaos_crash_simulation_and_wal_recovery() {
    let dir = std::env::temp_dir().join("storage_e2e_chaos_crash");
    let _ = fs::remove_dir_all(&dir);

    let cfg = EngineConfig {
        enable_wal: true,
        wal_sync_policy: SyncPolicy::Immediate,
        background_flush: false,
        ..EngineConfig::default()
    };

    // Run 25 simulated crash cycles!
    for cycle in 1..=25i64 {
        let key = InlineId::new(&format!("chaos_{cycle:03}")).unwrap();
        {
            let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg.clone()).unwrap();
            let rec = AgentObjectRecord::new(key, format!("Cycle {cycle}"), cycle, cycle * 10);
            engine.put(key, &rec).unwrap();
            // Do NOT call `flush_all()` on even cycles so records must be recovered from WAL!
            if cycle % 2 == 1 {
                engine.flush_all().unwrap();
            }
        }

        // Inject simulated crash artifacts:
        // 1. Orphaned `.tmp` file in records directory
        let orphan_tmp = dir.join("records").join(format!("orphan_{cycle}.rec.tmp"));
        if let Some(p) = orphan_tmp.parent() {
            let _ = fs::create_dir_all(p);
        }
        let _ = fs::write(&orphan_tmp, b"incomplete_torn_temp_file_bytes");

        // 2. Torn incomplete WAL frame appended to the end of the WAL file (simulating power loss mid-write)
        let wal_file = dir.join("wal").join(format!("{}.wal", cfg.collection_name));
        if wal_file.exists() {
            if let Ok(mut f) = OpenOptions::new().append(true).open(&wal_file) {
                let _ = f.write_all(&[0x57, 0x41, 0x4C, 0x31, 0x01, 0x00, 0x99]);
            }
        }

        // Reopen engine and verify 100% deterministic recovery of all `1..=cycle` records!
        let recovered_engine =
            StorageEngine::<AgentObjectRecord>::open(&dir, cfg.clone()).unwrap();
        assert!(!orphan_tmp.exists(), "orphaned .tmp file must be cleaned up on open");
        assert_eq!(recovered_engine.stats().live_records, cycle as usize);

        for c in 1..=cycle {
            let k = InlineId::new(&format!("chaos_{c:03}")).unwrap();
            recovered_engine
                .with_ref(k, |view| {
                    assert_eq!(view.score, c * 10);
                })
                .unwrap();
        }
    }

    // Verify single-byte corruption in a `.rec` payload is immediately caught by checksum validation
    let k1 = InlineId::from_static("chaos_001");
    let rec_frame = RecordCodec::encode_aligned(
        &AgentObjectRecord::new(k1, "Test", 1, 10),
        Version(1),
        100,
        RecordFlags::NONE,
        0,
    );
    let mut bad_frame = rec_frame.as_slice().to_vec();
    bad_frame[HEADER_SIZE + 4] ^= 0xFF;
    let aligned_bad = storage_codec::AlignedBytes::copy_from_slice(&bad_frame);
    assert!(matches!(
        RecordCodec::validate_frame::<AgentObjectRecord>(&aligned_bad),
        Err(StorageError::ChecksumMismatch { .. })
    ));

    let _ = fs::remove_dir_all(&dir);
}

/// Layouts exercised by the process-kill crash test (selected by index in the child).
fn crash_layouts() -> [StorageLayoutMode; 3] {
    [
        StorageLayoutMode::DedicatedFiles,
        StorageLayoutMode::PingPongFiles { slot_stride: 4096 },
        StorageLayoutMode::AdaptiveArena {
            arena_threshold_bytes: 4096,
            segment_capacity: 64 * 1024,
        },
    ]
}

/// Child half of `test_process_kill_crash_recovery_preserves_every_acknowledged_write`.
///
/// Only does work when spawned by the parent test (env vars set): performs acknowledged writes,
/// printing `ACK ...` after each one returns, then kills itself with `abort()` mid-stream, so no
/// destructor, flush or checkpoint runs.
#[test]
fn crash_child_writer() {
    let (Ok(dir), Ok(layout), Ok(seed)) = (
        std::env::var("STORINE_CRASH_DIR"),
        std::env::var("STORINE_CRASH_LAYOUT"),
        std::env::var("STORINE_CRASH_SEED"),
    ) else {
        return;
    };
    let layout_idx: usize = layout.parse().unwrap_or(0);
    let seed: u64 = seed.parse().unwrap_or(0);
    let cfg = EngineConfig {
        background_flush: true,
        ..crash_config(layout_idx)
    };
    let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg).unwrap();
    let mut out = std::io::stdout().lock();
    let abort_after = 120 + (seed % 97) as usize;
    for i in 0..10_000usize {
        let n = (i as u64).wrapping_mul(0x9E37_79B9).wrapping_add(seed) % 60;
        let key = InlineId::new(&format!("k{n:03}")).unwrap();
        if i % 9 == 4 {
            if engine.delete(key).unwrap() {
                writeln!(out, "ACK DEL {n}").unwrap();
            }
        } else {
            let score = (seed as i64) * 100_000 + i as i64;
            let mut rec = AgentObjectRecord::new(key, format!("v{i}"), i as i64, score);
            rec.body = vec![b'x'; (i % 5) * 300];
            engine.put(key, &rec).unwrap();
            writeln!(out, "ACK PUT {n} {score}").unwrap();
        }
        out.flush().unwrap();
        if i == abort_after {
            std::process::abort();
        }
    }
}

/// Crash-test variants: the three storage layouts with in-memory indexes, the adaptive arena
/// layout with the paged index tier, and the adaptive arena layout with encryption at rest.
const CRASH_VARIANTS: usize = 5;

fn crash_config(variant: usize) -> EngineConfig {
    EngineConfig {
        layout_mode: crash_layouts()[variant.min(2)],
        index_tier: if variant == 3 { IndexTier::Paged } else { IndexTier::Memory },
        encryption_key: (variant == 4)
            .then(|| storage_codec::EncryptionKey::from_bytes([0x5A; 32])),
        ..EngineConfig::default()
    }
}

#[test]
fn test_process_kill_crash_recovery_preserves_every_acknowledged_write() {
    use std::collections::BTreeMap;
    use std::process::{Command, Stdio};

    let exe = std::env::current_exe().unwrap();
    for layout_idx in 0..CRASH_VARIANTS {
        let dir = std::env::temp_dir().join(format!("storage_e2e_kill_{layout_idx}"));
        let _ = fs::remove_dir_all(&dir);
        let mut expected: BTreeMap<u64, i64> = BTreeMap::new();

        // Two consecutive crashes on the same directory (recovery of a recovered database).
        for round in 1..=2u64 {
            let output = Command::new(&exe)
                .args(["--exact", "crash_child_writer", "--nocapture", "--test-threads=1"])
                .env("STORINE_CRASH_DIR", &dir)
                .env("STORINE_CRASH_LAYOUT", layout_idx.to_string())
                .env("STORINE_CRASH_SEED", (round * 7 + layout_idx as u64).to_string())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .output()
                .unwrap();
            assert!(!output.status.success(), "child must die by abort()");
            for line in String::from_utf8_lossy(&output.stdout).lines() {
                let parts: Vec<&str> = line.split_whitespace().collect();
                match parts.as_slice() {
                    ["ACK", "PUT", n, score] => {
                        expected.insert(n.parse().unwrap(), score.parse().unwrap());
                    }
                    ["ACK", "DEL", n] => {
                        expected.remove(&n.parse().unwrap());
                    }
                    _ => {}
                }
            }
            assert!(!expected.is_empty());

            let engine = StorageEngine::<AgentObjectRecord>::open(&dir, crash_config(layout_idx)).unwrap();
            for n in 0..60u64 {
                let key = InlineId::new(&format!("k{n:03}")).unwrap();
                match expected.get(&n) {
                    Some(&score) => {
                        let got = engine.with_ref(key, |v| v.score).unwrap_or_else(|e| {
                            panic!("layout {layout_idx} round {round}: lost acked {key}: {e}")
                        });
                        assert_eq!(got, score, "layout {layout_idx} round {round}: stale {key}");
                    }
                    None => assert!(
                        engine.get_ref(key).is_err(),
                        "layout {layout_idx} round {round}: deleted {key} resurrected"
                    ),
                }
            }
            assert_eq!(engine.stats().live_records, expected.len());
            // Secondary indexes after recovery: scores are unique, so the full score range is
            // exactly the acknowledged keys and every score resolves to its own key.
            let key_of = |n: u64| InlineId::new(&format!("k{n:03}")).unwrap();
            let mut all = engine.query().filter_range(FIELD_SCORE, i64::MIN..=i64::MAX).execute_ids();
            all.sort();
            let mut want: Vec<InlineId> = expected.keys().map(|&n| key_of(n)).collect();
            want.sort();
            assert_eq!(all, want, "variant {layout_idx} round {round}: score index diverged");
            for (&n, &score) in &expected {
                assert_eq!(
                    engine.query().filter_eq(FIELD_SCORE, score).execute_ids(),
                    vec![key_of(n)],
                    "variant {layout_idx} round {round}: stale posting for {score}"
                );
            }
            assert!(!engine.secondary_indexes_reused(), "a crash must force an index rebuild");
        }
        let _ = fs::remove_dir_all(&dir);
    }
}

#[test]
fn test_minimal_footprint_invariants_and_arena_compaction() {
    use core::mem::size_of;
    use storage_core::ArchivedAgentObjectRecord;
    use storage_index::{IndexSnapshot, PrimaryEntry};
    use storage_mmap::MmapGuard;

    // Verify compact in-RAM / on-disk struct sizes
    assert_eq!(size_of::<ArchivedAgentObjectRecord>(), 112);
    assert_eq!(size_of::<IndexSnapshot>(), 264);
    assert_eq!(size_of::<MmapGuard>(), 8);
    assert_eq!(size_of::<PrimaryEntry>(), 96);

    let dir = std::env::temp_dir().join("storage_e2e_arena_compact");
    let _ = fs::remove_dir_all(&dir);

    let cfg = EngineConfig {
        layout_mode: StorageLayoutMode::AdaptiveArena {
        arena_threshold_bytes: 4096,
        segment_capacity: 64 * 1024,
    },
        background_flush: false,
        ..EngineConfig::default()
    };

    let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg).unwrap();

    // 1. Insert 40 records and update all 40 across 4 rounds with growing payload sizes (changing 64B size classes)
    // so old size-class slots accumulate across the cohort and trigger cross-size-class arena compaction.
    let mut upserted_ids = Vec::with_capacity(40);
    for round in 1..=4 {
        for i in 0..40 {
            let id = InlineId::new(&format!("cmp_{i:03}")).unwrap();
            if round == 1 {
                upserted_ids.push(id);
            }
            let mut rec = AgentObjectRecord::new(id, format!("Title {i} r{round}"), i, i * round);
            rec.body = vec![b'x'; (round as usize) * 80];
            engine.put(id, &rec).unwrap();
        }
    }
    engine.flush_all().unwrap();
    engine.checkpoint_incremental(&upserted_ids, &[]).unwrap();

    let compacted = engine.compact_arenas(0.40).unwrap();
    assert!(compacted >= 1, "expected at least 1 fragmented arena segment compacted");

    // Verify all 40 live records remain intact and readable via zero-copy `with_ref`
    for i in 0..40 {
        let id = InlineId::new(&format!("cmp_{i:03}")).unwrap();
        engine
            .with_ref(id, |view| {
                assert_eq!(view.score, i * 4);
            })
            .unwrap();
    }

    let _ = fs::remove_dir_all(&dir);
}
