//! `PrimaryTier::Cold` (`TASK-092`, `F-PIDX-03`): the primary index answers exactly like the
//! memory tier through writes, deletes, checkpoints, restarts and tier switches, while RAM holds
//! only what changed since the last checkpoint.

use std::fs;
use std::path::PathBuf;

use storage_codec::EncryptionKey;
use storage_core::{
    AgentObjectRecord, EncryptionConfig, EngineConfig, IndexTier, PrimaryTier, StorageEngine, StorageLayoutMode,
    FIELD_PRIMARY_TAG, FIELD_SCORE,
};
use storage_types::testing::SweepRng;
use storage_types::InlineId;

const N: u32 = 3_000;

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("storage_cold_tier_{name}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn cfg(primary_tier: PrimaryTier, index_tier: IndexTier, encrypted: bool) -> EngineConfig {
    EngineConfig {
        layout_mode: StorageLayoutMode::AdaptiveArena { arena_threshold_bytes: 4096, segment_capacity: 1 << 20 },
        primary_tier,
        index_tier,
        encryption: encrypted.then(|| EncryptionConfig::strict(1, EncryptionKey::from_bytes([0x11; 32]))),
        ..EngineConfig::default()
    }
}

fn id(i: u32) -> InlineId {
    InlineId::new(&format!("rec{i:05}")).unwrap()
}

fn record(i: u32, v: i64) -> AgentObjectRecord {
    let mut r = AgentObjectRecord::new(id(i), "t", 1_000 + i64::from(i), v);
    r.tags = vec![InlineId::new(["red", "green", "blue"][(i % 3) as usize]).unwrap()];
    r.embedding = vec![(i % 17) as f32, 1.0, (v % 5) as f32];
    r
}

/// Everything a client can observe, for comparing two engines.
fn observe(e: &StorageEngine<AgentObjectRecord>) -> (usize, Vec<Option<i64>>, Vec<InlineId>, Vec<InlineId>, usize) {
    let scores = (0..N + 200).map(|i| e.get_owned(id(i)).ok().map(|(_, r)| r.score)).collect();
    let mut range = e.query().filter_range(FIELD_SCORE, 100..=900).execute_ids();
    range.sort();
    let mut red = e
        .query()
        .filter_tag(FIELD_PRIMARY_TAG, InlineId::new("red").unwrap())
        .execute_ids();
    red.sort();
    let prefixed = e.primary_index().id_prefix_bitmap("rec01").len();
    (e.primary_index().len(), scores, range, red, prefixed)
}

fn workload(cold: &StorageEngine<AgentObjectRecord>, mem: &StorageEngine<AgentObjectRecord>, rng: &mut SweepRng, ops: usize) {
    for _ in 0..ops {
        let i = rng.below((N + 200) as usize) as u32;
        if rng.below(5) == 0 {
            assert_eq!(cold.delete(id(i)).unwrap(), mem.delete(id(i)).unwrap());
        } else {
            let v = rng.below(1_000) as i64;
            cold.put(id(i), &record(i, v)).unwrap();
            mem.put(id(i), &record(i, v)).unwrap();
        }
    }
}

#[test]
fn test_cold_primary_tier_matches_memory_tier_and_bounds_resident_entries() {
    for (variant, (index_tier, encrypted)) in [(IndexTier::Memory, false), (IndexTier::Paged, true)].into_iter().enumerate() {
        let cold_dir = temp_dir(&format!("cold{variant}"));
        let mem_dir = temp_dir(&format!("mem{variant}"));
        let mut rng = SweepRng::new(0x5EED + variant as u64);
        let mem = StorageEngine::<AgentObjectRecord>::open(&mem_dir, cfg(PrimaryTier::Memory, index_tier, encrypted)).unwrap();
        {
            let cold = StorageEngine::<AgentObjectRecord>::open(&cold_dir, cfg(PrimaryTier::Cold, index_tier, encrypted)).unwrap();
            for i in 0..N {
                cold.put(id(i), &record(i, i64::from(i % 1000))).unwrap();
                mem.put(id(i), &record(i, i64::from(i % 1000))).unwrap();
            }
            cold.flush_all().unwrap();
            assert!(cold.primary_index().is_tiered());
            let resident = cold.primary_index().resident_entries();
            assert!(resident < (N / 10) as usize, "after a checkpoint RAM holds only recent changes ({resident})");
            assert_eq!(observe(&cold), observe(&mem));

            // Mixed writes / deletes on top of the tier, with checkpoints in between.
            for _ in 0..3 {
                workload(&cold, &mem, &mut rng, 800);
                assert_eq!(observe(&cold), observe(&mem));
                cold.flush_all().unwrap();
                assert_eq!(observe(&cold), observe(&mem));
            }
            assert!(!cold.query().top_k_similar(&[3.0, 1.0, 2.0], 5).is_empty());
        }

        // Clean restart: the tier is mapped, not loaded; only reads promote entries into RAM.
        {
            let cold = StorageEngine::<AgentObjectRecord>::open(&cold_dir, cfg(PrimaryTier::Cold, index_tier, encrypted)).unwrap();
            assert!(cold.primary_index().is_tiered());
            assert_eq!(cold.primary_index().resident_entries(), 0, "nothing loaded at open");
            assert!(cold.primary_index().cold_tier_ram_bytes() > 0);
            if index_tier == IndexTier::Paged {
                assert!(cold.secondary_indexes_reused(), "paged stamps match the cold tier's stamp");
            }
            assert_eq!(observe(&cold), observe(&mem));
            workload(&cold, &mem, &mut rng, 500);
        }
        // Restart after unflushed writes (WAL replay on top of the tier).
        {
            let cold = StorageEngine::<AgentObjectRecord>::open(&cold_dir, cfg(PrimaryTier::Cold, index_tier, encrypted)).unwrap();
            assert_eq!(observe(&cold), observe(&mem));
        }

        // Switch to the memory tier (cold files materialized, then replaced by primary.idx) ...
        let idx_dir = cold_dir.join("indexes");
        let names = || -> Vec<String> {
            fs::read_dir(&idx_dir).unwrap().flatten().map(|e| e.file_name().to_string_lossy().into_owned()).collect()
        };
        {
            let back = StorageEngine::<AgentObjectRecord>::open(&cold_dir, cfg(PrimaryTier::Memory, index_tier, encrypted)).unwrap();
            assert!(!back.primary_index().is_tiered());
            assert_eq!(observe(&back), observe(&mem));
            back.flush_all().unwrap();
        }
        assert!(names().iter().all(|n| !n.contains("_primary.cold")), "cold files removed: {:?}", names());
        assert!(names().iter().any(|n| n.ends_with("_primary.idx")));
        // ... and back to the cold tier (primary.idx converted at the first checkpoint).
        {
            let cold = StorageEngine::<AgentObjectRecord>::open(&cold_dir, cfg(PrimaryTier::Cold, index_tier, encrypted)).unwrap();
            cold.flush_all().unwrap();
            assert!(cold.primary_index().is_tiered());
            assert_eq!(observe(&cold), observe(&mem));
        }
        assert!(names().iter().all(|n| !n.ends_with("_primary.idx")), "primary.idx superseded: {:?}", names());
        if encrypted {
            // The cold generation is sealed like every other index file.
            let generation = names().into_iter().find(|n| n.contains("_primary.cold.")).unwrap();
            let bytes = fs::read(idx_dir.join(generation)).unwrap();
            assert_eq!(&bytes[128..132], b"PENC");
            assert!(!bytes.windows(8).any(|w| w == b"rec00123"), "record id in a sealed cold tier");
        }
        drop(mem);
        let _ = fs::remove_dir_all(&cold_dir);
        let _ = fs::remove_dir_all(&mem_dir);
    }
}
