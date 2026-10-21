//! `IndexTier::Paged` (`TASK-083`, `F-PIDX-01`, `F-PIDX-02`): every query answers exactly like the
//! in-memory tier through writes, checkpoints and restarts; indexes are reused after a clean
//! shutdown and rebuilt (correctly) after a crash.

use std::fs;
use std::path::PathBuf;

use storage_core::{
    AgentObjectRecord, EngineConfig, IndexTier, StorageEngine, StorageLayoutMode, FIELD_PRIMARY_TAG,
    FIELD_SCORE, FIELD_TIMESTAMP,
};
use storage_types::testing::SweepRng;
use storage_types::InlineId;

/// A named query evaluated against an engine.
type NamedQuery<'a> = (&'a str, Box<dyn Fn(&StorageEngine<AgentObjectRecord>) -> Vec<InlineId>>);

const TAGS: [&str; 6] = ["red", "green", "greenish", "blue", "black", "white"];

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("storage_paged_tier_{name}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn cfg(tier: IndexTier) -> EngineConfig {
    EngineConfig {
        layout_mode: StorageLayoutMode::AdaptiveArena { arena_threshold_bytes: 4096, segment_capacity: 1 << 20 },
        index_tier: tier,
        ..EngineConfig::default()
    }
}

fn tag(i: usize) -> InlineId {
    InlineId::new(TAGS[i % TAGS.len()]).unwrap()
}

fn id(i: u32) -> InlineId {
    InlineId::new(&format!("r{i:05}")).unwrap()
}

/// Timestamps are unique per record so ordered results have no ties (ties break by ordinal,
/// which legitimately differs between independently restarted engines).
fn record(i: u32, rng: &mut SweepRng) -> AgentObjectRecord {
    let mut r = AgentObjectRecord::new(id(i), "t", 1_000_000 + i64::from(i) * 7, rng.below(500) as i64);
    r.tags = (0..rng.below(3)).map(|_| tag(rng.below(TAGS.len()))).collect();
    r
}

fn sorted(mut v: Vec<InlineId>) -> Vec<InlineId> {
    v.sort();
    v
}

/// Runs every query family against both engines and asserts identical answers.
fn assert_same_answers(mem: &StorageEngine<AgentObjectRecord>, paged: &StorageEngine<AgentObjectRecord>, rng: &mut SweepRng) {
    for _ in 0..12 {
        let a = tag(rng.below(TAGS.len()));
        let b = tag(rng.below(TAGS.len()));
        let lo = rng.below(400) as i64;
        let hi = lo + rng.below(200) as i64;
        let ts_lo = 1_000_000 + rng.below(2_000) as i64 * 7;
        let ts_hi = ts_lo + rng.below(3_000) as i64;
        let unordered: Vec<NamedQuery<'_>> = vec![
            ("tag", Box::new(move |e| e.query().filter_tag(FIELD_PRIMARY_TAG, a).execute_ids())),
            ("tag and tag", Box::new(move |e| e.query().filter_tag(FIELD_PRIMARY_TAG, a).filter_tag(FIELD_PRIMARY_TAG, b).execute_ids())),
            ("not tag", Box::new(move |e| e.query().filter_has_tag_field(FIELD_PRIMARY_TAG).filter_not_tag(FIELD_PRIMARY_TAG, a).execute_ids())),
            ("any tag", Box::new(move |e| e.query().filter_any_tag(FIELD_PRIMARY_TAG, &[a, b]).execute_ids())),
            ("prefix", Box::new(|e| e.query().filter_tag_prefix(FIELD_PRIMARY_TAG, "gre").execute_ids())),
            ("missing tags", Box::new(|e| e.query().filter_missing_tag_field(FIELD_PRIMARY_TAG).execute_ids())),
            ("score range", Box::new(move |e| e.query().filter_range(FIELD_SCORE, lo..=hi).execute_ids())),
            ("score eq", Box::new(move |e| e.query().filter_eq(FIELD_SCORE, lo).execute_ids())),
            ("score ne", Box::new(move |e| e.query().filter_ne(FIELD_SCORE, lo).execute_ids())),
            ("score not range", Box::new(move |e| e.query().filter_not_range(FIELD_SCORE, lo..=hi).execute_ids())),
            ("tag + range", Box::new(move |e| e.query().filter_tag(FIELD_PRIMARY_TAG, a).filter_range(FIELD_SCORE, lo..=hi).execute_ids())),
        ];
        for (name, q) in &unordered {
            assert_eq!(sorted(q(paged)), sorted(q(mem)), "query `{name}`");
        }
        // Ordered, paginated scans over the (tie-free) timestamp field.
        for desc in [false, true] {
            let page = |e: &StorageEngine<AgentObjectRecord>, off: usize| {
                let q = e.query().filter_range(FIELD_TIMESTAMP, ts_lo..=ts_hi).offset(off).limit(25);
                let q = if desc { q.order_desc() } else { q.order_asc() };
                let p = q.execute_page_ids();
                (p.items, p.total_matched, p.has_more)
            };
            let off = rng.below(30);
            assert_eq!(page(paged, off), page(mem, off), "paginated timestamp scan desc={desc}");
            let tagged = |e: &StorageEngine<AgentObjectRecord>| {
                let q = e.query().filter_tag(FIELD_PRIMARY_TAG, a).filter_range(FIELD_TIMESTAMP, ts_lo..=ts_hi).limit(40);
                let q = if desc { q.order_desc() } else { q.order_asc() };
                q.execute_page_ids().items
            };
            assert_eq!(tagged(paged), tagged(mem), "paginated tag + timestamp scan desc={desc}");
        }
    }
}

#[test]
fn test_paged_tier_matches_memory_tier_through_checkpoints_and_restarts() {
    let mem_dir = temp_dir("mem");
    let paged_dir = temp_dir("paged");
    let mut rng = SweepRng::new(77);
    let mut q_rng = SweepRng::new(78);
    let mem = StorageEngine::<AgentObjectRecord>::open(&mem_dir, cfg(IndexTier::Memory)).unwrap();
    let mut paged = Some(StorageEngine::<AgentObjectRecord>::open(&paged_dir, cfg(IndexTier::Paged)).unwrap());
    for round in 0..6u32 {
        for _ in 0..800 {
            let i = rng.below(2_000) as u32;
            let r = record(i, &mut rng);
            if rng.below(5) == 0 {
                let _ = mem.delete(id(i));
                let _ = paged.as_ref().unwrap().delete(id(i));
            } else {
                mem.put(id(i), &r).unwrap();
                paged.as_ref().unwrap().put(id(i), &r).unwrap();
            }
        }
        assert_same_answers(&mem, paged.as_ref().unwrap(), &mut q_rng);
        match round % 3 {
            0 => {
                paged.as_ref().unwrap().flush_all().unwrap();
                assert_same_answers(&mem, paged.as_ref().unwrap(), &mut q_rng);
            }
            1 => {
                // Clean restart: persisted indexes are reused, not rebuilt.
                drop(paged.take());
                let reopened = StorageEngine::<AgentObjectRecord>::open(&paged_dir, cfg(IndexTier::Paged)).unwrap();
                assert!(reopened.secondary_indexes_reused(), "clean restart must reuse paged indexes");
                paged = Some(reopened);
                assert_same_answers(&mem, paged.as_ref().unwrap(), &mut q_rng);
            }
            _ => {}
        }
    }
    drop(paged);
    drop(mem);
    let _ = fs::remove_dir_all(&mem_dir);
    let _ = fs::remove_dir_all(&paged_dir);
}

#[test]
fn test_paged_indexes_are_rebuilt_when_not_provably_current() {
    let dir = temp_dir("rebuild");
    let mut rng = SweepRng::new(5);
    let mem_dir = temp_dir("rebuild_mem");
    let mem = StorageEngine::<AgentObjectRecord>::open(&mem_dir, cfg(IndexTier::Memory)).unwrap();
    {
        let paged = StorageEngine::<AgentObjectRecord>::open(&dir, cfg(IndexTier::Paged)).unwrap();
        for i in 0..1_500u32 {
            let r = record(i, &mut rng);
            paged.put(id(i), &r).unwrap();
            mem.put(id(i), &r).unwrap();
        }
    }
    // 1. Clean restart reuses.
    {
        let paged = StorageEngine::<AgentObjectRecord>::open(&dir, cfg(IndexTier::Paged)).unwrap();
        assert!(paged.secondary_indexes_reused());
    }
    // 2. Damaged index file: rebuilt, answers still exact.
    let tags_manifest = dir.join("indexes").join("default_tags.idx");
    let mut m = fs::read(&tags_manifest).unwrap();
    m[10] ^= 0xFF;
    fs::write(&tags_manifest, m).unwrap();
    {
        let paged = StorageEngine::<AgentObjectRecord>::open(&dir, cfg(IndexTier::Paged)).unwrap();
        assert!(!paged.secondary_indexes_reused());
        assert_same_answers(&mem, &paged, &mut SweepRng::new(6));
    }
    // 3. Primary checkpoint from a different run (stamp mismatch): rebuilt.
    let primary = dir.join("indexes").join("default_primary.idx");
    let saved_primary = fs::read(&primary).unwrap();
    {
        let paged = StorageEngine::<AgentObjectRecord>::open(&dir, cfg(IndexTier::Paged)).unwrap();
        assert!(paged.secondary_indexes_reused());
        let r = record(7, &mut rng);
        paged.put(id(7), &r).unwrap();
        mem.put(id(7), &r).unwrap();
        paged.flush_all().unwrap();
    }
    let current_primary = fs::read(&primary).unwrap();
    fs::write(&primary, &saved_primary).unwrap();
    {
        // Older primary + newer indexes: the stamps differ, so the indexes are not trusted.
        let paged = StorageEngine::<AgentObjectRecord>::open(&dir, cfg(IndexTier::Paged)).unwrap();
        assert!(!paged.secondary_indexes_reused());
    }
    fs::write(&primary, current_primary).unwrap();
    drop(mem);
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::remove_dir_all(&mem_dir);
}
