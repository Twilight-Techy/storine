//! Persisted HNSW graph (`hnsw.idx`, `F-HNSW-02`) and stable ordinals (checkpoint v3): the graph
//! is reused across restarts without re-insertion, reconciled with writes it missed, and rebuilt
//! when the file is damaged. Index contents are exact on every path; search quality is checked
//! as recall@10 against brute force (`F-HNSW-01`: `>= 95%`).

use std::fs;
use std::path::{Path, PathBuf};

use storage_core::{AgentObjectRecord, EngineConfig, StorageEngine, StorageLayoutMode};
use storage_index::cosine_similarity_f32;
use storage_types::InlineId;

const DIM: usize = 16;
const COUNT: u32 = 600; // > 256 so top-K uses the HNSW graph
const DELETED: [u32; 3] = [10, 20, 30];

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("storage_vec_persist_{name}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn cfg() -> EngineConfig {
    EngineConfig {
        layout_mode: StorageLayoutMode::AdaptiveArena { arena_threshold_bytes: 4096, segment_capacity: 1 << 20 },
        ..EngineConfig::default()
    }
}

/// Deterministic pseudo-random vectors (xorshift), all components in `[0, 1)`.
fn vector(i: u32) -> Vec<f32> {
    let mut x = u64::from(i).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..DIM)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x % 10_000) as f32 / 10_000.0
        })
        .collect()
}

/// A vector unlike any dataset vector (dataset values are all non-negative).
fn unique_vector() -> Vec<f32> {
    (0..DIM).map(|d| if d % 2 == 0 { 1.0 } else { -1.0 }).collect()
}

fn id(i: u32) -> InlineId {
    InlineId::new(&format!("v{i:05}")).unwrap()
}

fn record(i: u32, v: Vec<f32>) -> AgentObjectRecord {
    let mut r = AgentObjectRecord::new(id(i), "vec", 1, i64::from(i));
    r.embedding = v;
    r
}

fn top_ids(engine: &StorageEngine<AgentObjectRecord>, q: &[f32]) -> Vec<InlineId> {
    engine.query().top_k_similar(q, 10).into_iter().map(|(id, _)| id).collect()
}

fn hnsw_file(dir: &Path) -> PathBuf {
    dir.join("indexes").join("default_hnsw.idx")
}

/// Mean recall@10 of the engine against exact brute-force search over `live`.
fn recall_at_10(engine: &StorageEngine<AgentObjectRecord>, live: &[(InlineId, Vec<f32>)]) -> f64 {
    let mut total = 0.0;
    let queries = 25u32;
    for qi in 0..queries {
        let q = vector(50_000 + qi);
        let mut exact: Vec<(f32, InlineId)> =
            live.iter().map(|(k, v)| (cosine_similarity_f32(&q, v), *k)).collect();
        exact.sort_by(|a, b| b.0.total_cmp(&a.0));
        let truth: Vec<InlineId> = exact.iter().take(10).map(|e| e.1).collect();
        let got = top_ids(engine, &q);
        total += truth.iter().filter(|k| got.contains(k)).count() as f64 / 10.0;
    }
    total / f64::from(queries)
}

#[test]
fn test_hnsw_graph_persists_reconciles_and_recovers() {
    let dir = temp_dir("main");
    let mut live: Vec<(InlineId, Vec<f32>)> = (0..COUNT)
        .filter(|i| !DELETED.contains(i))
        .map(|i| (id(i), vector(i)))
        .collect();
    let queries: Vec<Vec<f32>> = (0..5).map(|i| vector(70_000 + i)).collect();
    let before: Vec<Vec<InlineId>>;
    let stale_graph: Vec<u8>;
    {
        let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg()).unwrap();
        for i in 0..COUNT {
            engine.put(id(i), &record(i, vector(i))).unwrap();
        }
        // Deleted records leave ordinal holes that must survive restarts.
        for i in DELETED {
            engine.delete(id(i)).unwrap();
        }
        engine.flush_all().unwrap();
        assert!(hnsw_file(&dir).exists(), "checkpoint must persist the HNSW graph");
        stale_graph = fs::read(hnsw_file(&dir)).unwrap();
        before = queries.iter().map(|q| top_ids(&engine, q)).collect();
        let r = recall_at_10(&engine, &live);
        assert!(r >= 0.95, "recall@10 = {r}");
    }

    // 1. Reopen: the persisted graph is reused as-is (no insertion => generation unchanged), so
    //    results are identical to before the restart.
    {
        let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg()).unwrap();
        assert_eq!(engine.vector_index().generation(), 0, "graph must be reused, not rebuilt");
        assert_eq!(engine.vector_index().len(), live.len());
        let after: Vec<Vec<InlineId>> = queries.iter().map(|q| top_ids(&engine, q)).collect();
        assert_eq!(after, before);
        // A write the saved graph will not know about (the old file is restored below, as a
        // crash before the next checkpoint would leave it).
        engine.put(id(177), &record(177, unique_vector())).unwrap();
    }
    fs::write(hnsw_file(&dir), &stale_graph).unwrap();
    live.retain(|(k, _)| *k != id(177));
    live.push((id(177), unique_vector()));

    // 2. Reconciliation replaces exactly the vector the stale graph had wrong.
    {
        let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg()).unwrap();
        let vi = engine.vector_index();
        assert!(vi.generation() >= 1, "stale vector must be re-inserted");
        assert!(vi.live_ordinals().iter().any(|&o| vi.vector_matches(o, &unique_vector())));
        assert!(!vi.live_ordinals().iter().any(|&o| vi.vector_matches(o, &vector(177))));
        assert_eq!(vi.len(), live.len());
        let r = recall_at_10(&engine, &live);
        assert!(r >= 0.95, "recall@10 = {r}");
        engine.flush_all().unwrap();
    }

    // 3. A damaged graph file is rebuilt from the records.
    let mut bytes = fs::read(hnsw_file(&dir)).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    fs::write(hnsw_file(&dir), bytes).unwrap();
    {
        let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg()).unwrap();
        let vi = engine.vector_index();
        assert_eq!(vi.len(), live.len());
        let ords = vi.live_ordinals();
        for (_, v) in &live {
            assert!(ords.iter().any(|&o| vi.vector_matches(o, v)));
        }
        let r = recall_at_10(&engine, &live);
        assert!(r >= 0.95, "recall@10 = {r}");
    }
    let _ = fs::remove_dir_all(&dir);
}

/// Connectivity guarantee: a vector unlike everything else (an outlier) is still reachable, both
/// when updated in place and when freshly inserted.
#[test]
fn test_outlier_vectors_remain_reachable() {
    let dir = temp_dir("outlier");
    let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg()).unwrap();
    for i in 0..COUNT {
        engine.put(id(i), &record(i, vector(i))).unwrap();
    }
    engine.put(id(177), &record(177, unique_vector())).unwrap();
    assert_eq!(top_ids(&engine, &unique_vector()).first(), Some(&id(177)));
    engine.delete(id(177)).unwrap();
    engine.put(id(9_999), &record(9_999, unique_vector())).unwrap();
    assert_eq!(top_ids(&engine, &unique_vector()).first(), Some(&id(9_999)));
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}
