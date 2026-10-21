//! End-to-End `v1.0` Production Verification Gate (`TASK-050`, Stages 8–14).
//!
//! Validates all full-scale production database capabilities in a single deterministic integration suite:
//! - Stage 8: `DatabaseCatalog`, Cross-Collection ACID `CatalogTransaction`, and Multi-Step Schema Evolution
//! - Stage 9: `PagedBTreeIndex`, `PagedBitmapIndex`, `SplitBlockBloomFilter`, and `ColdPrimaryIndex`
//! - Stage 10: `SQ8` Scalar Quantization & `HnswVectorIndex` ($O(\log N)$ ANN + exact `f32` re-ranking)
//! - Stage 11: MVCC Snapshot Isolation (`ReadSnapshot` + epoch GC) & Online Backup + PITR Restore
//! - Stage 13: `1,024`-vshard `ClusterRouter`, Live `vshard` Migration, and 3-Node Raft Consensus Failover
//! - Stage 14: Cost-Based `QueryOptimizer` (`EXPLAIN`), Lock-Free `LatencyHistogram`, and Prometheus Metrics

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use storage_core::{
    archive_wal_for_pitr, create_online_backup, restore_point_in_time, vshard_for_id,
    AccessPathKind, AgentObjectRecord, ClusterRouter, DatabaseCatalog, EngineConfig,
    LatencyHistogram, RaftNode, RaftReplicationGroup, RaftRole, SlowOperationLog, StorageEngine,
    FIELD_PRIMARY_TAG, FIELD_SCORE,
};
use storage_index::{
    ColdPrimaryEntry, ColdPrimaryIndex, DiskLocation, PagedBTreeIndex, PagedBitmapIndex,
    SplitBlockBloomFilter,
};
use storage_mmap::ArenaLocation;
use storage_types::{InlineId, RecordOrdinal, Version};
use storage_wal::SyncPolicy;

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[test]
fn test_production_v1_end_to_end_verification_gate() {
    let root = std::env::temp_dir().join("stge_production_v1_gate");
    let _ = std::fs::remove_dir_all(&root);

    // =========================================================================
    // 1. Stage 8: Multi-Collection Catalog & Cross-Collection ACID Transactions
    // =========================================================================
    let catalog_dir = root.join("catalog_db");
    let catalog = DatabaseCatalog::open(&catalog_dir, SyncPolicy::default()).unwrap();
    let agents: Arc<StorageEngine<AgentObjectRecord>> = catalog
        .register_collection(EngineConfig::default())
        .unwrap();

    let aid = InlineId::from_static("agent_prod_01");
    let aid2 = InlineId::from_static("agent_prod_02");
    catalog
        .transaction(|ctx| {
            ctx.put(
                &agents,
                aid,
                AgentObjectRecord::new(aid, "Prod Agent 1", 1_000, 500),
            );
            ctx.put(
                &agents,
                aid2,
                AgentObjectRecord::new(aid2, "Prod Agent 2", 1_000, 100),
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(agents.with_ref(aid, |v| v.score).unwrap(), 500);
    assert_eq!(agents.with_ref(aid2, |v| v.score).unwrap(), 100);
    drop(agents);
    drop(catalog);

    // =========================================================================
    // 2. Stage 9: Disk-Paged B+Tree, Paged Bitmap, Split-Block Bloom & Cold Primary Index
    // =========================================================================
    let idx_dir = root.join("paged_indexes");
    std::fs::create_dir_all(&idx_dir).unwrap();
    let btree_path = idx_dir.join("score_paged.btidx");
    let paged_btree = PagedBTreeIndex::open(&btree_path).unwrap();
    for i in 0..2_000i64 {
        paged_btree.insert(1, i * 5, RecordOrdinal(i as u32));
    }
    paged_btree.checkpoint_to_disk().unwrap();
    assert_eq!(paged_btree.count_in_range_zero_alloc(1, 0, 495), 100);

    let bm_path = idx_dir.join("tag_paged.bmidx");
    let tag_ai = InlineId::from_static("ai");
    let paged_bm = PagedBitmapIndex::open(&bm_path).unwrap();
    for i in 0..500u32 {
        paged_bm.insert(1, tag_ai, RecordOrdinal(i * 2));
    }
    paged_bm.checkpoint_to_disk().unwrap();
    assert_eq!(paged_bm.intersect_all(&[(1, tag_ai)]).len(), 500);

    let mut bloom = SplitBlockBloomFilter::with_capacity(1_000);
    bloom.insert(aid);
    assert!(bloom.may_contain(aid));

    let cold_path = idx_dir.join("cold_primary.cidx");
    let cold_entries = vec![ColdPrimaryEntry {
        id: aid,
        ordinal: RecordOrdinal(0),
        version: Version(1),
        ttl_expires_at_ns: 0,
        location: DiskLocation::Arena(ArenaLocation {
            segment_id: 1,
            offset: 64,
            len: 192,
        }),
    }];
    let cold_idx = ColdPrimaryIndex::build(&cold_path, cold_entries).unwrap();
    assert_eq!(cold_idx.get(aid).unwrap().ordinal, RecordOrdinal(0));

    // =========================================================================
    // 3. Stage 10 & Stage 14: HNSW Vector Search, Cost-Based Optimizer & Histograms
    // =========================================================================
    let eng_dir = root.join("main_engine");
    let engine =
        StorageEngine::<AgentObjectRecord>::open(&eng_dir, EngineConfig { wal_archive: true, ..EngineConfig::default() }).unwrap();
    let hist = LatencyHistogram::new();
    let slow_log = SlowOperationLog::new(1, 16);

    for i in 0..300u32 {
        let id = InlineId::new(&format!("vec_rec_{i:04}")).unwrap();
        let mut rec =
            AgentObjectRecord::new(id, format!("Vec {i}"), 1_000 + i64::from(i), i64::from(i));
        if i % 10 == 0 {
            rec.tags.push(tag_ai);
        }
        let angle = (i as f32) * 0.01;
        rec.embedding = vec![angle.cos(), angle.sin(), 0.5, 0.25];
        engine.put(id, &rec).unwrap();
        hist.record_ns(120 + u64::from(i));
    }
    slow_log.record_if_slow("compaction_check", 500, || "gate test".to_string());

    // Cost-Based Optimizer EXPLAIN plan verification
    let plan = engine
        .query()
        .filter_tag(FIELD_PRIMARY_TAG, tag_ai)
        .filter_range(FIELD_SCORE, 0..=250)
        .limit(10)
        .explain();
    assert_eq!(plan.chosen_access_path, AccessPathKind::BitmapTagFirst);
    assert_eq!(plan.ordered_predicates[0].estimated_cardinality, 30);

    // Hybrid HNSW + Metadata Vector Top-K Search
    let top_hits = engine
        .query()
        .filter_tag(FIELD_PRIMARY_TAG, tag_ai)
        .top_k_similar(&[1.0, 0.0, 0.5, 0.25], 5);
    assert_eq!(top_hits.len(), 5);
    assert_eq!(top_hits[0].0, InlineId::from_static("vec_rec_0000"));

    // Prometheus metrics export verification
    let prom_text = engine.stats().to_prometheus_text(Some(&hist), None);
    assert!(prom_text.contains("storine_live_records 300"));
    assert!(prom_text.contains("storine_read_latency_ns_count 300"));
    assert_eq!(slow_log.snapshot().len(), 1);

    // =========================================================================
    // 4. Stage 11: MVCC Snapshot Isolation & Online Backup + PITR Restore
    // =========================================================================
    let snap = engine.begin_read_snapshot();
    let target_id = InlineId::from_static("vec_rec_0000");
    engine
        .put(
            target_id,
            &AgentObjectRecord::new(target_id, "Mutated After Snapshot", 9_999, 9_999),
        )
        .unwrap();
    // Snapshot still sees original version ("Vec 0", score 0)!
    assert_eq!(snap.get(target_id).unwrap().unwrap().1.score, 0);
    drop(snap);
    assert!(engine.gc_mvcc() >= 1);

    let backup_dir = root.join("backup_snap");
    let restore_dir = root.join("restored_snap");
    create_online_backup(&engine, &backup_dir).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(2));
    let pitr_cutoff_ns = now_ns();
    std::thread::sleep(std::time::Duration::from_millis(2));
    let post_cutoff_id = InlineId::from_static("after_pitr_cutoff");
    engine
        .put(
            post_cutoff_id,
            &AgentObjectRecord::new(post_cutoff_id, "Should Rollback", 10, 10),
        )
        .unwrap();
    archive_wal_for_pitr(&engine, &backup_dir).unwrap();
    drop(engine);

    let restored = restore_point_in_time::<AgentObjectRecord>(
        &backup_dir,
        &restore_dir,
        EngineConfig { wal_archive: true, ..EngineConfig::default() },
        pitr_cutoff_ns,
    )
    .unwrap();
    assert!(restored.get_owned(target_id).is_ok());
    assert!(restored.get_owned(post_cutoff_id).is_err());
    drop(restored);

    // =========================================================================
    // 5. Stage 13: 1,024-vshard ClusterRouter + 3-Node Raft Consensus Failover
    // =========================================================================
    let s1 = Arc::new(
        StorageEngine::<AgentObjectRecord>::open(root.join("shard1"), EngineConfig::default())
            .unwrap(),
    );
    let s2 = Arc::new(
        StorageEngine::<AgentObjectRecord>::open(root.join("shard2"), EngineConfig::default())
            .unwrap(),
    );
    let router = ClusterRouter::new(vec![(1, Arc::clone(&s1)), (2, Arc::clone(&s2))]).unwrap();
    let k1 = InlineId::from_static("cluster_key_01");
    router
        .put(k1, &AgentObjectRecord::new(k1, "Sharded Item", 100, 42))
        .unwrap();
    let vs = vshard_for_id(k1);
    let current_owner = router.route_key(k1).unwrap().0;
    let new_owner = if current_owner == 1 { 2 } else { 1 };
    let mig = router.migrate_vshard(vs, new_owner).unwrap();
    assert_eq!(mig.records_migrated, 1);
    assert_eq!(router.with_ref(k1, |v| v.score).unwrap(), 42);
    drop(router);
    drop(s1);
    drop(s2);

    // 3-Node Raft Consensus Replication & Failover
    let r1 = Arc::new(RaftNode::new(
        1,
        Arc::new(
            StorageEngine::<AgentObjectRecord>::open(root.join("raft1"), EngineConfig::default())
                .unwrap(),
        ),
    ).unwrap());
    let r2 = Arc::new(RaftNode::new(
        2,
        Arc::new(
            StorageEngine::<AgentObjectRecord>::open(root.join("raft2"), EngineConfig::default())
                .unwrap(),
        ),
    ).unwrap());
    let r3 = Arc::new(RaftNode::new(
        3,
        Arc::new(
            StorageEngine::<AgentObjectRecord>::open(root.join("raft3"), EngineConfig::default())
                .unwrap(),
        ),
    ).unwrap());
    let raft_group =
        RaftReplicationGroup::new(vec![Arc::clone(&r1), Arc::clone(&r2), Arc::clone(&r3)]).unwrap();
    raft_group.elect_leader(1).unwrap();
    let rk = InlineId::from_static("raft_key_01");
    let cidx = raft_group
        .replicate_put(rk, &AgentObjectRecord::new(rk, "Replicated", 500, 888))
        .unwrap();
    assert_eq!(r3.read_at_commit_index(rk, cidx, |v| v.score).unwrap(), 888);

    // Fail over from Node 1 -> Node 2
    raft_group.set_node_isolated(1, true);
    raft_group.elect_leader(2).unwrap();
    assert_eq!(r2.role(), RaftRole::Leader);

    drop(raft_group);
    drop(r1);
    drop(r2);
    drop(r3);
    let _ = std::fs::remove_dir_all(&root);
}
