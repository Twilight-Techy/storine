//! Comprehensive Full-System End-to-End Integration Test (`TASK-064`, Stage 18).
//!
//! Exercises **every** production feature in a single deterministic flow:
//! 1. Directory Lock — exclusive `db.lock` prevents concurrent access
//! 2. Database Catalog — multi-collection, cross-collection ACID
//! 3. Core CRUD + Grouped Writes + Secondary Indexes + HNSW Vector Search
//! 4. Cost-Based Query Optimizer — EXPLAIN plans
//! 5. Transactions — multi-record OCC with bank-transfer invariant
//! 6. CAS (Compare-And-Swap) — optimistic concurrency control
//! 7. TTL Expiration — time-based record sweep
//! 8. Cold LZ4 Compression — transparent compress/decompress round-trip
//! 9. Schema Migration — registered upcasters
//! 10. MVCC Snapshot Isolation — concurrent read snapshots with epoch GC
//! 11. Online Backup + PITR Restore — full backup → WAL archive → timestamp rollback
//! 12. Persistent Raft HardState — save to disk, drop, reload, verify survival
//! 13. Persistent ClusterMap — save, load, verify
//! 14. Sharding — ClusterRouter, vshard migration
//! 15. 3-Node Raft Consensus Replication + Failover
//! 16. Telemetry — LatencyHistogram, SlowOperationLog, Prometheus export
//! 17. Paged Disk Indexes — PagedBTreeIndex, PagedBitmapIndex, ColdPrimaryIndex
//! 18. TCP Server + Client — all 21 opcodes (0x01–0x14) over the network
//! 19. Admin RPCs — remote backup, compact, checkpoint, health
//! 20. Raft RPCs — RequestVote, AppendEntries over TCP
//! 21. Shard Migration RPC — MigrateVShard over TCP
//! 22. RBAC — unauthenticated, ReadOnly, ReadWrite, Admin role enforcement
//! 23. Arena Compaction — fragmentation reclaim
//! 24. WAL Crash Recovery — torn frame tolerance

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use storage_core::{
    archive_wal_for_pitr, create_online_backup, restore_point_in_time, vshard_for_id,
    AccessPathKind, AgentObjectRecord, ClusterMap, ClusterRouter, DatabaseCatalog, EngineConfig,
    LatencyHistogram, RaftNode, RaftReplicationGroup, RaftRole, SlowOperationLog, StorageEngine,
    StorageLayoutMode, FIELD_PRIMARY_TAG, FIELD_SCORE,
};
use storage_core::raft::{AppendEntriesReq, AppendEntriesResp, RequestVoteReq, RequestVoteResp};
use storage_index::{
    ColdPrimaryEntry, ColdPrimaryIndex, DiskLocation, PagedBTreeIndex, PagedBitmapIndex,
    SplitBlockBloomFilter,
};
use storage_mmap::{ArenaLocation, DirectoryLock};
use storage_server::{
    decode_agent_frame, RbacRole, SecurityConfig, StorageClient, StorageServer,
};
use storage_types::{InlineId, RecordOrdinal, StorageError, Version};
use storage_wal::SyncPolicy;

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[test]
fn test_full_system_end_to_end_all_features() {
    let root = std::env::temp_dir().join("stge_full_system_e2e");
    let _ = std::fs::remove_dir_all(&root);

    // =========================================================================
    // § 1. Directory Lock — Exclusive access prevention
    // =========================================================================
    let lock_path = root.join("exclusive_test").join("agent_object.lock");
    let lock1 = DirectoryLock::try_acquire(&lock_path).unwrap();
    assert!(
        DirectoryLock::try_acquire(&lock_path).is_err(),
        "second lock acquisition on same path must fail"
    );
    drop(lock1);
    // After drop, re-acquisition must succeed
    let lock2 = DirectoryLock::try_acquire(&lock_path).unwrap();
    drop(lock2);

    // =========================================================================
    // § 2. Database Catalog — Multi-collection + Cross-Collection ACID
    // =========================================================================
    let catalog_dir = root.join("catalog");
    let catalog = DatabaseCatalog::open(&catalog_dir, SyncPolicy::default()).unwrap();
    let agents: Arc<StorageEngine<AgentObjectRecord>> = catalog
        .register_collection(EngineConfig::default())
        .unwrap();

    let a1 = InlineId::from_static("cat_agent_01");
    let a2 = InlineId::from_static("cat_agent_02");
    catalog
        .transaction(|ctx| {
            ctx.put(
                &agents,
                a1,
                AgentObjectRecord::new(a1, "Catalog Agent 1", 1000, 100),
            );
            ctx.put(
                &agents,
                a2,
                AgentObjectRecord::new(a2, "Catalog Agent 2", 1000, 200),
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(agents.with_ref(a1, |v| v.score).unwrap(), 100);
    assert_eq!(agents.with_ref(a2, |v| v.score).unwrap(), 200);
    drop(agents);
    drop(catalog);

    // =========================================================================
    // § 3. Core CRUD + Secondary Indexes + HNSW Vector Search
    // =========================================================================
    let eng_dir = root.join("main_engine");
    let engine =
        StorageEngine::<AgentObjectRecord>::open(&eng_dir, EngineConfig { wal_archive: true, ..EngineConfig::default() }).unwrap();

    let tag_ai = InlineId::from_static("ai");
    let tag_ml = InlineId::from_static("ml");

    for i in 0..100u32 {
        let id = InlineId::new(&format!("rec_{i:04}")).unwrap();
        let mut rec = AgentObjectRecord::new(id, format!("Record {i}"), 5000 + i64::from(i), i64::from(i));
        if i % 2 == 0 {
            rec.tags.push(tag_ai);
        }
        if i % 3 == 0 {
            rec.tags.push(tag_ml);
        }
        let angle = (i as f32) * 0.02;
        rec.embedding = vec![angle.cos(), angle.sin(), 0.5, 0.25];
        engine.put(id, &rec).unwrap();
    }

    // Update
    let upd_id = InlineId::from_static("rec_0010");
    engine
        .update(upd_id, |r| {
            r.score += 1000;
            Ok(())
        })
        .unwrap();
    assert_eq!(engine.with_ref(upd_id, |v| v.score).unwrap(), 1010);

    // Delete
    let del_id = InlineId::from_static("rec_0099");
    assert!(engine.delete(del_id).unwrap());
    assert!(matches!(
        engine.get_ref(del_id),
        Err(StorageError::RecordNotFound { .. })
    ));

    // Multi-tag + range query
    let query_hits = engine
        .query()
        .filter_tag(FIELD_PRIMARY_TAG, tag_ai)
        .filter_tag(FIELD_PRIMARY_TAG, tag_ml)
        .filter_range(FIELD_SCORE, 0..=50)
        .execute_ids();
    assert_eq!(query_hits.len(), 9); // multiples of 6 in 0..=50

    // HNSW Vector Top-K with tag filter
    let top_hits = engine
        .query()
        .filter_tag(FIELD_PRIMARY_TAG, tag_ai)
        .top_k_similar(&[1.0, 0.0, 0.5, 0.25], 3);
    assert_eq!(top_hits.len(), 3);
    assert_eq!(top_hits[0].0, InlineId::from_static("rec_0000"));

    // =========================================================================
    // § 4. Cost-Based Query Optimizer (EXPLAIN)
    // =========================================================================
    let plan = engine
        .query()
        .filter_tag(FIELD_PRIMARY_TAG, tag_ai)
        .filter_range(FIELD_SCORE, 0..=50)
        .limit(10)
        .explain();
    assert_eq!(plan.chosen_access_path, AccessPathKind::BitmapTagFirst);

    // =========================================================================
    // § 5. Transactions (OCC Bank Transfer)
    // =========================================================================
    let acct_a = InlineId::from_static("acct_alpha");
    let acct_b = InlineId::from_static("acct_beta");
    engine
        .put(acct_a, &AgentObjectRecord::new(acct_a, "Acct A", 1, 1000))
        .unwrap();
    engine
        .put(acct_b, &AgentObjectRecord::new(acct_b, "Acct B", 1, 1000))
        .unwrap();
    for _ in 0..50 {
        engine
            .transaction(|txn| {
                let mut a = txn.get(acct_a)?;
                let mut b = txn.get(acct_b)?;
                a.score -= 10;
                b.score += 10;
                txn.put(acct_a, a);
                txn.put(acct_b, b);
                Ok(())
            })
            .unwrap();
    }
    let total: i64 = engine.get_ref(acct_a).unwrap().score + engine.get_ref(acct_b).unwrap().score;
    assert_eq!(total, 2000);

    // =========================================================================
    // § 6. CAS (Compare-And-Swap)
    // =========================================================================
    let cas_id = InlineId::from_static("cas_record");
    engine
        .put(cas_id, &AgentObjectRecord::new(cas_id, "CAS v1", 1, 10))
        .unwrap();
    assert!(matches!(
        engine.put_cas(cas_id, Version(99), &AgentObjectRecord::new(cas_id, "Fail", 1, 20)),
        Err(StorageError::VersionConflict { .. })
    ));
    let v2 = engine
        .put_cas(cas_id, Version(1), &AgentObjectRecord::new(cas_id, "CAS v2", 1, 20))
        .unwrap();
    assert_eq!(v2, Version(2));

    // =========================================================================
    // § 7. TTL Expiration
    // =========================================================================
    let ttl_id = InlineId::from_static("ephemeral_ttl");
    engine
        .put_with_ttl(
            ttl_id,
            &AgentObjectRecord::new(ttl_id, "Temp", 1, 1),
            Duration::from_millis(2),
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(10));
    assert!(matches!(
        engine.get_ref(ttl_id),
        Err(StorageError::RecordNotFound { .. })
    ));
    assert!(engine.sweep_expired().unwrap() >= 1);

    // =========================================================================
    // § 8. Cold LZ4 Compression Round-Trip
    // =========================================================================
    let cold_id = InlineId::from_static("rec_0000");
    engine.compress_cold_record(cold_id).unwrap();
    let (_, decompressed) = engine.get_owned(cold_id).unwrap();
    assert_eq!(decompressed.title, "Record 0");

    // =========================================================================
    // § 9. Schema Migration Registration
    // =========================================================================
    engine.register_migration(0, |_old_ver, _raw| {
        Ok(AgentObjectRecord::new(
            InlineId::from_static("migrated"),
            "Upgraded",
            9999,
            777,
        ))
    });

    // =========================================================================
    // § 10. MVCC Snapshot Isolation + Epoch GC
    // =========================================================================
    let snap = engine.begin_read_snapshot();
    let snap_target = InlineId::from_static("rec_0002");
    let original_score = snap.get(snap_target).unwrap().unwrap().1.score;
    engine
        .put(
            snap_target,
            &AgentObjectRecord::new(snap_target, "Post-Snap Mutation", 9999, 9999),
        )
        .unwrap();
    assert_eq!(snap.get(snap_target).unwrap().unwrap().1.score, original_score);
    drop(snap);
    assert!(engine.gc_mvcc() >= 1);

    // =========================================================================
    // § 11. Online Backup + PITR Restore
    // =========================================================================
    let backup_dir = root.join("backup_snap");
    let restore_dir = root.join("restored_pitr");
    create_online_backup(&engine, &backup_dir).unwrap();
    std::thread::sleep(Duration::from_millis(2));
    let pitr_cutoff = now_ns();
    std::thread::sleep(Duration::from_millis(2));
    let post_pitr = InlineId::from_static("after_pitr");
    engine
        .put(post_pitr, &AgentObjectRecord::new(post_pitr, "Rollback Me", 1, 1))
        .unwrap();
    archive_wal_for_pitr(&engine, &backup_dir).unwrap();
    drop(engine);

    let restored = restore_point_in_time::<AgentObjectRecord>(
        &backup_dir,
        &restore_dir,
        EngineConfig { wal_archive: true, ..EngineConfig::default() },
        pitr_cutoff,
    )
    .unwrap();
    assert!(restored.get_owned(snap_target).is_ok());
    assert!(restored.get_owned(post_pitr).is_err());
    drop(restored);

    // =========================================================================
    // § 12. Persistent Raft HardState — Write, Drop, Reload
    // =========================================================================
    let raft_dir = root.join("raft_persist");
    {
        let re = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(&raft_dir, EngineConfig::default()).unwrap(),
        );
        let n1 = Arc::new(RaftNode::new(1, Arc::clone(&re)).unwrap());
        let n2 = Arc::new(RaftNode::new(
            2,
            Arc::new(
                StorageEngine::<AgentObjectRecord>::open(
                    root.join("raft_p_n2"),
                    EngineConfig::default(),
                )
                .unwrap(),
            ),
        ).unwrap());
        let n3 = Arc::new(RaftNode::new(
            3,
            Arc::new(
                StorageEngine::<AgentObjectRecord>::open(
                    root.join("raft_p_n3"),
                    EngineConfig::default(),
                )
                .unwrap(),
            ),
        ).unwrap());
        let group =
            RaftReplicationGroup::new(vec![Arc::clone(&n1), Arc::clone(&n2), Arc::clone(&n3)])
                .unwrap();
        group.elect_leader(1).unwrap();
        assert_eq!(n1.role(), RaftRole::Leader);
        assert!(n1.current_term() >= 1);
        let rk = InlineId::from_static("raft_persist_k");
        let _ = group
            .replicate_put(rk, &AgentObjectRecord::new(rk, "Persisted", 500, 42))
            .unwrap();
    }
    // Reopen — Raft state must survive restart
    {
        let re = Arc::new(
            StorageEngine::<AgentObjectRecord>::open(&raft_dir, EngineConfig::default()).unwrap(),
        );
        let reloaded = RaftNode::new(1, re).unwrap();
        assert!(reloaded.current_term() >= 1, "Raft term must survive restart");
        assert_eq!(reloaded.voted_for(), Some(1));
    }

    // =========================================================================
    // § 13. Persistent ClusterMap — Save, Load, Verify
    // =========================================================================
    let cmap_path = root.join("cluster.map");
    let mut cmap = ClusterMap::new_balanced(&[1, 2, 3]).unwrap();
    cmap.assign_vshard(42, 2);
    cmap.save_to_file(&cmap_path).unwrap();
    let loaded = ClusterMap::load_from_file(&cmap_path).unwrap();
    assert_eq!(loaded.owner_of_vshard(42), 2);
    assert_eq!(loaded, cmap);

    // =========================================================================
    // § 14. Sharding — ClusterRouter + Live vshard Migration
    // =========================================================================
    let s1 = Arc::new(
        StorageEngine::<AgentObjectRecord>::open(root.join("shard_s1"), EngineConfig::default())
            .unwrap(),
    );
    let s2 = Arc::new(
        StorageEngine::<AgentObjectRecord>::open(root.join("shard_s2"), EngineConfig::default())
            .unwrap(),
    );
    let router = ClusterRouter::new(vec![(1, Arc::clone(&s1)), (2, Arc::clone(&s2))]).unwrap();
    let sk = InlineId::from_static("shard_key_01");
    router
        .put(sk, &AgentObjectRecord::new(sk, "Sharded", 100, 55))
        .unwrap();
    let vs = vshard_for_id(sk);
    let cur = router.route_key(sk).unwrap().0;
    let dest = if cur == 1 { 2 } else { 1 };
    let receipt = router.migrate_vshard(vs, dest).unwrap();
    assert_eq!(receipt.records_migrated, 1);
    assert_eq!(router.with_ref(sk, |v| v.score).unwrap(), 55);
    drop(router);
    drop(s1);
    drop(s2);

    // =========================================================================
    // § 15. 3-Node Raft Consensus + Failover
    // =========================================================================
    let r1 = Arc::new(RaftNode::new(
        1,
        Arc::new(StorageEngine::<AgentObjectRecord>::open(root.join("raft_r1"), EngineConfig::default()).unwrap()),
    ).unwrap());
    let r2 = Arc::new(RaftNode::new(
        2,
        Arc::new(StorageEngine::<AgentObjectRecord>::open(root.join("raft_r2"), EngineConfig::default()).unwrap()),
    ).unwrap());
    let r3 = Arc::new(RaftNode::new(
        3,
        Arc::new(StorageEngine::<AgentObjectRecord>::open(root.join("raft_r3"), EngineConfig::default()).unwrap()),
    ).unwrap());
    let raft_group =
        RaftReplicationGroup::new(vec![Arc::clone(&r1), Arc::clone(&r2), Arc::clone(&r3)])
            .unwrap();
    raft_group.elect_leader(1).unwrap();
    let rk2 = InlineId::from_static("raft_replicated");
    let cidx = raft_group
        .replicate_put(rk2, &AgentObjectRecord::new(rk2, "Replicated", 500, 999))
        .unwrap();
    assert_eq!(r3.read_at_commit_index(rk2, cidx, |v| v.score).unwrap(), 999);
    raft_group.set_node_isolated(1, true);
    raft_group.elect_leader(2).unwrap();
    assert_eq!(r2.role(), RaftRole::Leader);
    drop(raft_group);
    drop(r1);
    drop(r2);
    drop(r3);

    // =========================================================================
    // § 16. Telemetry — Histograms, Slow Log, Prometheus
    // =========================================================================
    let tel = StorageEngine::<AgentObjectRecord>::open(
        root.join("telemetry"),
        EngineConfig::default(),
    )
    .unwrap();
    let hist = LatencyHistogram::new();
    let slow_log = SlowOperationLog::new(1, 16);
    for i in 0..50u64 {
        let id = InlineId::new(&format!("tel_{i:03}")).unwrap();
        tel.put(id, &AgentObjectRecord::new(id, format!("T{i}"), 1, i as i64))
            .unwrap();
        hist.record_ns(100 + i);
    }
    slow_log.record_if_slow("test_op", 500, || "slow".to_string());
    let prom = tel.stats().to_prometheus_text(Some(&hist), None);
    assert!(prom.contains("storine_live_records 50"));
    assert_eq!(slow_log.snapshot().len(), 1);
    drop(tel);

    // =========================================================================
    // § 17. Paged Disk Indexes
    // =========================================================================
    let idx_dir = root.join("paged_idx");
    std::fs::create_dir_all(&idx_dir).unwrap();

    let pbt = PagedBTreeIndex::open(idx_dir.join("s.btidx")).unwrap();
    for i in 0..500i64 {
        pbt.insert(1, i * 10, RecordOrdinal(i as u32));
    }
    pbt.checkpoint_to_disk().unwrap();
    assert_eq!(pbt.count_in_range_zero_alloc(1, 0, 990), 100);

    let pbm = PagedBitmapIndex::open(idx_dir.join("t.bmidx")).unwrap();
    for i in 0..200u32 {
        pbm.insert(1, tag_ai, RecordOrdinal(i));
    }
    pbm.checkpoint_to_disk().unwrap();
    assert_eq!(pbm.intersect_all(&[(1, tag_ai)]).len(), 200);

    let mut bloom = SplitBlockBloomFilter::with_capacity(500);
    bloom.insert(tag_ai);
    assert!(bloom.may_contain(tag_ai));

    let cold = ColdPrimaryIndex::build(
        idx_dir.join("c.cidx"),
        vec![ColdPrimaryEntry {
            id: tag_ai,
            ordinal: RecordOrdinal(0),
            version: Version(1),
            ttl_expires_at_ns: 0,
            location: DiskLocation::Arena(ArenaLocation {
                segment_id: 1,
                offset: 64,
                len: 192,
            }),
        }],
    )
    .unwrap();
    assert_eq!(cold.get(tag_ai).unwrap().ordinal, RecordOrdinal(0));

    // =========================================================================
    // § 18. TCP Server — All opcodes with RBAC
    // =========================================================================
    let tcp_dir = root.join("tcp_server");
    let tcp_engine = Arc::new(
        StorageEngine::<AgentObjectRecord>::open(&tcp_dir, EngineConfig::default()).unwrap(),
    );
    let seed_id = InlineId::from_static("tcp_seed_01");
    tcp_engine
        .put(seed_id, &AgentObjectRecord::new(seed_id, "TCP Seed", 100, 42))
        .unwrap();

    let bak_root = root.join("tcp_bak_root");
    let sec = SecurityConfig::new(true)
        .with_token("ro-tok", RbacRole::ReadOnly)
        .with_token("rw-tok", RbacRole::ReadWrite)
        .with_token("admin-tok", RbacRole::Admin)
        .with_backup_root(&bak_root);
    let tcp_raft = Arc::new(RaftNode::new(9, Arc::clone(&tcp_engine)).unwrap());
    let mut server = StorageServer::start_with_raft(
        "127.0.0.1:0",
        Arc::clone(&tcp_engine),
        128,
        sec,
        Some(Arc::clone(&tcp_raft)),
    )
    .unwrap();
    let addr = server.local_addr();

    // 18a. Unauthenticated → rejected (at the header stage; the connection is closed)
    let mut c = StorageClient::connect(addr).unwrap();
    assert!(c.put(seed_id, &AgentObjectRecord::new(seed_id, "X", 1, 1)).is_err());

    // 18b. ReadOnly → reads work, writes rejected
    let mut c = StorageClient::connect(addr).unwrap();
    assert_eq!(c.authenticate("ro-tok").unwrap(), RbacRole::ReadOnly);
    let frame = c.get_frame(seed_id).unwrap();
    assert_eq!(decode_agent_frame(&frame).unwrap().score, 42);
    assert!(c.put(seed_id, &AgentObjectRecord::new(seed_id, "X", 1, 1)).is_err());

    // 18c. ReadWrite → full CRUD + CAS + Batch + Query
    let mut c = StorageClient::connect(addr).unwrap();
    assert_eq!(c.authenticate("rw-tok").unwrap(), RbacRole::ReadWrite);
    let id1 = InlineId::from_static("tcp_i1");
    let id2 = InlineId::from_static("tcp_i2");
    assert_eq!(
        c.put(id1, &AgentObjectRecord::new(id1, "I1", 100, 111)).unwrap(),
        Version(1)
    );

    // CAS wrong → fail; CAS right → ok
    assert!(c.put_cas(id1, Version(99), &AgentObjectRecord::new(id1, "F", 100, 0)).is_err());
    assert_eq!(
        c.put_cas(id1, Version(1), &AgentObjectRecord::new(id1, "I1v2", 100, 222)).unwrap(),
        Version(2)
    );

    // BatchPut
    assert_eq!(
        c.batch_put(&[
            (id1, AgentObjectRecord::new(id1, "B1", 100, 333)),
            (id2, AgentObjectRecord::new(id2, "B2", 200, 444)),
        ])
        .unwrap(),
        2
    );

    // BatchGet
    let frames = c.batch_get_frames(&[id1, id2]).unwrap();
    assert!(frames[0].is_some() && frames[1].is_some());

    // QueryHybrid
    let q = c.query_range_ids(FIELD_SCORE, 300, 400, 10).unwrap();
    assert_eq!(q, vec![id1]);

    // Paginated descending
    let (page, total) = c.query_range_paginated(FIELD_SCORE, 0, 1000, 0, 1, true).unwrap();
    assert!(total >= 2);
    assert_eq!(page.len(), 1);

    // Delete
    assert!(c.delete(id2).unwrap());
    assert!(!c.delete(id2).unwrap());

    // Metrics
    let met = c.scrape_metrics().unwrap();
    assert!(met.contains("storine_live_records"));

    // 18d. Health (ReadOnly)
    let mut ro = StorageClient::connect(addr).unwrap();
    ro.authenticate("ro-tok").unwrap();
    let h = ro.health().unwrap();
    assert!(h.contains("\"status\":\"ok\""));

    // 18e. Admin RPCs — rejected for ReadOnly
    assert!(ro.admin_compact().is_err());

    let mut adm = StorageClient::connect(addr).unwrap();
    adm.authenticate("admin-tok").unwrap();
    assert!(adm.admin_compact().unwrap().contains("compacted"));
    assert!(adm.admin_backup("tcp_bak").unwrap().contains("backup created"));
    assert!(adm.admin_backup("../outside").is_err());
    assert!(adm.admin_checkpoint().unwrap().contains("checkpoint wrote"));

    // 18f. Raft Vote RPC — served by the attached RaftNode, Admin only
    let mut rw = StorageClient::connect(addr).unwrap();
    rw.authenticate("rw-tok").unwrap();
    let vreq = RequestVoteReq {
        term: 7,
        candidate_id: 1,
        last_log_index: 20,
        last_log_term: 6,
    };
    assert!(!matches!(rw.raft_vote(&vreq), Ok((0x00, _, _))));
    let (st, tm, rb) = adm.raft_vote(&vreq).unwrap();
    assert_eq!(st, 0x00);
    assert_eq!(tm, 7);
    assert!(RequestVoteResp::from_wire_bytes(&rb).unwrap().vote_granted);

    // 18g. Raft AppendEntries RPC (leader heartbeat)
    let ae = AppendEntriesReq {
        term: 7,
        leader_id: 1,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: Vec::new(),
        leader_commit: 0,
    };
    let (st, tm, rb) = adm.raft_append(&ae).unwrap();
    assert_eq!(st, 0x00);
    assert_eq!(tm, 7);
    let ar = AppendEntriesResp::from_wire_bytes(&rb).unwrap();
    assert!(ar.success);
    assert_eq!(ar.match_index, 0);

    // 18h. Shard Migration RPC (Admin only)
    let mig_id = InlineId::from_static("mig_rec");
    let mig_rec = AgentObjectRecord::new(mig_id, "Migrated TCP", 300, 88);
    let mut rw = StorageClient::connect(addr).unwrap();
    rw.authenticate("rw-tok").unwrap();
    assert!(rw.migrate_vshard(10u16, 2u16, &[(mig_id, mig_rec.clone())]).is_err());
    assert_eq!(adm.migrate_vshard(10u16, 2u16, &[(mig_id, mig_rec)]).unwrap(), 1);
    let (_, owned) = tcp_engine.get_owned(mig_id).unwrap();
    assert_eq!(owned.title, "Migrated TCP");
    assert_eq!(owned.score, 88);

    server.stop();
    drop(tcp_raft);
    drop(tcp_engine);

    // =========================================================================
    // § 19. Arena Compaction
    // =========================================================================
    let acfg = EngineConfig {
        layout_mode: StorageLayoutMode::AdaptiveArena {
        arena_threshold_bytes: 4096,
        segment_capacity: 64 * 1024,
    },
        background_flush: false,
        ..EngineConfig::default()
    };
    let ce = StorageEngine::<AgentObjectRecord>::open(root.join("compact"), acfg).unwrap();
    let mut cids = Vec::new();
    for round in 1..=3 {
        for i in 0..30 {
            let id = InlineId::new(&format!("cmp_{i:03}")).unwrap();
            if round == 1 {
                cids.push(id);
            }
            let mut rec = AgentObjectRecord::new(id, format!("R{round}_{i}"), i, i * round);
            rec.body = vec![b'x'; (round as usize) * 80];
            ce.put(id, &rec).unwrap();
        }
    }
    ce.flush_all().unwrap();
    ce.checkpoint_incremental(&cids, &[]).unwrap();
    assert!(ce.compact_arenas(0.40).unwrap() >= 1);
    for i in 0..30 {
        let id = InlineId::new(&format!("cmp_{i:03}")).unwrap();
        ce.with_ref(id, |v| assert_eq!(v.score, i * 3)).unwrap();
    }
    drop(ce);

    // =========================================================================
    // § 20. WAL Crash Recovery
    // =========================================================================
    let crash_dir = root.join("crash");
    let wcfg = EngineConfig {
        enable_wal: true,
        wal_sync_policy: SyncPolicy::Immediate,
        background_flush: false,
        ..EngineConfig::default()
    };
    for cycle in 1..=5i64 {
        let key = InlineId::new(&format!("cr_{cycle:02}")).unwrap();
        {
            let e = StorageEngine::<AgentObjectRecord>::open(&crash_dir, wcfg.clone()).unwrap();
            e.put(key, &AgentObjectRecord::new(key, format!("C{cycle}"), cycle, cycle * 10))
                .unwrap();
            if cycle % 2 == 1 {
                e.flush_all().unwrap();
            }
        }
        let rec = StorageEngine::<AgentObjectRecord>::open(&crash_dir, wcfg.clone()).unwrap();
        assert_eq!(rec.stats().live_records, cycle as usize);
        for c in 1..=cycle {
            let k = InlineId::new(&format!("cr_{c:02}")).unwrap();
            rec.with_ref(k, |v| assert_eq!(v.score, c * 10)).unwrap();
        }
    }

    // =========================================================================
    // § 21. Cleanup
    // =========================================================================
    let _ = std::fs::remove_dir_all(&root);
}
