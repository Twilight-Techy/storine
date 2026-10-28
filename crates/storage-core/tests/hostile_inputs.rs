//! Hostile-input sweeps for every `storage-core` decoder and for a whole database directory
//! (`RULE-TEST-01`, `RULE-ERR-01`): Raft wire messages, Raft persistent state, `ClusterMap`,
//! backup manifests, and engine open + full read-back after any single file is damaged.
//! Every case must return `Ok` or `Err` — never panic.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use storage_core::backup::load_backup_manifest;
use storage_core::{
    create_online_backup, AgentObjectRecord, AppendEntriesReq, AppendEntriesResp, ClusterMap,
    EngineConfig, RaftLogEntry, RaftNode, RequestVoteReq, RequestVoteResp, StorageEngine,
    StorageLayoutMode, FIELD_PRIMARY_TAG, FIELD_SCORE,
};
use storage_types::testing::SweepRng;
use storage_types::{InlineId, RecordFlags, Version};
use storage_codec::RecordCodec;

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("storage_core_hostile_{name}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn files_under(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = fs::read_dir(dir) else { return };
    for entry in rd.flatten() {
        let p = entry.path();
        if p.is_dir() {
            files_under(&p, out);
        } else if p.extension().is_none_or(|e| e != "lock") {
            out.push(p);
        }
    }
}

fn copy_dir(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for entry in fs::read_dir(src).unwrap().flatten() {
        let to = dst.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &to);
        } else if entry.path().extension().is_none_or(|e| e != "lock") {
            fs::copy(entry.path(), to).unwrap();
        }
    }
}

fn record(i: u32) -> AgentObjectRecord {
    let id = InlineId::new(&format!("rec_{i:04}")).unwrap();
    let mut r = AgentObjectRecord::new(id, format!("title {i}"), i64::from(i), i64::from(i) * 3);
    r.tags = vec![InlineId::from_static("even"), InlineId::new(&format!("t{}", i % 5)).unwrap()];
    r.embedding = vec![(i % 7) as f32 * 0.1; 8];
    if i % 4 == 0 {
        r.body = vec![b'x'; 6_000]; // large: dedicated file in the adaptive layout
    }
    r
}

#[test]
fn test_hostile_raft_wire_messages_never_panic() {
    let mut rng = SweepRng::new(21);
    let frame = RecordCodec::encode_aligned(&record(1), Version(1), 1, RecordFlags::NONE, 0);
    let append = AppendEntriesReq {
        term: 3,
        leader_id: 1,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![
            RaftLogEntry::record(3, 1, InlineId::new("a").unwrap(), Some(frame)),
            RaftLogEntry::record(3, 2, InlineId::new("b").unwrap(), None),
        ],
        leader_commit: 1,
    }
    .to_wire_bytes();
    let vote = RequestVoteReq { term: 2, candidate_id: 1, last_log_index: 4, last_log_term: 1, pre_vote: false }.to_wire_bytes();
    let vote_resp = RequestVoteResp { term: 2, vote_granted: true }.to_wire_bytes();
    let append_resp = AppendEntriesResp { term: 2, success: true, match_index: 9 }.to_wire_bytes();
    for _ in 0..20_000 {
        let _ = AppendEntriesReq::from_wire_bytes(&rng.hostile_copy(&append));
        let _ = RequestVoteReq::from_wire_bytes(&rng.hostile_copy(&vote));
        let _ = RequestVoteResp::from_wire_bytes(&rng.hostile_copy(&vote_resp));
        let _ = AppendEntriesResp::from_wire_bytes(&rng.hostile_copy(&append_resp));
    }
}

#[test]
fn test_hostile_cluster_map_and_manifest_never_panic() {
    let dir = temp_dir("map");
    fs::create_dir_all(&dir).unwrap();
    let map_path = dir.join("cluster.map");
    ClusterMap::new_balanced(&[1, 2, 3]).unwrap().save_to_file(&map_path).unwrap();
    let good_map = fs::read(&map_path).unwrap();

    let db = dir.join("db");
    let engine = StorageEngine::<AgentObjectRecord>::open(&db, EngineConfig::default()).unwrap();
    for i in 0..5 {
        engine.put(record(i).id, &record(i)).unwrap();
    }
    let backup = dir.join("backup");
    create_online_backup(&engine, &backup).unwrap();
    let manifest_path = backup.join("backup.meta");
    let good_manifest = fs::read(&manifest_path).unwrap();

    let mut rng = SweepRng::new(22);
    for round in 0..400 {
        let bytes = if round < 40 { good_map[..good_map.len() * round / 40].to_vec() } else { rng.hostile_copy(&good_map) };
        fs::write(&map_path, bytes).unwrap();
        let _ = ClusterMap::load_from_file(&map_path);
        let bytes = if round < 40 {
            good_manifest[..good_manifest.len() * round / 40].to_vec()
        } else {
            rng.hostile_copy(&good_manifest)
        };
        fs::write(&manifest_path, bytes).unwrap();
        let _ = load_backup_manifest(&manifest_path);
    }
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_corrupting_any_database_file_never_panics_on_open_or_read() {
    let base = temp_dir("db");
    let pristine = base.join("pristine");
    let cfg = || EngineConfig {
        layout_mode: StorageLayoutMode::AdaptiveArena { arena_threshold_bytes: 4096, segment_capacity: 1 << 20 },
        ..EngineConfig::default()
    };
    let ids: Vec<InlineId> = (0..60).map(|i| record(i).id).collect();
    {
        let engine = Arc::new(StorageEngine::<AgentObjectRecord>::open(&pristine, cfg()).unwrap());
        for i in 0..60 {
            engine.put(ids[i as usize], &record(i)).unwrap();
        }
        engine.flush_all().unwrap();
        // Leave some writes only in the WAL and the primary-index delta.
        for i in 0..10u32 {
            let mut r = record(i);
            r.score += 1_000;
            engine.put(ids[i as usize], &r).unwrap();
        }
        engine.delete(ids[59]).unwrap();
        let raft = RaftNode::new(1, Arc::clone(&engine)).unwrap();
        let frame = RecordCodec::encode_aligned(&record(77), Version(1), 1, RecordFlags::NONE, 0);
        let _ = raft.handle_append_entries(AppendEntriesReq {
            term: 1,
            leader_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![RaftLogEntry::record(1, 1, record(77).id, Some(frame))],
            leader_commit: 0,
        });
    }

    let mut files = Vec::new();
    files_under(&pristine, &mut files);
    assert!(files.len() >= 4, "expected several database files, found {files:?}");
    let mut rng = SweepRng::new(23);
    for round in 0..60 {
        let work = base.join(format!("round_{round}"));
        copy_dir(&pristine, &work);
        let mut copies = Vec::new();
        files_under(&work, &mut copies);
        copies.sort();
        let victim = &copies[rng.below(copies.len())];
        let good = fs::read(victim).unwrap();
        fs::write(victim, rng.hostile_copy(&good)).unwrap();

        if let Ok(engine) = StorageEngine::<AgentObjectRecord>::open(&work, cfg()) {
            let engine = Arc::new(engine);
            for &id in &ids {
                let _ = engine.get_owned(id);
                let _ = engine.with_ref(id, |v| v.score);
                if let Ok(r) = engine.get_ref(id) {
                    let _ = (r.score, r.view().get_str(&r.title).len());
                }
            }
            let _ = engine
                .query()
                .filter_tag(FIELD_PRIMARY_TAG, InlineId::from_static("even"))
                .filter_range(FIELD_SCORE, 0..=10_000)
                .execute_ids();
            let _ = engine.query().top_k_similar(&[0.2; 8], 5);
            let _ = engine.put(ids[1], &record(1));
            let _ = RaftNode::new(1, Arc::clone(&engine));
            let _ = engine.flush_all();
        }
        let _ = fs::remove_dir_all(&work);
    }
    let _ = fs::remove_dir_all(&base);
}
