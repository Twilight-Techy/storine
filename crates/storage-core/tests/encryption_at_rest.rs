//! Encryption at rest (`TASK-084`, `F-SEC-02`): no record plaintext reaches disk or the WAL,
//! reads work with the right key, fail cleanly with a wrong or missing key, and tampering is
//! detected rather than returning altered data.

use std::fs;
use std::path::{Path, PathBuf};

use storage_codec::EncryptionKey;
use storage_core::{AgentObjectRecord, EngineConfig, StorageEngine, StorageLayoutMode, FIELD_SCORE};
use storage_types::InlineId;

const MARKER: &[u8] = b"TOP-SECRET-PAYLOAD-7f3a";

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("storage_encrypt_{name}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn key(byte: u8) -> EncryptionKey {
    EncryptionKey::from_bytes([byte; 32])
}

fn cfg(layout: StorageLayoutMode, k: Option<EncryptionKey>) -> EngineConfig {
    EngineConfig {
        layout_mode: layout,
        encryption_key: k,
        ..EngineConfig::default()
    }
}

fn record(i: u32) -> AgentObjectRecord {
    let id = InlineId::new(&format!("s{i:04}")).unwrap();
    let mut r = AgentObjectRecord::new(id, format!("title {}", String::from_utf8_lossy(MARKER)), 1, i64::from(i));
    r.body = MARKER.repeat(1 + (i as usize % 3) * 40); // small (arena) and large (dedicated file)
    r.embedding = vec![i as f32 * 0.01; 8];
    r
}

fn files_under(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).into_iter().flatten().flatten() {
        let p = entry.path();
        if p.is_dir() {
            files_under(&p, out);
        } else {
            out.push(p);
        }
    }
}

fn assert_no_plaintext(dir: &Path) {
    let mut files = Vec::new();
    files_under(dir, &mut files);
    assert!(!files.is_empty());
    for f in files {
        let bytes = fs::read(&f).unwrap_or_default();
        assert!(
            !bytes.windows(MARKER.len()).any(|w| w == MARKER),
            "plaintext found in {}",
            f.display()
        );
    }
}

#[test]
fn test_encrypted_engine_keeps_plaintext_off_disk_and_reads_back() {
    let layouts = [
        StorageLayoutMode::AdaptiveArena { arena_threshold_bytes: 4096, segment_capacity: 1 << 20 },
        StorageLayoutMode::DedicatedFiles,
    ];
    for (li, layout) in layouts.into_iter().enumerate() {
        let dir = temp_dir(&format!("main{li}"));
        {
            let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg(layout, Some(key(1)))).unwrap();
            for i in 0..200 {
                engine.put(record(i).id, &record(i)).unwrap();
            }
            // Before any flush the frames live only in RAM and the (encrypted) WAL.
            assert_no_plaintext(&dir);
            engine.flush_all().unwrap();
            assert!(engine.decrypted_cache_bytes() > 0, "flushed plaintext is cached");
            for i in (0..200).step_by(7) {
                let r = engine.get_owned(record(i).id).unwrap().1;
                assert_eq!(r, record(i));
            }
            engine.delete(record(5).id).unwrap();
        }
        assert_no_plaintext(&dir);

        // Right key: everything reads back, including secondary and vector indexes.
        {
            let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg(layout, Some(key(1)))).unwrap();
            for i in 0..200u32 {
                let got = engine.get_owned(record(i).id);
                if i == 5 {
                    assert!(got.is_err(), "deleted record must stay deleted");
                } else {
                    assert_eq!(got.unwrap().1, record(i));
                    assert_eq!(engine.with_ref(record(i).id, |v| v.score).unwrap(), i64::from(i));
                }
            }
            let ids = engine.query().filter_range(FIELD_SCORE, 10..=19).execute_ids();
            assert_eq!(ids.len(), 10);
            assert!(!engine.query().top_k_similar(&[0.5; 8], 5).is_empty());
        }

        // Wrong key or no key: reads fail instead of returning garbage.
        {
            let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg(layout, Some(key(2)))).unwrap();
            assert!(engine.get_owned(record(1).id).is_err());
            assert!(engine.with_ref(record(1).id, |v| v.score).is_err());
        }
        {
            let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg(layout, None)).unwrap();
            assert!(engine.get_owned(record(1).id).is_err());
        }
        let _ = fs::remove_dir_all(&dir);
    }
}

#[test]
fn test_tampered_ciphertext_is_detected() {
    let dir = temp_dir("tamper");
    let layout = StorageLayoutMode::AdaptiveArena { arena_threshold_bytes: 4096, segment_capacity: 1 << 20 };
    {
        let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg(layout, Some(key(3)))).unwrap();
        for i in 0..20 {
            engine.put(record(i * 3).id, &record(i * 3)).unwrap();
        }
    }
    // Flip one byte inside every arena segment's record area.
    let mut files = Vec::new();
    files_under(&dir.join("segments"), &mut files);
    for f in &files {
        let mut b = fs::read(f).unwrap();
        if b.len() > 4096 + 200 {
            b[4096 + 150] ^= 0x01;
            fs::write(f, b).unwrap();
        }
    }
    let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg(layout, Some(key(3)))).unwrap();
    let mut failures = 0;
    for i in 0..20 {
        match engine.get_owned(record(i * 3).id) {
            Ok((_, r)) => assert_eq!(r, record(i * 3), "tampering must never yield altered data"),
            Err(_) => failures += 1,
        }
    }
    assert!(failures >= 1, "the corrupted record must fail authentication");
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}
