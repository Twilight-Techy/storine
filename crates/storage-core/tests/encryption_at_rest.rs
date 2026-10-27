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
        encryption: k.map(|k| storage_core::EncryptionConfig::strict(1, k)),
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

/// Index files (`TASK-088`) must not reveal record ids either: `s0123` is a record id.
fn assert_index_files_sealed(dir: &Path) {
    let mut files = Vec::new();
    files_under(&dir.join("indexes"), &mut files);
    assert!(!files.is_empty(), "index files written");
    for f in files {
        let bytes = fs::read(&f).unwrap_or_default();
        assert!(bytes.starts_with(b"STGESEL1"), "index file not sealed: {}", f.display());
        assert!(!bytes.windows(5).any(|w| w == b"s0123"), "record id in {}", f.display());
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
        assert_index_files_sealed(&dir);

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

        // Wrong key or no key: the sealed primary index cannot be opened, so the engine refuses
        // to open instead of serving errors or garbage.
        assert!(StorageEngine::<AgentObjectRecord>::open(&dir, cfg(layout, Some(key(2)))).is_err());
        assert!(StorageEngine::<AgentObjectRecord>::open(&dir, cfg(layout, None)).is_err());
        // The key still works afterwards (failed opens changed nothing).
        {
            let engine = StorageEngine::<AgentObjectRecord>::open(&dir, cfg(layout, Some(key(1)))).unwrap();
            assert_eq!(engine.get_owned(record(1).id).unwrap().1, record(1));
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

fn arena() -> StorageLayoutMode {
    StorageLayoutMode::AdaptiveArena { arena_threshold_bytes: 4096, segment_capacity: 1 << 20 }
}

thread_local! {
    static LAYOUT: std::cell::Cell<StorageLayoutMode> = std::cell::Cell::new(arena());
}

fn open_with(dir: &Path, enc: Option<storage_core::EncryptionConfig>) -> storage_types::Result<StorageEngine<AgentObjectRecord>> {
    let layout_mode = LAYOUT.with(std::cell::Cell::get);
    StorageEngine::<AgentObjectRecord>::open(dir, EngineConfig { layout_mode, encryption: enc, ..EngineConfig::default() })
}

fn assert_all_readable(engine: &StorageEngine<AgentObjectRecord>, n: u32) {
    for i in 0..n {
        assert_eq!(engine.get_owned(record(i).id).unwrap().1, record(i), "record {i}");
    }
}

/// Plaintext database -> encrypted online -> key rotation -> back to plaintext, with every step
/// verified on disk and every record readable throughout.
#[test]
fn test_enable_rotate_and_disable_encryption_online() {
    for (i, layout) in [
        arena(),
        StorageLayoutMode::DedicatedFiles,
        StorageLayoutMode::PingPongFiles { slot_stride: 4096 },
    ]
    .into_iter()
    .enumerate()
    {
        LAYOUT.with(|l| l.set(layout));
        lifecycle(&format!("lifecycle{i}"));
    }
}

fn lifecycle(name: &str) {
    use storage_codec::KeyRing;
    use storage_core::EncryptionConfig;
    let dir = temp_dir(name);
    {
        let engine = open_with(&dir, None).unwrap();
        for i in 0..150 {
            engine.put(record(i).id, &record(i)).unwrap();
        }
    }

    // 1. Turn encryption on for existing plaintext data.
    {
        let engine = open_with(&dir, Some(EncryptionConfig {
            keys: KeyRing::new().with_key(1, key(1)),
            active_key: Some(1),
            accept_unencrypted: true,
        }))
        .unwrap();
        assert_all_readable(&engine, 150);
        let report = engine.reseal_all().unwrap();
        assert_eq!(report.resealed, 150);
        assert_eq!(engine.reseal_all().unwrap().resealed, 0, "idempotent");
        // Switch to strict at runtime: plaintext no longer accepted, everything still reads.
        engine.set_encryption(Some(EncryptionConfig::strict(1, key(1)))).unwrap();
        assert_all_readable(&engine, 150);
    }
    assert_no_plaintext(&dir);
    assert_index_files_sealed(&dir);
    assert_all_readable(&open_with(&dir, Some(EncryptionConfig::strict(1, key(1)))).unwrap(), 150);

    // 2. Rotate 1 -> 2 online, with a concurrent writer.
    {
        let engine = std::sync::Arc::new(open_with(&dir, Some(EncryptionConfig {
            keys: KeyRing::new().with_key(1, key(1)).with_key(2, key(2)),
            active_key: Some(2),
            accept_unencrypted: false,
        }))
        .unwrap());
        let writer = {
            let engine = std::sync::Arc::clone(&engine);
            std::thread::spawn(move || {
                for i in 150..250 {
                    engine.put(record(i).id, &record(i)).unwrap();
                }
            })
        };
        let report = engine.reseal_all().unwrap();
        writer.join().unwrap();
        assert!(report.resealed > 0);
        engine.reseal_all().unwrap();
        assert_all_readable(&engine, 250);
    }
    assert_no_plaintext(&dir);
    // Only key 2 is needed now.
    {
        let engine = open_with(&dir, Some(EncryptionConfig::strict(2, key(2)))).unwrap();
        assert_all_readable(&engine, 250);
    }
    assert_index_files_sealed(&dir);
    assert!(open_with(&dir, Some(EncryptionConfig::strict(1, key(1)))).is_err(), "retired key opens nothing");

    // 3. Turn encryption off.
    {
        let engine = open_with(&dir, Some(EncryptionConfig {
            keys: KeyRing::new().with_key(2, key(2)),
            active_key: None,
            accept_unencrypted: true,
        }))
        .unwrap();
        assert_eq!(engine.reseal_all().unwrap().resealed, 250);
    }
    let mut index_files = Vec::new();
    files_under(&dir.join("indexes"), &mut index_files);
    assert!(index_files.iter().all(|f| !fs::read(f).unwrap_or_default().starts_with(b"STGESEL1")));
    assert_all_readable(&open_with(&dir, None).unwrap(), 250);

    // Invalid configurations are refused.
    assert!(open_with(&dir, Some(EncryptionConfig {
        keys: KeyRing::new().with_key(1, key(1)),
        active_key: Some(9),
        accept_unencrypted: false,
    }))
    .is_err());
    assert!(open_with(&dir, Some(EncryptionConfig {
        keys: KeyRing::new().with_key(1, key(1)),
        active_key: None,
        accept_unencrypted: false,
    }))
    .is_err());
    let _ = fs::remove_dir_all(&dir);
}

/// Paged secondary indexes (`TASK-088`): tag values, numeric keys and ordinals are encrypted in
/// the generation files, reused after a clean restart, and rebuilt (never trusted) when a
/// generation is tampered with.
#[test]
fn test_paged_secondary_indexes_are_encrypted_and_tamper_checked() {
    use storage_core::{IndexTier, FIELD_PRIMARY_TAG};
    const TAG: &str = "tagsecret-q7";
    let dir = temp_dir("paged");
    let config = || EngineConfig {
        layout_mode: arena(),
        index_tier: IndexTier::Paged,
        encryption: Some(storage_core::EncryptionConfig::strict(1, key(1))),
        ..EngineConfig::default()
    };
    let tagged = |i: u32| {
        let mut r = record(i);
        if i % 3 == 0 {
            r.tags = vec![InlineId::new(TAG).unwrap()];
        }
        r
    };
    // Paged generation files are named `<manifest>.<generation as 16 hex digits>`.
    let is_generation = |f: &Path| f.to_string_lossy().contains(".idx.");
    let check = |engine: &StorageEngine<AgentObjectRecord>| {
        let tag = InlineId::new(TAG).unwrap();
        assert_eq!(engine.query().filter_tag(FIELD_PRIMARY_TAG, tag).execute_ids().len(), 100);
        assert_eq!(engine.query().filter_range(FIELD_SCORE, 100..=149).execute_ids().len(), 50);
    };
    {
        let engine = StorageEngine::<AgentObjectRecord>::open(&dir, config()).unwrap();
        for i in 0..300 {
            engine.put(tagged(i).id, &tagged(i)).unwrap();
        }
        engine.flush_all().unwrap();
        check(&engine);
    }

    // Every index file is sealed (paged generations carry their encryption section); no tag
    // text or record id appears anywhere on disk.
    let mut files = Vec::new();
    files_under(&dir.join("indexes"), &mut files);
    let mut generations = 0;
    for f in &files {
        let bytes = fs::read(f).unwrap();
        assert!(!bytes.windows(TAG.len()).any(|w| w == TAG.as_bytes()), "tag text in {}", f.display());
        assert!(!bytes.windows(5).any(|w| w == b"s0123"), "record id in {}", f.display());
        if is_generation(f) {
            generations += 1;
            assert!(bytes.get(128..132) == Some(&b"PENC"[..]), "unsealed generation {}", f.display());
        }
    }
    assert_eq!(generations, 2, "range + tag generations");
    assert_no_plaintext(&dir);

    // Clean restart: the encrypted generations are verified and reused.
    {
        let engine = StorageEngine::<AgentObjectRecord>::open(&dir, config()).unwrap();
        assert!(engine.secondary_indexes_reused());
        check(&engine);
    }

    // A flipped ciphertext byte in a generation fails its tag: the index is rebuilt, answers stay exact.
    // (The reopen above checkpointed into a new generation: list the files again.)
    let mut files = Vec::new();
    files_under(&dir.join("indexes"), &mut files);
    let generation = files.iter().find(|f| is_generation(f)).unwrap();
    let mut bytes = fs::read(generation).unwrap();
    bytes[4096 + 10] ^= 0x01;
    fs::write(generation, bytes).unwrap();
    {
        let engine = StorageEngine::<AgentObjectRecord>::open(&dir, config()).unwrap();
        assert!(!engine.secondary_indexes_reused(), "tampered generation must not be trusted");
        check(&engine);
    }
    let _ = fs::remove_dir_all(&dir);
}
