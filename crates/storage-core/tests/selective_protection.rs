//! Per-record protection (`TASK-086`, `F-SEC-02`): a record type's `Indexable::protection`
//! policy chooses, record by record, between encryption (confidential) and authentication-only
//! (plaintext but tamper-proof).

use std::fs;
use std::path::{Path, PathBuf};

use storage_codec::{impl_archive_pod, ArchiveBuilder, ArchivedString, EncryptionKey, ZeroCopyRecord};
use storage_core::{EncryptionConfig, EngineConfig, StorageEngine, StorageLayoutMode};
use storage_index::{Indexable, RecordProtection};
use storage_types::{InlineId, Result};

const PUBLIC: &[u8] = b"PUBLIC-NOTE-BODY-91c2";
const SECRET: &[u8] = b"SECRET-NOTE-BODY-4e7d";

#[derive(Clone, Debug, PartialEq)]
struct Note {
    id: InlineId,
    public: bool,
    text: String,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct ArchivedNote {
    id: InlineId,
    public: u64,
    text: ArchivedString,
}

impl_archive_pod!(ArchivedNote {
    id: InlineId,
    public: u64,
    text: ArchivedString,
});

impl ZeroCopyRecord for Note {
    type Archived = ArchivedNote;
    const SCHEMA_VERSION: u16 = 1;
    const COLLECTION_ID: u16 = 41;

    fn archive(&self, builder: &mut ArchiveBuilder<'_>) -> Self::Archived {
        ArchivedNote {
            id: self.id,
            public: u64::from(self.public),
            text: builder.write_str(&self.text),
        }
    }

    fn validate_archived(root: &Self::Archived, payload: &[u8]) -> Result<()> {
        InlineId::from_raw_bytes(root.id.to_raw_bytes())?;
        root.text.validate(payload)
    }

    fn unarchive(root: &Self::Archived, payload: &[u8]) -> Result<Self> {
        Ok(Self {
            id: root.id,
            public: root.public != 0,
            text: root.text.as_str(payload).to_owned(),
        })
    }
}

impl Indexable for Note {
    /// Public notes are integrity-protected only; everything else is encrypted.
    fn protection(root: &Self::Archived, _payload: &[u8]) -> RecordProtection {
        if root.public != 0 {
            RecordProtection::Authenticate
        } else {
            RecordProtection::Encrypt
        }
    }
}

fn note(i: u32) -> Note {
    let public = i % 2 == 0;
    let marker = if public { PUBLIC } else { SECRET };
    Note {
        id: InlineId::new(&format!("n{i:04}")).unwrap(),
        public,
        text: format!("{} #{i}", String::from_utf8_lossy(marker)),
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("storage_selective_{name}_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn cfg() -> EngineConfig {
    EngineConfig {
        layout_mode: StorageLayoutMode::AdaptiveArena { arena_threshold_bytes: 4096, segment_capacity: 1 << 20 },
        encryption: Some(EncryptionConfig::strict(1, EncryptionKey::from_bytes([3; 32]))),
        ..EngineConfig::default()
    }
}

fn file_contains(dir: &Path, needle: &[u8]) -> Vec<(PathBuf, usize)> {
    let mut hits = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if let Ok(bytes) = fs::read(&p) {
                if let Some(pos) = bytes.windows(needle.len()).position(|w| w == needle) {
                    hits.push((p, pos));
                }
            }
        }
    }
    hits
}

#[test]
fn test_policy_encrypts_secret_records_and_authenticates_public_ones() {
    let dir = temp_dir("policy");
    {
        let engine = StorageEngine::<Note>::open(&dir, cfg()).unwrap();
        for i in 0..100 {
            engine.put(note(i).id, &note(i)).unwrap();
        }
    }
    // Secret notes never reach disk in plaintext; public notes are stored readable.
    assert!(file_contains(&dir, SECRET).is_empty(), "secret plaintext on disk");
    let public_hits = file_contains(&dir, PUBLIC);
    assert!(!public_hits.is_empty(), "public notes are authenticated, not encrypted");

    {
        let engine = StorageEngine::<Note>::open(&dir, cfg()).unwrap();
        for i in 0..100 {
            assert_eq!(engine.get_owned(note(i).id).unwrap().1, note(i));
        }
        // Policy unchanged: nothing to re-seal.
        let report = engine.reseal_all().unwrap();
        assert_eq!(report.resealed, 0);
        assert_eq!(report.unchanged, 100);
    }

    // Tamper with a public (plaintext) note on disk: authentication catches it.
    let (path, pos) = public_hits
        .iter()
        .find(|(p, _)| p.to_string_lossy().contains("segments"))
        .cloned()
        .expect("public note in an arena segment");
    let mut bytes = fs::read(&path).unwrap();
    bytes[pos] ^= 0x20; // 'P' -> 'p'
    fs::write(&path, bytes).unwrap();
    let engine = StorageEngine::<Note>::open(&dir, cfg()).unwrap();
    let mut failures = 0;
    for i in (0..100).step_by(2) {
        match engine.get_owned(note(i).id) {
            Ok((_, n)) => assert_eq!(n, note(i), "tampering must never yield altered data"),
            Err(_) => failures += 1,
        }
    }
    assert_eq!(failures, 1, "exactly the tampered public note fails authentication");
    drop(engine);
    let _ = fs::remove_dir_all(&dir);
}
