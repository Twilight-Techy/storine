//! Online Non-Blocking Backup & Point-in-Time Recovery (PITR) (`TASK-041`, `F-DURA-05`, `F-OPS-04`).
//!
//! Allows taking hot online backups of a live `StorageEngine<T>` while concurrent readers and
//! writers continue operating, and restoring the database to any historical nanosecond timestamp
//! (`target_timestamp_ns`) by replaying archived WAL segments up to that exact point in time.

use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use storage_codec::{checksum_payload, ZeroCopyRecord};
use storage_index::Indexable;
use storage_mmap::atomic_write_replace;
use storage_types::{RecordHeader, Result, StorageError};
use storage_wal::{GroupCommitWal, WalOpKind};

use crate::engine::{EngineConfig, StorageEngine};

const BACKUP_META_MAGIC: u32 = 0x424B_5550; // "BKUP"

/// Metadata manifest describing an online backup and its WAL LSN watermark (`F-DURA-05`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackupManifest {
    pub collection_name: String,
    pub base_lsn: u64,
    pub created_at_ns: u64,
    pub files_copied: usize,
}

/// Creates an online hot backup of `engine` at `dest_dir`, rotating and archiving the WAL segment
/// so subsequent writes can be incrementally replayed for Point-in-Time Recovery (`TASK-041`).
pub fn create_online_backup<T: ZeroCopyRecord + Indexable>(
    engine: &StorageEngine<T>,
    dest_dir: &Path,
) -> Result<BackupManifest> {
    let _snap = engine.begin_read_snapshot();
    let base_lsn = engine.current_wal_lsn();
    let files_copied = engine.create_snapshot(dest_dir)?;
    let created_at_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;

    let manifest = BackupManifest {
        collection_name: engine.collection_name().to_string(),
        base_lsn,
        created_at_ns,
        files_copied,
    };
    save_backup_manifest(&dest_dir.join("backup.meta"), &manifest)?;
    Ok(manifest)
}

/// Appends incremental archived WAL segments from a live `source_engine_dir` into `backup_dir/wal_pitr`
/// so Point-in-Time Recovery can replay mutations that occurred after the base backup was taken.
pub fn archive_wal_for_pitr<T: ZeroCopyRecord + Indexable>(
    engine: &StorageEngine<T>,
    backup_dir: &Path,
) -> Result<usize> {
    let wal_pitr_dir = backup_dir.join("wal_pitr");
    if !wal_pitr_dir.exists() {
        fs::create_dir_all(&wal_pitr_dir)
            .map_err(|e| StorageError::io("create wal_pitr directory", e))?;
    }
    if let Some(seg_path) = engine.rotate_wal_segment()? {
        if let Some(fname) = seg_path.file_name() {
            let dst = wal_pitr_dir.join(fname);
            fs::copy(&seg_path, &dst)
                .map_err(|e| StorageError::io("copy rotated WAL segment to backup_dir", e))?;
            return Ok(1);
        }
    }
    Ok(0)
}

/// Restores a `StorageEngine<T>` from `backup_dir` into `restore_dir`, replaying archived PITR WAL
/// segments only up to `target_timestamp_ns` (inclusive) (`TASK-041`, `F-DURA-05`).
pub fn restore_point_in_time<T: ZeroCopyRecord + Indexable>(
    backup_dir: &Path,
    restore_dir: &Path,
    config: EngineConfig,
    target_timestamp_ns: u64,
) -> Result<StorageEngine<T>> {
    if !backup_dir.exists() {
        return Err(StorageError::invalid_key("backup_dir does not exist"));
    }
    let _manifest = load_backup_manifest(&backup_dir.join("backup.meta"))?;

    if restore_dir.exists() {
        fs::remove_dir_all(restore_dir)
            .map_err(|e| StorageError::io("clear restore_dir", e))?;
    }
    copy_dir_excluding_pitr(backup_dir, restore_dir)?;

    let engine = StorageEngine::<T>::open(restore_dir, config)?;

    // Replay incremental PITR WAL segments (`backup_dir/wal_pitr/wal_XXXXXX.log`) up to `target_timestamp_ns`
    let wal_pitr_dir = backup_dir.join("wal_pitr");
    if wal_pitr_dir.exists() {
        let segs = GroupCommitWal::list_archived_segments(&wal_pitr_dir)?;
        let mut last_seen_ts = 0u64;
        for seg in segs {
            let entries = GroupCommitWal::recover_committed(&seg)?;
            for entry in entries {
                if entry.collection_id != T::COLLECTION_ID && entry.collection_id != 0 {
                    continue;
                }
                let entry_ts = match entry.kind {
                    WalOpKind::Upsert => {
                        if let Ok(hdr) = RecordHeader::from_slice_verified(&entry.frame_bytes) {
                            last_seen_ts = hdr.updated_at_ns;
                            hdr.updated_at_ns
                        } else {
                            last_seen_ts
                        }
                    }
                    _ => last_seen_ts,
                };
                if entry_ts <= target_timestamp_ns {
                    engine.replay_wal_entry(&entry)?;
                }
            }
        }
        engine.flush_all()?;
    }

    Ok(engine)
}

fn copy_dir_excluding_pitr(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst).map_err(|e| StorageError::io("create restore dir", e))?;
    for entry in fs::read_dir(src).map_err(|e| StorageError::io("read backup dir", e))? {
        let entry = entry.map_err(|e| StorageError::io("read backup dir entry", e))?;
        let fname = entry.file_name();
        if fname == "wal_pitr" || fname == "backup.meta" {
            continue;
        }
        let path = entry.path();
        let dst_path = dst.join(fname);
        if path.is_dir() {
            copy_dir_excluding_pitr(&path, &dst_path)?;
        } else {
            fs::copy(&path, &dst_path).map_err(|e| StorageError::io("copy backup file", e))?;
        }
    }
    Ok(())
}

fn save_backup_manifest(path: &Path, manifest: &BackupManifest) -> Result<()> {
    let mut payload = Vec::with_capacity(32 + manifest.collection_name.len());
    payload.extend_from_slice(&manifest.base_lsn.to_le_bytes());
    payload.extend_from_slice(&manifest.created_at_ns.to_le_bytes());
    payload.extend_from_slice(&(manifest.files_copied as u64).to_le_bytes());
    let name_bytes = manifest.collection_name.as_bytes();
    payload.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
    payload.extend_from_slice(name_bytes);

    let crc = checksum_payload(&payload);
    let mut full = Vec::with_capacity(12 + payload.len());
    full.extend_from_slice(&BACKUP_META_MAGIC.to_le_bytes());
    full.extend_from_slice(&crc.to_le_bytes());
    full.extend_from_slice(&payload);
    atomic_write_replace(path, &full, true)
}

pub fn load_backup_manifest(path: &Path) -> Result<BackupManifest> {
    let bytes = fs::read(path).map_err(|e| StorageError::io("read backup.meta", e))?;
    if bytes.len() < 40 {
        return Err(StorageError::corrupted_header("backup.meta truncated"));
    }
    let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    if magic != BACKUP_META_MAGIC {
        return Err(StorageError::corrupted_header("backup.meta invalid magic"));
    }
    let expected_crc = u64::from_le_bytes([
        bytes[4], bytes[5], bytes[6], bytes[7], bytes[8], bytes[9], bytes[10], bytes[11],
    ]);
    let payload = &bytes[12..];
    let actual_crc = checksum_payload(payload);
    if actual_crc != expected_crc {
        return Err(StorageError::checksum_mismatch(expected_crc, actual_crc));
    }

    let base_lsn = u64::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3], payload[4], payload[5], payload[6],
        payload[7],
    ]);
    let created_at_ns = u64::from_le_bytes([
        payload[8],
        payload[9],
        payload[10],
        payload[11],
        payload[12],
        payload[13],
        payload[14],
        payload[15],
    ]);
    let files_copied = u64::from_le_bytes([
        payload[16],
        payload[17],
        payload[18],
        payload[19],
        payload[20],
        payload[21],
        payload[22],
        payload[23],
    ]) as usize;
    let name_len = u32::from_le_bytes([payload[24], payload[25], payload[26], payload[27]]) as usize;
    let name_slice = payload
        .get(28..28 + name_len)
        .ok_or_else(|| StorageError::corrupted_header("backup.meta name out of bounds"))?;
    let collection_name = core::str::from_utf8(name_slice)
        .map_err(|_| StorageError::corrupted_header("backup.meta invalid UTF-8"))?
        .to_string();

    Ok(BackupManifest {
        collection_name,
        base_lsn,
        created_at_ns,
        files_copied,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::{EngineConfig, StorageEngine};
    use crate::model::AgentObjectRecord;
    use std::time::{SystemTime, UNIX_EPOCH};
    use storage_types::InlineId;

    fn now_ns() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }

    #[test]
    fn test_online_backup_and_pitr_timestamp_recovery() {
        let base = std::env::temp_dir().join("stge_test_pitr_suite");
        let _ = std::fs::remove_dir_all(&base);
        let db_dir = base.join("live_db");
        let backup_dir = base.join("backup_snap");
        let restore_dir = base.join("restored_db");

        let engine =
            StorageEngine::<AgentObjectRecord>::open(&db_dir, EngineConfig::default()).unwrap();
        let id1 = InlineId::from_static("pitr_rec_01");
        let id2 = InlineId::from_static("pitr_rec_02");
        let id3 = InlineId::from_static("pitr_rec_03");

        // 1. Insert id1 and take online backup
        engine
            .put(id1, &AgentObjectRecord::new(id1, "Record 1 Base", 100, 10))
            .unwrap();
        let manifest = create_online_backup(&engine, &backup_dir).unwrap();
        assert!(manifest.files_copied >= 1);

        // 2. Write id2 after backup and capture target timestamp for PITR
        engine
            .put(id2, &AgentObjectRecord::new(id2, "Record 2 Before Target TS", 200, 20))
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let target_ts_ns = now_ns();
        std::thread::sleep(std::time::Duration::from_millis(2));

        // 3. Write id3 AFTER target_ts_ns (simulating an accidental write we want to roll back before)
        engine
            .put(id3, &AgentObjectRecord::new(id3, "Record 3 After Target TS", 300, 30))
            .unwrap();
        let archived = archive_wal_for_pitr(&engine, &backup_dir).unwrap();
        assert_eq!(archived, 1);
        drop(engine);

        // 4. Restore to `target_ts_ns`!
        let restored = restore_point_in_time::<AgentObjectRecord>(
            &backup_dir,
            &restore_dir,
            EngineConfig::default(),
            target_ts_ns,
        )
        .unwrap();

        // id1 (from base backup) and id2 (<= target_ts_ns) must be present, while id3 (> target_ts_ns) must NOT exist!
        assert!(restored.get_owned(id1).is_ok());
        assert_eq!(
            restored.get_owned(id2).unwrap().1.title,
            "Record 2 Before Target TS"
        );
        assert!(
            restored.get_owned(id3).is_err(),
            "id3 committed after target_ts_ns must not be replayed during PITR"
        );

        drop(restored);
        let _ = std::fs::remove_dir_all(&base);
    }
}


