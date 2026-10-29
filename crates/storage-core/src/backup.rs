//! Online Backup & Point-in-Time Recovery (PITR) (`TASK-041`, `F-DURA-05`, `F-OPS-04`).
//!
//! - `create_online_backup` copies a consistent snapshot of a live engine (writers pause only
//!   while dirty records are flushed and files are copied; readers continue) and records the WAL
//!   LSN the snapshot covers.
//! - `archive_wal_for_pitr` ships the WAL history written since then. It requires
//!   `EngineConfig::wal_archive`, because checkpoints otherwise discard WAL frames.
//! - `restore_point_in_time` restores the snapshot and replays archived WAL entries in LSN order,
//!   stopping at the first entry newer than the target timestamp, so the result is a consistent
//!   prefix of the commit history.

use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use storage_codec::{checksum_payload, ZeroCopyRecord};
use storage_index::Indexable;
use storage_mmap::atomic_write_replace;
use storage_types::{RecordHeader, Result, StorageError};
use storage_wal::{GroupCommitWal, WalOpKind};

use crate::engine::{ensure_fresh_destination, EngineConfig, StorageEngine};

const BACKUP_META_MAGIC: u32 = 0x424B_5550; // "BKUP"

/// Metadata manifest describing an online backup and its WAL LSN watermark (`F-DURA-05`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackupManifest {
    pub collection_name: String,
    /// Every WAL operation with `lsn <= base_lsn` is contained in the backup's data files.
    pub base_lsn: u64,
    pub created_at_ns: u64,
    pub files_copied: usize,
}

/// Creates an online backup of `engine` at `dest_dir` (which must be empty or absent and outside
/// the database directory) (`TASK-041`).
pub fn create_online_backup<T: ZeroCopyRecord + Indexable>(
    engine: &StorageEngine<T>,
    dest_dir: &Path,
) -> Result<BackupManifest> {
    let (files_copied, base_lsn) = engine.create_snapshot_with_lsn(dest_dir)?;
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

/// Copies the WAL history of `engine` into `backup_dir/wal_pitr` so Point-in-Time Recovery can
/// replay mutations that happened after the base backup. Returns the number of segment files
/// copied. Requires `EngineConfig::wal_archive = true`.
pub fn archive_wal_for_pitr<T: ZeroCopyRecord + Indexable>(
    engine: &StorageEngine<T>,
    backup_dir: &Path,
) -> Result<usize> {
    let Some(archive_dir) = engine.wal_archive_dir().map(Path::to_path_buf) else {
        return Err(StorageError::invalid_key(
            "PITR requires EngineConfig::wal_archive = true (checkpoints otherwise discard WAL history)",
        ));
    };
    let wal_pitr_dir = backup_dir.join("wal_pitr");
    storage_types::vfs::at(&wal_pitr_dir).create_dir_all(&wal_pitr_dir)
        .map_err(|e| StorageError::io("create wal_pitr directory", e))?;

    // Checkpoint first: every frame up to the checkpoint is moved into the archive.
    engine.flush_all()?;

    let mut copied = 0usize;
    for seg in GroupCommitWal::list_archived_segments(&archive_dir)? {
        let Some(fname) = seg.file_name() else {
            continue;
        };
        let dst = wal_pitr_dir.join(fname);
        let same_size = fs::metadata(&dst)
            .ok()
            .zip(fs::metadata(&seg).ok())
            .is_some_and(|(a, b)| a.len() == b.len());
        if !same_size {
            storage_types::vfs::at(&dst).copy(&seg, &dst)
                .map_err(|e| StorageError::io("copy archived WAL segment to backup_dir", e))?;
            copied += 1;
        }
    }
    // Frames written concurrently after the checkpoint are still in the active WAL.
    if engine.copy_active_wal(&wal_pitr_dir.join("wal_active.log"))? {
        copied += 1;
    }
    Ok(copied)
}

/// Restores `backup_dir` into `restore_dir` (which must be empty or absent) and replays archived
/// PITR WAL entries in LSN order up to `target_timestamp_ns` (inclusive) (`TASK-041`, `F-DURA-05`).
pub fn restore_point_in_time<T: ZeroCopyRecord + Indexable>(
    backup_dir: &Path,
    restore_dir: &Path,
    config: EngineConfig,
    target_timestamp_ns: u64,
) -> Result<StorageEngine<T>> {
    if !backup_dir.exists() {
        return Err(StorageError::invalid_key("backup_dir does not exist"));
    }
    let manifest = load_backup_manifest(&backup_dir.join("backup.meta"))?;
    ensure_fresh_destination(restore_dir, Some(backup_dir))?;
    copy_dir_excluding_pitr(backup_dir, restore_dir)?;

    let engine = StorageEngine::<T>::open(restore_dir, config)?;

    let wal_pitr_dir = backup_dir.join("wal_pitr");
    if wal_pitr_dir.exists() {
        let mut entries = Vec::new();
        for seg in GroupCommitWal::list_archived_segments(&wal_pitr_dir)? {
            entries.extend(GroupCommitWal::recover_committed(&seg)?);
        }
        entries.retain(|e| {
            e.lsn > manifest.base_lsn
                && (e.collection_id == T::COLLECTION_ID || e.collection_id == 0)
        });
        entries.sort_by_key(|e| e.lsn);
        entries.dedup_by_key(|e| e.lsn);

        // Keep the longest LSN-ordered prefix whose timestamps are `<= target`.
        let mut last_ts = 0u64;
        let mut cut = entries.len();
        for (i, entry) in entries.iter().enumerate() {
            let ts = match entry.kind {
                WalOpKind::Upsert => RecordHeader::from_slice_verified(&entry.frame_bytes)
                    .map_or(last_ts, |h| h.updated_at_ns),
                WalOpKind::Delete => entry.delete_timestamp_ns().unwrap_or(last_ts),
                _ => last_ts,
            };
            if ts > target_timestamp_ns {
                cut = i;
                break;
            }
            last_ts = ts;
        }
        entries.truncate(cut);
        engine.replay_wal_entries(&entries)?;
        engine.flush_all()?;
    }

    Ok(engine)
}

fn copy_dir_excluding_pitr(src: &Path, dst: &Path) -> Result<()> {
    storage_types::vfs::at(dst).create_dir_all(dst).map_err(|e| StorageError::io("create restore dir", e))?;
    for entry in fs::read_dir(src).map_err(|e| StorageError::io("read backup dir", e))? {
        let entry = entry.map_err(|e| StorageError::io("read backup dir entry", e))?;
        let fname = entry.file_name();
        if fname == "wal_pitr" || fname == "backup.meta" {
            continue;
        }
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "lock") {
            continue;
        }
        let dst_path = dst.join(fname);
        if path.is_dir() {
            copy_dir_excluding_pitr(&path, &dst_path)?;
        } else {
            storage_types::vfs::at(&dst_path).copy(&path, &dst_path).map_err(|e| StorageError::io("copy backup file", e))?;
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
    use storage_types::wire::{read_u32_le, read_u64_le};
    let bytes = fs::read(path).map_err(|e| StorageError::io("read backup.meta", e))?;
    if bytes.len() < 40 {
        return Err(StorageError::corrupted_header("backup.meta truncated"));
    }
    if read_u32_le(&bytes, 0) != BACKUP_META_MAGIC {
        return Err(StorageError::corrupted_header("backup.meta invalid magic"));
    }
    let expected_crc = read_u64_le(&bytes, 4);
    let payload = &bytes[12..];
    let actual_crc = checksum_payload(payload);
    if actual_crc != expected_crc {
        return Err(StorageError::checksum_mismatch(expected_crc, actual_crc));
    }

    let base_lsn = read_u64_le(payload, 0);
    let created_at_ns = read_u64_le(payload, 8);
    let files_copied = read_u64_le(payload, 16) as usize;
    let name_len = read_u32_le(payload, 24) as usize;
    let name_slice = payload
        .get(28..28usize.saturating_add(name_len))
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
        let cfg = EngineConfig {
            wal_archive: true,
            ..EngineConfig::default()
        };

        let engine = StorageEngine::<AgentObjectRecord>::open(&db_dir, cfg.clone()).unwrap();
        let id1 = InlineId::from_static("pitr_rec_01");
        let id2 = InlineId::from_static("pitr_rec_02");
        let id3 = InlineId::from_static("pitr_rec_03");

        // 1. Insert id1 and take online backup
        engine
            .put(id1, &AgentObjectRecord::new(id1, "Record 1 Base", 100, 10))
            .unwrap();
        let manifest = create_online_backup(&engine, &backup_dir).unwrap();
        assert!(manifest.files_copied >= 1);

        // Backups refuse to overwrite existing data.
        assert!(create_online_backup(&engine, &backup_dir).is_err());
        assert!(create_online_backup(&engine, &db_dir.join("nested")).is_err());

        // 2. Write id2 after backup, then a checkpoint (which used to destroy PITR history)
        engine
            .put(id2, &AgentObjectRecord::new(id2, "Record 2 Before Target TS", 200, 20))
            .unwrap();
        engine.flush_all().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let target_ts_ns = now_ns();
        std::thread::sleep(std::time::Duration::from_millis(2));

        // 3. Write id3 and delete id1 AFTER target_ts_ns
        engine
            .put(id3, &AgentObjectRecord::new(id3, "Record 3 After Target TS", 300, 30))
            .unwrap();
        engine.delete(id1).unwrap();
        let archived = archive_wal_for_pitr(&engine, &backup_dir).unwrap();
        assert!(archived >= 1);
        drop(engine);

        // 4. Restore to `target_ts_ns`!
        let restored = restore_point_in_time::<AgentObjectRecord>(
            &backup_dir,
            &restore_dir,
            cfg.clone(),
            target_ts_ns,
        )
        .unwrap();

        assert!(restored.get_owned(id1).is_ok(), "delete after target must not be replayed");
        assert_eq!(
            restored.get_owned(id2).unwrap().1.title,
            "Record 2 Before Target TS"
        );
        assert!(
            restored.get_owned(id3).is_err(),
            "id3 committed after target_ts_ns must not be replayed during PITR"
        );
        drop(restored);

        // Restoring into a non-empty directory is refused instead of deleting it.
        assert!(restore_point_in_time::<AgentObjectRecord>(
            &backup_dir,
            &restore_dir,
            cfg,
            target_ts_ns,
        )
        .is_err());
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn test_pitr_requires_wal_archive() {
        let base = std::env::temp_dir().join("stge_test_pitr_requires_archive");
        let _ = std::fs::remove_dir_all(&base);
        let engine =
            StorageEngine::<AgentObjectRecord>::open(base.join("db"), EngineConfig::default())
                .unwrap();
        assert!(archive_wal_for_pitr(&engine, &base.join("bak")).is_err());
        drop(engine);
        let _ = std::fs::remove_dir_all(&base);
    }
}
