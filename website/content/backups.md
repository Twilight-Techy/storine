---
title: Backups and recovery
description: Online backups of a running server, offline copies, cluster backups, and point-in-time recovery to any moment covered by the archived write-ahead log.
---

| Method | Works on | Restores to |
| :--- | :--- | :--- |
| [Online backup](#online-backups) | standalone and single-group servers | the moment of the backup |
| [Offline copy](#offline-copies) | any server, stopped | the moment it stopped |
| [Point-in-time recovery](#point-in-time-recovery) | standalone and single-group servers | any moment in the archive |
| [Cluster backup](#cluster-backups) | range clusters | the moment of the backup |

Test your restores: a backup you have never restored is a hope, not a backup.

## Online backups

Set `STORAGE_BACKUP_ROOT` on the server to a folder where backups may go. Then, with an admin token:

```bash
STORAGE_TOKEN=ops-token-0123456789abcdef storage-cli remote-backup 10.0.0.1:7878 nightly-2026-11-16
```

The server writes a consistent copy of the database to `STORAGE_BACKUP_ROOT/nightly-2026-11-16`, with a manifest of the write-ahead-log position it covers, while it keeps serving. The name is a relative path without `..`, `:` or a backslash. Without `STORAGE_BACKUP_ROOT`, remote backups are refused. At most 2 admin jobs run at once.

To restore, stop the server (or use another machine) and start a server on a copy of the backup folder. Copy the backup off the machine, for example to object storage, so it survives losing the disk.

## Offline copies

Stop the server and copy its data directory. Start a server on the copy to restore it. This is the simplest backup of all, and the only one for the local `storage-cli` commands.

## Point-in-time recovery

Point-in-time recovery restores the database as it was at any moment: just before a bad deploy deleted a collection, for example.

### Set it up

1. Start the server with `STORAGE_WAL_ARCHIVE` set, ideally to a folder on another disk than the data:

   ```bash
   STORAGE_WAL_ARCHIVE=/mnt/archive STORAGE_BACKUP_ROOT=/mnt/backups storage-server /var/lib/storine 10.0.0.1:7878
   ```

   Each checkpoint (every 10 seconds while the server is written) appends the WAL it cuts to `/mnt/archive/documents/`. Archived WAL older than `STORAGE_WAL_ARCHIVE_RETAIN` (7 days by default; also `48h` or `90m`) is deleted. Keep it longer than the time between backups.

2. Take backups regularly with `storage-cli remote-backup`.

The health report shows `wal_archive`, the archive folder, so you can check it is on.

### Recover

With the server stopped:

```bash
storage-cli restore-pitr /mnt/backups/nightly-2026-11-16 /var/lib/storine-restored 2026-11-16T14:05:00Z \
  --archive /mnt/archive/documents \
  --wal /var/lib/storine/wal/documents.wal
```

- The time is RFC 3339, or nanoseconds since the epoch.
- `--archive` reads the archived WAL. Repeat it for several folders.
- `--wal` adds a WAL file, here the live one, for the writes since the last checkpoint. Leave it out if the data directory was lost.
- With encryption at rest, run it with the same `STORAGE_ENCRYPTION_KEYS`.

The restored directory holds every write up to that moment and none after. Start a server on it, check it, then swap it in.

A single-group node's restore is one node's data, which is all of it: start it as a standalone server, or found a new cluster from it.

Range clusters refuse `STORAGE_WAL_ARCHIVE`, because each node's log holds only its own ranges' history. Use cluster backups there.

## Cluster backups

On a range cluster, with `STORAGE_BACKUP_ROOT` set on the nodes, `remote-backup` takes a backup of the whole cluster, and `remote-restore` restores one into an empty cluster:

```bash
storage-cli remote-backup 10.0.0.1:7878 nightly
storage-cli remote-restore 10.0.0.1:7878 nightly
```

## Embedded programs

The same pieces are functions in `storage-core`: `EngineConfig::wal_archive` or `wal_archive_dir`, `create_online_backup`, `archive_wal_for_pitr` and `restore_point_in_time_with`. See [embedding](embedding.md#backups-and-point-in-time-recovery).
