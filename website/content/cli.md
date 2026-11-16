---
title: Command-line tool
description: storage-cli checks health, scrapes metrics, takes and restores backups, changes cluster membership and follows change feeds.
---

```text
storage-cli <command> [args...]
storage-cli --help
storage-cli --version
```

## Remote commands

Remote commands talk to a running server. They take an address as `host:port` with an **IP address**, not a host name.

| Environment variable | Meaning |
| :--- | :--- |
| `STORAGE_TOKEN` | Token to authenticate with |
| `STORAGE_TLS_CA` | PEM certificates to trust. Turns TLS on |
| `STORAGE_TLS_SERVER_NAME` | The name to expect in the server's certificate. Default: its address |

| Command | Role | Does |
| :--- | :--- | :--- |
| `remote-health <addr>` | read | Print the node's [health report](monitoring.md#health) as JSON |
| `remote-metrics <addr>` | read | Print [metrics](monitoring.md#metrics) in Prometheus text format. Standalone and single-group nodes |
| `remote-checkpoint <addr>` | admin | Checkpoint now |
| `remote-compact <addr>` | admin | Compact arena segments now |
| `remote-backup <addr> <name>` | admin | Take an online backup into `STORAGE_BACKUP_ROOT/<name>` on the server. On a range cluster, a cluster-wide backup |
| `remote-restore <addr> <name>` | admin | Restore a cluster backup into an empty range cluster |
| `changefeed <addr> <out.jsonl> <cursor-file> [options]` | admin | Follow a range cluster's changes into a JSON-lines file |
| `cluster <addr> [members]` | read | Members, their roles, the leader, the term and the commit index |
| `cluster <addr> add <id> <host:port>` | admin | Add a voter. It joins as a learner and is promoted once caught up |
| `cluster <addr> add-learner <id> <host:port>` | admin | Add a non-voting learner |
| `cluster <addr> promote <id>` | admin | Promote a caught-up learner to voter |
| `cluster <addr> remove <id>` | admin | Remove a member, the leader included |

Membership changes can be sent to any member: a follower's redirect to the leader is followed. See [clusters](clusters.md#changing-membership).

```bash
export STORAGE_TOKEN=ops-token-0123456789abcdef
storage-cli remote-health 10.0.0.1:7878
storage-cli cluster 10.0.0.1:7878 members
storage-cli remote-backup 10.0.0.1:7878 nightly-2026-11-16
```

### Change feeds

```text
storage-cli changefeed <addr> <out.jsonl> <cursor-file> [--collection C] [--from wall.logical] [--once] [--interval-ms N]
```

| Option | Meaning |
| :--- | :--- |
| `--collection C` | Only this collection's changes |
| `--from wall.logical` | Start at this timestamp instead of the cursor file's |
| `--once` | Read what is there, then exit |
| `--interval-ms N` | How often to poll for more |

Each change is one JSON line, appended to the output file. The feed is at-least-once: after a restart it resumes from the cursor file, and a change may repeat. Range clusters only. More in [range clusters](range-clusters.md#change-feeds).

## Point-in-time recovery

```text
storage-cli restore-pitr <backup> <restore-dir> <time> [--archive DIR]... [--wal FILE]...
```

Restores a backup as it was at `<time>` (RFC 3339, or nanoseconds since the epoch) into a new directory, replaying archived WAL (`--archive`, a server's `STORAGE_WAL_ARCHIVE/documents`) and WAL files (`--wal`). It runs offline, without a server. The steps are in [backups and recovery](backups.md#point-in-time-recovery).

## Local commands

These open a data directory directly, with the server stopped:

| Command | Does |
| :--- | :--- |
| `stats <db_dir>` | Engine and memory-mapping statistics |
| `verify <db_dir>` | Check every record's checksums and relationships |
| `compact <db_dir> [min_ratio]` | Compact fragmented arena segments |
| `snapshot <db_dir> <dest_dir>` | Copy a consistent snapshot |
| `backup <db_dir> <backup_dir>` | An online backup with the WAL position it covers |
| `archive-wal <db_dir> <backup_dir>` | Archive the WAL into a backup, for point-in-time recovery |
| `inspect-header <rec_file>` | Print and verify a record file's 64-byte header |
| `get <db_dir> <id>`, `put <db_dir> <id> <title> <score> [tags]` | Read and write a demo record |
| `bench <db_dir> [count]` | A quick throughput benchmark |

> [!WARNING]
> Apart from `restore-pitr` and `inspect-header`, the local commands open the engine's typed demo record store (collection `default`), not the `documents` store a server writes. For a server's data, use the remote commands, or stop the server and copy its directory.
