---
title: Range clusters
description: Split data into key ranges, each replicated by its own Raft group, spread across any number of nodes, with transactions that span them.
---

A **range cluster** splits the key space into **ranges**. Each range is replicated by its own Raft group of three voters, and ranges move between nodes, split as they grow and merge as they shrink. Any node accepts any request and coordinates it, including transactions across ranges on different nodes.

Use it when the data or the write load is more than three machines should each hold in full, or when you want to [place data by region](placement.md).

## Starting one

Add `--ranges` to every node:

```bash
export STORAGE_AUTH_TOKENS="admin-token-0123456789:admin" STORAGE_CLUSTER_TOKEN="node-token-0123456789"
storage-server ./n1 10.0.0.1:7878 --ranges --node-id 1 --peers 1=10.0.0.1:7878,2=10.0.0.2:7878,3=10.0.0.3:7878
# the same on 10.0.0.2 (--node-id 2) and 10.0.0.3 (--node-id 3)
```

On first start every listed node holds one range over the whole key space. As it grows, the range splits and the pieces spread. A restart reopens the node's ranges. With authentication, `STORAGE_CLUSTER_TOKEN` is required.

Clients connect to any node, and list several as seeds:

```text
storine://token@10.0.0.1:7878,10.0.0.2:7878,10.0.0.3:7878
```

## How it behaves

| Setting | Value |
| :--- | :--- |
| Replicas per range | 3 voters (set per collection with [placement rules](placement.md)) |
| Split | above 100,000 keys or 2,000 writes a second |
| Merge | below 10,000 keys |
| Rebalancing | planned every second. A node silent for 10 seconds counts as dead, and its replicas are rebuilt elsewhere |
| Election, heartbeat | 300–600 ms, 50 ms. Idle groups stop heartbeating after 3 idle beats |
| Engine checkpoint | every 5 seconds while written |
| Log kept after a checkpoint | 8,192 entries. A replica further behind gets a snapshot, sent in 64 KiB chunks |
| Client request timeout | 10 seconds |

- **Transactions** that touch several ranges are coordinated by the node that received them, with timestamp ordering. Clocks may differ by at most 500 ms between nodes: keep them synced with NTP.
- **Storage.** Range nodes store their engines with `AsyncMemoryFirst`, without syncing each write, because the Raft logs are the durable record.
- **Leases.** Each range has a leaseholder that serves its writes and current reads. Placement rules can choose which region leases prefer.

## What differs from a single-group cluster

| | Range cluster |
| :--- | :--- |
| Collection listing | Only collections with an index or metadata are listed |
| Membership changes, metrics, compaction and checkpoint requests | Refused. Nodes are listed in `--peers`, and the cluster manages itself |
| Backups | Cluster-wide backups with `remote-backup` and `remote-restore` |
| Point-in-time recovery | Not available. `STORAGE_WAL_ARCHIVE` is refused, since each node's log holds only its own ranges' history |
| Change feeds | Available |

## Cluster backups

With `STORAGE_BACKUP_ROOT` set on the nodes:

```bash
STORAGE_TOKEN=admin-token-0123456789 storage-cli remote-backup 10.0.0.1:7878 nightly
STORAGE_TOKEN=admin-token-0123456789 storage-cli remote-restore 10.0.0.1:7878 nightly   # into an empty cluster
```

## Change feeds

A change feed streams every change in the cluster as JSON lines:

```bash
STORAGE_TOKEN=admin-token-0123456789 storage-cli changefeed 10.0.0.1:7878 changes.jsonl feed.cursor --collection orders
```

It polls for new changes, appends each as a line to the output file, and saves its position in the cursor file, so a restart resumes where it stopped. Delivery is **at least once**: a change may repeat after a restart, so make consumers idempotent, for example by document key and version. `--from wall.logical` starts at a timestamp, `--once` exits when caught up, and `--interval-ms` sets the polling interval. The admin role is required.

The client libraries do not expose change feeds yet.

## Health

Each node's [health report](monitoring.md#health) has `mode: "ranges"`, the number of ranges, log sizes, queue and batch sizes, `dropped_batches`, and `placement`: each range it holds, with its span, its voters, and whether this node leads it.
