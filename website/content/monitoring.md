---
title: Monitoring
description: The health report, Prometheus metrics and slow-request log, what each field means, and what to alert on.
---

## Health

`storage-cli remote-health <addr>` prints a node's health report as JSON. Any role may read it. A standalone server:

```json
{"status":"ok","live_records":0,"index_capacity":3584,"total_reads":0,"total_writes":0,"hot_dirty_bytes":0,"arena":{"segments":1,"used":0,"dead":0,"live":0,"compactions":0,"removed":0,"errors":0,"last_error":""},"encryption":"off"}
```

| Field | Meaning |
| :--- | :--- |
| `status` | `ok` while the node serves |
| `live_records` | Documents stored |
| `index_capacity` | Documents the index holds before it next grows. `reserve` and `STORAGE_EXPECTED_DOCUMENTS` raise it |
| `total_reads`, `total_writes` | Since the server started |
| `hot_dirty_bytes` | Written but not yet flushed to the record files. Stays low unless the disk falls behind |
| `arena.segments`, `used`, `dead`, `live` | Segment files and their bytes. `dead` is space that compaction will reclaim |
| `arena.compactions`, `removed`, `errors`, `last_error` | Compactions run, segments they removed, and failures |
| `encryption` | `"off"`, or the active key, the keys held, and `accept_plaintext` |
| `wal_archive` | The archive folder, when `STORAGE_WAL_ARCHIVE` is set |

A range node reports instead `mode: "ranges"`, `ranges`, `log_entries`, `log_bytes`, queue and batch sizes, `dropped_batches`, and:

| Field | Meaning |
| :--- | :--- |
| `placement` | Each range the node holds: its span, its voters, and whether this node leads it |
| `placement_rules` | With `--zones`: the rules file, and the error that kept its last change from applying, or `null` |

## Metrics

`storage-cli remote-metrics <addr>` prints Prometheus text format, on standalone and single-group nodes:

| Metric | Type | Meaning |
| :--- | :--- | :--- |
| `storine_live_records` | gauge | Documents stored |
| `storine_total_reads`, `storine_total_writes` | counter | Operations since start |
| `storine_coalesced_flushes` | counter | Writes to one key merged before reaching disk |
| `storine_hot_dirty_bytes` | gauge | Unflushed bytes |
| `storine_arena_segments` | gauge | Segment files open |
| `storine_arena_used_bytes`, `storine_arena_dead_bytes` | gauge | Segment bytes written, and dead |
| `storine_arena_compactions`, `storine_arena_compacted_segments`, `storine_arena_compaction_errors` | counter | Compactions, segments they removed, failures |
| `storine_read_latency_ns`, `storine_write_latency_ns` | summary | Quantiles 0.5, 0.99 and 0.999 |
| `storine_slow_rpcs` | gauge | Requests over 10 ms held in the slow-request log, which keeps the last 256 |

The server has no HTTP endpoint. To feed Prometheus, run the scrape from a small exporter script or a textfile collector, for example every 15 seconds:

```bash
storage-cli remote-metrics 127.0.0.1:7878 > /var/lib/node_exporter/storine.prom.tmp && \
  mv /var/lib/node_exporter/storine.prom.tmp /var/lib/node_exporter/storine.prom
```

## Clusters

`storage-cli cluster <addr> members` prints the members, their roles, the leader, the term and the commit index. The console's overview shows each node's commit and applied indexes live.

## What to alert on

| Signal | Why |
| :--- | :--- |
| `remote-health` fails or `status` is not `ok` | The node is down or unreachable |
| No leader, or the term rising quickly | A cluster that keeps re-electing has a network or overload problem |
| A follower's applied index falling behind the commit index | The node is slow or partitioned |
| `hot_dirty_bytes` near 64 MiB | The disk cannot keep up; writers will start to flush themselves |
| `arena.errors` or `storine_arena_compaction_errors` rising | Compaction is failing; disk space will not be reclaimed |
| `arena.dead` large and growing | Compaction has not run; see [storage](storage.md#compaction) |
| Disk free space | The WAL, segments and archive all need room |
| p99 write latency rising | Disk or replication slowdown |
| `placement_rules` holds an error | A rules change was refused |

## Logs

The server prints only its start line and messages that need attention, such as a reseal report or a failure to prune archived WAL. It has no log file: run it under systemd or another supervisor that keeps its output. The SQL gateway logs every statement with `STORINE_SQL_LOG=1`.
