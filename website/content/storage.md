---
title: Storage and disk
description: What is in a data directory, how writes reach disk, checkpoints, recovery after a crash, compaction, and how much memory and disk to plan for.
---

## The data directory

```text
data/
  documents.lock                      held while a process has the directory open
  segments/documents/seg_00001.dat    arena segments: the documents
  wal/documents.wal                   the write-ahead log, cut at each checkpoint
  indexes/documents_primary.idx       primary index checkpoint; documents_hnsw.idx, _range.idx and
                                      _tags.idx hold the others once they exist
```

Cluster nodes also keep their Raft logs and snapshots in the data directory. Two processes cannot open one directory at once.

Files move freely between machines and operating systems: checksums are bit-identical whether computed with CPU instructions or without.

## Records

Each record is a 64-byte header followed by its payload. The header holds the record's version, schema version, flags, timestamps and lengths, a hash of the payload (an XXH64-family hash combined with CRC32C), and a checksum of the header itself. CRC32C uses the CPU instruction (SSE4.2 on x86-64, the CRC extension on ARMv8) when present.

Documents are stored in **arena segments**: memory-mapped files that start at 4 KiB and double up to 64 MiB. Documents over 64 KiB get a file of their own. Reads map the files and decode records in place, without copying or allocating. A record's bytes are validated once, when first mapped; later reads are bounds-checked, so a corrupted or hostile file cannot cause undefined behaviour.

## The write-ahead log

A write goes first to the **write-ahead log** (WAL), then to an in-memory buffer of recent changes, where readers see it at once. A background flusher writes buffered records to their files every 10 ms, and several writes to one key in that time are written once.

A server syncs the WAL to disk before it acknowledges a write, and shares each sync between the writers that arrive together (group commit). An acknowledged write survives a power failure. If the WAL hits an I/O error, the server stops accepting writes rather than acknowledge one whose durability it cannot promise.

Embedded programs can choose other sync policies: see [embedding](embedding.md#sync-policies).

## Checkpoints

A **checkpoint** flushes every buffered record, syncs the record files and segments, and cuts the WAL, so recovery only replays what came after.

| Who | Checkpoints |
| :--- | :--- |
| Standalone server | every 10 seconds while it is written |
| Single-group cluster node | every 10 seconds if something was applied, then compacts its Raft log |
| Range-cluster node | every 5 seconds while it is written |
| Embedded program | when it calls `checkpoint()` or `flush_all()`, and when the engine is dropped |

`storage-cli remote-checkpoint` asks for one now. Writers that log faster than the checkpoint can copy are slowed to about half its pace while it finishes, rather than stopped, so a checkpoint always ends.

## Recovery

At start, the server replays the WAL in bounded batches (up to 4,096 entries or 32 MiB), ignores a torn final frame, and applies only committed transactions. Records touched by a batch are written once, and their files synced in parallel. A crash at any moment, including during a checkpoint, loses nothing that was acknowledged.

## Compaction

Rewritten and deleted documents leave dead space in segments. **Compaction** copies a segment's live records forward and deletes the old file. It runs on its own when at least 64 MiB of a store is dead and dead space is at least half of what is used, and on demand with `storage-cli remote-compact`. Until it runs, segments can hold up to twice the live data.

## Memory and disk to plan for

- The primary index takes about 21 bytes per document on disk, and more in memory.
- The buffer of unflushed writes is capped at 64 MiB.
- Memory-mapped files count towards the page cache, not the process's own heap. At most 16,384 mappings and 4 GiB are mapped at once; colder ones are unmapped.
- Segments can hold up to twice the live data before compaction.
- With `STORAGE_WAL_ARCHIVE`, plan for the archive too: about as much as you write in the retention period.

Measured in the 0.2.0 release soaks (2 hours under constant faults, 4-vCPU VMs): a single-group node peaked at 850–940 MiB resident and 50–145 MiB on disk; a range node at 630–750 MiB resident and 104–228 MiB on disk, flat over time.
