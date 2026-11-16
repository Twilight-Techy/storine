---
title: Architecture
description: How Storine is put together, from the crates in the workspace to the path a request takes through the server, the engine and the disk.
---

Storine is a Rust workspace with no third-party runtime crates. `unsafe` code is confined to two crates, `storage-codec` and `storage-mmap`, and every release checks them with Miri and AddressSanitizer.

```text
 clients ──► storage-server ─────────────┐        storine-sql ──► (embedded store or a server)
 (8 SDKs,     reactor shards, TLS,       │        storine-console ──► (any server, via the Rust SDK)
  console)    RBAC, admission control    │
                       │                 │
              ┌────────┴────────┐  Raft (single group)  /  range cluster (many Raft groups)
              ▼                 ▼
         DocumentStore ── StorageEngine<T> ── WAL (storage-wal)
                               │
          primary index · secondary, vector and columnar indexes (storage-index, storage-columnar)
                               │
              arena segments / record files, memory-mapped (storage-mmap)
```

## Crates

| Crate | Contents |
| :--- | :--- |
| `storage-types` | `InlineId` (keys up to 23 bytes, stored inline), the 64-byte `RecordHeader`, versions, `Decimal`, errors |
| `storage-codec` | The zero-copy archive format, document values, checksums (XXH64-family and CRC32C), vector scoring, SIMD column kernels, and the cryptography: ChaCha20-Poly1305, SHA-2, x25519, Ed25519 |
| `storage-mmap` | Memory mapping on Unix and Windows, the bounded mapping pool, arena segments, atomic file replacement, and the network pollers (`epoll`, `kqueue`, AFD poll) |
| `storage-wal` | The group-commit write-ahead log and its recovery |
| `storage-index` | The primary index and its checkpoints, tag bitmaps, numeric ranges, paged index tiers, the HNSW vector index |
| `storage-columnar` | Column segments for analytics |
| `storage-graph` | One-to-one, one-to-many and many-to-many relationships |
| `storage-core` | `StorageEngine<T>`, `DocumentStore`, queries, analytics, Raft and range clusters |
| `storage-tls` | TLS 1.3 and 1.2, and certificate issuing |
| `storage-server` | `storage-server`, the network server |
| `storage-cli` | `storage-cli` |
| `storage-sql` | `storine-sql`, the SQL gateway |
| `storage-console` | `storine-console`, the web console |

## The path of a write

1. A **reactor** thread reads the 29-byte request header and runs admission control: the opcode, the connection's role, the body size and the rate limit, all before reading the body.
2. The body is decoded and the request runs. On a standalone server it goes straight to the `DocumentStore`. On a cluster leader it is proposed to Raft, and waits until a majority has logged it.
3. The store checks the request id against the last 24 hours, checks each precondition, applies field operations to private copies, and writes the batch to the **WAL** as one atomic frame group.
4. The WAL syncs, shared with concurrent writers (group commit). The records go into the in-memory buffer of recent changes, where reads see them at once, and the indexes are updated.
5. The client gets the new versions. Within about 10 ms the background flusher writes the records to arena segments; every 10 seconds a checkpoint syncs them and cuts the WAL.

## The path of a read

The primary index maps the key to a location: the buffer of recent changes, or a record in a memory-mapped segment. The record is decoded in place, with no copy and no heap allocation on a warm read. Queries use secondary indexes to propose candidates, check every candidate against the filter, and read from one MVCC snapshot.

## Clusters

- A **single-group cluster** is one Raft group over the whole store. The leader's log is written outside the Raft state lock so concurrent writes share disk syncs; followers acknowledge before applying, and apply in batches.
- A **range cluster** runs many Raft groups, one per key range, with an allocator on each node that splits, merges and moves ranges, and places them by the placement rules. Transactions across ranges are coordinated with timestamps, tolerating 500 ms of clock offset.

## Design rules

The project's rules, enforced by CI and the release checks:

- no third-party runtime dependencies;
- `unsafe` only in `storage-codec` and `storage-mmap`, each block justified;
- zero heap allocations on warm read paths, checked by a counting allocator in the benchmarks;
- no panics on I/O, network input or untrusted bytes;
- on-disk formats identical on every build and platform;
- binary-size ceilings: 5.5 MiB `storage-server`, 1.75 MiB `storage-cli`, 8 MiB `storine-sql`, 1 MiB `storine-console`.

See [contributing](../../../CONTRIBUTING.md) and the [release process](../../../docs/RELEASE.md).
