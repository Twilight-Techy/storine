---
title: Embedding the engine
description: Use Storine as a library inside a Rust program, with a document store or typed zero-copy records, and configure its durability, memory, encryption and backups.
---

The engine is the `storage-core` crate. A program that links it reads and writes a data directory directly, with no server and no network. It is the same engine a server runs.

```toml
[dependencies]
storage-core = { git = "https://github.com/twilight-techy/storine" }
storage-codec = { git = "https://github.com/twilight-techy/storine" }   # values and field operations
storage-types = { git = "https://github.com/twilight-techy/storine" }   # Decimal, errors, ids
```

There are two kinds of store:

| Store | Holds | Use it for |
| :--- | :--- | :--- |
| `DocumentStore` | Documents in collections, as a server does | Most programs. The same data model as the clients |
| `StorageEngine<T>` | Records of one Rust type `T`, stored as a zero-copy archive | The lowest latency: reads decode the record in place with no allocation |

Two processes cannot open one data directory at once: the first holds `documents.lock` while it has it open.

## A document store

This program is in the repository as [`crates/storage-core/examples/documents.rs`](../../crates/storage-core/examples/documents.rs), and the test suite compiles it:

```rust
{{include:crates/storage-core/examples/documents.rs}}
```

Run it with `cargo run -p storage-core --example documents`.

### Its methods

| Method | Does |
| :--- | :--- |
| `open(dir, config)` | Opens or creates a store in `dir`, replaying the write-ahead log if the last run did not close cleanly |
| `get(collection, key)` | The `Document` (`key`, `version`, `fields`), or `None` |
| `get_fields(collection, key, &paths)` | Only those dotted paths, up to 256 |
| `read_bytes(collection, key, field, offset, length)` | A range of up to 16 MiB of a bytes or text field |
| `put`, `insert`, `delete` | Create or replace; create only; delete (returns whether it existed) |
| `patch(collection, key, ops)` | Apply `FieldOp`s (`Set`, `Unset`, `Add`, `Append`) to the current version |
| `write(writes, request_id)` | Many `DocWrite`s across collections, atomically, each with a `Precondition` (`None`, `Absent`, `Exists`, `Version`). A request id is applied at most once |
| `query(&DocQuery)` | Filter, sort, limit, offset or `after_key`, and `nearest` for vector search. Returns a `DocPage` |
| `scan_collection(collection)` | Every document of a collection |
| `create_index`, `drop_index` | [Secondary indexes](indexes.md) |
| `create_vector_index(collection, field, dims)`, `drop_vector_index` | [Vector indexes](indexes.md#vector-indexes) |
| `create_columnar`, `drop_columnar`, `columnar` | [Columnar storage](analytics.md#columnar-storage) |
| `aggregate(&AggregateQuery)`, `scan_columns(&ScanQuery)` | [Analytics](analytics.md) |
| `list_collections()`, `collection_info(name)` | Collections with metadata or documents, and one collection's indexes and metadata |
| `set_collection_extra(collection, map)` | Store metadata on a collection |
| `drop_collection(name)` | Delete a collection's documents, indexes and metadata. Returns the number of documents |
| `engine()` | The underlying `StorageEngine<DocumentRecord>`, for `checkpoint`, `stats`, `compact_arenas`, snapshots and backups |

> [!IMPORTANT]
> An embedded store does not checkpoint on its own. Call `store.engine().checkpoint()` from time to time (a server does it every 10 seconds while written), or the write-ahead log grows until the store is dropped, and the next open replays all of it.

## Typed records

`StorageEngine<T>` stores one Rust type. `T` implements the engine's `ZeroCopyRecord` and `Indexable` traits, which describe its archived layout and which fields are indexed. `AgentObjectRecord` in `crates/storage-core/src/model.rs` is a complete example with numeric and tag indexes, relationships and an embedding.

Reads are zero-copy: `with_ref(id, |view| ...)` and `get_ref(id)` give a view into the memory-mapped record, with no heap allocation on a warm read. Writes include `insert`, `put`, `put_with_ttl`, `update`, `put_cas` and `update_if_version` (compare-and-swap on the version), `delete`, and `transaction` for atomic batches.

### Relationships

Typed records can link to each other one-to-one, one-to-many and many-to-many (`OneToOne<T>`, `OneToMany<T>`, `ManyToMany<T>`). Link updates lock keys in a fixed order, so concurrent updates cannot deadlock. `update_pair_atomic` changes two records together, `get_related_refs` follows links, and `verify_relationships` reports links to records that no longer exist.

### Schema migrations

Give a record type a `SCHEMA_VERSION`. Records written by an older version are upgraded when they are read or replayed, by migrations you register with `register_migration` or `register_step_upcaster`. `upgrade_legacy_schemas` rewrites every old record at once.

### Snapshots and time to live

- `begin_read_snapshot()` gives a consistent view (MVCC) while writes continue.
- Records written with a TTL read as missing once expired. Call `sweep_expired()` to delete them; nothing does it for an embedded engine.

## Configuration

`EngineConfig::default()` suits most programs. `DocumentStore::open` sets the document defaults itself.

| Field | Default | Meaning |
| :--- | :--- | :--- |
| `collection_name` | `"default"` (`"documents"` for a `DocumentStore`) | The store's file stem |
| `layout_mode` | `DedicatedFiles` (the document layout for a `DocumentStore`) | How records are laid out on disk, below |
| `wal_sync_policy` | `GroupCommit { max_batch_entries: 64 }` | When a write is durable, below |
| `enable_wal` | `true` | Without a WAL, buffered writes are lost on a crash |
| `background_flush`, `flush_interval_ms` | `true`, `10` | A background thread writes buffered records to their files every 10 ms. Several writes to one key in that time are written once |
| `max_hot_dirty_bytes` | 64 MiB | Writes not yet flushed may use this much memory. Beyond it, writers flush themselves |
| `mmap_pool` | 16,384 mappings, 4 GiB mapped | Cold mappings are unmapped beyond these |
| `wal_archive`, `wal_archive_dir` | `false`, none | Keep the WAL that checkpoints cut, for point-in-time recovery. Setting a directory turns it on |
| `index_tier` | `Memory` | `Paged` keeps secondary indexes in memory-mapped pages on disk |
| `primary_tier` | `Memory` | `Cold` keeps the primary index as of the last checkpoint on disk |
| `encryption`, `decrypted_cache_bytes` | none, 256 MiB | Encryption at rest, below |

### Layouts

| `StorageLayoutMode` | How |
| :--- | :--- |
| `AdaptiveArena` | Records are appended to memory-mapped segment files that start at 4 KiB and double up to the segment size. Records above a threshold get their own file. Compaction reclaims dead space. Documents use it with a 64 KiB threshold and 64 MiB segments |
| `DedicatedFiles` | One file per record, replaced atomically through a temporary file and a rename |
| `PingPongFiles { slot_stride }` | One file per record with two slots, written in turn. For large records rewritten often |

### Sync policies

| `SyncPolicy` | A write returns after | Lost on a crash |
| :--- | :--- | :--- |
| `Immediate` | its own `fsync` | nothing |
| `GroupCommit { max_batch_entries }` | an `fsync` shared with writers arriving at the same time | nothing |
| `AsyncMemoryFirst` | it is staged in memory. The flusher hands it to the OS every `flush_interval_ms`, with no `fsync` | a process crash loses the last flush interval; a power failure, what the OS had not written back |

Range-cluster nodes use `AsyncMemoryFirst`, because their Raft logs are the durable record.

### Cold tiers

For stores larger than memory:

- `primary_tier: PrimaryTier::Cold` keeps the primary index as of the last checkpoint in a page-structured file, and holds only later changes in memory, with a Bloom filter in front. A lookup that misses memory reads one page.
- `index_tier: IndexTier::Paged` keeps secondary indexes as memory-mapped B+tree pages and bitmap blocks, reused at open after a clean shutdown.

Either can be switched in both directions; the next checkpoint converts the files. The server does not expose these tiers yet.

### Encryption at rest

Set `encryption: Some(EncryptionConfig { keys, active_key, accept_unencrypted })`. Records, the WAL and index files are sealed with ChaCha20-Poly1305 under the active key, bound to each record's id and metadata. File names and sizes, and WAL frame headers (ids, versions), stay visible.

To rotate keys while running, add a key to the ring, call `set_encryption` with the new active key, then `reseal_all()`, which reseals every record with it and scrubs superseded copies. The server does this for you at start: see [encryption at rest](encryption.md).

## Backups and point-in-time recovery

| Function | Does |
| :--- | :--- |
| `create_online_backup(engine, dir)` | A consistent copy of a running store, with a manifest of the WAL position it covers |
| `archive_wal_for_pitr(engine, backup_dir)` | Copies archived WAL into a backup. Needs `wal_archive` |
| `restore_point_in_time_with(backup, logs, restore_dir, config, time_ns)` | Restores a backup and replays WAL files up to a moment, then opens the result |

The steps are the same as for a server: see [backups and recovery](backups.md).
