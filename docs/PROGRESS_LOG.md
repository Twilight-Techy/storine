# Project Progress & Security/Quality Audit Log (`PROGRESS_LOG.md`)

## Overview
- **Project**: Zero-Copy Memory-Mapped Object Storage Engine (`storage-engine`)
- **Status**: **COMPLETED** (`TASK-001` through `TASK-050` 100% Built, Audited, Optimized, and Verified)
- **Canonical Reference Docs**:
  1. `docs/IMPLEMENTATION_PLAN.md`
  2. `docs/FEATURES.md`
  3. `docs/RULES.md`
  4. `docs/TASKS.md`
  5. `docs/MASTER_PROMPT.md`
  6. `docs/PROGRESS_LOG.md`
  7. `docs/BENCHMARK_REPORT.md`

---

## Task Execution & Audit Log

### Stage 0: Workspace Foundation, Compiler Tuning & Benchmark Harness
- **[x] `TASK-001`: Initialize Multi-Crate Cargo Workspace & Optimization Profiles**
  - Created 10-crate workspace (`storage-types`, `storage-codec`, `storage-mmap`, `storage-wal`, `storage-index`, `storage-graph`, `storage-core`, `storage-cli`, `storage-server`, `benches`) plus `.cargo/config.toml` (`target-cpu=native`) and release profile (`opt-level = 3`, `lto = "fat"`, `codegen-units = 1`, `panic = "abort"`, `strip = "symbols"`).
  - Enforced `#![forbid(unsafe_code)]` across all safe crates (`storage-wal`, `storage-index`, `storage-graph`, `storage-core`, `storage-cli`, `storage-server`) and `#![deny(unsafe_op_in_unsafe_fn)]` across low-level primitives (`storage-types`, `storage-codec`, `storage-mmap`).
- **[x] `TASK-002`: Build Allocation-Counting & Latency Benchmark Harness**
  - Implemented `TrackingAllocator` in `benches/src/lib.rs` using thread-local `Cell<u64>` counters (`alloc_count`, `dealloc_count`, `alloc_bytes`) and `LatencyHistogram` (`p50`, `p95`, `p99`, `p99.9`).
  - Verified with `assert_zero_alloc!` gate that detects even a single 1-byte heap allocation.

### Stage 1: Core Zero-Allocation Types & Zero-Copy Codec
- **[x] `TASK-003`: Implement `InlineId` (24-Byte Stack-Inlined Primary Key)**
  - Built `#[repr(C, align(8))]` 24-byte `InlineId` (`crates/storage-types/src/id.rs`) with 3-register `u64` bitwise equality and deterministic 64-bit mixing hash.
  - Benchmarked at **0.6 ns p50** (`544,988,828 ops/sec`, `0.00 allocs/op`).
- **[x] `TASK-004`: Implement 64-Byte Cache-Line-Aligned `RecordHeader` & Flags**
  - Built `#[repr(C, align(64))]` `RecordHeader` (`crates/storage-types/src/header.rs`) with `header_checksum` covering bytes `0..56` (`magic`, versions, flags, `updated_at_ns`, `payload_checksum`, `payload_len`, `uncompressed_len`, `root_offset`, `ttl_expires_at_ns`) so corrupted `payload_len` values are rejected before slicing payload bytes.
- **[x] `TASK-005`: Implement SIMD Checksums (`XXH3-64` & Hardware `CRC32C`)**
  - Implemented `xxh3_64` (4-lane 256-bit stripe accumulation) and hardware SSE4.2 `_mm_crc32_u64` dual-stream intrinsics (`crates/storage-codec/src/checksum.rs`).
  - Benchmarked `XXH3-64` at **17.45 GB/s** (`42.8 ns p50` per 1 KB) and `CRC32C` at **19.50 GB/s** (`41.4 ns p50` per 1 KB).
- **[x] `TASK-006`: Implement Zero-Copy Record Codec & Pooled Scratch Buffers**
  - Built relative-offset zero-copy archive system (`ZeroCopyRecord`, `ArchivedString`, `ArchivedBytes`, `ArchivedSlice<A>`, `ArchivedOption<A>`, `RecordView`, `RecordCodec`) and 64-byte-aligned thread-local `AlignedScratchBuf` (`crates/storage-codec/src/archive.rs`, `crates/storage-codec/src/scratch.rs`).
  - Benchmarked `decode_unchecked` at **0.5 ns p50** (`0.00 allocs/op`).

### Stage 2: Cross-Platform `mmap` Engine, Bounded Pool & Disk Layout
- **[x] `TASK-007`: Implement Cross-Platform `MmapFile` & OS Virtual Memory Hints**
  - Implemented native Win32 (`CreateFileMappingW`, `MapViewOfFile`, `PrefetchVirtualMemory`, `FlushViewOfFile`) and Unix (`mmap`, `madvise`, `msync`) wrappers (`crates/storage-mmap/src/raw.rs`) with `// SAFETY:` proofs on every `unsafe` block.
- **[x] `TASK-008`: Implement `MmapPool` with W-TinyLFU / Clock Eviction & Reader Guards**
  - Implemented 64-shard `MmapPool` (`crates/storage-mmap/src/pool.rs`) with atomic reader reference counts (`MmapGuard`) and non-blocking Clock eviction of cold unreferenced slots.
- **[x] `TASK-009`: Implement Sharded & Hierarchical Path Resolver (Zero-Alloc Stack Paths)**
  - Implemented 256-byte stack-allocated `StackRelPath` (`crates/storage-mmap/src/path.rs`) with path-traversal and NTFS Alternate Data Stream (`:`) sanitization.
  - Benchmarked at **29.7 ns p50** (`22.5M ops/sec`, `0.00 allocs/op`).
- **[x] `TASK-010`: Implement Atomic Disk Persistence (Temp+Rename & Ping-Pong Double Slot)**
  - Implemented `atomic_write_replace` (`MoveFileExW(..., MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)`) and `write_ping_pong_slot` / `read_ping_pong_active` (`crates/storage-mmap/src/persist.rs`).
  - **Audit Fix**: Added monotonic `AtomicU64` sequence numbers to `.tmp` filenames (`<target>.<seq>.tmp`) so concurrent background flushes and cold compression operations never collide on temporary filenames.

### Stage 3: Primary Index, Per-Record Concurrency, Write Coalescing & WAL
- **[x] `TASK-011`: Implement Sharded Primary Index & `HotDirtyArc` State**
  - Implemented 64-shard `CachePadded` `PrimaryIndex` (`crates/storage-index/src/primary.rs`) storing `RecordOrdinal`, `RecordVersion`, `DiskLocation`, `ttl_expires_at_ns`, and `dirty_buf: Option<AlignedBytes>`.
- **[x] `TASK-012`: Implement Memory-Mapped Primary Index Checkpointing (`primary.idx`)**
  - Implemented `save_primary_checkpoint` and `load_primary_checkpoint` (`crates/storage-index/src/checkpoint.rs`) with `IDXC` magic header and `XXH3-64` integrity verification.
- **[x] `TASK-013`: Implement Striped Per-Record Write Queues & Closure Mutation API**
  - Built 2048-way `CachePadded<Mutex<()>>` `WriteStripePool` (`crates/storage-core/src/write_queue.rs`) and `StorageEngine` (`crates/storage-core/src/engine.rs`) supporting `insert_model`, `with_ref`, `update`, `update_if_version` (OCC), and `delete`.
  - **Audit & Optimization**: Connected `ArenaManager::with_slice` directly to `StorageEngine::with_ref` and skipped `SystemTime::now()` syscalls when `ttl_expires_at_ns == 0`, achieving **1,580,173 ops/sec** (`0.5 µs p50`, `0.00 allocs/op`).
- **[x] `TASK-014`: Implement Group-Commit Write-Ahead Log (`storage-wal`)**
  - Built `GroupCommitWal` (`crates/storage-wal/src/lib.rs`) with 72-byte headers, leader-follower batching, torn-tail truncation recovery, and `Drop` flush.
  - **Optimization**: Added a zero-allocation uncontended leader fast-path to `GroupCommitWal::append_single` and made `IndexSnapshot`/`IndexDelta` 100% stack-allocated `Copy` structs, accelerating `StorageEngine::update` to **145,080 ops/sec** (`4.9 µs p50`, `1.00 alloc/op`).
- **[x] `TASK-015`: Implement Background Async Flusher & Write Coalescer**
  - Built `DirtyTracker` (`crates/storage-core/src/write_queue.rs`) using an atomic `AtomicU64` bitset across `RecordOrdinal`s and background flusher worker in `StorageEngine`.
  - Verified **70.7% write reduction** (`14,672` physical flushes for `50,000` rapid updates across 100 hot keys).

### Stage 4: Secondary Indexes & Zero-Allocation Query Engine
- **[x] `TASK-016`: Define `Indexable` Trait & Differential Index Delta Extractor**
  - Implemented stack-allocated `IndexSnapshot` (`InlineVec<InlineId, 8>` tags, `InlineVec<(InlineId, i64), 8>` numerics, 64-bit `embedding_hash`) and `IndexDelta::compute` (`crates/storage-index/src/traits.rs`) that skips secondary index lock acquisition when indexed fields are unchanged.
- **[x] `TASK-017`: Implement Cache-Conscious B+Tree Range Index**
  - Implemented `NumericRangeIndex` (`crates/storage-index/src/btree.rs`) with `insert`, `remove`, `range_ordinals`, and `range_count`.
- **[x] `TASK-018`: Implement Compressed Bitmap Equality & Tag Index**
  - Implemented `CompressedBitmap` and `BitmapTagIndex` (`crates/storage-index/src/bitmap.rs`) using a flat cache-contiguous sorted `Vec<(u32, [u64; 64])>` container array with 1-cycle fast-path `contains` and in-place `filter_in_place`.
- **[x] `TASK-019`: Implement Zero-Allocation Query Builder & Prefetching Iterator**
  - Implemented stack-inlined `QueryBuilder` (`crates/storage-core/src/query.rs`) combining tag intersections, numeric range filters, and SIMD vector top-$K$ scoring.
  - Benchmarked combined Tag + B+Tree Range query at **86,737 queries/sec** (`6.1 µs p50`, `1.00 alloc/op` for the output `Vec`).

### Stage 5: Explicit Relationships, Hierarchies & Referential Integrity
- **[x] `TASK-020`: Implement Zero-Copy Relationship Containers (`OneToOne`, `OneToMany`, `ManyToMany`)**
  - Implemented `OneToOne`, `OneToMany`, `ManyToMany`, and `RelationshipEdgeSpec` (`crates/storage-graph/src/containers.rs`) with sorted `Vec<InlineId>` binary-search membership and zero-copy `ArchivedSlice<InlineId>` access.
- **[x] `TASK-021`: Implement Deadlock-Free Bidirectional `link` & `unlink` Operations**
  - Implemented canonical lock ordering (`WriteStripePool::lock_pair`) and bidirectional `link`/`unlink` in `StorageEngine` (`crates/storage-core/src/engine.rs`).
- **[x] `TASK-022`: Implement Referential Integrity Policies (`Restrict`, `Cascade`, `Unlink`)**
  - Implemented `IntegrityPolicy::{Restrict, Cascade, Unlink}` (`crates/storage-graph/src/integrity.rs`) enforced automatically on `StorageEngine::delete`.
- **[x] `TASK-023`: Implement Batched Graph Traversal & Hierarchy Bundle Co-Mapping**
  - Implemented `StorageEngine::traverse_bfs` (`crates/storage-core/src/engine.rs`) and hierarchical parent-folder disk path resolution (`resolve_record_rel_path` in `crates/storage-mmap/src/path.rs`).

### Stage 6: Multi-Record Transactions, Schema Evolution, Arena Compaction & AI Features
- **[x] `TASK-024`: Implement Multi-Record Atomic Transactions (`db.transaction`)**
  - Implemented `Transaction` (`crates/storage-core/src/engine.rs`) with OCC read-set version validation, canonical write-set stripe lock acquisition, and atomic `WalOpKind::TxnCommit` barrier frames.
- **[x] `TASK-025`: Implement Adaptive Slotted Arena Segments (`seg_XXXXX.dat`) & Compactor**
  - Implemented 64 MB memory-mapped `ArenaSegment` and `ArenaManager` (`crates/storage-mmap/src/arena.rs`) + live segment compaction (`StorageEngine::compact_arenas`).
- **[x] `TASK-026`: Implement Schema Evolution (`schema_version` Migration Pipeline)**
  - Implemented `MigrationRegistry` (`crates/storage-core/src/engine.rs`) and on-the-fly `get_migrated_raw` / `migrate_and_upgrade_record` pipelines (`v1 -> v2 -> ...`).
- **[x] `TASK-027`: Implement TTL Expiration, Cold LZ4 Compression & SIMD Vector Scan**
  - Implemented lazy TTL expiration on read + `sweep_expired_ttl`, cold LZ4 compression (`compress_cold_record` via `crates/storage-codec/src/compress.rs`), and 8-way unrolled SIMD dot-product/cosine similarity top-$K$ search (`crates/storage-index/src/vector.rs`, benchmarked at **72.8 µs p50** over 10k 32-D embeddings).

### Stage 7: CLI, Network Server & Chaos Crash-Testing Verification
- **[x] `TASK-028`: Build `storage-cli` Inspection, Compaction, Backup & Benchmark Tool**
  - Implemented `storage-cli` (`crates/storage-cli/src/main.rs`) supporting `stats`, `put`, `get`, `delete`, `inspect-header`, `verify-integrity`, `compact`, `snapshot`, and `bench`.
- **[x] `TASK-029`: Build `storage-server` (Zero-Copy Binary Protocol over TCP)**
  - Implemented `StorageServer` and `StorageClient` (`crates/storage-server/src/lib.rs`, `crates/storage-server/src/main.rs`) with `SENG` binary framing (`GET`, `PUT`, `DELETE`, `STATS`, `PING`).
- **[x] `TASK-030`: Execute Full Chaos Crash-Testing & End-to-End Performance Gate**
  - Built comprehensive integration & chaos test suite in `crates/storage-core/tests/chaos_and_e2e.rs` covering 16-thread hot-key contention, OCC conflicts, multi-account ACID bank transfers, graph `Restrict`/`Unlink`/`Cascade` policies, TTL/LZ4/schema evolution, TCP server round-trips, and WAL torn-tail crash recovery.
  - All workspace test suites (`cargo test --workspace`) and release benchmark gates (`cargo run --release -p storage-benches`) pass with zero warnings and zero errors. Recorded full metrics in `docs/BENCHMARK_REPORT.md`.

### Pre-Stage 8 Optimization Pass: Minimal Storage Footprint (Code + Disk + RAM) & Maximum Speed
- **[x] Zero-Overhead `FastHashMap` & `FastHashSet` (`crates/storage-types/src/lib.rs`)**:
  - Replaced cryptographic `SipHash-1-3` inside all internal shard maps with a 1-cycle non-cryptographic `FastHasher` (`i.rotate_right(13)`), boosting `StorageEngine::with_ref` throughput to **4.21M ops/sec (`200 ns p50`)**.
- **[x] Compact `STGEIDX2` On-Disk Checkpoint Format (`crates/storage-index/src/checkpoint.rs`)**:
  - Replaced fixed 88-byte `STGEIDX1` records with variable-length `InlineId::write_compact_to` and LEB128 varint fields + 64B-unit arena offsets, shrinking `primary.idx` on disk by **4.63x (`19.0 bytes/record` vs `88.0 bytes/record`)**.
- **[x] Lazy 4 KiB Initial Arena Segment Allocation & In-RAM Struct Packing**:
  - Reduced initial `ArenaSegment` file allocation (`INITIAL_SEGMENT_BYTES`) to `4 KiB` (1 OS page), used `InlineId::EMPTY` as the `None` niche inside `ArchivedOneToOne` (`112B` `ArchivedAgentObjectRecord`), and eliminated heap `index_snapshots`/`embeddings` maps in `StorageEngine` via zero-copy `extract_archived_snapshot` and `archived_embedding_slice` (**saving `544 MB` per 1M records**).

### Stage 8: Multi-Collection Catalog & Cross-Collection Heterogeneous Graph (`TASK-031` – `TASK-033`)
- **[x] `TASK-031`: Implement `DatabaseCatalog`, `CollectionId` Registry & Shared WAL**
  - Built `DatabaseCatalog` (`crates/storage-core/src/catalog.rs`) managing isolated heterogeneous `CollectionHandle<T>` instances (`CollectionId(u16)`), atomic `catalog.meta` persistence (`STGCAT01` magic + `XXH3-64` checksum), and a shared `catalog.wal` group-commit Write-Ahead Log across all collections.
- **[x] `TASK-032`: Implement Cross-Collection ACID Transactions & Heterogeneous Graph Edges**
  - Built `CatalogTransaction` (`crates/storage-core/src/catalog.rs`) with global canonical `(collection_id, stripe)` deadlock-free lock ordering, OCC read-set validation across multiple collections, and cross-collection `delete_with_cross_collection_policy` enforcing `IntegrityPolicy::{Restrict, Cascade, Unlink}` across distinct collection types (`Users -> Orders`).
- **[x] `TASK-033`: Implement Typed Schema Evolution & Compaction Upcaster (`SchemaRegistry`)**
  - Built `SchemaRegistry<T>` (`crates/storage-core/src/migration.rs`) and `StorageEngine::register_step_upcaster` / `set_step_upcaster_decoder` / `upgrade_legacy_schemas`, plus automatic persistent schema upcasting during `StorageEngine::compact_arenas`.

### Stage 9: Disk-Backed Paged Indexes & Cold-Tier Primary Index (`TASK-034` – `TASK-035`)
- **[x] `TASK-034`: Implement Disk-Backed Paged B+Tree Range Index (`btree_<field>.idx`)**
  - Built `PagedBTreeIndex` (`crates/storage-index/src/paged_btree.rs`) using 4 KiB `MmapRegion` leaf pages (`BTRE` magic, 32-byte header + delta-encoded `(key_delta, ordinal)` pairs + `XXH3-64` page checksum), compact 24-byte `SeparatorEntry` directory in RAM, and zero-allocation `count_in_range_zero_alloc`.
- **[x] `TASK-035`: Implement Disk-Backed Paged Bitmap Index (`bitmap_<tag>.idx`) & Cold-Tier Primary Index**
  - Built `PagedBitmapIndex`, 64-byte cache-line `SplitBlockBloomFilter` (8-hash single-cache-line probe), and `ColdPrimaryIndex` (`crates/storage-index/src/paged_bitmap.rs`) reducing cold primary index RAM overhead to `< 2.5 bytes/record` while serving point lookups in at most 1 page read.

### Stage 10: Sub-Linear HNSW Vector Index & `SQ8` Scalar Quantization (`TASK-036` – `TASK-038`)
- **[x] `TASK-036`: Implement `SQ8` Scalar Quantizer & Int8 SIMD Distance Kernels**
  - Built `QuantizedVecSq8` (`crates/storage-index/src/quantize.rs`) compressing `f32` embeddings `4x` to `1 byte/dimension` (`u8`) with per-vector `(min, scale)` calibration and 16-lane unrolled `dot_u8_unrolled` / `sq8_cosine_similarity` kernels (`0` heap allocations).
- **[x] `TASK-037`: Implement Multi-Layer HNSW Graph Index & Mmap Persistence (`hnsw.idx`)**
  - Built `HnswVectorIndex` (`crates/storage-index/src/hnsw.rs`) with exponential layer assignment, diversity-pruned neighbor lists (`M = 16`, `M_max0 = 32`), thread-local generation-stamped `VisitedTable` (zero per-query bitset allocation), and atomic `hnsw.idx` persistence (`HNSW` magic + `XXH3-64`).
- **[x] `TASK-038`: Implement Filter-Aware Hybrid Vector + Metadata Search**
  - Integrated `HnswVectorIndex` into `StorageEngine` (`enable_hnsw_index`, `save_hnsw_index`, `load_hnsw_index`, `score_embeddings`) with adaptive pre-filter vs HNSW predicate-aware traversal + two-stage `SQ8` coarse search and exact `f32` re-ranking.

### Stage 11: MVCC Snapshot Isolation & Point-in-Time Recovery (`TASK-039` – `TASK-041`)
- **[x] `TASK-039`: Implement Monotonic `CommitTs`, Version Chains & `ReadSnapshot<'a, T>`**
  - Built `CommitTs`, 64-shard `MvccManager`, and RAII `ReadSnapshot<'a, T>` (`crates/storage-core/src/mvcc.rs`, `StorageEngine::begin_read_snapshot`) with a fast `has_active_snapshots()` atomic check so hot writes incur zero version-chain cloning when no snapshot is open.
- **[x] `TASK-040`: Implement Epoch-Based MVCC Garbage Collector & Segment Compaction Integration**
  - Built `MvccManager::gc_obsolete_versions` and `StorageEngine::gc_mvcc`, automatically reclaiming historical version entries older than `min_active_read_ts` and integrating MVCC GC into `compact_arenas`.
- **[x] `TASK-041`: Implement Segmented WAL Rotation & Point-in-Time Recovery (PITR)**
  - Added monotonic LSN assignment (`current_lsn`), `rotate_segment` (`wal_000001.log`, ...), `list_archived_segments`, `recover_all_segments`, and `prune_archived_segments_before_lsn` in `GroupCommitWal` (`crates/storage-wal/src/lib.rs`), plus `create_online_backup`, `archive_wal_for_pitr`, and `restore_point_in_time` (`crates/storage-core/src/backup.rs`).

### Stage 12: Native Kernel-Polled Networking, Wire Protocol v2 & Security (`TASK-042` – `TASK-044`)
- **[x] `TASK-042`: Implement Native OS Socket Readiness Poller (`storage-mmap::net_poll`)**
  - Built `SocketPoller` (`crates/storage-mmap/src/net_poll.rs`) wrapping native Win32 `WSAPoll` (`ws2_32.dll`) on Windows and POSIX `poll` on Unix, and upgraded `StorageServer`'s `ReactorShard` workers (`crates/storage-server/src/lib.rs`) to sleep in the kernel at `0.0%` CPU when idle and wake in $O(k)$ ready sockets.
- **[x] `TASK-043`: Implement Wire Protocol v2 (`CAS_PUT`, `BATCH_GET`, `BATCH_PUT`, `QUERY_HYBRID`)**
  - Implemented `OP_CAS_PUT = 0x08`, `OP_BATCH_GET = 0x09`, `OP_BATCH_PUT = 0x0A`, `OP_QUERY_HYBRID = 0x0B`, `OP_AUTH = 0x0C`, and `OP_METRICS = 0x0D` in `StorageServer` and `StorageClient` (`crates/storage-server/src/lib.rs`) plus `StorageEngine::put_cas`.
- **[x] `TASK-044`: Implement Token Auth, RBAC, Rate Limiting & AEAD At-Rest Encryption**
  - Built constant-time token authentication, `RbacRole` (`Unauthenticated`, `ReadOnly`, `ReadWrite`, `Admin`), and per-connection `TokenBucket` rate limiting (`crates/storage-server/src/security.rs`), plus RFC 8439 ChaCha20 20-round ARX stream cipher + 128-bit keyed MAC at-rest encryption (`encrypt_payload_aead`, `decrypt_payload_aead` in `crates/storage-codec/src/crypto.rs`, and `RecordFlags::ENCRYPTED`).

### Stage 13: Horizontal Scale: Consistent-Hash Sharding & Raft Consensus (`TASK-045` – `TASK-047`)
- **[x] `TASK-045`: Implement `1,024` Virtual Shards (`vshard_id`) & Compact `ClusterMap`**
  - Built `vshard_for_id`, 2 KiB `ClusterMap` (`1,024 x u16` owners + epoch + `XXH3-64` checksum), and `ClusterRouter` (`crates/storage-core/src/sharding.rs`).
- **[x] `TASK-046`: Implement Raft Consensus Group (Leader Election & Quorum Log Replication)**
  - Built `RaftNode` and `RaftReplicationGroup` (`crates/storage-core/src/raft.rs`) supporting deterministic leader election, quorum log replication (`(term, lsn, vshard_id)`), automatic leader failover, and linearizable follower read scaling (`read_at_commit_index`).
- **[x] `TASK-047`: Implement Live Online Shard Migration & Rebalancer**
  - Built `migrate_vshard` and `compute_vshard_digest` (`crates/storage-core/src/sharding.rs`) performing online virtual shard transfer, atomic `ClusterMap` epoch cutover, source shard cleanup, and 64-bit `XXH3-64` digest verification.

### Stage 14: Cost-Based Query Optimizer, Prometheus Telemetry & `v1.0` Gate (`TASK-048` – `TASK-050`)
- **[x] `TASK-048`: Implement Cost-Based Query Optimizer & `EXPLAIN` Execution Plans**
  - Built `QueryOptimizer`, `QueryExplainPlan`, and `AccessPathKind` (`crates/storage-core/src/optimizer.rs`) plus `QueryBuilder::explain()` (`crates/storage-core/src/query.rs`) ordering predicates by exact cardinality and short-circuiting empty intersections in $O(1)$.
- **[x] `TASK-049`: Implement Lock-Free Latency Histograms, Slow-Query Log & Prometheus Exporter**
  - Built 64-bucket lock-free `LatencyHistogram`, bounded `SlowOperationLog`, and `EngineStats::to_prometheus_text()` (`crates/storage-core/src/telemetry.rs`), exposed over `OP_METRICS` (`StorageClient::metrics_prometheus`).
- **[x] `TASK-050`: Execute Full-Scale `v1.0` Distributed Chaos, Security & Performance Gate**
  - Built and executed `crates/storage-core/tests/production_v1_gate.rs` verifying all Stages 8–14 capabilities end-to-end (`42` total workspace tests passing, `0` warnings, `0` errors).

### Stage 15: On-Disk I/O, Slot Recycling, WAL Compression & Incremental Delta Checkpoint Optimizations (`TASK-051` – `TASK-054`)
- **[x] `TASK-051`: Implement Positional `write_all_at` (`seek_write`/`pwrite`) & Lazy Superblock Fast-Forward Recovery in `ArenaSegment`**
  - Replaced the 4-syscall `(seek + write + seek + write)` arena flush path in `crates/storage-mmap/src/arena.rs` with a single positional `write_all_at` syscall (`75%` syscall reduction) and lazy superblock `write_head` updates (`SUPERBLOCK_SYNC_INTERVAL = 64`) with deterministic startup fast-forward recovery across valid checksummed `RecordHeader` slots.
- **[x] `TASK-052`: Implement In-Segment 64-Byte Size-Class Free-Slot Reuse (`reclaim_dead_slot`)**
  - Added `free_slots: FastHashMap<u32, Vec<u32>>` inside `ArenaSegment` (`crates/storage-mmap/src/arena.rs`) and wired `arena.reclaim_dead_slot(old_loc)` into `StorageEngine` (`crates/storage-core/src/engine.rs`) when no MVCC snapshot is active so same-size updates reuse freed 64B slots in-place without growing `.seg` files.
- **[x] `TASK-053`: Implement Transparent LZ4 WAL Frame Compression for Payloads `>= 1 KiB`**
  - Added `WAL_FLAG_LZ4 = 0x01` and `uncompressed_len` (`bytes 60..64`) to the 64-byte WAL header (`crates/storage-wal/src/lib.rs`), transparently compressing WAL payloads `>= 1024` bytes via `compress_lz4` and decompressing during `recover_committed`.
- **[x] `TASK-054`: Implement Incremental $O(\Delta)$ Primary Index Delta Checkpointing (`STGEDLT2`)**
  - Implemented `append_primary_checkpoint_delta`, `delta_checkpoint_path`, and automatic `.delta` replay in `load_primary_checkpoint` (`crates/storage-index/src/checkpoint.rs`), plus `StorageEngine::checkpoint_incremental` with automatic 1 MiB compaction (`43` total workspace tests passing, `0` warnings, `0` errors).

### Stage 16: Engine-Level Set Filtering, Bidirectional Sorting & Keyset/Offset Pagination (`TASK-055` – `TASK-056`)
- **[x] `TASK-055`: Implement Hardware-Accelerated Bitmap `AND NOT` / `OR` Windowing & Bidirectional Keyset-Seekable B+Tree Scans**
  - Added `and_not`, `and_not_in_place`, `collect_window` (using 1-cycle `POPCNT` word skipping for `offset` and `BLSR`/`leading_zeros` for `Asc`/`Desc`), `union_tags`, `exclude_from_bitmap`, and `matches_ordinal` in `crates/storage-index/src/bitmap.rs`; added `SortOrder`, `KeysetCursor`, and `range_collect_paginated` in `crates/storage-index/src/btree.rs`; and added $O(1)$ array-indexed `scan_ordinals_paginated` in `crates/storage-index/src/primary.rs`.
- **[x] `TASK-056`: Implement Zero-Allocation `QueryBuilder` Set Filters, Zero-Copy Payload Predicates, `QueryPage` & Paginated TCP RPC**
  - Upgraded `QueryBuilder` (`crates/storage-core/src/query.rs`) with stack-inlined `filter_not_tag`, `filter_any_tag`, `filter_range_f64`, `filter_range_u64`, `order_asc`, `order_desc`, `offset`, `after_cursor`, `execute_page_ids`, `execute_page_refs`, `execute_page_filtered_with`, and `for_each_ref`, plus `StorageClient::query_range_paginated` (`crates/storage-server/src/lib.rs`, `44` total workspace tests passing, `0` warnings, `0` errors).

### Stage 17: Full Filtering Coverage (`TASK-057` – `TASK-058`)
- **[x] `TASK-057`: Implement Index-Backed Relational Operators, Prefix Scans, Key-Set Filters & Field Existence Bitmap Primitives**
  - Added zero-allocation string matchers on `InlineId` (`starts_with_str`, `ends_with_str`, `contains_str` in `crates/storage-types/src/id.rs`) and `ArchivedString` / `RecordView` (`eq_str`, `starts_with`, `ends_with`, `contains`, `eq_ignore_ascii_case`, `contains_ignore_ascii_case`, `str_eq`, `str_starts_with`, `str_ends_with`, `str_contains`, `str_contains_ignore_ascii_case` in `crates/storage-codec/src/archive.rs`); added `union_tag_prefix` and `all_on_field_bitmap` in `crates/storage-index/src/bitmap.rs`; added $O(\log U)$ `eq_bitmap`, `in_values_bitmap`, and `all_on_field_bitmap` in `crates/storage-index/src/btree.rs`; and added `ids_to_bitmap` and `id_prefix_bitmap` in `crates/storage-index/src/primary.rs`.
- **[x] `TASK-058`: Integrate All 5 Filtering Categories & Composable Zero-Copy `RecordView` Predicates into `QueryBuilder`**
  - Extended `QueryBuilder` (`crates/storage-core/src/query.rs`) with relational operators (`filter_eq`, `filter_eq_u64`, `filter_eq_f64`, `filter_ne`, `filter_ne_u64`, `filter_ne_f64`, `filter_gt`, `filter_gte`, `filter_lt`, `filter_lte`, `filter_gt_f64`, `filter_gte_f64`, `filter_lt_f64`, `filter_lte_f64`, `filter_not_range`, `filter_in_i64`), prefix/key-set matchers (`filter_tag_prefix`, `filter_id_prefix`, `filter_ids`, `filter_not_ids`), field existence checks (`filter_has_tag_field`, `filter_missing_tag_field`, `filter_has_numeric_field`, `filter_missing_numeric_field`), hybrid graph neighborhood filtering (`filter_within_bfs`), and composable zero-copy `.filter(|view: &RecordView<'_, T>| -> bool)` honored across all terminal methods (`execute_ids`, `execute_refs`, `execute_page_ids`, `execute_page_refs`, `for_each_ref`, and `top_k_similar`).
