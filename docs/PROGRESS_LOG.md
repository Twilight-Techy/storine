# Project Progress & Security/Quality Audit Log (`PROGRESS_LOG.md`)

## Overview
- **Project**: Zero-Copy Memory-Mapped Object Storage Engine (`storage-engine`)
- **Status**: `TASK-001` through `TASK-079` complete. The 2026-10-20 full-project audit (Stage 19) fixed the soundness, durability and security findings listed below. Open items are tracked at the end of Stage 19 and in `docs/FEATURES.md` §7.
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

### Stage 18: Production DevOps — Directory Lock, Persistent Raft/ClusterMap, TCP Raft RPCs, Admin RPCs & CLI PITR (`TASK-059` – `TASK-063`)
- **[x] `TASK-059`: Implement Exclusive `DirectoryLock` (`db.lock`) Preventing Concurrent Multi-Process Corruption**
  - Built cross-platform `DirectoryLock` (`crates/storage-mmap/src/persist.rs`) using `LockFileEx` (Windows) / `flock` (Unix), acquired on `<collection>.lock` during `open_with_shared_wal` in `StorageEngine`, auto-releases on `Drop`. `.lock` files excluded from backup/snapshot copies (`crates/storage-core/src/engine.rs`, `crates/storage-core/src/backup.rs`).
- **[x] `TASK-060`: Persist Crash-Safe Raft `HardState` (`raft.meta`) & `ClusterMap` (`cluster.map`) with XXH3-64 Checksums**
  - Added `RaftState::save_to_disk` / `load_from_disk` with `RAFT_META_MAGIC` (0x52414654) persisting `current_term`, `voted_for`, `commit_index`, `last_applied`, and full log; added `ClusterMap::save_to_file` / `load_from_file` with `CLUSTER_MAP_MAGIC` (0x434D4150); all state-mutating Raft methods now call `save_to_disk` atomically (`crates/storage-core/src/raft.rs`, `crates/storage-core/src/sharding.rs`).
- **[x] `TASK-061`: Implement TCP Admin RPCs (`OP_ADMIN_BACKUP`, `OP_ADMIN_COMPACT`, `OP_ADMIN_CHECKPOINT`, `OP_HEALTH`) with RBAC Enforcement**
  - Added opcodes `0x11`–`0x14` to `StorageServer` and `StorageClient` (`crates/storage-server/src/lib.rs`): `OP_ADMIN_BACKUP` (gated `Admin`, calls `create_online_backup`), `OP_ADMIN_COMPACT` (gated `Admin`, calls `compact_arenas`), `OP_ADMIN_CHECKPOINT` (gated `Admin`, calls `checkpoint_incremental`), `OP_HEALTH` (open to `ReadOnly+`, returns JSON status).
- **[x] `TASK-062`: Implement CLI PITR & Remote Admin Commands in `storage-cli`**
  - Added `backup`, `archive-wal`, `restore-pitr` (local PITR commands calling `create_online_backup`, `archive_wal_for_pitr`, `restore_point_in_time`) and `remote-health`, `remote-compact`, `remote-backup`, `remote-checkpoint`, `remote-metrics` (remote TCP admin commands via `StorageClient`) to `crates/storage-cli/src/main.rs`.
- **[x] `TASK-063`: Implement TCP Raft Consensus & Shard Migration RPCs (`OP_RAFT_VOTE`, `OP_RAFT_APPEND`, `OP_MIGRATE_VSHARD`)**
  - Added opcodes `0x0E`–`0x10` with full wire serialization round-trip for `RequestVoteReq`/`RequestVoteResp`, `AppendEntriesReq`/`AppendEntriesResp`, and virtual shard record import via `OP_MIGRATE_VSHARD`; added corresponding `StorageClient` methods (`raft_vote`, `raft_append`, `migrate_vshard`).
  - **Exit Criteria**: `test_tcp_raft_admin_health_and_shard_migration_rpcs` verifies all 7 new RPCs including RBAC enforcement, Raft wire round-trip, shard import, and admin backup/compact/checkpoint (`46` total workspace tests passing, `0` failures).

### Stage 18 (continued): Full-System Verification & Lint Hygiene (`TASK-064` – `TASK-066`)
- **[x] `TASK-064`**: Full-system integration test (`crates/storage-server/tests/full_system_e2e.rs`) covering all 24 feature categories.
- **[x] `TASK-065`**: SAFETY comments, path traversal guard, `mem::take` optimization.
- **[x] `TASK-066`**: Zero Clippy warnings across all crates.

### Stage 19: Full-Project Audit Remediation (`TASK-067` – `TASK-079`, 2026-10-20)

A line-by-line review of every crate against `RULES.md` found that several earlier "verified" claims did not hold in code. The per-task protocol was applied to each fix: consult the 6 docs, implement, audit, test, clippy, benchmark gate, log.

#### Security & Correctness Audit Findings (all fixed)

| # | Severity | Finding | Fix (task) |
| :--- | :--- | :--- | :--- |
| 1 | **Critical** | Safe APIs could reach Undefined Behavior on hostile or corrupted bytes. `ArchivedString::as_str` used `get_unchecked` + `from_utf8_unchecked`; `RecordRef::view` / `Deref` and engine reads used `decode_unchecked`; `storage-types` had unchecked casts; `storage-graph` / `storage-core` had dropped `forbid(unsafe_code)`. | `ArchivePod` + `impl_archive_pod!` (compile-time layout proof), safe O(1) `decode_fast`, checked accessors, archived relations moved into codec, `forbid(unsafe_code)` restored (`TASK-067`) |
| 2 | **Critical** | Checksums depended on build flags: the SSE4.2 and software `CRC32C` paths were different algorithms, so every database written by the default `target-cpu=native` build would read as corrupted on a portable build or another CPU architecture. | Bit-identical portable path, runtime CPU detection, golden-value format pin (`TASK-077`) |
| 3 | **Critical** | WAL could acknowledge writes out of order or after an I/O error. LSNs were assigned outside the lock, recovery rewrote the file, and checkpoints could reuse LSNs / txn ids. | WAL rebuilt: LSN under lock, sequence tickets, fail-stop, atomic `.next` + rename truncation, monotonic markers (`TASK-068`) |
| 4 | **High** | Acknowledged writes could be lost on crash. `flush_all` raced concurrent writers, WAL truncation ran before replayed state was durable, and multi-record deletes were not atomic. | Barrier-based `flush_all`, replay-then-truncate, single atomic WAL transaction for deletes, MVCC writer gate. Verified by a real process-kill test (`TASK-069`) |
| 5 | **High** | Use-after-reuse: arena slots could be overwritten in place while a reader still held a view into them. | Slot reuse gated on `strong_count == 1` and no live retired regions (`TASK-070`) |
| 6 | **High** | An unauthenticated server could bind any address; `OP_ADMIN_BACKUP` accepted arbitrary client paths and could delete existing directories; request bodies were allocated before role / size / rate checks. | Loopback-only without auth, `backup_root` confinement plus fresh-destination rule, header-stage admission (`TASK-075`, `TASK-073`) |
| 7 | **High** | Crafted index files could panic or index out of bounds (HNSW loader, paged B+Tree `entry_count`, missing page CRC checks). | Bounds-checked loaders, CRC validation, crafted-file tests (`TASK-071`) |
| 8 | **High** | "AEAD" used an ad-hoc MAC, not Poly1305; nonce reuse was possible. | Real RFC 8439 ChaCha20-Poly1305 with RFC test vectors and a unique-nonce generator (`TASK-076`) |
| 9 | **Medium** | Raft granted votes and accepted entries even when persisting them failed (`let _ = save_to_disk(..)`), so a crash could lead to two votes in one term; the whole state, including the log, was rewritten on every change. | `RaftStorage`, vote / term / log-matching rules, `apply_committed` (`TASK-074`) |
| 10 | **Medium** | Vshards used the same low hash bits as in-node shards (skewed placement). Migration did not freeze writers and had no rollback. | High-bit vshards, freeze + write gate + rollback (`TASK-074`) |
| 11 | **Medium** | Unconstrained queries returned nothing; exclusion and window interactions were wrong; `top_k` applied pagination windows. | Query fixes plus tests (`TASK-072`) |
| 12 | **Medium** | Backups had no LSN consistency point; PITR silently lost history because the WAL was truncated without archiving. | LSN-tagged snapshots under `lock_all`; PITR requires `wal_archive` (`TASK-073`) |
| 13 | **Low** | Docs overstated reality: wrong API in the README quick start, wrong CLI argument order, nonexistent `SyncMode`, "XXH3" naming, stale binary size, unwired features presented as integrated, undefined rule / feature IDs cited in code. | Documentation corrected, verified-status section, MSRV declared (`TASK-079`) |

#### Complexity & Footprint
- All hot paths keep their asymptotic bounds: O(1) point reads (`decode_fast` adds a constant number of comparisons), O(len) UTF-8 validation per `as_str` call, O(1) amortized WAL append per frame.
- `RecordRef<T>` shrank by 176 bytes (shared static zero page instead of an embedded zeroed fallback). Portable `CRC32C` uses a single 1 KiB compile-time table.
- In-RAM and on-disk record and index footprints are unchanged (`112 B` archived root, `96 B` `PrimaryEntry`, `19.0 B/record` `primary.idx`).

#### Verification
- `cargo test --workspace`: **68 passed, 0 failed** (21 new tests, including a real process-kill crash test, RFC 8439 vectors, a checksum golden-format pin and crafted-file loaders).
- `cargo clippy --workspace --all-targets`: **0 warnings** with `target-cpu=native`, and **0 warnings** with portable `target-cpu=x86-64`.
- `cargo run --release -p storage-benches`: all zero-allocation gates pass. Interleaved A/B against `4a004ec` shows `with_ref` / `get_ref` / `update` at parity within noise. The sound `get_str` costs ~6 ns more than the UB-capable baseline. Details in `docs/BENCHMARK_REPORT.md` §5.

#### Open Items (not fixed in Stage 19)
- **Binary size**: `storage-server` is `582 KiB` (baseline `516 KiB`), above the `RULE-MEM-03` `< 500 KiB` budget. The baseline already exceeded it.
- **Unwired features**: engine-level encryption at rest (`F-SEC-02`), paged / cold indexes (`F-PIDX-*`) and `hnsw.idx` persistence exist as tested components but are not used by `StorageEngine` (`docs/FEATURES.md` §7).

### Stage 20: Panic-Freedom, Master-Prompt Audit & Feature Wiring (`TASK-080` – )

#### `TASK-080`: Panic-Free Production Code & Hostile-Input Sweeps
- **Implementation**: checked capacity math in `scratch.rs` (`aligned_cap`, `layout_for`, `alloc_failed`), fallible `AlignedBytes::try_copy_from_slice`, `CachePadded::boxed_array`, compile-time `InlineId` constants (`NO_KEY`, `TXN_COMMIT_ID`, `CHECKPOINT_ID`), and the shared `storage_types::testing::SweepRng` generator (`#[doc(hidden)]`, stripped by LTO).
- **Audit findings fixed**:

  | Severity | Finding | Fix |
  | :--- | :--- | :--- |
  | **Critical** | `decode_owned` on a crafted tombstone frame panicked and called `decode_unchecked` on an unvalidated root (UB from safe code) | Reject tombstones; validate and decode the same buffer |
  | **High** | `decompress_lz4` allocated up to 256 MiB for a 3-byte input (memory amplification; abort on OOM) | Reject expansion > 256x input before allocating; `try_reserve_exact` |
  | **High** | `bitmap.idx` directory allocated `chunk_count` entries straight from the file; bitmap blocks had no checksum | v2 format (`BMX2`): blocks checksum, bounded counts, block-index / chunk-order validation |
  | **Medium** | `btree.idx` leaf CRC excluded the page header (min / max key, field id, count) | `page_crc` covers every byte except the CRC field |
  | **Low** | Network / disk-sourced buffers used aborting allocation | `try_copy_from_slice` at every untrusted site |

- **Remaining panic-capable sites**: compile-time `const` assertions only, plus `debug_assert!` (debug builds). Slice indexing on validated buffers remains (`~670` sites); untrusted entry points are proven panic-free by the sweeps rather than by rewriting every index, which would add branches on hot paths.
- **Complexity / footprint**: unchanged asymptotics; `bitmap.idx` open now verifies the block region once, O(file).
- **Verification**: `cargo test --workspace` **81 passed / 0 failed**; `cargo clippy --workspace --all-targets` **0 warnings** (native and `target-cpu=x86-64`); `cargo run --release -p storage-benches` all zero-allocation gates pass, latencies within the noise ranges recorded in `BENCHMARK_REPORT.md` §5.

#### `TASK-081`: Master-Prompt Audit (whole codebase)

| # | Pillar | Finding | Fix |
| :--- | :--- | :--- | :--- |
| 1 | Vertical scaling | `total_reads` / `total_writes`: one atomic RMW per read / write shared by every core (cache line bounces on every read) | `StripedCounter` (64 padded per-thread slots, summed on `stats()`) |
| 2 | CPU / false sharing | `PrimaryIndex` ordinal-table `RwLock` shared a cache line with the `shards` pointer read by every lookup | `CachePadded` |
| 3 | Complexity | Unfiltered top-K with `<= 256` vectors cloned **every** primary entry (`O(N)` allocation per query, independent of how few records have embeddings) | `HnswVectorIndex::live_ordinals` (`O(#vectors)`) |
| 4 | Complexity | `sweep_expired` cloned every primary entry | Walks the TTL ordinal bitmap (`O(#TTL records)`), expiry re-checked under the stripe lock |
| 5 | DoS / scaling | Accept thread did a blocking 5 ms write per rejected over-limit connection (a flood capped accepts for everyone) | Single non-blocking write |
| 6 | CPU hogging | Reactor workers with any connection woke every 25 ms and re-registered every socket, even when all were idle; `stop()` could lose its condvar wake-up | Per-shard loopback self-wake socket in the poll set → infinite poll timeout when idle; new connections, admin-job completion and shutdown write to it; `stop()` notifies under the lock |
| 7 | Memory bloat | Raft log (with full record frames) was never compacted: unbounded RAM and disk | Raft §7 compaction with persisted `snapshot_index` / `snapshot_term`; engine WAL `sync_to_disk` before discarding; loader skips covered entries (crash-safe ordering); lagging followers get an explicit "snapshot required" error |
| 8 | Observability gap | `LatencyHistogram` / `SlowOperationLog` were never recorded; `/metrics` had no latency | Per-shard read / write RPC histograms (no cross-core contention), merged on export; slow-op log (`StorageServer::slow_operations`, 10 ms default) |

- **Complexity / footprint**: top-K and TTL sweeps went from O(N) with full copies to O(#vectors) / O(#TTL records). Raft memory is bounded by `RAFT_COMPACT_THRESHOLD + RAFT_RETAIN_TAIL` entries. `StripedCounter` costs 4 KiB per counter. Each reactor shard holds 2 extra loopback sockets.
- **Verification**: `cargo test --workspace` **83 passed / 0 failed**; clippy **0 warnings** (native and portable); benchmark gate: all zero-allocation gates pass. `with_ref` reached `112 ns` p50 / `5.9 M ops/s` (best on this machine), `update` `3.0 µs` p50. Other latencies are within the documented noise ranges (the machine stayed heavily loaded).
- **Still open**: `WSAPoll` / `poll` keep each wake-up `O(connections per shard)` in the kernel (`epoll` / IOCP needed for `O(active)`). `InstallSnapshot` for followers behind the compaction point (reseed from a backup today). Release binary size above the `RULE-MEM-03` budget.

#### `TASK-082`: HNSW Persistence via Stable Ordinals
- **Implementation**:
  - Checkpoint v3: each entry's ordinal is stored as a varint and restored exactly.
  - `HnswVectorIndex`: `generation`, `vector_matches`, `live_ordinals`; tracked base-layer inbound degrees.
  - Engine: `hnsw_path`, `hnsw_saved_generation`, reconciliation at open, save at checkpoint.
- **Bugs found and fixed**:
  1. Delta-frame replay order (deletes now applied before upserts): a re-created key was deleted on replay.
  2. Unreachable outlier vectors in HNSW (connectivity invariant).
  3. Reactor wake latency under an untimed wait (hybrid poll policy).
- **Complexity / footprint**:
  - Open cost with a current `hnsw.idx`: from `O(N)` HNSW insertions (`O(N log N · ef · D)`) down to `O(N · D)` comparisons.
  - Checkpoint: `O(V)` graph write only when the graph changed.
  - `primary.idx` grows by about `1.9 B/record` for the ordinal varint (`19.0 -> 20.9 B/record` in the benchmark): the price of stable ordinals, which ordinal-keyed persisted indexes require.
- **Verification**: `86` tests passing, `0` clippy warnings. On a quiet run of the benchmark gate, all allocation gates passed; `get_ref` `225 ns`, `update` `2.8 µs` (`267 k ops/s`), secondary index `5.9 µs`. TCP GET was at parity with the previous poll policy in interleaved A/B runs.

#### `TASK-083`: Paged Secondary-Index Tier
- **Audit of the existing paged indexes**:
  - `checkpoint_to_disk` decoded every posting into a `BTreeMap` and built the whole file in RAM (an `O(N)` spike at every checkpoint, defeating paging).
  - It held the index write lock across the entire disk write (blocking every query and index update, a `RULE-CONC-01` violation).
  - `PagedBitmapIndex` did the same.
  - About 8 of the 14 query operations used by the query engine were missing.
- **Design**:
  - Disjoint base / frozen / hot sources keep counts and merges exact.
  - Freeze in `O(1)`; stream-merge without locks; generation files behind an atomic manifest (a mapped file cannot be replaced on Windows).
  - Stamps prove that the indexes and the primary checkpoint come from the same checkpoint.
  - Reuse requires a clean shutdown: an empty WAL proves no write raced the last checkpoint. A crash forces a rebuild, the same cost the memory tier pays on every open.
  - The WAL is synced before index generations are published, so a write lost under `AsyncMemoryFirst` can never leave postings behind.
- **Rejected alternative**: a consistent cut under `lock_all` would pause writers for an `O(N)` primary snapshot. Snapshot-then-freeze without the WAL-replay rule would leave ghost postings for records inserted between snapshot and freeze, because replay reallocates their ordinals.
- **Complexity / footprint**:
  - Paged-tier RAM = separator directories (`24 B` / 4 KiB page) + tag directory (`8 B` per non-empty 4096-ordinal chunk) + delta since the last checkpoint.
  - Checkpoint memory `O(page)`.
  - Paginated scans `O(offset + limit)` plus the delta range.
  - Memory-tier dispatch overhead measured at parity, interleaved in-process: direct `4.8–6.3 µs` vs tiered `4.9–5.1 µs` per query; `89–97 ns` per insert+remove in both.
- **Verification**: `91` tests passing, `0` warnings (native and portable). Benchmark gate: allocation gates pass. Whole-engine latencies sat within load noise (the untouched `InlineId` hash ran at 2x its usual time); the isolated dispatch benchmark above is the authoritative comparison.
- **Still open**: cold primary tier (`F-PIDX-03`). The ordinal table and primary hash map remain `O(N)` RAM; tiering them needs a paged ordinal table.

#### `TASK-084`: Encryption at Rest
- **Design**:
  - Records stay plaintext in RAM (`hot_dirty`, MVCC history, decrypted cache) and are sealed only on their way to disk or the WAL.
  - A zero-copy read cannot work on ciphertext, so the first read of a version decrypts into a 64-byte-aligned buffer cached by `(id, version)`. Later reads are an `Arc` bump: RULE-MEM-01 holds for warm reads.
  - Cache memory is bounded (`decrypted_cache_bytes`, random replacement per shard, so hits take only a read lock).
- **Security properties**:
  - AEAD with fresh nonces.
  - Authenticated header metadata and record id (no frame swaps, no TTL or version edits).
  - Tampering yields errors, never altered data.
  - No downgrade: plaintext frames are rejected once a key is configured.
  - Key material is zeroed on drop and never printed.
- **Not encrypted (documented)**: record ids and locations in `primary.idx`, indexed tag / numeric values in the secondary-index files, quantized vectors in `hnsw.idx`, and the WAL frame headers (ids and versions).
- **Benchmark gate**:
  - Encrypted warm `with_ref` `268 ns` p50, `0.00` allocs/op (plaintext `with_ref` `300 ns` in the same run).
  - Cold decrypt-per-read `2.9 µs`, `6` allocs.
  - The gate caught one regression in this task: an extra WAL body copy on the unencrypted write path (`update` `5.00 -> 6.00` allocs/op). Fixed by borrowing the frame unless it must be sealed; back to `5.00`.
- **Verification**: `95` tests passing, `0` clippy warnings (native and portable).

#### `TASK-085`: Key Ring, Online Enable / Disable, Rotation
- **Design**: key ids are stored in each sealed frame and bound by the AEAD; settings are swappable at runtime (atomic fast-path flag plus `RwLock<Arc<..>>`). `reseal_all` is WAL-backed and keeps versions; superseded frames are scrubbed (all segments evacuated; ping-pong double write).
- **Findings**:
  - Data remanence: re-sealing alone left old plaintext in dead arena slots, and segment dead-byte counters do not survive restarts. Fixed with the full evacuation.
  - The ping-pong same-version write targeted the slot readers ignore. Fixed, with a dedicated regression test.
- **Limits (documented)**: deleting a file does not guarantee physical erasure on SSDs, so rotating away from a compromised key protects future data only. WAL segments archived for PITR keep their original keys.
- **Verification**: `97` tests passing, `0` warnings; allocation gates unchanged; encrypted warm read `275 ns` `0.00` allocs.

#### `TASK-086`: Per-Record Protection Policy
- **Design**: the record type decides per record, zero-copy, through `Indexable::protection`: `Encrypt` (confidentiality + integrity) or `Authenticate` (integrity only: plaintext payload plus a `32 B` MAC trailer). Both forms share the AAD that binds id, key id and header metadata. The engine's single `desired_protection` drives writes, WAL bodies and `reseal_all`, so a policy change is applied online by the existing WAL-backed, scrubbing re-seal.
- **Security audit**: no downgrade (plain frames still rejected with a key configured); tag comparison is constant time; the trailer is located from the authenticated `payload_len`; the hostile sweep covers flipped bits and truncations of authenticated frames. Authenticated frames are verified on every cold read into the cache rather than read zero-copy, because a cached "verified" bit could outlive a key revocation.
- **Complexity / footprint**: authenticated write = one Poly1305 pass (`O(n)`, no ChaCha20 over the payload); `+32 B` per authenticated record vs `+32 B` (`key_id + nonce + tag`) per encrypted record.
- **Verification**: `99` tests passing, `0` warnings (native and portable); benchmark allocation gates unchanged (encrypted warm `with_ref` `0.00` allocs/op).

#### `TASK-087`: Faster Cold Decryption
- **Implementation**:
  - ChaCha20 keystream in vertical SIMD layout (each register = one state word of 4 / 8 blocks), transposed only for the final XOR. AVX2 is selected at runtime; SSE2 is the x86-64 baseline.
  - Poly1305 moved to radix 2^44.
  - Frames built in place in their final aligned buffer.
- **Measured** (isolated A/B, 4 KiB, loaded machine; ratios are what count): ChaCha20 old `41–64 µs` → SSE2 `18–32 µs` → AVX2 `8–15 µs`; Poly1305 `16–18 µs` → `11.5–13 µs`. A portable lane-array attempt measured slower than the old loop and was dropped.
- **Engine gate**: encrypted cold read `6.1 µs` → `4.2 µs` p50 and `6` → `4` allocs/op. Warm reads unchanged at `0` allocs.
- **Safety audit**: `unsafe` only for the intrinsics and unaligned 16-byte loads and stores, each with a `SAFETY` note. The AVX2 path runs only after `is_x86_feature_detected!`. Every SIMD path is checked against the scalar reference at all lengths and near counter wrap. MSRV 1.82: no `as_chunks`, and `#[target_feature]` only on `unsafe fn`.

#### `TASK-088`: Index Files Encrypted at Rest
- **Design**:
  - The sealed unit's AAD = magic | key id | position | file name.
  - Paged generations: one streamed AEAD region with random-access decryption. The open-time verification piggybacks on the existing full validation pass, so reads pay only the keystream (a 4 KiB page ≈ one SIMD ChaCha20 pass), not the MAC.
  - Delta frames are bound to their offset (sealed appends are serialized).
- **Security audit**: no downgrade (plaintext index files refused with keys configured); constant-time tags; truncation, flips, swapped files and reordered frames are all detected; tampered paged generations are rebuilt, never served. Limits are documented: file names and sizes, WAL frame headers, and record file names stay visible; there is no rollback protection against restoring an entire older directory.
- **Complexity / footprint**:
  - `+40 B` per sealed file and `+52 B` per sealed delta frame.
  - Paged reads: `+1` keystream pass per page or block touched, on stack buffers (no allocations).
  - Open: one extra MAC pass over each paged generation.
- **Verification**: `108` tests passing; `0` clippy warnings (native and portable).

#### `TASK-089`: Raft InstallSnapshot
- **Implementation**: wire types `InstallSnapshotReq` / `InstallSnapshotResp`; snapshot file format with a per-record checksum binding id and frame; leader generation via an MVCC point-in-time read; follower staging, two-pass install (verify, then apply), crash marker and recovery; automatic use in `RaftReplicationGroup`; TCP opcode `0x15`.
- **Correctness audit**:
  - Lock order is `snapshot_lock` → `apply_lock` everywhere.
  - The snapshot point is consistent: `last_applied` and the read snapshot are taken together under `apply_lock`.
  - Log entries after `last_included_index` are kept only on a term match (Raft §7).
  - `commit_index` never moves backwards.
  - Corruption is caught before the engine changes. Unknown or failed installs never leave a readable half state.
- **Complexity / footprint**:
  - Leader: `O(N)` snapshot write, `O(ids)` memory.
  - Follower: `O(largest frame)` streaming memory plus an `O(ids)` set.
  - Network: `≤ 1 MiB` per RPC, resumable.
- **Verification**: raft tests (4) and server tests pass; `0` clippy warnings.

#### `TASK-092`: Cold Primary Tier
- **Design**:
  - The primary index becomes two layers: an immutable cold generation (the state at the last checkpoint) and a RAM delta (entries changed since, tombstones, overridden ordinal slots).
  - Checkpoints are stream-merges into a new generation followed by a per-shard install that evicts whatever did not change since the cut.
  - The ordinal table moved to disk as a dense array, so `RecordOrdinal` resolution stays `O(1)`.
- **Correctness audit**:
  - The invariant tombstones ∩ RAM = ∅ prevents a re-created record from being hidden by its own tombstone after eviction.
  - Removes always tombstone, because a checkpoint in progress may be writing the id into the next tier; stale tombstones are pruned at install.
  - The tier pointer is re-checked under the write lock (`with_tier_copy`), so a swap between the lock-free tier read and the update retries instead of miscounting.
  - Ordinal slots in the new tier are derived from the same cut as the entries, so allocations and removals in flight can never make them disagree. `ColdTier::write` also re-checks that live slots equal the number of entries.
  - A crash before the manifest switch keeps the previous generation, and the WAL is only truncated after the checkpoint completes.
- **Complexity / footprint**:
  - RAM per record: about `100 B` + `25 B` ordinal slot (memory tier) → about `1.25 B` Bloom filter + `8 B` per page (≈ 50 records) + only the records changed since the last checkpoint.
  - Disk: about `25–30 B` per record for entries + `24 B` per ordinal slot.
  - Lookups miss RAM at the cost of one page decode (4 KiB; decrypted when sealed).
  - Open: `O(file)` validation pass, no entries loaded.
- **Benchmark finding, fixed in this task**: the first page format decoded a page's varint entries sequentially to find one id; the new benchmark row showed `44 µs` p50 per tier read. Adding a `u16` slot-offset array at the page end (`2 B` per entry) lets lookups binary-search by decoding only about 6 ids: an isolated probe went from `25.6 µs` to `1.7–2.6 µs` on the loaded machine.
- **Limits (documented)**: a reader holding a superseded generation keeps it mapped until it finishes (on Windows its file is deleted at the next open); scans over tier-only records re-validate frames.

#### `TASK-091`: Binary Size Budget
- **Decision (user, 2026-10-27)**: the `RULE-MEM-03` budget is raised from `500 KiB` to `< 1 MiB`. It is a ceiling, not a target: size keeps being minimized wherever it costs no hot-path speed.
- **Measured** (Windows x86-64, fat LTO, stripped, `opt-level = 3`): `storage-server` `903.0 KiB`, `storage-cli` `830.5 KiB`. For reference, before the cold tier landed: `647` / `613 KiB` at `opt-level = "s"` and `591.5` / `572 KiB` at `"z"`. Neither was adopted: `opt-level` stays `3` for hot-path speed.

#### `TASK-090`: `epoll` Reactor on Linux (implemented, Linux test run pending)
- **Implementation**: an `EpollPoller` (raw syscalls, level-triggered, token per socket) and a Linux reactor loop that registers each socket once and drives only ready connections and those awaiting admin jobs. The per-wake-up cost goes from `O(connections per shard)` (`poll` rebuilds and scans the full descriptor list) to `O(ready)`. Fallback: `STORINE_REACTOR=poll`.
- **Safety audit**: three `unsafe` FFI calls, each with a `SAFETY` note. The epoll descriptor is owned and closed once. Sockets are unregistered before they are closed, so a reused fd number can never inherit a stale registration. Tokens are mapped back through a map that is updated on `swap_remove`.
- **Verification**: Linux type-check and clippy clean for the whole workspace (the Linux target was installed for this; there is no Linux linker or WSL here, so nothing Linux-specific has executed). The first Linux build also surfaced three latent clippy warnings in Unix-only mmap code, now fixed. **Not yet run on Linux**: the epoll unit test and the server tests must pass there before this task is closed.

