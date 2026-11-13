# End-to-End Tasks Order: Commencement to Completion (`TASKS.md`)

This document defines the strict chronological execution order for building the entire storage engine and full-scale distributed production database from an empty directory to a benchmark-verified v1.0 release. Each stage must pass all exit criteria, complexity gates, zero-allocation checks, and security audits before advancing to the next.

---

## Stage 0: Workspace Foundation, Compiler Tuning & Benchmark Harness

- [x] **TASK-001: Initialize Multi-Crate Cargo Workspace & Optimization Profiles**
  - **Deliverables**: Root `Cargo.toml`, `.cargo/config.toml`, and crate skeletons (`storage-types`, `storage-codec`, `storage-mmap`, `storage-wal`, `storage-index`, `storage-graph`, `storage-core`, `storage-cli`, `storage-server`, `benches`).
  - **Optimizations**: Configure `opt-level = 3`, `lto = "fat"`, `codegen-units = 1`, `panic = "abort"`, and workspace lints (`deny(unsafe_op_in_unsafe_fn)`, `clippy::perf`).
  - **Exit Criteria**: `cargo check --workspace --all-targets` succeeds with zero warnings.

- [x] **TASK-002: Build Allocation-Counting & Latency Benchmark Harness**
  - **Deliverables**: `benches/src/lib.rs` and `benches/src/main.rs` with a custom `TrackingAllocator` wrapper (counting allocations/bytes per thread) and latency percentile recorder (`p50`, `p95`, `p99`, `p99.9`).
  - **Optimizations**: Thread-local atomic-free counter increments on `alloc`/`dealloc` so measurement overhead is `< 2 ns`.
  - **Exit Criteria**: Harness accurately detects a single 1-byte heap allocation in a test closure and fails `assert_zero_alloc!`.

---

## Stage 1: Core Zero-Allocation Types & Zero-Copy Codec

- [x] **TASK-003: Implement `InlineId` (24-Byte Stack-Inlined Primary Key)**
  - **Deliverables**: `crates/storage-types/src/id.rs`.
  - **Optimizations**: `#[repr(C, align(8))]` 24-byte stack-inlined key (`<= 23` UTF-8 bytes + length byte) with single-multiply Fibonacci hash (`inline_id_hash64`).
  - **Exit Criteria**: `size_of::<InlineId>() == 24`, `0` heap allocations, lookup `< 5 ns`.

- [x] **TASK-004: Implement 64-Byte Cache-Line-Aligned `RecordHeader` & Flags**
  - **Deliverables**: `crates/storage-types/src/header.rs`.
  - **Optimizations**: `#[repr(C, align(64))]` fixed 64-byte header (`magic`, `format_version`, `schema_version`, `collection_id`, `record_version`, `updated_at_ns`, `payload_checksum`, `header_checksum`, `payload_len`, `root_offset`, `flags`).
  - **Exit Criteria**: Compile-time `assert_eq!(size_of::<RecordHeader>(), 64)` and `align_of::<RecordHeader>() == 64`.

- [x] **TASK-005: Implement SIMD Checksums (`XXH3-64` & Hardware `CRC32C`)**
  - **Deliverables**: `crates/storage-codec/src/checksum.rs`.
  - **Optimizations**: 4-way unrolled `XXH3-64` combined with hardware SSE4.2 `_mm_crc32_u64` intrinsics.
  - **Exit Criteria**: Unit tests for corruption detection (single bit-flip detection) + throughput benchmark.

- [x] **TASK-006: Implement Zero-Copy Record Codec, Full Data Types & Pooled Scratch Buffers**
  - **Deliverables**: `crates/storage-codec/src/archive.rs`, `crates/storage-codec/src/types.rs`, `crates/storage-codec/src/scratch.rs`.
  - **Optimizations**: Thread-local 64-byte-aligned `AlignedScratchBuf` encoding `[RecordHeader (64B) | Payload]`, plus `ArchivedBool`, `ArchivedChar`, `ArchivedU128`, `ArchivedI128`, `ArchivedUuid`, `ArchivedStringSlice`, and $O(\log N)$ binary-searched `ArchivedMap` (`PropertyValue`).
  - **Exit Criteria**: Round-trip encode + zero-copy read benchmark shows `0` allocations on read and `3 ns` access latency.

---

## Stage 2: Cross-Platform `mmap` Engine, Bounded Pool & Disk Layout

- [x] **TASK-007: Implement Cross-Platform `MmapRegion` & OS Virtual Memory Hints**
  - **Deliverables**: `crates/storage-mmap/src/raw.rs`.
  - **Optimizations**: Native Win32 `CreateFileMappingW` / `MapViewOfFile` / `PrefetchVirtualMemory` and Unix `mmap` / `madvise` wrappers with embedded `verified: AtomicBool` and `active_guards: AtomicUsize`.
  - **Exit Criteria**: Cross-platform tests passing with `// SAFETY:` proofs documented on every `unsafe` block.

- [x] **TASK-008: Implement `MmapPool` with Clock-PRO / Second-Chance Eviction & Reader Guards**
  - **Deliverables**: `crates/storage-mmap/src/pool.rs`.
  - **Optimizations**: 4-shard table tracking active `MmapRegion`s with 8-byte `MmapGuard` reader pins and Second-Chance eviction when `mapped_bytes` or `open_files` reaches `PoolBudget`.
  - **Exit Criteria**: Stress test keeps open OS handles strictly bounded with zero use-after-unmap bugs under concurrent reader threads.

- [x] **TASK-009: Implement Sharded & Hierarchical Path Resolver (Zero-Alloc Stack Paths)**
  - **Deliverables**: `crates/storage-mmap/src/path.rs`.
  - **Optimizations**: Formats `<base>/<collection>/<shard_hi>/<shard_lo>/<id>.rec` into a stack buffer with traversal sanitization.
  - **Exit Criteria**: Zero heap allocations on stack path formatting; blocks `..` / path injection attempts.

- [x] **TASK-010: Implement Atomic Disk Persistence (Temp+Rename & Ping-Pong Double Slot)**
  - **Deliverables**: `crates/storage-mmap/src/persist.rs`.
  - **Optimizations**: Atomic `.tmp` write + `fsync` + `MoveFileExW` / `rename` and pre-allocated 2-slot Ping-Pong record files.
  - **Exit Criteria**: Simulated mid-write crash leaves previous version 100% intact and readable.

---

## Stage 3: Primary Index, Per-Record Concurrency, Write Coalescing & WAL

- [x] **TASK-011: Implement 64-Shard Primary Index & Strict $O(1)$ Ordinal Recycling**
  - **Deliverables**: `crates/storage-index/src/primary.rs`.
  - **Optimizations**: 64-shard `CachePadded<RwLock<ShardTable>>` with `96-byte` `PrimaryEntry` and $O(1)$ `Some(InlineId::EMPTY)` sentinel transition for deleted `RecordOrdinal` recycling.
  - **Exit Criteria**: Point read lookup `< 200 ns` with `0.00` heap allocations and $O(1)$ ordinal release.

- [x] **TASK-012: Implement Compact Varint Primary Index Checkpointing (`primary.idx`)**
  - **Deliverables**: `crates/storage-index/src/checkpoint.rs`.
  - **Optimizations**: `STGEIDX2` compact varint + 64B-unit arena offset encoding reducing checkpoint size from `88 B/record` to **`19.0 B/record`** (`4.63x` smaller).
  - **Exit Criteria**: Restart benchmark loads index in single-digit milliseconds with full checksum validation.

- [x] **TASK-013: Implement 2,048-Way Striped Per-Record Lock Table & Closure Mutation API**
  - **Deliverables**: `crates/storage-core/src/write_queue.rs`, `crates/storage-core/src/engine.rs`.
  - **Optimizations**: 2,048 `CachePadded` per-key lock stripes + 64-shard `hot_dirty` buffer with hard `max_hot_dirty_bytes` (`64 MiB`) backpressure.
  - **Exit Criteria**: Concurrent disjoint updates scale across cores; same-key updates serialize cleanly with zero lost updates.

- [x] **TASK-014: Implement Group-Commit Write-Ahead Log (`storage-wal`)**
  - **Deliverables**: `crates/storage-wal/src/lib.rs`.
  - **Optimizations**: Leader-follower batching queue merging concurrent writers into a single `WriteFile` + `FlushFileBuffers`/`fsync`, with automatic torn-frame truncation on crash recovery.
  - **Exit Criteria**: Crash recovery replays all committed frames and truncates torn trailing bytes cleanly.

- [x] **TASK-015: Implement Background Async Flusher & Write Coalescer**
  - **Deliverables**: `crates/storage-core/src/engine.rs` (`WriteCoalescer` & `flush_dirty_batch`).
  - **Optimizations**: Deduplicates rapid updates to hot keys (`71.6%+` reduction in physical disk writes) and automatically re-enqueues keys on transient flush errors.
  - **Exit Criteria**: Hot-key benchmark proves massive write amplification reduction with 100% latest-value visibility.

---

## Stage 4: Secondary Indexes & Zero-Allocation Query Engine

- [x] **TASK-016: Define `Indexable` Trait & Zero-Copy Differential Index Delta Extractor**
  - **Deliverables**: `crates/storage-index/src/traits.rs`.
  - **Optimizations**: `264-byte` Struct-of-Arrays `IndexSnapshot` extracted zero-copy from `&T::Archived` (`0` duplicate heap maps in RAM); skips secondary index locks when indexed fields are unchanged.
  - **Exit Criteria**: Updating a non-indexed field incurs zero secondary index lock acquisitions.

- [x] **TASK-017: Implement Order-Preserving B+Tree Numeric Range Index**
  - **Deliverables**: `crates/storage-index/src/btree.rs`.
  - **Optimizations**: Inline-first `LeafPostingList` (`[RecordOrdinal; 4]`) + bijective IEEE-754 `sortable_f64_to_i64` and `sortable_u64_to_i64` encodings.
  - **Exit Criteria**: Range scan over `i64`, `u64`, and `f64` fields resolves matching ordinals in microseconds.

- [x] **TASK-018: Implement Word-Aligned `CompressedBitmap` Tag Index**
  - **Deliverables**: `crates/storage-index/src/bitmap.rs`.
  - **Optimizations**: 64-bit word-aligned bitmap index driving multi-tag `AND` intersections from the smallest posting list using hardware `TZCNT` (`trailing_zeros()`).
  - **Exit Criteria**: Multi-tag intersection executes in microseconds.

- [x] **TASK-019: Implement Fluent Query Builder & Hybrid Search Execution**
  - **Deliverables**: `crates/storage-core/src/query.rs`.
  - **Optimizations**: Intersects tag bitmaps and numeric range bitmaps before touching record payloads.
  - **Exit Criteria**: End-to-end hybrid query verified in `benches` (`5.9 µs` p50).

---

## Stage 5: Explicit Relationships, Hierarchies & Referential Integrity

- [x] **TASK-020: Implement Zero-Copy Relationship Containers (`OneToOne`, `OneToMany`, `ManyToMany`)**
  - **Deliverables**: `crates/storage-graph/src/containers.rs`.
  - **Optimizations**: `InlineId::EMPTY` niche in `ArchivedOneToOne` (`-8 bytes` per record) and sorted `ArchivedSlice<InlineId>` for $O(\log K)$ binary-search membership checks on `mmap`.
  - **Exit Criteria**: Relationship membership check and slice access require `0.00` heap allocations.

- [x] **TASK-021: Implement Deadlock-Free Bidirectional `update_pair_atomic`**
  - **Deliverables**: `crates/storage-graph/src/integrity.rs`, `crates/storage-core/src/engine.rs`.
  - **Optimizations**: Acquires locks in canonical ascending `(CollectionId, InlineId)` order and commits both records in one atomic WAL transaction.
  - **Exit Criteria**: Concurrent opposite-order `(A, B)` and `(B, A)` linking runs with zero deadlocks.

- [x] **TASK-022: Implement Referential Integrity Policies (`Restrict`, `Cascade`, `Unlink`)**
  - **Deliverables**: `crates/storage-core/src/engine.rs` (`delete_with_policy`, `verify_relationships`).
  - **Optimizations**: Cycle-safe BFS cascade deletion and canonical multi-lock peer unlinking.
  - **Exit Criteria**: Verified zero dangling edges after `Unlink` and `Cascade` deletes.

- [x] **TASK-023: Implement Batched Zero-Copy Graph Traversal & Parent-Group Co-Location**
  - **Deliverables**: `crates/storage-core/src/engine.rs` (`get_related_refs`).
  - **Optimizations**: Co-locates records sharing a `parent_group` into the same arena segment group for OS page-cache locality.
  - **Exit Criteria**: Batch neighbor resolution returns zero-copy `RecordRef<T>` handles in microseconds.

---

## Stage 6: Multi-Record Transactions, Arena Compaction & SIMD Vector Search

- [x] **TASK-024: Implement Multi-Record Atomic Transactions (`db.transaction`)**
  - **Deliverables**: `crates/storage-core/src/engine.rs` (`TransactionBatch`).
  - **Optimizations**: Optimistic read-version checking + canonical sorted lock acquisition + single multi-op WAL transaction commit.
  - **Exit Criteria**: Multi-record atomic invariant tests pass under contention.

- [x] **TASK-025: Implement 4 KiB-Growable Slotted Arena Segments & Live Compactor**
  - **Deliverables**: `crates/storage-mmap/src/arena.rs`, `crates/storage-core/src/engine.rs` (`compact_arenas`).
  - **Optimizations**: Segments start at `4 KiB` (1 OS page) and double on demand up to `64 MiB`; `compact_arenas` evacuates live records and deletes dead `.dat` files on disk.
  - **Exit Criteria**: Reclaims 100% of dead segment files without blocking concurrent readers or writers.

- [x] **TASK-026: Implement Schema Version Validation & Collection ID Isolation**
  - **Deliverables**: `crates/storage-codec/src/archive.rs`.
  - **Optimizations**: Enforces `hdr.schema_version` and `hdr.collection_id == T::COLLECTION_ID` on frame verification.
  - **Exit Criteria**: Cross-collection and mismatched-schema reads are deterministically caught.

- [x] **TASK-027: Implement TTL Expiration, Cold LZ4 Compression & $O(N + K \log K)$ SIMD Vector Search**
  - **Deliverables**: `crates/storage-codec/src/compress.rs`, `crates/storage-index/src/vector.rs`, `crates/storage-core/src/engine.rs`.
  - **Optimizations**: 8-way unrolled SIMD cosine similarity (`87 ns` / 64-dim vector) over zero-copy `mmap` embeddings + Hoare/Floyd-Rivest Quickselect (`select_nth_unstable_by`) for linear $O(N + K \log K)$ top-$K$ selection.
  - **Exit Criteria**: SIMD cosine benchmark exceeds `11.5M vectors/sec` with `0.00` heap allocations.

---

## Stage 7: CLI, Sharded Non-Blocking Network Server & Chaos Verification

- [x] **TASK-028: Build `storage-cli` Inspection, Compaction & Benchmark Tool**
  - **Deliverables**: `crates/storage-cli/src/main.rs`.
  - **Exit Criteria**: Compiles to a self-contained `441.5 KiB` binary supporting `put`, `get`, `delete`, `stats`, and `compact`.

- [x] **TASK-029: Build `storage-server` (Sharded Non-Blocking Event Reactor & Admission Control)**
  - **Deliverables**: `crates/storage-server/src/lib.rs`, `crates/storage-server/src/main.rs`.
  - **Optimizations**: `~88 bytes` per idle `NonBlockingConn` multiplexed across `ReactorShard` workers, zero-alloc `IoSlice` vectored socket writes (`34.0 µs` p50), per-connection TCP backpressure, fallible `try_reserve_exact` OOM defense, and dynamic `max_connections` admission control (`STATUS_OVERLOADED`).
  - **Exit Criteria**: Passes end-to-end zero-copy streaming and overload admission control tests; compiles to `370.5 KiB`.

- [x] **TASK-030: Execute Full Chaos Crash-Testing & End-to-End Performance Gate**
  - **Deliverables**: `crates/storage-core/tests/chaos_and_e2e.rs`, `docs/BENCHMARK_REPORT.md`.
  - **Exit Criteria**: 25 consecutive simulated power-loss cycles with torn WAL frames recover 100% of committed state; all zero-allocation and latency benchmarks documented in `docs/BENCHMARK_REPORT.md`.

---

## Stage 8: Multi-Collection `DatabaseCatalog` & Online Schema Evolution (`v1.0`)

- [x] **TASK-031: Implement `DatabaseCatalog` Multi-Collection Manager & Shared Resource Pool**
  - **Deliverables**: `crates/storage-core/src/catalog.rs` (`F-CAT-01`, `RULE-MEM-04`, `RULE-SCALE-02`).
  - **Optimizations**:
    - Host multiple named collections (`"users"`, `"orders"`, `"vectors"`, etc.) inside one database root directory sharing a single `MmapPool`, a single `GroupCommitWal`, and a unified `max_hot_dirty_bytes` memory budget.
    - Store collection metadata (`name -> collection_id`, schema version, record count) in a compact checksummed `catalog.meta` file (`< 4 KiB`).
    - Provide type-safe `catalog.collection::<T>("name")` handles with $O(1)$ lookup and zero lock contention across collections.
  - **Exit Criteria**: Opening 16 distinct collections in a single `DatabaseCatalog` uses 1 shared `GroupCommitWal` and 1 shared `MmapPool`, with unit tests verifying complete key and `collection_id` isolation.

- [x] **TASK-032: Implement Cross-Collection Atomic Transactions (`CatalogTransaction`)**
  - **Deliverables**: `crates/storage-core/src/catalog.rs` (`F-CAT-02`, `RULE-CONC-01`, `RULE-ERR-01`).
  - **Optimizations**:
    - Allow a single atomic transaction to read, insert, update, and delete records across multiple collections (`collection_id_a`, `collection_id_b`) simultaneously.
    - Sort all locked keys globally by `(collection_id, InlineId)` before acquiring stripe locks so cross-collection AB-BA deadlocks are mathematically impossible.
    - Write all cross-collection mutations as a single `GroupCommitWal::append_transaction` batch and replay them atomically on crash recovery.
  - **Exit Criteria**: Multi-threaded cross-collection transfer test (moving items between `accounts` and `ledger` collections under simulated crashes) preserves 100% cross-collection invariants with zero deadlocks.

- [x] **TASK-033: Implement Online Lazy Schema Evolution & Upcasting Pipeline (`SchemaRegistry`)**
  - **Deliverables**: `crates/storage-core/src/migration.rs` (`F-CAT-03`, `F-OPS-01`, `RULE-CPU-04`).
  - **Optimizations**:
    - Register zero-downtime migration functions (`fn(from_ver: u16, raw_payload: &[u8]) -> Result<AlignedBytes>`) stepping records from older `schema_version`s (`v1 -> v2 -> ... -> T::SCHEMA_VERSION`).
    - Keep the fast path (`hdr.schema_version == T::SCHEMA_VERSION`) branch-predicted and 0-alloc (`187 ns`); annotate the upcasting path with `#[cold]` and persist upgraded frames automatically during background `compact_arenas`.
  - **Exit Criteria**: Records written under `SchemaV1` are transparently read as `SchemaV2` and permanently upgraded to `SchemaV2` on disk after `compact_arenas`.

---

## Stage 9: Disk-Backed Paged Primary & Secondary Indexes (`v1.0`)

- [x] **TASK-034: Implement Memory-Mapped Paged B+Tree Range Index (`PagedBTreeIndex`)**
  - **Deliverables**: `crates/storage-index/src/paged_btree.rs` (`F-PIDX-01`, `RULE-MEM-01`, `RULE-CPLX-01`).
  - **Optimizations**:
    - Serialize sorted `(i64_key, RecordOrdinal)` postings into 4 KiB-aligned disk pages (`btree_<field>.idx`) with base-key delta compression and a tiny in-RAM top-level page separator array (`16 bytes` per 4 KiB page = `0.4%` RAM overhead).
    - Map index pages via `MmapPool` and perform $O(\log B)$ binary search directly on the mapped page bytes with **`0.00` heap allocations**.
  - **Exit Criteria**: Range queries over `PagedBTreeIndex` match in-memory `NumericRangeIndex` results 100% while keeping heap resident index memory `< 1%` of data size and performing `0` heap allocations during page lookup.

- [x] **TASK-035: Implement Memory-Mapped Paged Bitmap Tag Index & Cold Primary Bloom Pages**
  - **Deliverables**: `crates/storage-index/src/paged_bitmap.rs`, `crates/storage-index/src/primary.rs` (`F-PIDX-02`, `F-PIDX-03`, `RULE-MEM-03`).
  - **Optimizations**:
    - Persist tag `CompressedBitmap` word arrays into a 64-byte-aligned memory-mapped file (`bitmap.idx`) so multi-tag `AND`/`OR` intersections run directly over `mmap` `[u64]` slices with `0` heap allocations.
    - Add a compact 64-bit split-block Bloom filter (`~10 bits/key`) for cold `STGEIDX2` primary index pages so non-existent key lookups reject in $O(1)$ without touching cold disk pages.
  - **Exit Criteria**: Multi-tag intersections over `PagedBitmapIndex` execute directly from `mmap` pages with `0` heap allocations for word access; Bloom filter rejects `> 98%` of missing keys in `1` CPU cache line read.

---

## Stage 10: Sub-Linear HNSW Vector Index & `SQ8` Scalar Quantization (`v1.0`)

- [x] **TASK-036: Implement `SQ8` Scalar Quantizer & 8-Way Integer SIMD Distance Kernel**
  - **Deliverables**: `crates/storage-index/src/quantize.rs` (`F-HNSW-01`, `RULE-MEM-03`, `RULE-CPU-02`).
  - **Optimizations**:
    - Quantize `f32` vectors into compact 8-bit `QuantizedVecSq8` (`min: f32`, `scale: f32`, `inv_norm: f32`, `codes: [u8; D]`), shrinking vector footprint by **`4x` (`75%` reduction)**.
    - Implement an 8-way loop-unrolled `u8` dot-product SIMD kernel using integer accumulators (`u32`) with zero floating-point instructions inside the inner loop.
  - **Exit Criteria**: `SQ8` quantization reduces 64-dim vector storage from `256 B` to `76 B`, achieves `> 99%` top-10 recall vs exact `f32` cosine similarity, and runs with `0.00` heap allocations on scoring.

- [x] **TASK-037: Implement Multi-Layer HNSW Proximity Graph Index ($O(\log N)$ ANN Search)**
  - **Deliverables**: `crates/storage-index/src/hnsw.rs` (`F-HNSW-02`, `RULE-CPLX-01`, `RULE-MEM-04`).
  - **Optimizations**:
    - Build a multi-layer navigable small-world graph (`M = 16`, `M0 = 32`, `ef_construction = 64`, `ef_search = 32`) over `RecordOrdinal`s using flat cache-aligned neighbor slots (`[u32; 32]`) and `SQ8` quantized vectors.
    - Use a thread-local generation-stamped visited table (`visited_gen: Vec<u32>`, incremented per query) so HNSW beam search never allocates or zeros a `HashSet` per query.
    - Support persisting and memory-mapping the HNSW graph + `SQ8` vectors to `hnsw.idx`.
  - **Exit Criteria**: HNSW search over 20,000+ vectors executes in logarithmic $O(\log N)$ time (`> 5x` faster than brute-force scan) with `> 95%` recall and `0` per-node heap allocations during graph traversal.

- [x] **TASK-038: Integrate Adaptive Filter-Aware Hybrid HNSW Planner into `StorageEngine`**
  - **Deliverables**: `crates/storage-core/src/engine.rs`, `crates/storage-core/src/query.rs` (`F-HNSW-03`, `RULE-CPLX-01`).
  - **Optimizations**:
    - Automatically maintain the HNSW + `SQ8` index on record insert, update (when `embedding_hash` changes), and delete.
    - In `search_hybrid`, automatically choose:
      1. **Exact SIMD Scan + Quickselect** when metadata filters narrow candidates to $\le \max(512, 0.05 N)$.
      2. **Predicate-Guided HNSW Traversal + Exact Top-$2K$ Re-Ranking** when unfiltered or when candidate selectivity $> 5\%$.
  - **Exit Criteria**: Unfiltered and lightly-filtered top-$K$ vector queries use HNSW in $O(\log N)$ time and re-rank top candidates with exact `f32` cosine similarity from `mmap`.

---

## Stage 11: Multi-Version Concurrency Control (MVCC), Segmented WAL & PITR (`v1.0`)

- [x] **TASK-039: Implement MVCC Snapshot Isolation (`ReadSnapshot`, `CommitTs` & Epoch GC)**
  - **Deliverables**: `crates/storage-core/src/mvcc.rs`, `crates/storage-core/src/engine.rs` (`F-MVCC-01`, `F-MVCC-02`, `F-MVCC-03`, `RULE-MEM-04`).
  - **Optimizations**:
    - Assign a strictly monotonic 64-bit `commit_ts` on every write/transaction.
    - Provide `engine.begin_snapshot() -> ReadSnapshot<'_, T>` which registers an active `read_ts` in a lock-free watermark tracker.
    - When a record is updated or deleted while `active_snapshots > 0`, retain the prior `StorageLocation` + `commit_ts` in a bounded shard-local version chain so `snapshot.get_ref(id)` and `snapshot.with_ref(id, ...)` always return the exact state as of `snapshot.read_ts()`.
    - Automatically garbage-collect historical versions as soon as the last `ReadSnapshot` older than `commit_ts` is dropped (`0` memory bloat when no snapshots are open).
  - **Exit Criteria**: A `ReadSnapshot` opened at $t_1$ continues reading $t_1$'s exact record versions and range results even after 1,000 concurrent updates and deletes at $t_2 > t_1$, and 100% of historical version entries are reclaimed when the snapshot drops.

- [x] **TASK-040: Implement Segmented WAL Rotation (`wal_XXXXXX.log`) & LSN Watermarks**
  - **Deliverables**: `crates/storage-wal/src/lib.rs` (`F-PITR-01`, `RULE-CONC-01`, `RULE-ERR-01`).
  - **Optimizations**:
    - Assign a monotonic 64-bit `lsn` (Log Sequence Number) to every WAL frame.
    - Automatically rotate `wal_000001.log -> wal_000002.log` when a segment reaches `max_segment_bytes` (configurable, default `64 MiB`).
    - Reclaim or archive sealed WAL segments whose `max_lsn <= checkpoint_lsn` without blocking concurrent writers.
  - **Exit Criteria**: Writing across multiple WAL segment rotations and simulating a mid-segment crash replays all segments in LSN order and prunes obsolete segments cleanly after checkpointing.

- [x] **TASK-041: Implement Online Hot Backup & Point-in-Time Recovery (`PITR`)**
  - **Deliverables**: `crates/storage-core/src/backup.rs` (`F-PITR-02`, `RULE-ERR-01`).
  - **Optimizations**:
    - `engine.create_online_backup(backup_dir)` flushes a consistent checkpoint, hardlinks/copies immutable arena segments and WAL files, and writes a checksummed `backup.manifest` without stopping foreground reads or writes.
    - `StorageEngine::restore_point_in_time(backup_dir, restore_dir, target_timestamp_ns)` restores the base checkpoint and replays WAL frames strictly up to `updated_at_ns <= target_timestamp_ns`.
  - **Exit Criteria**: Taking an online backup during concurrent writes and restoring to an intermediate timestamp $T_{\text{mid}}$ recovers the exact database state as of $T_{\text{mid}}$ with 0 later mutations leaked.

---

## Stage 12: OS Kernel Event Polling, Full Wire Protocol & Security (`v1.0`)

- [x] **TASK-042: Upgrade `StorageServer` Reactor with OS Kernel Socket Readiness Polling (`WSAPoll` / `poll`)**
  - **Deliverables**: `crates/storage-mmap/src/net_poll.rs`, `crates/storage-server/src/lib.rs` (`F-NET-01`, `RULE-CPU-03`, `RULE-SCALE-01`).
  - **Optimizations**:
    - Implement zero-dependency OS socket readiness polling (`WSAPoll` on Windows `ws2_32`, `poll` on Unix `libc`) inside each `ReactorShard`.
    - When all connections on a shard are idle, the worker blocks in the OS kernel poller (`0.0%` CPU usage) and wakes immediately when any socket becomes readable/writable or a new connection arrives.
  - **Exit Criteria**: 1,000 idle connections consume `0%` CPU while active RPC connections maintain `> 20,000 ops/sec` zero-copy throughput.

- [x] **TASK-043: Implement Full Database Wire Protocol v2 (`CAS`, `BATCH_GET`, `BATCH_PUT`, `QUERY_HYBRID`, `METRICS`)**
  - **Deliverables**: `crates/storage-server/src/lib.rs` (`F-NET-02`, `RULE-MEM-01`, `RULE-ERR-01`).
  - **Optimizations**:
    - Add binary opcodes:
      - `OP_CAS_PUT (0x08)`: Conditional versioned update (`STATUS_CONFLICT = 0x04`).
      - `OP_BATCH_GET (0x09)`: Multi-key pipelined zero-copy frame streaming in 1 network round-trip.
      - `OP_BATCH_PUT (0x0A)`: Multi-key atomic transaction write in 1 network round-trip.
      - `OP_QUERY_HYBRID (0x0B)`: Tag + Numeric Range + Top-$K$ Vector similarity query over TCP.
      - `OP_METRICS (0x0D)`: Returns live Prometheus/OpenMetrics text telemetry.
  - **Exit Criteria**: Integration tests verify `cas_put`, `batch_get`, `batch_put`, `query_hybrid`, and `metrics` over live TCP sockets.

- [x] **TASK-044: Implement Token Authentication, RBAC, Rate Limiting & Encryption-at-Rest**
  - **Deliverables**: `crates/storage-server/src/security.rs`, `crates/storage-codec/src/crypto.rs` (`F-SEC-01`, `F-SEC-02`, `RULE-ERR-01`).
  - **Optimizations**:
    - Constant-time token authentication (`OP_AUTH = 0x0C`) assigning `RbacRole::ReadOnly`, `RbacRole::ReadWrite`, or `RbacRole::Admin` per connection.
    - Per-connection O(1) token-bucket rate limiter (`TokenBucket`) shedding abusive floods with `STATUS_RATE_LIMITED`.
    - Pure-Rust 64-byte-block ChaCha20 20-round ARX stream cipher + 128-bit subkey MAC for optional transparent payload encryption-at-rest (`RecordFlags::ENCRYPTED`).
  - **Exit Criteria**: Unauthenticated/unauthorized writes are rejected with `STATUS_UNAUTHORIZED (0x05)`, rate-exceeded clients receive `STATUS_RATE_LIMITED`, and encrypted payloads detect single-bit tampering.

---

## Stage 13: Distributed Replication (Raft) & Horizontal Cluster Sharding (`v1.0`)

- [x] **TASK-045: Implement `1,024` Consistent-Hash Virtual Shards (`vshards`) & `ClusterRouter`**
  - **Deliverables**: `crates/storage-core/src/sharding.rs` (`F-DIST-01`, `RULE-SCALE-03`, `RULE-CPLX-01`).
  - **Optimizations**:
    - Map every `InlineId` to a deterministic virtual shard `vshard = (id.fast_hash() & 1023) as u16` in $O(1)$ (`1` bitwise AND).
    - Build `ClusterMap` & `ClusterRouter` maintaining epoch-versioned `vshard -> NodeId` ownership tables (`2 KiB` total in RAM) and routing multi-node client operations automatically.
  - **Exit Criteria**: `ClusterRouter` deterministically partitions keys across multiple `StorageEngine` nodes and routes `put`/`with_ref`/`delete` to the owning shard node in $O(1)$ time.

- [x] **TASK-046: Implement Embedded Raft Consensus Replication & Follower Read Scaling**
  - **Deliverables**: `crates/storage-core/src/raft.rs` (`F-DIST-02`, `RULE-CONC-01`, `RULE-ERR-01`).
  - **Optimizations**:
    - Implement a state-machine Raft consensus group (`Leader`, `Follower`, `Candidate`) replicating log entries across nodes with `(current_term, voted_for, commit_index, last_applied)`.
    - Support follower read-replica offloading (`read_at_commit_index`) for horizontal read scaling.
  - **Exit Criteria**: A 3-node Raft cluster replicates writes across quorum, survives isolating the leader node, elects a new leader automatically, and preserves 100% committed state.

- [x] **TASK-047: Implement Online Zero-Downtime Virtual Shard Migration**
  - **Deliverables**: `crates/storage-core/src/sharding.rs` (`F-DIST-03`, `RULE-SCALE-03`).
  - **Optimizations**:
    - Stream all live records belonging to `vshard` from Source Node A to Target Node B, verify 64-bit `XXH3-64` shard digest across source and destination, atomically bump `ClusterMap` epoch, and prune source records.
  - **Exit Criteria**: Migrating `vshard` from Node 1 to Node 3 while preserving zero-copy reads loses zero records and updates cluster routing seamlessly.

---

## Stage 14: Cost-Based Query Optimizer, Lock-Free Observability & Final Verification (`v1.0`)

- [x] **TASK-048: Implement Cardinality-Aware Cost-Based Query Optimizer & `EXPLAIN ANALYZE`**
  - **Deliverables**: `crates/storage-core/src/optimizer.rs`, `crates/storage-core/src/query.rs` (`F-OPT-01`, `RULE-CPLX-01`).
  - **Optimizations**:
    - Inspect live cardinality statistics from `BitmapTagIndex` and `NumericRangeIndex` in `< 200 ns` (`0` heap allocations) to compute selectivity and order predicates from highest selectivity to lowest cost.
    - Expose `query.explain()` returning a structured `QueryExplainPlan` with estimated cost, chosen driver index (`EmptyShortCircuit`, `BitmapTagFirst`, `BTreeRangeFirst`), and ordered predicate estimates.
  - **Exit Criteria**: Optimizer accurately selects the narrowest index driver across skewed datasets and produces deterministic `QueryExplainPlan` output.

- [x] **TASK-049: Implement Lock-Free Logarithmic Latency Histograms & Prometheus Telemetry**
  - **Deliverables**: `crates/storage-core/src/telemetry.rs` (`F-OPT-02`, `RULE-CPU-01`, `RULE-MEM-01`).
  - **Optimizations**:
    - Record nanosecond operation latencies (`read`, `write`, `query`, `wal_sync`, `rpc`) into cache-line-aligned 64-bucket logarithmic atomic histograms (`leading_zeros` bucket index in `1` instruction, `0` heap allocations).
    - Compute live `p50`, `p95`, `p99`, and `p99.9` percentiles, maintain a bounded `SlowOperationLog` ring buffer, and format standard Prometheus/OpenMetrics text output (`to_prometheus_text`).
  - **Exit Criteria**: Recording latency samples incurs `0` heap allocations and exports valid Prometheus histogram + gauge metrics.

- [x] **TASK-050: Execute Full-Scale v1.0 Distributed, MVCC, HNSW & Chaos Benchmark Gate**
  - **Deliverables**: `crates/storage-core/tests/production_v1_gate.rs`, `benches/src/main.rs`, `docs/BENCHMARK_REPORT.md`, `docs/PROGRESS_LOG.md`.
  - **Verification**:
    1. Run end-to-end verification across all 50 tasks: Multi-Collection Catalog, Paged B+Tree/Bitmap Indexes, `SQ8` + HNSW ANN Search, MVCC Snapshot Isolation, Segmented WAL + Online PITR Backup, Kernel-Polled TCP Reactor + Auth/RBAC/Encryption-at-Rest, 3-Node Raft Replication + Shard Migration, and Cost-Based Optimizer + Telemetry.
    2. Run `cargo test --workspace` and `cargo run --release -p storage-benches` verifying `0.00` hot-read allocations, sub-microsecond hot reads, minimal binary/data footprint, and 100% chaos crash recovery.
  - **Exit Criteria**: All tests pass with zero warnings, benchmark report updated with v1.0 metrics, and all changes committed and pushed to `origin/main`.

---

## Stage 15: On-Disk I/O, Slot Recycling, WAL Compression & Incremental Delta Checkpoint Optimizations (`TASK-051` – `TASK-054`)

- [x] **TASK-051: Implement Positional `write_all_at` (`seek_write`/`pwrite`) & Lazy Superblock Fast-Forward Recovery in `ArenaSegment`**
  - **Deliverables**: `crates/storage-mmap/src/arena.rs`.
  - **Optimizations**:
    - Replace the 4-syscall `(seek(offset) + write_all(frame) + seek(12) + write_all(head))` pattern in `ArenaSegment::append_frame` with a single positional `write_all_at(&w.file, frame, offset as u64)` (`FileExt::seek_write` on Windows, `FileExt::write_at` on Unix), cutting per-flush syscalls by `75%` (`4 -> 1`).
    - Eliminate SSD page-0 write amplification by updating the 64-byte superblock `write_head` lazily (every `SUPERBLOCK_SYNC_INTERVAL = 64` appends, on segment capacity doubling, on explicit `sync_data`, or on `Drop`), and fast-forwarding `write_head` across valid checksummed `RecordHeader` slots at `ArenaSegment::open_or_create` startup.
  - **Exit Criteria**: `test_arena_positional_write_fast_forward_recovery_and_slot_reuse` verifies that even if a process crashes before the 64-append superblock flush interval, reopening the segment deterministically fast-forwards `write_head` across all valid appended frames without data loss.

- [x] **TASK-052: Implement In-Segment 64-Byte Size-Class Free-Slot Reuse (`reclaim_dead_slot`)**
  - **Deliverables**: `crates/storage-mmap/src/arena.rs`, `crates/storage-core/src/engine.rs`.
  - **Optimizations**:
    - Maintain `free_slots: FastHashMap<u32, Vec<u32>>` (mapping `64B-aligned size class -> stack of free offsets`) inside `ArenaWriter`.
    - When no MVCC snapshot is active (`!self.mvcc.has_active_snapshots()`), `delete_record_locked` and `flush_single_record_impl` call `arena.reclaim_dead_slot(old_loc)` so subsequent writes of the same 64B size class pop and overwrite the dead slot in-place in $O(1)$ without growing `.seg` files or waiting for full segment compaction.
  - **Exit Criteria**: Repeated updates or delete+reinsert cycles of same-sized records reuse existing 64B slots in-place with zero `.seg` file growth.

- [x] **TASK-053: Implement Transparent LZ4 WAL Frame Compression for Payloads `>= 1 KiB`**
  - **Deliverables**: `crates/storage-wal/src/lib.rs`.
  - **Optimizations**:
    - Introduce `WAL_FLAG_LZ4 = 0x01` in header byte `7` and store `uncompressed_len: u32` in previously reserved header bytes `60..64` of the 64-byte WAL frame header.
    - Automatically compress WAL payloads `>= 1024` bytes (`WAL_LZ4_THRESHOLD`) via `storage_codec::compress_lz4` when compressible, and transparently decompress on recovery via `storage_codec::decompress_lz4` (`recover_committed`), cutting sequential WAL disk bandwidth by `2x–4x` on medium/large records while keeping small frames (`< 1 KiB`) zero-overhead.
  - **Exit Criteria**: `test_wal_group_commit_lz4_compression_and_torn_frame_recovery` verifies an 8 KiB record compresses to `< 2 KiB` on disk and recovers bit-for-bit identically.

- [x] **TASK-054: Implement Incremental $O(\Delta)$ Primary Index Delta Checkpointing (`STGEDLT2`)**
  - **Deliverables**: `crates/storage-index/src/checkpoint.rs`, `crates/storage-index/src/lib.rs`, `crates/storage-core/src/engine.rs`.
  - **Optimizations**:
    - Implement `append_primary_checkpoint_delta` and `delta_checkpoint_path` (`<collection>_primary.idx.delta`) writing compact checksummed `STGEDLT2` delta frames containing only modified upserted entries and deleted `InlineId` tombstones in $O(\Delta)$ time and bytes.
    - Replay all valid `.delta` frames automatically during `load_primary_checkpoint`, clean up `.delta` when a full `save_primary_checkpoint` executes, and expose `StorageEngine::checkpoint_incremental` with automatic full compaction once `.delta` reaches `1 MiB`.
  - **Exit Criteria**: `test_v2_checkpoint_and_incremental_delta_roundtrip` and `test_minimal_footprint_invariants_and_arena_compaction` verify $O(\Delta)$ incremental checkpointing and recovery.

---

## Stage 16: Engine-Level Set Filtering (`AND NOT`, `IN`/`OR`), Bidirectional Sorting & $O(\log N)$ Keyset/Offset Pagination (`TASK-055` – `TASK-056`)

- [x] **TASK-055: Implement Hardware-Accelerated Bitmap `AND NOT` / `OR` Windowing & Bidirectional Keyset-Seekable B+Tree Scans**
  - **Deliverables**: `crates/storage-index/src/bitmap.rs`, `crates/storage-index/src/btree.rs`, `crates/storage-index/src/primary.rs`, `crates/storage-index/src/lib.rs`.
  - **Optimizations**:
    - Implement `CompressedBitmap::and_not`, `and_not_in_place` (`0` heap allocations), `BitmapTagIndex::union_tags` (`WHERE tag IN (...)`), `exclude_from_bitmap`, `filter_not_in_place`, and `matches_ordinal`.
    - Implement `CompressedBitmap::collect_window` using hardware `POPCNT` (`count_ones`) to skip whole 64-bit words during `offset` skipping in 1 CPU cycle (`64x` faster than bit-by-bit iteration), supporting both ascending (`BLSR`) and descending (`leading_zeros`) traversal plus `after_ordinal` cursor seeking.
    - Implement `SortOrder::{Asc, Desc}`, `KeysetCursor`, `NumericRangeIndex::range_collect_paginated` ($O(\log N)$ B+Tree cursor seek via `BTreeMap::range` + `.rev()`), and `PrimaryIndex::scan_ordinals_paginated` ($O(1)$ array-indexed ordinal cursor seek).
  - **Exit Criteria**: Unit tests in `bitmap.rs` and `btree.rs` verify `AND NOT`, `POPCNT` window skipping, descending B+Tree range pagination, and $O(\log N)$ `KeysetCursor` continuation.

- [x] **TASK-056: Implement Zero-Allocation `QueryBuilder` Set Filters, Zero-Copy Payload Predicates, `QueryPage` & Paginated TCP RPC**
  - **Deliverables**: `crates/storage-core/src/query.rs`, `crates/storage-core/src/lib.rs`, `crates/storage-server/src/lib.rs`.
  - **Optimizations**:
    - Extend `QueryBuilder` with stack-inlined `.filter_not_tag(field_id, tag)`, `.filter_any_tag(field_id, &[tags])`, `.filter_range_f64(field_id, min, max)`, `.filter_range_u64(field_id, min, max)`, `.order_asc()`, `.order_desc()`, `.offset(skip)`, and `.after_cursor(cursor)`.
    - Add `QueryPage<Item>` (`items`, `total_matched`, `has_more`, `next_cursor`), `execute_page_ids()`, `execute_page_refs()`, `execute_page_filtered_with(|archived: &T::Archived| -> bool)` (evaluating caller predicates directly on zero-copy `mmap` pointers before pagination), and `for_each_ref`.
    - Extend `OP_QUERY_HYBRID` and `StorageClient::query_range_paginated` in `storage-server` to support `offset`, `limit`, `descending`, and `total_matched` over TCP.
  - **Exit Criteria**: `test_query_filtering_and_pagination_end_to_end` and `test_tcp_server_wire_protocol_v2_cas_batch_query_auth_and_metrics` pass with zero warnings (`44` total workspace tests passing).

---

## Stage 17: Full Filtering Coverage — Relational Operators, Prefix/Key-Set Matchers, Nullability, Hybrid Graph BFS & Composable Zero-Copy `RecordView` Predicates (`TASK-057` – `TASK-058`)

- [x] **TASK-057: Implement Index-Backed Relational Operators (`=`, `!=`, `>`, `>=`, `<`, `<=`, `NOT BETWEEN`, `IN`), Prefix Scans (`STARTS WITH`), Key-Set Filters & Field Existence (`IS NULL` / `IS NOT NULL`)**
  - **Deliverables**: `crates/storage-types/src/id.rs`, `crates/storage-codec/src/archive.rs`, `crates/storage-index/src/bitmap.rs`, `crates/storage-index/src/btree.rs`, `crates/storage-index/src/primary.rs`.
  - **Optimizations**:
    - Add zero-allocation string matchers on `InlineId` (`starts_with_str`, `ends_with_str`, `contains_str`), `ArchivedString` (`eq_str`, `starts_with`, `ends_with`, `contains`, `eq_ignore_ascii_case`, `contains_ignore_ascii_case`), and `RecordView` (`str_eq`, `str_starts_with`, `str_ends_with`, `str_contains`, `str_contains_ignore_ascii_case`).
    - Implement `BitmapTagIndex::union_tag_prefix` ($O(M)$ prefix scan over tag entries) and `BitmapTagIndex::all_on_field_bitmap` (`IS NOT NULL` for tag fields).
    - Implement `NumericRangeIndex::eq_bitmap` ($O(\log U)$ exact B+Tree lookup without range traversal), `NumericRangeIndex::in_values_bitmap` ($O(K \log U)$ multi-point lookup), and `NumericRangeIndex::all_on_field_bitmap` (`IS NOT NULL` for numeric fields).
    - Implement `PrimaryIndex::ids_to_bitmap` ($O(K)$ lock-free shard point lookups) and `PrimaryIndex::id_prefix_bitmap` (`STARTS WITH` on primary keys).
  - **Exit Criteria**: All index primitives execute in $O(\log N)$ or bitwise-64-bit word time with zero string allocations.

- [x] **TASK-058: Integrate All 5 Filtering Categories & Composable `.filter(|view: &RecordView<T>|)` into `QueryBuilder` Across All Terminal Methods**
  - **Deliverables**: `crates/storage-core/src/query.rs`.
  - **Optimizations**:
    - Add relational comparison methods (`filter_eq`, `filter_eq_u64`, `filter_eq_f64`, `filter_ne`, `filter_ne_u64`, `filter_ne_f64`, `filter_gt`, `filter_gte`, `filter_lt`, `filter_lte`, `filter_gt_f64`, `filter_gte_f64`, `filter_lt_f64`, `filter_lte_f64`, `filter_not_range`, `filter_in_i64`).
    - Add prefix and key-set methods (`filter_tag_prefix`, `filter_id_prefix`, `filter_ids`, `filter_not_ids`) and field existence methods (`filter_has_tag_field`, `filter_missing_tag_field`, `filter_has_numeric_field`, `filter_missing_numeric_field`).
    - Add hybrid graph neighborhood filtering (`filter_within_bfs(root_id, max_depth, extract_neighbors)`) intersecting zero-copy BFS reachability bitmaps directly with secondary index bitmaps.
    - Add composable zero-copy `.filter(|view: &RecordView<'_, T>| -> bool)` honored across all terminal methods (`resolve_ordinals`, `execute_ids`, `execute_refs`, `execute_page_ids`, `execute_page_refs`, `for_each_ref`, and `top_k_similar`) while preserving the 1-allocation fast path when unused (`is_simple_unpaginated`).
  - **Exit Criteria**: `test_query_filtering_and_pagination_end_to_end` verifies all 11 filtering scenarios end-to-end (`44` workspace tests passing, `0` warnings).


## Stage 18: Production DevOps — Directory Lock, Persistent Raft/ClusterMap, TCP Raft RPCs, Admin RPCs & CLI PITR (`TASK-059` – `TASK-063`)

- [x] **TASK-059: Implement Exclusive `DirectoryLock` (`db.lock`) Preventing Concurrent Multi-Process Corruption**
  - Build a cross-platform `DirectoryLock` struct in `crates/storage-mmap/src/persist.rs` using `LockFileEx` (Windows) / `flock` (Unix) with `try_acquire()` and auto-release on `Drop`. Integrate into `StorageEngine::open_with_shared_wal()` to acquire `<collection>.lock`. Skip `.lock` files in backup/snapshot copies.
  - **Exit Criteria**: `test_directory_lock_prevents_concurrent_open_and_releases_on_drop` in `storage-mmap` verifies mutual exclusion and auto-release.

- [x] **TASK-060: Persist Crash-Safe Raft `HardState` (`raft.meta`) & `ClusterMap` (`cluster.map`) with XXH3-64 Checksums**
  - Add `RaftState::save_to_disk()` / `load_from_disk()` with `RAFT_META_MAGIC` persisting `current_term`, `voted_for`, `commit_index`, `last_applied`, and full serialized log with XXH3-64 integrity check. Add `ClusterMap::save_to_file()` / `load_from_file()` with `CLUSTER_MAP_MAGIC`. Wire all state-mutating Raft methods to persist atomically.
  - **Exit Criteria**: Raft and sharding unit tests pass with disk persistence; existing `test_raft_3_node_quorum_replication_failover_and_follower_reads` and `test_sharded_cluster_router_and_online_vshard_migration` continue passing.

- [x] **TASK-061: Implement TCP Admin RPCs (`OP_ADMIN_BACKUP=0x11`, `OP_ADMIN_COMPACT=0x12`, `OP_HEALTH=0x13`, `OP_ADMIN_CHECKPOINT=0x14`) with RBAC Enforcement**
  - Add 4 new opcodes to `StorageServer` dispatch (`crates/storage-server/src/lib.rs`): `OP_ADMIN_BACKUP` (Admin-gated, calls `create_online_backup()`), `OP_ADMIN_COMPACT` (Admin-gated, calls `compact_arenas(0.5)`), `OP_HEALTH` (ReadOnly+, returns JSON `{"status":"ok",...}`), `OP_ADMIN_CHECKPOINT` (Admin-gated, calls `checkpoint_incremental(&[],&[])`).
  - Add corresponding `StorageClient` methods: `admin_backup()`, `admin_compact()`, `health()`, `admin_checkpoint()`.
  - **Exit Criteria**: `test_tcp_raft_admin_health_and_shard_migration_rpcs` validates RBAC enforcement (non-Admin rejected), health check JSON, and successful admin operations.

- [x] **TASK-062: Implement CLI PITR & Remote Admin Commands in `storage-cli`**
  - Add local commands: `backup <db_dir> <backup_dir>`, `archive-wal <db_dir> <backup_dir>`, `restore-pitr <backup_dir> <restore_dir> <target_ts_ns>`.
  - Add remote admin commands: `remote-health <addr>`, `remote-compact <addr>`, `remote-backup <addr> <dest_path>`, `remote-checkpoint <addr>`, `remote-metrics <addr>`.
  - Add `storage-server` dependency to `crates/storage-cli/Cargo.toml`.
  - **Exit Criteria**: CLI builds cleanly, all workspace tests pass.

- [x] **TASK-063: Implement TCP Raft Consensus & Shard Migration RPCs (`OP_RAFT_VOTE=0x0E`, `OP_RAFT_APPEND=0x0F`, `OP_MIGRATE_VSHARD=0x10`)**
  - Add 3 new opcodes: `OP_RAFT_VOTE` (ReadWrite+, deserializes/echoes `RequestVoteReq`), `OP_RAFT_APPEND` (ReadWrite+, deserializes `AppendEntriesReq`, returns `AppendEntriesResp`), `OP_MIGRATE_VSHARD` (Admin-gated, imports shipped record frames into local engine).
  - Add `StorageClient` methods: `raft_vote()`, `raft_append()`, `migrate_vshard()`.
  - **Exit Criteria**: `test_tcp_raft_admin_health_and_shard_migration_rpcs` validates Raft wire round-trip, shard import lands records in engine, RBAC enforcement (`46` total workspace tests, `0` failures).

## Stage 18 (continued): Full-System Verification & Lint Hygiene (`TASK-064` – `TASK-066`)

*(Back-filled from commit history: `dd2e165`, `8752747`, `4a004ec`.)*

- [x] **TASK-064: Comprehensive Full-System Integration Test Covering All 24 Feature Categories**
  - `crates/storage-server/tests/full_system_e2e.rs`.
- [x] **TASK-065: Master Prompt Audit Fixes (SAFETY Comments, Path Traversal Guard, `mem::take` Optimization)**
- [x] **TASK-066: Clean Clippy Workspace (Zero Warnings Across All 9 Crates)**


## Stage 19: Full-Project Audit Remediation — Soundness, Durability, Security & Documentation Accuracy (`TASK-067` – `TASK-079`)

- [x] **TASK-067: Eliminate Undefined-Behavior-Capable `unsafe` & Restore `forbid(unsafe_code)` Everywhere Required (`RULE-SAFE-01`, `RULE-SAFE-04`)**
  - **Deliverables**: `storage-codec` (`archive.rs`, `types.rs`, `relations.rs`), `storage-types` (`id.rs`, `header.rs`, `wire.rs`), `storage-graph`, `storage-core/src/model.rs`.
  - Add the `unsafe trait ArchivePod` (all bit patterns valid, no padding) and the `impl_archive_pod!` macro, which proves layout at compile time so record crates stay `forbid(unsafe_code)`.
  - Add safe O(1) `RecordCodec::decode_fast` / `decode_fast_or` / `header_ref`. Make `ArchivedString::as_str` and `ArchivedSlice::as_slice` bounds-, alignment- and UTF-8-checked.
  - Move the archived relationship types into `storage-codec::relations` (sorted-order validation) so `storage-graph` is `forbid(unsafe_code)` again. Replace every `try_into().unwrap()` byte read with the `storage_types::wire` readers.
  - **Exit Criteria**: `test_decode_fast_is_safe_on_corrupted_frames`; no `unsafe` outside `storage-codec` / `storage-mmap`.

- [x] **TASK-068: Rebuild `GroupCommitWal` Ordering, Durability & Recovery (`RULE-CONC-03`)**
  - Assign LSNs under the lock, use per-submission sequence tickets, and fail stop after I/O errors. Keep LSNs and txn ids monotonic via checkpoint markers. `checkpoint_truncate_before` writes `.next` and renames it atomically (archiving removed frames when `wal_archive` is set).
  - `recover_committed` never mutates the file. `AsyncMemoryFirst` buffers and is flushed by the background flusher (`flush_buffered`).
  - **Exit Criteria**: torn-tail, txn-restart, checkpoint-archive, concurrent-durability and async-buffering tests.

- [x] **TASK-069: Engine Crash Consistency — Flush Barrier, Replay-Then-Truncate, Atomic Deletes & MVCC Writer Gate (`RULE-SAFE-03`, `RULE-CONC-01`)**
  - `StripedLockTable::barrier` / `lock_all`. `flush_all` = cutoff, barrier, drain, `checkpoint_and_truncate`. Open replays the WAL (validating or migrating frames) and then truncates.
  - `delete_with_policy` logs one atomic WAL transaction before removing anything. Add the MVCC `writer_guard` gate and `forget` / `has_chain`; the TTL ordinal bitmap; and `data_sync` mirroring the WAL policy.
  - **Exit Criteria**: `test_process_kill_crash_recovery_preserves_every_acknowledged_write` (child process aborted mid-stream; 3 layouts x 2 rounds).

- [x] **TASK-070: Arena & Mmap Lifetime Safety (`RULE-SAFE-02`)**
  - Reuse a freed slot only when the region `Arc::strong_count == 1` and no retired regions are alive. Add per-collection `segments/<stem>` directories (with legacy migration), deferred `pending_removals`, `sync_all`, and parent-directory `fsync` after renames on Unix.
  - **Exit Criteria**: `test_slot_reuse_is_blocked_while_a_reader_holds_the_mapping`.

- [x] **TASK-071: Index & Loader Hardening Against Corrupted / Crafted Files**
  - `PagedBTreeIndex` validates `entry_count` and page CRCs. `PagedBitmapIndex` keys `hot_removed` by `(field, tag)`. The HNSW loader is bounds-checked, with safe visited marking. `PrimaryIndex::complete_background_flush` applies only on a version match. B+Tree posting lists keep the inline-smallest invariant.
  - **Exit Criteria**: `test_crafted_hnsw_file_is_rejected_not_panicking`, `test_duplicate_key_postings_stay_sorted_for_keyset_pagination`.

- [x] **TASK-072: Query Correctness — Unconstrained Queries, Exclusions, Windows & `top_k`**
  - `exclusion()`, `check_extra`, `check_view` and `window_with_view`. Unconstrained queries return every live record. `top_k_similar` ignores pagination windows.

- [x] **TASK-073: Consistent Backups & Point-in-Time Recovery (`RULE-SAFE-06`)**
  - Snapshots record their WAL LSN under `lock_all`. `archive_wal_for_pitr` requires `EngineConfig::wal_archive`. Restore replays the LSN-ordered prefix into a fresh destination; existing data is never deleted.
  - **Exit Criteria**: `test_pitr_requires_wal_archive`.

- [x] **TASK-074: Raft Safety & Sharding Correctness**
  - `RaftStorage` (meta plus append-only log). `RaftNode::new -> Result`. Vote / term / log-matching safety rules, `apply_committed`, and per-follower `next_index`.
  - High-bit `vshard_for_id`. `migrate_vshard` freezes the shard behind the MVCC write gate and rolls back on failure.
  - **Exit Criteria**: `test_raft_vote_safety_rules`; 20,000-key vshard distribution test.

- [x] **TASK-075: Server Security & Admission Control (`RULE-SAFE-06`, `F-SEC-01`, `F-SEC-03`, `F-SEC-04`)**
  - Role, body-limit and rate-limit checks happen at the header stage, before any allocation. Unauthenticated servers bind loopback only. `backup_root` confinement. An admin job pool (max 2 concurrent jobs, results delivered over mpsc). Raft RPCs are served by an optional `RaftNode` (Admin only). Client-side bounds checks.
  - `storage-server` binary: `STORAGE_AUTH_TOKENS`, `STORAGE_BACKUP_ROOT`, `STORAGE_RATE_LIMIT`, `STORAGE_MAX_CONNECTIONS`. `storage-cli`: `STORAGE_TOKEN`.
  - **Exit Criteria**: `test_header_stage_admission_and_hostile_inputs`, `test_backup_path_resolution_rejects_escapes`, `test_token_auth_and_role_parse`.

- [x] **TASK-076: Real RFC 8439 ChaCha20-Poly1305 AEAD Primitives (`F-SEC-02`, primitives only)**
  - Real Poly1305 tag (replacing the previous ad-hoc 128-bit MAC), `aead_seal` / `aead_open` with AAD, and `generate_unique_nonce`.
  - **Exit Criteria**: `test_chacha20_block_rfc8439_vector`, `test_poly1305_rfc8439_vector`, `test_aead_rfc8439_vector`, `test_seal_open_and_tamper_detection`.

- [x] **TASK-077: Build-Independent Checksum Format (`RULE-SAFE-05`)**
  - The SSE4.2 and portable `CRC32C` paths produced different checksums, so databases written by `target-cpu=native` builds were unreadable by portable builds and on other architectures. The portable path is now a bit-identical table-driven implementation, and hardware is detected at runtime when not enabled at compile time.
  - **Exit Criteria**: `test_checksum_format_is_identical_on_every_code_path` (golden values), `test_crc32c_table_matches_standard_check_value`. Zero warnings with and without `target-cpu=native`.

- [x] **TASK-078: Benchmark Gate & Hot-Path Regression Fixes (`RULE-TEST-02`, `RULE-MEM-01`)**
  - Interleaved A/B gate against `4a004ec`. Fixes: overlapping-word ASCII fast path in `ArchivedString::as_str`; shared static zero page instead of a 176-byte per-`RecordRef` fallback; `AsyncMemoryFirst` buffering.
  - **Exit Criteria**: `0.00` allocs/op on every hot read; `with_ref` / `get_ref` / `update` at parity within noise (`docs/BENCHMARK_REPORT.md` §5).

- [x] **TASK-079: Documentation Accuracy & MSRV**
  - Correct README (compile-checked examples, CLI argument order, auth configuration, `SyncPolicy` names, benchmark caveats, binary size), FEATURES (verified status section, definitions for feature IDs referenced in code), RULES (`ArchivePod` model, `RULE-SAFE-02` – `06`, `RULE-CONC-02` / `03`), CONTRIBUTING and BENCHMARK_REPORT.
  - Declare `rust-version = "1.82"` workspace-wide.

## Stage 20: Panic-Freedom, Master-Prompt Audit & Feature Wiring (`TASK-080` – )

- [x] **TASK-080: Panic-Free Production Code & Hostile-Input Sweeps for Every Decoder (`RULE-ERR-01`, `RULE-TEST-01`)**
  - *(Continues the last request of the previous Gemini/Antigravity session: "refactor the ~110 `try_into().unwrap()` sites to truly infallible patterns".)* Those sites were already replaced by `storage_types::wire` readers in Stage 19. The remaining panic paths are now removed: `AlignedBytes` / `AlignedScratchBuf` capacity math (checked; `Layout` can no longer be invalid), `CachePadded::boxed_array` for the four shard tables (no `unreachable!()`), and production `InlineId::from_static` calls moved into `const` items (an invalid literal is a compile error). Only compile-time `const` assertions and one `debug_assert!` remain.
  - Add `AlignedBytes::try_copy_from_slice` (returns `ResourceExhausted`) and use it for every network-, disk- and decompression-sourced buffer.
  - **Bugs found and fixed by the sweeps**:
    - `RecordCodec::decode_owned` panicked, and reached `decode_unchecked` on an unvalidated root (UB), for crafted tombstone frames. It now rejects tombstones and decompresses once.
    - `decompress_lz4` preallocated the claimed size (up to 256 MiB) for a 3-byte input. Impossible expansion ratios are now rejected and the allocation is fallible.
    - `PagedBitmapIndex` allocated per an untrusted chunk count and left bitmap blocks unchecksummed. The new v2 format adds a blocks checksum, bounded counts, block-index and chunk-order validation.
    - `PagedBTreeIndex` leaf checksums did not cover the page header (separator keys). `page_crc` now covers the whole page.
  - **Exit Criteria**: hostile-input sweeps (truncation, raw corruption, and re-sealed corruption with valid checksums) for record frames, LZ4, `InlineId` / `RecordHeader`, WAL files and forged WAL frames, primary checkpoints and deltas, HNSW, paged B+Tree, paged bitmap, arena segments, Raft wire messages, `ClusterMap`, backup manifests, whole-database corruption (open + full read-back), and every server opcode (all reactor workers still answer). `81` tests passing, `0` warnings (native and portable).

- [x] **TASK-081: Master-Prompt Audit — Scaling, CPU, Complexity, Memory & Observability (`RULE-SCALE-02`, `RULE-CPU-01`, `RULE-CPU-03`, `RULE-CPLX-01`, `RULE-MEM-04`)**
  - Whole-codebase review against the master prompt's pillars. Fixes:
    - `StripedCounter`: per-thread, cache-line-padded read and write telemetry counters (previously one atomic shared by every core).
    - Cache-padded ordinal-table lock (false sharing).
    - Top-K over a small vector index enumerates `HnswVectorIndex::live_ordinals` (previously cloned every primary entry).
    - `sweep_expired` walks the TTL bitmap and re-checks expiry under the stripe lock.
    - Non-blocking rejection of over-limit connections.
    - Per-shard self-wake socket: reactor workers block in the kernel with an infinite timeout (previously re-polled every connection every 25 ms), plus a lost-wakeup-free `stop()`.
    - Raft §7 log compaction (`compact_log_through`, automatic above 8,192 applied entries with a 1,024-entry tail; `snapshot_index` / `snapshot_term` persisted crash-safely; WAL `sync_to_disk` first).
    - Per-shard read and write RPC latency histograms plus a slow-operation log, exported via `OP_METRICS`.
  - **Exit Criteria**: `test_raft_log_compaction_bounds_memory_and_survives_restart`, `test_rpc_latency_histograms_and_slow_log_are_recorded_and_exported`; `83` tests passing, `0` warnings (native and portable).

- [x] **TASK-082: Wire HNSW Graph Persistence via Stable Ordinals (`F-HNSW-02`, `F-INDX-02`)**
  - **Primary checkpoint v3** (`STGEIDX3` / `STGEDLT3`) persists every record's ordinal; `PrimaryIndex::restore_entry_at` restores it exactly and `finish_restore` rebuilds the free list from the holes. v2 / v1 files still load (dense ordinals). Delta replay now applies a frame's deletes before its upserts: a key deleted and re-created in one window was previously deleted on replay.
  - **Engine**: loads `indexes/<stem>_hnsw.idx` at open. `rebuild_secondary_indexes` reconciles the graph with the records using one zero-copy `O(D)` `vector_matches` per vector: changed or missing vectors are re-inserted and nodes of records without vectors removed. A missing, stale or corrupted file therefore only costs rebuild work. Checkpoints save the graph atomically when its generation changed.
  - **HNSW connectivity fix** (bug found while testing): an outlier vector could receive zero inbound edges and become unreachable even by an exact-match query. Every node now keeps at least one base-layer inbound edge (tracked inbound degrees, eviction never removes a node's last inbound link, forced link from the nearest neighbor).
  - **Reactor poll policy** (benchmark-gate finding on `TASK-081`): an untimed wait resumed measurably slower on Windows under request/response traffic. Workers use 1 ms / 5 ms timed waits while recently active and block indefinitely only after about 128 ms idle.
  - **Exit Criteria**: `test_hnsw_graph_persists_reconciles_and_recovers` (graph reused with zero re-insertions and identical results; stale-graph reconciliation; corrupted-file rebuild; recall@10 `>= 95%` on every path), `test_outlier_vectors_remain_reachable`, `test_v3_checkpoint_preserves_ordinals_with_holes_and_deltas`; `86` tests passing, `0` warnings.

- [x] **TASK-083: Wire the Paged Secondary-Index Tier (`F-PIDX-01`, `F-PIDX-02`, `RULE-CONC-01`, `RULE-MEM-04`)**
  - **`PagedBTreeIndex` rewrite.**
    - Immutable generation files behind an atomic manifest (`paged_files`).
    - Three disjoint sources: base pages, a frozen delta and a hot delta.
    - Streaming `O(page)`-memory merge checkpoints that hold no lock during I/O (`begin_checkpoint` / `finish_checkpoint`).
    - Full query parity with `NumericRangeIndex`, including keyset pagination.
    - Pages validated once at open (sorted, contiguous, whole-page CRC).
    - The previous version materialized every posting in RAM and held the index write lock across the whole disk write.
  - **`PagedBitmapIndex` rewrite.**
    - Same architecture: chunk-streaming merge, `BMX3` generation file with header, directory and blocks checksums and per-tag cardinality.
    - Point-lookup `contains` / `matches_ordinal`.
    - Full query parity with `BitmapTagIndex`.
  - **Bloom filter / `ColdPrimaryIndex`** moved unchanged to `cold_primary`.
  - **Engine.**
    - `EngineConfig::index_tier` (`IndexTier::{Memory, Paged}`) with tier-agnostic `RangeIndex` / `TagIndex` dispatch.
    - Checkpoint v3 header `stamp`. `save_checkpoint_files` stamps the primary checkpoint and both paged indexes alike, without pausing writers; the WAL is synced before index generations are published.
    - Paged indexes are reused at open only after a clean shutdown (matching stamps, no delta frames, no WAL replay, not in shared-catalog-WAL mode). Otherwise they are rebuilt from the records, spilling every 1 M records.
  - **Exit Criteria**:
    - `test_paged_btree_matches_in_memory_index_through_checkpoints`, `test_paged_bitmap_matches_in_memory_index_through_checkpoints`, `test_paged_btree_checkpoint_never_blocks_writers_and_keeps_concurrent_writes`.
    - `test_paged_tier_matches_memory_tier_through_checkpoints_and_restarts` (every query family identical, including paginated scans).
    - `test_paged_indexes_are_rebuilt_when_not_provably_current`.
    - The process-kill crash test, extended to the paged tier, verifies that recovered secondary indexes are exact.
    - `91` tests passing, `0` warnings.

- [x] **TASK-084: Wire Encryption at Rest (`F-SEC-02`, `RULE-SAFE-07`, `RULE-MEM-01`)**
  - **Configuration**: `EngineConfig::encryption_key: Option<EncryptionKey>` (redacted `Debug`, zeroed on drop) and `decrypted_cache_bytes` (default 256 MiB).
  - **Frame encryption**: `RecordCodec::encrypt_frame` / `decrypt_frame` seal the payload with ChaCha20-Poly1305 (unique 96-bit nonce per frame). The authenticated data binds the ciphertext to the record id and every header field that decryption does not recompute, so frames cannot be swapped between records and metadata (e.g. TTL) cannot be edited undetected. `validate_frame` / `decode_fast` / `decode_owned` reject encrypted frames (one bitmask test).
  - **Engine**:
    - Every on-disk frame (arena segments, dedicated and ping-pong files, compaction rewrites) and every WAL upsert body (single, pair, cascade, transaction, cross-collection catalog transaction) is sealed. Replay decrypts.
    - Reads go through a bounded, sharded decrypted-frame cache, filled at flush time and on cold reads. MVCC history keeps plaintext.
    - With a key configured, plaintext frames are rejected (no downgrade attack); encryption must therefore be enabled at database creation.
  - **Exit Criteria**:
    - `test_frame_encryption_roundtrip_and_tamper_detection` (wrong key, wrong record id, flipped ciphertext, forged TTL all rejected).
    - `test_encrypted_engine_keeps_plaintext_off_disk_and_reads_back` (no plaintext in any file, before and after flush, for both layouts; correct reads and queries with the right key; errors with a wrong or no key).
    - `test_tampered_ciphertext_is_detected`.
    - Process-kill crash test variant with encryption (encrypted WAL replay).
    - Codec hostile sweep over encrypted frames.
    - Benchmark gate: encrypted warm `with_ref` at `0.00` allocs/op.
    - `95` tests passing, `0` warnings.

- [x] **TASK-085: Encryption Key Ring, Online Enable / Disable and Key Rotation (`F-SEC-02`, `RULE-SAFE-07`)**
  - **Configuration**:
    - `EncryptionConfig { keys: KeyRing, active_key, accept_unencrypted }` replaces the single key and is validated at open and on `set_encryption`.
    - Sealed frames carry the id of the key that sealed them (in the payload, authenticated): old keys keep decrypting while the active key seals new writes.
  - **Online conversion**: `StorageEngine::set_encryption` (runtime swap) + `reseal_all`.
    - Every record is converted under its own stripe lock and WAL-logged at the same version (content unchanged, so indexes, MVCC and OCC versions are unaffected).
    - A final checkpoint drops WAL frames in the old form.
    - Superseded copies are scrubbed: every arena segment is evacuated, and ping-pong files are written twice so both slots hold the new form.
    - `accept_unencrypted` is a migration-only state.
  - **Bug found and fixed (`write_ping_pong_slot`)**: on a version tie the writer overwrote slot 0 while readers prefer slot 1, so a same-version rewrite (WAL replay, re-seal) was invisible. On ties the writer now overwrites the reader-preferred slot unless it already holds exactly that frame. Every overwrite still leaves a same-or-newer valid slot.
  - **Exit Criteria**:
    - `test_enable_rotate_and_disable_encryption_online`, for arena, dedicated-file and ping-pong layouts: plaintext → encrypted with no plaintext left in any file → rotation under a concurrent writer → only the new key needed → back to plaintext; invalid configurations refused.
    - `test_ping_pong_same_version_rewrite_is_visible_and_reaches_both_slots`.
    - `97` tests passing, `0` warnings; benchmark allocation gates unchanged.

- [x] **TASK-086: Per-Record Protection Policy — Encrypt vs Authenticate-Only (`F-SEC-02`, `RULE-SAFE-07`)**
  - **Policy hook**: `Indexable::protection(root, payload) -> RecordProtection { Encrypt (default), Authenticate }`, evaluated zero-copy on each plain frame as it is sealed for disk or the WAL. Frames the policy cannot inspect (compressed, tombstones) are encrypted.
  - **Authenticated-only frame** (`RecordFlags::AUTHENTICATED`, bit 6): the payload stays plaintext and in place, followed by a `32 B` trailer `[key_id u32 | 12 B nonce | 16 B Poly1305 tag]`. The tag covers the record id, the key id, every header field and the payload (same AAD as encrypted frames), so tampering, frame swaps and metadata edits are detected. No cipher work: cheaper writes and cold reads for non-secret records that must not be forged.
  - **No downgrade**: with a key configured, a frame that is neither encrypted nor authenticated is still rejected (unless `accept_unencrypted`). `reseal_all` converts records whose stored form differs from the current policy (policy changes apply online).
  - **Documented limits**: authenticated frames are read through the decrypted-frame cache after verification (not zero-copy on the first read), so a runtime key change can never leave a stale "verified" mark. Without any key configured, an authenticated frame is readable as plaintext without verification (the same trust level as an unencrypted database).
  - **Exit Criteria**:
    - `test_policy_encrypts_secret_records_and_authenticates_public_ones`: secret plaintext never on disk; public plaintext on disk; correct reads after reopen; `reseal_all` is a no-op under an unchanged policy; one flipped byte in a public record fails exactly that record.
    - `test_authenticated_frames_stay_readable_and_detect_tampering` (codec) and the hostile sweep over authenticated frames.
    - `99` tests passing, `0` warnings (native and portable); benchmark allocation gates unchanged.

- [x] **TASK-087: Faster Cold Decryption — SIMD ChaCha20, 64-bit Poly1305, In-Place Frames (`F-SEC-02`, `RULE-MEM-01`, `RULE-CPU-02`)**
  - **ChaCha20**: x86-64 multi-block keystream: 8 blocks per step with AVX2 (runtime-detected), 4 with SSE2 (x86-64 baseline), one-block scalar tail and fallback on other targets. Word-wide XOR on the scalar path.
  - **Poly1305**: radix 2^44 (three 64-bit limbs, 9 widening multiplies per block instead of 25).
  - **Frames sealed / opened in place**: `seal_in_place` / `open_in_place` primitives and `AlignedBytes::try_build` write the final 64-byte-aligned frame directly. The intermediate plaintext / ciphertext `Vec`s are gone. A failed open never releases plaintext.
  - **Rejected**: a portable `[u32; 4]`-lane version (the compiler did not vectorize it; slower than the one-block loop).
  - **Exit Criteria**:
    - RFC 8439 vectors.
    - `test_chacha20_xor_matches_block_reference_at_all_lengths` (lengths 0–600, 4096, 4097, 65535; counters near wrap; covers the AVX2, SSE2 and scalar paths).
    - `test_poly1305_radix44_matches_radix26_reference`: differential against the previous radix-2^26 implementation, including all-`0xFF` and all-zero keys and messages.
    - `test_in_place_open_rejects_tampering_without_touching_the_buffer`.
    - Benchmark: encrypted cold read `6.1 µs` / `6` allocs → `4.2 µs` / `4` allocs.
    - `0` warnings (native and portable, MSRV 1.82 respected).

- [x] **TASK-088: Index Files Encrypted at Rest (`F-SEC-02`, `RULE-SAFE-07`)**
  - **Whole-file seal** (`storage_codec::sealed_file`): `primary.idx` and `hnsw.idx` are one ChaCha20-Poly1305 unit bound to the file name (no moving files between collections). They are built and encrypted in one buffer.
  - **Delta frames**: each delta frame is wrapped and sealed, bound to its byte offset, so frames cannot be reordered or duplicated. A torn final frame is dropped as before; earlier corruption is an error.
  - **Paged secondary indexes**: the whole region after the header page is sealed while streamed to disk (`RegionSealer`; tag in the header page). The tag is verified once at open. Pages, blocks and words are decrypted on access by random-access keystream (`chacha20_xor_at`) into stack buffers, with no MAC work on reads. A tampered generation is never trusted: the engine rebuilds it.
  - **Follows the encryption settings**: written with the active key; plaintext refused unless `accept_unencrypted` (no downgrade); `set_encryption` updates the paged indexes; `reseal_all` rewrites every index file (forces an HNSW save).
  - **Behavior change**: a wrong or missing key now fails at `open` instead of at the first read.
  - **Still visible on disk (documented)**: file names and sizes, WAL frame headers (record ids, versions), record file names in the dedicated-file layouts. Separator keys and other in-RAM index data are plaintext in memory.
  - **Exit Criteria**:
    - `test_sealed_file_roundtrip_binding_and_no_downgrade` (every byte flip, every truncation, wrong context or position).
    - `test_sealed_checkpoint_and_delta_frames_bind_file_and_position`.
    - Paged B-tree / bitmap sealed tests (no plaintext key or tag bytes; verify; refuse without key; rotate to plaintext; tamper detection).
    - Engine: `test_paged_secondary_indexes_are_encrypted_and_tamper_checked`; the encryption lifecycle asserts every index file is sealed after enable and rotation and plaintext after disable.
    - `108` tests passing, `0` warnings.

- [x] **TASK-089: Raft InstallSnapshot (`F-DIST-02`, Raft §7)**
  - **Leader**:
    - `build_install_snapshot(offset, max_chunk)` streams a snapshot file `<stem>_raft.snapshot`: `[magic | last_index | last_term | count | check]` + per record `[id | len | check | frame]`.
    - The snapshot is taken at `last_applied` (cut under `apply_lock` with an MVCC read snapshot), then written without blocking applies. Memory is `O(ids)`.
    - It is regenerated only when it no longer covers the compaction point.
  - **Follower**:
    - `handle_install_snapshot` stages chunks in `<stem>_raft.snapshot.recv`. Duplicates and gaps are answered with the expected offset; a transfer must start with the announced header.
    - On the last chunk the whole file is verified before the engine is touched. The engine state is then replaced (upserts plus deletion of ids not in the snapshot), and the log is kept after `last_included_index` only if it agrees there.
    - A damaged transfer is discarded and restarted, never applied.
  - **Crash safety**: an `_raft.installing` marker brackets the engine update. At start the install is redone from the received file (it is idempotent). If that file is unusable, the replica resets to empty (engine and Raft state) and is rebuilt by the leader. A half-installed state is never served.
  - **Transport**: `RaftReplicationGroup` sends snapshots automatically when `next_index` was compacted. TCP `OP_RAFT_SNAPSHOT = 0x15` (Admin role, body ≤ 1 MiB chunk) with `StorageClient::raft_install_snapshot`.
  - **Documented limits**: assumes the replicated engine is written only through Raft. The final chunk applies the snapshot on the receiving reactor thread, like `RaftAppend` applies entries.
  - **Exit Criteria**:
    - `test_install_snapshot_catches_up_a_follower_behind_the_compacted_log`: catch-up through the group; stray local data removed; tiny chunks with duplicate and gap handling; a damaged final chunk restarts the transfer; crash recovery without a usable file resets to empty.
    - TCP refusal of a headerless transfer.
    - `0` warnings.

- [x] **TASK-092: Cold Primary Tier — Disk-Resident Primary Index and Ordinal Table (`F-PIDX-03`, `RULE-MEM-03`, `RULE-MEM-04`)**
  - **Format** (`cold_tier.rs`): one immutable generation per checkpoint behind an atomic manifest (`<stem>_primary.cold`). It holds 4 KiB entry pages sorted by `(hash, id)` (v3 varint entries, per-page checksums), a dense ordinal table (`24 B` per slot), page separators and a split-block Bloom filter.
    - Sealed as one streamed region when encryption is on; pages and ordinal slots are decrypted on access.
    - Fully validated at open (`RULE-SAFE-04`): checksums, order, ordinal ↔ entry agreement, tag.
  - **`PrimaryIndex` tiers**:
    - RAM holds entries changed since the last checkpoint, plus tombstones and overridden ordinal slots.
    - Lookups go RAM → tombstones → tier (Bloom filter, one page decode, no shard lock held across tier reads, `RULE-CONC-01`).
    - Checkpoints take a cut under all shard read locks, stream-merge it with the previous tier (`O(page + Bloom filter)` memory; ordinal slots derived per slot in `O(1)`, no `O(N)` map), then install it shard by shard while writers run. Entries unchanged since the cut are evicted, stale tombstones dropped, matching overrides released.
    - The memory tier pays one relaxed atomic load on writes and an `Option` check on lookup misses.
  - **Engine**:
    - `EngineConfig::primary_tier`; open maps the tier (no entries loaded).
    - Deltas and WAL replay apply on top; paged secondary-index reuse works against the tier's stamp.
    - Switching to the memory tier materializes the tier and replaces it with `primary.idx` at the next checkpoint; switching back converts `primary.idx`.
  - **Design decision**: reads do not promote tier-only entries into RAM (a scan would refill RAM). Their frames are re-validated on each read instead.
  - **Exit Criteria**:
    - `test_cold_tier_matches_memory_tier_through_checkpoints_and_restarts` (index level, plaintext and sealed): 6,000 random operations, 6 tier rebuilds, restarts from the tier alone, scans and bitmaps identical, any tampered byte rejected at open.
    - `test_cold_primary_tier_matches_memory_tier_and_bounds_resident_entries` (engine): parity with a memory-tier engine through writes, deletes, checkpoints, restarts, WAL replay and tier switches in both directions; `0` resident entries after a clean open; with paged indexes + encryption.
    - Crash variant 6 (cold tier + paged + encrypted, killed mid-run twice).
    - Benchmark row `with_ref (Cold primary tier)`: `4.2 µs` p50, `0.00` allocs/op (it was `44 µs` before the slot-offset page index). All other allocation gates unchanged.
    - `111` tests passing, `0` clippy warnings (native and portable).

- [x] **TASK-090: `epoll` Reactor on Linux (`RULE-CPU-03`, `F-NET-01`)** — *closed 2026-10-28 after the Linux test run*
  - **`EpollPoller`** (`storage-mmap`, Linux only): persistent, level-triggered registration with caller tokens; `add` / `modify` / `delete` / `wait` over raw `epoll_*` syscalls (no external crates; `epoll_event` packed on x86-64 per the kernel ABI).
  - **Reactor** (`reactor_worker_loop_epoll`, now generic over `ReadyPoller`): each socket is registered once; write interest changes only when a connection's output state changes; a wake-up drives only the ready sockets plus connections awaiting a job: `O(active)` instead of `O(connections)`. `STORINE_REACTOR=poll` falls back to `poll`.
  - **Linux verification**: `cargo test --workspace` green (including `test_epoll_reports_only_ready_sockets_and_tracks_interest` and every server test through the epoll reactor). New `reactor-scale` benchmark (one active connection next to `N` idle ones): epoll stays at ~34k ops/s (`p50 ≈ 27 µs`) from 0 to 8,000 idle connections, while `poll` falls to 9.8k ops/s at 4,000 and 4.4k ops/s (`p50 217 µs`) at 8,000 idle connections. With a single connection the two are within noise (A/B/A/B: 32.8k / 37.2k / 31.6k / 36.4k ops/s), as expected for a 2-descriptor poll set.

- [x] **TASK-091: Release Binary Size Budget (`RULE-MEM-03`)** — *resolved 2026-10-27: budget raised to `< 1 MiB` by the user*
  - Measured (Windows x86-64, fat LTO, stripped): `storage-server` `829 KiB` / `storage-cli` `760 KiB` at `opt-level = 3`; `647` / `613 KiB` at `"s"`; `591.5` / `572 KiB` at `"z"`. No compiler setting alone meets the budget.
  - Linker-map attribution (`.text` ≈ `692 KiB`): `storage_index` `134 KiB` (paged indexes, cold tier, HNSW), `storage_server` `107 KiB`, `core` `97 KiB` (sort monomorphizations, formatting), `storage_core` `97 KiB`, `std` `85 KiB` + `rustc_demangle` `17 KiB` (panic and backtrace machinery, removable only with nightly `build-std` + `panic_immediate_abort`).
  - Options were: (a) Cargo features for optional subsystems; (b) nightly `build-std`; (c) revise the budget. The user chose (c): `< 1 MiB`, treated as a ceiling, with code size still minimized wherever it costs no hot-path speed.
  - **Exit Criteria**: `storage-server` `903.0 KiB` and `storage-cli` `830.5 KiB` (after the cold primary tier), both `< 1 MiB`.

## Stage 22: Write Safety for Money-Grade Workloads — No ABA, Exact Decimals, Atomic Operations, Conditional & Idempotent Writes, Documents (`TASK-093` – `TASK-098`)

*(Follow-Up Prompt 2: close the gaps raised in the audit answers — lost updates, blind overwrites, version reuse, float money, `i128` indexing.)*

- [x] **TASK-093: Monotonic Versions Across Delete + Re-Create (No OCC / CAS ABA) (`RULE-SAFE-08`)**
  - A per-collection **version floor** (highest version ever deleted) is raised by every deletion, including WAL replay of deletions that are no longer in the primary index, and persisted at every checkpoint (`<stem>.vfloor`, checksummed, written before the primary checkpoint). A created record starts above it, so a stale CAS / transaction version can never match a re-created record.
  - **Exit Criteria**: `test_versions_are_never_reused_after_delete_and_recreate` (in-process, after a clean restart, after a WAL-only deletion, and through transactions).
- [x] **TASK-094: Exact `Decimal` Type, `i128` / Decimal Indexing (`F-TYPE-01`)**
  - `storage_types::Decimal`: `i128` mantissa + scale `<= 38`; checked add / sub / mul, division and rescale with explicit `RoundingMode` (half-even default); numeric equality, total order and canonical hashing; strict parser (DoS-safe on hostile exponents). `sortable_key` maps every numeric type into one monotonic `i64` key space (`compress_i128_to_i64` beyond `2^62`); `IndexSnapshot::push_decimal` / `push_i128`.
  - **Exit Criteria**: decimal unit tests (`0.1 + 0.2 == 0.3`, overflow errors, all rounding modes, key monotonicity over the full range).
- [x] **TASK-095: Nested Values & Atomic Field Operations (`F-TYPE-02`, `F-CONC-09`)**
  - `PropertyValue` gains `Decimal`, `List`, `Map` (depth `<= 32`, sorted unique keys validated, cyclic hostile layouts rejected). `FieldOp::{Set, Unset, Add { min, max }, Append}` with checked arithmetic; `Indexable::apply_field_ops`.
  - **SVE** (Storine Value Encoding): the simple tag + little-endian value encoding every SDK implements (golden bytes pinned).
  - **Exit Criteria**: SVE round-trip / truncation / bit-flip / huge-count / depth tests; field-op overdraft and overflow tests.
- [x] **TASK-096: Unified Conditional Commit (`StorageEngine::commit`) & Replica Write Guard (`F-CONC-10`, `RULE-SAFE-03`)**
  - `WriteOp { id, kind: Put | PutWithTtl | Delete | Patch | Check | Rewrite, precondition: None | Absent | Exists | Version }`: canonical lock order, staged evaluation, one WAL transaction, one version per touched record. `TransactionBatch` is built on it. `Rewrite` keeps the version (metadata-only changes).
  - `set_replica_mode`: every direct write API returns `ReadOnlyReplica` (consensus applies through the internal path).
  - New error variants: `ConditionFailed`, `InvalidValue`, `NotLeader`, `CommitUnknown`, `ReadOnlyReplica`, `Rejected`.
- [x] **TASK-097: Document Store (`F-DOC-01` – `F-DOC-04`)**
  - `DocumentRecord` (collection, key, typed fields, index ids) in one engine (cross-collection atomic transactions). Ids `from_any_str("<collection>/<key>")` with full-key verification (hash collisions are errors, never silent overwrites).
  - Up to 4 secondary indexes per collection (numbers in one order-preserving key space, hashed equality tags for text); every index hit re-checked exactly; index builds rewrite at the same version.
  - `DocumentStore::write`: atomic batches, preconditions, field patches, idempotent request ids (a `$idem/<id>` marker with an `Absent` precondition in the same transaction, 24 h window).
  - Filters (`=`, `!=`, `<`, `<=`, `>`, `>=`, between, in, prefix, contains, exists, missing, key prefix, and / or / not), multi-key ordering, offset / limit and key-order cursors; two-pass execution (only the requested page is materialized).
  - **Exit Criteria**: 800 concurrent `+0.01` deposits lose nothing; overdraft refused atomically; idempotent transfer retry applied once; indexed and unindexed queries return identical results before and after restart; hostile names / keys rejected.
- [x] **TASK-098: Document & Transaction Wire Protocol (`OP_DOC_*`, `OP_TXN`) (`F-NET-03`)**
  - `OP_DOC_GET 0x20`, `OP_DOC_WRITE 0x21`, `OP_DOC_QUERY 0x22`, `OP_DOC_ADMIN 0x23` (index builds and drops run on the admin job pool), `OP_TXN 0x24` (typed-frame batch with per-item preconditions: closes the blind `OP_BATCH_PUT` gap), `OP_DOC_INFO 0x25`. New statuses `NotLeader 0x07`, `Invalid 0x08`, `CommitUnknown 0x09`; conflicts report the failing write index and current version.
  - `storage-server` binary serves documents by default (`--records agent` for the legacy typed collection). `StorageClient::doc_get` / `doc_write` / `doc_query` / `doc_command`.
  - **Exit Criteria**: `test_document_api_over_tcp` (concurrent clients, conflicts, idempotency, index job, typed txn, hostile bodies on every new opcode).

## Stage 23: Replication Integrity — Deterministic Replicated Commands, Read-Only Replicas, Safe Reads, a Networked Raft Runtime & Membership Changes (`TASK-099` – `TASK-101`, `TASK-121`)

*(Follow-Up Prompt 2: close the Raft gaps raised in the audit answer.)*

- [x] **TASK-099: Deterministic Replicated Commands & Replica Write Guard (`F-DIST-02`, `RULE-SAFE-09`)**
  - Log entries carry a `RaftCommand`: `Upsert`, `Delete`, `Ops` (any conditional atomic batch: puts, TTL puts, deletes, field patches, checks, rewrites), `Noop`, `SweepExpired`. Kinds `0`/`1` are the original encoding, so existing logs load.
  - Apply goes through `StorageEngine::commit_at`: versions are `>=` the log index (identical on every replica and monotonic across delete + re-create); TTL decisions use the leader's proposal time; failed preconditions are replicated *outcomes*, not apply failures; only I/O / memory errors stop the apply loop.
  - `RaftNode::new` puts the engine in replica mode: every direct write API returns `ReadOnlyReplica` (leader included); snapshot install and resets use internal exact-version paths. Snapshots keep each record's version and TTL.
  - Proposers wait for the applied outcome (`propose_ops` + `wait_applied`); an unconfirmed write returns `CommitUnknown` (it may still commit: retry with the same request id). A new leader appends a `Noop` (commits earlier-term entries, gives read-index a current-term entry).
  - **Exit Criteria**: raft unit tests updated for the no-op; `test_replicas_agree_refuse_direct_writes_and_reject_deterministically`.
- [x] **TASK-100: Networked Raft Runtime (`F-DIST-05`)**
  - `RaftRuntime`: randomized election timers (deadline sleeps, no busy loop), **pre-vote** (a partitioned node cannot disrupt the cluster when it returns), **leader stickiness** (no votes while a live leader is heard from), one replicator thread per peer with bounded `AppendEntries` (entries and bytes), automatic `InstallSnapshot`, quorum commit with the current-term rule, **leader lease** (90% of the minimum election timeout, measured from the send time of acknowledged requests) with a read-index fallback, replicated TTL sweeps.
  - `RaftTransport` trait; `LocalNetwork` in-process transport with partitions (round-trips the wire encoding).
  - **Bug found and fixed**: lock-order deadlock between `propose` (state → outcomes) and `wait_applied` (outcomes → state), caught by a 15-run stress loop; apply now publishes the outcome before advancing `last_applied`.
  - **Exit Criteria**: `test_partitioned_leader_loses_lease_reports_commit_unknown_and_retries_apply_once`, `test_ttl_sweep_is_replicated_and_snapshot_keeps_versions`; 13 consecutive stress runs green.
- [x] **TASK-101: Clustered Server, TCP Transport, Redirects & Follower Reads (`F-DIST-05`, `F-NET-04`)**
  - `TcpRaftTransport` (`OP_RAFT_*`, Admin token, per-peer cached connection, connect / read / write timeouts). `StorageServer::start_cluster` / `start_documents_cluster`: writes (typed and document) run on a bounded worker pool through consensus (never on reactor threads); followers answer `NotLeader` with the leader's id and client address; reads are served by the leader under its lease (else after a read barrier) or by any replica for connections that opt in (`OP_SET_OPTIONS`, bit 0). `OP_CLUSTER_INFO` reports node, leader, term, commit / applied indexes and members.
  - All typed write opcodes share one path (`write_reply` → `ServerShared::commit`); `OP_MIGRATE_VSHARD` imports through consensus on clustered nodes.
  - `DocumentStore` metadata cache is validated against the `$meta` document version on every use (correct on followers and after leader changes); index builds are exact under concurrent writes (*building* → rewrite → *ready*).
  - Binary: `--node-id N --peers 1=host:port,...` and `STORAGE_CLUSTER_TOKEN`.
  - **Exit Criteria**: `test_three_node_tcp_cluster_writes_redirects_and_failover` (8 consecutive runs green), `test_index_build_stays_exact_under_concurrent_writes`; 3-process smoke test of the release binary elects one leader.
  - Static membership at the time; membership changes followed in `TASK-121`.

- [x] **TASK-121: Cluster Membership Changes (`F-DIST-02`, `F-DIST-05`)** (`storage-core/src/raft.rs`, `raft_runtime.rs`, `storage-server`, `storage-cli`)
  - The configuration is a log entry (`RaftCommand::Config`, entry kind 5) in force from the moment it is appended and rolled back with a truncated suffix. Changes are single-server (Raft thesis §4.1): at most one voter added, promoted or removed per change, one change in flight, and only after the leader has committed an entry of its own term.
  - Learners receive the log but neither vote nor count towards commit, leases or elections. `add_voter` adds a learner, waits until it has the committed log, then promotes it. A leader that removes itself keeps replicating without counting itself and steps down when the change commits. Nodes that are not voters never campaign.
  - The configuration at the compaction point is persisted (`<stem>_raft.members`), and snapshots carry it (format `STGESNP2`; version 1 still read). The start-up `--peers` list only bootstraps a new cluster.
  - Runtime: per-member replicator threads start and stop with the configuration; the TCP transport learns new members' addresses from it; redirects and `OP_CLUSTER_INFO` use it (members now with roles).
  - Server: `OP_CLUSTER_CHANGE 0x28` (Admin; add voter / add learner / promote / remove; leader only, followers answer `NotLeader`), run on the worker pool. Binary: `--node-id N --join` starts a node that waits to be added. CLI: `storage-cli cluster <addr> members | add | add-learner | promote | remove` (follows a follower's redirect).
  - **Also fixed**: a race in document index builds: a write that read the index ids just before a build started could commit without the new index. Every write now checks the collection's `$meta` version in its own commit (rebuilt on a change), and the rebuild pass retries changed documents instead of skipping them.
  - **Exit Criteria**: `test_membership_add_voter_then_remove_the_leader`, `test_learners_do_not_count_and_change_rules_hold`, `test_membership_survives_compaction_snapshots_and_restart` (15 consecutive runs green), `test_cluster_grows_and_the_leader_removes_itself_over_tcp` (10 runs green); a release-binary smoke test grew 3 processes to 4 through `storage-cli`, removed the leader (the joined node was elected), and restarted a node with its stale `--peers`, which kept the log's configuration.

## Stage 24: Windows Readiness Reactor — AFD Poll over IOCP (`TASK-102`)

- [x] **TASK-102: `epoll`-Equivalent Reactor on Windows (`F-NET-01`, `RULE-CPU-03`, `RULE-SAFE-01`)**
  - **`AfdPoller`** (`storage-mmap`, Windows only): the `wepoll` / `mio` technique. One `\Device\Afd` handle (opened with `NtCreateFile`) is associated with an I/O completion port; every socket's base provider handle (`SIO_BSP_HANDLE_POLL`, then `SIO_BASE_HANDLE`) gets an `IOCTL_AFD_POLL` request; `GetQueuedCompletionStatusEx` returns only completed (ready) sockets: `O(ready)` wake-ups like `epoll`, without `WSAPoll`'s `O(registered)` scans and known bugs. Requests are re-armed before each wait (level-triggered); interest changes and deletions cancel the in-flight request (`NtCancelIoFileEx`); buffers of cancelled requests are kept (`zombies`) until their completion is dequeued, so the kernel never writes into freed memory; completions are matched to sockets in `O(1)` by allocation address.
  - **Reactor**: the same `ReadyPoller` loop as Linux; `STORINE_REACTOR=poll` falls back to `WSAPoll`.
  - **Verification so far**: `cargo clippy --workspace --all-targets --target x86_64-pc-windows-msvc`: `0` warnings (type-checked and linted against the Windows standard library, including `deny(clippy::undocumented_unsafe_blocks)`; not executed: no Windows machine here). Linux behavior unchanged.
  - **Run under Wine 9.0** (2026-10-28; Windows binaries cross-compiled with `x86_64-pc-windows-gnu`): all 30 workspace test binaries pass on the AFD reactor, including `test_afd_poll_reports_only_ready_sockets_and_tracks_interest`, the server, document API, end-to-end, TCP cluster and membership tests, and the storage-sql tests. Two defects surfaced and were fixed:
    - `AfdPoller::delete` looked a socket up by its handle value instead of the token it was registered with, so a socket added under another token could not be deleted. It now keeps a socket→token map, as `epoll` deletes by descriptor.
    - Every wait re-armed by walking all registered sockets (`O(registered)` in user space). Only sockets whose request completed are now re-armed.
  - `reactor-scale` under Wine: AFD 1,957 / 1,587 / 684 / 160 ops/s versus `WSAPoll` 2,749 / 731 / 88 / 25 ops/s at 0 / 1,000 / 4,000 / 8,000 idle connections. That is 6x better than `WSAPoll` at 8,000, but not flat: Wine emulates AFD polling in `wineserver`, so these numbers say little about real Windows.
  - **Exit Criteria (verified on real Windows, `x86_64-pc-windows-msvc`, 2026-11-08)**: `test_afd_poll_reports_only_ready_sockets_and_tracks_interest` and the `storage-server` suite pass on the AFD reactor. `cargo run --release -p storage-benches --bin reactor-scale` across 2 interleaved A/B pairs at `0 / 1,000 / 4,000 / 8,000` idle connections: default (`AfdPoller` over IOCP) stays flat at `8,589 / 8,281 / 9,246 / 9,671 ops/s` (`p50=84–104 µs`, `p99=289–463 µs`) and `7,837 / 7,317 / 7,023 / 7,996 ops/s` (`p50=90–108 µs`, `p99=600–987 µs`), while `STORINE_REACTOR=poll` (`WSAPoll`) collapses to `8,949 / 1,731 / 346 / 49 ops/s` (`p50=11.06 ms`, `p99=198.15 ms` at 8,000) and `5,814 / 920 / 168 / 42 ops/s` (`p50=10.79 ms`, `p99=185.91 ms` at 8,000) — a **~190×** throughput advantage for `AfdPoller` at 8,000 idle connections.

## Stage 25: Client SDKs — Protocol Specification, Conformance Vectors & Eight Languages (`TASK-103` – `TASK-111`)

*(Follow-Up Prompt 2: "build sdks for popular backend languages … with simplicity and user intuitiveness/clarity included as rules"; Follow-Up Prompt 3: PHP and Ruby as TODO.)*

- [x] **TASK-103: Protocol Specification & Golden Conformance Vectors (`RULE-DX-01` – `RULE-DX-05`)**
  - `protocol/PROTOCOL.md`: framing, statuses, SVE values, every document request / response, retry and redirect rules (request ids reused across retries, `NotLeader` hints, `CommitUnknown`).
  - `protocol/vectors/vectors.json`: values, a document, 22 requests and the response bodies, generated from the server's own encoders and pinned by `storage-server/tests/protocol_vectors.rs` (regenerate with `STORINE_UPDATE_VECTORS=1`).
- [x] **TASK-104: Python SDK** (`sdks/python`) — reference implementation; `Decimal`, `datetime`, `uuid`; context-manager transactions.
- [x] **TASK-105: TypeScript / Node.js SDK** (`sdks/typescript`) — promise API, exact `Decimal`, `bigint` versions, connection pool.
- [x] **TASK-106: Go SDK** (`sdks/go`) — `context.Context` on every call, `errors.Is(err, storine.ErrConflict)`, `-race` clean.
- [x] **TASK-107: Java SDK** (`sdks/java`) — `BigDecimal`, `Optional`, lazy `Stream` scans, lambda transactions, unchecked exceptions.
- [x] **TASK-108: C# / .NET SDK** (`sdks/csharp`) — async-first with `CancellationToken`, `decimal` money, nanosecond `Timestamp`, `& | !` filters, `IAsyncEnumerable` scans.
- [x] **TASK-109: Rust SDK** (`sdks/rust`) — standalone crate (not a workspace member), `forbid(unsafe_code)`, `doc!` / `list!` macros, `& | !` filters.
  - **Exit Criteria (103 – 109)**: every SDK passes every request / response / value vector byte for byte, rejects every truncation of a hostile value, and passes the same end-to-end suite against the release server: CRUD with every value type, 800 concurrent `+0.01` deposits summing exactly, overdraft refused atomically, atomic transfers, version-conditioned writes, indexed vs. unindexed queries, pagination over 120 documents, TTL expiry, invalid names rejected, and a 3-node cluster whose leader is killed mid-run with every deposit applied exactly once.
- [x] **TASK-110: PHP SDK** (`sdks/php`, Follow-Up Prompt 4) — PHP 8.1+, no extensions; `Storine\Decimal` (exact, keeps its scale; PHP has no decimal type and 64-bit ints, so 128-bit values go through a pure-PHP `BigInt`), `Filter::and(...)` / `FieldOp::add(...)->min(...)` factories like Java, `...IfVersion` variants, generator scans, Composer PSR-4 or a plain autoloader; dependency-free test harness (vectors + end to end with forked workers and the 3-node failover).
- [x] **TASK-111: Ruby SDK** (`sdks/ruby`, Follow-Up Prompt 4) — Ruby 3.2+, standard library only; `Storine::Decimal` (exact, keeps its scale, converts to `BigDecimal`, which drops trailing zeros), nanosecond `Time`, keyword bounds `Storine.add(path, amount, min: 0)`, `& | ~` filters, block transactions, Enumerator scans; minitest vectors + end to end (threads, 3-node failover).


## Stage 26: SQL Gateway — PostgreSQL, MySQL & SQL Server Wire Compatibility (`TASK-112` – `TASK-120`)

*(Follow-Up Prompt 2: "build the SQL adapter separately … so that existing projects with SQL and/or ORMs can work with it with zero changes to their codebase or at least almost zero. It should support the different popular SQL formats.")*

- [x] **TASK-112: SQL Engine over Documents** (`crates/storage-sql`) — tables as document collections (primary key encoded in the document key), schemas in `_sql_tables`, unique markers with absent preconditions, sequences, row locks, foreign keys with referential actions, an OCC transaction overlay (statement rollback, savepoints, autocommit retries), filter push-down, primary-key lookups, hash / lookup joins, window functions, CTEs (recursive), views, `DISTINCT ON`, correlated `LATERAL`, set-returning functions.
- [x] **TASK-113: PostgreSQL v3 Endpoint** — SCRAM-SHA-256, simple and extended protocol, text and binary formats (`numeric`, arrays), `pg_catalog` / `information_schema` emulation with stable OIDs, `COPY` in text / CSV / binary.
- [x] **TASK-114: MySQL Endpoint** — `mysql_native_password` with auth switch, text protocol, `COM_STMT_*` binary prepared statements, accurate server status flags, `information_schema` / `SHOW` emulation.
- [x] **TASK-115: Real-Client Compatibility Matrix** (`tests/clients/run_all.sh`) — psycopg2, psycopg 3, asyncpg, PyMySQL, SQLAlchemy, Django; pg, mysql2, Sequelize, Knex, TypeORM, Drizzle, Prisma; pgx, lib/pq, go-sql-driver, GORM; pgJDBC, Connector/J, Hibernate; Npgsql, MySqlConnector, EF Core; PDO, pgsql, mysqli, Eloquent; ActiveRecord (pg, trilogy).
  - **Driver security updates (2026-11-10)**: GitHub reported 26 Dependabot alerts (8 critical), all in these test harnesses and none in storine's crates.
    - Go: `golang.org/x/crypto` 0.24.0 → 0.58.0 and `golang.org/x/net` 0.26.0 → 0.62.0.
    - Java: pgJDBC 42.7.4 → 42.7.12 and `mssql-jdbc` 12.8.1 → 12.8.2.
    - .NET: `MySqlConnector` 2.3.7 → 2.6.1.
    - Prisma: an npm override pins `deepmerge-ts` to 8.0.2.
    - The Java suite had never compiled under Debian's Maven, which defaults to compiler plugin 3.1. That plugin ignores `maven.compiler.release` and targets Java 5, so the pom now pins plugin 3.13.0.
    - The Modal SDK image moved from Go 1.23.4 and Debian's Node 18, both past end of life, to Go 1.27.3 and Node 22.23.4. Current `tedious` cannot load under Node 18.
    - `run_all.sh` takes `STORINE_CLIENTS` to run chosen suites.
    - `tools/modal/sweep.py --clients go,java,dotnet,prisma` runs those suites on Modal, with the result kept on the volume. All four passed (`clients5`): Go, Java, .NET 10 of 10, and Prisma on PostgreSQL, MySQL and SQL Server.
- [x] **TASK-116: Integrity Semantics** — no lost updates (versioned writes), REPEATABLE READ / SERIALIZABLE read validation, race-free foreign keys (reference markers), separate namespaces per `CREATE DATABASE`.
- [x] **TASK-117: TLS for Both Endpoints** — zero-dependency TLS 1.3 (`storage-sql/src/tls.rs`): `TLS_CHACHA20_POLY1305_SHA256`, `x25519` (HelloRetryRequest when the client guessed another group), Ed25519 certificates (self-signed and persisted under `--data`, or supplied as PEM), ALPN, KeyUpdate, `--require-tls`. New `storage-codec` primitives: OS CSPRNG (`rand`), SHA-512, X25519 and Ed25519 (`curve25519`, constant-time, RFC 7748 / RFC 8032 vectors).
- [x] **TASK-118: SQL Server (TDS) Endpoint** (`storage-sql/src/tds.rs`, `exec/mssql.rs`, `exec/merge.rs`) — TDS 7.1 – 7.4 and 8.0: PRELOGIN / LOGIN7, SQL batches, RPC (`sp_executesql`, `sp_prepare` / `sp_execute` / `sp_prepexec`) with typed parameters, transaction-manager requests, ENVCHANGE transaction tracking; TLS in every SQL Server mode (login-only, full TLS in PRELOGIN, TDS 8 strict) plus TLS 1.2 for every endpoint; T-SQL (`TOP`, `OUTPUT`, `MERGE`, table variables, `IF`, `EXEC`, `NEXT VALUE FOR`, `UPDATE ... FROM` alias targets, T-SQL built-ins); `sys.*` catalogs, SQL Server-flavoured `INFORMATION_SCHEMA` and the `sp_*` metadata procedures; declared types and constraint names kept in the schema. Tested with pymssql, SQLAlchemy, tedious, Sequelize, TypeORM, Prisma, go-mssqldb, GORM, mssql-jdbc, Hibernate, Microsoft.Data.SqlClient and EF Core.
- [x] **TASK-119: SQL Server Access-Token Logins — OpenID Connect** (`storage-sql/src/oidc.rs`, `rsa.rs`, `tds.rs`) — the LOGIN7 FEDAUTH feature extension (security-token library) with a JWT from any OpenID Connect issuer: RS256 / RS384 / RS512 (in-house RSASSA-PKCS1-v1_5, 2048-bit minimum) and EdDSA signatures, the issuer's JWKS file re-read on change, `iss` / `aud` / `exp` / `nbf` checks, configurable user claim, FEATUREEXTACK reply, and a refusal that names the reason. Tested with Microsoft.Data.SqlClient, mssql-jdbc, go-mssqldb and tedious.
- [x] **TASK-120: SQL Server Integrated Logins — Kerberos** (`storage-sql/src/kerberos.rs`, `aes.rs`, `tds.rs`) — LOGIN7 SSPI tokens: SPNEGO or plain GSS-API Kerberos AP-REQs from any realm (MIT, Heimdal, FreeIPA, Samba AD, Active Directory), checked against a service keytab (re-read on change); AES encryption types 17 / 18 (RFC 3962) and 19 / 20 (RFC 8009) with in-house AES (AES-NI when available), CTS mode, n-fold / DK and KDF-HMAC-SHA2; ticket and authenticator validation, clock skew, replay cache; AP-REP for mutual authentication, sent as SQL Server sends it. Tested against an MIT KDC with Microsoft.Data.SqlClient (GSSAPI), mssql-jdbc (JavaKerberos) and go-mssqldb (gokrb5).

## Stage 27: Vector Search for Documents and SQL (`TASK-122`)

*(Follow-up: "Can it handle unstructured data?" … "Close the gaps.")*

- [x] **TASK-122: Named Vector Indexes, Nearest-Neighbour Queries & pgvector Compatibility (`F-DOC-05`, `F-SQL-08`)** (`storage-core/src/engine.rs`, `document.rs`, `document_wire.rs`, `storage-server`, `sdks/*`, `storage-sql/src/vector.rs`)
  - **Engine**: per-collection named HNSW indexes over any list-of-numbers field (`Indexable::NAMED_EMBEDDINGS`), kept up to date on write, WAL replay, delete and rebuild at open; up to 4 vector indexes per collection, 4,096 dimensions.
  - **Documents**: `create_vector_index(collection, field, dims)` / `drop_vector_index` (built in the background, `ready` in the index info); `DocQuery.nearest {field, vector, k, exact}` combines with every filter, results carry `$score` (cosine distance). Below 2,048 candidates the scan is exact; above, HNSW with oversampling, exact re-ranking and an exact fallback when filters leave too few. The secondary-index limit went from 4 to 16 per collection.
  - **Wire and SDKs**: an optional trailing `nearest` block on `OP_DOC_QUERY`, admin sub-commands 7 / 8 and info 9 (`protocol/PROTOCOL.md`, new conformance vectors); `nearest(...)`, `createVectorIndex` / `dropVectorIndex` in all 8 SDKs.
  - **SQL (pgvector)**: the `vector(n)` type with pgvector's text and binary formats; `<->`, `<=>`, `<#>`, `<+>`; `l2_distance`, `cosine_distance`, `inner_product`, `l1_distance`, `vector_dims`, `vector_norm`, `l2_normalize`, `subvector`; element-wise `+ - *`, `sum` / `avg`; casts from text and numeric arrays; `pg_type` rows so client adapters find the type by name; `CREATE EXTENSION vector` accepted. `CREATE INDEX ... USING hnsw (col vector_cosine_ops)` (or `ivfflat`) creates a store vector index, and `ORDER BY col <=> $1 LIMIT k` (with or without `WHERE`) reads only the nearest candidates, falling back to a full scan when filters leave fewer than `k`.
  - **Exit Criteria**: engine recall and Raft replica-equality tests; all 8 SDK suites; `test_python.py` pgvector section (psycopg 3 and psycopg2 adapters, SQLAlchemy `Vector`): distances equal numpy's, recall >= 8 of 10 with the HNSW index on 3,000 vectors, filtered and fallback queries exact, binary format, dimension errors (SQLSTATE `22000`); the full 14-suite client matrix green.
- [x] **TASK-123: Ranged Reads of Large Values & Projected Gets (`F-DOC-06`)** (`storage-core/src/document.rs`, `storage-server/src/documents.rs`, `protocol/PROTOCOL.md` §5.1, `sdks/*`)
  - `OP_DOC_GET` takes an optional shape: a byte range of one `bytes` / `text` field (`u64 total | u32 n | bytes`, at most 16 MiB per read, copied straight from the zero-copy record), or a list of up to 256 field paths (only those fields are sent, nested paths nested).
  - `DocumentStore::read_bytes` / `get_fields`; in every SDK `get(key, fields)`, `read_bytes` and `stream_bytes`, which reads chunk by chunk and fails with a conflict if the document changes in between.
  - **Exit Criteria**: engine and TCP tests (3 and 5 MB values: middle, tail, past-the-end and text ranges, chunked reassembly, projections, errors); new conformance vectors; a `large values` end-to-end test in all 8 SDKs.
- [x] **TASK-124: Connection Poolers, Session Resets & Application Locks (`F-SQL-09`)** (`storage-sql/src/exec/advisory.rs`, `parser.rs`, `pg.rs`, `exec/mssql.rs`, `tests/clients/pgbouncer`)
  - Testing PgBouncer found that `DISCARD ALL` and `RESET` were accepted but did nothing (one client's prepared statements and settings would reach the next client on a pooled connection), that `CLOSE ALL` was refused (asyncpg's pool reset) and that advisory locks were missing. `DISCARD ALL` now drops SQL and protocol-level prepared statements, portals, settings and session locks (and fails inside a transaction, as in PostgreSQL); `RESET ALL` / `RESET name` restore session defaults; `CLOSE ALL` is accepted.
  - Application locks stored in the database (lease-renewed, version-conditioned, shared by every gateway): PostgreSQL advisory locks (session / transaction, exclusive / shared, `lock_timeout`), MySQL `GET_LOCK` and family, SQL Server `sp_getapplock` / `sp_releaseapplock` with `EXEC @status = ...` (the old `sp_getapplock` stub granted every lock without locking).
  - **Exit Criteria**: `tests/clients/pgbouncer` (session and transaction pooling; 24 clients x 25 locked deposits exact on a pool of 4 server connections; concurrent transfers; aborted and abandoned transactions do not leak; `DISCARD ALL` between clients on one server connection; psycopg 3 server-side prepares, asyncpg pool, SQLAlchemy `NullPool`); executor tests for every lock function and the session resets; the full client matrix (15 suites) green.


## Stage 28: Columnar HTAP Analytics (`TASK-125` – `TASK-131`)

*(Follow-Up Prompt 5: beat DuckDB / ClickHouse at analytics without slowing anything else.)*

- [x] **TASK-125: Column Segment Format (`F-OLAP-01`)** — immutable mapped column segments: row groups of 16,384 (65,536 originally; reduced in `TASK-128` so a query's decoded columns stay in L2), per-chunk encodings chosen by size (constant, bit-packed FOR, delta-FOR, dictionary, RLE, bitmap, LZ4 text), validity bitmaps, zone maps, exact `i128` decimals, checksums, sealing, validate-once, golden format tests, hostile-bytes tests.
  - `crates/storage-columnar` (`forbid(unsafe_code)`): `bits` (bit packing, bounds-checked reader), `int` (CONST / FOR / DELTA with a restart every 2,048 values / DICT / RLE, chosen by computed size, exact for every `i64`), `column` (every document value kind; variant chunks for fields of several kinds; nested maps flattened to dotted paths, lists stored as SVE; sorted byte dictionaries so codes compare like values; zone maps), `segment` (streamed writer with atomic publish, mapped reader, per-unit checksums verified once, per-unit sealing, a sorted id-hash index for locating rows to delete).
  - **Exit Criteria**: 8 tests: every encoding round-trips exactly (extremes included); every value kind round-trips through a segment (plain and sealed); a sealed segment moved to another context and a plaintext segment under required encryption are refused; a flipped byte inside any checksummed unit is always detected and truncations always fail; the file format is pinned by a golden checksum. Measured on 1M `lineitem`-shaped rows: 45.9 B/row versus 452.8 B/row for the same fields in the row-store encoding (9.9x smaller); integer decode 2.07 ns/value (the generic unpack path; per-width kernels are `TASK-127`).
- [x] **TASK-126: HTAP Maintenance (`F-OLAP-02`)** — opt-in per collection; `O(1)` commit bookkeeping; background merge with atomic segment swap; rebuild of delta and delete state at open; consistent `segments − deleted + delta` reads.
  - **Engine**: `StorageEngine::watch_tag(field, tag)` records the id of every record that carries the tag before or after a write, on every write path (local commit, Raft apply, live WAL replay, snapshot install), under the MVCC writer guard. One relaxed atomic load when nothing is watched. Snapshot registration takes that guard exclusively, so a write visible to a snapshot has always been recorded.
  - **`storage-core::columnar`**: `create_columnar` / `drop_columnar` / `columnar(name)` on `DocumentStore`. The flag lives in `$meta`, so replicas learn it through Raft and build their own segments (derived data, never replicated).
  - **Snapshots**: the MVCC snapshot is registered before the watch is drained. Delta rows are hidden in segments, and delta documents are read as of the snapshot.
    - **Fixed 2026-11-09**: the snapshot was registered before the delta lock was taken. A merge that published in between gave the snapshot segment rows read from the current state, which is newer than the snapshot, next to delta documents read as of the snapshot. One leg of a transfer could then show without the other. This happened once in a workspace run on Modal; 300 repeats, alone and four at a time, never hit it. The snapshot is now registered under the delta lock, so no merge can publish between the two. Unit test `a_merge_racing_a_snapshot_never_shows_half_a_transfer` runs a transfer, a merge and a second transfer at that point: the old order gives 1,600 for 1,500, the fixed one gives 1,500.
  - **Merges**: the background merger drains, then reads current state without holding a snapshot, so no MVCC history is pinned. Small or mostly dead segments (above 25%) are rewritten from their own live rows, with at most 8 segments. Publication goes through an atomically replaced manifest, and unnamed files are swept at open.
  - **Reconciliation at open**: every live row and every document is checked against the current version: `O(rows)` time and 8 B/row of transient memory.
  - **Also fixed** (found by the replica test, present since `TASK-099`): the Raft command decoder assumed operations of at least 49 bytes, but deletes and checks are 46. Every replicated document delete failed as "truncated" (document deletes from SDKs and SQL `DELETE` against a cluster). The minimum is corrected, and every payload-free batch now round-trips with all its truncations refused.
  - **Exit Criteria**:
    - `columnar_htap` (5 tests): the view equals the row store before the first build, after merges, after updates, deletes and inserts, across restarts with an undrained delta, and through 12 compaction rounds (at most 8 segments).
    - A 4,000-step seeded random model run (puts, deletes, merges, reopens).
    - About 9,000 snapshots taken during about 2,600 concurrent transfers and about 240 merges, every one with the exact constant total (no snapshot saw a transfer half done).
    - Encrypted segments contain no plaintext.
    - Crash leftovers and a corrupted manifest never affect results.
    - Raft: every replica's columnar view equals its row store and the leader's, before and after merges, with writes arriving only through consensus.
- [x] **TASK-127: Vectorized Kernels (`F-OLAP-03`)** — `storage-codec::simd`: filters on values and dictionary codes, bitmap combination, checked / exact aggregates, deterministic float sums, hash group-by, top-K; AVX2 / SSE4.2 / NEON with portable fallback, bit-identical across paths.
  - **Kernels**: a `multiversion!` macro compiles each kernel twice (portable, and AVX2 chosen once at runtime; `STORINE_SIMD=portable` forces the portable copy). On aarch64 the portable copy is the NEON build, since NEON is baseline there. Kernels: per-width bit unpacking (byte-aligned groups of 8 values with constant offsets), frame-of-reference add, signed / unsigned / float range masks, equality, bitmap and / or / and-not, popcount, masked and plain exact `i128` sums, masked min / max, `min_max_i64`, `abs_bound_i64` (bitwise only, because AVX2 has neither a 64-bit arithmetic shift nor a 64-bit min / max), and dictionary-code lookup (a one-register bitmap for dictionaries of at most 64 entries).
  - **Filters** (`analytics::filter`): every leaf is judged by `Filter::matches_value`, the document engine's own semantics, evaluated per dictionary entry or at the bounds of a convex interval rather than per row. Convex predicates over sorted integer dictionaries binary-search the matching code run and test codes with the range kernel. Later `AND` leaves run only over still-active 64-row words.
  - **Aggregation**: exact `i128` / decimal sums that widen or fail, never wrap; Shewchuk float sums, so results are independent of order and thread count; HyperLogLog, sparse until 256 registers. Expressions run in plain `i64` when operand bounds prove no overflow. Grouping by dictionary-coded keys gives each row a row-group-local slot and scatters count / sum / avg into a few accumulators, with four interleaved copies and `i64` lanes when bounds allow. Results are ordered by `order_by` then group keys, and `limit` gives top-K.
  - **Exit Criteria**: `analytics_exact`, comparing 300 random filters × 6 groupings × 19 aggregates over segments plus delta, gives the same result as aggregating the documents, and the filters select exactly the documents the document query engine selects. Kernel tests check every kernel against a scalar model on both paths. TPC-H Q1 / Q6 at 1M rows return exactly DuckDB's numbers.
- [x] **TASK-128: Parallel, Memory-Bounded Execution (`F-OLAP-04`)** — morsel-driven workers, zone-map pruning, late materialization, per-query memory budget with partitioned spill for group-by and sort.
  - **Execution**: one morsel per row group on up to 16 workers, with per-worker states merged in a fixed order. Footer zone maps skip a row group before any chunk is read, and the reason is reported as `pruned_row_groups`. To stay sound, pruning judges only comparison leaves over columns with no nulls, uses `matches_value` at the bound the leaf needs, and a zone whose kind is ambiguous (Int / Timestamp) must be excluded under every kind it may hold. Argument columns are gathered only for selected rows when the selection is sparse. Decode buffers are pooled per thread.
  - **Memory**: group tables, distinct sets and sketches are charged to the budget (default 256 MiB). Workers spill partial groups between row groups once usage passes ¾ of it. Each spill goes to 64 hash partitions in sealed frames under an ephemeral per-query key, so plaintext never reaches disk and runs left by a dead process are unreadable and swept. Each partition is then merged and finished on its own, with top-K per partition. Memory stays below the budget plus one row group's worth per worker (worker count is reduced to fit), the disk budget defaults to 16 GiB, and exceeding either fails with a clear error. Sorting applies to the materialized result rows.
  - **Exit Criteria**:
    - `zone_map_pruning_is_exact`: 400 random filters over clustered columns, with every numeric probe kind, nulls, signed zeros and a mixed-kind row group; results are identical and more than 100 row groups were pruned.
    - `spilled_group_by_equals_in_memory`: 25k-group and 18k-group tables plus a delta, run under a 256 KiB budget; results are exactly the in-memory ones (including ordering and limits) and no runs are left behind. Without a spill directory the budget fails cleanly.
    - Spill unit test: runs round-trip, contain no plaintext, are removed once read, and dead runs are swept.
  - **Measured** (1M `lineitem` rows, interleaved with DuckDB 2.0 on this 4-core machine, medians):

    | Query | Storine, 4 threads | DuckDB, 4 threads | Storine, 1 thread | DuckDB, 1 thread |
    |---|---|---|---|---|
    | Q1 | 6.3–7.3 ms | 7.8–9.6 ms | 22.4 ms | 18.8–19.7 ms |
    | Q6 | 2.5–2.9 ms | 2.9–3.5 ms | 6.3–6.8 ms | 5.5–6.0 ms |

    Single-threaded, DuckDB is still ahead. Columnar storage is 51.8 MB versus DuckDB's 26 MB.
- [x] **TASK-129: Aggregation & Columnar Scan Protocol (`F-OLAP-05`)** — `OP_DOC_AGGREGATE`, `OP_DOC_SCAN_COLUMNS`, `PROTOCOL.md`, conformance vectors, `aggregate(...)` in all 8 SDKs.
  - **Protocol:**
    - `OP_DOC_AGGREGATE` (`0x29`, §5.5) carries a filter, group keys (fields / buckets), aggregates over arithmetic expressions, ordering by output column, a limit, and memory and spill budgets. It returns typed rows plus matched / pruned / spilled counters.
    - `OP_DOC_SCAN_COLUMNS` (`0x2A`, §5.6) returns chosen fields of the matching documents in key-ordered pages. It is read from column segments plus the delta, with pruning and vectorized filters, and only the page's rows are materialized.
    - Admin commands `10` / `11` enable and disable columnar storage.
    - Collections without segments answer from the row store.
  - **Never on the reactor**: analytic requests run on their own job threads, at most 8 at a time (then `Overloaded`). In the TCP test, 145k point reads were served alongside 790 concurrent aggregates, worst case 3.5 ms.
  - **SDKs**: Python, TypeScript, Go, Java, C#, Rust, PHP and Ruby share one vocabulary: `aggregate`, `count` / `sum` / `avg` / `min` / `max` / `count_distinct` / `approx_count_distinct`, `field` / `lit` expressions, `bucket`, `order_by` by output name, `scan_columns`, and `enable_columnar` / `disable_columnar`.
  - **Exit Criteria**:
    - Every SDK reproduces the 7 new request vectors byte for byte and decodes the 2 response vectors.
    - Every SDK passes an end-to-end test against the real server with expectations computed from its own data, before and after columnar storage.
    - Server tests: wire round-trips, scans equal to document scans across pages, hostile bodies refused, and OLTP latency under concurrent analytics.
- [x] **TASK-130: SQL Push-Down (`F-OLAP-06`)** — single-table aggregation, filters, projections and `ORDER BY ... LIMIT` pushed down from all three dialects; joins over columnar scans with hash joins; `EXPLAIN` shows the push-down.
  - **Done: aggregation push-down** (`storage-sql/src/exec/pushdown.rs`). A single-table `GROUP BY` runs as one `OP_DOC_AGGREGATE` when the store's result provably equals the gateway's:
    - WHERE translates to an equivalent filter (not a wider one). Constant conversions must be exact; `<>` adds an exists check; `IN` skips NULLs; `LIKE 'prefix%'`.
    - Keys and arguments are plain columns of types compared alike on both sides, with no default filled in for missing fields.
    - Aggregates are COUNT / COUNT(DISTINCT) / SUM (columns, or NUMERIC arithmetic) / AVG (finished in the gateway with each dialect's scale rule) / MIN / MAX.
    - Floats, an open transaction's own writes, validated reads, window functions, subqueries, and anything the store refuses stay local.
    - HAVING, projection, ORDER BY, DISTINCT and LIMIT run as before.
  - **Done: columnar storage from SQL**: PostgreSQL `WITH (columnar = on)` / `ALTER TABLE ... SET (columnar = on|off)`, MySQL `ENGINE = COLUMNAR`, SQL Server `CREATE / DROP CLUSTERED COLUMNSTORE INDEX`. `EXPLAIN` reports the push-down and whether it reads column segments; `SET storine_pushdown = off` disables it per session.
  - **Exit Criteria (met for the aggregation part)**:
    - 15 queries in PostgreSQL, MySQL and SQL Server sessions return identical rows and column types with push-down on and off.
    - This holds over the row store, over column segments, and with a delta of inserts, updates and deletes.
    - 12 of the 15 push down; the 3 that must not stay local.
    - 20k rows: 1.0 ms pushed against 230 ms local.
  - **Done: projection push-down** (`select.rs` `projection_for` / `projected`). Scans read only the columns a query uses, through `OP_DOC_SCAN_COLUMNS` (column segments when the collection is columnar). Joins use the existing hash join over the projected rows, and `ORDER BY ... LIMIT` runs on the narrow rows. Projection stays off for wildcards, subqueries, LATERAL, NATURAL / USING joins, window functions, validated reads, and columns with a filled-in default.
  - **Exit Criteria (projection)**: 12 query shapes (joins, `ORDER BY ... LIMIT`, filters, table functions) return identical rows with projection on and off, over the row store and over column segments, and inside a transaction.
- [x] **TASK-131: Analytics Benchmarks vs DuckDB (`F-OLAP-07`)** — TPC-H Q1 / Q6 and a ClickBench subset, interleaved A/B/A/B on this machine, plus storage footprint; results published in `BENCHMARK_REPORT.md`.
  - Published in `BENCHMARK_REPORT.md` §6, with a TPC-H-shaped `lineitem` and a ClickBench-shaped `hits`. Both are synthetic data with the same columns and skew, not the official datasets, and the report says so.
  - Storine is at parity or ahead on scans, filters and low-cardinality aggregation.
  - It is 6–10× behind DuckDB on high-cardinality grouping and grouped distinct counts. That is recorded as open work, not claimed.

## Stage 29: Deterministic Simulation & Fault Injection (`TASK-132` – `TASK-136`)

- [x] **TASK-132: `Vfs` I/O Seam & Simulated Power Loss (`F-SIM-01`)** — every durable write path through one trait; the simulated file system drops, tears or reorders unsynced writes on a simulated power cut; no indirection on mapped hot reads.
  - **Seam** (`storage-types/src/vfs.rs`): `Vfs` / `VfsFile`. `vfs::at(path)` returns the simulator mounted over the path, or `RealVfs`; with nothing mounted that costs one relaxed atomic load per open.
    - Covered: the WAL (append, checkpoint replacement, archive), record files (temp + rename, ping-pong), arena segments, primary checkpoints and deltas, paged and cold index generations and manifests, HNSW files, column segments, Raft log, meta, snapshots and installs, and backups.
    - Mapped files are mapped from `VfsFile::file()`: hot reads are unchanged (bench gate: `with_ref` p50 66 ns, 0 allocations).
  - **Simulator** (`SimVfs`, over a real directory like a page cache): it remembers each file's contents at its last sync and the journal of creates, renames and removes. On a power cut:
    - every unsynced 512-byte sector is kept or lost on its own (torn and reordered writes);
    - the length lands anywhere between the synced and the written length;
    - unsynced directory changes roll back to a random journal prefix.
  - **Modes and capture**: `CutMode::LoseAll` / `KeepAll` (process crash), crash points (`fail_at(k)`), and freezing (writes after the crash, including through mappings, don't count). Same seed, same disk.
  - **Bugs found and fixed**:
    - a new WAL's directory entry was never synced;
    - a new ping-pong record file was never synced;
    - a new primary-checkpoint delta was never synced;
    - Raft snapshots (taken and installed) were renamed into place without a directory sync, before the log was compacted behind them;
    - the arena directory was never synced after adding segments;
    - an arena segment whose superblock never reached the disk made recovery refuse the database (now re-initialised and scanned).
  - **Exit Criteria (met)**:
    - The WAL test (120 seeds × 3 lives, checkpoints included) loses no acknowledged frame and invents none; removing the new-WAL directory sync makes it fail.
    - The engine test (`tests/power_loss.rs`: 4 layouts / index tiers × 24 seeds × 3 lives, power cuts and process crashes; 200 seeds verified) holds these properties:
      - acknowledged writes all present;
      - the in-flight write all-or-nothing, transactions included;
      - no torn records;
      - the score index equal to the records;
      - no file written around the seam.
- [x] **TASK-133: Deterministic Cluster Simulator (`F-SIM-02`)** — seeded scheduler, simulated network and clocks; Raft safety invariants checked every run; failing seeds reproduce bit for bit.
  - **Driver** (`storage-core/src/raft_sim.rs`): real `RaftNode`s, each with an engine on its own `SimVfs` disk, run from one seeded event queue, with no threads and no real clocks. It drives elections with pre-vote, pipelined replication with varied batch sizes, quorum commit, chunked snapshot installs and read-index reads.
  - **Faults**:
    - message delay, loss, duplication, reordering and very late delivery;
    - partitions (one node isolated, random splits, one-way links), healed later;
    - crashes with power cuts or process kills, then restart from disk;
    - per-node clock skew and timer jumps;
    - random log compaction.
  - **Checked after every event**: election safety, log matching, leader completeness, state-machine safety (no index commits or applies twice with different terms, including after recovery), and applied ≤ committed. **At the end**: convergence to identical engine contents, and a linearizable client history (`TASK-134` checker).
  - **Bugs found and fixed**:
    - **Wrong success after a leader change.** A proposal replaced after a leader change was reported as committed with the replacement's outcome; `RaftRuntime::commit` could return success for a write that never happened. Outcomes are now keyed by index and term, and a replaced proposal returns `CommitUnknown`.
    - **Stale suffix committed.** A follower holding a stale suffix committed and reported as matching entries that a (late or partial) AppendEntries never covered, which could apply an entry the cluster never committed. It now commits up to `min(leader_commit, last new entry)` (Raft Figure 2).
  - **Regression**: both bugs have unit tests that fail without the fix. With the stale-suffix fix reverted, the simulator finds a state-machine safety violation within 65 seeds.
  - **Exit Criteria (met)**:
    - 500 seeds × 3 nodes and 500 seeds × 5 nodes pass. Together: 5,756 crashes (2,914 power cuts), 10,757 partitions, 3,953 snapshot installs, about 69,700 acknowledged writes and 66,800 linearizable reads.
    - The same seed gives the same run (`same_seed_same_run`).
    - `STORINE_SIM_SEED=s` replays one seed and `STORINE_SIM_SEEDS=n` widens the sweep.
- [x] **TASK-134: Linearizability Checker & Process Chaos (`F-SIM-03`)** — P-compositional history checker; fault proxy (partitions, delays, half-open sockets) and `SIGKILL` / `SIGSTOP` of real binaries.
  - **Checker** (`storage-core/src/linearizability.rs`): Wing & Gong's search with Lowe's memoisation of (linearized set, state), applied per key (P-compositionality). Unknown outcomes (`CommitUnknown`, timeouts, crashed servers) return at infinity, so each may or may not take effect after its invocation. A step budget turns a search that is too concurrent to decide into `Inconclusive`, never a pass. Unit tests cover stale reads, reads going back in time, unknown writes, and random linearizable histories.
  - **Process chaos** (`storage-server/tests/process_chaos.rs`):
    - **Setup**: three real `storage-server` processes. Every directed Raft link goes through a fault proxy that can pass, drop (refuse and cut connections), black-hole (half-open: accept and swallow) or delay traffic.
    - **Nemesis**: `SIGKILL` with restart from the data directory, `SIGSTOP` / `SIGCONT` (a paused leader must not serve a stale read when it wakes), isolation through the proxies.
    - **Workload**: 4 clients write and read 3 keys throughout; the history must be linearizable.
  - **Exit Criteria (met)**: a 120 s run had 3 kills, 19 pauses, 5 partitions, 16 half-open isolations and 9 delay episodes. It made 29,979 acknowledged writes (133 unknown) and 30,492 reads, and the history was linearizable. The default run is 20 s (`STORINE_CHAOS_SECS`).
- [x] **TASK-135: Structure-Aware Fuzzing (`F-SIM-04`)** — generators for every decoder and parser, millions of iterations, corpora from the conformance vectors.
  - **Engine, codec and storage decoders** (`storage-core/tests/fuzz_decoders.rs`, `storage-columnar/tests/fuzz_chunks.rs`, `storage-server/tests/fuzz_requests.rs`, plus the WAL and record-server sweeps):
    - **Corpus**: the published conformance vectors (`protocol/vectors/vectors.json`).
    - **Generators**: real encoders build random values, documents, write batches, nested filters, queries, field operations, Raft messages and op batches, record frames, LZ4 blocks, and integer and value column chunks of every kind (mixed kinds, nulls).
    - **Mutations**: flips, truncation, insertion and deletion, splices, and edge values in length and count fields.
    - **Checks**:
      - no panic;
      - every generated input round-trips byte for byte (the query limit is normalised as PROTOCOL.md specifies);
      - whatever a decoder accepts re-encodes and decodes again;
      - the document server keeps serving after every few hundred hostile requests.
  - **Run** (release with debug assertions and overflow checks, `STORINE_FUZZ_ITERS=100–200`): about 10M decoder inputs × 25 decoders, 3.5M column-chunk decodes, 500k forged WAL frames, 200k mutated document requests and 90k mutated record requests over TCP. No panics.
  - **SQL gateway**:
    - **Corpus and generators**: SQL text in four dialects (corpus scanned from the executor tests; token, keyword, unicode and splice mutations; pathological nesting up to 100k levels) plus the value, vector and COPY parsers.
    - **Every built-in function**, called with edge-case arguments.
    - **Private decoders** (in-crate): PostgreSQL, MySQL, TDS, TLS 1.3, Kerberos and OIDC.
    - **Wire level**: real PostgreSQL, MySQL and TDS servers with TLS and password logins, 120k hostile sessions; a panic hook, a 64 MiB per-allocation tripwire and periodic `SELECT 1` health checks.
  - **SQL gateway fixes**:
    - **Parser**: unbounded recursion (now nesting depth 160, operator chains 4000 and tree height 1000, failing with SQLSTATE 54001).
    - **Wire allocation**: messages allocated their claimed length before any bytes arrived; they now grow as bytes arrive.
    - **Overflow**: about 30 integer overflows in date, time, interval and numeric decoding and arithmetic.
    - **Text handling**: UTF-8 slicing panics.
    - **Functions**: argument-count panics; uncapped `repeat` / `lpad` / `REPLICATE` / `SPACE` results (now capped at 1 GiB, SQLSTATE 54000).
    - **Formatter**: output that did not parse back (keyword columns, `--1`, quoted function names, deep chains).
  - **SQL gateway run**: about 290M in-crate iterations plus 11M public-API inputs, release build with debug assertions and overflow checks on. No failures.
- [x] **TASK-136: Crash-Point Sweeps (`F-SIM-05`)** — kill at every `k`-th I/O call, recover, check acknowledged writes, torn state and index equality.
  - `tests/power_loss.rs::crash_at_every_io_call`: a fixed 40-operation workload (puts, deletes, 3-record transactions, checkpoints) is stopped at every I/O call `k = 1 ..= total + 1` in each of 4 layouts / index tiers, then recovered:
    - after a random power cut, a process kill and a total loss of unsynced data;
    - with the same checks as `TASK-132`.
  - `STORINE_SWEEP_STRIDE` thins the sweep for quick runs. `k = 1` found the lost-arena-superblock bug.

## Stage 30: Scale-Out & Multi-Region (`TASK-137` – `TASK-144`)

- [x] **TASK-137: Multi-Raft Ranges (`F-GEO-01`)** — many Raft groups per node over one engine / WAL; coalesced heartbeats; quiescent idle ranges.
  - **Ranges**: a range is a `RaftNode` group over a `KeySpan` (raw id bytes) of the node's single engine and WAL (`RaftNode::open_group`).
    - Each group has its own log and hard state, and snapshots are built, installed and reset within the span only.
    - Writes for keys outside the span are refused at apply, identically on every replica.
    - TTL sweeps of a shared engine run only in a whole-engine group.
  - **Splits are metadata only**: a replicated `Split { at, right }` entry shrinks the left span and creates the right-hand group from the same data and replicas.
    - Order: the right group's state, then the left span, then `last_applied`, so a crash redoes the split or finds it done.
    - The right group starts in the split entry's term, so log terms never decrease.
  - **Driver** (`dist/node.rs`, sans-IO): elections with pre-vote, pipelined replication, quorum commit, read-index reads, chunked snapshot catch-up and split handling for every range.
    - All of a node's traffic to a peer travels as **one batch per node pair**.
    - **Quiescence**: an idle leader sends `Quiesce` and goes silent; followers then drop their election timers while the leader's node keeps sending (possibly empty) batches.
    - A node restart (new incarnation), a silent leader node, or a campaign by the quiesced-on node wakes the range. Pre-votes are refused while a leader is live (thesis §9.6).
  - **Bugs found by the simulator and fixed**:
    - (2026-11-03) **an acknowledged write that was lost**: Raft matched apply outcomes to waiters by log index alone. After a leader change replaced a proposal, the same node could later propose again at that index, and the earlier waiter took the new entry's outcome as its own success (zones seed 166: a write of key 5 acknowledged, never applied, then a stale read). Proposals now return `(index, term)` and `wait_applied` takes both, so a waiter only ever gets its own entry's outcome; every caller (single-group runtime, range servers, simulators) passes the term;
    - (2026-11-03) a split proposed twice at one key (the allocator plans again before the first applies) applied twice: the replay check took any split at the span's current end for a replay of itself, and announced a second right-hand range over keys another range already held. Only the entry that set the span now counts as a replay (`a_repeated_split_splits_once`, zones seed 39);
    - a new Raft log file's directory entry was never synced, so acknowledged entries were lost on a power cut;
    - a range's first configuration lived only in memory, so after a restart its replicas never campaigned;
    - a split-created group started in term 0 below its snapshot term, which broke leader completeness;
    - followers stayed quiescent on a leader that had restarted.
  - **Exit Criteria (met)**:
    - **Idle cost**: 200 idle ranges on 3 nodes exchange exactly 240 batches in 2 s (one per node pair per heartbeat) and 0 range messages.
    - **3 nodes**: 300 seeds with splits during writes, crashes with power cuts and partitions, checked for per-range election safety, log matching, leader completeness, state-machine safety, convergence and linearizability across splits.
    - **3 regions**: 300 more seeds on 5 nodes with region latencies and outages.
    - **Totals**: 1,127 splits, 1,143 crashes and about 26,700 acknowledged writes; all pass.
- [x] **TASK-138: Meta Range, Routing & Rebalancing (`F-GEO-02`)** — replicated range descriptors; leaseholder routing in servers and SDKs; splits, merges and replica moves balanced by count, disk and load.
  - **Done (replica moves and lease transfers)**: `NodeDriver::add_learner` / `promote` / `remove_member` / `transfer_lease` (`TimeoutNow`), simulated with 5 nodes, replication 3 and faults (`replicas_move_and_leases_transfer_safely`). Bugs found and fixed:
    - a learner created after a split replayed it against the narrower current span: snapshots (format 3) now carry the span in force at their index, and installing one adopts it;
    - destroying a replica deleted keys of an overlapping local replica (`destroy_keeping`);
    - lost `Init` messages and orphan learners (`Init` resent with each forced snapshot; orphans collected after 10 s);
    - removed replicas that never learned of their removal (`Gone`, sent by any replica whose committed configuration excludes the sender).
  - The simulator's leader-completeness check now only demands entries committed in earlier terms (a legitimately elected lower-term leader may lack a later-term commit).
  - **Done (merges)**: two Raft commands.
    - `Freeze` ends all writes to the right-hand range.
    - `Merge` carries that range's final records, so every replica of the left range applies the same state, lagging ones included.
    - A merge is proposed only by the node leading both ranges, with equal voters and every replica of the right range having applied everything (followers report their applied index).
    - Absorbed replicas are retired at once, keeping their records. Replicas elsewhere are told `Gone(merged)`, also from a persisted tombstone.
    - Tested by `ranges_merge_safely` (400 seeds).
  - **Done (allocator)**: `dist/alloc.rs` is a pure, unit-tested planner. `NodeDriver` runs it on its leaseholders every interval, from store loads gossiped in the per-node-pair batches. It covers:
    - repair (promote, replace dead replicas, up/down to the replication factor);
    - count and bytes rebalancing: nodes take turns, nothing starts while any store reports a move in progress, and a move always relieves the store that motivated it;
    - lease balancing;
    - (2026-11-03) a range this node just proposed to freeze counts as frozen in its own planning at once: in the same allocator round, a neighbour aligned its replicas before the freeze applied, and the merge stalled behind the resulting removal (meta seed 12);
    - (2026-11-03) turns leave an idle interval between them: in back-to-back turns, two nodes 15 ms apart (less than the cross-region latency) each moved a replica onto the same store before the other's move was gossiped, overshot, and swung back forever (zones seed 89);
    - size and load splits (median key / median of sampled written keys);
    - merges of small neighbours (lease co-location, replica alignment by the left range only, freeze, merge).
    - `allocator_splits_merges_and_rebalances_safely` runs 5 nodes with no simulator-driven changes. Over 400 seeds it applied 4,587 splits and 7,158 merges; every seed stayed linearizable, settled (allocators idle for 2 s) and balanced.
  - **Replica-safety fixes found on the way**. A node never holds two live replicas of one key:
    - an `Init` overlapping a live or frozen local replica is refused;
    - a snapshot install that would widen a span over another live replica is refused and restarted;
    - leaders never send a snapshot older than the range's last split or merge.
  - **Tombstones**: a destroyed replica persists the log position (term, index) of the configuration that removed it. An `Init` from an older position, e.g. a stale leader's uncommitted configuration, is refused. Re-creating it would revive a replica with no log and no votes, which could elect that stale leader.
  - **Other fixes**:
    - members re-added while their removal was pending kept losing their progress;
    - a removed node's `Missing` now completes its removal (this was a message storm);
    - a node that caught up past a split by snapshot never created the right-hand range: now `Missing` triggers `Init`;
    - learners without a leader for 10 s probe their members;
    - orphan learners are collected without deleting keys other local replicas own;
    - WAL syncs before span changes (split, freeze).
  - **Done (meta range and routing)**:
    - Leaseholders publish their range's descriptor to the meta range when its generation or lease changes, over the per-node-pair batches (`RaftMsg::App`), and retry until acknowledged. A merged range's descriptor names the ranges it absorbed, which the meta range drops.
    - The meta range serves `MetaOp::Lookup` / `MetaOp::Publish`. Its index is rebuilt from the descriptor records on each new lease, and older generations change nothing.
    - Frozen ranges serve nothing (the client re-routes), and timestamp caches start over after a split or merge.
    - Clients in the transaction simulator route through their own range caches (`meta_routing`) while the allocator splits, merges and moves ranges under them (`bank_transfers_with_meta_routing_and_allocator`).
    - Routing in the SDKs and the SQL gateway needs the range server over TCP and moves to `TASK-143`.
  - **Bugs fixed while finishing it** (2026-11-03):
    - **Merge chains could strand a frozen range**: a range that had frozen its right neighbour later acted as the small right half of its own left neighbour, handed its lease away and never merged; and a left neighbour aligned the replicas of a range that was mid-merge, so the two ranges' voters could never match again. Now a pending merge comes first, a merging range is never aligned by its neighbour, and a merging range whose replicas drifted (aligned before the neighbour saw the freeze) moves them back to the frozen range's. Found as seeds 10 (`txn_simulation`, an account unreadable at the end) and 266 (`multi_range_simulation`, a range stuck at four voters).
    - **Latch holders** were request ids alone; requests from different clients (and the server's own publications) can share one. They are now (client, request id).
    - The serializability checker ordered transactions committed at one timestamp arbitrarily. A read at `ts` sees writes at `ts`, so it now looks for any order the reads allow (seed 12 was this false alarm, not an engine bug).
  - **Exit Criteria (met)**: 300 seeds of every multi-range and transaction simulation, including meta routing with the allocator.
- [x] **TASK-139: Hybrid Logical Clocks & Uncertainty (`F-GEO-03`)** — HLC timestamps, maximum-offset enforcement with self-fencing, uncertainty-interval reads, optional commit-wait.
  - `dist/hlc.rs`:
    - `Timestamp (wall ns, logical)`, which sorts as 12 big-endian bytes;
    - `Hlc::now` / `update` (monotonic even when the physical clock steps back; causal across messages);
    - a remote timestamp more than `max_offset` ahead is refused and **fences** the node until an operator lifts it;
    - `Uncertainty` windows `(read_ts, read_ts + max_offset]` with restart;
    - `commit_wait` (waits until `ts` is in the past on every clock);
    - `SystemClock` and `ManualClock`.
  - Every node batch carries the sender's timestamp. Transactions and reads use the clock from `TASK-140` on.
- [x] **TASK-140: Distributed Transactions (`F-GEO-04`)** — transaction records, write intents, parallel commits, serializable timestamp ordering with read refresh, deadlock detection, intent resolution, exactly-once coordinators.
  - **MVCC keys** (`dist/mvcc.rs`): each key's record holds its committed versions at HLC timestamps (tombstones for deletes, GC below a threshold) and at most one write intent naming its transaction record. Transaction records are ordinary keys (`_txn/<id>`), so they replicate, split and route like data. Pure rules:
    - reads with uncertainty restarts;
    - writes pushed above newer commits and the timestamp cache;
    - resolution at exactly the commit timestamp;
    - read refresh.
  - **Range server** (`dist/server.rs`): leaseholder evaluation with per-key latches, under a read-index confirmation. Each effect is proposed as one version-checked record write, so every replica applies the same decision, and a concurrent change re-evaluates.
    - **Timestamp cache**: tagged with the reading transaction, so a transaction's own reads never push its writes. A new leaseholder starts it at its clock: the HLC carried by heartbeats and votes bounds every read the old leader can still serve.
    - **Record rules**: stage, conditional commit and abort, and wound-wait push (absent records too, via the intent's age).
    - **Staging-recovery probes** that prevent a missing intent from ever landing below the staged timestamp.
  - **Coordinator** (`dist/txn.rs`, over a `KvSender`):
    - buffered writes;
    - **parallel commit** (intents plus a staging record in one round trip; committed once every intent lands at or below the staged timestamp; then an explicit commit and intent resolution);
    - read refresh when pushed;
    - uncertainty restarts;
    - **wound-wait** (older wounds, younger waits: no wait-for cycles, hence no deadlocks);
    - abandoned-transaction recovery;
    - **definite outcomes**: after the first commit round, any failure is settled from the record (staging records recovered), and only an unreachable record yields `CommitUnknown`.
  - **Simulator** (`dist/txn_sim.rs`): real coordinators on threads that only run while holding a baton, handed out in seeded order. Interleavings are concurrent, and each seed replays exactly.
    - **Checks**: bank audits (snapshot reads of every account) see the conserved total; every committed transaction, replayed in commit-timestamp order, read exactly what the serial order gives; the final state equals the serial result.
  - **Bugs found by the simulator and fixed**:
    - a transaction's own reads pushed its own writes forever;
    - a recovery decided on a stale staging record and raced a re-stage (decisions are now conditional on the examined timestamp and epoch);
    - a re-sent intent of an implicitly committed transaction moved to a later timestamp, splitting the commit;
    - a late replayed write of an already resolved transaction became a zombie intent, which resurrected an old value (writes now remember their committer, and resolution drops intents above the commit timestamp);
    - post-commit failures were reported as "nothing committed".
  - **Bug found later (2026-11-03, by the 300-seed sweeps)**: after a parallel commit, coordinators sent the explicit commit to the record and resolved the intents without checking that the record write succeeded. When it failed (crash, partition), an intent became a plain version while the record still read "staging". A later recovery saw that intent missing, concluded the commit never completed, and aborted the rest: a committed transfer was half undone, and audits saw a total that was not conserved (seeds regions 106, meta 33, follower 232, scan 58, global 127; the first two reproduce on the code before `TASK-141`). Coordinators now resolve intents only once the record is confirmed final, and as it says; otherwise the intents stay for a push to recover. `abort` had the same flaw. The seeds are replayed by `regression_seeds`.
  - **Bug found later (2026-11-03, meta seed 146)**: range servers proposed each record write conditioned on the leaseholder's engine version of the record. Engine versions are node-local (each node's engine is shared by many ranges and numbers versions from its own history), so the same write applied on one replica and failed its condition on another: replicas of a range diverged, and a committed transaction record read "staging" on the next leaseholder, which aborted it. Writes are now conditioned on existence, which is replicated state (per-key latches and read-index ordering already keep evaluations from interleaving). The transaction simulator now also checks state-machine safety directly: replicas that applied the same index must hold identical records.
  - **Exit Criteria (met)**:
    - 400 seeds: calm, faults, and 5 nodes in 3 regions with outages.
    - About 10,600 committed transfers and 3,400 snapshot audits, all serializable, under 1,175 crashes (power cuts) and 2,559 partitions.
- [x] **TASK-141: Locality, Zone Configurations & Survival Goals (`F-GEO-05`)** — region / zone labels, constraints, lease preferences, zone / region survival; closed-timestamp follower reads; global and regional-by-row tables.
  - **Zone configurations** (`dist/zone.rs`): nodes declare a region and zone (`NodeConfig::localities`). A configuration per key span sets the voter count, required / prohibited regions and zones, ordered lease preferences, a survival goal (zone or region) and a closed-timestamp policy. `ZoneConfig::validate` refuses what the cluster cannot meet (too few eligible nodes, a survival goal no spread achieves, a lag below or a lead not above twice the clock offset). `NodeDriver::set_zones` validates and applies changes at runtime.
  - **Allocator** (`dist/alloc.rs`, through a `Placement` per range):
    - a range straddling two configurations splits at the boundary, and neighbours under different configurations never merge;
    - replicas go only to allowed stores, spread over regions then zones before load;
    - a replica on a disallowed store, or a failure domain holding a quorum against the survival goal, is moved (the leaseholder hands its lease over first if it is the one to move);
    - over-replicated ranges drop disallowed, then dead, then the most crowded domain's replicas;
    - rebalancing by load never lowers diversity;
    - the lease goes to the most preferred region with a live, caught-up voter, and lease balancing stays among equally preferred voters.
  - **Closed timestamps and follower reads**:
    - Each heartbeat round, a leaseholder picks a candidate (its clock minus the lag, or plus the lead) at its last log index. It is confirmed once a quorum acknowledges that round: those voters saw the round's batch clock, and every later leader's election quorum shares one of them, so its clock is above the candidate. Confirmed values travel on appends of the current term and are usable once the index is applied.
    - Writes land above the closed timestamp. A new leaseholder, or a range just split or merged, starts above the largest lead configured plus the clock offset, so a lagging replica of a key's old range can never serve a read that a later write lands under. Dropping a global span keeps its promises as a floor until they pass.
    - Any replica serves a `Get` whose timestamp and uncertainty window are at or below its closed timestamp, with no timestamp-cache entry (no write can land there); otherwise it redirects to the leaseholder.
    - `Txn::begin_as_of` reads a past snapshot (no uncertainty window, writes refused), which replicas serve once it is closed.
  - **Global tables** (`zone::global`): the span closes ahead of time (lead). Writes land in the future and their coordinators commit-wait until the timestamp has passed, so any replica serves present-time reads. The lead exceeds twice the offset, so every clock recognises such a timestamp as synthetic: servers and coordinators order by it but never absorb it into their clocks. Global ranges do not quiesce, since their followers need a closed timestamp that keeps up.
  - **Regional-by-row tables** (`zone::regional_by_row`): a row's key starts with its home region, and each region's span prefers leases there (`zone::regional_key`, `zone::home_region`).
  - **Exit Criteria (met)**:
    - `zone_configs_place_replicas_and_leases`: 6 nodes in 3 regions, the allocator alone placing two configurations (constraints with a prohibited region and lease preference; 5-voter region survival) under crashes and partitions. Every settled range honours its configuration and none straddles a boundary.
    - `follower_reads_at_closed_timestamps`: audits `AS OF` 400 ms ago from random replicas under faults. They are conserved and fit the serial order, and replicas serve hundreds of them.
    - `global_table_reads_from_any_replica`: global accounts on 5 nodes in 3 regions with outages; present-time reads from any replica stay serializable and linearizable.
    - `zone_changes_keep_closed_promises` and planner unit tests for every rule above.
    - 300 seeds of each simulation.
  - **Not verifiable here**: real multi-region latency; the simulator's latencies stand in for it.
- [x] **TASK-142: Region-Scale Verification (`F-GEO-06`)** — simulator runs with region latencies and outages; multi-process clusters behind the fault proxy.
  - **Simulator**: `dist::sim` and `dist::txn_sim` run 5 nodes in 3 regions, with per-pair latency (0.2–2 ms within a region, 20–60 ms across), clock skew within the offset bound, and whole-region outages (`three_regions_with_region_outages`, `bank_transfers_across_regions`).
  - **Processes**: `range_cluster_under_kills_pauses_and_partitions` runs 3 real `storage-server --ranges` processes behind the fault proxy (drop, black-hole, delay on every directed link) with SIGKILL and SIGSTOP. Single-key operations stay linearizable, and concurrent atomic transfers conserve the total. 90 s on Modal: 3 SIGKILLs, 15 SIGSTOPs, 4 partitions, 12 half-open links, 5 delays; 2,247 writes and 2,289 transfers.
  - **Not verifiable here**: real multi-region latency and petabyte scale.
- [x] **TASK-143: SDK & SQL Integration (`F-GEO-07`)** — range-aware SDK routing in 8 languages; SQL transactions across ranges.
  - **Decision (under the master prompt's mandate)**: gateway routing, as CockroachDB does. Every node accepts any request and coordinates it, forwarding each key's work to its leaseholder through its own range cache. SDKs keep the one-address-or-list connection they already have and gain no per-language range caches, which keeps them small and alike (`RULE-DX-01`). The cost is at most one extra hop. Follower reads stay an explicit opt-in.
  - **Done (groundwork, 2026-11-03)**:
    - `dist/wire.rs`: versioned encodings of node batches (every `RaftMsg`) and of every KV request and response. Decoders bound every count by the bytes left, refuse unknown tags and trailing bytes, and never panic (every truncation and single-bit flip is tested). Golden bytes pin the format.
    - **Ordered ids**: `StorageEngine::enable_ordered_ids` / `ids_in_span` keep live ids in byte order (an opt-in index in the primary index; off, the write path pays one relaxed load). Range servers enable it, so span scans cost `O(log N + |R|)`.
    - **Span scans**: `KvRequest::Scan` pages through one range at a time and says where to resume. It waits for in-flight writes in its span and marks the scanned span in the timestamp cache, so no write lands under it (phantoms). Intents come back as `KeyConflict`. `RefreshSpan` re-validates a scanned span when the transaction's timestamp moves.
    - `Txn::scan` overlays the transaction's own writes and refreshes scanned spans with its reads. `scan_audits_are_serializable` audits every account with one scan across ranges under faults.
    - Range servers refuse to serve or overwrite a record whose stored key differs from the requested one (long keys hash into ids).
  - **Done (document layer)**: `dist/docs.rs` serves the whole document API as serializable transactions: conditional batches, field operations, idempotent request ids, TTL, queries, vector search, collection metadata. A document is `{"v": version, "f": fields}` and a delete leaves `{"v": version}`, so versions are never reused. Secondary indexes, vector indexes and column segments are metadata here: queries scan, vector search scores exactly, and analytics run on the coordinator over one snapshot. All are correct and `O(collection)` per call; distributed indexes are not built yet.
  - **Done (one-phase commits)**: a batch whose keys share a range commits at the leaseholder in one Raft entry (`KvRequest::Atomic`), under latches on all its keys, with no intents and no transaction record. Writers of a hot key queue FIFO. A batch meeting an intent waits up to 250 ms with its latches released; anything it cannot do (keys across ranges, a lingering intent, any domain error) falls back to a full transaction, which also words the error. Request ids live inside the document's value (`"r"`, 60 s window, at most 4,096) so they are always in the write's range; a full transaction also keeps a `$req` marker.
  - **Done (real-time runtime)**: `dist/runtime.rs` has each node's event loop (`RangeNode`), the `Transport` trait with an in-process implementation, and `Gateway`, a `KvSender` routing to leaseholders through a meta-range cache, following redirects, with bounded parallelism per call. `storage-server --ranges` adds `TcpRangeTransport` (per-peer sender threads with bounded queues, pooled KV connections) and two Admin opcodes, `OP_RANGE_BATCH` and `OP_KV`. Blocking work runs on a bounded worker pool.
  - **Done (SDKs and SQL)**: all 8 SDK suites pass against range clusters with no SDK code changes (`tools/range-server.sh`, `modal run tools/modal/sweep.py --sdks all`). Their only range-mode condition: Python and Ruby skip waiting for column-segment pruning. `storage-sql/tests/range_cluster.rs` runs SQL across ranges: transfers, rollback, aggregates, joins, subqueries, unique / check / foreign-key constraints, and concurrent `SERIALIZABLE` updates with no lost update.
  - **Durability redesign for range groups** (Raft logs are the durable record):
    - the engine's WAL is not fsynced per write (`range_wal_policy`) and applied indexes are saved at checkpoints (`NodeConfig::checkpoint_us`): capture applied, sync the WAL, then save. A crash replays from the last checkpoint;
    - range writes are unconditional whole-record puts and deletes (enforced in `NodeDriver::propose`), so replay converges whatever the engine still holds;
    - entries below the span's last change are skipped on replay: the WAL was synced before that change was saved;
    - logs are compacted at checkpoints (`NodeConfig::log_retain`), where before they grew without bound.
  - **Bounded history**: keys keep versions for 60 s and at most 64 (`MvccKey::trim`), with a floor: a read below it restarts above it, a refresh across it fails, and an `AS OF` read below it is an error.
  - **Bugs found and fixed on the way**:
    - a single-voter range never committed its first entry, so it never served;
    - the gateway deadlocked on a cache read guard held across a write lock, after any leadership change;
    - replies waited for the next input or tick: the event loop now polls until nothing moves;
    - a metastable collapse under contention (99 to 3 updates/s): a metadata refresh by write transaction put intents in everyone's way, and each writer meeting an intent joined the slow path;
    - a batch writing one key twice read its own request id back and dropped itself, reporting success;
    - a thread per request in `Gateway::send`.
  - **Single-group clusters**: a field patch without a request id now gets a generated one, so a log replay after a crash (engine commit durable, applied index not) cannot apply it twice.
  - **Measured** (on the 2-core development machine, one node, Python SDK): an uncontended put went from 91 ms to 3.5 ms; 8 writers contending on one document from about 1 to 75 updates/s, all exact. Under the same 90 s of faults on Modal, a range cluster made 2,247 writes (it was 462) against 66,887 for a single Raft group, whose writes are one local commit rather than a coordinated one: range mode is still far slower, and each entry is still one proposal with one fsync (no batching of proposals yet).
  - **Exit Criteria (met)**: `documents_under_faults` and `documents_with_ranges_moving` (300 seeds): atomic transfers conserved, counters retried with one request id within acknowledged .. attempted. A mutation disabling request-id detection is caught (seed 65). Plus the 8 SDK suites, the SQL test and the chaos run above.
- [x] **TASK-144: Cluster Backups & Changefeeds (`F-GEO-08`)** — consistent cluster-wide backups at one timestamp; resumable change data capture.
  - **One primitive, two features**: both are historical reads of the whole key space, evaluated like reads: an intent at or below the timestamp is resolved first, and a timestamp-cache mark lands later writes above it. So once a span is read, nothing can still appear at or below that timestamp, whatever moves, splits or fails.
  - **Backup** (`dist/backup.rs`): `T` = coordinator clock + maximum offset, and the read starts once the clock has passed it, so a backup holds every write acknowledged before it began.
    - Checksummed segments, then a manifest stored last and atomically: a backup without a manifest does not exist. A retried backup keeps its directory; a failed one removes it.
    - Holds each user document's stored layout, so versions, expiry and the version floors of deleted documents survive (`RULE-SAFE-08`), plus collection metadata. Not `_txn`, `_meta`, `$req` or MVCC history.
  - **Restore**: into a cluster with no user data, through ordinary transactions. Every segment is verified first, so a damaged backup changes nothing. A marker makes a failed restore re-runnable with the same backup and refuses any other. Restored values get new commit timestamps. Writers are not kept out during a restore.
  - **Changefeeds** (`dist/feed.rs`): the whole state is a cursor the consumer keeps, so any node serves the next page. A pass reads every version in `(resolved, to]` and then resolves `to`.
    - Per-key timestamp order; across keys, order by resolved marks; delivery at least once.
    - A cursor can start at a backup's timestamp: snapshot + feed = current state.
    - `storage-cli changefeed` writes a JSON-lines file with a cursor file beside it. Each page is fsynced before its cursor; a line torn by a crash is cut off on reopen. Values JSON cannot carry exactly (large integers, bytes, decimals, NaN) are tagged, never approximated.
  - **History protection is advisory; detection is what guarantees correctness.** A protection is in memory on every node with a TTL (backup 120 s, feed 10 min, renewed while running). `trimmed_below` detects whatever it failed to keep:
    - a backup starts over at a newer `T` (up to 4 attempts), and never writes out a wrong value;
    - a feed fails with `HistoryLost`, and the consumer starts over from a new backup.
    - Bounds under protection: 1024 old versions and 16 MiB per key (`HISTORY_MAX_BYTES` also applies without protection).
  - **Bug found and fixed**: trimming a key's last tombstone dropped the versions it hid without advancing `trimmed_below`, so an `AS OF` read below the delete reported the key as never having existed. Scan pages also gained an 8 MiB budget.
  - **Serving**: `OP_ADMIN_BACKUP` (range mode), `OP_ADMIN_RESTORE` and `OP_CHANGEFEED`, all Admin (a feed reads every collection and holds history back on every node), on the worker pool, with one backup or restore per node. Backup names resolve under `STORAGE_BACKUP_ROOT` as for single-node backups. `storage-cli remote-backup`, `remote-restore`, `changefeed`.
  - **Known limits**: each feed pass and backup reads every key (`O(keys)`, not `O(changes)`); protection does not survive a node restart (detected, not silent); expiry is not a change event; no incremental backups; feeds and restore are not in the 8 SDKs (backups are not either: operator tools).
  - **Exit Criteria (met)**: `backups_and_changefeeds_under_faults` and `..._with_ranges_moving` (300 seeds, power cuts, partitions, ranges moving): every backup and every resolved feed state equals the serial order of committed transactions at its timestamp; a backup at or above every commit acknowledged before it began (writers' clocks lead the backup client's); feed pages re-delivered after simulated consumer crashes. Three mutations are caught: no timestamp-cache mark on version scans, version scans ignoring intents, no offset in `T`. TCP test `range_backup`: restore of a backup taken under load conserves money, backup + feed equals the live cluster, a damaged backup is refused, torn feed lines are cut. `changefeeds_get_every_version_or_fail_loudly`: a protected feed sees all 150 versions of a hot key; an unprotected one gets `HistoryLost`, not a gap.

## Stage 31: Enterprise SQL & Ecosystem (`TASK-145` – `TASK-152`)

- [x] **TASK-145: Roles, Grants & Row-Level Security (`F-ENT-01`)** — enforced `GRANT` / `REVOKE` (previously accepted and ignored), users with SCRAM verifiers, row-level security policies, in all three dialects.
  - **Roles** (`exec/acl.rs`): global documents holding verifiers only (SCRAM-SHA-256 and the MySQL native hash), never passwords; `pg_dump` and MySQL hash forms are accepted as they are. Memberships are transitive, with admin rights and no cycles. `--user` accounts are bootstrap superusers. Without any `--user` (trust mode) every name that is not a role stays a superuser, since a client may claim any name; a role keeps only what it was given.
  - **Syntax**: PostgreSQL (`CREATE/ALTER/DROP ROLE|USER`, `GRANT`, `REVOKE [GRANT OPTION FOR] ... [CASCADE]`, `ALTER DEFAULT PRIVILEGES`, `SET ROLE`, `SET SESSION AUTHORIZATION`, `REASSIGN OWNED`, `DROP OWNED`, `CREATE/ALTER/DROP POLICY`, `ALTER TABLE ... ENABLE/FORCE ROW LEVEL SECURITY`, `OWNER TO`); MySQL (`CREATE USER ... IDENTIFIED BY`, `*.*`, `db.*`, `SHOW GRANTS`, `SET PASSWORD`, roles); T-SQL (`CREATE LOGIN/USER`, `ON OBJECT::`, `ON SCHEMA::`, `DENY`, `GRANT CREATE TABLE`, `ALTER ROLE ... ADD MEMBER`, fixed `db_*` roles, `sysadmin`, `EXECUTE AS`, `REVERT`). Answers from `has_*_privilege`, `pg_has_role`, `pg_roles`, `IS_MEMBER`, `HAS_PERMS_BY_NAME` are real now.
  - **Enforced where rows are reached, not by walking statements** (a missed AST node would be a hole): every table scan and DML target scan asks for its restriction; every fast path (filter push-down, atomic update, primary-key lookups through scans, lookup joins, vector and aggregate push-down) backs off under one. Hidden columns fail when resolved in any clause. No filter is pushed down under a restriction, so `WHERE secret = 'x'` cannot answer "any rows?".
  - **Row-level security** as in PostgreSQL: permissive policies or-ed (none: no rows), restrictive and-ed, `USING` for visibility, `WITH CHECK` (or `USING`) for new rows, owners and `BYPASSRLS` exempt unless `FORCE`. Policies bind sessions of every dialect. Views run with their owner's rights.
  - **Refused precisely instead of over-granting** (`RULE-DX-05`): schemas other than `public` / `dbo` (schemas are not namespaces here, so such a grant would cover every table); MySQL accounts restricted to a host (not tied to client addresses here); `CREATE SECURITY POLICY` (use `CREATE POLICY`); `VALID UNTIL`; per-role settings; privileges on another database; `AS` / `GRANTED BY`.
  - **Known limits**: grants made on one gateway bind others within 2 s (at once on its own); MySQL roles are always active (native MySQL needs `SET ROLE`); `USAGE` on a schema is not checked (one namespace); `information_schema.table_privileges` and `column_privileges` list explicit grants (since `TASK-150`), the rest of that family stays empty; T-SQL user and login are one role; SSO principals (OIDC, Kerberos) hold nothing until granted; the document API and SDKs authorize with their own tokens and roles, not these; `COPY FROM` into a table with row-level security is refused.
  - **Exit Criteria (met)**: `tests/access_control.rs` (10 tests): 38 statement shapes refused without a grant; column privileges in 17 clause shapes including probes for values that do not exist; row security through point lookups, push-down, joins, subqueries, atomic updates and upserts; views; role escalation paths; all three dialects. Real clients with catalog roles: psql (SCRAM), PyMySQL (native), python-tds (TDS): right password in, wrong password and unknown user out, unprivileged write refused. Full workspace on Modal: 61 test binaries.
- [x] **TASK-146: Cancellation, Timeouts & Limits (`F-ENT-02`)** — PostgreSQL `CancelRequest`, MySQL `KILL`, TDS attention; statement / lock / idle-in-transaction timeouts; per-query memory limits.
  - **Session control** (`exec/control.rs`): each session has a control reachable through the gateway's registry. Checks run in expression evaluation (every plan's rows pass through it), per join candidate, per document of a storage scan, and in lock waits. A single blocking storage request is not interrupted; it ends within its own timeout.
  - **Cancel**: `CancelRequest` (a random per-connection key, compared in constant time; replaces a fixed constant), `pg_cancel_backend`, MySQL `KILL QUERY`. A cancel reaching a session between statements does nothing.
  - **TDS attention**: arrives on the connection the request is running on, so a watcher looks at the socket (`peek`, which also works under TLS). Any byte during a request is attention, since nothing else may be sent then; end of stream means the client is gone, and the statement stops too.
  - **Terminate**: `pg_terminate_backend`, MySQL `KILL` and `COM_PROCESS_KILL`, T-SQL `KILL`: permanent for the session, and an idle connection is woken by shutting down its socket's read side, then told why. Superusers or the session's own user only.
  - **Timeouts**: `statement_timeout`, MySQL `max_execution_time`; `lock_timeout`, MySQL `innodb_lock_wait_timeout`, T-SQL `SET LOCK_TIMEOUT` (`-1` forever, `0` none), for row locks and advisory locks; `idle_in_transaction_session_timeout` (a janitor closes the connection with `25P03`).
  - **Memory**: `max_query_memory` (session setting; default 4 GiB) counts rows a statement holds: documents as a scan reads them, and join output as it is produced, so an exploding join stops mid-way. A subquery's working rows stop counting when it returns. Error `53200` (MySQL 1038, T-SQL 701).
  - **Known limits**: memory is estimated from row contents, not measured; projected column scans are counted after they return; MySQL `KILL` of a query blocked in one storage request waits for that request.
  - **Exit Criteria (met)**: `tests/control.rs` (5 tests) including raw PostgreSQL wire: a wrong key does nothing, the right one stops the query; idle-transaction and terminate messages arrive before the close. Real clients: PyMySQL `KILL QUERY` stopped a query at 0.33 s with the connection usable, `KILL` with 1927, `max_execution_time` with 3024; python-tds attention acknowledged at 0.43 s with the connection usable. Full workspace on Modal: 62 test binaries.
- [x] **TASK-147: Activity, Statistics & `EXPLAIN` (`F-ENT-03`)** — `pg_stat_activity`, `pg_stat_statements`, `pg_locks`, `processlist`, `sys.dm_exec_*`; real `EXPLAIN [ANALYZE]`.
  - **Activity** (`exec/activity.rs`): each session publishes its state (statement text as received, timings, transaction, what it waits for). `pg_stat_activity` lists every session; a role that is no superuser sees others' sessions but `<insufficient privilege>` for their statements. MySQL (`SHOW PROCESSLIST`, `information_schema.processlist`, `performance_schema.threads`) and SQL Server (`sys.dm_exec_sessions`, `dm_exec_requests`, `dm_exec_connections`, `sp_who`) show only a role's own sessions without rights, as there.
  - **Statement statistics**: per gateway, by user, database and text with literals replaced (literals never stored), up to 5000 entries, least-used evicted; successful statements only. `pg_stat_statements` and `pg_stat_statements_reset()`, `performance_schema.events_statements_summary_by_digest`, `sys.dm_exec_query_stats` with `sys.dm_exec_sql_text`.
  - **Locks**: `pg_locks` lists row locks, advisory locks, and waiters (`granted = false`) from what the sessions report.
  - **`EXPLAIN`** (`exec/explain.rs`): built from the executor's own decisions, calling the same primary-key, filter push-down, aggregate push-down and vector rules: `Primary Key Lookup`, `Index Scan`, `Seq Scan` with `Storage Filter`, `Aggregate Push-Down`, `Hash Join`, `Nested Loop`, `Sort`, `Limit`, row security. Under row security it shows a full scan, because that is what runs.
  - **`EXPLAIN ANALYZE`**: runs the statement (its changes stay, as in PostgreSQL) with a trace of rows, time and loops at each scan, join and select. `FORMAT JSON` for tools. MySQL's traditional table, `FORMAT=JSON`, `FORMAT=TREE`. SQL Server `SET SHOWPLAN_TEXT ON`.
  - **Also added**: T-SQL `CROSS APPLY` and `OUTER APPLY` (needed by the standard `dm_exec_sql_text` query), and functions on the right of a join that read columns of the left side.
  - **Known limits**: statistics are per gateway and reset on restart; subqueries inside expressions are shown as filter text, not as sub-plans; a sort or aggregate shows the actual rows of the select holding it; view bodies are not traced; prepared TDS statements report their prepare text.
  - **Exit Criteria (met)**: `tests/activity.rs` (5 tests): a concurrent statement seen running with its text, a lock wait in `pg_stat_activity` and `pg_locks`, statements grouped without literals, privacy across roles, plan shapes against actual rows in all three dialects.
- [x] **TASK-148: Cost-Based Optimizer (`F-ENT-04`)** — `ANALYZE` histograms and HyperLogLog distinct counts, join reordering, index and push-down choice, subquery decorrelation.
  - **Statistics** (`exec/stats.rs`): `ANALYZE`, MySQL `ANALYZE TABLE`, T-SQL `UPDATE STATISTICS`. Row count; per column null fraction, HyperLogLog distinct count over every row (2^14 registers), and from a reservoir sample of 30,000 rows an equi-depth histogram (100 buckets) and the 10 most common values. Stored as documents, so every gateway plans alike; shown in `pg_stats`.
  - **Join order** (`exec/optimizer.rs`): chains of inner joins and comma lists are flattened and ordered by dynamic programming over subsets up to 10 relations, greedy beyond. Column positions are restored afterwards, so `SELECT *` does not change. Outer joins, `USING`, `NATURAL`, lateral items, derived tables and CTEs keep the written form. No statistics on a leaf: the written order stays.
  - **Index and push-down choice**: the store picks an index for a pushed filter. Reads under a restriction push nothing down; a planned join fetches only matching right rows when the left side has at most 2000 rows. `EXPLAIN` shows the order, estimates and costs.
  - **Decorrelation**: correlated `EXISTS` / `NOT EXISTS` and a correlated scalar `count` / `sum` / `avg` / `min` / `max`, when the correlation is `inner = outer` equalities, run once as a hash set or group map. A row whose key class differs (MySQL's `1 = '1'`) falls back to running per row.
  - **Bugs found by the differential test, both older than this task**:
    - Hash joins compared by a canonical key while `=` may coerce across types, so MySQL's `int_col = text_col` lost matches. A join now compares pair by pair when the two sides' key classes differ.
    - A correlated subquery's `WHERE id = id`, or any name both queries have, could be evaluated against the enclosing row when choosing a primary-key lookup or a pushed filter, giving wrong rows. Names now bind to the query's own columns first.
  - **Numbers** (release build on Modal, one gateway, optimizer off / on / off / on): badly written 4-way join over 20k-row tables 837 / 227 / 381 / 211 ms; comma form 502 / 197 / 398 / 207 ms; **already best-written order 135 / 131 / 132 / 145 ms (no gain, up to about 10% lost)**; point join 49 / 48 / 50 / 47 ms; correlated `EXISTS` over 20k x 20k over 60 s (stopped) / 319 / over 60 s (stopped) / 350 ms; correlated `NOT EXISTS` 1956 / 147 / 1874 / 132 ms; correlated scalar aggregate 6172 / 160 / 6213 / 163 ms.
  - **Known limits**: no automatic `ANALYZE`, and stale statistics can pick a worse order (never a wrong answer); correlated `IN` and non-equality correlation still run per row; estimates assume independent columns; lookups ignore index availability.
  - **Exit Criteria (met)**: `tests/optimizer.rs`: 47 hand-written shapes and 150 random joins give identical columns and rows with the optimizer off and on, in three dialects, with and without statistics, and inside a transaction with stale statistics; restrictions hold under any order; estimates checked against true counts.
- [x] **TASK-149: SQL Surface for Tools & Ad-Hoc Use (`F-ENT-05`)** — cursors, temporary tables, materialized views, functions (SQL, PL/pgSQL subset), triggers, `LISTEN` / `NOTIFY`, generated columns, range / network types.
  - **Temporary tables** (`exec/temp.rs`): `CREATE TEMP TABLE` used to create a shared, permanent table. It is now private to its session, hides a permanent table of the same name, works with constraints, indexes and transactions, honors `ON COMMIT PRESERVE ROWS | DELETE ROWS | DROP`, and ends with its session or `DISCARD ALL`. MySQL `TEMPORARY`, T-SQL `#name`. Gateways keep a lease, so temporary collections of a dead gateway get swept. Limits: no foreign keys, no rename, no triggers, and an idle session's tables go if its gateway's lease lapses (1 hour with no temporary-table activity).
  - **Cursors** (`exec/cursor.rs`): `DECLARE` / `FETCH` / `MOVE` / `CLOSE` with every direction, `WITH HOLD`, psycopg named cursors; T-SQL `DECLARE c CURSOR`, `OPEN`, `FETCH ... INTO`, `@@FETCH_STATUS`, `DEALLOCATE`. All are insensitive and scrollable: rows are materialized at `DECLARE` / `OPEN` and count against `max_query_memory`. T-SQL has no `WHILE` here yet.
  - **Materialized views**: `CREATE`, `REFRESH` (atomic; `CONCURRENTLY` accepted, same behavior), `DROP`, indexable, in `pg_matviews`. `WITH NO DATA` gives an empty view rather than PostgreSQL's error when read.
  - **Generated columns**: `GENERATED ALWAYS AS (expr) STORED`, MySQL `AS (expr)`, T-SQL `name AS expr`. `VIRTUAL` columns are stored too (same answers). Direct writes are refused with `428C9`.
  - **Functions and procedures** (`exec/routine.rs`, `parser/plpgsql.rs`): `LANGUAGE sql` and a PL/pgSQL subset: declarations, assignment, `IF`, `WHILE`, `LOOP`, `FOR` over integers and rows, `EXIT`, `CONTINUE`, `RETURN`, `RETURN NEXT`, `RETURN QUERY`, `PERFORM`, `RAISE`, `SELECT ... INTO`, `RETURNING ... INTO`, `EXCEPTION WHEN` with block rollback. `SETOF`, `TABLE(...)`, defaults, `CALL`, `EXECUTE` privilege, `pg_proc`.
    - A body can write only when it runs with the session itself: `SELECT f(...)` alone, `CALL`, a trigger. Called inside a larger query it can only read, and a write fails precisely (`0A000`).
    - Refused at `CREATE`: `EXECUTE`, `CASE`, cursors in bodies, `OUT` arguments, other languages, `SECURITY DEFINER` (it would run with the caller's rights here). PostgreSQL dialect only.
  - **Triggers**: `BEFORE` / `AFTER`, `INSERT` / `UPDATE` / `DELETE`, row and statement, `WHEN`, `TG_*`, `NEW` / `OLD`, `RETURN NULL` skipping the row, nesting limit 32, `pg_trigger`, `TRIGGER` privilege. PostgreSQL dialect only. Foreign-key cascades do not fire them. `INSTEAD OF`, `TRUNCATE`, `UPDATE OF` and transition tables are refused.
  - **`LISTEN` / `NOTIFY`** (`exec/notify.rs`): delivered on commit, identical ones collapsed, dropped on rollback, sent to an idle connection by a thread started on first `LISTEN`. Other gateways learn through polled documents (up to 100 ms, best effort).
  - **Types** (`nettypes.rs`): `inet`, `cidr`, `macaddr` with validation, `<<`, `>>`, `host`, `masklen`, `family`, `network`, `netmask`; `int4range`, `int8range`, `numrange`, `tsrange`, `tstzrange`, `daterange` with canonical forms, `@>`, `&&`, constructors, `isempty`, `lower_inc`, `upper_inc`, `lower_inf`, `upper_inf`. Bounds use `range_lower` / `range_upper`, since `lower` / `upper` on a plain text value mean text functions here. Operators on stored text recognize a range by its form.
  - **Exit Criteria (met)**: `tests/sql_surface.rs` (8 tests). Real clients: psql received an asynchronous notification from another session; psycopg2 `select()` woke on `NOTIFY` and its named cursor iterated; a trigger, a cursor and a temporary table over psql.
- [x] **TASK-150: BI & GUI Tool Suites (`F-ENT-06`)** — Metabase, DBeaver (JDBC metadata), pgAdmin 4, Grafana, Superset, Tableau / Power BI through psqlODBC; run headless where possible.
  - **How to run**: `modal run tools/modal/sweep.py --tools all` (or `--tools grafana,metabase`), which builds the gateway and runs `crates/storage-sql/tests/clients/tools/run.sh`: one gateway, all three protocols, no display. Final run: all three JDBC endpoints with 0 failures, 177 tool checks with 0 failures.
  - **What is the real program and what is not** (a stand-in is named as one):

    | Tool | What ran | Over |
    |---|---|---|
    | Metabase v0.51.3 | The program: setup, add database, sync, fields and foreign keys found, a built question, a native question | PostgreSQL, MySQL, SQL Server |
    | Grafana 11.3.0 | The program with a provisioned data source: health check, the query builder's table and column queries, a time series | PostgreSQL |
    | DBeaver | **Not the program**: every `DatabaseMetaData` call it browses with, through pgjdbc 42.7.4, mysql-connector-j 9.1.0, mssql-jdbc 12.8.1 | All three |
    | Superset | **Not the program**: SQLAlchemy's inspector, which is what it browses with, and the shape of query its charts send | PostgreSQL, MySQL |
    | Tableau, Power BI | **Not the programs**: the real psqlODBC driver with `UseDeclareFetch`, plus SQL in the style they generate (temporary filter tables, `DATE_TRUNC`, folded subqueries) | PostgreSQL |
    | pgAdmin 4 | **Not the program**: real psycopg 3 with its named cursor, plus catalog queries transcribed from its templates | PostgreSQL |

    A tool's own later versions may ask for more; these versions are what is claimed.
  - **Found by the real clients and fixed** (each has a regression test in `tests/sql_surface.rs` or `tests/control.rs`):
    - **`HAVING` with neither `GROUP BY` nor an aggregate was ignored**, returning rows it should have removed. Older correctness bug, found because Connector/J's `getTables` listed views as tables. It filters now, and MySQL's `HAVING` can name a select-list alias (an alias wins over a column of that name, as Connector/J needs).
    - **A cursor's rows were tagged `SELECT n`, not `FETCH n`**: psqlODBC looks for the tag, so every query it sent with `UseDeclareFetch` failed, without a server error.
    - SQL cursors are portals too: Describe and Close of one work (psycopg's named cursors).
    - `(composite).field` and `information_schema._pg_expandarray`, which pgjdbc's keys and indexes depend on. A composite value is carried as a JSON object keyed by field; there are no user-defined composite types.
    - Parenthesized joins (`((a JOIN b ON ..) JOIN c ON ..)`, around subqueries too): psqlODBC writes all its catalog joins this way.
    - Catalogs: `pg_get_viewdef` returned NULL and is real; `regclass` knows views; `pg_attribute` has index columns; `pg_constraint.conindid` names a foreign key's index; `indkey[n]` reads an int2vector zero-based; `max_index_keys`; `has_any_column_privilege`; `information_schema.table_privileges`, `column_privileges` and `routines` hold real rows (they were empty).
    - T-SQL: `sp_datatype_info[_100]`, `sp_special_columns[_100]`, `sp_stored_procedures`, `sp_table_privileges`, `sp_column_privileges`, `sp_describe_undeclared_parameters`, and a system procedure named without `EXEC`.
    - MySQL: `timediff`, `time_format` (Metabase's time zone probe).
  - **Test bugs fixed, not the database's**: Grafana's URL credentials, a wrong row count, `pyodbc.foreignKeys(table=)` asking for the wrong direction, and Connector/J's `getExportedKeys`. That last query joins every unique key of the referenced table, so `customers`, with a primary key and a unique email, gives two rows on MySQL too (read from the driver's source; not run against a MySQL server).
  - **Known limits**: `sp_describe_undeclared_parameters` suggests `nvarchar(4000)` for every parameter instead of inferring from the statement (values coerce where they are used, but a driver showing parameter types shows that one). `sp_sproc_columns` is refused. A view's privileges are listed by its name; only explicit grants appear in `information_schema` and `sp_*_privileges`, not an owner's implicit rights. `(j).field` also reads a JSON column's key, where PostgreSQL would refuse. HAVING over a plain column without `GROUP BY` is accepted in PostgreSQL and T-SQL too, where they would refuse it. The MySQL gateway under `--no-tls` refuses an SSL attempt: Metabase's MariaDB driver logs one failed try, then connects.
- [x] **TASK-151: Soak Under Faults (`F-ENT-07`)** — hours of mixed OLTP + analytics with fault injection; memory and descriptor ceilings checked throughout. *Reopened 2026-11-08 for memory; closed 2026-11-10. Both modes pass the 2-hour soak: range mode on 2026-11-08, single group on 2026-11-10 once arena segments were compacted automatically (below).*
  - **What runs**: `process_chaos::soak_range_cluster` and `soak_single_group` (`--ignored`). Three real server processes behind fault proxies; writes, reads, transfers, event batches, aggregates and scans run together while a fault is injected every few seconds (delay, half-open, partition, `SIGKILL`, `SIGSTOP`). Every window checks node RSS (ceiling 6 GiB) and descriptors (4,096), and the descriptor count late in the run against early in it. The run ends with a full read and a balance check. `STORINE_SOAK_SECS` and `STORINE_SOAK_WINDOW_SECS` set the length.
  - **Result, 2 hours per mode on Modal** (`modal run tools/modal/sweep.py --steady --suite-args "-p storage-server --test process_chaos soak -- --ignored --nocapture" --env STORINE_SOAK_SECS=7200,STORINE_SOAK_WINDOW_SECS=600`, commit `24b7414`): both pass, with balances conserved at 600.
    - Range cluster: 12 windows, RSS 2.6–4.5 GiB, descriptors at most 98–123 per window with no climb, 0 in `CLOSE_WAIT`. About 59,000 writes, 60,000 reads, 46,000 transfers and 44,000 event batches; 657 faults.
    - Single group: 12 windows, RSS 1.0–2.7 GiB, descriptors at most 77–194 per window. They stay flat, but 17–120 sockets sit in `CLOSE_WAIT` at each sample (see the limits below). About 1.8 million writes and 1.8 million reads; 660 faults.
  - **What the soak found, all fixed**:
    - A connection whose request was waiting on the worker pool was never read again, so a client that gave up left its socket behind. A single group under faults reached 913 descriptors in 2 hours. The reactor now sees end-of-stream while a job is pending and releases the connection.
    - A single-group leader cut off from a quorum stayed leader indefinitely, so requests queued behind it. It now steps down after twice the maximum election timeout without a quorum (check quorum), with a test. Lease reads also require a quorum acknowledgement within 0.9 of the minimum election timeout.
    - Reactors took the Raft state lock to route a request, and that lock is held across `fsync`. Under a paused follower, one slow disk write stalled every connection on the reactor, leaving 184 sockets in `CLOSE_WAIT`. Routing now uses `RaftNode::try_read_state` and never waits; a request that finds the lock busy is routed as a leader would route it, and the job itself checks again.
    - A probe test, `abandoned_requests_do_not_hold_descriptors`, runs 64 clients that hang up mid-request under each of delay, half-open and paused followers. Before the fixes the half-open peak was 3,965 descriptors; after them it is 125, 124 and 124.
  - **Test-harness bugs, not the database's**: chaos clusters took ports by binding port 0 and releasing it, so two clusters running in parallel could share a port and read each other's data. That showed up as "not linearizable" and "accounts unreadable". Ports are now unique per process, from 10,000–15,999, below gVisor's ephemeral range (16,000+), which caused "address already in use" on Modal. A node that exits at start now fails the test with its stderr. 40 of 40 `process_chaos` runs pass after the fix.
  - **Infrastructure**: a shared Cargo target volume on Modal gave stale artifacts (Cargo trusts mtimes). It was removed, and runs from before that were repeated. Preemptible containers restart a 2-hour run from zero, so the soak runs with `--steady` (non-preemptible, about 3× the price; this one cost about $3).
  - **Follow-up, 2026-11-08** (the limits the first pass left open):
    - **Single-group `CLOSE_WAIT`, cause found and fixed**: nodes handled `OP_RAFT_VOTE` / `APPEND` / `SNAPSHOT` on the reactor thread, and those calls take the Raft state lock, which is held across `fsync`. One slow sync stopped every connection on that reactor. Range mode only queues peer messages (`OP_RANGE_BATCH` → `deliver`), which is why it had none.
      - Raft RPCs now run on a worker pool of their own (`ServerShared::raft_pool`, 4 workers). It is kept apart from the write pool, so writes waiting for a quorum cannot delay the appends that form it.
      - Test: `test_raft_rpcs_waiting_on_the_lock_do_not_stall_other_connections`.
      - In the next soak: 0 sockets in `CLOSE_WAIT` (from 17–120), at most 74 descriptors (from 194), and single-group register writes more than doubled (357,000 in the first 10 minutes, from 154,000).
    - **Undecided keys, fixed**: `check_keyed` stopped at the first key it could not decide, so later keys went unchecked as well. `check_each_key` checks every key, and a violation is reported ahead of an undecided key (test `an_undecided_key_does_not_hide_a_violation_after_it`). Soak register keys are now fresh every 2 s instead of every window: one key had gathered about 50,000 operations, hundreds of them of unknown outcome. Any undecided key now fails the soak. Since then, 0 undecided of about 450–900 keys per window.
    - **Ports, fixed**: the allocator moved to `storage_types::testing::free_local_addr`, used by every TCP test (`cluster_tcp.rs`, `cluster_membership_tcp.rs`, `range_backup.rs`, `storage-sql/tests/range_cluster.rs`, `process_chaos.rs`).
    - **Verified**: workspace tests on Modal (66 binaries, 0 failures), workspace clippy, and 40 of 40 `process_chaos` runs.
  - **Why reopened: memory over the ceiling**. The soak prints each window's peak memory from `/proc/<pid>/smaps`, because gVisor reports no `RssAnon` / `RssFile`. It shows private memory, live mapped files, deleted mapped files, mappings, threads and the data's size on disk. Before the fixes below, range mode reached 6.3–6.9 GB of private memory within minutes on 61 MB of data, and single group reached 6.9 GB after about 50 minutes.
  - **Causes found and fixed, 2026-11-08**:
    - **Leader resend flood (range mode)**: the leader resent every unacknowledged entry on every tick, so a paused or cut-off follower got the same megabytes again and again, queued in memory on both sides. `Progress` now holds one window in flight at a time, at most 4 × `max_entries_per_append`. It is resent after `election_min_us`, and heartbeats in between carry no entries (`RaftNode::build_heartbeat`). Test `a_cut_off_follower_gets_a_bounded_window_and_catches_up`: at most 2,000 entries reach a cut-off follower across 600 proposals, against 35,447 before; it catches up after healing.
    - **The single-group WAL never shrank**: only range mode checkpointed, so the WAL grew with every byte written, and recovery reads it whole. Single-group nodes now checkpoint every `RaftConfig::checkpoint_interval` (10 s) when something was applied. Test `test_nodes_checkpoint_on_their_own_and_the_wal_stays_bounded`: 2 MB of writes leave each WAL under 256 KiB, where it reached 554,698 bytes before.
    - **Allocator retention (both modes)**: glibc kept freed memory. The live heap was about 100 MB while RSS was about 2 GB (`MALLOC_ARENA_MAX=2` did not change it). Servers now call `malloc_trim(0)` every 10 s from a thread of their own (`storage_mmap::release_free_heap`, the only new `unsafe`, in `storage-mmap`).
    - **A Raft safety hole the replication change exposed**: a learner with an empty log could vote for a candidate from a stale configuration (sim seed 262, election safety). Non-voters now grant no votes or pre-votes (`test_a_non_voter_grants_no_votes`). All three simulators pass at 300 seeds.
    - **Visibility**: range-mode `OP_HEALTH` reports ranges, log entries and bytes, pending requests, latches, timestamp-cache size, replies, queued in and out batch bytes, and dropped batches. A server built with `--features heap-stats` also reports the live and peak heap (`heap_live`, `heap_peak`; `storage_codec::peak_alloc::LiveAlloc`). The soak prints these with `STORINE_SOAK_VERBOSE=1` above `STORINE_SOAK_VERBOSE_MB` (default 2500).
  - **Measured, 2-hour soak in both modes (`--steady`, 12 windows of 10 minutes, about 660 faults per mode)**:
    - Range mode **passed**: peak RSS 757–1,546 MiB per window, against a 6 GiB ceiling, with no climb from first windows to last. Disk was 105–456 MiB, balances were conserved, every key was linearizable and none was undecided.
    - Single group stayed bounded too: peak RSS 563–1,864 MiB, ending at 669 MiB, with every key linearizable. Disk rose to 1,352 MiB and fell back to 350 MiB. **It then failed the final check**: no node answered all 60 balance reads within 60 s after healing. The soak now prints each node's last error and the end of its stderr when this happens.
    - **Single-group rerun (2026-11-09, `soak19`)**: the final read passed, balances were conserved at 600, and every key was linearizable. It **failed the memory check**: peak RSS rose from 1,808 MiB (windows 2–3) to 4,156 MiB (window 12). The growth was mapped segment files, not heap. Node 1 held 3.9 GB of arena segments (57 files) for about 440,000 live records; its heap stayed at 450–1,100 MiB. The earlier final-read failure did not come back, so its cause is still unknown.
    - **Arena segments were never compacted on their own (fixed 2026-11-10)**:
      - Dead space was reclaimed only by `OP_ADMIN_COMPACT`, or by in-place slot reuse, which needs the same size class and no reader holding the mapping.
      - Each segment's dead-byte count lived only in memory and started at 0 on every open. After a restart, the space freed before it was never counted, so even a manual compaction skipped it. Node 1 was killed many times in the last 40 minutes.
      - Opening the engine now recounts each segment's dead bytes from what the index references (`ArenaManager::recount_dead_bytes`).
      - `StorageEngine::checkpoint` (the single-group checkpoint loop and `OP_ADMIN_CHECKPOINT`) now ends with `maybe_compact_arenas`. That rewrites every segment at least half dead once dead space reaches 64 MiB (`AUTO_COMPACT_MIN_DEAD_BYTES`) and half of all segment bytes.
      - Compaction also deleted old segment files before syncing the new frames and saving the checkpoint. A crash in between would have left a checkpoint pointing at deleted files. The files are now deleted last.
      - Test `test_dead_arena_space_survives_restart_and_is_compacted_automatically`: 6 rounds of 100 records in growing size classes, then a restart. It fails without the recount ("dead space from before the restart was not compacted"). With it, more than half the segment bytes are reclaimed and every record reads back after a further restart.
    - **Single-group rerun with the fix (2026-11-10, `soak20`, commit `3604c24`) passed**:
      - It ran 12 windows of 10 minutes with 661 faults.
      - Peak RSS per window was 484–1,270 MiB against a 6 GiB ceiling, and the last window was 973 MiB.
      - Disk was 121–378 MiB, against up to 3.9 GB in `soak19`.
      - At most 71 descriptors were open.
      - Balances were conserved at 600, and every key was linearizable (867–908 keys per window, none undecided).
      - It made 7.30 million writes, against 9.22 million in `soak19`. Compaction may cost some throughput, or it may be run-to-run variance; this was not measured further.
      - 487,695 operations were refused or timed out during faults, against 470,677 in `soak19`.
  - **Known limits**:
    - Arena data on disk can reach about twice the live data plus 64 MiB before automatic compaction starts, and a compaction rewrites whole segments (64 MiB each). The soak checks no ceiling on disk. Range mode stores records in their own files (`DedicatedFiles`, rewritten in place), not arena segments, so it has no dead slots to compact. Its disk stayed at 105–456 MiB in the 2-hour soak.
    - A node that restarts peaks at about 2–4× its data while recovering or catching up: one range node opening about 425 MB reached a heap of 1.0–1.6 GB. WAL recovery still reads the WAL whole; checkpoint truncation no longer does (below).
  - **Follow-ups (2026-11-10)**:
    - **Compaction costs no measurable throughput.** `soak20` made 7.30 million writes against 9.22 million in `soak19`. The new benchmark `cargo run --release -p storage-benches --bin compaction-cost` runs three modes, alternating: no checkpoint, the old `flush_all` checkpoint, and `checkpoint` with compaction. It ran 2 pairs of 60 s each, with 4 writers over 200,000 records, a 10 s checkpoint and a memory-first WAL, on a 2-core arm64 machine. Writes per second were 115,409 / 121,372 (none), 113,777 / 124,710 (flush) and 143,260 / 108,632 (compact): the spread between runs is larger than any difference between modes. Disk ended at 1,344–1,472 MiB without compaction and 128–160 MiB with it. The soak gap is run-to-run variance.
    - **Checkpoints stalled writes for seconds (fixed).** The benchmark showed writes waiting 5–9 s behind each checkpoint in both checkpointing modes, against at most 0.08–0.14 s without checkpoints. The cause was `GroupCommitWal::checkpoint_truncate_before`: it read, filtered, rewrote and fsynced the whole WAL while holding the WAL's I/O lock, and at about 50,000 writes/s the WAL held hundreds of MB between checkpoints. Now:
      - It notes how far the WAL reaches, then streams that prefix frame by frame into `<wal>.next` without the lock, with one frame in memory at a time instead of the whole file decoded.
      - It copies what was appended meanwhile in up to 16 unlocked rounds, syncing each.
      - It takes the lock only to copy the last piece (under 1 MiB, or whatever accumulated in the last round) and swap the files.
      - The old file is closed after the lock is released, because freeing a large file's blocks took 100–270 ms inside it.
      - Frames appended during the truncation all postdate the cutoff, or are frames the old rewrite also kept, so they are copied unfiltered.
      - With this, the worst write in the benchmark was 0.10–0.30 s in all three modes, and the locked phase took about 1 ms with no tail.
      - Test `test_truncation_keeps_every_frame_appended_while_it_runs`: four writers append 3 KiB frames through repeated truncations. It fails if the final copy is skipped (frames lost) and passes with it.
    - **The `soak17` final-read failure is still unexplained.** It did not come back in six single-group runs after it: `soak19` and `soak20` (2 h each), and four 30-minute probes (`readprobe5`–`8`). Every one of them read all balances after healing, conserved them and found every key linearizable. `soak17` ran before the soak reported each node's last error and stderr, so there is nothing left to go on. The reporting stays in place for the next time it happens. The fault thread is joined before the final read, and every fault undoes itself (killed nodes are respawned, stopped ones resumed), so the harness does not leave a node down.
    - **Segment growth in one probe (`readprobe7`), investigated**:
      - In one 30-minute probe, the peak node's disk grew 73 → 632 → 1,033 MiB over its three windows, and its open segment files 2 → 32 → 86. That is about one new segment per 11 s, the checkpoint interval. The other probes stayed at 73–155 MiB.
      - New visibility:
        - `OP_HEALTH` (single group) reports `"arena"`: segments, used, dead and live bytes (live from the index), compactions, segments removed, compaction errors and the last error. Prometheus has the same as `storine_arena_*`.
        - A verbose soak prints each node's arena every window.
      - The instrumented probes did not reproduce the growth. Four ran 30 minutes in full (`diskprobe9`–`12`). Two more (`diskprobe13`, `14`) ran 20 minutes with the fix below, before the Modal spend limit stopped them. All showed 1–2 segments per node, disk at most 182 MiB, and no compaction errors. The dead-byte accounting was exact: used bytes equal live plus dead, to the byte, in every sample.
      - Found in the code, and matching the pattern: one record that could not be moved aborted the whole compaction. The compaction had already rolled over to a new segment, so each checkpoint added a segment, removed none, and the dead space kept the trigger firing. The single-group checkpoint loop only checks `is_ok()`, so this was invisible.
      - **Fixed**:
        - A record that cannot be moved keeps its segment and is counted and reported (`arena_compaction_errors`, `last_compaction_error`). Everything else is still compacted.
        - The active segment is rolled over only when it is itself being compacted.
        - `reseal_all` still fails if any record cannot be moved, since a copy left behind would defeat the reseal.
        - Test `test_compaction_moves_everything_but_an_unreadable_record`: one record's frame is corrupted on disk. Before, every compaction failed (`ChecksumMismatch`). Now the other 99 records move, five retries add no segment, and the old segment is freed once the record is rewritten.
      - **Not confirmed**: `readprobe7` ran before the error counter existed, so whether a record failed to move there, and why, is not known. A corrupt live frame would itself be a durability bug, and none was found. If the growth comes back, health now shows the error.
    - The soak runs on one Modal machine with local disks. It does not cover real networks or disks that fail.
- [x] **TASK-152: Final Audit (Stages 28–31)** — full-project review for gaps, bugs and vulnerabilities; docs brought in line with the code.
  - **How**: three independent read-only reviews (the distributed core; SQL access control and execution; wire protocols and parsers), each finding read against the code before it was fixed, with a regression test where one can be written. A review can miss things: `dist/alloc.rs`, `dist/zone.rs`, the simulators and part of `dist/node.rs` were not read in full, and the fixes for the wire limits, optimizer and Raft were tested, not re-reviewed.
  - **Rules**: `unsafe` moved out of `storage-sql` into `storage-codec` behind safe APIs (`aes_ni`, `peak_alloc`, `RULE-SAFE-01`). Left as is: `benches/` (a measuring tool, never linked into the server). 4,446 vendored Prisma files left the git index (still in history, so the repository does not shrink).
  - **Could take down the server** (release builds abort on panic):
    - PL/pgSQL nesting overflowed the stack before any check. Now under the parser's depth limit, and bodies nested in bodies are limited too.
    - Expressions the parser allowed overflowed the evaluator's 2 MiB stack. Connection threads have 16 MiB. An embedded caller's own thread still has what it has.
    - `SET statement_timeout` with an absurd value panicked at the next statement. It is refused.
    - A cycle through views and routines multiplied two depth limits. They now share one (64).
    - `thread::spawn` panicked when threads ran out, and a full descriptor table spun the accept loop. Both are refused with a logged line (`listener.rs`).
    - Exponential backtracking in `FROM ((((..` (a 1 KiB statement could never finish). Now one linear look-ahead.
  - **Before login**: a client that says nothing is closed after 30 s, on all three protocols; connections are capped (`--max-connections`, default 1024); MySQL no longer commits 16 MiB for a 4-byte header; messages before login are capped at 1 MiB (MySQL, TDS).
  - **Authentication**:
    - A range cluster could not authenticate at all: `OP_AUTH` was refused, so any cluster with tokens failed closed. It works, with a test.
    - Kerberos read the realm from the unencrypted part of the ticket: a cross-realm user could become a local one of the same name. It now uses the realm of the key that decrypted the ticket.
    - `--oidc-*` or `--kerberos-keytab` without `--user` left every client trusted. It now refuses to start.
    - SCRAM told real names from unknown ones by whether the salt changed, and did a key derivation per `--user` connection. Salts are now fixed per name and verifiers derived once. Salts, nonces and secrets come from the OS generator instead of SipHash.
    - TDS still does one key derivation for a password check on a catalog role.
  - **Privilege escalation**:
    - `GRANT` on a temporary table wrote a by-name grant that applied to the real table of that name once the temporary one was gone: any role could reach any table. Refused.
    - `CREATEROLE` could join `sysadmin`, or reset the password of a role that is a superuser through membership. Refused.
    - A grant option on one column could be widened to the whole table. Now limited to its columns.
  - **Row security**:
    - A temporary table could stand in for a table a policy or a view reads, returning every row. Policies, views and materialized view refreshes see no temporary tables now.
    - `pg_stats` showed values hidden by row security. Now hidden for a role under a policy.
    - `WITH CHECK` ran before `BEFORE` triggers, so a trigger could store a row the policy forbids. It now runs after them.
    - `REFRESH MATERIALIZED VIEW` ran with the refresher's rights. It now runs as the owner.
    - A policy with a subquery stored as the text "(subquery)" and failed when read back, so such policies never worked. The source text is stored now. A subquery in `DEFAULT` or `CHECK` is refused for the same reason.
    - Pooled connections kept temporary tables, variables and cursors after `sp_reset_connection` and `COM_RESET_CONNECTION`, and `DISCARD TEMP` did nothing. All now share one reset.
    - `REVOKE EXECUTE ON ALL FUNCTIONS` did nothing. It now covers every function.
  - **Wrong answers**:
    - `EXISTS (SELECT count(*) ..)` was false where it must be true: an aggregate always gives a row.
    - A correlated aggregate argument failed under the optimizer.
    - Reordering joins could rebind an unqualified `ON` column to another table.
    - Subquery caches keyed by a freed node's address could answer with another query's result.
    - Equal float and numeric values hashed apart, so hash joins lost matches.
    - `COMMIT` of a failed transaction delivered its `NOTIFY`, and `ROLLBACK TO` kept them.
    - `..` inside a PL/pgSQL string was rewritten.
  - **Limits**: rows a routine returns count against `max_query_memory`. Too many parameters or more than 4,096 result columns are refused instead of wrapping a wire count. A TDS error message is bounded so it cannot overflow its token's length and be read as forged tokens. COPY stops buffering after a failure and takes records in linear time. MySQL refuses `1; DROP TABLE t` from a client that did not ask for `CLIENT_MULTI_STATEMENTS`.
  - **Distributed correctness**:
    - A partitioned leader answered writes that propose nothing (a delete of a key it thought absent, a failed check) from stale state. They now confirm by read index first.
    - Range leaders never stepped down. They do now, once they cannot hear a quorum for twice the election timeout, so requests get an answer instead of waiting.
    - A read restarted above its uncertainty limit passed intents below it and could see half a transaction.
    - A failed follower log append stayed in memory and its retry acknowledged it; a restart then failed. The next write now rewrites the log.
    - A crash during a snapshot install could keep an old log branch above the snapshot.
    - Read errors during scans silently dropped rows, including from backups, which still reported success. They are errors now.
    - A latch stayed held forever if its replica was destroyed or its leader deposed.
    - Collecting a tombstone raised the history bound past it, so re-creating a dropped collection broke all-collection feeds.
  - **Distributed resources**:
    - Coordinators and `OP_KV` shared a worker pool, which collapsed under load. They are separate now.
    - The Raft log in memory was limited by entries only (tens of gigabytes with large documents). It also compacts by bytes now (64 MiB, keeping 16 MiB).
    - Peer batches and pending requests were unbounded. Batches are now capped at 1 GiB per node (Raft resends what is dropped), and callers that have gone are forgotten after a minute.
    - Scans could starve under steady writes. Writes arriving after a waiting scan now wait behind it. That is a real cost: a write can wait for a scan on its span.
    - Range mode now has the limit on concurrent analytics that the other modes had.
  - **A review of these fixes found more, all fixed**: reading a temporary table and the real table of its name in one statement shared a cached access decision (introduced by the fix above), so a view could read a table its owner has no rights on. `GRANT`, `DROP ROLE` and `RENAME` missed superusers by membership. A crash between emptying the log and saving the hard state stopped a node from opening. Failed leader appends left the log file ahead of memory. Valid `FROM` forms were refused. Stepping down did not answer writes already proposed. The review also found that TDS's reset bit in a packet's status, which pooled clients use, was ignored: it now resets as `sp_reset_connection` does.
  - **Known limits**: a view, or a function called from a view or policy, cannot read a temporary table: it fails with `42P01`, or reads a real table of that name. An embedded `Session` on a small thread stack can still overflow on the tallest expressions the parser allows. Non-superuser `CREATEROLE` holders can still manage ordinary roles' passwords (by design). Check quorum uses any message from a peer as contact, not per-range acknowledgement, so a node reachable for other ranges does not step down for this one until its peers' replies stop.

## Stage 32: Write Path Speed (`TASK-153`)

- [ ] **TASK-153: Faster Than LMDB and RocksDB in Every Case** — every throughput and latency row of `benches/kv-compare` at equal durability, without weakening recovery or adding runtime crates.
  - **Done (2026-11-12)**: pooled frames, batched arena flushes, a staging buffer for the memory-first WAL, adaptive group-commit linger, WAL preallocation, one lookup per `put`. Storine now leads every throughput row and every p99 row (`BENCHMARK_REPORT.md` section 8).
  - **Done (2026-11-12, tail)**: spinning hot locks, capacity kept across staging and flush-queue swaps, appenders write the WAL staging buffer only past 4 MiB. The 4-thread overwrite p99.9 fell from 252–288 µs to 54–103 µs, and the p99 from 14–20 µs to 5–6 µs (section 8.1).
  - **Done (2026-11-12, traced tails)**: chunked WAL staging, a flush queue pre-grown outside its lock, and the harness's `sync` before durable phases. The 4-writer overwrite p99.9 is 25.5–27.5 µs against RocksDB's 54–66 µs (section 8.2). Two `fsync`s in flight was measured slower on gp3/ext4 and is set to one.
  - **Tried and dropped (2026-11-12)**: incrementally growing hash maps for the primary index and the flush queue, and WAL zeros written by a background direct-I/O job. Each cost more than it saved (section 8.3). The flush queue keeps its ids in 512-id chunks.
  - **Done (2026-11-12, other gaps)** (section 8.4):
    - The TCP `GET` round trip is back to 8.5 µs from 19.5 µs: reactors poll for 50 µs after work before sleeping.
    - ARM uses hardware CRC32C (on-disk format unchanged); one-writer overwrites there went from 311 k to 580–615 k per second.
    - Indexed records apply their index changes under one lock per index: 4-writer overwrite +32 %, p99 2.5× lower, worse p99.9/p99.99.
    - Hot `with_ref` (58 ns) and `update` (855–870 ns) re-measured: mostly recovered from the earlier regression.
  - **Open**:
    - Fill p99.99: 0.74 ms against RocksDB's 16–20 µs. Whole-shard rehashes; a capacity hint at open is the option left.
    - 4-writer overwrite p99.99: 2.3–3.0 ms against 0.46–1.32 ms; not traced.
    - Durable p99.9 and p99.99 against LMDB: four writers 6.0–6.4 ms against 2.9–3.2 ms (LMDB starves three writers for over 4 s); one writer p99.99 6.3–11.5 ms against 3.4–4.4 ms.
    - Indexed records: the remaining cost is the ordered index (`BTreeMap`) itself.
    - Not investigated in this task (`soak17`'s final-read failure: found and fixed in `TASK-154`): `soak17`'s final-read failure and `readprobe7`'s arena growth (from `TASK-151`), the arena's space amplification of up to 2× until compaction, and whether 64 MiB is the right default dirty budget.

## Stage 33: Production Release (`TASK-154`)

- [x] **TASK-154: Ready for Production and Release** — what a 0.1.0 release needs: encrypted client and cluster traffic with no cost when off, recovery in bounded memory, a release checklist run on the release commit, release notes and a compatibility policy.
  - **Done (2026-11-12)**:
    - TLS 1.3 on the native port and between cluster nodes, in a new zero-dependency crate `storage-tls`. It has a client with name, validity and chain checks, a sans-IO session the reactors drive, and a handshake pool with a deadline. `gen-ca` and `gen-cert` issue Ed25519 certificates. All eight SDKs take `storines://` URLs and have a TLS test.
    - WAL recovery streams the log in bounded batches instead of reading it whole (`GroupCommitWal::for_each_committed`).
    - The frame pool's shared store is capped at 64 MiB in total.
    - Release checklist (`docs/RELEASE.md`), CI on x86_64 and arm64 (`.github/workflows/ci.yml`), `CHANGELOG.md`, `docs/COMPATIBILITY.md`.
    - Build output that had been committed by mistake (`target-fp/`) is no longer tracked, and `target*/` is ignored.
  - **Found and fixed (2026-11-12), the range soak's final-read failure (also `soak17`'s)**:
    - A range node killed during the soak never came back, so it refused connections. It was stuck single-threaded in recovery for minutes, one `fdatasync` + rename + directory `fsync` per logged write.
    - Range nodes opened their engine with `EngineConfig::default()`, whose layout is one file per record, where `DocumentStore` uses arenas. One node held 20,155 record files and an 83 MB log.
    - Killed again about every 3 minutes, before recovery finished, it started over each time.
    - Fix: range nodes use `DOCUMENT_LAYOUT`. Replay applies the log in batches of up to 4,096 frames or 32 MiB and writes each record once per batch, syncing record files from up to 16 threads.
    - That node's data, copied from the soak machine, reopens in 2.3 s.
  - **Found and fixed (2026-11-13), two more from the range soak on the fixed tree**:
    - Range nodes never checkpointed their engine. `NodeDriver::checkpoint` only syncs the WAL, so the WAL and the arena grew until the next restart: 374 MB of WAL and 3.2 GB of segments per node after 25 minutes. Before the layout fix, frequent restarts and in-place record files had hidden this. `RangeNode` now runs a checkpoint thread (`RuntimeConfig::checkpoint_interval`, 5 s) that flushes, cuts the WAL and compacts while the engine is written. In a local 15-minute range soak, WALs stayed at 1–3 MB, segments at 41–104 MB and RSS under 300 MB per node. New test: `range_nodes_checkpoint_and_cut_their_logs` (logs reach 974 KB without the thread).
    - Under `AsyncMemoryFirst`, `flush_all` truncated the WAL without syncing what it had flushed. Raft nodes (single group and ranges) sync the WAL, checkpoint, then compact their Raft logs, so a power loss could have lost applied writes. Now, whenever a WAL is cut, record files are synced as they are flushed and the arena is synced before the cut.
  - **Verified (2026-11-13), on the release candidate**: the release workspace suite on x86_64 and arm64, 40 of 40 process-chaos runs on each, and both 2-hour soaks. In each soak balances were conserved and every key linearizable. The single-group RSS peak was 334 MiB; the range RSS peak was 1.12 GiB, level after the first hour; disk stayed under 450 MiB. Checks 6–8 ran on the tree before the two checkpoint fixes, which don't touch the paths they cover.
  - **Resolved (2026-11-13), binary size**: `storage-server` 3.87 MB, `storage-cli` 1.25 MB, `storine-sql` 5.58 MB (Linux x86_64, stripped), above `RULE-MEM-03`'s `< 1 MiB`. They were already 3.74, 1.19 and 5.51 MB before this task; TLS added 66–132 KB. The user chose to raise the budget rather than split subsystems into Cargo features. The new ceilings are `storage-server` `< 4.5 MiB`, `storage-cli` `< 1.5 MiB` and `storine-sql` `< 6.5 MiB`, about 20 % above today's sizes. `docs/RELEASE.md` check 9 measures them.

## Stage 34: Binaries for Every Platform and a Web Console (`TASK-155`)

- [x] **TASK-155: Release Binaries and `storine-console` (0.1.1)** — downloadable, checked binaries for Linux, macOS and Windows with no loss of speed against a build for the exact CPU, and a browser console for operating a server or cluster.
  - **Portable builds**:
    - Release builds target the architecture's baseline (`x86-64`, generic ARMv8; Rust's defaults on macOS) instead of `native`. Measured against native on `c7i.xlarge` and `c7g.xlarge` (`tools/release/compare-cpu-targets.sh`, five alternating rounds, `BENCHMARK_REPORT.md` section 8.6).
    - Found: vector scoring used `f32::mul_add`, which without FMA in the target is a call to libm's `fmaf`. Cosine similarity on 64 dimensions went from 22 to 449 ns. The kernels moved to `storage_codec::vector` with an AVX2 + FMA copy chosen at run time; results are bit-identical either way.
    - Found: the bench runner's checksum input was not behind `black_box`, so native builds partly hoisted the loop (310 ns where a call takes about 1.6 µs on Graviton2).
    - A startup check for missing CPU features was tried and dropped: with fat LTO the standard library's start-up code is compiled for the target CPU too, so an unsupported binary dies before `main`. Baseline targets make the check unnecessary.
  - **Platforms**: a `kqueue` reactor for macOS (`KqueuePoller`); `poll`'s `nfds_t` is `unsigned int` there. CI runs clippy and the whole test suite on Linux x86_64 and arm64, macOS arm64 and Windows x86_64.
  - **Release pipeline**: `tools/release/build-dist.sh` builds, checks (size ceilings; glibc 2.28 and libc-only linking on Linux; system-only dylibs on macOS), smoke-tests (certificates, a server, a health check, the console) and packages each platform; `.github/workflows/release.yml` runs it for each `v*` tag on GitHub's runners (Linux in `rockylinux:8`) and attaches the archives and `SHA256SUMS`.
  - **`storine-console`** (`crates/storage-console`): a hand-written HTTP/1.1 server and JSON API over the Rust SDK, and a single-page app with no libraries. It covers the cluster and health overview, collections, documents (filter, sort, keyset or offset pages, a JSON editor with version checks), grouped aggregates, indexes and dropping a collection. Values keep their types through the editor (`$decimal`, `$time`, `$bytes`, `$uuid`, `$id`, `$int` above 2^53, `1.0` for floats).
    - Security: loopback by default; a per-run key in `X-Console-Key` on every API call (which also forces a CORS preflight that is never granted); `Host` must name the console (DNS rebinding); a strict CSP with no inline script or style; no caching or framing.
    - Tests: JSON and value mapping, HTTP parsing, filters, and an end-to-end test against a real `storage-server`. Checked in headless Chromium: every view, light and dark, 390 px wide, no console errors.
  - Rust SDK 0.1.1: `Collection::page`, `Client::health`.
  - **Found and fixed**: a standalone server never cut its WAL. Only Raft and range nodes checkpointed on a timer, so a standalone server's log grew with every write until a restart replayed all of it. The server's maintenance thread now checkpoints a standalone engine every 10 s while it is written. Test: `a_standalone_server_cuts_its_wal`.
  - **Found and fixed (first Windows run)**: every durable write failed on Windows: the WAL fsynced through a handle opened read-only, which `FlushFileBuffers` refuses (Linux and macOS sync any descriptor). The sync handle is now read-write, and append-mode files are opened for writing on Windows with each write moved to the end (`storage_types::vfs`).
  - **Second Windows run**: two timing-bound tests failed on the slower runner: a columnar test expected 100 transfers in about 2 s (89 happened), and the TLS cluster test's first write returned `CommitUnknown` after its 3 s commit timeout. The first now runs until enough transfers happened (up to 2 minutes); the second retries `CommitUnknown` with the same request id, as every client does.
  - **Found (first macOS run)**: two tests assumed Linux: one counts descriptors in `/proc` (now Linux-only), and one set a socket option on a connection the server had already reset, which macOS refuses with `EINVAL` (now read as closed).
  - **Sizes as shipped** (release build, Linux x86_64): `storage-server` 4,631,336 B, `storage-cli` 1,371,920 B, `storine-sql` 6,676,624 B, `storine-console` 759,088 B, about 2 % under the 4.5 / 6.5 MiB ceilings. The user had said to raise the ceilings if needed: they are now 5.5, 1.75, 8 and 1 MiB.
  - **Found and fixed**: CI's size check measured the runner's `native` build (AVX-512), 25 % larger than what ships (`storage-server` 4.84 MB against 3.74 MB). It now builds for `x86-64`, as the release does.
  - **Docs**: `README.md` is now a short introduction; `docs/TECHNICAL.md` is a new technical reference written from the code (an inventory of every flag, variable, default and limit). Corrected stale statements in `CHANGELOG.md`, `protocol/PROTOCOL.md` (TLS 1.2, body limits per opcode, change feeds), the SQL gateway's README (functions, procedures, triggers, cursors, temporary tables, materialized views and `LISTEN` / `NOTIFY` exist) and the Python SDK's README (16 indexes, not 4); `docs/FEATURES.md` now says it is the original specification.

## Stage 35: Encryption, Point-in-Time Recovery and Placement in the Server (`TASK-156`)

- [x] **TASK-156: Server Access to Encryption at Rest, PITR and Multi-Region Placement** — the three existed in the engine and library only; the server binary had no setting for any of them.
  - **Encryption at rest** (`storage_server::at_rest`): a key file (`gen-key`; refused if others can read it), the active key, and a plaintext allowance for the conversion. Every start reseals: `StorageEngine::reseal_all` plus `RaftNode::reseal_storage`, which rewrites the Raft log and drops a snapshot written under older settings. Range nodes reseal before serving; others in the background, switching to strict once done.
    - **Found**: Raft logs and snapshots stored document contents in plaintext even with the engine encrypted. Log frames are now sealed (bound to the log's file name) and snapshot records too (bound to their position), with the engine's live settings.
    - Tests: `test_encrypted_raft_logs_and_snapshots_hold_no_plaintext` (plaintext before, none after; a sealed snapshot installed on a lagging follower; a forged plaintext frame refused); `tests/at_rest.rs` (the binary turns encryption on for existing data, rotates and retires the old key, refuses the wrong key; a range cluster leaves no plaintext in any file).
  - **Point-in-time recovery**: `STORAGE_WAL_ARCHIVE` (`EngineConfig::wal_archive_dir`, another disk preferred) with hourly pruning past `STORAGE_WAL_ARCHIVE_RETAIN`. `restore-pitr` restores document data (detected from the backup manifest), reads archive directories and WAL files (`restore_point_in_time_with`), and takes RFC 3339 times. Refused on range clusters. Test (`storage-cli/tests/pitr.rs`, encryption on): backup, writes on both sides of a moment, a crash; the restore as of the moment holds exactly the writes before it, and one with the leftover WAL holds everything.
  - **Placement** (`storage_server::placement`): localities in `--peers`, rules in `--zones` (parsed into `ZoneConfigs`, checked against the localities), and each node's ranges in its health report (`NodeDriver::placement_json`, published by the event loop). Test (`tests/placement.rs`): six nodes in two regions; an eu-only collection's replicas and leases end in eu and a us-preferred collection's leases in us, within seconds. Without the eu rule, the same check fails.

