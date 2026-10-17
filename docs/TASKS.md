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

