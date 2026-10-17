# End-to-End Tasks Order: Commencement to Completion (`TASKS.md`)

This document defines the strict chronological execution order for building the entire storage engine from an empty directory to a production-hardened, benchmark-verified system. Each stage must pass all exit criteria before advancing to the next.

---

## Stage 0: Workspace Foundation, Compiler Tuning & Benchmark Harness

- [x] **TASK-001: Initialize Multi-Crate Cargo Workspace & Optimization Profiles**
  - **Deliverables**: Root `Cargo.toml`, `.cargo/config.toml`, and crate skeletons (`storage-types`, `storage-codec`, `storage-mmap`, `storage-wal`, `storage-index`, `storage-graph`, `storage-core`, `storage-cli`, `storage-server`, `benches`).
  - **Optimizations**: Configure `opt-level = 3`, `lto = "fat"`, `codegen-units = 1`, `panic = "abort"`, `mimalloc` global allocator, and workspace lints (`deny(unsafe_op_in_unsafe_fn)`, `clippy::perf`).
  - **Exit Criteria**: `cargo check --workspace --all-targets` succeeds with zero warnings.

- [x] **TASK-002: Build Allocation-Counting & Latency Benchmark Harness**
  - **Deliverables**: `benches/harness.rs` with a custom `TrackingAllocator` wrapper (counting allocations/bytes per thread) and latency percentile recorder (`p50`, `p95`, `p99`, `p99.9`).
  - **Optimizations**: Thread-local atomic-free counter increments on `alloc`/`dealloc` so measurement overhead is `< 2 ns`.
  - **Exit Criteria**: Harness accurately detects a single 1-byte heap allocation in a test closure and fails `assert_zero_alloc!`.

---

## Stage 1: Core Zero-Allocation Types & Zero-Copy `rkyv` Codec

- [x] **TASK-003: Implement `InlineId` (24-Byte Stack-Inlined Primary Key)**
  - **Deliverables**: `crates/storage-types/src/id.rs`.
  - **Optimizations**: `#[repr(C, align(8))]` 24-byte union storing keys `<= 23` bytes inline on the stack with length tag, falling back to `Arc<str>` only for longer keys; implement `rkyv::{Archive, Serialize, Deserialize}`, `Hash` (via `XXH3`), `Ord`, and `Copy`/`Clone` fast paths.
  - **Exit Criteria**: `size_of::<InlineId>() == 24`, zero heap allocations for keys `<= 23` bytes, and Criterion lookup benchmark `< 5 ns`.

- [x] **TASK-004: Implement 64-Byte Cache-Line-Aligned `RecordHeader` & Flags**
  - **Deliverables**: `crates/storage-types/src/header.rs`.
  - **Optimizations**: `#[repr(C, align(64))]` fixed 64-byte header (`magic`, `format_version`, `schema_version`, `record_version`, `updated_at_ns`, `checksum`, `payload_len`, `root_offset`, `flags`). Zero-cost cast via `bytemuck::Pod` / `Zeroable`.
  - **Exit Criteria**: Compile-time `assert_eq!(size_of::<RecordHeader>(), 64)` and `align_of::<RecordHeader>() == 64`.

- [x] **TASK-005: Implement SIMD Checksums (`XXH3-64` & Hardware `CRC32C`)**
  - **Deliverables**: `crates/storage-codec/src/checksum.rs`.
  - **Optimizations**: Use `xxhash-rust` (`xxh3_64`) and `crc32c` with runtime/compile-time AVX2/SSE4.2/NEON dispatch for `> 15 GB/s` hashing throughput.
  - **Exit Criteria**: Unit tests for corruption detection (single bit-flip detection) + throughput benchmark.

- [x] **TASK-006: Implement Zero-Copy `rkyv` Record Codec & Pooled Scratch Buffers**
  - **Deliverables**: `crates/storage-codec/src/lib.rs`, `crates/storage-codec/src/scratch.rs`.
  - **Optimizations**: Thread-local 64-byte-aligned `AlignedBufferPool` that encodes `[RecordHeader (64B) | rkyv Payload]` in a single contiguous buffer with zero heap allocations after initial warmup. Provide `decode_verified` (once-per-mmap `bytecheck`) and `decode_unchecked` (1-cycle hot read).
  - **Exit Criteria**: Round-trip encode + zero-copy read benchmark shows `0` allocations on read and `< 15 ns` access latency.

---

## Stage 2: Cross-Platform `mmap` Engine, Bounded Pool & Disk Layout

- [x] **TASK-007: Implement Cross-Platform `MmapFile` & OS Virtual Memory Hints**
  - **Deliverables**: `crates/storage-mmap/src/raw.rs` (`memmap2` + native Windows `PrefetchVirtualMemory` / Unix `madvise` wrappers).
  - **Optimizations**: Support read-only shared maps, pre-allocated writable ping-pong slots, and explicit kernel page-cache hints (`WillNeed`, `Random`, `Sequential`, `DontNeed`).
  - **Exit Criteria**: Cross-platform tests passing on Windows and Unix with `// SAFETY:` proofs documented on every `unsafe` block.

- [x] **TASK-008: Implement `MmapPool` with W-TinyLFU / Clock Eviction & Reader Guards**
  - **Deliverables**: `crates/storage-mmap/src/pool.rs`, `crates/storage-mmap/src/guard.rs`.
  - **Optimizations**: 64-shard table tracking active `MmapSlot`s with atomic reader refcounts (`active_readers`) and Clock/frequency bits. When `active_mmaps >= max_open_mmaps`, evict and unmap cold slots where `active_readers == 0` without blocking hot readers.
  - **Exit Criteria**: Stress test mapping 50,000 files with `max_open_mmaps = 1,024` keeps open OS handles strictly `<= 1,024` with zero use-after-unmap bugs under 16 reader threads.

- [x] **TASK-009: Implement Sharded & Hierarchical Path Resolver (Zero-Alloc Stack Paths)**
  - **Deliverables**: `crates/storage-mmap/src/path.rs`.
  - **Optimizations**: Format `<base>/<collection>/<hierarchy>/<shard_hi>/<shard_lo>/<id>.rec` directly into a stack-allocated `[u8; 256]` buffer without allocating `String` or `PathBuf` on the heap.
  - **Exit Criteria**: `assert_zero_alloc!` passes during path resolution; supports both flat sharded collections and nested parent-child folder hierarchies.

- [x] **TASK-010: Implement Atomic Disk Persistence (Temp+Rename & Ping-Pong Double Slot)**
  - **Deliverables**: `crates/storage-mmap/src/persist.rs`.
  - **Optimizations**:
    1. **Atomic Replace**: Write to pooled `.tmp` file, flush data, and atomic rename (`MoveFileExW` on Windows / `rename` on POSIX).
    2. **Ping-Pong Double-Slot File**: Pre-allocate `[Slot 0 | Slot 1]` inside a single `.rec` file for hot records; write new version to inactive slot and atomically flip the active slot indicator, eliminating directory entry metadata churn.
  - **Exit Criteria**: Simulated mid-write crash leaves previous version 100% intact and readable.

---

## Stage 3: Primary Index, Per-Record Concurrency, Write Coalescing & WAL

- [x] **TASK-011: Implement Sharded SwissTable Primary Index & `HotDirtyArc` State**
  - **Deliverables**: `crates/storage-index/src/primary.rs`.
  - **Optimizations**: 64-shard `CachePadded<RwLock<HashTable<PrimaryEntry>>>` hashed by `XXH3`. Each `PrimaryEntry` holds `InlineId`, `RecordOrdinal (u32)`, `RecordVersion (u64)`, `DiskLocation`, and an optional `ArcSwapOption<AlignedRecordBuf>` for in-flight un-flushed updates.
  - **Exit Criteria**: Wait-free/read-lock lookup `< 25 ns` with zero heap allocations.

- [x] **TASK-012: Implement Memory-Mapped Primary Index Checkpointing (`primary.idx`)**
  - **Deliverables**: `crates/storage-index/src/checkpoint.rs`.
  - **Optimizations**: Dump and load the primary index table as a contiguous `rkyv` / flat binary array with `XXH3` checksum so engine startup with 1,000,000 records completes in `< 10 ms` without filesystem directory scanning.
  - **Exit Criteria**: Restart benchmark with 100k+ records loads index in single-digit milliseconds.

- [x] **TASK-013: Implement Striped Per-Record Write Queues & Closure Mutation API**
  - **Deliverables**: `crates/storage-core/src/write_queue.rs`, `crates/storage-core/src/engine.rs`.
  - **Optimizations**: 2048-way `CachePadded` per-record lock stripes. Implement `db.insert`, `db.get_ref`, `db.update`, `db.update_if_version` (OCC), and `db.delete`. Mutations immediately publish `HotDirtyArc` so concurrent readers see updates in `< 100 ns` before disk I/O runs.
  - **Exit Criteria**: 16 threads updating disjoint keys scale linearly; 16 threads updating the *same* key serialize cleanly with zero lost updates.

- [x] **TASK-014: Implement Group-Commit Write-Ahead Log (`storage-wal`)**
  - **Deliverables**: `crates/storage-wal/src/lib.rs`, `crates/storage-wal/src/recovery.rs`.
  - **Optimizations**: Lock-free/batched append queue where a single leader thread writes all pending frames from concurrent writers in one `writev`/`WriteFile` call and performs a single `fsync`, amortizing disk sync latency across all active threads.
  - **Exit Criteria**: Group-commit throughput reaches `> 200,000 durable updates/sec` under 32 concurrent writer threads; crash recovery replays all committed frames accurately.

- [x] **TASK-015: Implement Background Async Flusher & Write Coalescer**
  - **Deliverables**: `crates/storage-core/src/flusher.rs`.
  - **Optimizations**: MPSC dirty-key ring buffer deduplicating dirty `RecordOrdinal`s via an atomic bitset (`AtomicU64` bitmap). If a record is mutated 500 times before the flusher pops its bit, only the latest `HotDirtyArc` version is written to disk once!
  - **Exit Criteria**: Hot-key benchmark (100,000 updates to 100 keys) demonstrates `> 95%` write coalescing reduction in disk file writes while maintaining 100% latest-value visibility.

---

## Stage 4: Secondary Indexes & Zero-Allocation Query Engine

- [x] **TASK-016: Define `Indexable` Trait & Differential Index Delta Extractor**
  - **Deliverables**: `crates/storage-index/src/traits.rs`.
  - **Optimizations**: Stack-allocated `IndexValues` extraction before and after `db.update` closure; skip index lock acquisition entirely if the indexed fields did not change during the record update.
  - **Exit Criteria**: Updating a non-indexed field on a record incurs zero secondary index overhead.

- [x] **TASK-017: Implement Cache-Conscious B+Tree Range Index (Timestamps & Numerics)**
  - **Deliverables**: `crates/storage-index/src/btree.rs`.
  - **Optimizations**: Fixed cache-line-friendly nodes mapping `OrdKey (u64/i64/OrderedFloat)` to `SmallVec<[RecordOrdinal; 4]>`, supporting zero-allocation `range(min..=max)`, `prefix`, and reverse iteration (`rev()`).
  - **Exit Criteria**: Range scan over 1M indexed records resolves 1,000 matching ordinals in `< 20 µs`.

- [x] **TASK-018: Implement RoaringBitmap Equality & Tag Index**
  - **Deliverables**: `crates/storage-index/src/bitmap.rs`.
  - **Optimizations**: Maps discrete tags/enums/foreign keys to compressed `RoaringBitmap`s of 32-bit `RecordOrdinal`s, enabling SIMD bitwise `AND`, `OR`, and `AND NOT` set intersections across multiple query predicates before touching a single record file.
  - **Exit Criteria**: 3-way tag intersection across 1M records executes in `< 10 µs`.

- [x] **TASK-019: Implement Zero-Allocation Query Builder & Prefetching Iterator**
  - **Deliverables**: `crates/storage-core/src/query.rs`.
  - **Optimizations**: Fluent iterator API (`db.query::<T>().filter_tag(...).range_by(...)`) that intersects secondary index bitmaps/ranges first, issues batched OS page-cache prefetch hints (`MADV_WILLNEED`) for the next $N$ matching records, and yields `RecordRef<'a, T>` views lazily.
  - **Exit Criteria**: End-to-end indexed query benchmark verified with zero heap allocations on iterator traversal.

---

## Stage 5: Explicit Relationships, Hierarchies & Referential Integrity

- [x] **TASK-020: Implement Zero-Copy Relationship Containers (`OneToOne`, `OneToMany`, `ManyToMany`)**
  - **Deliverables**: `crates/storage-graph/src/containers.rs`.
  - **Optimizations**: Compact sorted `SmallVec` / `ArchivedVec<InlineId>` containers with binary-search membership checks (`O(log deg)`) and zero-copy slice access directly from `mmap`.
  - **Exit Criteria**: Relationship membership check and iteration require zero heap allocations on archived records.

- [x] **TASK-021: Implement Deadlock-Free Bidirectional `link` & `unlink` Operations**
  - **Deliverables**: `crates/storage-graph/src/ops.rs`.
  - **Optimizations**: Acquire per-record write locks in canonical ascending `(CollectionId, InlineId)` order, update both forward and reverse ID lists atomically, and log as a single coupled WAL batch.
  - **Exit Criteria**: High-contention concurrent `link`/`unlink` fuzz test (16 threads randomly linking/unlinking overlapping nodes) runs 100,000 operations with zero deadlocks and 100% graph symmetry.

- [x] **TASK-022: Implement Referential Integrity Policies (`Restrict`, `Cascade`, `Unlink`)**
  - **Deliverables**: `crates/storage-graph/src/integrity.rs`.
  - **Optimizations**: When deleting a record, automatically enforce configured edge rules—rejecting deletion (`Restrict`), recursively batch-deleting children (`Cascade`), or stripping the deleted ID from peer records (`Unlink`).
  - **Exit Criteria**: Unit and property tests verify zero dangling references after arbitrary delete cascades.

- [x] **TASK-023: Implement Batched Graph Traversal & Hierarchy Bundle Co-Mapping**
  - **Deliverables**: `crates/storage-graph/src/traverse.rs`, `crates/storage-mmap/src/bundle.rs`.
  - **Optimizations**: Support BFS/DFS and 1-hop/N-hop neighbor expansion (`db.related::<Child>(&user.posts)`) with RoaringBitmap visited tracking, plus co-locating tightly coupled parent/child hierarchies into shared parent folders or contiguous `.bundle` mmap files.
  - **Exit Criteria**: Traversing 1,000 child records of a parent entity completes in microseconds with zero dangling reference errors.

---

## Stage 6: Multi-Record Transactions, Schema Evolution, Arena Compaction & AI Features

- [x] **TASK-024: Implement Multi-Record Atomic Transactions (`db.transaction`)**
  - **Deliverables**: `crates/storage-core/src/transaction.rs`.
  - **Optimizations**: Optimistic read-set version tracking + sorted lock acquisition on write-set commit, writing an atomic `WalTxnCommit` barrier so all mutated records become visible simultaneously or roll back cleanly on conflict/crash.
  - **Exit Criteria**: Classic bank-transfer invariant test (100 threads transferring balances across 50 accounts with simulated crashes) preserves exact total balance across all runs.

- [x] **TASK-025: Implement Adaptive Slotted Arena Segments (`seg_XXXXX.dat`) & Compactor**
  - **Deliverables**: `crates/storage-mmap/src/arena.rs`, `crates/storage-core/src/compactor.rs`.
  - **Optimizations**: Pack sub-4KB records into 64 MB memory-mapped arena segments at 64-byte alignment using a lock-free bump pointer; background compactor reclaims dead/updated slots when fragmentation exceeds a configurable threshold.
  - **Exit Criteria**: Storing 1,000,000 tiny 128-byte records uses `< 5` OS file descriptors and zero 4 KB page-slack waste.

- [x] **TASK-026: Implement Schema Evolution (`schema_version` Migration Pipeline)**
  - **Deliverables**: `crates/storage-core/src/migration.rs`.
  - **Optimizations**: Register pure transformation hooks (`v1 -> v2`) that lazily upgrade records when read or during background segment compaction without downtime.
  - **Exit Criteria**: Database populated with `UserV1` records seamlessly serves `UserV2` structs after registering migration hook.

- [x] **TASK-027: Implement TTL Expiration, Cold LZ4/Zstd Compression & SIMD Vector Scan**
  - **Deliverables**: `crates/storage-core/src/ttl.rs`, `crates/storage-codec/src/compress.rs`, `crates/storage-index/src/vector.rs`.
  - **Optimizations**:
    1. Timing-wheel TTL sweeper for ephemeral records.
    2. Optional LZ4 / Zstd compression for cold archived records.
    3. 64-byte-aligned SIMD cosine/dot-product scoring over zero-copy `ArchivedVec<f32>` embeddings for AI agent memory retrieval.
  - **Exit Criteria**: Hybrid tag + timestamp + SIMD vector top-K search over 100k AI memory records executes in `< 2 ms`.

---

## Stage 7: CLI, Network Server & Chaos Crash-Testing Verification

- [x] **TASK-028: Build `storage-cli` Inspection, Compaction, Backup & Benchmark Tool**
  - **Deliverables**: `crates/storage-cli/src/main.rs`.
  - **Features**: Commands for `stats`, `get`, `inspect-header`, `verify-integrity`, `compact`, `snapshot`, and `bench`.
  - **Exit Criteria**: CLI builds as a single static binary and passes integration tests on live database directories.

- [x] **TASK-029: Build `storage-server` (Zero-Copy Binary Protocol over TCP/UDS)**
  - **Deliverables**: `crates/storage-server/src/main.rs`, `crates/storage-server/src/protocol.rs`.
  - **Optimizations**: Length-prefixed binary framing that streams `rkyv` archived record slices directly from `Mmap` pages to socket buffers (`writev`) without deserializing on the server.
  - **Exit Criteria**: Local loopback/UDS server benchmark exceeds `150,000 req/sec` with sub-millisecond `p99` latency.

- [x] **TASK-030: Execute Full Chaos Crash-Testing & End-to-End Performance Gate**
  - **Deliverables**: `crates/storage-core/tests/chaos_crash.rs`, `docs/BENCHMARK_REPORT.md`.
  - **Verification**:
    1. Spawn real child processes performing heavy concurrent writes, relationship links, index updates, and compactions, and `SIGKILL` / `TerminateProcess` them at random sub-millisecond intervals across 1,000+ iterations.
    2. Verify 100% deterministic recovery, zero checksum mismatches, zero orphaned temp files, and zero index/relationship inconsistencies.
    3. Produce final `BENCHMARK_REPORT.md` proving all latency, throughput, memory, and zero-allocation targets.
