# Engineering, Safety & Extreme Optimization Rules (`RULES.md`)

> **Core Philosophy**: Every CPU cycle, cache line, heap allocation, file descriptor, disk byte, and system call is budgeted. We never trade correctness or features for speed—instead, we design data structures, algorithms, and memory layouts so that the fastest path is also the smallest, safest, and most deterministic path.

---

## 1. Zero-Allocation, Anti-Bloat & Minimal Footprint Rules

### RULE-MEM-01: Zero Heap Allocations on the Hot Read Path
- Calling `db.with_ref::<T>(id, ...)` or `db.get_ref::<T>(id)` on a clean mapped record **MUST NOT** perform a single heap allocation (`malloc` / `alloc`).
- All read views (`RecordView<'a, T>`, `RecordRef<T>`) borrow directly from the pinned `MmapRegion` page or in-flight `hot_dirty` buffer.
- Verified continuously via `TrackingAllocator` (`benches/src/lib.rs`) asserting `allocs_per_op == 0.00`.

### RULE-MEM-02: Stack-Inlined Small Types & Zero Duplicate In-RAM Maps
- **Primary Keys (`InlineId`)**: Keys `<= 23` bytes are stored inline on the stack in a 24-byte `#[repr(C, align(8))]` struct with zero heap indirection.
- **Zero Duplicate Payload Maps**: Secondary index snapshots (`IndexSnapshot`) and float embeddings MUST be extracted zero-copy directly from the memory-mapped `&T::Archived` frame during updates and searches rather than duplicated in auxiliary in-RAM hash maps.
- **Pooled Scratch Buffers**: Serialization and compression MUST reuse thread-local 64-byte-aligned `AlignedScratchBuf` buffers via `.clear()`.

### RULE-MEM-03: Struct Layout Packing, Code Size & On-Disk Data Density
- Order struct fields to eliminate alignment padding waste (`IndexSnapshot` uses Struct-of-Arrays packing; `ArchivedOneToOne` uses `InlineId::EMPTY` as the `None` niche).
- Every core structure (`RecordHeader`, `InlineId`, `PrimaryEntry`, `MmapGuard`) MUST enforce compile-time size assertions.
- On-disk checkpoints (`primary.idx`), paged B+Tree nodes, HNSW neighbor lists, and WAL frames MUST use compact varint, delta, bit-packed, or scalar-quantized (`SQ8`) encodings to keep disk footprint minimal.
- Zero external runtime crates are used in production binaries, and every release binary stays `< 1 MiB` with `lto = "fat"` and `strip = "symbols"` (budget raised from `500 KiB` on 2026-10-27, `TASK-091`). The budget is a ceiling, not a target: keep minimizing code size wherever it costs no hot-path speed.

### RULE-MEM-04: Strict Resource Ceilings & Anti-Memory-Bloat Reclamation
- Enforce hard, runtime-configurable ceilings on:
  1. `max_hot_dirty_bytes` (default `64 MiB` — triggers immediate synchronous dirty-batch flush backpressure when reached).
  2. `PoolBudget::max_mapped_bytes` (default `1 GiB`) and `max_open_files` (default `512`) via 4-way sharded Clock-PRO / Second-Chance eviction.
  3. `max_connections` (default `100,000`, dynamically tunable at runtime via `set_max_connections`).
- Transient per-connection request/response buffers (`body_buf`, `out_buf`) MUST be released back to `0` capacity as soon as a large frame completes so idle connections never retain bloated heap buffers (`~88 bytes` per idle connection).
- `RecordOrdinal`s of deleted records MUST be recycled in $O(1)$ time via a free-list after secondary index cleanup so ordinal bitmaps and tables never bloat monotonically under heavy churn.

---

## 2. CPU Efficiency, Anti-CPU-Hogging & SIMD Optimization Rules

### RULE-CPU-01: Eliminate False Sharing via 64-Byte Cache-Line Alignment
- Any concurrently mutated structure (`PrimaryIndex` shards, `StripedLockTable` stripes, `MmapPool` shards, reactor queues) MUST be wrapped in `#[repr(C, align(64))]` (`CachePadded<T>`) so independent CPU cores never invalidate each other's L1/L2 cache lines.

### RULE-CPU-02: Hardware-Accelerated Hashing, Checksums & Vector Kernels
- Use single-multiply Fibonacci / `XXH3-64` hashing (`FastHashMap` / `inline_id_hash64`) instead of SipHash.
- All record payloads, index checkpoints, and WAL frames MUST use 4-way unrolled `XXH3-64` combined with hardware SSE4.2/ARMv8 `CRC32C` instructions.
- Vector similarity scoring (`cosine_similarity_f32` and `SQ8` quantized dot-product) MUST use 8-way loop-unrolled SIMD accumulation.

### RULE-CPU-03: Zero CPU Hogging & Event-Driven Readiness
- **No Unbounded Spin Loops**: Worker threads (`ReactorShard`, `GroupCommitWal`, background flusher, Raft replication workers) MUST NEVER spin unboundedly at 100% CPU when idle.
- Use OS-native readiness polling (`IOCP` / `WSAPoll` on Windows, `epoll` / `kqueue` / `poll` on Unix) or bounded adaptive backoff (`spin_loop` -> `yield_now` -> `Condvar::wait_timeout`) so idle threads consume `0%` CPU.
- If the OS returns transient descriptor exhaustion (`EMFILE` / `ENFILE`) on `accept()`, back off cleanly rather than busy-spinning.

### RULE-CPU-04: Branch Prediction, Inlining & Compiler Profiles
- Annotate hot accessors with `#[inline]` and slow/error/recovery paths with `#[cold]` and `#[inline(never)]`.
- Use power-of-two shard counts (`64`, `2048`) so shard selection compiles to a single bitwise mask (`hash & (N - 1)`).

---

## 3. Optimal Time & Space Complexity Rules (`RULE-CPLX-*`)

### RULE-CPLX-01: Asymptotic Time Complexity Mandates
- **Point Lookups, Writes & Ordinal Recycling**: Strictly **$O(1)$** average time in `PrimaryIndex` (including $O(1)$ sentinel transition `Some(InlineId::EMPTY) -> None` during ordinal release; never linear $O(F)$ free-list scans).
- **Multi-Tag Bitmap Queries**: **$O(W_{\min} \cdot k)$** by sorting posting lists by word count so the smallest bitmap drives bitwise `AND` intersections using hardware `TZCNT` (`trailing_zeros()`).
- **Range & Paged B+Tree Queries**: **$O(\log B + |R|)$** using order-preserving integer/IEEE-754 float keys (`sortable_f64_to_i64`, `sortable_u64_to_i64`).
- **Top-$K$ Exact & ANN Vector Search**:
  - Exact candidate scoring: **$O(N \cdot D + N + K \log K)$** using linear $O(N)$ Hoare/Floyd-Rivest Quickselect (`select_nth_unstable_by`) instead of $O(N \log N)$ full sorting.
  - Unfiltered ANN search: **$O(\log N)$** greedy beam search over multi-layer memory-mapped HNSW graphs with `SQ8` quantized vectors.
- **Relationship Membership Checks**: **$O(\log K)$** binary search directly over sorted `InlineId` slices in `mmap`.

### RULE-CPLX-02: Asymptotic Space Complexity Mandates
- Per-record index overhead in RAM must remain strictly bounded (and $O(1)$ when paged to disk).
- Auxiliary space during point reads must be strictly **$O(1)$ (`0` heap bytes)**.

---

## 4. Scaling & Concurrency Rules (Users, Vertical Compute & Horizontal Compute)

### RULE-SCALE-01: Million-User Connection Multiplexing & Backpressure
- Never spawn 1 OS thread (`2 MiB` stack) per client connection. Multiplex connections across a fixed pool of non-blocking `ReactorShard` workers (`~88 bytes` per idle connection).
- Enforce strict per-connection TCP write backpressure: never read a new request from a socket while its previous `out_buf` response is still draining (`WouldBlock`).
- Shed excess connections above `max_connections` immediately with `STATUS_OVERLOADED` (`0x03`).

### RULE-SCALE-02: Vertical Compute Scaling (Multi-Core Linear Throughput)
- Partition all in-memory state across cache-line-aligned shards (`64` index shards, `2,048` lock stripes, sharded `MmapPool`, sharded reactor threads) and use immutable `MmapGuard` / `Arc` frames so readers and writers never block each other.
- Skip secondary index locks completely during `update` if indexed fields did not change (`IndexSnapshot::diff`).

### RULE-SCALE-03: Horizontal Compute Scaling (Sharded Cluster & Raft Replication)
- Partition the keyspace into deterministic virtual shards (`vshards`, e.g., `1,024` hash/range buckets) that can be mapped across physical cluster nodes and migrated online without downtime.
- Replicate `GroupCommitWal` batches across nodes using leader-based Raft consensus with pipelined append entries and learner/follower read replicas.

---

## 5. Total Graceful Error Handling & Safety Rules (`RULE-ERR-*` & `RULE-SAFE-*`)

### RULE-ERR-01: Zero Panics, Fallible Allocations & Lock Poison Recovery
- Production library and server code MUST NEVER call `.unwrap()` or `.expect()` on fallible I/O, network sockets, untrusted bytes, or user parameters.
- All `Mutex` and `RwLock` acquisitions MUST recover from poisoned locks via `.unwrap_or_else(|poisoned| poisoned.into_inner())` so a panic in a user closure never takes down the engine.
- Network and dynamic buffer allocations for untrusted sizes MUST use fallible allocation (`Vec::try_reserve_exact`), returning `STATUS_OVERLOADED` / `StorageError::Io(OutOfMemory)` on heap exhaustion instead of aborting the process.
- Background flush failures (`ENOSPC` / transient I/O errors) MUST keep the dirty record intact in `hot_dirty` and automatically re-enqueue the key into `WriteCoalescer` for retry on the next flush cycle.

### RULE-SAFE-01: Strict Isolation of `unsafe` Code & `// SAFETY:` Proofs
- `#![forbid(unsafe_code)]` is mandatory in `storage-types`, `storage-wal`, `storage-index`, `storage-graph`, `storage-core`, `storage-server`, and `storage-cli`.
- `unsafe` is permitted ONLY in `storage-codec` and `storage-mmap` with `#![deny(unsafe_op_in_unsafe_fn)]` and `#![deny(clippy::undocumented_unsafe_blocks)]`.
- Use `ArchivedBool` (`u8` validated `<= 1`) and `ArchivedChar` (`u32` validated via `char::from_u32`) in zero-copy structs so corrupted disk bytes never trigger Rust Undefined Behavior.
- **Archived types are plain-old-data (`ArchivePod`)**: every `ZeroCopyRecord::Archived` type MUST implement the `unsafe` marker trait `storage_codec::ArchivePod` (any bit pattern is a valid value, no padding, no pointers). Crates under `forbid(unsafe_code)` implement it only through `storage_codec::impl_archive_pod!`, which proves at compile time (via `offset_of!` / `size_of!`) that the listed fields are themselves `ArchivePod` and cover the struct with no padding; a hand-written `unsafe impl` is never allowed outside `storage-codec` and `storage-mmap`.
- **No safe API may reach UB on hostile bytes**: every safe accessor over archived bytes (`ArchivedString::as_str`, `ArchivedSlice::as_slice`, `RecordCodec::decode_fast`) bounds-, alignment- and UTF-8-checks its input, returning an empty / zeroed value instead of panicking. `RecordCodec::decode_unchecked` stays an `unsafe fn` (benchmarks only).

### RULE-SAFE-02: Lifetime-Bound Mapped Memory
- Every view into mapped memory (`RecordView<'a, T>`, `RecordRef<T>`, `MmapGuard`) MUST keep its `Arc<MmapRegion>` (or `hot_dirty` frame) alive for as long as the view exists. An arena slot may be reused in place only when no reader holds its region (`Arc::strong_count == 1`) and no retired region is alive; otherwise the write goes to fresh space.

### RULE-SAFE-03: Controlled Mutation
- Records change only through engine APIs (`put`, `update`, `cas_update`, `transaction`, `delete_with_policy`) that hold the record's stripe lock and the MVCC writer gate, log the change to the WAL **before** publishing it, bump the `Version`, and apply the differential index update. Multi-record changes (deletes with cascades, transactions) are written as one atomic WAL transaction.

### RULE-SAFE-04: Validate Once, Read in O(1)
- Untrusted bytes (disk pages, WAL frames, network frames) pass `RecordCodec::validate_frame::<T>` (checksums + `T::validate_archived`) exactly once when first mapped or loaded; the result is cached (`MmapRegion::verified`, `PrimaryEntry::disk_verified`). Subsequent hot reads use the safe O(1) `RecordCodec::decode_fast` (header, bounds, alignment and identity checks only). If previously validated bytes change underneath a `RecordRef`, `decode_fast_or` returns an all-zero view (backed by a static page), never UB.

### RULE-SAFE-05: Build- and Platform-Independent On-Disk Formats
- Every on-disk and wire encoding (checksums, varints, archive layouts, WAL frames) MUST produce identical bytes regardless of compile flags (`target-cpu`, `target-feature`), OS, or CPU. Hardware-accelerated paths (SSE4.2 `crc32`) MUST be bit-identical to their portable fallbacks and are selected at runtime when not enabled at compile time. Every such encoding ships a golden-value test that pins the format.

### RULE-SAFE-06: Network Exposure, Admission & Path Confinement
- An unauthenticated server (`SecurityConfig::require_auth = false`) MUST refuse to bind a non-loopback address.
- Role checks, body-size limits and rate limits are applied at the request-header stage, **before** any body bytes are allocated or read.
- Client-supplied paths (e.g. `OP_ADMIN_BACKUP` names) are confined to an operator-configured root (`backup_root`): only plain relative components are accepted, and admin backups are disabled when no root is configured. Backup and restore destinations must be fresh (empty or absent) and outside the database directory; existing data is never deleted.

### RULE-SAFE-07: Encryption at Rest Without Downgrade
- With `EngineConfig::encryption` set, every record frame written to disk or the WAL is protected: encrypted, or (only where the record type's `Indexable::protection` policy says so) authenticated with a MAC. Both bind the record id, key id and header metadata. Any unprotected record frame read back is rejected, never trusted (the only exception is the explicit migration state `accept_unencrypted`). Plaintext of encrypted records lives only in RAM (`hot_dirty`, MVCC history, the bounded decrypted-frame cache). Key material is never logged and is zeroed on drop.
- Index files follow the same settings: sealed with the active key, plaintext index files refused (no downgrade), and a tampered paged generation is rebuilt, never served.

### RULE-SAFE-08: Money-Grade Writes (No Lost Updates, No ABA, No Float Money)
- **Versions are never reused**: a record created after a deletion starts above the collection's persisted version floor. Any code path that creates records MUST take its version from `next_version`.
- **No client read-modify-write over the network**: every remote mutation of an existing value is either an atomic field operation (`FieldOp`) or a conditional write (`Precondition::Version`). Blind overwrites stay available but are never the documented way to update a value derived from its previous value.
- **Exact numbers**: monetary values are `Decimal` (or integer minor units); arithmetic is checked and fails instead of wrapping, saturating or rounding silently; rounding happens only where the caller names a `RoundingMode`.
- **Retries are safe**: mutating requests that a client may retry (timeouts, `CommitUnknown`) carry a request id and are applied at most once.

### RULE-SAFE-09: Replicated State Changes Only Through Consensus
- A replicated engine is in replica mode: every write goes through the Raft log and the deterministic apply path (`commit_at` with the log index as minimum version and the leader's proposal time as "now"). Nothing that depends on a replica's local clock, randomness or iteration order may influence applied state.
- A leader serves reads only under a valid lease (quorum-acknowledged within 90% of the minimum election timeout, with vote stickiness enabled) or after a read-index round; followers serve reads only to clients that explicitly accept staleness.
- A write whose replication cannot be confirmed is reported as `CommitUnknown`, never as success or failure.
- Lock order inside a Raft node is `state -> outcomes` and `progress` is never held while taking node state.

### RULE-CONC-01: Canonical Lock Ordering & Lock-Free I/O
- Never hold primary index shard locks across disk I/O (`WriteFile`/`fsync`).
- Multi-record mutations (`update_pair_atomic`, `transaction`, `delete_with_policy`) MUST lock keys in strictly ascending canonical `(CollectionId, InlineId)` order (`StripedLockTable::lock_many`) so AB-BA deadlocks are mathematically impossible.
- Whole-engine operations (`flush_all`, `checkpoint`, snapshots / backups) take every stripe via `StripedLockTable::lock_all` (ascending order) or use `barrier()`; no other lock may be held while doing so.

### RULE-CONC-02: Cross-Collection Lock Ordering
- `DatabaseCatalog` transactions group touched keys by `CollectionId` in ascending order and lock each collection's stripes in ascending `(collection_id, stripe_index)` order, so cross-collection transactions can never deadlock with each other or with single-collection writes.

### RULE-CONC-03: Group-Commit WAL Ordering & Fail-Stop
- WAL LSNs are assigned under the WAL lock, so file order equals LSN order. A writer is acknowledged only after the batch containing its frames is durable under the configured `SyncPolicy`. After any WAL write or `fsync` error the WAL fails stop (every later append returns an error) rather than acknowledging writes whose durability is unknown.

---

## 6. Verification, Benchmarking & Discipline Rules (`RULE-TEST-*`)

### RULE-TEST-01: Mandatory Unit, Corruption, Concurrency & Chaos Crash Tests
- Every subsystem must ship with unit tests, single-bit flip / torn-frame corruption tests, multi-threaded contention tests, and chaos crash-recovery verification.
- Crash recovery is verified with a real process kill (`test_process_kill_crash_recovery_preserves_every_acknowledged_write` aborts a child writer mid-stream), not only by dropping the engine in-process.
- Performance comparisons on shared or loaded machines MUST interleave baseline and candidate runs (A/B/A/B) instead of comparing single runs.

### RULE-TEST-02: Benchmark & Footprint Gate
- Every stage is verified against `cargo run --release -p storage-benches` to guarantee zero hot-read heap allocations (`0.00 Allocs/Op`), sub-microsecond hot reads, and minimal binary/data footprint.
