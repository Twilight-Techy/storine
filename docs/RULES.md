# Engineering, Safety & Extreme Optimization Rules (`RULES.md`)

> **Core Philosophy**: Every CPU cycle, cache line, heap allocation, file descriptor, and system call is budgeted. We never trade correctness or features for speed—instead, we design data structures and memory layouts so that the fastest path is also the cleanest, safest, and most deterministic path.

---

## 1. Zero-Allocation & Memory Footprint Rules

### RULE-MEM-01: Zero Heap Allocations on the Hot Read Path
- Calling `db.get_ref::<T>(id)` or querying a primary/secondary index on a warm record **MUST NOT** perform a single heap allocation (`malloc` / `alloc`).
- All read guards (`RecordRef<'a, T>`) must borrow directly from the pinned `Mmap` page or the in-flight `HotDirtyArc` buffer on the stack.
- Every hot-path function is verified in CI using a custom `CountingAllocator` wrapper that asserts `alloc_count == 0` during read benchmarks.

### RULE-MEM-02: Stack-Inlined Small Types Over Heap Containers
- **Primary Keys (`InlineId`)**: Keys `<= 23` bytes MUST be stored inline on the stack (using a 24-byte tagged representation) with zero heap indirection.
- **Temporary Collections**: Use `SmallVec<[T; N]>` or `ArrayVec<T, N>` for index delta lists, relationship batch operations, and path construction (`ArrayString` / stack buffer `[u8; 256]`) instead of `Vec<T>` or `String`/`PathBuf` heap allocations.
- **Scratch Buffers**: `rkyv` serialization during writes MUST reuse thread-local or queue-pooled `AlignedVec` scratch buffers via `.clear()` rather than allocating and dropping buffers per write.

### RULE-MEM-03: Struct Layout Packing & Size Assertions
- Order struct fields from largest alignment to smallest alignment (or let `repr(Rust)` optimize while verifying with `const _: () = assert!(std::mem::size_of::<T>() <= LIMIT);`).
- Every core metadata entry (`RecordLocation`, `RecordHeader`, `WalFrameHeader`, `IndexEntry`) MUST have a compile-time `static_assertions::const_assert_eq!(std::mem::size_of::<...>(), ...)` so accidental struct bloat breaks compilation immediately.

### RULE-MEM-04: Global Allocator & Strict Resource Ceilings
- Production binaries and benchmarks MUST use `mimalloc` (`#[global_allocator]`) to minimize allocator fragmentation and multi-threaded lock contention.
- The engine MUST enforce hard configurable ceilings on:
  1. `max_active_mmaps` (default: `16,384` regions)
  2. `max_open_file_descriptors` (default: `4,096` FDs)
  3. `max_hot_dirty_bytes` (default: `256 MB` before backpressure / immediate flush)

---

## 2. CPU, Cache-Line & SIMD Optimization Rules

### RULE-CPU-01: Eliminate False Sharing via 64-Byte Cache-Line Alignment
- Any concurrently mutated structure (striped lock slots, per-shard atomic counters, WAL ring-buffer head/tail pointers, flush queue nodes) MUST be annotated with `#[repr(C, align(64))]` (or `crossbeam_utils::CachePadded<T>`) so independent threads never invalidate each other's L1/L2 CPU cache lines.

### RULE-CPU-02: Hardware-Accelerated Hashing & Checksums
- Never use `std::collections::HashMap` with default SipHash-1-3 on internal keys.
- Use `hashbrown::HashMap` / `HashTable` with `xxh3` (`xxhash-rust` with AVX2/SSE2/NEON) or `ahash` (AES-NI).
- All record payloads and WAL frames MUST use 64-bit `XXH3` or hardware-accelerated `CRC32C` instructions.

### RULE-CPU-03: Branch Prediction, Inlining & Cold Error Paths
- Annotate hot accessors (`InlineId::as_bytes`, `RecordRef::get`, `RecordHeader::validate_fast`, shard index calculations) with `#[inline]`.
- Annotate all error construction, corruption recovery, schema migration, and slow-path fallback functions with `#[cold]` and `#[inline(never)]` so the instruction cache (I-cache) on the fast path stays compact.
- Use power-of-two shard counts (`N = 64`, `2048`) so modulo arithmetic compiles to a single bitwise mask (`hash & (N - 1)`) instead of a 20+ cycle integer division (`div`).

### RULE-CPU-04: Compiler & Linker Optimization Profiles
- Workspace `Cargo.toml` release and benchmark profiles MUST enforce:
  ```toml
  [profile.release]
  opt-level = 3
  lto = "fat"
  codegen-units = 1
  panic = "abort"
  strip = "symbols"
  ```
- Support `.cargo/config.toml` `target-cpu=native` builds to unlock AVX2/AVX-512/NEON instructions for SIMD index search and checksumming.

---

## 3. `unsafe` Rust, `mmap` & Memory Safety Rules

### RULE-SAFE-01: Strict Isolation of `unsafe` Code
- `#![forbid(unsafe_code)]` MUST be enabled at the crate root of `storage-index`, `storage-graph`, `storage-wal`, `storage-cli`, and `storage-server`.
- `unsafe` is permitted **ONLY** in `storage-mmap` (OS virtual memory FFI), `storage-codec` (validated `rkyv::archived_value` zero-copy cast), and `storage-types` (`bytemuck`/`repr(C)` header transmutation), and those crates MUST enable `#![deny(unsafe_op_in_unsafe_fn)]`.

### RULE-SAFE-02: Mandatory `// SAFETY:` Proofs on Every `unsafe` Block
- Every single `unsafe { ... }` block MUST be preceded by a `// SAFETY:` comment documenting:
  1. **Pointer Validity & Alignment**: Proof that the byte slice is at least 16-byte / 64-byte aligned and at least `sizeof::<RecordHeader>() + payload_len` bytes long.
  2. **Lifetime & Pinning Invariant**: Proof that the underlying `Mmap` region cannot be unmapped or truncated while any `RecordRef<'a, T>` guard is alive.
  3. **No Aliasing Mutation (`&mut` vs `&`)**: Proof that no thread modifies a mapped file in-place while readers hold shared references to that mapping slot.

### RULE-SAFE-03: Never Expose Mutable `mmap` Pointers to Callers
- Callers **NEVER** receive `&mut T` pointing directly into an active shared `mmap` region.
- All updates run inside `db.update::<T>(id, |draft: &mut T| ...)` where `draft` is an isolated mutable instance. Only after the closure succeeds, validates invariants, increments `record_version`, updates secondary indexes, and serializes a fresh `rkyv` frame is the new state atomically swapped into visibility.

### RULE-SAFE-04: Validate Once, Read Zero-Cost Forever
- Newly mapped disk pages or crash-recovered WAL frames MUST pass `XXH3-64` checksum verification and `rkyv::check_archived_root::<T>` bytecheck validation **once** when loaded.
- Once verified, the `MmapSlot` marks an atomic `VERIFIED` bit so all subsequent reads bypass bytecheck and dereference the root offset directly in $O(1)$ time.

---

## 4. Concurrency, Locking & I/O Rules

### RULE-CONC-01: Never Hold Locks Across Disk I/O
- Primary index shard locks (`parking_lot::RwLock`) may only be held for pointer swaps or hash table lookups (`< 100 ns`).
- Disk writes (`WriteFile`/`pwrite`), `fsync`/`FlushFileBuffers`, and file renames MUST execute outside of global or shard-wide locks.

### RULE-CONC-02: Canonical Lock Ordering (Deadlock Freedom by Construction)
- Whenever an operation locks more than one record (e.g., bidirectional relationship `link(id_a, id_b)` or multi-record `transaction`), per-record write queues MUST be acquired in **strictly ascending lexicographical `InlineId` order** (with tie-breaking by collection ID). Deadlocks are structurally impossible.

### RULE-CONC-03: Coalesce Writes & Batch `fsync`
- Never call `fsync` per individual update when multiple concurrent updates are in flight.
- The Group-Commit WAL batches all concurrent writer threads waiting during a flush window into a single sequential write + `fsync`.
- The background record flusher coalesces multiple sequential versions of the same record in RAM into a single disk write.

---

## 5. Verification, Benchmarking & Discipline Rules

### RULE-TEST-01: Every Subsystem Ships With Unit, Property & Crash Tests
- No task is complete until it passes:
  1. Unit tests covering edge cases (empty records, max-size records, corrupted checksums, version conflicts).
  2. Property-based tests (`proptest`) verifying randomized sequences of `insert`/`update`/`delete`/`link`/`query` against an in-memory reference model.
  3. Miri / sanitizer checks on all data structures.

### RULE-TEST-02: Benchmark-Driven Optimization Gate
- Every core subsystem (`InlineId`, `rkyv` codec, `MmapPool`, `PrimaryIndex`, `BTreeIndex`, `RoaringIndex`, `WriteCoalescer`, `GroupCommitWal`) has a dedicated Criterion benchmark tracking `p50`, `p99`, `p99.9` latency, throughput (ops/sec), and bytes allocated per op.
- Any change that regresses hot-path throughput or latency by `> 3%` or introduces heap allocations on the read path must be redesigned before moving to the next task.
