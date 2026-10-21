# StorageEngine End-to-End Benchmark, Footprint & Zero-Allocation Verification Report (`TASK-030` & `TASK-050` `v1.0` Gate)

> **Note (2026-10-20 audit):** Sections 1–4 are the historical `TASK-030` / `TASK-050` results. All engine benchmarks run with `SyncPolicy::AsyncMemoryFirst` (no `fsync` per write), so write latencies are memory-first numbers. The checksum is an XXH64-family 4-lane hash combined with a dual-lane CRC32C (not reference XXH3). The TCP `GET` row counts client-side allocations. The `RecordView` row uses `unsafe decode_unchecked`; safe engine reads use `decode_fast`. See **Section 5** for the post-audit re-verification.

## 1. Build & Hardware Profile
- **Compiler Profile**: `release` (`opt-level = 3`, `lto = "fat"`, `codegen-units = 1`, `panic = "abort"`, `strip = "symbols"`, `-C target-cpu=native`)
- **Platform**: Windows (`x86_64-pc-windows-msvc`), Native Win32 `CreateFileMappingW` / `MapViewOfFile` / `PrefetchVirtualMemory` + `WSAPoll` (`ws2_32.dll`) + Hardware SSE4.2 CRC32C Intrinsics
- **Zero-Allocation Gate**: Enforced via `#[global_allocator] static GLOBAL_ALLOC: TrackingAllocator` (`benches/src/lib.rs`) using thread-local allocation and byte counters.

---

## 2. Latency Distribution (`p50`, `p99`, `p99.9`), Throughput & Allocation Verification

| Benchmark Workload | Throughput (`ops/sec`) | `p50` (`ns`) | `p99` (`ns`) | `p99.9` (`ns`) | Heap Allocs / Op |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **InlineId (24B) Hash + Eq** | `365,807,963 / s` | `3 ns` | `6 ns` | `8 ns` | **`0.00`** |
| **Zero-Copy `RecordView` Read (`decode_unchecked`)** | `279,828,019 / s` | `4 ns` | `6 ns` | `10 ns` | **`0.00`** |
| **SIMD Cosine Similarity (`64-dim f32`, 8-way unrolled)** | `10,812,550 / s` | `87 ns` | `105 ns` | `124 ns` | **`0.00`** |
| **`StorageEngine::with_ref` (0-Alloc Hot Read)** | **`4,212,584 / s`** *(+166.6% vs baseline)* | **`200 ns`** | `700 ns` | `1,600 ns` | **`0.00`** |
| **`StorageEngine::get_ref` (Mmap Arena Segment)** | **`4,023,976 / s`** *(+168.3% vs baseline)* | **`231 ns`** | `369 ns` | `1,150 ns` | **`0.00`** |
| **SIMD Checksum (`XXH3-64 + SSE4.2 CRC32C`, 4 KB)** | `508,874 / s` (`~2.08 GB/s`) | `1,900 ns` | `2,200 ns` | `2,800 ns` | **`0.00`** |
| **`StorageEngine::update` (Coalesced Hot Write)** | **`204,029 / s`** *(+40.6% vs baseline)* | **`4,700 ns`** (`4.7 µs`) | **`8,100 ns`** | `17,800 ns` | **`5.00`** |
| **Secondary Index (`Tag Bitmap + B+Tree Range`)** | **`142,962 / s`** *(+64.8% vs baseline)* | **`5,900 ns`** (`5.9 µs`) | `12,700 ns` | `33,200 ns` | **`1.00`** |
| **`StorageServer` TCP Binary `GET` Frame RPC (`WSAPoll` Reactor)** | **`20,183 / s`** *(+83.9% vs baseline)* | **`43,300 ns`** (`43.3 µs`) | `110,900 ns` | `187,900 ns` | **`3.00`** *(client-side; `0.00` on server via `IoSlice`)* |

---

## 3. Minimal Storage Footprint Verification (Code Binary, On-Disk Data & In-RAM Data)

| Footprint Dimension | Before Optimization | After Optimization | Improvement |
| :--- | ---: | ---: | :--- |
| **`StorageServer` Per-Connection Memory & Idle CPU** | `2 MiB` OS thread stack + `128 KB` `BufReader`/`BufWriter` per connection | **`~88 bytes` per idle `NonBlockingConn`** multiplexed on kernel-polled `WSAPoll`/`poll` `ReactorShard` workers (`0.0%` idle CPU) | **`24,000x` smaller per idle connection (`~88 MB` vs `~2.1 TB` @ 1M connections) & `0.0%` idle CPU** |
| **`primary.idx` On-Disk Checkpoint (`STGEIDX2`)** | `88.0 bytes / record` (`176,016 B` @ 2k records) | **`19.0 bytes / record`** (`38,008 B` @ 2k records) | **`4.63x` smaller on disk** (varint + compact `InlineId` + 64B-unit arena offsets) |
| **Cold-Tier Primary Index (`ColdPrimaryIndex`)** | `96 bytes / record` in RAM | **`< 2.5 bytes / record` in RAM** (`SplitBlockBloomFilter` + 24B per 4 KiB page directory) | **`> 38x` smaller RAM footprint for billion-key cold tiers** |
| **`SQ8` Scalar Quantized HNSW Vectors (`QuantizedVecSq8`)** | `4 bytes / dim` (`f32`, `6.14 GB` @ 1M x 1536D) | **`1 byte / dim` (`u8` + `min`/`scale`, `1.54 GB` @ 1M x 1536D)** | **`4x` smaller vector memory footprint with exact `f32` top-$K$ re-ranking** |
| **Initial Arena Segment File (`seg_XXXXX.dat`)** | `4 MiB – 16 MiB` upfront | **`4 KiB` (`4,096 B` — 1 OS page)**, doubling on demand | **`1,024x – 4,096x` smaller initial disk footprint** |
| **Live Arena Segment Compaction & Slot Reuse (`reclaim_dead_slot` + `compact_arenas`)** | Dead slots retained until full compaction | **$O(1)$ in-place 64B size-class free-slot recycling (`free_slots`)** + full segment compaction for cross-size-class fragmentation | **Zero `.seg` file growth under steady-state same-size updates + 100% dead segment reclamation** |
| **Arena Flush Syscalls & SSD Page-0 Write Amplification (`TASK-051`)** | `4` syscalls/flush (`seek + write + seek + write`) + page-0 rewrite on every record | **`1` positional `write_all_at` (`seek_write`/`pwrite`) syscall + lazy superblock sync (every 64 appends) with startup fast-forward recovery** | **`75%` fewer syscalls per flush & `64x` lower SSD page-0 write amplification** |
| **WAL Sequential Disk Bandwidth (`TASK-053`)** | Uncompressed raw frames in `wal.log` | **Transparent LZ4 frame compression (`WAL_FLAG_LZ4`) for payloads `>= 1 KiB`** | **`2x–4x` smaller WAL frames on disk (`8 KiB -> < 2 KiB`) with zero overhead for `< 1 KiB` frames** |
| **Primary Index Checkpointing (`TASK-054`)** | Full $O(N)$ `primary.idx` rewrite on every checkpoint | **Incremental $O(\Delta)$ delta frames (`STGEDLT2`) in `primary.idx.delta` + auto-compaction at `1 MiB`** | **$O(\Delta)$ checkpoint I/O proportional only to modified keys** |
| **`ArchivedAgentObjectRecord` On-Disk & Mmap Root** | `120 bytes` | **`112 bytes`** (`InlineId::EMPTY` niche in `ArchivedOneToOne`) | **`-8 bytes` per record on disk & in RAM** |
| **`IndexSnapshot` & Embedding Heap Maps** | `544 bytes / record` (`index_snapshots` + `embeddings` in RAM) | **`0 bytes / record`** (extracted zero-copy from `&T::Archived` via `extract_archived_snapshot` & `archived_embedding_slice`) | **`-544 MB` per 1M records (`100%` heap map elimination)** |
| **Unflushed `hot_dirty` Write Buffer** | Bounded only by flush timer | **`max_hot_dirty_bytes` (`64 MiB` cap)** + background flush | **Strict hard ceiling on dirty in-RAM write buffers** |
| **`IndexSnapshot` Stack Diff Struct** | `352 bytes` | **`264 bytes`** (Struct-of-Arrays packing) | **`-88 bytes` (`-25%`) stack frame during index delta** |
| **`PrimaryEntry` In-RAM Shard Entry** | `104 bytes` | **`96 bytes`** (removed redundant `updated_at_ns`) | **`-8 bytes` per record in RAM** |
| **`MmapGuard` Handle + Verification State** | `16 bytes` + heap `Arc<AtomicBool>` | **`8 bytes`** (`verified: AtomicBool` embedded in `MmapRegion`) | **`-50%` guard size + `0` extra heap alloc per mapped file** |
| **Deleted `RecordOrdinal` Table Growth** | Monotonic growth ($O(F)$ free-list scan) | $O(1)$ recycled free-list (`Some(InlineId::EMPTY)` sentinel) | **Bounded to peak live record count in strict $O(1)$ time** |
| **Top-$K$ Embedding Search (`score_embeddings`)** | $O(N \log N)$ full sort | **$O(\log N)$ HNSW `SQ8` graph search + $O(N + K \log K)$ Quickselect fallback** | **Sub-linear $O(\log N)$ ANN graph search with exact `f32` re-ranking** |

---

## 4. Write Coalescing, Durability & `v1.0` Production Verification (`TASK-030`, `TASK-050` & `TASK-056`)
- **Live Records**: `2,000`
- **Total Foreground Writes**: `7,256`
- **Coalesced Background Flushes Saved**: `5,193` (**`71.6%` reduction in physical disk writes**)
- **Full-Scale `v1.0` + Stages 15–16 Production Verification Gate (`44` Workspace Tests Passing, `0` Warnings)**:
  - **Multi-Collection Catalog & Heterogeneous Graph (`TASK-031`–`TASK-033`)**: Cross-collection ACID transactions (`Users` + `Orders`), `IntegrityPolicy::{Restrict, Cascade, Unlink}` enforcement across collections, and typed schema evolution (`v1 -> v2 -> v3`) during arena compaction.
  - **Paged Disk Indexes (`TASK-034`–`TASK-035`)**: 4 KiB memory-mapped `PagedBTreeIndex`, `PagedBitmapIndex`, 64-byte cache-line `SplitBlockBloomFilter`, and `< 2.5B/record` `ColdPrimaryIndex`.
  - **HNSW & `SQ8` Vector Quantization (`TASK-036`–`TASK-038`)**: Multi-layer `HnswVectorIndex` with `4x` `SQ8` quantization, `hnsw.idx` persistence, and filter-aware hybrid search (`>= 95%` recall@10 with exact `f32` re-ranking).
  - **MVCC Snapshot Isolation & PITR (`TASK-039`–`TASK-041`)**: Wait-free `ReadSnapshot` isolation against concurrent writers, epoch-based MVCC GC, segmented WAL rotation, and exact LSN Point-in-Time Recovery (`restore_point_in_time`).
  - **Kernel-Polled Networking, Wire Protocol v2 & Security (`TASK-042`–`TASK-044`)**: `WSAPoll`/`poll` reactor, `CAS_PUT`, `BATCH_PUT`, `BATCH_GET`, `QUERY_HYBRID`, constant-time token auth + RBAC + rate limiting, and RFC 8439 ChaCha20 + 128-bit MAC AEAD encryption at rest.
  - **Consistent-Hash Sharding & Raft Consensus (`TASK-045`–`TASK-047`)**: `1,024` virtual shards, 2 KiB `ClusterMap`, live `migrate_vshard` with 64-bit `XXH3-64` digest verification, and 3-node `RaftReplicationGroup` leader election, quorum commit, and leader failover.
  - **Cost-Based Optimizer & Prometheus Telemetry (`TASK-048`–`TASK-050`)**: `QueryBuilder::explain()` cardinality ordering, $O(1)$ zero-cardinality short-circuiting, lock-free 64-bucket `LatencyHistogram`, and `/metrics` Prometheus exposition.
  - **On-Disk I/O, Slot Recycling, WAL Compression & Incremental Checkpoints (`TASK-051`–`TASK-054`)**: Single-syscall positional `write_all_at` + lazy superblock fast-forward recovery, $O(1)$ 64B size-class dead-slot reuse (`reclaim_dead_slot`), transparent LZ4 WAL frame compression (`>= 1 KiB`), and incremental $O(\Delta)$ `STGEDLT2` delta checkpoints (`primary.idx.delta`).
  - **Engine-Level Set Filtering, Bidirectional Sorting & Keyset/Offset Pagination (`TASK-055`–`TASK-056`)**: 64-bit `POPCNT`-accelerated bitmap `AND NOT` / `OR` windowing (`filter_not_tag`, `filter_any_tag`), bidirectional `SortOrder::{Asc, Desc}` B+Tree range scans, $O(\log N)$ `KeysetCursor` + `offset`/`limit` pagination (`QueryPage<T>`), zero-copy `execute_page_filtered_with` (`|archived: &T::Archived|`), and paginated `OP_QUERY_HYBRID` TCP RPC.
  - **Full Filtering Coverage (`TASK-057`–`TASK-058`)**: $O(\log N)$ relational operators (`filter_eq`, `filter_ne`, `filter_gt`, `filter_gte`, `filter_lt`, `filter_lte`, `filter_not_range`, `filter_in_i64`), prefix/key-set matchers (`filter_tag_prefix`, `filter_id_prefix`, `filter_ids`, `filter_not_ids`), field existence checks (`filter_has_tag_field`, `filter_missing_tag_field`, `filter_has_numeric_field`, `filter_missing_numeric_field`), hybrid graph neighborhood filtering (`filter_within_bfs`), zero-allocation `ArchivedString` / `RecordView` string matchers, and composable `.filter(|view: &RecordView<'_, T>| -> bool)` across all terminal query and vector search methods.

---

## 5. Post-Audit Re-Verification (`TASK-067` – `TASK-079`, 2026-10-20)

Command: `cargo run --release -p storage-benches`. The development machine carried ~50% background CPU load from other applications during this session, so single runs varied by up to 4x even for untouched pure-CPU code (`InlineId` hash 1–6 ns, checksum 1.6–3.2 µs). Baseline (`4a004ec`) and candidate were therefore compared with **interleaved A/B runs** of a direct-timing probe (millions of iterations per operation, 3 pairs), which is the authoritative comparison below.

### 5.1 Zero-Allocation & Footprint Gates (all pass)

| Gate | Result |
| :--- | :--- |
| `with_ref` / `get_ref` / `RecordView` read allocations | **`0.00` / op** |
| `update` / secondary index / TCP `GET` (client) allocations | `5.00` / `1.00` / `3.00` per op (unchanged from baseline) |
| `ArchivedAgentObjectRecord` / `PrimaryEntry` / `IndexSnapshot` / `MmapGuard` | `112 B` / `96 B` / `264 B` / `8 B` (unchanged) |
| `primary.idx` | `19.0 B/record` after Stage 19; `20.9 B/record` after `TASK-082` (checkpoint v3 stores each ordinal) |
| Write coalescing | `70.7–71.1%` physical write reduction (unchanged) |

### 5.2 Interleaved A/B Latency (median of 3 pairs, ns/op)

| Operation | Baseline `4a004ec` | After audit | Notes |
| :--- | ---: | ---: | :--- |
| `RecordView` decode + `get_str` (23-byte title) | `4.3` | `10.7` | Baseline `as_str` used `get_unchecked` + `from_utf8_unchecked`, which was UB on hostile offsets. Now bounds + UTF-8 checked per call (overlapping-word ASCII fast path, ~3x faster than `<[u8]>::is_ascii`). |
| `decode_unchecked` vs `decode_fast_or` (per decode) | `2.2–3.5` | `5.1–11.9` | Safe O(1) header, bounds, alignment and identity checks on every `RecordRef` deref. |
| `with_ref` (hot, in-RAM frame) | `328` | `271` | Parity (within noise). |
| `with_ref` (flushed, arena mmap) | `296` | `292` | Parity. |
| `get_ref` (flushed, arena mmap) | `427` | `533` | Within run-to-run noise (pairs: `513/533`, `317/370`, `427/624`); the only code difference on this path is the safe `decode_fast` (+3–8 ns). `RecordRef` no longer carries a 176-byte zeroed fallback (shared static zero page instead). |
| `update` (32 hot keys) | `27.7 µs` | `30.2 µs` | Parity within noise. |

Two regressions found by this gate were fixed before sign-off: the per-call `is_ascii` cost in `ArchivedString::as_str` (replaced by an overlapping-word check) and a 176-byte per-`RecordRef` zeroed fallback (replaced by a static zero page). Earlier in the audit, `AsyncMemoryFirst` per-write flush syscalls were also replaced by buffering plus background flushing.

### 5.3 Binary Size

| Binary (Windows x86_64, release) | Baseline `4a004ec` | After audit |
| :--- | ---: | ---: |
| `storage-server` | `516.0 KiB` | `582.0 KiB` |
| `storage-cli` | `537.5 KiB` | `582.0 KiB` |

The earlier `370.5 KiB` figure was stale; the baseline already exceeded the `RULE-MEM-03` budget (`< 500 KiB`). Linker-map attribution of the `+59 KiB` `.text` growth: `storage-server` `+32 KiB` (header-stage admission, admin job pool, Raft RPC wiring, environment security config), `std` `+15 KiB` (threads and mpsc channels for the admin pool), WAL `+3.5 KiB`, mmap `+7 KiB`, index `+5 KiB`. Open item: bring the binaries back under budget without sacrificing hot-path speed (`opt-level` must stay `3`).
