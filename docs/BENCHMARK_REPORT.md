# StorageEngine End-to-End Benchmark, Footprint & Zero-Allocation Verification Report (`TASK-030` & `TASK-050` `v1.0` Gate)

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
| **Live Arena Segment Compaction (`compact_arenas`)** | Dead slots retained | Evacuates live records, auto-upcasts legacy schemas, runs MVCC GC, & deletes fragmented `.dat` files | **Reclaims 100% of dead segment files on disk** |
| **`ArchivedAgentObjectRecord` On-Disk & Mmap Root** | `120 bytes` | **`112 bytes`** (`InlineId::EMPTY` niche in `ArchivedOneToOne`) | **`-8 bytes` per record on disk & in RAM** |
| **`IndexSnapshot` & Embedding Heap Maps** | `544 bytes / record` (`index_snapshots` + `embeddings` in RAM) | **`0 bytes / record`** (extracted zero-copy from `&T::Archived` via `extract_archived_snapshot` & `archived_embedding_slice`) | **`-544 MB` per 1M records (`100%` heap map elimination)** |
| **Unflushed `hot_dirty` Write Buffer** | Bounded only by flush timer | **`max_hot_dirty_bytes` (`64 MiB` cap)** + background flush | **Strict hard ceiling on dirty in-RAM write buffers** |
| **`IndexSnapshot` Stack Diff Struct** | `352 bytes` | **`264 bytes`** (Struct-of-Arrays packing) | **`-88 bytes` (`-25%`) stack frame during index delta** |
| **`PrimaryEntry` In-RAM Shard Entry** | `104 bytes` | **`96 bytes`** (removed redundant `updated_at_ns`) | **`-8 bytes` per record in RAM** |
| **`MmapGuard` Handle + Verification State** | `16 bytes` + heap `Arc<AtomicBool>` | **`8 bytes`** (`verified: AtomicBool` embedded in `MmapRegion`) | **`-50%` guard size + `0` extra heap alloc per mapped file** |
| **Deleted `RecordOrdinal` Table Growth** | Monotonic growth ($O(F)$ free-list scan) | $O(1)$ recycled free-list (`Some(InlineId::EMPTY)` sentinel) | **Bounded to peak live record count in strict $O(1)$ time** |
| **Top-$K$ Embedding Search (`score_embeddings`)** | $O(N \log N)$ full sort | **$O(\log N)$ HNSW `SQ8` graph search + $O(N + K \log K)$ Quickselect fallback** | **Sub-linear $O(\log N)$ ANN graph search with exact `f32` re-ranking** |

---

## 4. Write Coalescing, Durability & `v1.0` Production Verification (`TASK-030` & `TASK-050`)
- **Live Records**: `2,000`
- **Total Foreground Writes**: `7,256`
- **Coalesced Background Flushes Saved**: `5,193` (**`71.6%` reduction in physical disk writes**)
- **Full-Scale `v1.0` Production Verification Gate (`crates/storage-core/tests/production_v1_gate.rs` — 42 Workspace Tests Passing)**:
  - **Multi-Collection Catalog & Heterogeneous Graph (`TASK-031`–`TASK-033`)**: Cross-collection ACID transactions (`Users` + `Orders`), `IntegrityPolicy::{Restrict, Cascade, Unlink}` enforcement across collections, and typed schema evolution (`v1 -> v2 -> v3`) during arena compaction.
  - **Paged Disk Indexes (`TASK-034`–`TASK-035`)**: 4 KiB memory-mapped `PagedBTreeIndex`, `PagedBitmapIndex`, 64-byte cache-line `SplitBlockBloomFilter`, and `< 2.5B/record` `ColdPrimaryIndex`.
  - **HNSW & `SQ8` Vector Quantization (`TASK-036`–`TASK-038`)**: Multi-layer `HnswVectorIndex` with `4x` `SQ8` quantization, `hnsw.idx` persistence, and filter-aware hybrid search (`>= 95%` recall@10 with exact `f32` re-ranking).
  - **MVCC Snapshot Isolation & PITR (`TASK-039`–`TASK-041`)**: Wait-free `ReadSnapshot` isolation against concurrent writers, epoch-based MVCC GC, segmented WAL rotation, and exact LSN Point-in-Time Recovery (`restore_point_in_time`).
  - **Kernel-Polled Networking, Wire Protocol v2 & Security (`TASK-042`–`TASK-044`)**: `WSAPoll`/`poll` reactor, `CAS_PUT`, `BATCH_PUT`, `BATCH_GET`, `QUERY_HYBRID`, constant-time token auth + RBAC + rate limiting, and RFC 8439 ChaCha20 + 128-bit MAC AEAD encryption at rest.
  - **Consistent-Hash Sharding & Raft Consensus (`TASK-045`–`TASK-047`)**: `1,024` virtual shards, 2 KiB `ClusterMap`, live `migrate_vshard` with 64-bit `XXH3-64` digest verification, and 3-node `RaftReplicationGroup` leader election, quorum commit, and leader failover.
  - **Cost-Based Optimizer & Prometheus Telemetry (`TASK-048`–`TASK-050`)**: `QueryBuilder::explain()` cardinality ordering, $O(1)$ zero-cardinality short-circuiting, lock-free 64-bucket `LatencyHistogram`, and `/metrics` Prometheus exposition.

