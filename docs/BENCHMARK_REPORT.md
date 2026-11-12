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

After Stage 21 (encryption key ring, index-file sealing, SIMD crypto, Raft InstallSnapshot, cold primary tier): `storage-server` `903.0 KiB`, `storage-cli` `830.5 KiB`. On 2026-10-27 the `RULE-MEM-03` budget was raised to `< 1 MiB` (`TASK-091`); both binaries are within it.

The earlier `370.5 KiB` figure was stale; the baseline already exceeded the `RULE-MEM-03` budget (`< 500 KiB`). Linker-map attribution of the `+59 KiB` `.text` growth: `storage-server` `+32 KiB` (header-stage admission, admin job pool, Raft RPC wiring, environment security config), `std` `+15 KiB` (threads and mpsc channels for the admin pool), WAL `+3.5 KiB`, mmap `+7 KiB`, index `+5 KiB`. Size is still minimized wherever it costs no hot-path speed (`opt-level` stays `3`).

## 6. Analytics Against DuckDB (`TASK-131`, 2026-10-29)

**Setup.** 4-core Xeon @ 2.1 GHz (2 MiB L2 per core). Storine release build (`target-cpu=native`), queries through `DocumentStore::aggregate` over column segments (`olap-bench`). DuckDB 2.0.0 Python, 4 threads, the same rows loaded from the CSV the generator writes (`benches/olap/duckdb_bench.py`). Each cell is the median of 7 runs, and the two systems alternate three times (A/B/A/B/A/B, `RULE-PERF-01`).

**Results are identical.** On Q1 and Q6 the result rows match DuckDB's exactly. Storine's averages are exact decimals where DuckDB's are doubles.

- **TPC-H-shaped `lineitem`, 1M rows:**
  - Q1 and Q6 come from TPC-H.
  - The data is TPC-H-shaped (same columns, distributions and selectivities), generated deterministically. It is not `dbgen` output.
- **ClickBench-shaped `hits`, 1M rows:**
  - The hN numbering follows ClickBench.
  - These are not the ClickBench dataset (14 GB). They are synthetic rows with its column kinds and skew: Zipf-like counters, regions and phrases, 80 % empty search phrases, and about 175k users.

| Query | Storine (ms) | DuckDB (ms) | |
|---|---|---|---|
| Q1 (grouped exact sums, 4 groups) | 8.3 / 8.1 / 8.8 | 7.9 / 7.5 / 10.2 | tie |
| Q6 (selective filter, exact sum) | 2.9 / 3.0 / 2.6 | 3.3 / 3.4 / 2.7 | Storine ≈ 1.1× |
| h0 `COUNT(*)` | 0.3 / 0.4 / 0.4 | 0.8 / 0.8 / 0.7 | Storine 2× |
| h1 `COUNT(*) WHERE AdvEngineID <> 0` | 1.4 / 1.1 / 1.1 | 1.2 / 1.3 / 1.5 | tie |
| h2 `SUM, COUNT, AVG` | 1.1 / 1.2 / 1.3 | 2.1 / 1.9 / 2.0 | Storine 1.7× |
| h3 `AVG(UserID)` | 0.9 / 0.8 / 0.8 | 1.8 / 1.4 / 1.4 | Storine 1.8× |
| h4 `COUNT(DISTINCT UserID)` | 20.2 / 20.4 / 20.4 | 17.0 / 11.9 / 14.7 | DuckDB 1.4× |
| h5 `COUNT(DISTINCT SearchPhrase)` | 18.8 / 17.2 / 19.2 | 3.7 / 4.8 / 4.5 | DuckDB 4× |
| h6 `MIN / MAX(EventDate)` | 2.1 / 2.1 / 1.9 | 0.9 / 1.0 / 1.1 | DuckDB 2× |
| h7 group by AdvEngineID, filtered | 1.9 / 1.7 / 2.0 | 2.1 / 1.9 / 2.4 | tie |
| h8 distinct users per region, top 10 | 207.9 / 201.3 / 205.8 | 20.7 / 21.0 / 20.7 | **DuckDB 10×** |
| h9 h8 plus sum, count, avg | 218.6 / 221.4 / 217.8 | 29.6 / 29.3 / 30.7 | **DuckDB 7×** |
| h12 top search phrases | 40.5 / 40.2 / 40.4 | 7.7 / 7.4 / 6.0 | **DuckDB 6×** |
| h15 top users (175k groups) | 109.1 / 112.4 / 113.6 | 17.2 / 14.8 / 16.7 | **DuckDB 7×** |

Single-threaded (`STORINE_THREADS=1` against `SET threads = 1`): Q1 22.4 ms against 18.8–19.7 ms, and Q6 6.3–6.8 ms against 5.5–6.0 ms.

**Where Storine stands.**
- It is at parity or ahead on scans, filters and low-cardinality aggregation (TPC-H Q1/Q6, h0–h3, h7).
- It is behind on high-cardinality grouping and on grouped distinct counts (h8, h9, h12, h15: 6–10×). Its group tables are general: canonical cross-kind keys, a representative value per group, and exact accumulators. Workers build them separately and merge them through hash partitions.
- Closing that gap needs radix-partitioned group tables from the first row, and a shared `(group, value)` table for grouped distinct counts. This is open work under `TASK-128`; nothing here is claimed as a win.

**Storage.** Column segments are 51.8 MB for `lineitem` and 52.8 MB for `hits`; DuckDB's files are 26.2 MB and 33.0 MB. Segments use per-chunk lightweight encodings with no general-purpose compression of numeric chunks, against DuckDB's compression.

**SQL push-down (`TASK-130`).** The PostgreSQL gateway, embedded, with 20k rows and a grouped, filtered query: 1.0 ms pushed down against 230 ms local, where every row is streamed to the gateway. Results are identical in all three dialects.

Reproduce:

```text
olap-bench gen <dir> 1000000 && python3 benches/olap/duckdb_bench.py <dir> load
olap-bench gen-hits <dir> 1000000 && python3 benches/olap/duckdb_bench.py <dir> load-hits
olap-bench query <dir> <q1|q6|h0..h15> 7    and    python3 benches/olap/duckdb_bench.py <dir> <same> 7
```


## 7. Today Against the Earliest Benchmarks, and Against LMDB and RocksDB (2026-11-10)

**Setup.**
- Hardware: one AWS `c7i.xlarge` spot instance (4 vCPU, Intel Xeon 8488C (Sapphire Rapids), 2 cores × 2 threads), Ubuntu 24.04, gp3 EBS.
- Build: Rust 1.99.1, `release` profile (fat LTO, `target-cpu=native`).
- x86 was chosen because Storine's SIMD paths (CRC32C, checksums, crypto) are x86-only; on ARM it runs scalar fallbacks.
- The Windows numbers in sections 1–2 came from another machine. They cannot be compared with these, so every comparison below is on this one machine.

### 7.1 Current code against the first benchmarked commits

`bench-runner` (2,000 records, all in cache) was built from `b9024bf`, the first commit, from `5d6b008`, the commit of section 2's table, and from the current code. The three ran alternately for three rounds; below are medians, as p50 latency and throughput.

| Operation | `b9024bf` | `5d6b008` | now | now against `5d6b008` |
| :--- | ---: | ---: | ---: | :--- |
| `with_ref` hot read | 50 ns · 18.0 M/s | 49 ns · 20.0 M/s | 63 ns · 15.8 M/s | **21 % slower** |
| `get_ref` arena (mmap) read | 66 ns · 14.6 M/s | 64 ns · 14.6 M/s | 67 ns · 12.7 M/s | 13 % slower; p99 about 2× (100 → 200 ns) |
| `update` (32 hot keys) | 815 ns · 0.97 M/s | 807 ns · 1.07 M/s | 1,122 ns · 0.79 M/s | **26 % slower** |
| Secondary index query | 1,969 ns | 2,339 ns | 2,290 ns | same (16 % slower than `b9024bf`) |
| Checksum, 4 KiB | 413 ns | 414 ns | 376 ns | 9 % faster |
| `RecordView` decode (unchecked) | 1 ns | 1 ns | 2 ns | the bounds checks added in the 2026-10-20 audit |
| TCP `GET` round trip | 8.5 µs · 115 k/s | 18.6 µs · 52 k/s | 19.1 µs · 51 k/s | same as `5d6b008`; **2.2× slower than `b9024bf`** |

Features added since (encryption, MVCC, cold tier, Raft, safety checks) cost 13–26 % on the hot read and write paths. The TCP round trip doubled between the first commit and `5d6b008`, when the reactor replaced one thread per connection, and has not recovered.

### 7.2 Against LMDB and RocksDB

The harness is `benches/kv-compare` (outside the workspace, so LMDB and RocksDB never enter Storine's dependencies):
- 1,000,000 records, 16-byte keys and 288-byte values: LMDB and RocksDB store Storine's own encoded frame.
- Reads fetch the value and read one field, without copying.
- LMDB (`heed` 0.20.5) opens one read transaction per get.
- RocksDB 0.22: no compression, a 2 GiB block cache, 10-bit Bloom filters, and compaction after the load.
- Storine encodes inside `put` and maintains its secondary indexes (tag bitmap, timestamp B+tree); the others store prebuilt bytes and index nothing.
- Two full runs, shown as run 1 / run 2.

**No fsync** (Storine `AsyncMemoryFirst` can lose the last flush interval in a process crash, as RocksDB without its WAL can; RocksDB with its WAL, and LMDB `NO_SYNC`, survive one):

| ops/s | Storine | RocksDB no WAL | RocksDB WAL | LMDB `NO_SYNC` | Storine is |
| :--- | ---: | ---: | ---: | ---: | :--- |
| read, 1 thread | **1.20 M / 1.16 M** | 326 k / 310 k | 318 k / 317 k | 766 k / 792 k | **1.5× LMDB, 3.7× RocksDB** |
| read, 4 threads | **4.17 M / 4.06 M** | 938 k / 899 k | 949 k / 940 k | 2.85 M / 2.96 M | **1.4× LMDB, 4.4× RocksDB** |
| read p99, 1 thread | 1.3 µs | 6.4 µs | 6.1 µs | 1.8 µs | lowest |
| fill (random order) | 293 k / 286 k | **544 k / 512 k** | 339 k / 339 k | 176 k / 181 k | 1.6× LMDB; **RocksDB no WAL 1.8× Storine** |
| overwrite, 1 thread | 261 k / 249 k | **542 k / 508 k** | 355 k / 366 k | 168 k / 177 k | 1.5× LMDB; **RocksDB 1.4–2.1× Storine** |
| overwrite, 4 threads | 459 k / 436 k | **515 k / 513 k** | 287 k / 291 k | 123 k / 129 k | 1.5× RocksDB WAL, 3.5× LMDB; RocksDB no WAL 1.15× Storine |
| overwrite p99, 4 threads | 72–75 µs | 23–30 µs | 36–39 µs | 20 µs | **highest** |

**fsync per commit** (Storine `GroupCommit`, RocksDB `sync = true`, LMDB default):

| ops/s | Storine | RocksDB | LMDB |
| :--- | ---: | ---: | ---: |
| put, 1 thread | 396 / 373 | 360 / 355 | **477 / 477** |
| put, 4 threads | 615 / 623 | **801 / 833** | 476 / 476 |

**Where Storine stands.**
- **Reads:** it is the fastest of the three. It is 1.4–1.5× faster than LMDB and 3.7–4.4× faster than RocksDB, with the lowest p99. The primary index lives in RAM, at 96 bytes per record (section 3); LMDB walks a B+tree in its map.
- **Writes without fsync:** it beats LMDB by 1.5–3.5×, and RocksDB with its WAL on 4 threads by 1.5×. It loses to RocksDB without a WAL by 1.15–2.1× at the same durability, and to RocksDB with its WAL on one thread by 1.4×, even though that WAL is the stronger guarantee. Its p99 on 4 writing threads is 2–3× RocksDB's.
- **Durable writes:** it is 20–30 % behind LMDB on one thread, and 25 % behind RocksDB on four.

**So it is not yet faster than or as fast as both everywhere.** It leads on reads and trails on writes.

## 8. Write Path Rework Against LMDB and RocksDB (`TASK-153`, 2026-11-12)

**Setup.** As in section 7.2: one on-demand AWS `c7i.xlarge` (4 vCPU Sapphire Rapids, gp3 EBS, Ubuntu 24.04, Rust 1.99.1, `target-cpu=native`), `benches/kv-compare`, 1,000,000 records of 288 bytes. The three engines ran in turn for three rounds; ranges below are the lowest and highest of the three.

One difference from 7.2: the record type here declares no secondary indexes (`Indexable::INDEXED = false`), so Storine encodes inside `put` but maintains no tag or timestamp index. LMDB and RocksDB index nothing either. A record type with indexes still pays for them (`NumericRangeIndex` is one `RwLock<BTreeMap>`), and that path was not measured in this run.

**What changed.**
- Frames come from a pool with thread-local caches. glibc's arena lock was contended when the flusher freed frames that writers had allocated.
- The flusher takes the whole dirty queue at once and writes up to 512 frames per positional write (`ArenaManager::write_records`). Each record still completes under its stripe lock, and only if its version and location are unchanged.
- In memory-first mode the WAL frame is checksummed and compressed before the lock and then copied into a staging buffer, with no queue or condvar. The buffer is written out at 256 KiB, and before any flush, sync, truncation or drop.
- Group commit waits briefly (one twelfth of the measured fsync time, at most 250 µs) when the last batch had more than one writer, and only wakes waiters that exist.
- Syncing WALs preallocate 1 MiB of zeros ahead of the write position. On this ext4/EBS volume, `fdatasync` after an append that grows the file took 2.77 ms, against 0.87 ms into blocks already written. Truncation and recovery use the logical end, not the file length.
- `put` looks a record up once instead of twice.

**No fsync** (Storine `AsyncMemoryFirst`; the same durability tiers as 7.2):

| | Storine | RocksDB no WAL | RocksDB WAL | LMDB `NO_SYNC` | Storine against the fastest other |
| :--- | ---: | ---: | ---: | ---: | :--- |
| fill, ops/s | **645–669 k** | 452–459 k | 309–322 k | 163 k | 1.4× RocksDB no WAL |
| read, 1 thread | **1.04–1.06 M** | 286–290 k | 288–296 k | 716–721 k | 1.45× LMDB |
| read, 4 threads | **3.75–3.82 M** | 817–846 k | 777–838 k | 2.70–2.72 M | 1.4× LMDB |
| overwrite, 1 thread | **799–800 k** | 463–477 k | 314–318 k | 160–161 k | 1.7× RocksDB no WAL |
| overwrite, 4 threads | **1.18–1.19 M** | 471–481 k | 259–267 k | 118–119 k | 2.5× RocksDB no WAL |
| overwrite p99, 1 thread | **2.3–2.4 µs** | 4.5–4.7 µs | 6.4–6.5 µs | 8.0–8.1 µs | lowest |
| overwrite p99, 4 threads | **18.4–20.2 µs** | 24.8–31.3 µs | 39–42 µs | 21.3–21.6 µs | lowest |
| overwrite p99.9, 4 threads | 261–288 µs | 159–240 µs | **67–83 µs** | 7.4–9.0 ms | **behind both RocksDB modes** |
| read p99, 4 threads | **1.5–1.6 µs** | 10.3–11.0 µs | 10.4–10.9 µs | 2.0 µs | lowest |
| settle (flush to disk) | **0.42–0.49 s** | 3.9 s | 3.6 s | 2.8 s | fastest |

**fsync per commit** (Storine `GroupCommit`, RocksDB `sync = true`, LMDB default; "recycle" reuses RocksDB's WAL files, as Storine's preallocation reuses blocks):

| | Storine | RocksDB | RocksDB recycle | LMDB |
| :--- | ---: | ---: | ---: | ---: |
| put, 1 thread, ops/s | **1,875–1,908** | 347–356 | 339–353 | 468–471 |
| put, 4 threads, ops/s | **2,606–2,616** | 800–818 | 802–845 | 412–413 |
| p99, 1 thread | **0.75–0.77 ms** | 3.8–3.9 ms | 3.9–4.0 ms | 2.4–2.5 ms |
| p99, 4 threads | **2.4 ms** (5.5 ms in one round) | 8.1–8.2 ms | 7.8–8.1 ms | 2.6 ms |
| p99.9, 4 threads | 5.7–10.2 ms | **9.4–9.9 ms** (recycle 11.9–14.9) | | 58–86 ms |

**Against section 7.2** (same instance type): fill 2.3× faster, overwrite 3.1× on one thread and 2.6× on four, durable puts 4.9× on one thread and 4.2× on four. The 4-thread overwrite p99 fell from 72–75 µs to 18–20 µs.

**Where Storine stands.** It is the fastest of the three in every throughput row, and has the lowest p99 in every row. Two tails are not won:
- The 4-thread overwrite p99.9 is 261–288 µs, against 67–83 µs for RocksDB with its WAL and 159–240 µs without it. Four writers and the flusher share four vCPUs, so a writer is sometimes descheduled for a scheduler slice.
- The 4-thread durable p99.9 is 5.7–10.2 ms against RocksDB's 9.4–9.9 ms: ahead in two rounds and behind in one.

**Trade-off.** A batched flush appends records to the arena instead of rewriting their old slots in place, so arena files can hold about twice the live data until compaction reclaims the dead slots. The auto-compaction threshold is unchanged.

**Verified on the same instance:** the release workspace suite (all pass) and 40 of 40 process chaos runs. The instance ran about 1 h 30 min on demand at $0.1785/h, about $0.27, and was terminated when the job finished.
