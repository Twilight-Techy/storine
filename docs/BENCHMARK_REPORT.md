# StorageEngine End-to-End Benchmark, Footprint & Zero-Allocation Verification Report (`TASK-030`)

## 1. Build & Hardware Profile
- **Compiler Profile**: `release` (`opt-level = 3`, `lto = "fat"`, `codegen-units = 1`, `panic = "abort"`, `strip = "symbols"`, `-C target-cpu=native`)
- **Platform**: Windows (`x86_64-pc-windows-msvc`), Native Win32 `CreateFileMappingW` / `MapViewOfFile` / `PrefetchVirtualMemory` + Hardware SSE4.2 CRC32C Intrinsics
- **Zero-Allocation Gate**: Enforced via `#[global_allocator] static GLOBAL_ALLOC: TrackingAllocator` (`benches/src/lib.rs`) using thread-local allocation and byte counters.

---

## 2. Latency Distribution (`p50`, `p99`, `p99.9`), Throughput & Allocation Verification

| Benchmark Workload | Throughput (`ops/sec`) | `p50` (`ns`) | `p99` (`ns`) | `p99.9` (`ns`) | Heap Allocs / Op |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **InlineId (24B) Hash + Eq** | `309,306,931 / s` | `3 ns` | `9 ns` | `9 ns` | **`0.00`** |
| **Zero-Copy `RecordView` Read (`decode_unchecked`)** | `315,300,767 / s` | `3 ns` | `6 ns` | `10 ns` | **`0.00`** |
| **SIMD Cosine Similarity (`64-dim f32`, 8-way unrolled)** | `11,572,960 / s` | `87 ns` | `106 ns` | `20,762 ns` | **`0.00`** |
| **`StorageEngine::with_ref` (0-Alloc Hot Read)** | **`3,126,316 / s`** *(+97.8% vs baseline)* | **`187 ns`** | `2,600 ns` | `18,812 ns` | **`0.00`** |
| **`StorageEngine::get_ref` (Mmap Arena Segment)** | **`1,978,552 / s`** *(+31.9% vs baseline)* | **`400 ns`** | `4,500 ns` | `6,300 ns` | **`0.00`** |
| **SIMD Checksum (`XXH3-64 + SSE4.2 CRC32C`, 4 KB)** | `448,492 / s` (`~1.84 GB/s`) | `1,900 ns` | `2,500 ns` | `44,100 ns` | **`0.00`** |
| **`StorageEngine::update` (Coalesced Hot Write)** | **`186,555 / s`** *(+28.6% vs baseline)* | **`4,600 ns`** (`4.6 µs`) | **`12,800 ns`** | `174,700 ns` | **`5.00`** |
| **Secondary Index (`Tag Bitmap + B+Tree Range`)** | **`92,866 / s`** | **`5,900 ns`** (`5.9 µs`) | `38,500 ns` | `224,400 ns` | **`1.00`** |
| **`StorageServer` TCP Binary `GET` Frame RPC (Sharded Non-Blocking Reactor)** | **`22,260 / s`** *(+102.8% vs baseline)* | **`34,000 ns`** (`34.0 µs`) | `308,400 ns` | `475,200 ns` | **`3.00`** *(client-side; `0.00` on server via `IoSlice`)* |

---

## 3. Minimal Storage Footprint Verification (Code Binary, On-Disk Data & In-RAM Data)

| Footprint Dimension | Before Optimization | After Optimization | Improvement |
| :--- | ---: | ---: | :--- |
| **`StorageServer` Per-Connection Memory** | `2 MiB` OS thread stack + `128 KB` `BufReader`/`BufWriter` per connection | **`~88 bytes` per idle `NonBlockingConn`** multiplexed on fixed `ReactorShard` workers (`2..=16` threads) | **`24,000x` smaller per idle connection (`~88 MB` vs `~2.1 TB` @ 1M connections)** |
| **`primary.idx` On-Disk Checkpoint (`STGEIDX2`)** | `88.0 bytes / record` (`176,016 B` @ 2k records) | **`19.0 bytes / record`** (`38,008 B` @ 2k records) | **`4.63x` smaller on disk** (varint + compact `InlineId` + 64B-unit arena offsets) |
| **Initial Arena Segment File (`seg_XXXXX.dat`)** | `4 MiB – 16 MiB` upfront | **`4 KiB` (`4,096 B` — 1 OS page)**, doubling on demand | **`1,024x – 4,096x` smaller initial disk footprint** |
| **Live Arena Segment Compaction (`compact_arenas`)** | Dead slots retained | Evacuates live records & deletes fragmented `.dat` files | **Reclaims 100% of dead segment files on disk** |
| **`ArchivedAgentObjectRecord` On-Disk & Mmap Root** | `120 bytes` | **`112 bytes`** (`InlineId::EMPTY` niche in `ArchivedOneToOne`) | **`-8 bytes` per record on disk & in RAM** |
| **`IndexSnapshot` & Embedding Heap Maps** | `544 bytes / record` (`index_snapshots` + `embeddings` in RAM) | **`0 bytes / record`** (extracted zero-copy from `&T::Archived` via `extract_archived_snapshot` & `archived_embedding_slice`) | **`-544 MB` per 1M records (`100%` heap map elimination)** |
| **Unflushed `hot_dirty` Write Buffer** | Bounded only by flush timer | **`max_hot_dirty_bytes` (`64 MiB` cap)** + background flush | **Strict hard ceiling on dirty in-RAM write buffers** |
| **`IndexSnapshot` Stack Diff Struct** | `352 bytes` | **`264 bytes`** (Struct-of-Arrays packing) | **`-88 bytes` (`-25%`) stack frame during index delta** |
| **`PrimaryEntry` In-RAM Shard Entry** | `104 bytes` | **`96 bytes`** (removed redundant `updated_at_ns`) | **`-8 bytes` per record in RAM** |
| **`MmapGuard` Handle + Verification State** | `16 bytes` + heap `Arc<AtomicBool>` | **`8 bytes`** (`verified: AtomicBool` embedded in `MmapRegion`) | **`-50%` guard size + `0` extra heap alloc per mapped file** |
| **Deleted `RecordOrdinal` Table Growth** | Monotonic growth ($O(F)$ free-list scan) | $O(1)$ recycled free-list (`Some(InlineId::EMPTY)` sentinel) | **Bounded to peak live record count in strict $O(1)$ time** |
| **Top-$K$ Embedding Search (`score_embeddings`)** | $O(N \log N)$ full sort | **$O(N + K \log K)$ Quickselect (`select_nth_unstable_by`)** | **Linear $O(N)$ partitioning + $O(K \log K)$ sort on top $K$** |
| **Release Binary: `storage-server.exe`** | Unstripped | **`379,392 bytes` (`370.5 KiB`)** | Zero external runtime crates; fat LTO + symbol stripping |
| **Release Binary: `storage-cli.exe`** | Unstripped | **`452,096 bytes` (`441.5 KiB`)** | Self-contained single binary |
| **Release Binary: `bench-runner.exe`** | Unstripped | **`452,608 bytes` (`442.0 KiB`)** | Self-contained benchmark harness |

---

## 4. Write Coalescing & Durability Telemetry
- **Live Records**: `2,000`
- **Total Foreground Writes**: `7,256`
- **Coalesced Background Flushes Saved**: `5,192` (**`71.6%` reduction in physical disk writes**)
- **Chaos Crash-Recovery Verification**:
  - `25` consecutive simulated power-loss cycles with injected torn trailing WAL frames (`[0x57, 0x41, 0x4C, 0x31, ...]`) and orphaned `.rec.tmp` files in `test_chaos_crash_simulation_and_wal_recovery` recovered `100%` of committed state with zero data loss or corruption.
  - Single-bit payload flip detection verified across both `RecordHeader` (`header_checksum` covering bytes `0..56`) and payload (`payload_checksum` combining 4-way `XXH3-64` and hardware SSE4.2 `_mm_crc32_u64`).
