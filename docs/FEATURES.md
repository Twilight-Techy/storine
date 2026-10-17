# Features Specification: High-Performance Zero-Copy Object Storage Engine

## 1. Core Storage & Zero-Copy Memory Engine

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-CORE-01** | **Zero-Copy `mmap` Read Path** | Reads return a lifetime-bound `RecordRef<'a, T>` pointing directly into the OS virtual memory page (`&T::Archived`) with zero heap allocations and zero deserialization copies. | v0.1 |
| **F-CORE-02** | **64-Byte Cache-Line Aligned Record Header** | Every record is prefixed with a `#[repr(C, align(64))]` binary header storing magic bytes, format version, schema version, monotonic `record_version`, nanosecond timestamp, payload length, and `XXH3-64` checksum. | v0.1 |
| **F-CORE-03** | **Stack-Inlined Primary Keys (`InlineId`)** | Primary keys up to 23 bytes are stored entirely inline on the stack/struct without heap pointer indirection, eliminating allocator overhead on key lookups and relationship lists. | v0.1 |
| **F-CORE-04** | **Sharded Record-Per-File Storage** | Each record is isolated in a 2-level BLAKE3/XXH3 hex-sharded directory (`<collection>/<h1>/<h2>/<id>.rec`) to prevent filesystem directory bottlenecking while preserving per-record isolation. | v0.1 |
| **F-CORE-05** | **Hierarchical Parent-Folder Grouping** | Tightly coupled child records can be physically grouped under their parent's folder path or packed into a contiguous **Hierarchy Bundle** so related records share OS page-cache locality. | v0.2 |
| **F-CORE-06** | **Adaptive Slotted Arena Segments** | Small records (`< 4 KB`) can automatically pack into 64 MB memory-mapped Arena Segments (`seg_XXXXX.dat`) at 64-byte alignment to eliminate inode exhaustion and 4 KB page-slack waste at 10M+ record scale. | v0.6 |
| **F-CORE-07** | **Bounded `MmapPool` & FD Eviction** | Strictly enforces `max_open_files` and `max_active_mmaps` budgets via a lock-free W-TinyLFU / Clock cache, automatically unmapping cold regions when `active_readers == 0`. | v0.2 |
| **F-CORE-08** | **Platform-Native OS Page Hints** | Issues `madvise` (`MADV_WILLNEED`, `MADV_RANDOM`, `MADV_SEQUENTIAL`, `MADV_DONTNEED`) on Unix and `PrefetchVirtualMemory` on Windows to optimize kernel page-cache readahead and eviction. | v0.2 |

---

## 2. Concurrency, Mutation & Async Persistence

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-CONC-01** | **Controlled Closure Mutation API** | Mutations execute through `db.update::<T>(id, \|rec: &mut T\| ...)` rather than raw pointer writes, guaranteeing validation, version increments, checksum calculation, and index updates. | v0.1 |
| **F-CONC-02** | **Immediate In-Memory Visibility (`HotDirtyArc`)** | As soon as a mutation closure completes, the newly serialized `rkyv` buffer is atomically published via `ArcSwap` in the primary index. Subsequent readers immediately see the updated state in RAM before disk I/O finishes. | v0.3 |
| **F-CONC-03** | **Per-Record Serialized Write Queues** | Writes to the same record ID are serialized through a fine-grained striped write queue; writes to different record IDs execute completely in parallel with zero global lock contention. | v0.3 |
| **F-CONC-04** | **Automatic Write Coalescing** | Rapid burst updates to the same record coalesce in memory so the background disk flusher writes only the latest converged version to the record file, slashing SSD write amplification by 10x–100x under hot-key contention. | v0.3 |
| **F-CONC-05** | **Atomic File Replace & Ping-Pong Slots** | Supports both classic atomic temp-file + rename replacement and pre-allocated 2-slot Ping-Pong record files (toggling the active slot via atomic header update without directory metadata syscalls). | v0.3 |
| **F-CONC-06** | **Optimistic Concurrency Control (OCC)** | Every mutation increments the 64-bit `record_version`. Supports `db.update_if_version(id, expected_ver, \|rec\| ...)` for lock-free CAS semantics across application workflows. | v0.3 |
| **F-CONC-07** | **Multi-Record Atomic Transactions** | Supports `db.transaction(\|txn\| ...)` across multiple records by acquiring per-record locks in canonical sorted `RecordId` order (deadlock-free) and committing atomically via a single WAL batch. | v0.6 |

---

## 3. Durability, Integrity & Crash Safety

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-DURA-01** | **Group-Commit Write-Ahead Log (WAL)** | Batches concurrent mutations from multiple threads into a single sequential WAL append and `fsync`, achieving high durability throughput even on consumer NVMe drives. | v0.3 |
| **F-DURA-02** | **Configurable Durability Policies** | Per-collection or per-write `SyncPolicy`: `Immediate` (fsync WAL before return), `GroupCommit { max_delay_us }` (batched fsync), or `AsyncMemoryFirst` (immediate RAM return, background flush). | v0.3 |
| **F-DURA-03** | **Hardware-Accelerated Checksums** | Every record header and WAL frame is protected by `XXH3-64` / hardware SIMD `CRC32C` checksums to detect bit-rot, torn writes, or disk corruption immediately. | v0.1 |
| **F-DURA-04** | **Deterministic Crash Recovery & Replay** | On startup, scans WAL segments since the last checkpoint, verifies frame checksums, replays un-flushed record mutations, and cleans up orphaned `.tmp` files in milliseconds. | v0.3 |
| **F-DURA-05** | **Once-Per-Map `rkyv` Bytecheck Validation** | Validates untrusted or newly mapped disk bytes using `rkyv::check_archived_root` once upon mapping, then sets a verified flag so all subsequent hot reads skip bounds/pointer validation. | v0.1 |

---

## 4. Primary & Secondary Indexing

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-INDX-01** | **Sharded SwissTable Primary Index** | 64-shard `hashbrown` primary key table hashed with `XXH3`, mapping `InlineId -> RecordLocation` (file/segment, offset, length, version, hot-buffer pointer) in `< 30 ns`. | v0.1 |
| **F-INDX-02** | **Memory-Mapped Primary Index Checkpoints** | Periodically checkpoints the primary key directory into a compact `primary.idx` mmap file for near-instantaneous database startup without directory tree walks. | v0.2 |
| **F-INDX-03** | **Cache-Conscious B+Tree Range Indexes** | Secondary indexes on timestamps (`u64`/`i64`), floats (`OrderedFloat`), and numeric fields supporting fast `range(start..=end)`, `gt`, `lt`, and ordered top-$K$ scans without scanning record files. | v0.4 |
| **F-INDX-04** | **RoaringBitmap Tag & Equality Indexes** | Compressed bitmap indexes mapping discrete enum/tag/status values to dense 32-bit `RecordOrdinal`s for sub-microsecond multi-condition `AND` / `OR` / `NOT` filtering. | v0.4 |
| **F-INDX-05** | **Differential Atomic Index Maintenance** | During `db.update()`, diffs old vs. new indexed field values and mutates secondary indexes **only** when the indexed attribute actually changed, atomically tied to the record write. | v0.4 |

---

## 5. Explicit Relationships & Graph Navigation

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-REL-01** | **Inline Zero-Copy Relationship Containers** | Native `OneToOne<T>`, `OneToMany<T>`, and `ManyToMany<T>` containers stored directly inside parent/child records as sorted `ArchivedVec<InlineId>`—no SQL join tables required. | v0.5 |
| **F-REL-02** | **Atomic Bidirectional Link/Unlink** | `db.link(parent_id, child_id)` and `db.unlink(parent_id, child_id)` atomically update both records' relationship lists in canonical lock order so forward and reverse edges never drift out of sync. | v0.5 |
| **F-REL-03** | **Configurable Referential Integrity (`OnDelete`)** | Enforces `OnDelete::Restrict` (reject deletion if children exist), `OnDelete::Cascade` (recursively delete dependent child records), or `OnDelete::Unlink` (cleanly remove stale IDs from peers). | v0.5 |
| **F-REL-04** | **Zero-Copy Graph & Hierarchy Traversal** | Lazy iterator API (`db.get_related::<Child>(&parent.posts)`) that prefetches child `mmap` pages in batch (`madvise(MADV_WILLNEED)`) and yields zero-copy `RecordRef<'a, Child>` views. | v0.5 |
| **F-REL-05** | **Orphan Detection & Integrity Verifier** | Online and CLI utility that scans relationship edges across collections using RoaringBitmaps of valid ordinals to detect and repair any dangling references in $O(E)$ time. | v0.5 |

---

## 6. Lifecycle, Operations & AI-First Extensions

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-OPS-01** | **Zero-Downtime Schema Evolution** | Records carry `schema_version` in their 64-byte header; registered migration functions transparently upgrade older record versions on read or during background compaction. | v0.6 |
| **F-OPS-02** | **TTL & Automatic Expiration** | Optional per-record TTL timestamp indexed in a hierarchical timing wheel / min-heap for automatic background expiration of ephemeral caches, sessions, and short-term AI memories. | v0.6 |
| **F-OPS-03** | **Cold-Record LZ4 / Zstd Dictionary Compression** | Transparently compresses cold records (with trained per-collection Zstd dictionaries) while keeping hot records uncompressed and directly memory-mapped. | v0.7 |
| **F-OPS-04** | **Point-in-Time Snapshots & Hot Backup** | Creates consistent point-in-time snapshots using hardlinks / copy-on-write segment freezes and WAL watermarks without pausing readers or writers. | v0.7 |
| **F-OPS-05** | **AI Memory & SIMD Vector Slice Support** | Native support for 64-byte-aligned fixed-dimension embedding arrays (`ArchivedVec<f32>` / `i8`) inside records with SIMD (AVX2/AVX-512/NEON) dot-product/cosine similarity filtering combined with metadata indexes. | v0.7 |
| **F-OPS-06** | **Lock-Free Telemetry & Latency Histograms** | Built-in thread-local counters for cache hit rate, active mmaps, resident bytes, WAL batch size, and coalescing ratio, exportable with zero allocation overhead. | v0.6 |
| **F-OPS-07** | **CLI & Optional Network Server Mode** | `storage-cli` for database inspection, benchmarking, and compaction, plus `storage-server` exposing a length-prefixed binary protocol over TCP/UDS for multi-process deployment. | v0.8 |
