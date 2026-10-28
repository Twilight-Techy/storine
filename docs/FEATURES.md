# Features Specification: Storine Full-Scale Production Storage Engine & Distributed Database

## 1. Core Storage & Zero-Copy Memory Engine (`v0.1 – v0.8` Completed)

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-CORE-01** | **Zero-Copy `mmap` Read Path** | Reads return a lifetime-bound `RecordView<'a, T>` / `RecordRef<T>` pointing directly into the OS virtual memory page (`&T::Archived`) with `0.00` heap allocations and zero deserialization copies (`187 ns` p50). | v0.1 |
| **F-CORE-02** | **64-Byte Cache-Line Aligned Record Header** | Every record is prefixed with a `#[repr(C, align(64))]` binary header storing magic bytes, format version, schema version, collection ID, monotonic `record_version`, nanosecond timestamp, payload length, and `XXH3-64 + CRC32C` header/payload checksums. | v0.1 |
| **F-CORE-03** | **Stack-Inlined Primary Keys (`InlineId`)** | Primary keys up to 23 bytes are stored entirely inline on the stack in a 24-byte `#[repr(C, align(8))]` struct (`3 ns` hash + equality) without heap pointer indirection. | v0.1 |
| **F-CORE-04** | **Sharded Record-Per-File Storage** | Large/isolated records are stored in a 2-level hex-sharded directory (`<collection>/<h1>/<h2>/<id>.rec`) with stack-formatted paths (`0` heap allocations). | v0.1 |
| **F-CORE-05** | **Hierarchical Parent-Group Co-Location** | Child records sharing a `parent_group` are physically co-located into the same arena segment group so related records share OS page-cache locality. | v0.2 |
| **F-CORE-06** | **4 KiB-Growable Slotted Arena Segments** | Records pack into 64-byte-aligned memory-mapped Arena Segments (`seg_XXXXX.dat`) starting at `4 KiB` (1 OS page) and doubling on demand up to `64 MiB`, with live background compaction reclaiming 100% of dead segment files. | v0.6 |
| **F-CORE-07** | **Bounded `MmapPool` & Clock-PRO Eviction** | Strictly enforces `max_open_files` and `max_mapped_bytes` budgets via a 4-shard Clock-PRO / Second-Chance cache, automatically unmapping cold regions when `active_guards == 0`. | v0.2 |
| **F-CORE-08** | **Platform-Native OS Page Hints** | Issues `madvise` (`MADV_WILLNEED`, `MADV_RANDOM`, `MADV_SEQUENTIAL`, `MADV_DONTNEED`) on Unix and `PrefetchVirtualMemory` on Windows. | v0.2 |
| **F-CORE-09** | **Full-Spectrum Zero-Copy Data Types** | Native zero-copy support for integers (`i8..i64`, `u8..u64`), 8B-aligned 128-bit numbers (`ArchivedI128`, `ArchivedU128`), floats (`f32`, `f64`), UB-safe `ArchivedBool` & `ArchivedChar`, `ArchivedUuid`, `ArchivedString`, `ArchivedBytes`, `ArchivedSlice<T>`, `ArchivedStringSlice`, and $O(\log N)$ binary-searched dynamic `ArchivedMap` (`PropertyValue`). | v0.8 |

---

## 2. Concurrency, Mutation & Async Persistence (`v0.1 – v0.8` Completed)

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-CONC-01** | **Controlled Closure Mutation API** | Mutations execute through `db.update(id, \|rec: &mut T\| ...)` guaranteeing validation, version increments, SIMD checksum calculation, and differential index updates. | v0.1 |
| **F-CONC-02** | **Immediate In-Memory Visibility (`hot_dirty`)** | Mutations immediately publish a 64-byte-aligned `AlignedBytes` frame into a 64-shard `hot_dirty` buffer (`4.6 µs` p50) so subsequent readers see the new state in RAM before disk flush finishes. | v0.3 |
| **F-CONC-03** | **2,048-Way Striped Per-Record Lock Table** | Writes to the same record ID serialize on its cache-line-padded stripe (`StripedLockTable`); writes to different keys execute in parallel across all CPU cores. | v0.3 |
| **F-CONC-04** | **Automatic Write Coalescing & Hard Dirty Cap** | Rapid burst updates to the same key coalesce in `WriteCoalescer` (`71.6%+` reduction in disk writes), bounded by `max_hot_dirty_bytes` (`64 MiB`) synchronous flush backpressure to prevent memory bloat. | v0.3 |
| **F-CONC-05** | **Atomic File Replace & Ping-Pong Double Slots** | Supports both atomic `.tmp` + rename replacement and pre-allocated 2-slot Ping-Pong record files (`[Slot 0 \| Slot 1]`). | v0.3 |
| **F-CONC-06** | **Optimistic Concurrency Control (`cas_update`)** | Every mutation increments the 64-bit `Version`. `db.cas_update(id, expected_ver, ...)` atomically rejects stale updates with `StorageError::CasConflict`. | v0.3 |
| **F-CONC-07** | **Deadlock-Free Multi-Record Transactions** | `db.transaction(\|txn\| ...)` and `db.update_pair_atomic(...)` lock keys in strictly ascending canonical `(CollectionId, InlineId)` order and commit atomically via a single WAL transaction batch. | v0.6 |

---

## 3. Durability, Integrity & Crash Safety (`v0.1 – v0.8` Completed)

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-DURA-01** | **Group-Commit Write-Ahead Log (WAL)** | Batches concurrent mutations from multiple threads into a single sequential WAL write and `fsync` (`GroupCommitWal`). | v0.3 |
| **F-DURA-02** | **Configurable Durability Modes** | `SyncPolicy::Immediate` (`fsync` per write), `SyncPolicy::GroupCommit` (one `fsync` per concurrent batch), and `SyncPolicy::AsyncMemoryFirst` (buffered in RAM, written to the OS by the background flusher, never `fsync`ed; a crash can lose the last flush interval). | v0.3 |
| **F-DURA-03** | **Hardware-Accelerated Dual Checksums** | Every `RecordHeader` (`header_checksum`) and payload (`payload_checksum`) is protected by a 4-lane XXH64-family hash combined with a dual-lane `CRC32C` (SSE4.2 instruction when available, bit-identical portable table otherwise, so the on-disk format never depends on build flags). | v0.1 |
| **F-DURA-04** | **Torn-Frame Crash Recovery & Replay** | On startup, scans WAL frames, truncates trailing torn frames from power loss, replays committed transactions, and cleans up orphaned `.tmp` files. | v0.3 |
| **F-DURA-05** | **Once-Per-Map Archive Verification** | Validates untrusted disk bytes once when mapped (`MmapRegion::verified`), then sets an embedded `AtomicBool`; later hot reads use the safe O(1) `RecordCodec::decode_fast` (header, bounds and alignment checks only, no checksum). | v0.1 |

---

## 4. Primary, Secondary & Vector Indexing (`v0.1 – v0.8` Completed)

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-INDX-01** | **64-Shard Cache-Padded Primary Index & $O(1)$ Ordinal Recycling** | Maps `InlineId -> PrimaryEntry` (`96 bytes`) across 64 cache-line-aligned shards with strict $O(1)$ deleted `RecordOrdinal` recycling (`Some(InlineId::EMPTY)` sentinel). | v0.1 |
| **F-INDX-02** | **Compact Varint Primary Checkpoints (`STGEIDX3`)** | Persists `PrimaryIndex` to `primary.idx` at **`~21 bytes/record`** (LEB128 varints + 64B-unit arena offsets), including each record's ordinal so ordinal-keyed persisted indexes stay valid across restarts. | v0.2 |
| **F-INDX-03** | **Order-Preserving B+Tree Numeric Range Index** | Indexes `i64`, `u64` (`sortable_u64_to_i64`), `f64` (`sortable_f64_to_i64`), and `bool` fields for fast `[min, max]` range scans. | v0.4 |
| **F-INDX-04** | **Word-Aligned `CompressedBitmap` Tag Index** | Maps categorical tags to 64-bit word-aligned bitmaps, driving multi-tag `AND`/`OR` intersections from the smallest posting list via hardware `TZCNT`. | v0.4 |
| **F-INDX-05** | **Zero-Copy Differential Index Maintenance** | Extracts old `IndexSnapshot` (`264B` stack SoA struct) directly from `&T::Archived` (`0` heap maps) and mutates secondary indexes only when indexed fields actually change. | v0.4 |
| **F-INDX-06** | **$O(N + K \log K)$ SIMD Cosine Vector Scorer** | 8-way unrolled SIMD cosine similarity (`87 ns` / 64-dim vector) over zero-copy `mmap` slices paired with linear $O(N)$ Hoare/Floyd-Rivest Quickselect (`select_nth_unstable_by`). | v0.7 |

---

## 5. Explicit Relationships & Graph Navigation (`v0.5` Completed)

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-REL-01** | **Inline Zero-Copy Relationship Containers** | Native `OneToOne<T>` (`InlineId::EMPTY` niche), `OneToMany<T>`, and `ManyToMany<T>` stored as sorted `ArchivedSlice<InlineId>` with $O(\log K)$ zero-alloc binary search. | v0.5 |
| **F-REL-02** | **Deadlock-Free Bidirectional Atomic Linking** | `update_pair_atomic` and `transaction` update forward and reverse relationship edges atomically in canonical lock order. | v0.5 |
| **F-REL-03** | **Referential Integrity Policies (`OnDelete`)** | Enforces `OnDelete::Restrict`, `OnDelete::Unlink`, and cycle-safe BFS `OnDelete::Cascade`. | v0.5 |
| **F-REL-04** | **Zero-Copy Batch Graph Traversal & Auditor** | `get_related_refs` resolves neighbor `RecordRef<T>` views in batch; `verify_relationships` detects any `DanglingEdge`. | v0.5 |

---

## 6. Full-Scale Production Database Extensions (`v1.0` Roadmap: `TASK-031` to `TASK-050`)

### 6.1 Multi-Collection Database Catalog & Online Schema Evolution (`F-CAT-*`)

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-CAT-01** | **Multi-Collection `DatabaseCatalog` Manager** | Hosts multiple named typed and dynamic collections (`CollectionId 1..65535`) inside a single database directory sharing one `MmapPool`, one `GroupCommitWal`, and global memory budgets (`max_hot_dirty_bytes`). | v1.0 |
| **F-CAT-02** | **Cross-Collection Atomic Transactions** | Executes deadlock-free atomic transactions spanning multiple collections (`users`, `orders`, `vectors`) using canonical `(CollectionId, InlineId)` lock ordering and a single multi-collection WAL commit frame. | v1.0 |
| **F-CAT-03** | **Online Lazy Schema Migration Pipeline** | Registers `SchemaUpcaster` hooks (`v1 -> v2 -> v3`) per collection that transparently upgrade older on-disk records during reads or background arena compaction with zero downtime. | v1.0 |

### 6.2 Disk-Backed Paged Primary & Secondary Indexes (`F-PIDX-*`)

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-PIDX-01** | **Memory-Mapped Paged B+Tree Range Index** | Persists B+Tree numeric/timestamp index nodes into 4 KiB cache-aligned disk pages (`btree_<field>.idx`) with prefix/delta key compression so billion-record range indexes require near-zero heap RAM. | v1.0 |
| **F-PIDX-02** | **Memory-Mapped Paged Bitmap Tag Index** | Persists compressed tag posting bitmaps into an `mmap`-backed container file (`bitmap_<coll>.idx`) with copy-on-write page updates and `0.00` heap allocations during multi-tag intersections. | v1.0 |
| **F-PIDX-03** | **2-Level Paged Primary Index Directory (`ColdIndexPage`)** | Allows cold primary index partitions to reside in memory-mapped disk pages with an inline 64-bit Bloom filter header, bounding in-RAM primary index memory even when indexing 100M–1B+ records. | v1.0 |

### 6.3 Sub-Linear HNSW Vector Index & Scalar Quantization (`F-HNSW-*`)

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-HNSW-01** | **`SQ8` Scalar Quantization (`4x` Smaller Vectors)** | Compresses `f32` embeddings into 8-bit signed/unsigned integers (`[u8; D]` + `min/scale` header) reducing vector memory and disk footprint by **75% (`4x`)** while retaining `> 99%` recall via two-stage exact re-ranking on the top-$K$ candidates. | v1.0 |
| **F-HNSW-02** | **Multi-Layer HNSW Graph Index ($O(\log N)$ Search)** | Builds a navigable small-world proximity graph (`M = 16`, `ef_construction = 64`) over `RecordOrdinal`s with memory-mapped adjacency lists (`hnsw.idx`) and bitset visited tracking so unfiltered vector search runs in logarithmic **$O(\log N)$** time. | v1.0 |
| **F-HNSW-03** | **Filter-Aware Hybrid HNSW Search** | Dynamically switches between pre-filtered SIMD exact scan (when bitmap selectivity `< 5%`) and predicate-guided HNSW graph traversal (when selectivity `>= 5%` or unfiltered) for optimal time complexity across all filter ratios. | v1.0 |

### 6.4 Multi-Version Concurrency Control (MVCC) & Snapshot Isolation (`F-MVCC-*`)

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-MVCC-01** | **Monotonic Hybrid Logical Clock (`CommitTs`) & Version Chains** | Assigns a strictly increasing 64-bit `commit_ts` to every transaction and links historical record versions in a bounded delta/arena chain (`prev_location`, `prev_commit_ts`) without bloating hot records. | v1.0 |
| **F-MVCC-02** | **Wait-Free Point-in-Time `ReadSnapshot` Isolation** | `db.begin_read_snapshot()` captures a consistent `read_ts` watermark so long-running range scans, graph traversals, and multi-read transactions see a 100% consistent point-in-time view without blocking concurrent writers. | v1.0 |
| **F-MVCC-03** | **Watermark Epoch Garbage Collection** | Tracks the oldest active `read_ts` across all readers and automatically reclaims obsolete historical versions during `hot_dirty` flush and arena compaction so version chains never cause memory or disk bloat. | v1.0 |

### 6.5 Segmented WAL Rotation, Incremental Checkpoints & Hot Backups (`F-PITR-*`)

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-PITR-01** | **Segmented WAL Rotation (`wal_XXXXXX.log`) & LSN Tracking** | Rotates WAL files at configurable size limits (e.g., `64 MiB`), assigns monotonic 64-bit Log Sequence Numbers (`LSN`), and deletes/archives old WAL segments automatically once their `max_lsn <= checkpoint_lsn`. | v1.0 |
| **F-PITR-02** | **Online Hot Backup & Point-in-Time Recovery (PITR)** | Creates consistent online backups (`.bak` manifest + hardlinked/copied arena segments + WAL stream) (readers continue; writers pause only while dirty records are flushed and files are copied), and replays the archived WAL prefix up to a target timestamp (`PITR`, requires `EngineConfig::wal_archive = true`). | v1.0 |

### 6.6 Kernel Event Polling, Complete Wire Protocol & Security (`F-NET-*` & `F-SEC-*`)

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-NET-01** | **OS Kernel Socket Readiness Polling ($O(\text{active})$ Reactor)** | Upgrades `ReactorShard` with OS-level socket readiness notification (`WSAPoll` on Windows, `poll`/`epoll` on Unix) so waking active connections out of 1,000,000 idle connections uses `0%` idle CPU and $O(\text{active})$ dispatch. | v1.0 |
| **F-NET-02** | **Full Database Wire Protocol (`QUERY`, `CAS`, `TXN`, `BATCH`, `BACKUP`)** | Extends `StorageServer` & `StorageClient` with zero-copy binary RPC opcodes for `OP_CAS_PUT`, `OP_BATCH_GET`, `OP_BATCH_PUT`, `OP_QUERY_HYBRID`, `OP_PING`, and `OP_SNAPSHOT_BACKUP`. | v1.0 |
| **F-SEC-01** | **Token Authentication, RBAC & Per-Connection Rate Quotas** | Supports HMAC/Bearer token authentication (`OP_AUTH`), role-based permissions (`ReadOnly`, `ReadWrite`, `Admin`), and token-bucket rate limiting per connection to prevent abusive clients from hogging CPU or I/O. | v1.0 |
| **F-SEC-02** | **Authenticated Page & WAL Encryption-at-Rest** | Optional high-speed stream cipher + MAC encryption (`ChaCha20-Poly1305` / `XChaCha20` hardware-friendly ARX cipher in pure Rust) for on-disk arena segments and WAL frames. | v1.0 |

### 6.7 Distributed Replication (Raft) & Horizontal Cluster Sharding (`F-DIST-*`)

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-DIST-01** | **Consistent-Hash Virtual Shards (`1,024 vshards`) & Cluster Router** | Maps keys to `1,024` deterministic virtual shards (`vshard = hash64(id) >> 54`, the high 10 bits, independent of the low bits that pick in-node shards) with a compact cluster topology map (`ClusterMap`) routing client requests to the owning physical node in $O(1)$ time. | v1.0 |
| **F-DIST-02** | **Raft Consensus Replication & Pipelined WAL Streaming** | Replicates `GroupCommitWal` batches across a multi-node replica group (`Leader`, `Follower`, `Learner`) with term/vote persistence, heartbeat election timeouts, and read-scale follower queries. | v1.0 |
| **F-DIST-03** | **Online Zero-Downtime Shard Migration** | Streams a virtual shard's arena records and incremental WAL tail from a source node to a target node while live traffic continues, atomically flipping shard ownership on completion. | v1.0 |

### 6.8 Cost-Based Query Optimizer & Lock-Free Observability (`F-OPT-*`)

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-OPT-01** | **Cardinality-Aware Cost-Based Query Planner & `EXPLAIN`** | Estimates predicate selectivity across `BitmapTagIndex`, `NumericRangeIndex`, and `HNSW` vector index in `< 200 ns` to choose the lowest-cost execution plan and exposes structured `QueryExplain` diagnostics. | v1.0 |
| **F-OPT-02** | **Lock-Free HDR-Style Latency Histograms & Prometheus Exporter** | Records power-of-two bucketed latency histograms (`read`, `write`, `query`, `wal_sync`, `rpc`) using thread-local / sharded atomic counters (`0` allocations) and exports Prometheus/OpenMetrics text format via `OP_METRICS` and CLI. | v1.0 |


### 6.9 Operations, Observability & Supplementary Features (implemented; referenced from code)

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-OPS-01** | **Schema Migration Hooks** | `register_migration` / `SchemaRegistry` upcasters upgrade older `schema_version` frames on read and during compaction. | v0.8 |
| **F-OPS-02** | **Record TTL & Expiry Sweeps** | `put_with_ttl` stores `ttl_expires_at_ns`; expired records are invisible to reads and removed by `sweep_expired`. | v0.8 |
| **F-OPS-03** | **Cold LZ4 Record Compression** | `compress_cold_record` stores cold records LZ4-compressed (`RecordFlags::COMPRESSED_LZ4`); they are read via `get_owned`. | v0.8 |
| **F-OPS-04** | **Consistent Snapshots & Online Backups** | `create_snapshot_with_lsn` / `create_online_backup` copy a consistent database image tagged with its WAL LSN into a fresh directory. | v0.8 |
| **F-OPS-05** | **Hybrid Metadata + Vector Top-K** | `QueryBuilder::top_k_similar` intersects index filters with SIMD cosine scoring (HNSW above 256 vectors). | v0.8 |
| **F-OPS-06** | **Engine Telemetry (`EngineStats`)** | Live record, read/write, coalescing, WAL and mmap-pool counters via `StorageEngine::stats`. | v0.8 |
| **F-OPS-07** | **`storage-cli` Operations Utility** | CRUD, stats, compaction, backup / WAL archive / PITR restore, and remote admin commands. | v0.8 |
| **F-OBS-01** | **Lock-Free Latency Histograms** | 64-bucket logarithmic `LatencyHistogram` (`[AtomicU64; 64]`), `0` allocations per record. | v1.0 |
| **F-OBS-02** | **Prometheus Text Exporter** | `EngineStats::to_prometheus_text` served over `OP_METRICS`. | v1.0 |
| **F-OBS-03** | **Bounded Slow-Operation Log** | Fixed-capacity ring buffer of operations slower than a threshold. | v1.0 |
| **F-CONC-08** | **Snapshot-Isolation Read Guards** | `begin_read_snapshot` returns a `ReadSnapshot` that sees a consistent `read_ts` view; writers keep prior versions while it is alive. | v1.0 |
| **F-DIST-04** | **Follower Reads at Commit Index** | `read_at_commit_index` serves reads from followers only up to the replicated commit index. | v1.0 |
| **F-INDX-07** | **HNSW + Exact Re-Ranking Search** | Multi-layer HNSW candidate search followed by exact `f32` re-ranking of the top `2K` candidates. | v1.0 |
| **F-INDX-08** | **Split-Block Bloom Filter** | 64-byte cache-line-aligned split-block Bloom filter used by the paged/cold index directories. | v1.0 |
| **F-REL-05** | **Relationship Integrity Auditor** | `verify_relationships` reports every dangling relationship edge. | v0.5 |
| **F-SEC-03** | **Per-Connection Token-Bucket Rate Limiting** | Zero-allocation `TokenBucket` applied at the request-header stage, before the body is read. | v1.0 |
| **F-SEC-04** | **Constant-Time Auth & Confined Admin Paths** | Constant-time token comparison and `backup_root` confinement of client-supplied backup names. | v1.0 |

### 6.10 Money-Grade Write Safety & Documents (`F-TYPE-*`, `F-DOC-*`, Stage 22)

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-TYPE-01** | **Exact Decimal & 128-bit Numbers** | `Decimal` (`i128` mantissa, scale `<= 38`) with checked arithmetic and explicit rounding; decimals and `i128` are indexable through one order-preserving numeric key space. Money is never a binary float. | v1.1 |
| **F-TYPE-02** | **Nested Lists & Maps** | `PropertyValue::List` / `Map` archived zero-copy (depth `<= 32`). | v1.1 |
| **F-CONC-09** | **Atomic Field Operations** | Server-side `set`, `unset`, checked `add` with `min` / `max` bounds (no overdraft), `append` — no client read-modify-write, no lost updates. | v1.1 |
| **F-CONC-10** | **Conditional Atomic Commits** | Any mix of puts, deletes, patches and checks with `absent` / `exists` / `version` preconditions committed atomically (`StorageEngine::commit`, `OP_TXN`, `OP_DOC_WRITE`). Versions are monotonic across delete + re-create (no ABA). | v1.1 |
| **F-CONC-11** | **Idempotent Requests** | A client request id makes a retried write apply at most once (crash-safe marker committed in the same transaction; 24 h window). | v1.1 |
| **F-DOC-01** | **Schema-Flexible Document Collections** | Named collections of keyed documents with typed fields in one engine; cross-collection transactions. | v1.1 |
| **F-DOC-02** | **Document Secondary Indexes** | Up to 4 per collection; equality, range and `IN` predicates use them; results always exact. | v1.1 |
| **F-DOC-03** | **Document Queries** | Filters (comparison, between, in, prefix, contains, exists / missing, key prefix, and / or / not), ordering, offset / limit, cursors. | v1.1 |
| **F-DOC-04** | **Document Wire Protocol** | `OP_DOC_*` opcodes with the SVE value encoding (`protocol/PROTOCOL.md`), used by every SDK and the SQL adapter. | v1.1 |
| **F-DIST-05** | **Networked Raft Runtime & Clustered Serving** | Election timers, pre-vote, per-peer replication, leader lease; servers route writes through consensus, redirect from followers (`NotLeader` + leader address) and serve opt-in follower reads (`--node-id`, `--peers`). | v1.1 |
| **F-NET-04** | **Session Options & Cluster Info** | `OP_SET_OPTIONS` (follower reads) and `OP_CLUSTER_INFO` (leader discovery for SDKs). | v1.1 |
| **F-NET-03** | **Rich Status Codes** | `NotLeader`, `Invalid`, `CommitUnknown`, conflict details (write index + current version). | v1.1 |

### 6.11 Client SDKs (`F-SDK-*`, Stage 25)

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-SDK-01** | **Protocol Specification & Golden Vectors** | `protocol/PROTOCOL.md` plus byte-exact request/response vectors every SDK must reproduce. | v1.1 |
| **F-SDK-02** | **Official SDKs** | Python, TypeScript, Go, Java, C#, Rust, PHP and Ruby (`sdks/<language>/`): the same API in each language, no runtime dependencies, exact decimals, at-most-once retries, leader discovery and failover. | v1.1 |

### 6.12 SQL Gateway (`F-SQL-*`, Stage 26)

| Feature ID | Feature Name | Description | Target Tier |
| :--- | :--- | :--- | :--- |
| **F-SQL-01** | **PostgreSQL, MySQL & SQL Server Wire Protocols** | `storine-sql` (`crates/storage-sql`) speaks PostgreSQL v3, MySQL and TDS 7.1 – 8.0, so existing drivers and ORMs connect by changing only the connection string. | v1.2 |
| **F-SQL-02** | **Three SQL Dialects** | PostgreSQL, MySQL and T-SQL syntax and functions over one executor: joins, subqueries, CTEs (recursive), window functions, `MERGE`, upserts, `RETURNING` / `OUTPUT`, views, sequences, databases. | v1.2 |
| **F-SQL-03** | **Catalog Emulation** | `pg_catalog`, `information_schema`, `SHOW`, `sys.*` and the `sp_*` metadata procedures, faithful enough for ORM migrations and introspection (Django, Rails, Prisma, EF Core, Hibernate, TypeORM, ...). | v1.2 |
| **F-SQL-04** | **Money-Safe Transactions** | Versioned writes (no lost updates), REPEATABLE READ / SERIALIZABLE read validation, row locks, constraints enforced by atomic preconditions (race-free foreign keys), exact decimals on every wire format. | v1.2 |
| **F-SQL-05** | **Zero-Dependency TLS** | TLS 1.3 and 1.2 (ChaCha20-Poly1305, x25519, Ed25519) on all three endpoints, including every SQL Server encryption mode and TDS 8 strict. | v1.2 |
| **F-SQL-07** | **Platform-Neutral SQL Server Logins** | SQL Server authentication plus OpenID Connect access tokens from any issuer (JWKS, RS256/384/512, EdDSA) and Kerberos from any realm (keytab), without tying deployments to a cloud or to Windows. | v1.2 |
| **F-SQL-06** | **Real-Client Compatibility Matrix** | `tests/clients/run_all.sh`: 14 suites over Python, Node, Go, Java, .NET, PHP and Ruby drivers and ORMs. | v1.2 |

---

## 7. Verified Implementation Status (full-project audits, 2026-10-20/21, `TASK-067` – `TASK-081`)

The tables above are the target specification. The following rows are **not** fully realized in the engine as of this audit; everything not listed here is implemented and covered by tests.

| Feature ID | Status |
| :--- | :--- |
| **F-SEC-02** | Wired as `EngineConfig::encryption` (`TASK-084` – `TASK-086`): record frames in arena segments, record files and WAL bodies are ChaCha20-Poly1305 sealed. Key ring with per-frame key ids; online enable / disable / rotation via `set_encryption` + `reseal_all` (scrubs superseded copies). Per-record policy `Indexable::protection`: encrypt, or authenticate only (plaintext + MAC). Index files sealed too (`TASK-088`): `primary.idx` + delta frames, `hnsw.idx`, paged secondary-index generations (decrypted per page on access). SIMD ChaCha20 (AVX2 / SSE2) + 64-bit Poly1305 (`TASK-087`). Still visible: file names and sizes, WAL frame headers (ids, versions), record file names in dedicated-file layouts. |
| **F-PIDX-01 / 02** | Wired as `EngineConfig::index_tier = IndexTier::Paged` (`TASK-083`). Rebuilt from the records after a crash; reused after a clean shutdown. |
| **F-PIDX-03** | Wired as `EngineConfig::primary_tier = PrimaryTier::Cold` (`TASK-092`): the primary entries and the ordinal table as of the last checkpoint live in an immutable page-structured generation (`<stem>_primary.cold`, sealed when encryption is on); RAM = entries changed since the checkpoint + tombstones + `8 B`/page separators + ~`10 bits`/record Bloom filter. Open maps the tier instead of loading it. Tier switches in both directions. |
| **F-NET-01** | `O(active)` readiness reactors: `epoll` on Linux (`TASK-090`, verified: flat latency up to 8,000 idle connections where `poll` is 7.7x slower) and AFD poll over IOCP on Windows (`TASK-102`, linted for Windows, awaiting a Windows test run). Other Unix systems use `poll`. Idle workers block indefinitely (`0%` CPU). |
| **F-SEC-01** | Bearer tokens only (no HMAC). An unauthenticated server refuses to bind a non-loopback address. Configure tokens with `STORAGE_AUTH_TOKENS=token:role,...` (tokens of at least 16 characters). |
| **F-DIST-02** | Complete (`TASK-099` – `TASK-101`): persistent term/vote/log, deterministic replicated commands (identical versions on every replica), replica write guard, pre-vote, leader stickiness, lease reads with read-index fallback, `InstallSnapshot`, `CommitUnknown` + idempotent retries, networked runtime over TCP; membership changes (`TASK-121`): learners, add / promote / remove one voter at a time through the log, leader self-removal, configuration carried by snapshots. |
