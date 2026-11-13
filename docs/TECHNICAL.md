# Storine technical reference

How Storine works and how to run it: every program, option, environment variable, default and limit, as of version 0.1.1. For a first look, start with the [README](../README.md). The wire protocol is specified in [`protocol/PROTOCOL.md`](../protocol/PROTOCOL.md), measurements are in [`BENCHMARK_REPORT.md`](BENCHMARK_REPORT.md), and upgrade rules in [`COMPATIBILITY.md`](COMPATIBILITY.md).

**Contents**

1. [Architecture](#1-architecture)
2. [Data model](#2-data-model)
3. [Storage engine](#3-storage-engine)
4. [Indexes, queries and analytics](#4-indexes-queries-and-analytics)
5. [Embedding the engine](#5-embedding-the-engine)
6. [The server](#6-the-server)
7. [Clusters](#7-clusters)
8. [Security](#8-security)
9. [Clients](#9-clients)
10. [SQL gateway](#10-sql-gateway)
11. [Web console](#11-web-console)
12. [Command-line tool](#12-command-line-tool)
13. [Operations](#13-operations)
14. [Limits](#14-limits)
15. [Known limitations](#15-known-limitations)

---

## 1. Architecture

Storine is a Rust workspace with no third-party runtime crates. `unsafe` code is confined to two crates, `storage-codec` and `storage-mmap`, and checked with Miri and AddressSanitizer on every release ([`RELEASE.md`](RELEASE.md)).

```text
 clients ──► storage-server ─────────────┐        storine-sql ──► (embedded store or a server)
 (8 SDKs,     reactor shards, TLS,       │        storine-console ──► (any server, via the Rust SDK)
  console)    RBAC, admission control    │
                       │                 │
              ┌────────┴────────┐  Raft (single group)  /  range cluster (many Raft groups)
              ▼                 ▼
         DocumentStore ── StorageEngine<T> ── WAL (storage-wal)
                               │
          primary index · secondary, vector and columnar indexes (storage-index, storage-columnar)
                               │
              arena segments / record files, memory-mapped (storage-mmap)
```

| Crate | Contents |
| :--- | :--- |
| `storage-types` | `InlineId` (keys up to 23 bytes, inline), the 64-byte `RecordHeader`, versions, errors |
| `storage-codec` | Zero-copy archive format, document values, checksums (XXH64-family + CRC32C), vector scoring, SIMD column kernels, ChaCha20-Poly1305, SHA-2, x25519, Ed25519 |
| `storage-mmap` | Memory mapping on Unix and Windows, the bounded mapping pool, arena segments, atomic file replacement, network readiness pollers (`epoll`, `kqueue`, AFD poll) |
| `storage-wal` | The group-commit write-ahead log and its recovery |
| `storage-index` | Primary index and its checkpoints, tag bitmaps, numeric ranges, paged index tiers, HNSW vector index |
| `storage-columnar` | Column segments for analytics |
| `storage-graph` | One-to-one, one-to-many and many-to-many relationships |
| `storage-core` | `StorageEngine<T>`, `DocumentStore`, queries, analytics, Raft and range clusters |
| `storage-tls` | TLS 1.3 and 1.2, certificate issuing |
| `storage-server` | `storage-server`: the network server |
| `storage-cli` | `storage-cli`: local and remote administration |
| `storage-sql` | `storine-sql`: the SQL gateway |
| `storage-console` | `storine-console`: the web console |

---

## 2. Data model

A **database** holds **collections** of **documents**. A document is a key (up to 1,024 bytes of UTF-8) and a map of fields. A collection exists once a document is written to it; collection names are up to 255 bytes, and `$meta` and `$idem` are reserved.

Field values:

| Type | Notes |
| :--- | :--- |
| null, boolean | |
| 64-bit signed and unsigned integers, 128-bit integers | |
| 64-bit float | |
| decimal | Exact, up to 38 digits of scale. SDKs map it to the language's exact decimal type, never to a float |
| timestamp | Nanoseconds since the Unix epoch |
| UUID, 24-byte id | |
| text, bytes | Large byte or text values can be read in ranges of up to 16 MiB |
| list, map | Nested up to 32 levels |

Every write gives the document a new **version**. Writes can carry a **precondition** (must not exist, must exist, or must be at a version), and a request may hold many writes across collections, applied **atomically**. Field operations run on the server: `set`, `unset`, `append`, and `add` with optional `min` / `max` bounds. `add` is exact and checked: integers stay integers, overflow is refused, and a bound that would be crossed fails the whole request (`add("balance", -25).min(0)` cannot overdraw).

Clients attach a random **request id** to each logical write and reuse it on every retry. The server applies a request id at most once within 24 hours, so timeouts, reconnects and leader changes never apply a write twice.

---

## 3. Storage engine

### Records and files

Each record is a 64-byte header followed by its payload. The header holds the record's version, schema version, flags, timestamps, lengths, an XXH64-family hash combined with CRC32C over the payload, and a checksum of the header itself. CRC32C uses the CPU instruction (SSE4.2 on x86-64, the CRC extension on ARMv8) when present, chosen at run time, and a bit-identical portable version otherwise, so files move freely between machines.

Records are stored in one of three layouts (`EngineConfig::layout_mode`):

| Layout | How | Used by |
| :--- | :--- | :--- |
| `AdaptiveArena` | Records are appended to memory-mapped segment files (`seg_XXXXX.dat`), which start at 4 KiB and double up to the segment capacity. Records larger than the threshold get their own file. Dead space is reclaimed by compaction | Documents (`DOCUMENT_LAYOUT`: 64 KiB threshold, 64 MiB segments), so every server |
| `DedicatedFiles` | One file per record, replaced atomically (temporary file and rename) | The default for typed `StorageEngine<T>` |
| `PingPongFiles` | One file per record with two slots, written alternately | Typed engines with large, frequently rewritten records |

Reads map files into memory and decode records in place, without copying or allocating. A record's bytes are validated once, when first mapped; later reads are bounds-checked decodes, so a corrupted or hostile file cannot cause undefined behaviour.

### Writes, the WAL and durability

A write goes to the **write-ahead log** first, then into an in-memory buffer of recent changes (`hot_dirty`, at most `max_hot_dirty_bytes`, 64 MiB by default), where readers see it at once. A background flusher writes buffered records to their files every `flush_interval_ms` (10 ms); several writes to one key in that time are written once.

The WAL's `SyncPolicy` decides when a write is durable:

| Policy | A write returns after | Lost on power failure |
| :--- | :--- | :--- |
| `Immediate` | its own `fsync` | nothing |
| `GroupCommit { max_batch_entries }` (default, 64) | an `fsync` it shares with concurrent writers | nothing |
| `AsyncMemoryFirst` | the log write reaches the OS (no `fsync`) | the writes since the last flush. A process crash loses nothing |

The WAL is one file per engine (`wal/<name>.wal`). A **checkpoint** flushes every buffered record, syncs the record files and segments, then cuts the WAL: recovery replays only what was written since. Who checkpoints:

| Mode | Checkpoint |
| :--- | :--- |
| Standalone server | every 10 s while it is being written |
| Single-group Raft node | every 10 s if something was applied (then the Raft log is compacted) |
| Range-cluster node | every 5 s while written |
| Embedded engine | when the application calls `checkpoint()` or `flush_all()`, and when the engine is dropped |

**Recovery** at open streams the WAL in bounded batches (up to 4,096 entries or 32 MiB), ignores a torn final frame, and applies only committed transactions. Records touched by a batch are written once and their files synced in parallel. On an I/O error the WAL stops accepting writes rather than acknowledge one of unknown durability.

### Arena compaction

Rewritten and deleted records leave dead space in segments. Compaction copies a segment's live records forward and deletes the old file. It runs automatically when at least 64 MiB of a store is dead and dead space is at least half of what is used, and on demand (`storage-cli remote-compact`, `compact_arenas()`). Until it runs, segments can hold up to twice the live data.

### Other engine features

- **MVCC snapshots**: `begin_read_snapshot` gives a consistent view while writes continue.
- **TTL**: documents written with a time to live expire. Single-group Raft nodes sweep expired documents every 60 s; embedded engines call `sweep_expired`.
- **Schema migrations**: a record type's `SCHEMA_VERSION` and registered migrations upgrade older records when they are read or replayed (`register_migration`, `register_step_upcaster`, `upgrade_legacy_schemas`).
- **Encryption at rest** (embedded API): `EngineConfig::encryption` seals record frames, WAL entries and index files with ChaCha20-Poly1305, bound to each record's id and metadata. A key ring allows rotation online (`set_encryption`, then `reseal_all`, which also scrubs superseded copies). Each record type can choose to encrypt or only authenticate (`Indexable::protection`). Warm reads stay allocation-free through a cache of decrypted frames (`decrypted_cache_bytes`, 256 MiB). File names and sizes, and WAL frame headers (ids, versions), remain visible. Raft logs and snapshots of a replicated engine are sealed with the same keys. The server enables it with a key file (section 8).
- **Cold tiers**: `primary_tier: Cold` keeps the primary index as of the last checkpoint on disk, holding only later changes in memory; `index_tier: Paged` does the same for secondary indexes with memory-mapped B+tree pages and bitmap blocks.
- **Relationships**: typed engines link records one-to-one, one-to-many and many-to-many, locking keys in a fixed order so concurrent link updates cannot deadlock, and `verify_relationships` checks for dangling links.

---

## 4. Indexes, queries and analytics

### Indexes

- **Primary**: a 64-shard hash index from key to location, in memory, saved at checkpoints (`primary.idx`, about 21 bytes per record).
- **Secondary**: up to 16 per collection, each on one top-level field. Creating one builds it from existing documents; until then it is reported as `building`. Queries re-check every index hit, so results are always exact.
- **Vector**: up to 4 per collection, up to 4,096 dimensions. Nearest-neighbour queries score candidates exactly when there are fewer than 2,048 and use an HNSW graph above that (M 16, ef_construction 64, ef_search 48), whose candidates are then scored exactly. The graph is saved at checkpoints and reused at open. Scores use cosine similarity, with AVX2 + FMA on x86-64 when present.

### Queries

A query has an optional filter, sort keys (up to 16), a limit (default 1,000, at most 10,000) and an offset, or a key to resume after (keyset paging, without sort keys). Filters: `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `between`, `in`, `prefix`, `contains`, `exists`, `missing`, `key_prefix`, combined with `and`, `or`, `not`. Fields are dotted paths into nested maps. Numbers compare across integer, decimal and float (`1 == 1.0 == 1.00`); ordered comparisons hold within one kind, and sorting uses a total order: null < bool < numbers < timestamps < text < bytes < uuid < id < list < map.

### Analytics

`aggregate` groups and aggregates on the server: `count`, `sum`, `avg`, `min`, `max`, `count_distinct`, `approx_count_distinct` (HyperLogLog, about 0.8 % error), over fields or arithmetic expressions, grouped by fields or numeric buckets. Integer and decimal sums are exact, widening to 128 bits or decimals; float sums are correctly rounded, so results never depend on thread count or layout. A query uses at most 256 MiB for its group tables by default and spills beyond that (up to 16 GiB), and at most 8 analytic queries run at once.

Enabling **columnar** storage on a collection also keeps it as column segments, built in the background, so aggregates and `scan_columns` (exporting chosen fields in key order, up to 100,000 rows a page) read only the columns they need, skip row groups by zone maps, and run vectorized and in parallel (`STORINE_THREADS` sets the parallelism). Results are the same with or without it.

---

## 5. Embedding the engine

Add `storage-core` to a Rust project and open a `DocumentStore` (documents) or a `StorageEngine<T>` (typed, zero-copy records):

```rust
use storage_core::{DocumentStore, EngineConfig};

let store = DocumentStore::open("./data", EngineConfig::default())?;
store.put("accounts", "alice", fields)?;
let doc = store.get("accounts", "alice")?;
store.checkpoint()?; // cut the WAL: an embedded engine does not do it on its own
```

`EngineConfig` fields and defaults:

| Field | Default | Meaning |
| :--- | :--- | :--- |
| `collection_name` | `"default"` (`"documents"` for a `DocumentStore`) | Name of the engine's storage collection and file stem |
| `layout_mode` | `DedicatedFiles` (`DOCUMENT_LAYOUT` for a `DocumentStore`) | Section 3 |
| `wal_sync_policy` | `GroupCommit { max_batch_entries: 64 }` | Section 3 |
| `enable_wal` | `true` | Without a WAL, buffered writes are lost on a crash |
| `background_flush`, `flush_interval_ms` | `true`, 10 | The background flusher |
| `max_hot_dirty_bytes` | 64 MiB | Writers flush synchronously beyond this |
| `mmap_pool` | 16,384 mappings, 4 GiB mapped | Cold mappings are unmapped beyond these |
| `wal_archive` | `false` | Keep cut WAL files for point-in-time recovery |
| `index_tier`, `primary_tier` | `Memory`, `Memory` | Section 3, cold tiers |
| `encryption`, `decrypted_cache_bytes` | none, 256 MiB | Section 3, encryption at rest |

`DocumentStore` offers `get`, `get_fields` (only some paths), `read_bytes` (a range of a large value), `put`, `insert`, `delete`, `patch`, `write` (an atomic batch across collections, with preconditions and a request id), `query`, `scan_collection`, index and vector index management, columnar storage, `aggregate`, `scan_columns`, `list_collections`, `collection_info` and `drop_collection`. Backups: `create_online_backup`, `archive_wal_for_pitr` and `restore_point_in_time` (section 13).

---

## 6. The server

```text
storage-server [data_dir] [bind_addr] [options]
storage-server gen-ca <dir>
storage-server gen-cert <ca-dir> <out-dir> <name>[,<name>...]
```

`data_dir` defaults to `./server_data` and `bind_addr` to `127.0.0.1:7878`.

| Option | Default | Meaning |
| :--- | :--- | :--- |
| `--max-connections N` | `STORAGE_MAX_CONNECTIONS`, else 100,000 | Connections beyond this are answered `Overloaded` and closed |
| `--node-id N` | | This node's id (1–65,535) in a cluster |
| `--peers 1=host:port,2=host:port,...` | | The cluster's founding members, this node included. On a range cluster each may add its locality: `1=host:port@region/zone` |
| `--join` | | Start without members and wait to be added (section 7) |
| `--ranges` | | Run as a range-cluster node (needs `--node-id` and `--peers`) |
| `--records agent` | | Serve the typed demo record engine instead of documents |
| `--zones FILE` | | Placement rules of a range cluster (section 7), the same file on every node |
| `--once` | | Start, print the banner and exit (for scripts and tests) |

| Environment variable | Meaning |
| :--- | :--- |
| `STORAGE_AUTH_TOKENS` | `token:role,...`, roles `read`, `write` or `admin`; tokens at least 16 characters. Turns authentication on |
| `STORAGE_CLUSTER_TOKEN` | Token nodes use with each other (at least 16 characters, admin role). Required for a cluster with authentication |
| `STORAGE_TLS_CERT`, `STORAGE_TLS_KEY` | PEM certificate chain and PKCS#8 Ed25519 key: TLS on the port, for clients and peers |
| `STORAGE_TLS_CA` | Certificates a node trusts when calling peers (default: its own certificate) |
| `STORAGE_RATE_LIMIT` | `rps:burst`: a token bucket per connection, one token per request |
| `STORAGE_BACKUP_ROOT` | Directory under which admins may create and restore backups remotely. Unset: remote backups are off |
| `STORAGE_MAX_CONNECTIONS` | As `--max-connections` |
| `STORAGE_ENCRYPTION_KEYS` | Key file: turns encryption at rest on (section 8) |
| `STORAGE_ENCRYPTION_ACTIVE_KEY` | Key id that seals new data (default: the highest id), or `none` to decrypt |
| `STORAGE_ENCRYPTION_ALLOW_PLAINTEXT` | `1`: accept existing plaintext while it is encrypted (turning encryption on for a database with data) |
| `STORAGE_WAL_ARCHIVE` | Directory where checkpoints archive the WAL for point-in-time recovery (section 13); not on range clusters |
| `STORAGE_WAL_ARCHIVE_RETAIN` | How long archived WAL is kept: `7d` by default, or hours (`48h`) or minutes (`90m`) |
| `STORINE_REACTOR=poll` | Use `poll` instead of `epoll` / `kqueue` / AFD poll |
| `STORINE_THREADS` | Analytics parallelism |
| `STORINE_SIMD=portable` | Use the portable column kernels |

The server prints one line when it is listening and exits with status 1 and a message on an error. It has no log file.

### Inside

- **Reactors**: one thread per CPU (between 2 and 16) runs non-blocking sockets with `epoll` on Linux, `kqueue` on macOS and AFD poll on Windows, so a wake-up costs time proportional to the active connections. An idle connection takes about 88 bytes. After a request a reactor keeps polling for 50 µs, which keeps a loopback `GET` near 9 µs.
- **Admission control** reads the 29-byte header first and rejects, before reading the body: an unknown opcode, a role too low, a body over the opcode's limit (section 14), then a rate-limit breach. A rejected connection gets an error and is closed.
- **Concurrency limits**: 2 admin jobs (backup, compaction) and 8 analytic queries at a time.
- **TLS handshakes** run on 4 threads with a 10 s deadline and a queue of 1,024, so a slow client never holds a reactor.
- Freed heap memory is returned to the operating system every 10 s.

---

## 7. Clusters

### Single-group Raft

Three or five servers with `--node-id` and `--peers` replicate one copy of the data with Raft. Writes go to the leader and are acknowledged once a majority has logged them; reads go to the leader unless a client allows stale follower reads. Followers answer `NotLeader` with the leader's address, and every client follows it.

```bash
export STORAGE_AUTH_TOKENS="admin-token-0123456789:admin" STORAGE_CLUSTER_TOKEN="node-token-0123456789"
storage-server ./n1 10.0.0.1:7878 --node-id 1 --peers 1=10.0.0.1:7878,2=10.0.0.2:7878,3=10.0.0.3:7878
```

| Setting | Value |
| :--- | :--- |
| Election timeout | 300–600 ms |
| Heartbeat | 60 ms |
| Commit timeout | 5 s, after which the client gets `CommitUnknown` and retries with the same request id |
| Append batch | 512 entries or 4 MiB |
| Peer RPC timeout | 500 ms |
| Checkpoint and log compaction | every 10 s if something was applied |
| Members | at most 256 |

**Membership** changes one node at a time with `storage-cli cluster <addr> add | add-learner | promote | remove`. A new node starts with `--join` and its own `--node-id` and waits to be added; a voter added with `add` catches up as a learner first (it has 60 s). `--peers` only founds a cluster: after a membership change, the configuration in the Raft log wins.

### Range clusters

With `--ranges`, data is split into key ranges, each replicated by its own Raft group, and moved between nodes as it grows. Any node accepts any request and coordinates it, including transactions that span ranges on different nodes (with timestamp ordering and a maximum clock offset of 500 ms). Range nodes store their engines with `AsyncMemoryFirst`: the Raft logs are the durable record.

| Setting | Value |
| :--- | :--- |
| Replicas per range | 3 voters |
| Split | above 100,000 keys or 2,000 writes/s |
| Merge | below 10,000 keys |
| Rebalancing | planned every 1 s; a node silent for 10 s counts as dead |
| Election, heartbeat | 300–600 ms, 50 ms; idle groups stop heartbeating after 3 idle beats |
| Engine checkpoint | every 5 s while written |
| Log retained after a checkpoint | 8,192 entries; a replica further behind gets a snapshot (64 KiB chunks) |
| Client request timeout | 10 s |

Range clusters also offer **cluster backups** (`storage-cli remote-backup` / `remote-restore`) and **change feeds**: `storage-cli changefeed` streams every change as JSON lines, at least once, resuming from a cursor file. Membership, metrics, compaction and checkpoint requests are refused in range mode.

### Placement across regions

Give every node its locality in `--peers` (`1=10.0.0.1:7878@us-east/a`: region `us-east`, zone `a`) and every node the same placement rules with `--zones <file>`. Each node's allocator applies the rules: it splits ranges at rule boundaries, moves replicas to where the rules allow, and moves leases (which serve writes and current reads) to preferred regions. One rule per line:

```text
default replicas=3 survival=zone                             # everything without a rule of its own
collection accounts survival=region lease=us-east,eu-west    # survive losing a region; leases in us-east first
collection eu_customers require=region:eu-west lease=eu-west # this collection's data stays in eu-west
collection orders prefix=eu/ require=region:eu-west          # only the keys starting with eu/
collection logs avoid=zone:c                                 # never in zone c
collection rates global lead=1500ms                          # present-time reads from any replica
collection profiles regional-by-row regions=us-east,eu-west  # each row in its home region
```

| Option | Meaning |
| :--- | :--- |
| `replicas=N` | Voters (1–15) |
| `survival=zone` / `survival=region` | Losing any one zone (or region) keeps a majority: no zone (or region) may hold a majority of the voters, so this needs at least three |
| `require=region:R`, `require=zone:Z` | Replicas only there; several of one kind are alternatives |
| `avoid=region:R`, `avoid=zone:Z` | Never there |
| `lease=R1,R2` | Preferred leaseholder regions, in order |
| `follower-reads=3s` / `off` | How far behind followers may serve reads (default 3 s) |
| `global lead=1500ms` | Writes are timestamped ahead and wait out the lead, so every replica serves present-time reads; the lead must exceed twice the maximum clock offset (500 ms) |
| `regional-by-row regions=R1,R2` | Keys written as `<region>\u0001<key>` live in their region, with leases there |

The server refuses rules the localities cannot satisfy, such as too few nodes in a required region, or region survival with fewer than three regions. Every node's health report lists the ranges it holds in `placement`: span, voters, and whether it leads each. Change rules or localities with a rolling restart, giving every node the new file.

### TLS between nodes

With `STORAGE_TLS_CERT` / `STORAGE_TLS_KEY` set, Raft and range traffic is encrypted and each node checks its peers' certificates against `STORAGE_TLS_CA`, by the host it is listed under in `--peers`. Peers still authenticate with `STORAGE_CLUSTER_TOKEN` inside the encrypted channel.

```bash
storage-server gen-ca ./tls/ca                                    # ca.crt, ca.key: keep ca.key offline
storage-server gen-cert ./tls/ca ./tls/n1 10.0.0.1,db1.internal   # node.crt, node.key
```

---

## 8. Security

| Role | May |
| :--- | :--- |
| `read` | Read documents, query, aggregate, health, metrics, cluster info |
| `write` | And write documents, transactions, create and drop indexes |
| `admin` | And back up, restore, compact, checkpoint, change membership, read change feeds; nodes use this role with each other |

- **Authentication** is by bearer token, sent once per connection and compared in constant time. With no `STORAGE_AUTH_TOKENS`, the server serves only on a loopback address and refuses to start on any other.
- **TLS** on the native port: TLS 1.3 with `TLS_CHACHA20_POLY1305_SHA256`, or TLS 1.2 with `TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256` and extended master secret for clients that offer nothing newer; `x25519`; Ed25519 certificates (from `gen-ca` / `gen-cert`, or any CA issuing Ed25519 keys). Full handshakes only; clients are not asked for certificates.
- **Limits** protect memory before authentication: each opcode has a body limit, checked from the header (section 14); a connection that sends a request it may not make is closed.
- **Encryption at rest**: `storage-server gen-key <file>` creates a key file (readable by its owner only; the server refuses a file others can read), and `STORAGE_ENCRYPTION_KEYS=<file>` turns encryption on. Records, the WAL, index files, Raft logs and snapshots are sealed with ChaCha20-Poly1305 (section 3), and the server refuses plaintext and data sealed with a key it does not have. Every node of a cluster needs the same key file, since snapshots travel between nodes as stored. Keep a copy of the keys away from the data: data sealed with a lost key is lost.
  - **Turning it on for existing data**: one restart with the key file and `STORAGE_ENCRYPTION_ALLOW_PLAINTEXT=1`. The server reseals everything in the background, then refuses plaintext again on its own; drop the setting before the next restart. The health report shows `"accept_plaintext": false` once done.
  - **Rotating keys**: `gen-key` adds a key to the file, and the newest is used after a restart, which reseals everything with it. Once that restart's reseal has finished and a checkpoint has passed, the database no longer needs the old key, but backups and archived WAL made before the rotation still do: keep it in the file (or in a safe place) for as long as you might restore them.
  - **Turning it off**: `STORAGE_ENCRYPTION_ACTIVE_KEY=none` and a restart decrypt everything; then remove the settings.
  - Range nodes reseal before they start serving; standalone and single-group nodes reseal while they serve.

---

## 9. Clients

| Language | Package | Requires |
| :--- | :--- | :--- |
| Python | `storine` | Python 3.8 |
| TypeScript / JavaScript | `storine` (npm) | Node.js 18 |
| Go | `github.com/twilight-techy/storine/sdks/go` | Go 1.21 |
| Java | `io.storine:storine` | Java 17 |
| C# | `Storine` | .NET 8 |
| Rust | `storine` | Rust 1.70; 1.82 for TLS (the `tls` feature) |
| PHP | `storine/storine` | PHP 8.1 |
| Ruby | `storine` | Ruby 3.2, with the `bigdecimal` gem |

Connection strings: `storine://[token@]host:port[,host:port...][?timeout_ms=5000&follower_reads=1]`. Several hosts are a cluster's seeds. `storines://` connects with TLS, and `tls_ca` (the certificates to trust; giving it also turns TLS on) and `tls_server_name` may be given as options or query parameters. .NET on Windows cannot use TLS, as its TLS stack lacks Ed25519.

Every client keeps a connection pool, retries writes with the same request id after timeouts, reconnects and `CommitUnknown`, follows `NotLeader` redirects and moves to another seed when a server is unreachable. All of them pass the same byte-for-byte protocol vectors (`protocol/vectors/`) and an end-to-end suite against a real server, standalone and as a 3-node cluster with its leader killed.

---

## 10. SQL gateway

`storine-sql` speaks the PostgreSQL, MySQL and SQL Server (TDS) protocols and dialects in front of an embedded store (`--data DIR`) or a running server or cluster (`--storine URL`). Its [README](../crates/storage-sql/README.md) lists the supported SQL, drivers and ORMs it is tested with, and its limitations.

| Option | Default |
| :--- | :--- |
| `--pg ADDR` / `--no-pg` | `127.0.0.1:5432` |
| `--mysql ADDR` / `--no-mysql` | `127.0.0.1:3306` |
| `--mssql ADDR` / `--no-mssql` | `127.0.0.1:1433` |
| `--user NAME:PASSWORD` (repeatable), or `STORINE_SQL_USERS` | none: every client is trusted, so keep it on loopback (it warns, but does not refuse, on another address) |
| `--database NAME` | `storine` |
| `--tls-cert F --tls-key F`, `--no-tls`, `--require-tls` | TLS on, with a certificate generated for localhost (kept in `DIR/tls/` with `--data`) |
| `--max-connections N` | 1,024; a client that has not logged in after 30 s is closed |
| `--oidc-issuer`, `--oidc-audience`, `--oidc-jwks`, `--oidc-user-claim` | SQL Server access-token logins (RS256/384/512, EdDSA; 60 s leeway). Needs a `--user` |
| `--kerberos-keytab FILE`, `--kerberos-keep-realm` | SQL Server Kerberos logins (5 min clock skew). Needs a `--user` |

`STORINE_SQL_LOG=1` logs every statement.

---

## 11. Web console

```text
storine-console [--listen 127.0.0.1:7880] [--allow-host HOST:PORT]... [storine://...]
```

The console serves a single-page app and a JSON API, and talks to Storine through the Rust client, so the server itself has no web code. It prints a link containing a key made for that run (or `STORINE_CONSOLE_KEY`, at least 16 letters, digits, `-` or `_`). It connects with `STORAGE_TOKEN`, `STORAGE_TLS_CA` and `STORAGE_TLS_SERVER_NAME` like `storage-cli`, or from its connect page.

- **Views**: overview (node role, term, commit and applied indexes, members, the node's health, collections with counts); documents (conditions on fields, key prefix, sort, page size, keyset or offset pages); a JSON editor (create, replace with a version check, delete with a version check); analytics (group by, aggregates, order); indexes (secondary and vector, with their state); settings (metadata, dropping a collection after typing its name).
- **Typed values** in the editor: `{"$decimal": "9.99"}`, `{"$time": "2026-11-13T08:00:00Z"}`, `{"$bytes": "<base64>"}`, `{"$uuid": "…"}`, `{"$id": "<48 hex digits>"}`, `{"$int": "…"}` for integers beyond 2^53, and `{"$float": "NaN"}`. `1` is an integer and `1.0` a float.
- **Security**: loopback by default; every API request must carry the key (`X-Console-Key`), which also forces browsers to ask before sending it cross-origin, a request the console never grants; a request whose `Host` does not name the console is refused (DNS rebinding); the page runs under a strict content security policy with no inline script or style, and is never cached or framed. On another address it serves plain HTTP: prefer an SSH tunnel. At most 64 connections are served at once.

---

## 12. Command-line tool

Remote commands take a `host:port` address (an IP address, not a name) and authenticate with `STORAGE_TOKEN`; `STORAGE_TLS_CA` turns TLS on and `STORAGE_TLS_SERVER_NAME` sets the name to check.

| Command | Does |
| :--- | :--- |
| `remote-health <addr>` | The node's health report |
| `remote-metrics <addr>` | Metrics in Prometheus text format (standalone and single-group nodes) |
| `remote-checkpoint <addr>` | Checkpoint now (admin) |
| `remote-compact <addr>` | Compact arenas now (admin) |
| `remote-backup <addr> <name>` | Online backup into `STORAGE_BACKUP_ROOT/<name>` (admin); a cluster backup on a range cluster |
| `remote-restore <addr> <name>` | Restore a cluster backup (admin, range clusters) |
| `restore-pitr <backup> <restore-dir> <time> [--archive DIR]... [--wal FILE]...` | Point-in-time recovery of a backup (section 13); runs offline |
| `changefeed <addr> <out.jsonl> <cursor-file> [--collection C] [--from wall.logical] [--once] [--interval-ms N]` | Stream changes (admin, range clusters) |
| `cluster <addr> [members]` | Members, leader, term and commit index |
| `cluster <addr> add <id> <host:port>` / `add-learner` / `promote <id>` / `remove <id>` | Change membership (admin) |

Local commands open a data directory directly, with the server stopped: `backup`, `archive-wal`, `restore-pitr`, `snapshot`, `stats`, `compact`, `verify`, `inspect-header`, `bench`, and `get` / `put`. They open the engine's typed demo record store (collection `default`), not the `documents` store a server writes, so for a server's data use the remote commands; `inspect-header` and copying a stopped data directory work on either.

---

## 13. Operations

### Installing

Release archives for Linux (x86_64, arm64; glibc 2.28 or newer), macOS (arm64, x86_64; macOS 11 or newer) and Windows (x86_64) hold the four programs, the licences, README and changelog, and are listed in `SHA256SUMS`. They are built for each architecture's baseline CPU and choose hardware CRC32C, AES and AVX2/FMA at run time; on x86-64 and arm64 they measured as fast as builds for the exact CPU ([`BENCHMARK_REPORT.md`](BENCHMARK_REPORT.md) section 8.6). From source, `cargo build --release` (Rust 1.82) builds for the machine it runs on.

### Data directory

```text
data/
  documents.lock                      held while a process has the directory open
  segments/documents/seg_00001.dat    arena segments
  wal/documents.wal                   the write-ahead log, cut at each checkpoint
  indexes/documents_primary.idx       primary index checkpoint; documents_hnsw.idx, _range.idx and
                                      _tags.idx hold the others once they exist
```

Cluster nodes also keep their Raft logs and snapshots in the data directory. Two processes cannot open one directory at once.

### Monitoring

`storage-cli remote-health` (or `OP_HEALTH`) returns JSON. A standalone or single-group node reports `status`, `live_records`, `total_reads`, `total_writes`, `hot_dirty_bytes` (written but not yet flushed) and `arena` (`segments`, `used`, `dead`, `live`, `compactions`, `removed`, `errors`, `last_error`). A range node reports `mode: "ranges"`, `ranges`, `log_entries`, `log_bytes`, queue and batch sizes, and `dropped_batches`. Every node also reports `encryption` (`"off"`, or the active key, the keys it holds and whether it still accepts plaintext), a standalone or single-group node `wal_archive` (the archive directory, when set), and a range node `placement` (the ranges it holds: span, voters, and whether it leads each).

`storage-cli remote-metrics` returns Prometheus text: `storine_live_records`, `storine_total_reads`, `storine_total_writes`, `storine_coalesced_flushes`, `storine_hot_dirty_bytes`, `storine_arena_*`, read and write latency summaries (`storine_read_latency_ns`, `storine_write_latency_ns`) and `storine_slow_rpcs` (requests over 10 ms; the last 256 are kept).

### Memory and disk

In the 2-hour soaks under constant faults, a single-group node peaked at 334 MiB resident and 110 MiB on disk, and a range node at 1.12 GiB resident (level after the first hour) and 433 MiB on disk ([`PROGRESS_LOG.md`](PROGRESS_LOG.md), 2026-11-13). Arena segments can hold up to twice the live data until compaction runs.

### Backups and recovery

- **Online backup of a running server**: set `STORAGE_BACKUP_ROOT` on the server, then `storage-cli remote-backup <addr> <name>`. On a range cluster this is a cluster-wide backup, restored with `remote-restore`.
- **Offline**: stop the server and copy the data directory.
- **Point-in-time recovery** (standalone and single-group servers):
  1. Start the server with `STORAGE_WAL_ARCHIVE=<dir>`, ideally on another disk than the data. Each checkpoint (every 10 s while written) appends the WAL it cuts to `<dir>/documents/`. Archived segments older than `STORAGE_WAL_ARCHIVE_RETAIN` (7 days by default) are deleted; keep it longer than the time between backups.
  2. Take backups regularly: `storage-cli remote-backup <addr> <name>` (with `STORAGE_BACKUP_ROOT`).
  3. To recover, with the server stopped: `storage-cli restore-pitr <backup> <restore-dir> 2026-11-13T08:00:00Z --archive <dir>/documents`, adding `--wal <data>/wal/documents.wal` to include the writes after the last checkpoint if the data directory survived. The time is RFC 3339 or nanoseconds since the epoch. The restored directory holds every write up to that moment and none after; start a server on it. With encryption at rest, run the restore with the same `STORAGE_ENCRYPTION_KEYS`.
  4. A single-group node's restore is one node's data: start it as a standalone server, or found a new cluster from it.

  Range clusters refuse `STORAGE_WAL_ARCHIVE`: each node's log holds only its own ranges' history. Use cluster backups there.
- **Embedded engines** use the same pieces directly: `EngineConfig::wal_archive` or `wal_archive_dir`, `create_online_backup`, and `restore_point_in_time_with`.

### Upgrades

Patch releases can be rolled through a cluster one node at a time; minor releases before 1.0 need the whole cluster stopped, or a backup restored into a new one. Downgrades are not supported once a newer version has written a data directory. Details: [`COMPATIBILITY.md`](COMPATIBILITY.md).

---

## 14. Limits

| Limit | Value |
| :--- | :--- |
| Document key | 1,024 bytes |
| Collection name | 255 bytes |
| Document or transaction body | 256 MiB |
| Value nesting | 32 levels |
| Decimal scale | 38 digits |
| Secondary indexes per collection | 16 |
| Vector indexes per collection | 4, up to 4,096 dimensions; `k` up to 10,000 |
| Query page | 1,000 by default, 10,000 at most; up to 16 sort keys |
| Fields in one projected read | 256 |
| Ranged read of a value | 16 MiB |
| Aggregate result | 1,000,000 rows, 64 group keys and 64 aggregates, expressions 32 deep |
| Columnar scan page | 1,000 by default, 100,000 at most |
| Request bodies by opcode | 256 MiB writes and transactions; 1 MiB queries, aggregates and admin; 128 KiB document reads; 4 KiB authentication and backups |
| Connections | 100,000 per server by default (`--max-connections`) |
| Concurrent analytic queries / admin jobs | 8 / 2 per server |
| Request id deduplication window | 24 hours |
| Raft members | 256 |

---

## 15. Known limitations

- **Tail latency** trails RocksDB and LMDB in some cases: fill p99.99 (about 0.7 ms against RocksDB's 16–20 µs), 4-writer overwrite p99.99 (2.1–3.0 ms against 0.5–1.3 ms), and durable p99.9 / p99.99 against LMDB ([`BENCHMARK_REPORT.md`](BENCHMARK_REPORT.md) sections 8.4–8.5).
- The **server binary** does not expose cold or paged index tiers; they are in the embedded API. Point-in-time recovery is not available on range clusters (they have cluster backups), and placement rules change only with a rolling restart.
- **TLS** supports one suite per version and Ed25519 certificates only; no client certificates.
- **Change feeds** exist for range clusters only (`storage-cli changefeed`); the SDKs do not expose them.
- **Soak testing** ran for 2 hours per cluster mode under constant faults; no multi-day soak has run, and the standalone server's periodic checkpoint is new in 0.1.1.
- **macOS and Windows** pass the test suite in CI and smoke tests in the release build; the performance figures and soaks are from Linux.
- The local `storage-cli` data commands work on the typed demo store, not on document data directories (section 12).
