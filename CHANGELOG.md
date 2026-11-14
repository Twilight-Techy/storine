# Changelog

Versions follow [Semantic Versioning](https://semver.org). Before 1.0, a minor version (0.x) may change on-disk or wire formats; [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md) says what each kind of release may change and how to upgrade.

## Unreleased

Three features that existed only in the embedded engine are now in the server: encryption at rest, point-in-time recovery and multi-region placement ([`docs/TECHNICAL.md`](docs/TECHNICAL.md) sections 7, 8 and 13).

### Added

- **Encryption at rest in the server**: `STORAGE_ENCRYPTION_KEYS` names a key file (`storage-server gen-key <file>` makes one; it must be readable by its owner only). Records, the WAL, index files, and now Raft logs and snapshots are sealed. Every start reseals whatever does not match the settings, so turning encryption on (with `STORAGE_ENCRYPTION_ALLOW_PLAINTEXT=1` once), rotating keys and turning it off (`STORAGE_ENCRYPTION_ACTIVE_KEY=none`) each take one restart. Health reports show the active key.
- **Point-in-time recovery of a server**: `STORAGE_WAL_ARCHIVE=<dir>` archives the WAL at each checkpoint, kept for `STORAGE_WAL_ARCHIVE_RETAIN` (7 days by default). `storage-cli restore-pitr` now restores a server's document data, takes an RFC 3339 time, and reads the archive directory and a leftover WAL directly (`--archive`, `--wal`). Standalone and single-group servers; range clusters keep their cluster backups.
- **Multi-region placement in range clusters**: localities in `--peers` (`id=host:port@region/zone`) and placement rules in `--zones <file>`: replica counts, required or avoided regions and zones, preferred leaseholder regions, zone or region survival, follower-read staleness, global collections and regional-by-row collections. Health reports list each node's ranges with their voters and leaders. Each node reloads its rules file within 2 seconds of a change; rules that do not fit are refused and reported, and the previous ones stay.
- `server-load` (`benches`): throughput and latency of writes and reads against a running server or cluster, and `tools/release/compare-versions.sh` to compare two source trees.
- `EngineConfig::wal_archive_dir`, `restore_point_in_time_with`, `StorageEngine::encryption`, `RaftNode::reseal_storage`, and `StorageError::Config` for invalid settings.

### Measured

- Against v0.1.0, with the new features off: no regression in the engine, network or server benchmarks ([`BENCHMARK_REPORT.md`](docs/BENCHMARK_REPORT.md) section 8.7). Found on the way, unchanged since 0.1.0: a single-group cluster commits a write in about 90 ms.

### Changed

- With encryption on, Raft log frames and snapshot records are sealed, so a node with encryption can only exchange snapshots with nodes holding the same keys. Data written without encryption is read as before.

## 0.1.1 (2026-11-13)

Binaries for Linux, macOS and Windows, and a web console. On-disk formats, the client protocol and the node-to-node protocol are unchanged from 0.1.0: a rolling upgrade works, and 0.1.0 and 0.1.1 nodes can run in one cluster.

### Added

- **Release binaries** for Linux (x86_64, arm64; glibc 2.28 or newer), macOS (Apple silicon, Intel; macOS 11 or newer) and Windows (x86_64), with `SHA256SUMS`. GitHub Actions builds, checks and smoke-tests them for each tagged release (`.github/workflows/release.yml`).
- **`storine-console`**: a web console in the browser. It shows the cluster and node health, browses, filters and edits documents with version checks, runs grouped aggregates and manages indexes. It listens on loopback, and only a browser opened with the link it prints (holding a per-run key) can use it.
- **macOS server support**: a `kqueue` reactor, like `epoll` on Linux and AFD poll on Windows. CI now runs the full test suite on macOS and Windows too.
- Rust SDK: `Collection::page` (one page of a query, with the total and the next page's key) and `Client::health`.

### Changed

- Release builds target each architecture's baseline CPU instead of the build machine's. Vector scoring (`dot_product_f32`, `cosine_similarity_f32`) moved to `storage-codec`, which picks an AVX2 + FMA copy at run time: a baseline build without it scored vectors 20 times slower. Other hot paths already picked hardware CRC32C, AES and AVX2 at run time. Measured against native builds on x86_64 and arm64: [`BENCHMARK_REPORT.md`](docs/BENCHMARK_REPORT.md) section 8.6.
- The bench runner's checksum benchmark measured a loop the compiler had partly hoisted on native builds. It now reads its input through `black_box`.

### Fixed

- A standalone server (no `--peers`) never cut its write-ahead log: nothing checkpointed its engine except an admin request, so the log grew with every write until a restart, which then replayed all of it. It now checkpoints every 10 s while it is written, as cluster nodes already did. New test: `a_standalone_server_cuts_its_wal`.

- **Windows: the write-ahead log could not be synced.** The WAL fsyncs through a second handle, which was opened read-only; Linux and macOS sync any descriptor, but Windows' `FlushFileBuffers` needs write access and refused, so every durable write failed. Append-mode files (Raft logs, index deltas) are now also opened for writing on Windows, with each write moved to the end. Windows had not been tested before 0.1.1; CI now runs the whole suite there.

- Binary-size ceilings (`docs/RULES.md`) apply to the baseline build that ships, which is larger than a native build. Released sizes, Linux x86_64: `storage-server` 4.6 MB, `storage-cli` 1.4 MB, `storine-sql` 6.7 MB, `storine-console` 0.8 MB; macOS arm64: 3.6, 1.2, 5.2 and 0.7 MB. The ceilings are now 5.5, 1.75, 8 and 1 MiB.

### Documentation

- The README is now a short introduction; [`docs/TECHNICAL.md`](docs/TECHNICAL.md) is a new technical reference written from the current code: every program, option, default and limit.
- Corrected: the native port accepts TLS 1.2 as well as 1.3; range clusters have change feeds (`storage-cli changefeed`); request body limits differ by opcode; the SQL gateway supports functions, procedures, triggers, cursors, temporary tables, materialized views and `LISTEN` / `NOTIFY`.

## 0.1.0 (2026-11-13)

The first release. Everything below is new.

### Engine

- Embedded storage engine (`StorageEngine<T>`, `DocumentStore`) with zero-copy memory-mapped reads, 64-byte record frames checked with CRC32C (hardware on x86_64 and arm64) and an XXH64-family hash, and zero third-party runtime crates.
- Write-ahead log with three policies: `Immediate`, `GroupCommit` (writers share one `fsync`) and `AsyncMemoryFirst`. Torn trailing frames are ignored on recovery. Recovery streams the log: memory stays bounded however long it is.
- Storage layouts: arena segments with live compaction (the default for documents), one file per record, and ping-pong slots.
- Indexes: primary hash index (in memory, or a cold on-disk tier), tag bitmaps, numeric ranges (in memory, or paged), HNSW vector search, and columnar segments for analytics.
- MVCC snapshots, transactions, conditional and idempotent writes, atomic field operations, exact decimals, TTL, schema migrations.
- Encryption at rest (ChaCha20-Poly1305) with key rotation and per-record policy.
- Online backup and point-in-time recovery.

### Server and clusters

- `storage-server`: a sharded non-blocking TCP server with a binary protocol ([`protocol/PROTOCOL.md`](protocol/PROTOCOL.md)), RBAC tokens and admission control.
- **TLS 1.3** on the native port: `STORAGE_TLS_CERT` / `STORAGE_TLS_KEY`, with no third-party crates (`storage-tls`). Handshakes run on their own threads with a 10 s deadline, so a slow client never stalls a reactor. TLS-off performance is unchanged.
- **Encrypted cluster traffic**: with `STORAGE_TLS_CA`, nodes verify each other's certificates (by the host named in `--peers`) and Raft and range traffic is encrypted. Peers still authenticate with `STORAGE_CLUSTER_TOKEN`, inside the encrypted channel.
- `storage-server gen-ca <dir>` and `gen-cert <ca-dir> <out-dir> <names>` issue an Ed25519 CA and node certificates.
- Single-group Raft replication with membership changes, and range-partitioned clusters (`--ranges`) with distributed transactions, rebalancing and multi-region placement.

### Clients

- SDKs for Python, TypeScript, Go, Java, C#, Rust, PHP and Ruby, checked against one set of protocol vectors. Each takes `storines://` URLs and a CA file for TLS.
- `storine-sql`: a SQL gateway speaking the PostgreSQL, MySQL and SQL Server wire protocols.
- `storage-cli`, including over TLS (`STORAGE_TLS_CA`, `STORAGE_TLS_SERVER_NAME`).

### Performance

Measured on AWS `c7i.xlarge` against LMDB and RocksDB at equal durability ([`docs/BENCHMARK_REPORT.md`](docs/BENCHMARK_REPORT.md) section 8):

- Storine leads in every throughput row and every p99 row.
- Network: a TCP `GET` round trip takes 8.6–9.0 µs, or 11.5–11.7 µs over TLS.
- Release binaries (Linux x86_64, stripped, no shared libraries beyond libc): `storage-server` 3.9 MB, `storage-cli` 1.3 MB, `storine-sql` 5.6 MB.

### Fixed before release

- A restarted range-cluster node could take minutes to come back. Range nodes stored one file per record, and recovery `fsync`ed one file per logged write. A node killed again before it finished started over each time. Range nodes now use the arena layout, as `DocumentStore` does. Recovery writes each record once per batch, and syncs record files in parallel. One stuck node's data now reopens in 2.3 s, where it had been at it for over 4 minutes.

- Range nodes never cut their WAL or compacted their storage while running: their periodic checkpoint only synced the log. In the soak, logs reached 374 MB in 25 minutes. Each node now checkpoints its engine every 5 s while written, on its own thread. In a 15-minute range soak, logs stayed under 3 MB.
- A checkpoint under `SyncPolicy::AsyncMemoryFirst` cut the WAL without syncing the records it had just flushed. Raft nodes sync the WAL themselves and then drop log entries, so a power loss (not a process crash) could have lost applied writes. Records are now synced before any WAL is cut.

### Known limitations

- Tail latency, against LMDB and RocksDB (`BENCHMARK_REPORT.md` section 8.4):
  - Fill p99.99 is 0.74 ms, against RocksDB's 16–20 µs.
  - 4-writer overwrite p99.99 is 2.3–3.0 ms, against 0.46–1.32 ms.
  - Durable p99.9 and p99.99 trail LMDB.
- Arena segments can hold up to twice the live data until compaction reclaims them.
- TLS supports one suite per version: TLS 1.3 with `TLS_CHACHA20_POLY1305_SHA256`, and TLS 1.2 with `TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256` for clients without 1.3; x25519 and Ed25519 certificates. Certificates from other CAs must use Ed25519 keys. Clients are not asked for certificates; they authenticate with tokens.
- Change feeds exist for range clusters only (`storage-cli changefeed`), and the SDKs do not expose them.
- Soak testing ran for 2 hours per mode under constant kills, pauses and partitions; both modes passed with balances conserved and every key linearizable. No multi-day soak has been run.
- A range-cluster node peaked at 1.12 GiB of resident memory in the soak, against 334 MiB for a single-group node.
- Upgrading from one 0.x minor version to another needs a stop of the whole cluster or a backup and restore. Rolling upgrades are supported only between patch releases of the same minor version ([`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md)).
