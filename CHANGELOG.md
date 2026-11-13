# Changelog

Versions follow [Semantic Versioning](https://semver.org). Before 1.0, a minor version (0.x) may change on-disk or wire formats; [`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md) says what each kind of release may change and how to upgrade.

## 0.1.0 (unreleased)

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
- TLS supports one suite: TLS 1.3 with `TLS_CHACHA20_POLY1305_SHA256`, x25519 and Ed25519 certificates. Certificates from other CAs must use Ed25519 keys. Clients are not asked for certificates; they authenticate with tokens.
- Change feeds are not in the wire protocol yet.
- Soak testing ran for 2 hours per mode under constant kills, pauses and partitions; both modes passed with balances conserved and every key linearizable. No multi-day soak has been run.
- A range-cluster node peaked at 1.12 GiB of resident memory in the soak, against 334 MiB for a single-group node.
- Upgrading from one 0.x minor version to another needs a stop of the whole cluster or a backup and restore. Rolling upgrades are supported only between patch releases of the same minor version ([`docs/COMPATIBILITY.md`](docs/COMPATIBILITY.md)).
