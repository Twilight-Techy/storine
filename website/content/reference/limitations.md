---
title: Known limitations
description: What Storine does not do yet, and where it trails other databases.
---

- **Tail latency** trails RocksDB in one case: 4 writers overwriting on 4 CPUs, p99.99 2.2–2.5 ms against 0.26–0.30 ms. That is the scheduler: four writers plus the flusher are more busy threads than CPUs. With a CPU to spare, it is about 70 µs. Bulk loads match RocksDB when the store is told its size (`expected_records`). Durable p99.9 and p99.99 trail LMDB on fast-sync disks ([benchmark report](../../../docs/BENCHMARK_REPORT.md), sections 8.5 and 8.11).
- **Single-group cluster writes wait for two disk syncs** in a row, the leader's log and then a follower's: about 8 ms on disks whose syncs take 4 ms. Concurrency raises throughput (section 8.8).
- **The server does not expose the cold and paged index tiers**. They are in the [embedded API](../embedding.md#cold-tiers).
- **Point-in-time recovery is not available on range clusters**, which have cluster backups instead.
- **Changing a node's locality** in a range cluster takes a restart of every node.
- **Range clusters list only collections with an index or metadata.**
- **TLS** supports one cipher suite per version, Ed25519 certificates only, and no client certificates. .NET on Windows cannot connect with TLS, as its TLS stack lacks Ed25519.
- **Change feeds** exist for range clusters only, through `storage-cli changefeed`. The client libraries do not expose them.
- **The local `storage-cli` data commands** work on the typed demo store, not on a server's document data.
- **Upgrades** between minor versions before 1.0 need every node of a cluster stopped, and a data directory written by a newer version cannot be opened by an older one ([upgrades](../../../docs/COMPATIBILITY.md)).
- **Client libraries** are installed from the repository; they are not on package registries yet.
- **Testing**: soaks ran for 2 hours per cluster mode under constant faults; no multi-day soak has run. Performance figures and soaks are from Linux; macOS and Windows pass the test suite in CI and smoke tests in the release build.
- **The SQL gateway** has limitations of its own, listed in its [documentation](../../../crates/storage-sql/README.md#known-limitations).
