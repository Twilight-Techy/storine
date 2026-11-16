---
title: Introduction
description: Storine is a document database you can embed in a Rust program, run as a server, or replicate across a cluster. This page explains what it is, what ships in a release, and where to go next.
---

Storine stores **collections of documents**. A document is a key and a map of fields, much like a JSON object, with a few extra types that JSON lacks: exact decimals, timestamps, UUIDs, 128-bit integers and raw bytes. Every write gives the document a new version, writes can check a version first, and one request can change many documents across collections atomically.

The engine is written in Rust with no third-party runtime crates. The network server, TLS, the SQL gateway and the web console are built on the standard library alone, and each program is a single file of a few megabytes.

## Three ways to run it

| | How | Read |
| :--- | :--- | :--- |
| **Embedded** | Link the `storage-core` crate into a Rust program and open a store in a folder | [Embedding the engine](embedding.md) |
| **Server** | Run `storage-server` and connect from any of eight languages, from SQL tools or from the web console | [Running a server](server.md) |
| **Cluster** | Start three or five servers that know each other. They replicate with Raft and fail over on their own. With `--ranges`, data is split into ranges spread across nodes and placed by region | [Clusters](clusters.md) |

The data model and the guarantees are the same in all three.

## What a release contains

Each release ships four programs for Linux, macOS and Windows:

| Program | What it does |
| :--- | :--- |
| `storage-server` | The database server, standalone or as a cluster node. It also creates TLS certificates and encryption keys |
| `storine-console` | A web console for any server or cluster: browse and edit documents, run aggregates, manage indexes, watch health |
| `storine-sql` | A gateway that speaks the PostgreSQL, MySQL and SQL Server protocols, so existing apps can use Storine |
| `storage-cli` | Administration from the command line: health, metrics, backups, restores, membership changes, change feeds |

Client libraries for Python, TypeScript and JavaScript, Go, Java, C#, Rust, PHP and Ruby live in the repository under `sdks/`. See [Clients](clients/index.md).

## What makes it different

- **Exact numbers.** Decimals keep up to 38 digits and map to each language's exact decimal type, never to a float. Server-side `add` is checked: integers stay integers, overflow is refused, and a bound such as `min=0` fails the whole request instead of letting a balance go negative.
- **Writes happen once.** Clients tag each logical write with a request id and reuse it on every retry. The server applies a request id at most once within 24 hours, so timeouts, reconnects and leader changes never apply a write twice.
- **Fast.** Reads take about 60 nanoseconds in process and 9 microseconds over the network. Records are memory-mapped and decoded in place without copying. See the [benchmark report](../../docs/BENCHMARK_REPORT.md).
- **Secure defaults.** Without tokens, a server only listens on localhost. Tokens carry a role, TLS takes two settings, and a key file encrypts everything on disk.
- **Small.** No runtime dependencies, and every tool is one file you can copy to a machine.

## Where to go next

- [Install Storine](installation.md), then follow the [quick start](quick-start.md).
- Learn the [core concepts](concepts.md): collections, documents, values, versions and request ids.
- Running it for real? Read [security](security.md), [backups](backups.md) and [monitoring](monitoring.md).
- Every option on one page: the [configuration reference](reference/configuration.md).

> [!NOTE]
> Storine {{version}} is young. Its test suite, crash tests and 2-hour soak tests under constant faults pass on every release, but it has not yet run in production for long. Read the [known limitations](reference/limitations.md), and keep backups.
