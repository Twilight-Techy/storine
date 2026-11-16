---
title: Clients
description: Official clients for eight languages share one API, need only their standard library, apply every write at most once, and follow a cluster's leader on their own.
---

| Language | Requires | Money type | Page |
| :--- | :--- | :--- | :--- |
| Python | 3.8 | `decimal.Decimal` | [Python](../../../sdks/python/README.md) |
| TypeScript, JavaScript | Node.js 18 | `Decimal` | [TypeScript](../../../sdks/typescript/README.md) |
| Go | 1.21 | `storine.Decimal` | [Go](../../../sdks/go/README.md) |
| Java | 17 | `BigDecimal` | [Java](../../../sdks/java/README.md) |
| C# | .NET 8 | `decimal` | [C#](../../../sdks/csharp/README.md) |
| Rust | 1.70; 1.82 for TLS (the `tls` feature) | `storine::Decimal` | [Rust](../../../sdks/rust/README.md) |
| PHP | 8.1 | `Storine\Decimal` | [PHP](../../../sdks/php/README.md) |
| Ruby | 3.2, with the `bigdecimal` gem | `Storine::Decimal` | [Ruby](../../../sdks/ruby/README.md) |

No SQL driver needed? The [SQL gateway](../../../crates/storage-sql/README.md) lets PostgreSQL, MySQL and SQL Server drivers and ORMs talk to Storine.

## Installing

The clients are installed from the repository; they are not on package registries yet. Each client's page has its install command, and so does the [quick start](../quick-start.md#3-get-a-client).

## One API

Every client uses the same words, in its language's style:

| Area | Operations |
| :--- | :--- |
| Connecting | `connect(url, options)`, `collection(name)`, `transaction()`, `collections()`, `close()` |
| Writing | `put`, `insert`, `replace`, `update`, `delete`, with `if_version` |
| Field operations | `set`, `unset`, `add(path, amount).min(...).max(...)`, `append` |
| Reading | `get`, `get` with `fields`, `read_bytes`, `stream_bytes` |
| Querying | `find`, `first`, `count`, `scan`, `nearest` |
| Filters | `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `between`, `in`, `prefix`, `contains`, `exists`, `missing`, `key_prefix`, `and`, `or`, `not` |
| Indexes | `create_index`, `drop_index`, `create_vector_index`, `drop_vector_index`, `vector_indexes`, `info`, `drop` |
| Analytics | `aggregate`, `scan_columns`, `enable_columnar`, `disable_columnar` |

The guides ([documents](../documents.md), [queries](../queries.md), [indexes](../indexes.md), [analytics](../analytics.md)) explain each one with Python examples.

## Connection strings

```text
storine://[token@]host:port[,host:port...][?timeout_ms=5000&follower_reads=1]
```

- Several hosts are a cluster's seeds. The client finds the leader.
- `storines://` connects with TLS. `tls_ca` names the PEM certificates to trust (giving it also turns TLS on), and `tls_server_name` the name the certificate must carry. Both may be options or query parameters.
- .NET on Windows cannot use TLS, as its TLS stack lacks Ed25519.

## What every client does for you

- **A connection pool**, safe to share between threads or tasks.
- **Authentication** right after connecting, when a token is given.
- **At-most-once writes.** Each logical write gets a random request id, reused on every retry after a timeout, a reconnect, `CommitUnknown` or a leader change. The server applies an id at most once within 24 hours.
- **Failover.** On `NotLeader` it retries on the leader; when a server is unreachable it tries another seed.
- **Exact numbers.** Decimals map to the language's exact decimal type, never to a float.
- **Backoff** on `Overloaded` for analytic queries.

## How they are tested

All eight pass the same byte-for-byte [protocol vectors](../../../protocol/vectors/vectors.json), and the same end-to-end suite against a real server: standalone, as a 3-node cluster with its leader killed, and over TLS. The release checks run every suite against a single server and against a range cluster.

## Writing a client for another language

Implement sections 3 to 6 of the [wire protocol](../../../protocol/PROTOCOL.md), make every vector pass, then port an existing client's end-to-end suite. See [writing a client](../../../sdks/README.md).
