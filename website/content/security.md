---
title: Security
description: Tokens and roles, TLS for clients and between cluster nodes, request limits, and how the server protects itself before a client authenticates.
---

A new server is safe by default: without tokens, it only serves on a loopback address and refuses to start on any other. Before you open it to a network, give it tokens, and usually TLS.

## Checklist

1. Set `STORAGE_AUTH_TOKENS` with one token per application and per operator, each with the lowest role it needs.
2. Turn on TLS with `STORAGE_TLS_CERT` and `STORAGE_TLS_KEY`.
3. In a cluster, set `STORAGE_CLUSTER_TOKEN` and give every node a certificate.
4. Turn on [encryption at rest](encryption.md) if the disks or backups could fall into other hands.
5. Keep the console and the SQL gateway on loopback, or behind TLS and passwords.
6. Firewall the port to the machines that need it.

## Tokens and roles

```bash
STORAGE_AUTH_TOKENS="web-app-0123456789abcdef:write,reports-0123456789abcdef:read,ops-0123456789abcdef:admin" \
  storage-server ./data 10.0.0.1:7878
```

`STORAGE_AUTH_TOKENS` is a comma-separated list of `token:role`. Tokens are at least 16 characters; make them long and random, for example with `openssl rand -hex 24`.

| Role | May |
| :--- | :--- |
| `read` | Read documents, query, aggregate, read health, metrics and cluster information |
| `write` | Everything `read` may, and write documents, run transactions, create and drop indexes, vector indexes and columnar storage, set metadata and drop collections |
| `admin` | Everything `write` may, and back up, restore, compact, checkpoint, change cluster membership and read change feeds. Cluster nodes use this role with each other |

A client sends its token once per connection, in the connection string (`storine://token@host:port`) or as an option. The server compares tokens in constant time. A request the role does not allow gets `Unauthorized`, and the connection is closed.

To change tokens, change the variable and restart the server. In a cluster, restart one node at a time.

## TLS

The server makes its own Ed25519 certificates:

```bash
storage-server gen-ca ./tls/ca                                    # ca.crt, ca.key: keep ca.key offline
storage-server gen-cert ./tls/ca ./tls/db1 10.0.0.1,db1.internal  # node.crt, node.key for those names
```

Then point the server at its certificate and key:

```bash
STORAGE_TLS_CERT=./tls/db1/node.crt STORAGE_TLS_KEY=./tls/db1/node.key \
STORAGE_AUTH_TOKENS=... storage-server ./data 10.0.0.1:7878
```

With a certificate, the port requires TLS from every client and every peer. Clients connect with `storines://`, or give the CA to trust with `tls_ca`, which also turns TLS on:

```text
storines://token@db1.internal:7878?tls_ca=/etc/storine/ca.crt
storine://token@10.0.0.1:7878?tls_ca=/etc/storine/ca.crt&tls_server_name=db1.internal
```

`tls_server_name` sets the name the certificate must carry, when it differs from the host you connect to. `storage-cli` and the console use `STORAGE_TLS_CA` and `STORAGE_TLS_SERVER_NAME`.

What the server speaks:

- TLS 1.3 with `TLS_CHACHA20_POLY1305_SHA256`;
- TLS 1.2 with `TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256` and extended master secret, for clients that offer nothing newer;
- key exchange with `x25519`, and Ed25519 certificates, from `gen-ca` and `gen-cert` or from any CA that issues Ed25519 keys.

Full handshakes only, and clients are not asked for certificates. Every client supports this, except .NET on Windows, whose TLS stack lacks Ed25519.

### TLS between nodes

With a certificate set, Raft and range traffic between nodes is encrypted too. Each node checks its peers' certificates against `STORAGE_TLS_CA` (default: its own certificate), by the host each peer is listed under in `--peers`. So give each node a certificate for the address it is listed under, all signed by one CA:

```bash
storage-server gen-ca ./tls/ca
for n in 1 2 3; do storage-server gen-cert ./tls/ca ./tls/n$n 10.0.0.$n; done
# on node n: STORAGE_TLS_CERT=tls/n$n/node.crt STORAGE_TLS_KEY=tls/n$n/node.key STORAGE_TLS_CA=tls/ca/ca.crt
```

Peers still authenticate with `STORAGE_CLUSTER_TOKEN` inside the encrypted channel.

## Limits that protect the server

The server checks each request's header before reading its body, and closes the connection of a client that sends one it may not:

| Check | Limit |
| :--- | :--- |
| Body size, per operation | 256 MiB writes and transactions; 1 MiB queries, aggregates and admin requests; 128 KiB document reads; 4 KiB authentication and backups |
| Role | The operation must be allowed for the connection's role |
| Rate | `STORAGE_RATE_LIMIT=rps:burst`: a token bucket per connection, one token per request |
| Connections | `--max-connections`, 100,000 by default. Beyond it, `Overloaded` |
| Analytic queries, admin jobs | 8 and 2 at a time |
| TLS handshakes | 10 seconds each, on 4 threads, at most 1,024 waiting |

## The console and the SQL gateway

- The [console](console.md) listens on loopback, and only a browser holding the link it prints can use it. On another address it serves plain HTTP: use an SSH tunnel instead. It connects to Storine with its own token, so give it a `read` token if it only needs to look.
- The [SQL gateway](../../crates/storage-sql/README.md) trusts every client unless you give it users with `--user`, and serves TLS with a generated certificate by default. Keep it on loopback without users.

## Reporting a vulnerability

Please report security problems privately through GitHub's [security advisories](https://github.com/Twilight-Techy/storine/security/advisories/new) rather than in a public issue.
