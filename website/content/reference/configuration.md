---
title: Configuration reference
description: Every command-line option, environment variable and connection-string parameter of every Storine program, on one page.
---

## storage-server

```text
storage-server [data_dir] [bind_addr] [options]
storage-server gen-ca <dir>
storage-server gen-cert <ca-dir> <out-dir> <name>[,<name>...]
storage-server gen-key <key-file>
```

| Argument or option | Default | Meaning |
| :--- | :--- | :--- |
| `data_dir` | `./server_data` | The data directory |
| `bind_addr` | `127.0.0.1:7878` | The address to listen on |
| `--max-connections N` | 100,000 | Connection ceiling |
| `--node-id N` | | Cluster node id, 1–65,535 |
| `--peers 1=host:port[@region/zone],...` | | Founding members, this node included |
| `--join` | | Wait to be added to a cluster |
| `--ranges` | | Range-cluster node |
| `--zones FILE` | | Placement rules |
| `--records agent` | | Serve the typed demo store |
| `--once` | | Start, report, exit |
| `-h`, `--help`; `-V`, `--version` | | Help; version |

| Environment variable | Default | Meaning |
| :--- | :--- | :--- |
| `STORAGE_AUTH_TOKENS` | none: loopback only | `token:role,...`; roles `read`, `write`, `admin`; tokens of 16+ characters |
| `STORAGE_CLUSTER_TOKEN` | | Node-to-node token, 16+ characters. Required for an authenticated cluster |
| `STORAGE_TLS_CERT`, `STORAGE_TLS_KEY` | | PEM certificate chain and PKCS#8 Ed25519 key: TLS on |
| `STORAGE_TLS_CA` | the node's own certificate | Certificates trusted for peers |
| `STORAGE_RATE_LIMIT` | none | `rps:burst` per connection |
| `STORAGE_MAX_CONNECTIONS` | 100,000 | As `--max-connections` |
| `STORAGE_BACKUP_ROOT` | none: remote backups off | Where remote backups go |
| `STORAGE_ENCRYPTION_KEYS` | none: encryption off | Key file |
| `STORAGE_ENCRYPTION_ACTIVE_KEY` | the highest key id | Sealing key, or `none` to decrypt |
| `STORAGE_ENCRYPTION_ALLOW_PLAINTEXT` | off | `1`: accept plaintext while encrypting |
| `STORAGE_WAL_ARCHIVE` | none | WAL archive folder for point-in-time recovery |
| `STORAGE_WAL_ARCHIVE_RETAIN` | `7d` | Archive retention: `Nd`, `Nh` or `Nm` |
| `STORAGE_EXPECTED_DOCUMENTS` | `0`: grow as needed | About how many documents the database will hold, at most 1,000,000,000 (`_` or `,` may group digits). The index is sized for them at start. On a range cluster, set the whole cluster's count on every node; each sizes for its share |
| `STORINE_REACTOR` | the platform's best | `poll` to use `poll` |
| `STORINE_THREADS` | the CPU count | Analytics parallelism |
| `STORINE_SIMD` | the CPU's best | `portable` for the portable kernels |

Details: [running a server](../server.md).

## Connection strings (clients)

```text
storine://[token@]host:port[,host:port...][?timeout_ms=5000&follower_reads=1]
storines://...                                         TLS
```

| Parameter | Default | Meaning |
| :--- | :--- | :--- |
| `token@` | none | Authentication token |
| several hosts | | Seeds of a cluster. The client finds the leader |
| `timeout_ms` | 5,000 | Request timeout |
| `follower_reads` | `0` | `1`: reads may go to followers and be slightly behind |
| `tls_ca` | | PEM file of certificates to trust. Turns TLS on |
| `tls_server_name` | the host connected to | The name the server's certificate must carry |

Each client also takes these as options, plus a pool size. Details: [clients](../clients/index.md).

## storage-cli

| Environment variable | Meaning |
| :--- | :--- |
| `STORAGE_TOKEN` | Token for remote commands |
| `STORAGE_TLS_CA` | PEM certificates to trust: TLS on |
| `STORAGE_TLS_SERVER_NAME` | Expected certificate name. Default: the address |

Commands: [command-line tool](../cli.md).

## storine-console

| Option or variable | Default | Meaning |
| :--- | :--- | :--- |
| `--listen ADDR` | `127.0.0.1:7880` | Where to serve |
| `--allow-host HOST:PORT` | | Another `Host` header to answer to. Repeatable |
| `storine://...` | | The server or cluster |
| `STORAGE_TOKEN`, `STORAGE_TLS_CA`, `STORAGE_TLS_SERVER_NAME` | | As for `storage-cli` |
| `STORINE_CONSOLE_KEY` | random per run | A fixed console key, 16+ letters, digits, `-` or `_` |

Details: [the web console](../console.md).

## storine-sql

```text
storine-sql (--data DIR | --storine URL) [options]
```

| Option or variable | Default | Meaning |
| :--- | :--- | :--- |
| `--data DIR` | | Serve an embedded database in `DIR` |
| `--storine URL` | | Serve in front of a running server or cluster |
| `--pg ADDR`, `--no-pg` | `127.0.0.1:5432` | PostgreSQL endpoint |
| `--mysql ADDR`, `--no-mysql` | `127.0.0.1:3306` | MySQL endpoint |
| `--mssql ADDR`, `--no-mssql` | `127.0.0.1:1433` | SQL Server endpoint |
| `--user NAME:PASSWORD` (repeatable), `STORINE_SQL_USERS` | none: every client trusted | Users |
| `--database NAME` | `storine` | The default database |
| `--tls-cert FILE --tls-key FILE` | a generated certificate for localhost | TLS certificate |
| `--no-tls`, `--require-tls` | | Turn TLS off; refuse clients without it |
| `--max-connections N` | 1,024 | Open connections. A client silent for 30 s before logging in is closed |
| `--oidc-issuer URL`, `--oidc-audience AUD`, `--oidc-jwks FILE`, `--oidc-user-claim CLAIM` | | SQL Server access-token logins |
| `--kerberos-keytab FILE`, `--kerberos-keep-realm` | | SQL Server Kerberos logins |
| `STORINE_SQL_LOG=1` | off | Log every statement |

Details: [SQL gateway](../../../crates/storage-sql/README.md).

## Install scripts

| Variable | Default | Meaning |
| :--- | :--- | :--- |
| `STORINE_VERSION` | the latest release | Version to install |
| `STORINE_INSTALL_DIR` | `~/.local/bin`, `/usr/local/bin` as root, `%LOCALAPPDATA%\Programs\Storine\bin` | Install folder |

## Embedded engine

`EngineConfig` fields are listed in [embedding the engine](../embedding.md#configuration).

## Placement rules

The `--zones` file format is in [placement across regions](../placement.md#rules).
