---
title: Running a server
description: Every option, environment variable and default of storage-server, what it prints, and how it handles connections and load.
---

```text
storage-server [data_dir] [bind_addr] [options]
storage-server gen-ca <dir>
storage-server gen-cert <ca-dir> <out-dir> <name>[,<name>...]
storage-server gen-key <key-file>
```

`data_dir` defaults to `./server_data` and `bind_addr` to `127.0.0.1:7878`. The directory is created if it does not exist. Two processes cannot use one data directory at once.

```bash
storage-server ./data 127.0.0.1:7878
```

The server prints one line once it listens, then nothing more unless something needs attention:

```text
storage-server listening on 127.0.0.1:7878 (data_dir: ./data, mode: documents, max_connections: 100000, auth: disabled (loopback only))
```

On an error it prints `error: ...` and exits with status 1. It has no log file: run it under a service manager such as systemd, which keeps its output.

Stop it with `Ctrl C` or `SIGTERM`. Every acknowledged write is in the write-ahead log, so a stop of any kind, including a crash or `kill -9`, loses nothing that was acknowledged: the next start replays the log.

## Options

| Option | Default | Meaning |
| :--- | :--- | :--- |
| `--max-connections N` | `STORAGE_MAX_CONNECTIONS`, else 100,000 | Connections beyond this are answered `Overloaded` and closed |
| `--node-id N` | | This node's id in a cluster, 1 to 65,535 |
| `--peers 1=host:port,2=host:port,...` | | The cluster's founding members, this node included. On a range cluster each may add its locality: `1=host:port@region/zone` |
| `--join` | | Start without members and wait to be added to an existing cluster |
| `--ranges` | | Run as a range-cluster node. Needs `--node-id` and `--peers` |
| `--zones FILE` | | Placement rules for a range cluster, the same file on every node |
| `--records agent` | | Serve the typed demo record store instead of documents |
| `--once` | | Start, print the line above, and exit. For scripts and checks |
| `-h`, `--help` | | Print the options and exit |
| `-V`, `--version` | | Print the version and exit |

An unknown option is an error, so a mistyped flag never becomes the name of the data directory.

Clusters are described in [clusters](clusters.md), [range clusters](range-clusters.md) and [placement](placement.md).

## Environment variables

| Variable | Meaning |
| :--- | :--- |
| `STORAGE_AUTH_TOKENS` | `token:role,...` with roles `read`, `write` or `admin`. Tokens are at least 16 characters. Turns authentication on. Without it, the server refuses any address but loopback |
| `STORAGE_CLUSTER_TOKEN` | The token nodes use with each other, at least 16 characters, with the admin role. Required for a cluster with authentication |
| `STORAGE_TLS_CERT`, `STORAGE_TLS_KEY` | A PEM certificate chain and a PKCS#8 Ed25519 key. TLS on the port, for clients and for peers |
| `STORAGE_TLS_CA` | Certificates a node trusts when it calls its peers. Default: its own certificate |
| `STORAGE_RATE_LIMIT` | `rps:burst`: a token bucket per connection, one token per request. A connection that runs out gets `RateLimited` and is closed |
| `STORAGE_BACKUP_ROOT` | The folder under which admins may create and restore backups remotely. Unset: remote backups are off |
| `STORAGE_MAX_CONNECTIONS` | As `--max-connections` |
| `STORAGE_ENCRYPTION_KEYS` | A key file. Turns [encryption at rest](encryption.md) on |
| `STORAGE_ENCRYPTION_ACTIVE_KEY` | The key id that seals new data (default: the highest), or `none` to decrypt everything |
| `STORAGE_ENCRYPTION_ALLOW_PLAINTEXT` | `1` accepts existing plaintext while it is encrypted, to turn encryption on for a database that has data |
| `STORAGE_WAL_ARCHIVE` | A folder where checkpoints archive the WAL for [point-in-time recovery](backups.md#point-in-time-recovery). Not on range clusters |
| `STORAGE_WAL_ARCHIVE_RETAIN` | How long archived WAL is kept: `7d` by default, or hours (`48h`) or minutes (`90m`) |
| `STORINE_REACTOR=poll` | Use `poll` instead of `epoll`, `kqueue` or AFD poll |
| `STORINE_THREADS` | Analytics parallelism |
| `STORINE_SIMD=portable` | Use the portable column kernels instead of the CPU-specific ones |

## Making certificates and keys

The server makes its own TLS certificates and encryption keys, so you do not need OpenSSL:

| Command | Writes |
| :--- | :--- |
| `gen-ca <dir>` | `ca.crt` and `ca.key`: a certificate authority. Keep `ca.key` offline |
| `gen-cert <ca-dir> <out-dir> <name>[,<name>...]` | `node.crt` and `node.key`, signed by the CA, valid for each name (host names or IP addresses) |
| `gen-key <key-file>` | Adds a new encryption key to the file, creating it readable by its owner only |

Their use is in [security](security.md#tls) and [encryption at rest](encryption.md).

## Running it as a service

A minimal systemd unit:

```ini
[Unit]
Description=Storine
After=network-online.target

[Service]
User=storine
ExecStart=/usr/local/bin/storage-server /var/lib/storine 10.0.0.1:7878
EnvironmentFile=/etc/storine/env
Restart=on-failure
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```

Put the secrets in `/etc/storine/env`, readable by the service user only:

```bash
STORAGE_AUTH_TOKENS=app-token-0123456789abcdef:write,ops-token-0123456789abcdef:admin
STORAGE_TLS_CERT=/etc/storine/tls/node.crt
STORAGE_TLS_KEY=/etc/storine/tls/node.key
```

Raise the open-file limit if you expect many connections: each connection is one descriptor.

## Inside the server

- **Reactors.** One thread per CPU, between 2 and 16, runs non-blocking sockets with `epoll` on Linux, `kqueue` on macOS and AFD poll on Windows, so a wake-up costs time in proportion to the active connections, not all of them. An idle connection takes about 88 bytes. After a request a reactor keeps polling for 50 µs, which keeps a loopback read near 9 µs.
- **Admission control.** The server reads each request's 29-byte header first and, before reading the body, rejects an unknown opcode, a role too low for it, a body over its size limit, and a rate-limit breach. A rejected connection gets an error and is closed, so a client cannot make the server buffer a large body it may not send.
- **Concurrency limits.** At most 2 admin jobs (backup, compaction) and 8 analytic queries run at once.
- **TLS handshakes** run on 4 threads with a 10-second deadline and a queue of 1,024, so a slow client never holds a reactor.
- **Checkpoints.** A standalone server checkpoints every 10 seconds while it is written: it flushes buffered records, syncs the files, and cuts the write-ahead log, so a restart replays only the last few seconds.
- **Memory.** Freed heap memory is returned to the operating system every 10 seconds.

The server has no web code: the [console](console.md) and the [SQL gateway](../../crates/storage-sql/README.md) are separate programs that connect to it as clients.
