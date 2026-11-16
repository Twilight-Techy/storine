---
title: Troubleshooting
description: Symptoms you may meet when running Storine, what causes them, and how to fix them.
---

## The server will not start

| Message | Fix |
| :--- | :--- |
| `refusing to serve without authentication on a non-loopback address` | Set `STORAGE_AUTH_TOKENS`, or listen on `127.0.0.1`. See [security](security.md) |
| `STORAGE_AUTH_TOKENS entries must be token:role` | Write tokens as `token:role,token:role`, with roles `read`, `write` or `admin` |
| `I/O error during acquire exclusive database lock (another process or engine instance already holds .lock)` | Another process has the data directory open. Stop it first: two processes cannot share one |
| `--peers must list this node's own --node-id` | Include this node in `--peers` |
| `STORAGE_CLUSTER_TOKEN must be at least 16 characters` | Use a longer node token, the same on every node |
| `STORAGE_TLS_CERT needs STORAGE_TLS_KEY` | Set both |
| An unknown option | Check the spelling against `storage-server --help` |
| The key file is refused | The key file must be readable by its owner only: `chmod 600` |

## Clients cannot connect

- **`UnavailableError`**: no seed answered. Check the address, the firewall, and that the server listens on that interface: a server on `127.0.0.1` is not reachable from other machines.
- **`UnauthorizedError`**: the token is missing, wrong, or its role is too low for what you asked. Writes need `write`; backups and membership changes need `admin`.
- **TLS errors**: the server has a certificate, so use `storines://` or `tls_ca`. The name you connect to must be in the certificate, or set `tls_server_name`. .NET on Windows cannot use TLS.
- **`RateLimitedError`**: the connection exceeded `STORAGE_RATE_LIMIT`.
- **`OverloadedError`**: the server is at `--max-connections`, or 8 analytic queries are running. Retry later.

## Writes fail

- **`ConflictError` on `insert`**: the key exists. Use `put` to create or replace.
- **`ConflictError` with `if_version`**: someone changed the document since you read it. Read it again.
- **`InvalidError` on `update`**: an `add` crossed its `min` or `max`, mixed incompatible kinds (a float into a decimal), or overflowed. Nothing changed.
- **`InvalidError` on a vector field**: the field has a vector index, and the value is not a list of exactly that many numbers.
- **`NotFoundError` on `update`**: `update` changes existing documents only. Use `put` first.

## A cluster has no leader

A cluster needs a majority of its voters to elect a leader: 2 of 3, or 3 of 5. Check that the nodes can reach each other on their ports, that all have the same `STORAGE_CLUSTER_TOKEN`, and with TLS, that each certificate names the host its node is listed under in `--peers`. `storage-cli cluster <addr> members` shows what each node thinks.

## Range-cluster transactions fail or wait

Clocks must agree within 500 ms. Run NTP on every node.

## Disk usage keeps growing

- **Dead space** in segments is reclaimed by compaction once at least 64 MiB is dead and it is half of what is used. Check `arena.dead` in the health report, and run `storage-cli remote-compact` to compact now.
- **The WAL** is cut at each checkpoint. On a server that is written, that happens every 10 seconds. An embedded program must call `checkpoint()` itself.
- **The WAL archive** keeps `STORAGE_WAL_ARCHIVE_RETAIN` of history, 7 days by default.

## Memory

Memory-mapped files are counted in the page cache, which the operating system reclaims under pressure. The process's own memory is bounded by the buffer of unflushed writes (64 MiB), the indexes, and up to 256 MiB per analytic query. A high `hot_dirty_bytes` means the disk cannot keep up with writes.

## The console says the link is invalid

Each run makes a new key, so links from an earlier run stop working. Use the link printed by the running console, or set `STORINE_CONSOLE_KEY` for a fixed one. When reaching it through another host name, add `--allow-host`.

## Still stuck?

Search the [issues](https://github.com/Twilight-Techy/storine/issues), or open one with the version (`storage-server --version`), the operating system, the command line, and the error.
