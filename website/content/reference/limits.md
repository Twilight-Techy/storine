---
title: Limits
description: Every size, count and rate limit in Storine, in one table.
---

| Limit | Value |
| :--- | :--- |
| Document key | 1,024 bytes of UTF-8 |
| Collection name | 255 bytes |
| Document or transaction body | 256 MiB |
| Value nesting | 32 levels |
| Decimal scale | 38 digits |
| Secondary indexes per collection | 16, each on one top-level field |
| Vector indexes per collection | 4, up to 4,096 dimensions; `k` up to 10,000 |
| Query page | 1,000 by default, 10,000 at most |
| Sort keys | 16 |
| Fields in one projected read | 256 |
| Ranged read of a value | 16 MiB per call |
| Aggregate result | 1,000,000 rows, 64 group keys, 64 aggregates, expressions 32 deep, 256 MiB response |
| Aggregate memory | 256 MiB of group tables by default, then spill up to 16 GiB |
| Columnar scan page | 1,000 by default, 100,000 at most; 64 fields |
| Request bodies by operation | 256 MiB writes and transactions; 1 MiB queries, aggregates, collection commands and scans; 128 KiB document reads; 4 KiB authentication and backups; 512 bytes membership changes |
| Connections | 100,000 per server by default (`--max-connections`) |
| Concurrent analytic queries | 8 per server |
| Concurrent admin jobs | 2 per server |
| TLS handshakes | 10 seconds each, 1,024 waiting |
| Request id deduplication | 24 hours |
| Raft members | 256 |
| Replicas per range | 1 to 15, 3 by default |
| Clock offset between range nodes | 500 ms |
| Tokens | at least 16 characters |
| Console connections | 64 at once |
| SQL gateway connections | 1,024 by default; 30 seconds to log in |
