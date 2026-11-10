# Work Log Since the Last `main` Fast-Forward

- **Branch**: `claude/intelligent-carson-x9hkkg`
- **Merged**: on 2026-11-09, `main` was merged into the branch, and `main` was then fast-forwarded to it. Work continues on `main`, and this file is kept as the record of the branch.
- **Base**: `main` at `2fb4dc1` (2026-10-27, "TASK-091 size budget to 1 MiB; TASK-090 epoll reactor on Linux")
- **Head when written**: `5389fd8` (2026-10-29), 90 commits ahead of `main`
- **Detailed status per task**: [`TASKS.md`](TASKS.md). This file is the summary: what was done, what was in progress when work stopped, and what is left.

## 1. What the work was for

The work followed the follow-up prompts recorded in [`MASTER_PROMPT.md`](MASTER_PROMPT.md):

| Stages | Goal |
|---|---|
| 22–27 | Money-grade write safety, replication integrity, client SDKs, a SQL gateway, vector search |
| 28 | Compete with DuckDB / ClickHouse on columnar analytics |
| 29 | Deterministic simulation and fault injection |
| 30 | Compete with CockroachDB / Spanner on scale-out and multi-region |
| 31 | Compete on the enterprise PostgreSQL ecosystem |

All of it is bound by [`RULES.md`](RULES.md):
- zero external runtime crates;
- `unsafe` only in `storage-codec` / `storage-mmap`;
- interleaved A/B/A/B benchmarks behind every "faster" claim.

The latest instruction from the user is to do everything that remains, **except** the binary-size work and the Windows task (TASK-102).

## 2. Done (committed and pushed)

### Stage 22 — Write safety (TASK-093 – 098)
- Monotonic versions across delete and re-create (no ABA).
- An exact `Decimal` type, and `i128` / Decimal indexing.
- Nested values with atomic field operations.
- One conditional commit path.
- A document store and its wire protocol (`OP_DOC_*`, `OP_TXN`).

### Stage 23 — Replication integrity (TASK-099 – 101, 121)
- Deterministic replicated commands and a replica write guard.
- `CommitUnknown` outcomes, pre-vote and leader leases.
- A networked Raft runtime and a clustered TCP server with redirects and follower reads.
- Dynamic membership: learners, promotion, removal.

### Stage 24 — Windows reactor (TASK-102)
- AFD-poll / IOCP reactor implemented and exercised under Wine.
- **Left open on purpose**, per the user.

### Stage 25 — SDKs (TASK-103 – 111)
- Protocol specification and golden conformance vectors.
- SDKs in Python, TypeScript, Go, Java, C#, Rust, PHP and Ruby.

### Stage 26 — SQL gateway (TASK-112 – 120)
- PostgreSQL, MySQL and SQL Server (TDS) wire endpoints.
- A real-client and ORM compatibility matrix.
- Lost-update-free writes and REPEATABLE READ / SERIALIZABLE.
- Zero-dependency TLS.
- OpenID Connect and Kerberos logins for SQL Server.

### Stage 27 — Vector search and large values (TASK-122 – 124)
- Named HNSW indexes and pgvector compatibility.
- Ranged reads of large values.
- PgBouncer support, session resets and application locks.

### Stage 28 — Columnar HTAP analytics (TASK-125 – 131)
- Column segments and HTAP maintenance.
- Vectorized kernels, parallel memory-bounded execution with zone maps and spill.
- An aggregate / column-scan protocol in all eight SDKs.
- SQL push-down, including hash joins over projected scans.
- A benchmark report against DuckDB, run interleaved (`BENCHMARK_REPORT.md`).

### Stage 29 — Simulation and fault injection (TASK-132 – 136)
- **TASK-132**: a `Vfs` I/O seam with simulated power loss (torn sectors, lost directory operations). Several durability bugs it exposed were fixed (missing directory syncs and others).
- **TASK-133**: a deterministic Raft simulator. Raft outcome and stale-suffix bugs it exposed were fixed.
- **TASK-134**: a linearizability checker, plus process chaos against real binaries behind a fault proxy (SIGKILL / SIGSTOP).
- **TASK-135**: structure-aware fuzzing of every decoder and parser, including the SQL gateway, Kerberos and OIDC tokens.
- **TASK-136**: crash-point sweeps.

### Stage 30 — Scale-out (partially done)
- **TASK-137, multi-Raft ranges.** Many range groups share one engine and WAL. Splits are metadata-only. Each node pair exchanges one batch, and idle ranges quiesce.
- **TASK-139, hybrid logical clocks.** HLC with offset enforcement and self-fencing, uncertainty windows and commit-wait.
- **TASK-140, distributed transactions.**
  - MVCC intents and transaction records.
  - Parallel commits, read refresh and wound-wait.
  - Recovery of abandoned transactions, and definite outcomes for the client.
  - Checked for serializability by the bank-transfer simulator.
- **TASK-138, done parts** (commits `13e5642`, `cca0ed9`):
  - **Replica moves and lease transfers**: add learner → promote → remove; `TimeoutNow`; `Gone` notices for removed replicas.
  - **Merges**: a `Freeze` command, then a `Merge` command that carries the absorbed range's final records.
  - **Allocator** (`dist/alloc.rs`, a pure planner run on leaseholders): repair; replica and lease rebalancing by count and bytes; size and load splits; merges of small neighbours.
  - **Replica-safety rules**:
    - a node never holds two live replicas of one key;
    - destroyed replicas leave tombstones, ordered by (term, index);
    - snapshots carry their key span (format 3);
    - leaders never send a snapshot older than the range's last split or merge.
  - **Range descriptors and range caches** (`dist/meta.rs`).
  - Six multi-range simulations pass 400 seeds each: ranges, moves, region outages, merges, the allocator, and idle quiescence.
  - Every bug found along the way is listed under TASK-138 in `TASKS.md`.

## 3. Resumed 2026-11-03

- **TASK-138**: done. The two failing seeds were a checker bug (seed 12) and a merge-chain allocator bug (seed 10, and seed 266 found by the sweep). Details are in `TASKS.md`.
- **TASK-141**: done. Zone configurations, survival goals, lease preferences, closed timestamps with follower reads, `AS OF` transactions, global tables and regional-by-row layout.
- **Six older bugs found by the 300-seed sweeps and fixed** (details in `TASKS.md` and `PROGRESS_LOG.md`): parallel-commit intents resolved before the record was final; replicas diverging on node-local version preconditions; Raft waiters taking another proposal's outcome at a reused index; a repeated split creating duplicate ranges; rebalancing oscillation in back-to-back turns; neighbours aligning a range in the round it froze. All pinned as regression seeds.
- **Verification**: every multi-range and transaction simulation passes 300 seeds; full `storage-core`, `storage-index` and `storage-sql` suites pass; workspace clippy is clean.
- **TASK-142, TASK-143**: done. `storage-server --ranges`, the document layer, one-phase commits, all 8 SDK suites and SQL against range clusters, chaos with real processes. Known limits are listed in `PROGRESS_LOG.md`.
- **Heavy compute runs on Modal**: `tools/modal/sweep.py`.

## 4. Left to do

### Stage 30
- **TASK-144**: done. `dist/backup.rs`, `dist/feed.rs`, `OP_ADMIN_RESTORE`, `OP_CHANGEFEED`, `storage-cli changefeed`. Stage 30 is complete; Stage 31 (`TASK-145` onward) is next.
- **TASK-145**: done. `exec/acl.rs`, `parser/acl.rs`, `tests/access_control.rs`.
- **TASK-146**: done. `exec/control.rs`, `tests/control.rs`.
- **TASK-147**: done. `exec/activity.rs`, `exec/explain.rs`, `tests/activity.rs`.
- **TASK-148**: done. `exec/stats.rs`, `exec/optimizer.rs`, `tests/optimizer.rs`. Fixed a Raft §8 stale read in single-group mode.
- **TASK-149**: done. `exec/temp.rs`, `exec/cursor.rs`, `exec/routine.rs`, `exec/notify.rs`, `parser/plpgsql.rs`, `nettypes.rs`, `tests/sql_surface.rs`.
- **TASK-150**: done. `tools/modal/sweep.py --tools all`; 0 JDBC failures and 177 tool checks with 0 failures.
- **TASK-151**: done 2026-11-10 (reopened 2026-11-08 for memory). The causes were found and fixed: a leader resend flood, a single-group WAL that was never checkpointed, and allocator retention. Range mode now passes the 2-hour soak at under 1.6 GiB. Single group passed its final read in a 2026-11-09 rerun but failed the memory check: arena segments were never compacted on their own. That was fixed on 2026-11-10, and the single-group rerun passed (peak RSS 484–1,270 MiB, disk 121–378 MiB). Done (`TASKS.md`).
- **TASK-152**: done. The final audit; findings and fixes are in `TASKS.md`.
- **Stage 31**: complete.

### Stage 31 — Enterprise SQL (TASK-145 – 152)
- Enforced `GRANT` / `REVOKE` and row-level security.
- Cancellation, timeouts and limits.
- `pg_stat_*`, `EXPLAIN ANALYZE` and equivalents.
- A cost-based optimizer.
- More SQL surface: cursors, temporary tables, materialized views, functions, triggers, `LISTEN` / `NOTIFY`.
- BI and GUI tool suites.
- A soak test under faults.
- A final audit.

### Not planned, per the user
- Binary-size work.
- TASK-102 Windows verification.

## 5. Does it support transactions?

Yes:
- **Engine**: atomic multi-record, cross-collection transactions with MVCC snapshot isolation.
- **Document API**: atomic conditional batches with idempotent request ids; `transaction()` in every SDK.
- **SQL**: `BEGIN` / `COMMIT` / `ROLLBACK`, savepoints, REPEATABLE READ and SERIALIZABLE.
- **Across ranges** (Stage 30): distributed serializable transactions. Exposing them through SQL and the SDKs is TASK-143.

### Stage 31: tool suites (`TASK-150`) — 2026-11-04

- **Result**: real Metabase and Grafana, and the real drivers behind DBeaver, Superset, Tableau, Power BI and pgAdmin, all pass (0 JDBC failures, 177 tool checks with 0 failures). Which are real programs and which are stand-ins is in a table under `TASK-150`.
- **Why it found bugs other tests had not**: the first run had 48 failures, mostly catalog gaps, a few in my own tests.
- **Older correctness bug**: `HAVING` without `GROUP BY` or an aggregate was silently ignored. Any such query returned too many rows.
- **Protocol bug**: cursor rows carried the tag `SELECT n` instead of `FETCH n`, which broke psqlODBC's `UseDeclareFetch` without any server error. Found by reading the driver's source after the gateway log showed nothing.
- **Decision**: a test that expected one exported key on MySQL was wrong. The driver's own query returns two for a table with two unique keys. The test now says so, with the reason.
- **Next**: `TASK-151`, `TASK-152`.

### Stage 31: final audit (`TASK-152`) — 2026-11-05

- **How**: three independent read-only reviews (distributed core; SQL access control and execution; wire protocols and parsers). Each finding was read against the code before it was fixed. The fixes were then reviewed again, which found more.
- **Most serious findings**:
  - `GRANT` on a temporary table reached the real table of that name.
  - Kerberos took the realm from the unencrypted part of the ticket.
  - Range clusters could not authenticate at all.
  - Several inputs could abort the server: stack depth, an absurd timeout, thread exhaustion, and exponential parsing.
  - A partitioned leader answered some writes from stale state.
- **Rules**: `unsafe` is now only in `storage-codec` and `storage-mmap`.
- **Full list and known limits**: `TASK-152` in `TASKS.md`.

### Stage 31: soak under faults (`TASK-151`) — 2026-11-07

- **Result**: 2 hours per mode, with five kinds of fault, on Modal. Both modes pass with balances conserved. Range mode stays at most 123 descriptors with none in `CLOSE_WAIT`; single group stays at most 194 with no climb. Numbers are in `TASKS.md`.
- **Three server bugs the soak found**:
  - Connections were held after clients hung up: 913 descriptors in 2 hours.
  - Leaders cut off from a quorum never stepped down.
  - Reactors waited on a Raft lock that is held across `fsync`.
  
  A probe test now covers all three.
- **Harness bugs**: parallel chaos clusters could share ports and read each other's data, which looked like a linearizability failure until the reads showed the other cluster's values. Ports are now unique and kept below gVisor's ephemeral range.
- **Cost**: preemption restarted two runs from zero before `--steady` existed. The final run cost about $3; the month's Modal total for storine is about $27.
- **Not fully explained**: in single-group mode, 17–120 sockets sit in `CLOSE_WAIT` at each sample, bounded but not zero.

### Stage 31: soak follow-up (`TASK-151` reopened) — 2026-11-08

- **Fixed**:
  - Single-group nodes handled Raft RPCs on reactor threads, behind a lock held across `fsync`, which is where the `CLOSE_WAIT` sockets came from. They now run on a worker pool of their own. `CLOSE_WAIT` went to 0, and single-group throughput more than doubled.
  - The linearizability check stopped at the first undecided key, leaving later keys unchecked. Soak keys now rotate every 2 s, and an undecided key fails the run.
  - Every TCP test uses one port allocator.
- **Verified**: workspace tests (66 binaries), clippy, and 40 of 40 chaos runs on Modal.
- **Found, not fixed**: memory, read from `smaps`.
  - Range mode holds 2.6–6.9 GB of heap for about 60 MB of data, more with more distinct keys.
  - Single group spikes to 4.7 GB on a node that is probably catching up by snapshot.
  - The 2026-11-07 pass had passed under a 6 GiB ceiling with little margin.
- **Modal**: workspace, chaos and three soaks (one 2 h run that failed at 1 h, and two diagnostic runs). $2.98 on 2026-11-08 by the billing report at commit time (the last run may not be billed yet); about $30 for storine this month.
- **Memory, fixed later the same day**:
  - The range leader holds one bounded window in flight per follower.
  - Single-group nodes checkpoint every 10 s.
  - `malloc_trim` runs every 10 s.
  - Non-voters grant no votes (sim seed 262).
  - Range health reports its queues, and `heap-stats` reports the live heap.
- **Result of the 2-hour soak**: range mode passes at a peak RSS of 757–1,546 MiB. Single group stays bounded at 563–1,864 MiB but fails the final balance read after healing. That failure is open, and diagnostics were added.
- **Modal**: $8.94 on 2026-11-08 and $35.90 this month for storine, by the billing report at commit time.

### Merge into `main` — 2026-11-09

- `main` (`TASK-102` on Windows, plus Windows test fixes) was merged in. The only conflict was in `PROGRESS_LOG.md`, and both entries were kept.
- The workspace run after the merge exposed a rare columnar snapshot race, which dates from `TASK-126`. It is fixed, with a deterministic unit test (`TASKS.md`, `TASK-126`).
- Modal runs are now named, and their output is kept on the volume `storine-runs` (`--collect <name>`).

### Single-group soak rerun and arena compaction (`TASK-151`) — 2026-11-10

- **`soak19`** (2 h, single group): the final read, balances and linearizability all passed. The memory check failed (peak RSS 1,808 → 4,156 MiB): one node held 3.9 GB of arena segments for about 440,000 live records.
- **Fixed**: dead bytes are recounted on open, `StorageEngine::checkpoint` compacts automatically, and compaction deletes old segment files only after the checkpoint is saved. There is a regression test.
- **Verified (locally)**: clippy for the whole workspace with `-D warnings`; `storage-mmap` and `storage-core` (21 test binaries, 174 tests); `storage-server` (12 binaries, 22 tests; the 2 soaks are ignored). No Modal run.
- **`soak20`** (the rerun with the fix) passed: peak RSS 484–1,270 MiB per window, disk 121–378 MiB, balances conserved, and every key linearizable. `TASK-151` is done.
- **Modal**: soak20 cost $3.09 and soak19 cost $3.07 (2026-11-09 and 2026-11-10). Storine has spent $45.02 on Modal this month, by the billing report at commit time.
