# Work Log Since the Last `main` Fast-Forward

- **Branch**: `claude/intelligent-carson-x9hkkg`
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

## 3. In progress when work stopped

Commit `5389fd8` is marked **WIP** and is known to have failures:

- **What it adds** (TASK-138, routing):
  - Range servers publish their descriptors to the meta range, sent between nodes as `RaftMsg::App` messages and acknowledged back.
  - The meta range serves `MetaOp::Lookup` / `MetaOp::Publish`.
  - Range servers refuse frozen ranges (the client re-routes).
  - Timestamp caches start over after a split or merge.
  - In the transaction simulator, clients can route through their own range caches (`meta_routing`), with the allocator moving ranges underneath them.
  - New test: `bank_transfers_with_meta_routing_and_allocator`.
- **Known failures, unfixed**:
  1. `STORINE_SIM_SEED=12 cargo test --release -p storage-core --test txn_simulation bank_transfers_are_serializable_under_faults` fails its serializability check.
     - It passes at `cca0ed9`, and it passes with `self.publish(now_ns)` in `NodeServer::poll` disabled.
     - So the cause is in the new publication path, or the path exposes an older bug by changing message timing.
  2. `STORINE_SIM_SEED=10 cargo test --release -p storage-core --test txn_simulation bank_transfers_with_meta_routing_and_allocator` fails its final-state check: an account reads as absent.
- **Last fully green commit**: `cca0ed9`.
- **Debugging aid**: `STORINE_SIM_TRACE=1` prints replica lifecycle and allocator decisions from every node.

## 4. Left to do

### Finish TASK-138
1. Fix the two failing seeds above.
2. Sweep `txn_simulation` and `multi_range_simulation` at 300+ seeds.
3. Run the full `storage-core` suite and `cargo clippy --workspace --all-targets`.
4. Commit TASK-138 as done.

### Stage 30
- **TASK-141**: locality and zone configurations, constraints and lease preferences, zone / region survival goals, closed-timestamp follower reads, global and regional-by-row tables.
- **TASK-142**: multi-process range clusters behind the fault proxy (needs TASK-143's TCP runtime).
- **TASK-143**:
  - a TCP runtime for `NodeServer` (batches already carry HLC timestamps);
  - range-aware routing in all eight SDKs;
  - SQL transactions across ranges.
- **TASK-144**: consistent cluster-wide backups at one timestamp; resumable changefeeds.

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
