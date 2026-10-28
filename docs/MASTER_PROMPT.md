# Master Autonomous Execution Prompt (`MASTER_PROMPT.md`)

## Exact User Prompt (Recorded Verbatim)

> Alright! Go ahead and build the whole thing end to end. You can create a file to record and log progress and mark each task as done when you're done with it. Refer to the 6 docs (including this prompt and the log file) before you commence each task and review your work at the end of each task to find gaps, issues, bugs and security vulnerabilities, then fix them before moving on to the next task. Whatever needs my decision, decide yourself based on the data that's available to you and your own initiative. When you're done with the whole project, review the entire project to find gaps, issues, bugs and security vulnerabilities and fix them. Keep doing that until you're satisified and are confident with the outcome. You might want to record this prompt exactly as it is somewhere so you can keep refering to it as you build. I want you to build the entire project fully autonomously so do not stop until you're completely done with the project.

---

## Follow-Up Master Prompt (Recorded Verbatim)

> Alright! Please write the end to end implementation plan, features and tasks order doc for all those features from commencement to completion. Update the rules doc too if necessary. Note that we still want to squeeze out every last possible optimization from the codebase to keep speed and performance at the absolute highest and memory, cpu use and storage footprint (for both code and data) at the absolute lowest without sacrificing any performance and features. Also optimize time and space complexity as much as absolutely possible as well as scaling and concurrency. By scaling, I mean both users and compute (vertical and horizontal). Handle every single possible error gracefully. Also, avoid memory bloat and CPU hogging. The master prompt still holds while building. Add this as a follow up prompt to the master prompt exactly as it is and update the Mandatory Per-Task Protocol with the new things I've added as necessary.

---

## Follow-Up Prompt 2: Close Gaps, SDKs & SQL Adapter (Recorded Verbatim)

> Alright! Go ahead and close all the gaps and implement everything we raised from your last 2 answers according to our rules and master prompt files in docs. Make sure to read them whole first. Take the best possible decisions where they arise and do not stop to ask me. Work until you're completely done. Then build sdks for popular backend languages to work with it accoriding to our master prompt and rules with simplicity and user intuitiveness/clarity included as rules. (I'm not sure if those should go in a new repo or the same one) Then build the SQL adapter separately (I'm not sure if that should go in a new repo or the same one) so that existing projects with SQL and/or ORMs can work with it with zero changes to their codebase or at least almost zero. It should support the different popular SQL formats. Answer the questions about repos first and stop for my response before you start working.

## Follow-Up Prompt 3: Go-Ahead (Recorded Verbatim)

> Yeah! Those are fine. You can add PHP and Ruby as to do. Do not stop until you're completely done with all the work. Record my last 2 prompts (this one included) as follow ups in the master prompt exactly as is so you  can reference them and do not lose them as context. This session resets at 8:20 a.m. so you need to wake yourself to continue working shortly after the reset in case you hit the limit before then and so on every 5 hours till the end of today. The goal is to maximize credits and minimize wasted time.
>
> Alright! Let's go.

### Agreed Decisions (from the answer to Follow-Up Prompt 2, approved in Follow-Up Prompt 3)

- **Repository layout**: everything stays in this repository.
  - SDKs live in `sdks/<language>/` (outside the Cargo workspace), all generated against one wire-protocol spec and shared conformance vectors in `protocol/`.
  - SDK languages: Python, TypeScript/Node, Go, Java, C#, Rust (`sdks/rust` / `storage-client`). PHP and Ruby are recorded as TODO.
  - The SQL adapter is a separate crate, `crates/storage-sql`, with its own binary (`storine-sql`), speaking the PostgreSQL v3 and MySQL wire protocols (then SQL Server TDS) so existing drivers and ORMs work by changing only the connection string. SQLite is "near-zero change" (no network protocol to impersonate).
- **Work order**: (1) close every engine gap raised in the two audit answers (lost updates over TCP, `put` blind overwrites, version ABA after delete/re-insert, `BATCH_PUT` without OCC, money-safe decimal type and checked arithmetic, `i128` indexing, Raft integrity gaps, read-only replicas, Windows IOCP-class readiness); (2) protocol spec + conformance vectors; (3) SDKs; (4) SQL adapter. Commit and push after each stage.

---

## The 6 Canonical Project Documents

Before commencing each task (`TASK-001` through `TASK-050`), consult and align with all 6 documents:

1. [`docs/IMPLEMENTATION_PLAN.md`](./IMPLEMENTATION_PLAN.md) — End-to-End Architecture & Phased Implementation Plan (Stages 0–15)
2. [`docs/FEATURES.md`](./FEATURES.md) — Complete Features Specification (`F-CORE-*`, `F-CONC-*`, `F-DURA-*`, `F-INDX-*`, `F-REL-*`, `F-OPS-*`, `F-CAT-*`, `F-PIDX-*`, `F-HNSW-*`, `F-NET-*`, `F-SEC-*`, `F-MVCC-*`, `F-PITR-*`, `F-DIST-*`, `F-OPT-*`)
3. [`docs/RULES.md`](./RULES.md) — Engineering, Memory, CPU, Complexity, Scaling, Error-Handling, Concurrency & `unsafe` Safety Rules (`RULE-MEM-*`, `RULE-CPU-*`, `RULE-CPLX-*`, `RULE-SCALE-*`, `RULE-ERR-*`, `RULE-SAFE-*`, `RULE-CONC-*`, `RULE-TEST-*`)
4. [`docs/TASKS.md`](./TASKS.md) — Ordered Tasks Checklist (`TASK-001` to `TASK-050`)
5. [`docs/MASTER_PROMPT.md`](./MASTER_PROMPT.md) — This verbatim autonomous mandate and workflow protocol
6. [`docs/PROGRESS_LOG.md`](./PROGRESS_LOG.md) — Live execution, audit, vulnerability review, and verification log

---

## Mandatory Per-Task Protocol

1. **Pre-Task Review**:
   - Consult all 6 canonical documents (`IMPLEMENTATION_PLAN.md`, `FEATURES.md`, `RULES.md`, `TASKS.md`, `MASTER_PROMPT.md`, `PROGRESS_LOG.md`) before writing any code.
   - Verify target feature IDs, dependencies, asymptotic time/space complexity bounds, and memory/CPU/storage budgets.
2. **Extreme-Optimization Implementation**:
   - Build the complete, production-grade implementation for the task (never placeholders, TODOs, or stubs).
   - **Maximum Speed & Performance**: Squeeze out every last nanosecond via zero-copy `mmap` access, cache-line alignment (`64B`), branchless/inlined hot paths, and SIMD kernels (`XXH3`, `CRC32C`, quantized vector distance).
   - **Minimum Footprint (Memory, CPU, Code & On-Disk Data)**: Eliminate duplicate in-RAM structures, shrink struct padding and on-disk encodings (varint, bit-packing, scalar quantization), reclaim transient buffers immediately to prevent memory bloat, and keep compiled binary size minimal (zero external runtime crates).
   - **Optimal Time & Space Complexity**: Enforce the best theoretical and practical asymptotic bounds ($O(1)$ lookups/ordinals, $O(\log N)$ paged B+Tree/HNSW/MVCC operations, $O(N + K \log K)$ top-$K$ selection, $O(\text{active})$ kernel readiness polling).
   - **Massive Scaling & Concurrency (Users, Vertical Compute & Horizontal Compute)**: Ensure lock-free/fine-grained sharded concurrency across all CPU cores (vertical scaling), sharded non-blocking connection multiplexing with admission control for millions of simultaneous users, and clean Raft/partition boundaries across cluster nodes (horizontal scaling).
   - **Zero CPU Hogging & Zero Memory Bloat**: Never use unbounded busy-wait spin loops; use OS kernel readiness notifications (`IOCP`/`epoll`/`kqueue`) or `Condvar` parking on idle paths, and enforce hard memory caps (`max_hot_dirty_bytes`, `PoolBudget`, `try_reserve_exact`).
   - **Total Graceful Error Handling**: Handle every possible error condition (corrupted frames/headers, bit-flips, disk full `ENOSPC`, socket exhaustion `EMFILE`, OOM via `try_reserve_exact`, lock poisoning via `into_inner()`, CAS conflicts, wrong collection/schema/parameters, and server overload `STATUS_OVERLOADED`) without panicking or losing state.
3. **Post-Task Audit**:
   - Audit all new and modified code for functional gaps, edge-case bugs, race conditions, deadlocks, memory bloat, CPU spin hazards, and security vulnerabilities (path traversal, integer overflow, unaligned pointer casts, torn reads/writes, unauthorized access, and DoS vectors).
   - Fix every single finding immediately before advancing.
4. **Verification & Logging**:
   - Run `cargo test` and `cargo clippy` (plus `cargo run --release -p storage-benches` when touching hot paths), mark the task `[x]` in `docs/TASKS.md`, and record the complete implementation, complexity, footprint, and security audit report in `docs/PROGRESS_LOG.md`.
