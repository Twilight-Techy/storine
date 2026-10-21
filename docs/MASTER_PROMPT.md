# Master Autonomous Execution Prompt (`MASTER_PROMPT.md`)

## Exact User Prompt (Recorded Verbatim)

> Alright! Go ahead and build the whole thing end to end. You can create a file to record and log progress and mark each task as done when you're done with it. Refer to the 6 docs (including this prompt and the log file) before you commence each task and review your work at the end of each task to find gaps, issues, bugs and security vulnerabilities, then fix them before moving on to the next task. Whatever needs my decision, decide yourself based on the data that's available to you and your own initiative. When you're done with the whole project, review the entire project to find gaps, issues, bugs and security vulnerabilities and fix them. Keep doing that until you're satisified and are confident with the outcome. You might want to record this prompt exactly as it is somewhere so you can keep refering to it as you build. I want you to build the entire project fully autonomously so do not stop until you're completely done with the project.

---

## Follow-Up Master Prompt (Recorded Verbatim)

> Alright! Please write the end to end implementation plan, features and tasks order doc for all those features from commencement to completion. Update the rules doc too if necessary. Note that we still want to squeeze out every last possible optimization from the codebase to keep speed and performance at the absolute highest and memory, cpu use and storage footprint (for both code and data) at the absolute lowest without sacrificing any performance and features. Also optimize time and space complexity as much as absolutely possible as well as scaling and concurrency. By scaling, I mean both users and compute (vertical and horizontal). Handle every single possible error gracefully. Also, avoid memory bloat and CPU hogging. The master prompt still holds while building. Add this as a follow up prompt to the master prompt exactly as it is and update the Mandatory Per-Task Protocol with the new things I've added as necessary.

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
