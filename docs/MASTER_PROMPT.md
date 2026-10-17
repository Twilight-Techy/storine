# Master Autonomous Execution Prompt (`MASTER_PROMPT.md`)

## Exact User Prompt (Recorded Verbatim)

> Alright! Go ahead and build the whole thing end to end. You can create a file to record and log progress and mark each task as done when you're done with it. Refer to the 6 docs (including this prompt and the log file) before you commence each task and review your work at the end of each task to find gaps, issues, bugs and security vulnerabilities, then fix them before moving on to the next task. Whatever needs my decision, decide yourself based on the data that's available to you and your own initiative. When you're done with the whole project, review the entire project to find gaps, issues, bugs and security vulnerabilities and fix them. Keep doing that until you're satisified and are confident with the outcome. You might want to record this prompt exactly as it is somewhere so you can keep refering to it as you build. I want you to build the entire project fully autonomously so do not stop until you're completely done with the project.

---

## The 6 Canonical Project Documents

Before commencing each task (`TASK-001` through `TASK-030`), consult and align with all 6 documents:

1. [`docs/IMPLEMENTATION_PLAN.md`](./IMPLEMENTATION_PLAN.md) — End-to-End Architecture & Phased Implementation Plan
2. [`docs/FEATURES.md`](./FEATURES.md) — Complete Features Specification (`F-CORE-*`, `F-CONC-*`, `F-DURA-*`, `F-INDX-*`, `F-REL-*`, `F-OPS-*`)
3. [`docs/RULES.md`](./RULES.md) — Engineering, Memory, CPU, Concurrency & `unsafe` Safety Rules (`RULE-MEM-*`, `RULE-CPU-*`, `RULE-SAFE-*`, `RULE-CONC-*`, `RULE-TEST-*`)
4. [`docs/TASKS.md`](./TASKS.md) — Ordered Tasks Checklist (`TASK-001` to `TASK-030`)
5. [`docs/MASTER_PROMPT.md`](./MASTER_PROMPT.md) — This verbatim autonomous mandate and workflow protocol
6. [`docs/PROGRESS_LOG.md`](./PROGRESS_LOG.md) — Live execution, audit, vulnerability review, and verification log

## Mandatory Per-Task Protocol
1. **Pre-Task Review**: Check the 6 docs to confirm requirements, feature IDs, optimization rules, and dependencies.
2. **Implementation**: Build the complete, production-grade implementation for the task (never placeholders or stubs).
3. **Post-Task Audit**: Review the code for gaps, edge-case bugs, concurrency hazards, memory/resource leaks, and security vulnerabilities (e.g. path traversal, integer overflow, unaligned access, torn reads/writes, DoS vectors), and fix every finding immediately.
4. **Verification & Logging**: Run `cargo test` / `cargo clippy`, mark the task `[x]` in `docs/TASKS.md`, and record the full audit and verification report in `docs/PROGRESS_LOG.md`.
