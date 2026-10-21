# Contributing to Storine

Thank you for your interest in contributing to **Storine**! Storine is built with strict performance, memory footprint, and safety invariants. Please review the guidelines below before opening an issue or pull request.

---

## 1. Core Engineering Invariants

To keep Storine sub-microsecond, zero-copy, and crash-safe, every contribution must adhere to the following rules (detailed further in [`docs/RULES.md`](docs/RULES.md)):

1. **Zero External Runtime Crates**:
   - All workspace crates compile using only the Rust standard library (`std`, `core`, `alloc`) and OS-native system calls (`Win32` on Windows, `libc`/POSIX syscalls on Unix). Do not add external crates to `Cargo.toml` dependencies without prior discussion.
2. **Strict `unsafe` Isolation (`RULE-SAFE-01`)**:
   - `#![forbid(unsafe_code)]` is enforced on all high-level crates (`storage-types`, `storage-wal`, `storage-index`, `storage-graph`, `storage-core`, `storage-server`, `storage-cli`).
   - `unsafe` blocks are permitted **only** inside `storage-codec` (zero-copy pointer casts and hardware SSE4.2 intrinsics) and `storage-mmap` (OS `mmap` / `CreateFileMappingW` FFI), and every `unsafe` block **must** have an explicit `// SAFETY:` justification comment (`#![deny(clippy::undocumented_unsafe_blocks)]`).
   - Archived record types implement `storage_codec::ArchivePod` only via `storage_codec::impl_archive_pod!(ArchivedFoo { field: Type, ... })`, which proves the no-padding / all-fields-pod contract at compile time. Never hand-write `unsafe impl ArchivePod` outside `storage-codec`.
   - Safe accessors over archived bytes must stay sound on hostile input: bounds-, alignment- and UTF-8-check, return empty/zeroed values, never panic.
3. **Zero Heap Allocations on Hot Read Paths (`RULE-MEM-01`)**:
   - `StorageEngine::with_ref` and `StorageEngine::get_ref` must execute with **`0.00` heap allocations per operation** when reading clean memory-mapped records.
   - This is automatically verified by the `TrackingAllocator` gate in `benches/src/main.rs`.
4. **No Panics or Poisoned Lock Crashes in Production Code**:
   - Never call `.unwrap()` or `.expect()` on fallible I/O, network sockets, user inputs, or untrusted bytes. Always propagate a descriptive `StorageError`.
   - Always recover from poisoned `Mutex` / `RwLock` guards via `.unwrap_or_else(|poisoned| poisoned.into_inner())`.
5. **Build-Independent On-Disk Formats (`RULE-SAFE-05`)**:
   - Checksums and encodings must produce identical bytes on every build and platform; a hardware fast path must be bit-identical to its portable fallback and covered by a golden-value test.
6. **64-Byte Cache-Line Alignment**:
   - All on-disk and in-memory record frames must remain 64-byte aligned (`RecordHeader` is exactly 64 bytes), and shared concurrent shard tables (`PrimaryIndex`, `StripedLockTable`) must remain cache-line padded (`#[repr(align(64))]`) to prevent false sharing.

---

## 2. Development Workflow

### Prerequisites
- Rust stable toolchain (`1.82+`, `2021` edition; enforced by `rust-version` in `Cargo.toml`)

### Building the Workspace
```bash
cargo build --workspace
```

### Running the Full Test Suite
Run all unit, integration, concurrency, corruption, and chaos crash-recovery tests (including a real process-kill test) across all crates:
```bash
cargo test --workspace
```

### Running the Zero-Allocation & Latency Benchmark Suite
Verify that hot-read zero-allocation gates (`0.00 Allocs/Op`) and `p50`/`p99`/`p99.9` latency targets pass:
```bash
cargo run --release -p storage-benches
```
On a shared or busy machine, compare against the baseline by interleaving runs (baseline, candidate, baseline, candidate) rather than trusting a single run.

### Running Lints
Ensure there are zero compiler or Clippy warnings:
```bash
cargo clippy --workspace --all-targets
```

---

## 3. Submitting a Pull Request

1. **Fork & Branch**: Create a descriptive feature or bugfix branch from `main` (e.g., `feat/arena-prefetch` or `fix/wal-truncation`).
2. **Add Tests**:
   - Any bug fix or new capability should include a unit or integration test.
   - Changes touching on-disk formats (`RecordHeader`, `STGEIDX2`, or `WAL1`) must include corruption/bit-flip detection tests.
3. **Verify Benchmarks**: If your change touches `storage-codec`, `storage-mmap`, `storage-index`, `storage-core`, or `storage-server`, run `cargo run --release -p storage-benches` and include the before/after throughput and `p50`/`p99` numbers in your PR description.
4. **Commit Messages**: Use clear, descriptive commit messages following Conventional Commits (`feat: ...`, `fix: ...`, `perf: ...`, `docs: ...`, `test: ...`).

---

## 4. License Agreement

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in Storine by you shall be dual-licensed under the **MIT License** ([`LICENSE`](LICENSE)) and **Apache License, Version 2.0** ([`LICENSE-APACHE`](LICENSE-APACHE)), without any additional terms or conditions.
