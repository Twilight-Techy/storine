# Release checklist

What has to pass before a version is tagged, and how each check is run. CI (`.github/workflows/ci.yml`) covers checks 1, 2 and 9 on every push, on x86_64 and arm64. The rest are run by hand on the release candidate's exact commit, and their results go in `docs/PROGRESS_LOG.md`.

| # | Check | How | Pass |
| :--- | :--- | :--- | :--- |
| 1 | Lints | `cargo clippy --workspace --all-targets -- -D warnings` | no warnings |
| 2 | Test suite | `cargo test --release --workspace --no-fail-fast`, on x86_64 and arm64 | every test passes |
| 3 | Process chaos | `cargo test --release -p storage-server --test process_chaos`, 40 times, on x86_64 and arm64 | 40 of 40 on each |
| 4 | Soak, single group | `STORINE_SOAK_SECS=7200 STORINE_SOAK_WINDOW_SECS=600 cargo test --release -p storage-server --test process_chaos soak_single_group -- --ignored --nocapture`, 8 vCPU and 16 GB | the test passes: balances conserved, every key linearizable, memory and descriptors under their ceilings and not climbing |
| 5 | Soak, range cluster | as 4, with `soak_range_cluster` | as 4 |
| 6 | Undefined behaviour in `unsafe` code | Miri: `MIRIFLAGS="-Zmiri-strict-provenance -Zmiri-disable-isolation" cargo +nightly miri test -p storage-codec --lib -- scratch checksum`. AddressSanitizer: `LSAN_OPTIONS=suppressions=tools/lsan.supp RUSTFLAGS=-Zsanitizer=address cargo +nightly test -Zbuild-std --target <host> --no-fail-fast -p storage-codec -p storage-mmap -p storage-wal --lib -- --skip freed_memory_can_be_released` | no report. The skipped test checks that glibc's `malloc_trim` returns memory, which it cannot under AddressSanitizer's allocator; the suppression covers a test that leaks a segment on purpose (it simulates a crash). |
| 7 | Benchmarks | `cargo run --release -p storage-benches --bin bench-runner`, and `benches/kv-compare` against LMDB and RocksDB (`docs/BENCHMARK_REPORT.md` section 8) | no unexplained regression against the last release |
| 8 | SDKs | each SDK's suite (commands in `tools/modal/sweep.py`, `SDK_SUITES`) against the release `storage-server` (`STORINE_SERVER`): conformance vectors, end to end on one node and a 3-node cluster, and TLS (`storines://`, certificates from `gen-ca` / `gen-cert`) | every suite passes |
| 9 | Binary size | `cargo build --release --workspace --bins` on Linux x86_64 (CI checks it on every push) | under the `RULE-MEM-03` ceilings: 4.5, 1.5, 6.5 and 1 MiB (`storine-console`) |
| 10 | Release notes | the version's entry in `CHANGELOG.md`: what changed, what it fixes, known limitations, and which formats and protocols changed (`docs/COMPATIBILITY.md`) | written, with numbers from checks 7, 4–5 and 9 |

`unsafe` is allowed only in `storage-codec` and `storage-mmap` (`docs/RULES.md`), which is why check 6 covers those crates. It also covers `storage-wal`, whose tests drive the codec's frame pool from many threads.

Costs on AWS on-demand (us-east-1, 2026-11): checks 2 and 3 on a `c7i.xlarge` (x86_64) or `c7g.xlarge` (arm64) take about 1.5 h, about $0.30. Each soak on a `c7i.2xlarge` takes about 2.5 h with the build, about $0.90. Launch instances with `--instance-initiated-shutdown-behavior terminate` and a `shutdown -h +N` backstop, and terminate them when the job reports.
