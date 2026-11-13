#!/usr/bin/env bash
# Measures what a release build's CPU target costs against a native build: builds
# `bench-runner` and `kv-compare` (Storine only) once per target, then runs them in
# alternating rounds so drift and noise fall on every target alike.
#
#   tools/release/compare-cpu-targets.sh <out-dir> <rounds> name=RUSTFLAGS [name=RUSTFLAGS ...]
#   tools/release/compare-cpu-targets.sh ~/cmp 5 native="-C target-cpu=native" v3="-C target-cpu=x86-64-v3"
#
# Results: <out-dir>/<name>.<round>.bench and .kv; summarize with summarize-cpu-targets.py.
set -euo pipefail
trap 'echo "FAILED at line $LINENO: $BASH_COMMAND (exit $?)"' ERR
out=$1 rounds=$2
shift 2
repo=$(cd "$(dirname "$0")/../.." && pwd)
mkdir -p "$out"
names=()
for spec in "$@"; do
  name=${spec%%=*} flags=${spec#*=}
  names+=("$name")
  echo "== build $name ($flags)"
  (cd "$repo" && RUSTFLAGS="$flags" CARGO_TARGET_DIR="$out/target-$name" cargo build -q --release -p storage-benches --bin bench-runner)
  (cd "$repo/benches/kv-compare" && RUSTFLAGS="$flags" CARGO_TARGET_DIR="$out/target-kv-$name" cargo build -q --release --no-default-features)
done
for r in $(seq 1 "$rounds"); do
  for name in "${names[@]}"; do
    echo "== round $r $name $(date +%T)"
    "$out/target-$name/release/bench-runner" > "$out/$name.$r.bench" 2>&1
    KV_DURABLE_OPS=500 "$out/target-kv-$name/release/kv-compare" "$out/kvdata" "${KV_RECORDS:-500000}" "${KV_THREADS:-4}" storine,indexed > "$out/$name.$r.kv" 2>&1
    rm -rf "$out/kvdata"
  done
done
echo DONE
