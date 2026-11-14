#!/usr/bin/env bash
# Compares two source trees (e.g. the last release and the candidate) on the same machine:
# the engine benchmarks (`bench-runner`, `kv-compare` Storine only) and the server under load
# (`server-load`, from the candidate tree, against each tree's `storage-server`: standalone,
# and a 3-node single-group cluster on loopback). Rounds alternate between the trees so drift
# and noise fall on both alike.
#
#   tools/release/compare-versions.sh <out-dir> <rounds> name=<tree> name=<tree>
#
# Results: <out-dir>/<name>.<round>.{bench,kv,srv,raft}; summarize with summarize-cpu-targets.py.
set -euo pipefail
trap 'echo "FAILED at line $LINENO: $BASH_COMMAND (exit $?)"' ERR
out=$1 rounds=$2
shift 2
here=$(cd "$(dirname "$0")/../.." && pwd)
mkdir -p "$out"
names=()
for spec in "$@"; do
  name=${spec%%=*} tree=${spec#*=}
  names+=("$name")
  echo "== build $name ($tree)"
  (cd "$tree" && CARGO_TARGET_DIR="$out/target-$name" cargo build -q --release -p storage-benches --bin bench-runner -p storage-server --bin storage-server)
  (cd "$tree/benches/kv-compare" && CARGO_TARGET_DIR="$out/target-kv-$name" cargo build -q --release --no-default-features)
done
(cd "$here" && CARGO_TARGET_DIR="$out/target-load" cargo build -q --release -p storage-benches --bin server-load)
load="$out/target-load/release/server-load"
port=21000
for r in $(seq 1 "$rounds"); do
  for name in "${names[@]}"; do
    echo "== round $r $name $(date +%T)"
    bin="$out/target-$name/release"
    "$bin/bench-runner" > "$out/$name.$r.bench" 2>&1
    KV_DURABLE_OPS=500 "$out/target-kv-$name/release/kv-compare" "$out/kvdata" 500000 4 storine,indexed > "$out/$name.$r.kv" 2>&1
    rm -rf "$out/kvdata"
    # Standalone server.
    port=$((port + 10))
    rm -rf "$out/srvdata"
    "$bin/storage-server" "$out/srvdata" "127.0.0.1:$port" > /dev/null 2>&1 &
    pid=$!
    sleep 1
    "$load" "127.0.0.1:$port" 4 20 > "$out/$name.$r.srv" 2>&1
    kill $pid; wait $pid 2> /dev/null || true
    # Three-node cluster.
    peers="1=127.0.0.1:$((port + 1)),2=127.0.0.1:$((port + 2)),3=127.0.0.1:$((port + 3))"
    pids=()
    for i in 1 2 3; do
      rm -rf "$out/raft$i"
      "$bin/storage-server" "$out/raft$i" "127.0.0.1:$((port + i))" --node-id $i --peers "$peers" > /dev/null 2>&1 &
      pids+=($!)
    done
    sleep 3
    "$load" "127.0.0.1:$((port + 1)),127.0.0.1:$((port + 2)),127.0.0.1:$((port + 3))" 4 20 > "$out/$name.$r.raft" 2>&1
    kill "${pids[@]}"; wait 2> /dev/null || true
  done
done
echo DONE
