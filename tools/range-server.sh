#!/bin/sh
# Runs storage-server in range mode with the arguments the SDK end-to-end suites pass
# (`data_dir addr [--node-id N --peers ...]`), so every SDK suite can run against a range
# cluster unchanged: STORINE_SERVER=tools/range-server.sh <suite>.
# A single node (no --peers) becomes a one-node range cluster.
here=$(cd "$(dirname "$0")/.." && pwd)
bin="${STORINE_RANGE_BIN:-$here/target/release/storage-server}"
case " $* " in
  *" --peers "*) exec "$bin" "$@" --ranges ;;
  *) exec "$bin" "$@" --ranges --node-id 1 --peers "1=$2" ;;
esac
