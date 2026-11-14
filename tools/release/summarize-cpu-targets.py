#!/usr/bin/env python3
"""Summarizes compare-cpu-targets.sh and compare-versions.sh output: per benchmark, each target's median over rounds,
and its change against the first target named.

    tools/release/summarize-cpu-targets.py <out-dir> native v3 v2 x86-64
"""
import re
import statistics
import sys
from pathlib import Path

out, names = Path(sys.argv[1]), sys.argv[2:]
BENCH = re.compile(r"^(.+?)\s*\|\s*(\d+)/s\s*\|\s*(\d+)\s*\|\s*(\d+)\s*\|")
SRV = re.compile(r"^server-load (\w+)\s+(\d+) ops/s\s+p50\s+(\d+) ns\s+p99\s+(\d+) ns")
KV = re.compile(r"^(\S.{21}) (storine \S+)\s+(\d+) ops/s\s+p50\s+(\d+) ns\s+p99\s+(\d+) ns")

# rows[benchmark][target] = [(throughput, p50, p99), ...] over rounds
rows: dict[str, dict[str, list]] = {}
for name in names:
    for f in sorted(out.glob(f"{name}.*.bench")):
        for line in f.read_text().splitlines():
            if m := BENCH.match(line):
                rows.setdefault(m[1].strip(), {}).setdefault(name, []).append(tuple(map(int, m.groups()[1:4])))
    for kind, label in (("srv", "server standalone"), ("raft", "server 3-node cluster")):
        for f in sorted(out.glob(f"{name}.*.{kind}")):
            for line in f.read_text().splitlines():
                if m := SRV.match(line):
                    rows.setdefault(f"{label}: {m[1]}", {}).setdefault(name, []).append(tuple(map(int, m.groups()[1:4])))
    for f in sorted(out.glob(f"{name}.*.kv")):
        for line in f.read_text().splitlines():
            if m := KV.match(line):
                key = f"kv {m[2]}: {m[1].strip()}"
                rows.setdefault(key, {}).setdefault(name, []).append(tuple(map(int, m.groups()[2:5])))

base = names[0]
print(f"{'benchmark':<58}" + "".join(f"{n:>26}" for n in names))
for bench, by in rows.items():
    if base not in by:
        continue
    b_thr = statistics.median(r[0] for r in by[base])
    b_p50 = statistics.median(r[1] for r in by[base])
    cells = []
    for n in names:
        if n not in by:
            cells.append(f"{'-':>26}")
            continue
        thr = statistics.median(r[0] for r in by[n])
        p50 = statistics.median(r[1] for r in by[n])
        if n == base:
            cells.append(f"{thr:>14,.0f}/s {p50:>7.0f}ns")
        else:
            d = (thr / b_thr - 1) * 100 if b_thr else 0
            cells.append(f"{d:>+13.1f}% {p50:>8.0f}ns")
    print(f"{bench[:58]:<58}" + "".join(f"{c:>26}" for c in cells))
print(f"\nmedians over {min(len(v) for by in rows.values() for v in by.values())}+ rounds; % = throughput against {base}")
