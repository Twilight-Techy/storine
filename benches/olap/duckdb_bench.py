"""DuckDB side of the analytics benchmark (TASK-131): same lineitem rows, Q1 and Q6.

    python3 duckdb_bench.py <dir> load            # dir/lineitem.csv -> dir/lineitem.duckdb
    python3 duckdb_bench.py <dir> <q1|q6> <reps>  # timings and result rows
"""
import os
import sys
import time

import duckdb

SCHEMA = """CREATE TABLE lineitem (l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, l_linenumber BIGINT,
 l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), l_tax DECIMAL(15,2),
 l_returnflag VARCHAR, l_linestatus VARCHAR, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE,
 l_shipinstruct VARCHAR, l_shipmode VARCHAR, l_comment VARCHAR)"""

Q1 = """SELECT l_returnflag, l_linestatus, sum(l_quantity), sum(l_extendedprice),
 sum(l_extendedprice * (1 - l_discount)), sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
 avg(l_quantity), avg(l_extendedprice), avg(l_discount), count(*)
 FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' GROUP BY 1, 2 ORDER BY 1, 2"""

Q6 = """SELECT sum(l_extendedprice * l_discount) FROM lineitem
 WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'
 AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"""


def main():
    d, what = sys.argv[1], sys.argv[2]
    con = duckdb.connect(f"{d}/lineitem.duckdb")
    if what == "load":
        t = time.time()
        con.execute(SCHEMA)
        con.execute(f"COPY lineitem FROM '{d}/lineitem.csv' (DELIMITER '|')")
        con.execute("CHECKPOINT")
        print(f"duckdb loaded in {time.time() - t:.1f} s")
        return
    reps = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    if os.environ.get("DUCKDB_THREADS"):
        con.execute(f"SET threads = {int(os.environ['DUCKDB_THREADS'])}")
    q = {"q1": Q1, "q6": Q6}[what]
    times, rows = [], None
    for _ in range(reps):
        t = time.perf_counter()
        rows = con.execute(q).fetchall()
        times.append((time.perf_counter() - t) * 1000)
    times.sort()
    print(f"duckdb {what}: min {times[0]:.1f} ms, median {times[len(times) // 2]:.1f} ms ({reps} runs, {con.execute('SELECT current_setting(\'threads\')').fetchone()[0]} threads)")
    for r in rows:
        print("  " + " | ".join(str(x) for x in r))


if __name__ == "__main__":
    main()
