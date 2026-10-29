"""DuckDB side of the analytics benchmark (TASK-131): the same lineitem rows (Q1, Q6) and the
same ClickBench-shaped hits rows (h0 .. h15, numbered as in ClickBench).

    python3 duckdb_bench.py <dir> load               # dir/lineitem.csv -> dir/lineitem.duckdb
    python3 duckdb_bench.py <dir> load-hits          # dir/hits.csv -> the hits table
    python3 duckdb_bench.py <dir> <q1|q6|hN> <reps>  # timings and result rows
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


HITS_SCHEMA = """CREATE TABLE hits (WatchID BIGINT, UserID BIGINT, CounterID BIGINT, EventDate DATE,
 EventTime BIGINT, AdvEngineID BIGINT, ResolutionWidth BIGINT, RegionID BIGINT, SearchPhrase VARCHAR,
 SearchEngineID BIGINT, IsRefresh BIGINT, URL VARCHAR)"""

HITS = {
    "h0": "SELECT COUNT(*) FROM hits",
    "h1": "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0",
    "h2": "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits",
    "h3": "SELECT AVG(UserID) FROM hits",
    "h4": "SELECT COUNT(DISTINCT UserID) FROM hits",
    "h5": "SELECT COUNT(DISTINCT SearchPhrase) FROM hits",
    "h6": "SELECT MIN(EventDate), MAX(EventDate) FROM hits",
    "h7": "SELECT AdvEngineID, COUNT(*) FROM hits WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC, 1",
    "h8": "SELECT RegionID, COUNT(DISTINCT UserID) AS u FROM hits GROUP BY RegionID ORDER BY u DESC, 1 LIMIT 10",
    "h9": "SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth), COUNT(DISTINCT UserID) FROM hits GROUP BY RegionID ORDER BY c DESC, 1 LIMIT 10",
    "h12": "SELECT SearchPhrase, COUNT(*) AS c FROM hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC, 1 LIMIT 10",
    "h15": "SELECT UserID, COUNT(*) FROM hits GROUP BY UserID ORDER BY COUNT(*) DESC, 1 LIMIT 10",
}


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
    if what == "load-hits":
        t = time.time()
        con.execute(HITS_SCHEMA)
        con.execute(f"COPY hits FROM '{d}/hits.csv' (DELIMITER '|', QUOTE '', ESCAPE '')")
        con.execute("CHECKPOINT")
        print(f"duckdb loaded hits in {time.time() - t:.1f} s")
        return
    reps = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    if os.environ.get("DUCKDB_THREADS"):
        con.execute(f"SET threads = {int(os.environ['DUCKDB_THREADS'])}")
    q = {"q1": Q1, "q6": Q6, **HITS}[what]
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
