---
title: Analytics
description: Group and aggregate on the server with exact sums, export columns in pages, and keep a columnar copy of a collection for vectorized, parallel scans.
---

`aggregate` filters a collection, groups it, and computes aggregates on the server, as of one snapshot. Only the result rows travel back.

```python
import storine
from storine import avg, bucket, count, count_distinct, eq, field, ge

sales = db.collection("sales")
res = sales.aggregate(
    {
        "orders": count(),
        "revenue": storine.sum(field("price") * "qty"),
        "avg_qty": avg("qty"),
        "cheapest": storine.min("price"),
        "products": count_distinct("sku"),
    },
    ge("day", 20),                    # filter: any query filter, or {field: value}
    group_by=["region"],              # fields and/or buckets
    order_by="-revenue",              # output column names; "-" for descending
    limit=10,
)
for row in res:                       # dicts: group keys first, then aggregates, in order
    print(row["region"], row["revenue"])
print(res.matched, res.pruned_row_groups, res.spilled_bytes)
```

## Aggregates

| Aggregate | Result |
| :--- | :--- |
| `count()` | The number of rows |
| `count(x)` | The number of non-null values of `x` |
| `sum(x)` | The sum. Exact for integers and decimals |
| `avg(x)` | The mean. For integers and decimals, a decimal with 6 more fractional digits than the inputs |
| `min(x)`, `max(x)` | The smallest and largest, in the [sort order](queries.md#sorting) |
| `count_distinct(x)` | The exact number of distinct values |
| `approx_count_distinct(x)` | An estimate by HyperLogLog, with about 0.8 % standard error, in fixed memory |

`sum` and `avg` skip nulls and values that are not numbers. With no number at all, the result is null.

In Python, `storine.sum`, `storine.min` and `storine.max` are not exported by `import *`, so the built-ins keep their names.

### Expressions

An aggregate's argument is a field path, or an expression built from `field(path)`, `lit(value)`, numbers, `+`, `-`, `*` and unary `-`, nested up to 32 levels:

```python
storine.sum(field("price") * "qty")          # a string next to an expression is a field
storine.sum(2 * field("qty") - 1)
avg(field("net") + field("tax"))
```

Arithmetic follows the same rules as `add`: exact for integers and decimals, and overflow is an error, never a wrapped value.

## Grouping

`group_by` takes field paths, and numeric **buckets**:

```python
sales.aggregate({"n": count()}, group_by=[bucket("day", 7, offset=0)])     # weeks: 0, 7, 14, ...
sales.aggregate({"n": count()}, group_by=["region", "channel"])
```

`bucket(path, width, offset)` puts a value `v` in group `offset + floor((v - offset) / width) * width`.

Group keys use value equality: `1`, `1.0` and `1.00` are one group. Null and missing values form one group. Without `group_by`, the result is exactly one row.

Up to 64 group keys and 64 aggregates per query, and up to 1,000,000 result rows.

## Exact results

- Integer sums are exact, widening from 64 to 128 bits, then to decimals. A sum beyond 38 digits is refused rather than rounded.
- Decimal sums never round.
- Float sums are correctly rounded, so a result never depends on how many threads ran or how the data was laid out.

## Memory and concurrency

Group tables use at most 256 MiB per query by default, and spill to disk beyond that, up to 16 GiB. Set other limits per query with `memory_budget=` and `spill_budget=` (bytes; 0 keeps the server's default). `res.spilled_bytes` tells you a query spilled.

A server runs at most **8** analytic queries at once. Beyond that, a query gets `Overloaded`, which the clients retry with backoff.

## Exporting columns

`scan_columns` reads chosen fields of every matching document, in key order and in pages, without sending whole documents:

```python
for key, values in sales.scan_columns(["qty", "price"], where=eq("region", "eu"), page_size=5000):
    ...                                   # ColumnRow(key, {"qty": 3, "price": Decimal("9.99")})
```

Pages hold 1,000 rows by default and up to 100,000. A missing field reads as null, and a path naming a map returns the map. Each page is consistent on its own. The iterator's `.total` is the number of matches, once the first page is read.

## Columnar storage

Enabling columnar storage keeps a second copy of the collection as **column segments**, built in the background:

```python
sales.enable_columnar()
sales.disable_columnar()       # deletes the segments; analytics read the row store again
```

With it, `aggregate` and `scan_columns`:

- read only the columns they need;
- skip row groups whose zone maps rule them out (`pruned_row_groups` counts them);
- run vectorized with SIMD kernels, and in parallel.

Writes not yet merged into segments are read from the row store, so results are always current and the same with or without columnar storage. It costs extra disk and background work, so use it on collections you aggregate often.

`STORINE_THREADS` sets the server's analytics parallelism. `STORINE_SIMD=portable` turns off the CPU-specific kernels.

How fast it is against DuckDB is in section 6 of the [benchmark report](../../docs/BENCHMARK_REPORT.md).
