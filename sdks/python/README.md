# Storine for Python

Standard library only, Python 3.8+. The clients are installed from the [repository](https://github.com/Twilight-Techy/storine); they are not on package registries yet.

```bash
pip install "git+https://github.com/Twilight-Techy/storine#subdirectory=sdks/python"
```

```python
from decimal import Decimal
import storine
from storine import add, eq, gt

db = storine.connect("storine://127.0.0.1:7878")          # or "storine://token@h1:7878,h2:7878,h3:7878"
accounts = db.collection("accounts")

accounts.insert("alice", {"owner": "Alice", "balance": Decimal("100.00")})
accounts.insert("bob", {"owner": "Bob", "balance": Decimal("0.00")})

# Atomic, exact, server-side: no lost updates, no overdraft.
accounts.update("alice", add("balance", Decimal("-25.00"), min=0))

# Several writes, all or nothing.
with db.transaction() as tx:
    tx.update("accounts", "alice", add("balance", Decimal("-10.00"), min=0))
    tx.update("accounts", "bob", add("balance", Decimal("10.00")))

doc = accounts.get("alice")             # Document: a dict with .key and .version
print(doc["balance"], doc.version)      # 65.00 ...

accounts.create_index("owner")
rich = accounts.find(gt("balance", Decimal("50")), order_by="-balance", limit=10)
print(accounts.count(eq("owner", "Bob")))
for d in accounts.scan():               # every document, any collection size
    ...
```

## API

| Call | Does |
| --- | --- |
| `storine.connect(url, token=None, timeout=5.0, follower_reads=False)` | Connection pool to a server or cluster (thread-safe). |
| `db.collection(name)` / `db[name]` | A collection handle. |
| `c.get(key, fields=None)` | `Document` or `None`. `fields=["name", "meta.size"]` returns only those (dotted) paths; nested paths come back nested. |
| `c.read_bytes(key, field, offset=0, length=16 MiB)` / `c.stream_bytes(key, field, chunk_size=1 << 20)` | Part of a bytes / text field (see *Large values*). |
| `c.put(key, fields, ttl=None)` | Create or replace; returns the version. `ttl` in seconds. |
| `c.insert(key, fields, ttl=None)` | Create only (`ConflictError` if it exists). |
| `c.replace(key, fields, if_version=None)` | Replace an existing document (optionally only at a version). |
| `c.update(key, *changes, if_version=None)` | Atomic server-side changes: `set_`, `unset`, `add(path, amount, min=, max=)`, `append`, or a `{field: value}` dict. |
| `c.delete(key, if_version=None)` | `True` if it existed. |
| `c.find(where, order_by=, limit=, offset=)` / `c.first(...)` / `c.count(where)` / `c.scan(where)` | Queries. `where` is a filter (`eq`, `ne`, `lt`, `le`, `gt`, `ge`, `between`, `in_`, `prefix`, `contains`, `exists`, `missing`, `key_prefix`, combined with `&`, `|`, `~`) or `{field: value}`. `order_by="-price"` sorts descending. |
| `c.nearest(field, vector, k=10, where=None, exact=False)` | The `k` documents whose `field` (a list of numbers) is most similar to `vector` (cosine), best first; `doc.score` holds the similarity. `exact=True` scores every candidate instead of using the vector index. |
| `c.create_vector_index(field, dims)` / `c.drop_vector_index(field)` / `c.vector_indexes()` | Vector indexes for `nearest` (at most 4 per collection); writes of other values to an indexed field are refused. |
| `c.reserve(count)` | Room for about `count` more documents before a bulk import, so writes don't pause while the index grows (at most 100,000,000 per call; range clusters size every node for its share). Needs server 0.3. |
| `c.create_index(field)` / `c.drop_index(field)` / `c.info()` / `c.drop()` | Indexes (up to 16 per collection) and collection management. |
| `c.aggregate(aggregates, where, group_by=, order_by=, limit=, memory_budget=0, spill_budget=0)` / `c.scan_columns(fields, where, page_size=1000)` / `c.enable_columnar()` / `c.disable_columnar()` | Analytics (see below). |
| `db.transaction()` | Buffered writes committed atomically (`with` block or `.commit()`); `tx.require(...)` adds a version check. |
| `db.collections()` / `db.cluster_info()` / `db.close()` | Every collection's name; the node id, leader, term and commit indexes of the server in use; close the pool. |

**Large values**: a document can hold a large binary field (a file) next to small metadata. Read the metadata alone
with `get(key, fields=[...])`, a slice with `read_bytes` (a `ByteRange` with `version`, `total` and `data`, or `None`
if the document does not exist; at most 16 MiB per call, empty `data` past the end), or the whole value chunk by
chunk with `stream_bytes`, which raises `NotFoundError` if the document does not exist and `ConflictError` if it
changes mid-read:

```python
meta = files.get("f1", fields=["name", "meta.size"])
head = files.read_bytes("f1", "data", 0, 4096).data
with open("cat.png", "wb") as f:
    for chunk in files.stream_bytes("f1", "data"):
        f.write(chunk)
```

## Analytics

Server-side grouping and aggregation, exact (integer and `Decimal` sums never round) and as of one snapshot.
`enable_columnar()` also keeps the collection as column segments (built in the background) for vectorized,
parallel execution; results are the same either way.

```python
import storine
from storine import avg, bucket, count, count_distinct, eq, field, ge

sales = db.collection("sales")
sales.enable_columnar()                                   # optional; disable_columnar() drops the segments

res = sales.aggregate(
    {"orders": count(), "revenue": storine.sum(field("price") * "qty"), "avg_qty": avg("qty"),
     "cheapest": storine.min("price"), "products": count_distinct("sku")},
    ge("day", 20),                                        # where: a filter or {field: value}
    group_by=["region"],                                  # field paths and/or bucket("day", 7, offset=0)
    order_by="-revenue", limit=10)                        # output column names; "-" = descending
for row in res:                                           # dicts: group keys first, then aggregates in order
    print(row["region"], row["revenue"])                  # e.g. eu Decimal('12345.50')
print(res.matched, res.pruned_row_groups, res.spilled_bytes)

for key, values in sales.scan_columns(["qty", "price"], where=eq("region", "eu"), page_size=5000):
    ...                                                   # ColumnRow(key, {"qty": 3, "price": Decimal("9.99")}), key order
```

Aggregates: `count()` (rows), `count(x)` (non-null values), `sum`, `avg` (a `Decimal` with 6 more fractional
digits for exact inputs), `min`, `max`, `count_distinct`, `approx_count_distinct` (HyperLogLog). An argument is a
field path or an expression built from `field(path)`, `lit(value)` and numbers with `+`, `-`, `*` and unary `-`
(`2 * field("qty") - 1`). `storine.sum` / `storine.min` / `storine.max` are not exported by `import *`, so the
builtins are never shadowed. `scan_columns` returns a lazy iterator that follows the server's pages; its `.total`
is the number of matches once the first page is read. `memory_budget=` / `spill_budget=` (bytes, 0 = server
default) bound the group tables before they spill to disk. `OverloadedError` (too many analytic queries running)
is retried with backoff automatically.

**Values**: `None`, `bool`, `int` (64- or 128-bit), `float`, `Decimal` (exact, up to 38 digits — use it for money),
`str`, `bytes`, `datetime` (stored as UTC; naive datetimes are taken as UTC), `uuid.UUID`, lists and dicts.

**Reliability built in**: every write carries a request id that the client reuses on its own retries, so
timeouts, reconnects and cluster leader changes never apply a write twice. Clients follow cluster redirects
automatically; reads come from the leader unless `follower_reads=True`.

**Errors** (`storine.StorineError` subclasses): `NotFoundError`, `ConflictError` (`.write_index`,
`.current_version`), `InvalidError` (bad value, bound violated — don't retry), `UnauthorizedError`,
`OverloadedError` / `RateLimitedError` (retry later), `UnavailableError`, `ServerError`.

## Tests

```bash
python3 -m unittest tests.test_vectors          # protocol conformance (no server needed)
cargo build --release -p storage-server && python3 -m unittest tests.test_e2e
```
