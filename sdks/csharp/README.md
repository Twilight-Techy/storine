# Storine for .NET

No dependencies, .NET 8+. Async-first, thread-safe.

```bash
dotnet add package Storine
```

```csharp
using Storine;
using static Storine.FieldOp;
using static Storine.Filter;

await using var db = StorineClient.Connect("storine://127.0.0.1:7878"); // or "storine://token@h1:7878,h2:7878,h3:7878"
var accounts = db.Collection("accounts");

await accounts.InsertAsync("alice", new Doc { ["owner"] = "Alice", ["balance"] = 100.00m });
await accounts.InsertAsync("bob", new Doc { ["owner"] = "Bob", ["balance"] = 0.00m });

// Atomic, exact, server-side: no lost updates, no overdraft.
await accounts.UpdateAsync("alice", Add("balance", -25.00m).Min(0));

// Several writes, all or nothing.
await db.TransactionAsync(tx =>
{
    tx.Update("accounts", "alice", Add("balance", -10.00m).Min(0));
    tx.Update("accounts", "bob", Add("balance", 10.00m));
});

var doc = await accounts.GetAsync("alice");          // null if it does not exist
Console.WriteLine($"{doc!["balance"]} {doc.Version}"); // 65.00 ...

await accounts.CreateIndexAsync("owner");
var rich = await accounts.FindAsync(Gt("balance", 50m), orderBy: "-balance", limit: 10);
long bobs = await accounts.CountAsync(Eq("owner", "Bob"));
await foreach (var d in accounts.ScanAsync()) { /* every document, any collection size */ }
```

## API

| Call | Does |
| --- | --- |
| `StorineClient.Connect(url, new StorineOptions { Token, Timeout, FollowerReads, PoolSize })` | Connection pool to a server or cluster. Options are optional. |
| `db.Collection(name)` | A collection handle. |
| `c.GetAsync(key)` | `Document` (`Key`, `Version`, `Fields`, `doc["field"]`) or `null`. |
| `c.GetAsync(key, fields: new[] { "name", "meta.size" })` | `GetAsync` returning only those (dotted) paths. |
| `c.PutAsync(key, fields, ttl: null)` | Create or replace; returns the version. |
| `c.InsertAsync(key, fields)` | Create only (`ConflictException` if it exists). |
| `c.ReplaceAsync(key, fields, ifVersion: null)` | Replace an existing document (optionally only at a version). |
| `c.UpdateAsync(key, changes...)` / `c.UpdateAsync(key, changes[], ifVersion)` | Atomic server-side changes: `Set`, `Unset`, `Add(path, amount).Min(x).Max(y)`, `Append`. |
| `c.DeleteAsync(key, ifVersion: null)` | `true` if it existed. |
| `c.FindAsync(where, orderBy, limit, offset)` / `c.FirstAsync` / `c.CountAsync(where)` / `c.ScanAsync(where)` | Queries. Filters: `Eq`, `Ne`, `Lt`, `Le`, `Gt`, `Ge`, `Between`, `In`, `Prefix`, `Contains`, `Exists`, `Missing`, `KeyPrefix`, `And`, `Or`, `Not`, `Match(doc)`, combined with `&`, `|`, `!`. `orderBy: "-price"` (or `new[] {"a", "-b"}`) sorts descending. |
| `c.CreateIndexAsync` / `c.DropIndexAsync` / `c.InfoAsync` / `c.DropAsync` | Indexes (up to 16 per collection) and collection management. |
| `c.NearestAsync(field, vector, k: 10, where: null, exact: false)` | The `k` documents whose `field` is most similar to `vector` (cosine), best first; `doc.Score` is the similarity. |
| `c.CreateVectorIndexAsync(field, dims)` / `c.DropVectorIndexAsync(field)` / `c.VectorIndexesAsync()` | Vector indexes for `NearestAsync` (`VectorIndex(Field, Dims, Ready)`). |
| `db.TransactionAsync(tx => ...)` | Writes committed atomically when the lambda returns; `tx.RequireVersion` / `tx.RequireExists` add checks. |
| `db.CollectionsAsync()` / `db.ClusterInfoAsync()` | Introspection. |

Every async method takes an optional `CancellationToken`.

**Large values**: a bytes (or text) field can be read in pieces without fetching the document.
`c.ReadBytesAsync(key, field, offset = 0, length = 16 MiB)` returns a `ByteRange` (`Version`, `Total`, `Data`; `null`
if the document does not exist; `Data` is empty past the end). `c.StreamBytesAsync(key, field, chunkSize = 1 MiB)` is
an `IAsyncEnumerable<ReadOnlyMemory<byte>>` that throws `ConflictException` if the document changes mid-read:

```csharp
var meta = await files.GetAsync("f1", fields: new[] { "name", "meta.size" }); // skips the blob
await foreach (var chunk in files.StreamBytesAsync("f1", "data")) await output.WriteAsync(chunk);
```

**Values**: `null`, `bool`, integer types (`ulong` is stored unsigned), `BigInteger` / `Int128` (up to 128 bits), `double`,
`decimal` (exact — use it for money; `BigDecimal` for up to 38 digits), `string`, `byte[]`, `DateTime` / `DateTimeOffset` /
`Timestamp` (nanoseconds), `Guid`, lists / arrays and string-keyed dictionaries (`Doc`). Decoded values use `long`, `ulong`,
`BigInteger`, `double`, `decimal` (or `BigDecimal` beyond 28 digits), `string`, `byte[]`, `Timestamp` (`.ToDateTime()`), `Guid`,
`List<object?>` and `Doc`.

**Reliability built in**: every write carries a request id that the client reuses on its own retries, so
timeouts, reconnects and cluster leader changes never apply a write twice. Clients follow cluster redirects
automatically; reads come from the leader unless `FollowerReads = true`.

**Errors** (all `StorineException`): `NotFoundException`, `ConflictException` (`WriteIndex`, `CurrentVersion`),
`InvalidException` (bad value, bound violated — don't retry), `UnauthorizedException`, `OverloadedException` /
`RateLimitedException` (retry later), `UnavailableException`, `ServerException`.

## Tests

```bash
cargo build --release -p storage-server   # for the end-to-end tests (they pass trivially without it)
cd tests/Storine.Tests && dotnet test     # protocol conformance + single node + 3-node failover
```
