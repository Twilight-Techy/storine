# Storine for Go

Standard library only, Go 1.21+.

```bash
go get github.com/twilight-techy/storine/sdks/go
```

```go
import storine "github.com/twilight-techy/storine/sdks/go"

db, err := storine.Connect("storine://127.0.0.1:7878") // or "storine://token@h1:7878,h2:7878,h3:7878"
if err != nil { ... }
defer db.Close()
accounts := db.Collection("accounts")

accounts.Insert(ctx, "alice", storine.Doc{"owner": "Alice", "balance": storine.MustDecimal("100.00")})
accounts.Insert(ctx, "bob", storine.Doc{"owner": "Bob", "balance": storine.MustDecimal("0.00")})

// Atomic, exact, server-side: no lost updates, no overdraft.
_, err = accounts.Update(ctx, "alice", storine.Add("balance", storine.MustDecimal("-25.00")).Min(0))

// Several writes, all or nothing.
_, err = db.Transaction(ctx, func(tx *storine.Tx) error {
	tx.Update("accounts", "alice", storine.Add("balance", storine.MustDecimal("-10.00")).Min(0))
	tx.Update("accounts", "bob", storine.Add("balance", storine.MustDecimal("10.00")))
	return nil
})

doc, err := accounts.Get(ctx, "alice")         // nil (and no error) if it does not exist
fmt.Println(doc.Fields["balance"], doc.Version) // 65.00 ...

accounts.CreateIndex(ctx, "owner")
rich, err := accounts.Find(ctx, storine.Query{
	Where:   storine.Gt("balance", storine.MustDecimal("50")),
	OrderBy: []string{"-balance"},
	Limit:   10,
})
n, err := accounts.Count(ctx, storine.Eq("owner", "Bob"))
err = accounts.Scan(ctx, nil, func(d *storine.Document) error { ...; return nil }) // any collection size
```

## API

| Call | Does |
| --- | --- |
| `storine.Connect(url, storine.Options{Token, Timeout, FollowerReads, PoolSize})` | Connection pool to a server or cluster (safe for concurrent use). Options are optional. |
| `db.Collection(name)` | A collection handle. |
| `c.Get(ctx, key)` | `*Document` (`Key`, `Version`, `Fields`) or `nil`. |
| `c.Put(ctx, key, fields)` / `c.PutWithTTL(ctx, key, fields, ttl)` | Create or replace; returns the version. |
| `c.Insert(ctx, key, fields)` | Create only (`ErrConflict` if it exists). |
| `c.Replace(ctx, key, fields)` / `c.ReplaceIfVersion(ctx, key, version, fields)` | Replace an existing document (optionally only at a version). |
| `c.Update(ctx, key, changes...)` / `c.UpdateIfVersion(ctx, key, version, changes...)` | Atomic server-side changes: `Set`, `Unset`, `Add(path, amount).Min(x).Max(y)`, `Append`. |
| `c.Delete(ctx, key)` / `c.DeleteIfVersion(ctx, key, version)` | `true` if it existed. |
| `c.Find(ctx, Query{Where, OrderBy, Limit, Offset})` / `c.First` / `c.Count(ctx, where)` / `c.Scan(ctx, where, fn)` | Queries. `Where` is a filter: `Eq`, `Ne`, `Lt`, `Le`, `Gt`, `Ge`, `Between`, `In`, `Prefix`, `Contains`, `Exists`, `Missing`, `KeyPrefix`, `And`, `Or`, `Not`, or `Match(Doc{...})`; `nil` matches all. `"-price"` sorts descending. |
| `c.Nearest(ctx, field, vector, NearestOptions{K, Where, Exact})` | The `K` (default 10) documents whose `field` (a list of numbers) is most similar to `vector` (`[]float32`, cosine), best first; similarity in `doc.Score()`. |
| `c.CreateVectorIndex(ctx, field, dims)` / `c.DropVectorIndex(ctx, field)` / `c.VectorIndexes(ctx)` | Vector indexes for `Nearest` (`[]VectorIndex{Field, Dims, Ready}`). |
| `c.CreateIndex` / `c.DropIndex` / `c.Info` / `c.Drop` | Indexes (up to 16 per collection) and collection management. |
| `db.Transaction(ctx, func(tx *Tx) error)` | Writes committed atomically when `fn` returns nil; `tx.RequireVersion` / `tx.RequireExists` add checks. |
| `db.Collections(ctx)` / `db.ClusterInfo(ctx)` | Introspection. |

**Values**: `nil`, `bool`, all integer types (`int`… → int64, `uint64`/`uint` → uint64), `*big.Int` (up to 128 bits),
`float64`, `storine.Decimal` (exact, up to 38 digits — use it for money; `MustDecimal("19.99")`, `ParseDecimal`, `NewDecimal`),
`string`, `[]byte`, `time.Time` (nanoseconds, returned as UTC), `storine.UUID`, slices and `map[string]T` (as lists and maps).
Decoded values use `int64`, `uint64`, `*big.Int`, `float64`, `Decimal`, `string`, `[]byte`, `time.Time`, `UUID`, `[]any`, `storine.Doc`.

**Reliability built in**: every write carries a request id that the client reuses on its own retries, so
timeouts, reconnects and cluster leader changes never apply a write twice. Clients follow cluster redirects
automatically; reads come from the leader unless `FollowerReads` is set. Every call honors `ctx` cancellation.

**Errors** are `*storine.Error`; test with `errors.Is`: `ErrNotFound`, `ErrConflict` (`WriteIndex`, `CurrentVersion`),
`ErrInvalid` (bad value, bound violated — don't retry), `ErrUnauthorized`, `ErrOverloaded` / `ErrRateLimited` (retry later),
`ErrUnavailable`, `ErrServer`.

## Tests

```bash
go test -run 'Vector|Hostile|Mapping' .                       # protocol conformance (no server needed)
cargo build --release -p storage-server && go test -race .   # plus end-to-end, single node and 3-node failover
```
