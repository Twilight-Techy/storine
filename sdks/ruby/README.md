# Storine for Ruby

Standard library only, Ruby 3.2+.

```ruby
require "storine"

D = Storine.method(:Decimal)

db = Storine.connect("storine://127.0.0.1:7878")        # or "storine://token@h1:7878,h2:7878,h3:7878"
accounts = db.collection("accounts")

accounts.insert("alice", { "owner" => "Alice", "balance" => D.("100.00") })
accounts.insert("bob", { "owner" => "Bob", "balance" => D.("0.00") })

# Atomic, exact, server-side: no lost updates, no overdraft.
accounts.update("alice", Storine.add("balance", D.("-25.00"), min: 0))

# Several writes, all or nothing.
db.transaction do |tx|
  tx.update("accounts", "alice", Storine.add("balance", D.("-10.00"), min: 0))
  tx.update("accounts", "bob", Storine.add("balance", D.("10.00")))
end

doc = accounts.get("alice")              # Document: a Hash with #key and #version
puts doc["balance"], doc.version         # 65.00 ...

accounts.create_index("owner")
rich = accounts.find(Storine.gt("balance", D.("50")), order_by: "-balance", limit: 10)
puts accounts.count(Storine.eq("owner", "Bob"))
accounts.scan { |d| ... }                # every document, any collection size (Enumerator without a block)
```

## API

| Call | Does |
| --- | --- |
| `Storine.connect(url, token: nil, timeout: 5.0, follower_reads: false)` | Connection pool to a server or cluster (thread-safe). |
| `db.collection(name)` / `db[name]` | A collection handle. |
| `c.get(key, fields: nil)` | `Document` or `nil`. `fields: ["name", "meta.size"]` returns only those (dotted) paths; nested paths come back nested. |
| `c.read_bytes(key, field, offset = 0, length = 16 MiB)` / `c.stream_bytes(key, field, chunk_size: 1 << 20)` | Part of a bytes / text field (see *Large values*). |
| `c.put(key, fields, ttl: nil)` | Create or replace; returns the version. `ttl:` in seconds. |
| `c.insert(key, fields)` | Create only (`ConflictError` if it exists). |
| `c.replace(key, fields, if_version: nil)` | Replace an existing document (optionally only at a version). |
| `c.update(key, *changes, if_version: nil)` | Atomic server-side changes: `Storine.set`, `unset`, `add(path, amount, min:, max:)`, `append`, or a `{ field => value }` Hash. |
| `c.delete(key, if_version: nil)` | `true` if it existed. |
| `c.find(where, order_by:, limit:, offset:)` / `c.first(...)` / `c.count(where)` / `c.scan(where)` | Queries. `where` is a filter (`Storine.eq`, `ne`, `lt`, `le`, `gt`, `ge`, `between`, `in`, `prefix`, `contains`, `exists`, `missing`, `key_prefix`, `and`, `or`, `not`; or combine with `&`, `\|`, `~`) or `{ field => value }`. `order_by: "-price"` sorts descending. |
| `c.nearest(field, vector, k: 10, where: nil, exact: false)` | The `k` documents whose `field` (a list of numbers) is most similar to `vector` (cosine), best first; `doc.score` holds the similarity. `exact: true` scores every candidate instead of using the vector index. |
| `c.create_vector_index(field, dims)` / `c.drop_vector_index(field)` / `c.vector_indexes` | Vector indexes for `nearest`; writes of other values to an indexed field are refused. |
| `c.create_index(field)` / `c.drop_index(field)` / `c.info` / `c.drop` | Indexes (up to 16 per collection) and collection management. |
| `db.transaction { \|tx\| ... }` | Buffered writes committed atomically when the block returns (or `tx.commit`); `tx.require(...)` adds a version check. |

**Large values**: a document can hold a large binary field (a file) next to small metadata. Read the metadata alone
with `get(key, fields: [...])`, a slice with `read_bytes` (a `Storine::ByteRange` with `version`, `total` and `data`, or
`nil` if the document does not exist; at most 16 MiB per call, empty `data` past the end), or the whole value chunk by
chunk with `stream_bytes` (yields to a block, or returns an Enumerator), which raises `NotFoundError` if the document
does not exist and `ConflictError` if it changes mid-read:

```ruby
meta = files.get("f1", fields: ["name", "meta.size"])
head = files.read_bytes("f1", "data", 0, 4096).data
File.open("cat.png", "wb") { |f| files.stream_bytes("f1", "data") { |chunk| f.write(chunk) } }
```

**Values**: `nil`, `true` / `false`, `Integer` (64- or 128-bit), `Float`, `Storine::Decimal` (exact, up to 38
digits, keeps its scale — use it for money; `BigDecimal` is accepted too and `#to_d` converts back), `String`
(UTF-8 text; binary strings such as `"\x00".b` are bytes), `Time` (UTC, nanosecond precision),
`Storine::Uuid`, `Storine::Id`, `Storine::UInt64`, arrays and hashes (String or Symbol keys; read back as
Strings).

**Reliability built in**: every write carries a request id that the client reuses on its own retries, so
timeouts, reconnects and cluster leader changes never apply a write twice. Clients follow cluster redirects
automatically; reads come from the leader unless `follower_reads: true`.

**Errors** (`Storine::Error` subclasses): `NotFoundError`, `ConflictError` (`#write_index`,
`#current_version`), `InvalidError` (bad value, bound violated — don't retry), `UnauthorizedError`,
`OverloadedError` / `RateLimitedError` (retry later), `UnavailableError`, `ServerError`.

## Tests

```bash
ruby -Ilib test/test_vectors.rb                 # protocol conformance (no server needed)
cargo build --release -p storage-server && ruby -Ilib test/test_e2e.rb
```
