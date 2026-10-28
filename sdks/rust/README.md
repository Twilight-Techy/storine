# Storine for Rust

No dependencies, `#![forbid(unsafe_code)]`, blocking I/O (from async code, call it inside `spawn_blocking`).

```toml
[dependencies]
storine = { git = "https://github.com/twilight-techy/storine", package = "storine" }
```

```rust
use storine::{add, dec, doc, eq, gt, Query};

let db = storine::connect("storine://127.0.0.1:7878")?; // or "storine://token@h1:7878,h2:7878,h3:7878"
let accounts = db.collection("accounts");

accounts.insert("alice", doc! { "owner" => "Alice", "balance" => dec("100.00") })?;
accounts.insert("bob", doc! { "owner" => "Bob", "balance" => dec("0.00") })?;

// Atomic, exact, server-side: no lost updates, no overdraft.
accounts.update("alice", [add("balance", dec("-25.00")).min(0)])?;

// Several writes, all or nothing.
db.transaction(|tx| {
    tx.update("accounts", "alice", [add("balance", dec("-10.00")).min(0)]);
    tx.update("accounts", "bob", [add("balance", dec("10.00"))]);
    Ok(())
})?;

let doc = accounts.get("alice")?.expect("exists");         // Option<Document>
println!("{:?} {}", doc.get("balance"), doc.version);       // Some(Decimal(65.00)) ...

accounts.create_index("owner")?;
let rich = accounts.find(Query::new().filter(gt("balance", dec("50"))).order_by("-balance").limit(10))?;
let bobs = accounts.count(eq("owner", "Bob"))?;
for d in accounts.scan(None) { let d = d?; /* every document, any collection size */ }

```

## API

| Call | Does |
| --- | --- |
| `storine::connect(url)` / `connect_with(url, Options { token, timeout, follower_reads, pool_size })` | Connection pool to a server or cluster (`Send + Sync`). |
| `db.collection(name)` | A collection handle. |
| `c.get(key)` | `Option<Document>` (`key`, `version`, `fields`, `get(field)`). |
| `c.put(key, fields)` / `c.put_with_ttl(key, fields, ttl)` | Create or replace; returns the version. |
| `c.insert(key, fields)` | Create only (`Error::Conflict` if it exists). |
| `c.replace(key, fields)` / `c.replace_if_version(key, version, fields)` | Replace an existing document (optionally only at a version). |
| `c.update(key, [changes])` / `c.update_if_version(key, version, [changes])` | Atomic server-side changes: `set`, `unset`, `add(path, amount).min(x).max(y)`, `append`. |
| `c.delete(key)` / `c.delete_if_version(key, version)` | `true` if it existed. |
| `c.find(filter or Query)` / `c.first(..)` / `c.count(filter)` / `c.scan(filter)` | Queries. Filters: `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `between`, `is_in`, `prefix`, `contains`, `exists`, `missing`, `key_prefix`, `and`, `or`, `matches(&doc)`, combined with `&`, `|`, `!`. `order_by("-price")` sorts descending. `scan` is a lazy iterator. |
| `c.nearest(field, &vector, k, filter)` / `c.nearest_exact(..)` | The `k` documents whose `field` (a list of numbers) is most similar to `vector` (cosine), best first; `doc.score()` holds the similarity. `nearest_exact` scores every candidate instead of using the vector index. |
| `c.create_vector_index(field, dims)` / `c.drop_vector_index(field)` / `c.vector_indexes()` | Vector indexes for `nearest`; writes of other values to an indexed field are refused. |
| `c.create_index` / `c.drop_index` / `c.info` / `c.drop` | Indexes (up to 16 per collection) and collection management. |
| `db.transaction(\|tx\| ...)` | Writes committed atomically when the closure returns `Ok`; `tx.require_version` / `tx.require_exists` add checks. |
| `db.collections()` / `db.cluster_info()` | Introspection. |

**Values** (`storine::Value`): `Null`, `Bool`, `Int` (i64; all smaller integer types convert), `UInt` (u64), `Int128`,
`Float`, `Decimal` (exact, up to 38 digits — use it for money; `dec("19.99")`, `Decimal::parse`, `Decimal::new`), `Text`,
`Bytes`, `Timestamp` (ns; `SystemTime` converts), `Uuid`, `Id`, `List` (`list![..]`) and `Map` (`doc! {..}`). Accessors:
`as_i64`, `as_decimal`, `as_str`, `as_list`, `as_map`, `as_system_time`, ...

**Reliability built in**: every write carries a request id that the client reuses on its own retries, so
timeouts, reconnects and cluster leader changes never apply a write twice. Clients follow cluster redirects
automatically; reads come from the leader unless `follower_reads` is set.

**Errors** (`storine::Error`): `NotFound { write_index }`, `Conflict { write_index, current_version }`, `Invalid` (bad value,
bound violated — don't retry), `Unauthorized`, `Overloaded` / `RateLimited` (retry later), `Unavailable`, `Server`.
`err.is_retryable()` tells which ones may succeed later.

## Tests

```bash
cargo build --release -p storage-server   # (from the repository root) for the end-to-end tests
cargo test                                # protocol conformance + single node + 3-node failover
```
