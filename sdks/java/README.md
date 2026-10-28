# Storine for Java

No runtime dependencies, Java 17+.

```xml
<dependency>
  <groupId>io.storine</groupId>
  <artifactId>storine</artifactId>
  <version>0.1.0</version>
</dependency>
```

```java
import static io.storine.FieldOp.add;
import static io.storine.Filter.*;

import io.storine.*;
import java.math.BigDecimal;
import java.util.Map;

try (Storine db = Storine.connect("storine://127.0.0.1:7878")) {   // or "storine://token@h1:7878,h2:7878,h3:7878"
    Collection accounts = db.collection("accounts");

    accounts.insert("alice", Map.of("owner", "Alice", "balance", new BigDecimal("100.00")));
    accounts.insert("bob", Map.of("owner", "Bob", "balance", new BigDecimal("0.00")));

    // Atomic, exact, server-side: no lost updates, no overdraft.
    accounts.update("alice", add("balance", new BigDecimal("-25.00")).min(0));

    // Several writes, all or nothing.
    db.transaction(tx -> {
        tx.update("accounts", "alice", add("balance", new BigDecimal("-10.00")).min(0));
        tx.update("accounts", "bob", add("balance", new BigDecimal("10.00")));
    });

    Document doc = accounts.get("alice").orElseThrow();
    System.out.println(doc.get("balance") + " " + doc.version());   // 65.00 ...

    accounts.createIndex("owner");
    var rich = accounts.find(new Query().where(gt("balance", new BigDecimal("50"))).orderBy("-balance").limit(10));
    long bobs = accounts.count(eq("owner", "Bob"));
    accounts.scan().forEach(d -> { /* every document, any collection size */ });
}
```

## API

| Call | Does |
| --- | --- |
| `Storine.connect(url[, Options.defaults().token(..).timeout(..).followerReads(..).poolSize(..)])` | Connection pool to a server or cluster (thread-safe). |
| `db.collection(name)` | A collection handle. |
| `c.get(key)` | `Optional<Document>` (`key()`, `version()`, `fields()`, `get(field)`). |
| `c.put(key, fields[, ttl])` | Create or replace; returns the version. |
| `c.insert(key, fields)` | Create only (`ConflictException` if it exists). |
| `c.replace(key, fields)` / `c.replaceIfVersion(key, version, fields)` | Replace an existing document (optionally only at a version). |
| `c.update(key, changes...)` / `c.updateIfVersion(key, version, changes...)` | Atomic server-side changes: `FieldOp.set`, `unset`, `add(path, amount).min(x).max(y)`, `append`. |
| `c.delete(key)` / `c.deleteIfVersion(key, version)` | `true` if it existed. |
| `c.find(Filter or Query)` / `c.first(query)` / `c.count(where)` / `c.scan(where)` | Queries. Filters: `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `between`, `in`, `prefix`, `contains`, `exists`, `missing`, `keyPrefix`, `and`, `or`, `not`, `match(Map)`. `orderBy("-price")` sorts descending. `scan` is a lazy `Stream`. |
| `c.createIndex` / `c.dropIndex` / `c.info` / `c.drop` | Indexes (up to 16 per collection) and collection management. |
| `c.nearest(field, vector, k[, where, exact])` | The `k` documents whose `field` is most similar to `vector` (cosine), best first; `doc.score()` is the similarity. |
| `c.createVectorIndex(field, dims)` / `c.dropVectorIndex(field)` / `c.vectorIndexes()` | Vector indexes for `nearest` (`VectorIndex(field, dims, ready)`). |
| `db.transaction(tx -> ...)` | Writes committed atomically when the lambda returns; `tx.requireVersion` / `tx.requireExists` add checks. |
| `db.collections()` / `db.clusterInfo()` | Introspection. |

**Values**: `null`, `Boolean`, `Byte`/`Short`/`Integer`/`Long` (int64), `UInt64`, `BigInteger` (up to 128 bits), `Float`/`Double`,
`BigDecimal` (exact, up to 38 digits — use it for money), `String`, `byte[]`, `Instant` (nanoseconds), `UUID`, collections/arrays
and `Map<String, ?>`. Decoded values use `Long`, `UInt64`, `BigInteger`, `Double`, `BigDecimal`, `String`, `byte[]`, `Instant`,
`UUID`, `List<Object>` and `Map<String, Object>`.

**Reliability built in**: every write carries a request id that the client reuses on its own retries, so
timeouts, reconnects and cluster leader changes never apply a write twice. Clients follow cluster redirects
automatically; reads come from the leader unless `followerReads(true)`.

**Errors** (unchecked, all `StorineException`): `NotFoundException`, `ConflictException` (`writeIndex()`, `currentVersion()`),
`InvalidException` (bad value, bound violated — don't retry), `UnauthorizedException`, `OverloadedException` /
`RateLimitedException` (retry later), `UnavailableException`, `ServerException`.

## Tests

```bash
cargo build --release -p storage-server   # for the end-to-end tests (skipped without it)
mvn test                                  # protocol conformance + single node + 3-node failover
```
