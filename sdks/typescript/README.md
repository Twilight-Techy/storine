# Storine for Node.js / TypeScript

No runtime dependencies. Node.js 18+.

```ts
import { connect, add, gt, eq, Decimal } from "storine";

const db = connect("storine://127.0.0.1:7878");    // or "storine://token@h1:7878,h2:7878,h3:7878"
const accounts = db.collection("accounts");

await accounts.insert("alice", { owner: "Alice", balance: new Decimal("100.00") });
await accounts.insert("bob", { owner: "Bob", balance: new Decimal("0.00") });

// Atomic, exact, server-side: no lost updates, no overdraft.
await accounts.update("alice", add("balance", new Decimal("-25.00"), { min: 0 }));

// Several writes, all or nothing.
await db.transaction((tx) => {
  tx.update("accounts", "alice", add("balance", new Decimal("-10.00"), { min: 0 }));
  tx.update("accounts", "bob", add("balance", new Decimal("10.00")));
});

const doc = await accounts.get("alice");          // { key, version, fields } or undefined
console.log(doc?.fields.balance.toString());      // "65.00"

await accounts.createIndex("owner");
const rich = await accounts.find(gt("balance", new Decimal("50")), { orderBy: "-balance", limit: 10 });
console.log(await accounts.count(eq("owner", "Bob")));
for await (const d of accounts.scan()) { /* every document, any size */ }
db.close();
```

## API

| Call | Does |
| --- | --- |
| `connect(url, { token, timeoutMs, followerReads, poolSize })` | Lazy connection pool to a server or cluster. |
| `db.collection(name)` | A collection handle. |
| `c.get(key, { fields })` | `{ key, version, fields }` or `undefined`; `fields: ["name", "meta.size"]` returns only those paths. |
| `c.put(key, fields, { ttl })` | Create or replace; resolves to the version (`bigint`). `ttl` in seconds. |
| `c.insert(key, fields)` | Create only (`ConflictError` if it exists). |
| `c.replace(key, fields, { ifVersion })` | Replace an existing document (optionally only at a version). |
| `c.update(key, ...changes)` / `c.updateIfVersion(key, version, ...changes)` | Atomic server-side changes: `set`, `unset`, `add(path, amount, { min, max })`, `append`, or `{ field: value }`. |
| `c.delete(key, { ifVersion })` | `true` if it existed. |
| `c.find(where, { orderBy, limit, offset })`, `c.first`, `c.count`, `c.scan` | Queries. `where` is a filter (`eq`, `ne`, `lt`, `le`, `gt`, `ge`, `between`, `isIn`, `prefix`, `contains`, `exists`, `missing`, `keyPrefix`, `and`, `or`, `not`) or `{ field: value }`. `orderBy: "-price"` sorts descending. |
| `c.nearest(field, vector, { k = 10, where, exact = false })` | The `k` documents whose `field` (a list of numbers) is most similar to `vector` (cosine), best first; similarity in `doc.score`. |
| `c.createVectorIndex(field, dims)`, `c.dropVectorIndex(field)`, `c.vectorIndexes()` | Vector indexes for `nearest` (`[{ field, dims, ready }]`). |
| `c.createIndex(field)`, `c.dropIndex(field)`, `c.info()`, `c.drop()` | Indexes (up to 16 per collection) and collection management. |
| `db.transaction(tx => ...)` | Buffered writes committed atomically; `tx.require(...)` adds a version check. |

**Large values**: a bytes (or text) field can be read in pieces without fetching the document.
`c.readBytes(key, field, offset = 0, length = 16 MiB)` resolves to `{ version, total, data }` (`undefined` if
the document does not exist; `data` is empty past the end). `c.streamBytes(key, field, { chunkSize = 1 MiB })`
is an async generator of chunks that throws `ConflictError` if the document changes mid-read:

```ts
const meta = await files.get("f1", { fields: ["name", "meta.size"] }); // skips the blob
for await (const chunk of files.streamBytes("f1", "data")) out.write(chunk);
```

**Values**: `null`, `boolean`, integers (`number` up to 2^53, `bigint` up to 128 bits), floats (`number`;
`float(5)` forces an integral float), `Decimal` (exact, 38 digits — use it for money), `string`, `Uint8Array`,
`Date` (UTC), `Uuid`, arrays and plain objects. Integers beyond 2^53 come back as `bigint`; versions are `bigint`.

**Reliability built in**: every write carries a request id reused on the client's own retries, so timeouts,
reconnects and leader changes never apply a write twice; cluster redirects are followed automatically; reads go
to the leader unless `followerReads: true`.

**Errors**: `NotFoundError`, `ConflictError` (`writeIndex`, `currentVersion`), `InvalidError` (don't retry),
`UnauthorizedError`, `OverloadedError` / `RateLimitedError` (retry later), `UnavailableError`, `ServerError`.

## Tests

```bash
npm install && npm test        # conformance vectors + end-to-end (needs target/release/storage-server)
```
