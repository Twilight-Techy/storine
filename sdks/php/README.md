# Storine for PHP

Standard library only (no extensions beyond PHP's defaults), PHP 8.1+.

```php
use Storine\{Decimal, FieldOp, Filter, Storine, Transaction};

require 'vendor/autoload.php';          // or: require 'path/to/sdks/php/src/autoload.php';

$db = Storine::connect('storine://127.0.0.1:7878');  // or 'storine://token@h1:7878,h2:7878,h3:7878'
$accounts = $db->collection('accounts');

$accounts->insert('alice', ['owner' => 'Alice', 'balance' => Decimal::of('100.00')]);
$accounts->insert('bob', ['owner' => 'Bob', 'balance' => Decimal::of('0.00')]);

// Atomic, exact, server-side: no lost updates, no overdraft.
$accounts->update('alice', FieldOp::add('balance', Decimal::of('-25.00'))->min(0));

// Several writes, all or nothing.
$db->transaction(function (Transaction $tx) {
    $tx->update('accounts', 'alice', FieldOp::add('balance', Decimal::of('-10.00'))->min(0));
    $tx->update('accounts', 'bob', FieldOp::add('balance', Decimal::of('10.00')));
});

$doc = $accounts->get('alice');           // Document: read like an array, plus ->key and ->version
echo $doc['balance'], ' ', $doc->version;  // 65.00 ...

$accounts->createIndex('owner');
$rich = $accounts->find(Filter::gt('balance', Decimal::of('50')), orderBy: '-balance', limit: 10);
echo $accounts->count(Filter::eq('owner', 'Bob'));
foreach ($accounts->scan() as $d) {       // every document, any collection size
    // ...
}
```

## API

| Call | Does |
| --- | --- |
| `Storine::connect($url, token:, timeout: 5.0, followerReads: false)` | Connection pool to a server or cluster. |
| `$db->collection($name)` | A collection handle. |
| `$c->get($key, fields: null)` | `Document` or `null`. `fields: ['name', 'meta.size']` returns only those (dotted) paths; nested paths come back nested. |
| `$c->readBytes($key, $field, $offset, $length)` / `$c->streamBytes($key, $field, chunkSize: 1 << 20)` | Part of a bytes / text field (see *Large values*). |
| `$c->put($key, $fields, ttl: null)` | Create or replace; returns the version. `ttl` in seconds. |
| `$c->insert($key, $fields)` | Create only (`ConflictException` if it exists). |
| `$c->replace($key, $fields)` / `$c->replaceIfVersion($key, $version, $fields)` | Replace an existing document (optionally only at a version). |
| `$c->update($key, ...$changes)` / `$c->updateIfVersion($key, $version, ...$changes)` | Atomic server-side changes: `FieldOp::set`, `unset`, `add($path, $amount)->min(...)->max(...)`, `append`, or `['field' => value]` arrays. |
| `$c->delete($key)` / `$c->deleteIfVersion($key, $version)` | `delete` returns `true` if it existed. |
| `$c->find($where, orderBy:, limit:, offset:)` / `$c->first(...)` / `$c->count($where)` / `$c->scan($where)` | Queries. `$where` is a filter (`Filter::eq`, `ne`, `lt`, `le`, `gt`, `ge`, `between`, `in`, `prefix`, `contains`, `exists`, `missing`, `keyPrefix`, `and`, `or`, `not`) or `['field' => value]`. `orderBy: '-price'` sorts descending. `scan` is a generator. |
| `$c->nearest($field, $vector, k: 10, where: null, exact: false)` | The `k` documents whose `$field` (a list of numbers) is most similar to `$vector` (cosine), best first; `$doc->score()` holds the similarity. `exact: true` scores every candidate instead of using the vector index. |
| `$c->createVectorIndex($field, $dims)` / `$c->dropVectorIndex($field)` / `$c->vectorIndexes()` | Vector indexes for `nearest`; writes of other values to an indexed field are refused. |
| `$c->createIndex($field)` / `$c->dropIndex($field)` / `$c->info()` / `$c->drop()` | Indexes (up to 16 per collection) and collection management. |
| `$c->aggregate($aggregates, where:, groupBy:, orderBy:, limit:, memoryBudget:, spillBudget:)` / `$c->scanColumns($fields, where:, pageSize: 1000)` / `$c->enableColumnar()` / `$c->disableColumnar()` | Analytics (see *Analytics*). |
| `$db->transaction(fn (Transaction $tx) => ...)` | Buffered writes committed atomically when the callable returns (or `$db->transaction()` then `->commit()`); `requireVersion` / `requireExists` add checks. |

**Large values**: a document can hold a large `Bytes` field (a file) next to small metadata. Read the metadata alone
with `get($key, fields: [...])`, a slice with `readBytes` (a `ByteRange` with `version`, `total` and `data`, or `null` if
the document does not exist; at most 16 MiB per call, empty `data` past the end), or the whole value chunk by chunk
with the `streamBytes` generator, which throws `NotFoundException` if the document does not exist and
`ConflictException` if it changes mid-read:

```php
$meta = $files->get('f1', fields: ['name', 'meta.size']);
$head = $files->readBytes('f1', 'data', 0, 4096)->data;
$out = fopen('cat.png', 'wb');
foreach ($files->streamBytes('f1', 'data') as $chunk) {
    fwrite($out, $chunk);
}
```

**Values**: `null`, `bool`, `int`, `float`, `Decimal` (exact, up to 38 digits, keeps its scale — use it for money,
never floats), `string` (UTF-8 text), `Bytes` (binary data), `DateTimeInterface` (stored as UTC; `Timestamp` for
nanosecond precision), `Uuid`, `Id`, `Int128` / `UInt64` (integers beyond PHP's 64-bit `int`), lists and maps.
Arrays with keys `0..n-1` are lists, other arrays are maps; an empty array is an empty list (use
`new \stdClass()` for an empty map).

**Reliability built in**: every write carries a request id that the client reuses on its own retries, so
timeouts, reconnects and cluster leader changes never apply a write twice. Clients follow cluster redirects
automatically; reads come from the leader unless `followerReads: true`.

**Errors** (`Storine\StorineException` subclasses): `NotFoundException`, `ConflictException` (`writeIndex`,
`currentVersion`), `InvalidException` (bad value, bound violated — don't retry), `UnauthorizedException`,
`OverloadedException` / `RateLimitedException` (retry later), `UnavailableException`, `ServerException`.

## Analytics

Group and aggregate on the server, exactly (integer and decimal sums never round), as of one snapshot:

```php
use Storine\{Agg, Expr, Filter};

$sales = $db->collection('sales');
$sales->enableColumnar();                  // optional: column segments + vectorized execution
$r = $sales->aggregate(
    ['orders' => Agg::count(), 'revenue' => Agg::sum(Expr::field('price')->times('qty')),
     'avgQty' => Agg::avg('qty'), 'customers' => Agg::countDistinct('customer')],
    where: Filter::ge('day', 20), groupBy: 'region', orderBy: '-revenue', limit: 10);
$r->rows;      // [['region' => 'eu', 'orders' => 1204, 'revenue' => Decimal, 'avgQty' => Decimal, 'customers' => 310], ...]
$r->matched;   // rows that matched; also ->prunedRowGroups, ->spilledBytes, ->columns

$sales->aggregate(['qty' => Agg::sum('qty')], groupBy: [Agg::bucket('day', 7), 'region']);  // weekly buckets
foreach ($sales->scanColumns(['qty', 'price'], where: ['region' => 'eu'], pageSize: 500) as $key => $fields) {
    // only those fields ($fields['qty'], $fields['price']), in key order, page by page
}
$sales->disableColumnar();
```

- Aggregates (`Agg::`): `count()` (rows), `count($x)` (non-null values), `sum`, `avg` (a `Decimal` for exact
  inputs), `min`, `max`, `countDistinct`, `approxCountDistinct`, keyed by output name in output order.
- An argument is a field path (`'qty'`) or an expression: `Expr::field($path)`, `Expr::lit($value)` (or a plain
  number / `Decimal`), combined with `->plus($x)`, `->minus($x)`, `->times($x)`, `->neg()`; strings inside are
  field paths.
- `groupBy`: field paths and `Agg::bucket($field, $width, $offset = 0)`; rows hold the group keys (named by their
  field path) then the aggregates. `orderBy` names output columns (`'-revenue'`).
- `memoryBudget` / `spillBudget` (bytes, 0 = server default) bound the query; `OverloadedException` (too many
  analytic queries) is retried with backoff automatically.

## Tests

```bash
php tests/harness.php tests/VectorsTest.php    # protocol conformance (no server needed)
cargo build --release -p storage-server && php tests/harness.php
```
