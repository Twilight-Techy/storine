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
| `$c->get($key)` | `Document` or `null`. |
| `$c->put($key, $fields, ttl: null)` | Create or replace; returns the version. `ttl` in seconds. |
| `$c->insert($key, $fields)` | Create only (`ConflictException` if it exists). |
| `$c->replace($key, $fields)` / `$c->replaceIfVersion($key, $version, $fields)` | Replace an existing document (optionally only at a version). |
| `$c->update($key, ...$changes)` / `$c->updateIfVersion($key, $version, ...$changes)` | Atomic server-side changes: `FieldOp::set`, `unset`, `add($path, $amount)->min(...)->max(...)`, `append`, or `['field' => value]` arrays. |
| `$c->delete($key)` / `$c->deleteIfVersion($key, $version)` | `delete` returns `true` if it existed. |
| `$c->find($where, orderBy:, limit:, offset:)` / `$c->first(...)` / `$c->count($where)` / `$c->scan($where)` | Queries. `$where` is a filter (`Filter::eq`, `ne`, `lt`, `le`, `gt`, `ge`, `between`, `in`, `prefix`, `contains`, `exists`, `missing`, `keyPrefix`, `and`, `or`, `not`) or `['field' => value]`. `orderBy: '-price'` sorts descending. `scan` is a generator. |
| `$c->createIndex($field)` / `$c->dropIndex($field)` / `$c->info()` / `$c->drop()` | Indexes (up to 4 per collection) and collection management. |
| `$db->transaction(fn (Transaction $tx) => ...)` | Buffered writes committed atomically when the callable returns (or `$db->transaction()` then `->commit()`); `requireVersion` / `requireExists` add checks. |

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

## Tests

```bash
php tests/harness.php tests/VectorsTest.php    # protocol conformance (no server needed)
cargo build --release -p storage-server && php tests/harness.php
```
