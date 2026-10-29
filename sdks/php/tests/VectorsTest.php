<?php

declare(strict_types=1);

// Conformance: the SDK reproduces the server's encodings byte for byte (protocol/vectors).

use Storine\{Agg, Bytes, Decimal, Expr, FieldOp, Filter, Id, Int128, InvalidException, Sve, Timestamp, UInt64, Uuid, Wire, Write};

function vectors(): array
{
    static $v = null;
    return $v ??= json_decode((string) file_get_contents(__DIR__ . '/../../../protocol/vectors/vectors.json'), true);
}

function native(array $desc): mixed
{
    $v = $desc['value'] ?? null;
    return match ($desc['type']) {
        'null' => null,
        'bool' => $v,
        'int64' => (int) $v,
        'uint64' => new UInt64($v),
        'float64' => unpack('E', (string) hex2bin($desc['bits']))[1],
        'int128' => new Int128($v),
        'timestamp' => new Timestamp((int) $v),
        'uuid' => new Uuid((string) hex2bin($v)),
        'id' => new Id((string) hex2bin($v)),
        'text' => $v,
        'bytes' => new Bytes((string) hex2bin($v)),
        'decimal' => Decimal::of($v),
        'list' => array_map('native', $v),
        'map' => array_combine(array_column($v, 0), array_map(fn ($kv) => native($kv[1]), $v)) ?: new \stdClass(),
    };
}

/** Values compared by type and exact content (decimals by text, so the scale counts). */
function comparable(mixed $x): mixed
{
    return match (true) {
        $x instanceof Decimal => ['dec', (string) $x],
        $x instanceof Int128, $x instanceof UInt64 => ['int', $x->value],
        is_int($x) => ['int', (string) $x],
        is_float($x) => ['float', bin2hex(pack('e', $x))],
        $x instanceof Timestamp => ['ts', $x->ns],
        $x instanceof \DateTimeInterface => ['ts', $x->getTimestamp() * 1_000_000_000 + (int) $x->format('u') * 1000],
        $x instanceof Bytes => ['bytes', bin2hex($x->data)],
        $x instanceof Uuid, $x instanceof Id => [get_class($x), bin2hex($x->bytes)],
        $x instanceof \stdClass => [],
        is_array($x) => array_map('comparable', $x),
        default => $x,
    };
}

function testValuesEncodeAndDecode(): void
{
    foreach (vectors()['values'] as $vec) {
        $value = native($vec['value']);
        same($vec['hex'], bin2hex(Sve::encodeValue($value)), $vec['name']);
        same(comparable($value), comparable(Sve::decode((string) hex2bin($vec['hex']))), $vec['name']);
    }
}

function testDocumentBody(): void
{
    $doc = vectors()['documents'][0];
    $out = '';
    Sve::encodeMap(native($doc['fields']), $out);
    same($doc['hex'], bin2hex($out));
}

function testRequests(): void
{
    $d = ['balance' => Decimal::of('100.00'), 'owner' => 'alice', 'active' => true];
    $rid = (string) hex2bin('0102030405060708090a0b0c0d0e0f10');
    $built = [
        'get accounts/alice' => Wire::encodeGet('accounts', 'alice'),
        'get accounts/alice: bytes 1000..1004 of photo' => Wire::encodeGetRange('accounts', 'alice', 'photo', 1000, 4),
        'get accounts/alice: fields owner, meta.size' => Wire::encodeGetFields('accounts', 'alice', ['owner', 'meta.size']),
        'write: put' => Wire::encodeWrites([new Write('accounts', 'alice', 1, 0, 0, $d)], null),
        'write: insert (must not exist) with request id' => Wire::encodeWrites([new Write('accounts', 'alice', 1, 1, 0, $d)], str_repeat("\xAB", 16)),
        'write: replace if version 7' => Wire::encodeWrites([new Write('accounts', 'alice', 1, 3, 7, $d)], null),
        'write: put with ttl 1500 ms' => Wire::encodeWrites([new Write('sessions', 's1', 2, 0, 0, ['u' => 1], 1500)], null),
        'write: delete' => Wire::encodeWrites([new Write('accounts', 'alice', 3)], null),
        'write: delete if exists' => Wire::encodeWrites([new Write('accounts', 'alice', 3, 2)], null),
        'write: transfer 25.00 (two patches, no overdraft) with request id' => Wire::encodeWrites([
            new Write('accounts', 'alice', 4, 2, 0, null, 0, [FieldOp::add('balance', Decimal::of('-25.00'))->min(0)]),
            new Write('accounts', 'bob', 4, 2, 0, null, 0, [FieldOp::add('balance', Decimal::of('25.00'))]),
        ], $rid),
        'write: patch set / unset / append / add with min and max' => Wire::encodeWrites([
            new Write('c', 'k', 4, 2, 0, null, 0, [FieldOp::set('profile.name', 'Ann'), FieldOp::unset('tmp'),
                FieldOp::append('log', 3), FieldOp::add('n', 1)->min(0)->max(10)]),
        ], null),
        'write: check version 3' => Wire::encodeWrites([new Write('c', 'k', 5, 3, 3)], null),
        'query: all (default limit)' => Wire::encodeQuery('items', null, [], 0, 0, null),
        'query: eq text' => Wire::encodeQuery('items', Filter::eq('kind', 'book'), [], 10, 0, null),
        'query: and(gte, lt) sorted desc, offset 20' => Wire::encodeQuery('items',
            Filter::and(Filter::ge('price', Decimal::of('10')), Filter::lt('price', Decimal::of('20.50'))), Wire::parseOrderBy('-price'), 10, 20, null),
        'query: between' => Wire::encodeQuery('items', Filter::between('n', 1, 5), [], 0, 0, null),
        'query: in' => Wire::encodeQuery('items', Filter::in('n', [1, 2]), [], 0, 0, null),
        'query: prefix / contains / exists / missing' => Wire::encodeQuery('items', Filter::or(
            Filter::prefix('name', 'An'), Filter::contains('tags', 'x'), Filter::exists('email'), Filter::missing('deleted_at')), [], 0, 0, null),
        'query: not / ne / le / gt / key prefix' => Wire::encodeQuery('items', Filter::and(
            Filter::not(Filter::ne('a', 1)), Filter::le('b', 2.5), Filter::gt('c', 0), Filter::keyPrefix('user:')), [], 0, 0, null),
        'query: after key cursor' => Wire::encodeQuery('items', null, [], 100, 0, 'k0099'),
        'query: multi-key sort' => Wire::encodeQuery('items', null, Wire::parseOrderBy(['a', '-b']), 5, 0, null),
        'query: nearest neighbours with a filter' => Wire::encodeQuery('items', Filter::eq('kind', 'book'), [], 5, 0, null,
            ['embedding', [0.25, -1.5, 3.0], 5, false]),
        'query: nearest neighbours, exact' => Wire::encodeQuery('items', null, [], 0, 0, null, ['v', [1.0, 0.0], 1, true]),
        // Analytics (TASK-129).
        'aggregate: by region where day >= 20, count / sum(price * qty) / avg(qty), sum desc, limit 2' => Wire::encodeAggregate('sales',
            ['n' => Agg::count(), 'revenue' => Agg::sum(Expr::field('price')->times('qty')), 'avg_qty' => Agg::avg('qty')],
            Filter::ge('day', 20), 'region', '-revenue', 2)[0],
        'aggregate: totals (count distinct, approx count distinct, min, max, count of field)' => Wire::encodeAggregate('sales', [
            'regions' => Agg::countDistinct('region'), 'customers' => Agg::approxCountDistinct(Expr::field('customer')),
            'cheapest' => Agg::min('price'), 'dearest' => Agg::max('price'), 'with_coupon' => Agg::count('coupon')])[0],
        'aggregate: weekly buckets of day (width 7, offset 3) and region, sum(qty - 1) and sum(-price + 2.5), budgets' => Wire::encodeAggregate('sales',
            ['a' => Agg::sum(Expr::field('qty')->minus(1)), 'b' => Agg::sum(Expr::field('price')->neg()->plus(Decimal::of('2.5')))],
            groupBy: [Agg::bucket('day', 7, 3), 'region'], orderBy: ['day', '-region'], memoryBudget: 1 << 20, spillBudget: 1 << 30)[0],
        'scan: qty, price where region = eu, limit 500' => Wire::encodeScanColumns('sales', Filter::eq('region', 'eu'), ['qty', 'price'], null, 500),
        'scan: meta.size after s0099 (default limit)' => Wire::encodeScanColumns('sales', null, ['meta.size'], 's0099', 0),
        'admin: enable columnar sales' => "\x0a" . pack('V', 5) . 'sales',
        'admin: disable columnar sales' => "\x0b" . pack('V', 5) . 'sales',
    ];
    $seen = 0;
    foreach (vectors()['requests'] as $vec) {
        if (isset($built[$vec['name']])) {
            same($vec['body_hex'], bin2hex($built[$vec['name']]), $vec['name']);
            $seen++;
        }
    }
    same(count($built), $seen, 'request vectors found');
}

function testResponses(): void
{
    $r = array_column(vectors()['responses'], null, 'name');
    same([[12, null], false], Wire::decodeWriteResponse((string) hex2bin($r['write ok: [v12, deleted]']['body_hex'])));
    same([42, 'k1', [['k1', 3, ['n' => 1]]]], Wire::decodePage((string) hex2bin($r['page: total 42, next k1, one doc']['body_hex'])));
    $range = (string) hex2bin($r['byte range: total 5000, 4 bytes']['body_hex']);
    same([5000, "\xde\xad\xbe\xef"], Wire::decodeByteRange($range));
    throws(InvalidException::class, fn () => Wire::decodeByteRange(substr($range, 0, -1)), 'truncated byte range');
}

function testAnalyticsResponses(): void
{
    $r = array_column(vectors()['responses'], null, 'name');
    $agg = Wire::decodeAggregateResult((string) hex2bin($r['aggregate: matched 4, pruned 2, rows [eu, 3, 12.50] [us, 1, null]']['body_hex']));
    same(comparable([['eu', 3, Decimal::of('12.50')], ['us', 1, null]]), comparable($agg['rows']));
    same('12.50', (string) $agg['rows'][0][2]);
    same([4, 2, 0, 3], [$agg['matched'], $agg['prunedRowGroups'], $agg['spilledBytes'], $agg['columns']]);
    $scan = (string) hex2bin($r['scan page: total 2, next s1, row s1 [7, 9.99]']['body_hex']);
    $page = Wire::decodeScanPage($scan);
    same([2, 's1', 2], [$page['total'], $page['next'], $page['fields']]);
    same(comparable([['s1', [7, Decimal::of('9.99')]]]), comparable($page['rows']));
    for ($cut = 0; $cut < strlen($scan); $cut++) {
        throws(InvalidException::class, fn () => Wire::decodeScanPage(substr($scan, 0, $cut)), "scan page cut at $cut");
    }
}

function testAnalyticsArgumentRules(): void
{
    throws(InvalidException::class, fn () => Wire::encodeAggregate('c', ['n' => Agg::count()], orderBy: 'nope'), 'unknown order column');
    throws(InvalidException::class, fn () => Wire::encodeAggregate('c', ['region' => Agg::count()], groupBy: 'region'), 'duplicate column');
    throws(InvalidException::class, fn () => Wire::encodeAggregate('c', ['n' => 'count']), 'not an Agg');
    throws(InvalidException::class, fn () => Agg::bucket('day', 0), 'zero width');
    throws(InvalidException::class, fn () => Wire::encodeScanColumns('c', null, ['a'], null, 100_001), 'page too large');
    // Plain numbers are constants; strings are field paths.
    same(bin2hex(Wire::encodeAggregate('c', ['x' => Agg::sum(Expr::lit(1)->plus(Expr::field('a')))])[0]),
        bin2hex(Wire::encodeAggregate('c', ['x' => Agg::sum(Expr::of(1)->plus('a'))])[0]), 'shorthands');
}

function testHostileBytesNeverCrash(): void
{
    $values = vectors()['values'];
    $good = (string) hex2bin($values[count($values) - 1]['hex']);
    for ($cut = 0; $cut < strlen($good); $cut++) {
        throws(InvalidException::class, fn () => Sve::decode(substr($good, 0, $cut)), "cut at $cut");
    }
    throws(InvalidException::class, fn () => Sve::decode("\x0C\xFF\xFF\xFF\xFF"), 'huge list count');
}

function testValueMappingRules(): void
{
    throws(InvalidException::class, fn () => Decimal::of('NaN'), 'NaN');
    throws(InvalidException::class, fn () => new Int128('1' . str_repeat('0', 40)), 'over 128 bits');
    throws(InvalidException::class, fn () => Sve::encodeValue("\xFF\xFE"), 'invalid UTF-8 text');
    same('1000', (string) Sve::decode(Sve::encodeValue(Decimal::of('1E+3'))));
    $big = Decimal::of('1234567890123456789012345678.0123456789');
    same((string) $big, (string) Sve::decode(Sve::encodeValue($big)));
    // Whole microseconds come back as DateTimeImmutable, finer values as Timestamp.
    same(-1001, Sve::decode(Sve::encodeValue(new Timestamp(-1001)))->ns);
    same('1969-12-31 23:59:59.999999', Sve::decode(Sve::encodeValue(new Timestamp(-1000)))->format('Y-m-d H:i:s.u'));
    $t = new \DateTimeImmutable('2024-05-01 12:30:00.123456', new \DateTimeZone('UTC'));
    same($t->format('Y-m-d H:i:s.u'), Sve::decode(Sve::encodeValue($t))->format('Y-m-d H:i:s.u'));
}

function testDecimalArithmeticIsExact(): void
{
    $a = Decimal::of('0.10');
    same('0.30', (string) $a->add('0.2'));
    same('-0.05', (string) $a->sub('0.15'));
    same('0.0300', (string) $a->mul('0.30'));
    same('59.97', (string) Decimal::of('19.99')->mul(3));
    check(Decimal::of('1.50')->equals('1.5'), '1.50 == 1.5');
    same(1, Decimal::of('2')->compareTo(1));
    same(-1, Decimal::of('-170141183460469231731687303715884105728')->compareTo('0'));
}
