<?php

declare(strict_types=1);

// Conformance: the SDK reproduces the server's encodings byte for byte (protocol/vectors).

use Storine\{Bytes, Decimal, FieldOp, Filter, Id, Int128, InvalidException, Sve, Timestamp, UInt64, Uuid, Wire, Write};

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
