<?php

declare(strict_types=1);

// End-to-end tests against a real storage-server (single node and a 3-node cluster).
// The server binary is $STORINE_SERVER or <repo>/target/release/storage-server
// (build it with `cargo build --release -p storage-server`); without it the tests are skipped.

use Storine\{Agg, Bytes, ConflictException, Decimal, Expr, FieldOp, Filter, InvalidException, NotFoundException, Storine,
    Transaction, UnavailableException, Uuid};

function server_bin(): string
{
    return getenv('STORINE_SERVER') ?: dirname(__DIR__, 3) . '/target/release/storage-server';
}

function free_port(): int
{
    $s = stream_socket_server('tcp://127.0.0.1:0');
    $port = (int) substr(strrchr(stream_socket_get_name($s, false), ':'), 1);
    fclose($s);
    return $port;
}

function wait_port(int $port, float $timeout = 10.0): void
{
    $deadline = microtime(true) + $timeout;
    while (true) {
        $c = @stream_socket_client("tcp://127.0.0.1:$port", $errno, $errstr, 0.2);
        if ($c) {
            fclose($c);
            return;
        }
        if (microtime(true) > $deadline) {
            throw new \RuntimeException("server on port $port did not start");
        }
        usleep(50_000);
    }
}

/** @return resource the server process */
function start_server(string $dir, int $port, string ...$extra)
{
    $cmd = array_merge([server_bin(), $dir, "127.0.0.1:$port"], $extra);
    $p = proc_open($cmd, [0 => ['file', '/dev/null', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']], $pipes);
    wait_port($port);
    return $p;
}

function stop_server($p, int $signal = 15): void
{
    if (is_resource($p) && proc_get_status($p)['running']) {
        proc_terminate($p, $signal);
    }
    if (is_resource($p)) {
        proc_close($p);
    }
}

function rm_tree(string $dir): void
{
    if (is_dir($dir)) {
        exec('rm -rf ' . escapeshellarg($dir));
    }
}

/** Runs `$test` against a fresh single-node server. */
function with_server(callable $test): void
{
    if (!is_executable(server_bin())) {
        throw new Skip('storage-server binary not built');
    }
    $dir = sys_get_temp_dir() . '/storine_php_' . bin2hex(random_bytes(4));
    $port = free_port();
    $p = start_server($dir, $port);
    try {
        $db = Storine::connect("storine://127.0.0.1:$port");
        $test($db, $port);
        $db->close();
    } finally {
        stop_server($p);
        rm_tree($dir);
    }
}

function testCrudTypesAndVersions(): void
{
    with_server(function (Storine $db) {
        $c = $db->collection('people');
        $when = new \DateTimeImmutable('2024-05-01 12:30:00', new \DateTimeZone('UTC'));
        $uid = Uuid::random();
        $doc = ['name' => 'Ann', 'age' => 37, 'score' => 9.5, 'price' => Decimal::of('19.99'), 'joined' => $when,
            'id' => $uid, 'raw' => new Bytes("\x00\xff"), 'tags' => ['a', 'b'], 'address' => ['city' => 'Lagos'],
            'nothing' => null, 'ok' => true];
        $v1 = $c->insert('ann', $doc);
        $got = $c->get('ann');
        same($v1, $got->version);
        same('19.99', (string) $got['price']);
        same($when->format('c'), $got['joined']->format('c'));
        same((string) $uid, (string) $got['id']);
        same("\x00\xff", $got['raw']->data);
        same(['a', 'b'], $got['tags']);
        same(['city' => 'Lagos'], $got['address']);
        check(array_key_exists('nothing', $got->toArray()) && $got['nothing'] === null, 'null field kept');
        throws(ConflictException::class, fn () => $c->insert('ann', $doc), 'insert twice');
        $v2 = $c->update('ann', FieldOp::set('address.city', 'Abuja'), FieldOp::unset('nothing'), FieldOp::append('tags', 'c'));
        check($v2 > $v1, 'version grows');
        $got = $c->get('ann');
        same(['city' => 'Abuja'], $got['address']);
        check(!isset($got['nothing']), 'unset');
        same(['a', 'b', 'c'], $got['tags']);
        throws(ConflictException::class, fn () => $c->replaceIfVersion('ann', $v1, ['name' => 'x']), 'stale replace');
        $c->replaceIfVersion('ann', $v2, ['name' => 'Ann B']);
        same(['name' => 'Ann B'], $c->get('ann')->toArray());
        throws(NotFoundException::class, fn () => $c->update('nobody', FieldOp::set('a', 1)), 'update missing');
        check($c->delete('ann'), 'delete existing');
        check(!$c->delete('ann'), 'delete missing');
        check($c->get('ann') === null, 'gone');
    });
}

function testMoneyIsExactAndNeverLost(): void
{
    with_server(function (Storine $db, int $port) {
        $acct = $db->collection('accounts');
        $acct->put('a', ['balance' => Decimal::of('0.00')]);
        $db->close();
        // 8 processes x 100 concurrent deposits (PHP has no threads).
        $pids = [];
        for ($i = 0; $i < 8; $i++) {
            $pid = pcntl_fork();
            if ($pid === 0) {
                $c = Storine::connect("storine://127.0.0.1:$port")->collection('accounts');
                for ($j = 0; $j < 100; $j++) {
                    $c->update('a', FieldOp::add('balance', Decimal::of('0.01')));
                }
                exit(0);
            }
            $pids[] = $pid;
        }
        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
            same(0, pcntl_wexitstatus($status), 'worker exit status');
        }
        same('8.00', (string) $acct->get('a')['balance']);
        throws(InvalidException::class, fn () => $acct->update('a', FieldOp::add('balance', Decimal::of('-8.01'))->min(0)), 'overdraft');
        same('8.00', (string) $acct->get('a')['balance']);
    });
}

function testTransactionIsAtomic(): void
{
    with_server(function (Storine $db) {
        $acct = $db->collection('bank');
        $acct->put('alice', ['balance' => Decimal::of('50')]);
        $acct->put('bob', ['balance' => Decimal::of('0')]);
        $db->transaction(function (Transaction $tx) {
            $tx->update('bank', 'alice', FieldOp::add('balance', Decimal::of('-20'))->min(0));
            $tx->update('bank', 'bob', FieldOp::add('balance', Decimal::of('20')));
        });
        throws(InvalidException::class, fn () => $db->transaction(function (Transaction $tx) {
            $tx->update('bank', 'alice', FieldOp::add('balance', Decimal::of('-31'))->min(0));
            $tx->update('bank', 'bob', FieldOp::add('balance', Decimal::of('31')));
        }), 'overdraft in a transaction');
        same('30', (string) $acct->get('alice')['balance']);
        same('20', (string) $acct->get('bob')['balance']);
    });
}

function testQueriesIndexesAndPagination(): void
{
    with_server(function (Storine $db) {
        $items = $db->collection('items');
        for ($i = 0; $i < 120; $i++) {
            $items->put(sprintf('i%03d', $i), ['n' => $i, 'kind' => $i % 2 === 0 ? 'even' : 'odd', 'price' => Decimal::of($i)->mul('0.25')]);
        }
        $q = Filter::and(Filter::between('n', 10, 19), Filter::eq('kind', 'odd'));
        $keys = fn (array $docs) => array_map(fn ($d) => $d->key, $docs);
        $before = $keys($items->find($q, '-n'));
        same(['i019', 'i017', 'i015', 'i013', 'i011'], $before);
        $items->createIndex('n');
        $items->createIndex('kind');
        same($before, $keys($items->find($q, '-n')));
        check(in_array('n', $items->info()['indexes'], true), 'index listed');
        same(19, $items->count(Filter::gt('price', Decimal::of('25'))));
        same(60, $items->count(['kind' => 'even']));
        same(120, iterator_count($items->scan(batch: 7)));
        same('i119', $items->first(orderBy: '-price')->key);
        check(in_array('items', $db->collections(), true), 'collections listed');
    });
}

function testVectorSearch(): void
{
    with_server(function (Storine $db) {
        $c = $db->collection('php_docs');
        $c->createVectorIndex('emb', dims: 3);
        same([['field' => 'emb', 'dims' => 3, 'ready' => true]], $c->vectorIndexes());
        foreach ([['a', [1, 0, 0], 'x'], ['b', [0, 1, 0], 'y'], ['c', [0.7, 0.7, 0], 'x']] as [$key, $vec, $kind]) {
            $c->put($key, ['emb' => $vec, 'kind' => $kind]);
        }
        $keys = fn (array $docs) => array_map(fn ($d) => $d->key, $docs);
        $hits = $c->nearest('emb', [1, 0.1, 0], k: 2);
        same(['a', 'c'], $keys($hits));
        check($hits[0]->score() > $hits[1]->score(), 'scores descending');
        same(['a', 'c'], $keys($c->nearest('emb', [1, 0.1, 0], k: 5, where: ['kind' => 'x'], exact: true)));
        throws(InvalidException::class, fn () => $c->put('bad', ['emb' => [1, 2]]), 'wrong dimensions on write');
        throws(InvalidException::class, fn () => $c->nearest('emb', [1, 2], k: 1), 'wrong dimensions on query');
        $c->dropVectorIndex('emb');
        same([], $c->vectorIndexes());
    });
}

function testLargeValues(): void
{
    with_server(function (Storine $db) {
        $c = $db->collection('php_files');
        $blob = random_bytes(3 * 1024 * 1024 + 17);
        $v = $c->put('f1', ['data' => new Bytes($blob), 'name' => 'cat.png', 'meta' => ['size' => strlen($blob), 'type' => 'image/png']]);
        $d = $c->get('f1', fields: ['name', 'meta.size', 'absent']);
        same([['meta' => ['size' => strlen($blob)], 'name' => 'cat.png'], $v], [$d->toArray(), $d->version]);
        same(null, $c->get('nope', fields: ['name']));
        $r = $c->readBytes('f1', 'data', 1000, 16);
        same([$v, strlen($blob), substr($blob, 1000, 16)], [$r->version, $r->total, $r->data]);
        same('', $c->readBytes('f1', 'data', strlen($blob) + 5, 10)->data);
        $chunks = iterator_to_array($c->streamBytes('f1', 'data', chunkSize: 1 << 20), false);
        same(4, count($chunks));
        check(implode('', $chunks) === $blob, 'stream reassembles the value');
        same('cat', $c->readBytes('f1', 'name', 0, 3)->data);
        same(null, $c->readBytes('nope', 'data'));
        throws(NotFoundException::class, fn () => iterator_to_array($c->streamBytes('nope', 'data')), 'stream of a missing document');
        throws(InvalidException::class, fn () => $c->readBytes('f1', 'meta'), 'not bytes');
        throws(InvalidException::class, fn () => $c->readBytes('f1', 'data', 0, 17 * 1024 * 1024), 'over 16 MiB');
        throws(InvalidException::class, fn () => $c->readBytes('f1', 'data', -1, 10), 'negative offset');
        $stream = $c->streamBytes('f1', 'data', chunkSize: 1 << 20);
        $stream->current();
        $c->update('f1', FieldOp::set('name', 'dog.png'));
        throws(ConflictException::class, fn () => $stream->next(), 'document changed mid-stream');
    });
}

function testTtlAndErrors(): void
{
    with_server(function (Storine $db) {
        $s = $db->collection('sessions');
        $s->put('short', ['u' => 1], ttl: 0.2);
        check($s->get('short') !== null, 'present before expiry');
        usleep(400_000);
        check($s->get('short') === null, 'expired');
        throws(InvalidException::class, fn () => $s->put('bad', ['a.b' => 1]), 'dotted field name');
        throws(InvalidException::class, fn () => $db->collection('bad/name')->put('k', ['x' => 1]), 'bad collection name');
    });
}

function testAnalytics(): void
{
    with_server(function (Storine $db) {
        $sales = $db->collection('php_sales');
        $regions = ['eu', 'us', 'apac'];
        $data = []; // key => [region, qty, price in cents, day]
        $insert = function (int $from, int $to) use ($db, $regions, &$data) {
            for ($start = $from; $start < $to; $start += 100) {
                $db->transaction(function (Transaction $tx) use ($start, $to, $regions, &$data) {
                    for ($i = $start; $i < min($start + 100, $to); $i++) {
                        $row = [$regions[$i % 3], ($i * 7) % 50 + 1, ($i * 37) % 10000 + 1, $i % 60];
                        $key = sprintf('s%04d', $i);
                        $data[$key] = $row;
                        $tx->insert('php_sales', $key, ['region' => $row[0], 'qty' => $row[1], 'price' => Decimal::fromParts($row[2], 2), 'day' => $row[3]]);
                    }
                });
            }
        };
        $insert(0, 3000);
        $cents = fn (int $c) => Decimal::fromParts($c, 2);
        $halfEven = function (int $num, int $den): int {
            $q = intdiv($num, $den);
            $twice = 2 * ($num % $den);
            return $twice > $den || ($twice === $den && $q % 2 !== 0) ? $q + 1 : $q;
        };
        $isDec = function (mixed $v, Decimal $expected, string $what) {
            check($v instanceof Decimal, "$what is a Decimal");
            check($v->equals($expected), "$what: expected $expected, got $v");
        };
        $aggs = ['n' => Agg::count(), 'revenue' => Agg::sum(Expr::field('price')->times('qty')), 'avg_qty' => Agg::avg('qty'),
            'min_price' => Agg::min('price'), 'max_price' => Agg::max('price'), 'days' => Agg::countDistinct('day')];

        $checkGrouped = function () use ($sales, $aggs, &$data, $cents, $halfEven, $isDec) {
            $g = [];
            foreach ($data as [$region, $qty, $price, $day]) {
                $x = $g[$region] ?? ['n' => 0, 'revenue' => 0, 'qty' => 0, 'min' => $price, 'max' => $price, 'days' => []];
                $x['n']++;
                $x['revenue'] += $price * $qty;
                $x['qty'] += $qty;
                $x['min'] = min($x['min'], $price);
                $x['max'] = max($x['max'], $price);
                $x['days'][$day] = true;
                $g[$region] = $x;
            }
            uasort($g, fn ($a, $b) => $b['revenue'] <=> $a['revenue']);
            $expected = array_slice($g, 0, 2, true);
            $res = $sales->aggregate($aggs, groupBy: 'region', orderBy: '-revenue', limit: 2);
            same(count($data), $res->matched, 'matched');
            same(['region', 'n', 'revenue', 'avg_qty', 'min_price', 'max_price', 'days'], $res->columns, 'columns');
            same(array_keys($expected), array_column($res->rows, 'region'), 'regions by revenue');
            foreach ($res->rows as $i => $row) {
                $x = $expected[$row['region']];
                same($res->columns, array_keys($row), 'row columns');
                same($x['n'], $row['n'], 'count');
                $isDec($row['revenue'], $cents($x['revenue']), 'revenue');
                same((string) Decimal::fromParts($halfEven($x['qty'] * 1_000_000, $x['n']), 6), (string) $row['avg_qty'], 'avg');
                $isDec($row['min_price'], $cents($x['min']), 'min');
                $isDec($row['max_price'], $cents($x['max']), 'max');
                same(count($x['days']), $row['days'], 'count distinct');
            }
            return $res;
        };
        $checkBuckets = function () use ($sales, &$data) {
            $w = [];
            foreach ($data as [, $qty, , $day]) {
                $b = intdiv($day, 7) * 7;
                $w[$b] ??= ['day' => $b, 'n' => 0, 'qty' => 0];
                $w[$b]['n']++;
                $w[$b]['qty'] += $qty;
            }
            ksort($w);
            same(array_values($w), $sales->aggregate(['n' => Agg::count(), 'qty' => Agg::sum('qty')], groupBy: Agg::bucket('day', 7), orderBy: 'day')->rows, 'weekly buckets');
        };
        $checkFiltered = function () use ($sales, &$data) {
            $sel = array_filter($data, fn ($r) => $r[0] === 'us' && $r[3] >= 30);
            $qty = array_sum(array_column($sel, 1));
            $res = $sales->aggregate(['n' => Agg::count(), 'qty' => Agg::sum('qty'), 'qty_minus_1' => Agg::sum(Expr::field('qty')->minus(1))],
                where: Filter::and(Filter::eq('region', 'us'), Filter::ge('day', 30)));
            same([['n' => count($sel), 'qty' => $qty, 'qty_minus_1' => $qty - count($sel)]], $res->rows, 'filtered');
            same(count($sel), $res->matched, 'filtered matched');
        };

        $checkGrouped();
        $sales->enableColumnar();
        $checkGrouped();
        $checkBuckets();
        $checkFiltered();
        $insert(3000, 3030); // not-yet-merged writes are included too
        same(0, $checkGrouped()->spilledBytes, 'no spill');
        $checkBuckets();
        $checkFiltered();

        $eu = array_filter($data, fn ($r) => $r[0] === 'eu');
        ksort($eu, SORT_STRING);
        check(count($eu) > 1000, 'several pages');
        $got = [];
        foreach ($sales->scanColumns(['qty', 'price'], where: Filter::eq('region', 'eu'), pageSize: 500) as $key => $fields) {
            $got[] = [$key, $fields];
        }
        same(array_keys($eu), array_column($got, 0), 'eu keys in key order');
        foreach ($got as [$key, $fields]) {
            same(['qty', 'price'], array_keys($fields), 'scan fields');
            same($eu[$key][1], $fields['qty'], "qty of $key");
            $isDec($fields['price'], $cents($eu[$key][2]), "price of $key");
        }
        $missing = iterator_to_array($sales->scanColumns(['nope'], where: ['region' => 'apac', 'day' => 2]));
        check($missing !== [] && array_values(array_unique($missing, SORT_REGULAR)) === [['nope' => null]], 'missing field reads as null');

        throws(InvalidException::class, fn () => $sales->aggregate(['n' => Agg::count()], orderBy: 'missing'), 'unknown order column');

        $sales->disableColumnar();
        $checkGrouped();
        $checkFiltered();
    });
}

function testThreeNodesRedirectAndFailover(): void
{
    if (!is_executable(server_bin())) {
        throw new Skip('storage-server binary not built');
    }
    $base = sys_get_temp_dir() . '/storine_php_cluster_' . bin2hex(random_bytes(4));
    $ports = [free_port(), free_port(), free_port()];
    $peers = implode(',', array_map(fn ($i) => ($i + 1) . "=127.0.0.1:{$ports[$i]}", array_keys($ports)));
    $procs = [];
    foreach ($ports as $i => $p) {
        $procs[] = proc_open([server_bin(), "$base/n" . ($i + 1), "127.0.0.1:$p", '--node-id', (string) ($i + 1), '--peers', $peers],
            [0 => ['file', '/dev/null', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']], $pipes);
    }
    try {
        foreach ($ports as $p) {
            wait_port($p);
        }
        // Any seed works: the client follows redirects to the leader.
        $db = Storine::connect('storine://' . implode(',', array_map(fn ($p) => "127.0.0.1:$p", $ports)), timeout: 2.0);
        $acct = $db->collection('acct');
        $deadline = microtime(true) + 15;
        while (true) {
            try {
                $acct->put('a', ['bal' => Decimal::of('100')]);
                break;
            } catch (UnavailableException $e) {
                if (microtime(true) > $deadline) {
                    throw $e;
                }
                usleep(100_000);
            }
        }
        for ($i = 0; $i < 10; $i++) {
            $acct->update('a', FieldOp::add('bal', Decimal::of('1')));
        }
        same('110', (string) $acct->get('a')['bal']);
        $leader = $db->clusterInfo()['nodeId'];
        stop_server($procs[$leader - 1], 9);
        // The client fails over to the new leader; every deposit applies exactly once.
        for ($i = 0; $i < 5; $i++) {
            $acct->update('a', FieldOp::add('bal', Decimal::of('1')));
        }
        same('115', (string) $acct->get('a')['bal']);
        $db->close();
    } finally {
        foreach ($procs as $p) {
            stop_server($p);
        }
        rm_tree($base);
    }
}
