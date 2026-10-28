<?php

declare(strict_types=1);

// End-to-end tests against a real storage-server (single node and a 3-node cluster).
// The server binary is $STORINE_SERVER or <repo>/target/release/storage-server
// (build it with `cargo build --release -p storage-server`); without it the tests are skipped.

use Storine\{Bytes, ConflictException, Decimal, FieldOp, Filter, InvalidException, NotFoundException, Storine,
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
