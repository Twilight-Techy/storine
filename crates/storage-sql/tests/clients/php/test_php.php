<?php
// PHP clients against storine-sql: PDO (pgsql and mysql, emulated and native prepares),
// the pgsql extension and mysqli.
//
//   storine-sql --data /tmp/db --pg 127.0.0.1:55432 --mysql 127.0.0.1:53306 &
//   php test_php.php [pg_port] [mysql_port]

declare(strict_types=1);

if (($argv[1] ?? '') === 'worker') {
    $c = new PDO($argv[2], $argv[3], 'x', [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    for ($j = 0; $j < 25; $j++) {
        $c->exec("UPDATE php_items SET qty = qty + 1 WHERE sku = 'C3'");
    }
    exit(0);
}

$pgPort = (int)($argv[1] ?? 55432);
$myPort = (int)($argv[2] ?? 53306);

function check(bool $ok, string $what): void
{
    if (!$ok) {
        fwrite(STDERR, "FAILED: $what\n");
        exit(1);
    }
}

function section(string $name, callable $fn): void
{
    $fn();
    echo "OK  $name\n";
}

function pdoSuite(PDO $db, string $label, bool $mysql): void
{
    section("PDO $label", function () use ($db, $mysql) {
        $db->exec('DROP TABLE IF EXISTS php_items');
        $db->exec($mysql
            ? 'CREATE TABLE php_items (id INT AUTO_INCREMENT PRIMARY KEY, sku VARCHAR(20) NOT NULL UNIQUE, price DECIMAL(10,2), qty INT DEFAULT 0, made DATETIME)'
            : 'CREATE TABLE php_items (id SERIAL PRIMARY KEY, sku VARCHAR(20) NOT NULL UNIQUE, price NUMERIC(10,2), qty INT DEFAULT 0, made TIMESTAMP)');
        $ins = $db->prepare('INSERT INTO php_items (sku, price, made) VALUES (:sku, :price, :made)');
        foreach ([['A1', '9.99'], ['B2', '1.25'], ['C3', '2.00']] as [$sku, $price]) {
            $ins->execute([':sku' => $sku, ':price' => $price, ':made' => '2024-01-02 03:04:05']);
        }
        check($db->lastInsertId($mysql ? null : 'php_items_id_seq') === '3', 'lastInsertId');
        $q = $db->prepare('SELECT sku, price, qty, made FROM php_items WHERE price > ? ORDER BY id');
        $q->execute([1]);
        $rows = $q->fetchAll(PDO::FETCH_ASSOC);
        check(count($rows) === 3, 'row count');
        check($rows[0]['sku'] === 'A1' && $rows[0]['price'] === '9.99', 'decimal as exact string: ' . var_export($rows[0], true));
        check(str_starts_with((string)$rows[0]['made'], '2024-01-02 03:04:05'), 'datetime');
        $db->beginTransaction();
        $db->exec("UPDATE php_items SET qty = 50 WHERE sku = 'A1'");
        $db->rollBack();
        check($db->query("SELECT qty FROM php_items WHERE sku = 'A1'")->fetchColumn() == 0, 'rollback');
        $db->beginTransaction();
        $db->exec("UPDATE php_items SET qty = qty + 7 WHERE sku = 'B2'");
        $db->commit();
        check($db->query("SELECT qty FROM php_items WHERE sku = 'B2'")->fetchColumn() == 7, 'commit');
        try {
            $db->exec("INSERT INTO php_items (sku) VALUES ('A1')");
            check(false, 'duplicate accepted');
        } catch (PDOException $e) {
            check($e->getCode() === '23000' || $e->getCode() === '23505', 'duplicate SQLSTATE ' . $e->getCode());
        }
        $n = $db->exec('UPDATE php_items SET qty = qty + 1');
        check($n === 3, "affected rows $n");
        // Concurrent increments from separate processes: none lost.
        $procs = [];
        for ($i = 0; $i < 4; $i++) {
            $dsn = $GLOBALS['dsn_' . ($mysql ? 'my' : 'pg')];
            $procs[] = proc_open([PHP_BINARY, __FILE__, 'worker', $dsn, $mysql ? 'root' : 'app'], [], $pipes);
        }
        foreach ($procs as $p) {
            check(proc_close($p) === 0, 'worker exit');
        }
        check($db->query("SELECT qty FROM php_items WHERE sku = 'C3'")->fetchColumn() == 101, 'concurrent increments');
    });
}

$GLOBALS['dsn_pg'] = "pgsql:host=127.0.0.1;port=$pgPort;dbname=shop";
$GLOBALS['dsn_my'] = "mysql:host=127.0.0.1;port=$myPort;dbname=shop;charset=utf8mb4";
$opts = [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION];

pdoSuite(new PDO($GLOBALS['dsn_pg'], 'app', 'x', $opts), 'pgsql (native prepares)', false);
pdoSuite(new PDO($GLOBALS['dsn_pg'], 'app', 'x', $opts + [PDO::ATTR_EMULATE_PREPARES => true]), 'pgsql (emulated prepares)', false);
pdoSuite(new PDO($GLOBALS['dsn_my'], 'root', '', $opts), 'mysql (emulated prepares)', true);
pdoSuite(new PDO($GLOBALS['dsn_my'], 'root', '', $opts + [PDO::ATTR_EMULATE_PREPARES => false]), 'mysql (native prepares)', true);

section('pgsql extension', function () use ($pgPort) {
    $c = pg_connect("host=127.0.0.1 port=$pgPort dbname=shop user=app password=x");
    check($c !== false, 'connect');
    $r = pg_query_params($c, 'SELECT sku, price FROM php_items WHERE sku = $1', ['A1']);
    check(pg_fetch_row($r) === ['A1', '9.99'], 'pg_query_params');
    check(@pg_query($c, 'SELECT nosuchcol FROM php_items') === false, 'error result');
    pg_close($c);
});

section('mysqli', function () use ($myPort) {
    mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT);
    $m = new mysqli('127.0.0.1', 'root', '', 'shop', $myPort);
    $st = $m->prepare('SELECT sku, price, qty FROM php_items WHERE sku = ?');
    $sku = 'B2';
    $st->bind_param('s', $sku);
    $st->execute();
    $row = $st->get_result()->fetch_assoc();
    check($row['sku'] === 'B2' && $row['price'] === '1.25' && $row['qty'] === 8, 'mysqli prepared: ' . var_export($row, true));
    $m->autocommit(false);
    $m->query("UPDATE php_items SET qty = 0 WHERE sku = 'B2'");
    $m->rollback();
    $m->autocommit(true);
    check((int)$m->query("SELECT qty FROM php_items WHERE sku = 'B2'")->fetch_row()[0] === 8, 'mysqli rollback');
    $m->close();
});

echo "all php client tests passed\n";
