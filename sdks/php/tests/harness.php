<?php

declare(strict_types=1);

// A minimal test harness (no dependencies): functions named test* run in order.

require_once __DIR__ . '/../src/autoload.php';

final class Skip extends \Exception
{
}

function check(bool $ok, string $what): void
{
    $GLOBALS['assertions'] = ($GLOBALS['assertions'] ?? 0) + 1;
    if (!$ok) {
        throw new \AssertionError($what);
    }
}

function same(mixed $expected, mixed $actual, string $what = ''): void
{
    check($expected === $actual, ($what !== '' ? "$what: " : '') . 'expected ' . var_export($expected, true) . ', got ' . var_export($actual, true));
}

function throws(string $class, callable $f, string $what = ''): void
{
    try {
        $f();
    } catch (\Throwable $e) {
        check($e instanceof $class, "$what: expected $class, got " . get_class($e) . ': ' . $e->getMessage());
        return;
    }
    check(false, "$what: expected $class, nothing thrown");
}

/** Runs every test* function defined in `$file`; returns the number of failures. */
function run_tests(string $file): int
{
    $before = get_defined_functions()['user'];
    require $file;
    $tests = array_filter(array_diff(get_defined_functions()['user'], $before), fn ($f) => str_starts_with($f, 'test'));
    $failures = 0;
    foreach ($tests as $t) {
        try {
            $t();
            echo "ok    $t\n";
        } catch (Skip $s) {
            echo "skip  $t: {$s->getMessage()}\n";
        } catch (\Throwable $e) {
            $failures++;
            echo "FAIL  $t: " . get_class($e) . ': ' . $e->getMessage() . "\n      at " . $e->getFile() . ':' . $e->getLine() . "\n";
        }
    }
    return $failures;
}

if (realpath($_SERVER['SCRIPT_FILENAME']) === __FILE__) {
    $files = array_slice($argv, 1) ?: glob(__DIR__ . '/*Test.php');
    $failures = 0;
    foreach ($files as $f) {
        echo "== " . basename($f) . "\n";
        $failures += run_tests($f);
    }
    echo ($failures === 0 ? 'all tests passed' : "$failures failed") . " ({$GLOBALS['assertions']} assertions)\n";
    exit($failures === 0 ? 0 : 1);
}
