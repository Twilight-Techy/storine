<?php

declare(strict_types=1);

// TLS on the native port, with certificates from `storage-server gen-ca` / `gen-cert`.

require_once __DIR__ . '/E2ETest.php';

use Storine\Storine;

function testTlsRoundTripAndUntrustedServer(): void
{
    if (!is_executable(server_bin())) {
        throw new Skip('storage-server binary not built');
    }
    $d = sys_get_temp_dir() . '/storine_php_tls_' . bin2hex(random_bytes(4));
    try {
        foreach ([['gen-ca', "$d/ca"], ['gen-cert', "$d/ca", "$d/node", '127.0.0.1,localhost'], ['gen-ca', "$d/other"]] as $args) {
            exec(escapeshellarg(server_bin()) . ' ' . implode(' ', array_map('escapeshellarg', $args)) . ' > /dev/null', $out, $rc);
            same(0, $rc, implode(' ', $args));
        }
        $port = free_port();
        $env = getenv();
        $env['STORAGE_TLS_CERT'] = "$d/node/node.crt";
        $env['STORAGE_TLS_KEY'] = "$d/node/node.key";
        $p = proc_open([server_bin(), "$d/data", "127.0.0.1:$port"],
            [0 => ['file', '/dev/null', 'r'], 1 => ['file', '/dev/null', 'w'], 2 => ['file', '/dev/null', 'w']], $pipes, null, $env);
        try {
            wait_port($port);
            $big = str_repeat('x', 100_000);
            $db = Storine::connect("storines://127.0.0.1:$port", tlsCa: "$d/ca/ca.crt");
            $db->collection('t')->put('k', ['v' => $big]);
            same($big, $db->collection('t')->get('k')['v'], 'read back over TLS');
            $named = Storine::connect("storines://127.0.0.1:$port?tls_ca=$d/ca/ca.crt&tls_server_name=localhost");
            check($named->collection('t')->get('k') !== null, 'named server');
            $other = Storine::connect("storines://127.0.0.1:$port", timeout: 1.0, tlsCa: "$d/other/ca.crt");
            throws(\Throwable::class, fn () => $other->collection('t')->get('k'), 'untrusted CA');
        } finally {
            stop_server($p);
        }
    } finally {
        rm_tree($d);
    }
}
