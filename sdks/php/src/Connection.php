<?php

declare(strict_types=1);

namespace Storine;

/** One TCP connection speaking the binary protocol. @internal */
final class Connection
{
    private const MAX_BODY = 256 * 1024 * 1024;

    /** @var resource */
    private $sock;

    /** @param array{0: string, 1: int} $address */
    public function __construct(public readonly array $address, private readonly float $timeout, ?string $token, bool $followerReads)
    {
        [$host, $port] = $address;
        $target = 'tcp://' . (str_contains($host, ':') ? "[$host]" : $host) . ":$port";
        $ctx = stream_context_create(['socket' => ['tcp_nodelay' => true]]);
        $sock = @stream_socket_client($target, $errno, $errstr, $timeout, STREAM_CLIENT_CONNECT, $ctx);
        if ($sock === false) {
            throw new TransportException("cannot connect to $host:$port: $errstr");
        }
        $this->sock = $sock;
        stream_set_timeout($sock, (int) $timeout, (int) (fmod($timeout, 1.0) * 1_000_000));
        if ($token !== null) {
            [$status, , $body] = $this->call(Wire::OP_AUTH, $token);
            if ($status !== Wire::ST_OK) {
                $this->close();
                throw new UnauthorizedException("authentication failed: $body", $status);
            }
        }
        if ($followerReads) {
            $this->call(Wire::OP_SET_OPTIONS, "\x01");
        }
    }

    /** @return array{0: int, 1: int, 2: string} status, version, body */
    public function call(int $op, string $body): array
    {
        $this->writeAll(chr($op) . str_repeat("\x00", 24) . pack('V', strlen($body)) . $body);
        $h = unpack('Cstatus/Pversion/Vlen', $this->read(13));
        if ($h['len'] > self::MAX_BODY) {
            throw new ServerException('response too large');
        }
        return [$h['status'], $h['version'], $this->read($h['len'])];
    }

    public function close(): void
    {
        if (is_resource($this->sock)) {
            fclose($this->sock);
        }
    }

    private function writeAll(string $data): void
    {
        while ($data !== '') {
            $n = @fwrite($this->sock, $data);
            if ($n === false || $n === 0) {
                $this->failIfTimedOut();
                throw new TransportException('connection lost while sending');
            }
            $data = substr($data, $n);
        }
    }

    private function read(int $n): string
    {
        $out = '';
        while (strlen($out) < $n) {
            $chunk = @fread($this->sock, min($n - strlen($out), 1 << 20));
            if ($chunk === false || $chunk === '') {
                $this->failIfTimedOut();
                throw new TransportException('server closed the connection');
            }
            $out .= $chunk;
        }
        return $out;
    }

    private function failIfTimedOut(): void
    {
        if (stream_get_meta_data($this->sock)['timed_out'] ?? false) {
            throw new TransportException("no response within {$this->timeout} s");
        }
    }
}
