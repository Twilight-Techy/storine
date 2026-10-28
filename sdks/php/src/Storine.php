<?php

declare(strict_types=1);

namespace Storine;

/**
 * A connection pool to a Storine server or cluster. Create one with Storine::connect().
 * Writes are retried automatically with the same request id (applied at most once) across
 * timeouts, reconnects and leader changes.
 *
 *     $db = Storine::connect('storine://127.0.0.1:7878');
 *     $accounts = $db->collection('accounts');
 *     $accounts->insert('alice', ['balance' => Decimal::of('100.00')]);
 *     $accounts->update('alice', FieldOp::add('balance', Decimal::of('-25.00'))->min(0));
 */
final class Storine
{
    /** @var list<array{0: string, 1: int}> */
    private array $seeds;
    /** @var array{0: string, 1: int} */
    private array $target;
    /** @var list<Connection> */
    private array $pool = [];

    /** @param list<array{0: string, 1: int}> $seeds */
    public function __construct(array $seeds, private readonly ?string $token = null, private readonly float $timeout = 5.0,
                                private readonly bool $followerReads = false, private readonly int $retries = 8)
    {
        if ($seeds === []) {
            throw new InvalidException('no server address given');
        }
        $this->seeds = array_values($seeds);
        $this->target = $this->seeds[0];
    }

    /**
     * Connects to a Storine server or cluster. `$url` is
     * `storine://[token@]host:port[,host:port...]` (or just `host:port`); options may also be
     * query parameters: `?timeout_ms=5000&follower_reads=1`.
     */
    public static function connect(string $url = 'storine://127.0.0.1:7878', ?string $token = null, float $timeout = 5.0,
                                   bool $followerReads = false): self
    {
        if (!str_contains($url, '://')) {
            $url = 'storine://' . $url;
        }
        [$scheme, $rest] = explode('://', $url, 2);
        if ($scheme !== 'storine') {
            throw new InvalidException('connection strings start with storine://');
        }
        [$netloc, $query] = array_pad(explode('?', $rest, 2), 2, '');
        $netloc = rtrim($netloc, '/');
        $at = strrpos($netloc, '@');
        if ($at !== false) {
            $token ??= rawurldecode(substr($netloc, 0, $at));
            $netloc = substr($netloc, $at + 1);
        }
        $seeds = [];
        foreach (explode(',', $netloc) as $h) {
            if ($h !== '') {
                $seeds[] = self::parseAddress($h);
            }
        }
        parse_str($query, $q);
        if (isset($q['timeout_ms'])) {
            $timeout = ((int) $q['timeout_ms']) / 1000;
        }
        if (isset($q['follower_reads'])) {
            $followerReads = in_array($q['follower_reads'], ['1', 'true', 'yes'], true);
        }
        return new self($seeds, $token, $timeout, $followerReads);
    }

    /** "host:port" (port 7878 when omitted; "[::1]:7878" for IPv6). @return array{0: string, 1: int} */
    public static function parseAddress(string $text): array
    {
        $t = trim($text);
        if (preg_match('/^\[(.*)\](?::(\d+))?$/', $t, $m)) {
            return [$m[1], (int) ($m[2] ?? 7878)];
        }
        $colon = strrpos($t, ':');
        if ($colon === false || substr_count($t, ':') > 1) {
            return [$t, 7878];
        }
        return [substr($t, 0, $colon), (int) substr($t, $colon + 1)];
    }

    /** A handle on the collection `$name` (created on first write). */
    public function collection(string $name): Collection
    {
        return new Collection($this, $name);
    }

    /**
     * An atomic batch of writes. With a callable, commits when it returns and gives the new
     * versions; without, returns the Transaction for ->commit():
     *
     *     $db->transaction(function (Transaction $tx) { $tx->update('accounts', 'a', ...); });
     *
     * @return Transaction|list<?int>
     */
    public function transaction(?callable $body = null): Transaction|array
    {
        $tx = new Transaction($this);
        if ($body === null) {
            return $tx;
        }
        $body($tx);
        return $tx->commit();
    }

    /**
     * Applies `$writes` atomically (exactly once, even across retries); returns new versions.
     * @param list<Write> $writes
     * @return list<?int>
     * @internal
     */
    public function write(array $writes): array
    {
        $body = Wire::encodeWrites($writes, random_bytes(16));
        [$status, , $rbody] = $this->request(Wire::OP_DOC_WRITE, $body, true);
        if ($status === Wire::ST_NOT_FOUND) {
            // Body: u32 index of the write that patched a missing document, then the message.
            $e = new NotFoundException(strlen($rbody) > 4 ? substr($rbody, 4) : 'document not found', $status);
            $e->writeIndex = strlen($rbody) >= 4 ? unpack('V', $rbody)[1] : null;
            throw $e;
        }
        if ($status !== Wire::ST_OK) {
            $this->fail($status, $rbody);
        }
        return Wire::decodeWriteResponse($rbody)[0];
    }

    /** Names of every declared collection. @return list<string> */
    public function collections(): array
    {
        [$status, , $body] = $this->request(Wire::OP_DOC_INFO, "\x03", true);
        if ($status !== Wire::ST_OK) {
            $this->fail($status, $body);
        }
        $r = new Reader($body);
        $out = [];
        for ($i = 0, $n = $r->count(17); $i < $n; $i++) {
            $out[] = Wire::decodeInfo($r)['name'];
        }
        return $out;
    }

    /** Node id, leader, term and commit indexes of the server currently used. */
    public function clusterInfo(): array
    {
        [$status, , $body] = $this->request(Wire::OP_CLUSTER_INFO, '', true);
        if ($status !== Wire::ST_OK) {
            $this->fail($status, $body);
        }
        return Wire::decodeClusterInfo($body);
    }

    /** Closes every pooled connection. */
    public function close(): void
    {
        foreach ($this->pool as $c) {
            $c->close();
        }
        $this->pool = [];
    }

    /**
     * Sends a request, following leader redirects and retrying transient failures.
     * `$idempotent` requests (all reads, and writes that carry a request id) are resent after a
     * connection failure or an unknown commit outcome. Retries continue for up to three times
     * the timeout (enough to ride out a leader election).
     * @return array{0: int, 1: int, 2: string}
     * @internal
     */
    public function request(int $op, string $body, bool $idempotent): array
    {
        $deadline = self::now() + max(3 * $this->timeout, 1.0);
        $delay = 0.02;
        $last = null;
        $failed = [];
        $attempts = 0;
        $backoff = static function () use (&$delay): void {
            usleep((int) ($delay * 1_000_000));
            $delay = min($delay * 2, 0.5);
        };
        while ($attempts < $this->retries || self::now() < $deadline) {
            if (++$attempts > 1000) {
                break;
            }
            try {
                $c = $this->acquire();
            } catch (TransportException $e) {
                $last = $e;
                $failed[] = $this->target;
                $this->redirect(null, $failed);
                $backoff();
                continue;
            }
            try {
                [$status, $version, $rbody] = $c->call($op, $body);
            } catch (TransportException $e) {
                $c->close();
                $last = $e;
                if (!$idempotent) {
                    throw new UnavailableException('connection lost during a request: ' . $e->getMessage());
                }
                $failed[] = $c->address;
                $this->redirect(null, $failed);
                $backoff();
                continue;
            }
            if ($status === Wire::ST_NOT_LEADER) {
                $c->close();
                $hint = $rbody !== '' ? $rbody : null;
                if ($hint !== null && in_array(self::parseAddress($hint), $failed, true)) {
                    $hint = null; // stale hint: the old leader is down
                }
                $this->redirect($hint, $failed);
                $last = new UnavailableException('no cluster leader is available yet');
                if ($hint === null) {
                    $backoff();
                }
                continue;
            }
            if (($status === Wire::ST_COMMIT_UNKNOWN || $status === Wire::ST_OVERLOADED) && $idempotent) {
                $c->close();
                $last = $status === Wire::ST_OVERLOADED
                    ? new OverloadedException($rbody !== '' ? $rbody : 'server overloaded', $status)
                    : new UnavailableException('write outcome unknown and retries exhausted');
                $backoff();
                continue;
            }
            if ($status === Wire::ST_UNAUTHORIZED || $status === Wire::ST_RATE_LIMITED) {
                $c->close(); // the server closed the connection
            } else {
                $this->release($c);
            }
            return [$status, $version, $rbody];
        }
        if ($last instanceof StorineException) {
            throw $last;
        }
        throw new UnavailableException('could not reach any server: ' . ($last?->getMessage() ?? 'no attempt made'));
    }

    /** Throws the exception for a failed response. @internal */
    public function fail(int $status, string $body): never
    {
        switch ($status) {
            case Wire::ST_CONFLICT:
                [$index, $version, $msg] = Wire::decodeConflict($body);
                throw new ConflictException($msg, $index, $version);
            case Wire::ST_NOT_FOUND:
                throw new NotFoundException($body !== '' ? $body : 'document not found', $status);
            case Wire::ST_INVALID:
                throw new InvalidException($body, $status);
            case Wire::ST_UNAUTHORIZED:
                throw new UnauthorizedException($body !== '' ? $body : 'not authorized', $status);
            case Wire::ST_RATE_LIMITED:
                throw new RateLimitedException($body !== '' ? $body : 'rate limited', $status);
            case Wire::ST_OVERLOADED:
                throw new OverloadedException($body !== '' ? $body : 'server overloaded', $status);
            case Wire::ST_COMMIT_UNKNOWN:
                throw new UnavailableException('write outcome unknown; retry the same call', $status);
            default:
                throw new ServerException($body !== '' ? $body : "server error (status $status)", $status);
        }
    }

    private static function now(): float
    {
        return hrtime(true) / 1e9;
    }

    private function acquire(): Connection
    {
        while ($this->pool !== []) {
            $c = array_pop($this->pool);
            if ($c->address === $this->target) {
                return $c;
            }
            $c->close();
        }
        return new Connection($this->target, $this->timeout, $this->token, $this->followerReads);
    }

    private function release(Connection $c): void
    {
        if ($c->address === $this->target && count($this->pool) < 16) {
            $this->pool[] = $c;
            return;
        }
        $c->close();
    }

    /** @param list<array{0: string, 1: int}> $avoid */
    private function redirect(?string $address, array $avoid): void
    {
        if ($address !== null) {
            $this->target = self::parseAddress($address);
            return;
        }
        $others = array_values(array_filter($this->seeds, fn ($s) => $s !== $this->target && !in_array($s, $avoid, true)));
        if ($others === []) {
            $others = array_values(array_filter($this->seeds, fn ($s) => $s !== $this->target)) ?: $this->seeds;
        }
        $this->target = $others[array_rand($others)];
    }
}
