<?php

declare(strict_types=1);

namespace Storine;

/** Request / response encodings of the document API (protocol/PROTOCOL.md §5-§6). @internal */
final class Wire
{
    public const OP_AUTH = 0x0C, OP_DOC_GET = 0x20, OP_DOC_WRITE = 0x21, OP_DOC_QUERY = 0x22, OP_DOC_ADMIN = 0x23,
        OP_DOC_INFO = 0x25, OP_SET_OPTIONS = 0x26, OP_CLUSTER_INFO = 0x27;
    public const ST_OK = 0, ST_NOT_FOUND = 1, ST_ERROR = 2, ST_OVERLOADED = 3, ST_CONFLICT = 4, ST_UNAUTHORIZED = 5,
        ST_RATE_LIMITED = 6, ST_NOT_LEADER = 7, ST_INVALID = 8, ST_COMMIT_UNKNOWN = 9;

    public static function encodeGet(string $collection, string $key): string
    {
        $out = '';
        Sve::putStr($out, $collection);
        Sve::putStr($out, $key);
        return $out;
    }

    /** @param list<Write> $writes */
    public static function encodeWrites(array $writes, ?string $requestId): string
    {
        $out = $requestId === null ? "\x00" : "\x01" . $requestId;
        $out .= pack('V', count($writes));
        foreach ($writes as $w) {
            Sve::putStr($out, $w->collection);
            Sve::putStr($out, $w->key);
            $out .= chr($w->precondition);
            if ($w->precondition === 3) {
                $out .= pack('P', $w->version);
            }
            $out .= chr($w->kind);
            if ($w->kind === 1) {
                Sve::encodeMap($w->fields ?? [], $out);
            } elseif ($w->kind === 2) {
                $out .= pack('P', $w->ttlMs);
                Sve::encodeMap($w->fields ?? [], $out);
            } elseif ($w->kind === 4) {
                $out .= pack('V', count($w->ops));
                foreach ($w->ops as $op) {
                    $op->encode($out);
                }
            }
        }
        return $out;
    }

    /** @param list<array{0: string, 1: bool}> $orderBy */
    public static function encodeQuery(string $collection, ?Filter $filter, array $orderBy, int $limit, int $offset, ?string $afterKey): string
    {
        $out = '';
        Sve::putStr($out, $collection);
        if ($filter === null) {
            $out .= "\x00";
        } else {
            $out .= "\x01";
            $filter->encode($out);
        }
        $out .= pack('V', count($orderBy));
        foreach ($orderBy as [$path, $desc]) {
            Sve::putStr($out, $path);
            $out .= $desc ? "\x01" : "\x00";
        }
        $out .= pack('VV', $limit, $offset);
        if ($afterKey === null) {
            $out .= "\x00";
        } else {
            $out .= "\x01";
            Sve::putStr($out, $afterKey);
        }
        return $out;
    }

    /**
     * 'price' / '-price' (descending) / ['a', '-b'] / ['a' => 'desc'].
     * @return list<array{0: string, 1: bool}>
     */
    public static function parseOrderBy(string|array|null $orderBy): array
    {
        if ($orderBy === null) {
            return [];
        }
        $out = [];
        foreach ((array) $orderBy as $k => $v) {
            if (is_string($k)) {
                $out[] = [$k, in_array(strtolower((string) $v), ['desc', 'descending', '-1'], true)];
            } elseif (is_string($v)) {
                $out[] = str_starts_with($v, '-') ? [substr($v, 1), true] : [$v, false];
            } else {
                throw new InvalidException("orderBy entries are 'field', '-field' or 'field' => 'desc'");
            }
        }
        return $out;
    }

    /** @return array{0: list<?int>, 1: bool} versions (null: deleted) and whether it was a duplicate */
    public static function decodeWriteResponse(string $body): array
    {
        $r = new Reader($body);
        $duplicate = $r->u8() !== 0;
        $n = $r->count(9);
        $versions = [];
        for ($i = 0; $i < $n; $i++) {
            $exists = $r->u8() !== 0;
            $v = $r->u64Int();
            $versions[] = $exists ? $v : null;
        }
        return [$versions, $duplicate];
    }

    /** @return array{0: int, 1: ?string, 2: list<array{0: string, 1: int, 2: array}>} */
    public static function decodePage(string $body): array
    {
        $r = new Reader($body);
        $total = $r->u32();
        $next = $r->u8() !== 0 ? $r->str() : null;
        $n = $r->count(16);
        $docs = [];
        for ($i = 0; $i < $n; $i++) {
            $key = $r->str();
            $version = $r->u64Int();
            $docs[] = [$key, $version, $r->map()];
        }
        return [$total, $next, $docs];
    }

    /** @return array{0: ?int, 1: ?int, 2: string} write index, current version, message */
    public static function decodeConflict(string $body): array
    {
        if (strlen($body) < 13) {
            return [null, null, $body];
        }
        $u = unpack('Vindex/Cexists/Pversion', $body);
        return [$u['index'] === 0xFFFFFFFF ? null : $u['index'], $u['exists'] ? $u['version'] : null, substr($body, 13)];
    }

    /** @return array{name: string, indexes: list<string>, building: list<string>, metadata: array} */
    public static function decodeInfo(Reader $r): array
    {
        $name = $r->str();
        $indexes = [];
        for ($i = 0, $n = $r->count(4); $i < $n; $i++) {
            $indexes[] = $r->str();
        }
        $building = [];
        for ($i = 0, $n = $r->count(4); $i < $n; $i++) {
            $building[] = $r->str();
        }
        return ['name' => $name, 'indexes' => $indexes, 'building' => $building, 'metadata' => $r->map()];
    }

    public static function decodeClusterInfo(string $body): array
    {
        $r = new Reader($body);
        $nodeId = $r->u64Int();
        $isLeader = $r->u8() !== 0;
        $leaderId = $r->u64Int();
        $leaderAddr = $r->str();
        $term = $r->u64Int();
        $commit = $r->u64Int();
        $applied = $r->u64Int();
        $members = [];
        for ($i = 0, $n = $r->count(12); $i < $n; $i++) {
            $id = $r->u64Int();
            $members[$id] = $r->str();
        }
        return ['nodeId' => $nodeId, 'isLeader' => $isLeader, 'leaderId' => $leaderId ?: null,
            'leaderAddr' => $leaderAddr !== '' ? $leaderAddr : null, 'term' => $term, 'commitIndex' => $commit,
            'lastApplied' => $applied, 'members' => $members];
    }
}
