<?php

declare(strict_types=1);

namespace Storine;

/** Request / response encodings of the document API (protocol/PROTOCOL.md §5-§6). @internal */
final class Wire
{
    public const OP_AUTH = 0x0C, OP_DOC_GET = 0x20, OP_DOC_WRITE = 0x21, OP_DOC_QUERY = 0x22, OP_DOC_ADMIN = 0x23,
        OP_DOC_INFO = 0x25, OP_SET_OPTIONS = 0x26, OP_CLUSTER_INFO = 0x27, OP_DOC_AGGREGATE = 0x29,
        OP_DOC_SCAN_COLUMNS = 0x2A;
    public const ST_OK = 0, ST_NOT_FOUND = 1, ST_ERROR = 2, ST_OVERLOADED = 3, ST_CONFLICT = 4, ST_UNAUTHORIZED = 5,
        ST_RATE_LIMITED = 6, ST_NOT_LEADER = 7, ST_INVALID = 8, ST_COMMIT_UNKNOWN = 9;
    /** Most bytes one ranged read returns (16 MiB). */
    public const MAX_BYTE_RANGE = 16 * 1024 * 1024;
    /** Most rows one `scanColumns` page returns. */
    public const MAX_SCAN_PAGE = 100_000;

    public static function encodeGet(string $collection, string $key): string
    {
        $out = '';
        Sve::putStr($out, $collection);
        Sve::putStr($out, $key);
        return $out;
    }

    /** `get` shape 1: `u8 1 | str field | u64 offset | u32 length` (a byte range of one field). */
    public static function encodeGetRange(string $collection, string $key, string $field, int $offset, int $length): string
    {
        if ($offset < 0) {
            throw new InvalidException('offset must not be negative');
        }
        if ($length < 0 || $length > self::MAX_BYTE_RANGE) {
            throw new InvalidException('length must be between 0 and ' . self::MAX_BYTE_RANGE);
        }
        $out = self::encodeGet($collection, $key) . "\x01";
        Sve::putStr($out, $field);
        return $out . pack('PV', $offset, $length);
    }

    /**
     * `get` shape 2: `u8 2 | u32 count | str path*` (only these dotted paths).
     * @param list<string> $paths
     */
    public static function encodeGetFields(string $collection, string $key, array $paths): string
    {
        if (count($paths) > 256) {
            throw new InvalidException('at most 256 fields can be projected');
        }
        $out = self::encodeGet($collection, $key) . "\x02" . pack('V', count($paths));
        foreach ($paths as $p) {
            Sve::putStr($out, $p);
        }
        return $out;
    }

    /** @return array{0: int, 1: string} total, data (`u64 total | u32 n | n bytes`) */
    public static function decodeByteRange(string $body): array
    {
        $r = new Reader($body);
        $total = $r->u64Int();
        $data = $r->take($r->u32());
        if ($r->remaining() !== 0) {
            throw new InvalidException('trailing bytes after byte range');
        }
        return [$total, $data];
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

    /**
     * @param list<array{0: string, 1: bool}> $orderBy
     * @param ?array{0: string, 1: list<float>, 2: int, 3: bool} $nearest field, vector, k, exact
     */
    public static function encodeQuery(string $collection, ?Filter $filter, array $orderBy, int $limit, int $offset, ?string $afterKey, ?array $nearest = null): string
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
        if ($nearest !== null) {
            // Optional trailing block: u8 1 | str field | u32 k | u8 exact | u32 dims | dims x f32 LE.
            [$field, $vector, $k, $exact] = $nearest;
            $out .= "\x01";
            Sve::putStr($out, $field);
            $out .= pack('VCV', $k, $exact ? 1 : 0, count($vector));
            foreach ($vector as $x) {
                $out .= pack('g', (float) $x);
            }
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

    // ── Analytics (§5.5, §5.6) ───────────────────────────────────────────────────────────────

    /**
     * `OP_DOC_AGGREGATE`; also returns the output column names (group keys, then aggregates).
     * @param array<string, Agg> $aggregates output name => aggregate, in output order
     * @param string|Bucket|list<string|Bucket>|null $groupBy
     * @return array{0: string, 1: list<string>}
     */
    public static function encodeAggregate(string $collection, array $aggregates, ?Filter $filter = null, string|Bucket|array|null $groupBy = null,
        string|array|null $orderBy = null, int $limit = 0, int $memoryBudget = 0, int $spillBudget = 0): array
    {
        $keys = $groupBy === null ? [] : (is_array($groupBy) ? array_values($groupBy) : [$groupBy]);
        if (count($keys) > 64 || count($aggregates) > 64) {
            throw new InvalidException('at most 64 group keys and 64 aggregates');
        }
        $columns = [];
        foreach ($keys as $k) {
            if (!is_string($k) && !$k instanceof Bucket) {
                throw new InvalidException('group keys are field paths or Agg::bucket(...)');
            }
            $columns[] = is_string($k) ? $k : $k->field;
        }
        foreach ($aggregates as $name => $agg) {
            if (!$agg instanceof Agg) {
                throw new InvalidException("aggregate '$name' is not built with Agg::count / sum / avg / min / max / countDistinct / approxCountDistinct");
            }
            $columns[] = (string) $name;
        }
        $index = [];
        foreach ($columns as $i => $c) {
            if (isset($index[$c])) {
                throw new InvalidException("duplicate output column '$c'");
            }
            $index[$c] = $i;
        }
        $order = [];
        foreach (self::parseOrderBy($orderBy) as [$name, $desc]) {
            if (!isset($index[$name])) {
                throw new InvalidException("orderBy '$name' is not an output column (" . implode(', ', $columns) . ')');
            }
            $order[] = [$index[$name], $desc];
        }
        if (count($order) > 64) {
            throw new InvalidException('at most 64 order columns');
        }
        if ($limit < 0 || $limit > 0xFFFFFFFF) {
            throw new InvalidException('limit must be a non-negative 32-bit integer');
        }
        if ($memoryBudget < 0 || $spillBudget < 0) {
            throw new InvalidException('budgets must not be negative');
        }
        $out = '';
        Sve::putStr($out, $collection);
        if ($filter === null) {
            $out .= "\x00";
        } else {
            $out .= "\x01";
            $filter->encode($out);
        }
        $out .= pack('V', count($keys));
        foreach ($keys as $k) {
            if (is_string($k)) {
                $out .= "\x01";
                Sve::putStr($out, $k);
            } else {
                $out .= "\x02";
                Sve::putStr($out, $k->field);
                $out .= pack('PP', $k->width, $k->offset);
            }
        }
        $out .= pack('V', count($aggregates));
        foreach ($aggregates as $agg) {
            $agg->encode($out);
        }
        $out .= pack('V', count($order));
        foreach ($order as [$i, $desc]) {
            $out .= pack('VC', $i, $desc ? 1 : 0);
        }
        $out .= pack('VPP', $limit, $memoryBudget, $spillBudget);
        return [$out, $columns];
    }

    /**
     * `u64 matched | u64 pruned | u64 spilled | u32 columns | u32 rows | values`.
     * @return array{matched: int, prunedRowGroups: int, spilledBytes: int, columns: int, rows: list<list<mixed>>}
     */
    public static function decodeAggregateResult(string $body): array
    {
        $r = new Reader($body);
        $matched = $r->u64Int();
        $pruned = $r->u64Int();
        $spilled = $r->u64Int();
        $columns = $r->u32();
        $n = $r->u32();
        if ($columns > 0 && $n * $columns > $r->remaining()) {
            throw new InvalidException('truncated response');
        }
        $rows = [];
        for ($i = 0; $i < $n; $i++) {
            $row = [];
            for ($j = 0; $j < $columns; $j++) {
                $row[] = $r->value();
            }
            $rows[] = $row;
        }
        if ($r->remaining() !== 0) {
            throw new InvalidException('trailing bytes after aggregate result');
        }
        return ['matched' => $matched, 'prunedRowGroups' => $pruned, 'spilledBytes' => $spilled, 'columns' => $columns, 'rows' => $rows];
    }

    /** `OP_DOC_SCAN_COLUMNS` (`$limit` 0 = the server default of 1,000). @param list<string> $fields */
    public static function encodeScanColumns(string $collection, ?Filter $filter, array $fields, ?string $afterKey, int $limit): string
    {
        if (count($fields) > 64) {
            throw new InvalidException('at most 64 fields can be scanned');
        }
        if ($limit < 0 || $limit > self::MAX_SCAN_PAGE) {
            throw new InvalidException('pageSize must be between 1 and ' . self::MAX_SCAN_PAGE);
        }
        $out = '';
        Sve::putStr($out, $collection);
        if ($filter === null) {
            $out .= "\x00";
        } else {
            $out .= "\x01";
            $filter->encode($out);
        }
        $out .= pack('V', count($fields));
        foreach ($fields as $f) {
            Sve::putStr($out, $f);
        }
        if ($afterKey === null) {
            $out .= "\x00";
        } else {
            $out .= "\x01";
            Sve::putStr($out, $afterKey);
        }
        return $out . pack('V', $limit);
    }

    /**
     * `u64 total | u8 has_next | str next | u32 n_fields | u32 rows | (str key | values)*`.
     * @return array{total: int, next: ?string, fields: int, rows: list<array{0: string, 1: list<mixed>}>}
     */
    public static function decodeScanPage(string $body): array
    {
        $r = new Reader($body);
        $total = $r->u64Int();
        $next = $r->u8() !== 0 ? $r->str() : null;
        $fields = $r->u32();
        $n = $r->count(4 + $fields);
        $rows = [];
        for ($i = 0; $i < $n; $i++) {
            $key = $r->str();
            $values = [];
            for ($j = 0; $j < $fields; $j++) {
                $values[] = $r->value();
            }
            $rows[] = [$key, $values];
        }
        if ($r->remaining() !== 0) {
            throw new InvalidException('trailing bytes after scan page');
        }
        return ['total' => $total, 'next' => $next, 'fields' => $fields, 'rows' => $rows];
    }
}
