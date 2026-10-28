<?php

declare(strict_types=1);

namespace Storine;

/** A named set of documents. Every method is one atomic, exactly-once operation. */
final class Collection
{
    use Writes;

    public function __construct(private readonly Storine $client, public readonly string $name)
    {
    }

    /** The document `$key`, or null if it does not exist. */
    public function get(string $key): ?Document
    {
        [$status, $version, $body] = $this->client->request(Wire::OP_DOC_GET, Wire::encodeGet($this->name, $key), true);
        if ($status === Wire::ST_NOT_FOUND) {
            return null;
        }
        if ($status !== Wire::ST_OK) {
            $this->client->fail($status, $body);
        }
        return new Document($key, $version, (new Reader($body))->map());
    }

    /** Creates or replaces the document; returns its new version. `$ttl` in seconds. */
    public function put(string $key, array|\stdClass $fields, ?float $ttl = null): int
    {
        return $this->client->write([self::putWrite($this->name, $key, $fields, $ttl, 0)])[0] ?? 0;
    }

    /** Creates the document; throws ConflictException if it already exists. */
    public function insert(string $key, array|\stdClass $fields, ?float $ttl = null): int
    {
        return $this->client->write([self::putWrite($this->name, $key, $fields, $ttl, 1)])[0] ?? 0;
    }

    /** Replaces an existing document; ConflictException if it does not exist. */
    public function replace(string $key, array|\stdClass $fields): int
    {
        return $this->client->write([self::putWrite($this->name, $key, $fields, null, 2)])[0] ?? 0;
    }

    /** Replaces the document only if it is at `$version`; ConflictException otherwise. */
    public function replaceIfVersion(string $key, int $version, array|\stdClass $fields): int
    {
        return $this->client->write([self::putWrite($this->name, $key, $fields, null, 3, $version)])[0] ?? 0;
    }

    /**
     * Applies field changes atomically on the server; returns the new version. Changes are
     * FieldOp::set / unset / add / append or ['field' => value] arrays:
     *
     *     $accounts->update('alice', FieldOp::add('balance', Decimal::of('-25'))->min(0));
     *
     * Throws NotFoundException if the document does not exist.
     */
    public function update(string $key, FieldOp|array ...$changes): int
    {
        return $this->client->write([new Write($this->name, $key, 4, 0, 0, null, 0, self::ops($changes))])[0] ?? 0;
    }

    /** `update` only if the document is at `$version` (ConflictException otherwise). */
    public function updateIfVersion(string $key, int $version, FieldOp|array ...$changes): int
    {
        return $this->client->write([new Write($this->name, $key, 4, 3, $version, null, 0, self::ops($changes))])[0] ?? 0;
    }

    /** Deletes the document; returns false if it did not exist. */
    public function delete(string $key): bool
    {
        try {
            $this->client->write([new Write($this->name, $key, 3, 2)]);
            return true;
        } catch (ConflictException $e) {
            if ($e->currentVersion === null) {
                return false;
            }
            throw $e;
        }
    }

    /** Deletes the document only if it is at `$version` (ConflictException otherwise). */
    public function deleteIfVersion(string $key, int $version): void
    {
        $this->client->write([new Write($this->name, $key, 3, 3, $version)]);
    }

    /**
     * Documents matching `$where` (a Filter or ['field' => value]); at most `$limit` (default
     * 1,000). `$orderBy`: 'price', '-price' (descending) or ['a', '-b'].
     * @return list<Document>
     */
    public function find(Filter|array|null $where = null, string|array|null $orderBy = null, ?int $limit = null, int $offset = 0): array
    {
        return $this->query($where, $orderBy, $limit ?? 0, $offset, null)[2];
    }

    /** The first matching document, or null. */
    public function first(Filter|array|null $where = null, string|array|null $orderBy = null): ?Document
    {
        return $this->find($where, $orderBy, 1)[0] ?? null;
    }

    /** Number of matching documents. */
    public function count(Filter|array|null $where = null): int
    {
        return $this->query($where, null, 1, 0, null)[0];
    }

    /** Every matching document in key order (any collection size). @return \Generator<int, Document> */
    public function scan(Filter|array|null $where = null, int $batch = 1000): \Generator
    {
        $after = null;
        do {
            [, $next, $docs] = $this->query($where, null, $batch, 0, $after);
            yield from $docs;
            $after = $next;
        } while ($next !== null);
    }

    /**
     * The `$k` documents whose `$field` (a list of numbers) is most similar to `$vector` (cosine),
     * best first, among those matching `$where`. Each document's similarity is in `$doc->score()`
     * (also `$doc['$score']`). Uses the field's vector index when it has one (approximate
     * candidates, exact scores); `exact: true` scores every candidate.
     * @param list<int|float> $vector
     * @return list<Document>
     */
    public function nearest(string $field, array $vector, int $k = 10, Filter|array|null $where = null, bool $exact = false): array
    {
        return $this->query($where, null, $k, 0, null, [$field, array_map('floatval', array_values($vector)), $k, $exact])[2];
    }

    /**
     * Indexes a top-level field holding `$dims` numbers for `nearest` (at most 4 per collection);
     * existing documents included. Writes of other values to that field are then refused.
     */
    public function createVectorIndex(string $field, int $dims): void
    {
        $this->admin(Wire::OP_DOC_ADMIN, "\x07", $field, pack('V', $dims));
    }

    public function dropVectorIndex(string $field): void
    {
        $this->admin(Wire::OP_DOC_ADMIN, "\x08", $field);
    }

    /** @return list<array{field: string, dims: int, ready: bool}> */
    public function vectorIndexes(): array
    {
        $r = new Reader($this->admin(Wire::OP_DOC_INFO, "\x09")[1]);
        $result = [];
        for ($i = 0, $n = $r->count(9); $i < $n; $i++) {
            $field = $r->str();
            $dims = $r->u32();
            $result[] = ['field' => $field, 'dims' => $dims, 'ready' => $r->u8() === 1];
        }
        return $result;
    }

    /** Indexes a top-level field (at most 16 per collection); existing documents included. */
    public function createIndex(string $field): void
    {
        $this->admin(Wire::OP_DOC_ADMIN, "\x01", $field);
    }

    public function dropIndex(string $field): void
    {
        $this->admin(Wire::OP_DOC_ADMIN, "\x02", $field);
    }

    /** Deletes every document and the collection's metadata; returns the number deleted. */
    public function drop(): int
    {
        return $this->admin(Wire::OP_DOC_ADMIN, "\x04")[0];
    }

    /** Indexes and metadata of the collection. */
    public function info(): array
    {
        return Wire::decodeInfo(new Reader($this->admin(Wire::OP_DOC_INFO, "\x06")[1]));
    }

    /** @return array{0: int, 1: ?string, 2: list<Document>} */
    private function query(Filter|array|null $where, string|array|null $orderBy, int $limit, int $offset, ?string $after, ?array $nearest = null): array
    {
        $body = Wire::encodeQuery($this->name, Filter::from($where), Wire::parseOrderBy($orderBy), $limit, $offset, $after, $nearest);
        [$status, , $rbody] = $this->client->request(Wire::OP_DOC_QUERY, $body, true);
        if ($status !== Wire::ST_OK) {
            $this->client->fail($status, $rbody);
        }
        [$total, $next, $docs] = Wire::decodePage($rbody);
        return [$total, $next, array_map(static fn ($d) => new Document($d[0], $d[1], $d[2]), $docs)];
    }

    /** @return array{0: int, 1: string} */
    private function admin(int $op, string $code, ?string $field = null, string $tail = ''): array
    {
        $out = $code;
        Sve::putStr($out, $this->name);
        if ($field !== null) {
            Sve::putStr($out, $field);
        }
        $out .= $tail;
        [$status, $version, $rbody] = $this->client->request($op, $out, true);
        if ($status !== Wire::ST_OK) {
            $this->client->fail($status, $rbody);
        }
        return [$version, $rbody];
    }
}
