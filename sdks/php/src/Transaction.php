<?php

declare(strict_types=1);

namespace Storine;

/** Writes buffered locally and committed atomically (all or none) by commit(). */
final class Transaction
{
    use Writes;

    /** @var list<Write> */
    private array $writes = [];
    /** @var list<?int> versions of the last commit */
    public array $versions = [];

    public function __construct(private readonly Storine $client)
    {
    }

    public function put(string $collection, string $key, array|\stdClass $fields, ?float $ttl = null): self
    {
        return $this->add(self::putWrite($collection, $key, $fields, $ttl, 0));
    }

    public function insert(string $collection, string $key, array|\stdClass $fields): self
    {
        return $this->add(self::putWrite($collection, $key, $fields, null, 1));
    }

    public function replace(string $collection, string $key, array|\stdClass $fields): self
    {
        return $this->add(self::putWrite($collection, $key, $fields, null, 2));
    }

    public function replaceIfVersion(string $collection, string $key, int $version, array|\stdClass $fields): self
    {
        return $this->add(self::putWrite($collection, $key, $fields, null, 3, $version));
    }

    public function update(string $collection, string $key, FieldOp|array ...$changes): self
    {
        return $this->add(new Write($collection, $key, 4, 0, 0, null, 0, self::ops($changes)));
    }

    public function updateIfVersion(string $collection, string $key, int $version, FieldOp|array ...$changes): self
    {
        return $this->add(new Write($collection, $key, 4, 3, $version, null, 0, self::ops($changes)));
    }

    public function delete(string $collection, string $key): self
    {
        return $this->add(new Write($collection, $key, 3));
    }

    public function deleteIfVersion(string $collection, string $key, int $version): self
    {
        return $this->add(new Write($collection, $key, 3, 3, $version));
    }

    /** Commit only if the document is at `$version`. */
    public function requireVersion(string $collection, string $key, int $version): self
    {
        return $this->add(new Write($collection, $key, 5, 3, $version));
    }

    /** Commit only if the document exists (or, with false, does not exist). */
    public function requireExists(string $collection, string $key, bool $exists = true): self
    {
        return $this->add(new Write($collection, $key, 5, $exists ? 2 : 1));
    }

    /** Applies every buffered write atomically; returns the new versions in order. @return list<?int> */
    public function commit(): array
    {
        if ($this->writes === []) {
            return [];
        }
        $this->versions = $this->client->write($this->writes);
        $this->writes = [];
        return $this->versions;
    }

    private function add(Write $w): self
    {
        $this->writes[] = $w;
        return $this;
    }
}
