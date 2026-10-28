<?php

declare(strict_types=1);

namespace Storine;

/**
 * A document: its fields (read like an array: `$doc['balance']`), plus `key` and `version`.
 * @implements \ArrayAccess<string, mixed>
 * @implements \IteratorAggregate<string, mixed>
 */
final class Document implements \ArrayAccess, \IteratorAggregate, \Countable, \JsonSerializable
{
    /** @param array<string, mixed> $fields */
    public function __construct(public readonly string $key, public readonly int $version, private array $fields)
    {
    }

    /** Similarity of a `nearest` result (null for other queries). */
    public function score(): ?float
    {
        return $this->fields['$score'] ?? null;
    }

    /** @return array<string, mixed> */
    public function toArray(): array
    {
        return $this->fields;
    }

    public function offsetExists(mixed $offset): bool
    {
        return array_key_exists($offset, $this->fields);
    }

    public function offsetGet(mixed $offset): mixed
    {
        return $this->fields[$offset] ?? null;
    }

    public function offsetSet(mixed $offset, mixed $value): void
    {
        throw new \LogicException('a Document is read-only: build a new array of fields and put / replace it');
    }

    public function offsetUnset(mixed $offset): void
    {
        throw new \LogicException('a Document is read-only');
    }

    public function getIterator(): \ArrayIterator
    {
        return new \ArrayIterator($this->fields);
    }

    public function count(): int
    {
        return count($this->fields);
    }

    public function jsonSerialize(): array
    {
        return $this->fields;
    }
}
