<?php

declare(strict_types=1);

namespace Storine;

/**
 * One write of an atomic request (built by Collection / Transaction methods).
 * kind: 1 put, 2 put+ttl, 3 delete, 4 patch, 5 check; precondition: 0 none, 1 absent,
 * 2 exists, 3 version.
 * @internal
 */
final class Write
{
    /** @param list<FieldOp> $ops */
    public function __construct(
        public readonly string $collection,
        public readonly string $key,
        public readonly int $kind,
        public readonly int $precondition = 0,
        public readonly int $version = 0,
        public readonly array|\stdClass|null $fields = null,
        public readonly int $ttlMs = 0,
        public readonly array $ops = [],
    ) {
    }
}
