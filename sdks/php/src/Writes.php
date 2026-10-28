<?php

declare(strict_types=1);

namespace Storine;

/** Shared write builders of Collection and Transaction. @internal */
trait Writes
{
    private static function putWrite(string $collection, string $key, array|\stdClass $fields, ?float $ttl, int $pre, int $version = 0): Write
    {
        $ttlMs = self::ttlMs($ttl);
        return new Write($collection, $key, $ttlMs > 0 ? 2 : 1, $pre, $version, $fields, $ttlMs);
    }

    private static function ttlMs(?float $ttl): int
    {
        if ($ttl === null) {
            return 0;
        }
        if ($ttl <= 0) {
            throw new InvalidException('ttl must be positive (seconds)');
        }
        return max(1, (int) ($ttl * 1000));
    }

    /** @param list<FieldOp|array> $changes @return list<FieldOp> */
    private static function ops(array $changes): array
    {
        $ops = [];
        foreach ($changes as $c) {
            if ($c instanceof FieldOp) {
                $ops[] = $c;
            } else {
                foreach ($c as $k => $v) {
                    $ops[] = FieldOp::set((string) $k, $v);
                }
            }
        }
        if ($ops === []) {
            throw new InvalidException('update needs at least one change');
        }
        return $ops;
    }
}
