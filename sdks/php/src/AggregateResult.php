<?php

declare(strict_types=1);

namespace Storine;

/** The result of `Collection::aggregate`. */
final class AggregateResult
{
    /**
     * @param list<array<string, mixed>> $rows one per group: group-key columns (named by field path), then the aggregates in order
     * @param list<string> $columns column names, in order
     */
    public function __construct(
        public readonly array $rows,
        public readonly array $columns,
        /** Rows that matched the filter. */
        public readonly int $matched,
        /** Row groups skipped by zone maps without being read (columnar collections). */
        public readonly int $prunedRowGroups,
        /** Bytes spilled to disk (> 0: the query outgrew its memory budget). */
        public readonly int $spilledBytes,
    ) {
    }
}
