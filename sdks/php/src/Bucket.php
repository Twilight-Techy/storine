<?php

declare(strict_types=1);

namespace Storine;

/** A bucketed group key (build with `Agg::bucket($field, $width, $offset)`). */
final class Bucket
{
    public function __construct(public readonly string $field, public readonly int $width, public readonly int $offset = 0)
    {
        if ($width <= 0) {
            throw new InvalidException('bucket width must be positive');
        }
    }
}
