<?php

declare(strict_types=1);

namespace Storine;

/**
 * A slice of a bytes (or text) field, returned by `Collection::readBytes`: `$data` starts at the
 * requested offset (empty past the end of the value), `$total` is the whole value's length and
 * `$version` the version of the document it was read from.
 */
final class ByteRange
{
    public function __construct(public readonly int $version, public readonly int $total, public readonly string $data)
    {
    }
}
