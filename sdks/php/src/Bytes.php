<?php

declare(strict_types=1);

namespace Storine;

/** Binary data (PHP strings are stored as UTF-8 text; wrap raw bytes in Bytes). */
final class Bytes implements \Stringable
{
    public function __construct(public readonly string $data)
    {
    }

    public function __toString(): string
    {
        return $this->data;
    }
}
