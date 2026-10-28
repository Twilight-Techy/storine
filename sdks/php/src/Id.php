<?php

declare(strict_types=1);

namespace Storine;

/** An engine id value (24 raw bytes). */
final class Id implements \Stringable
{
    public function __construct(public readonly string $bytes)
    {
        if (strlen($bytes) !== 24) {
            throw new InvalidException('an Id is exactly 24 bytes');
        }
    }

    public function __toString(): string
    {
        return bin2hex($this->bytes);
    }
}
