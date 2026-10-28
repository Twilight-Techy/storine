<?php

declare(strict_types=1);

namespace Storine;

/** A signed 128-bit integer beyond PHP's 64-bit ints (decimal text). Smaller values decode as int. */
final class Int128 implements \Stringable
{
    public readonly string $value;

    public function __construct(int|string $value)
    {
        [$negative, $digits] = BigInt::split((string) $value);
        $this->value = ($negative ? '-' : '') . $digits;
        BigInt::toBytes($this->value, 16); // range check
    }

    public function __toString(): string
    {
        return $this->value;
    }
}
