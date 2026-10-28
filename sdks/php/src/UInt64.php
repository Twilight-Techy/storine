<?php

declare(strict_types=1);

namespace Storine;

/** An unsigned 64-bit integer (plain ints are signed). Values above PHP_INT_MAX are decoded as UInt64. */
final class UInt64 implements \Stringable
{
    public readonly string $value;

    public function __construct(int|string $value)
    {
        $v = (string) $value;
        if (BigInt::compare($v, '0') < 0 || BigInt::compare($v, '18446744073709551615') > 0) {
            throw new InvalidException('UInt64 out of range');
        }
        $this->value = BigInt::split($v)[1];
    }

    public function __toString(): string
    {
        return $this->value;
    }
}
