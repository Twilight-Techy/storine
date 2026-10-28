<?php

declare(strict_types=1);

namespace Storine;

/** A timestamp with nanosecond precision (returned when a DateTimeImmutable would lose precision). */
final class Timestamp
{
    public function __construct(public readonly int $ns)
    {
    }

    /** The timestamp as a UTC DateTimeImmutable (truncated to microseconds). */
    public function toDateTime(): \DateTimeImmutable
    {
        return Sve::dateTime(intdiv($this->ns, 1000) - ($this->ns % 1000 < 0 ? 1 : 0));
    }
}
