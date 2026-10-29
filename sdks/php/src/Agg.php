<?php

declare(strict_types=1);

namespace Storine;

/**
 * One aggregate of `Collection::aggregate`, and the `bucket` group key:
 *
 *     $sales->aggregate(['orders' => Agg::count(), 'revenue' => Agg::sum(Expr::field('price')->times('qty'))],
 *         groupBy: 'region', orderBy: '-revenue');
 *
 * An argument is a field path string or an `Expr` (numbers / Decimals become constants).
 */
final class Agg
{
    public const COUNT = 1, SUM = 2, AVG = 3, MIN = 4, MAX = 5, COUNT_DISTINCT = 6, APPROX_COUNT_DISTINCT = 7;

    private function __construct(public readonly int $func, public readonly ?Expr $arg)
    {
    }

    /** `count()` counts rows; `count($arg)` counts rows where `$arg` is not null. */
    public static function count(mixed $arg = null): self
    {
        return new self(self::COUNT, $arg === null ? null : Expr::of($arg));
    }

    /** Exact sum of integers / decimals (a float sum if any input is a float); null over no values. */
    public static function sum(mixed $arg): self
    {
        return new self(self::SUM, Expr::of($arg));
    }

    /** Mean: a Decimal for exact inputs (6 more fractional digits, half-even), a float otherwise. */
    public static function avg(mixed $arg): self
    {
        return new self(self::AVG, Expr::of($arg));
    }

    /** Smallest value in the document sort order (nulls ignored). */
    public static function min(mixed $arg): self
    {
        return new self(self::MIN, Expr::of($arg));
    }

    /** Largest value in the document sort order (nulls ignored). */
    public static function max(mixed $arg): self
    {
        return new self(self::MAX, Expr::of($arg));
    }

    /** Exact number of distinct non-null values. */
    public static function countDistinct(mixed $arg): self
    {
        return new self(self::COUNT_DISTINCT, Expr::of($arg));
    }

    /** HyperLogLog estimate of the number of distinct values (about 0.8 % standard error). */
    public static function approxCountDistinct(mixed $arg): self
    {
        return new self(self::APPROX_COUNT_DISTINCT, Expr::of($arg));
    }

    /** Group key: `$offset + floor((v - $offset) / $width) * $width` of a numeric field (column named `$field`). */
    public static function bucket(string $field, int $width, int $offset = 0): Bucket
    {
        return new Bucket($field, $width, $offset);
    }

    /** @internal */
    public function encode(string &$out): void
    {
        $out .= chr($this->func);
        if ($this->arg === null) {
            $out .= "\x00";
        } else {
            $out .= "\x01";
            $this->arg->encode($out);
        }
    }
}
