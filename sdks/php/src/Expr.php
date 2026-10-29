<?php

declare(strict_types=1);

namespace Storine;

/**
 * An arithmetic expression over a document's fields, for aggregates:
 *
 *     Agg::sum(Expr::field('price')->times('qty')->minus(1))
 *
 * Wherever an expression is accepted, a string is a field path (`'qty'` is `Expr::field('qty')`)
 * and any other value (int, float, Decimal, ...) is a constant (`Expr::lit(...)`).
 */
final class Expr
{
    public const FIELD = 1, CONST = 2, ADD = 3, SUB = 4, MUL = 5, NEG = 6;
    private const MAX_DEPTH = 32;

    /** @param list<Expr> $args */
    private function __construct(
        public readonly int $op,
        public readonly string $path = '',
        public readonly mixed $value = null,
        public readonly array $args = [],
    ) {
    }

    /** The value of the (dotted) field `$path`. */
    public static function field(string $path): self
    {
        return new self(self::FIELD, $path);
    }

    /** A constant (`Expr::lit(Decimal::of('2.5'))`; a PHP float is a float64). */
    public static function lit(mixed $value): self
    {
        return new self(self::CONST, '', $value);
    }

    /** An Expr as is, a string as a field path, anything else as a constant. */
    public static function of(mixed $x): self
    {
        return match (true) {
            $x instanceof self => $x,
            is_string($x) => self::field($x),
            default => self::lit($x),
        };
    }

    /** `$this + $x`. */
    public function plus(mixed $x): self
    {
        return new self(self::ADD, '', null, [$this, self::of($x)]);
    }

    /** `$this - $x`. */
    public function minus(mixed $x): self
    {
        return new self(self::SUB, '', null, [$this, self::of($x)]);
    }

    /** `$this * $x`. */
    public function times(mixed $x): self
    {
        return new self(self::MUL, '', null, [$this, self::of($x)]);
    }

    /** `-$this`. */
    public function neg(): self
    {
        return new self(self::NEG, '', null, [$this]);
    }

    /** @internal */
    public function encode(string &$out, int $depth = 0): void
    {
        if ($depth > self::MAX_DEPTH) {
            throw new InvalidException('expression nesting is too deep (max 32)');
        }
        $out .= chr($this->op);
        if ($this->op === self::FIELD) {
            Sve::putStr($out, $this->path);
        } elseif ($this->op === self::CONST) {
            Sve::encode($this->value, $out);
        } else {
            foreach ($this->args as $a) {
                $a->encode($out, $depth + 1);
            }
        }
    }
}
