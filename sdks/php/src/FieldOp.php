<?php

declare(strict_types=1);

namespace Storine;

/**
 * One atomic change applied server-side by `update`:
 *
 *     FieldOp::add('balance', Decimal::of('-25.00'))->min(0)   // never overdraws
 */
final class FieldOp
{
    private bool $hasMin = false;
    private bool $hasMax = false;
    private mixed $minBound = null;
    private mixed $maxBound = null;

    private function __construct(public readonly int $kind, public readonly string $path, public readonly mixed $value = null)
    {
    }

    /** Set the field at `path` (dotted for nested maps) to `value`. */
    public static function set(string $path, mixed $value): self
    {
        return new self(1, $path, $value);
    }

    /** Remove the field at `path`. */
    public static function unset(string $path): self
    {
        return new self(2, $path);
    }

    /** Add `amount` (negative to subtract) exactly and atomically; bound it with ->min() / ->max(). */
    public static function add(string $path, mixed $amount): self
    {
        return new self(3, $path, $amount);
    }

    /** Append `value` to the list at `path` (a missing field becomes a one-element list). */
    public static function append(string $path, mixed $value): self
    {
        return new self(4, $path, $value);
    }

    /** Refuse an `add` whose result would be below `bound`. */
    public function min(mixed $bound): self
    {
        $op = clone $this;
        $op->hasMin = true;
        $op->minBound = $bound;
        return $op;
    }

    /** Refuse an `add` whose result would be above `bound`. */
    public function max(mixed $bound): self
    {
        $op = clone $this;
        $op->hasMax = true;
        $op->maxBound = $bound;
        return $op;
    }

    /** @internal */
    public function encode(string &$out): void
    {
        $out .= chr($this->kind);
        Sve::putStr($out, $this->path);
        if ($this->kind === 1 || $this->kind === 4) {
            Sve::encode($this->value, $out);
        } elseif ($this->kind === 3) {
            Sve::encode($this->value, $out);
            $out .= chr(($this->hasMin ? 1 : 0) | ($this->hasMax ? 2 : 0));
            if ($this->hasMin) {
                Sve::encode($this->minBound, $out);
            }
            if ($this->hasMax) {
                Sve::encode($this->maxBound, $out);
            }
        }
    }
}
