<?php

declare(strict_types=1);

namespace Storine;

/**
 * An exact decimal that keeps its scale ("1234.50" stays "1234.50"): a 128-bit mantissa and up
 * to 38 fractional digits. Use it for money (PHP floats are binary and inexact).
 *
 *     $price = Decimal::of('19.99');
 *     $total = $price->mul(3)->add('0.03');   // "59.97" + "0.03" = "60.00"
 */
final class Decimal implements \Stringable, \JsonSerializable
{
    public const MAX_SCALE = 38;
    private const MAX_MANTISSA = '170141183460469231731687303715884105727';
    private const MIN_MANTISSA = '-170141183460469231731687303715884105728';

    /** @param string $mantissa decimal digits with an optional '-' */
    private function __construct(public readonly string $mantissa, public readonly int $scale)
    {
    }

    /** A Decimal from "19.99", "1E+3", an int, or a Decimal. Floats are refused (inexact). */
    public static function of(self|string|int $value): self
    {
        if ($value instanceof self) {
            return $value;
        }
        if (is_int($value)) {
            return self::make((string) $value, 0);
        }
        if (!preg_match('/^\s*([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d+))?\s*$/', $value, $m)
            || ($m[2] === '' && ($m[3] ?? '') === '')) {
            throw new InvalidException("not a decimal: \"$value\"");
        }
        $frac = $m[3] ?? '';
        $mantissa = ($m[1] === '-' ? '-' : '') . ($m[2] . $frac === '' ? '0' : $m[2] . $frac);
        return self::make($mantissa, strlen($frac) - (int) ($m[4] ?? 0));
    }

    /** A Decimal from its mantissa and scale: fromParts("1999", 2) is 19.99. */
    public static function fromParts(string|int $mantissa, int $scale): self
    {
        return self::make((string) $mantissa, $scale);
    }

    private static function make(string $mantissa, int $scale): self
    {
        [$negative, $digits] = BigInt::split($mantissa);
        if ($scale < 0) { // e.g. "1E+3": fold the exponent into the mantissa
            $digits = $digits === '0' ? '0' : $digits . str_repeat('0', -$scale);
            $scale = 0;
        }
        while ($scale > self::MAX_SCALE && str_ends_with($digits, '0') && $digits !== '0') {
            $digits = substr($digits, 0, -1);
            $scale--;
        }
        if ($scale > self::MAX_SCALE) {
            throw new InvalidException('decimals have at most 38 fractional digits');
        }
        $m = ($negative ? '-' : '') . $digits;
        if (BigInt::compare($m, self::MAX_MANTISSA) > 0 || BigInt::compare($m, self::MIN_MANTISSA) < 0) {
            throw new InvalidException('decimal does not fit in 38 digits');
        }
        return new self($m, $scale);
    }

    public function add(self|string|int $other): self
    {
        [$a, $b, $s] = $this->aligned(self::of($other));
        return self::make(self::addInts($a, $b), $s);
    }

    public function sub(self|string|int $other): self
    {
        return $this->add(self::of($other)->negate());
    }

    public function mul(self|string|int $other): self
    {
        $o = self::of($other);
        return self::make(self::mulInts($this->mantissa, $o->mantissa), $this->scale + $o->scale);
    }

    public function negate(): self
    {
        return new self(str_starts_with($this->mantissa, '-') ? substr($this->mantissa, 1)
            : ($this->mantissa === '0' ? '0' : '-' . $this->mantissa), $this->scale);
    }

    /** -1, 0 or 1 (by value: 1.50 equals 1.5). */
    public function compareTo(self|string|int $other): int
    {
        [$a, $b] = $this->aligned(self::of($other));
        return BigInt::compare($a, $b);
    }

    public function equals(self|string|int $other): bool
    {
        return $this->compareTo($other) === 0;
    }

    public function isNegative(): bool
    {
        return str_starts_with($this->mantissa, '-');
    }

    public function __toString(): string
    {
        if ($this->scale === 0) {
            return $this->mantissa;
        }
        $negative = str_starts_with($this->mantissa, '-');
        $digits = str_pad(ltrim($this->mantissa, '-'), $this->scale + 1, '0', STR_PAD_LEFT);
        return ($negative ? '-' : '') . substr($digits, 0, -$this->scale) . '.' . substr($digits, -$this->scale);
    }

    public function jsonSerialize(): string
    {
        return (string) $this;
    }

    /** [mantissa a, mantissa b, common scale]. */
    private function aligned(self $o): array
    {
        $s = max($this->scale, $o->scale);
        $up = static fn (self $d) => $d->mantissa === '0' ? '0' : $d->mantissa . str_repeat('0', $s - $d->scale);
        return [$up($this), $up($o), $s];
    }

    private static function addInts(string $a, string $b): string
    {
        [$na, $da] = BigInt::split($a);
        [$nb, $db] = BigInt::split($b);
        if ($na === $nb) {
            return ($na ? '-' : '') . self::addDigits($da, $db);
        }
        $c = BigInt::compare($da, $db);
        if ($c === 0) {
            return '0';
        }
        return $c > 0 ? ($na ? '-' : '') . self::subDigits($da, $db) : ($nb ? '-' : '') . self::subDigits($db, $da);
    }

    private static function addDigits(string $a, string $b): string
    {
        $out = '';
        $carry = 0;
        for ($i = strlen($a) - 1, $j = strlen($b) - 1; $i >= 0 || $j >= 0 || $carry; $i--, $j--) {
            $v = ($i >= 0 ? (int) $a[$i] : 0) + ($j >= 0 ? (int) $b[$j] : 0) + $carry;
            $out = ($v % 10) . $out;
            $carry = intdiv($v, 10);
        }
        return ltrim($out, '0') ?: '0';
    }

    /** $a - $b for digit strings with $a >= $b. */
    private static function subDigits(string $a, string $b): string
    {
        $out = '';
        $borrow = 0;
        for ($i = strlen($a) - 1, $j = strlen($b) - 1; $i >= 0; $i--, $j--) {
            $v = (int) $a[$i] - ($j >= 0 ? (int) $b[$j] : 0) - $borrow;
            $borrow = $v < 0 ? 1 : 0;
            $out = ($v + 10 * $borrow) . $out;
        }
        return ltrim($out, '0') ?: '0';
    }

    private static function mulInts(string $a, string $b): string
    {
        [$na, $da] = BigInt::split($a);
        [$nb, $db] = BigInt::split($b);
        $r = array_fill(0, strlen($da) + strlen($db), 0);
        for ($i = strlen($da) - 1; $i >= 0; $i--) {
            for ($j = strlen($db) - 1; $j >= 0; $j--) {
                $k = $i + $j + 1;
                $v = (int) $da[$i] * (int) $db[$j] + $r[$k];
                $r[$k] = $v % 10;
                $r[$k - 1] += intdiv($v, 10);
            }
        }
        $digits = ltrim(implode('', $r), '0') ?: '0';
        return ($na !== $nb && $digits !== '0' ? '-' : '') . $digits;
    }
}
