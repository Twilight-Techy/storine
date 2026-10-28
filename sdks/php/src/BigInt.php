<?php

declare(strict_types=1);

namespace Storine;

/**
 * Fixed-width two's complement integers of any size, as decimal strings: what PHP's 64-bit
 * integers cannot hold (128-bit integers, decimal mantissas, unsigned 64-bit values).
 * @internal
 */
final class BigInt
{
    private const BASE = 1_000_000_000;

    /** Little-endian two's complement bytes of the decimal string $text (e.g. "-42"). */
    public static function toBytes(string $text, int $size): string
    {
        [$negative, $digits] = self::split($text);
        $limbs = self::limbs($digits);
        $bytes = [];
        for ($i = 0; $i < $size; $i++) {
            $bytes[] = self::divmod($limbs, 256);
        }
        if (!self::isZero($limbs)) {
            throw new InvalidException("integer does not fit in " . (8 * $size) . " bits");
        }
        if ($negative) {
            $carry = 1;
            foreach ($bytes as $i => $b) {
                $v = ((~$b) & 0xff) + $carry;
                $bytes[$i] = $v & 0xff;
                $carry = $v >> 8;
            }
        }
        return pack('C*', ...$bytes);
    }

    /** The decimal string of little-endian bytes ($signed: two's complement). */
    public static function fromBytes(string $bytes, bool $signed): string
    {
        $b = array_values(unpack('C*', $bytes));
        $negative = $signed && ($b[count($b) - 1] & 0x80) !== 0;
        if ($negative) {
            $carry = 1;
            foreach ($b as $i => $v) {
                $x = ((~$v) & 0xff) + $carry;
                $b[$i] = $x & 0xff;
                $carry = $x >> 8;
            }
        }
        $limbs = [0];
        for ($i = count($b) - 1; $i >= 0; $i--) {
            $carry = $b[$i];
            foreach ($limbs as $k => $l) {
                $v = $l * 256 + $carry;
                $limbs[$k] = $v % self::BASE;
                $carry = intdiv($v, self::BASE);
            }
            while ($carry > 0) {
                $limbs[] = $carry % self::BASE;
                $carry = intdiv($carry, self::BASE);
            }
        }
        $text = (string) $limbs[count($limbs) - 1];
        for ($k = count($limbs) - 2; $k >= 0; $k--) {
            $text .= str_pad((string) $limbs[$k], 9, '0', STR_PAD_LEFT);
        }
        return ($negative && $text !== '0' ? '-' : '') . $text;
    }

    /** -1, 0 or 1 comparing two decimal integer strings. */
    public static function compare(string $a, string $b): int
    {
        [$na, $da] = self::split($a);
        [$nb, $db] = self::split($b);
        if ($da === '0') {
            $na = false;
        }
        if ($db === '0') {
            $nb = false;
        }
        if ($na !== $nb) {
            return $na ? -1 : 1;
        }
        $c = strlen($da) <=> strlen($db) ?: strcmp($da, $db) <=> 0;
        return $na ? -$c : $c;
    }

    /** [negative, digits without sign or leading zeros]. */
    public static function split(string $text): array
    {
        if (!preg_match('/^([+-]?)(\d+)$/', $text, $m)) {
            throw new InvalidException("not an integer: $text");
        }
        $digits = ltrim($m[2], '0');
        return [$m[1] === '-' && $digits !== '', $digits === '' ? '0' : $digits];
    }

    /** @return list<int> base-1e9 limbs, least significant first */
    private static function limbs(string $digits): array
    {
        $out = [];
        for ($end = strlen($digits); $end > 0; $end -= 9) {
            $start = max(0, $end - 9);
            $out[] = (int) substr($digits, $start, $end - $start);
        }
        return $out;
    }

    /** Divides the limbs in place by $d; returns the remainder. */
    private static function divmod(array &$limbs, int $d): int
    {
        $rem = 0;
        for ($k = count($limbs) - 1; $k >= 0; $k--) {
            $v = $rem * self::BASE + $limbs[$k];
            $limbs[$k] = intdiv($v, $d);
            $rem = $v % $d;
        }
        while (count($limbs) > 1 && $limbs[count($limbs) - 1] === 0) {
            array_pop($limbs);
        }
        return $rem;
    }

    private static function isZero(array $limbs): bool
    {
        return count($limbs) === 1 && $limbs[0] === 0;
    }
}
