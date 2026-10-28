<?php

declare(strict_types=1);

namespace Storine;

/**
 * Storine Value Encoding (SVE): PHP values <-> wire bytes (protocol/PROTOCOL.md §4).
 *
 * null, bool, int (64-bit), Int128, UInt64, float, Decimal (exact), string (UTF-8 text),
 * Bytes, DateTimeInterface / Timestamp, Uuid, Id, lists (arrays with keys 0..n-1) and maps
 * (other arrays, \stdClass, \ArrayObject). An empty array is an empty list; use
 * `new \stdClass()` for an empty map.
 * @internal
 */
final class Sve
{
    public const NULL = 0, BOOL = 1, INT64 = 2, UINT64 = 3, FLOAT64 = 4, INT128 = 5, TIMESTAMP = 6, UUID = 7, ID = 8,
        TEXT = 9, BYTES = 10, DECIMAL = 11, LIST = 12, MAP = 13;
    public const MAX_DEPTH = 32;

    public static function putStr(string &$out, string $s): void
    {
        if (!preg_match('//u', $s)) {
            throw new InvalidException('text is not valid UTF-8');
        }
        $out .= pack('V', strlen($s)) . $s;
    }

    public static function encode(mixed $value, string &$out, int $depth = 0): void
    {
        if ($depth > self::MAX_DEPTH) {
            throw new InvalidException('value nesting is too deep (max 32)');
        }
        switch (true) {
            case $value === null:
                $out .= chr(self::NULL);
                break;
            case is_bool($value):
                $out .= chr(self::BOOL) . chr($value ? 1 : 0);
                break;
            case is_int($value):
                $out .= chr(self::INT64) . pack('P', $value);
                break;
            case is_float($value):
                $out .= chr(self::FLOAT64) . pack('e', $value);
                break;
            case is_string($value):
                $out .= chr(self::TEXT);
                self::putStr($out, $value);
                break;
            case $value instanceof Decimal:
                $out .= chr(self::DECIMAL) . BigInt::toBytes($value->mantissa, 16) . chr($value->scale);
                break;
            case $value instanceof Int128:
                $out .= chr(self::INT128) . BigInt::toBytes($value->value, 16);
                break;
            case $value instanceof UInt64:
                $out .= chr(self::UINT64) . BigInt::toBytes($value->value, 8);
                break;
            case $value instanceof Bytes:
                $out .= chr(self::BYTES) . pack('V', strlen($value->data)) . $value->data;
                break;
            case $value instanceof Timestamp:
                $out .= chr(self::TIMESTAMP) . pack('P', $value->ns);
                break;
            case $value instanceof \DateTimeInterface:
                $ns = $value->getTimestamp() * 1_000_000_000 + (int) $value->format('u') * 1000;
                $out .= chr(self::TIMESTAMP) . pack('P', $ns);
                break;
            case $value instanceof Uuid:
                $out .= chr(self::UUID) . $value->bytes;
                break;
            case $value instanceof Id:
                $out .= chr(self::ID) . $value->bytes;
                break;
            case is_array($value) && array_is_list($value):
                $out .= chr(self::LIST) . pack('V', count($value));
                foreach ($value as $item) {
                    self::encode($item, $out, $depth + 1);
                }
                break;
            case is_array($value) || $value instanceof \stdClass || $value instanceof \ArrayObject:
                $out .= chr(self::MAP);
                self::encodeMap($value, $out, $depth + 1);
                break;
            default:
                throw new InvalidException('cannot store a value of type ' . get_debug_type($value));
        }
    }

    /** Appends `u32 count | (str key, value)*` with keys in ascending byte order. */
    public static function encodeMap(iterable|\stdClass $fields, string &$out, int $depth = 0): void
    {
        $items = [];
        foreach ($fields instanceof \stdClass ? get_object_vars($fields) : $fields as $k => $v) {
            $items[(string) $k] = $v;
        }
        ksort($items, SORT_STRING);
        $out .= pack('V', count($items));
        foreach ($items as $k => $v) {
            self::putStr($out, (string) $k);
            self::encode($v, $out, $depth);
        }
    }

    public static function encodeValue(mixed $value): string
    {
        $out = '';
        self::encode($value, $out);
        return $out;
    }

    public static function decode(string $bytes): mixed
    {
        $r = new Reader($bytes);
        $v = $r->value();
        if ($r->remaining() > 0) {
            throw new InvalidException('trailing bytes after value');
        }
        return $v;
    }

    /** A UTC DateTimeImmutable from microseconds since the epoch. */
    public static function dateTime(int $us): \DateTimeImmutable
    {
        $sec = intdiv($us, 1_000_000);
        $micro = $us % 1_000_000;
        if ($micro < 0) {
            $sec--;
            $micro += 1_000_000;
        }
        return (new \DateTimeImmutable('@' . $sec))->setTimezone(new \DateTimeZone('UTC'))
            ->setTime((int) gmdate('H', $sec), (int) gmdate('i', $sec), (int) gmdate('s', $sec), $micro);
    }
}
