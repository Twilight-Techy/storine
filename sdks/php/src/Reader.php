<?php

declare(strict_types=1);

namespace Storine;

/** Bounds-checked cursor over untrusted bytes. @internal */
final class Reader
{
    public function __construct(private readonly string $buf, private int $pos = 0)
    {
    }

    public function remaining(): int
    {
        return strlen($this->buf) - $this->pos;
    }

    public function take(int $n): string
    {
        if ($n < 0 || $this->pos + $n > strlen($this->buf)) {
            throw new InvalidException('truncated response');
        }
        $b = substr($this->buf, $this->pos, $n);
        $this->pos += $n;
        return $b;
    }

    public function u8(): int
    {
        return ord($this->take(1));
    }

    public function u32(): int
    {
        return unpack('V', $this->take(4))[1];
    }

    /** An unsigned 64-bit value: int when it fits, else UInt64. */
    public function u64(): int|UInt64
    {
        $b = $this->take(8);
        return (ord($b[7]) & 0x80) ? new UInt64(BigInt::fromBytes($b, false)) : unpack('P', $b)[1];
    }

    /** A version or count field (always below 2^63 in practice). */
    public function u64Int(): int
    {
        $v = $this->u64();
        if ($v instanceof UInt64) {
            throw new InvalidException('64-bit counter out of range');
        }
        return $v;
    }

    public function i64(): int
    {
        return unpack('q', $this->take(8))[1];
    }

    public function str(): string
    {
        $s = $this->take($this->u32());
        if (!preg_match('//u', $s)) {
            throw new InvalidException('invalid UTF-8 in response');
        }
        return $s;
    }

    public function count(int $minSize): int
    {
        $n = $this->u32();
        if ($n * max($minSize, 1) > $this->remaining()) {
            throw new InvalidException('truncated response');
        }
        return $n;
    }

    public function value(int $depth = 0): mixed
    {
        $tag = $this->u8();
        switch ($tag) {
            case Sve::NULL:
                return null;
            case Sve::BOOL:
                $b = $this->u8();
                if ($b > 1) {
                    throw new InvalidException('bool byte must be 0 or 1');
                }
                return $b === 1;
            case Sve::INT64:
                return $this->i64();
            case Sve::UINT64:
                return $this->u64();
            case Sve::FLOAT64:
                return unpack('e', $this->take(8))[1];
            case Sve::INT128:
                $text = BigInt::fromBytes($this->take(16), true);
                return BigInt::compare($text, (string) PHP_INT_MAX) <= 0 && BigInt::compare($text, (string) PHP_INT_MIN) >= 0
                    ? (int) $text : new Int128($text);
            case Sve::TIMESTAMP:
                $ns = $this->i64();
                return $ns % 1000 === 0 ? Sve::dateTime(intdiv($ns, 1000)) : new Timestamp($ns);
            case Sve::UUID:
                return new Uuid($this->take(16));
            case Sve::ID:
                return new Id($this->take(24));
            case Sve::TEXT:
                return $this->str();
            case Sve::BYTES:
                return new Bytes($this->take($this->u32()));
            case Sve::DECIMAL:
                $mantissa = BigInt::fromBytes($this->take(16), true);
                $scale = $this->u8();
                if ($scale > Decimal::MAX_SCALE) {
                    throw new InvalidException('decimal scale above 38');
                }
                return Decimal::fromParts($mantissa, $scale);
            case Sve::LIST:
                if ($depth >= Sve::MAX_DEPTH) {
                    throw new InvalidException('value nesting is too deep');
                }
                $n = $this->count(1);
                $out = [];
                for ($i = 0; $i < $n; $i++) {
                    $out[] = $this->value($depth + 1);
                }
                return $out;
            case Sve::MAP:
                if ($depth >= Sve::MAX_DEPTH) {
                    throw new InvalidException('value nesting is too deep');
                }
                return $this->map($depth + 1);
            default:
                throw new InvalidException("unknown value tag $tag");
        }
    }

    /** @return array<string, mixed> */
    public function map(int $depth = 0): array
    {
        $n = $this->count(5);
        $out = [];
        for ($i = 0; $i < $n; $i++) {
            $k = $this->str();
            if (array_key_exists($k, $out)) {
                throw new InvalidException('duplicate map key');
            }
            $out[$k] = $this->value($depth);
        }
        return $out;
    }
}
