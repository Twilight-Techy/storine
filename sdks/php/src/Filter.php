<?php

declare(strict_types=1);

namespace Storine;

/**
 * A query condition:
 *
 *     Filter::and(Filter::between('n', 10, 19), Filter::eq('kind', 'odd'))
 */
final class Filter
{
    /** @param list<mixed> $values @param list<Filter> $children */
    private function __construct(
        public readonly int $kind,
        public readonly string $path = '',
        public readonly array $values = [],
        public readonly array $children = [],
    ) {
    }

    /** Field equals value (numbers compare numerically; null matches missing fields). */
    public static function eq(string $path, mixed $value): self
    {
        return new self(1, $path, [$value]);
    }

    public static function ne(string $path, mixed $value): self
    {
        return new self(2, $path, [$value]);
    }

    public static function lt(string $path, mixed $value): self
    {
        return new self(3, $path, [$value]);
    }

    public static function le(string $path, mixed $value): self
    {
        return new self(4, $path, [$value]);
    }

    public static function gt(string $path, mixed $value): self
    {
        return new self(5, $path, [$value]);
    }

    public static function ge(string $path, mixed $value): self
    {
        return new self(6, $path, [$value]);
    }

    /** `low <= field <= high`. */
    public static function between(string $path, mixed $low, mixed $high): self
    {
        return new self(7, $path, [$low, $high]);
    }

    /** Field equals any of `values`. */
    public static function in(string $path, array $values): self
    {
        return new self(8, $path, array_values($values));
    }

    /** Text field starts with `text`. */
    public static function prefix(string $path, string $text): self
    {
        return new self(9, $path, [$text]);
    }

    /** Text field contains `value`, or list field has an element equal to `value`. */
    public static function contains(string $path, mixed $value): self
    {
        return new self(10, $path, [$value]);
    }

    /** Field is present and not null. */
    public static function exists(string $path): self
    {
        return new self(11, $path);
    }

    /** Field is absent or null. */
    public static function missing(string $path): self
    {
        return new self(12, $path);
    }

    /** Document key starts with `text`. */
    public static function keyPrefix(string $text): self
    {
        return new self(13, '', [$text]);
    }

    public static function and(self ...$filters): self
    {
        return new self(14, '', [], $filters);
    }

    public static function or(self ...$filters): self
    {
        return new self(15, '', [], $filters);
    }

    public static function not(self $filter): self
    {
        return new self(16, '', [], [$filter]);
    }

    /** A Filter, a ['field' => value] array (all equal), or null. @internal */
    public static function from(self|array|null $where): ?self
    {
        if ($where === null || $where instanceof self) {
            return $where;
        }
        $parts = [];
        foreach ($where as $k => $v) {
            $parts[] = $v instanceof self ? $v : self::eq((string) $k, $v);
        }
        return match (count($parts)) {
            0 => null,
            1 => $parts[0],
            default => self::and(...$parts),
        };
    }

    /** @internal */
    public function encode(string &$out, int $depth = 0): void
    {
        if ($depth > 32) {
            throw new InvalidException('filter nesting is too deep (max 32)');
        }
        $out .= chr($this->kind);
        switch ($this->kind) {
            case 1: case 2: case 3: case 4: case 5: case 6: case 10:
                Sve::putStr($out, $this->path);
                Sve::encode($this->values[0], $out);
                break;
            case 7:
                Sve::putStr($out, $this->path);
                Sve::encode($this->values[0], $out);
                Sve::encode($this->values[1], $out);
                break;
            case 8:
                Sve::putStr($out, $this->path);
                $out .= pack('V', count($this->values));
                foreach ($this->values as $v) {
                    Sve::encode($v, $out);
                }
                break;
            case 9:
                Sve::putStr($out, $this->path);
                Sve::putStr($out, $this->values[0]);
                break;
            case 11: case 12:
                Sve::putStr($out, $this->path);
                break;
            case 13:
                Sve::putStr($out, $this->values[0]);
                break;
            case 14: case 15:
                $out .= pack('V', count($this->children));
                foreach ($this->children as $c) {
                    $c->encode($out, $depth + 1);
                }
                break;
            case 16:
                $this->children[0]->encode($out, $depth + 1);
                break;
        }
    }
}
