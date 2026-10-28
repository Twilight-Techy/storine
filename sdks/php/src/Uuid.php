<?php

declare(strict_types=1);

namespace Storine;

/** A UUID (16 bytes): Uuid::parse('123e4567-e89b-12d3-a456-426614174000') or Uuid::random(). */
final class Uuid implements \Stringable
{
    public function __construct(public readonly string $bytes)
    {
        if (strlen($bytes) !== 16) {
            throw new InvalidException('a UUID is exactly 16 bytes');
        }
    }

    public static function parse(string $text): self
    {
        $hex = str_replace('-', '', $text);
        if (!preg_match('/^[0-9a-fA-F]{32}$/', $hex)) {
            throw new InvalidException("not a UUID: $text");
        }
        return new self((string) hex2bin($hex));
    }

    public static function random(): self
    {
        $b = random_bytes(16);
        $b[6] = chr((ord($b[6]) & 0x0f) | 0x40);
        $b[8] = chr((ord($b[8]) & 0x3f) | 0x80);
        return new self($b);
    }

    public function __toString(): string
    {
        $h = bin2hex($this->bytes);
        return substr($h, 0, 8) . '-' . substr($h, 8, 4) . '-' . substr($h, 12, 4) . '-' . substr($h, 16, 4) . '-' . substr($h, 20);
    }
}
