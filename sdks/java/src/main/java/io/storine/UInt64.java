package io.storine;

import java.math.BigInteger;

/** An unsigned 64-bit integer (Java has no unsigned long). */
public record UInt64(long bits) {
    /** From a non-negative value up to 2^64 - 1. */
    public static UInt64 of(BigInteger value) {
        if (value.signum() < 0 || value.bitLength() > 64) throw new InvalidException("not an unsigned 64-bit value: " + value);
        return new UInt64(value.longValue());
    }

    public BigInteger toBigInteger() {
        return new BigInteger(Long.toUnsignedString(bits));
    }

    @Override
    public String toString() {
        return Long.toUnsignedString(bits);
    }
}
