package io.storine;

import java.util.Arrays;
import java.util.HexFormat;

/** An engine id value (24 raw bytes). */
public final class StorineId {
    private final byte[] bytes;

    public StorineId(byte[] bytes) {
        if (bytes.length != 24) throw new InvalidException("ids are 24 bytes");
        this.bytes = bytes.clone();
    }

    public byte[] bytes() {
        return bytes.clone();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof StorineId other && Arrays.equals(bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return "StorineId(" + HexFormat.of().formatHex(bytes) + ")";
    }
}
