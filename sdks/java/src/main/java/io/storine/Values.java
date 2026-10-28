package io.storine;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

/**
 * The Storine value encoding (protocol/PROTOCOL.md §3).
 *
 * <p>Java to Storine: {@code null}, {@code Boolean}, {@code Byte/Short/Integer/Long} (int64), {@link UInt64},
 * {@code BigInteger} (int64 or int128), {@code Float/Double} (float64), {@code BigDecimal} (exact decimal, at most
 * 38 fractional digits), {@code String}, {@code byte[]}, {@code Instant} (nanoseconds), {@code UUID},
 * {@link StorineId}, any {@code Collection} or array (list) and {@code Map<String, ?>} (map).
 *
 * <p>Storine to Java: {@code null}, {@code Boolean}, {@code Long}, {@link UInt64}, {@code Double},
 * {@code BigInteger}, {@code Instant}, {@code UUID}, {@link StorineId}, {@code String}, {@code byte[]},
 * {@code BigDecimal}, {@code List<Object>}, {@code Map<String, Object>} (keys in ascending order).
 */
public final class Values {
    private Values() {}

    static final int NULL = 0, BOOL = 1, INT64 = 2, UINT64 = 3, FLOAT64 = 4, INT128 = 5, TIMESTAMP = 6, UUID_TAG = 7,
            ID = 8, TEXT = 9, BYTES = 10, DECIMAL = 11, LIST = 12, MAP = 13;
    static final int MAX_DEPTH = 32;
    static final int MAX_SCALE = 38;
    private static final BigInteger I64_MIN = BigInteger.valueOf(Long.MIN_VALUE);
    private static final BigInteger I64_MAX = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger I128_MIN = BigInteger.ONE.shiftLeft(127).negate();
    private static final BigInteger I128_MAX = BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE);

    /** The encoding of one value. */
    public static byte[] encode(Object value) {
        Out out = new Out();
        encode(value, out, 0);
        return out.toByteArray();
    }

    /** Decodes exactly one value; {@link InvalidException} on malformed input. */
    public static Object decode(byte[] bytes) {
        Reader r = new Reader(bytes);
        Object v = r.value(0);
        if (r.remaining() != 0) throw new InvalidException("trailing bytes after value");
        return v;
    }

    /** Output buffer with little-endian helpers. */
    static final class Out extends ByteArrayOutputStream {
        void u8(int v) {
            write(v);
        }

        void u32(long v) {
            for (int i = 0; i < 4; i++) write((int) (v >>> (8 * i)));
        }

        void u64(long v) {
            for (int i = 0; i < 8; i++) write((int) (v >>> (8 * i)));
        }

        void raw(byte[] b) {
            write(b, 0, b.length);
        }

        void str(String s) {
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            u32(b.length);
            raw(b);
        }

        void i128(BigInteger v) {
            byte[] be = v.toByteArray(); // two's complement, big-endian, minimal
            byte fill = (byte) (v.signum() < 0 ? 0xFF : 0);
            for (int i = 0; i < 16; i++) {
                int j = be.length - 1 - i;
                write(j >= 0 ? be[j] : fill);
            }
        }
    }

    static void encode(Object v, Out out, int depth) {
        if (depth > MAX_DEPTH) throw new InvalidException("value nesting is too deep (max 32)");
        if (v == null) {
            out.u8(NULL);
        } else if (v instanceof Boolean b) {
            out.u8(BOOL);
            out.u8(b ? 1 : 0);
        } else if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
            out.u8(INT64);
            out.u64(((Number) v).longValue());
        } else if (v instanceof UInt64 u) {
            out.u8(UINT64);
            out.u64(u.bits());
        } else if (v instanceof Double || v instanceof Float) {
            out.u8(FLOAT64);
            out.u64(Double.doubleToRawLongBits(((Number) v).doubleValue()));
        } else if (v instanceof BigInteger n) {
            if (n.compareTo(I64_MIN) >= 0 && n.compareTo(I64_MAX) <= 0) {
                out.u8(INT64);
                out.u64(n.longValue());
            } else if (n.compareTo(I128_MIN) >= 0 && n.compareTo(I128_MAX) <= 0) {
                out.u8(INT128);
                out.i128(n);
            } else {
                throw new InvalidException("integer does not fit in 128 bits");
            }
        } else if (v instanceof BigDecimal d) {
            BigDecimal x = normalize(d);
            out.u8(DECIMAL);
            out.i128(x.unscaledValue());
            out.u8(x.scale());
        } else if (v instanceof String s) {
            out.u8(TEXT);
            out.str(s);
        } else if (v instanceof byte[] b) {
            out.u8(BYTES);
            out.u32(b.length);
            out.raw(b);
        } else if (v instanceof Instant t) {
            out.u8(TIMESTAMP);
            try {
                out.u64(Math.addExact(Math.multiplyExact(t.getEpochSecond(), 1_000_000_000L), t.getNano()));
            } catch (ArithmeticException e) {
                throw new InvalidException("timestamp out of range (years 1677-2262)");
            }
        } else if (v instanceof UUID u) {
            out.u8(UUID_TAG);
            out.u64(Long.reverseBytes(u.getMostSignificantBits()));
            out.u64(Long.reverseBytes(u.getLeastSignificantBits()));
        } else if (v instanceof StorineId id) {
            out.u8(ID);
            out.raw(id.bytes());
        } else if (v instanceof Map<?, ?> m) {
            out.u8(MAP);
            encodeFields(m, out, depth + 1);
        } else if (v instanceof Collection<?> c) {
            out.u8(LIST);
            out.u32(c.size());
            for (Object item : c) encode(item, out, depth + 1);
        } else if (v instanceof Object[] a) {
            encode(Arrays.asList(a), out, depth);
        } else {
            throw new InvalidException("cannot store a value of type " + v.getClass().getName());
        }
    }

    /** BigDecimal to the wire form: scale 0..38, no negative scales. */
    static BigDecimal normalize(BigDecimal d) {
        BigDecimal x = d;
        if (x.scale() < 0) x = x.setScale(0);
        if (x.scale() > MAX_SCALE) {
            x = x.stripTrailingZeros();
            if (x.scale() < 0) x = x.setScale(0);
            if (x.scale() > MAX_SCALE) throw new InvalidException("decimals have at most 38 fractional digits: " + d);
        }
        BigInteger m = x.unscaledValue();
        if (m.compareTo(I128_MIN) < 0 || m.compareTo(I128_MAX) > 0) {
            throw new InvalidException("decimal does not fit in 38 digits: " + d);
        }
        return x;
    }

    /** {@code u32 count | (str key, value)*} with keys in ascending UTF-8 byte order. */
    static void encodeFields(Map<?, ?> fields, Out out, int depth) {
        TreeMap<byte[], Object> sorted = new TreeMap<>(Arrays::compareUnsigned);
        for (Map.Entry<?, ?> e : fields.entrySet()) {
            if (!(e.getKey() instanceof String k)) throw new InvalidException("map keys must be strings");
            sorted.put(k.getBytes(StandardCharsets.UTF_8), e.getValue());
        }
        out.u32(sorted.size());
        for (Map.Entry<byte[], Object> e : sorted.entrySet()) {
            out.u32(e.getKey().length);
            out.raw(e.getKey());
            encode(e.getValue(), out, depth);
        }
    }

    /** Bounded reader: every count is checked against the bytes present before allocating. */
    static final class Reader {
        private final byte[] b;
        private int pos;

        Reader(byte[] b) {
            this.b = b;
        }

        int remaining() {
            return b.length - pos;
        }

        private void need(long n) {
            if (n < 0 || n > remaining()) throw new InvalidException("truncated response");
        }

        int u8() {
            need(1);
            return b[pos++] & 0xFF;
        }

        long u32() {
            need(4);
            long v = 0;
            for (int i = 0; i < 4; i++) v |= (long) (b[pos + i] & 0xFF) << (8 * i);
            pos += 4;
            return v;
        }

        long u64() {
            need(8);
            long v = 0;
            for (int i = 0; i < 8; i++) v |= (long) (b[pos + i] & 0xFF) << (8 * i);
            pos += 8;
            return v;
        }

        byte[] take(long n) {
            need(n);
            byte[] out = Arrays.copyOfRange(b, pos, pos + (int) n);
            pos += (int) n;
            return out;
        }

        String str() {
            byte[] raw = take(u32());
            try {
                return StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(java.nio.ByteBuffer.wrap(raw)).toString();
            } catch (CharacterCodingException e) {
                throw new InvalidException("invalid UTF-8 in response");
            }
        }

        int count(int minSize) {
            long n = u32();
            if (n * Math.max(minSize, 1) > remaining()) throw new InvalidException("truncated response");
            return (int) n;
        }

        BigInteger i128() {
            byte[] le = take(16);
            byte[] be = new byte[16];
            for (int i = 0; i < 16; i++) be[i] = le[15 - i];
            return new BigInteger(be);
        }

        Object value(int depth) {
            int tag = u8();
            switch (tag) {
                case NULL:
                    return null;
                case BOOL: {
                    int x = u8();
                    if (x > 1) throw new InvalidException("bool byte must be 0 or 1");
                    return x == 1;
                }
                case INT64:
                    return u64();
                case UINT64:
                    return new UInt64(u64());
                case FLOAT64:
                    return Double.longBitsToDouble(u64());
                case INT128:
                    return i128();
                case TIMESTAMP: {
                    long ns = u64();
                    return Instant.ofEpochSecond(Math.floorDiv(ns, 1_000_000_000L), Math.floorMod(ns, 1_000_000_000L));
                }
                case UUID_TAG: {
                    long hi = Long.reverseBytes(u64());
                    long lo = Long.reverseBytes(u64());
                    return new UUID(hi, lo);
                }
                case ID:
                    return new StorineId(take(24));
                case TEXT:
                    return str();
                case BYTES:
                    return take(u32());
                case DECIMAL: {
                    BigInteger m = i128();
                    int scale = u8();
                    if (scale > MAX_SCALE) throw new InvalidException("decimal scale above 38");
                    return new BigDecimal(m, scale);
                }
                case LIST: {
                    if (depth >= MAX_DEPTH) throw new InvalidException("value nesting is too deep");
                    int n = count(1);
                    List<Object> out = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) out.add(value(depth + 1));
                    return out;
                }
                case MAP:
                    if (depth >= MAX_DEPTH) throw new InvalidException("value nesting is too deep");
                    return fields(depth + 1);
                default:
                    throw new InvalidException("unknown value tag " + tag);
            }
        }

        Map<String, Object> fields(int depth) {
            int n = count(5);
            Map<String, Object> out = new LinkedHashMap<>();
            for (int i = 0; i < n; i++) {
                String k = str();
                if (out.containsKey(k)) throw new InvalidException("duplicate map key");
                out.put(k, value(depth));
            }
            return out;
        }
    }
}
