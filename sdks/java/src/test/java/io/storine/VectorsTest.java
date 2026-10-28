package io.storine;

import static io.storine.FieldOp.add;
import static io.storine.FieldOp.append;
import static io.storine.FieldOp.set;
import static io.storine.FieldOp.unset;
import static io.storine.Filter.and;
import static io.storine.Filter.between;
import static io.storine.Filter.contains;
import static io.storine.Filter.eq;
import static io.storine.Filter.exists;
import static io.storine.Filter.ge;
import static io.storine.Filter.gt;
import static io.storine.Filter.in;
import static io.storine.Filter.keyPrefix;
import static io.storine.Filter.le;
import static io.storine.Filter.lt;
import static io.storine.Filter.missing;
import static io.storine.Filter.ne;
import static io.storine.Filter.not;
import static io.storine.Filter.or;
import static io.storine.Filter.prefix;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Conformance: the SDK reproduces the server's encodings byte for byte (protocol/vectors). */
class VectorsTest {
    private static final HexFormat HEX = HexFormat.of();

    @SuppressWarnings("unchecked")
    static Map<String, Object> vectors() throws Exception {
        String json = Files.readString(Path.of("..", "..", "protocol", "vectors", "vectors.json"));
        return (Map<String, Object>) MiniJson.parse(json);
    }

    @SuppressWarnings("unchecked")
    static List<Map<String, Object>> list(Object o) {
        return (List<Map<String, Object>>) o;
    }

    @SuppressWarnings("unchecked")
    static Object nativeValue(Map<String, Object> d) {
        Object v = d.get("value");
        return switch ((String) d.get("type")) {
            case "null" -> null;
            case "bool" -> v;
            case "int64" -> Long.parseLong((String) v);
            case "uint64" -> UInt64.of(new BigInteger((String) v));
            case "float64" -> Double.longBitsToDouble(Long.parseUnsignedLong((String) d.get("bits"), 16));
            case "int128" -> new BigInteger((String) v);
            case "timestamp" -> {
                long ns = Long.parseLong((String) v);
                yield Instant.ofEpochSecond(Math.floorDiv(ns, 1_000_000_000L), Math.floorMod(ns, 1_000_000_000L));
            }
            case "uuid" -> {
                String h = (String) v;
                yield UUID.fromString(h.substring(0, 8) + "-" + h.substring(8, 12) + "-" + h.substring(12, 16) + "-"
                        + h.substring(16, 20) + "-" + h.substring(20));
            }
            case "id" -> new StorineId(HEX.parseHex((String) v));
            case "text" -> v;
            case "bytes" -> HEX.parseHex((String) v);
            case "decimal" -> new BigDecimal((String) v);
            case "list" -> {
                List<Object> out = new ArrayList<>();
                for (Object x : (List<Object>) v) out.add(nativeValue((Map<String, Object>) x));
                yield out;
            }
            case "map" -> {
                Map<String, Object> out = new LinkedHashMap<>();
                for (Object p : (List<Object>) v) {
                    List<Object> pair = (List<Object>) p;
                    out.put((String) pair.get(0), nativeValue((Map<String, Object>) pair.get(1)));
                }
                yield out;
            }
            default -> throw new AssertionError(d);
        };
    }

    /** Comparable form: byte arrays as hex, decimals by string. */
    static Object comparable(Object x) {
        if (x instanceof byte[] b) return "bytes:" + HEX.formatHex(b);
        if (x instanceof BigDecimal d) return "dec:" + d.toPlainString();
        if (x instanceof List<?> l) return l.stream().map(VectorsTest::comparable).toList();
        if (x instanceof Map<?, ?> m) {
            Map<Object, Object> out = new HashMap<>();
            m.forEach((k, v) -> out.put(k, comparable(v)));
            return out;
        }
        return x;
    }

    @Test
    @SuppressWarnings("unchecked")
    void valuesEncodeAndDecode() throws Exception {
        for (Map<String, Object> vec : list(vectors().get("values"))) {
            Object value = nativeValue((Map<String, Object>) vec.get("value"));
            assertEquals(vec.get("hex"), HEX.formatHex(Values.encode(value)), (String) vec.get("name"));
            Object back = Values.decode(HEX.parseHex((String) vec.get("hex")));
            assertEquals(comparable(value), comparable(back), (String) vec.get("name"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void documentBody() throws Exception {
        Map<String, Object> doc = list(vectors().get("documents")).get(0);
        Values.Out out = new Values.Out();
        Values.encodeFields((Map<String, Object>) nativeValue((Map<String, Object>) doc.get("fields")), out, 1);
        assertEquals(doc.get("hex"), HEX.formatHex(out.toByteArray()));
    }

    @Test
    void requests() throws Exception {
        Map<String, Object> d = Map.of("balance", new BigDecimal("100.00"), "owner", "alice", "active", true);
        byte[] rid = HEX.parseHex("0102030405060708090a0b0c0d0e0f10");
        byte[] ab = new byte[16];
        Arrays.fill(ab, (byte) 0xab);
        Map<String, byte[]> built = new HashMap<>();
        built.put("get accounts/alice", Wire.get("accounts", "alice"));
        built.put("get accounts/alice: bytes 1000..1004 of photo", Wire.getRange("accounts", "alice", "photo", 1000, 4));
        built.put("get accounts/alice: fields owner, meta.size", Wire.getFields("accounts", "alice", List.of("owner", "meta.size")));
        built.put("write: put", Wire.writes(List.of(Wire.Write.put("accounts", "alice", d, 0, 0, 0)), null));
        built.put("write: insert (must not exist) with request id", Wire.writes(List.of(Wire.Write.put("accounts", "alice", d, 0, 1, 0)), ab));
        built.put("write: replace if version 7", Wire.writes(List.of(Wire.Write.put("accounts", "alice", d, 0, 3, 7)), null));
        built.put("write: put with ttl 1500 ms", Wire.writes(List.of(Wire.Write.put("sessions", "s1", Map.of("u", 1),
                Transaction.ttlMs(Duration.ofMillis(1500)), 0, 0)), null));
        built.put("write: delete", Wire.writes(List.of(Wire.Write.simple("accounts", "alice", 3, 0, 0)), null));
        built.put("write: delete if exists", Wire.writes(List.of(Wire.Write.simple("accounts", "alice", 3, 2, 0)), null));
        built.put("write: transfer 25.00 (two patches, no overdraft) with request id", Wire.writes(List.of(
                Wire.Write.patch("accounts", "alice", 2, 0, add("balance", new BigDecimal("-25.00")).min(0)),
                Wire.Write.patch("accounts", "bob", 2, 0, add("balance", new BigDecimal("25.00")))), rid));
        built.put("write: patch set / unset / append / add with min and max", Wire.writes(List.of(Wire.Write.patch("c", "k", 2, 0,
                set("profile.name", "Ann"), unset("tmp"), append("log", 3), add("n", 1).min(0).max(10))), null));
        built.put("write: check version 3", Wire.writes(List.of(Wire.Write.simple("c", "k", 5, 3, 3)), null));
        Query all = new Query();
        built.put("query: all (default limit)", Wire.query("items", all, null));
        built.put("query: eq text", Wire.query("items", all.where(eq("kind", "book")).limit(10), null));
        built.put("query: and(gte, lt) sorted desc, offset 20", Wire.query("items", all.where(and(ge("price", new BigDecimal("10")),
                lt("price", new BigDecimal("20.50")))).orderBy("-price").limit(10).offset(20), null));
        built.put("query: between", Wire.query("items", all.where(between("n", 1, 5)), null));
        built.put("query: in", Wire.query("items", all.where(in("n", 1, 2)), null));
        built.put("query: prefix / contains / exists / missing", Wire.query("items", all.where(or(prefix("name", "An"),
                contains("tags", "x"), exists("email"), missing("deleted_at"))), null));
        built.put("query: not / ne / le / gt / key prefix", Wire.query("items", all.where(and(not(ne("a", 1)), le("b", 2.5),
                gt("c", 0), keyPrefix("user:"))), null));
        built.put("query: after key cursor", Wire.query("items", all.limit(100), "k0099"));
        built.put("query: multi-key sort", Wire.query("items", all.orderBy("a", "-b").limit(5), null));
        built.put("query: nearest neighbours with a filter", Wire.query("items", all.where(eq("kind", "book")).limit(5), null,
                new Wire.Nearest("embedding", new float[] {0.25f, -1.5f, 3.0f}, 5, false)));
        built.put("query: nearest neighbours, exact", Wire.query("items", all, null, new Wire.Nearest("v", new float[] {1.0f, 0.0f}, 1, true)));
        built.put("admin: create index items.price", Wire.admin(1, "items", "price"));
        built.put("info: collection items", Wire.admin(6, "items", null));
        built.put("info: list collections", new byte[] {3});
        int seen = 0;
        for (Map<String, Object> vec : list(vectors().get("requests"))) {
            byte[] b = built.get((String) vec.get("name"));
            if (b == null) throw new AssertionError("no builder for " + vec.get("name"));
            assertEquals(vec.get("body_hex"), HEX.formatHex(b), (String) vec.get("name"));
            seen++;
        }
        assertEquals(built.size(), seen);
    }

    @Test
    void responses() throws Exception {
        Map<String, byte[]> r = new HashMap<>();
        for (Map<String, Object> v : list(vectors().get("responses"))) r.put((String) v.get("name"), HEX.parseHex((String) v.get("body_hex")));
        assertEquals(Arrays.asList(12L, null), Wire.writeResponse(r.get("write ok: [v12, deleted]")));
        Wire.Page p = Wire.page(r.get("page: total 42, next k1, one doc"));
        assertEquals(42, p.total());
        assertEquals("k1", p.nextKey());
        assertEquals(new Document("k1", 3, Map.of("n", 1L)), p.docs().get(0));
        Wire.Range range = Wire.byteRange(r.get("byte range: total 5000, 4 bytes"));
        assertEquals(5000, range.total());
        assertArrayEquals(new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef}, range.data());
    }

    @Test
    void hostileBytesNeverCrash() throws Exception {
        List<Map<String, Object>> values = list(vectors().get("values"));
        byte[] good = HEX.parseHex((String) values.get(values.size() - 1).get("hex"));
        for (int cut = 0; cut < good.length; cut++) {
            byte[] part = Arrays.copyOf(good, cut);
            assertThrows(InvalidException.class, () -> Values.decode(part));
        }
        assertThrows(InvalidException.class, () -> Values.decode(new byte[] {0x0C, -1, -1, -1, -1}));
    }

    @Test
    void valueMappingRules() {
        assertThrows(InvalidException.class, () -> Values.encode(BigInteger.ONE.shiftLeft(130)));
        assertThrows(InvalidException.class, () -> Values.encode(new Object()));
        assertThrows(InvalidException.class, () -> Values.encode(new BigDecimal("1E-40")));
        assertEquals(new BigDecimal("1000"), Values.decode(Values.encode(new BigDecimal("1E+3"))));
        BigDecimal big = new BigDecimal("1234567890123456789012345678.0123456789");
        assertEquals(big, Values.decode(Values.encode(big)));
        assertEquals(new BigDecimal("1.5"), Values.decode(Values.encode(new BigDecimal("1.500000000000000000000000000000000000000000"))));
        assertArrayEquals(Values.encode(List.of(1L, 2L)), Values.encode(new Integer[] {1, 2}));
    }
}
