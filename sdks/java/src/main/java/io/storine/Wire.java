package io.storine;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Request / response encodings of the document API (protocol/PROTOCOL.md §5-§6). */
final class Wire {
    private Wire() {}

    static final int OP_AUTH = 0x0C, OP_DOC_GET = 0x20, OP_DOC_WRITE = 0x21, OP_DOC_QUERY = 0x22,
            OP_DOC_ADMIN = 0x23, OP_DOC_INFO = 0x25, OP_SET_OPTIONS = 0x26, OP_CLUSTER_INFO = 0x27,
            OP_DOC_AGGREGATE = 0x29, OP_DOC_SCAN_COLUMNS = 0x2A;
    static final int ST_OK = 0, ST_NOT_FOUND = 1, ST_ERROR = 2, ST_OVERLOADED = 3, ST_CONFLICT = 4,
            ST_UNAUTHORIZED = 5, ST_RATE_LIMITED = 6, ST_NOT_LEADER = 7, ST_INVALID = 8, ST_COMMIT_UNKNOWN = 9;

    /** One write of an atomic request. kind: 1 put, 2 put+ttl, 3 delete, 4 patch, 5 check. pre: 0 none, 1 absent, 2 exists, 3 version. */
    record Write(String collection, String key, int kind, int pre, long version, Map<String, ?> fields, long ttlMs,
            List<FieldOp> ops) {
        static Write put(String c, String k, Map<String, ?> fields, long ttlMs, int pre, long version) {
            if (fields == null) throw new InvalidException("a document is a map of fields");
            return new Write(c, k, ttlMs > 0 ? 2 : 1, pre, version, fields, ttlMs, List.of());
        }

        static Write patch(String c, String k, int pre, long version, FieldOp... ops) {
            if (ops.length == 0) throw new InvalidException("update needs at least one change (set, unset, add, append)");
            return new Write(c, k, 4, pre, version, null, 0, List.of(ops));
        }

        static Write simple(String c, String k, int kind, int pre, long version) {
            return new Write(c, k, kind, pre, version, null, 0, List.of());
        }
    }

    static byte[] get(String collection, String key) {
        Values.Out out = new Values.Out();
        out.str(collection);
        out.str(key);
        return out.toByteArray();
    }

    /** Most bytes one ranged read returns (16 MiB). */
    static final int MAX_BYTE_RANGE = 16 * 1024 * 1024;

    /** {@code OP_DOC_GET} with a byte-range shape: bytes {@code offset .. offset + length} of one bytes/text field. */
    static byte[] getRange(String collection, String key, String field, long offset, int length) {
        if (offset < 0) throw new InvalidException("offset must not be negative");
        if (length < 0 || length > MAX_BYTE_RANGE)
            throw new InvalidException("length must be between 0 and " + MAX_BYTE_RANGE);
        Values.Out out = new Values.Out();
        out.str(collection);
        out.str(key);
        out.u8(1);
        out.str(field);
        out.u64(offset);
        out.u32(length);
        return out.toByteArray();
    }

    /** {@code OP_DOC_GET} with a projection shape: only the given (dotted) field paths. */
    static byte[] getFields(String collection, String key, List<String> paths) {
        if (paths.size() > 256) throw new InvalidException("at most 256 fields can be projected");
        Values.Out out = new Values.Out();
        out.str(collection);
        out.str(key);
        out.u8(2);
        out.u32(paths.size());
        for (String p : paths) out.str(p);
        return out.toByteArray();
    }

    record Range(long total, byte[] data) {}

    /** {@code u64 total | u32 n | n bytes}. */
    static Range byteRange(byte[] body) {
        Values.Reader r = new Values.Reader(body);
        long total = r.u64();
        byte[] data = r.take(r.u32());
        if (r.remaining() != 0) throw new InvalidException("trailing bytes after byte range");
        return new Range(total, data);
    }

    static byte[] writes(List<Write> writes, byte[] requestId) {
        Values.Out out = new Values.Out();
        if (requestId == null) {
            out.u8(0);
        } else {
            out.u8(1);
            out.raw(requestId);
        }
        out.u32(writes.size());
        for (Write w : writes) {
            out.str(w.collection());
            out.str(w.key());
            out.u8(w.pre());
            if (w.pre() == 3) out.u64(w.version());
            out.u8(w.kind());
            if (w.kind() == 1) {
                Values.encodeFields(w.fields(), out, 1);
            } else if (w.kind() == 2) {
                out.u64(w.ttlMs());
                Values.encodeFields(w.fields(), out, 1);
            } else if (w.kind() == 4) {
                out.u32(w.ops().size());
                for (FieldOp op : w.ops()) op.encode(out);
            }
        }
        return out.toByteArray();
    }

    /** The optional nearest-neighbour block of a query: the {@code k} documents whose {@code field} is most similar to {@code vector}. */
    record Nearest(String field, float[] vector, int k, boolean exact) {}

    static byte[] query(String collection, Query q, String afterKey) {
        return query(collection, q, afterKey, null);
    }

    static byte[] query(String collection, Query q, String afterKey, Nearest nearest) {
        Values.Out out = new Values.Out();
        out.str(collection);
        if (q.where == null) {
            out.u8(0);
        } else {
            out.u8(1);
            q.where.encode(out, 0);
        }
        out.u32(q.orderBy.size());
        for (String f : q.orderBy) {
            boolean desc = f.startsWith("-");
            out.str(desc ? f.substring(1) : f);
            out.u8(desc ? 1 : 0);
        }
        out.u32(q.limit);
        out.u32(q.offset);
        if (afterKey == null) {
            out.u8(0);
        } else {
            out.u8(1);
            out.str(afterKey);
        }
        if (nearest != null) {
            // Optional trailing block: u8 1 | str field | u32 k | u8 exact | u32 dims | dims x f32.
            out.u8(1);
            out.str(nearest.field());
            out.u32(nearest.k());
            out.u8(nearest.exact() ? 1 : 0);
            out.u32(nearest.vector().length);
            for (float f : nearest.vector()) out.u32(Float.floatToRawIntBits(f) & 0xFFFFFFFFL);
        }
        return out.toByteArray();
    }

    private static void filterOpt(Filter where, Values.Out out) {
        if (where == null) {
            out.u8(0);
        } else {
            out.u8(1);
            where.encode(out, 0);
        }
    }

    /** {@code OP_DOC_AGGREGATE} (§5.5); order-by names are resolved to output column indexes. */
    static byte[] aggregate(String collection, Aggregation a) {
        List<String> cols = a.columns();
        if (new java.util.HashSet<>(cols).size() != cols.size())
            throw new InvalidException("output column names must be unique: " + cols);
        Values.Out out = new Values.Out();
        out.str(collection);
        filterOpt(a.where, out);
        out.u32(a.groupBy.size());
        for (GroupKey k : a.groupBy) k.encode(out);
        out.u32(a.aggs.size());
        for (Agg g : a.aggs.values()) g.encode(out);
        out.u32(a.orderBy.size());
        for (String o : a.orderBy) {
            boolean desc = o.startsWith("-");
            String name = desc ? o.substring(1) : o;
            int idx = cols.indexOf(name);
            if (idx < 0) throw new InvalidException("orderBy names an unknown column '" + name + "' (columns: " + cols + ")");
            out.u32(idx);
            out.u8(desc ? 1 : 0);
        }
        out.u32(a.limit);
        out.u64(a.memoryBudget);
        out.u64(a.spillBudget);
        return out.toByteArray();
    }

    /** {@code u64 matched | u64 pruned | u64 spilled | u32 columns | u32 rows | rows x columns x value}. */
    static AggregateResult aggregateResult(byte[] body, List<String> columns) {
        Values.Reader r = new Values.Reader(body);
        long matched = r.u64(), pruned = r.u64(), spilled = r.u64();
        long cols = r.u32();
        int n = r.count((int) Math.min(cols, Integer.MAX_VALUE));
        if (n > 0 && cols != columns.size())
            throw new InvalidException("aggregate response has " + cols + " columns, expected " + columns.size());
        List<LinkedHashMap<String, Object>> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            LinkedHashMap<String, Object> row = new LinkedHashMap<>();
            for (String c : columns) row.put(c, r.value(0));
            rows.add(row);
        }
        if (r.remaining() != 0) throw new InvalidException("trailing bytes after aggregate result");
        return new AggregateResult(rows, matched, pruned, spilled);
    }

    /** {@code OP_DOC_SCAN_COLUMNS} (§5.6). */
    static byte[] scanColumns(String collection, Filter where, List<String> fields, String afterKey, int limit) {
        Values.Out out = new Values.Out();
        out.str(collection);
        filterOpt(where, out);
        out.u32(fields.size());
        for (String f : fields) out.str(f);
        if (afterKey == null) {
            out.u8(0);
        } else {
            out.u8(1);
            out.str(afterKey);
        }
        out.u32(limit);
        return out.toByteArray();
    }

    record ScanPage(long total, String nextKey, List<ColumnRow> rows) {}

    /** {@code u64 total | u8 has_next | str next? | u32 fields | u32 rows | rows x (str key | fields x value)}. */
    static ScanPage scanPage(byte[] body, List<String> fields) {
        Values.Reader r = new Values.Reader(body);
        long total = r.u64();
        String next = r.u8() != 0 ? r.str() : null;
        long nf = r.u32();
        if (nf != fields.size()) throw new InvalidException("scan response has " + nf + " fields, expected " + fields.size());
        int n = r.count(4 + fields.size());
        List<ColumnRow> rows = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            String key = r.str();
            Map<String, Object> values = new LinkedHashMap<>();
            for (String f : fields) values.put(f, r.value(0));
            rows.add(new ColumnRow(key, values));
        }
        if (r.remaining() != 0) throw new InvalidException("trailing bytes after scan page");
        return new ScanPage(total, next, rows);
    }

    static byte[] admin(int sub, String collection, String field) {
        Values.Out out = new Values.Out();
        out.u8(sub);
        out.str(collection);
        if (field != null) out.str(field);
        return out.toByteArray();
    }

    static byte[] createVectorIndex(String collection, String field, int dims) {
        Values.Out out = new Values.Out();
        out.u8(7);
        out.str(collection);
        out.str(field);
        out.u32(dims);
        return out.toByteArray();
    }

    static List<VectorIndex> vectorIndexes(byte[] body) {
        Values.Reader r = new Values.Reader(body);
        int n = r.count(9);
        List<VectorIndex> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            String field = r.str();
            int dims = (int) r.u32();
            out.add(new VectorIndex(field, dims, r.u8() == 1));
        }
        return out;
    }

    /** New version of each write; {@code null} for a deleted / absent document. */
    static List<Long> writeResponse(byte[] body) {
        Values.Reader r = new Values.Reader(body);
        r.u8(); // duplicate flag
        int n = r.count(9);
        List<Long> versions = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            boolean exists = r.u8() != 0;
            long v = r.u64();
            versions.add(exists ? v : null);
        }
        return versions;
    }

    record Page(long total, String nextKey, List<Document> docs) {}

    static Page page(byte[] body) {
        Values.Reader r = new Values.Reader(body);
        long total = r.u32();
        String next = r.u8() != 0 ? r.str() : null;
        int n = r.count(16);
        List<Document> docs = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            String key = r.str();
            long version = r.u64();
            docs.add(new Document(key, version, r.fields(1)));
        }
        return new Page(total, next, docs);
    }

    static ConflictException conflict(byte[] body) {
        if (body.length < 13) return new ConflictException(new String(body, StandardCharsets.UTF_8), -1, null);
        Values.Reader r = new Values.Reader(body);
        long index = r.u32();
        boolean exists = r.u8() != 0;
        long version = r.u64();
        String msg = new String(body, 13, body.length - 13, StandardCharsets.UTF_8);
        return new ConflictException(msg, index == 0xFFFFFFFFL ? -1 : (int) index, exists ? version : null);
    }

    static CollectionInfo info(Values.Reader r) {
        String name = r.str();
        List<String> indexes = strings(r);
        List<String> building = strings(r);
        return new CollectionInfo(name, indexes, building, r.fields(1));
    }

    private static List<String> strings(Values.Reader r) {
        int n = r.count(4);
        List<String> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) out.add(r.str());
        return out;
    }

    static ClusterInfo clusterInfo(byte[] body) {
        Values.Reader r = new Values.Reader(body);
        long nodeId = r.u64();
        boolean leader = r.u8() != 0;
        long leaderId = r.u64();
        String leaderAddr = r.str();
        long term = r.u64(), commit = r.u64(), applied = r.u64();
        int n = r.count(12);
        Map<Long, String> members = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            long id = r.u64();
            members.put(id, r.str());
        }
        return new ClusterInfo(nodeId, leader, leaderId == 0 ? null : leaderId, leaderAddr.isEmpty() ? null : leaderAddr,
                term, commit, applied, members);
    }
}
