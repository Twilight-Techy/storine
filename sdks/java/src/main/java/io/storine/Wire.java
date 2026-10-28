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
            OP_DOC_ADMIN = 0x23, OP_DOC_INFO = 0x25, OP_SET_OPTIONS = 0x26, OP_CLUSTER_INFO = 0x27;
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

    static byte[] query(String collection, Query q, String afterKey) {
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
        return out.toByteArray();
    }

    static byte[] admin(int sub, String collection, String field) {
        Values.Out out = new Values.Out();
        out.u8(sub);
        out.str(collection);
        if (field != null) out.str(field);
        return out.toByteArray();
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
