package io.storine;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Writes collected by {@link Storine#transaction}. Methods only buffer; nothing is sent until the body returns. */
public final class Transaction {
    final List<Wire.Write> writes = new ArrayList<>();

    Transaction() {}

    static long ttlMs(Duration ttl) {
        if (ttl == null || ttl.isNegative() || ttl.isZero()) throw new InvalidException("ttl must be positive");
        return Math.max(1, ttl.toMillis());
    }

    private Transaction add(Wire.Write w) {
        writes.add(w);
        return this;
    }

    /** Creates or replaces a document. */
    public Transaction put(String collection, String key, Map<String, ?> fields) {
        return add(Wire.Write.put(collection, key, fields, 0, 0, 0));
    }

    /** Creates or replaces a document that expires after {@code ttl}. */
    public Transaction put(String collection, String key, Map<String, ?> fields, Duration ttl) {
        return add(Wire.Write.put(collection, key, fields, ttlMs(ttl), 0, 0));
    }

    /** Creates a document; the transaction fails with {@link ConflictException} if it exists. */
    public Transaction insert(String collection, String key, Map<String, ?> fields) {
        return add(Wire.Write.put(collection, key, fields, 0, 1, 0));
    }

    /** Replaces an existing document. */
    public Transaction replace(String collection, String key, Map<String, ?> fields) {
        return add(Wire.Write.put(collection, key, fields, 0, 2, 0));
    }

    /** Replaces a document still at {@code version}. */
    public Transaction replaceIfVersion(String collection, String key, long version, Map<String, ?> fields) {
        return add(Wire.Write.put(collection, key, fields, 0, 3, version));
    }

    /** Applies field changes to an existing document. */
    public Transaction update(String collection, String key, FieldOp... changes) {
        return add(Wire.Write.patch(collection, key, 0, 0, changes));
    }

    /** Applies field changes to a document still at {@code version}. */
    public Transaction updateIfVersion(String collection, String key, long version, FieldOp... changes) {
        return add(Wire.Write.patch(collection, key, 3, version, changes));
    }

    /** Deletes a document if it exists. */
    public Transaction delete(String collection, String key) {
        return add(Wire.Write.simple(collection, key, 3, 0, 0));
    }

    /** Deletes a document still at {@code version}. */
    public Transaction deleteIfVersion(String collection, String key, long version) {
        return add(Wire.Write.simple(collection, key, 3, 3, version));
    }

    /** Commits only if the document is at {@code version} (it is not written). */
    public Transaction requireVersion(String collection, String key, long version) {
        return add(Wire.Write.simple(collection, key, 5, 3, version));
    }

    /** Commits only if the document exists ({@code true}) or does not ({@code false}). */
    public Transaction requireExists(String collection, String key, boolean exists) {
        return add(Wire.Write.simple(collection, key, 5, exists ? 2 : 1, 0));
    }
}
