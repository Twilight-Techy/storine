package io.storine;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** A named set of documents. Every method is one atomic, exactly-once operation. Thread-safe. */
public final class Collection {
    private final Storine client;
    private final String name;

    Collection(Storine client, String name) {
        this.client = client;
        this.name = name;
    }

    public String name() {
        return name;
    }

    private long one(Wire.Write w) {
        Long v = client.commit(List.of(w)).get(0);
        return v == null ? 0 : v;
    }

    /** The document {@code key}, or empty if it does not exist. */
    public Optional<Document> get(String key) {
        Storine.Reply r = client.request(Wire.OP_DOC_GET, Wire.get(name, key));
        if (r.status() == Wire.ST_NOT_FOUND) return Optional.empty();
        if (r.status() != Wire.ST_OK) throw Storine.error(r.status(), r.body());
        return Optional.of(new Document(key, r.version(), new Values.Reader(r.body()).fields(1)));
    }

    /** Creates or replaces the document; returns its new version. */
    public long put(String key, Map<String, ?> fields) {
        return one(Wire.Write.put(name, key, fields, 0, 0, 0));
    }

    /** {@link #put} for a document that expires after {@code ttl}. */
    public long put(String key, Map<String, ?> fields, Duration ttl) {
        return one(Wire.Write.put(name, key, fields, Transaction.ttlMs(ttl), 0, 0));
    }

    /** Creates the document; {@link ConflictException} if it already exists. */
    public long insert(String key, Map<String, ?> fields) {
        return one(Wire.Write.put(name, key, fields, 0, 1, 0));
    }

    /** Replaces an existing document; {@link ConflictException} if it does not exist. */
    public long replace(String key, Map<String, ?> fields) {
        return one(Wire.Write.put(name, key, fields, 0, 2, 0));
    }

    /** Replaces the document only if it is still at {@code version}; {@link ConflictException} otherwise. */
    public long replaceIfVersion(String key, long version, Map<String, ?> fields) {
        return one(Wire.Write.put(name, key, fields, 0, 3, version));
    }

    /**
     * Applies field changes atomically on the server; returns the new version.
     * {@code accounts.update("alice", add("balance", new BigDecimal("-25.00")).min(0))}.
     * {@link NotFoundException} if the document does not exist; {@link InvalidException} if a bound would be violated.
     */
    public long update(String key, FieldOp... changes) {
        return one(Wire.Write.patch(name, key, 0, 0, changes));
    }

    /** {@link #update} applied only if the document is still at {@code version}; {@link ConflictException} otherwise. */
    public long updateIfVersion(String key, long version, FieldOp... changes) {
        return one(Wire.Write.patch(name, key, 3, version, changes));
    }

    /** Deletes the document; returns false if it did not exist. */
    public boolean delete(String key) {
        try {
            one(Wire.Write.simple(name, key, 3, 2, 0));
            return true;
        } catch (ConflictException e) {
            if (e.currentVersion() == null) return false;
            throw e;
        }
    }

    /** Deletes the document only if it is still at {@code version}; {@link ConflictException} otherwise. */
    public void deleteIfVersion(String key, long version) {
        one(Wire.Write.simple(name, key, 3, 3, version));
    }

    private Wire.Page query(Query q, String after) {
        return Wire.page(client.call(Wire.OP_DOC_QUERY, Wire.query(name, q, after)).body());
    }

    /** Documents matching {@code query}. */
    public List<Document> find(Query query) {
        return query(query, null).docs();
    }

    /** Documents matching {@code where} (at most 1,000; use {@link #find(Query)} to sort, limit and page). */
    public List<Document> find(Filter where) {
        return find(new Query().where(where));
    }

    /** The first document matching {@code query}, or empty. */
    public Optional<Document> first(Query query) {
        return find(query.limit(1)).stream().findFirst();
    }

    /** Number of documents matching {@code where} ({@code null}: all). */
    public long count(Filter where) {
        return query(new Query().where(where).limit(1), null).total();
    }

    /** Number of documents. */
    public long count() {
        return count(null);
    }

    /** Every document matching {@code where} ({@code null}: all) in key order, fetched lazily page by page. */
    public Stream<Document> scan(Filter where) {
        Iterator<Document> it = new Iterator<>() {
            private Iterator<Document> page = List.<Document>of().iterator();
            private String after;
            private boolean done;

            @Override
            public boolean hasNext() {
                while (!page.hasNext() && !done) {
                    Wire.Page p = query(new Query().where(where).limit(1000), after);
                    page = p.docs().iterator();
                    after = p.nextKey();
                    done = after == null;
                }
                return page.hasNext();
            }

            @Override
            public Document next() {
                if (!hasNext()) throw new NoSuchElementException();
                return page.next();
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED | Spliterator.NONNULL), false);
    }

    /** Every document, in key order. */
    public Stream<Document> scan() {
        return scan(null);
    }

    /**
     * The {@code k} documents whose {@code field} (a list of numbers) is most similar to {@code vector} (cosine), best
     * first. Each document's similarity is {@link Document#score()}.
     */
    public List<Document> nearest(String field, float[] vector, int k) {
        return nearest(field, vector, k, null, false);
    }

    /**
     * {@link #nearest(String, float[], int)} among the documents matching {@code where} ({@code null}: all). Uses the
     * field's vector index when it has one (approximate candidates, exact scores); {@code exact} scores every candidate.
     */
    public List<Document> nearest(String field, float[] vector, int k, Filter where, boolean exact) {
        if (k < 0) throw new InvalidException("k must not be negative");
        Wire.Nearest n = new Wire.Nearest(field, vector.clone(), k, exact);
        return Wire.page(client.call(Wire.OP_DOC_QUERY, Wire.query(name, new Query().where(where).limit(k), null, n)).body()).docs();
    }

    /**
     * Indexes a top-level field holding {@code dims} numbers for {@link #nearest} (at most 4 per collection); existing
     * documents included. Writes of other values to that field are then refused.
     */
    public void createVectorIndex(String field, int dims) {
        if (dims <= 0) throw new InvalidException("dims must be positive");
        client.call(Wire.OP_DOC_ADMIN, Wire.createVectorIndex(name, field, dims));
    }

    public void dropVectorIndex(String field) {
        client.call(Wire.OP_DOC_ADMIN, Wire.admin(8, name, field));
    }

    /** The collection's vector indexes. */
    public List<VectorIndex> vectorIndexes() {
        return Wire.vectorIndexes(client.call(Wire.OP_DOC_INFO, Wire.admin(9, name, null)).body());
    }

    /** Indexes a top-level field (at most 16 per collection); existing documents included. */
    public void createIndex(String field) {
        client.call(Wire.OP_DOC_ADMIN, Wire.admin(1, name, field));
    }

    public void dropIndex(String field) {
        client.call(Wire.OP_DOC_ADMIN, Wire.admin(2, name, field));
    }

    /** Deletes every document and the collection's metadata; returns the number deleted. */
    public long drop() {
        return client.call(Wire.OP_DOC_ADMIN, Wire.admin(4, name, null)).version();
    }

    /** Indexes and metadata of the collection. */
    public CollectionInfo info() {
        return Wire.info(new Values.Reader(client.call(Wire.OP_DOC_INFO, Wire.admin(6, name, null)).body()));
    }
}
