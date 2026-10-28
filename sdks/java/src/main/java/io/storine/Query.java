package io.storine;

import java.util.List;

/**
 * What {@code find} returns: {@code new Query().where(gt("price", 10)).orderBy("-price").limit(10)}. The empty
 * query matches every document (up to 1,000). Immutable.
 */
public final class Query {
    final Filter where;
    final List<String> orderBy;
    final int limit, offset;

    public Query() {
        this(null, List.of(), 0, 0);
    }

    private Query(Filter where, List<String> orderBy, int limit, int offset) {
        this.where = where;
        this.orderBy = orderBy;
        this.limit = limit;
        this.offset = offset;
    }

    /** Only documents matching {@code filter}. */
    public Query where(Filter filter) {
        return new Query(filter, orderBy, limit, offset);
    }

    /** Sort keys: {@code "price"} ascending, {@code "-price"} descending (default: key order). */
    public Query orderBy(String... fields) {
        return new Query(where, List.of(fields), limit, offset);
    }

    /** At most {@code n} documents (default 1,000). */
    public Query limit(int n) {
        if (n < 0) throw new InvalidException("limit must not be negative");
        return new Query(where, orderBy, n, offset);
    }

    /** Skips the first {@code n} matches. */
    public Query offset(int n) {
        if (n < 0) throw new InvalidException("offset must not be negative");
        return new Query(where, orderBy, limit, n);
    }
}
