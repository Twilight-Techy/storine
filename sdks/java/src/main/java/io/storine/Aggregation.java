package io.storine;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What {@link Collection#aggregate} computes, built like a {@link Query}:
 * {@code new Aggregation().agg("orders", count()).agg("revenue", sum("price")).where(ge("day", 20)).groupBy("region").orderBy("-revenue").limit(10)}.
 * Output columns are the group keys (named by their field paths), then the aggregates in the order added. Immutable.
 */
public final class Aggregation {
    final Map<String, Agg> aggs;
    final Filter where;
    final List<GroupKey> groupBy;
    final List<String> orderBy;
    final int limit;
    final long memoryBudget, spillBudget;

    public Aggregation() {
        this(new LinkedHashMap<>(), null, List.of(), List.of(), 0, 0, 0);
    }

    private Aggregation(Map<String, Agg> aggs, Filter where, List<GroupKey> groupBy, List<String> orderBy, int limit,
            long memoryBudget, long spillBudget) {
        this.aggs = aggs;
        this.where = where;
        this.groupBy = groupBy;
        this.orderBy = orderBy;
        this.limit = limit;
        this.memoryBudget = memoryBudget;
        this.spillBudget = spillBudget;
    }

    /** Adds the aggregate {@code agg} as output column {@code name} (after those added before). */
    public Aggregation agg(String name, Agg agg) {
        if (name == null || agg == null) throw new InvalidException("an aggregate needs a name and a function");
        if (aggs.containsKey(name)) throw new InvalidException("duplicate aggregate name '" + name + "'");
        Map<String, Agg> m = new LinkedHashMap<>(aggs);
        m.put(name, agg);
        return new Aggregation(m, where, groupBy, orderBy, limit, memoryBudget, spillBudget);
    }

    /** Only documents matching {@code filter}. */
    public Aggregation where(Filter filter) {
        return new Aggregation(aggs, filter, groupBy, orderBy, limit, memoryBudget, spillBudget);
    }

    /** Group keys: field paths ({@code String}) and/or {@link Agg#bucket} keys (default: one row over every match). */
    public Aggregation groupBy(Object... keys) {
        List<GroupKey> ks = new ArrayList<>();
        for (Object k : keys) {
            if (k instanceof String s) ks.add(GroupKey.field(s));
            else if (k instanceof GroupKey g) ks.add(g);
            else throw new InvalidException("group keys are field paths or Agg.bucket(...)");
        }
        return new Aggregation(aggs, where, List.copyOf(ks), orderBy, limit, memoryBudget, spillBudget);
    }

    /** Sort by output columns: {@code "revenue"} ascending, {@code "-revenue"} descending (default: group-key order). */
    public Aggregation orderBy(String... columns) {
        return new Aggregation(aggs, where, groupBy, List.of(columns), limit, memoryBudget, spillBudget);
    }

    /** At most {@code n} rows (0: every group). */
    public Aggregation limit(int n) {
        if (n < 0) throw new InvalidException("limit must not be negative");
        return new Aggregation(aggs, where, groupBy, orderBy, n, memoryBudget, spillBudget);
    }

    /** Bytes of group tables kept in memory before spilling to disk (0: the server default, 256 MiB). */
    public Aggregation memoryBudget(long bytes) {
        if (bytes < 0) throw new InvalidException("memoryBudget must not be negative");
        return new Aggregation(aggs, where, groupBy, orderBy, limit, bytes, spillBudget);
    }

    /** Bytes of spill runs allowed (0: the server default, 16 GiB). */
    public Aggregation spillBudget(long bytes) {
        if (bytes < 0) throw new InvalidException("spillBudget must not be negative");
        return new Aggregation(aggs, where, groupBy, orderBy, limit, memoryBudget, bytes);
    }

    /** Output column names: group keys, then aggregates. */
    public List<String> columns() {
        List<String> cols = new ArrayList<>();
        for (GroupKey k : groupBy) cols.add(k.name());
        cols.addAll(aggs.keySet());
        return cols;
    }
}
