package io.storine;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * What {@link Collection#aggregate} returns. Each row maps output column to value in column order (group keys, then
 * aggregates). {@code matched}: documents that passed the filter; {@code prunedRowGroups}: columnar row groups skipped
 * by zone maps; {@code spilledBytes > 0}: the query outgrew its memory budget and spilled to disk.
 */
public record AggregateResult(List<LinkedHashMap<String, Object>> rows, long matched, long prunedRowGroups, long spilledBytes) {
    public AggregateResult {
        rows = List.copyOf(rows);
    }
}
