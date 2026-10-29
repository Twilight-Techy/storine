package io.storine;

import java.util.Map;

/** One row of {@link Collection#scanColumns}: the document key and each requested field path's value (null if missing). */
public record ColumnRow(String key, Map<String, Object> values) {
    public ColumnRow {
        values = java.util.Collections.unmodifiableMap(values);
    }

    /** The value of the requested field {@code path}. */
    public Object get(String path) {
        return values.get(path);
    }
}
