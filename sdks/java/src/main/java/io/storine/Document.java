package io.storine;

import java.util.Map;

/** A stored document: its key, version and fields (an unmodifiable map). */
public record Document(String key, long version, Map<String, Object> fields) {
    public Document {
        fields = java.util.Collections.unmodifiableMap(fields);
    }

    /** The field {@code name}, or {@code null}. */
    public Object get(String name) {
        return fields.get(name);
    }

    /** Similarity of a {@link Collection#nearest} result (the {@code $score} field), or {@code null} for other queries. */
    public Double score() {
        return fields.get("$score") instanceof Double d ? d : null;
    }
}
