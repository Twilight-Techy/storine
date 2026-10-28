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
}
