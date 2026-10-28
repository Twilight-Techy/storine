package io.storine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** A query condition. Build with the static methods ({@code import static io.storine.Filter.*}). Immutable. */
public final class Filter {
    final int kind;
    final String path;
    final List<Object> values;
    final List<Filter> children;

    private Filter(int kind, String path, List<Object> values, List<Filter> children) {
        this.kind = kind;
        this.path = path;
        this.values = values;
        this.children = children;
    }

    private static Filter cmp(int kind, String path, Object value) {
        return new Filter(kind, path, Arrays.asList(value), List.of());
    }

    /** Field equals value (numbers compare numerically; {@code null} matches missing fields). */
    public static Filter eq(String path, Object value) {
        return cmp(1, path, value);
    }

    public static Filter ne(String path, Object value) {
        return cmp(2, path, value);
    }

    public static Filter lt(String path, Object value) {
        return cmp(3, path, value);
    }

    public static Filter le(String path, Object value) {
        return cmp(4, path, value);
    }

    public static Filter gt(String path, Object value) {
        return cmp(5, path, value);
    }

    public static Filter ge(String path, Object value) {
        return cmp(6, path, value);
    }

    /** {@code low <= field <= high}. */
    public static Filter between(String path, Object low, Object high) {
        return new Filter(7, path, Arrays.asList(low, high), List.of());
    }

    /** Field equals any of {@code values}. */
    public static Filter in(String path, Collection<?> values) {
        return new Filter(8, path, new ArrayList<>(values), List.of());
    }

    /** Field equals any of {@code values}. */
    public static Filter in(String path, Object... values) {
        return new Filter(8, path, Arrays.asList(values.clone()), List.of());
    }

    /** Text field starts with {@code text}. */
    public static Filter prefix(String path, String text) {
        return cmp(9, path, text);
    }

    /** Text field contains {@code value}, or list field has an element equal to {@code value}. */
    public static Filter contains(String path, Object value) {
        return cmp(10, path, value);
    }

    /** Field is present and not null. */
    public static Filter exists(String path) {
        return new Filter(11, path, List.of(), List.of());
    }

    /** Field is absent or null. */
    public static Filter missing(String path) {
        return new Filter(12, path, List.of(), List.of());
    }

    /** Document key starts with {@code text}. */
    public static Filter keyPrefix(String text) {
        return new Filter(13, "", List.of(text), List.of());
    }

    public static Filter and(Filter... filters) {
        return new Filter(14, "", List.of(), List.of(filters));
    }

    public static Filter or(Filter... filters) {
        return new Filter(15, "", List.of(), List.of(filters));
    }

    public static Filter not(Filter filter) {
        return new Filter(16, "", List.of(), List.of(filter));
    }

    /** Every field equals the given value: {@code match(Map.of("kind", "book"))}. */
    public static Filter match(Map<String, ?> fields) {
        List<Filter> parts = new ArrayList<>();
        new TreeMap<>(fields).forEach((k, v) -> parts.add(eq(k, v)));
        return parts.size() == 1 ? parts.get(0) : and(parts.toArray(new Filter[0]));
    }

    void encode(Values.Out out, int depth) {
        if (depth > Values.MAX_DEPTH) throw new InvalidException("filter nesting is too deep (max 32)");
        out.u8(kind);
        switch (kind) {
            case 1, 2, 3, 4, 5, 6, 10 -> {
                out.str(path);
                Values.encode(values.get(0), out, 0);
            }
            case 7 -> {
                out.str(path);
                Values.encode(values.get(0), out, 0);
                Values.encode(values.get(1), out, 0);
            }
            case 8 -> {
                out.str(path);
                out.u32(values.size());
                for (Object v : values) Values.encode(v, out, 0);
            }
            case 9 -> {
                out.str(path);
                out.str((String) values.get(0));
            }
            case 11, 12 -> out.str(path);
            case 13 -> out.str((String) values.get(0));
            case 14, 15 -> {
                out.u32(children.size());
                for (Filter c : children) c.encode(out, depth + 1);
            }
            default -> children.get(0).encode(out, depth + 1);
        }
    }
}
