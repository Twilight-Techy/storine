package io.storine;

/**
 * A group key of an {@link Aggregation}: a field's value ({@code "region"} or {@link Agg#bucket}). Its output column is
 * named after its field path. Immutable.
 */
public final class GroupKey {
    final String path;
    final boolean bucket;
    final long width, offset;

    private GroupKey(String path, boolean bucket, long width, long offset) {
        if (path == null || path.isEmpty()) throw new InvalidException("a group key path must not be empty");
        this.path = path;
        this.bucket = bucket;
        this.width = width;
        this.offset = offset;
    }

    static GroupKey field(String path) {
        return new GroupKey(path, false, 0, 0);
    }

    static GroupKey bucket(String path, long width, long offset) {
        if (width <= 0) throw new InvalidException("bucket width must be positive");
        return new GroupKey(path, true, width, offset);
    }

    /** The output column name (the field path). */
    public String name() {
        return path;
    }

    void encode(Values.Out out) {
        out.u8(bucket ? 2 : 1);
        out.str(path);
        if (bucket) {
            out.u64(width);
            out.u64(offset);
        }
    }
}
