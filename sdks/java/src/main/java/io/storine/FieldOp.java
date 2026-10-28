package io.storine;

/**
 * One atomic change applied server-side by {@code update}: {@link #set}, {@link #unset}, {@link #add},
 * {@link #append}. Immutable.
 */
public final class FieldOp {
    final int kind;
    final String path;
    final Object value;
    final Object min, max;
    final boolean hasMin, hasMax;

    private FieldOp(int kind, String path, Object value, Object min, boolean hasMin, Object max, boolean hasMax) {
        this.kind = kind;
        this.path = path;
        this.value = value;
        this.min = min;
        this.hasMin = hasMin;
        this.max = max;
        this.hasMax = hasMax;
    }

    /** Sets the field at {@code path} (dotted for nested maps) to {@code value}. */
    public static FieldOp set(String path, Object value) {
        return new FieldOp(1, path, value, null, false, null, false);
    }

    /** Removes the field at {@code path}. */
    public static FieldOp unset(String path) {
        return new FieldOp(2, path, null, null, false, null, false);
    }

    /**
     * Adds {@code amount} (negative to subtract) exactly and atomically. Bound the result with {@link #min} /
     * {@link #max}: {@code add("balance", new BigDecimal("-25.00")).min(0)} never overdraws.
     */
    public static FieldOp add(String path, Object amount) {
        return new FieldOp(3, path, amount, null, false, null, false);
    }

    /** Appends {@code value} to the list at {@code path} (a missing field becomes a one-element list). */
    public static FieldOp append(String path, Object value) {
        return new FieldOp(4, path, value, null, false, null, false);
    }

    /** Refuses ({@link InvalidException}) an add whose result would be below {@code bound}. */
    public FieldOp min(Object bound) {
        return new FieldOp(kind, path, value, bound, true, max, hasMax);
    }

    /** Refuses ({@link InvalidException}) an add whose result would be above {@code bound}. */
    public FieldOp max(Object bound) {
        return new FieldOp(kind, path, value, min, hasMin, bound, true);
    }

    void encode(Values.Out out) {
        out.u8(kind);
        out.str(path);
        if (kind == 1 || kind == 4) {
            Values.encode(value, out, 0);
        } else if (kind == 3) {
            Values.encode(value, out, 0);
            out.u8((hasMin ? 1 : 0) | (hasMax ? 2 : 0));
            if (hasMin) Values.encode(min, out, 0);
            if (hasMax) Values.encode(max, out, 0);
        }
    }
}
