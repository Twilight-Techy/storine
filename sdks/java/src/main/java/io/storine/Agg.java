package io.storine;

/**
 * One aggregate of an {@link Aggregation}, plus the static helpers that build aggregates, expressions and buckets
 * ({@code import static io.storine.Agg.*}):
 *
 * <pre>{@code
 * new Aggregation()
 *     .agg("orders", count())
 *     .agg("revenue", sum(field("price").times(field("qty"))))
 *     .agg("avg_qty", avg("qty"))
 *     .groupBy("region", bucket("day", 7, 0))
 * }</pre>
 *
 * A {@code String} argument is a field path. Immutable.
 */
public final class Agg {
    static final int COUNT = 1, SUM = 2, AVG = 3, MIN = 4, MAX = 5, COUNT_DISTINCT = 6, APPROX_COUNT_DISTINCT = 7;

    final int func;
    final Expr arg;

    private Agg(int func, Expr arg) {
        this.func = func;
        this.arg = arg;
    }

    private static Expr path(String p) {
        return Expr.field(p);
    }

    private static Agg of(int func, Expr arg) {
        if (arg == null) throw new InvalidException("this aggregate needs an argument");
        return new Agg(func, arg);
    }

    /** Number of matching rows (of the group). */
    public static Agg count() {
        return new Agg(COUNT, null);
    }

    /** Number of non-null values of the field. */
    public static Agg count(String field) {
        return of(COUNT, path(field));
    }

    /** Number of non-null values of the expression. */
    public static Agg count(Expr arg) {
        return of(COUNT, arg);
    }

    /** Exact sum (integers and decimals stay exact). */
    public static Agg sum(String field) {
        return of(SUM, path(field));
    }

    public static Agg sum(Expr arg) {
        return of(SUM, arg);
    }

    /** Mean: a decimal with 6 more fractional digits for exact inputs, a double for floats. */
    public static Agg avg(String field) {
        return of(AVG, path(field));
    }

    public static Agg avg(Expr arg) {
        return of(AVG, arg);
    }

    /** Smallest value in the total sort order. */
    public static Agg min(String field) {
        return of(MIN, path(field));
    }

    public static Agg min(Expr arg) {
        return of(MIN, arg);
    }

    /** Largest value in the total sort order. */
    public static Agg max(String field) {
        return of(MAX, path(field));
    }

    public static Agg max(Expr arg) {
        return of(MAX, arg);
    }

    /** Exact number of distinct non-null values. */
    public static Agg countDistinct(String field) {
        return of(COUNT_DISTINCT, path(field));
    }

    public static Agg countDistinct(Expr arg) {
        return of(COUNT_DISTINCT, arg);
    }

    /** HyperLogLog estimate of the number of distinct values (about 0.8 % standard error). */
    public static Agg approxCountDistinct(String field) {
        return of(APPROX_COUNT_DISTINCT, path(field));
    }

    public static Agg approxCountDistinct(Expr arg) {
        return of(APPROX_COUNT_DISTINCT, arg);
    }

    /** The field at {@code path} (dotted) as an expression. */
    public static Expr field(String path) {
        return Expr.field(path);
    }

    /** A constant (number, decimal, ...) as an expression. */
    public static Expr lit(Object value) {
        return Expr.lit(value);
    }

    /** Group key {@code offset + floor((v - offset) / width) * width} of an integer field; its column is {@code path}. */
    public static GroupKey bucket(String path, long width, long offset) {
        return GroupKey.bucket(path, width, offset);
    }

    /** {@link #bucket(String, long, long)} with offset 0. */
    public static GroupKey bucket(String path, long width) {
        return GroupKey.bucket(path, width, 0);
    }

    void encode(Values.Out out) {
        out.u8(func);
        if (arg == null) {
            out.u8(0);
        } else {
            out.u8(1);
            arg.encode(out, 0);
        }
    }
}
