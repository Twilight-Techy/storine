package io.storine;

/**
 * An arithmetic expression over a document's fields, the argument of an {@link Agg}:
 * {@code field("price").times(field("qty"))}, {@code field("qty").minus(1)}, {@code field("price").neg().plus(lit(new BigDecimal("2.5")))}.
 * Build with {@link Agg#field} and {@link Agg#lit}. Immutable.
 */
public final class Expr {
    static final int FIELD = 1, CONST = 2, ADD = 3, SUB = 4, MUL = 5, NEG = 6;

    final int kind;
    final String path;
    final Object value;
    final Expr a, b;

    private Expr(int kind, String path, Object value, Expr a, Expr b) {
        this.kind = kind;
        this.path = path;
        this.value = value;
        this.a = a;
        this.b = b;
    }

    static Expr field(String path) {
        if (path == null || path.isEmpty()) throw new InvalidException("a field path must not be empty");
        return new Expr(FIELD, path, null, null, null);
    }

    static Expr lit(Object value) {
        Values.encode(value); // rejects unsupported values early
        return new Expr(CONST, null, value, null, null);
    }

    private static Expr of(Object x) {
        return x instanceof Expr e ? e : lit(x);
    }

    /** {@code this + other}. */
    public Expr plus(Expr other) {
        return new Expr(ADD, null, null, this, other);
    }

    /** {@code this + n} ({@code n} a constant number). */
    public Expr plus(Number n) {
        return plus(of(n));
    }

    /** {@code this - other}. */
    public Expr minus(Expr other) {
        return new Expr(SUB, null, null, this, other);
    }

    /** {@code this - n}. */
    public Expr minus(Number n) {
        return minus(of(n));
    }

    /** {@code this * other}. */
    public Expr times(Expr other) {
        return new Expr(MUL, null, null, this, other);
    }

    /** {@code this * n}. */
    public Expr times(Number n) {
        return times(of(n));
    }

    /** {@code -this}. */
    public Expr neg() {
        return new Expr(NEG, null, null, this, null);
    }

    void encode(Values.Out out, int depth) {
        if (depth > Values.MAX_DEPTH) throw new InvalidException("expression nesting is too deep (max 32)");
        out.u8(kind);
        switch (kind) {
            case FIELD -> out.str(path);
            case CONST -> Values.encode(value, out, 0);
            case NEG -> a.encode(out, depth + 1);
            default -> {
                a.encode(out, depth + 1);
                b.encode(out, depth + 1);
            }
        }
    }
}
