using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Storine;

/// <summary>
/// An arithmetic expression over a document's fields, the argument of an <see cref="Agg"/>:
/// <c>Agg.Field("price") * Agg.Field("qty")</c>, <c>Agg.Field("qty") - 1</c>, <c>-Agg.Field("price") + 2.5m</c>.
/// A string converts to a field (dotted path); numbers and decimals convert to constants. Immutable.
/// </summary>
public sealed class Expr
{
    internal const int FieldKind = 1, ConstKind = 2, AddKind = 3, SubKind = 4, MulKind = 5, NegKind = 6;

    private readonly int kind;
    private readonly string? path;
    private readonly object? value;
    private readonly Expr? a, b;

    private Expr(int kind, string? path, object? value, Expr? a, Expr? b)
    {
        this.kind = kind;
        this.path = path;
        this.value = value;
        this.a = a;
        this.b = b;
    }

    /// <summary>The field at <paramref name="path"/> (dotted for nested maps).</summary>
    public static Expr Field(string path) => string.IsNullOrEmpty(path)
        ? throw new InvalidException("a field path must not be empty")
        : new(FieldKind, path, null, null, null);

    /// <summary>A constant (number, decimal, ...).</summary>
    public static Expr Lit(object? value)
    {
        Values.Encode(value); // rejects unsupported values early
        return new(ConstKind, null, value, null, null);
    }

    /// <summary><c>this + other</c>.</summary>
    public Expr Plus(Expr other) => new(AddKind, null, null, this, other);

    /// <summary><c>this - other</c>.</summary>
    public Expr Minus(Expr other) => new(SubKind, null, null, this, other);

    /// <summary><c>this * other</c>.</summary>
    public Expr Times(Expr other) => new(MulKind, null, null, this, other);

    /// <summary><c>-this</c>.</summary>
    public Expr Neg() => new(NegKind, null, null, this, null);

    /// <summary><c>a + b</c>.</summary>
    public static Expr operator +(Expr a, Expr b) => a.Plus(b);

    /// <summary><c>a - b</c>.</summary>
    public static Expr operator -(Expr a, Expr b) => a.Minus(b);

    /// <summary><c>a * b</c>.</summary>
    public static Expr operator *(Expr a, Expr b) => a.Times(b);

    /// <summary><c>-a</c>.</summary>
    public static Expr operator -(Expr a) => a.Neg();

    /// <summary>A field path.</summary>
    public static implicit operator Expr(string path) => Field(path);

    /// <summary>An integer constant.</summary>
    public static implicit operator Expr(int v) => Lit(v);

    /// <summary>An integer constant.</summary>
    public static implicit operator Expr(long v) => Lit(v);

    /// <summary>A float constant.</summary>
    public static implicit operator Expr(double v) => Lit(v);

    /// <summary>An exact decimal constant.</summary>
    public static implicit operator Expr(decimal v) => Lit(v);

    /// <summary>An exact decimal constant (up to 38 digits).</summary>
    public static implicit operator Expr(BigDecimal v) => Lit(v);

    internal void Encode(Values.Writer w, int depth)
    {
        if (depth > Values.MaxDepth) throw new InvalidException("expression nesting is too deep (max 32)");
        w.U8(kind);
        switch (kind)
        {
            case FieldKind:
                w.Str(path!);
                break;
            case ConstKind:
                Values.Write(value, w, 0);
                break;
            case NegKind:
                a!.Encode(w, depth + 1);
                break;
            default:
                a!.Encode(w, depth + 1);
                b!.Encode(w, depth + 1);
                break;
        }
    }
}

/// <summary>
/// A group key of an <see cref="Aggregation"/>: a field path (a string converts) or <see cref="Agg.Bucket"/>. Its
/// output column is named after its field path. Immutable.
/// </summary>
public sealed class GroupKey
{
    private readonly bool bucket;
    private readonly long width, offset;

    private GroupKey(string path, bool bucket, long width, long offset)
    {
        if (string.IsNullOrEmpty(path)) throw new InvalidException("a group key path must not be empty");
        Name = path;
        this.bucket = bucket;
        this.width = width;
        this.offset = offset;
    }

    /// <summary>The output column name (the field path).</summary>
    public string Name { get; }

    /// <summary>Group by the value of the field at <paramref name="path"/>.</summary>
    public static GroupKey Field(string path) => new(path, false, 0, 0);

    internal static GroupKey Bucket(string path, long width, long offset) => width <= 0
        ? throw new InvalidException("bucket width must be positive")
        : new(path, true, width, offset);

    /// <summary>A field path.</summary>
    public static implicit operator GroupKey(string path) => Field(path);

    internal void Encode(Values.Writer w)
    {
        w.U8(bucket ? 2 : 1);
        w.Str(Name);
        if (bucket)
        {
            w.U64((ulong)width);
            w.U64((ulong)offset);
        }
    }
}

/// <summary>
/// One aggregate of an <see cref="Aggregation"/>, plus the static helpers that build aggregates, expressions and
/// buckets (<c>using static Storine.Agg;</c>): <c>Count()</c>, <c>Sum(Field("price") * Field("qty"))</c>,
/// <c>Avg("qty")</c>. A string argument is a field path. Immutable.
/// </summary>
public sealed class Agg
{
    private readonly int func;
    private readonly Expr? arg;

    private Agg(int func, Expr? arg)
    {
        this.func = func;
        this.arg = arg;
    }

    private static Agg Of(int func, Expr arg) => new(func, arg ?? throw new InvalidException("this aggregate needs an argument"));

    /// <summary>Number of matching rows (of the group).</summary>
    public static Agg Count() => new(1, null);

    /// <summary>Number of non-null values of <paramref name="arg"/>.</summary>
    public static Agg Count(Expr arg) => Of(1, arg);

    /// <summary>Exact sum (integers and decimals stay exact).</summary>
    public static Agg Sum(Expr arg) => Of(2, arg);

    /// <summary>Mean: a decimal with 6 more fractional digits for exact inputs, a double for floats.</summary>
    public static Agg Avg(Expr arg) => Of(3, arg);

    /// <summary>Smallest value in the total sort order.</summary>
    public static Agg Min(Expr arg) => Of(4, arg);

    /// <summary>Largest value in the total sort order.</summary>
    public static Agg Max(Expr arg) => Of(5, arg);

    /// <summary>Exact number of distinct non-null values.</summary>
    public static Agg CountDistinct(Expr arg) => Of(6, arg);

    /// <summary>HyperLogLog estimate of the number of distinct values (about 0.8 % standard error).</summary>
    public static Agg ApproxCountDistinct(Expr arg) => Of(7, arg);

    /// <summary>The field at <paramref name="path"/> as an expression.</summary>
    public static Expr Field(string path) => Expr.Field(path);

    /// <summary>A constant as an expression.</summary>
    public static Expr Lit(object? value) => Expr.Lit(value);

    /// <summary>Group key <c>offset + floor((v - offset) / width) * width</c> of an integer field; its column is <paramref name="path"/>.</summary>
    public static GroupKey Bucket(string path, long width, long offset = 0) => GroupKey.Bucket(path, width, offset);

    internal void Encode(Values.Writer w)
    {
        w.U8(func);
        if (arg is null)
        {
            w.U8(0);
        }
        else
        {
            w.U8(1);
            arg.Encode(w, 0);
        }
    }
}

/// <summary>
/// What <see cref="Collection.AggregateAsync"/> computes:
/// <c>new Aggregation().Agg("orders", Count()).Agg("revenue", Sum("price")).Where(Ge("day", 20)).GroupBy("region").OrderBy("-revenue").Limit(10)</c>.
/// Output columns are the group keys (named by their field paths), then the aggregates in the order added. Immutable.
/// </summary>
public sealed class Aggregation
{
    internal IReadOnlyList<(string Name, Agg Agg)> Aggs { get; private set; } = Array.Empty<(string, Agg)>();
    internal Filter? Filter { get; private set; }
    internal IReadOnlyList<GroupKey> Keys { get; private set; } = Array.Empty<GroupKey>();
    internal IReadOnlyList<string> Order { get; private set; } = Array.Empty<string>();
    internal uint LimitValue { get; private set; }
    internal ulong MemoryBudgetValue { get; private set; }
    internal ulong SpillBudgetValue { get; private set; }

    private Aggregation With(Action<Aggregation> change)
    {
        var c = (Aggregation)MemberwiseClone();
        change(c);
        return c;
    }

    /// <summary>Adds <paramref name="agg"/> as output column <paramref name="name"/> (after those added before).</summary>
    public Aggregation Agg(string name, Agg agg)
    {
        if (name is null || agg is null) throw new InvalidException("an aggregate needs a name and a function");
        if (Aggs.Any(a => a.Name == name)) throw new InvalidException($"duplicate aggregate name '{name}'");
        return With(c => c.Aggs = Aggs.Append((name, agg)).ToList());
    }

    /// <summary>Only documents matching <paramref name="filter"/>.</summary>
    public Aggregation Where(Filter? filter) => With(c => c.Filter = filter);

    /// <summary>Group keys: field paths and/or <see cref="Storine.Agg.Bucket"/> (default: one row over every match).</summary>
    public Aggregation GroupBy(params GroupKey[] keys) => With(c => c.Keys = keys.ToList());

    /// <summary>Sort by output columns: <c>"revenue"</c> ascending, <c>"-revenue"</c> descending (default: group-key order).</summary>
    public Aggregation OrderBy(params string[] columns) => With(c => c.Order = columns.ToList());

    /// <summary>At most <paramref name="n"/> rows (0: every group).</summary>
    public Aggregation Limit(int n) =>
        n < 0 ? throw new InvalidException("limit must not be negative") : With(c => c.LimitValue = (uint)n);

    /// <summary>Bytes of group tables kept in memory before spilling to disk (0: the server default, 256 MiB).</summary>
    public Aggregation MemoryBudget(ulong bytes) => With(c => c.MemoryBudgetValue = bytes);

    /// <summary>Bytes of spill runs allowed (0: the server default, 16 GiB).</summary>
    public Aggregation SpillBudget(ulong bytes) => With(c => c.SpillBudgetValue = bytes);

    /// <summary>Output column names: group keys, then aggregates.</summary>
    public IReadOnlyList<string> Columns => Keys.Select(k => k.Name).Concat(Aggs.Select(a => a.Name)).ToList();
}

/// <summary>
/// A result row: column name to value, enumerated in column order (also indexable by position). Read-only.
/// </summary>
public sealed class ResultRow : IReadOnlyDictionary<string, object?>
{
    private readonly List<KeyValuePair<string, object?>> cells;
    private readonly Dictionary<string, int> index = new(StringComparer.Ordinal);

    internal ResultRow(List<KeyValuePair<string, object?>> cells)
    {
        this.cells = cells;
        for (int i = 0; i < cells.Count; i++) index.TryAdd(cells[i].Key, i);
    }

    /// <summary>The value of column <paramref name="name"/> (<see cref="KeyNotFoundException"/> if there is none).</summary>
    public object? this[string name] => index.TryGetValue(name, out int i) ? cells[i].Value : throw new KeyNotFoundException(name);

    /// <summary>The value of the column at <paramref name="position"/>.</summary>
    public object? this[int position] => cells[position].Value;

    /// <summary>Column names in order.</summary>
    public IEnumerable<string> Keys => cells.Select(c => c.Key);

    /// <summary>Values in column order.</summary>
    public IEnumerable<object?> Values => cells.Select(c => c.Value);

    /// <summary>Number of columns.</summary>
    public int Count => cells.Count;

    /// <inheritdoc/>
    public bool ContainsKey(string key) => index.ContainsKey(key);

    /// <inheritdoc/>
    public bool TryGetValue(string key, out object? value)
    {
        bool ok = index.TryGetValue(key, out int i);
        value = ok ? cells[i].Value : null;
        return ok;
    }

    /// <inheritdoc/>
    public IEnumerator<KeyValuePair<string, object?>> GetEnumerator() => cells.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

/// <summary>
/// What <see cref="Collection.AggregateAsync"/> returns. <c>Matched</c>: documents that passed the filter;
/// <c>PrunedRowGroups</c>: columnar row groups skipped by zone maps; <c>SpilledBytes &gt; 0</c>: the query outgrew its
/// memory budget and spilled to disk.
/// </summary>
public sealed record AggregateResult(IReadOnlyList<ResultRow> Rows, ulong Matched, ulong PrunedRowGroups, ulong SpilledBytes);

/// <summary>One row of <see cref="Collection.ScanColumnsAsync"/>: the document key and each requested path's value (null if missing).</summary>
public sealed record ColumnRow(string Key, ResultRow Fields)
{
    /// <summary>The value of the requested field <paramref name="path"/>.</summary>
    public object? this[string path] => Fields[path];
}
