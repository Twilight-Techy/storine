using System;
using System.Collections.Generic;
using System.Linq;

namespace Storine;

/// <summary>
/// One atomic change applied server-side by <c>UpdateAsync</c>: <see cref="Set"/>, <see cref="Unset"/>,
/// <see cref="Add"/>, <see cref="Append"/>. Immutable.
/// </summary>
public sealed record FieldOp
{
    internal int Kind { get; init; }
    internal string Path { get; init; } = "";
    internal object? Value { get; init; }
    internal object? MinBound { get; init; }
    internal object? MaxBound { get; init; }
    internal bool HasMin { get; init; }
    internal bool HasMax { get; init; }

    /// <summary>Sets the field at <paramref name="path"/> (dotted for nested maps) to <paramref name="value"/>.</summary>
    public static FieldOp Set(string path, object? value) => new() { Kind = 1, Path = path, Value = value };

    /// <summary>Removes the field at <paramref name="path"/>.</summary>
    public static FieldOp Unset(string path) => new() { Kind = 2, Path = path };

    /// <summary>
    /// Adds <paramref name="amount"/> (negative to subtract) exactly and atomically. Bound the result with
    /// <see cref="Min"/> / <see cref="Max"/>: <c>FieldOp.Add("balance", -25.00m).Min(0)</c> never overdraws.
    /// </summary>
    public static FieldOp Add(string path, object amount) => new() { Kind = 3, Path = path, Value = amount };

    /// <summary>Appends <paramref name="value"/> to the list at <paramref name="path"/> (a missing field becomes a one-element list).</summary>
    public static FieldOp Append(string path, object? value) => new() { Kind = 4, Path = path, Value = value };

    /// <summary>Refuses (<see cref="InvalidException"/>) an Add whose result would be below <paramref name="bound"/>.</summary>
    public FieldOp Min(object bound) => this with { MinBound = bound, HasMin = true };

    /// <summary>Refuses (<see cref="InvalidException"/>) an Add whose result would be above <paramref name="bound"/>.</summary>
    public FieldOp Max(object bound) => this with { MaxBound = bound, HasMax = true };

    internal void Encode(Values.Writer w)
    {
        w.U8(Kind);
        w.Str(Path);
        if (Kind is 1 or 4)
        {
            Values.Write(Value, w, 0);
        }
        else if (Kind == 3)
        {
            Values.Write(Value, w, 0);
            w.U8((HasMin ? 1 : 0) | (HasMax ? 2 : 0));
            if (HasMin) Values.Write(MinBound, w, 0);
            if (HasMax) Values.Write(MaxBound, w, 0);
        }
    }
}

/// <summary>
/// A query condition: <c>Filter.Gt("price", 10m) &amp; Filter.Eq("kind", "book")</c>. Combine with <c>&amp;</c>,
/// <c>|</c> and <c>!</c>. Immutable.
/// </summary>
public sealed class Filter
{
    private readonly int kind;
    private readonly string path;
    private readonly object?[] values;
    private readonly Filter[] children;

    private Filter(int kind, string path, object?[] values, Filter[] children)
    {
        this.kind = kind;
        this.path = path;
        this.values = values;
        this.children = children;
    }

    private static Filter Cmp(int kind, string path, object? value) => new(kind, path, new[] { value }, Array.Empty<Filter>());

    /// <summary>Field equals value (numbers compare numerically; null matches missing fields).</summary>
    public static Filter Eq(string path, object? value) => Cmp(1, path, value);

    /// <summary>Field differs from value.</summary>
    public static Filter Ne(string path, object? value) => Cmp(2, path, value);

    /// <summary>Field &lt; value.</summary>
    public static Filter Lt(string path, object value) => Cmp(3, path, value);

    /// <summary>Field &lt;= value.</summary>
    public static Filter Le(string path, object value) => Cmp(4, path, value);

    /// <summary>Field &gt; value.</summary>
    public static Filter Gt(string path, object value) => Cmp(5, path, value);

    /// <summary>Field &gt;= value.</summary>
    public static Filter Ge(string path, object value) => Cmp(6, path, value);

    /// <summary><c>low &lt;= field &lt;= high</c>.</summary>
    public static Filter Between(string path, object low, object high) => new(7, path, new[] { low, high }, Array.Empty<Filter>());

    /// <summary>Field equals any of <paramref name="values"/>.</summary>
    public static Filter In(string path, params object?[] values) => new(8, path, (object?[])values.Clone(), Array.Empty<Filter>());

    /// <summary>Text field starts with <paramref name="text"/>.</summary>
    public static Filter Prefix(string path, string text) => Cmp(9, path, text);

    /// <summary>Text field contains value, or list field has an element equal to value.</summary>
    public static Filter Contains(string path, object value) => Cmp(10, path, value);

    /// <summary>Field is present and not null.</summary>
    public static Filter Exists(string path) => new(11, path, Array.Empty<object?>(), Array.Empty<Filter>());

    /// <summary>Field is absent or null.</summary>
    public static Filter Missing(string path) => new(12, path, Array.Empty<object?>(), Array.Empty<Filter>());

    /// <summary>Document key starts with <paramref name="text"/>.</summary>
    public static Filter KeyPrefix(string text) => new(13, "", new object?[] { text }, Array.Empty<Filter>());

    /// <summary>Every filter holds.</summary>
    public static Filter And(params Filter[] filters) => new(14, "", Array.Empty<object?>(), (Filter[])filters.Clone());

    /// <summary>At least one filter holds.</summary>
    public static Filter Or(params Filter[] filters) => new(15, "", Array.Empty<object?>(), (Filter[])filters.Clone());

    /// <summary>The filter does not hold.</summary>
    public static Filter Not(Filter filter) => new(16, "", Array.Empty<object?>(), new[] { filter });

    /// <summary>Every field equals the given value: <c>Filter.Match(new Doc { ["kind"] = "book" })</c>.</summary>
    public static Filter Match(IDictionary<string, object?> fields)
    {
        var parts = fields.OrderBy(kv => kv.Key, StringComparer.Ordinal).Select(kv => Eq(kv.Key, kv.Value)).ToArray();
        return parts.Length == 1 ? parts[0] : And(parts);
    }

    /// <summary><c>a AND b</c>.</summary>
    public static Filter operator &(Filter a, Filter b) => And(a, b);

    /// <summary><c>a OR b</c>.</summary>
    public static Filter operator |(Filter a, Filter b) => Or(a, b);

    /// <summary><c>NOT a</c>.</summary>
    public static Filter operator !(Filter a) => Not(a);

    internal void Encode(Values.Writer w, int depth)
    {
        if (depth > Values.MaxDepth) throw new InvalidException("filter nesting is too deep (max 32)");
        w.U8(kind);
        switch (kind)
        {
            case 1 or 2 or 3 or 4 or 5 or 6 or 10:
                w.Str(path);
                Values.Write(values[0], w, 0);
                break;
            case 7:
                w.Str(path);
                Values.Write(values[0], w, 0);
                Values.Write(values[1], w, 0);
                break;
            case 8:
                w.Str(path);
                w.U32(values.Length);
                foreach (object? v in values) Values.Write(v, w, 0);
                break;
            case 9:
                w.Str(path);
                w.Str((string)values[0]!);
                break;
            case 11 or 12:
                w.Str(path);
                break;
            case 13:
                w.Str((string)values[0]!);
                break;
            case 14 or 15:
                w.U32(children.Length);
                foreach (Filter c in children) c.Encode(w, depth + 1);
                break;
            default:
                children[0].Encode(w, depth + 1);
                break;
        }
    }
}

/// <summary>Sort keys: <c>"price"</c> ascending, <c>"-price"</c> descending. Converts from a string or string[].</summary>
public sealed class OrderBy
{
    internal IReadOnlyList<(string Path, bool Desc)> Keys { get; }

    /// <summary>Sort by these fields in order.</summary>
    public OrderBy(params string[] fields) =>
        Keys = fields.Select(f => f.StartsWith('-') ? (f[1..], true) : (f, false)).ToList();

    /// <summary>One sort key.</summary>
    public static implicit operator OrderBy(string field) => new(field);

    /// <summary>Several sort keys.</summary>
    public static implicit operator OrderBy(string[] fields) => new(fields);
}
