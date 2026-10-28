using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Text;

namespace Storine;

/// <summary>A document's fields: a dictionary of plain .NET values.</summary>
public class Doc : Dictionary<string, object?>
{
    /// <summary>An empty document.</summary>
    public Doc() : base(StringComparer.Ordinal) { }

    /// <summary>A copy of <paramref name="fields"/>.</summary>
    public Doc(IDictionary<string, object?> fields) : base(fields, StringComparer.Ordinal) { }
}

/// <summary>An unsigned 64-bit integer stored as Storine uint64 (plain <c>ulong</c> works too).</summary>
public readonly record struct UInt64Value(ulong Value);

/// <summary>An engine id value (24 raw bytes).</summary>
public sealed class StorineId : IEquatable<StorineId>
{
    private readonly byte[] bytes;

    /// <summary>From exactly 24 bytes.</summary>
    public StorineId(byte[] bytes)
    {
        if (bytes.Length != 24) throw new InvalidException("ids are 24 bytes");
        this.bytes = (byte[])bytes.Clone();
    }

    /// <summary>A copy of the raw bytes.</summary>
    public byte[] ToArray() => (byte[])bytes.Clone();

    /// <inheritdoc/>
    public bool Equals(StorineId? other) => other is not null && bytes.AsSpan().SequenceEqual(other.bytes);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => Equals(obj as StorineId);

    /// <inheritdoc/>
    public override int GetHashCode() => BitConverter.ToInt32(bytes, 0);

    /// <inheritdoc/>
    public override string ToString() => "StorineId(" + Convert.ToHexString(bytes).ToLowerInvariant() + ")";
}

/// <summary>
/// A point in time with nanosecond precision (.NET DateTime keeps only 100 ns ticks).
/// Converts implicitly from <see cref="DateTime"/> (UTC or local) and <see cref="DateTimeOffset"/>.
/// </summary>
public readonly record struct Timestamp(long UnixNanoseconds)
{
    private const long UnixEpochTicks = 621355968000000000L;

    /// <summary>From a DateTime (Unspecified kind is taken as UTC).</summary>
    public static Timestamp FromDateTime(DateTime t)
    {
        DateTime utc = t.Kind == DateTimeKind.Local ? t.ToUniversalTime() : t;
        try
        {
            return new Timestamp(checked((utc.Ticks - UnixEpochTicks) * 100));
        }
        catch (OverflowException)
        {
            throw new InvalidException("timestamp out of range (years 1677-2262)");
        }
    }

    /// <summary>As a UTC DateTimeOffset (truncated to 100 ns).</summary>
    public DateTimeOffset ToDateTimeOffset() =>
        new(UnixEpochTicks + Math.DivRem(UnixNanoseconds, 100, out long rem) - (rem < 0 ? 1 : 0), TimeSpan.Zero);

    /// <summary>As a UTC DateTime (truncated to 100 ns).</summary>
    public DateTime ToDateTime() => ToDateTimeOffset().UtcDateTime;

    /// <summary>Implicit conversion from DateTime.</summary>
    public static implicit operator Timestamp(DateTime t) => FromDateTime(t);

    /// <summary>Implicit conversion from DateTimeOffset.</summary>
    public static implicit operator Timestamp(DateTimeOffset t) => FromDateTime(t.UtcDateTime);

    /// <inheritdoc/>
    public override string ToString() => ToDateTimeOffset().ToString("o");
}

/// <summary>
/// An exact decimal with up to 38 digits: Mantissa / 10^Scale. Values that fit .NET <c>decimal</c> (28 digits) are
/// decoded as <c>decimal</c>; larger ones as <see cref="BigDecimal"/>.
/// </summary>
public readonly record struct BigDecimal(BigInteger Mantissa, byte Scale)
{
    /// <summary>Plain notation with exactly Scale fractional digits.</summary>
    public override string ToString()
    {
        string digits = BigInteger.Abs(Mantissa).ToString();
        if (Scale > 0)
        {
            if (digits.Length <= Scale) digits = new string('0', Scale - digits.Length + 1) + digits;
            digits = digits[..^Scale] + "." + digits[^Scale..];
        }
        return Mantissa.Sign < 0 ? "-" + digits : digits;
    }

    /// <summary>Parses "19.99", "-0.001", "1E+3".</summary>
    public static BigDecimal Parse(string text)
    {
        var m = System.Text.RegularExpressions.Regex.Match(text.Trim(), @"^([+-]?)(\d*)(?:\.(\d*))?(?:[eE]([+-]?\d{1,6}))?$");
        if (!m.Success || (m.Groups[2].Length == 0 && m.Groups[3].Length == 0))
            throw new InvalidException("not a decimal number: " + text);
        string frac = m.Groups[3].Value;
        BigInteger digits = BigInteger.Parse("0" + m.Groups[2].Value + frac);
        int scale = frac.Length - (m.Groups[4].Success && m.Groups[4].Length > 0 ? int.Parse(m.Groups[4].Value) : 0);
        if (digits.IsZero) return new BigDecimal(0, (byte)Math.Min(frac.Length, Values.MaxScale));
        if (scale < -40 || scale > 200) throw new InvalidException("decimal out of range: " + text);
        if (scale < 0)
        {
            digits *= BigInteger.Pow(10, -scale);
            scale = 0;
        }
        while (scale > Values.MaxScale && (digits % 10).IsZero)
        {
            digits /= 10;
            scale--;
        }
        if (scale > Values.MaxScale) throw new InvalidException("decimals have at most 38 fractional digits: " + text);
        if (m.Groups[1].Value == "-") digits = -digits;
        return new BigDecimal(digits, (byte)scale).Checked();
    }

    internal BigDecimal Checked()
    {
        if (Scale > Values.MaxScale) throw new InvalidException("decimal scale must be at most 38");
        if (Mantissa < Values.I128Min || Mantissa > Values.I128Max) throw new InvalidException("decimal does not fit in 38 digits");
        return this;
    }

    /// <summary>Exact conversion from a .NET decimal (keeps its scale: 100.00m has scale 2).</summary>
    public static BigDecimal FromDecimal(decimal d)
    {
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(d, bits);
        BigInteger mantissa = ((BigInteger)(uint)bits[2] << 64) | ((BigInteger)(uint)bits[1] << 32) | (uint)bits[0];
        byte scale = (byte)((bits[3] >> 16) & 0xFF);
        return new BigDecimal(bits[3] < 0 ? -mantissa : mantissa, scale);
    }

    /// <summary>The value as a .NET decimal when it fits exactly (28 digits), else null.</summary>
    public decimal? ToDecimalExact()
    {
        if (Scale > 28 || BigInteger.Abs(Mantissa) >= BigInteger.One << 96) return null;
        BigInteger abs = BigInteger.Abs(Mantissa);
        int lo = (int)(uint)(abs & uint.MaxValue), mid = (int)(uint)((abs >> 32) & uint.MaxValue), hi = (int)(uint)(abs >> 64);
        return new decimal(lo, mid, hi, Mantissa.Sign < 0, Scale);
    }
}

/// <summary>
/// The Storine value encoding (protocol/PROTOCOL.md §3).
/// .NET to Storine: null, bool, sbyte..long and byte..uint (int64), ulong / <see cref="UInt64Value"/> (uint64),
/// <see cref="BigInteger"/> / Int128 (int64 or int128), float / double, decimal / <see cref="BigDecimal"/> (exact
/// decimal), string, byte[], DateTime / DateTimeOffset / <see cref="Timestamp"/>, Guid, <see cref="StorineId"/>,
/// dictionaries with string keys (map) and any other IEnumerable (list).
/// Storine to .NET: null, bool, long, ulong, double, BigInteger, Timestamp, Guid, StorineId, string, byte[],
/// decimal (or BigDecimal beyond 28 digits), List&lt;object?&gt; and <see cref="Doc"/>.
/// </summary>
public static class Values
{
    internal const byte Null = 0, Bool = 1, Int64 = 2, UInt64 = 3, Float64 = 4, Int128Tag = 5, TimestampTag = 6, Uuid = 7,
        Id = 8, Text = 9, Bytes = 10, Decimal = 11, List = 12, Map = 13;
    internal const int MaxDepth = 32;
    internal const int MaxScale = 38;
    internal static readonly BigInteger I128Min = -(BigInteger.One << 127);
    internal static readonly BigInteger I128Max = (BigInteger.One << 127) - 1;
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    /// <summary>The encoding of one value.</summary>
    public static byte[] Encode(object? value)
    {
        var w = new Writer();
        Write(value, w, 0);
        return w.ToArray();
    }

    /// <summary>Decodes exactly one value; <see cref="InvalidException"/> on malformed input.</summary>
    public static object? Decode(byte[] bytes)
    {
        var r = new Reader(bytes);
        object? v = r.Value(0);
        if (r.Remaining != 0) throw new InvalidException("trailing bytes after value");
        return v;
    }

    internal sealed class Writer
    {
        private readonly MemoryStream buf = new();

        public byte[] ToArray() => buf.ToArray();

        public void U8(int v) => buf.WriteByte((byte)v);

        public void U32(long v)
        {
            Span<byte> b = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32LittleEndian(b, (uint)v);
            buf.Write(b);
        }

        public void U64(ulong v)
        {
            Span<byte> b = stackalloc byte[8];
            BinaryPrimitives.WriteUInt64LittleEndian(b, v);
            buf.Write(b);
        }

        public void Raw(ReadOnlySpan<byte> b) => buf.Write(b);

        public void Str(string s)
        {
            byte[] b = Encoding.UTF8.GetBytes(s);
            U32(b.Length);
            Raw(b);
        }

        public void I128(BigInteger v)
        {
            Span<byte> b = stackalloc byte[16];
            b.Fill(v.Sign < 0 ? (byte)0xFF : (byte)0);
            v.TryWriteBytes(b, out _, isUnsigned: false, isBigEndian: false);
            buf.Write(b);
        }
    }

    internal static void Write(object? v, Writer w, int depth)
    {
        if (depth > MaxDepth) throw new InvalidException("value nesting is too deep (max 32)");
        switch (v)
        {
            case null:
                w.U8(Null);
                break;
            case bool b:
                w.U8(Bool);
                w.U8(b ? 1 : 0);
                break;
            case long or int or short or sbyte or byte or ushort or uint:
                w.U8(Int64);
                w.U64((ulong)Convert.ToInt64(v));
                break;
            case ulong u:
                w.U8(UInt64);
                w.U64(u);
                break;
            case UInt64Value u:
                w.U8(UInt64);
                w.U64(u.Value);
                break;
            case double or float:
                w.U8(Float64);
                w.U64((ulong)BitConverter.DoubleToInt64Bits(Convert.ToDouble(v)));
                break;
            case Int128 i:
                Write((BigInteger)i, w, depth);
                break;
            case BigInteger n:
                if (n >= long.MinValue && n <= long.MaxValue)
                {
                    w.U8(Int64);
                    w.U64((ulong)(long)n);
                }
                else if (n >= I128Min && n <= I128Max)
                {
                    w.U8(Int128Tag);
                    w.I128(n);
                }
                else
                {
                    throw new InvalidException("integer does not fit in 128 bits");
                }
                break;
            case decimal d:
                Write(BigDecimal.FromDecimal(d), w, depth);
                break;
            case BigDecimal d:
                d.Checked();
                w.U8(Decimal);
                w.I128(d.Mantissa);
                w.U8(d.Scale);
                break;
            case string s:
                w.U8(Text);
                w.Str(s);
                break;
            case byte[] b:
                w.U8(Bytes);
                w.U32(b.Length);
                w.Raw(b);
                break;
            case Timestamp t:
                w.U8(TimestampTag);
                w.U64((ulong)t.UnixNanoseconds);
                break;
            case DateTime t:
                Write(Timestamp.FromDateTime(t), w, depth);
                break;
            case DateTimeOffset t:
                Write(Timestamp.FromDateTime(t.UtcDateTime), w, depth);
                break;
            case Guid g:
                w.U8(Uuid);
                w.Raw(g.ToByteArray(bigEndian: true));
                break;
            case StorineId id:
                w.U8(Id);
                w.Raw(id.ToArray());
                break;
            case IDictionary dict:
                w.U8(Map);
                WriteFields(dict, w, depth + 1);
                break;
            case IEnumerable items:
                var list = items.Cast<object?>().ToList();
                w.U8(List);
                w.U32(list.Count);
                foreach (object? item in list) Write(item, w, depth + 1);
                break;
            default:
                throw new InvalidException("cannot store a value of type " + v.GetType().FullName);
        }
    }

    /// <summary><c>u32 count | (str key, value)*</c> with keys in ascending UTF-8 byte order.</summary>
    internal static void WriteFields(IDictionary fields, Writer w, int depth)
    {
        var entries = new List<(byte[] Key, object? Value)>();
        foreach (DictionaryEntry e in fields)
        {
            if (e.Key is not string k) throw new InvalidException("map keys must be strings");
            entries.Add((Encoding.UTF8.GetBytes(k), e.Value));
        }
        entries.Sort((a, b) => a.Key.AsSpan().SequenceCompareTo(b.Key));
        w.U32(entries.Count);
        foreach (var (key, value) in entries)
        {
            w.U32(key.Length);
            w.Raw(key);
            Write(value, w, depth);
        }
    }

    /// <summary>Bounded reader: every count is checked against the bytes present before allocating.</summary>
    internal sealed class Reader
    {
        private readonly byte[] b;
        private int pos;

        public Reader(byte[] b) => this.b = b;

        public int Remaining => b.Length - pos;

        private ReadOnlySpan<byte> Take(long n)
        {
            if (n < 0 || n > Remaining) throw new InvalidException("truncated response");
            var s = b.AsSpan(pos, (int)n);
            pos += (int)n;
            return s;
        }

        public byte U8() => Take(1)[0];

        public uint U32() => BinaryPrimitives.ReadUInt32LittleEndian(Take(4));

        public ulong U64() => BinaryPrimitives.ReadUInt64LittleEndian(Take(8));

        public byte[] Bytes(long n) => Take(n).ToArray();

        public string Str()
        {
            var raw = Take(U32());
            try
            {
                return StrictUtf8.GetString(raw);
            }
            catch (ArgumentException)
            {
                throw new InvalidException("invalid UTF-8 in response");
            }
        }

        public int Count(int minSize)
        {
            long n = U32();
            if (n * Math.Max(minSize, 1) > Remaining) throw new InvalidException("truncated response");
            return (int)n;
        }

        public BigInteger I128() => new(Take(16), isUnsigned: false, isBigEndian: false);

        public object? Value(int depth)
        {
            byte tag = U8();
            switch (tag)
            {
                case Null:
                    return null;
                case Bool:
                    byte x = U8();
                    if (x > 1) throw new InvalidException("bool byte must be 0 or 1");
                    return x == 1;
                case Int64:
                    return (long)U64();
                case UInt64:
                    return U64();
                case Float64:
                    return BitConverter.Int64BitsToDouble((long)U64());
                case Int128Tag:
                    return I128();
                case TimestampTag:
                    return new Timestamp((long)U64());
                case Uuid:
                    return new Guid(Take(16), bigEndian: true);
                case Id:
                    return new StorineId(Bytes(24));
                case Text:
                    return Str();
                case Values.Bytes:
                    return Bytes(U32());
                case Decimal:
                {
                    var m = I128();
                    byte scale = U8();
                    if (scale > MaxScale) throw new InvalidException("decimal scale above 38");
                    var d = new BigDecimal(m, scale);
                    return d.ToDecimalExact() is decimal exact ? exact : d;
                }
                case List:
                {
                    if (depth >= MaxDepth) throw new InvalidException("value nesting is too deep");
                    int n = Count(1);
                    var list = new List<object?>(n);
                    for (int i = 0; i < n; i++) list.Add(Value(depth + 1));
                    return list;
                }
                case Map:
                    if (depth >= MaxDepth) throw new InvalidException("value nesting is too deep");
                    return Fields(depth + 1);
                default:
                    throw new InvalidException("unknown value tag " + tag);
            }
        }

        public Doc Fields(int depth)
        {
            int n = Count(5);
            var doc = new Doc();
            for (int i = 0; i < n; i++)
            {
                string k = Str();
                if (doc.ContainsKey(k)) throw new InvalidException("duplicate map key");
                doc[k] = Value(depth);
            }
            return doc;
        }
    }
}
