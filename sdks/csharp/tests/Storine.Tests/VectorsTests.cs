using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Text.Json;
using Xunit;
using static Storine.FieldOp;
using static Storine.Filter;

namespace Storine.Tests;

/// <summary>Conformance: the SDK reproduces the server's encodings byte for byte (protocol/vectors).</summary>
public class VectorsTests
{
    internal static string RepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !Directory.Exists(Path.Combine(dir.FullName, "protocol", "vectors"))) dir = dir.Parent;
        return dir?.FullName ?? throw new InvalidOperationException("repository root not found");
    }

    private static readonly JsonElement V =
        JsonDocument.Parse(File.ReadAllText(Path.Combine(RepoRoot(), "protocol", "vectors", "vectors.json"))).RootElement;

    private static string Hex(byte[] b) => Convert.ToHexString(b).ToLowerInvariant();

    private static object? Native(JsonElement d)
    {
        var v = d.TryGetProperty("value", out var x) ? x : default;
        return d.GetProperty("type").GetString() switch
        {
            "null" => null,
            "bool" => v.GetBoolean(),
            "int64" => long.Parse(v.GetString()!),
            "uint64" => ulong.Parse(v.GetString()!),
            "float64" => BitConverter.Int64BitsToDouble(Convert.ToInt64(d.GetProperty("bits").GetString(), 16)),
            "int128" => BigInteger.Parse(v.GetString()!),
            "timestamp" => new Timestamp(long.Parse(v.GetString()!)),
            "uuid" => Guid.Parse(v.GetString()!),
            "id" => new StorineId(Convert.FromHexString(v.GetString()!)),
            "text" => v.GetString(),
            "bytes" => Convert.FromHexString(v.GetString()!),
            "decimal" => BigDecimal.Parse(v.GetString()!) is var bd && bd.ToDecimalExact() is decimal m ? m : bd,
            "list" => v.EnumerateArray().Select(Native).ToList(),
            "map" => Map(v),
            var t => throw new InvalidOperationException(t),
        };
    }

    private static Doc Map(JsonElement pairs)
    {
        var doc = new Doc();
        foreach (var p in pairs.EnumerateArray()) doc[p[0].GetString()!] = Native(p[1]);
        return doc;
    }

    /// <summary>Comparable form: bytes as hex, decimals with scale, containers recursively.</summary>
    internal static object? Comparable(object? x) => x switch
    {
        byte[] b => "bytes:" + Hex(b),
        decimal d => "dec:" + d.ToString(System.Globalization.CultureInfo.InvariantCulture),
        BigDecimal d => "dec:" + d,
        IDictionary m => m.Keys.Cast<string>().OrderBy(k => k, StringComparer.Ordinal).Select(k => k + "=" + Str(Comparable(m[k]))).Aggregate("{", (a, b) => a + b + ";") + "}",
        string s => s,
        IEnumerable l => l.Cast<object?>().Select(i => Str(Comparable(i))).Aggregate("[", (a, b) => a + b + ";") + "]",
        _ => x,
    };

    private static string Str(object? o) => o is null ? "null" : o is string s ? s : o.GetType().Name + ":" + o;

    [Fact]
    public void ValuesEncodeAndDecode()
    {
        foreach (var vec in V.GetProperty("values").EnumerateArray())
        {
            string name = vec.GetProperty("name").GetString()!;
            object? value = Native(vec.GetProperty("value"));
            Assert.True(vec.GetProperty("hex").GetString() == Hex(Values.Encode(value)), name);
            object? back = Values.Decode(Convert.FromHexString(vec.GetProperty("hex").GetString()!));
            Assert.Equal(Str(Comparable(value)), Str(Comparable(back)));
        }
    }

    [Fact]
    public void DocumentBody()
    {
        var doc = V.GetProperty("documents")[0];
        var w = new Values.Writer();
        Values.WriteFields((Doc)Native(doc.GetProperty("fields"))!, w, 1);
        Assert.Equal(doc.GetProperty("hex").GetString(), Hex(w.ToArray()));
    }

    [Fact]
    public void Requests()
    {
        var d = new Doc { ["balance"] = 100.00m, ["owner"] = "alice", ["active"] = true };
        byte[] rid = Convert.FromHexString("0102030405060708090a0b0c0d0e0f10");
        byte[] ab = Enumerable.Repeat((byte)0xab, 16).ToArray();
        byte[] Ws(byte[]? id, params Wire.Write[] w) => Wire.Writes(w, id);
        byte[] Q(Filter? f, OrderBy? o = null, uint limit = 0, uint offset = 0, string? after = null) =>
            Wire.Query("items", f, o, limit, offset, after);
        var built = new Dictionary<string, byte[]>
        {
            ["get accounts/alice"] = Wire.Get("accounts", "alice"),
            ["write: put"] = Ws(null, Wire.Write.Put("accounts", "alice", d, 0, 0, 0)),
            ["write: insert (must not exist) with request id"] = Ws(ab, Wire.Write.Put("accounts", "alice", d, 0, 1, 0)),
            ["write: replace if version 7"] = Ws(null, Wire.Write.Put("accounts", "alice", d, 0, 3, 7)),
            ["write: put with ttl 1500 ms"] = Ws(null, Wire.Write.Put("sessions", "s1", new Doc { ["u"] = 1 }, Transaction.TtlMs(TimeSpan.FromMilliseconds(1500)), 0, 0)),
            ["write: delete"] = Ws(null, new Wire.Write("accounts", "alice", 3)),
            ["write: delete if exists"] = Ws(null, new Wire.Write("accounts", "alice", 3, 2)),
            ["write: transfer 25.00 (two patches, no overdraft) with request id"] = Ws(rid,
                Wire.Write.Patch("accounts", "alice", 2, 0, new[] { Add("balance", -25.00m).Min(0) }),
                Wire.Write.Patch("accounts", "bob", 2, 0, new[] { Add("balance", 25.00m) })),
            ["write: patch set / unset / append / add with min and max"] = Ws(null, Wire.Write.Patch("c", "k", 2, 0,
                new[] { Set("profile.name", "Ann"), Unset("tmp"), Append("log", 3), Add("n", 1).Min(0).Max(10) })),
            ["write: check version 3"] = Ws(null, new Wire.Write("c", "k", 5, 3, 3)),
            ["query: all (default limit)"] = Q(null),
            ["query: eq text"] = Q(Eq("kind", "book"), limit: 10),
            ["query: and(gte, lt) sorted desc, offset 20"] = Q(Ge("price", 10m) & Lt("price", 20.50m), "-price", 10, 20),
            ["query: between"] = Q(Between("n", 1, 5)),
            ["query: in"] = Q(In("n", 1, 2)),
            ["query: prefix / contains / exists / missing"] = Q(Or(Prefix("name", "An"), Contains("tags", "x"), Exists("email"), Missing("deleted_at"))),
            ["query: not / ne / le / gt / key prefix"] = Q(And(!Ne("a", 1), Le("b", 2.5), Gt("c", 0), KeyPrefix("user:"))),
            ["query: after key cursor"] = Q(null, limit: 100, after: "k0099"),
            ["query: multi-key sort"] = Q(null, new[] { "a", "-b" }, 5),
            ["admin: create index items.price"] = Wire.Admin(1, "items", "price"),
            ["info: collection items"] = Wire.Admin(6, "items"),
            ["info: list collections"] = new byte[] { 3 },
        };
        int seen = 0;
        foreach (var vec in V.GetProperty("requests").EnumerateArray())
        {
            string name = vec.GetProperty("name").GetString()!;
            Assert.True(built.ContainsKey(name), "no builder for " + name);
            Assert.True(vec.GetProperty("body_hex").GetString() == Hex(built[name]), name);
            seen++;
        }
        Assert.Equal(built.Count, seen);
    }

    [Fact]
    public void Responses()
    {
        var r = V.GetProperty("responses").EnumerateArray()
            .ToDictionary(v => v.GetProperty("name").GetString()!, v => Convert.FromHexString(v.GetProperty("body_hex").GetString()!));
        Assert.Equal(new ulong?[] { 12, null }, Wire.WriteResponse(r["write ok: [v12, deleted]"]));
        var p = Wire.ReadPage(r["page: total 42, next k1, one doc"]);
        Assert.Equal(42u, p.Total);
        Assert.Equal("k1", p.NextKey);
        Assert.Equal(("k1", 3ul, 1L), (p.Docs[0].Key, p.Docs[0].Version, (long)p.Docs[0]["n"]!));
    }

    [Fact]
    public void HostileBytesNeverCrash()
    {
        var values = V.GetProperty("values");
        byte[] good = Convert.FromHexString(values[values.GetArrayLength() - 1].GetProperty("hex").GetString()!);
        for (int cut = 0; cut < good.Length; cut++) Assert.Throws<InvalidException>(() => Values.Decode(good[..cut]));
        Assert.Throws<InvalidException>(() => Values.Decode(new byte[] { 0x0C, 0xFF, 0xFF, 0xFF, 0xFF }));
    }

    [Fact]
    public void ValueMappingRules()
    {
        Assert.Throws<InvalidException>(() => Values.Encode(BigInteger.One << 130));
        Assert.Throws<InvalidException>(() => Values.Encode(new object()));
        Assert.Throws<InvalidException>(() => BigDecimal.Parse("1E-40"));
        Assert.Equal(1000m, Values.Decode(Values.Encode(BigDecimal.Parse("1E+3"))));
        var big = BigDecimal.Parse("1234567890123456789012345678.0123456789");
        Assert.Equal(big, Values.Decode(Values.Encode(big)));
        Assert.Equal("19.99", Values.Decode(Values.Encode(19.99m))!.ToString());
        Assert.Equal("100.00", Values.Decode(Values.Encode(100.00m))!.ToString());
        var when = new DateTime(2024, 5, 1, 12, 30, 0, DateTimeKind.Utc);
        Assert.Equal(when, ((Timestamp)Values.Decode(Values.Encode(when))!).ToDateTime());
        Assert.Equal(Hex(Values.Encode(new List<object> { 1L, 2L })), Hex(Values.Encode(new[] { 1, 2 })));
    }
}
