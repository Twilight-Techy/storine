using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using static Storine.Agg;
using static Storine.FieldOp;
using static Storine.Filter;

namespace Storine.Tests;

/// <summary>Starts and stops storage-server processes for the end-to-end tests.</summary>
internal static class Server
{
    /// <summary>$STORINE_SERVER or &lt;repo&gt;/target/release/storage-server (cargo build --release -p storage-server).</summary>
    public static readonly string Binary = Environment.GetEnvironmentVariable("STORINE_SERVER")
        ?? Path.Combine(VectorsTests.RepoRoot(), "target", "release", "storage-server");

    public static bool Built => File.Exists(Binary);

    public static int FreePort()
    {
        var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        int port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    public static Process Start(string dir, int port, params string[] extra)
    {
        var psi = new ProcessStartInfo(Binary) { RedirectStandardOutput = true, RedirectStandardError = true };
        psi.ArgumentList.Add(dir);
        psi.ArgumentList.Add($"127.0.0.1:{port}");
        foreach (var a in extra) psi.ArgumentList.Add(a);
        var p = Process.Start(psi)!;
        p.OutputDataReceived += (_, _) => { };
        p.ErrorDataReceived += (_, _) => { };
        p.BeginOutputReadLine();
        p.BeginErrorReadLine();
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (true)
        {
            try
            {
                using var c = new TcpClient();
                c.Connect(IPAddress.Loopback, port);
                return p;
            }
            catch (SocketException)
            {
                if (DateTime.UtcNow > deadline)
                {
                    p.Kill();
                    throw new InvalidOperationException($"server on port {port} did not start");
                }
                Thread.Sleep(50);
            }
        }
    }

    public static void Stop(Process p)
    {
        if (!p.HasExited)
        {
            p.Kill();
            p.WaitForExit();
        }
    }

    public static string TempDir()
    {
        string d = Path.Combine(Path.GetTempPath(), "storine_cs_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(d);
        return d;
    }
}

/// <summary>One server shared by the single-node tests.</summary>
public sealed class SingleNodeFixture : IDisposable
{
    public readonly StorineClient? Db;
    private readonly Process? proc;
    private readonly string? dir;

    public SingleNodeFixture()
    {
        if (!Server.Built) return;
        dir = Server.TempDir();
        int port = Server.FreePort();
        proc = Server.Start(dir, port);
        Db = StorineClient.Connect($"storine://127.0.0.1:{port}");
    }

    public void Dispose()
    {
        Db?.Dispose();
        if (proc is not null) Server.Stop(proc);
        if (dir is not null) Directory.Delete(dir, true);
    }
}

public class E2ETests : IClassFixture<SingleNodeFixture>
{
    private readonly StorineClient? db;

    public E2ETests(SingleNodeFixture f) => db = f.Db;

    // Every test returns early when the server binary is not built.
    private StorineClient Db => db!;

    [Fact]
    public async Task CrudTypesAndVersions()
    {
        if (!Server.Built) return;
        var c = Db.Collection("people");
        var doc = new Doc
        {
            ["name"] = "Ann", ["age"] = 37L, ["big"] = BigInteger.One << 100, ["score"] = 9.5, ["price"] = 19.99m,
            ["joined"] = new Timestamp(1714566600_123456789), ["id"] = Guid.NewGuid(), ["raw"] = new byte[] { 0, 0xff },
            ["tags"] = new List<object?> { "a", "b" }, ["address"] = new Doc { ["city"] = "Lagos" }, ["nothing"] = null, ["ok"] = true,
        };
        ulong v1 = await c.InsertAsync("ann", doc);
        var got = await c.GetAsync("ann");
        Assert.Equal(v1, got!.Version);
        Assert.Equal(VectorsTests.Comparable(doc), VectorsTests.Comparable(got.Fields));
        await Assert.ThrowsAsync<ConflictException>(() => c.InsertAsync("ann", doc));
        ulong v2 = await c.UpdateAsync("ann", Set("address.city", "Abuja"), Unset("nothing"), Append("tags", "c"));
        Assert.True(v2 > v1);
        got = await c.GetAsync("ann");
        Assert.Equal("Abuja", ((Doc)got!["address"]!)["city"]);
        Assert.False(got.Fields.ContainsKey("nothing"));
        Assert.Equal(new object?[] { "a", "b", "c" }, (List<object?>)got["tags"]!);
        var e = await Assert.ThrowsAsync<ConflictException>(() => c.ReplaceAsync("ann", new Doc { ["name"] = "x" }, ifVersion: v1));
        Assert.Equal(v2, e.CurrentVersion);
        await c.ReplaceAsync("ann", new Doc { ["name"] = "Ann B" }, ifVersion: v2);
        Assert.Equal("Ann B", (await c.GetAsync("ann"))!["name"]);
        await Assert.ThrowsAsync<NotFoundException>(() => c.UpdateAsync("nobody", Set("a", 1)));
        Assert.True(await c.DeleteAsync("ann"));
        Assert.False(await c.DeleteAsync("ann"));
        Assert.Null(await c.GetAsync("ann"));
    }

    [Fact]
    public async Task MoneyIsExactAndNeverLost()
    {
        if (!Server.Built) return;
        var acct = Db.Collection("accounts");
        await acct.PutAsync("a", new Doc { ["balance"] = 0.00m });
        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
        {
            for (int i = 0; i < 100; i++) await acct.UpdateAsync("a", Add("balance", 0.01m));
        })));
        Assert.Equal(8.00m, (await acct.GetAsync("a"))!["balance"]);
        await Assert.ThrowsAsync<InvalidException>(() => acct.UpdateAsync("a", Add("balance", -8.01m).Min(0)));
        Assert.Equal("8.00", (await acct.GetAsync("a"))!["balance"]!.ToString());
    }

    [Fact]
    public async Task TransactionIsAtomic()
    {
        if (!Server.Built) return;
        var bank = Db.Collection("bank");
        await bank.PutAsync("alice", new Doc { ["balance"] = 50m });
        await bank.PutAsync("bob", new Doc { ["balance"] = 0m });
        await Db.TransactionAsync(tx =>
        {
            tx.Update("bank", "alice", Add("balance", -20m).Min(0));
            tx.Update("bank", "bob", Add("balance", 20m));
        });
        await Assert.ThrowsAsync<InvalidException>(() => Db.TransactionAsync(tx =>
        {
            tx.Update("bank", "alice", Add("balance", -31m).Min(0));
            tx.Update("bank", "bob", Add("balance", 31m));
        }));
        var alice = (await bank.GetAsync("alice"))!;
        Assert.Equal(30m, alice["balance"]);
        Assert.Equal(20m, (await bank.GetAsync("bob"))!["balance"]);
        var e = await Assert.ThrowsAsync<ConflictException>(() => Db.TransactionAsync(tx =>
        {
            tx.RequireVersion("bank", "alice", alice.Version + 100);
            tx.Put("bank", "carol", new Doc());
        }));
        Assert.Equal(0, e.WriteIndex);
        Assert.Null(await bank.GetAsync("carol"));
    }

    [Fact]
    public async Task QueriesIndexesAndPagination()
    {
        if (!Server.Built) return;
        var items = Db.Collection("items");
        for (int i = 0; i < 120; i++)
            await items.PutAsync($"i{i:000}", new Doc { ["n"] = i, ["kind"] = i % 2 == 0 ? "even" : "odd", ["price"] = i / 4m });
        var q = Between("n", 10, 19) & Eq("kind", "odd");
        var want = new[] { "i019", "i017", "i015", "i013", "i011" };
        Assert.Equal(want, (await items.FindAsync(q, orderBy: "-n")).Select(d => d.Key));
        await items.CreateIndexAsync("n");
        await items.CreateIndexAsync("kind");
        Assert.Equal(want, (await items.FindAsync(q, orderBy: "-n")).Select(d => d.Key));
        var info = await items.InfoAsync();
        Assert.Contains("n", info.Indexes.Concat(info.Building));
        Assert.Equal(19, await items.CountAsync(Gt("price", 25m)));
        Assert.Equal(60, await items.CountAsync(Match(new Doc { ["kind"] = "even" })));
        int seen = 0;
        await foreach (var _ in items.ScanAsync()) seen++;
        Assert.Equal(120, seen);
        Assert.Equal("i119", (await items.FirstAsync(orderBy: "-price"))!.Key);
        Assert.Contains("items", await Db.CollectionsAsync());
    }

    [Fact]
    public async Task VectorSearch()
    {
        if (!Server.Built) return;
        var c = Db.Collection("cs_docs");
        await c.CreateVectorIndexAsync("emb", 3);
        Assert.Equal(new[] { new VectorIndex("emb", 3, true) }, await c.VectorIndexesAsync());
        await c.PutAsync("a", new Doc { ["emb"] = new[] { 1, 0, 0 }, ["kind"] = "x" });
        await c.PutAsync("b", new Doc { ["emb"] = new[] { 0, 1, 0 }, ["kind"] = "y" });
        await c.PutAsync("c", new Doc { ["emb"] = new[] { 0.7, 0.7, 0 }, ["kind"] = "x" });
        var hits = await c.NearestAsync("emb", new[] { 1f, 0.1f, 0f }, k: 2);
        Assert.Equal(new[] { "a", "c" }, hits.Select(d => d.Key));
        Assert.True(hits[0].Score > hits[1].Score);
        var filtered = await c.NearestAsync("emb", new[] { 1f, 0.1f, 0f }, k: 5, where: Eq("kind", "x"), exact: true);
        Assert.Equal(new[] { "a", "c" }, filtered.Select(d => d.Key));
        await Assert.ThrowsAsync<InvalidException>(() => c.PutAsync("bad", new Doc { ["emb"] = new[] { 1, 2 } })); // wrong dimensions
        await Assert.ThrowsAsync<InvalidException>(() => c.NearestAsync("emb", new[] { 1f, 2f }, k: 1));
        await c.DropVectorIndexAsync("emb");
        Assert.Empty(await c.VectorIndexesAsync());
    }

    [Fact]
    public async Task LargeValues()
    {
        if (!Server.Built) return;
        var c = Db.Collection("cs_files");
        var blob = new byte[3 * 1024 * 1024 + 17];
        new Random(7).NextBytes(blob);
        ulong v = await c.PutAsync("f1", new Doc { ["data"] = blob, ["name"] = "cat.png", ["meta"] = new Doc { ["size"] = blob.Length, ["type"] = "image/png" } });
        var d = (await c.GetAsync("f1", fields: new[] { "name", "meta.size", "absent" }))!;
        Assert.Equal(v, d.Version);
        Assert.Equal(new[] { "meta", "name" }, d.Fields.Keys.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal("cat.png", d["name"]);
        var meta = (IDictionary<string, object?>)d["meta"]!;
        Assert.Equal(new[] { "size" }, meta.Keys);
        Assert.Equal((long)blob.Length, meta["size"]);
        var r = (await c.ReadBytesAsync("f1", "data", 1000, 16))!;
        Assert.Equal((v, (ulong)blob.Length), (r.Version, r.Total));
        Assert.Equal(blob[1000..1016], r.Data);
        Assert.Empty((await c.ReadBytesAsync("f1", "data", (ulong)blob.Length + 5, 10))!.Data);
        var all = new MemoryStream();
        await foreach (var chunk in c.StreamBytesAsync("f1", "data", chunkSize: 1 << 20)) all.Write(chunk.Span);
        Assert.Equal(blob, all.ToArray());
        Assert.Equal("cat"u8.ToArray(), (await c.ReadBytesAsync("f1", "name", 0, 3))!.Data);
        Assert.Null(await c.ReadBytesAsync("nope", "data"));
        await Assert.ThrowsAsync<NotFoundException>(async () => { await foreach (var _ in c.StreamBytesAsync("nope", "data")) { } });
        await Assert.ThrowsAsync<InvalidException>(() => c.ReadBytesAsync("f1", "meta")); // not bytes
        await Assert.ThrowsAsync<InvalidException>(() => c.ReadBytesAsync("f1", "data", 0, 17 * 1024 * 1024));
        await using var chunks = c.StreamBytesAsync("f1", "data", chunkSize: 1 << 20).GetAsyncEnumerator();
        Assert.True(await chunks.MoveNextAsync());
        await c.UpdateAsync("f1", Set("name", "dog.png"));
        await Assert.ThrowsAsync<ConflictException>(async () => { while (await chunks.MoveNextAsync()) { } });
    }

    /// <summary>Value and scale: 12.50 differs from 12.5.</summary>
    private static string Exact(IEnumerable<ResultRow> rows) =>
        string.Join("\n", rows.Select(r => string.Join(" | ", r.Select(c => $"{c.Key}={c.Value?.GetType().Name}:{c.Value}"))));

    private static string Exact(IEnumerable<IEnumerable<KeyValuePair<string, object?>>> rows) =>
        string.Join("\n", rows.Select(r => string.Join(" | ", r.Select(c => $"{c.Key}={c.Value?.GetType().Name}:{c.Value}"))));

    [Fact]
    public async Task Analytics()
    {
        if (!Server.Built) return;
        var sales = Db.Collection("cs_sales");
        string[] regions = { "eu", "us", "apac" };
        const int n = 3000;
        var docs = Enumerable.Range(0, n).Select(i => new Doc
        {
            ["region"] = regions[i % 3],
            ["qty"] = (long)((i * 7) % 13 + 1),
            ["price"] = ((i * 37L) % 5000 + 100) / 100.00m, // scale 2
            ["day"] = (long)(i % 30),
        }).ToList();
        for (int b = 0; b < n; b += 100)
        {
            int from = b;
            await Db.TransactionAsync(tx =>
            {
                for (int i = from; i < from + 100; i++) tx.Put("cs_sales", $"s{i:00000}", docs[i]);
            });
        }

        // Expected grouped totals, computed from the inserted data.
        static decimal Avg6(long sum, long count)
        {
            // The server's avg of exact inputs: 6 more fractional digits, half-even.
            var scaled = (BigInteger)sum * 1_000_000;
            var q = BigInteger.DivRem(scaled, count, out var rem);
            var twice = BigInteger.Abs(rem) * 2;
            if (twice > count || (twice == count && !q.IsEven)) q += scaled.Sign;
            return new BigDecimal(q, 6).ToDecimalExact()!.Value;
        }
        var expected = docs.GroupBy(d => (string)d["region"]!).Select(g => new List<KeyValuePair<string, object?>>
        {
            new("region", g.Key),
            new("orders", (long)g.Count()),
            new("revenue", g.Sum(d => (decimal)d["price"]! * (long)d["qty"]!)), // scale 2
            new("avg_qty", Avg6(g.Sum(d => (long)d["qty"]!), g.Count())),
            new("min_price", g.Min(d => (decimal)d["price"]!)),
            new("max_price", g.Max(d => (decimal)d["price"]!)),
            new("days", (long)g.Select(d => (long)d["day"]!).Distinct().Count()),
        }).OrderByDescending(r => (decimal)r[2].Value!).Take(2).ToList();

        var grouped = new Aggregation()
            .Agg("orders", Count())
            .Agg("revenue", Sum(Field("price") * Field("qty")))
            .Agg("avg_qty", Avg("qty"))
            .Agg("min_price", Min("price"))
            .Agg("max_price", Max("price"))
            .Agg("days", CountDistinct("day"))
            .GroupBy("region").OrderBy("-revenue").Limit(2);
        var res = await sales.AggregateAsync(grouped);
        Assert.Equal((ulong)n, res.Matched);
        Assert.Equal(Exact(expected), Exact(res.Rows));
        await Assert.ThrowsAsync<InvalidException>(() => sales.AggregateAsync(grouped.OrderBy("-nope")));

        await sales.EnableColumnarAsync();
        Assert.Equal(Exact(expected), Exact((await sales.AggregateAsync(grouped)).Rows));

        // Weekly buckets of day: offset + floor((day - offset) / width) * width.
        var weeks = docs.GroupBy(d => 3 + (long)Math.Floor(((long)d["day"]! - 3) / 7.0) * 7).OrderBy(g => g.Key)
            .Select(g => new List<KeyValuePair<string, object?>>
            {
                new("day", g.Key), new("n", (long)g.Count()), new("qty", g.Sum(d => (long)d["qty"]!)),
            });
        var byWeek = await sales.AggregateAsync(new Aggregation().Agg("n", Count()).Agg("qty", Sum("qty")).GroupBy(Bucket("day", 7, 3)));
        Assert.Equal(Exact(weeks), Exact(byWeek.Rows));

        // Filtered, ungrouped: exactly one row.
        var us = docs.Where(d => (string)d["region"]! == "us" && (long)d["qty"]! >= 5).ToList();
        var filtered = await sales.AggregateAsync(new Aggregation().Agg("n", Count()).Agg("qty", Sum("qty"))
            .Where(Eq("region", "us") & Ge("qty", 5)));
        Assert.Equal((ulong)us.Count, filtered.Matched);
        Assert.Equal(Exact(new[] { new List<KeyValuePair<string, object?>> { new("n", (long)us.Count), new("qty", us.Sum(d => (long)d["qty"]!)) } }),
            Exact(filtered.Rows));

        // Column scan across pages, in key order.
        var eu = new List<ColumnRow>();
        await foreach (var row in sales.ScanColumnsAsync(new[] { "qty", "price" }, Eq("region", "eu"), pageSize: 500)) eu.Add(row);
        Assert.Equal(Enumerable.Range(0, n).Where(i => i % 3 == 0).Select(i => $"s{i:00000}"), eu.Select(r => r.Key));
        foreach (var row in eu)
        {
            var d = docs[int.Parse(row.Key[1..])];
            Assert.Equal(d["qty"], row["qty"]);
            Assert.Equal(d["price"]!.ToString(), row["price"]!.ToString());
        }

        await sales.DisableColumnarAsync();
        Assert.Equal(Exact(expected), Exact((await sales.AggregateAsync(grouped)).Rows));
    }

    [Fact]
    public async Task TtlAndErrors()
    {
        if (!Server.Built) return;
        var s = Db.Collection("sessions");
        await s.PutAsync("short", new Doc { ["u"] = 1 }, ttl: TimeSpan.FromMilliseconds(200));
        Assert.NotNull(await s.GetAsync("short"));
        await Task.Delay(400);
        Assert.Null(await s.GetAsync("short"));
        await Assert.ThrowsAsync<InvalidException>(() => s.PutAsync("bad", new Doc { ["a.b"] = 1 }));
        await Assert.ThrowsAsync<InvalidException>(() => Db.Collection("bad/name").PutAsync("k", new Doc()));
        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => s.GetAsync("x", cancelled.Token));
    }

    [Fact]
    public async Task ThreeNodesRedirectAndFailover()
    {
        if (!Server.Built) return;
        string baseDir = Server.TempDir();
        int[] ports = { Server.FreePort(), Server.FreePort(), Server.FreePort() };
        string peers = string.Join(",", ports.Select((p, i) => $"{i + 1}=127.0.0.1:{p}"));
        var procs = new List<Process>();
        try
        {
            for (int i = 0; i < 3; i++)
                procs.Add(Server.Start(Path.Combine(baseDir, $"n{i + 1}"), ports[i], "--node-id", (i + 1).ToString(), "--peers", peers));
            // Any seed works: the client follows redirects to the leader.
            await using var cluster = StorineClient.Connect("storine://" + string.Join(",", ports.Select(p => $"127.0.0.1:{p}")),
                new StorineOptions { Timeout = TimeSpan.FromSeconds(2) });
            var acct = cluster.Collection("acct");
            var deadline = DateTime.UtcNow.AddSeconds(15);
            while (true)
            {
                try
                {
                    await acct.PutAsync("a", new Doc { ["bal"] = 100m });
                    break;
                }
                catch (UnavailableException) when (DateTime.UtcNow < deadline)
                {
                    await Task.Delay(100);
                }
            }
            for (int i = 0; i < 10; i++) await acct.UpdateAsync("a", Add("bal", 1m));
            Assert.Equal(110m, (await acct.GetAsync("a"))!["bal"]);
            var info = await cluster.ClusterInfoAsync();
            Assert.True(info.IsLeader);
            Server.Stop(procs[(int)info.NodeId - 1]);
            // The client fails over to the new leader; every deposit applies exactly once.
            for (int i = 0; i < 5; i++) await acct.UpdateAsync("a", Add("bal", 1m));
            Assert.Equal(115m, (await acct.GetAsync("a"))!["bal"]);
        }
        finally
        {
            foreach (var p in procs) Server.Stop(p);
            Directory.Delete(baseDir, true);
        }
    }
}
