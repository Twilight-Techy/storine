using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Storine;

/// <summary>Options of <see cref="StorineClient.Connect"/>. Every property is optional.</summary>
public sealed record StorineOptions
{
    /// <summary>Bearer token (also accepted in the URL: <c>storine://token@host:port</c>).</summary>
    public string? Token { get; init; }

    /// <summary>Timeout of one request (default 5 s). Retries continue for up to 3x this.</summary>
    public TimeSpan Timeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Allow possibly stale reads from cluster followers (default false: leader reads).</summary>
    public bool FollowerReads { get; init; }

    /// <summary>Idle connections kept to the current server (default 16).</summary>
    public int PoolSize { get; init; } = 16;
}

/// <summary>
/// A connection pool to a Storine server or cluster. Thread-safe; create one per application.
/// <code>
/// await using var db = StorineClient.Connect("storine://127.0.0.1:7878");
/// var accounts = db.Collection("accounts");
/// await accounts.InsertAsync("alice", new Doc { ["balance"] = 100.00m });
/// await accounts.UpdateAsync("alice", FieldOp.Add("balance", -25.00m).Min(0)); // no overdraft, no lost update
/// </code>
/// Writes carry a request id that the client reuses on its own retries, so timeouts, reconnects and leader changes
/// never apply a write twice.
/// </summary>
public sealed class StorineClient : IDisposable, IAsyncDisposable
{
    private const int MaxBody = 256 * 1024 * 1024;
    private readonly string[] seeds;
    private readonly StorineOptions options;
    private readonly Stack<Conn> pool = new();
    private readonly object gate = new();
    private string target;

    private StorineClient(string[] seeds, StorineOptions options)
    {
        this.seeds = seeds;
        this.options = options;
        target = seeds[0];
    }

    /// <summary>
    /// A client for <c>storine://[token@]host:port[,host:port...]</c> (or just <c>host:port</c>). Query parameters
    /// <c>timeout_ms=5000</c> and <c>follower_reads=1</c> are accepted too. Connections open lazily.
    /// </summary>
    public static StorineClient Connect(string url, StorineOptions? options = null)
    {
        var o = options ?? new StorineOptions();
        if (!url.Contains("://")) url = "storine://" + url;
        if (!url.StartsWith("storine://", StringComparison.Ordinal)) throw new InvalidException("connection strings start with storine://");
        string rest = url["storine://".Length..];
        string query = "";
        int qm = rest.IndexOf('?');
        if (qm >= 0)
        {
            query = rest[(qm + 1)..];
            rest = rest[..qm];
        }
        int at = rest.LastIndexOf('@');
        if (at >= 0)
        {
            if (o.Token is null) o = o with { Token = Uri.UnescapeDataString(rest[..at]) };
            rest = rest[(at + 1)..];
        }
        foreach (string kv in query.Split('&', StringSplitOptions.RemoveEmptyEntries))
        {
            string[] p = kv.Split('=', 2);
            string v = p.Length > 1 ? p[1] : "";
            if (p[0] == "timeout_ms")
            {
                if (!int.TryParse(v, out int ms) || ms <= 0) throw new InvalidException("timeout_ms must be a positive integer");
                o = o with { Timeout = TimeSpan.FromMilliseconds(ms) };
            }
            else if (p[0] == "follower_reads")
            {
                o = o with { FollowerReads = v is "1" or "true" or "yes" };
            }
        }
        if (o.Timeout <= TimeSpan.Zero) throw new InvalidException("Timeout must be positive");
        if (o.PoolSize <= 0) throw new InvalidException("PoolSize must be positive");
        string[] hosts = rest.TrimEnd('/').Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(Normalize).ToArray();
        if (hosts.Length == 0) throw new InvalidException("no server address given");
        return new StorineClient(hosts, o);
    }

    private static string Normalize(string address)
    {
        if (address.StartsWith('[')) return address.Contains("]:") ? address : address + ":7878";
        int colons = address.Count(c => c == ':');
        return colons switch { 1 => address, 0 => address + ":7878", _ => "[" + address + "]:7878" };
    }

    /// <summary>A handle on the collection <paramref name="name"/> (created on first write).</summary>
    public Collection Collection(string name) => new(this, name);

    /// <summary>
    /// Collects writes in <paramref name="body"/>, then commits them atomically (all or none). Returns the new version
    /// of each write in order (null for deletes and checks). If <paramref name="body"/> throws, nothing is written.
    /// <code>
    /// await db.TransactionAsync(tx => {
    ///     tx.Update("accounts", "alice", FieldOp.Add("balance", -10m).Min(0));
    ///     tx.Update("accounts", "bob", FieldOp.Add("balance", 10m));
    /// });
    /// </code>
    /// </summary>
    public async Task<IReadOnlyList<ulong?>> TransactionAsync(Action<Transaction> body, CancellationToken ct = default)
    {
        var tx = new Transaction();
        body(tx);
        if (tx.Writes.Count == 0) return Array.Empty<ulong?>();
        return await CommitAsync(tx.Writes, ct).ConfigureAwait(false);
    }

    /// <summary>Names of every declared collection.</summary>
    public async Task<IReadOnlyList<string>> CollectionsAsync(CancellationToken ct = default)
    {
        var reply = await CallAsync(Wire.OpDocInfo, new byte[] { 3 }, ct).ConfigureAwait(false);
        var r = new Values.Reader(reply.Body);
        int n = r.Count(17);
        var names = new List<string>(n);
        for (int i = 0; i < n; i++) names.Add(Wire.Info(r).Name);
        return names;
    }

    /// <summary>Node id, leader, term and commit indexes of the server currently used.</summary>
    public async Task<ClusterInfo> ClusterInfoAsync(CancellationToken ct = default) =>
        Wire.ReadClusterInfo((await CallAsync(Wire.OpClusterInfo, Array.Empty<byte>(), ct).ConfigureAwait(false)).Body);

    /// <summary>Closes every pooled connection.</summary>
    public void Dispose()
    {
        Conn[] all;
        lock (gate)
        {
            all = pool.ToArray();
            pool.Clear();
        }
        foreach (var c in all) c.Dispose();
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    // ── plumbing ──

    internal readonly record struct Reply(int Status, ulong Version, byte[] Body);

    private sealed class Conn : IDisposable
    {
        public readonly string Address;
        private readonly TcpClient tcp;
        private readonly NetworkStream stream;

        private Conn(string address, TcpClient tcp)
        {
            Address = address;
            this.tcp = tcp;
            stream = tcp.GetStream();
        }

        public static async Task<Conn> OpenAsync(string address, CancellationToken ct)
        {
            int colon = address.LastIndexOf(':');
            string host = address[..colon].Trim('[', ']');
            int port = int.Parse(address[(colon + 1)..]);
            var tcp = new TcpClient { NoDelay = true };
            try
            {
                await tcp.ConnectAsync(host, port, ct).ConfigureAwait(false);
            }
            catch
            {
                tcp.Dispose();
                throw;
            }
            return new Conn(address, tcp);
        }

        public async Task<Reply> CallAsync(byte op, byte[] body, CancellationToken ct)
        {
            var frame = new byte[29 + body.Length];
            frame[0] = op;
            BinaryPrimitives.WriteUInt32LittleEndian(frame.AsSpan(25), (uint)body.Length);
            body.CopyTo(frame, 29);
            await stream.WriteAsync(frame, ct).ConfigureAwait(false);
            var hdr = new byte[13];
            await stream.ReadExactlyAsync(hdr, ct).ConfigureAwait(false);
            uint n = BinaryPrimitives.ReadUInt32LittleEndian(hdr.AsSpan(9));
            if (n > MaxBody) throw new IOException("response too large");
            var rbody = new byte[n];
            await stream.ReadExactlyAsync(rbody, ct).ConfigureAwait(false);
            return new Reply(hdr[0], BinaryPrimitives.ReadUInt64LittleEndian(hdr.AsSpan(1)), rbody);
        }

        public void Dispose() => tcp.Dispose();
    }

    private sealed class DialFailure : Exception
    {
        public readonly string Address;

        public DialFailure(string address, Exception inner) : base(inner.Message, inner) => Address = address;
    }

    private async Task<Conn> AcquireAsync(CancellationToken ct)
    {
        string t;
        lock (gate)
        {
            t = target;
            while (pool.Count > 0)
            {
                var c = pool.Pop();
                if (c.Address == t) return c;
                c.Dispose();
            }
        }
        Conn conn;
        try
        {
            conn = await Conn.OpenAsync(t, ct).ConfigureAwait(false);
        }
        catch (Exception e) when (e is SocketException or IOException or OperationCanceledException && !ct.IsCancellationRequested)
        {
            throw new DialFailure(t, e);
        }
        try
        {
            if (options.Token is not null)
            {
                var r = await conn.CallAsync(Wire.OpAuth, Encoding.UTF8.GetBytes(options.Token), ct).ConfigureAwait(false);
                if (r.Status != Wire.StOk) throw new UnauthorizedException("authentication failed: " + Encoding.UTF8.GetString(r.Body), r.Status);
            }
            if (options.FollowerReads) await conn.CallAsync(Wire.OpSetOptions, new byte[] { 1 }, ct).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            conn.Dispose();
            if (e is IOException or SocketException) throw new DialFailure(t, e);
            throw;
        }
        return conn;
    }

    private void Release(Conn c)
    {
        lock (gate)
        {
            if (c.Address == target && pool.Count < options.PoolSize)
            {
                pool.Push(c);
                return;
            }
        }
        c.Dispose();
    }

    /// <summary>Switches to <paramref name="address"/>, or (when null) to a random other seed not known to have failed.</summary>
    private void Redirect(string? address, HashSet<string> failed)
    {
        lock (gate)
        {
            if (address is not null)
            {
                target = Normalize(address);
                return;
            }
            var others = seeds.Where(s => s != target && !failed.Contains(s)).ToList();
            if (others.Count == 0) others = seeds.Where(s => s != target).ToList();
            if (others.Count == 0) others = seeds.ToList();
            target = others[Random.Shared.Next(others.Count)];
        }
    }

    /// <summary>
    /// Sends a request, following leader redirects and retrying transient failures for up to three times the timeout
    /// (enough to ride out a leader election). Every request is idempotent: reads by nature, writes by request id.
    /// </summary>
    internal async Task<Reply> RequestAsync(byte op, byte[] body, CancellationToken ct)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromTicks(Math.Max(3 * options.Timeout.Ticks, TimeSpan.TicksPerSecond));
        int delayMs = 20;
        StorineException? lastError = null;
        Exception? lastCause = null;
        var failed = new HashSet<string>();
        async Task Backoff()
        {
            await Task.Delay(delayMs, ct).ConfigureAwait(false);
            delayMs = Math.Min(delayMs * 2, 500);
        }
        for (int attempt = 0; attempt < 1000 && (attempt < 8 || DateTime.UtcNow < deadline); attempt++)
        {
            ct.ThrowIfCancellationRequested();
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(options.Timeout);
            Conn c;
            try
            {
                c = await AcquireAsync(timeout.Token).ConfigureAwait(false);
            }
            catch (DialFailure e)
            {
                lastCause = e;
                failed.Add(e.Address);
                Redirect(null, failed);
                await Backoff().ConfigureAwait(false);
                continue;
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                lastCause = new TimeoutException("timed out connecting to " + target);
                failed.Add(target);
                Redirect(null, failed);
                continue;
            }
            Reply r;
            try
            {
                r = await c.CallAsync(op, body, timeout.Token).ConfigureAwait(false);
            }
            catch (Exception e) when (e is IOException or SocketException or OperationCanceledException && !ct.IsCancellationRequested)
            {
                c.Dispose();
                lastCause = e;
                failed.Add(c.Address);
                Redirect(null, failed);
                await Backoff().ConfigureAwait(false);
                continue;
            }
            catch
            {
                c.Dispose();
                throw;
            }
            switch (r.Status)
            {
                case Wire.StNotLeader:
                {
                    c.Dispose();
                    string? hint = Encoding.UTF8.GetString(r.Body);
                    if (hint.Length == 0 || failed.Contains(Normalize(hint))) hint = null; // stale: the old leader is down
                    Redirect(hint, failed);
                    lastError = new UnavailableException("no cluster leader is available yet", r.Status);
                    if (hint is null) await Backoff().ConfigureAwait(false);
                    continue;
                }
                case Wire.StCommitUnknown or Wire.StOverloaded:
                    c.Dispose();
                    lastError = r.Status == Wire.StOverloaded
                        ? new OverloadedException(r.Body.Length > 0 ? Encoding.UTF8.GetString(r.Body) : "server overloaded", r.Status)
                        : new UnavailableException("write outcome unknown and retries exhausted", r.Status);
                    await Backoff().ConfigureAwait(false);
                    continue;
                case Wire.StUnauthorized or Wire.StRateLimited:
                    c.Dispose(); // the server closed the connection
                    break;
                default:
                    Release(c);
                    break;
            }
            return r;
        }
        throw lastError ?? new UnavailableException("could not reach any server: " + (lastCause?.Message ?? "?"));
    }

    internal static StorineException Error(int status, byte[] body)
    {
        string t = Encoding.UTF8.GetString(body);
        return status switch
        {
            Wire.StConflict => Wire.Conflict(body),
            Wire.StNotFound => new NotFoundException(t.Length == 0 ? "document not found" : t, status),
            Wire.StInvalid => new InvalidException(t, status),
            Wire.StUnauthorized => new UnauthorizedException(t.Length == 0 ? "not authorized" : t, status),
            Wire.StRateLimited => new RateLimitedException(t.Length == 0 ? "rate limited" : t, status),
            Wire.StOverloaded => new OverloadedException(t.Length == 0 ? "server overloaded" : t, status),
            Wire.StCommitUnknown => new UnavailableException("write outcome unknown; retry the same call", status),
            _ => new ServerException(t.Length == 0 ? $"server error (status {status})" : t, status),
        };
    }

    /// <summary><see cref="RequestAsync"/> that throws on any status but OK.</summary>
    internal async Task<Reply> CallAsync(byte op, byte[] body, CancellationToken ct)
    {
        var r = await RequestAsync(op, body, ct).ConfigureAwait(false);
        if (r.Status != Wire.StOk) throw Error(r.Status, r.Body);
        return r;
    }

    /// <summary>Applies writes atomically, exactly once even across retries; returns the new versions.</summary>
    internal async Task<List<ulong?>> CommitAsync(IReadOnlyList<Wire.Write> writes, CancellationToken ct)
    {
        byte[] requestId = RandomNumberGenerator.GetBytes(16);
        var r = await RequestAsync(Wire.OpDocWrite, Wire.Writes(writes, requestId), ct).ConfigureAwait(false);
        if (r.Status == Wire.StNotFound)
        {
            // Body: u32 index of the write that patched a missing document, then the message.
            int index = -1;
            string msg = "document not found";
            if (r.Body.Length >= 4)
            {
                index = (int)BinaryPrimitives.ReadUInt32LittleEndian(r.Body);
                if (r.Body.Length > 4) msg = Encoding.UTF8.GetString(r.Body, 4, r.Body.Length - 4);
            }
            throw new NotFoundException(msg, r.Status, index);
        }
        if (r.Status != Wire.StOk) throw Error(r.Status, r.Body);
        return Wire.WriteResponse(r.Body);
    }
}

/// <summary>A named set of documents. Every method is one atomic, exactly-once operation. Thread-safe.</summary>
public sealed class Collection
{
    private readonly StorineClient client;

    internal Collection(StorineClient client, string name)
    {
        this.client = client;
        Name = name;
    }

    /// <summary>The collection's name.</summary>
    public string Name { get; }

    private async Task<ulong> OneAsync(Wire.Write w, CancellationToken ct) =>
        (await client.CommitAsync(new[] { w }, ct).ConfigureAwait(false))[0] ?? 0;

    /// <summary>The document <paramref name="key"/>, or null if it does not exist.</summary>
    public Task<Document?> GetAsync(string key, CancellationToken ct = default) => GetAsync(key, Wire.Get(Name, key), ct);

    /// <summary>
    /// <see cref="GetAsync(string, CancellationToken)"/> returning only the given field paths (such as <c>"name"</c> or
    /// <c>"meta.size"</c>; at most 256), so metadata can be read without transferring a large value stored beside it.
    /// Nested paths come back nested; missing paths are left out.
    /// </summary>
    public Task<Document?> GetAsync(string key, IEnumerable<string> fields, CancellationToken ct = default) =>
        GetAsync(key, Wire.GetFields(Name, key, fields.ToList()), ct);

    private async Task<Document?> GetAsync(string key, byte[] request, CancellationToken ct)
    {
        var r = await client.RequestAsync(Wire.OpDocGet, request, ct).ConfigureAwait(false);
        if (r.Status == Wire.StNotFound) return null;
        if (r.Status != Wire.StOk) throw StorineClient.Error(r.Status, r.Body);
        return new Document(key, r.Version, new Values.Reader(r.Body).Fields(1));
    }

    /// <summary>
    /// Bytes <c>offset .. offset + length</c> of a bytes (or text) field, without reading the rest of the document; null if
    /// the document does not exist. <paramref name="length"/> is at most 16 MiB (the default); past the end of the value,
    /// <c>Data</c> is empty. <see cref="InvalidException"/> if the field is missing or not bytes/text.
    /// </summary>
    public async Task<ByteRange?> ReadBytesAsync(string key, string field, ulong offset = 0, int length = Wire.MaxByteRange,
        CancellationToken ct = default)
    {
        var req = Wire.GetRange(Name, key, field, offset, length);
        var r = await client.RequestAsync(Wire.OpDocGet, req, ct).ConfigureAwait(false);
        if (r.Status == Wire.StNotFound) return null;
        if (r.Status != Wire.StOk) throw StorineClient.Error(r.Status, r.Body);
        var (total, data) = Wire.ReadByteRange(r.Body);
        return new ByteRange(r.Version, total, data);
    }

    /// <summary>
    /// A large bytes (or text) field, chunk by chunk (each at most <paramref name="chunkSize"/> bytes, default 1 MiB).
    /// Throws <see cref="NotFoundException"/> if the document does not exist and <see cref="ConflictException"/> if it
    /// changes while being read (the chunks would otherwise mix two versions).
    /// </summary>
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> StreamBytesAsync(string key, string field, int chunkSize = 1 << 20,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ulong offset = 0;
        ulong? version = null;
        while (true)
        {
            var r = await ReadBytesAsync(key, field, offset, chunkSize, ct).ConfigureAwait(false)
                ?? throw new NotFoundException($"document '{key}' does not exist", Wire.StNotFound);
            if (version is not null && r.Version != version)
                throw new ConflictException("the document changed while it was being read", -1, r.Version);
            version = r.Version;
            if (r.Data.Length == 0) yield break;
            offset += (ulong)r.Data.Length;
            yield return r.Data;
            if (offset >= r.Total) yield break;
        }
    }

    /// <summary>Creates or replaces the document (expiring after <paramref name="ttl"/> if given); returns its new version.</summary>
    public Task<ulong> PutAsync(string key, IDictionary fields, TimeSpan? ttl = null, CancellationToken ct = default) =>
        OneAsync(Wire.Write.Put(Name, key, fields, Transaction.TtlMs(ttl), 0, 0), ct);

    /// <summary>Creates the document; <see cref="ConflictException"/> if it already exists.</summary>
    public Task<ulong> InsertAsync(string key, IDictionary fields, CancellationToken ct = default) =>
        OneAsync(Wire.Write.Put(Name, key, fields, 0, 1, 0), ct);

    /// <summary>
    /// Replaces an existing document (only if it is still at <paramref name="ifVersion"/> when given);
    /// <see cref="ConflictException"/> otherwise.
    /// </summary>
    public Task<ulong> ReplaceAsync(string key, IDictionary fields, ulong? ifVersion = null, CancellationToken ct = default) =>
        OneAsync(Wire.Write.Put(Name, key, fields, 0, ifVersion is null ? 2 : 3, ifVersion ?? 0), ct);

    /// <summary>
    /// Applies field changes atomically on the server; returns the new version:
    /// <c>UpdateAsync("alice", FieldOp.Add("balance", -25.00m).Min(0))</c>.
    /// <see cref="NotFoundException"/> if the document does not exist; <see cref="InvalidException"/> if a bound would be violated.
    /// </summary>
    public Task<ulong> UpdateAsync(string key, params FieldOp[] changes) => UpdateAsync(key, changes, null, default);

    /// <summary>
    /// <see cref="UpdateAsync(string, FieldOp[])"/>, applied only if the document is still at
    /// <paramref name="ifVersion"/> when given (<see cref="ConflictException"/> otherwise).
    /// </summary>
    public Task<ulong> UpdateAsync(string key, FieldOp[] changes, ulong? ifVersion, CancellationToken ct = default) =>
        OneAsync(Wire.Write.Patch(Name, key, ifVersion is null ? 0 : 3, ifVersion ?? 0, changes), ct);

    /// <summary>
    /// Deletes the document (only if it is still at <paramref name="ifVersion"/> when given); returns false if it did
    /// not exist.
    /// </summary>
    public async Task<bool> DeleteAsync(string key, ulong? ifVersion = null, CancellationToken ct = default)
    {
        try
        {
            await OneAsync(new Wire.Write(Name, key, 3, ifVersion is null ? 2 : 3, ifVersion ?? 0), ct).ConfigureAwait(false);
            return true;
        }
        catch (ConflictException e) when (ifVersion is null && e.CurrentVersion is null)
        {
            return false;
        }
    }

    private async Task<Wire.Page> QueryAsync(Filter? where, OrderBy? orderBy, int limit, int offset, string? after, CancellationToken ct)
    {
        if (limit < 0 || offset < 0) throw new InvalidException("limit and offset must not be negative");
        var r = await client.CallAsync(Wire.OpDocQuery, Wire.Query(Name, where, orderBy, (uint)limit, (uint)offset, after), ct)
            .ConfigureAwait(false);
        return Wire.ReadPage(r.Body);
    }

    /// <summary>
    /// Documents matching <paramref name="where"/> (null: all), sorted by <paramref name="orderBy"/> (<c>"-price"</c>
    /// descending; default key order), at most <paramref name="limit"/> (0: server default, 1,000).
    /// </summary>
    public async Task<IReadOnlyList<Document>> FindAsync(Filter? where = null, OrderBy? orderBy = null, int limit = 0,
        int offset = 0, CancellationToken ct = default) =>
        (await QueryAsync(where, orderBy, limit, offset, null, ct).ConfigureAwait(false)).Docs;

    /// <summary>The first matching document, or null.</summary>
    public async Task<Document?> FirstAsync(Filter? where = null, OrderBy? orderBy = null, CancellationToken ct = default) =>
        (await FindAsync(where, orderBy, 1, 0, ct).ConfigureAwait(false)).FirstOrDefault();

    /// <summary>Number of documents matching <paramref name="where"/> (null: all).</summary>
    public async Task<long> CountAsync(Filter? where = null, CancellationToken ct = default) =>
        (await QueryAsync(where, null, 1, 0, null, ct).ConfigureAwait(false)).Total;

    /// <summary>Every document matching <paramref name="where"/> (null: all) in key order, fetched page by page.</summary>
    public async IAsyncEnumerable<Document> ScanAsync(Filter? where = null, [EnumeratorCancellation] CancellationToken ct = default)
    {
        string? after = null;
        do
        {
            var page = await QueryAsync(where, null, 1000, 0, after, ct).ConfigureAwait(false);
            foreach (var d in page.Docs) yield return d;
            after = page.NextKey;
        } while (after is not null);
    }

    /// <summary>
    /// The <paramref name="k"/> documents whose <paramref name="field"/> (a list of numbers) is most similar to
    /// <paramref name="vector"/> (cosine), best first, among those matching <paramref name="where"/> (null: all). Each
    /// document's similarity is <see cref="Document.Score"/>. Uses the field's vector index when it has one (approximate
    /// candidates, exact scores); <paramref name="exact"/> scores every candidate.
    /// </summary>
    public async Task<IReadOnlyList<Document>> NearestAsync(string field, float[] vector, int k = 10, Filter? where = null,
        bool exact = false, CancellationToken ct = default)
    {
        if (k < 0) throw new InvalidException("k must not be negative");
        var nearest = new Wire.Nearest(field, (float[])vector.Clone(), k, exact);
        var r = await client.CallAsync(Wire.OpDocQuery, Wire.Query(Name, where, null, (uint)k, 0, null, nearest), ct)
            .ConfigureAwait(false);
        return Wire.ReadPage(r.Body).Docs;
    }

    /// <summary>
    /// Indexes a top-level field holding <paramref name="dims"/> numbers for <see cref="NearestAsync"/> (at most 4 per
    /// collection); existing documents included. Writes of other values to that field are then refused.
    /// </summary>
    public Task CreateVectorIndexAsync(string field, int dims, CancellationToken ct = default)
    {
        if (dims <= 0) throw new InvalidException("dims must be positive");
        return client.CallAsync(Wire.OpDocAdmin, Wire.CreateVectorIndex(Name, field, dims), ct);
    }

    /// <summary>Removes the vector index on <paramref name="field"/>.</summary>
    public Task DropVectorIndexAsync(string field, CancellationToken ct = default) =>
        client.CallAsync(Wire.OpDocAdmin, Wire.Admin(8, Name, field), ct);

    /// <summary>The collection's vector indexes.</summary>
    public async Task<IReadOnlyList<VectorIndex>> VectorIndexesAsync(CancellationToken ct = default) =>
        Wire.VectorIndexes((await client.CallAsync(Wire.OpDocInfo, Wire.Admin(9, Name), ct).ConfigureAwait(false)).Body);

    /// <summary>Indexes a top-level field (at most 16 per collection); existing documents included.</summary>
    public Task CreateIndexAsync(string field, CancellationToken ct = default) =>
        client.CallAsync(Wire.OpDocAdmin, Wire.Admin(1, Name, field), ct);

    /// <summary>Removes the index on <paramref name="field"/>.</summary>
    public Task DropIndexAsync(string field, CancellationToken ct = default) =>
        client.CallAsync(Wire.OpDocAdmin, Wire.Admin(2, Name, field), ct);

    /// <summary>Deletes every document and the collection's metadata; returns the number deleted.</summary>
    public async Task<ulong> DropAsync(CancellationToken ct = default) =>
        (await client.CallAsync(Wire.OpDocAdmin, Wire.Admin(4, Name), ct).ConfigureAwait(false)).Version;

    /// <summary>Indexes and metadata of the collection.</summary>
    public async Task<CollectionInfo> InfoAsync(CancellationToken ct = default) =>
        Wire.Info(new Values.Reader((await client.CallAsync(Wire.OpDocInfo, Wire.Admin(6, Name), ct).ConfigureAwait(false)).Body));
}

/// <summary>Writes collected by <see cref="StorineClient.TransactionAsync"/>. Methods only buffer; nothing is sent until the body returns.</summary>
public sealed class Transaction
{
    internal readonly List<Wire.Write> Writes = new();

    internal Transaction() { }

    internal static ulong TtlMs(TimeSpan? ttl)
    {
        if (ttl is null) return 0;
        if (ttl.Value <= TimeSpan.Zero) throw new InvalidException("ttl must be positive");
        return (ulong)Math.Max(1, (long)ttl.Value.TotalMilliseconds);
    }

    private Transaction Add(Wire.Write w)
    {
        Writes.Add(w);
        return this;
    }

    /// <summary>Creates or replaces a document (expiring after <paramref name="ttl"/> if given).</summary>
    public Transaction Put(string collection, string key, IDictionary fields, TimeSpan? ttl = null) =>
        Add(Wire.Write.Put(collection, key, fields, TtlMs(ttl), 0, 0));

    /// <summary>Creates a document; the transaction fails with <see cref="ConflictException"/> if it exists.</summary>
    public Transaction Insert(string collection, string key, IDictionary fields) => Add(Wire.Write.Put(collection, key, fields, 0, 1, 0));

    /// <summary>Replaces an existing document (only at <paramref name="ifVersion"/> when given).</summary>
    public Transaction Replace(string collection, string key, IDictionary fields, ulong? ifVersion = null) =>
        Add(Wire.Write.Put(collection, key, fields, 0, ifVersion is null ? 2 : 3, ifVersion ?? 0));

    /// <summary>Applies field changes to an existing document.</summary>
    public Transaction Update(string collection, string key, params FieldOp[] changes) => Add(Wire.Write.Patch(collection, key, 0, 0, changes));

    /// <summary>Applies field changes to a document still at <paramref name="version"/>.</summary>
    public Transaction UpdateIfVersion(string collection, string key, ulong version, params FieldOp[] changes) =>
        Add(Wire.Write.Patch(collection, key, 3, version, changes));

    /// <summary>Deletes a document if it exists (only at <paramref name="ifVersion"/> when given).</summary>
    public Transaction Delete(string collection, string key, ulong? ifVersion = null) =>
        Add(new Wire.Write(collection, key, 3, ifVersion is null ? 0 : 3, ifVersion ?? 0));

    /// <summary>Commits only if the document is at <paramref name="version"/> (it is not written).</summary>
    public Transaction RequireVersion(string collection, string key, ulong version) => Add(new Wire.Write(collection, key, 5, 3, version));

    /// <summary>Commits only if the document exists (true) or does not (false).</summary>
    public Transaction RequireExists(string collection, string key, bool exists) => Add(new Wire.Write(collection, key, 5, exists ? 2 : 1));
}
