using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Storine;

/// <summary>A stored document: its key, version and fields.</summary>
public sealed record Document(string Key, ulong Version, Doc Fields)
{
    /// <summary>The field <paramref name="name"/>, or null.</summary>
    public object? this[string name] => Fields.TryGetValue(name, out object? v) ? v : null;

    /// <summary>Similarity of a <c>NearestAsync</c> result (the <c>$score</c> field), or null for other queries.</summary>
    public double? Score => this["$score"] as double?;
}

/// <summary>
/// A slice of a bytes (or text) field read with <c>ReadBytesAsync</c>: <c>Data</c> starts at the requested offset,
/// <c>Total</c> is the whole value's length and <c>Version</c> the document version it was read from.
/// </summary>
public sealed record ByteRange(ulong Version, ulong Total, byte[] Data);

/// <summary>A vector index of a collection (<c>Ready</c>: built from the existing documents).</summary>
public sealed record VectorIndex(string Field, int Dims, bool Ready);

/// <summary>A collection's indexes (<c>Building</c>: being built from existing documents) and metadata.</summary>
public sealed record CollectionInfo(string Name, IReadOnlyList<string> Indexes, IReadOnlyList<string> Building, Doc Metadata);

/// <summary>The server currently used and its view of the cluster (leader fields null when unknown).</summary>
public sealed record ClusterInfo(ulong NodeId, bool IsLeader, ulong? LeaderId, string? LeaderAddr, ulong Term,
    ulong CommitIndex, ulong LastApplied, IReadOnlyDictionary<ulong, string> Members);

/// <summary>Request / response encodings of the document API (protocol/PROTOCOL.md §5-§6).</summary>
internal static class Wire
{
    internal const byte OpAuth = 0x0C, OpDocGet = 0x20, OpDocWrite = 0x21, OpDocQuery = 0x22, OpDocAdmin = 0x23,
        OpDocInfo = 0x25, OpSetOptions = 0x26, OpClusterInfo = 0x27;
    internal const int StOk = 0, StNotFound = 1, StError = 2, StOverloaded = 3, StConflict = 4, StUnauthorized = 5,
        StRateLimited = 6, StNotLeader = 7, StInvalid = 8, StCommitUnknown = 9;

    /// <summary>kind: 1 put, 2 put+ttl, 3 delete, 4 patch, 5 check. pre: 0 none, 1 absent, 2 exists, 3 version.</summary>
    internal sealed record Write(string Collection, string Key, int Kind, int Pre = 0, ulong Version = 0,
        IDictionary? Fields = null, ulong TtlMs = 0, IReadOnlyList<FieldOp>? Ops = null)
    {
        internal static Write Put(string c, string k, IDictionary fields, ulong ttlMs, int pre, ulong version) =>
            new(c, k, ttlMs > 0 ? 2 : 1, pre, version, fields ?? throw new InvalidException("a document is a dictionary of fields"), ttlMs);

        internal static Write Patch(string c, string k, int pre, ulong version, FieldOp[] ops) => ops.Length == 0
            ? throw new InvalidException("UpdateAsync needs at least one change (Set, Unset, Add, Append)")
            : new(c, k, 4, pre, version, Ops: ops);
    }

    internal static byte[] Get(string collection, string key)
    {
        var w = new Values.Writer();
        w.Str(collection);
        w.Str(key);
        return w.ToArray();
    }

    /// <summary>Most bytes one ranged read returns (16 MiB).</summary>
    internal const int MaxByteRange = 16 * 1024 * 1024;

    /// <summary><c>OP_DOC_GET</c> with a byte-range shape: bytes <c>offset .. offset + length</c> of one bytes/text field.</summary>
    internal static byte[] GetRange(string collection, string key, string field, ulong offset, int length)
    {
        if (length < 0 || length > MaxByteRange) throw new InvalidException($"length must be between 0 and {MaxByteRange}");
        var w = new Values.Writer();
        w.Str(collection);
        w.Str(key);
        w.U8(1);
        w.Str(field);
        w.U64(offset);
        w.U32(length);
        return w.ToArray();
    }

    /// <summary><c>OP_DOC_GET</c> with a projection shape: only the given (dotted) field paths.</summary>
    internal static byte[] GetFields(string collection, string key, IReadOnlyList<string> paths)
    {
        if (paths.Count > 256) throw new InvalidException("at most 256 fields can be projected");
        var w = new Values.Writer();
        w.Str(collection);
        w.Str(key);
        w.U8(2);
        w.U32(paths.Count);
        foreach (var p in paths) w.Str(p);
        return w.ToArray();
    }

    /// <summary><c>u64 total | u32 n | n bytes</c>.</summary>
    internal static (ulong Total, byte[] Data) ReadByteRange(byte[] body)
    {
        var r = new Values.Reader(body);
        ulong total = r.U64();
        byte[] data = r.Bytes(r.U32());
        if (r.Remaining != 0) throw new InvalidException("trailing bytes after byte range");
        return (total, data);
    }

    internal static byte[] Writes(IReadOnlyList<Write> writes, byte[]? requestId)
    {
        var w = new Values.Writer();
        if (requestId is null)
        {
            w.U8(0);
        }
        else
        {
            w.U8(1);
            w.Raw(requestId);
        }
        w.U32(writes.Count);
        foreach (var x in writes)
        {
            w.Str(x.Collection);
            w.Str(x.Key);
            w.U8(x.Pre);
            if (x.Pre == 3) w.U64(x.Version);
            w.U8(x.Kind);
            switch (x.Kind)
            {
                case 1:
                    Values.WriteFields(x.Fields!, w, 1);
                    break;
                case 2:
                    w.U64(x.TtlMs);
                    Values.WriteFields(x.Fields!, w, 1);
                    break;
                case 4:
                    w.U32(x.Ops!.Count);
                    foreach (var op in x.Ops) op.Encode(w);
                    break;
            }
        }
        return w.ToArray();
    }

    /// <summary>The optional nearest-neighbour block of a query: the <c>K</c> documents whose <c>Field</c> is most similar to <c>Vector</c>.</summary>
    internal sealed record Nearest(string Field, float[] Vector, int K, bool Exact);

    internal static byte[] Query(string collection, Filter? where, OrderBy? orderBy, uint limit, uint offset, string? afterKey,
        Nearest? nearest = null)
    {
        var w = new Values.Writer();
        w.Str(collection);
        if (where is null)
        {
            w.U8(0);
        }
        else
        {
            w.U8(1);
            where.Encode(w, 0);
        }
        var keys = orderBy?.Keys ?? new List<(string, bool)>();
        w.U32(keys.Count);
        foreach (var (path, desc) in keys)
        {
            w.Str(path);
            w.U8(desc ? 1 : 0);
        }
        w.U32(limit);
        w.U32(offset);
        if (afterKey is null)
        {
            w.U8(0);
        }
        else
        {
            w.U8(1);
            w.Str(afterKey);
        }
        if (nearest is not null)
        {
            // Optional trailing block: u8 1 | str field | u32 k | u8 exact | u32 dims | dims x f32.
            w.U8(1);
            w.Str(nearest.Field);
            w.U32(nearest.K);
            w.U8(nearest.Exact ? 1 : 0);
            w.U32(nearest.Vector.Length);
            foreach (float f in nearest.Vector) w.U32(BitConverter.SingleToUInt32Bits(f));
        }
        return w.ToArray();
    }

    internal static byte[] CreateVectorIndex(string collection, string field, int dims)
    {
        var w = new Values.Writer();
        w.U8(7);
        w.Str(collection);
        w.Str(field);
        w.U32(dims);
        return w.ToArray();
    }

    internal static List<VectorIndex> VectorIndexes(byte[] body)
    {
        var r = new Values.Reader(body);
        int n = r.Count(9);
        var list = new List<VectorIndex>(n);
        for (int i = 0; i < n; i++)
        {
            string field = r.Str();
            int dims = (int)r.U32();
            list.Add(new VectorIndex(field, dims, r.U8() == 1));
        }
        return list;
    }

    internal static byte[] Admin(int sub, string collection, string? field = null)
    {
        var w = new Values.Writer();
        w.U8(sub);
        w.Str(collection);
        if (field is not null) w.Str(field);
        return w.ToArray();
    }

    /// <summary>New version of each write; null for a deleted / absent document.</summary>
    internal static List<ulong?> WriteResponse(byte[] body)
    {
        var r = new Values.Reader(body);
        r.U8(); // duplicate flag
        int n = r.Count(9);
        var versions = new List<ulong?>(n);
        for (int i = 0; i < n; i++)
        {
            bool exists = r.U8() != 0;
            ulong v = r.U64();
            versions.Add(exists ? v : null);
        }
        return versions;
    }

    internal sealed record Page(uint Total, string? NextKey, List<Document> Docs);

    internal static Page ReadPage(byte[] body)
    {
        var r = new Values.Reader(body);
        uint total = r.U32();
        string? next = r.U8() != 0 ? r.Str() : null;
        int n = r.Count(16);
        var docs = new List<Document>(n);
        for (int i = 0; i < n; i++)
        {
            string key = r.Str();
            ulong version = r.U64();
            docs.Add(new Document(key, version, r.Fields(1)));
        }
        return new Page(total, next, docs);
    }

    internal static ConflictException Conflict(byte[] body)
    {
        if (body.Length < 13) return new ConflictException(Encoding.UTF8.GetString(body), -1, null);
        var r = new Values.Reader(body);
        uint index = r.U32();
        bool exists = r.U8() != 0;
        ulong version = r.U64();
        return new ConflictException(Encoding.UTF8.GetString(body, 13, body.Length - 13),
            index == uint.MaxValue ? -1 : (int)index, exists ? version : null);
    }

    internal static CollectionInfo Info(Values.Reader r)
    {
        string name = r.Str();
        var indexes = Strings(r);
        var building = Strings(r);
        return new CollectionInfo(name, indexes, building, r.Fields(1));
    }

    private static List<string> Strings(Values.Reader r)
    {
        int n = r.Count(4);
        var list = new List<string>(n);
        for (int i = 0; i < n; i++) list.Add(r.Str());
        return list;
    }

    internal static ClusterInfo ReadClusterInfo(byte[] body)
    {
        var r = new Values.Reader(body);
        ulong nodeId = r.U64();
        bool leader = r.U8() != 0;
        ulong leaderId = r.U64();
        string addr = r.Str();
        ulong term = r.U64(), commit = r.U64(), applied = r.U64();
        int n = r.Count(12);
        var members = new Dictionary<ulong, string>();
        for (int i = 0; i < n; i++)
        {
            ulong id = r.U64();
            members[id] = r.Str();
        }
        return new ClusterInfo(nodeId, leader, leaderId == 0 ? null : leaderId, addr.Length == 0 ? null : addr,
            term, commit, applied, members);
    }
}
