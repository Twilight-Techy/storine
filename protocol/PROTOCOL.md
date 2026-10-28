# Storine Wire Protocol (document API)

This is the contract every SDK implements. It is deliberately small: fixed little-endian fields,
length-prefixed strings, one tag byte per value. Golden byte sequences for every encoding live in
[`vectors/`](vectors/); an SDK is conformant when it reproduces them byte for byte and passes its
end-to-end test against a real `storage-server`.

All integers are **little-endian**. `str` = `u32` byte length + UTF-8 bytes. Sizes are in bytes.

## 1. Connection

- Plain TCP (put TLS in front with a proxy or sidecar if the network is untrusted). Default port `7878`.
- One request at a time per connection: send a request, read its response, repeat. Use one
  connection per thread / task, or a pool.
- Connection strings: `storine://[token@]host:port[,host:port...][?timeout_ms=5000&follower_reads=1]`.
  Several hosts form the seed list of a cluster.

## 2. Framing

**Request** (29-byte header + body):

| Offset | Size | Field |
|---:|---:|---|
| 0 | 1 | `opcode` |
| 1 | 24 | `key` — unused by the document API: send 24 zero bytes |
| 25 | 4 | `body_len` (`u32`) |
| 29 | `body_len` | body |

**Response** (13-byte header + body):

| Offset | Size | Field |
|---:|---:|---|
| 0 | 1 | `status` |
| 1 | 8 | `version` (`u64`, meaning depends on the opcode) |
| 9 | 4 | `body_len` (`u32`) |
| 13 | `body_len` | body |

Bodies are capped at 256 MiB. A request rejected before its body was read (unauthorized,
too large, rate limited) gets an error response **and the server closes the connection**: reconnect.

## 3. Status codes

| Code | Name | Meaning / what an SDK does |
|---:|---|---|
| `0x00` | `Ok` | Success. |
| `0x01` | `NotFound` | The document (or the patched document) does not exist. |
| `0x02` | `Error` | Server-side failure (I/O…); message in the body. |
| `0x03` | `Overloaded` | Too many connections / queue full: retry later with backoff. |
| `0x04` | `Conflict` | A precondition failed. Body: `u32 write_index | u8 exists | u64 current_version | message`. |
| `0x05` | `Unauthorized` | Not authenticated or role too low (connection closed). |
| `0x06` | `RateLimited` | Per-connection rate limit exceeded (connection closed): retry later. |
| `0x07` | `NotLeader` | Cluster follower. `version` = leader node id (`0` unknown), body = leader `host:port` (may be empty). Retry on the leader; with an empty address, wait briefly and retry any seed. |
| `0x08` | `Invalid` | Malformed request, bad value, type mismatch, bound violated (e.g. overdraft). Do not retry. |
| `0x09` | `CommitUnknown` | A replicated write could not be confirmed (it may still commit). Retry **with the same request id**. |

Error bodies (other than `Conflict`) are UTF-8 messages meant for humans.

## 4. Values (SVE — Storine Value Encoding)

One tag byte, then the payload:

| Tag | Type | Payload | Native mapping (examples) |
|---:|---|---|---|
| `0x00` | null | – | `None` / `null` / `nil` |
| `0x01` | bool | `u8` (`0` or `1`) | `bool` |
| `0x02` | int64 | `i64` | integers that fit in 64 bits |
| `0x03` | uint64 | `u64` | unsigned 64-bit integers (only when explicitly requested) |
| `0x04` | float64 | IEEE-754 bits as `u64` | `float` / `double` |
| `0x05` | int128 | `i128` (16 bytes) | big integers (`int`, `bigint`, `BigInteger`, `Int128`) |
| `0x06` | timestamp | `i64` nanoseconds since 1970-01-01T00:00:00Z | native date-times (UTC) |
| `0x07` | uuid | 16 bytes, RFC 4122 byte order | `UUID` / `Guid` |
| `0x08` | id | 24 raw bytes (engine `InlineId`) | opaque wrapper type |
| `0x09` | text | `str` | `string` |
| `0x0A` | bytes | `u32` length + bytes | byte arrays |
| `0x0B` | decimal | `i128` mantissa + `u8` scale (`0..=38`); value = `mantissa / 10^scale` | `Decimal` / `BigDecimal` / `decimal` |
| `0x0C` | list | `u32` count + values | arrays / lists |
| `0x0D` | map | `u32` count + (`str` key + value)* — keys unique | dictionaries / objects |

Rules: nesting depth at most 32; map keys unique; encoders emit map keys in ascending byte order
(deterministic bytes, as in the vectors); field names in a document
must be non-empty and must not contain `.` (use nested maps). Decoders must reject truncated
input, unknown tags, invalid UTF-8, bool bytes other than 0/1, scales above 38, and duplicate keys.

A **document body** is a map body: `u32 count | (str field, value)*`.

## 5. Document operations

All bodies below start at offset 29 of the request.

### 5.1 `OP_DOC_GET` (`0x20`, role: read)

Request: `str collection | str key`, optionally followed by one shape:

```
(none)                                         whole document
u8 1 | str field | u64 offset | u32 length      a byte range of one field
u8 2 | u32 count | str path*                    only these fields (count <= 256)
```

Response: `Ok` (`version` = document version) or `NotFound` (no such document).

- **Whole document**: body = document body.
- **Byte range**: body = `u64 total | u32 n | n bytes`. `field` (a dotted path) must hold
  `bytes` or `text` (its UTF-8 bytes), else `Invalid`. `total` is the value's full length; the
  bytes are `offset .. min(offset + length, total)`, so a read at or past the end returns `n = 0`.
  `length` is at most 16 MiB (`Invalid` above). Read a large value in chunks, checking that
  `version` stays the same between chunks.
- **Fields**: body = document body holding only the listed paths that exist; a nested path
  `a.b` comes back nested (`{a: {b: …}}`). Use it to read metadata stored beside a large value
  without transferring the value.

### 5.2 `OP_DOC_WRITE` (`0x21`, role: write)

Applies every write **atomically** (all or nothing, across collections).

```
u8 has_request_id | [16] request_id (if has_request_id = 1)
u32 count
count × ( str collection | str key | precondition | write )

precondition := u8 kind
    0 = none | 1 = must not exist | 2 = must exist | 3 = version, followed by u64 version
write := u8 kind
    1 = put        → document body
    2 = put + TTL  → u64 ttl_ms | document body
    3 = delete
    4 = patch      → u32 count | field_op*      (the document must exist)
    5 = check      (precondition only)
```

`field_op` (applied in order to a private copy; any failure aborts the whole request):

```
u8 kind | str path              (dotted path into nested maps: "limits.daily")
    1 = set     → value
    2 = unset
    3 = add     → value delta | u8 flags (bit0: min follows, bit1: max follows) | [value min] | [value max]
    4 = append  → value          (to a list; a missing field becomes a one-element list)
```

`add` is exact and checked: integers stay integers (overflow → `Invalid`), mixing with a decimal
gives a decimal, floats only combine with floats; a missing field counts as zero; the result must
satisfy `min <= result <= max` (e.g. `min = 0` makes an overdraft impossible).

Response `Ok`: `u8 duplicate | u32 count | count × (u8 exists | u64 version)` — the version of each
written document afterwards. `duplicate = 1` means this request id was already applied (nothing
was written again; versions are current). Failures: `Conflict` (see §3), `NotFound` (patch of a
missing document; body starts with `u32 write_index`), `Invalid`.

**Request ids**: 16 random bytes generated by the SDK for each logical write and **reused on every
retry of that write** (timeouts, reconnects, `CommitUnknown`, `NotLeader`). The server applies a
request id at most once within 24 hours.

### 5.3 `OP_DOC_QUERY` (`0x22`, role: read)

```
str collection
u8 has_filter | filter (if 1)
u32 sort_count (<= 16) | sort_count × (str path | u8 descending)
u32 limit      (0 = default 1,000; capped at 10,000)
u32 offset
u8 has_after | str after_key (if 1)   (resume after this key; only without sort keys)
[u8 1 | str field | u32 k | u8 exact | u32 dims | dims × f32]   (optional: nearest neighbours)
```

The optional trailing block (absent for ordinary queries) asks for the `k` documents whose
`field` (a list of numbers) is most similar to the vector by cosine similarity, best first,
among those matching the filter. Clients send `limit = k`, `offset = 0`, no sort keys and no
cursor. Each result carries its similarity in the extra field `$score` (`f64`). With a vector
index on the field (below) the server proposes candidates from an HNSW graph and scores them
exactly; `exact = 1`, or a small candidate set, scores every candidate. Wrong dimensions are
`Invalid`.

`filter`:

```
u8 kind
    1 eq  | 2 ne  | 3 lt  | 4 le  | 5 gt  | 6 ge     → str path | value
    7 between (inclusive)                           → str path | value low | value high
    8 in                                            → str path | u32 n | value*
    9 prefix (text starts with)                     → str path | str prefix
   10 contains (substring of text, or list element) → str path | value
   11 exists (present and not null)                 → str path
   12 missing (absent or null)                      → str path
   13 key_prefix                                    → str prefix
   14 and | 15 or                                   → u32 n | filter*
   16 not                                           → filter
```

Comparison semantics: numbers compare numerically across int / decimal / float (`1 == 1.0 == 1.00`);
ordered comparisons hold only between values of the same kind (number vs number, text vs text …);
`eq null` matches missing or null fields. Total sort order across kinds: null < bool < numbers <
timestamps < text < bytes < uuid < id < list < map; ties broken by document key.

Response `Ok` (`version` = total matches):
`u32 total | u8 has_next | str next_after_key (if 1) | u32 count | count × (str key | u64 version | document body)`.

### 5.4 Collections: `OP_DOC_ADMIN` (`0x23`, role: write) and `OP_DOC_INFO` (`0x25`, role: read)

Body: `u8 command | arguments`.

| Command | Opcode | Arguments | Response |
|---:|---|---|---|
| `1` create index | `0x23` | `str collection | str field` | `Ok` once the index is built (existing documents included) |
| `2` drop index | `0x23` | `str collection | str field` | `Ok` |
| `3` list collections | `0x25` | – | `u32 n | info*` |
| `4` drop collection | `0x23` | `str collection` | `Ok`, `version` = documents deleted |
| `5` set metadata | `0x23` | `str collection | map body` | `Ok` |
| `6` collection info | `0x25` | `str collection` | `info` |
| `7` create vector index | `0x23` | `str collection | str field | u32 dims` | `Ok` once existing documents are indexed; later writes of anything but `dims` numbers to the field are `Invalid` |
| `8` drop vector index | `0x23` | `str collection | str field` | `Ok` |
| `9` vector indexes | `0x25` | `str collection` | `u32 n | (str field | u32 dims | u8 ready)*` |

`info := str name | u32 n | str index* | u32 m | str building_index* | map body (metadata)`.
At most 16 secondary indexes and 4 vector indexes (up to 4,096 dimensions) per collection; indexes
cover one top-level field; filters are always exact (index hits are re-checked).

## 6. Session and cluster

| Opcode | Role | Body | Response |
|---|---|---|---|
| `OP_AUTH` `0x0C` | – | token (UTF-8) | `Ok`, `version` = role (`1` read, `2` write, `3` admin) or `Unauthorized` |
| `OP_SET_OPTIONS` `0x26` | read | `u8 flags` (bit 0: allow possibly stale follower reads) | `Ok` |
| `OP_CLUSTER_INFO` `0x27` | read | – | `u64 node_id | u8 is_leader | u64 leader_id | str leader_addr | u64 term | u64 commit | u64 applied | u32 n | (u64 node_id | str addr)* | n × u8 role` (`1` voter, `0` learner; absent from servers before membership changes) |
| `OP_CLUSTER_CHANGE` `0x28` | admin | `u8 action | u16 node_id | str addr` (action `1` add voter, `2` add learner, `3` promote, `4` remove; `addr` for adds) | `Ok` once the change is applied; `NotLeader` on followers; `Invalid` when refused (change in flight, more than one voter, unknown member) |
| `OP_HEALTH` `0x13` | read | – | JSON text |

A standalone server reports `node_id = 0`, `is_leader = 1`.

**SDK behavior**: authenticate right after connecting (if a token is configured); on `NotLeader`
connect to the given address (or another seed) and resend the same request (same request id);
on connection errors and `CommitUnknown`, reconnect and resend writes with the same request id;
reads go to the leader unless the user enabled follower reads (then send `OP_SET_OPTIONS 1`).

## 7. Typed-record opcodes (Rust embedders)

`0x01`–`0x0B`, `0x24 OP_TXN` and the admin opcodes operate on raw engine record frames and are
documented in `crates/storage-server/src/lib.rs`. SDKs do not need them.

## 8. Not yet in the protocol (TODO)

PHP and Ruby SDKs (TODO), TLS termination inside the server, streaming change feeds.
