---
title: Core concepts
description: Databases, collections, documents, field values, versions, preconditions and request ids, and the guarantees Storine makes about each.
---

## Collections and documents

A **database** holds **collections**, and a collection holds **documents**.

- A **collection** has a name of up to 255 bytes. It comes into being when the first document is written to it, so there is nothing to create first. Indexes, vector indexes, columnar storage and metadata belong to a collection. The names `$meta` and `$idem` are reserved for the system, and collections whose names start with `$` are left out of collection listings.
- A **document** is a **key** of up to 1,024 bytes of UTF-8 and a map of **fields**. The key is unique within its collection. Field names must not be empty and must not contain a dot: nest maps instead, and reach into them with dotted paths such as `address.city`.

One server holds one database. A server's documents live in its data directory, described in [storage and disk](storage.md).

## Field values

| Type | Notes |
| :--- | :--- |
| null, boolean | |
| integer | 64-bit signed, 64-bit unsigned, and 128-bit |
| float | 64-bit IEEE 754 |
| decimal | Exact, with up to 38 digits after the point. Clients map it to the language's exact decimal type, never to a binary float |
| timestamp | Nanoseconds since 1970-01-01 UTC |
| UUID | 16 bytes |
| id | 24 opaque bytes |
| text | UTF-8 |
| bytes | Raw bytes. Large text and byte values can be read in ranges of up to 16 MiB |
| list, map | Nested up to 32 levels |

How each language represents these types is on its client's page and in [values](reference/values.md). The console and the JSON views use typed forms such as `{"$decimal": "9.99"}` for the types JSON has no word for.

**Use decimals for money.** `Decimal("0.10") + Decimal("0.20")` is exactly `0.30` in Storine, on every client and in every aggregate.

## Comparing values

Numbers compare by value across kinds: the integer `1`, the float `1.0` and the decimal `1.00` are equal, in filters and in group keys. Ordered comparisons (`lt`, `ge`, `between` and so on) only match values of the same kind: a number against a number, text against text.

Sorting needs an order across kinds too. Storine sorts:

```text
null < booleans < numbers < timestamps < text < bytes < UUIDs < ids < lists < maps
```

and breaks ties by document key, so a sort is always stable.

## Versions

Every write gives the document a new **version**, a number that only grows. Reads return it, and writes return the version they created. Versions let you write safely without locks:

1. Read the document and remember its version.
2. Write it back with the precondition "only if it is still at that version".
3. If someone changed it in between, the write fails with a conflict, and you read it again.

The console's editor works this way, and so does every client's `replace(..., if_version=...)`.

## Preconditions

Each write in a request may carry one precondition:

| Precondition | The write fails unless |
| :--- | :--- |
| must not exist | there is no document with that key (what `insert` uses) |
| must exist | the document exists |
| version | the document is at exactly that version |

A transaction can also hold a **check**: a precondition on a document that the transaction does not write, such as "the exchange rate is still at version 42". A failed precondition fails the whole request with a `Conflict`, which tells you which write failed and the document's current version.

## Atomic requests

A request may hold many writes across collections. They are applied **atomically**: all of them or none. Clients expose this as a transaction:

```python
with db.transaction() as tx:
    tx.update("accounts", "alice", add("balance", Decimal("-10.00"), min=0))
    tx.update("accounts", "bob", add("balance", Decimal("10.00")))
    tx.require("rates", "eur", version=42)     # a check: fails the whole transaction if the rate moved
```

A transaction's writes are buffered in the client and sent as one request when it commits. They do not read: compute what to write first, and use preconditions and field operations so the server can check your assumptions. Atomicity holds on a single server, on a single-group cluster, and on a range cluster across ranges on different nodes.

## Field operations

Instead of sending the whole document, a write can send **field operations**, which the server applies to the current version:

| Operation | Effect |
| :--- | :--- |
| `set(path, value)` | Set a field, creating nested maps as needed |
| `unset(path)` | Remove a field |
| `append(path, value)` | Add to the end of a list. A missing field becomes a one-element list |
| `add(path, amount, min, max)` | Add a number. A missing field counts as zero |

`add` is exact and checked:

- integers stay integers, and an overflow is refused instead of wrapping;
- an integer added to a decimal gives a decimal, and floats only combine with floats;
- if the result would cross `min` or `max`, the whole request fails with `Invalid` and nothing changes.

So `add("balance", -25, min=0)` can never overdraw an account, even with many clients updating it at once. There is no read-modify-write race, because the server does the arithmetic.

## Request ids: every write happens once

A network can fail after the server applied a write but before the client heard back. Retrying blindly would apply it twice. Storine avoids this:

- the client attaches a random 16-byte **request id** to each logical write;
- it reuses that id on every retry: after a timeout, a reconnect, a `CommitUnknown` reply, or a redirect to a new cluster leader;
- the server applies a request id **at most once within 24 hours**, and answers a repeat with the result of the first.

Every official client does this for you. You never see request ids unless you write your own client ([wire protocol](../../protocol/PROTOCOL.md), section 5.2).

## Time to live

A document written with a **TTL** (in seconds in the clients) expires when it runs out. From that moment it reads as missing: `get`, queries, counts, scans and aggregates all leave it out. A single-group cluster's leader deletes expired documents every 60 seconds. Writing the document again without a TTL keeps it.

## Consistency

- On a single server, every read sees every acknowledged write.
- On a cluster, reads go to the leader by default and see every acknowledged write. A client may allow **follower reads**, which can be slightly behind in exchange for spreading load (`follower_reads=1` in the connection string).
- Queries and aggregates run on one consistent snapshot. A page of results never mixes two versions of a transaction.
- A write is acknowledged once it is durable: on a single server, after the write-ahead log is synced to disk (by default); on a cluster, once a majority of nodes have it in their logs.

## Durability

A write goes to the **write-ahead log** first. By default the server syncs the log to disk before acknowledging, and shares each sync between writers arriving at the same time ("group commit"), so a power failure loses nothing that was acknowledged. Embedded programs can choose other policies, described in [storage and disk](storage.md#the-write-ahead-log).
