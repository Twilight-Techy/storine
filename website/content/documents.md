---
title: Documents and writes
description: Every way to write a document, how to read all or part of one, preconditions, transactions, time to live and large values.
---

The examples on this page are in Python. Every client has the same operations under the same names, in its language's style: `if_version` in Python and Ruby, `ifVersion` in TypeScript, `IfVersion` in Go, `updateIfVersion` in Java and PHP, and so on. Your client's page lists its exact signatures.

```python
import storine
from decimal import Decimal
from storine import add, append, set_, unset

db = storine.connect("storine://127.0.0.1:7878")
people = db.collection("people")
```

## Writing

| Operation | Creates | Replaces | Fails when |
| :--- | :---: | :---: | :--- |
| `put(key, fields)` | yes | yes | never, for a valid document |
| `insert(key, fields)` | yes | no | the key exists (`Conflict`) |
| `replace(key, fields)` | no | yes | the key does not exist (`Conflict`) |
| `replace(key, fields, if_version=v)` | no | yes | the document is not at version `v` (`Conflict`) |
| `update(key, *changes)` | no | changes fields | the key does not exist (`NotFound`), or a bound is crossed (`Invalid`) |
| `delete(key)` | | | never. Returns whether the document existed |
| `delete(key, if_version=v)` | | | the document is not at version `v` (`Conflict`) |

Each returns the new version, except `delete`.

```python
v1 = people.put("ada", {"name": "Ada", "born": 1815, "langs": ["en", "fr"]})
people.insert("alan", {"name": "Alan", "born": 1912})
people.replace("ada", {"name": "Ada Lovelace", "born": 1815}, if_version=v1)
people.delete("alan")
```

`put` and `replace` replace the whole document: fields you leave out are gone. To change some fields and keep the rest, use `update`.

## Updating fields on the server

`update` sends field operations, applied by the server to the current version in one atomic step:

```python
people.update("ada",
    set_("address.city", "London"),     # creates the nested map if needed
    unset("langs"),
    append("notes", "wrote the first program"),
    add("visits", 1),
)
people.update("ada", {"title": "Countess"})   # a dict is a set_ for each field
```

`add` takes an optional `min` and `max`. The result must stay within them, or the whole request fails with `Invalid` and nothing changes:

```python
accounts.update("alice", add("balance", Decimal("-25.00"), min=0))       # never below zero
accounts.update("room-7", add("guests", 1, max=12))                      # never above 12
```

The rules of `add` are in [core concepts](concepts.md#field-operations). Python names the setter `set_` so it does not hide the built-in `set`.

## Transactions

A transaction buffers writes and sends them as one atomic request. Any write type works inside it, across any collections:

```python
with db.transaction() as tx:                          # commits when the block ends
    tx.update("accounts", "alice", add("balance", Decimal("-10.00"), min=0))
    tx.update("accounts", "bob", add("balance", Decimal("10.00")))
    tx.insert("transfers", "t-1001", {"from": "alice", "to": "bob", "amount": Decimal("10.00")})
    tx.require("limits", "alice", version=7)          # a check on a document it does not write
```

If any write or check fails, none of them happens, and the error says which write failed. `commit()` returns each document's new version, in order. An exception inside the `with` block discards the transaction.

`tx.require(collection, key, version=v)` checks a version, `tx.require(collection, key)` checks that a document exists, and `tx.require(collection, key, exists=False)` that it does not.

A transaction is limited only by the request size, 256 MiB.

## Reading

```python
doc = people.get("ada")          # a Document, or None
doc["name"], doc.key, doc.version
```

A document read is a dict of its fields with `.key` and `.version`. Values come back as the types they were written with: a decimal as `Decimal`, a timestamp as a UTC `datetime`, and so on.

### Some fields only

`get` can return only some fields, given as dotted paths. Nested paths come back nested:

```python
people.get("ada", fields=["name", "address.city"])     # {"name": ..., "address": {"city": ...}}
```

At most 256 paths per read. This is how to read the metadata stored beside a large value without transferring the value.

## Large values

A document can hold a large `bytes` or `text` field, such as a file, next to small metadata. Read a slice of it, or stream all of it in chunks of up to 16 MiB:

```python
files = db.collection("files")
files.put("f1", {"name": "cat.png", "size": len(data), "data": data})

head = files.read_bytes("f1", "data", 0, 4096)       # ByteRange(version, total, data)
with open("cat.png", "wb") as out:
    for chunk in files.stream_bytes("f1", "data", chunk_size=1 << 20):
        out.write(chunk)
```

`read_bytes` returns `None` if the document does not exist, and empty `data` past the end of the value. `stream_bytes` checks the version between chunks, and raises `ConflictError` if the document changed while it was being read, so you never get a file that mixes two versions. A whole document is limited to 256 MiB.

## Time to live

```python
sessions = db.collection("sessions")
sessions.put("s-81f2", {"user": "ada"}, ttl=3600)     # seconds
```

An expired document reads as missing at once, everywhere. See [core concepts](concepts.md#time-to-live).

## Values

Write native values and the client picks the Storine type:

```python
from datetime import datetime, timezone
import uuid

people.put("grace", {
    "name": "Grace",                                   # text
    "born": 1906,                                      # integer
    "height_m": 1.68,                                  # float
    "salary": Decimal("12500.00"),                     # decimal
    "joined": datetime(1943, 7, 1, tzinfo=timezone.utc),   # timestamp
    "badge": uuid.uuid4(),                             # UUID
    "photo": b"\x89PNG...",                            # bytes
    "ranks": ["lieutenant", "rear admiral"],           # list
    "address": {"city": "Arlington"},                  # map
})
```

Each client's mapping is in [values](reference/values.md).

## Collections

```python
db.collections()                 # names of every collection
people.info()                    # {"name", "indexes", "building", metadata ...}
people.drop()                    # deletes every document, index and the metadata; returns the count
```

A collection exists once a document is written to it, or once it has an index or metadata. Range clusters list only collections that have an index or metadata.

## Errors

| Error | When | Retry? |
| :--- | :--- | :--- |
| `ConflictError` | A precondition failed. Carries the index of the failed write and the document's current version | Read again, then decide |
| `NotFoundError` | `update` on a missing document | No |
| `InvalidError` | A malformed value, a type mismatch, a crossed bound | No |
| `UnauthorizedError` | No token, or a role too low for the request | No |
| `OverloadedError`, `RateLimitedError` | The server is busy, or this connection sent too many requests | Later, with backoff |
| `UnavailableError` | No server could be reached | Later |

The clients retry timeouts, reconnects, `CommitUnknown` and leader changes themselves, with the same request id, so a write is never applied twice. The full list is in [errors](reference/errors.md).
