---
title: Indexes
description: Secondary indexes that make filters fast, and vector indexes for nearest-neighbour search.
---

## The primary index

Every collection is indexed by document key. The primary index is a 64-shard hash index held in memory, about 21 bytes per document, and saved to disk at each checkpoint so a restart does not rebuild it. Reads by key never scan.

## Secondary indexes

A secondary index covers one **top-level field** and speeds up filters on it: `eq`, `in`, ranges (`lt`, `le`, `gt`, `ge`, `between`) and `prefix`.

```python
people.create_index("city")
people.find(eq("city", "Oslo"))          # now uses the index
people.info()["indexes"]                 # ["city"]
people.drop_index("city")
```

- Up to **16** per collection.
- Creating one indexes the existing documents first. Until it is done, the collection reports the index under `building`, and queries keep working without it. `create_index` returns once the index is built.
- An index never changes results. It proposes candidates, and each one is checked against the whole filter, so results are exact even for values of mixed kinds.
- Creating and dropping indexes needs the `write` role.

## Vector indexes

Store embeddings as a list of numbers in a top-level field, and find the documents most similar to a query vector by **cosine similarity**:

```python
docs = db.collection("docs")
docs.create_vector_index("embedding", 384)            # dimensions
docs.put("d1", {"title": "Raft", "embedding": [0.12, -0.03, ...]})

hits = docs.nearest("embedding", query_vector, k=10, where=eq("lang", "en"))
for d in hits:
    print(d.key, d.score)                                # best first
docs.vector_indexes()                                    # [{"field": "embedding", "dims": 384, "ready": True}]
docs.drop_vector_index("embedding")
```

- Up to **4** vector indexes per collection, of up to **4,096** dimensions, and `k` up to 10,000.
- Once a field has a vector index, writing anything other than a list of exactly that many numbers to it is refused with `Invalid`.
- `nearest` takes an optional filter, and returns the best `k` among the documents that match it.
- Without an index, `nearest` scores every matching document. That is exact and fine for small collections.

### How the search works

When there are fewer than 2,048 candidates, Storine scores all of them exactly. Above that it uses an **HNSW** graph to propose candidates (M 16, ef_construction 64, ef_search 48) and then scores those exactly, so the scores are exact while the candidate set is approximate. Pass `exact=True` to score every candidate instead.

The graph is saved at checkpoints and reused when the server starts. Scoring uses AVX2 with FMA on x86-64 when the CPU has it.

## Columnar storage

Columnar storage is a different kind of index: a second copy of a collection, kept as column segments, for [analytics](analytics.md#columnar-storage).

## Through SQL

The [SQL gateway](../../crates/storage-sql/README.md) creates the same secondary indexes from `CREATE INDEX`, and vector indexes from pgvector-style statements.
