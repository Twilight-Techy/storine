---
title: Queries
description: Filter, sort and page through a collection, count matches, and scan collections of any size.
---

A query has an optional **filter**, up to 16 **sort keys**, a **limit** and either an **offset** or a key to **resume after**. It runs on the server, on one consistent snapshot, and its results are always exact: an index only proposes candidates, and every candidate is checked against the filter.

```python
from storine import eq, gt, between, in_, prefix, contains, exists, missing, key_prefix

people = db.collection("people")
people.find(gt("born", 1900), order_by="-born", limit=20)
people.first(eq("name", "Ada"))
people.count(eq("address.city", "London"))
for doc in people.scan(prefix("name", "A")):
    ...
```

## Filters

| Filter | Matches documents where | Example |
| :--- | :--- | :--- |
| `eq(path, v)` | the field equals `v`. `eq(path, None)` matches a missing or null field | `eq("city", "Oslo")` |
| `ne(path, v)` | the field does not equal `v` | `ne("status", "done")` |
| `lt`, `le`, `gt`, `ge` | the field is less than, at most, greater than, at least `v` | `ge("age", 18)` |
| `between(path, lo, hi)` | `lo <= field <= hi`, both ends included | `between("price", 10, 20)` |
| `in_(path, values)` | the field equals one of the values | `in_("city", ["Oslo", "Lima"])` |
| `prefix(path, p)` | the text field starts with `p` | `prefix("email", "admin@")` |
| `contains(path, v)` | the text field contains `v` as a substring, or the list field contains `v` as an element | `contains("tags", "vip")` |
| `exists(path)` | the field is present and not null | `exists("phone")` |
| `missing(path)` | the field is absent or null | `missing("deleted_at")` |
| `key_prefix(p)` | the document key starts with `p` | `key_prefix("2026-11/")` |

Combine filters with `and`, `or` and `not`. In Python these are the operators `&`, `|` and `~`, or the functions `and_`, `or_` and `not_`:

```python
adults_in_oslo = eq("city", "Oslo") & ge("age", 18)
either = eq("city", "Oslo") | eq("city", "Lima")
not_done = ~eq("status", "done")
```

A dict is a shortcut for `eq` on each field, combined with `and`:

```python
people.find({"city": "Oslo", "active": True})
```

### Paths

A path names a field, and dots reach into nested maps: `address.city` is the `city` field of the map in `address`. A path that does not exist reads as missing.

### How values compare

- Numbers compare by value across kinds: `eq("qty", 1)` matches `1`, `1.0` and `Decimal("1.00")`.
- `lt`, `le`, `gt`, `ge` and `between` match only values of the same kind as the one given: `gt("age", 18)` never matches the text `"30"`.
- `eq(path, None)` matches null and missing fields.
- Text compares by its UTF-8 bytes.

## Sorting

`order_by` takes one path or a list of paths. A leading `-` sorts that key in descending order:

```python
people.find(order_by=["city", "-born"])
```

Sorting uses one order across all kinds of values:

```text
null < booleans < numbers < timestamps < text < bytes < UUIDs < ids < lists < maps
```

Ties are broken by document key, so the same query always returns the same order. Without sort keys, results come in key order.

## Paging

`find` returns at most `limit` documents: 1,000 by default and 10,000 at most. There are two ways to get the next page.

**Offset**: skip that many results. Simple, and works with any sort, but each page re-reads the ones it skips.

```python
page3 = people.find(eq("city", "Oslo"), order_by="name", limit=50, offset=100)
```

**Keyset**: resume after the last key of the previous page. Each page costs the same however deep you go. It works only in key order, without sort keys, and it is what `scan` does for you:

```python
for doc in people.scan(eq("city", "Oslo")):     # every match, in pages of 1,000
    ...
```

The Rust client also returns one page with its total and the next page's key (`Collection::page`), for building paging into an API.

## Counting

`count(where)` returns the number of matches without sending documents. Every query response carries the total too.

## Making queries fast

A query without a usable index reads the whole collection. That is fine for small collections and for one-off work. For queries you run often:

- create a [secondary index](indexes.md) on a field you filter by with `eq`, `in`, ranges or `prefix`;
- filter by `key_prefix` where your keys allow it: keys like `2026-11/order-123` make a month's orders a key range;
- for grouping and sums, use [aggregates](analytics.md) instead of fetching documents.

## Vector search

To find the documents most similar to a vector, use `nearest`, described with [vector indexes](indexes.md#vector-indexes).

## Limits

| Limit | Value |
| :--- | :--- |
| Results per page | 1,000 by default, 10,000 at most |
| Sort keys | 16 |
| Query request size | 1 MiB |
