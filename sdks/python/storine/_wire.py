"""Request / response encodings of the document API (protocol/PROTOCOL.md §5-§6)."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from ._sve import Reader, encode, encode_map, put_str
from .errors import InvalidError

OP_AUTH = 0x0C
OP_DOC_GET = 0x20
OP_DOC_WRITE = 0x21
OP_DOC_QUERY = 0x22
OP_DOC_ADMIN = 0x23
OP_DOC_INFO = 0x25
OP_SET_OPTIONS = 0x26
OP_CLUSTER_INFO = 0x27

(ST_OK, ST_NOT_FOUND, ST_ERROR, ST_OVERLOADED, ST_CONFLICT, ST_UNAUTHORIZED, ST_RATE_LIMITED,
 ST_NOT_LEADER, ST_INVALID, ST_COMMIT_UNKNOWN) = range(10)


# ── Field operations ─────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class FieldOp:
    """One atomic change applied server-side by `update` (build with set_/unset/add/append)."""

    kind: int
    path: str
    value: Any = None
    min: Any = None
    max: Any = None
    has_min: bool = False
    has_max: bool = False


def set_(path: str, value: Any) -> FieldOp:
    """Set the field at `path` (dotted for nested maps) to `value`."""
    return FieldOp(1, path, value)


def unset(path: str) -> FieldOp:
    """Remove the field at `path`."""
    return FieldOp(2, path)


_MISSING = object()


def add(path: str, amount: Any, *, min: Any = _MISSING, max: Any = _MISSING) -> FieldOp:  # noqa: A002
    """Add `amount` (negative to subtract) exactly and atomically; refuse results below `min` or above `max`.

    `add("balance", Decimal("-25.00"), min=0)` withdraws without ever allowing an overdraft.
    """
    return FieldOp(3, path, amount, None if min is _MISSING else min, None if max is _MISSING else max,
                   min is not _MISSING, max is not _MISSING)


def append(path: str, value: Any) -> FieldOp:
    """Append `value` to the list at `path` (a missing field becomes a one-element list)."""
    return FieldOp(4, path, value)


def _encode_op(op: FieldOp, out: bytearray) -> None:
    out.append(op.kind)
    put_str(out, op.path)
    if op.kind in (1, 4):
        encode(op.value, out)
    elif op.kind == 3:
        encode(op.value, out)
        out.append((1 if op.has_min else 0) | (2 if op.has_max else 0))
        if op.has_min:
            encode(op.min, out)
        if op.has_max:
            encode(op.max, out)


# ── Filters ──────────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Filter:
    """A query condition (build with eq, gt, between, and_, ...; combine with & | ~)."""

    kind: int
    path: str = ""
    values: Tuple[Any, ...] = ()
    children: Tuple["Filter", ...] = ()

    def __and__(self, other: "Filter") -> "Filter":
        return and_(self, other)

    def __or__(self, other: "Filter") -> "Filter":
        return or_(self, other)

    def __invert__(self) -> "Filter":
        return not_(self)


def eq(path: str, value: Any) -> Filter:
    """Field equals value (numbers compare numerically; `None` matches missing fields)."""
    return Filter(1, path, (value,))


def ne(path: str, value: Any) -> Filter:
    return Filter(2, path, (value,))


def lt(path: str, value: Any) -> Filter:
    return Filter(3, path, (value,))


def le(path: str, value: Any) -> Filter:
    return Filter(4, path, (value,))


def gt(path: str, value: Any) -> Filter:
    return Filter(5, path, (value,))


def ge(path: str, value: Any) -> Filter:
    return Filter(6, path, (value,))


def between(path: str, low: Any, high: Any) -> Filter:
    """`low <= field <= high`."""
    return Filter(7, path, (low, high))


def in_(path: str, values: Sequence[Any]) -> Filter:
    """Field equals any of `values`."""
    return Filter(8, path, tuple(values))


def prefix(path: str, text: str) -> Filter:
    """Text field starts with `text`."""
    return Filter(9, path, (text,))


def contains(path: str, value: Any) -> Filter:
    """Text field contains `value`, or list field has an element equal to `value`."""
    return Filter(10, path, (value,))


def exists(path: str) -> Filter:
    """Field is present and not null."""
    return Filter(11, path)


def missing(path: str) -> Filter:
    """Field is absent or null."""
    return Filter(12, path)


def key_prefix(text: str) -> Filter:
    """Document key starts with `text`."""
    return Filter(13, values=(text,))


def and_(*filters: Filter) -> Filter:
    return Filter(14, children=tuple(filters))


def or_(*filters: Filter) -> Filter:
    return Filter(15, children=tuple(filters))


def not_(f: Filter) -> Filter:
    return Filter(16, children=(f,))


def where_to_filter(where: Any) -> Optional[Filter]:
    """Accepts a Filter, a {field: value} dict (all equal), or None."""
    if where is None:
        return None
    if isinstance(where, Filter):
        return where
    if isinstance(where, dict):
        parts = [v if isinstance(v, Filter) else eq(k, v) for k, v in where.items()]
        if not parts:
            return None
        return parts[0] if len(parts) == 1 else and_(*parts)
    raise InvalidError("where= must be a filter (eq, gt, ...) or a {field: value} dict")


def _encode_filter(f: Filter, out: bytearray, depth: int = 0) -> None:
    if depth > 32:
        raise InvalidError("filter nesting is too deep (max 32)")
    out.append(f.kind)
    if f.kind in (1, 2, 3, 4, 5, 6, 10):
        put_str(out, f.path)
        encode(f.values[0], out)
    elif f.kind == 7:
        put_str(out, f.path)
        encode(f.values[0], out)
        encode(f.values[1], out)
    elif f.kind == 8:
        put_str(out, f.path)
        out += struct.pack("<I", len(f.values))
        for v in f.values:
            encode(v, out)
    elif f.kind == 9:
        put_str(out, f.path)
        put_str(out, f.values[0])
    elif f.kind in (11, 12):
        put_str(out, f.path)
    elif f.kind == 13:
        put_str(out, f.values[0])
    elif f.kind in (14, 15):
        out += struct.pack("<I", len(f.children))
        for c in f.children:
            _encode_filter(c, out, depth + 1)
    elif f.kind == 16:
        _encode_filter(f.children[0], out, depth + 1)


# ── Requests ─────────────────────────────────────────────────────────────────────────────────

@dataclass
class Write:
    """One write of an atomic request (built by Collection / Transaction methods)."""

    collection: str
    key: str
    kind: int  # 1 put, 2 put+ttl, 3 delete, 4 patch, 5 check
    precondition: int = 0  # 0 none, 1 absent, 2 exists, 3 version
    version: int = 0
    fields: Optional[Dict[str, Any]] = None
    ttl_ms: int = 0
    ops: Sequence[FieldOp] = ()


def encode_get(collection: str, key: str) -> bytes:
    out = bytearray()
    put_str(out, collection)
    put_str(out, key)
    return bytes(out)


MAX_BYTE_RANGE = 16 * 1024 * 1024
"""Most bytes one ranged read returns."""


def encode_get_range(collection: str, key: str, field: str, offset: int, length: int) -> bytes:
    if not 0 <= offset < 1 << 64:
        raise InvalidError("offset must be between 0 and 2**64 - 1")
    if not 0 <= length <= MAX_BYTE_RANGE:
        raise InvalidError(f"length must be between 0 and {MAX_BYTE_RANGE}")
    out = bytearray(encode_get(collection, key))
    out.append(1)
    put_str(out, field)
    out += struct.pack("<QI", offset, length)
    return bytes(out)


def encode_get_fields(collection: str, key: str, paths: Sequence[str]) -> bytes:
    if isinstance(paths, str):
        raise InvalidError("fields is a list of field paths")
    if len(paths) > 256:
        raise InvalidError("at most 256 fields can be projected")
    out = bytearray(encode_get(collection, key))
    out.append(2)
    out += struct.pack("<I", len(paths))
    for p in paths:
        put_str(out, p)
    return bytes(out)


def decode_byte_range(body: bytes) -> Tuple[int, bytes]:
    """`u64 total | u32 n | n bytes` -> (total, data)."""
    r = Reader(body)
    total = r.u64()
    data = r.take(r.u32())
    if r.remaining():
        raise InvalidError("trailing bytes after byte range")
    return total, data


def encode_writes(writes: Sequence[Write], request_id: Optional[bytes]) -> bytes:
    out = bytearray()
    if request_id is None:
        out.append(0)
    else:
        out.append(1)
        out += request_id
    out += struct.pack("<I", len(writes))
    for w in writes:
        put_str(out, w.collection)
        put_str(out, w.key)
        out.append(w.precondition)
        if w.precondition == 3:
            out += struct.pack("<Q", w.version)
        out.append(w.kind)
        if w.kind == 1:
            encode_map(w.fields or {}, out)
        elif w.kind == 2:
            out += struct.pack("<Q", w.ttl_ms)
            encode_map(w.fields or {}, out)
        elif w.kind == 4:
            out += struct.pack("<I", len(w.ops))
            for op in w.ops:
                _encode_op(op, out)
    return bytes(out)


def encode_query(collection: str, flt: Optional[Filter], order_by: Sequence[Tuple[str, bool]],
                 limit: int, offset: int, after_key: Optional[str],
                 nearest: Optional[Tuple[str, Sequence[float], int, bool]] = None) -> bytes:
    out = bytearray()
    put_str(out, collection)
    if flt is None:
        out.append(0)
    else:
        out.append(1)
        _encode_filter(flt, out)
    out += struct.pack("<I", len(order_by))
    for path, desc in order_by:
        put_str(out, path)
        out.append(1 if desc else 0)
    out += struct.pack("<II", limit, offset)
    if after_key is None:
        out.append(0)
    else:
        out.append(1)
        put_str(out, after_key)
    if nearest is not None:
        # Optional trailing block: u8 1 | str field | u32 k | u8 exact | u32 dims | dims x f32.
        field, vector, k, exact = nearest
        out.append(1)
        put_str(out, field)
        out += struct.pack("<IBI", k, 1 if exact else 0, len(vector))
        out += struct.pack(f"<{len(vector)}f", *vector)
    return bytes(out)


def parse_order_by(order_by: Any) -> List[Tuple[str, bool]]:
    """"price" / "-price" (descending) / ["a", "-b"] / [("a", "desc")]."""
    if order_by is None:
        return []
    items = [order_by] if isinstance(order_by, (str, tuple)) else list(order_by)
    out = []
    for it in items:
        if isinstance(it, tuple):
            path, direction = it
            out.append((path, str(direction).lower() in ("desc", "descending", "-1")))
        elif isinstance(it, str):
            out.append((it[1:], True) if it.startswith("-") else (it, False))
        else:
            raise InvalidError("order_by entries are 'field', '-field' or ('field', 'desc')")
    return out


# ── Responses ────────────────────────────────────────────────────────────────────────────────

def decode_write_response(body: bytes) -> Tuple[List[Optional[int]], bool]:
    r = Reader(body)
    duplicate = r.u8() != 0
    n = r.count(9)
    versions = []
    for _ in range(n):
        exists = r.u8() != 0
        v = r.u64()
        versions.append(v if exists else None)
    return versions, duplicate


def decode_page(body: bytes) -> Tuple[int, Optional[str], List[Tuple[str, int, Dict[str, Any]]]]:
    r = Reader(body)
    total = r.u32()
    next_key = r.str() if r.u8() else None
    n = r.count(16)
    docs = []
    for _ in range(n):
        key = r.str()
        version = r.u64()
        docs.append((key, version, r.map()))
    return total, next_key, docs


def decode_conflict(body: bytes) -> Tuple[Optional[int], Optional[int], str]:
    if len(body) < 13:
        return None, None, body.decode("utf-8", "replace")
    index, exists, version = struct.unpack("<IBQ", body[:13])
    return (None if index == 0xFFFFFFFF else index), (version if exists else None), body[13:].decode("utf-8", "replace")


def decode_info(r: Reader) -> Dict[str, Any]:
    name = r.str()
    indexes = [r.str() for _ in range(r.count(4))]
    building = [r.str() for _ in range(r.count(4))]
    meta = r.map()
    return {"name": name, "indexes": indexes, "building": building, "metadata": meta}


def decode_cluster_info(body: bytes) -> Dict[str, Any]:
    r = Reader(body)
    node_id = r.u64()
    is_leader = r.u8() != 0
    leader_id = r.u64()
    leader_addr = r.str()
    term, commit, applied = r.u64(), r.u64(), r.u64()
    members = {}
    for _ in range(r.count(12)):
        nid = r.u64()
        members[nid] = r.str()
    return {"node_id": node_id, "is_leader": is_leader, "leader_id": leader_id or None,
            "leader_addr": leader_addr or None, "term": term, "commit_index": commit,
            "last_applied": applied, "members": members}
