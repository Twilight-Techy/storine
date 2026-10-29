"""Analytics: aggregate expressions and the OP_DOC_AGGREGATE / OP_DOC_SCAN_COLUMNS encodings
(protocol/PROTOCOL.md §5.5, §5.6).

Note: this module defines `sum`, `min` and `max` (aggregate constructors), which shadow the
builtins inside it; it never uses the builtins.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Iterator, List, NamedTuple, Optional, Sequence, Tuple

from ._sve import Reader, encode, put_str
from ._wire import Filter, _encode_filter, parse_order_by
from .errors import InvalidError

OP_DOC_AGGREGATE = 0x29
OP_DOC_SCAN_COLUMNS = 0x2A

MAX_ITEMS = 64
MAX_SCAN_PAGE = 100_000

_EXPR_FIELD, _EXPR_CONST, _EXPR_ADD, _EXPR_SUB, _EXPR_MUL, _EXPR_NEG = 1, 2, 3, 4, 5, 6
_I64 = (-(1 << 63), (1 << 63) - 1)


# ── Expressions ──────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Expr:
    """An aggregate argument: `field(path)`, `lit(value)`, combined with `+ - *` and unary `-`.

    Plain numbers (int, float, Decimal) become `lit`; plain strings are field paths:
    `field("price") * "qty"`, `2 * field("qty") - 1`.
    """

    kind: int
    path: str = ""
    value: Any = None
    children: Tuple["Expr", ...] = ()

    def __add__(self, other: Any) -> "Expr":
        return Expr(_EXPR_ADD, children=(self, as_expr(other)))

    def __radd__(self, other: Any) -> "Expr":
        return Expr(_EXPR_ADD, children=(as_expr(other), self))

    def __sub__(self, other: Any) -> "Expr":
        return Expr(_EXPR_SUB, children=(self, as_expr(other)))

    def __rsub__(self, other: Any) -> "Expr":
        return Expr(_EXPR_SUB, children=(as_expr(other), self))

    def __mul__(self, other: Any) -> "Expr":
        return Expr(_EXPR_MUL, children=(self, as_expr(other)))

    def __rmul__(self, other: Any) -> "Expr":
        return Expr(_EXPR_MUL, children=(as_expr(other), self))

    def __neg__(self) -> "Expr":
        return Expr(_EXPR_NEG, children=(self,))

    def __pos__(self) -> "Expr":
        return self


def field(path: str) -> Expr:
    """The value of a field (dotted path for nested maps)."""
    if not isinstance(path, str):
        raise InvalidError("a field path is a string")
    return Expr(_EXPR_FIELD, path=path)


def lit(value: Any) -> Expr:
    """A constant (any storable value; numbers for arithmetic)."""
    return Expr(_EXPR_CONST, value=value)


def as_expr(x: Any) -> Expr:
    """Expr as is; a str is a field path; a number (int, float, Decimal) is a constant."""
    if isinstance(x, Expr):
        return x
    if isinstance(x, str):
        return field(x)
    if isinstance(x, (int, float, Decimal)):
        return lit(x)
    raise InvalidError(f"not an expression: {x!r} (use field(path), lit(value) or a number)")


def _encode_expr(e: Expr, out: bytearray, depth: int = 0) -> None:
    if depth > 32:
        raise InvalidError("expression nesting is too deep (max 32)")
    out.append(e.kind)
    if e.kind == _EXPR_FIELD:
        put_str(out, e.path)
    elif e.kind == _EXPR_CONST:
        encode(e.value, out)
    else:
        for c in e.children:
            _encode_expr(c, out, depth + 1)


# ── Aggregates and group keys ────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Aggregate:
    """One aggregate of `Collection.aggregate` (build with count, sum, avg, min, max, ...)."""

    func: int
    arg: Optional[Expr] = None


def count(arg: Any = None) -> Aggregate:
    """Number of rows; with `arg`, number of rows where it is not null."""
    return Aggregate(1, None if arg is None else as_expr(arg))


def sum(arg: Any) -> Aggregate:  # noqa: A001
    """Exact sum (integers and decimals never round; floats are correctly rounded)."""
    return Aggregate(2, as_expr(arg))


def avg(arg: Any) -> Aggregate:
    """Mean; a decimal (6 more fractional digits, half-even) for exact inputs."""
    return Aggregate(3, as_expr(arg))


def min(arg: Any) -> Aggregate:  # noqa: A001
    """Smallest non-null value (document total order)."""
    return Aggregate(4, as_expr(arg))


def max(arg: Any) -> Aggregate:  # noqa: A001
    """Largest non-null value (document total order)."""
    return Aggregate(5, as_expr(arg))


def count_distinct(arg: Any) -> Aggregate:
    """Exact number of distinct non-null values."""
    return Aggregate(6, as_expr(arg))


def approx_count_distinct(arg: Any) -> Aggregate:
    """HyperLogLog estimate of the number of distinct values (about 0.8 % standard error)."""
    return Aggregate(7, as_expr(arg))


@dataclass(frozen=True)
class GroupKey:
    """A group key: a field's value (a plain path string) or `bucket(field, width, offset)`."""

    kind: int
    path: str
    width: int = 0
    offset: int = 0


def bucket(field_path: str, width: int, offset: int = 0) -> GroupKey:
    """Groups numbers by `offset + floor((v - offset) / width) * width` (e.g. weeks of days)."""
    if not isinstance(field_path, str):
        raise InvalidError("bucket() takes a field path string")
    for name, v in (("width", width), ("offset", offset)):
        if not isinstance(v, int) or isinstance(v, bool) or not _I64[0] <= v <= _I64[1]:
            raise InvalidError(f"bucket {name} must be a 64-bit integer")
    return GroupKey(2, field_path, width, offset)


def _group_key(k: Any) -> GroupKey:
    if isinstance(k, GroupKey):
        return k
    if isinstance(k, str):
        return GroupKey(1, k)
    raise InvalidError("group_by entries are field paths or bucket(field, width, offset)")


# ── Requests ─────────────────────────────────────────────────────────────────────────────────

def _put_filter(out: bytearray, flt: Optional[Filter]) -> None:
    if flt is None:
        out.append(0)
    else:
        out.append(1)
        _encode_filter(flt, out)


def _u(value: Any, bits: int, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value < 1 << bits:
        raise InvalidError(f"{name} must be an integer between 0 and 2**{bits} - 1")
    return value


def encode_aggregate(collection: str, flt: Optional[Filter], group_by: Sequence[GroupKey],
                     aggregates: Sequence[Aggregate], order: Sequence[Tuple[int, bool]], limit: int,
                     memory_budget: int = 0, spill_budget: int = 0) -> bytes:
    for n, what in ((len(group_by), "group keys"), (len(aggregates), "aggregates"), (len(order), "order columns")):
        if n > MAX_ITEMS:
            raise InvalidError(f"at most {MAX_ITEMS} {what}")
    out = bytearray()
    put_str(out, collection)
    _put_filter(out, flt)
    out += struct.pack("<I", len(group_by))
    for k in group_by:
        out.append(k.kind)
        put_str(out, k.path)
        if k.kind == 2:
            out += struct.pack("<qq", k.width, k.offset)
    out += struct.pack("<I", len(aggregates))
    for a in aggregates:
        out.append(a.func)
        if a.arg is None:
            out.append(0)
        else:
            out.append(1)
            _encode_expr(a.arg, out)
    out += struct.pack("<I", len(order))
    for column, desc in order:
        out += struct.pack("<IB", column, 1 if desc else 0)
    out += struct.pack("<IQQ", _u(limit, 32, "limit"), _u(memory_budget, 64, "memory_budget"),
                       _u(spill_budget, 64, "spill_budget"))
    return bytes(out)


def encode_scan(collection: str, flt: Optional[Filter], fields: Sequence[str], after_key: Optional[str],
                limit: int) -> bytes:
    out = bytearray()
    put_str(out, collection)
    _put_filter(out, flt)
    out += struct.pack("<I", len(fields))
    for f in fields:
        put_str(out, f)
    if after_key is None:
        out.append(0)
    else:
        out.append(1)
        put_str(out, after_key)
    out += struct.pack("<I", _u(limit, 32, "limit"))
    return bytes(out)


def encode_columnar(collection: str, enable: bool) -> bytes:
    """OP_DOC_ADMIN command 10 (enable columnar) / 11 (disable columnar)."""
    out = bytearray([10 if enable else 11])
    put_str(out, collection)
    return bytes(out)


class AggregateQuery(NamedTuple):
    group_by: List[GroupKey]
    aggregates: List[Aggregate]
    columns: List[str]
    order: List[Tuple[int, bool]]


def build_aggregate(aggregates: Any, group_by: Any, order_by: Any) -> AggregateQuery:
    """Validates the user arguments of `Collection.aggregate` and names the output columns."""
    if not isinstance(aggregates, dict):
        raise InvalidError("aggregates is a dict of output name -> aggregate (count(), sum('x'), ...)")
    if group_by is None:
        keys: List[GroupKey] = []
    elif isinstance(group_by, (str, GroupKey)):
        keys = [_group_key(group_by)]
    else:
        keys = [_group_key(k) for k in group_by]
    aggs: List[Aggregate] = []
    for name, a in aggregates.items():
        if not isinstance(name, str):
            raise InvalidError("aggregate names are strings")
        if not isinstance(a, Aggregate):
            raise InvalidError(f"aggregate {name!r} must be built with count(), sum(), avg(), min(), max(), ...")
        aggs.append(a)
    columns = [k.path for k in keys] + list(aggregates)
    if len(set(columns)) != len(columns):
        raise InvalidError("output column names (group keys and aggregate names) must be distinct")
    index = {c: i for i, c in enumerate(columns)}
    order = []
    for name, desc in parse_order_by(order_by):
        if name not in index:
            raise InvalidError(f"order_by {name!r} is not an output column (one of {columns})")
        order.append((index[name], desc))
    return AggregateQuery(keys, aggs, columns, order)


# ── Responses ────────────────────────────────────────────────────────────────────────────────

def decode_aggregate_result(body: bytes) -> Tuple[int, int, int, int, List[List[Any]]]:
    """-> (matched, pruned_row_groups, spilled_bytes, columns, rows as lists)."""
    r = Reader(body)
    matched, pruned, spilled = r.u64(), r.u64(), r.u64()
    cols = r.u32()
    n = r.count(cols)
    rows = [[r.value() for _ in range(cols)] for _ in range(n)]
    if r.remaining():
        raise InvalidError("trailing bytes after aggregate result")
    return matched, pruned, spilled, cols, rows


def decode_scan_page(body: bytes) -> Tuple[int, Optional[str], int, List[Tuple[str, List[Any]]]]:
    """-> (total, next_after_key, fields, [(key, values)])."""
    r = Reader(body)
    total = r.u64()
    nxt = r.str() if r.u8() else None
    nf = r.u32()
    n = r.count(4 + nf)
    rows = []
    for _ in range(n):
        key = r.str()
        rows.append((key, [r.value() for _ in range(nf)]))
    if r.remaining():
        raise InvalidError("trailing bytes after scan page")
    return total, nxt, nf, rows


class AggregateResult:
    """Result of `Collection.aggregate`: `rows` (dicts of column name -> value, group keys first,
    then aggregates in the order given), `matched` (rows that passed the filter),
    `pruned_row_groups` and `spilled_bytes`. Iterating yields the rows."""

    def __init__(self, columns: List[str], rows: List[Dict[str, Any]], matched: int, pruned_row_groups: int,
                 spilled_bytes: int) -> None:
        self.columns = columns
        self.rows = rows
        self.matched = matched
        self.pruned_row_groups = pruned_row_groups
        self.spilled_bytes = spilled_bytes

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        return iter(self.rows)

    def __len__(self) -> int:
        return len(self.rows)

    def __getitem__(self, i: int) -> Dict[str, Any]:
        return self.rows[i]

    def __repr__(self) -> str:
        return (f"AggregateResult(rows={self.rows!r}, matched={self.matched}, "
                f"pruned_row_groups={self.pruned_row_groups}, spilled_bytes={self.spilled_bytes})")


def aggregate_result(columns: List[str], body: bytes) -> AggregateResult:
    matched, pruned, spilled, cols, rows = decode_aggregate_result(body)
    if rows and cols != len(columns):
        raise InvalidError(f"the server returned {cols} columns, expected {len(columns)}")
    return AggregateResult(columns, [dict(zip(columns, row)) for row in rows], matched, pruned, spilled)


class ColumnRow(NamedTuple):
    """One row of `Collection.scan_columns`: the document key and {field path: value}
    (a missing field reads as None). Unpacks as `key, values`."""

    key: str
    values: Dict[str, Any]
