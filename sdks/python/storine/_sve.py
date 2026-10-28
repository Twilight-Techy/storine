"""Storine Value Encoding (SVE): native Python values <-> wire bytes (protocol/PROTOCOL.md §4)."""

from __future__ import annotations

import datetime as _dt
import struct
import uuid as _uuid
from decimal import Decimal
from typing import Any, Dict, List, Tuple

from .errors import InvalidError

NULL, BOOL, INT64, UINT64, FLOAT64, INT128, TIMESTAMP, UUID, ID, TEXT, BYTES, DECIMAL, LIST, MAP = range(14)
MAX_DEPTH = 32
MAX_SCALE = 38
_I64_MIN, _I64_MAX = -(1 << 63), (1 << 63) - 1
_I128_MIN, _I128_MAX = -(1 << 127), (1 << 127) - 1
_EPOCH = _dt.datetime(1970, 1, 1, tzinfo=_dt.timezone.utc)


class UInt64(int):
    """An integer stored as an unsigned 64-bit value (plain ints are signed)."""


class Id(bytes):
    """An engine id value (24 raw bytes)."""

    def __new__(cls, raw: bytes) -> "Id":
        if len(raw) != 24:
            raise InvalidError("an Id is exactly 24 bytes")
        return super().__new__(cls, raw)


class Timestamp:
    """A timestamp with nanosecond precision (returned when a datetime would lose precision)."""

    __slots__ = ("ns",)

    def __init__(self, ns: int) -> None:
        self.ns = int(ns)

    @property
    def datetime(self) -> _dt.datetime:
        """The timestamp as an aware UTC datetime (truncated to microseconds)."""
        return _EPOCH + _dt.timedelta(microseconds=self.ns // 1000)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Timestamp) and other.ns == self.ns

    def __hash__(self) -> int:
        return hash(self.ns)

    def __repr__(self) -> str:
        return f"Timestamp(ns={self.ns})"


def _datetime_to_ns(value: _dt.datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=_dt.timezone.utc)  # naive datetimes are taken as UTC
    delta = value - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1_000_000_000 + delta.microseconds * 1000


def _decimal_parts(value: Decimal) -> Tuple[int, int]:
    if not value.is_finite():
        raise InvalidError("NaN and infinity are not decimals")
    sign, digits, exponent = value.as_tuple()
    mantissa = int("".join(map(str, digits)) or "0")
    if sign:
        mantissa = -mantissa
    scale = -exponent
    if scale < 0:  # e.g. Decimal("1E+3"): fold the exponent into the mantissa
        mantissa *= 10 ** (-scale)
        scale = 0
    while scale > MAX_SCALE and mantissa % 10 == 0:
        mantissa //= 10
        scale -= 1
    if scale > MAX_SCALE:
        raise InvalidError("decimals have at most 38 fractional digits")
    if not _I128_MIN <= mantissa <= _I128_MAX:
        raise InvalidError("decimal does not fit in 38 digits")
    return mantissa, scale


def _put_str(out: bytearray, s: str) -> None:
    b = s.encode("utf-8")
    out += struct.pack("<I", len(b))
    out += b


def encode(value: Any, out: bytearray, depth: int = 0) -> None:
    """Appends the SVE encoding of `value` to `out`."""
    if depth > MAX_DEPTH:
        raise InvalidError("value nesting is too deep (max 32)")
    if value is None:
        out.append(NULL)
    elif value is True or value is False:
        out += bytes((BOOL, 1 if value else 0))
    elif isinstance(value, UInt64):
        if not 0 <= value <= (1 << 64) - 1:
            raise InvalidError("UInt64 out of range")
        out.append(UINT64)
        out += struct.pack("<Q", int(value))
    elif isinstance(value, int):
        if _I64_MIN <= value <= _I64_MAX:
            out.append(INT64)
            out += struct.pack("<q", value)
        elif _I128_MIN <= value <= _I128_MAX:
            out.append(INT128)
            out += value.to_bytes(16, "little", signed=True)
        else:
            raise InvalidError("integer does not fit in 128 bits")
    elif isinstance(value, float):
        out.append(FLOAT64)
        out += struct.pack("<d", value)
    elif isinstance(value, Decimal):
        mantissa, scale = _decimal_parts(value)
        out.append(DECIMAL)
        out += mantissa.to_bytes(16, "little", signed=True)
        out.append(scale)
    elif isinstance(value, str):
        out.append(TEXT)
        _put_str(out, value)
    elif isinstance(value, Id):
        out.append(ID)
        out += bytes(value)
    elif isinstance(value, (bytes, bytearray, memoryview)):
        b = bytes(value)
        out.append(BYTES)
        out += struct.pack("<I", len(b))
        out += b
    elif isinstance(value, Timestamp):
        out.append(TIMESTAMP)
        out += struct.pack("<q", value.ns)
    elif isinstance(value, _dt.datetime):
        out.append(TIMESTAMP)
        out += struct.pack("<q", _datetime_to_ns(value))
    elif isinstance(value, _uuid.UUID):
        out.append(UUID)
        out += value.bytes
    elif isinstance(value, (list, tuple)):
        out.append(LIST)
        out += struct.pack("<I", len(value))
        for item in value:
            encode(item, out, depth + 1)
    elif isinstance(value, dict):
        out.append(MAP)
        encode_map(value, out, depth + 1)
    elif isinstance(value, _dt.date):
        raise InvalidError("dates are not supported: use a datetime (stored as a UTC timestamp)")
    else:
        raise InvalidError(f"cannot store a value of type {type(value).__name__}")


def encode_map(fields: Dict[str, Any], out: bytearray, depth: int = 0) -> None:
    """Appends `u32 count | (str key, value)*` with keys in ascending byte order."""
    items = []
    for k, v in fields.items():
        if not isinstance(k, str):
            raise InvalidError("document field names must be strings")
        items.append((k.encode("utf-8"), k, v))
    items.sort(key=lambda t: t[0])
    out += struct.pack("<I", len(items))
    for raw, _k, v in items:
        out += struct.pack("<I", len(raw))
        out += raw
        encode(v, out, depth)


class Reader:
    """Bounds-checked cursor over untrusted bytes."""

    __slots__ = ("buf", "pos")

    def __init__(self, buf: bytes, pos: int = 0) -> None:
        self.buf = buf
        self.pos = pos

    def remaining(self) -> int:
        return len(self.buf) - self.pos

    def take(self, n: int) -> bytes:
        if n < 0 or self.pos + n > len(self.buf):
            raise InvalidError("truncated response")
        b = self.buf[self.pos:self.pos + n]
        self.pos += n
        return b

    def u8(self) -> int:
        return self.take(1)[0]

    def u32(self) -> int:
        return struct.unpack("<I", self.take(4))[0]

    def u64(self) -> int:
        return struct.unpack("<Q", self.take(8))[0]

    def i64(self) -> int:
        return struct.unpack("<q", self.take(8))[0]

    def str(self) -> str:
        n = self.u32()
        try:
            return self.take(n).decode("utf-8")
        except UnicodeDecodeError as e:
            raise InvalidError("invalid UTF-8 in response") from e

    def count(self, min_size: int) -> int:
        n = self.u32()
        if n * max(min_size, 1) > self.remaining():
            raise InvalidError("truncated response")
        return n

    def value(self, depth: int = 0) -> Any:
        tag = self.u8()
        if tag == NULL:
            return None
        if tag == BOOL:
            b = self.u8()
            if b > 1:
                raise InvalidError("bool byte must be 0 or 1")
            return b == 1
        if tag == INT64:
            return self.i64()
        if tag == UINT64:
            return self.u64()
        if tag == FLOAT64:
            return struct.unpack("<d", self.take(8))[0]
        if tag == INT128:
            return int.from_bytes(self.take(16), "little", signed=True)
        if tag == TIMESTAMP:
            ns = self.i64()
            if ns % 1000 == 0:
                return _EPOCH + _dt.timedelta(microseconds=ns // 1000)
            return Timestamp(ns)
        if tag == UUID:
            return _uuid.UUID(bytes=self.take(16))
        if tag == ID:
            return Id(self.take(24))
        if tag == TEXT:
            return self.str()
        if tag == BYTES:
            return self.take(self.u32())
        if tag == DECIMAL:
            mantissa = int.from_bytes(self.take(16), "little", signed=True)
            scale = self.u8()
            if scale > MAX_SCALE:
                raise InvalidError("decimal scale above 38")
            # Built from text: exact (Decimal arithmetic would round to the 28-digit context).
            return Decimal(f"{mantissa}E-{scale}")
        if tag == LIST:
            if depth >= MAX_DEPTH:
                raise InvalidError("value nesting is too deep")
            n = self.count(1)
            return [self.value(depth + 1) for _ in range(n)]
        if tag == MAP:
            if depth >= MAX_DEPTH:
                raise InvalidError("value nesting is too deep")
            return self.map(depth + 1)
        raise InvalidError(f"unknown value tag {tag}")

    def map(self, depth: int = 0) -> Dict[str, Any]:
        n = self.count(5)
        out: Dict[str, Any] = {}
        for _ in range(n):
            k = self.str()
            if k in out:
                raise InvalidError("duplicate map key")
            out[k] = self.value(depth)
        return out


def decode(buf: bytes) -> Any:
    """Decodes exactly one value."""
    r = Reader(buf)
    v = r.value()
    if r.remaining():
        raise InvalidError("trailing bytes after value")
    return v


def encode_value(value: Any) -> bytes:
    out = bytearray()
    encode(value, out)
    return bytes(out)


def put_str(out: bytearray, s: str) -> None:
    _put_str(out, s)


__all__: List[str] = ["UInt64", "Id", "Timestamp", "encode", "encode_map", "decode", "encode_value", "Reader", "put_str"]
