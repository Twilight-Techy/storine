"""Storine client: connection handling, retries and the collection API."""

from __future__ import annotations

import os
import random
import socket
import struct
import threading
import time
from typing import Any, Dict, Iterator, List, NamedTuple, Optional, Sequence, Tuple
from urllib.parse import parse_qs, unquote, urlparse

from . import _analytics as an
from . import _wire as w
from ._sve import Reader, put_str
from .errors import (ConflictError, InvalidError, NotFoundError, OverloadedError, RateLimitedError,
                     ServerError, StorineError, UnauthorizedError, UnavailableError)

_ZERO_KEY = bytes(24)
_MAX_BODY = 256 * 1024 * 1024


class Document(dict):
    """A document: a plain dict of its fields, plus `.key` and `.version`."""

    def __init__(self, key: str, version: int, fields: Dict[str, Any]) -> None:
        super().__init__(fields)
        self.key = key
        self.version = version

    def __repr__(self) -> str:
        return f"Document(key={self.key!r}, version={self.version}, fields={dict.__repr__(self)})"

    @property
    def score(self) -> Optional[float]:
        """Similarity of a `nearest` result (None for other queries)."""
        return self.get("$score")


class ByteRange(NamedTuple):
    """A slice of a bytes / text field: `data` starts at the requested offset; `total` is the
    whole value's length and `version` the document version it was read from."""

    version: int
    total: int
    data: bytes


class _Connection:
    def __init__(self, address: Tuple[str, int], timeout: float, token: Optional[str], follower_reads: bool) -> None:
        self.address = address
        self.sock = socket.create_connection(address, timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.settimeout(timeout)
        if token:
            status, _v, body = self.call(w.OP_AUTH, token.encode("utf-8"))
            if status != w.ST_OK:
                self.close()
                raise UnauthorizedError("authentication failed: " + body.decode("utf-8", "replace"), status)
        if follower_reads:
            self.call(w.OP_SET_OPTIONS, b"\x01")

    def call(self, op: int, body: bytes) -> Tuple[int, int, bytes]:
        self.sock.sendall(struct.pack("<B24sI", op, _ZERO_KEY, len(body)) + body)
        hdr = self._read(13)
        status, version, n = struct.unpack("<BQI", hdr)
        if n > _MAX_BODY:
            raise ServerError("response too large")
        return status, version, self._read(n)

    def _read(self, n: int) -> bytes:
        chunks = []
        while n:
            c = self.sock.recv(min(n, 1 << 20))
            if not c:
                raise ConnectionError("server closed the connection")
            chunks.append(c)
            n -= len(c)
        return b"".join(chunks)

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


def _parse_address(text: str) -> Tuple[str, int]:
    host, _, port = text.strip().rpartition(":")
    if not host:
        host, port = port, "7878"
    return host.strip("[]"), int(port)


class Client:
    """A connection pool to a Storine server or cluster (thread-safe).

    Use `connect()` to create one. Writes are retried automatically with the same request id
    (applied at most once) across timeouts, reconnects and leader changes.
    """

    def __init__(self, seeds: Sequence[Tuple[str, int]], token: Optional[str] = None, timeout: float = 5.0,
                 follower_reads: bool = False, retries: int = 8) -> None:
        if not seeds:
            raise InvalidError("no server address given")
        self._seeds = list(seeds)
        self._token = token
        self._timeout = timeout
        self._follower_reads = follower_reads
        self._retries = retries
        self._target = self._seeds[0]
        self._pool: List[_Connection] = []
        self._lock = threading.Lock()

    # ── plumbing ──

    def _acquire(self) -> _Connection:
        with self._lock:
            target = self._target
            while self._pool:
                c = self._pool.pop()
                if c.address == target:
                    return c
                c.close()
        return _Connection(target, self._timeout, self._token, self._follower_reads)

    def _release(self, c: _Connection) -> None:
        with self._lock:
            if c.address == self._target and len(self._pool) < 16:
                self._pool.append(c)
                return
        c.close()

    def _redirect(self, address: Optional[str], avoid: Optional[set] = None) -> None:
        with self._lock:
            if address:
                self._target = _parse_address(address)
                return
            avoid = avoid or set()
            others = [s for s in self._seeds if s != self._target and s not in avoid]
            if not others:
                others = [s for s in self._seeds if s != self._target] or self._seeds
            self._target = random.choice(others)

    def _request(self, op: int, body: bytes, idempotent: bool) -> Tuple[int, int, bytes]:
        """Sends a request, following leader redirects and retrying transient failures.

        `idempotent` requests (all reads, and writes that carry a request id) are resent after a
        connection failure or an unknown commit outcome. Retries continue for up to three times the
        timeout (enough to ride out a leader election).
        """
        deadline = time.monotonic() + max(3 * self._timeout, 1.0)
        delay = 0.02
        last: Optional[BaseException] = None
        failed: set = set()
        attempts = 0
        while attempts < self._retries or time.monotonic() < deadline:
            attempts += 1
            if attempts > 1000:
                break
            try:
                c = self._acquire()
            except OSError as e:
                last = e
                failed.add(self._target)
                self._redirect(None, failed)
                time.sleep(delay)
                delay = min(delay * 2, 0.5)
                continue
            try:
                status, version, rbody = c.call(op, body)
            except (OSError, ConnectionError) as e:
                c.close()
                last = e
                if not idempotent:
                    raise UnavailableError(f"connection lost during a request: {e}") from e
                failed.add(c.address)
                self._redirect(None, failed)
                time.sleep(delay)
                delay = min(delay * 2, 0.5)
                continue
            if status == w.ST_NOT_LEADER:
                c.close()
                hint = rbody.decode("utf-8", "replace") or None
                if hint and _parse_address(hint) in failed:
                    hint = None  # stale hint: the old leader is down
                self._redirect(hint, failed)
                last = UnavailableError("no cluster leader is available yet")
                if not hint:
                    time.sleep(delay)
                    delay = min(delay * 2, 0.5)
                continue
            if status == w.ST_COMMIT_UNKNOWN and idempotent:
                c.close()
                last = UnavailableError("write outcome unknown and retries exhausted")
                time.sleep(delay)
                delay = min(delay * 2, 0.5)
                continue
            if status == w.ST_OVERLOADED and idempotent:
                c.close()
                last = OverloadedError(rbody.decode("utf-8", "replace") or "server overloaded", status)
                time.sleep(delay)
                delay = min(delay * 2, 0.5)
                continue
            if status in (w.ST_UNAUTHORIZED, w.ST_RATE_LIMITED):
                c.close()  # the server closed the connection
            else:
                self._release(c)
            return status, version, rbody
        if isinstance(last, StorineError):
            raise last
        raise UnavailableError(f"could not reach any server: {last}")

    @staticmethod
    def _raise(status: int, body: bytes) -> None:
        text = body.decode("utf-8", "replace")
        if status == w.ST_CONFLICT:
            index, version, msg = w.decode_conflict(body)
            raise ConflictError(msg, index, version)
        if status == w.ST_NOT_FOUND:
            raise NotFoundError(text or "document not found", status)
        if status == w.ST_INVALID:
            raise InvalidError(text, status)
        if status == w.ST_UNAUTHORIZED:
            raise UnauthorizedError(text or "not authorized", status)
        if status == w.ST_RATE_LIMITED:
            raise RateLimitedError(text or "rate limited", status)
        if status == w.ST_OVERLOADED:
            raise OverloadedError(text or "server overloaded", status)
        if status == w.ST_COMMIT_UNKNOWN:
            raise UnavailableError("write outcome unknown; retry the same call", status)
        raise ServerError(text or f"server error (status {status})", status)

    # ── public API ──

    def collection(self, name: str) -> "Collection":
        """A handle on the collection `name` (created on first write)."""
        return Collection(self, name)

    def __getitem__(self, name: str) -> "Collection":
        return self.collection(name)

    def transaction(self) -> "Transaction":
        """An atomic batch of writes: `with client.transaction() as tx: ...` commits on exit."""
        return Transaction(self)

    def write(self, writes: Sequence[w.Write]) -> List[Optional[int]]:
        """Applies `writes` atomically (exactly once, even across retries); returns new versions."""
        request_id = os.urandom(16)
        body = w.encode_writes(writes, request_id)
        status, _v, rbody = self._request(w.OP_DOC_WRITE, body, idempotent=True)
        if status == w.ST_NOT_FOUND:
            # Body: u32 index of the write that patched a missing document, then the message.
            index = struct.unpack("<I", rbody[:4])[0] if len(rbody) >= 4 else None
            err = NotFoundError(rbody[4:].decode("utf-8", "replace") or "document not found", status)
            err.write_index = index  # type: ignore[attr-defined]
            raise err
        if status != w.ST_OK:
            self._raise(status, rbody)
        versions, _duplicate = w.decode_write_response(rbody)
        return versions

    def collections(self) -> List[str]:
        """Names of every declared collection."""
        status, _v, body = self._request(w.OP_DOC_INFO, b"\x03", idempotent=True)
        if status != w.ST_OK:
            self._raise(status, body)
        r = Reader(body)
        return [w.decode_info(r)["name"] for _ in range(r.count(17))]

    def cluster_info(self) -> Dict[str, Any]:
        """Node id, leader, term and commit indexes of the server currently used."""
        status, _v, body = self._request(w.OP_CLUSTER_INFO, b"", idempotent=True)
        if status != w.ST_OK:
            self._raise(status, body)
        return w.decode_cluster_info(body)

    def close(self) -> None:
        """Closes every pooled connection."""
        with self._lock:
            pool, self._pool = self._pool, []
        for c in pool:
            c.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


def connect(url: str = "storine://127.0.0.1:7878", *, token: Optional[str] = None, timeout: float = 5.0,
            follower_reads: bool = False) -> Client:
    """Connects to a Storine server or cluster.

    `url` is `storine://[token@]host:port[,host:port...]` (or just `host:port`). Options may also
    be given as query parameters: `?timeout_ms=5000&follower_reads=1`.
    """
    if "://" not in url:
        url = "storine://" + url
    u = urlparse(url)
    if u.scheme != "storine":
        raise InvalidError("connection strings start with storine://")
    netloc = u.netloc
    if "@" in netloc:
        cred, netloc = netloc.rsplit("@", 1)
        token = token or unquote(cred)
    seeds = [_parse_address(h) for h in netloc.split(",") if h]
    q = parse_qs(u.query)
    if "timeout_ms" in q:
        timeout = int(q["timeout_ms"][0]) / 1000
    if "follower_reads" in q:
        follower_reads = q["follower_reads"][0] in ("1", "true", "yes")
    return Client(seeds, token=token, timeout=timeout, follower_reads=follower_reads)


def _ops(changes: Sequence[Any]) -> List[w.FieldOp]:
    ops: List[w.FieldOp] = []
    for c in changes:
        if isinstance(c, w.FieldOp):
            ops.append(c)
        elif isinstance(c, dict):
            ops.extend(w.set_(k, v) for k, v in c.items())
        else:
            raise InvalidError("update() takes field operations (set_, add, ...) or a {field: value} dict")
    if not ops:
        raise InvalidError("update() needs at least one change")
    return ops


def _ttl_ms(ttl: Optional[float]) -> int:
    if ttl is None:
        return 0
    if ttl <= 0:
        raise InvalidError("ttl must be positive (seconds)")
    return max(1, int(ttl * 1000))


class _Writer:
    """Shared write builders of Collection and Transaction."""

    def _put_write(self, collection: str, key: str, fields: Dict[str, Any], ttl: Optional[float], pre: int, version: int = 0) -> w.Write:
        if not isinstance(fields, dict):
            raise InvalidError("a document is a dict of fields")
        ttl_ms = _ttl_ms(ttl)
        return w.Write(collection, key, 2 if ttl_ms else 1, pre, version, fields, ttl_ms)


class Collection(_Writer):
    """A named set of documents. Every method is one atomic, exactly-once operation."""

    def __init__(self, client: Client, name: str) -> None:
        self.client = client
        self.name = name

    def get(self, key: str, *, fields: Optional[Sequence[str]] = None) -> Optional[Document]:
        """The document `key`, or `None` if it does not exist.

        With `fields` (paths such as `"name"` or `"meta.size"`), only those fields are sent, so
        metadata can be read without transferring a large value stored beside it.
        """
        req = w.encode_get(self.name, key) if fields is None else w.encode_get_fields(self.name, key, list(fields))
        status, version, body = self.client._request(w.OP_DOC_GET, req, idempotent=True)
        if status == w.ST_NOT_FOUND:
            return None
        if status != w.ST_OK:
            self.client._raise(status, body)
        return Document(key, version, Reader(body).map())

    def read_bytes(self, key: str, field: str, offset: int = 0, length: int = w.MAX_BYTE_RANGE) -> Optional[ByteRange]:
        """Bytes `offset .. offset + length` of a bytes (or text) field, without reading the rest
        of the document; `None` if the document does not exist. `length` is at most 16 MiB; past
        the end of the value, `data` is empty."""
        req = w.encode_get_range(self.name, key, field, offset, length)
        status, version, body = self.client._request(w.OP_DOC_GET, req, idempotent=True)
        if status == w.ST_NOT_FOUND:
            return None
        if status != w.ST_OK:
            self.client._raise(status, body)
        total, data = w.decode_byte_range(body)
        return ByteRange(version, total, data)

    def stream_bytes(self, key: str, field: str, *, chunk_size: int = 1 << 20, offset: int = 0) -> Iterator[bytes]:
        """Yields a large bytes field chunk by chunk (each at most `chunk_size` bytes).

        Raises NotFoundError if the document does not exist and ConflictError if it changes
        while being read (the chunks would otherwise mix two versions).
        """
        if not 0 < chunk_size <= w.MAX_BYTE_RANGE:
            raise InvalidError(f"chunk_size must be between 1 and {w.MAX_BYTE_RANGE}")
        version = None
        while True:
            r = self.read_bytes(key, field, offset, chunk_size)
            if r is None:
                raise NotFoundError(f"document {key!r} does not exist")
            if version is not None and r.version != version:
                raise ConflictError("the document changed while it was being read", current_version=r.version)
            version = r.version
            if not r.data:
                return
            offset += len(r.data)
            yield r.data
            if offset >= r.total:
                return

    def put(self, key: str, fields: Dict[str, Any], *, ttl: Optional[float] = None) -> int:
        """Creates or replaces the document; returns its new version. `ttl` in seconds."""
        return self.client.write([self._put_write(self.name, key, fields, ttl, 0)])[0] or 0

    def insert(self, key: str, fields: Dict[str, Any], *, ttl: Optional[float] = None) -> int:
        """Creates the document; raises ConflictError if it already exists."""
        return self.client.write([self._put_write(self.name, key, fields, ttl, 1)])[0] or 0

    def replace(self, key: str, fields: Dict[str, Any], *, if_version: Optional[int] = None) -> int:
        """Replaces an existing document (only at `if_version` when given); ConflictError otherwise."""
        pre, ver = (3, if_version) if if_version is not None else (2, 0)
        return self.client.write([self._put_write(self.name, key, fields, None, pre, ver)])[0] or 0

    def update(self, key: str, *changes: Any, if_version: Optional[int] = None) -> int:
        """Applies field changes atomically on the server; returns the new version.

        `changes` are `set_`, `unset`, `add`, `append` operations or `{field: value}` dicts:
        `accounts.update("alice", add("balance", Decimal("-25"), min=0))`.
        Raises NotFoundError if the document does not exist.
        """
        pre, ver = (3, if_version) if if_version is not None else (0, 0)
        return self.client.write([w.Write(self.name, key, 4, pre, ver, ops=_ops(changes))])[0] or 0

    def delete(self, key: str, *, if_version: Optional[int] = None) -> bool:
        """Deletes the document; returns False if it did not exist."""
        pre, ver = (3, if_version) if if_version is not None else (2, 0)
        try:
            self.client.write([w.Write(self.name, key, 3, pre, ver)])
            return True
        except ConflictError as e:
            if if_version is None and e.current_version is None:
                return False
            raise

    def find(self, where: Any = None, *, order_by: Any = None, limit: Optional[int] = None, offset: int = 0) -> List[Document]:
        """Documents matching `where` (a filter or {field: value}); at most `limit` (default 1,000)."""
        _total, _next, docs = self._query(where, order_by, limit or 0, offset, None)
        return docs

    def first(self, where: Any = None, *, order_by: Any = None) -> Optional[Document]:
        """The first matching document, or None."""
        docs = self.find(where, order_by=order_by, limit=1)
        return docs[0] if docs else None

    def count(self, where: Any = None) -> int:
        """Number of matching documents."""
        total, _next, _docs = self._query(where, None, 1, 0, None)
        return total

    def scan(self, where: Any = None, *, batch: int = 1000) -> Iterator[Document]:
        """Iterates over every matching document in key order (any collection size)."""
        after: Optional[str] = None
        while True:
            _total, nxt, docs = self._query(where, None, batch, 0, after)
            yield from docs
            if nxt is None:
                return
            after = nxt

    def nearest(self, field: str, vector: Sequence[float], k: int = 10, *, where: Any = None,
                exact: bool = False) -> List[Document]:
        """The `k` documents whose `field` (a list of numbers) is most similar to `vector` (cosine),
        best first, among those matching `where`. Each document's similarity is in `doc.score`
        (also `doc["$score"]`). Uses the field's vector index when it has one (approximate
        candidates, exact scores); `exact=True` scores every candidate."""
        _total, _next, docs = self._query(where, None, k, 0, None,
                                          nearest=(field, [float(x) for x in vector], k, exact))
        return docs

    def create_vector_index(self, field: str, dims: int) -> None:
        """Indexes a top-level field holding `dims` numbers for `nearest` (at most 4 per collection);
        existing documents included. Writes of other values to that field are then refused."""
        out = bytearray(b"\x07")
        put_str(out, self.name)
        put_str(out, field)
        out += struct.pack("<I", dims)
        self._admin(w.OP_DOC_ADMIN, bytes(out))

    def drop_vector_index(self, field: str) -> None:
        out = bytearray(b"\x08")
        put_str(out, self.name)
        put_str(out, field)
        self._admin(w.OP_DOC_ADMIN, bytes(out))

    def vector_indexes(self) -> List[Dict[str, Any]]:
        """`[{"field", "dims", "ready"}]` for the collection's vector indexes."""
        out = bytearray(b"\x09")
        put_str(out, self.name)
        _v, body = self._admin(w.OP_DOC_INFO, bytes(out))
        r = Reader(body)
        result = []
        for _ in range(r.count(9)):
            field = r.str()
            dims = r.u32()
            result.append({"field": field, "dims": dims, "ready": r.u8() == 1})
        return result

    # ── analytics ──

    def enable_columnar(self) -> None:
        """Also keeps the collection as column segments for `aggregate` / `scan_columns`
        (built in the background; results are exact either way)."""
        self._admin(w.OP_DOC_ADMIN, an.encode_columnar(self.name, True))

    def disable_columnar(self) -> None:
        """Deletes the column segments (analytics then read the row store)."""
        self._admin(w.OP_DOC_ADMIN, an.encode_columnar(self.name, False))

    def aggregate(self, aggregates: Dict[str, an.Aggregate], where: Any = None, *, group_by: Any = None,
                  order_by: Any = None, limit: Optional[int] = None, memory_budget: int = 0,
                  spill_budget: int = 0) -> an.AggregateResult:
        """Filters, groups and aggregates on the server, exactly and as of one snapshot.

        `aggregates` maps output names to `count()`, `count(x)`, `sum(x)`, `avg(x)`, `min(x)`,
        `max(x)`, `count_distinct(x)` or `approx_count_distinct(x)`, where `x` is a field path or an
        expression (`field("price") * "qty"`). `group_by` takes field paths and
        `bucket(field, width, offset)`; `order_by` names output columns (`"-revenue"` descending).
        Rows are dicts: group-key columns (named by their path) first, then the aggregates.
        Budgets are in bytes (0 = server default).
        """
        q = an.build_aggregate(aggregates, group_by, order_by)
        if limit is not None and (not isinstance(limit, int) or limit <= 0):
            raise InvalidError("limit must be a positive integer (or None for every group)")
        body = an.encode_aggregate(self.name, w.where_to_filter(where), q.group_by, q.aggregates, q.order,
                                   limit or 0, memory_budget, spill_budget)
        status, _v, rbody = self.client._request(an.OP_DOC_AGGREGATE, body, idempotent=True)
        if status != w.ST_OK:
            self.client._raise(status, rbody)
        return an.aggregate_result(q.columns, rbody)

    def scan_columns(self, fields: Sequence[str], where: Any = None, *, page_size: int = 1000) -> "ColumnScan":
        """Iterates over `ColumnRow(key, {field: value})` for every matching document in key
        order, reading only `fields` (dotted paths; missing fields are None), page by page.
        The returned iterator's `.total` is the number of matches once the first page is read."""
        if isinstance(fields, str):
            raise InvalidError("fields is a list of field paths")
        fields = list(fields)
        if len(fields) > an.MAX_ITEMS or not all(isinstance(f, str) for f in fields):
            raise InvalidError(f"fields is a list of at most {an.MAX_ITEMS} field paths")
        if not isinstance(page_size, int) or not 1 <= page_size <= an.MAX_SCAN_PAGE:
            raise InvalidError(f"page_size must be between 1 and {an.MAX_SCAN_PAGE}")
        return ColumnScan(self, fields, w.where_to_filter(where), page_size)

    def _query(self, where: Any, order_by: Any, limit: int, offset: int, after: Optional[str],
               nearest: Optional[Tuple[str, Sequence[float], int, bool]] = None) -> Tuple[int, Optional[str], List[Document]]:
        body = w.encode_query(self.name, w.where_to_filter(where), w.parse_order_by(order_by), limit, offset, after, nearest)
        status, _v, rbody = self.client._request(w.OP_DOC_QUERY, body, idempotent=True)
        if status != w.ST_OK:
            self.client._raise(status, rbody)
        total, nxt, docs = w.decode_page(rbody)
        return total, nxt, [Document(k, v, f) for k, v, f in docs]

    def create_index(self, field: str) -> None:
        """Indexes a top-level field (at most 16 per collection); existing documents included."""
        out = bytearray(b"\x01")
        put_str(out, self.name)
        put_str(out, field)
        self._admin(w.OP_DOC_ADMIN, bytes(out))

    def drop_index(self, field: str) -> None:
        out = bytearray(b"\x02")
        put_str(out, self.name)
        put_str(out, field)
        self._admin(w.OP_DOC_ADMIN, bytes(out))

    def drop(self) -> int:
        """Deletes every document and the collection's metadata; returns the number deleted."""
        out = bytearray(b"\x04")
        put_str(out, self.name)
        return self._admin(w.OP_DOC_ADMIN, bytes(out))[0]

    def info(self) -> Dict[str, Any]:
        """Indexes and metadata of the collection."""
        out = bytearray(b"\x06")
        put_str(out, self.name)
        _v, body = self._admin(w.OP_DOC_INFO, bytes(out))
        return w.decode_info(Reader(body))

    def _admin(self, op: int, body: bytes) -> Tuple[int, bytes]:
        status, version, rbody = self.client._request(op, body, idempotent=True)
        if status != w.ST_OK:
            self.client._raise(status, rbody)
        return version, rbody


class ColumnScan:
    """Iterator of `Collection.scan_columns`: yields `ColumnRow(key, values)`, fetching pages
    lazily by `next_after_key`. `total` is None until the first page has been read."""

    def __init__(self, collection: Collection, fields: List[str], flt: Optional[w.Filter], page_size: int) -> None:
        self._collection = collection
        self._fields = fields
        self._filter = flt
        self._page_size = page_size
        self._rows: List[an.ColumnRow] = []
        self._pos = 0
        self._after: Optional[str] = None
        self._done = False
        self.total: Optional[int] = None

    def __iter__(self) -> "ColumnScan":
        return self

    def __next__(self) -> an.ColumnRow:
        while self._pos >= len(self._rows):
            if self._done:
                raise StopIteration
            self._fetch()
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def _fetch(self) -> None:
        c = self._collection
        body = an.encode_scan(c.name, self._filter, self._fields, self._after, self._page_size)
        status, _v, rbody = c.client._request(an.OP_DOC_SCAN_COLUMNS, body, idempotent=True)
        if status != w.ST_OK:
            c.client._raise(status, rbody)
        total, nxt, nf, rows = an.decode_scan_page(rbody)
        if rows and nf != len(self._fields):
            raise InvalidError(f"the server returned {nf} fields, expected {len(self._fields)}")
        self.total = total
        self._rows = [an.ColumnRow(k, dict(zip(self._fields, vals))) for k, vals in rows]
        self._pos = 0
        if nxt is None:
            self._done = True
        else:
            self._after = nxt


class Transaction(_Writer):
    """Writes buffered locally and committed atomically (all or none) by `commit()`."""

    def __init__(self, client: Client) -> None:
        self.client = client
        self.writes: List[w.Write] = []
        self.versions: List[Optional[int]] = []

    def put(self, collection: str, key: str, fields: Dict[str, Any], *, ttl: Optional[float] = None) -> "Transaction":
        self.writes.append(self._put_write(collection, key, fields, ttl, 0))
        return self

    def insert(self, collection: str, key: str, fields: Dict[str, Any]) -> "Transaction":
        self.writes.append(self._put_write(collection, key, fields, None, 1))
        return self

    def replace(self, collection: str, key: str, fields: Dict[str, Any], *, if_version: Optional[int] = None) -> "Transaction":
        pre, ver = (3, if_version) if if_version is not None else (2, 0)
        self.writes.append(self._put_write(collection, key, fields, None, pre, ver))
        return self

    def update(self, collection: str, key: str, *changes: Any, if_version: Optional[int] = None) -> "Transaction":
        pre, ver = (3, if_version) if if_version is not None else (0, 0)
        self.writes.append(w.Write(collection, key, 4, pre, ver, ops=_ops(changes)))
        return self

    def delete(self, collection: str, key: str, *, if_version: Optional[int] = None) -> "Transaction":
        pre, ver = (3, if_version) if if_version is not None else (0, 0)
        self.writes.append(w.Write(collection, key, 3, pre, ver))
        return self

    def require(self, collection: str, key: str, *, version: Optional[int] = None, exists: bool = True) -> "Transaction":
        """Commit only if the document is at `version` (or simply exists / does not exist)."""
        if version is not None:
            self.writes.append(w.Write(collection, key, 5, 3, version))
        else:
            self.writes.append(w.Write(collection, key, 5, 2 if exists else 1))
        return self

    def commit(self) -> List[Optional[int]]:
        """Applies every buffered write atomically; returns the new versions in order."""
        if not self.writes:
            return []
        self.versions = self.client.write(self.writes)
        self.writes = []
        return self.versions

    def __enter__(self) -> "Transaction":
        return self

    def __exit__(self, exc_type: Any, *exc: Any) -> None:
        if exc_type is None:
            self.commit()
