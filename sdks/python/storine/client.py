"""Storine client: connection handling, retries and the collection API."""

from __future__ import annotations

import os
import random
import socket
import struct
import threading
import time
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, unquote, urlparse

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

    def get(self, key: str) -> Optional[Document]:
        """The document `key`, or `None` if it does not exist."""
        status, version, body = self.client._request(w.OP_DOC_GET, w.encode_get(self.name, key), idempotent=True)
        if status == w.ST_NOT_FOUND:
            return None
        if status != w.ST_OK:
            self.client._raise(status, body)
        return Document(key, version, Reader(body).map())

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

    def _query(self, where: Any, order_by: Any, limit: int, offset: int, after: Optional[str]) -> Tuple[int, Optional[str], List[Document]]:
        body = w.encode_query(self.name, w.where_to_filter(where), w.parse_order_by(order_by), limit, offset, after)
        status, _v, rbody = self.client._request(w.OP_DOC_QUERY, body, idempotent=True)
        if status != w.ST_OK:
            self.client._raise(status, rbody)
        total, nxt, docs = w.decode_page(rbody)
        return total, nxt, [Document(k, v, f) for k, v, f in docs]

    def create_index(self, field: str) -> None:
        """Indexes a top-level field (at most 4 per collection); existing documents included."""
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
