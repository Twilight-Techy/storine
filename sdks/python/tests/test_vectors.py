"""Conformance: the SDK reproduces the server's encodings byte for byte (protocol/vectors)."""

import datetime as dt
import json
import pathlib
import struct
import sys
import unittest
import uuid
from decimal import Decimal

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import storine  # noqa: E402
from storine import _sve, _wire as w  # noqa: E402

VECTORS = json.loads((pathlib.Path(__file__).resolve().parents[3] / "protocol" / "vectors" / "vectors.json").read_text())


def native(desc):
    t, v = desc["type"], desc.get("value")
    if t == "null":
        return None
    if t == "bool":
        return v
    if t == "int64":
        return int(v)
    if t == "uint64":
        return storine.UInt64(int(v))
    if t == "float64":
        return struct.unpack("<d", bytes.fromhex(desc["bits"])[::-1])[0]
    if t == "int128":
        return int(v)
    if t == "timestamp":
        ns = int(v)
        return storine.Timestamp(ns)
    if t == "uuid":
        return uuid.UUID(hex=v)
    if t == "id":
        return storine.Id(bytes.fromhex(v))
    if t == "text":
        return v
    if t == "bytes":
        return bytes.fromhex(v)
    if t == "decimal":
        return Decimal(v)
    if t == "list":
        return [native(x) for x in v]
    if t == "map":
        return {k: native(x) for k, x in v}
    raise AssertionError(t)


def comparable(x):
    """Decoded timestamps may come back as datetimes: compare as nanoseconds."""
    if isinstance(x, storine.Timestamp):
        return ("ts", x.ns)
    if isinstance(x, dt.datetime):
        d = x - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
        return ("ts", (d.days * 86400 + d.seconds) * 10**9 + d.microseconds * 1000)
    if isinstance(x, Decimal):
        return ("dec", str(x))
    if isinstance(x, list):
        return [comparable(i) for i in x]
    if isinstance(x, dict):
        return {k: comparable(v) for k, v in x.items()}
    return x


class VectorTests(unittest.TestCase):
    def test_values_encode_and_decode(self):
        for vec in VECTORS["values"]:
            with self.subTest(vec["name"]):
                value = native(vec["value"])
                self.assertEqual(_sve.encode_value(value).hex(), vec["hex"])
                decoded = _sve.decode(bytes.fromhex(vec["hex"]))
                self.assertEqual(comparable(decoded), comparable(value))

    def test_document_body(self):
        doc = VECTORS["documents"][0]
        out = bytearray()
        _sve.encode_map(native(doc["fields"]), out)
        self.assertEqual(out.hex(), doc["hex"])

    def test_requests(self):
        d = {"balance": Decimal("100.00"), "owner": "alice", "active": True}
        W = w.Write
        rid = bytes.fromhex("0102030405060708090a0b0c0d0e0f10")
        built = {
            "get accounts/alice": w.encode_get("accounts", "alice"),
            "write: put": w.encode_writes([W("accounts", "alice", 1, 0, 0, d)], None),
            "write: insert (must not exist) with request id": w.encode_writes([W("accounts", "alice", 1, 1, 0, d)], b"\xab" * 16),
            "write: replace if version 7": w.encode_writes([W("accounts", "alice", 1, 3, 7, d)], None),
            "write: put with ttl 1500 ms": w.encode_writes([W("sessions", "s1", 2, 0, 0, {"u": 1}, 1500)], None),
            "write: delete": w.encode_writes([W("accounts", "alice", 3)], None),
            "write: delete if exists": w.encode_writes([W("accounts", "alice", 3, 2)], None),
            "write: transfer 25.00 (two patches, no overdraft) with request id": w.encode_writes([
                W("accounts", "alice", 4, 2, ops=[storine.add("balance", Decimal("-25.00"), min=0)]),
                W("accounts", "bob", 4, 2, ops=[storine.add("balance", Decimal("25.00"))]),
            ], rid),
            "write: patch set / unset / append / add with min and max": w.encode_writes([
                W("c", "k", 4, 2, ops=[storine.set_("profile.name", "Ann"), storine.unset("tmp"),
                                       storine.append("log", 3), storine.add("n", 1, min=0, max=10)]),
            ], None),
            "write: check version 3": w.encode_writes([W("c", "k", 5, 3, 3)], None),
            "query: all (default limit)": w.encode_query("items", None, [], 0, 0, None),
            "query: eq text": w.encode_query("items", storine.eq("kind", "book"), [], 10, 0, None),
            "query: and(gte, lt) sorted desc, offset 20": w.encode_query(
                "items", storine.ge("price", Decimal("10")) & storine.lt("price", Decimal("20.50")), w.parse_order_by("-price"), 10, 20, None),
            "query: between": w.encode_query("items", storine.between("n", 1, 5), [], 0, 0, None),
            "query: in": w.encode_query("items", storine.in_("n", [1, 2]), [], 0, 0, None),
            "query: prefix / contains / exists / missing": w.encode_query("items", storine.or_(
                storine.prefix("name", "An"), storine.contains("tags", "x"), storine.exists("email"), storine.missing("deleted_at")), [], 0, 0, None),
            "query: not / ne / le / gt / key prefix": w.encode_query("items", storine.and_(
                ~storine.ne("a", 1), storine.le("b", 2.5), storine.gt("c", 0), storine.key_prefix("user:")), [], 0, 0, None),
            "query: after key cursor": w.encode_query("items", None, [], 100, 0, "k0099"),
            "query: multi-key sort": w.encode_query("items", None, w.parse_order_by(["a", "-b"]), 5, 0, None),
        }
        seen = 0
        for vec in VECTORS["requests"]:
            if vec["name"] in built:
                with self.subTest(vec["name"]):
                    self.assertEqual(built[vec["name"]].hex(), vec["body_hex"])
                    seen += 1
        self.assertEqual(seen, len(built))

    def test_responses(self):
        r = {v["name"]: v for v in VECTORS["responses"]}
        versions, dup = w.decode_write_response(bytes.fromhex(r["write ok: [v12, deleted]"]["body_hex"]))
        self.assertEqual((versions, dup), ([12, None], False))
        total, nxt, docs = w.decode_page(bytes.fromhex(r["page: total 42, next k1, one doc"]["body_hex"]))
        self.assertEqual((total, nxt, docs), (42, "k1", [("k1", 3, {"n": 1})]))

    def test_hostile_bytes_never_crash(self):
        good = bytes.fromhex(VECTORS["values"][-1]["hex"])
        for cut in range(len(good)):
            with self.assertRaises(storine.InvalidError):
                _sve.decode(good[:cut])
        with self.assertRaises(storine.InvalidError):
            _sve.decode(bytes([0x0C, 0xFF, 0xFF, 0xFF, 0xFF]))

    def test_value_mapping_rules(self):
        with self.assertRaises(storine.InvalidError):
            _sve.encode_value(Decimal("NaN"))
        with self.assertRaises(storine.InvalidError):
            _sve.encode_value(1 << 130)
        with self.assertRaises(storine.InvalidError):
            _sve.encode_value(dt.date(2024, 1, 1))
        self.assertEqual(_sve.decode(_sve.encode_value(Decimal("1E+3"))), Decimal("1000"))
        big = Decimal("1234567890123456789012345678.0123456789")
        self.assertEqual(_sve.decode(_sve.encode_value(big)), big)


if __name__ == "__main__":
    unittest.main()
