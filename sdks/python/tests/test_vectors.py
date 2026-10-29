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
from storine import _analytics as an, _sve, _wire as w  # noqa: E402

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
            "get accounts/alice: bytes 1000..1004 of photo": w.encode_get_range("accounts", "alice", "photo", 1000, 4),
            "get accounts/alice: fields owner, meta.size": w.encode_get_fields("accounts", "alice", ["owner", "meta.size"]),
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
            "query: nearest neighbours with a filter": w.encode_query(
                "items", storine.eq("kind", "book"), [], 5, 0, None, ("embedding", [0.25, -1.5, 3.0], 5, False)),
            "query: nearest neighbours, exact": w.encode_query("items", None, [], 0, 0, None, ("v", [1.0, 0.0], 1, True)),
        }
        built.update(self.analytics_requests())
        seen = 0
        for vec in VECTORS["requests"]:
            if vec["name"] in built:
                with self.subTest(vec["name"]):
                    self.assertEqual(built[vec["name"]].hex(), vec["body_hex"])
                    seen += 1
        self.assertEqual(seen, len(built))

    @staticmethod
    def analytics_requests():
        """Built through the same path as Collection.aggregate / scan_columns (minus the socket)."""
        def agg(aggregates, where=None, group_by=None, order_by=None, limit=None, memory_budget=0, spill_budget=0):
            q = an.build_aggregate(aggregates, group_by, order_by)
            return an.encode_aggregate("sales", w.where_to_filter(where), q.group_by, q.aggregates, q.order,
                                       limit or 0, memory_budget, spill_budget)
        f = storine.field
        return {
            "aggregate: by region where day >= 20, count / sum(price * qty) / avg(qty), sum desc, limit 2": agg(
                {"n": storine.count(), "revenue": storine.sum(f("price") * "qty"), "avg_qty": storine.avg("qty")},
                where=storine.ge("day", 20), group_by=["region"], order_by="-revenue", limit=2),
            "aggregate: totals (count distinct, approx count distinct, min, max, count of field)": agg({
                "regions": storine.count_distinct("region"), "customers": storine.approx_count_distinct("customer"),
                "cheapest": storine.min("price"), "dearest": storine.max(f("price")), "coupons": storine.count("coupon")}),
            "aggregate: weekly buckets of day (width 7, offset 3) and region, sum(qty - 1) and sum(-price + 2.5), budgets": agg(
                {"q": storine.sum(f("qty") - 1), "p": storine.sum(-f("price") + Decimal("2.5"))},
                group_by=[storine.bucket("day", 7, 3), "region"], order_by=["day", "-region"],
                memory_budget=1 << 20, spill_budget=1 << 30),
            "scan: qty, price where region = eu, limit 500": an.encode_scan(
                "sales", w.where_to_filter({"region": "eu"}), ["qty", "price"], None, 500),
            "scan: meta.size after s0099 (default limit)": an.encode_scan("sales", None, ["meta.size"], "s0099", 0),
            "admin: enable columnar sales": an.encode_columnar("sales", True),
            "admin: disable columnar sales": an.encode_columnar("sales", False),
        }

    def test_expressions(self):
        f = storine.field
        self.assertEqual(2 * f("a"), storine.Expr(5, children=(storine.lit(2), f("a"))))
        self.assertEqual(1 - f("a"), storine.Expr(4, children=(storine.lit(1), f("a"))))
        self.assertEqual(Decimal("1.5") + f("a"), storine.Expr(3, children=(storine.lit(Decimal("1.5")), f("a"))))
        self.assertEqual(f("a") * "b", storine.Expr(5, children=(f("a"), f("b"))))
        self.assertEqual(storine.sum("a"), storine.Aggregate(2, f("a")))
        self.assertEqual(storine.count(), storine.Aggregate(1, None))
        with self.assertRaises(storine.InvalidError):
            f("a") + None
        with self.assertRaises(storine.InvalidError):
            an.build_aggregate({"n": storine.count()}, ["region"], "-nope")
        with self.assertRaises(storine.InvalidError):
            an.build_aggregate({"region": storine.count()}, ["region"], None)
        with self.assertRaises(storine.InvalidError):
            an.build_aggregate({"n": "count"}, None, None)

    def test_responses(self):
        r = {v["name"]: v for v in VECTORS["responses"]}
        versions, dup = w.decode_write_response(bytes.fromhex(r["write ok: [v12, deleted]"]["body_hex"]))
        self.assertEqual((versions, dup), ([12, None], False))
        total, nxt, docs = w.decode_page(bytes.fromhex(r["page: total 42, next k1, one doc"]["body_hex"]))
        self.assertEqual((total, nxt, docs), (42, "k1", [("k1", 3, {"n": 1})]))
        total, data = w.decode_byte_range(bytes.fromhex(r["byte range: total 5000, 4 bytes"]["body_hex"]))
        self.assertEqual((total, data), (5000, bytes.fromhex("deadbeef")))
        agg = an.aggregate_result(["region", "n", "revenue"], bytes.fromhex(
            r["aggregate: matched 4, pruned 2, rows [eu, 3, 12.50] [us, 1, null]"]["body_hex"]))
        self.assertEqual((agg.matched, agg.pruned_row_groups, agg.spilled_bytes), (4, 2, 0))
        self.assertEqual([list(row.values()) for row in agg], [["eu", 3, Decimal("12.50")], ["us", 1, None]])
        self.assertEqual(str(agg.rows[0]["revenue"]), "12.50")
        self.assertEqual(agg.rows[1], {"region": "us", "n": 1, "revenue": None})
        total, nxt, nf, rows = an.decode_scan_page(bytes.fromhex(r["scan page: total 2, next s1, row s1 [7, 9.99]"]["body_hex"]))
        self.assertEqual((total, nxt, nf, rows), (2, "s1", 2, [("s1", [7, Decimal("9.99")])]))

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
