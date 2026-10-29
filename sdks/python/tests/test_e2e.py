"""End-to-end tests against a real storage-server (single node and a 3-node cluster).

The server binary is taken from $STORINE_SERVER or <repo>/target/release/storage-server
(build it with `cargo build --release -p storage-server`).
"""

import os
import pathlib
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from datetime import datetime, timezone
from decimal import ROUND_HALF_EVEN, Decimal

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import storine  # noqa: E402
from storine import add, append, avg, between, bucket, count, count_distinct, eq, field, ge, gt, set_, unset  # noqa: E402

REPO = pathlib.Path(__file__).resolve().parents[3]
SERVER = os.environ.get("STORINE_SERVER", str(REPO / "target" / "release" / "storage-server"))


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_port(port, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"server on port {port} did not start")


def start(data_dir, port, *extra):
    p = subprocess.Popen([SERVER, data_dir, f"127.0.0.1:{port}", *extra],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_port(port)
    return p


@unittest.skipUnless(os.path.exists(SERVER), "storage-server binary not built")
class SingleNode(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp(prefix="storine_py_")
        cls.port = free_port()
        cls.proc = start(cls.dir, cls.port)
        cls.db = storine.connect(f"storine://127.0.0.1:{cls.port}")

    @classmethod
    def tearDownClass(cls):
        cls.db.close()
        cls.proc.terminate()
        cls.proc.wait(10)
        shutil.rmtree(cls.dir, ignore_errors=True)

    def test_crud_types_and_versions(self):
        c = self.db.collection("people")
        when = datetime(2024, 5, 1, 12, 30, tzinfo=timezone.utc)
        uid = uuid.uuid4()
        doc = {"name": "Ann", "age": 37, "big": 1 << 100, "score": 9.5, "price": Decimal("19.99"),
               "joined": when, "id": uid, "raw": b"\x00\xff", "tags": ["a", "b"], "address": {"city": "Lagos"},
               "nothing": None, "ok": True}
        v1 = c.insert("ann", doc)
        got = c.get("ann")
        self.assertEqual(got.version, v1)
        self.assertEqual(dict(got), doc)
        with self.assertRaises(storine.ConflictError):
            c.insert("ann", doc)
        v2 = c.update("ann", set_("address.city", "Abuja"), unset("nothing"), append("tags", "c"))
        self.assertGreater(v2, v1)
        got = c.get("ann")
        self.assertEqual(got["address"], {"city": "Abuja"})
        self.assertNotIn("nothing", got)
        self.assertEqual(got["tags"], ["a", "b", "c"])
        with self.assertRaises(storine.ConflictError):
            c.replace("ann", {"name": "x"}, if_version=v1)
        c.replace("ann", {"name": "Ann B"}, if_version=v2)
        self.assertEqual(dict(c.get("ann")), {"name": "Ann B"})
        with self.assertRaises(storine.NotFoundError):
            c.update("nobody", set_("a", 1))
        self.assertTrue(c.delete("ann"))
        self.assertFalse(c.delete("ann"))
        self.assertIsNone(c.get("ann"))

    def test_money_is_exact_and_never_lost(self):
        acct = self.db.collection("accounts")
        acct.put("a", {"balance": Decimal("0.00")})
        errors = []

        def deposit():
            try:
                for _ in range(100):
                    acct.update("a", add("balance", Decimal("0.01")))
            except Exception as e:  # pragma: no cover
                errors.append(e)

        threads = [threading.Thread(target=deposit) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(acct.get("a")["balance"], Decimal("8.00"))
        with self.assertRaises(storine.InvalidError):
            acct.update("a", add("balance", Decimal("-8.01"), min=0))
        self.assertEqual(acct.get("a")["balance"], Decimal("8.00"))

    def test_transaction_is_atomic(self):
        acct = self.db.collection("bank")
        acct.put("alice", {"balance": Decimal("50")})
        acct.put("bob", {"balance": Decimal("0")})
        with self.db.transaction() as tx:
            tx.update("bank", "alice", add("balance", Decimal("-20"), min=0))
            tx.update("bank", "bob", add("balance", Decimal("20")))
        with self.assertRaises(storine.InvalidError):
            with self.db.transaction() as tx:
                tx.update("bank", "alice", add("balance", Decimal("-31"), min=0))
                tx.update("bank", "bob", add("balance", Decimal("31")))
        self.assertEqual(acct.get("alice")["balance"], Decimal("30"))
        self.assertEqual(acct.get("bob")["balance"], Decimal("20"))

    def test_queries_indexes_and_pagination(self):
        items = self.db.collection("items")
        for i in range(120):
            items.put(f"i{i:03d}", {"n": i, "kind": "even" if i % 2 == 0 else "odd", "price": Decimal(i) / 4})
        q = between("n", 10, 19) & eq("kind", "odd")
        before = [d.key for d in items.find(q, order_by="-n")]
        self.assertEqual(before, [f"i{i:03d}" for i in (19, 17, 15, 13, 11)])
        items.create_index("n")
        items.create_index("kind")
        self.assertEqual([d.key for d in items.find(q, order_by="-n")], before)
        self.assertIn("n", items.info()["indexes"])
        self.assertEqual(items.count(gt("price", Decimal("25"))), 19)
        self.assertEqual(items.count({"kind": "even"}), 60)
        self.assertEqual(len(list(items.scan(batch=7))), 120)
        self.assertEqual(items.first(order_by="-price").key, "i119")
        self.assertIn("items", self.db.collections())

    def test_vector_search(self):
        c = self.db.collection("py_docs")
        c.create_vector_index("emb", dims=3)
        self.assertEqual(c.vector_indexes(), [{"field": "emb", "dims": 3, "ready": True}])
        for key, vec, kind in [("a", [1, 0, 0], "x"), ("b", [0, 1, 0], "y"), ("c", [0.7, 0.7, 0], "x")]:
            c.put(key, {"emb": vec, "kind": kind})
        hits = c.nearest("emb", [1, 0.1, 0], k=2)
        self.assertEqual([d.key for d in hits], ["a", "c"])
        self.assertGreater(hits[0].score, hits[1].score)
        self.assertEqual([d.key for d in c.nearest("emb", [1, 0.1, 0], k=5, where={"kind": "x"}, exact=True)], ["a", "c"])
        with self.assertRaises(storine.InvalidError):
            c.put("bad", {"emb": [1, 2]})  # wrong dimensions
        with self.assertRaises(storine.InvalidError):
            c.nearest("emb", [1, 2], k=1)
        c.drop_vector_index("emb")
        self.assertEqual(c.vector_indexes(), [])

    def test_large_values(self):
        c = self.db.collection("py_files")
        blob = os.urandom(3 * 1024 * 1024 + 17)
        v = c.put("f1", {"data": blob, "name": "cat.png", "meta": {"size": len(blob), "type": "image/png"}})
        d = c.get("f1", fields=["name", "meta.size", "absent"])
        self.assertEqual((dict(d), d.version), ({"name": "cat.png", "meta": {"size": len(blob)}}, v))
        r = c.read_bytes("f1", "data", 1000, 16)
        self.assertEqual((r.version, r.total, r.data), (v, len(blob), blob[1000:1016]))
        self.assertEqual(c.read_bytes("f1", "data", len(blob) + 5, 10).data, b"")
        self.assertEqual(b"".join(c.stream_bytes("f1", "data", chunk_size=1 << 20)), blob)
        self.assertEqual(c.read_bytes("f1", "name", 0, 3).data, b"cat")
        self.assertIsNone(c.read_bytes("nope", "data"))
        with self.assertRaises(storine.NotFoundError):
            list(c.stream_bytes("nope", "data"))
        with self.assertRaises(storine.InvalidError):
            c.read_bytes("f1", "meta")  # not bytes
        with self.assertRaises(storine.InvalidError):
            c.read_bytes("f1", "data", 0, 17 * 1024 * 1024)
        chunks = c.stream_bytes("f1", "data", chunk_size=1 << 20)
        next(chunks)
        c.update("f1", set_("name", "dog.png"))
        with self.assertRaises(storine.ConflictError):
            list(chunks)

    def test_ttl_and_errors(self):
        s = self.db.collection("sessions")
        s.put("short", {"u": 1}, ttl=0.2)
        self.assertIsNotNone(s.get("short"))
        time.sleep(0.4)
        self.assertIsNone(s.get("short"))
        with self.assertRaises(storine.InvalidError):
            s.put("bad", {"a.b": 1})
        with self.assertRaises(storine.InvalidError):
            self.db.collection("bad/name").put("k", {})

    def test_analytics(self):
        sales = self.db.collection("py_sales")
        regions = ("eu", "us", "apac")
        docs = {}
        for i in range(3000):
            d = {"region": regions[i % 3], "qty": i % 17 + 1, "price": Decimal(i % 997 + 1).scaleb(-2), "day": i % 30}
            if i % 7 == 0:
                d["coupon"] = "C" + str(i % 5)
            docs[f"s{i:05d}"] = d
        keys = sorted(docs)
        for start in range(0, len(keys), 250):
            with self.db.transaction() as tx:
                for k in keys[start:start + 250]:
                    tx.put("py_sales", k, docs[k])

        def avg6(total, n):
            return (Decimal(total) / n).quantize(Decimal("0.000001"), rounding=ROUND_HALF_EVEN)

        def expect_by_region(rows):
            out = {}
            for r in regions:
                ds = [d for d in rows if d["region"] == r]
                if not ds:
                    continue
                out[r] = {"region": r, "n": len(ds), "revenue": sum(d["price"] * d["qty"] for d in ds),
                          "avg_qty": avg6(sum(d["qty"] for d in ds), len(ds)),
                          "cheapest": min(d["price"] for d in ds),
                          "dearest": max(d["price"] for d in ds),
                          "qtys": len({d["qty"] for d in ds}), "coupons": len([d for d in ds if "coupon" in d])}
            return sorted(out.values(), key=lambda x: x["revenue"], reverse=True)

        aggs = {"n": count(), "revenue": storine.sum(field("price") * "qty"), "avg_qty": avg("qty"),
                "cheapest": storine.min("price"), "dearest": storine.max("price"), "qtys": count_distinct("qty"),
                "coupons": count("coupon")}
        expected = expect_by_region(list(docs.values()))

        def check_main():
            res = sales.aggregate(aggs, group_by=["region"], order_by="-revenue", limit=2)
            self.assertEqual(res.matched, 3000)
            self.assertEqual(res.rows, expected[:2])
            self.assertEqual(list(res.rows[0]), ["region"] + list(aggs))  # column order
            full = sales.aggregate(aggs, group_by="region", order_by="region")
            self.assertEqual(full.rows, sorted(expected, key=lambda x: x["region"]))
            return res

        check_main()
        with self.assertRaises(storine.InvalidError):
            sales.aggregate(aggs, group_by=["region"], order_by="-nope")
        total = sales.aggregate({"n": count(), "q": storine.sum("qty"), "neg": storine.sum(-field("qty") + 1)})
        self.assertEqual(total.rows, [{"n": 3000, "q": sum(d["qty"] for d in docs.values()),
                                       "neg": sum(1 - d["qty"] for d in docs.values())}])

        sales.enable_columnar()
        check_main()  # segments are built in the background: exact either way
        deadline = time.time() + 15
        while sales.aggregate({"n": count()}, gt("qty", 1000)).pruned_row_groups == 0:  # zone maps skip it
            self.assertLess(time.time(), deadline, "column segments were never built")
            time.sleep(0.05)
        check_main()
        # Weekly buckets of day (offset 2): key = 2 + floor((day - 2) / 7) * 7.
        weeks = {}
        for d in docs.values():
            b = 2 + ((d["day"] - 2) // 7) * 7
            weeks.setdefault(b, []).append(d["qty"])
        res = sales.aggregate({"n": count(), "q": storine.sum("qty")}, group_by=[bucket("day", 7, 2)], order_by="day")
        self.assertEqual([(r["day"], r["n"], r["q"]) for r in res],
                         [(b, len(v), sum(v)) for b, v in sorted(weeks.items())])
        late = [d for d in docs.values() if d["day"] >= 20]
        res = sales.aggregate(aggs, ge("day", 20), group_by=["region"], order_by="-revenue")
        self.assertEqual(res.matched, len(late))
        self.assertEqual(res.rows, expect_by_region(late))

        eu_keys = [k for k in keys if docs[k]["region"] == "eu"]
        scan = sales.scan_columns(["qty", "price"], where=eq("region", "eu"), page_size=300)
        got = list(scan)
        self.assertEqual(scan.total, len(eu_keys))
        self.assertEqual([r.key for r in got], eu_keys)
        self.assertEqual([r.values for r in got], [{"qty": docs[k]["qty"], "price": docs[k]["price"]} for k in eu_keys])
        key, values = got[0]
        self.assertEqual((key, values["qty"]), (eu_keys[0], docs[eu_keys[0]]["qty"]))
        self.assertEqual(len(list(sales.scan_columns(["qty", "absent"], page_size=500))), 3000)
        self.assertEqual(next(iter(sales.scan_columns(["absent"]))).values, {"absent": None})

        sales.disable_columnar()
        check_main()


@unittest.skipUnless(os.path.exists(SERVER), "storage-server binary not built")
class Cluster(unittest.TestCase):
    def test_three_nodes_redirect_and_failover(self):
        base = tempfile.mkdtemp(prefix="storine_py_cluster_")
        ports = [free_port() for _ in range(3)]
        peers = ",".join(f"{i + 1}=127.0.0.1:{p}" for i, p in enumerate(ports))
        procs = [subprocess.Popen([SERVER, os.path.join(base, f"n{i + 1}"), f"127.0.0.1:{p}", "--node-id", str(i + 1), "--peers", peers],
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) for i, p in enumerate(ports)]
        try:
            for p in ports:
                wait_port(p)
            # Any seed works: the client follows redirects to the leader.
            db = storine.connect("storine://" + ",".join(f"127.0.0.1:{p}" for p in ports), timeout=2.0)
            acct = db.collection("acct")
            deadline = time.time() + 15
            while True:
                try:
                    acct.put("a", {"bal": Decimal("100")})
                    break
                except storine.UnavailableError:
                    if time.time() > deadline:
                        raise
                    time.sleep(0.1)
            for _ in range(10):
                acct.update("a", add("bal", Decimal("1")))
            self.assertEqual(acct.get("a")["bal"], Decimal("110"))
            leader = db.cluster_info()["node_id"]
            procs[leader - 1].kill()
            procs[leader - 1].wait(10)
            # The client fails over to the new leader; every deposit applies exactly once.
            for _ in range(5):
                acct.update("a", add("bal", Decimal("1")))
            self.assertEqual(acct.get("a")["bal"], Decimal("115"))
            db.close()
        finally:
            for p in procs:
                if p.poll() is None:
                    p.terminate()
                    p.wait(10)
            shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
