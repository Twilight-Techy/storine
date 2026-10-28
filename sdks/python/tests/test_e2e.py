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
from decimal import Decimal

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import storine  # noqa: E402
from storine import add, append, between, eq, gt, set_, unset  # noqa: E402

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
