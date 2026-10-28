"""storine-sql behind PgBouncer, in session and transaction pooling modes.

Run by run.sh, which starts PgBouncer on PORT with the aliases `shop_session` and `shop_tx`:

    python3 test_pgbouncer.py PORT

Checks that many clients share a few server connections without losing updates, that row
locks and transactions stay on one server connection in transaction mode, that protocol-level
prepared statements (psycopg 3, asyncpg) work through PgBouncer's statement tracking, that an
error or abandoned transaction does not leak into the next client, and that the server-side
connection count stays within the pool size.
"""

import asyncio
import decimal
import sys
import threading

import psycopg2

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 61632
D = decimal.Decimal
POOL = 4  # default_pool_size in run.sh


def conn(db, **kw):
    return psycopg2.connect(host="127.0.0.1", port=PORT, user="app", password="x", dbname=db, **kw)


def section(name):
    def wrap(fn):
        fn()
        print(f"OK  {name}")
        return fn
    return wrap


def server_connections(db):
    """Server connections PgBouncer holds for `db` (admin console `SHOW POOLS`)."""
    c = psycopg2.connect(host="127.0.0.1", port=PORT, user="app", password="x", dbname="pgbouncer")
    c.autocommit = True
    cur = c.cursor()
    cur.execute("SHOW POOLS")
    cols = [d[0] for d in cur.description]
    n = 0
    for row in cur.fetchall():
        r = dict(zip(cols, row))
        if r["database"] == db:
            n += sum(int(r[k]) for k in ("sv_active", "sv_idle", "sv_used", "sv_tested", "sv_login"))
    c.close()
    return n


def run_threads(n, fn):
    errors = []

    def wrapped(i):
        try:
            fn(i)
        except Exception as e:  # pragma: no cover
            errors.append(e)

    ts = [threading.Thread(target=wrapped, args=(i,)) for i in range(n)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert not errors, errors[:3]


def main():
    setup = conn("shop_session")
    setup.autocommit = True
    cur = setup.cursor()
    cur.execute("DROP TABLE IF EXISTS pgb_acct")
    cur.execute("CREATE TABLE pgb_acct (id TEXT PRIMARY KEY, balance NUMERIC(12,2) NOT NULL)")
    cur.execute("INSERT INTO pgb_acct VALUES ('a', 0), ('b', 1000), ('c', 0)")
    setup.close()

    for db in ("shop_session", "shop_tx"):
        @section(f"{db}: 24 clients x 25 locked read-modify-write deposits, nothing lost")
        def _():
            cur0 = conn(db)
            cur0.autocommit = True
            k0 = cur0.cursor()
            k0.execute("UPDATE pgb_acct SET balance = 0 WHERE id = 'a'")
            cur0.close()
            # Session mode: one server connection per client, so only POOL clients run at a time
            # (the others wait in PgBouncer's queue). Transaction mode: all 24 interleave on POOL.
            def worker(_):
                c = conn(db)
                k = c.cursor()
                for _ in range(25):
                    k.execute("SELECT balance FROM pgb_acct WHERE id = 'a' FOR UPDATE")
                    (b,) = k.fetchone()
                    k.execute("UPDATE pgb_acct SET balance = %s WHERE id = 'a'", (b + D("0.01"),))
                    c.commit()
                c.close()

            run_threads(24, worker)
            c = conn(db)
            k = c.cursor()
            k.execute("SELECT balance FROM pgb_acct WHERE id = 'a'")
            assert k.fetchone()[0] == D("6.00")
            c.close()
            assert server_connections(db) <= POOL, server_connections(db)

    @section("shop_tx: concurrent transfers keep the total exact")
    def _():
        def worker(i):
            c = conn("shop_tx")
            k = c.cursor()
            src, dst = ("b", "c") if i % 2 == 0 else ("c", "b")
            for _ in range(20):
                k.execute("SELECT id, balance FROM pgb_acct WHERE id IN ('b', 'c') ORDER BY id FOR UPDATE")
                bal = dict(k.fetchall())
                amt = D("1.25")
                if bal[src] >= amt:
                    k.execute("UPDATE pgb_acct SET balance = balance - %s WHERE id = %s", (amt, src))
                    k.execute("UPDATE pgb_acct SET balance = balance + %s WHERE id = %s", (amt, dst))
                c.commit()
            c.close()

        run_threads(16, worker)
        c = conn("shop_tx")
        k = c.cursor()
        k.execute("SELECT sum(balance), min(balance) FROM pgb_acct WHERE id IN ('b', 'c')")
        total, low = k.fetchone()
        assert total == D("1000.00") and low >= 0, (total, low)
        c.close()

    @section("shop_tx: a failed or rolled-back transaction does not leak into the next client")
    def _():
        c = conn("shop_tx")
        k = c.cursor()
        k.execute("INSERT INTO pgb_acct VALUES ('tmp', 1)")
        try:
            k.execute("INSERT INTO pgb_acct VALUES ('tmp', 2)")  # duplicate key: aborts the txn
            raise AssertionError("duplicate key accepted")
        except psycopg2.errors.UniqueViolation:
            c.rollback()
        k.execute("SELECT count(*) FROM pgb_acct WHERE id = 'tmp'")
        assert k.fetchone()[0] == 0
        c.commit()
        # A client that disconnects mid-transaction: PgBouncer must get the server connection
        # back to idle before giving it to anyone else.
        d = conn("shop_tx")
        kd = d.cursor()
        kd.execute("UPDATE pgb_acct SET balance = balance + 100 WHERE id = 'c'")
        d.close()  # never committed
        k.execute("SELECT balance FROM pgb_acct WHERE id IN ('b', 'c') ORDER BY id")
        b, cc = [r[0] for r in k.fetchall()]
        assert b + cc == D("1000.00"), (b, cc)
        c.commit()
        c.close()

    @section("shop_session: session state (SET, prepared statements) stays with the client")
    def _():
        c = conn("shop_session")
        c.autocommit = True
        k = c.cursor()
        k.execute("SET search_path TO public")
        k.execute("PREPARE get_bal (text) AS SELECT balance FROM pgb_acct WHERE id = $1")
        for _ in range(3):
            k.execute("EXECUTE get_bal ('a')")
            assert k.fetchone()[0] == D("6.00")
        k.execute("DEALLOCATE get_bal")
        c.close()

    @section("shop_one: DISCARD ALL between clients: no prepared statement or setting carries over")
    def _():
        a = conn("shop_one")
        a.autocommit = True
        k = a.cursor()
        k.execute("SELECT pg_backend_pid()")
        pid = k.fetchone()[0]
        k.execute("PREPARE leak AS SELECT 1")
        k.execute("SET search_path TO leaked")
        k.execute("SET application_name TO 'client-a'")
        a.close()
        b = conn("shop_one")
        b.autocommit = True
        k = b.cursor()
        k.execute("SELECT pg_backend_pid()")
        assert k.fetchone()[0] == pid, "expected the same server connection"
        k.execute("PREPARE leak AS SELECT 2")  # fails if the first client's statement survived
        k.execute("EXECUTE leak")
        assert k.fetchone()[0] == 2
        k.execute("SHOW search_path")
        assert "leaked" not in k.fetchone()[0]
        b.close()

    import psycopg

    @section("shop_tx: psycopg 3 server-side prepared statements (prepare_threshold=0)")
    def _():
        def worker(i):
            with psycopg.connect(host="127.0.0.1", port=PORT, user="app", password="x", dbname="shop_tx",
                                 prepare_threshold=0, autocommit=True) as c:
                for j in range(30):
                    row = c.execute("SELECT balance, %s::int FROM pgb_acct WHERE id = %s", (j, "a")).fetchone()
                    assert row == (D("6.00"), j), row

        run_threads(12, worker)

    import asyncpg

    @section("shop_tx: asyncpg (statement cache, transactions) over a shared pool")
    def _():
        async def go():
            pool = await asyncpg.create_pool(host="127.0.0.1", port=PORT, user="app", password="x",
                                             database="shop_tx", min_size=2, max_size=20)

            async def task(i):
                async with pool.acquire() as c:
                    for _ in range(10):
                        async with c.transaction():
                            b = await c.fetchval("SELECT balance FROM pgb_acct WHERE id = $1 FOR UPDATE", "a")
                            await c.execute("UPDATE pgb_acct SET balance = $1 WHERE id = $2", b + D("0.01"), "a")

            await asyncio.gather(*(task(i) for i in range(20)))
            v = await pool.fetchval("SELECT balance FROM pgb_acct WHERE id = 'a'")
            await pool.close()
            return v

        assert asyncio.run(go()) == D("8.00")
        assert server_connections("shop_tx") <= POOL

    import sqlalchemy as sa
    from sqlalchemy.pool import NullPool

    @section("shop_tx: SQLAlchemy with NullPool (pooling left to PgBouncer)")
    def _():
        eng = sa.create_engine(f"postgresql+psycopg2://app:x@127.0.0.1:{PORT}/shop_tx", poolclass=NullPool)

        def worker(_):
            for _ in range(10):
                with eng.begin() as c:
                    c.execute(sa.text("UPDATE pgb_acct SET balance = balance + 0.01 WHERE id = 'a'"))

        run_threads(10, worker)
        with eng.connect() as c:
            assert c.execute(sa.text("SELECT balance FROM pgb_acct WHERE id = 'a'")).scalar() == D("9.00")
        eng.dispose()

    print("all pgbouncer tests passed")


if __name__ == "__main__":
    main()
