"""Real-client compatibility tests for storine-sql.

Runs unmodified PostgreSQL / MySQL drivers and the SQLAlchemy ORM against a running gateway:

    storine-sql --data /tmp/db --pg 127.0.0.1:55432 --mysql 127.0.0.1:53306 &
    pip install psycopg2-binary "psycopg[binary]" asyncpg "sqlalchemy[asyncio]" pymysql
    python3 test_python.py 55432 53306

Each section prints OK or raises.
"""

import asyncio
import datetime as dt
import decimal
import sys
import threading
import uuid

PG_PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 55432
MY_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 53306
D = decimal.Decimal


def section(name):
    def wrap(fn):
        fn()
        print(f"OK  {name}")
        return fn
    return wrap


def run_all():
    import psycopg2

    @section("psycopg2: DDL, parameters, types, transactions")
    def _():
        c = psycopg2.connect(host="127.0.0.1", port=PG_PORT, user="app", password="x", dbname="shop")
        cur = c.cursor()
        cur.execute("DROP TABLE IF EXISTS p2_items")
        cur.execute("""CREATE TABLE p2_items (id SERIAL PRIMARY KEY, name VARCHAR(50) NOT NULL UNIQUE,
            price NUMERIC(10,2), qty INT DEFAULT 0, tags TEXT[], meta JSONB, ok BOOLEAN DEFAULT true,
            made TIMESTAMPTZ DEFAULT now(), day DATE, uid UUID)""")
        c.commit()
        u = uuid.uuid4()
        cur.execute("INSERT INTO p2_items (name, price, qty, tags, meta, day, uid) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
                    ("widget", D("19.99"), 3, ["a", "b"], '{"k": 1}', dt.date(2024, 5, 1), str(u)))
        (iid,) = cur.fetchone()
        c.commit()
        cur.execute("SELECT name, price, qty, tags, ok, day, uid, made FROM p2_items WHERE id = %s", (iid,))
        name, price, qty, tags, ok, day, uid, made = cur.fetchone()
        assert (name, price, qty, tags, ok, day) == ("widget", D("19.99"), 3, ["a", "b"], True, dt.date(2024, 5, 1)), (name, price, qty, tags, ok, day)
        assert str(uid) == str(u), uid
        assert isinstance(made, dt.datetime) and made.tzinfo is not None, made
        try:
            cur.execute("INSERT INTO p2_items (name) VALUES ('widget')")
            raise AssertionError("duplicate accepted")
        except psycopg2.errors.UniqueViolation:
            c.rollback()
        # Transaction rollback.
        cur.execute("UPDATE p2_items SET qty = 100 WHERE id = %s", (iid,))
        c.rollback()
        cur.execute("SELECT qty FROM p2_items WHERE id = %s", (iid,))
        assert cur.fetchone()[0] == 3
        cur.execute("SELECT count(*), sum(price) FROM p2_items")
        assert cur.fetchone() == (1, D("19.99"))
        c.close()

    @section("psycopg2: no lost updates under concurrency")
    def _():
        c = psycopg2.connect(host="127.0.0.1", port=PG_PORT, user="app", dbname="shop")
        c.autocommit = True
        cur = c.cursor()
        cur.execute("DROP TABLE IF EXISTS p2_acct")
        cur.execute("CREATE TABLE p2_acct (id TEXT PRIMARY KEY, balance NUMERIC(12,2) NOT NULL)")
        cur.execute("INSERT INTO p2_acct VALUES ('a', 0)")
        errors = []

        def worker():
            try:
                cc = psycopg2.connect(host="127.0.0.1", port=PG_PORT, user="app", dbname="shop")
                k = cc.cursor()
                for _ in range(50):
                    # Classic read-modify-write in a transaction with SELECT ... FOR UPDATE.
                    k.execute("SELECT balance FROM p2_acct WHERE id = 'a' FOR UPDATE")
                    (b,) = k.fetchone()
                    k.execute("UPDATE p2_acct SET balance = %s WHERE id = 'a'", (b + D("0.01"),))
                    cc.commit()
                cc.close()
            except Exception as e:  # pragma: no cover
                errors.append(e)

        ts = [threading.Thread(target=worker) for _ in range(6)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        assert not errors, errors
        cur.execute("SELECT balance FROM p2_acct WHERE id = 'a'")
        assert cur.fetchone()[0] == D("3.00")
        c.close()

    import psycopg

    @section("psycopg 3: server-side binding, binary results, executemany")
    def _():
        with psycopg.connect(f"host=127.0.0.1 port={PG_PORT} user=app dbname=shop") as c:
            c.execute("DROP TABLE IF EXISTS p3_t")
            c.execute("CREATE TABLE p3_t (id BIGINT PRIMARY KEY, amount NUMERIC(14,4), at TIMESTAMP, note TEXT)")
            with c.cursor() as cur:
                cur.executemany("INSERT INTO p3_t VALUES (%s, %s, %s, %s)",
                                [(i, D("1.2345") * i, dt.datetime(2024, 1, 1, 12, 0, i), f"n{i}") for i in range(1, 6)])
            c.commit()
            with c.cursor(binary=True) as cur:
                cur.execute("SELECT id, amount, at, note FROM p3_t WHERE id >= %s ORDER BY id", (3,))
                rows = cur.fetchall()
            assert rows[0] == (3, D("3.7035"), dt.datetime(2024, 1, 1, 12, 0, 3), "n3"), rows[0]
            assert len(rows) == 3
            row = c.execute("SELECT count(*) FROM p3_t WHERE note LIKE %s", ("n%",)).fetchone()
            assert row == (5,), row
            # COPY in both directions (text format), including NULLs and escapes.
            with c.cursor() as cur:
                with cur.copy("COPY p3_t (id, amount, note) FROM STDIN") as cp:
                    for i in range(100, 110):
                        cp.write_row((i, D("0.0001") * i, None if i % 2 else f"tab\there {i}"))
                with cur.copy("COPY (SELECT id, note FROM p3_t WHERE id >= 100 ORDER BY id) TO STDOUT") as cp:
                    out = list(cp.rows())
            c.commit()
            assert out[0] == ("100", "tab\there 100") and out[1] == ("101", None), out[:2]
            assert c.execute("SELECT sum(amount) FROM p3_t WHERE id >= 100").fetchone() == (D("0.1045"),)
            with c.cursor() as cur:
                with cur.copy("COPY (SELECT id, amount FROM p3_t WHERE id IN (100, 101) ORDER BY id) TO STDOUT (FORMAT BINARY)") as cp:
                    cp.set_types(["int8", "numeric"])
                    assert list(cp.rows()) == [(100, D("0.0100")), (101, D("0.0101"))]

    import os, ssl

    @section("TLS 1.3 and 1.2: psycopg sslmode=verify-full (pinned certificate), PyMySQL")
    def _():
        cert = os.environ.get("STORINE_TLS_CERT")
        mode = f"sslmode=verify-full sslrootcert={cert}" if cert else "sslmode=require"
        for version in ("TLSv1.3", "TLSv1.2"):
            dsn = f"host=localhost port={PG_PORT} user=app dbname=shop {mode} ssl_max_protocol_version={version}"
            with psycopg.connect(dsn) as c:
                assert c.info.pgconn.ssl_in_use
                assert len(c.execute("SELECT repeat('p', 100000)").fetchone()[0]) == 100000
        import pymysql as _pm
        for version in (ssl.TLSVersion.TLSv1_3, ssl.TLSVersion.TLSv1_2):
            ctx = ssl.create_default_context(cafile=cert) if cert else ssl._create_unverified_context()
            ctx.maximum_version = version
            m = _pm.connect(host="localhost", port=MY_PORT, user="root", database="shop", ssl=ctx)
            cur = m.cursor()
            cur.execute("SELECT 6 * 7")
            assert cur.fetchone() == (42,)
            m.close()

    import asyncpg

    @section("asyncpg: binary protocol, prepared statements, transactions")
    def _():
        async def main():
            c = await asyncpg.connect(host="127.0.0.1", port=PG_PORT, user="app", database="shop")
            await c.execute("DROP TABLE IF EXISTS ap_t")
            await c.execute("CREATE TABLE ap_t (id SERIAL PRIMARY KEY, name TEXT, score NUMERIC(8,2), active BOOLEAN, born DATE)")
            await c.executemany("INSERT INTO ap_t (name, score, active, born) VALUES ($1, $2, $3, $4)",
                                [("x", D("1.50"), True, dt.date(2000, 1, 2)), ("y", D("2.25"), False, None)])
            rows = await c.fetch("SELECT id, name, score, active, born FROM ap_t WHERE score > $1 ORDER BY id", D("1"))
            assert [tuple(r) for r in rows] == [(1, "x", D("1.50"), True, dt.date(2000, 1, 2)), (2, "y", D("2.25"), False, None)], rows
            async with c.transaction():
                await c.execute("UPDATE ap_t SET score = score + $1 WHERE id = $2", D("0.25"), 1)
            assert await c.fetchval("SELECT score FROM ap_t WHERE id = 1") == D("1.75")
            try:
                async with c.transaction():
                    await c.execute("UPDATE ap_t SET score = 0 WHERE id = 1")
                    raise RuntimeError("abort")
            except RuntimeError:
                pass
            assert await c.fetchval("SELECT score FROM ap_t WHERE id = 1") == D("1.75")
            # Bulk load through binary COPY.
            n = await c.copy_records_to_table(
                "ap_t", records=[(10 + i, f"bulk{i}", D("0.10"), i % 2 == 0, dt.date(2001, 1, 1 + i)) for i in range(20)],
                columns=["id", "name", "score", "active", "born"])
            assert n == "COPY 20", n
            assert await c.fetchval("SELECT sum(score) FROM ap_t WHERE name LIKE 'bulk%'") == D("2.00")
            assert await c.fetchval("SELECT born FROM ap_t WHERE id = 12") == dt.date(2001, 1, 3)
            await c.close()
        asyncio.run(main())

    import sqlalchemy as sa
    from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship

    def orm_suite(url, label):
        class Base(DeclarativeBase):
            pass

        class Customer(Base):
            __tablename__ = f"orm_customer"
            id: Mapped[int] = mapped_column(primary_key=True)
            email: Mapped[str] = mapped_column(sa.String(120), unique=True)
            balance: Mapped[D] = mapped_column(sa.Numeric(12, 2), default=D("0"))
            created: Mapped[dt.datetime] = mapped_column(sa.DateTime, default=dt.datetime(2024, 1, 1))
            orders: Mapped[list["Order"]] = relationship(back_populates="customer", cascade="all, delete-orphan")

        class Order(Base):
            __tablename__ = f"orm_order"
            id: Mapped[int] = mapped_column(primary_key=True)
            customer_id: Mapped[int] = mapped_column(sa.ForeignKey("orm_customer.id", ondelete="CASCADE"), index=True)
            total: Mapped[D] = mapped_column(sa.Numeric(12, 2))
            note: Mapped[str | None] = mapped_column(sa.Text)
            customer: Mapped[Customer] = relationship(back_populates="orders")

        eng = sa.create_engine(url)
        Base.metadata.drop_all(eng)
        Base.metadata.create_all(eng)
        insp = sa.inspect(eng)
        assert set(insp.get_table_names()) >= {"orm_customer", "orm_order"}, insp.get_table_names()
        cols = {c["name"] for c in insp.get_columns("orm_order")}
        assert cols == {"id", "customer_id", "total", "note"}, cols
        fks = insp.get_foreign_keys("orm_order")
        assert fks and fks[0]["referred_table"] == "orm_customer", fks
        with Session(eng) as s:
            a = Customer(email="a@x", balance=D("10.00"), orders=[Order(total=D("5.50")), Order(total=D("1.25"), note="n")])
            b = Customer(email="b@x")
            s.add_all([a, b])
            s.commit()
            got = s.scalars(sa.select(Customer).where(Customer.email == "a@x")).one()
            assert got.balance == D("10.00") and len(got.orders) == 2
            totals = s.execute(sa.select(Customer.email, sa.func.count(Order.id), sa.func.sum(Order.total))
                               .join(Order, isouter=True).group_by(Customer.email).order_by(Customer.email)).all()
            assert [tuple(t) for t in totals] == [("a@x", 2, D("6.75")), ("b@x", 0, None)], totals
            # Selectin / joined eager loading.
            from sqlalchemy.orm import joinedload, selectinload
            cs = s.scalars(sa.select(Customer).options(selectinload(Customer.orders)).order_by(Customer.id)).all()
            assert [len(c.orders) for c in cs] == [2, 0]
            os_ = s.scalars(sa.select(Order).options(joinedload(Order.customer)).order_by(Order.id)).all()
            assert {o.customer.email for o in os_} == {"a@x"}
            # Atomic update expression.
            s.execute(sa.update(Customer).where(Customer.id == got.id).values(balance=Customer.balance - D("2.50")))
            s.commit()
            assert s.get(Customer, got.id).balance == D("7.50")
            # Unique violation surfaces as IntegrityError.
            s.add(Customer(email="a@x"))
            try:
                s.commit()
                raise AssertionError("duplicate accepted")
            except sa.exc.IntegrityError:
                s.rollback()
            # Cascade delete through the ORM.
            s.delete(s.get(Customer, got.id))
            s.commit()
            assert s.scalar(sa.select(sa.func.count()).select_from(Order)) == 0
            # Pagination.
            for i in range(25):
                s.add(Customer(email=f"p{i:02d}@x"))
            s.commit()
            page = s.scalars(sa.select(Customer.email).where(Customer.email.like("p%")).order_by(Customer.email.desc()).limit(5).offset(5)).all()
            assert page == [f"p{i:02d}@x" for i in range(19, 14, -1)], page
        Base.metadata.drop_all(eng)
        eng.dispose()

    @section("SQLAlchemy ORM over psycopg2 (PostgreSQL)")
    def _():
        orm_suite(f"postgresql+psycopg2://app:x@127.0.0.1:{PG_PORT}/shop", "pg2")

    @section("SQLAlchemy ORM over psycopg 3 (PostgreSQL)")
    def _():
        orm_suite(f"postgresql+psycopg://app:x@127.0.0.1:{PG_PORT}/shop", "pg3")

    @section("SQLAlchemy ORM over asyncpg (PostgreSQL)")
    def _():
        from sqlalchemy.ext.asyncio import create_async_engine

        async def main():
            eng = create_async_engine(f"postgresql+asyncpg://app:x@127.0.0.1:{PG_PORT}/shop")
            async with eng.begin() as conn:
                await conn.execute(sa.text("DROP TABLE IF EXISTS aio_t"))
                await conn.execute(sa.text("CREATE TABLE aio_t (id INT PRIMARY KEY, v NUMERIC(6,2))"))
                await conn.execute(sa.text("INSERT INTO aio_t VALUES (:i, :v)"), [{"i": 1, "v": D("1.10")}, {"i": 2, "v": D("2.20")}])
            async with eng.connect() as conn:
                r = await conn.execute(sa.text("SELECT sum(v) FROM aio_t"))
                assert r.scalar() == D("3.30")
            await eng.dispose()
        asyncio.run(main())

    try:
        import numpy as np
        import pgvector  # noqa: F401
    except ImportError:
        np = None
    if np is not None:
        @section("pgvector: psycopg 3 / psycopg2 adapters, operators, HNSW index, SQLAlchemy Vector")
        def _():
            from pgvector.psycopg import register_vector
            from pgvector.psycopg2 import register_vector as register_vector2
            with psycopg.connect(f"host=127.0.0.1 port={PG_PORT} user=app dbname=shop", autocommit=True) as c:
                c.execute("CREATE EXTENSION IF NOT EXISTS vector")
                register_vector(c)
                c.execute("DROP TABLE IF EXISTS pv_items")
                c.execute("CREATE TABLE pv_items (id int PRIMARY KEY, cat int, embedding vector(8))")
                rng = np.random.default_rng(7)
                data = rng.standard_normal((3000, 8)).astype(np.float32)
                with c.cursor() as cur:
                    with cur.copy("COPY pv_items (id, cat, embedding) FROM STDIN") as cp:
                        for i, v in enumerate(data):
                            cp.write_row((i, i % 3, v))
                q = rng.standard_normal(8).astype(np.float32)
                ids = lambda sql, *a: [r[0] for r in c.execute(sql, a).fetchall()]
                exact = ids("SELECT id FROM pv_items ORDER BY embedding <=> %s LIMIT 10", q)
                # Distances agree with numpy (cosine, L2, inner product, L1).
                row = c.execute("SELECT embedding <=> %s, embedding <-> %s, embedding <#> %s, embedding <+> %s, embedding "
                                "FROM pv_items WHERE id = %s", (q, q, q, q, exact[0])).fetchone()
                v = row[4].to_numpy()
                cos = 1 - float(np.dot(v, q)) / float(np.linalg.norm(v) * np.linalg.norm(q))
                assert abs(row[0] - cos) < 1e-6 and abs(row[1] - float(np.linalg.norm(v - q))) < 1e-5, row
                assert abs(row[2] + float(np.dot(v, q))) < 1e-5 and abs(row[3] - float(np.abs(v - q).sum())) < 1e-5
                # The HNSW index serves ORDER BY ... LIMIT (approximate, like pgvector).
                c.execute("CREATE INDEX pv_items_emb ON pv_items USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)")
                c.execute("SET hnsw.ef_search = 40")
                approx = ids("SELECT id FROM pv_items ORDER BY embedding <=> %s LIMIT 10", q)
                assert len(set(approx) & set(exact)) >= 8, (approx, exact)
                cat = ids("SELECT id, embedding <=> %s AS distance FROM pv_items WHERE cat = 1 ORDER BY distance LIMIT 5", q)
                assert len(cat) == 5 and all(i % 3 == 1 for i in cat), cat
                assert sorted(ids("SELECT id FROM pv_items WHERE id < 3 ORDER BY embedding <=> %s LIMIT 10", q)) == [0, 1, 2]
                with c.cursor(binary=True) as cur:
                    cur.execute("SELECT embedding FROM pv_items WHERE id = 0")
                    assert np.allclose(cur.fetchone()[0].to_numpy(), data[0])
                assert c.execute("SELECT vector_dims(embedding), round(vector_norm('[3,4]')::numeric, 1) FROM pv_items LIMIT 1").fetchone() == (8, D("5.0"))
                try:
                    c.execute("INSERT INTO pv_items VALUES (99999, 0, '[1,2]')")
                    raise AssertionError("wrong dimensions accepted")
                except psycopg.errors.DataError:
                    pass
            c2 = psycopg2.connect(host="127.0.0.1", port=PG_PORT, user="app", dbname="shop")
            register_vector2(c2)
            cur = c2.cursor()
            cur.execute("SELECT id, embedding FROM pv_items ORDER BY embedding <-> %s LIMIT 1", (data[42],))
            rid, emb = cur.fetchone()
            emb = emb.to_numpy() if hasattr(emb, "to_numpy") else emb  # pgvector >= 0.4 returns Vector
            assert rid == 42 and np.allclose(emb, data[42])
            c2.close()
            from pgvector.sqlalchemy import Vector
            from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

            class Base(DeclarativeBase):
                pass

            class Item(Base):
                __tablename__ = "pv_orm_items"
                id: Mapped[int] = mapped_column(primary_key=True)
                embedding = mapped_column(Vector(3))

            eng = sa.create_engine(f"postgresql+psycopg://app:x@127.0.0.1:{PG_PORT}/shop")
            Base.metadata.drop_all(eng)
            Base.metadata.create_all(eng)
            with Session(eng) as s:
                s.add_all([Item(id=1, embedding=[1, 0, 0]), Item(id=2, embedding=[0, 1, 0]), Item(id=3, embedding=[0.9, 0.1, 0])])
                s.commit()
                near = s.scalars(sa.select(Item).order_by(Item.embedding.cosine_distance([1, 0.05, 0])).limit(2)).all()
                assert [i.id for i in near] == [1, 3], [i.id for i in near]
                far = s.scalars(sa.select(Item).order_by(Item.embedding.l2_distance([0, 1, 0])).limit(1)).one()
                assert far.id == 2 and list(far.embedding) == [0, 1, 0]
            Base.metadata.drop_all(eng)
            eng.dispose()

    if MY_PORT:
        import pymysql

        @section("PyMySQL: text protocol, parameters, transactions")
        def _():
            c = pymysql.connect(host="127.0.0.1", port=MY_PORT, user="root", password="", database="shop")
            cur = c.cursor()
            cur.execute("DROP TABLE IF EXISTS my_items")
            cur.execute("CREATE TABLE my_items (id INT AUTO_INCREMENT PRIMARY KEY, sku VARCHAR(20) NOT NULL UNIQUE, price DECIMAL(10,2), qty INT DEFAULT 0, made DATETIME, active TINYINT(1) DEFAULT 1)")
            cur.execute("INSERT INTO my_items (sku, price, made) VALUES (%s, %s, %s)", ("A1", D("9.99"), dt.datetime(2024, 3, 4, 5, 6, 7)))
            assert cur.lastrowid == 1, cur.lastrowid
            cur.executemany("INSERT INTO my_items (sku, price) VALUES (%s, %s)", [("B2", D("1.00")), ("C3", D("2.50"))])
            c.commit()
            cur.execute("SELECT sku, price, made, active FROM my_items ORDER BY id")
            rows = cur.fetchall()
            assert rows[0] == ("A1", D("9.99"), dt.datetime(2024, 3, 4, 5, 6, 7), 1), rows[0]
            cur.execute("UPDATE my_items SET qty = qty + 5 WHERE sku = %s", ("B2",))
            c.commit()
            cur.execute("SELECT qty FROM my_items WHERE sku = 'B2'")
            assert cur.fetchone() == (5,)
            cur.execute("INSERT INTO my_items (sku, price) VALUES ('A1', 1) ON DUPLICATE KEY UPDATE price = VALUES(price)")
            c.commit()
            cur.execute("SELECT price FROM my_items WHERE sku = 'A1'")
            assert cur.fetchone() == (D("1.00"),)
            try:
                cur.execute("INSERT INTO my_items (sku) VALUES ('A1')")
                raise AssertionError("duplicate accepted")
            except pymysql.err.IntegrityError as e:
                assert e.args[0] == 1062
            c.rollback()
            cur.execute("SHOW TABLES")
            assert ("my_items",) in cur.fetchall()
            # Switching autocommit back on must take effect: PyMySQL only sends SET autocommit
            # when the server status flags disagree, so the flags have to be accurate.
            assert not c.get_autocommit()
            c.autocommit(True)
            assert c.get_autocommit()
            cur.execute("UPDATE my_items SET qty = 42 WHERE sku = 'C3'")
            other = pymysql.connect(host="127.0.0.1", port=MY_PORT, user="root", database="shop")
            oc = other.cursor()
            oc.execute("SELECT qty FROM my_items WHERE sku = 'C3'")
            assert oc.fetchone() == (42,)
            other.close()
            c.close()

        @section("SQLAlchemy ORM over PyMySQL (MySQL)")
        def _():
            orm_suite(f"mysql+pymysql://root:@127.0.0.1:{MY_PORT}/shop", "my")


if __name__ == "__main__":
    run_all()
    print("all client tests passed")
