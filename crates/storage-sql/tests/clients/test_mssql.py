"""Real SQL Server clients against storine-sql's TDS endpoint.

    storine-sql --data /tmp/db --mssql 127.0.0.1:51433 &
    pip install pymssql "sqlalchemy>=2"
    python3 test_mssql.py 51433

Each section prints OK or raises.
"""

import datetime as dt
import decimal
import sys
import threading

MS_PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 51433
D = decimal.Decimal


def section(name):
    def wrap(fn):
        fn()
        print(f"OK  {name}")
        return fn
    return wrap


def run_all():
    import pymssql

    def connect(**kw):
        return pymssql.connect(server="127.0.0.1", port=MS_PORT, user="sa", password="x", database="shop", **kw)

    @section("pymssql (FreeTDS): DDL, parameters, types, transactions, errors")
    def _():
        c = connect()
        cur = c.cursor()
        cur.execute("DROP TABLE IF EXISTS pm_items")
        cur.execute("""CREATE TABLE pm_items (id INT IDENTITY(1,1) PRIMARY KEY, name NVARCHAR(50) NOT NULL UNIQUE,
            price DECIMAL(10,2), qty INT DEFAULT 0, ok BIT DEFAULT 1, made DATETIME2 DEFAULT SYSDATETIME(),
            day DATE, note NVARCHAR(MAX))""")
        c.commit()
        cur.execute("INSERT INTO pm_items (name, price, qty, day, note) OUTPUT INSERTED.id VALUES (%s, %s, %s, %s, %s)",
                    ("Zoë", D("19.99"), 3, dt.date(2024, 5, 1), "n" * 10000))
        (iid,) = cur.fetchone()
        c.commit()
        cur.execute("SELECT name, price, qty, ok, day, note, made FROM pm_items WHERE id = %s", (iid,))
        name, price, qty, ok, day, note, made = cur.fetchone()
        assert (name, price, qty, ok, day, note) == ("Zoë", D("19.99"), 3, True, dt.date(2024, 5, 1), "n" * 10000), (name, price, qty, ok, day)
        assert isinstance(made, dt.datetime), made
        cur.execute("SELECT TOP 1 name FROM pm_items ORDER BY id")
        assert cur.fetchone() == ("Zoë",)
        try:
            cur.execute("INSERT INTO pm_items (name) VALUES ('Zoë')")
            raise AssertionError("duplicate accepted")
        except pymssql.IntegrityError:
            c.rollback()
        cur.execute("UPDATE pm_items SET qty = 100 WHERE id = %s", (iid,))
        c.rollback()
        cur.execute("SELECT qty FROM pm_items WHERE id = %s", (iid,))
        assert cur.fetchone() == (3,)
        c.close()

    @section("pymssql: concurrent transfers in transactions stay exact")
    def _():
        c = connect(autocommit=True)
        cur = c.cursor()
        cur.execute("DROP TABLE IF EXISTS pm_acct")
        cur.execute("CREATE TABLE pm_acct (id INT PRIMARY KEY, balance DECIMAL(12,2) NOT NULL)")
        cur.execute("INSERT INTO pm_acct VALUES (1, 100.00), (2, 0.00)")

        def worker():
            w = connect()
            wc = w.cursor()
            done = 0
            while done < 10:
                try:
                    wc.execute("UPDATE pm_acct SET balance = balance - 1.00 WHERE id = 1")
                    wc.execute("UPDATE pm_acct SET balance = balance + 1.00 WHERE id = 2")
                    w.commit()
                    done += 1
                except pymssql.Error:
                    w.rollback()
            w.close()

        ts = [threading.Thread(target=worker) for _ in range(4)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        cur.execute("SELECT balance FROM pm_acct ORDER BY id")
        assert [r[0] for r in cur.fetchall()] == [D("60.00"), D("40.00")]
        c.close()

    from sqlalchemy import (DECIMAL, Column, DateTime, ForeignKey, Integer, String, Unicode, UnicodeText,
                            create_engine, func, inspect, select, text)
    from sqlalchemy.orm import DeclarativeBase, Session, relationship

    @section("SQLAlchemy ORM over pymssql (mssql dialect): create_all, reflection, relationships, paging")
    def _():
        class Base(DeclarativeBase):
            pass

        class Customer(Base):
            __tablename__ = "sa_ms_customer"
            id = Column(Integer, primary_key=True)
            name = Column(Unicode(80), nullable=False, unique=True)
            bio = Column(UnicodeText)
            orders = relationship("Order", back_populates="customer", cascade="all, delete-orphan")

        class Order(Base):
            __tablename__ = "sa_ms_order"
            id = Column(Integer, primary_key=True)
            customer_id = Column(ForeignKey("sa_ms_customer.id"), nullable=False)
            total = Column(DECIMAL(12, 2), nullable=False)
            placed = Column(DateTime, server_default=func.sysdatetime())
            sku = Column(String(20))
            customer = relationship("Customer", back_populates="orders")

        e = create_engine(f"mssql+pymssql://sa:x@127.0.0.1:{MS_PORT}/shop")
        Base.metadata.drop_all(e)
        Base.metadata.create_all(e)
        insp = inspect(e)
        assert {"sa_ms_customer", "sa_ms_order"} <= set(insp.get_table_names()), insp.get_table_names()
        cols = {c["name"]: c for c in insp.get_columns("sa_ms_order")}
        assert set(cols) == {"id", "customer_id", "total", "placed", "sku"}, cols
        assert insp.get_pk_constraint("sa_ms_order")["constrained_columns"] == ["id"]
        fks = insp.get_foreign_keys("sa_ms_order")
        assert fks and fks[0]["referred_table"] == "sa_ms_customer", fks
        with Session(e) as s:
            for i in range(5):
                s.add(Customer(name=f"c{i}", bio="b" * 5000, orders=[Order(total=D("10.50") * (i + 1), sku=f"S{i}")]))
            s.commit()
            page = s.scalars(select(Customer).order_by(Customer.id).offset(1).limit(2)).all()
            assert [c.name for c in page] == ["c1", "c2"], [c.name for c in page]
            total = s.scalar(select(func.sum(Order.total)))
            assert total == D("157.50"), total
            top = s.scalars(select(Customer).order_by(Customer.id.desc()).limit(1)).one()
            assert top.name == "c4" and top.orders[0].sku == "S4"
            s.delete(top)
            s.commit()
            assert s.scalar(select(func.count()).select_from(Order)) == 4
            assert s.execute(text("SELECT @@VERSION")).scalar()
        Base.metadata.drop_all(e)
        assert "sa_ms_order" not in inspect(e).get_table_names()


if __name__ == "__main__":
    run_all()
    print("all SQL Server client tests passed")
