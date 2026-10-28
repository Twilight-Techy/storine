"""Django ORM against storine-sql, on both the PostgreSQL and the MySQL endpoint.

    storine-sql --data /tmp/db --pg 127.0.0.1:55432 --mysql 127.0.0.1:53306 &
    pip install django psycopg2-binary pymysql
    python test_django.py postgres|mysql [port]

Runs the real migrations of django.contrib.auth / contenttypes (ALTER TABLE, indexes,
constraints) and then exercises the ORM: lookups, joins, aggregates, F() updates,
transactions, select_for_update and integrity errors.
"""

import sys
import threading
from decimal import Decimal

import django
from django.conf import settings

backend = sys.argv[1] if len(sys.argv) > 1 else "postgres"
if backend == "mysql":
    import pymysql

    pymysql.version_info = (2, 2, 7, "final", 0)  # Django checks the mysqlclient version.
    pymysql.install_as_MySQLdb()
    db = {"ENGINE": "django.db.backends.mysql", "NAME": "shop", "USER": "root", "HOST": "127.0.0.1", "PORT": sys.argv[2] if len(sys.argv) > 2 else "53306"}
else:
    db = {"ENGINE": "django.db.backends.postgresql", "NAME": "shop", "USER": "app", "PASSWORD": "x", "HOST": "127.0.0.1", "PORT": sys.argv[2] if len(sys.argv) > 2 else "55432"}

settings.configure(
    DATABASES={"default": db},
    INSTALLED_APPS=["django.contrib.contenttypes", "django.contrib.auth", "shop"],
    MIGRATION_MODULES={"shop": None},
    USE_TZ=True,
    DEFAULT_AUTO_FIELD="django.db.models.BigAutoField",
)
django.setup()

from django.contrib.auth.models import Group, Permission, User  # noqa: E402
from django.core.management import call_command  # noqa: E402
from django.db import IntegrityError, connection, transaction  # noqa: E402
from django.db.models import Count, F, Q, Sum  # noqa: E402

from shop.models import Customer, Order  # noqa: E402

# Start clean: drop whatever a previous run left behind.
with connection.cursor() as c:
    if backend == "mysql":
        c.execute("SET FOREIGN_KEY_CHECKS = 0")
    for t in connection.introspection.table_names():
        c.execute(f"DROP TABLE IF EXISTS {connection.ops.quote_name(t)} CASCADE" if backend != "mysql" else f"DROP TABLE IF EXISTS `{t}`")

    if backend == "mysql":
        c.execute("SET FOREIGN_KEY_CHECKS = 1")

call_command("migrate", run_syncdb=True, verbosity=0)
assert Permission.objects.filter(codename="add_user").exists()

u = User.objects.create_user("ada", "ada@x", "pw")
g = Group.objects.create(name="staff")
u.groups.add(g)
assert User.objects.filter(groups__name="staff").get().username == "ada"
assert User.objects.get(username="ada").check_password("pw")

a = Customer.objects.create(email="a@x", balance=Decimal("10.50"), meta={"tier": "gold", "tags": [1, 2]})
b = Customer.objects.create(email="b@x")
Order.objects.bulk_create([Order(customer=a, total=Decimal("1.10")), Order(customer=a, total=Decimal("2.20"), paid=True), Order(customer=b, total=Decimal("5"))])

assert Customer.objects.get(email="a@x").balance == Decimal("10.50")
assert Customer.objects.get(pk=a.pk).meta == {"tier": "gold", "tags": [1, 2]}
rows = list(Customer.objects.annotate(n=Count("orders"), s=Sum("orders__total")).order_by("email").values_list("email", "n", "s"))
assert rows == [("a@x", 2, Decimal("3.30")), ("b@x", 1, Decimal("5.00"))], rows
assert Order.objects.filter(Q(paid=True) | Q(total__gt=4)).count() == 2
assert list(Order.objects.filter(customer__email__startswith="a").order_by("-total").values_list("total", flat=True)) == [Decimal("2.20"), Decimal("1.10")]
assert Order.objects.select_related("customer").get(total=Decimal("5")).customer.email == "b@x"

# Atomic F() updates from many threads: no lost money.
def deposit():
    for _ in range(25):
        Customer.objects.filter(pk=a.pk).update(balance=F("balance") + Decimal("1.00"))
    connection.close()

threads = [threading.Thread(target=deposit) for _ in range(4)]
for t in threads:
    t.start()
for t in threads:
    t.join()
assert Customer.objects.get(pk=a.pk).balance == Decimal("110.50")

# Read-modify-write under select_for_update in concurrent transactions.
def transfer():
    for _ in range(10):
        with transaction.atomic():
            src = Customer.objects.select_for_update().get(pk=a.pk)
            dst = Customer.objects.select_for_update().get(pk=b.pk)
            src.balance -= Decimal("1")
            dst.balance += Decimal("1")
            src.save(update_fields=["balance"])
            dst.save(update_fields=["balance"])
    connection.close()

threads = [threading.Thread(target=transfer) for _ in range(3)]
for t in threads:
    t.start()
for t in threads:
    t.join()
assert Customer.objects.get(pk=a.pk).balance == Decimal("80.50")
assert Customer.objects.get(pk=b.pk).balance == Decimal("30.00")

# Transactions roll back; integrity errors surface as IntegrityError.
try:
    with transaction.atomic():
        Customer.objects.filter(pk=a.pk).update(balance=0)
        raise RuntimeError("abort")
except RuntimeError:
    pass
assert Customer.objects.get(pk=a.pk).balance == Decimal("80.50")
try:
    Customer.objects.create(email="a@x")
    raise AssertionError("duplicate email accepted")
except IntegrityError:
    pass
with transaction.atomic():
    sid = transaction.savepoint()
    Order.objects.create(customer=b, total=1)
    transaction.savepoint_rollback(sid)
assert Order.objects.count() == 3

a.delete()
assert Order.objects.count() == 1
print(f"django ({backend}) tests passed")
