---
title: Using an ORM
description: Point SQLAlchemy, Django, Prisma, TypeORM, Sequelize, Drizzle, GORM, Hibernate, Entity Framework Core, Eloquent or ActiveRecord at Storine through the SQL gateway, with a setup for each that the test suite runs.
---

Storine has two ways in:

| | The Storine clients | An ORM through the SQL gateway |
| :--- | :--- | :--- |
| Data model | Documents in collections | Tables, rows, joins, foreign keys |
| Speaks | Storine's own protocol | PostgreSQL, MySQL or SQL Server |
| Best for | New code, the fastest path, documents with nested fields, vector search, analytics | Existing apps and frameworks, teams that think in SQL, tools that need a SQL database |
| Read | [Clients](clients/index.md) | This page |

The eight Storine clients are not ORMs: they read and write documents directly, with no schema to declare. If your framework expects a SQL database, use its ORM through `storine-sql`, the [SQL gateway](../../crates/storage-sql/README.md). It speaks the PostgreSQL, MySQL and SQL Server protocols, so the ORM uses its usual driver and dialect and does not know it is talking to Storine. Your tables are stored as Storine documents, with Storine's durability, replication and exact decimals.

Every setup on this page is taken from a suite in `crates/storage-sql/tests/clients/` that runs against a real gateway.

## 1. Start the gateway

```bash
# a database in a folder, like SQLite but served over the network
storine-sql --data ./db --user app:secret

# or in front of a running Storine server or cluster
storine-sql --storine storine://token@10.0.0.1:7878,10.0.0.2:7878,10.0.0.3:7878 --user app:secret
```

It listens on `127.0.0.1` by default: PostgreSQL on 5432, MySQL on 3306 and SQL Server on 1433. Each option is in the [configuration reference](reference/configuration.md#storine-sql).

- **Users**: with `--user name:password` (repeatable) or `STORINE_SQL_USERS`, clients must log in. Without any, every client is trusted, so keep the gateway on loopback.
- **Databases**: the default database is `storine` (`--database`). A client that names a database that does not exist is served the default one, and `CREATE DATABASE` makes a separate one. Test databases (Django, Rails) and Prisma's shadow database rely on this, and it works.
- **TLS** is on by default, with a certificate made for `localhost` and kept in `DIR/tls/server.crt` with `--data`. Drivers that prefer TLS use it. To verify the certificate, give the driver that file (`sslmode=verify-full sslrootcert=./db/tls/server.crt`). Clients may also connect without TLS unless you pass `--require-tls`.

## 2. Pick a protocol

Use the **PostgreSQL** endpoint unless your app is written for MySQL or SQL Server. Every ORM below is tested on it, and it is the only one with pgvector's SQL for [vector search](../../crates/storage-sql/README.md#vector-search-pgvector).

Use the endpoint whose dialect your app already speaks. The gateway understands each dialect, including T-SQL on the SQL Server endpoint, and emulates the catalogs ORMs read to introspect a schema.

## 3. Connect your ORM

### Python: SQLAlchemy

```python
import sqlalchemy as sa

engine = sa.create_engine("postgresql+psycopg://app:secret@127.0.0.1:5432/shop")       # psycopg 3
# "postgresql+psycopg2://...", "mysql+pymysql://app:secret@127.0.0.1:3306/shop", "mssql+pymssql://..." also work
Base.metadata.create_all(engine)
```

Async works with `create_async_engine("postgresql+asyncpg://app:secret@127.0.0.1:5432/shop")`. Tested: `create_all` and `drop_all`, reflection with `inspect`, relationships with cascades, `selectinload` and `joinedload`, aggregates with outer joins, atomic `UPDATE ... SET balance = balance - x`, `IntegrityError` on a unique violation, paging. The pgvector adapter for SQLAlchemy works too.

### Python: Django

```python
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "shop",
        "USER": "app",
        "PASSWORD": "secret",
        "HOST": "127.0.0.1",
        "PORT": "5432",
    }
}
```

`manage.py migrate` runs Django's own apps' migrations, and the test runner creates and drops its `test_shop` database. `select_for_update` and transactions work. The MySQL backend is tested too, through PyMySQL.

### Node.js: Prisma

```prisma
datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}
```

```bash
DATABASE_URL="postgresql://app:secret@127.0.0.1:5432/shop?schema=public" npx prisma db push
```

Prisma 6 is tested on all three endpoints:

| Provider | URL |
| :--- | :--- |
| `postgresql` | `postgresql://app:secret@127.0.0.1:5432/shop?schema=public` |
| `mysql` | `mysql://app:secret@127.0.0.1:3306/shop` |
| `sqlserver` | `sqlserver://127.0.0.1:1433;database=shop;user=app;password=secret;encrypt=true;trustServerCertificate=true` |

`prisma db push` creates the schema, and introspecting it gives back exactly the same schema (`prisma migrate diff` finds no drift). `Decimal` fields stay exact.

### Node.js: TypeORM

```js
const { DataSource } = require("typeorm");

const ds = new DataSource({
  type: "postgres",              // or "mysql", "mssql"
  host: "127.0.0.1",
  port: 5432,
  username: "app",
  password: "secret",
  database: "shop",
  entities: [Wallet, Move],
});
await ds.initialize();
await ds.synchronize();
```

Tested: `synchronize` (a second run finds the schema up to date), repositories, relations, `@VersionColumn` optimistic locking, and transactions.

### Node.js: Sequelize, Drizzle and Knex

```js
const { Sequelize } = require("sequelize");
const sequelize = new Sequelize("postgres://app:secret@127.0.0.1:5432/shop", { logging: false });
// or "mysql://app:secret@127.0.0.1:3306/shop"; SQL Server:
// new Sequelize("shop", "app", "secret", { dialect: "mssql", host: "127.0.0.1", port: 1433,
//   dialectOptions: { options: { encrypt: false, trustServerCertificate: true } } })
await sequelize.sync();
```

```js
const { Pool } = require("pg");
const { drizzle } = require("drizzle-orm/node-postgres");
const db = drizzle(new Pool({ connectionString: "postgres://app:secret@127.0.0.1:5432/shop" }));
```

```js
const knex = require("knex")({ client: "pg", connection: { host: "127.0.0.1", port: 5432, user: "app", password: "secret", database: "shop" } });
```

Sequelize's `sync` (including `sync({ alter })` on SQL Server), associations, paging and transactions are tested; Drizzle on PostgreSQL and MySQL; Knex on PostgreSQL.

### Go: GORM

```go
import (
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

db, err := gorm.Open(postgres.Open("host=127.0.0.1 port=5432 user=app password=secret dbname=shop"), &gorm.Config{})
err = db.AutoMigrate(&Product{}, &Review{})
```

MySQL: `mysql.Open("app:secret@tcp(127.0.0.1:3306)/shop?parseTime=true")`. SQL Server: `sqlserver.Open("sqlserver://app:secret@127.0.0.1:1433?database=shop")`, where upserts use `MERGE`. `AutoMigrate`, preloading associations, `gorm.Expr` updates, upserts with `OnConflict` and transactions are tested.

### Java: Hibernate

```properties
hibernate.connection.url=jdbc:postgresql://127.0.0.1:5432/shop
hibernate.connection.username=app
hibernate.connection.password=secret
hibernate.hbm2ddl.auto=update
```

MySQL: `jdbc:mysql://127.0.0.1:3306/shop?allowPublicKeyRetrieval=true`. SQL Server: `jdbc:sqlserver://127.0.0.1:1433;databaseName=shop;encrypt=false`, with `SQLServerDialect`. Hibernate 6 is tested with `hbm2ddl` `create-drop` and `validate`, and `@Version` optimistic locking.

### .NET: Entity Framework Core

```csharp
options.UseNpgsql("Host=127.0.0.1;Port=5432;Username=app;Password=secret;Database=shop");
// MySQL (Pomelo): give the server version instead of auto-detecting it
options.UseMySql("Server=127.0.0.1;Port=3306;User ID=app;Password=secret;Database=shop;AllowPublicKeyRetrieval=true",
                 new MySqlServerVersion(new Version(8, 4, 3)));
// SQL Server
options.UseSqlServer("Server=tcp:127.0.0.1,1433;Database=shop;User ID=app;Password=secret;Encrypt=False");
```

Tested on all three providers: creating the tables, relationships, concurrency tokens, `ExecuteUpdate`, and on SQL Server batched `MERGE` inserts. `EnsureCreated` skips a database that already has any table, as it does elsewhere.

### PHP: Laravel Eloquent

```php
// config/database.php
'pgsql' => [
    'driver' => 'pgsql',
    'host' => '127.0.0.1',
    'port' => 5432,
    'database' => 'shop',
    'username' => 'app',
    'password' => 'secret',
],
```

Eloquent models and the schema builder are tested on the `pgsql` and `mysql` drivers. Plain PDO and `pgsql` / `mysqli` work too, with native and emulated prepares.

### Ruby: Rails ActiveRecord

```yaml
# config/database.yml
production:
  adapter: postgresql
  host: 127.0.0.1
  port: 5432
  database: shop
  username: app
  password: secret
```

ActiveRecord 8 is tested with the `postgresql` adapter and with `trilogy` for MySQL: migrations, associations, aggregates, `lock_version` optimistic locking, `update_counters` and transactions. Rails' test databases work, since `CREATE DATABASE` makes a separate database.

## Bulk imports

Before a large import, tell the store how many rows are coming. It sizes its index once, so the import doesn't pause each time the index doubles:

```sql
SELECT storine_reserve('orders', 1000000);
```

Run it as raw SQL from any ORM (`session.execute(text(...))`, `connection.cursor().execute(...)`, `prisma.$queryRaw`, `db.Exec`, `DB::select`, `ActiveRecord::Base.connection.execute`). Only a superuser may call it, and the gateway's own server token needs the admin role, since one call can take gigabytes of address space. It returns `true`. Run it from the import script with an admin login rather than from the app's everyday connection. One call covers up to 100,000,000 documents: each row counts as one, plus one per unique constraint. It is only a hint. It changes no data; on a range cluster every node is sized for its share. Most useful for imports of millions of rows; with fewer, the pauses are under a millisecond anyway.

To cover every import with no SQL at all, start the server with `STORAGE_EXPECTED_DOCUMENTS` set to about how many documents the database will hold ([server settings](server.md)). The ORM then needs nothing.

## Migrations and schema changes

ORMs create and change tables with ordinary DDL, which the gateway applies to its own catalog. Every way above of creating a schema is tested: `create_all`, `migrate`, `db push`, `synchronize`, `AutoMigrate`, `hbm2ddl`, Rails migrations and EF Core's table creation.

> [!WARNING]
> DDL is applied immediately, even inside a transaction, as on MySQL. On the PostgreSQL endpoint, a `ROLLBACK` undoes a transaction's data changes but not its `CREATE`, `ALTER` or `DROP`. So a migration that fails halfway leaves the steps it already ran: fix the cause and run it again, or undo those steps by hand.

Migration tools that take a lock first work: PostgreSQL advisory locks, MySQL `GET_LOCK` and SQL Server `sp_getapplock` are all implemented, and shared by every gateway in front of the same store.

## Transactions, retries and money

- **No lost updates.** Every written row carries the version it was read at, and a concurrent change makes the commit fail instead of being overwritten.
- **Autocommit statements** that lose such a race are re-run for you, so `UPDATE accounts SET balance = balance - 10 WHERE id = 1` from many connections at once is always exact.
- **Explicit transactions** that lose a race fail with SQLSTATE `40001` (MySQL error 1213, SQL Server error 1205), the "serialization failure, retry" error. Wrap transactions in your ORM's retry helper, or retry on that error.
- **Optimistic locking** works with every ORM's version column: `@Version`, `lock_version`, `@VersionColumn`, concurrency tokens.
- **`SELECT ... FOR UPDATE`** takes row locks, with `NOWAIT` and `SKIP LOCKED`.
- **Decimals** stay exact end to end: map money to `numeric(p, s)` / `decimal(p, s)` and to your language's decimal type.

Isolation levels behave as on the database you chose; the details are in the gateway's [transactions section](../../crates/storage-sql/README.md#transactions-and-money-safety).

## Connection pools

ORM pools work as usual. PgBouncer works in session and transaction pooling modes, and `DISCARD ALL` really resets a session. With transaction pooling, session state (`SET`, session advisory locks) does not carry between transactions, as with PostgreSQL.

## What to watch for

- **One schema per database.** `CREATE SCHEMA` is accepted, but every table lives in the database's single namespace, reported as `public` (and `dbo` on SQL Server).
- **Functions, procedures and triggers** are written in PostgreSQL syntax (`LANGUAGE sql` or a PL/pgSQL subset) on every endpoint. T-SQL procedure bodies are not accepted.
- **Extensions**: pgvector's SQL works; other PostgreSQL extensions do not.
- **SQL Server**: no MARS (one active result per connection) and logins by password, access token or Kerberos.
- **Unsupported statements** fail with SQLSTATE `0A000` and a message naming the feature; nothing is silently ignored except session settings that do not apply, such as `SET search_path`.

The full list is in the gateway's [known limitations](../../crates/storage-sql/README.md#known-limitations).

## Coming from SQLite

There is no SQLite endpoint. SQLite is a library compiled into your program, not a server with a protocol the gateway could speak, so `sqlite3` drivers and SQLite ORM dialects cannot connect to Storine.

What you can do instead:

- **Keep the "one folder, no setup" feel**: run `storine-sql --data ./db` beside your app, and switch the ORM's dialect from SQLite to PostgreSQL. Most SQLite SQL carries over: `INSERT OR REPLACE`, `ON CONFLICT ... DO NOTHING` and `ON CONFLICT ... DO UPDATE` are understood.
- **Embed it in-process** from Rust with `storage-core`: see [embedding the engine](embedding.md). That is the closest match to SQLite, without SQL.
- **Use a Storine client** if you do not need SQL: documents need no schema or migrations.

## Seeing ORM data from the rest of Storine

Tables are stored as Storine documents, so backups, replication, encryption at rest and point-in-time recovery cover them like any other data. Read and write tables through SQL, though: the gateway keeps its own catalog of tables, indexes and constraints, and checks them on the writes it handles.
