# storine-sql: PostgreSQL and MySQL for existing apps

`storine-sql` lets applications that already talk to PostgreSQL or MySQL use Storine by
**changing only their connection string**. It speaks the PostgreSQL v3 and MySQL wire
protocols, understands both SQL dialects, and emulates the catalogs that drivers and ORMs
introspect (`pg_catalog`, `information_schema`, `SHOW ...`). Your tables are stored as
Storine documents, so you keep Storine's durability, replication and exact decimals.

```text
storine-sql --data ./db                      # embedded database: PostgreSQL on 127.0.0.1:5432, MySQL on 127.0.0.1:3306
storine-sql --storine storine://h1,h2,h3     # or in front of a running Storine server / cluster
            [--pg 0.0.0.0:5432] [--mysql 0.0.0.0:3306] [--no-pg] [--no-mysql]
            [--user name:password]...        # without --user every client is trusted (keep it on localhost)
            [--database shop]                # name of the default database (default: storine)
```

Then point the application at it, exactly as you would at PostgreSQL or MySQL:

```text
postgresql://app:secret@127.0.0.1:5432/shop
mysql://app:secret@127.0.0.1:3306/shop
```

Users can also come from `STORINE_SQL_USERS=name:password,name2:password2`. PostgreSQL clients
authenticate with SCRAM-SHA-256, MySQL clients with `mysql_native_password` (both standard; no
plaintext passwords on the wire). Set `STORINE_SQL_LOG=1` to log every statement.

## Tested clients

Every suite below runs against a real `storine-sql` (`tests/clients/run_all.sh`):

| Language | Drivers | ORMs / query builders |
|---|---|---|
| Python | psycopg2, psycopg 3, asyncpg, PyMySQL | SQLAlchemy 2 (sync and asyncio, PostgreSQL and MySQL), Django 6 (migrations, test databases, `select_for_update`) |
| Node.js | pg, mysql2 | Sequelize, Knex, TypeORM (schema sync), Drizzle, Prisma 6 (`db push` with an exact introspection round trip) |
| Go | pgx, lib/pq, go-sql-driver/mysql | GORM (PostgreSQL and MySQL) |
| Java | pgJDBC, MySQL Connector/J | Hibernate 6 (both protocols, optimistic locking) |
| .NET | Npgsql, MySqlConnector | Entity Framework Core (Npgsql and Pomelo providers) |
| PHP | PDO (pgsql, mysql; native and emulated prepares), pgsql, mysqli | Laravel Eloquent and schema builder |
| Ruby | pg, trilogy | ActiveRecord 8 (migrations, locking, `update_counters`) |
| CLI | `psql`, `mysql` | |

Each suite also checks money handling under concurrency: parallel `balance = balance + x`
updates, transfers between rows in transactions, and stale-version rejection all come out exact.

## SQL support

- **Types**: `smallint`/`int`/`bigint`, `numeric(p,s)` / `decimal(p,s)` (exact, never binary
  floating point), `real`/`double`, `boolean`, `text`/`varchar(n)`/`char(n)`, `bytea`/`blob`,
  `date`, `time(p)`, `timestamp(p)` / `datetime(p)` / `timestamptz` (precision kept and
  rounded like PostgreSQL), `interval`, `uuid`, `json`/`jsonb`, arrays, MySQL `enum`.
- **Queries**: joins (inner, left, right, full, cross, `USING`, `NATURAL`, comma joins planned
  as hash joins), subqueries (scalar, `IN`, `EXISTS`, `ANY`/`ALL`, derived tables, correlated
  `LATERAL`), CTEs (including `WITH RECURSIVE`), `UNION`/`INTERSECT`/`EXCEPT`, `GROUP
  BY`/`HAVING`, `DISTINCT [ON]`, window functions (`row_number`, `rank`, `dense_rank`,
  `percent_rank`, `cume_dist`, `ntile`, `lag`, `lead`, `first/last/nth_value`, aggregates `OVER
  (PARTITION BY .. ORDER BY ..)`), set-returning functions in `FROM` and in select lists,
  `LIMIT`/`OFFSET`/`FETCH`, `FOR UPDATE [NOWAIT | SKIP LOCKED]`.
- **Writes**: `INSERT ... RETURNING`, multi-row `VALUES`, `INSERT ... SELECT`, `ON CONFLICT DO
  NOTHING / DO UPDATE` and MySQL `ON DUPLICATE KEY UPDATE`, `REPLACE`, `INSERT IGNORE`, `UPDATE
  ... FROM`, `DELETE ... USING`, `TRUNCATE`.
- **Schema**: `CREATE / ALTER / DROP TABLE` (add, drop, rename and retype columns, defaults,
  `NOT NULL`, `CHECK`, `UNIQUE`, primary and foreign keys with `CASCADE` / `SET NULL` / `SET
  DEFAULT` / `RESTRICT`), indexes, `SERIAL` / `IDENTITY` / `AUTO_INCREMENT`, sequences (`CREATE
  SEQUENCE`, `nextval`, `setval`), views (`CREATE [OR REPLACE] VIEW`, listed in the catalogs),
  `CREATE / DROP DATABASE` and `USE`.
- **Functions**: the common string, numeric, date/time, JSON (`->`, `->>`, `#>`, `@>`,
  `jsonb_build_object`, `json_agg`, `JSON_EXTRACT`, ...), array, regex, conditional and
  aggregate functions of both dialects, plus the catalog functions ORMs call (`format_type`,
  `pg_get_indexdef`, `pg_get_constraintdef`, `col_description`, `current_schemas`, ...).
- **Protocol features**: PostgreSQL simple and extended query protocol (named and unnamed
  statements and portals, parameter type inference, text and binary formats including
  `numeric` and arrays), `COPY ... FROM STDIN` / `TO STDOUT` in text, CSV and binary formats
  (psql `\copy`, psycopg `copy()`, asyncpg `copy_records_to_table`), MySQL text protocol and
  `COM_STMT_*` binary prepared statements, multi-statement queries, `LAST_INSERT_ID()`,
  `ROW_COUNT()`, `FOUND_ROWS()`, server status flags.

## Transactions and money safety

Every statement runs in a transaction: an explicit one (`BEGIN` ... `COMMIT`) or an implicit
one per statement. Writes are buffered and committed as **one atomic batch**, and every
written row carries the version it had when it was read, so a concurrent change makes the
commit fail instead of being overwritten. **No lost updates.**

- **Autocommit statements** that lose such a race are re-run transparently, so `UPDATE acct SET
  balance = balance - 10 WHERE id = 1` from many connections at once is always exact.
- **Explicit transactions** report SQLSTATE `40001` (MySQL error `1213`), which drivers and ORMs
  retry. `SELECT ... FOR UPDATE` takes row locks (honoring `lock_timeout` / `NOWAIT` / `SKIP LOCKED`).
- **Isolation levels**:
  - `READ COMMITTED` (PostgreSQL's default) and MySQL's default `REPEATABLE-READ` behave like those engines.
  - PostgreSQL `REPEATABLE READ` / `SERIALIZABLE` and MySQL `SERIALIZABLE` validate every row
    the transaction read. A stale write, write skew, or a looked-up key that appeared
    concurrently fails the commit with `40001`. This protects the naive finance pattern
    (read the balance, compute in the app, write the result) instead of losing a concurrent
    deposit. Under `READ COMMITTED`, use `FOR UPDATE` or an atomic `balance = balance + x`
    for that pattern, as you would on PostgreSQL.
- **Constraints** (primary keys, `UNIQUE`, foreign keys) are enforced by preconditions inside
  the same atomic batch, so they hold under concurrency. A parent `DELETE` racing a child
  `INSERT` cannot leave an orphan.
- `numeric` / `decimal` values are exact decimals end to end, including both binary wire formats.

## Databases

`CREATE DATABASE` makes a separate namespace: its own tables, sequences, constraints and
locks. Clients select it in the connection string, with `USE`, or with MySQL
`COM_INIT_DB`. `DROP DATABASE` removes it. Names that were never created are served by the
default database (`--database`), so clients connecting with any name keep working. This is
what Django's and Rails' test databases and Prisma's shadow databases rely on.

## Known limitations

- **No TLS yet**: run `storine-sql` on the same host or a private network, and use
  `sslmode=disable` / `prefer` (PostgreSQL) or `ssl-mode=DISABLED` / `PREFERRED` (MySQL).
  Clients that *require* TLS cannot connect yet.
- **DDL is applied immediately**, even inside a transaction (MySQL behavior). On the
  PostgreSQL endpoint, a `ROLLBACK` undoes the transaction's data changes but not its `CREATE
  / ALTER / DROP`. A migration that fails halfway leaves the steps it already ran.
- **One schema per database**: PostgreSQL `CREATE SCHEMA` is accepted, but every table lives
  in the database's single namespace (reported as `public`). Cross-database references
  (`otherdb.table`) are resolved in the current database.
- **Phantoms under SERIALIZABLE**: rows read and keys looked up are validated. A *new* row
  matching a range predicate that the transaction scanned is not detected (predicate locks).
- **`COPY ... FROM STDIN` outside a transaction commits every 5,000 rows** (inside `BEGIN` ...
  `COMMIT` it is all-or-nothing, up to the store's 256 MiB per-transaction limit). `COPY` to or
  from server-side files is not available; stream through `STDIN` / `STDOUT`.
- **Views** are read-only and evaluated when queried (no `INSERT` into a view, no materialized
  views).
- **Not supported**: stored procedures, functions and triggers, PostgreSQL extensions,
  `NOTIFY` (`LISTEN` is accepted but no notification is ever delivered). Unsupported
  statements fail with SQLSTATE `0A000` and a message naming the feature; nothing else is
  silently ignored except session settings that do not apply (e.g. `SET search_path`, `SET
  NAMES`).
- **SQL Server (TDS)** and **SQLite** endpoints are not available yet.

## Development

```text
cargo test -p storage-sql                       # executor and unit tests (embedded store)
cargo build --release -p storage-sql
target/release/storine-sql --data /tmp/db --pg 127.0.0.1:55432 --mysql 127.0.0.1:53306 &
crates/storage-sql/tests/clients/run_all.sh     # every real-client suite whose toolchain is installed
```
