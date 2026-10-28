# storine-sql: PostgreSQL, MySQL and SQL Server for existing apps

`storine-sql` lets applications that already talk to PostgreSQL, MySQL or Microsoft SQL Server
use Storine by **changing only their connection string**. It speaks the PostgreSQL v3, MySQL
and TDS (SQL Server) wire protocols, understands the three SQL dialects (including T-SQL), and
emulates the catalogs that drivers and ORMs introspect (`pg_catalog`, `information_schema`,
`SHOW ...`, `sys.*`, the `sp_*` metadata procedures). Your tables are stored as Storine
documents, so you keep Storine's durability, replication and exact decimals.

```text
storine-sql --data ./db                      # embedded database: PostgreSQL on 127.0.0.1:5432, MySQL on :3306, SQL Server on :1433
storine-sql --storine storine://h1,h2,h3     # or in front of a running Storine server / cluster
            [--pg 0.0.0.0:5432] [--mysql 0.0.0.0:3306] [--mssql 0.0.0.0:1433]
            [--no-pg] [--no-mysql] [--no-mssql]
            [--user name:password]...        # without --user every client is trusted (keep it on localhost)
            [--database shop]                # name of the default database (default: storine)
            [--tls-cert chain.pem --tls-key key.pem | --no-tls] [--require-tls]
```

Then point the application at it, exactly as you would at the database it was written for:

```text
postgresql://app:secret@127.0.0.1:5432/shop
mysql://app:secret@127.0.0.1:3306/shop
Server=tcp:127.0.0.1,1433;Database=shop;User ID=app;Password=secret;TrustServerCertificate=true
```

Users can also come from `STORINE_SQL_USERS=name:password,name2:password2`. PostgreSQL clients
authenticate with SCRAM-SHA-256, MySQL clients with `mysql_native_password` (both standard; no
plaintext passwords on the wire). SQL Server clients use SQL Server authentication, whose
password travels inside the (always TLS-encrypted, when TLS is on) login packet, as with SQL
Server itself. Set `STORINE_SQL_LOG=1` to log every statement.

## TLS

All three endpoints speak **TLS 1.3 and TLS 1.2**, implemented in-house like the rest of
Storine (no OpenSSL): `TLS_CHACHA20_POLY1305_SHA256` (TLS 1.3) or
`TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256` (TLS 1.2, with extended master secret), always
with `x25519` key exchange and an Ed25519 certificate. It is on by default:

- With `--data DIR`, a self-signed certificate for `localhost`, `127.0.0.1` and `::1` is
  created once in `DIR/tls/` (`server.crt`, `server.key` with mode 0600) and reused. Clients
  using `sslmode=require` / `ssl-mode=REQUIRED` connect as is. For full verification, pin it:
  `sslmode=verify-full sslrootcert=DIR/tls/server.crt`.
- `--tls-cert chain.pem --tls-key key.pem` serves your own certificate. The key must be
  Ed25519 (`openssl genpkey -algorithm ed25519 -out key.pem`, then have your CA sign it).
- `--require-tls` refuses clients that stay in plaintext; `--no-tls` turns TLS off.
- SQL Server: every encryption mode works. `Encrypt=Optional` / `encrypt=false` encrypts the
  login only (like SQL Server), `Encrypt=Mandatory` / `encrypt=true` the whole connection
  (TLS inside the TDS 7.x PRELOGIN exchange), and `Encrypt=Strict` / `encrypt=strict` is TDS
  8.0 (TLS first, ALPN `tds/8.0`). Drivers verify the certificate unless told to trust it
  (`TrustServerCertificate=true`), exactly as with a self-signed SQL Server certificate.

Verified with OpenSSL (libpq, psycopg, asyncpg, PyMySQL, PHP, Ruby, tedious, Prisma,
`openssl s_client`, including HelloRetryRequest, KeyUpdate and TLS 1.2), Java's JSSE (pgJDBC,
Connector/J, mssql-jdbc), .NET (Npgsql, MySqlConnector, Microsoft.Data.SqlClient), Go's
`crypto/tls` (pgx, go-sql-driver, go-mssqldb; TLS 1.3 and 1.2) and Node (pg, mysql2). Clients
restricted to AES cipher suites or to RSA/ECDSA certificates (some FIPS configurations)
cannot connect.

## SQL Server: access-token (OpenID Connect) logins

Besides user name and password, SQL Server clients can log in with an **access token from any
OpenID Connect provider** (Keycloak, Dex, Authentik, Zitadel, Auth0, Okta, Entra ID, ...) —
what the drivers call an access token (`SqlConnection.AccessToken`, mssql-jdbc's
`accessToken`, go-mssqldb's `NewAccessTokenConnector`, tedious'
`azure-active-directory-access-token`; the names are historical, nothing is tied to a cloud).
Tell `storine-sql` which issuer to trust:

```text
storine-sql --data /var/lib/storine-sql \
  --oidc-issuer https://id.example.com/realms/main \
  --oidc-audience storine \
  --oidc-jwks /etc/storine/jwks.json
```

- `--oidc-jwks` is the issuer's key set, the document at its `jwks_uri` (for Keycloak,
  `<issuer>/protocol/openid-connect/certs`): `curl -o /etc/storine/jwks.json <jwks_uri>`. The
  file is re-read whenever it changes, so key rotation is a periodic `curl`.
- A token is accepted when its signature verifies (RS256, RS384, RS512 or EdDSA; `none` and
  HMAC are refused), `iss` is the issuer, `aud` includes an `--oidc-audience` (repeatable), and
  `exp` / `nbf` hold (60 s clock-skew allowance).
- The session user (`SUSER_NAME()`, `CURRENT_USER`) is the token's `preferred_username`, else
  `upn`, `email`, `sub`; `--oidc-user-claim NAME` (repeatable) picks other claims.
- A rejected token fails the login with the reason (`token has expired`, `token audience ...`).
  Workflows where the driver asks the server where to sign in
  (`Authentication=ActiveDirectoryPassword` and similar) are not supported; acquire the token in
  the application and pass it.
- Use TLS (`Encrypt=true`, the default for these drivers), since the token is a bearer
  credential.
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

SQL Server (TDS 7.1 - 7.4 and 8.0):

| Language | Drivers | ORMs / query builders |
|---|---|---|
| Python | pymssql (FreeTDS) | SQLAlchemy 2 (`mssql+pymssql`: `create_all`, reflection) |
| Node.js | tedious, mssql | Sequelize (`sync({ alter })`), TypeORM (schema sync), Prisma 6 (`db push`, exact introspection round trip) |
| Go | go-mssqldb | GORM (`sqlserver` driver, MERGE upserts) |
| Java | mssql-jdbc | Hibernate 6 (`SQLServerDialect`, `hbm2ddl` create and validate) |
| .NET | Microsoft.Data.SqlClient | Entity Framework Core (SqlServer provider: batched MERGE inserts, `ExecuteUpdate`, concurrency tokens) |

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
- **T-SQL** (SQL Server endpoint): `TOP`, `OFFSET ... FETCH`, `[bracketed]` names,
  `OUTPUT INSERTED.* / DELETED.* [INTO @t]`, `MERGE`, `UPDATE e ... FROM t AS e` / `DELETE e
  FROM ...`, `DECLARE` (scalars and `TABLE` variables), `IF ... ELSE`, `EXEC` / `EXEC (sql)`,
  `sp_executesql`, `IDENTITY`, `NEXT VALUE FOR`, `ALTER COLUMN`, `ADD [CONSTRAINT] DEFAULT ...
  FOR`, `SET` options (`NOCOUNT`, `IMPLICIT_TRANSACTIONS`, ...), string `+`, and the usual
  built-ins (`ISNULL`, `LEN`, `CHARINDEX`, `DATEADD`, `DATEDIFF`, `DATEPART`, `EOMONTH`,
  `CONVERT`, `TRY_CAST`, `IIF`, `NEWID`, `SCOPE_IDENTITY`, `OBJECT_ID`, `SERVERPROPERTY`, ...).
  Catalogs: `sys.*` (objects, tables, columns, types, indexes, keys, foreign keys, defaults,
  identity columns, sequences, databases, ...), `INFORMATION_SCHEMA`, and the procedures
  drivers call (`sp_tables`, `sp_columns`, `sp_pkeys`, `sp_fkeys`, `sp_statistics`,
  `sp_helpindex`, `sp_rename`). Declared types (`datetime` vs `datetime2`, `varchar` vs
  `nvarchar`) and constraint names are kept, so migration tools see their schema unchanged.
- **Protocol features**: PostgreSQL simple and extended query protocol (named and unnamed
  statements and portals, parameter type inference, text and binary formats including
  `numeric` and arrays), `COPY ... FROM STDIN` / `TO STDOUT` in text, CSV and binary formats
  (psql `\copy`, psycopg `copy()`, asyncpg `copy_records_to_table`), MySQL text protocol and
  `COM_STMT_*` binary prepared statements, multi-statement queries, `LAST_INSERT_ID()`,
  `ROW_COUNT()`, `FOUND_ROWS()`, server status flags; TDS SQL batches, RPC (`sp_executesql`,
`sp_prepare` / `sp_execute` / `sp_prepexec` / `sp_unprepare`) with typed parameters including
`NVARCHAR(MAX)` / `VARBINARY(MAX)`, transaction-manager requests, multiple result sets.

## Transactions and money safety

Every statement runs in a transaction: an explicit one (`BEGIN` ... `COMMIT`) or an implicit
one per statement. Writes are buffered and committed as **one atomic batch**, and every
written row carries the version it had when it was read, so a concurrent change makes the
commit fail instead of being overwritten. **No lost updates.**

- **Autocommit statements** that lose such a race are re-run transparently, so `UPDATE acct SET
  balance = balance - 10 WHERE id = 1` from many connections at once is always exact.
- **Explicit transactions** report SQLSTATE `40001` (MySQL error `1213`, SQL Server error
  `1205`), which drivers and ORMs retry. `SELECT ... FOR UPDATE` takes row locks (honoring `lock_timeout` / `NOWAIT` / `SKIP LOCKED`).
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

- **TLS**: TLS 1.3 and 1.2, ChaCha20-Poly1305 only, Ed25519 certificates only (see above). No
  client-certificate authentication.
- **SQL Server**: SQL authentication and OpenID Connect access tokens. Integrated (Windows /
  Kerberos / NTLM) logins are refused with an error saying so; Kerberos (any realm: MIT,
  Heimdal, FreeIPA, Samba AD, Active Directory) is the planned, platform-neutral addition. One schema
  (`dbo`), no MARS (one active result per connection, as with `MultipleActiveResultSets=false`),
  no stored procedures of your own, cursors or temporary `#tables`; a statement error leaves the
  transaction open (`XACT_ABORT OFF`), as SQL Server does.
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
- An **SQLite** endpoint is not available (SQLite is an in-process library, not a server).

## Development

```text
cargo test -p storage-sql                       # executor and unit tests (embedded store)
cargo build --release -p storage-sql
target/release/storine-sql --data /tmp/db --pg 127.0.0.1:55432 --mysql 127.0.0.1:53306 --mssql 127.0.0.1:51433 &
crates/storage-sql/tests/clients/run_all.sh     # every real-client suite whose toolchain is installed
```
