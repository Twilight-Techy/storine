#!/usr/bin/env bash
# Runs every real-client compatibility suite against a running storine-sql.
#
#   storine-sql --data /tmp/db --pg 127.0.0.1:55432 --mysql 127.0.0.1:53306 --mssql 127.0.0.1:51433 &
#   tests/clients/run_all.sh
#
# Suites whose toolchain is missing are skipped with a note. Environment:
#   PYTHON          python with psycopg2, psycopg, asyncpg, sqlalchemy[asyncio], pymysql, django
#                   (and pymssql for the SQL Server suite)
#   NODE_PATH       node_modules with pg, mysql2, sequelize, knex (and prisma@6, @prisma/client@6,
#                   typeorm, tedious, mssql)
#   STORINE_SQL_BIN storine-sql binary for the private Prisma and login-method instances
#                   (default: target/release)
#   STORINE_DATA    the server's --data directory (its TLS certificate is then verified)
#   ELOQUENT_AUTOLOAD  vendor/autoload.php of `composer require illuminate/database:^11`
#   GEM_HOME/GEM_PATH  with activerecord, pg, trilogy (GEM_PATH also listing the default gems)
# The PgBouncer suite runs when `pgbouncer` is installed (it starts its own instance).
set -u
cd "$(dirname "$0")"
PG=${STORINE_PG_PORT:-55432}
MY=${STORINE_MYSQL_PORT:-53306}
MS=${STORINE_MSSQL_PORT:-51433}
export STORINE_PG="127.0.0.1:$PG" STORINE_MYSQL="127.0.0.1:$MY" STORINE_MSSQL="127.0.0.1:$MS"
PYTHON=${PYTHON:-python3}
# The server's certificate, for the suites that verify it (storine-sql --data DIR keeps it in DIR/tls).
[ -n "${STORINE_DATA:-}" ] && [ -f "$STORINE_DATA/tls/server.crt" ] && export STORINE_TLS_CERT="$STORINE_DATA/tls/server.crt"
failed=()
have() { command -v "$1" >/dev/null 2>&1; }
BIN=${STORINE_SQL_BIN:-../../../../target/release/storine-sql}

# Private instances listen above Linux's ephemeral port range (32768-60999), where no client
# connection can be holding the port. Login methods beyond passwords get private instances: OpenID Connect access
# tokens from a test issuer whose signing key is oidc/test-key.pem, and Kerberos (when MIT
# Kerberos' krb5kdc, kdb5_util and kinit are installed).
if [ -x "$BIN" ]; then
  authdir=$(mktemp -d)
  "$BIN" --data "$authdir" --no-pg --no-mysql --mssql 127.0.0.1:61633 --user sa:x \
    --oidc-issuer https://id.storine.test/realms/test --oidc-audience storine \
    --oidc-jwks "$PWD/oidc/jwks.json" >"$authdir.log" 2>&1 &
  authpid=$!
  for _ in $(seq 50); do (echo >/dev/tcp/127.0.0.1/61633) 2>/dev/null && break; sleep 0.2; done
  export STORINE_MSSQL_AUTH=127.0.0.1:61633 STORINE_OIDC_KEY="$PWD/oidc/test-key.pem"
fi
# Kerberos: a throwaway MIT realm (kerberos/realm.sh) and a storine-sql with its service keytab.
if [ -x "$BIN" ] && have krb5kdc && have kdb5_util && have kinit; then
  krbdir=$(mktemp -d)
  if kerberos/realm.sh "$krbdir/realm" 18888 61733 >"$krbdir/realm.log" 2>&1; then
    . "$krbdir/realm/env"
    "$BIN" --data "$krbdir/db" --no-pg --no-mysql --mssql 127.0.0.1:61733 --user sa:x \
      --kerberos-keytab "$STORINE_KRB5_KEYTAB" >"$krbdir/server.log" 2>&1 &
    krbpid=$!
    for _ in $(seq 50); do (echo >/dev/tcp/127.0.0.1/61733) 2>/dev/null && break; sleep 0.2; done
    export STORINE_MSSQL_KRB5=127.0.0.1:61733
  else
    echo "   (Kerberos realm setup failed: see $krbdir/realm.log; Kerberos tests skipped)"
  fi
fi
cleanup() {
  [ -n "${authpid:-}" ] && kill "$authpid" 2>/dev/null && rm -rf "$authdir" "$authdir.log"
  [ -n "${krbpid:-}" ] && kill "$krbpid" 2>/dev/null
  [ -f "${krbdir:-/nonexistent}/realm/kdc.pid" ] && kill "$(cat "$krbdir/realm/kdc.pid")" 2>/dev/null
  [ -n "${krbdir:-}" ] && rm -rf "$krbdir"
}
trap cleanup EXIT

run() {
  local name=$1; shift
  echo "== $name"
  if "$@"; then echo "   passed"; else echo "   FAILED"; failed+=("$name"); fi
}

run python "$PYTHON" test_python.py "$PG" "$MY"
have pgbouncer && run pgbouncer pgbouncer/run.sh "$PYTHON" "$PG"
run django-postgres "$PYTHON" django/test_django.py postgres "$PG"
run django-mysql "$PYTHON" django/test_django.py mysql "$MY"
have node && run node node test_node.js "$PG" "$MY"
have node && node -e 'require.resolve("typeorm")' 2>/dev/null && run typeorm-drizzle node test_typeorm_drizzle.js "$PG" "$MY"
"$PYTHON" -c 'import pymssql' 2>/dev/null && run mssql-python "$PYTHON" test_mssql.py "$MS"
have node && node -e 'require.resolve("tedious")' 2>/dev/null && run mssql-node node test_mssql.js "$MS"
have go && run go bash -c "cd go && go test -count=1 ./..."
have mvn && run java bash -c "cd java && mvn -q test"
# SqlClient trusts the server certificate (TDS 8 strict mode) through OpenSSL's store: add it
# to the usual CA bundle for the run.
dotnet_tests() (
  cd dotnet || exit 1
  if [ -n "${STORINE_TLS_CERT:-}" ]; then
    bundle=$(mktemp)
    cat "${SSL_CERT_FILE:-/etc/ssl/certs/ca-certificates.crt}" "$STORINE_TLS_CERT" >"$bundle" 2>/dev/null
    SSL_CERT_FILE=$bundle dotnet test --nologo -v q; rc=$?
    rm -f "$bundle"; exit $rc
  fi
  dotnet test --nologo -v q
)
have dotnet && run dotnet dotnet_tests
have php && run php php php/test_php.php "$PG" "$MY"
if have php && [ -n "${ELOQUENT_AUTOLOAD:-}" ]; then run eloquent php php/test_eloquent.php "$ELOQUENT_AUTOLOAD" "$PG" "$MY"; fi
have ruby && run activerecord ruby ruby/test_activerecord.rb "$PG" "$MY"

# Prisma's `db push` owns the whole database, so it gets a private, empty storine-sql.
if have node && [ -x "$BIN" ] && node -e 'require.resolve("prisma/package.json")' 2>/dev/null; then
  dir=$(mktemp -d)
  "$BIN" --data "$dir" --pg 127.0.0.1:61532 --mysql 127.0.0.1:61406 --mssql 127.0.0.1:61533 >"$dir.log" 2>&1 &
  pid=$!
  for _ in $(seq 50); do (echo >/dev/tcp/127.0.0.1/61532) 2>/dev/null && break; sleep 0.2; done
  run prisma node prisma/test_prisma.js 61532 61406 61533
  kill "$pid"; wait "$pid" 2>/dev/null; rm -rf "$dir" "$dir.log"
fi

if [ ${#failed[@]} -eq 0 ]; then echo "all client suites passed"; else echo "failed: ${failed[*]}"; exit 1; fi
