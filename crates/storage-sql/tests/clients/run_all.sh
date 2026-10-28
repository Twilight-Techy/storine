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
BIN=${STORINE_SQL_BIN:-../../../../target/release/storine-sql}

# Login methods beyond passwords get a private storine-sql: OpenID Connect access tokens from
# a test issuer whose signing key is oidc/test-key.pem.
if [ -x "$BIN" ]; then
  authdir=$(mktemp -d)
  "$BIN" --data "$authdir" --no-pg --no-mysql --mssql 127.0.0.1:51633 --user sa:x \
    --oidc-issuer https://id.storine.test/realms/test --oidc-audience storine \
    --oidc-jwks "$PWD/oidc/jwks.json" >"$authdir.log" 2>&1 &
  authpid=$!
  trap 'kill $authpid 2>/dev/null; rm -rf "$authdir" "$authdir.log"' EXIT
  for _ in $(seq 50); do (echo >/dev/tcp/127.0.0.1/51633) 2>/dev/null && break; sleep 0.2; done
  export STORINE_MSSQL_AUTH=127.0.0.1:51633 STORINE_OIDC_KEY="$PWD/oidc/test-key.pem"
fi

run() {
  local name=$1; shift
  echo "== $name"
  if "$@"; then echo "   passed"; else echo "   FAILED"; failed+=("$name"); fi
}
have() { command -v "$1" >/dev/null 2>&1; }

run python "$PYTHON" test_python.py "$PG" "$MY"
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
  "$BIN" --data "$dir" --pg 127.0.0.1:55532 --mysql 127.0.0.1:53406 --mssql 127.0.0.1:51533 >"$dir.log" 2>&1 &
  pid=$!
  for _ in $(seq 50); do (echo >/dev/tcp/127.0.0.1/55532) 2>/dev/null && break; sleep 0.2; done
  run prisma node prisma/test_prisma.js 55532 53406 51533
  kill "$pid"; wait "$pid" 2>/dev/null; rm -rf "$dir" "$dir.log"
fi

if [ ${#failed[@]} -eq 0 ]; then echo "all client suites passed"; else echo "failed: ${failed[*]}"; exit 1; fi
