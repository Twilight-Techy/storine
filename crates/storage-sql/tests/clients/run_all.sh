#!/usr/bin/env bash
# Runs every real-client compatibility suite against a running storine-sql.
#
#   storine-sql --data /tmp/db --pg 127.0.0.1:55432 --mysql 127.0.0.1:53306 &
#   tests/clients/run_all.sh
#
# Suites whose toolchain is missing are skipped with a note. Environment:
#   PYTHON          python with psycopg2, psycopg, asyncpg, sqlalchemy[asyncio], pymysql, django
#   NODE_PATH       node_modules with pg, mysql2, sequelize, knex (and prisma@6, @prisma/client@6)
#   STORINE_SQL_BIN storine-sql binary for the private Prisma instance (default: target/release)
#   ELOQUENT_AUTOLOAD  vendor/autoload.php of `composer require illuminate/database:^11`
#   GEM_HOME/GEM_PATH  with activerecord, pg, trilogy
set -u
cd "$(dirname "$0")"
PG=${STORINE_PG_PORT:-55432}
MY=${STORINE_MYSQL_PORT:-53306}
export STORINE_PG="127.0.0.1:$PG" STORINE_MYSQL="127.0.0.1:$MY"
PYTHON=${PYTHON:-python3}
failed=()

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
have go && run go bash -c "cd go && go test -count=1 ./..."
have mvn && run java bash -c "cd java && mvn -q test"
have dotnet && run dotnet bash -c "cd dotnet && dotnet test --nologo -v q"
have php && run php php php/test_php.php "$PG" "$MY"
if have php && [ -n "${ELOQUENT_AUTOLOAD:-}" ]; then run eloquent php php/test_eloquent.php "$ELOQUENT_AUTOLOAD" "$PG" "$MY"; fi
have ruby && run activerecord ruby ruby/test_activerecord.rb "$PG" "$MY"

# Prisma's `db push` owns the whole database, so it gets a private, empty storine-sql.
BIN=${STORINE_SQL_BIN:-../../../../target/release/storine-sql}
if have node && [ -x "$BIN" ] && node -e 'require.resolve("prisma/package.json")' 2>/dev/null; then
  dir=$(mktemp -d)
  "$BIN" --data "$dir" --pg 127.0.0.1:55532 --mysql 127.0.0.1:53406 >"$dir.log" 2>&1 &
  pid=$!
  for _ in $(seq 50); do (echo >/dev/tcp/127.0.0.1/55532) 2>/dev/null && break; sleep 0.2; done
  run prisma node prisma/test_prisma.js 55532 53406
  kill "$pid"; wait "$pid" 2>/dev/null; rm -rf "$dir" "$dir.log"
fi

if [ ${#failed[@]} -eq 0 ]; then echo "all client suites passed"; else echo "failed: ${failed[*]}"; exit 1; fi
