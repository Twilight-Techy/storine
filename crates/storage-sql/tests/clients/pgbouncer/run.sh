#!/usr/bin/env bash
# Runs test_pgbouncer.py through a private PgBouncer in front of a running storine-sql.
#
#   pgbouncer/run.sh PYTHON PG_PORT
#
# PgBouncer gets two database aliases for the same storine-sql database: `shop_session`
# (pool_mode=session) and `shop_tx` (pool_mode=transaction, with protocol-level prepared
# statements tracked by PgBouncer: max_prepared_statements), plus `shop_one`, a session pool
# of one server connection, so consecutive clients are known to share it. SCRAM-SHA-256 on
# both legs.
set -eu
PYTHON=$1 PG=$2 PORT=${STORINE_PGBOUNCER_PORT:-61632}
dir=$(mktemp -d)
cat >"$dir/pgbouncer.ini" <<INI
[databases]
shop_session = host=127.0.0.1 port=$PG dbname=shop pool_mode=session
shop_tx = host=127.0.0.1 port=$PG dbname=shop pool_mode=transaction
shop_one = host=127.0.0.1 port=$PG dbname=shop pool_mode=session pool_size=1
[pgbouncer]
listen_addr = 127.0.0.1
listen_port = $PORT
unix_socket_dir =
auth_type = scram-sha-256
auth_file = $dir/userlist.txt
admin_users = app
default_pool_size = 4
max_client_conn = 500
max_prepared_statements = 200
logfile = $dir/pgbouncer.log
pidfile = $dir/pgbouncer.pid
INI
echo '"app" "x"' >"$dir/userlist.txt"
chmod 755 "$dir"; chmod 644 "$dir"/*
# PgBouncer refuses to run as root.
as=()
if [ "$(id -u)" = 0 ]; then
  user=$(id -un postgres 2>/dev/null || echo nobody)
  chown "$user" "$dir"
  as=(runuser -u "$user" --)
fi
"${as[@]}" pgbouncer -d "$dir/pgbouncer.ini"
for _ in $(seq 50); do (echo >/dev/tcp/127.0.0.1/"$PORT") 2>/dev/null && break; sleep 0.1; done
trap 'kill "$(cat "$dir/pgbouncer.pid")" 2>/dev/null; sleep 0.2; rm -rf "$dir"' EXIT
rc=0
"$PYTHON" "$(dirname "$0")/test_pgbouncer.py" "$PORT" || rc=$?
# Any error PgBouncer logged about the server side is a failure too.
if grep -E "ERROR|FATAL|WARNING" "$dir/pgbouncer.log" | grep -v "closing because: client" ; then
  echo "pgbouncer logged errors (above)"; rc=1
fi
exit $rc
