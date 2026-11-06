#!/usr/bin/env bash
# Starts storine-sql and the tools, in one machine with no display (`TASK-150`).
#   STORINE_SQL_BIN  the gateway binary
#   TOOLS_DIR        where to download what is not installed
# See `tools/modal/sweep.py --tools` for the image that holds everything else.
set -u
cd "$(dirname "$0")"
BIN=${STORINE_SQL_BIN:-storine-sql}
TOOLS_DIR=${TOOLS_DIR:-/tmp/tools}
mkdir -p "$TOOLS_DIR"
data=$(mktemp -d)
STORINE_SQL_LOG=1 "$BIN" --data "$data" --pg 127.0.0.1:5432 --mysql 127.0.0.1:3306 --mssql 127.0.0.1:1433 --no-tls --user admin:secret > "$data.log" 2>&1 &
gateway=$!
trap 'kill $gateway 2>/dev/null; kill $(jobs -p) 2>/dev/null' EXIT
for i in $(seq 1 40); do PGPASSWORD=secret psql -h 127.0.0.1 -U admin -d storine -c 'select 1' >/dev/null 2>&1 && break; sleep 0.5; done
PGPASSWORD=secret psql -h 127.0.0.1 -U admin -d storine -v ON_ERROR_STOP=1 -q -f schema.sql || { echo "schema failed"; tail -20 "$data.log"; exit 1; }
status=0

# JDBC: what DBeaver and Metabase ask.
if command -v javac >/dev/null; then
  (cd "$TOOLS_DIR" && for u in org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar com/mysql/mysql-connector-j/9.1.0/mysql-connector-j-9.1.0.jar com/microsoft/sqlserver/mssql-jdbc/12.8.1.jre11/mssql-jdbc-12.8.1.jre11.jar; do [ -f "$(basename "$u")" ] || curl -fsSLO "https://repo1.maven.org/maven2/$u"; done)
  cp JdbcMetadata.java "$TOOLS_DIR/" && (cd "$TOOLS_DIR" && javac JdbcMetadata.java && java -cp ".:*" JdbcMetadata) || status=1
fi

# Grafana, provisioned with the PostgreSQL data source.
if [ ! -d "$TOOLS_DIR/grafana" ]; then
  (cd "$TOOLS_DIR" && curl -fsSL https://dl.grafana.com/oss/release/grafana-11.3.0.linux-amd64.tar.gz | tar xz && mv grafana-* grafana) || echo "note: grafana not downloaded"
fi
if [ -d "$TOOLS_DIR/grafana" ]; then
  mkdir -p "$TOOLS_DIR/grafana/conf/provisioning/datasources"
  cat > "$TOOLS_DIR/grafana/conf/provisioning/datasources/storine.yaml" <<YAML
apiVersion: 1
datasources:
  - name: storine
    uid: storine
    type: postgres
    url: 127.0.0.1:5432
    user: admin
    secureJsonData: { password: secret }
    jsonData: { database: storine, sslmode: disable, postgresVersion: 1600 }
YAML
  (cd "$TOOLS_DIR/grafana" && GF_PATHS_DATA="$TOOLS_DIR/grafana-data" ./bin/grafana server > "$TOOLS_DIR/grafana.log" 2>&1) &
fi

# Metabase.
if command -v java >/dev/null; then
  [ -f "$TOOLS_DIR/metabase.jar" ] || (cd "$TOOLS_DIR" && curl -fsSL -o metabase.jar https://downloads.metabase.com/v0.51.3/metabase.jar) || echo "note: metabase not downloaded"
  if [ -f "$TOOLS_DIR/metabase.jar" ]; then
    (cd "$TOOLS_DIR" && MB_JETTY_PORT=3100 MB_DB_FILE="$TOOLS_DIR/mb" java -Xmx2g -jar metabase.jar > "$TOOLS_DIR/metabase.log" 2>&1) &
  fi
fi

python3 test_tools.py "$@" || status=1
if [ $status -ne 0 ]; then
  echo "--- gateway errors"; grep -a -B1 " ERROR " "$data.log" | cut -c1-2500 | sed -E 's/^\[([a-z]+) [0-9]+/[\1/' | awk '!seen[$0]++' | tail -120
  [ -f "$TOOLS_DIR/metabase.log" ] && { echo "--- metabase log"; grep -iE "error|exception|FAIL" "$TOOLS_DIR/metabase.log" | tail -30 | cut -c1-300; }
  [ -f "$TOOLS_DIR/grafana.log" ] && { echo "--- grafana log"; grep -iE "lvl=eror|error" "$TOOLS_DIR/grafana.log" | tail -10 | cut -c1-300; }
fi
exit $status
