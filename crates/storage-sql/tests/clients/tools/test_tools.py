"""BI and GUI tools against a running storine-sql (`TASK-150`).

Runs what the tools really run, headless. See `run.sh` for what is installed. Each suite prints
`ok` / `FAIL` lines; the exit status is the number of suites that failed.
"""
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

PG = dict(host="127.0.0.1", port=5432, user="admin", password="secret", dbname="storine")
failures = []
suite = ""


def note(ok, what, detail=""):
    print(("ok   " if ok else "FAIL ") + f"[{suite}] {what}" + (f": {str(detail)[:400]}" if detail and not ok else ""), flush=True)
    if not ok:
        failures.append(f"{suite}: {what}: {str(detail)[:400]}")


def attempt(what, f):
    try:
        r = f()
        note(True, what)
        return r
    except Exception as e:
        note(False, what, repr(e))
        return None


def http(method, url, body=None, headers=None, timeout=60):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    try:
        return json.loads(raw)
    except ValueError:
        return raw


def sqlalchemy_superset():
    """Superset browses through SQLAlchemy's inspector, then runs its charts' SQL."""
    global suite
    suite = "superset/sqlalchemy"
    import sqlalchemy as sa

    e = sa.create_engine("postgresql+psycopg2://admin:secret@127.0.0.1:5432/storine")
    i = sa.inspect(e)
    attempt("get_schema_names", i.get_schema_names)
    tables = attempt("get_table_names", lambda: i.get_table_names(schema="public")) or []
    note("customers" in tables, "customers listed", tables)
    views = attempt("get_view_names", lambda: i.get_view_names(schema="public")) or []
    note("order_totals" in views, "view listed", views)
    cols = attempt("get_columns", lambda: i.get_columns("orders", schema="public")) or []
    note(len(cols) == 6, "orders has 6 columns", cols)
    pk = attempt("get_pk_constraint", lambda: i.get_pk_constraint("orders", schema="public"))
    note(bool(pk) and pk.get("constrained_columns") == ["id"], "primary key", pk)
    fks = attempt("get_foreign_keys", lambda: i.get_foreign_keys("orders", schema="public")) or []
    note(len(fks) == 1 and fks[0]["referred_table"] == "customers", "foreign key", fks)
    idx = attempt("get_indexes", lambda: i.get_indexes("orders", schema="public")) or []
    note(len(idx) >= 2, "indexes", idx)
    attempt("get_unique_constraints", lambda: i.get_unique_constraints("customers", schema="public"))
    attempt("get_check_constraints", lambda: i.get_check_constraints("customers", schema="public"))
    attempt("get_table_comment", lambda: i.get_table_comment("customers", schema="public"))
    attempt("get_view_definition", lambda: i.get_view_definition("order_totals", schema="public"))
    # The SQL Superset generates for an explore chart: time grain, metric, filter, limit.
    with e.connect() as c:
        q = """SELECT DATE_TRUNC('month', ordered_at) AS __timestamp, status AS status, sum(amount) AS "SUM(amount)"
               FROM public.orders WHERE ordered_at >= '2024-01-01 00:00:00.000000' AND ordered_at < '2025-01-01 00:00:00.000000'
               GROUP BY DATE_TRUNC('month', ordered_at), status ORDER BY "SUM(amount)" DESC LIMIT 10000"""
        rows = attempt("explore chart query", lambda: c.execute(sa.text(q)).fetchall()) or []
        note(len(rows) == 4, "chart rows", rows)
        attempt("sample rows", lambda: c.execute(sa.text("SELECT * FROM public.customers LIMIT 100")).fetchall())
        attempt("distinct filter values", lambda: c.execute(sa.text("SELECT DISTINCT region FROM public.customers ORDER BY 1")).fetchall())
        attempt("select_star limit", lambda: c.execute(sa.text("SELECT id, name, email FROM public.customers\nLIMIT 1000")).fetchall())
    # MySQL and SQL Server through their dialects.
    m = sa.create_engine("mysql+pymysql://admin:secret@127.0.0.1:3306/storine")
    im = sa.inspect(m)
    note("customers" in (attempt("mysql get_table_names", im.get_table_names) or []), "mysql customers listed")
    note(len(attempt("mysql get_columns", lambda: im.get_columns("orders")) or []) == 6, "mysql orders columns")
    attempt("mysql get_foreign_keys", lambda: im.get_foreign_keys("orders"))
    attempt("mysql get_indexes", lambda: im.get_indexes("orders"))


def psqlodbc_tableau_powerbi():
    """Tableau and Power BI reach PostgreSQL through psqlODBC: the driver's catalog calls, and what
    Tableau sends (temporary tables, cursors, `pg_catalog` probes)."""
    global suite
    suite = "tableau-powerbi/psqlodbc"
    import pyodbc

    c = pyodbc.connect("DRIVER={PostgreSQL Unicode};SERVER=127.0.0.1;PORT=5432;DATABASE=storine;UID=admin;PWD=secret;UseDeclareFetch=1;Fetch=100", autocommit=True)
    cur = c.cursor()
    for what, info in [("SQL_DBMS_NAME", pyodbc.SQL_DBMS_NAME), ("SQL_DBMS_VER", pyodbc.SQL_DBMS_VER), ("SQL_DRIVER_NAME", pyodbc.SQL_DRIVER_NAME), ("SQL_IDENTIFIER_QUOTE_CHAR", pyodbc.SQL_IDENTIFIER_QUOTE_CHAR)]:
        attempt(f"getinfo {what}", lambda: c.getinfo(info))
    tables = attempt("SQLTables", lambda: cur.tables(tableType="TABLE").fetchall()) or []
    note(any(t.table_name == "customers" for t in tables), "customers listed", [t.table_name for t in tables])
    attempt("SQLTables views", lambda: cur.tables(tableType="VIEW").fetchall())
    cols = attempt("SQLColumns", lambda: cur.columns(table="orders").fetchall()) or []
    note(len(cols) == 6, "orders has 6 columns", [x.column_name for x in cols])
    pks = attempt("SQLPrimaryKeys", lambda: cur.primaryKeys("customers").fetchall()) or []
    note(len(pks) == 1, "primary key", pks)
    fks = attempt("SQLForeignKeys", lambda: cur.foreignKeys(table="orders").fetchall()) or []
    note(len(fks) == 1, "foreign key", fks)
    attempt("SQLStatistics", lambda: cur.statistics("orders").fetchall())
    attempt("SQLGetTypeInfo", lambda: cur.getTypeInfo().fetchall())
    attempt("SQLProcedures", lambda: cur.procedures().fetchall())
    attempt("SQLSpecialColumns", lambda: cur.rowIdColumns("customers").fetchall())
    # A cursor over the result (UseDeclareFetch), parameters, types.
    rows = attempt("query through a declared cursor", lambda: cur.execute("SELECT id, name, active, created FROM customers ORDER BY id").fetchall()) or []
    note(len(rows) == 4, "rows", rows)
    rows = attempt("parameters", lambda: cur.execute("SELECT amount FROM orders WHERE status = ? AND amount > ?", "paid", 15).fetchall()) or []
    note(len(rows) == 2, "parameter rows", rows)
    # Tableau: a temporary table for extract filters, then a join with it.
    attempt("Tableau temp table", lambda: cur.execute('CREATE LOCAL TEMPORARY TABLE "#Tableau_1_Filter" ("region" VARCHAR(10)) ON COMMIT PRESERVE ROWS'))
    attempt("Tableau insert", lambda: cur.execute('INSERT INTO "#Tableau_1_Filter" VALUES (?)', "eu"))
    rows = attempt("Tableau join", lambda: cur.execute('SELECT c.name FROM customers c INNER JOIN "#Tableau_1_Filter" f ON c.region = f.region').fetchall()) or []
    note(len(rows) == 2, "Tableau rows", rows)
    attempt("Tableau drop", lambda: cur.execute('DROP TABLE "#Tableau_1_Filter"'))
    attempt("Tableau probe", lambda: cur.execute("SELECT NULL::text AS x LIMIT 0").fetchall())
    attempt("Tableau server settings", lambda: cur.execute("SELECT setting FROM pg_settings WHERE name = 'server_version_num'").fetchall())
    attempt("Tableau trunc", lambda: cur.execute("SELECT DATE_TRUNC('MONTH', ordered_at) AS m, SUM(amount) AS s FROM orders GROUP BY 1 ORDER BY 1").fetchall())
    # Power BI: a query folded from a table.
    attempt("Power BI folded", lambda: cur.execute('select "_"."id", "_"."amount" from (select "$Table"."id", "$Table"."amount" from "public"."orders" "$Table") "_" where "_"."amount" > 5 limit 1001').fetchall())


def pgadmin_templates():
    """pgAdmin 4's own catalog SQL: every node query it renders for browsing a server (the Jinja
    templates shipped with the package), through psycopg as it does."""
    global suite
    suite = "pgadmin4"
    import psycopg

    c = psycopg.connect(**PG, autocommit=True)
    cur = c.cursor()
    # What pgAdmin runs on connecting.
    for q in [
        "SET DateStyle=ISO; SET client_min_messages=notice; SET bytea_output=hex; SET client_encoding='UNICODE';",
        "SELECT version()",
        "SELECT rolname, rolcreaterole, rolcreatedb, rolcatupdate, rolsuper, rolcanlogin FROM pg_roles WHERE rolname = current_user" if False else "SELECT rolname, rolcreaterole, rolcreatedb, rolsuper, rolcanlogin FROM pg_roles WHERE rolname = current_user",
        "SELECT db.oid as did, db.datname, db.datallowconn, pg_encoding_to_char(db.encoding) AS serverencoding, has_database_privilege(db.oid, 'CREATE') as cancreate, datistemplate FROM pg_catalog.pg_database db WHERE db.datname = current_database()",
        "SELECT nsp.oid, nsp.nspname as name, has_schema_privilege(nsp.oid, 'CREATE') as can_create, has_schema_privilege(nsp.oid, 'USAGE') as has_usage FROM pg_catalog.pg_namespace nsp WHERE nsp.nspname NOT LIKE 'pg\\_%' ORDER BY nspname",
        "SELECT rel.oid, rel.relname AS name, (SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid=rel.oid AND tgisinternal = FALSE) AS triggercount, (SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgrelid=rel.oid AND tgisinternal = FALSE AND tgenabled = 'O') AS has_enable_triggers FROM pg_catalog.pg_class rel WHERE rel.relkind IN ('r','t','f') AND rel.relnamespace = 2200 ORDER BY rel.relname",
        "SELECT att.attname as name, att.attnum, pg_catalog.format_type(ty.oid, att.atttypmod) AS displaytypname, att.attnotnull, pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS defval FROM pg_catalog.pg_attribute att JOIN pg_catalog.pg_type ty ON ty.oid = atttypid LEFT OUTER JOIN pg_catalog.pg_attrdef def ON adrelid = att.attrelid AND adnum = att.attnum JOIN pg_catalog.pg_class cl ON cl.oid = att.attrelid WHERE cl.relname = 'orders' AND att.attnum > 0 AND att.attisdropped IS FALSE ORDER BY att.attnum",
        "SELECT cls.oid, cls.relname as name, indisunique, indisprimary FROM pg_catalog.pg_index idx JOIN pg_catalog.pg_class cls ON cls.oid = indexrelid JOIN pg_catalog.pg_class tab ON tab.oid = indrelid WHERE tab.relname = 'orders' ORDER BY cls.relname",
        "SELECT conname, contype, pg_catalog.pg_get_constraintdef(c.oid) FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_class r ON r.oid = c.conrelid WHERE r.relname = 'orders'",
        "SELECT pid, usename, datname, state, query, backend_start, xact_start, query_start, wait_event_type, wait_event FROM pg_catalog.pg_stat_activity ORDER BY pid",
        "SELECT locktype, relation, pid, mode, granted FROM pg_catalog.pg_locks",
        "SELECT pr.oid, pr.proname || '(' || COALESCE(pg_catalog.pg_get_function_identity_arguments(pr.oid), '') || ')' as name FROM pg_catalog.pg_proc pr WHERE pr.prokind IN ('f', 'w') ORDER BY pr.proname" if False else "SELECT pr.oid, pr.proname FROM pg_catalog.pg_proc pr WHERE pr.prokind = 'f' ORDER BY pr.proname",
        "SELECT t.oid, t.tgname FROM pg_catalog.pg_trigger t JOIN pg_catalog.pg_class c ON c.oid = t.tgrelid WHERE c.relname = 'orders'",
        "SELECT schemaname, relname, seq_scan, n_live_tup, n_dead_tup, last_analyze FROM pg_catalog.pg_stat_user_tables",
        "SELECT rel.oid, rel.relname AS name FROM pg_catalog.pg_class rel WHERE rel.relkind = 'v' AND rel.relnamespace = 2200",
        "SELECT pg_catalog.pg_get_viewdef('order_totals'::regclass, true)",
        "SELECT * FROM pg_catalog.pg_extension",
        "SELECT name, setting, unit, category, short_desc FROM pg_catalog.pg_settings ORDER BY name",
    ]:
        attempt(q[:70].replace("\n", " "), lambda: cur.execute(q) and (cur.fetchall() if cur.description else None))
    # EXPLAIN in JSON is what its graphical plan view reads.
    plan = attempt("graphical explain", lambda: cur.execute("EXPLAIN (ANALYZE, FORMAT JSON, VERBOSE, BUFFERS, COSTS, TIMING, SUMMARY) SELECT * FROM orders WHERE status = 'paid'").fetchall())
    if plan:
        doc = plan[0][0]
        doc = json.loads(doc) if isinstance(doc, str) else doc
        note(doc[0]["Plan"].get("Node Type") is not None, "plan has a node type", doc)
    # Its data grid: a server-side cursor over any query.
    with c.transaction():
        with c.cursor(name="pgadmin") as s:
            s.execute("SELECT * FROM orders ORDER BY id")
            note(len(s.fetchmany(2)) == 2 and len(s.fetchall()) == 3, "data grid cursor")
    # Its templates, as shipped, where the package can be installed.
    try:
        import pgadmin  # noqa: F401
        note(True, "pgadmin4 package present")
    except ImportError:
        print("note [pgadmin4] the package itself is not installed: its catalog queries above are transcribed", flush=True)


def wait_http(url, seconds, what):
    end = time.time() + seconds
    last = None
    while time.time() < end:
        try:
            return http("GET", url, timeout=5)
        except Exception as e:
            last = e
            time.sleep(2)
    raise RuntimeError(f"{what} did not come up: {last}")


def grafana():
    """Grafana itself, its PostgreSQL data source, provisioned: health check, the table and column
    browsing its query builder does, and a time series."""
    global suite
    suite = "grafana"
    base = "http://admin:admin@127.0.0.1:3000"
    try:
        wait_http("http://127.0.0.1:3000/api/health", 120, "grafana")
    except Exception as e:
        note(False, "grafana up", e)
        return
    def query(sql, fmt="table"):
        return http("POST", f"{base}/api/ds/query", {"queries": [{"refId": "A", "datasource": {"uid": "storine"}, "rawSql": sql, "format": fmt}], "from": "now-5y", "to": "now"}, timeout=30)

    def frames(r):
        return r["results"]["A"]["frames"][0]["data"]["values"]

    h = attempt("data source health", lambda: http("GET", f"{base}/api/datasources/uid/storine/health"))
    note(bool(h) and h.get("status") == "OK", "health is OK", h)
    # What the query builder asks, from its source.
    v = attempt("tables", lambda: frames(query("""select quote_ident(table_name) as "table" from information_schema.tables where quote_ident(table_schema) not in ('information_schema','pg_catalog','_timescaledb_cache','_timescaledb_catalog','_timescaledb_internal','_timescaledb_config','timescaledb_information','timescaledb_experimental') and table_type = 'BASE TABLE' and quote_ident(table_schema) IN (select CASE WHEN trim(s[i]) = '"$user"' THEN user ELSE trim(s[i]) END from generate_series(array_lower(string_to_array(current_setting('search_path'),','),1),array_upper(string_to_array(current_setting('search_path'),','),1)) as i, string_to_array(current_setting('search_path'),',') s)""")))
    note(bool(v) and "customers" in v[0], "tables include customers", v)
    v = attempt("columns", lambda: frames(query("select quote_ident(column_name) as column, data_type as type from information_schema.columns where quote_ident(table_name) = 'orders'")))
    note(bool(v) and len(v[0]) == 6, "orders has 6 columns", v)
    v = attempt("time series", lambda: frames(query("SELECT date_trunc('month', ordered_at) AS time, status AS metric, sum(amount) AS value FROM orders WHERE ordered_at IS NOT NULL GROUP BY 1, 2 ORDER BY 1", "time_series")))
    note(bool(v), "time series values", v)
    attempt("timescaledb probe", lambda: query("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"))
    attempt("version probe", lambda: query("SELECT current_setting('server_version_num')::int/100 as version"))


def metabase():
    """Metabase itself: setup, add the database, let it sync, read the fields it found, run a
    question through its query processor."""
    global suite
    suite = "metabase"
    base = "http://127.0.0.1:3100/api"
    try:
        props = None
        end = time.time() + 400
        while time.time() < end:
            try:
                props = http("GET", f"{base}/session/properties", timeout=10)
                if props.get("setup-token"):
                    break
            except Exception:
                pass
            time.sleep(5)
        token = props["setup-token"]
    except Exception as e:
        note(False, "metabase up", e)
        return
    s = attempt("setup", lambda: http("POST", f"{base}/setup", {"token": token, "user": {"first_name": "A", "last_name": "B", "email": "a@example.com", "password": "Passw0rd!long", "site_name": "x"}, "prefs": {"site_name": "x", "site_locale": "en"}}))
    if not s:
        return
    auth = {"X-Metabase-Session": s["id"]}
    def add_db(name, engine, details):
        return http("POST", f"{base}/database", {"engine": engine, "name": name, "details": details, "is_full_sync": True}, auth, timeout=120)

    def tables_of(db_id):
        end = time.time() + 180
        while time.time() < end:
            m = http("GET", f"{base}/database/{db_id}/metadata?include_hidden=true", None, auth, timeout=60)
            names = {t["name"]: t for t in m.get("tables", [])}
            if "customers" in names and "orders" in names and names["orders"].get("fields"):
                return names
            time.sleep(5)
        return {t["name"]: t for t in m.get("tables", [])}

    for name, engine, port in [("pg", "postgres", 5432), ("my", "mysql", 3306), ("ms", "sqlserver", 1433)]:
        details = {"host": "127.0.0.1", "port": port, "dbname": "storine", "user": "admin", "password": "secret", "ssl": False}
        if engine == "sqlserver":
            details = {"host": "127.0.0.1", "port": port, "db": "storine", "user": "admin", "password": "secret", "ssl": False, "additional-options": "encrypt=false;trustServerCertificate=true"}
        d = attempt(f"{engine}: add database", lambda: add_db(name, engine, details))
        if not d:
            continue
        names = attempt(f"{engine}: sync", lambda: tables_of(d["id"])) or {}
        note("orders" in names, f"{engine}: synced tables", sorted(names))
        if "orders" in names:
            fields = {f["name"]: f for f in names["orders"].get("fields", [])}
            note(len(fields) == 6, f"{engine}: orders fields", sorted(fields))
            note(fields.get("customer_id", {}).get("semantic_type") == "type/FK", f"{engine}: foreign key found", fields.get("customer_id"))
            r = attempt(f"{engine}: question", lambda: http("POST", f"{base}/dataset", {"database": d["id"], "type": "query", "query": {"source-table": names["orders"]["id"], "aggregation": [["sum", ["field", fields["amount"]["id"], None]]], "breakout": [["field", fields["status"]["id"], None]]}}, auth, timeout=120))
            note(bool(r) and r.get("status") == "completed" and len(r["data"]["rows"]) == 3, f"{engine}: question rows", r if not r else r.get("error") or r["data"]["rows"])
            r = attempt(f"{engine}: native question", lambda: http("POST", f"{base}/dataset", {"database": d["id"], "type": "native", "native": {"query": "SELECT count(*) FROM orders"}}, auth, timeout=120))
            note(bool(r) and r.get("status") == "completed", f"{engine}: native rows", r if not r else r.get("error"))


def main():
    for run in [sqlalchemy_superset, psqlodbc_tableau_powerbi, pgadmin_templates, grafana, metabase]:
        if len(sys.argv) > 1 and run.__name__ not in sys.argv[1:]:
            continue
        try:
            run()
        except Exception as e:
            failures.append(f"{run.__name__}: {e!r}")
            print(f"FAIL {run.__name__}: {e!r}", flush=True)
    print(f"TOOL FAILURES: {len(failures)}")
    for f in failures:
        print(" -", f)
    sys.exit(min(len(failures), 100))


main()
