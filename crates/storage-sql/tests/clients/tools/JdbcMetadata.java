import java.sql.*;
import java.util.*;

/** What DBeaver and Metabase ask a JDBC driver: every metadata call, on each endpoint. */
public class JdbcMetadata {
    static int failures = 0;

    interface Call { ResultSet run() throws SQLException; }

    static int check(String what, Call c) {
        try (ResultSet r = c.run()) {
            int n = 0;
            ResultSetMetaData m = r.getMetaData();
            while (r.next()) {
                for (int i = 1; i <= m.getColumnCount(); i++) r.getObject(i);
                n++;
            }
            System.out.println("ok   " + what + ": " + n + " rows");
            return n;
        } catch (SQLException e) {
            failures++;
            System.out.println("FAIL " + what + ": " + e.getMessage());
            return -1;
        }
    }

    static void expect(String what, boolean ok) {
        if (!ok) {
            failures++;
            System.out.println("FAIL " + what);
        }
    }

    static void run(String url, String user, String pass, String catalog, String schema) throws Exception {
        System.out.println("== " + url);
        try (Connection c = DriverManager.getConnection(url, user, pass)) {
            DatabaseMetaData d = c.getMetaData();
            System.out.println(d.getDatabaseProductName() + " " + d.getDatabaseProductVersion() + " / " + d.getDriverName());
            check("getCatalogs", d::getCatalogs);
            check("getSchemas", d::getSchemas);
            check("getTableTypes", d::getTableTypes);
            check("getTypeInfo", d::getTypeInfo);
            expect("tables include customers", check("getTables", () -> d.getTables(catalog, schema, "%", new String[] {"TABLE"})) >= 2);
            check("getTables views", () -> d.getTables(catalog, schema, "%", new String[] {"VIEW"}));
            expect("columns of customers", check("getColumns", () -> d.getColumns(catalog, schema, "customers", "%")) == 6);
            expect("primary key", check("getPrimaryKeys", () -> d.getPrimaryKeys(catalog, schema, "customers")) == 1);
            expect("imported key", check("getImportedKeys", () -> d.getImportedKeys(catalog, schema, "orders")) == 1);
            // Connector/J's query joins every unique key of the referenced table: customers has two,
            // so MySQL itself answers two rows (`DatabaseMetaDataUsingInfoSchema.getExportedKeys`).
            int exported = check("getExportedKeys", () -> d.getExportedKeys(catalog, schema, "customers"));
            expect("exported key", exported == (url.startsWith("jdbc:mysql") ? 2 : 1));
            check("getCrossReference", () -> d.getCrossReference(catalog, schema, "customers", catalog, schema, "orders"));
            check("getIndexInfo", () -> d.getIndexInfo(catalog, schema, "orders", false, true));
            check("getTablePrivileges", () -> d.getTablePrivileges(catalog, schema, "customers"));
            check("getColumnPrivileges", () -> d.getColumnPrivileges(catalog, schema, "customers", "%"));
            check("getProcedures", () -> d.getProcedures(catalog, schema, "%"));
            check("getFunctions", () -> d.getFunctions(catalog, schema, "%"));
            check("getBestRowIdentifier", () -> d.getBestRowIdentifier(catalog, schema, "customers", DatabaseMetaData.bestRowSession, true));
            check("getVersionColumns", () -> d.getVersionColumns(catalog, schema, "customers"));
            check("getClientInfoProperties", d::getClientInfoProperties);
            // What a data grid does: prepared query, result metadata, parameters.
            try (PreparedStatement p = c.prepareStatement("SELECT c.id, c.name, o.amount, o.ordered_at FROM customers c JOIN orders o ON o.customer_id = c.id WHERE o.amount > ? ORDER BY o.amount")) {
                p.setBigDecimal(1, new java.math.BigDecimal("6"));
                ResultSetMetaData m = p.getMetaData();
                expect("prepared metadata", m == null || m.getColumnCount() == 4);
                try (ResultSet r = p.executeQuery()) {
                    int n = 0;
                    while (r.next()) {
                        r.getInt(1); r.getString(2); r.getBigDecimal(3); r.getTimestamp(4);
                        n++;
                    }
                    expect("prepared rows", n == 4);
                }
                System.out.println("ok   prepared query");
            } catch (SQLException e) {
                failures++;
                System.out.println("FAIL prepared query: " + e.getMessage());
            }
        }
    }

    public static void main(String[] a) throws Exception {
        run("jdbc:postgresql://127.0.0.1:5432/storine", "admin", "secret", null, "public");
        run("jdbc:mysql://127.0.0.1:3306/storine?allowPublicKeyRetrieval=true&useSSL=false", "admin", "secret", "storine", null);
        run("jdbc:sqlserver://127.0.0.1:1433;databaseName=storine;encrypt=false;trustServerCertificate=true", "admin", "secret", "storine", "dbo");
        System.out.println("JDBC failures: " + failures);
        System.exit(failures == 0 ? 0 : 1);
    }
}
