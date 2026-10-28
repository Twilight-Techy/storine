package dev.storine;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

/** Microsoft's JDBC driver (pure Java TDS) against the SQL Server endpoint. */
class MssqlTest {
    static String url() {
        return url("encrypt=false"); // login-only encryption
    }

    static String url(String encryption) {
        String a = System.getenv().getOrDefault("STORINE_MSSQL", "127.0.0.1:51433");
        return "jdbc:sqlserver://" + a + ";databaseName=shop;user=sa;password=x;" + encryption;
    }

    /** Integrated (Windows) authentication is refused with a clear message, not a hang. */
    @Test
    void integratedAuthenticationIsRefusedClearly() {
        String a = System.getenv().getOrDefault("STORINE_MSSQL", "127.0.0.1:51433");
        String url = "jdbc:sqlserver://" + a + ";databaseName=shop;encrypt=false;loginTimeout=10;"
            + "integratedSecurity=true;authenticationScheme=NTLM;domain=EXAMPLE;user=ann;password=x";
        SQLException e = assertThrows(SQLException.class, () -> DriverManager.getConnection(url).close());
        assertTrue(e.getMessage().contains("use SQL Server authentication"), e.getMessage());
    }

    /**
     * Kerberos (integrated security with authenticationScheme=JavaKerberos) against a server
     * started with --kerberos-keytab: STORINE_MSSQL_KRB5 names it, STORINE_KRB5_DIR the test
     * realm made by kerberos/realm.sh (krb5.conf, and ann's ticket cache from kinit).
     */
    @Test
    void kerberosLogin() throws Exception {
        String addr = System.getenv("STORINE_MSSQL_KRB5"), dir = System.getenv("STORINE_KRB5_DIR");
        if (addr == null || dir == null) return;
        System.setProperty("java.security.krb5.conf", dir + "/krb5.conf");
        String url = "jdbc:sqlserver://" + addr.replace("127.0.0.1", "localhost")
            + ";databaseName=shop;encrypt=true;trustServerCertificate=true;loginTimeout=10;"
            + "integratedSecurity=true;authenticationScheme=JavaKerberos";
        try (Connection c = DriverManager.getConnection(url);
             ResultSet rs = c.createStatement().executeQuery("SELECT SUSER_NAME()")) {
            assertTrue(rs.next());
            assertEquals("ann", rs.getString(1));
        }
        // A ticket for a service whose key the server does not hold is refused with the reason.
        int port = Integer.parseInt(addr.substring(addr.lastIndexOf(':') + 1));
        SQLException e = assertThrows(SQLException.class, () -> DriverManager.getConnection(
            url + ";serverSpn=MSSQLSvc/other.storine.test:" + port + "@STORINE.TEST").close());
        assertTrue(e.getMessage().contains("no key for MSSQLSvc/other.storine.test"), e.getMessage());
    }

    /** A JWT signed with the test issuer's key (tests/clients/oidc/test-key.pem). */
    static String mintToken(String keyFile, String extraClaims) throws Exception {
        java.util.Base64.Encoder b64 = java.util.Base64.getUrlEncoder().withoutPadding();
        long exp = System.currentTimeMillis() / 1000 + 300;
        String head = b64.encodeToString("{\"alg\":\"RS256\",\"kid\":\"storine-test\",\"typ\":\"JWT\"}".getBytes());
        String body = b64.encodeToString(("{\"iss\":\"https://id.storine.test/realms/test\",\"aud\":\"storine\",\"exp\":"
            + exp + ",\"sub\":\"u-1\"" + extraClaims + "}").getBytes());
        String pem = java.nio.file.Files.readString(java.nio.file.Path.of(keyFile))
            .replaceAll("-----[A-Z ]+-----", "").replaceAll("\\s", "");
        java.security.PrivateKey key = java.security.KeyFactory.getInstance("RSA")
            .generatePrivate(new java.security.spec.PKCS8EncodedKeySpec(java.util.Base64.getDecoder().decode(pem)));
        java.security.Signature sig = java.security.Signature.getInstance("SHA256withRSA");
        sig.initSign(key);
        sig.update((head + "." + body).getBytes());
        return head + "." + body + "." + b64.encodeToString(sig.sign());
    }

    static Connection tokenConnection(String addr, String token) throws SQLException {
        java.util.Properties p = new java.util.Properties();
        p.setProperty("accessToken", token);
        return DriverManager.getConnection(
            "jdbc:sqlserver://" + addr + ";databaseName=shop;encrypt=true;trustServerCertificate=true;loginTimeout=10", p);
    }

    /**
     * OpenID Connect access tokens (the accessToken property). The main server has no issuer
     * configured and says so; STORINE_MSSQL_AUTH names one started with --oidc-* (run_all.sh).
     */
    @Test
    void accessTokenLogins() throws Exception {
        String a = System.getenv().getOrDefault("STORINE_MSSQL", "127.0.0.1:51433");
        SQLException off = assertThrows(SQLException.class, () -> tokenConnection(a, "a.b.c").close());
        assertTrue(off.getMessage().contains("not enabled"), off.getMessage());
        String auth = System.getenv("STORINE_MSSQL_AUTH"), key = System.getenv("STORINE_OIDC_KEY");
        if (auth == null || key == null) return;
        try (Connection c = tokenConnection(auth, mintToken(key, ",\"preferred_username\":\"ann\""));
             ResultSet rs = c.createStatement().executeQuery("SELECT SUSER_NAME()")) {
            assertTrue(rs.next());
            assertEquals("ann", rs.getString(1));
        }
        String expired = mintToken(key, ",\"exp\":1000");
        SQLException e = assertThrows(SQLException.class, () -> tokenConnection(auth, expired).close());
        assertTrue(e.getMessage().contains("expired"), e.getMessage());
    }

    /** Full TLS inside TDS 7.4, and TDS 8 strict mode verifying the pinned certificate. */
    @Test
    void tls() throws Exception {
        java.util.List<String> modes = new java.util.ArrayList<>(java.util.List.of("encrypt=true;trustServerCertificate=true"));
        String cert = System.getenv("STORINE_TLS_CERT");
        if (cert != null) {
            // serverCertificate pins the certificate (strict mode only).
            modes.add("encrypt=strict;hostNameInCertificate=localhost;serverCertificate=" + cert);
        }
        String big = "x".repeat(100_000);
        for (String mode : modes) {
            try (Connection c = DriverManager.getConnection(url(mode));
                 PreparedStatement ps = c.prepareStatement("SELECT ? + 'y'")) {
                ps.setString(1, big);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next(), mode);
                    assertEquals(big + "y", rs.getString(1), mode);
                }
            }
        }
    }

    @Test
    void jdbcCrudTransactionsAndKeys() throws Exception {
        try (Connection c = DriverManager.getConnection(url())) {
            assertEquals("Microsoft SQL Server", c.getMetaData().getDatabaseProductName());
            try (Statement st = c.createStatement()) {
                st.execute("DROP TABLE IF EXISTS jm_acct");
                st.execute("CREATE TABLE jm_acct (id INT IDENTITY(1,1) PRIMARY KEY, owner NVARCHAR(40) NOT NULL UNIQUE, balance DECIMAL(12,2) NOT NULL)");
            }
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO jm_acct (owner, balance) VALUES (?, ?)", Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, "ann");
                ps.setBigDecimal(2, new BigDecimal("100.00"));
                assertEquals(1, ps.executeUpdate());
                try (ResultSet k = ps.getGeneratedKeys()) {
                    assertTrue(k.next());
                    assertEquals(1, k.getInt(1));
                }
                ps.setString(1, "bob");
                ps.setBigDecimal(2, new BigDecimal("0.00"));
                ps.executeUpdate();
            }
            // Transfers in transactions from several threads: exact totals.
            ExecutorService pool = Executors.newFixedThreadPool(4);
            for (int i = 0; i < 4; i++) {
                pool.submit(() -> {
                    try (Connection cc = DriverManager.getConnection(url())) {
                        cc.setAutoCommit(false);
                        for (int j = 0; j < 10; j++) {
                            try (PreparedStatement a = cc.prepareStatement("UPDATE jm_acct SET balance = balance - ? WHERE owner = ?");
                                 PreparedStatement b = cc.prepareStatement("UPDATE jm_acct SET balance = balance + ? WHERE owner = ?")) {
                                a.setBigDecimal(1, BigDecimal.ONE);
                                a.setString(2, "ann");
                                a.executeUpdate();
                                b.setBigDecimal(1, BigDecimal.ONE);
                                b.setString(2, "bob");
                                b.executeUpdate();
                                cc.commit();
                            } catch (SQLException e) {
                                cc.rollback();
                                j--;
                            }
                        }
                    }
                    return null;
                });
            }
            pool.shutdown();
            assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS));
            try (PreparedStatement ps = c.prepareStatement("SELECT owner, balance FROM jm_acct ORDER BY owner");
                 ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(new BigDecimal("60.00"), rs.getBigDecimal(2));
                assertTrue(rs.next());
                assertEquals(new BigDecimal("40.00"), rs.getBigDecimal(2));
            }
            c.setAutoCommit(false);
            try (Statement st = c.createStatement()) {
                st.executeUpdate("UPDATE jm_acct SET balance = 0");
            }
            c.rollback();
            c.setAutoCommit(true);
            try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT SUM(balance) FROM jm_acct")) {
                assertTrue(rs.next());
                assertEquals(new BigDecimal("100.00"), rs.getBigDecimal(1));
            }
            SQLException dup = assertThrows(SQLException.class, () -> {
                try (Statement st = c.createStatement()) {
                    st.executeUpdate("INSERT INTO jm_acct (owner, balance) VALUES ('ann', 1)");
                }
            });
            assertEquals(2627, dup.getErrorCode());
        }
    }
}
