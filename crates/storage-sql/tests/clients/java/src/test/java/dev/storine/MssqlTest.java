package dev.storine;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

/** Microsoft's JDBC driver (pure Java TDS) against the SQL Server endpoint. */
class MssqlTest {
    static String url() {
        String a = System.getenv().getOrDefault("STORINE_MSSQL", "127.0.0.1:51433");
        return "jdbc:sqlserver://" + a + ";databaseName=shop;encrypt=false;user=sa;password=x";
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
