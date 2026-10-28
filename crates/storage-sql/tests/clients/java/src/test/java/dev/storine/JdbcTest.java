package dev.storine;

import static org.junit.jupiter.api.Assertions.*;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.Test;

class JdbcTest {
    static String pg() {
        String v = System.getProperty("storine.pg");
        return v == null || v.isEmpty() || v.startsWith("$") ? "127.0.0.1:55432" : v;
    }

    static String my() {
        String v = System.getProperty("storine.mysql");
        return v == null || v.isEmpty() || v.startsWith("$") ? "127.0.0.1:53306" : v;
    }

    static Connection pgConn() throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://" + pg() + "/shop", "app", "x");
    }

    static Connection myConn() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://" + my() + "/shop?useSSL=false&allowPublicKeyRetrieval=true", "root", "");
    }

    void exerciseJdbc(Connection c, String autoKey) throws Exception {
        try (Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jd_items");
            st.execute("CREATE TABLE jd_items (id " + autoKey + " PRIMARY KEY, name VARCHAR(40) NOT NULL UNIQUE, price DECIMAL(10,2), qty INT, made TIMESTAMP)");
        }
        try (PreparedStatement ps = c.prepareStatement("INSERT INTO jd_items (name, price, qty, made) VALUES (?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS)) {
            for (int i = 1; i <= 3; i++) {
                ps.setString(1, "item" + i);
                ps.setBigDecimal(2, new BigDecimal("1.25").multiply(BigDecimal.valueOf(i)));
                ps.setInt(3, i);
                ps.setTimestamp(4, Timestamp.valueOf(LocalDateTime.of(2024, 1, i, 10, 0)));
                ps.executeUpdate();
                try (ResultSet k = ps.getGeneratedKeys()) {
                    assertTrue(k.next());
                    assertEquals(i, k.getLong(1));
                }
            }
        }
        try (PreparedStatement ps = c.prepareStatement("SELECT name, price, qty, made FROM jd_items WHERE price >= ? ORDER BY id")) {
            ps.setBigDecimal(1, new BigDecimal("2.50"));
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("item2", rs.getString(1));
                assertEquals(new BigDecimal("2.50"), rs.getBigDecimal(2));
                assertEquals(2, rs.getInt(3));
                assertEquals(LocalDateTime.of(2024, 1, 2, 10, 0), rs.getTimestamp(4).toLocalDateTime());
                assertTrue(rs.next());
                assertFalse(rs.next());
            }
        }
        c.setAutoCommit(false);
        try (Statement st = c.createStatement()) {
            st.executeUpdate("UPDATE jd_items SET qty = 99");
        }
        c.rollback();
        c.setAutoCommit(true);
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT sum(qty), count(*) FROM jd_items")) {
            assertTrue(rs.next());
            assertEquals(6, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
        }
        SQLException dup = assertThrows(SQLException.class, () -> {
            try (Statement st = c.createStatement()) {
                st.executeUpdate("INSERT INTO jd_items (name) VALUES ('item1')");
            }
        });
        assertEquals("23", dup.getSQLState().substring(0, 2));
        DatabaseMetaData md = c.getMetaData();
        try (ResultSet t = md.getTables(null, null, "jd_items", new String[] {"TABLE"})) {
            assertTrue(t.next(), "metadata lists the table");
        }
        try (ResultSet cols = md.getColumns(null, null, "jd_items", null)) {
            int n = 0;
            while (cols.next()) {
                n++;
            }
            assertEquals(5, n);
        }
    }

    @Test
    void postgresJdbc() throws Exception {
        try (Connection c = pgConn()) {
            exerciseJdbc(c, "SERIAL");
        }
    }

    @Test
    void mysqlJdbc() throws Exception {
        try (Connection c = myConn()) {
            exerciseJdbc(c, "INT AUTO_INCREMENT");
        }
    }

    @Test
    void concurrentTransfersPostgres() throws Exception {
        try (Connection c = pgConn(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS jd_acct");
            st.execute("CREATE TABLE jd_acct (id INT PRIMARY KEY, balance NUMERIC(12,2) NOT NULL)");
            st.execute("INSERT INTO jd_acct VALUES (1, 50), (2, 50)");
        }
        ExecutorService pool = Executors.newFixedThreadPool(6);
        List<Future<?>> fs = new ArrayList<>();
        for (int w = 0; w < 6; w++) {
            final int dir = w % 2;
            fs.add(pool.submit(() -> {
                try (Connection c = pgConn()) {
                    c.setAutoCommit(false);
                    for (int i = 0; i < 20; i++) {
                        int from = dir == 0 ? 1 : 2, to = dir == 0 ? 2 : 1;
                        // Lock both rows in a fixed order to avoid lock waits in a cycle.
                        try (PreparedStatement lock = c.prepareStatement("SELECT balance FROM jd_acct WHERE id IN (1, 2) ORDER BY id FOR UPDATE")) {
                            lock.executeQuery().close();
                        }
                        try (PreparedStatement a = c.prepareStatement("UPDATE jd_acct SET balance = balance - 1 WHERE id = ?");
                             PreparedStatement b = c.prepareStatement("UPDATE jd_acct SET balance = balance + 1 WHERE id = ?")) {
                            a.setInt(1, from);
                            a.executeUpdate();
                            b.setInt(1, to);
                            b.executeUpdate();
                        }
                        c.commit();
                    }
                }
                return null;
            }));
        }
        for (Future<?> f : fs) {
            f.get();
        }
        pool.shutdown();
        try (Connection c = pgConn(); Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT sum(balance) FROM jd_acct")) {
            assertTrue(rs.next());
            assertEquals(new BigDecimal("100.00"), rs.getBigDecimal(1));
        }
    }

    @Entity(name = "Author")
    @Table(name = "hb_author")
    public static class Author {
        @Id
        @GeneratedValue(strategy = GenerationType.SEQUENCE)
        Long id;

        @Column(unique = true, nullable = false)
        String name;

        @Column(precision = 12, scale = 2)
        BigDecimal royalties = BigDecimal.ZERO;

        @OneToMany(mappedBy = "author", cascade = CascadeType.ALL, orphanRemoval = true)
        List<Book> books = new ArrayList<>();

        @Version
        Integer version;
    }

    @Entity(name = "Book")
    @Table(name = "hb_book")
    public static class Book {
        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        Long id;

        String title;

        @ManyToOne(fetch = FetchType.LAZY)
        Author author;
    }

    void hibernateSuite(String url, String user, String dialect) {
        Configuration cfg = new Configuration()
            .addAnnotatedClass(Author.class)
            .addAnnotatedClass(Book.class)
            .setProperty("hibernate.connection.url", url)
            .setProperty("hibernate.connection.username", user)
            .setProperty("hibernate.connection.password", "x")
            .setProperty("hibernate.hbm2ddl.auto", "create-drop")
            .setProperty("hibernate.show_sql", "false");
        if (dialect != null) {
            cfg.setProperty("hibernate.dialect", dialect);
        }
        try (SessionFactory sf = cfg.buildSessionFactory()) {
            Long id = sf.fromTransaction(s -> {
                Author a = new Author();
                a.name = "Ada";
                for (String t : List.of("One", "Two", "Three")) {
                    Book b = new Book();
                    b.title = t;
                    b.author = a;
                    a.books.add(b);
                }
                s.persist(a);
                Author b = new Author();
                b.name = "Bob";
                s.persist(b);
                return a.id;
            });
            sf.inTransaction(s -> {
                Author a = s.find(Author.class, id);
                assertEquals(3, a.books.size());
                a.royalties = a.royalties.add(new BigDecimal("12.34"));
            });
            List<Object[]> rows = sf.fromSession(s -> s.createQuery(
                "select a.name, count(b) from Author a left join a.books b group by a.name order by a.name", Object[].class).getResultList());
            assertEquals("Ada", rows.get(0)[0]);
            assertEquals(3L, rows.get(0)[1]);
            assertEquals(0L, rows.get(1)[1]);
            // Optimistic locking: a stale version is rejected.
            Author stale = sf.fromSession(s -> s.find(Author.class, id));
            sf.inTransaction(s -> s.find(Author.class, id).royalties = BigDecimal.ONE);
            assertThrows(OptimisticLockException.class, () -> sf.inTransaction(s -> {
                stale.royalties = BigDecimal.TEN;
                s.merge(stale);
            }));
            BigDecimal r = sf.fromSession(s -> s.find(Author.class, id).royalties);
            assertEquals(new BigDecimal("1.00"), r);
        }
    }

    @Test
    void hibernatePostgres() {
        hibernateSuite("jdbc:postgresql://" + pg() + "/shop", "app", null);
    }

    @Test
    void hibernateMySql() {
        hibernateSuite("jdbc:mysql://" + my() + "/shop?useSSL=false&allowPublicKeyRetrieval=true", "root", null);
    }

    // TLS through Java's own JSSE stack on both protocols.
    @Test
    void tlsBothProtocols() throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:postgresql://" + pg() + "/shop?sslmode=require", "app", "x");
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT repeat('j', 70000)")) {
            assertTrue(rs.next());
            assertEquals(70000, rs.getString(1).length());
        }
        try (Connection c = DriverManager.getConnection("jdbc:mysql://" + my() + "/shop?sslMode=REQUIRED", "root", "");
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT 6 * 7")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
        }
    }
}
