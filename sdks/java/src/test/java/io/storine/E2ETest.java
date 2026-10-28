package io.storine;

import static io.storine.FieldOp.add;
import static io.storine.FieldOp.append;
import static io.storine.FieldOp.set;
import static io.storine.FieldOp.unset;
import static io.storine.Filter.and;
import static io.storine.Filter.between;
import static io.storine.Filter.eq;
import static io.storine.Filter.gt;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end tests against a real storage-server (single node and a 3-node cluster). The binary is
 * $STORINE_SERVER or <repo>/target/release/storage-server (cargo build --release -p storage-server).
 */
class E2ETest {
    static final Path SERVER = Path.of(System.getenv().getOrDefault("STORINE_SERVER",
            Path.of("..", "..", "target", "release", "storage-server").toAbsolutePath().normalize().toString()));
    static Process proc;
    static Storine db;

    static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    static Process start(Path dir, int port, String... extra) throws Exception {
        List<String> cmd = new ArrayList<>(List.of(SERVER.toString(), dir.toString(), "127.0.0.1:" + port));
        cmd.addAll(List.of(extra));
        Process p = new ProcessBuilder(cmd).redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .redirectError(ProcessBuilder.Redirect.DISCARD).start();
        long deadline = System.nanoTime() + 10_000_000_000L;
        while (true) {
            try {
                new Socket("127.0.0.1", port).close();
                return p;
            } catch (IOException e) {
                if (System.nanoTime() > deadline) {
                    p.destroyForcibly();
                    throw new AssertionError("server on port " + port + " did not start");
                }
                Thread.sleep(50);
            }
        }
    }

    @BeforeAll
    static void setUp(@TempDir Path dir) throws Exception {
        assumeTrue(Files.exists(SERVER), "storage-server binary not built");
        int port = freePort();
        proc = start(dir, port);
        db = Storine.connect("storine://127.0.0.1:" + port);
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (db != null) db.close();
        if (proc != null) {
            proc.destroyForcibly();
            proc.waitFor();
        }
    }

    static BigDecimal dec(String s) {
        return new BigDecimal(s);
    }

    @Test
    void crudTypesAndVersions() {
        Collection c = db.collection("people");
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("name", "Ann");
        doc.put("age", 37L);
        doc.put("big", BigInteger.ONE.shiftLeft(100));
        doc.put("score", 9.5);
        doc.put("price", dec("19.99"));
        doc.put("joined", Instant.parse("2024-05-01T12:30:00Z"));
        doc.put("id", UUID.randomUUID());
        doc.put("tags", List.of("a", "b"));
        doc.put("address", Map.of("city", "Lagos"));
        doc.put("nothing", null);
        doc.put("ok", true);
        long v1 = c.insert("ann", doc);
        Document got = c.get("ann").orElseThrow();
        assertEquals(v1, got.version());
        assertEquals(doc, got.fields());
        assertThrows(ConflictException.class, () -> c.insert("ann", doc));
        long v2 = c.update("ann", set("address.city", "Abuja"), unset("nothing"), append("tags", "c"));
        assertTrue(v2 > v1);
        got = c.get("ann").orElseThrow();
        assertEquals(Map.of("city", "Abuja"), got.get("address"));
        assertFalse(got.fields().containsKey("nothing"));
        assertEquals(List.of("a", "b", "c"), got.get("tags"));
        ConflictException e = assertThrows(ConflictException.class, () -> c.replaceIfVersion("ann", v1, Map.of("name", "x")));
        assertEquals(v2, e.currentVersion());
        c.replaceIfVersion("ann", v2, Map.of("name", "Ann B"));
        assertEquals(Map.of("name", "Ann B"), c.get("ann").orElseThrow().fields());
        assertThrows(NotFoundException.class, () -> c.update("nobody", set("a", 1)));
        assertTrue(c.delete("ann"));
        assertFalse(c.delete("ann"));
        assertTrue(c.get("ann").isEmpty());
    }

    @Test
    void moneyIsExactAndNeverLost() throws Exception {
        Collection acct = db.collection("accounts");
        acct.put("a", Map.of("balance", dec("0.00")));
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
            futures.add(pool.submit(() -> {
                for (int i = 0; i < 100; i++) acct.update("a", add("balance", dec("0.01")));
            }));
        }
        for (Future<?> f : futures) f.get();
        pool.shutdown();
        assertEquals(dec("8.00"), acct.get("a").orElseThrow().get("balance"));
        assertThrows(InvalidException.class, () -> acct.update("a", add("balance", dec("-8.01")).min(0)));
        assertEquals(dec("8.00"), acct.get("a").orElseThrow().get("balance"));
    }

    @Test
    void transactionIsAtomic() {
        Collection bank = db.collection("bank");
        bank.put("alice", Map.of("balance", dec("50")));
        bank.put("bob", Map.of("balance", dec("0")));
        db.transaction(tx -> {
            tx.update("bank", "alice", add("balance", dec("-20")).min(0));
            tx.update("bank", "bob", add("balance", dec("20")));
        });
        assertThrows(InvalidException.class, () -> db.transaction(tx -> {
            tx.update("bank", "alice", add("balance", dec("-31")).min(0));
            tx.update("bank", "bob", add("balance", dec("31")));
        }));
        assertEquals(dec("30"), bank.get("alice").orElseThrow().get("balance"));
        assertEquals(dec("20"), bank.get("bob").orElseThrow().get("balance"));
        long v = bank.get("alice").orElseThrow().version();
        ConflictException e = assertThrows(ConflictException.class, () -> db.transaction(tx -> {
            tx.requireVersion("bank", "alice", v + 100);
            tx.put("bank", "carol", Map.of());
        }));
        assertEquals(0, e.writeIndex());
        assertTrue(bank.get("carol").isEmpty());
    }

    @Test
    void queriesIndexesAndPagination() {
        Collection items = db.collection("items");
        for (int i = 0; i < 120; i++) {
            items.put(String.format("i%03d", i), Map.of("n", i, "kind", i % 2 == 0 ? "even" : "odd",
                    "price", BigDecimal.valueOf(i * 25L, 2)));
        }
        Query q = new Query().where(and(between("n", 10, 19), eq("kind", "odd"))).orderBy("-n");
        List<String> want = List.of("i019", "i017", "i015", "i013", "i011");
        assertEquals(want, items.find(q).stream().map(Document::key).toList());
        items.createIndex("n");
        items.createIndex("kind");
        assertEquals(want, items.find(q).stream().map(Document::key).toList());
        CollectionInfo info = items.info();
        assertTrue(info.indexes().contains("n") || info.building().contains("n"));
        assertEquals(19, items.count(gt("price", dec("25"))));
        assertEquals(60, items.count(Filter.match(Map.of("kind", "even"))));
        assertEquals(120, items.scan().count());
        assertEquals("i119", items.first(new Query().orderBy("-price")).orElseThrow().key());
        assertTrue(db.collections().contains("items"));
    }

    @Test
    void vectorSearch() {
        Collection c = db.collection("java_docs");
        c.createVectorIndex("emb", 3);
        assertEquals(List.of(new VectorIndex("emb", 3, true)), c.vectorIndexes());
        c.put("a", Map.of("emb", List.of(1, 0, 0), "kind", "x"));
        c.put("b", Map.of("emb", List.of(0, 1, 0), "kind", "y"));
        c.put("c", Map.of("emb", List.of(0.7, 0.7, 0), "kind", "x"));
        List<Document> hits = c.nearest("emb", new float[] {1, 0.1f, 0}, 2);
        assertEquals(List.of("a", "c"), hits.stream().map(Document::key).toList());
        assertTrue(hits.get(0).score() > hits.get(1).score());
        assertEquals(List.of("a", "c"), c.nearest("emb", new float[] {1, 0.1f, 0}, 5, eq("kind", "x"), true).stream()
                .map(Document::key).toList());
        assertThrows(InvalidException.class, () -> c.put("bad", Map.of("emb", List.of(1, 2)))); // wrong dimensions
        assertThrows(InvalidException.class, () -> c.nearest("emb", new float[] {1, 2}, 1));
        c.dropVectorIndex("emb");
        assertEquals(List.of(), c.vectorIndexes());
    }

    @Test
    void largeValues() throws Exception {
        Collection c = db.collection("java_files");
        byte[] blob = new byte[3 * 1024 * 1024 + 17];
        new java.util.Random(7).nextBytes(blob);
        long v = c.put("f1", Map.of("data", blob, "name", "cat.png", "meta", Map.of("size", blob.length, "type", "image/png")));
        Document d = c.getFields("f1", "name", "meta.size", "absent").orElseThrow();
        assertEquals(Map.of("name", "cat.png", "meta", Map.of("size", (long) blob.length)), d.fields());
        assertEquals(v, d.version());
        ByteRange r = c.readBytes("f1", "data", 1000, 16).orElseThrow();
        assertEquals(v, r.version());
        assertEquals(blob.length, r.total());
        assertArrayEquals(Arrays.copyOfRange(blob, 1000, 1016), r.data());
        assertEquals(0, c.readBytes("f1", "data", blob.length + 5, 10).orElseThrow().data().length);
        try (InputStream in = c.streamBytes("f1", "data", 1 << 20)) {
            assertArrayEquals(blob, in.readAllBytes());
        }
        assertArrayEquals("cat".getBytes(StandardCharsets.UTF_8), c.readBytes("f1", "name", 0, 3).orElseThrow().data());
        assertTrue(c.readBytes("nope", "data").isEmpty());
        assertThrows(NotFoundException.class, () -> c.streamBytes("nope", "data").readAllBytes());
        assertThrows(InvalidException.class, () -> c.readBytes("f1", "meta")); // not bytes
        assertThrows(InvalidException.class, () -> c.readBytes("f1", "data", 0, 17 * 1024 * 1024));
        InputStream chunks = c.streamBytes("f1", "data", 1 << 20);
        assertEquals(1 << 20, chunks.readNBytes(1 << 20).length);
        c.update("f1", set("name", "dog.png"));
        assertThrows(ConflictException.class, chunks::readAllBytes);
    }

    @Test
    void ttlAndErrors() throws Exception {
        Collection s = db.collection("sessions");
        s.put("short", Map.of("u", 1), Duration.ofMillis(200));
        assertTrue(s.get("short").isPresent());
        Thread.sleep(400);
        assertTrue(s.get("short").isEmpty());
        assertThrows(InvalidException.class, () -> s.put("bad", Map.of("a.b", 1)));
        assertThrows(InvalidException.class, () -> db.collection("bad/name").put("k", Map.of()));
        assertNull(s.get("nobody").orElse(null));
    }

    @Test
    void threeNodesRedirectAndFailover(@TempDir Path base) throws Exception {
        int[] ports = {freePort(), freePort(), freePort()};
        StringBuilder peers = new StringBuilder();
        StringBuilder seeds = new StringBuilder();
        for (int i = 0; i < 3; i++) {
            if (i > 0) {
                peers.append(',');
                seeds.append(',');
            }
            peers.append(i + 1).append("=127.0.0.1:").append(ports[i]);
            seeds.append("127.0.0.1:").append(ports[i]);
        }
        Process[] procs = new Process[3];
        try {
            for (int i = 0; i < 3; i++) {
                procs[i] = start(base.resolve("n" + (i + 1)), ports[i], "--node-id", String.valueOf(i + 1), "--peers", peers.toString());
            }
            // Any seed works: the client follows redirects to the leader.
            try (Storine cluster = Storine.connect("storine://" + seeds, Options.defaults().timeout(Duration.ofSeconds(2)))) {
                Collection acct = cluster.collection("acct");
                long deadline = System.nanoTime() + 15_000_000_000L;
                while (true) {
                    try {
                        acct.put("a", Map.of("bal", dec("100")));
                        break;
                    } catch (UnavailableException e) {
                        if (System.nanoTime() > deadline) throw e;
                        Thread.sleep(100);
                    }
                }
                for (int i = 0; i < 10; i++) acct.update("a", add("bal", dec("1")));
                assertEquals(dec("110"), acct.get("a").orElseThrow().get("bal"));
                ClusterInfo info = cluster.clusterInfo();
                assertTrue(info.isLeader());
                Process leader = procs[(int) info.nodeId() - 1];
                leader.destroyForcibly();
                leader.waitFor();
                // The client fails over to the new leader; every deposit applies exactly once.
                for (int i = 0; i < 5; i++) acct.update("a", add("bal", dec("1")));
                assertEquals(dec("115"), acct.get("a").orElseThrow().get("bal"));
            }
        } finally {
            for (Process p : procs) {
                if (p != null) {
                    p.destroyForcibly();
                    p.waitFor();
                }
            }
        }
    }
}
