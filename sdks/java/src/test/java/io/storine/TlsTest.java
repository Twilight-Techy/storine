package io.storine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** TLS on the native port, with certificates from {@code storage-server gen-ca} / {@code gen-cert}. */
class TlsTest {
    static void run(String... args) throws Exception {
        Process p = new ProcessBuilder(args).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
        assertEquals(0, p.waitFor(), String.join(" ", args));
    }

    @Test
    void tlsRoundTripAndUntrustedServer(@TempDir Path d) throws Exception {
        assumeTrue(Files.exists(E2ETest.SERVER), "storage-server binary not built");
        String bin = E2ETest.SERVER.toString();
        run(bin, "gen-ca", d.resolve("ca").toString());
        run(bin, "gen-cert", d.resolve("ca").toString(), d.resolve("node").toString(), "127.0.0.1,localhost");
        run(bin, "gen-ca", d.resolve("other").toString());
        int port = E2ETest.freePort();
        ProcessBuilder pb = new ProcessBuilder(bin, d.resolve("data").toString(), "127.0.0.1:" + port)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.DISCARD);
        pb.environment().put("STORAGE_TLS_CERT", d.resolve("node/node.crt").toString());
        pb.environment().put("STORAGE_TLS_KEY", d.resolve("node/node.key").toString());
        Process server = pb.start();
        try {
            long deadline = System.nanoTime() + 10_000_000_000L;
            while (true) {
                try {
                    new java.net.Socket("127.0.0.1", port).close();
                    break;
                } catch (java.io.IOException e) {
                    if (System.nanoTime() > deadline) throw new AssertionError("server did not start");
                    Thread.sleep(50);
                }
            }
            String big = "x".repeat(100_000);
            try (Storine db = Storine.connect("storines://127.0.0.1:" + port,
                    Options.defaults().tlsCa(d.resolve("ca/ca.crt").toString()))) {
                db.collection("t").put("k", Map.of("v", big));
                assertEquals(big, db.collection("t").get("k").orElseThrow().fields().get("v"));
            }
            try (Storine named = Storine.connect("storines://127.0.0.1:" + port + "?tls_ca=" + d.resolve("ca/ca.crt")
                    + "&tls_server_name=localhost")) {
                assertTrue(named.collection("t").get("k").isPresent());
            }
            try (Storine other = Storine.connect("storines://127.0.0.1:" + port,
                    Options.defaults().tlsCa(d.resolve("other/ca.crt").toString()).timeout(Duration.ofSeconds(1)))) {
                assertThrows(RuntimeException.class, () -> other.collection("t").get("k"));
            }
        } finally {
            server.destroyForcibly();
            server.waitFor();
        }
    }
}
