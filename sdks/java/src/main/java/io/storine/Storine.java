package io.storine;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * A connection pool to a Storine server or cluster. Thread-safe; create one per application.
 *
 * <pre>{@code
 * try (Storine db = Storine.connect("storine://127.0.0.1:7878")) {
 *     Collection accounts = db.collection("accounts");
 *     accounts.insert("alice", Map.of("balance", new BigDecimal("100.00")));
 *     accounts.update("alice", FieldOp.add("balance", new BigDecimal("-25.00")).min(0)); // no overdraft, no lost update
 * }
 * }</pre>
 *
 * <p>Writes carry a request id that the client reuses on its own retries, so timeouts, reconnects and leader changes
 * never apply a write twice.
 */
public final class Storine implements AutoCloseable {
    private static final int MAX_BODY = 256 * 1024 * 1024;
    private static final SecureRandom RANDOM = new SecureRandom();

    private final List<String> seeds;
    private final Options options;
    private final ArrayDeque<Conn> pool = new ArrayDeque<>();
    private String target;

    private Storine(List<String> seeds, Options options) {
        this.seeds = List.copyOf(seeds);
        this.options = options;
        this.target = seeds.get(0);
    }

    /** {@link #connect(String, Options)} with default options. */
    public static Storine connect(String url) {
        return connect(url, Options.defaults());
    }

    /**
     * Connects to {@code storine://[token@]host:port[,host:port...]} (or just {@code host:port}). Query parameters
     * {@code timeout_ms=5000} and {@code follower_reads=1} are accepted too. Connections open lazily.
     */
    public static Storine connect(String url, Options options) {
        if (!url.contains("://")) url = "storine://" + url;
        if (!url.startsWith("storine://")) throw new InvalidException("connection strings start with storine://");
        String rest = url.substring("storine://".length());
        String query = "";
        int qm = rest.indexOf('?');
        if (qm >= 0) {
            query = rest.substring(qm + 1);
            rest = rest.substring(0, qm);
        }
        int at = rest.lastIndexOf('@');
        if (at >= 0) {
            if (options.token() == null) options = options.token(URLDecoder.decode(rest.substring(0, at), StandardCharsets.UTF_8));
            rest = rest.substring(at + 1);
        }
        for (String kv : query.split("&")) {
            if (kv.isEmpty()) continue;
            String[] p = kv.split("=", 2);
            String v = p.length > 1 ? p[1] : "";
            if (p[0].equals("timeout_ms")) {
                try {
                    options = options.timeout(Duration.ofMillis(Long.parseLong(v)));
                } catch (NumberFormatException e) {
                    throw new InvalidException("timeout_ms must be a positive integer");
                }
            } else if (p[0].equals("follower_reads")) {
                options = options.followerReads(v.equals("1") || v.equals("true") || v.equals("yes"));
            }
        }
        List<String> seeds = new ArrayList<>();
        for (String h : rest.replaceAll("/$", "").split(",")) {
            if (!h.isBlank()) seeds.add(normalize(h.trim()));
        }
        if (seeds.isEmpty()) throw new InvalidException("no server address given");
        return new Storine(seeds, options);
    }

    private static String normalize(String address) {
        if (address.startsWith("[")) return address.contains("]:") ? address : address + ":7878";
        int colons = address.length() - address.replace(":", "").length();
        if (colons == 1) return address;
        return colons == 0 ? address + ":7878" : "[" + address + "]:7878";
    }

    /** A handle on the collection {@code name} (created on first write). */
    public Collection collection(String name) {
        return new Collection(this, name);
    }

    /**
     * Collects writes in {@code body}, then commits them atomically (all or none). Returns the new version of each
     * write in order ({@code null} for deletes and checks). If {@code body} throws, nothing is written.
     *
     * <pre>{@code
     * db.transaction(tx -> {
     *     tx.update("accounts", "alice", FieldOp.add("balance", new BigDecimal("-10")).min(0));
     *     tx.update("accounts", "bob", FieldOp.add("balance", new BigDecimal("10")));
     * });
     * }</pre>
     */
    public List<Long> transaction(Consumer<Transaction> body) {
        Transaction tx = new Transaction();
        body.accept(tx);
        if (tx.writes.isEmpty()) return List.of();
        return commit(tx.writes);
    }

    /** Names of every declared collection. */
    public List<String> collections() {
        Values.Reader r = new Values.Reader(call(Wire.OP_DOC_INFO, new byte[] {3}).body);
        int n = r.count(17);
        List<String> names = new ArrayList<>(n);
        for (int i = 0; i < n; i++) names.add(Wire.info(r).name());
        return names;
    }

    /** Node id, leader, term and commit indexes of the server currently used. */
    public ClusterInfo clusterInfo() {
        return Wire.clusterInfo(call(Wire.OP_CLUSTER_INFO, new byte[0]).body);
    }

    /** Closes every pooled connection. */
    @Override
    public void close() {
        List<Conn> all;
        synchronized (this) {
            all = new ArrayList<>(pool);
            pool.clear();
        }
        all.forEach(Conn::close);
    }

    // ── plumbing ──

    record Reply(int status, long version, byte[] body) {}

    private static final class Conn {
        final String address;
        final Socket socket;
        final DataInputStream in;
        final OutputStream out;

        Conn(String address, int timeoutMs) throws IOException {
            this.address = address;
            int colon = address.lastIndexOf(':');
            String host = address.substring(0, colon).replace("[", "").replace("]", "");
            int port = Integer.parseInt(address.substring(colon + 1));
            socket = new Socket();
            try {
                socket.connect(new InetSocketAddress(host, port), timeoutMs);
                socket.setTcpNoDelay(true);
                socket.setSoTimeout(timeoutMs);
                in = new DataInputStream(socket.getInputStream());
                out = socket.getOutputStream();
            } catch (IOException e) {
                socket.close();
                throw e;
            }
        }

        Reply call(int op, byte[] body) throws IOException {
            byte[] frame = new byte[29 + body.length];
            frame[0] = (byte) op;
            for (int i = 0; i < 4; i++) frame[25 + i] = (byte) (body.length >>> (8 * i));
            System.arraycopy(body, 0, frame, 29, body.length);
            out.write(frame);
            out.flush();
            byte[] hdr = new byte[13];
            in.readFully(hdr);
            Values.Reader r = new Values.Reader(hdr);
            int status = r.u8();
            long version = r.u64();
            long n = r.u32();
            if (n > MAX_BODY) throw new IOException("response too large");
            byte[] rbody = new byte[(int) n];
            in.readFully(rbody);
            return new Reply(status, version, rbody);
        }

        void close() {
            try {
                socket.close();
            } catch (IOException ignored) {
                // already closed
            }
        }
    }

    private static final class DialFailure extends IOException {
        private static final long serialVersionUID = 1L;
        final String address;

        DialFailure(String address, IOException cause) {
            super(cause.getMessage(), cause);
            this.address = address;
        }
    }

    private Conn acquire() throws IOException {
        String t;
        synchronized (this) {
            t = target;
            while (!pool.isEmpty()) {
                Conn c = pool.pop();
                if (c.address.equals(t)) return c;
                c.close();
            }
        }
        int timeoutMs = (int) Math.min(Integer.MAX_VALUE, options.timeout().toMillis());
        Conn c;
        try {
            c = new Conn(t, timeoutMs);
        } catch (IOException e) {
            throw new DialFailure(t, e);
        }
        try {
            if (options.token() != null) {
                Reply r = c.call(Wire.OP_AUTH, options.token().getBytes(StandardCharsets.UTF_8));
                if (r.status != Wire.ST_OK) {
                    c.close();
                    throw new UnauthorizedException("authentication failed: " + text(r.body), r.status);
                }
            }
            if (options.followerReads()) c.call(Wire.OP_SET_OPTIONS, new byte[] {1});
        } catch (IOException e) {
            c.close();
            throw new DialFailure(t, e);
        }
        return c;
    }

    private void release(Conn c) {
        synchronized (this) {
            if (c.address.equals(target) && pool.size() < options.poolSize()) {
                pool.push(c);
                return;
            }
        }
        c.close();
    }

    /** Switches to {@code address}, or (when null) to a random other seed not known to have failed. */
    private synchronized void redirect(String address, Set<String> failed) {
        if (address != null) {
            target = normalize(address);
            return;
        }
        List<String> others = new ArrayList<>();
        for (String s : seeds) if (!s.equals(target) && !failed.contains(s)) others.add(s);
        if (others.isEmpty()) for (String s : seeds) if (!s.equals(target)) others.add(s);
        if (others.isEmpty()) others = seeds;
        target = others.get(ThreadLocalRandom.current().nextInt(others.size()));
    }

    private static String text(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UnavailableException("interrupted while waiting to retry");
        }
    }

    /**
     * Sends a request, following leader redirects and retrying transient failures for up to three times the timeout
     * (enough to ride out a leader election). Every request is idempotent: reads by nature, writes by request id.
     */
    Reply request(int op, byte[] body) {
        long deadline = System.nanoTime() + Math.max(3 * options.timeout().toNanos(), 1_000_000_000L);
        long delay = 20;
        StorineException lastError = null;
        Exception lastCause = null;
        Set<String> failed = new HashSet<>();
        for (int attempt = 0; attempt < 1000 && (attempt < 8 || System.nanoTime() < deadline); attempt++) {
            Conn c;
            try {
                c = acquire();
            } catch (DialFailure e) {
                lastCause = e;
                failed.add(e.address);
                redirect(null, failed);
                sleep(delay);
                delay = Math.min(delay * 2, 500);
                continue;
            } catch (IOException e) {
                lastCause = e;
                continue;
            }
            Reply r;
            try {
                r = c.call(op, body);
            } catch (IOException e) {
                c.close();
                lastCause = e;
                failed.add(c.address);
                redirect(null, failed);
                sleep(delay);
                delay = Math.min(delay * 2, 500);
                continue;
            }
            switch (r.status) {
                case Wire.ST_NOT_LEADER -> {
                    c.close();
                    String hint = text(r.body);
                    if (hint.isEmpty() || failed.contains(normalize(hint))) hint = null; // stale: the old leader is down
                    redirect(hint, failed);
                    lastError = new UnavailableException("no cluster leader is available yet", r.status);
                    if (hint == null) {
                        sleep(delay);
                        delay = Math.min(delay * 2, 500);
                    }
                    continue;
                }
                case Wire.ST_COMMIT_UNKNOWN, Wire.ST_OVERLOADED -> {
                    c.close();
                    lastError = r.status == Wire.ST_OVERLOADED
                            ? new OverloadedException(r.body.length > 0 ? text(r.body) : "server overloaded", r.status)
                            : new UnavailableException("write outcome unknown and retries exhausted", r.status);
                    sleep(delay);
                    delay = Math.min(delay * 2, 500);
                    continue;
                }
                case Wire.ST_UNAUTHORIZED, Wire.ST_RATE_LIMITED -> c.close(); // the server closed the connection
                default -> release(c);
            }
            return r;
        }
        if (lastError != null) throw lastError;
        throw new UnavailableException("could not reach any server: " + (lastCause == null ? "?" : lastCause.getMessage()));
    }

    static StorineException error(int status, byte[] body) {
        String t = text(body);
        return switch (status) {
            case Wire.ST_CONFLICT -> Wire.conflict(body);
            case Wire.ST_NOT_FOUND -> new NotFoundException(t.isEmpty() ? "document not found" : t, status, -1);
            case Wire.ST_INVALID -> new InvalidException(t, status);
            case Wire.ST_UNAUTHORIZED -> new UnauthorizedException(t.isEmpty() ? "not authorized" : t, status);
            case Wire.ST_RATE_LIMITED -> new RateLimitedException(t.isEmpty() ? "rate limited" : t, status);
            case Wire.ST_OVERLOADED -> new OverloadedException(t.isEmpty() ? "server overloaded" : t, status);
            case Wire.ST_COMMIT_UNKNOWN -> new UnavailableException("write outcome unknown; retry the same call", status);
            default -> new ServerException(t.isEmpty() ? "server error (status " + status + ")" : t, status);
        };
    }

    /** {@link #request} that throws on any status but OK. */
    Reply call(int op, byte[] body) {
        Reply r = request(op, body);
        if (r.status != Wire.ST_OK) throw error(r.status, r.body);
        return r;
    }

    /** Applies writes atomically, exactly once even across retries; returns the new versions. */
    List<Long> commit(List<Wire.Write> writes) {
        byte[] requestId = new byte[16];
        RANDOM.nextBytes(requestId);
        Reply r = request(Wire.OP_DOC_WRITE, Wire.writes(writes, requestId));
        if (r.status == Wire.ST_NOT_FOUND) {
            // Body: u32 index of the write that patched a missing document, then the message.
            int index = -1;
            String msg = "document not found";
            if (r.body.length >= 4) {
                index = (int) new Values.Reader(r.body).u32();
                if (r.body.length > 4) msg = new String(r.body, 4, r.body.length - 4, StandardCharsets.UTF_8);
            }
            throw new NotFoundException(msg, r.status, index);
        }
        if (r.status != Wire.ST_OK) throw error(r.status, r.body);
        return Wire.writeResponse(r.body);
    }
}
