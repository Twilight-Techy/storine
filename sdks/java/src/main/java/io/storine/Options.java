package io.storine;

import java.time.Duration;

/**
 * Connection options: {@code Options.defaults().token("...").timeout(Duration.ofSeconds(2))}. Immutable.
 *
 * <p>TLS: {@code tlsCa("/path/ca.crt")} trusts the certificates in a PEM file and turns TLS on ({@code storines://}
 * turns it on with the JVM's trusted roots); {@code tlsServerName("db1")} sets the name the server's certificate
 * must carry (default: the host connected to).
 */
public record Options(String token, Duration timeout, boolean followerReads, int poolSize, boolean tls,
                      String tlsCa, String tlsServerName) {
    public Options {
        if (timeout == null || timeout.isNegative() || timeout.isZero()) throw new InvalidException("timeout must be positive");
        if (poolSize <= 0) throw new InvalidException("poolSize must be positive");
    }

    /** Options without TLS (the constructor before TLS options existed). */
    public Options(String token, Duration timeout, boolean followerReads, int poolSize) {
        this(token, timeout, followerReads, poolSize, false, null, null);
    }

    /** No token, 5 s timeout, leader reads, 16 pooled connections, no TLS. */
    public static Options defaults() {
        return new Options(null, Duration.ofSeconds(5), false, 16, false, null, null);
    }

    /** Bearer token (also accepted in the URL: {@code storine://token@host:port}). */
    public Options token(String token) {
        return new Options(token, timeout, followerReads, poolSize, tls, tlsCa, tlsServerName);
    }

    /** Timeout of one request (default 5 s). Retries continue for up to 3x this. */
    public Options timeout(Duration timeout) {
        return new Options(token, timeout, followerReads, poolSize, tls, tlsCa, tlsServerName);
    }

    /** Allow possibly stale reads from cluster followers (default false: leader reads). */
    public Options followerReads(boolean followerReads) {
        return new Options(token, timeout, followerReads, poolSize, tls, tlsCa, tlsServerName);
    }

    /** Idle connections kept to the current server (default 16). */
    public Options poolSize(int poolSize) {
        return new Options(token, timeout, followerReads, poolSize, tls, tlsCa, tlsServerName);
    }

    /** Connect over TLS 1.3 (also {@code storines://}). */
    public Options tls(boolean tls) {
        return new Options(token, timeout, followerReads, poolSize, tls, tlsCa, tlsServerName);
    }

    /** PEM file of the certificates to trust; turns TLS on. */
    public Options tlsCa(String tlsCa) {
        return new Options(token, timeout, followerReads, poolSize, true, tlsCa, tlsServerName);
    }

    /** Name the server's certificate must carry (default: the host connected to). */
    public Options tlsServerName(String tlsServerName) {
        return new Options(token, timeout, followerReads, poolSize, tls, tlsCa, tlsServerName);
    }
}
