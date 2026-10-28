package io.storine;

import java.time.Duration;

/** Connection options: {@code Options.defaults().token("...").timeout(Duration.ofSeconds(2))}. Immutable. */
public record Options(String token, Duration timeout, boolean followerReads, int poolSize) {
    public Options {
        if (timeout == null || timeout.isNegative() || timeout.isZero()) throw new InvalidException("timeout must be positive");
        if (poolSize <= 0) throw new InvalidException("poolSize must be positive");
    }

    /** No token, 5 s timeout, leader reads, 16 pooled connections. */
    public static Options defaults() {
        return new Options(null, Duration.ofSeconds(5), false, 16);
    }

    /** Bearer token (also accepted in the URL: {@code storine://token@host:port}). */
    public Options token(String token) {
        return new Options(token, timeout, followerReads, poolSize);
    }

    /** Timeout of one request (default 5 s). Retries continue for up to 3x this. */
    public Options timeout(Duration timeout) {
        return new Options(token, timeout, followerReads, poolSize);
    }

    /** Allow possibly stale reads from cluster followers (default false: leader reads). */
    public Options followerReads(boolean followerReads) {
        return new Options(token, timeout, followerReads, poolSize);
    }

    /** Idle connections kept to the current server (default 16). */
    public Options poolSize(int poolSize) {
        return new Options(token, timeout, followerReads, poolSize);
    }
}
