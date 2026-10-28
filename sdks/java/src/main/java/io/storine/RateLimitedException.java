package io.storine;

/** This connection exceeded its rate limit. Retry later. */
public class RateLimitedException extends OverloadedException {
    private static final long serialVersionUID = 1L;

    public RateLimitedException(String message, int status) {
        super(message, status);
    }

    public RateLimitedException(String message) {
        super(message, -1);
    }
}
