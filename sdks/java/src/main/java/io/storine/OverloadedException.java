package io.storine;

/** The server is temporarily overloaded. Retry later. */
public class OverloadedException extends StorineException {
    private static final long serialVersionUID = 1L;

    public OverloadedException(String message, int status) {
        super(message, status);
    }

    public OverloadedException(String message) {
        super(message, -1);
    }
}
