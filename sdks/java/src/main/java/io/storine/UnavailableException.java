package io.storine;

/** No server (or no cluster leader) could be reached in time. Retry later. */
public class UnavailableException extends StorineException {
    private static final long serialVersionUID = 1L;

    public UnavailableException(String message, int status) {
        super(message, status);
    }

    public UnavailableException(String message) {
        super(message, -1);
    }
}
