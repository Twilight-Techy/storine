package io.storine;

/** Base class of every Storine error. Each says what happened and what to do about it. */
public class StorineException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final int status;

    public StorineException(String message, int status) {
        super(message);
        this.status = status;
    }

    /** The wire status, or -1 when the error was detected locally. */
    public int status() {
        return status;
    }
}
