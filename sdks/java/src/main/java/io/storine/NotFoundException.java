package io.storine;

/** The document does not exist (e.g. {@code update} of a missing key). */
public class NotFoundException extends StorineException {
    private static final long serialVersionUID = 1L;
    private final int writeIndex;

    public NotFoundException(String message, int status, int writeIndex) {
        super(message, status);
        this.writeIndex = writeIndex;
    }

    /** Position of the write (in a transaction) whose document was missing, or -1. */
    public int writeIndex() {
        return writeIndex;
    }
}
