package io.storine;

/**
 * A condition did not hold: the document already exists, does not exist, or changed. Re-read and decide again.
 */
public class ConflictException extends StorineException {
    private static final long serialVersionUID = 1L;
    private final int writeIndex;
    private final Long currentVersion;

    public ConflictException(String message, int writeIndex, Long currentVersion) {
        super(message, 4);
        this.writeIndex = writeIndex;
        this.currentVersion = currentVersion;
    }

    /** Position of the failing write in the request, or -1 if unknown. */
    public int writeIndex() {
        return writeIndex;
    }

    /** The document's version now, or {@code null} if it does not exist. */
    public Long currentVersion() {
        return currentVersion;
    }
}
