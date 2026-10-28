package io.storine;

/** The request was rejected as invalid (bad value, type mismatch, bound violated). Do not retry. */
public class InvalidException extends StorineException {
    private static final long serialVersionUID = 1L;

    public InvalidException(String message, int status) {
        super(message, status);
    }

    public InvalidException(String message) {
        super(message, -1);
    }
}
