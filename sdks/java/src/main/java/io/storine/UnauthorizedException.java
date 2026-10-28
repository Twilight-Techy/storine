package io.storine;

/** Authentication failed or the token's role does not allow this operation. */
public class UnauthorizedException extends StorineException {
    private static final long serialVersionUID = 1L;

    public UnauthorizedException(String message, int status) {
        super(message, status);
    }

    public UnauthorizedException(String message) {
        super(message, -1);
    }
}
