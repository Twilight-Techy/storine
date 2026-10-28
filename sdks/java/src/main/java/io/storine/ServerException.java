package io.storine;

/** The server failed internally (I/O error...). The message has the details. */
public class ServerException extends StorineException {
    private static final long serialVersionUID = 1L;

    public ServerException(String message, int status) {
        super(message, status);
    }

    public ServerException(String message) {
        super(message, -1);
    }
}
