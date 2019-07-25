package io.jes.ex;

public class BrokenStoreException extends RuntimeException {

    public BrokenStoreException(String message) {
        super(message);
    }

    public BrokenStoreException(Exception cause) {
        super(cause);
    }

    public BrokenStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
