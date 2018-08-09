package io.jes.ex;

public class BrokenStoreException extends RuntimeException {

    public BrokenStoreException(Exception cause) {
        super(cause);
    }
}
