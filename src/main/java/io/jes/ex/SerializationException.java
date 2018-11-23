package io.jes.ex;

public class SerializationException extends RuntimeException {

    public SerializationException(String message) {
        super(message);
    }

    public SerializationException(Throwable cause) {
        super("Exception during event serialization/deserialization", cause, false, false);
    }
}