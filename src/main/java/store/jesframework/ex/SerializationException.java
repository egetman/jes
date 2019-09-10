package store.jesframework.ex;

public class SerializationException extends RuntimeException {

    public SerializationException(String message) {
        super(message);
    }

    public SerializationException(Throwable cause) {
        super("Exception during serialization/deserialization", cause, false, false);
    }
}
