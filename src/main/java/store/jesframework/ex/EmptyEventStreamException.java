package store.jesframework.ex;

public class EmptyEventStreamException extends RuntimeException {

    public EmptyEventStreamException(String message) {
        super(message, null, false, false);
    }

}
