package store.jesframework.ex;

public class BrokenHandlerException extends RuntimeException {

    public BrokenHandlerException(String message) {
        super(message);
    }

    public BrokenHandlerException(String message, Throwable cause) {
        super(message, cause);
    }

}
