package store.jesframework.ex;

public class BrokenReactorException extends RuntimeException {

    public BrokenReactorException(String message) {
        super(message);
    }

    public BrokenReactorException(String message, Throwable cause) {
        super(message, cause);
    }
}
