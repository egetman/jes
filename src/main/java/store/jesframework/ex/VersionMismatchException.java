package store.jesframework.ex;

import static java.lang.String.format;

public class VersionMismatchException extends RuntimeException {

    public VersionMismatchException(long expected, long actual) {
        super(format("Event uuid version mismatch. Expected version: [%d] Actual version: [%d]", expected, actual));
    }
}
