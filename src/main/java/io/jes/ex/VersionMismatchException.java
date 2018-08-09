package io.jes.ex;

import static java.lang.String.format;

public class VersionMismatchException extends RuntimeException {

    public VersionMismatchException(long expected, long actual) {
        super(format("Wrong version number found on write. Expected version %d actual version %d", expected, actual));
    }
}
