package io.jes.ex;

import static java.lang.String.format;

public class VersionMismatchException extends RuntimeException {

    public VersionMismatchException(long expected, long actual) {
        super(format("Event stream version mismatch. Expected version: [%d] Actual version: [%d]", expected, actual));
    }
}
