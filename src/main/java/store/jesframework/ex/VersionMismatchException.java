package store.jesframework.ex;

import java.util.UUID;

import static java.lang.String.format;

public class VersionMismatchException extends RuntimeException {

    private static final String TEMPLATE = "Event stream version mismatch. Expected version: [%d] actual version: "
            + "[%d] for stream %s";

    public VersionMismatchException(UUID uuid, long expected, long actual) {
        super(format(TEMPLATE, expected, actual, uuid));
    }
}
