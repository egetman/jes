package store.jesframework.ex;

import java.util.UUID;

public class EventStreamRewriteUnsupportedException extends RuntimeException {

    private static final String ERROR = "Illegal try to rewrite existing stream: %s";

    public EventStreamRewriteUnsupportedException(UUID uuid) {
        super(String.format(ERROR, uuid));
    }
}
