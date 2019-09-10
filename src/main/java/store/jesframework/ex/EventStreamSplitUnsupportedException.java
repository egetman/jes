package store.jesframework.ex;

import java.util.Collection;
import java.util.UUID;
import javax.annotation.Nonnull;

public class EventStreamSplitUnsupportedException extends RuntimeException {

    private static final String ERROR = "This operation doesn't support stream split. Source stream resulted in "
            + "splitted streams: %s";

    public EventStreamSplitUnsupportedException(@Nonnull Collection<UUID> streamUuids) {
        super(String.format(ERROR, streamUuids));
    }
}
