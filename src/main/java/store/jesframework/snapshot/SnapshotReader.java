package store.jesframework.snapshot;

import java.util.Collection;
import java.util.UUID;
import javax.annotation.Nonnull;

import store.jesframework.Event;

public interface SnapshotReader {

    /**
     * Returns all events grouped by {@literal event uuid identifier}, also known as an {@literal aggregate
     * identifier}. Skips first {@literal skip} events.
     *
     * @param uuid identifier of event uuid to read.
     * @param skip events count to skip.
     * @return {@link Collection} of events stored in that {@literal EventStore}, grouped by {@literal uuid}.
     * @throws NullPointerException                if uuid is null.
     */
    Collection<Event> readBy(@Nonnull UUID uuid, long skip);

}
