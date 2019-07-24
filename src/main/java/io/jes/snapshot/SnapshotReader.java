package io.jes.snapshot;

import java.util.Collection;
import java.util.UUID;
import javax.annotation.Nonnull;

import io.jes.Event;

public interface SnapshotReader {

    /**
     * Returns all events grouped by {@literal event uuid identifier}, also known as an {@literal aggregate
     * identifier}. Skips first {@literal skip} events.
     *
     * @param uuid identifier of event uuid to read.
     * @param skip events count to skip.
     * @return {@link Collection} of events stored in that {@literal EventStore}, grouped by {@literal uuid}.
     * @throws NullPointerException                if uuid is null.
     * @throws io.jes.ex.EmptyEventStreamException if event stream with given {@code uuid} not found.
     */
    Collection<Event> readBy(@Nonnull UUID uuid, long skip);

}
