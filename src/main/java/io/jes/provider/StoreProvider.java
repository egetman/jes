package io.jes.provider;

import java.util.Collection;
import java.util.UUID;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import io.jes.Event;

/**
 * Basic {@literal EventStore} component, that provides protocol/tool-specific low level operations.
 */
public interface StoreProvider {

    /**
     * see {@link io.jes.JEventStore#readFrom(long)}.
     *
     * @param offset the offset to read from.
     * @return {@link Stream} of events stored in that {@literal EventStore}.
     */
    Stream<Event> readFrom(long offset);

    /**
     * see {@link io.jes.JEventStore#readBy(UUID)}.
     *
     * @param uuid identifier of event uuid to read.
     * @return {@link Collection} of events stored in that {@literal EventStore}, grouped by {@literal uuid}.
     */
    Collection<Event> readBy(@Nonnull UUID uuid);

    /**
     * see {@link io.jes.JEventStore#write(Event)}.
     *
     * @param event is an event to store.
     */
    void write(@Nonnull Event event);

    /**
     * see {@link io.jes.JEventStore#deleteBy(UUID)}.
     *
     * @param uuid of stream  to delete.
     */
    void deleteBy(@Nonnull UUID uuid);

}
