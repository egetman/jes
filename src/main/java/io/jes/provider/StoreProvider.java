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
     */
    Stream<Event> readFrom(long offset);

    /**
     * see {@link io.jes.JEventStore#readBy(UUID)}.
     */
    Collection<Event> readBy(@Nonnull UUID uuid);

    /**
     * see {@link io.jes.JEventStore#write(Event)}.
     */
    void write(@Nonnull Event event);

    /**
     * see {@link io.jes.JEventStore#deleteBy(UUID)}.
     */
    void deleteBy(@Nonnull UUID uuid);

}
