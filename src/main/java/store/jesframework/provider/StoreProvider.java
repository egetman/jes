package store.jesframework.provider;

import java.util.Collection;
import java.util.UUID;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import store.jesframework.Event;
import store.jesframework.JEventStore;

/**
 * Basic {@literal EventStore} component, that provides protocol/tool-specific low level operations.
 */
public interface StoreProvider {

    /**
     * see {@link JEventStore#readFrom(long)}.
     *
     * @param offset the offset to read from.
     * @return {@link Stream} of events stored in that {@literal EventStore}.
     */
    Stream<Event> readFrom(long offset);

    /**
     * see {@link JEventStore#readBy(UUID)}.
     *
     * @param uuid identifier of event uuid to read.
     * @return {@link Collection} of events stored in that {@literal EventStore}, grouped by {@literal uuid}.
     */
    Collection<Event> readBy(@Nonnull UUID uuid);

    /**
     * see {@link JEventStore#write(Event)}.
     *
     * @param event is an event to store.
     */
    void write(@Nonnull Event event);

    /**
     * see {@link JEventStore#write(Event...)}.
     *
     * @param events is an events to store.
     */
    default void write(@Nonnull Event... events) {
        for (Event event : events) {
            write(event);
        }
    }

    /**
     * see {@link JEventStore#deleteBy(UUID)}.
     *
     * @param uuid of stream  to delete.
     */
    void deleteBy(@Nonnull UUID uuid);

}
