package io.jes;

import java.util.Collection;
import java.util.UUID;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public interface JEventStore {

    /**
     * Returns all events of the Event Store from given offset.
     *
     * <p>A Stream potentially wraps underlying data store-specific resources and must, therefore, be closed after
     * usage. You can either manually close the Stream by using the close() method or by using a Java 7
     * try-with-resources block, as shown in the following example:
     * <code>
     *
     * <p>try (Stream{@code <}Event{@code >} uuid = store.readFrom(0)) {
     * uuid.forEach(…);
     * }
     * </code>
     *
     * @param offset the offset to read from.
     * @return {@link Stream} of events stored in that {@literal EventStore}.
     */
    Stream<Event> readFrom(long offset);

    /**
     * Returns all events grouped by {@literal event uuid identifier}, also known as an {@literal aggregate
     * identifier}.
     *
     * @param uuid identifier of event uuid to read.
     * @return {@link Collection} of events stored in that {@literal EventStore}, grouped by {@literal uuid}.
     * @throws NullPointerException if uuid is null.
     * @throws io.jes.ex.EmptyEventStreamException if event stream with given {@code uuid} not found.
     */
    Collection<Event> readBy(@Nonnull UUID uuid);

    /**
     * Write given event into {@literal Event Store}.
     * {@implNote there is no guarantee that write operation will be performed in sync manner}.
     *
     * @param event is an event to store.
     * @throws NullPointerException if event is null.
     */
    void write(@Nonnull Event event);

    /**
     * Delete a whole stream by it's {@literal uuid} (its a safe operation).
     *
     * @param uuid of stream  to delete.
     */
    void deleteBy(@Nonnull UUID uuid);

    /**
     * Copy whole contents of this {@literal Event Store} into given one.
     * {@implNote it's implementation specific to use STW pause during this operation}.
     *
     * @param store is an Event Store to copy all events.
     * @throws NullPointerException if store is null.
     * @see <a href="https://leanpub.com/esversioning/read#leanpub-auto-copy-transform">Copy-Transform pattern</a>.
     */
    void copyTo(@Nonnull JEventStore store);

    /**
     * Copy whole contents of this {@literal Event Store} into given one with possible events change/transformation.
     * {@implNote it's implementation specific to use STW pause during this operation}.
     *
     * @param store   is an Event Store to copy all events.
     * @param handler is an event transformator. It can transform one given event into another before storing it in
     *                the given {@literal store}.
     * @throws NullPointerException if store or handler is null.
     * @see <a href="https://leanpub.com/esversioning/read#leanpub-auto-copy-transform">Copy-Transform pattern</a>.
     */
    void copyTo(@Nonnull JEventStore store, @Nonnull UnaryOperator<Event> handler);

}
