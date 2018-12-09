package io.jes;

import java.util.Collection;
import java.util.UUID;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import io.jes.ex.EmptyEventStreamException;
import io.jes.provider.StoreProvider;
import io.jes.snapshot.SnapshotReader;

import static io.jes.util.Check.nonEmpty;
import static java.util.Objects.requireNonNull;

public class JEventStore {

    private static final String NON_NULL_UUID = "Event stream uuid must not be null";

    private final StoreProvider provider;
    private final boolean canReadSnapshots;

    public JEventStore(StoreProvider provider) {
        this.provider = requireNonNull(provider, "StoreProvider must not be null");
        this.canReadSnapshots = provider instanceof SnapshotReader;
    }

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
    public Stream<Event> readFrom(long offset) {
        return provider.readFrom(offset);
    }

    /**
     * Returns all events grouped by {@literal event uuid identifier}, also known as an {@literal aggregate
     * identifier}.
     *
     * @param uuid identifier of event uuid to read.
     * @return {@link Collection} of events stored in that {@literal EventStore}, grouped by {@literal uuid}.
     * @throws NullPointerException                if uuid is null.
     * @throws io.jes.ex.EmptyEventStreamException if event stream with given {@code uuid} not found.
     */
    public Collection<Event> readBy(@Nonnull UUID uuid) {
        final Collection<Event> events = provider.readBy(requireNonNull(uuid, NON_NULL_UUID));
        nonEmpty(events, () -> new EmptyEventStreamException("Event stream with uuid " + uuid + " not found"));
        return events;
    }

    Collection<Event> readBy(@Nonnull UUID uuid, long skip) {
        if (skip == 0) {
            return readBy(uuid);
        }
        if (skip < 0) {
            throw new IllegalArgumentException("'skip' argument must be greater than 0. Actual: " + skip);
        }
        if (!canReadSnapshots) {
            throw new IllegalStateException("Current provider doesn't support snapshotting");
        }
        return ((SnapshotReader) provider).readBy(requireNonNull(uuid, NON_NULL_UUID), skip);
    }

    /**
     * Write given event into {@literal Event Store}.
     * {@implNote there is no guarantee that write operation will be performed in sync manner}.
     *
     * @param event is an event to store.
     * @throws NullPointerException if event is null.
     */
    public void write(@Nonnull Event event) {
        provider.write(requireNonNull(event, "Event must not be null"));
    }

    /**
     * Delete a whole stream by it's {@literal uuid} (its a safe operation).
     *
     * @param uuid of stream  to delete.
     */
    public void deleteBy(@Nonnull UUID uuid) {
        provider.deleteBy(requireNonNull(uuid, NON_NULL_UUID));
    }

    /**
     * Copy whole contents of this {@literal Event Store} into given one.
     * {@implNote it's implementation specific to use STW pause during this operation}.
     *
     * @param store is an Event Store to copy all events.
     * @throws NullPointerException if store is null.
     * @see <a href="https://leanpub.com/esversioning/read#leanpub-auto-copy-transform">Copy-Transform pattern</a>.
     */
    @SuppressWarnings("WeakerAccess")
    public void copyTo(@Nonnull JEventStore store) {
        this.copyTo(store, UnaryOperator.identity());
    }

    /**
     * Copy whole contents of this {@literal Event Store} into given one with possible events change/transformation.
     * {@implNote it's implementation specific to use STW pause during this operation}.
     *
     * @param store   is an Event Store to copy all events.
     * @param handler is an event transformator. It can transform one given event into another before storing it in
     *                the given {@literal store}.
     * @throws NullPointerException if store or handler is null.
     * @see <a href="https://leanpub.com/esversioning/read#leanpub-auto-copy-transform">Copy-Transform pattern</a>.
     * todo: to sync or not to sync? need to verify that no events lost after first transfer.
     */
    @SuppressWarnings("WeakerAccess")
    public void copyTo(@Nonnull JEventStore store, @Nonnull UnaryOperator<Event> handler) {
        requireNonNull(store, "Store must not be null");
        requireNonNull(handler, "Handler must not be null");
        try (final Stream<Event> stream = readFrom(0)) {
            stream.map(handler).forEach(store::write);
        }
    }

}
