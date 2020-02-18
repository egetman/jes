package store.jesframework;

import java.util.Collection;
import java.util.UUID;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import store.jesframework.provider.StoreProvider;
import store.jesframework.snapshot.SnapshotReader;

import static java.util.Objects.requireNonNull;

public class JEventStore {

    private static final String NON_NULL_UUID = "Event stream uuid must not be null";

    private final StoreProvider provider;
    private final boolean canReadSnapshots;

    public JEventStore(@Nonnull StoreProvider provider) {
        this.provider = requireNonNull(provider, "StoreProvider must not be null");
        this.canReadSnapshots = provider instanceof SnapshotReader;
    }

    /**
     * Returns all events of the Event Store from given offset.
     *
     * <p>A Stream potentially wraps underlying data store-specific resources and must, therefore, be closed after
     * usage. You can either manually close the Stream by using the close() method or by using a Java 7
     * try-with-resources block, as shown in the following example:
     * <pre>
     * {@code
     * try (Stream<Event> uuid = store.readFrom(0)) {
     *      uuid.forEach(…);
     * }
     * }
     * </pre>
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
     */
    public Collection<Event> readBy(@Nonnull UUID uuid) {
        return provider.readBy(requireNonNull(uuid, NON_NULL_UUID));
    }

    Collection<Event> readBy(@Nonnull UUID uuid, long skip) {
        if (skip == 0) {
            return readBy(uuid);
        }
        if (skip < 0) {
            throw new IllegalArgumentException("'skip' argument must be greater than 0. Actual: " + skip);
        }
        if (!canReadSnapshots) {
            throw new IllegalStateException("The current provider doesn't support snapshotting");
        }
        return ((SnapshotReader) provider).readBy(requireNonNull(uuid, NON_NULL_UUID), skip);
    }

    /**
     * Write a given event into the {@literal Event Store}.
     * {@implNote there is no guarantee that write operation will be performed in sync manner}.
     *
     * @param event is an event to store.
     * @throws NullPointerException if event is null.
     */
    public void write(@Nonnull Event event) {
        provider.write(requireNonNull(event, "Event must not be null"));
    }

    /**
     * Write given events into {@literal Event Store}.
     * {@implNote there is no guarantee that write operation will be performed in sync manner}.
     *
     * @param events are events to store.
     * @throws NullPointerException if events is null.
     */
    public void write(@Nonnull Event... events) {
        provider.write(events);
    }

    /**
     * Delete a whole stream by its {@literal uuid} (it's a safe operation).
     * Note: not all {@link StoreProvider} can support deletion.
     *
     * @param uuid of stream to delete.
     * @throws UnsupportedOperationException if the underlying provider does not support delete operation.
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
     * @see <a href="https://leanpub.com/esversioning/read#leanpub-auto-copy-transform">Copy-Transform pattern</a>
     */
    @SuppressWarnings("WeakerAccess")
    public void copyTo(@Nonnull JEventStore store) {
        this.copyTo(store, UnaryOperator.identity());
    }

    //todo: to sync or not to sync? need to verify that no events lost after first transfer.
    /**
     * Copy whole contents of this {@literal Event Store} into given one with possible events change/transformation.
     * {@implNote it's implementation specific to use STW pause during this operation}.
     *
     * @param store   is an Event Store to copy all events.
     * @param handler is an event transformer. It can transform one given event into another before storing it in
     *                the given {@literal store}.
     * @throws NullPointerException if store or handler is null.
     * @see <a href="https://leanpub.com/esversioning/read#leanpub-auto-copy-transform">Copy-Transform pattern</a>
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
