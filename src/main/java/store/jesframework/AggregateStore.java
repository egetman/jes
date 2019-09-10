package store.jesframework;

import java.util.Collection;
import java.util.UUID;
import javax.annotation.Nonnull;

import store.jesframework.ex.EmptyEventStreamException;
import store.jesframework.snapshot.NoopSnapshotProvider;
import store.jesframework.snapshot.SnapshotProvider;

import static java.util.Objects.requireNonNull;

/**
 * Base store for working with aggregates.
 */
public class AggregateStore {

    final SnapshotProvider snapshotter;
    private final JEventStore eventStore;

    @SuppressWarnings({"unused"})
    public AggregateStore(@Nonnull JEventStore eventStore) {
        this(eventStore, new NoopSnapshotProvider());
    }

    public AggregateStore(@Nonnull JEventStore eventStore, @Nonnull SnapshotProvider snapshotProvider) {
        this.eventStore = requireNonNull(eventStore, "Event Store must not be null");
        this.snapshotter = requireNonNull(snapshotProvider, "Snapshot Provider must not be null");
    }

    /**
     * Returns specified aggregate of type {@code type} with restored state from {@link JEventStore}.
     * Note: if {@link SnapshotProvider} was specified during {@link AggregateStore} initialization, snapshotting
     * (aggregate caching) will be performed on {@code this#readBy(UUID, Class)} calls based on {@link SnapshotProvider}
     * implementation.
     *
     * @param uuid identifier of event stream (uuid) to read.
     * @param type class of aggregate to load
     * @param <T>  type of aggregate.
     * @return recreated/restored form {@link JEventStore} aggregate instance.
     * @throws NullPointerException if any of {@code uuid}/{@code type} is null.
     * @throws EmptyEventStreamException if no event stream found by given {@code uuid}.
     */
    @Nonnull
    public <T extends Aggregate> T readBy(@Nonnull UUID uuid, @Nonnull Class<T> type) {
        final T aggregate = snapshotter.initialStateOf(uuid, requireNonNull(type, "Aggregate type must not be null"));
        final Collection<Event> events = eventStore.readBy(uuid, aggregate.streamVersion());
        if (events.isEmpty()) {
            return aggregate;
        }
        aggregate.handleEventStream(events);
        return snapshotter.snapshot(aggregate);
    }

    /**
     * see {@link JEventStore#write(Event)}.
     *
     * @param event is an event to store.
     * @throws NullPointerException if event is null.
     */
    public void write(@Nonnull Event event) {
        eventStore.write(event);
    }

    /**
     * see {@link JEventStore#write(Event...)}.
     *
     * @param events are events to store.
     * @throws NullPointerException if events is null.
     */
    public void write(@Nonnull Event... events) {
        eventStore.write(events);
    }

}
