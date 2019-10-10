package store.jesframework;

import java.util.Collection;
import java.util.UUID;
import javax.annotation.Nonnull;

import store.jesframework.snapshot.AlwaysSnapshotStrategy;
import store.jesframework.snapshot.NoopSnapshotProvider;
import store.jesframework.snapshot.SnapshotProvider;
import store.jesframework.snapshot.SnapshotStrategy;

import static java.util.Objects.requireNonNull;

/**
 * Base store for working with aggregates.
 */
public class AggregateStore {

    private final JEventStore eventStore;
    private final SnapshotProvider snapshotProvider;
    private final SnapshotStrategy snapshotStrategy;

    @SuppressWarnings({"unused"})
    public AggregateStore(@Nonnull JEventStore eventStore) {
        this(eventStore, new NoopSnapshotProvider());
    }

    public AggregateStore(@Nonnull JEventStore eventStore, @Nonnull SnapshotProvider snapshotProvider) {
        this(eventStore, snapshotProvider, new AlwaysSnapshotStrategy());
    }

    public AggregateStore(@Nonnull JEventStore eventStore, @Nonnull SnapshotProvider snapshotProvider,
                          @Nonnull SnapshotStrategy snapshotStrategy) {
        this.eventStore = requireNonNull(eventStore, "Event Store must not be null");
        this.snapshotProvider = requireNonNull(snapshotProvider, "Snapshot Provider must not be null");
        this.snapshotStrategy = requireNonNull(snapshotStrategy, "Snapshot Strategy must not be null");
    }

    /**
     * This method provide the ability to retrieve the delegate instance.
     *
     * @return the underlying event store.
     */
    public JEventStore unwrap() {
        return eventStore;
    }

    /**
     * Returns specified aggregate of type {@code type} with restored state from {@link JEventStore}. Note: if {@link
     * SnapshotProvider} was specified during {@link AggregateStore} initialization, snapshotting (aggregate caching)
     * will be performed on {@code this#readBy(UUID, Class)} calls based on {@link SnapshotStrategy} implementation.
     *
     * @param uuid identifier of the event stream (uuid) to read.
     * @param type class of the aggregate to load.
     * @param <T>  type of the aggregate.
     * @return recreated/restored form {@link JEventStore} aggregate instance.
     * @throws NullPointerException if any of {@code uuid}/{@code type} is null.
     */
    @Nonnull
    public <T extends Aggregate> T readBy(@Nonnull UUID uuid, @Nonnull Class<T> type) {
        final T aggregate = snapshotProvider.initialStateOf(uuid, type);
        return readBy(uuid, aggregate);
    }

    /**
     * Returns specified aggregate with refreshed state from {@link JEventStore}. Note: if {@link SnapshotProvider} was
     * specified during {@link AggregateStore} initialization, snapshotting (aggregate caching) will be performed on
     * {@code this#readBy(UUID, Class)} calls based on {@link SnapshotStrategy} implementation.
     *
     * @param uuid      identifier of the event stream (uuid) to read.
     * @param aggregate instance of the aggregate to refresh.
     * @param <T>       type of the aggregate.
     * @return refreshed form {@link JEventStore} aggregate instance.
     * @throws NullPointerException if any of {@code uuid}/{@code type} is null.
     */
    @Nonnull
    public <T extends Aggregate> T readBy(@Nonnull UUID uuid, @Nonnull T aggregate) {
        final Collection<Event> events = eventStore.readBy(uuid, aggregate.streamVersion());
        if (events.isEmpty()) {
            return aggregate;
        }
        aggregate.handleEventStream(events);
        if (snapshotStrategy.isSnapshotNecessary(aggregate, events)) {
            return snapshotProvider.snapshot(aggregate);
        }
        return aggregate;
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
