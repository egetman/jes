package io.jes;

import java.util.Collection;
import java.util.UUID;
import javax.annotation.Nonnull;

import io.jes.snapshot.NoopSnapshotProvider;
import io.jes.snapshot.SnapshotProvider;

import static java.util.Objects.requireNonNull;

/**
 * Base store for working with aggregates.
 */
public class AggregateStore {

    final SnapshotProvider snapshotter;
    private final JEventStore eventStore;

    @SuppressWarnings({"unused", "WeakerAccess"})
    public AggregateStore(@Nonnull JEventStore eventStore) {
        this(eventStore, new NoopSnapshotProvider());
    }

    @SuppressWarnings("WeakerAccess")
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
     * @return recteated/restored form {@link JEventStore} aggregate instance.
     * @throws NullPointerException if any of {@code uuid}/{@code type} is null.
     */
    @Nonnull
    public <T extends Aggregate> T readBy(@Nonnull UUID uuid, @Nonnull Class<T> type) {
        final T aggregate = snapshotter.initialStateOf(uuid, requireNonNull(type, "Aggregate type must not be null"));
        final Collection<Event> events = eventStore.readBy(uuid, aggregate.streamVersion());
        aggregate.handleEventStream(events);
        return snapshotter.snapshot(aggregate);
    }

}
