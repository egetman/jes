package io.jes;

import java.util.Collection;
import java.util.UUID;
import javax.annotation.Nonnull;

import io.jes.snapshot.NoopSnapshotProvider;
import io.jes.snapshot.SnapshotProvider;

import static java.util.Objects.requireNonNull;

public class AggregateStoreImpl implements AggregateStore {

    private final JEventStore eventStore;
    private final SnapshotProvider snapshotter;

    @SuppressWarnings({"WeakerAccess", "unused"})
    public AggregateStoreImpl(@Nonnull JEventStore eventStore) {
        this(eventStore, new NoopSnapshotProvider());
    }

    @SuppressWarnings("WeakerAccess")
    public AggregateStoreImpl(@Nonnull JEventStore eventStore, @Nonnull SnapshotProvider snapshotProvider) {
        this.eventStore = requireNonNull(eventStore, "Event Store must not be null");
        this.snapshotter = requireNonNull(snapshotProvider, "Snapshot Provider must not be null");
    }

    @Nonnull
    @Override
    public <T extends Aggregate> T readBy(@Nonnull UUID uuid, @Nonnull Class<T> type) {
        final T aggregate = snapshotter.initialStateOf(uuid, requireNonNull(type, "Aggregate type must not be null"));
        final Collection<Event> events = ((JEventStoreImpl) eventStore).readBy(uuid, aggregate.streamVersion());
        aggregate.handleEventStream(events);
        return snapshotter.snapshot(aggregate);
    }

}
