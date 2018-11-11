package io.jes.aggregate;

import java.util.Collection;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.jes.Event;
import io.jes.JEventStore;
import io.jes.aggregate.snapshot.SnapshottingStrategy;
import io.jes.ex.AggregateCreationException;

public class AggregateStoreImpl implements AggregateStore {

    private final JEventStore eventStore;
    private final SnapshottingStrategy snapshottingStrategy;

    @SuppressWarnings("WeakerAccess")
    public AggregateStoreImpl(@Nonnull JEventStore eventStore) {
        this(eventStore, null);
    }

    @SuppressWarnings("WeakerAccess")
    public AggregateStoreImpl(@Nonnull JEventStore eventStore, @Nullable SnapshottingStrategy snapshottingStrategy) {
        this.eventStore = Objects.requireNonNull(eventStore, "Event Store must not be null");
        this.snapshottingStrategy = snapshottingStrategy != null ? snapshottingStrategy : SnapshottingStrategy.NONE;
    }

    @Nonnull
    @Override
    public <T extends Aggregate> T readBy(@Nonnull UUID uuid, Class<T> type) {
        final Collection<Event> events = eventStore.readBy(uuid);
        final T aggregate = initialStateOf(type);
        aggregate.handleEventStream(events);
        return aggregate;
    }

    @Override
    public void write(@Nonnull Event event) {
        eventStore.write(event);
    }

    <T extends Aggregate> T initialStateOf(Class<T> type) {
        try {
            return type.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new AggregateCreationException(type, e);
        }
    }
}
