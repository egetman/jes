package io.jes.snapshot;

import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;

import io.jes.Aggregate;
import io.jes.ex.AggregateCreationException;

public interface SnapshotProvider {

    @Nonnull
    @SuppressWarnings("unused")
    default <T extends Aggregate> T initialStateOf(@Nonnull UUID uuid, @Nonnull Class<T> type) {
        try {
            return type.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new AggregateCreationException(type, e);
        }
    }

    @Nonnull
    default <T extends Aggregate> T snapshot(@Nonnull T aggregate) {
        return Objects.requireNonNull(aggregate, "Aggregate must not be null");
    }

    /**
     * Resets whole snapshot store from aggregates snapshots.
     */
    @SuppressWarnings("unused")
    default void reset() {
        // do nothing
    }
}
