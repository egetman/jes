package io.jes.snapshot;

import java.lang.reflect.Constructor;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;

import io.jes.Aggregate;
import io.jes.ex.AggregateCreationException;

public interface SnapshotProvider {

    /**
     * Returns initial state for given {@literal type} aggregate.
     *
     * @param uuid is event stream (aggregate) identifier.
     * @param type is the class of aggregate.
     * @param <T>  is the type of agregate.
     * @return aggregate of type {@literal T} initialized with initial state.
     */
    @Nonnull
    default <T extends Aggregate> T initialStateOf(@Nonnull UUID uuid, @Nonnull Class<T> type) {
        try {
            final Constructor<T> constructor = type.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new AggregateCreationException(type, e);
        }
    }

    @Nonnull
    default <T extends Aggregate> T snapshot(@Nonnull T aggregate) {
        return Objects.requireNonNull(aggregate, "Aggregate must not be null");
        // do nothing - default impl
    }

    /**
     * Resets whole snapshot store from aggregates snapshots.
     */
    @SuppressWarnings("unused")
    default void reset(@Nonnull UUID uuid) {
        Objects.requireNonNull(uuid, "Uuid must not be null");
        // do nothing - default impl
    }
}
