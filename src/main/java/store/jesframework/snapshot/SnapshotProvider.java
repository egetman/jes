package store.jesframework.snapshot;

import java.lang.reflect.Constructor;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;

import store.jesframework.Aggregate;
import store.jesframework.ex.AggregateCreationException;

public interface SnapshotProvider {

    /**
     * Returns initial state for given {@literal type} aggregate.
     *
     * @param uuid is event stream (aggregate) identifier.
     * @param type is the class of aggregate.
     * @param <T>  is the type of aggregate.
     * @return aggregate of type {@literal T} initialized with initial state.
     */
    @Nonnull
    default <T extends Aggregate> T initialStateOf(@Nonnull UUID uuid, @Nonnull Class<T> type) {
        Objects.requireNonNull(type, "Aggregate type must not be null");
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
        // do nothing - default impl
        return Objects.requireNonNull(aggregate, "Aggregate must not be null");
    }

    /**
     * Resets the concrete aggregate snapshot by given {@code uuid}.
     *
     * @param uuid is an event stream (aggregate) identifier to remove.
     */
    @SuppressWarnings("unused")
    default void reset(@Nonnull UUID uuid) {
        // do nothing - default impl
        Objects.requireNonNull(uuid, "Uuid must not be null");
    }
}
