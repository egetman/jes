package store.jesframework.snapshot;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import javax.annotation.Nonnull;

import store.jesframework.Aggregate;
import store.jesframework.ex.AggregateCreationException;

import static java.util.Objects.requireNonNull;

class DefaultSnapshotProvider implements SnapshotProvider {

    private final Map<Class<? extends Aggregate>, BiFunction<UUID, Class<? extends Aggregate>, Aggregate>> conversions
            = new ConcurrentHashMap<>();

    /**
     * Add given rule for aggregate creation to be used as the default creation strategy. It can be useful if aggregate
     * class doesn't have a default constructor. Another point: default instantiation use reflection for aggregate
     * instance creation, so manualy pass the aggregate creation rule can give some performance enhancement.
     *
     * @param type       is a type of Aggregate to create.
     * @param conversion is a function that must return new aggregate instance, never null.
     * @throws NullPointerException       if any of {@code type, conversion} is null.
     * @throws AggregateCreationException if {@code conversion} return null as a conversion result.
     */
    @SuppressWarnings("WeakerAccess")
    public void addAggregateCreator(@Nonnull Class<? extends Aggregate> type,
                                    @Nonnull BiFunction<UUID, Class<? extends Aggregate>, Aggregate> conversion) {
        requireNonNull(type, "Type must not be null");
        requireNonNull(conversion, "Conversion must not be null");

        conversions.put(type, conversion);
    }

    @Nonnull
    @Override
    public <T extends Aggregate> T initialStateOf(@Nonnull UUID uuid, @Nonnull Class<T> type) {
        final BiFunction<UUID, Class<? extends Aggregate>, Aggregate> function = conversions.get(type);
        if (function != null) {
            //noinspection unchecked
            final T aggregate = (T) function.apply(uuid, type);
            if (aggregate == null) {
                throw new AggregateCreationException(type, new IllegalStateException("Aggregate creator return null"));
            }
            return aggregate;
        }
        return SnapshotProvider.super.initialStateOf(uuid, type);
    }
}
