package io.jes.snapshot;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;

import io.jes.Aggregate;

public class InMemorySnapshotProvider implements SnapshotProvider {

    private static final int MAX_CACHE_SIZE = 5000;
    private final Map<UUID, Aggregate> cache;

    @SuppressWarnings("unused")
    public InMemorySnapshotProvider() {
        this(MAX_CACHE_SIZE);
    }

    @SuppressWarnings("WeakerAccess")
    public InMemorySnapshotProvider(int cacheSize) {
        this.cache = new LinkedHashMap<UUID, Aggregate>(cacheSize, .75f, true) {
            @Override
            public boolean removeEldestEntry(Map.Entry<UUID, Aggregate> eldest) {
                return size() > cacheSize;
            }
        };
    }

    @Nonnull
    @Override
    public <T extends Aggregate> T initialStateOf(@Nonnull UUID uuid, @Nonnull Class<T> type) {
        final Aggregate aggregate = cache.get(Objects.requireNonNull(uuid, "Aggregate uuid must not be null"));
        if (aggregate == null) {
            return SnapshotProvider.super.initialStateOf(uuid, type);
        }
        //noinspection unchecked
        return (T) aggregate;
    }

    @Nonnull
    @Override
    public <T extends Aggregate> T snapshot(@Nonnull T aggregate) {
        cache.put(aggregate.uuid(), aggregate);
        return aggregate;
    }

    @Override
    public void reset() {
        cache.clear();
    }
}
