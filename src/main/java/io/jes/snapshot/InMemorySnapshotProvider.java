package io.jes.snapshot;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;

import io.jes.Aggregate;

public class InMemorySnapshotProvider implements SnapshotProvider {

    private static final int MAX_ENTRIES = 5000;
    private final Map<UUID, Aggregate> cache = new LinkedHashMap<UUID, Aggregate>(MAX_ENTRIES, .75f, true) {
        @Override
        public boolean removeEldestEntry(Map.Entry<UUID, Aggregate> eldest) {
            return size() > MAX_ENTRIES;
        }
    };

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
}
