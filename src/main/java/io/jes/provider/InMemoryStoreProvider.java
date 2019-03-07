package io.jes.provider;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import io.jes.Event;
import io.jes.ex.VersionMismatchException;

/**
 * In-memory {@link StoreProvider} implementation.
 */
public class InMemoryStoreProvider implements StoreProvider {

    private final List<Event> events = new CopyOnWriteArrayList<>();
    private final Map<UUID, LongAdder> streamsVersions = new ConcurrentHashMap<>();

    @Override
    public Stream<Event> readFrom(long offset) {
        return events.subList((int) offset, events.size()).stream();
    }

    // search for O(n)
    @Override
    public Collection<Event> readBy(@Nonnull UUID uuid) {
        return events.stream().filter(event -> uuid.equals(event.uuid())).collect(Collectors.toList());
    }

    // todo: need to make it thread safe to write versioned events
    @Override
    public void write(@Nonnull Event event) {
        if (event.uuid() != null) {
            streamsVersions.putIfAbsent(event.uuid(), new LongAdder());
            final LongAdder actualVersion = streamsVersions.get(event.uuid());
            final long expectedVersion = event.expectedStreamVersion();
            if (expectedVersion != -1 && actualVersion.longValue() != expectedVersion) {
                throw new VersionMismatchException(expectedVersion, actualVersion.longValue());
            }
        }
        events.add(event);
        if (event.uuid() != null) {
            streamsVersions.get(event.uuid()).increment();
        }
    }

    @Override
    public void deleteBy(@Nonnull UUID uuid) {
        events.removeIf(event -> uuid.equals(event.uuid()));
        streamsVersions.remove(uuid);
    }
}
