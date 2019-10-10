package store.jesframework.provider;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import store.jesframework.Event;
import store.jesframework.ex.VersionMismatchException;

/**
 * In-memory {@link StoreProvider} implementation.
 */
public class InMemoryStoreProvider implements StoreProvider {

    private final List<Event> events = new CopyOnWriteArrayList<>();
    private final Map<UUID, LongAdder> streamsVersions = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    @Override
    public Stream<Event> readFrom(long offset) {
        return events.stream().skip(offset);
    }

    // search for O(n)
    @Override
    public Collection<Event> readBy(@Nonnull UUID uuid) {
        return events.stream().filter(event -> uuid.equals(event.uuid())).collect(Collectors.toList());
    }

    // not optimal exclusive write
    @Override
    public void write(@Nonnull Event event) {
        try {
            lock.writeLock().lock();
            if (event.uuid() != null) {
                final long expectedVersion = event.expectedStreamVersion();
                // check current event stream version
                if (expectedVersion != -1) {
                    final LongAdder actual = streamsVersions.computeIfAbsent(event.uuid(), uuid -> new LongAdder());
                    if (actual.longValue() != expectedVersion) {
                        throw new VersionMismatchException(event.uuid(), expectedVersion, actual.longValue());
                    }
                }
            }
            events.add(event);
            // update written event stream version
            if (event.uuid() != null) {
                streamsVersions.computeIfAbsent(event.uuid(), uuid -> new LongAdder()).increment();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void deleteBy(@Nonnull UUID uuid) {
        events.removeIf(event -> uuid.equals(event.uuid()));
        streamsVersions.remove(uuid);
    }
}
