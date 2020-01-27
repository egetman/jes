package store.jesframework.provider;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.cache2k.Cache;
import org.cache2k.Cache2kBuilder;

import store.jesframework.Event;
import store.jesframework.ex.BrokenStoreException;
import store.jesframework.serializer.api.SerializationOption;
import store.jesframework.snapshot.SnapshotReader;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * This implementation is similar to {@link JdbcStoreProvider} except reads distribution. It's intended to be used in
 * cluster environments to use replicas for querying. It's provides {@literal read own writes} guarantee by caching last
 * requests (only for events with aggregate uuid).
 *
 * @param <T> type of event serialization.
 */
public class JdbcClusterStoreProvider<T> implements StoreProvider, SnapshotReader, AutoCloseable {

    /**
     * Fair enough(?) amount of time to avoid replication lag issues.
     */
    private static final int MAX_TRACKED_TIME = 5;

    private final JdbcStoreProvider<T> master;
    private final List<JdbcStoreProvider<T>> replicas = new ArrayList<>();
    private final Cache<UUID, Object> writesTracker;

    /**
     * Builds a {@link JdbcClusterStoreProvider} instance by given params.
     *
     * @param master   is a datasource that will handle all the writes and querying events by its {@link Event#uuid()}
     *                 (if such an event is tracked).
     * @param replicas are collection of datasources that will handle the sequential events reads.
     * @param timeout  the duration of events tracking. Must be greater than a replication lag. Note: cache has no max
     *                 entries size, so all tracked within {@code timeout} event uuid's will be kept in-memory.
     * @param timeUnit is just a time unit for {@code timeout}.
     * @param options  are serialization extensions.
     */
    @SuppressWarnings("squid:S2589")
    public JdbcClusterStoreProvider(@Nonnull DataSource master, @Nullable Collection<DataSource> replicas, int timeout,
                                    @Nonnull TimeUnit timeUnit, @Nullable SerializationOption... options) {
        //noinspection ConstantConditions
        if (timeout <= 0 || timeUnit == null) {
            throw new BrokenStoreException("Timeout must be > 0, timeunit must not be null: " + timeout + timeUnit);
        }

        this.master = new JdbcStoreProvider<>(master, options);
        if (replicas == null || replicas.isEmpty()) {
            this.replicas.add(this.master);
        } else {
            for (DataSource replica : replicas) {
                this.replicas.add(new JdbcStoreProvider<>(replica, true, options));
            }
        }
        writesTracker = Cache2kBuilder.of(UUID.class, Object.class)
                .name(getClass())
                .permitNullValues(true)
                .boostConcurrency(true)
                .entryCapacity(Long.MAX_VALUE)
                .expireAfterWrite(timeout, timeUnit)
                .build();
    }

    public JdbcClusterStoreProvider(@Nonnull DataSource master, @Nullable DataSource... replicas) {
        this(master, replicas != null ? Arrays.asList(replicas) : null, MAX_TRACKED_TIME, MINUTES);
    }

    @Override
    public Stream<Event> readFrom(long offset) {
        // it's ok to sequentially read events from replicas
        return nextReplica().readFrom(offset);
    }

    @Override
    public Collection<Event> readBy(@Nonnull UUID uuid) {
        if (isTracked(uuid)) {
            return master.readBy(uuid);
        }
        return nextReplica().readBy(uuid);
    }

    @Override
    public Collection<Event> readBy(@Nonnull UUID uuid, long skip) {
        if (isTracked(uuid)) {
            return master.readBy(uuid, skip);
        }
        return nextReplica().readBy(uuid, skip);
    }

    @Override
    public void write(@Nonnull Event event) {
        track(event.uuid());
        master.write(event);
    }

    @Override
    public void write(@Nonnull Event... events) {
        for (Event event : events) {
            track(event.uuid());
        }
        master.write(events);
    }

    @Override
    public void deleteBy(@Nonnull UUID uuid) {
        // in case of deletion we must track deleted events to avoid read stale information from a replica, where given
        // event (or event stream) may still be present.
        track(uuid);
        master.deleteBy(uuid);
    }

    private void track(@Nullable UUID uuid) {
        if (uuid != null) {
            writesTracker.put(uuid, null);
        }
    }

    private boolean isTracked(@Nullable UUID uuid) {
        if (uuid == null) {
            return false;
        }
        return writesTracker.containsKey(uuid);
    }

    private JdbcStoreProvider<T> nextReplica() {
        return replicas.get(ThreadLocalRandom.current().nextInt(replicas.size()));
    }

    @Override
    public void close() {
        master.close();
        for (JdbcStoreProvider<T> replica : replicas) {
            replica.close();
        }
        writesTracker.close();
    }

}
