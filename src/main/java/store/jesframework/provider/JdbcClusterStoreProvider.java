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
import store.jesframework.serializer.api.SerializationOption;
import store.jesframework.snapshot.SnapshotReader;

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
    public static final int MAX_TRACKED_TIME = 5;

    /**
     * Fair enough(?) amount of writes to be tracked until the replication lag passes.
     */
    public static final int MAX_TRACKED_WRITES = 1500;

    private final JdbcStoreProvider<T> master;
    private final List<JdbcStoreProvider<T>> replicas = new ArrayList<>();
    private final Cache<UUID, Object> writesTracker =
            Cache2kBuilder.of(UUID.class, Object.class).name(getClass()).permitNullValues(true).boostConcurrency(true).entryCapacity(MAX_TRACKED_WRITES).expireAfterWrite(MAX_TRACKED_TIME, TimeUnit.MINUTES).build();

    public JdbcClusterStoreProvider(@Nonnull DataSource master, @Nullable Collection<DataSource> replicas,
                                    @Nullable SerializationOption... options) {
        this.master = new JdbcStoreProvider<>(master, options);
        if (replicas == null || replicas.isEmpty()) {
            this.replicas.add(this.master);
        } else {
            for (DataSource replica : replicas) {
                this.replicas.add(new JdbcStoreProvider<>(replica, options));
            }
        }
    }

    public JdbcClusterStoreProvider(@Nonnull DataSource master, @Nullable DataSource... replicas) {
        this(master, replicas != null ? Arrays.asList(replicas) : null);
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

    @Override
    public Collection<Event> readBy(@Nonnull UUID uuid, long skip) {
        if (isTracked(uuid)) {
            return master.readBy(uuid, skip);
        }
        return nextReplica().readBy(uuid, skip);
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
        return writesTracker.get(uuid) != null;
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
    }

}
