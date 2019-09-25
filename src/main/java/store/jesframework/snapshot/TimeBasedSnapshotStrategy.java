package store.jesframework.snapshot;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import store.jesframework.Aggregate;
import store.jesframework.Event;

import static java.time.LocalDateTime.now;

/**
 * This strategy applied to the time of the last snapshot. If it more than specidied duration, a snapshot will be done.
 */
public class TimeBasedSnapshotStrategy implements SnapshotStrategy {

    private static final int TRACKING_SIZE = 2000;

    private final Duration snapshotAfter;
    private final Map<UUID, LocalDateTime> tracking;

    @SuppressWarnings("WeakerAccess")
    public TimeBasedSnapshotStrategy(@Nonnull Duration snapshotAfter) {
        this.snapshotAfter = Objects.requireNonNull(snapshotAfter, "Duration must not be null");
        if (snapshotAfter.isNegative()) {
            throw new IllegalArgumentException("Duration must not be negative");
        }
        // mb not the optimal solution
        this.tracking = Collections.synchronizedMap(new LinkedHashMap<UUID, LocalDateTime>(TRACKING_SIZE, .75f, true) {
            @Override
            public boolean removeEldestEntry(Map.Entry<UUID, LocalDateTime> eldest) {
                return size() > TRACKING_SIZE;
            }
        });
    }

    @Override
    public boolean isSnapshotNecessary(@Nonnull Aggregate aggregate, @Nullable Collection<Event> events) {
        final LocalDateTime now = now();
        final LocalDateTime lastSeen = tracking.put(aggregate.uuid(), now);

        if (lastSeen != null) {
            final Duration fromLastSnapshot = Duration.between(lastSeen, now);
            return !fromLastSnapshot.minus(snapshotAfter).isNegative();
        }
        return false;
    }
}
