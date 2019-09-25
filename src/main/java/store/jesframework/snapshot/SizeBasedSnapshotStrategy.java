package store.jesframework.snapshot;

import java.util.Collection;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import store.jesframework.Aggregate;
import store.jesframework.Event;

/**
 * This strategy applied to the count of the loaded events. If it raised more than {@code snapshotAfter}, a snapshot
 * will be done.
 */
public class SizeBasedSnapshotStrategy implements SnapshotStrategy {

    private final int snapshotAfter;

    @SuppressWarnings("WeakerAccess")
    public SizeBasedSnapshotStrategy(@Nonnegative int snapshotAfter) {
        if (snapshotAfter <= 0) {
            throw new IllegalArgumentException("Snapshot size must be positive: " + snapshotAfter);
        }
        this.snapshotAfter = snapshotAfter;
    }

    @Override
    public boolean isSnapshotNecessary(@Nullable Aggregate aggregate, @Nonnull Collection<Event> events) {
        return events.size() > snapshotAfter;
    }
}
