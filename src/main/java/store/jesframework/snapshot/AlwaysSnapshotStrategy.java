package store.jesframework.snapshot;

import java.util.Collection;
import javax.annotation.Nullable;

import store.jesframework.Aggregate;
import store.jesframework.Event;

/**
 *  A simple strategy that always tells to do a snapshot.
 */
public class AlwaysSnapshotStrategy implements SnapshotStrategy {

    @Override
    public boolean isSnapshotNecessary(@Nullable Aggregate aggregate, @Nullable Collection<Event> events) {
        return true;
    }
}
