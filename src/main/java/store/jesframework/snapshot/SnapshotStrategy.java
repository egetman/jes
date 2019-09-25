package store.jesframework.snapshot;

import java.util.Collection;

import javax.annotation.Nonnull;

import store.jesframework.Aggregate;
import store.jesframework.Event;

public interface SnapshotStrategy {

    /**
     * Indicates if aggregate snaphsot is necessary to do.
     *
     * @param aggregate is an aggregate with latest state (i.e. replayed all the events).
     * @param events    are loaded events from the last snapshot point, or from the beggining if there is no spanshots
     *                  yet.
     * @return true if snapshot is necessary, false otherwise.
     */
    boolean isSnapshotNecessary(@Nonnull Aggregate aggregate, @Nonnull Collection<Event> events);
}
