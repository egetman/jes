package io.jes;

import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Central domain object of {@literal Event Store}. A fact of any change in state.
 */
public interface Event {

    /**
     * Stream uuid.
     * If the event is a part of aggregate, it will return it's uuid, null otherwise.
     *
     * @return stream (aggregate) uuid, if present.
     */
    @Nullable
    default UUID uuid() {
        return null;
    }

    /**
     * @return expected stream (aggregate) version. Used for optimistic locking in concurrent env's (like user
     * interaction).
     */
    default long expectedStreamVersion() {
        return -1;
    }

}
