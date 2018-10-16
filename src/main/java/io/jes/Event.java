package io.jes;

import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Central domain object of {@literal Event Store}. A fact of any change in state.
 */
public interface Event {

    @Nullable
    default UUID uuid() {
        return null;
    }

    default long expectedStreamVersion() {
        return -1;
    }

}
