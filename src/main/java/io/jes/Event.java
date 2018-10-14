package io.jes;

import java.util.UUID;
import javax.annotation.Nullable;

public interface Event {

    @Nullable
    default UUID uuid() {
        return null;
    }

    default long expectedStreamVersion() {
        return -1;
    }

}
