package io.jes;

import javax.annotation.Nullable;

public interface Event {

    @Nullable
    default String stream() {
        return null;
    }

    default long expectedStreamVersion() {
        return -1;
    }

}
