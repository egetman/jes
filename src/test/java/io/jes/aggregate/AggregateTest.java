package io.jes.aggregate;

import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.jupiter.api.Test;

import io.jes.Event;

import static org.junit.jupiter.api.Assertions.*;

class AggregateTest {

    @Test
    void notOverridedUuidInvokationShouldThrowIllegalStateException() {
        assertThrows(IllegalStateException.class, () -> new Aggregate() {

            @Nullable
            @Override
            public <T extends Event> Consumer<T> applierFor(@Nonnull Class<T> type) {
                return null;
            }
        }.uuid());
    }

}