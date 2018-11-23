package io.jes;

import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void newAggregateShouldHave0StreamVersion() {
        assertEquals(0, new Aggregate() {
            @Nullable
            @Override
            public <T extends Event> Consumer<T> applierFor(@Nonnull Class<T> type) {
                return null;
            }
        }.streamVersion());
    }
}