package io.jes;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AggregateTest {

    @Test
    void notOverridedUuidInvokationShouldThrowNullPointerException() {
        assertThrows(NullPointerException.class, () -> new Aggregate().uuid());
    }

    @Test
    void newAggregateShouldHave0StreamVersion() {
        assertEquals(0, new Aggregate().streamVersion());
    }
}