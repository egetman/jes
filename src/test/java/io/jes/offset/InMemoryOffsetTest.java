package io.jes.offset;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InMemoryOffsetTest {

    @Test
    @SuppressWarnings("ConstantConditions")
    void shouldThrowNpeOnNullArguments() {
        final Offset offset = new InMemoryOffset();
        assertThrows(NullPointerException.class, () -> offset.value(null));
        assertThrows(NullPointerException.class, () -> offset.increment(null));
        assertThrows(NullPointerException.class, () -> offset.reset(null));
    }

    @Test
    void shouldIncrementOffsetValueByKey() {
        final String key = getClass().getName();
        final Offset offset = new InMemoryOffset();

        assertEquals(0, offset.value(key));
        offset.increment(key);
        assertEquals(1, offset.value(key));
        offset.increment(key);
        assertEquals(2, offset.value(key));
        assertEquals(0, offset.value(UUID.randomUUID().toString()));
    }

    @Test
    void shouldResetOffsetByKey() {
        final String first = UUID.randomUUID().toString();
        final String second = UUID.randomUUID().toString();

        final Offset offset = new InMemoryOffset();

        offset.increment(first);
        offset.increment(first);
        offset.increment(second);

        assertEquals(2, offset.value(first));
        assertEquals(1, offset.value(second));

        offset.reset(first);

        assertEquals(0, offset.value(first));
        assertEquals(1, offset.value(second));
    }
}