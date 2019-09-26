package store.jesframework.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PairTest {

    @Test
    void shouldReturnPassedValues() {
        Pair<Integer, String> pair = Pair.of(1, "Foo");
        assertEquals(1, pair.getKey());
        assertEquals("Foo", pair.getValue());
    }

    @Test
    void shouldPermitNullValues() {
        assertDoesNotThrow(() -> Pair.of(null, null));
        final Pair<Object, Object> nullPair = Pair.of(null, null);
        assertNull(nullPair.getKey());
        assertNull(nullPair.getValue());
    }

}