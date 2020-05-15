package store.jesframework.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@Execution(CONCURRENT)
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

    @Test
    void shouldBeEqualWithEqualValues() {
        assertEquals(Pair.of("Foo", "Bar"), Pair.of("Foo", "Bar"));
    }

    @Test
    void shouldIncludeObjectsInToString() {
        final Pair<String, String> pair = Pair.of("1", "2");
        assertTrue(pair.toString().contains("1"));
        assertTrue(pair.toString().contains("2"));
    }
}