package store.jesframework;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class EventTest {

    @Test
    void defaultEventBehaviorShouldBeAsExpected() {
        Event event = new Event() {};

        assertNull(event.uuid());
        assertEquals(-1, event.expectedStreamVersion());
    }

}