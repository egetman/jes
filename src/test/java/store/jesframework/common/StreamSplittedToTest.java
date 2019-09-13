package store.jesframework.common;

import org.junit.jupiter.api.Test;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamSplittedToTest {

    @Test
    @SuppressWarnings("ConstantConditions")
    void streamSplittedToEventShouldProtectItsInvariants() {
        assertThrows(NullPointerException.class, () -> new StreamSplittedTo(null, singleton(randomUUID())));
        assertThrows(NullPointerException.class, () -> new StreamSplittedTo(randomUUID(), null));
        assertThrows(IllegalArgumentException.class, () -> new StreamSplittedTo(randomUUID(), emptySet()));
    }

}