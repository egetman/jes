package store.jesframework.common;

import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamSplittedToTest {

    @Test
    @SuppressWarnings("ConstantConditions")
    void streamSplittedToEventShouldProtectItsInvariants() {
        final UUID uuid = randomUUID();
        final Set<UUID> emptySet = emptySet();
        final Set<UUID> uuids = singleton(uuid);
        assertThrows(NullPointerException.class, () -> new StreamSplittedTo(null, uuids));
        assertThrows(NullPointerException.class, () -> new StreamSplittedTo(uuid, null));
        assertThrows(IllegalArgumentException.class, () -> new StreamSplittedTo(uuid, emptySet));
    }

}