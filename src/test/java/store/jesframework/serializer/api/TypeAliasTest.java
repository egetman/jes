package store.jesframework.serializer.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TypeAliasTest {

    @Test
    void shouldHandleInvariants() {
        assertThrows(NullPointerException.class, () -> TypeAlias.ofShortName(null));
        assertThrows(NullPointerException.class, () -> TypeAlias.of(null, ""));
        assertThrows(NullPointerException.class, () -> TypeAlias.of(Object.class, null));
    }

}