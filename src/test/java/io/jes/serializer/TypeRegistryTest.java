package io.jes.serializer;

import java.util.Collections;
import java.util.HashMap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.jes.internal.Events;


class TypeRegistryTest {

    @Test
    void shouldReturnAllRegisteredAliases() {
        final TypeRegistry registry = new TypeRegistry();
        registry.addAlias(Events.SampleEvent.class, "Sample");
        registry.addAlias(Events.FancyEvent.class, "Fancy");

        Assertions.assertEquals(new HashMap<Class<?>, String>() {{
            put(Events.SampleEvent.class, "Sample");
            put(Events.FancyEvent.class, "Fancy");
        }}, registry.getAliases());
    }

    @Test
    void shouldReturnEmptyAliasesIfNotRegisterAny() {
        Assertions.assertEquals(Collections.<Class<?>, String>emptyMap(), new TypeRegistry().getAliases());
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    void shouldHandleInvariants() {
        Assertions.assertThrows(NullPointerException.class, () -> new TypeRegistry().addAlias(null, ""));
        Assertions.assertThrows(NullPointerException.class, () -> new TypeRegistry().addAlias(getClass(), null));
    }


}