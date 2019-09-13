package store.jesframework.serializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import store.jesframework.internal.Events;


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
    void shouldReturnAllBatchRegisteredAliases() {
        final TypeRegistry registry = new TypeRegistry();
        final Map<Class<?>, String> classesToAliases = new HashMap<>();
        classesToAliases.put(Events.SampleEvent.class, "Sample");
        classesToAliases.put(Events.FancyEvent.class, "Fancy");

        registry.addAliases(classesToAliases);

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
        Assertions.assertThrows(NullPointerException.class, () -> new TypeRegistry().addAliases(null));
    }


}