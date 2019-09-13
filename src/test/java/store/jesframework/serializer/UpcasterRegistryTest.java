package store.jesframework.serializer;

import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.jupiter.api.Test;

import store.jesframework.serializer.api.Upcaster;

import static java.util.function.UnaryOperator.identity;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UpcasterRegistryTest {

    @Test
    @SuppressWarnings("ConstantConditions")
    void upcasterRegistryShouldProtectItsInvariants() {
        final UpcasterRegistry<String> registry = new UpcasterRegistry<>();
        assertThrows(NullPointerException.class, () -> registry.addUpcaster(null));
        assertThrows(NullPointerException.class, () -> registry.addUpcaster(new SampleUpcaster(null, identity())));
    }

    @Test
    void upcasterRegisrtyShouldReturnRawTypeIfNoUpcastersRegistered() {
        final String event = "{\"@type\":\"ItemCreated\",\"itemName\":\"Dog\",\"quantity\":9}";
        final UpcasterRegistry<String> registry = new UpcasterRegistry<>();
        final String actual = registry.tryUpcast(event, "ItemCreated");
        assertSame(event, actual);
    }

    @Test
    void upcasterRegisrtyShouldReturnRawTypeIfNoEventTypeNameProvided() {
        final String event = "{\"@type\":\"ItemCreated\",\"itemName\":\"Dog\",\"quantity\":9}";
        final UpcasterRegistry<String> registry = new UpcasterRegistry<>();
        final String actual = registry.tryUpcast(event, null);
        assertSame(event, actual);
    }

    @Test
    void upcasterRegisrtyShouldReturnRawTypeIfNoUpcastersForTypeNameFound() {
        final String event = "{\"@type\":\"ItemCreated\",\"itemName\":\"Dog\",\"quantity\":9}";
        final UpcasterRegistry<String> registry = new UpcasterRegistry<>();
        registry.addUpcaster(new SampleUpcaster("Bar", str -> "foo"));
        final String actual = registry.tryUpcast(event, "ItemCreated");
        assertSame(event, actual);
    }

    @Test
    void upcasterRegisrtyShouldNotAllowUpcastersToReturnNull() {
        final String event = "{\"@type\":\"Sample\",\"itemName\":\"Dog\",\"quantity\":9}";
        final UpcasterRegistry<String> registry = new UpcasterRegistry<>();
        registry.addUpcaster(new SampleUpcaster("Sample", str -> null));
        assertDoesNotThrow(() -> registry.tryUpcast(event, "Sample"));
        // instead of null the source returned
        assertSame(event, registry.tryUpcast(event, "Sample"));
    }

    @Test
    void upcasterRegistryShouldReturnUpcastedRawEventWhenFoundUpcaster() {
        final String event = "{\"@type\":\"ItemCreated\",\"itemName\":\"Собака Гав\",\"quantity\":9}";
        final String expected = "{\"@type\":\"ItemCreated\",\"name\":\"Собака Гав\",\"quantity\":9,\"type\":\"Dog\"}";
        final UpcasterRegistry<String> registry = new UpcasterRegistry<>();
        registry.addUpcaster(new SampleUpcaster("ItemCreated", input -> {
            String updated = input.replace("itemName", "name");
            return updated.replace("}", ",\"type\":\"Dog\"}");
        }));
        final String actual = registry.tryUpcast(event, "ItemCreated");
        assertEquals(expected, actual);
    }

    static class SampleUpcaster implements Upcaster<String> {

        private final String nameToResolve;
        private final UnaryOperator<String> transformation;

        SampleUpcaster(@Nullable String nameToResolve, UnaryOperator<String> transformation) {
            this.nameToResolve = nameToResolve;
            this.transformation = transformation;
        }

        @Nonnull
        @Override
        public String upcast(@Nonnull String raw) {
            return transformation.apply(raw);
        }

        @Nonnull
        @Override
        public String eventTypeName() {
            //noinspection ConstantConditions
            return nameToResolve;
        }
    }
}