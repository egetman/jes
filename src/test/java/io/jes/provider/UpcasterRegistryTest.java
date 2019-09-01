package io.jes.provider;

import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.jupiter.api.Test;

import io.jes.serializer.upcaster.Upcaster;

import static java.util.function.UnaryOperator.identity;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
        final String actual = registry.tryUpcast(1, event);
        assertEquals(event, actual);
    }

    @Test
    void upcasterRegisrtyShouldReturnRawTypeIfNoUpcastersForTypeNameFound() {
        final String event = "{\"@type\":\"ItemCreated\",\"itemName\":\"Dog\",\"quantity\":9}";
        final UpcasterRegistry<String> registry = new UpcasterRegistry<>();
        registry.addUpcaster(new SampleUpcaster("Bar", str -> "foo"));
        final String actual = registry.tryUpcast(1, event);
        assertEquals(event, actual);
    }

    @Test
    void upcasterRegisrtyShouldNotAllowUpcastersToReturnNull() {
        final String event = "{\"@type\":\"Sample\",\"itemName\":\"Dog\",\"quantity\":9}";
        final UpcasterRegistry<String> registry = new UpcasterRegistry<>();
        registry.addUpcaster(new SampleUpcaster("Sample", str -> null));
        assertThrows(NullPointerException.class, () -> registry.tryUpcast(1, event));
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
        final String actual = registry.tryUpcast(1, event);
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
        public String upcast(long offset, String raw) {
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