package store.jesframework.serializer.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import store.jesframework.internal.FancyAggregate;
import store.jesframework.serializer.api.AliasingStrategy;
import store.jesframework.serializer.api.TypeAlias;
import store.jesframework.serializer.api.Upcaster;

import static java.util.function.UnaryOperator.identity;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static store.jesframework.internal.Events.FancyEvent;
import static store.jesframework.internal.Events.SampleEvent;

@Execution(CONCURRENT)
class ContextTest {

    @Test
    void shouldReturnAllRegisteredAliases() {
        final Context<?> context = Context.parse(
                TypeAlias.of(SampleEvent.class, "Sample"),
                TypeAlias.ofShortName(FancyEvent.class)
        );

        Assertions.assertEquals(new HashMap<Class<?>, String>() {{
            put(SampleEvent.class, "Sample");
            put(FancyEvent.class, "FancyEvent");
        }}, context.classesToAliases());

        Assertions.assertEquals(new HashMap<String, Class<?>>() {{
            put("Sample", SampleEvent.class);
            put("FancyEvent", FancyEvent.class);
        }}, context.aliasesToClasses());
    }

    @Test
    void shouldReturnAllBatchRegisteredAliases() {
        final Context<Object> context = Context.parse(AliasingStrategy.SHORT_CLASS_NAME);

        Assertions.assertTrue(context.classesToAliases().containsKey(SampleEvent.class));
        Assertions.assertTrue(context.classesToAliases().containsValue(SampleEvent.class.getSimpleName()));

        Assertions.assertTrue(context.classesToAliases().containsKey(FancyAggregate.class));
        Assertions.assertTrue(context.classesToAliases().containsValue(FancyAggregate.class.getSimpleName()));
    }

    @Test
    void shouldReturnEmptyAliasesIfNotRegisteredAny() {
        Assertions.assertEquals(Collections.<Class<?>, String>emptyMap(), Context.parse().classesToAliases());
        Assertions.assertEquals(Collections.<String, Class<?>>emptyMap(), Context.parse().aliasesToClasses());
    }

    @Test
    void shouldReturnDisabledUpcastingWhenNoUpcastersRegistered() {
        Assertions.assertFalse(Context.parse().isUpcastingEnabled());
    }

    @Test
    void shouldReturnEnabledUpcastingWhenAtLeastOneUpcasterRegistered() {
        Upcaster<String> upcaster = new SampleUpcaster("", UnaryOperator.identity());
        Assertions.assertTrue(Context.parse(upcaster).isUpcastingEnabled());
    }

    @Test
    void UpcastersWithoutEventNameShouldBeProhibited() {
        assertThrows(NullPointerException.class, () -> Context.parse((new SampleUpcaster(null, identity()))));
    }

    @Test
    void upcastingShouldReturnRawTypeIfNoUpcastersRegistered() {
        final String event = "{\"@type\":\"ItemCreated\",\"itemName\":\"Dog\",\"quantity\":9}";
        final Context<String> context = Context.parse();
        final String actual = context.tryUpcast(event, "ItemCreated");
        assertSame(event, actual);
    }

    @Test
    void upcastingShouldReturnRawTypeIfNoUpcastersForTypeNameFound() {
        final String event = "{\"@type\":\"ItemCreated\",\"itemName\":\"Dog\",\"quantity\":9}";
        final Context<String> context = Context.parse(new SampleUpcaster("Bar", str -> "foo"));
        final String actual = context.tryUpcast(event, "ItemCreated");
        assertSame(event, actual);
    }

    @Test
    void upcastingShouldNotReturnNull() {
        final String event = "{\"@type\":\"Sample\",\"itemName\":\"Dog\",\"quantity\":9}";
        final Context<String> context = Context.parse(new SampleUpcaster("Sample", str -> null));
        final String sample = assertDoesNotThrow(() -> context.tryUpcast(event, "Sample"));
        // instead of null the source returned
        assertSame(event, sample);
    }

    @Test
    void upcastingShouldReturnUpcastedRawEventWhenFoundUpcaster() {
        final String event = "{\"@type\":\"ItemCreated\",\"itemName\":\"Собака Гав\",\"quantity\":9}";
        final String expected = "{\"@type\":\"ItemCreated\",\"name\":\"Собака Гав\",\"quantity\":9,\"type\":\"Dog\"}";
        final Context<String> context = Context.parse(new SampleUpcaster("ItemCreated", input -> {
            final String updated = input.replace("itemName", "name");
            return updated.replace("}", ",\"type\":\"Dog\"}");
        }));

        final String actual = context.tryUpcast(event, "ItemCreated");
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