package store.jesframework.serializer;

import java.util.UUID;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import store.jesframework.Event;
import store.jesframework.common.UnknownTypeResolved;
import store.jesframework.ex.SerializationException;
import store.jesframework.internal.Events;
import store.jesframework.internal.Events.FancyEvent;
import lombok.SneakyThrows;
import net.bytebuddy.ByteBuddy;

class SerializerTest {

    private static Stream<Serializer<Event, ?>> createSerializers() {
        return Stream.of(
                new EventSerializerProxy<>(new KryoSerializer<>()),
                new EventSerializerProxy<>(new JacksonSerializer<>())
        );
    }

    private static Stream<Arguments> createInvariantsVerificationPair() {
        return Stream.of(
                Arguments.of(new KryoSerializer<Event>(), new byte[]{}),
                Arguments.of(new JacksonSerializer<Event>(), "")
        );
    }

    // for some serializers deserialization without default constructor is possible
    // if constructor has a metadata (lombok adds it via @java.beans.ConstructorProperties)
    @ParameterizedTest
    @MethodSource("createSerializers")
    <T> void shouldSerializeEventWithoutDefaultConstructor(@Nonnull Serializer<Event, T> serializer) {
        final Event event = new FancyEvent("FOO", UUID.randomUUID());
        final T serialized = serializer.serialize(event);
        Assertions.assertNotNull(serialized);

        final Event deserialized = serializer.deserialize(serialized);
        Assertions.assertEquals(event, deserialized);
    }

    @ParameterizedTest
    @MethodSource("createSerializers")
    <T> void shouldSerializeEventWithAbstractType(@Nonnull Serializer<Event, T> serializer) {
        final Event event = new Events.ColorChanged(new Events.Black());
        final T serialized = serializer.serialize(event);
        Assertions.assertNotNull(serialized);

        final Event deserialized = serializer.deserialize(serialized);
        Assertions.assertEquals(event, deserialized);
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("createSerializers")
    <T> void shouldReturnUnregisteredEventTypeWhenNoTypeInformationFound(@Nonnull Serializer<Event, T> serializer) {
        final Class<? extends Event> dynamicEvent = new ByteBuddy()
                .subclass(Event.class)
                .make()
                .load(getClass().getClassLoader())
                .getLoaded();
        final Event event = dynamicEvent.newInstance();
        final T serialized = serializer.serialize(event);

        final Event deserialized = serializer.deserialize(serialized);

        Assertions.assertTrue(deserialized instanceof UnknownTypeResolved);
        Assertions.assertEquals(event.getClass().getName(), ((UnknownTypeResolved) deserialized).type());
        Assertions.assertNotNull(((UnknownTypeResolved) deserialized).raw());
    }

    @ParameterizedTest
    @MethodSource("createInvariantsVerificationPair")
    <T> void shouldWrapNativeExceptionsIntoSerializationException(@Nonnull Serializer<Event, T> serializer,
                                                                  @Nonnull T wrongInput) {
        Assertions.assertThrows(SerializationException.class, () -> serializer.deserialize(wrongInput));
    }

    @Test
    void shouldSerializeObjectWithAliasIfTypeRegistryPassed() {
        final TypeRegistry registry = new TypeRegistry();
        registry.addAlias(Events.SampleEvent.class, "MyAlias");
        final Serializer<Event, String> serializer = SerializerFactory.newEventSerializer(String.class,
                SerializationOptions.USE_TYPE_ALIASES, registry);

        final String serialized = serializer.serialize(new Events.SampleEvent("Sample", UUID.randomUUID()));
        Assertions.assertTrue(serialized.contains("\"@type\":\"MyAlias\""));

        final Event deserialized = serializer.deserialize(serialized);
        Assertions.assertEquals(Events.SampleEvent.class, deserialized.getClass());
    }
}