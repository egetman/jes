package store.jesframework.serializer;

import java.util.Arrays;
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
import store.jesframework.serializer.api.EventSerializer;
import store.jesframework.serializer.api.Serializer;

class SerializerTest {

    private static Stream<EventSerializer<?>> eventSerializers() {
        return Stream.of(
                new EventSerializerProxy<>(new KryoEventSerializer(), new UpcasterRegistry<>()),
                new EventSerializerProxy<>(new JacksonEventSerializer(), new UpcasterRegistry<>())
        );
    }

    private static Stream<Arguments> createInvariantsVerificationPair() {
        return Stream.of(
                Arguments.of(new KryoEventSerializer(), new byte[]{}),
                Arguments.of(new JacksonEventSerializer(), "")
        );
    }

    // for some serializers deserialization without default constructor is possible
    // if constructor has a metadata (lombok adds it via @java.beans.ConstructorProperties)
    @ParameterizedTest
    @MethodSource("eventSerializers")
    <T> void shouldSerializeEventWithoutDefaultConstructor(@Nonnull EventSerializer<T> serializer) {
        final Event event = new FancyEvent("FOO", UUID.randomUUID());
        final T serialized = serializer.serialize(event);
        Assertions.assertNotNull(serialized);

        final Event deserialized = serializer.deserialize(serialized);
        Assertions.assertEquals(event, deserialized);
    }

    @ParameterizedTest
    @MethodSource("eventSerializers")
    <T> void shouldSerializeEventWithAbstractType(@Nonnull EventSerializer<T> serializer) {
        final Event event = new Events.ColorChanged(new Events.Black());
        final T serialized = serializer.serialize(event);
        Assertions.assertNotNull(serialized);

        final Event deserialized = serializer.deserialize(serialized);
        Assertions.assertEquals(event, deserialized);
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("eventSerializers")
    <T> void shouldReturnUnregisteredEventTypeWhenNoTypeInformationFound(@Nonnull EventSerializer<T> serializer) {
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
    <T> void shouldWrapNativeExceptionsIntoSerializationException(@Nonnull EventSerializer<T> serializer,
                                                                  @Nonnull T wrongInput) {
        Assertions.assertThrows(SerializationException.class, () -> serializer.deserialize(wrongInput));
    }

    @Test
    void shouldSerializeObjectWithAliasIfTypeRegistryPassed() {
        final TypeRegistry registry = new TypeRegistry();
        registry.addAlias(Events.SampleEvent.class, "MyAlias");
        final Serializer<Event, String> serializer = SerializerFactory.newEventSerializer(String.class, registry);

        final String serialized = serializer.serialize(new Events.SampleEvent("Sample", UUID.randomUUID()));
        Assertions.assertTrue(serialized.contains("\"@type\":\"MyAlias\""));

        final Event deserialized = serializer.deserialize(serialized);
        Assertions.assertEquals(Events.SampleEvent.class, deserialized.getClass());
    }

    @Test
    void jacksonEventSerializerShouldBeAbleToProcessEventNamesLargerThanDefault() {
        final int defaultNameSize = JacksonEventSerializer.DEFAULT_NAME_SIZE;
        final char[] largeName = new char[defaultNameSize * 3];
        Arrays.fill(largeName, 'O');
        final String eventName = new String(largeName);

        TypeRegistry registry = new TypeRegistry();
        registry.addAlias(Events.SampleEvent.class, eventName);

        final JacksonEventSerializer serializer = new JacksonEventSerializer(registry);
        final String serialized = serializer.serialize(new Events.SampleEvent("name", UUID.randomUUID()));

        Assertions.assertEquals(eventName, serializer.fetchTypeName(serialized));
    }
}