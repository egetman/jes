package io.jes.serializer;

import java.util.UUID;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.jes.Event;
import io.jes.ex.SerializationException;
import io.jes.internal.Events;
import io.jes.internal.Events.FancyEvent;

class SerializerTest {

    private static Stream<Serializer<Event, ?>> createSerializers() {
        return Stream.of(
                new KryoSerializer<>(),
                new JacksonSerializer<>()
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

    @ParameterizedTest
    @MethodSource("createInvariantsVerificationPair")
    <T> void shouldWrapNativeExceptionsIntoSerializationException(@Nonnull Serializer<Event, T> serializer,
                                                                  @Nonnull T wrongInput) {
        Assertions.assertThrows(SerializationException.class, () -> serializer.deserialize(wrongInput));
    }

}