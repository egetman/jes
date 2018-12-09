package io.jes.serializer;

import java.util.UUID;

import com.google.gson.GsonBuilder;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.jes.Event;
import io.jes.ex.SerializationException;
import io.jes.internal.Events.FancyEvent;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

class GsonEventSerializerTest {

    @Test
    void shouldSerializeEventWithoutDefaultConstructor() {
        final GsonEventSerializer serializer = new GsonEventSerializer();
        final Event event = new FancyEvent("FOO", UUID.randomUUID());
        final String serialized = serializer.serialize(event);
        Assertions.assertNotNull(serialized);

        final Event deserialized = serializer.deserialize(serialized);
        Assertions.assertEquals(event, deserialized);
    }

    @Test
    void shouldWrapNativeExceptionsIntoSerializationException() {
        final GsonBuilder builder = new GsonBuilder();
        final GsonEventSerializer serializer = new GsonEventSerializer(builder);

        final Event mock = mock(Event.class);
        //noinspection ResultOfMethodCallIgnored
        doThrow(new IllegalStateException()).when(mock).getClass();

        Assertions.assertThrows(SerializationException.class, () -> serializer.serialize(mock));
        Assertions.assertThrows(SerializationException.class, () -> serializer.deserialize("FOO"));
    }

}