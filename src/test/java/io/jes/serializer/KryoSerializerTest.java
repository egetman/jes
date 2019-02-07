package io.jes.serializer;

import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.jes.Event;
import io.jes.ex.SerializationException;

import static io.jes.internal.Events.Black;
import static io.jes.internal.Events.ColorChanged;
import static io.jes.internal.Events.FancyEvent;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

class KryoSerializerTest {

    @Test
    void shouldSerializeEventWithoutDefaultConstructor() {
        final KryoSerializer<Event> serializer = new KryoSerializer<>();
        final Event event = new FancyEvent("FOO", UUID.randomUUID());
        final byte[] serialized = serializer.serialize(event);
        Assertions.assertNotNull(serialized);

        final Event deserialized = serializer.deserialize(serialized);
        Assertions.assertEquals(event, deserialized);
    }

    @Test
    void shouldSerializeEventWithAbstractType() {
        final KryoSerializer<Event> serializer = new KryoSerializer<>();
        final Event event = new ColorChanged(new Black());
        final byte[] serialized = serializer.serialize(event);
        Assertions.assertNotNull(serialized);

        final Event deserialized = serializer.deserialize(serialized);
        Assertions.assertEquals(event, deserialized);
    }

    @Test
    void shouldWrapNativeExceptionsIntoSerializationException() {
        final KryoSerializer<Event> serializer = new KryoSerializer<>();

        final Event mock = mock(Event.class);
        doThrow(new IllegalStateException()).when(mock).expectedStreamVersion();

        Assertions.assertThrows(SerializationException.class, () -> serializer.serialize(mock));
        Assertions.assertThrows(SerializationException.class, () -> serializer.deserialize(new byte[]{}));
    }

}