package io.jes.serializer;

import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static io.jes.serializer.SerializerFactory.newAggregateSerializer;
import static io.jes.serializer.SerializerFactory.newEventBinarySerializer;
import static io.jes.serializer.SerializerFactory.newEventSerializer;
import static io.jes.serializer.SerializerFactory.newEventStringSerializer;

class SerializerFactoryTest {

    @Test
    void newBinarySerializerShouldReturnNonNullSerializer() {
        Assertions.assertNotNull(newEventBinarySerializer());
    }

    @Test
    void newStringSerializerShouldReturnNonNullSerializer() {
        Assertions.assertNotNull(newEventStringSerializer());
    }

    @Test
    void newEventSerializerShouldReturnNonNullSerializerWhenStringClassPassed() {
        Assertions.assertNotNull(newEventSerializer(String.class).getClass());
    }

    @Test
    void newEventSerializerShouldReturnNonNullSerializerWhenByteClassPassed() {
        Assertions.assertNotNull(newEventSerializer(byte[].class).getClass());
    }

    @Test
    void newEventSerializerShouldThrowIllegalArgumentExceptionOnUnknownSerializationType() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> newEventSerializer(Void.class));
    }

    @Test
    void newAggregateSerializerShouldReturnKryoImplAsDefaultWhenByteClassPassed() {
        Assertions.assertEquals(KryoSerializer.class, newAggregateSerializer(byte[].class).getClass());
    }

    @Test
    void newAggregateSerializerShouldReturnJacksonImplAsDefaultWhenStringClassPassed() {
        Assertions.assertEquals(JacksonSerializer.class, newAggregateSerializer(String.class).getClass());
    }

    @Test
    void newAggregateSerializerShouldThrowIllegalArgumentExceptionOnUnknownSerializationType() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> newAggregateSerializer(Stream.class));
    }

}