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
    void newBinarySerializerShouldReturnKryoImplAsDefault() {
        Assertions.assertEquals(KryoSerializer.class, newEventBinarySerializer().getClass());
    }

    @Test
    void newStringSerializerShouldReturnJacksonImplAsDefault() {
        Assertions.assertEquals(JacksonSerializer.class, newEventStringSerializer().getClass());
    }

    @Test
    void newEventSerializerShouldReturnJacksonImplAsDefaultWhenStringClassPassed() {
        Assertions.assertEquals(JacksonSerializer.class, newEventSerializer(String.class).getClass());
    }

    @Test
    void newEventSerializerShouldReturnKryoImplAsDefaultWhenByteClassPassed() {
        Assertions.assertEquals(KryoSerializer.class, newEventSerializer(byte[].class).getClass());
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