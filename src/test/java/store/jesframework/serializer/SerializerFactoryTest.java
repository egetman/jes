package store.jesframework.serializer;

import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static store.jesframework.serializer.SerializerFactory.newAggregateSerializer;
import static store.jesframework.serializer.SerializerFactory.newBinarySerializer;
import static store.jesframework.serializer.SerializerFactory.newEventSerializer;
import static store.jesframework.serializer.SerializerFactory.newStringSerializer;

class SerializerFactoryTest {

    @Test
    void newBinarySerializerShouldReturnNonNullSerializer() {
        Assertions.assertNotNull(newBinarySerializer());
    }

    @Test
    void newStringSerializerShouldReturnNonNullSerializer() {
        Assertions.assertNotNull(newStringSerializer());
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