package io.jes.serializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static io.jes.serializer.SerializerFactory.*;

class SerializerFactoryTest {

    @Test
    void newBinarySerializerShouldReturnKryoImplAsDefault() {
        Assertions.assertEquals(KryoEventSerializer.class, newBinarySerializer().getClass());
    }

    @Test
    void newStringSerializerShouldReturnGsonImplAsDefault() {
        Assertions.assertEquals(GsonEventSerializer.class, newStringSerializer().getClass());
    }

    @Test
    void newEventSerializerShouldReturnGsonImplAsDefaultWhenStringClassPassed() {
        Assertions.assertEquals(GsonEventSerializer.class, newEventSerializer(String.class).getClass());
    }

    @Test
    void newEventSerializerShouldReturnKryoImplAsDefaultWhenByteClassPassed() {
        Assertions.assertEquals(KryoEventSerializer.class, newEventSerializer(byte[].class).getClass());
    }

    @Test
    void newEventSerializerShouldThrowIllegalArgumentExceptionOnUnknownSerializationType() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> newEventSerializer(Void.class));
    }

}