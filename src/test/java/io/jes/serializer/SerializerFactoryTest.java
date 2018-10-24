package io.jes.serializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SerializerFactoryTest {

    @Test
    void newBinarySerializerShouldReturnKryoImplAsDefault() {
        Assertions.assertEquals(KryoEventSerializer.class, SerializerFactory.newBinarySerializer().getClass());
    }

    @Test
    void newStringSerializerShouldReturnGsonImplAsDefault() {
        Assertions.assertEquals(GsonEventSerializer.class, SerializerFactory.newStringSerializer().getClass());
    }

    @Test
    void newEventSerializerShouldReturnGsonImplAsDefaultWhenStringClassPassed() {
        Assertions.assertEquals(GsonEventSerializer.class,
                SerializerFactory.newEventSerializer(String.class).getClass());
    }

    @Test
    void newEventSerializerShouldReturnKryoImplAsDefaultWhenByteClassPassed() {
        Assertions.assertEquals(KryoEventSerializer.class,
                SerializerFactory.newEventSerializer(byte[].class).getClass());
    }

    @Test
    void newEventSerializerShouldThrowIllegalArgumentExceptionOnUnknownSerializationType() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> SerializerFactory.newEventSerializer(Void.class));
    }

}