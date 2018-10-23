package io.jes.serializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SerializerFactoryTest {

    @Test
    void newBinarySerializerShouldReturnKryoImplAsDefault() {
        Assertions.assertEquals(KryoBinaryEventSerializer.class, SerializerFactory.newBinarySerializer().getClass());
    }

    @Test
    void newStringSerializerShouldReturnGsonImplAsDefault() {
        Assertions.assertEquals(GsonStringEventSerializer.class, SerializerFactory.newStringSerializer().getClass());
    }

    @Test
    void newEventSerializerShouldReturnGsonImplAsDefaultWhenStringClassPassed() {
        Assertions.assertEquals(GsonStringEventSerializer.class,
                SerializerFactory.newEventSerializer(String.class).getClass());
    }

    @Test
    void newEventSerializerShouldReturnKryoImplAsDefaultWhenByteClassPassed() {
        Assertions.assertEquals(KryoBinaryEventSerializer.class,
                SerializerFactory.newEventSerializer(byte[].class).getClass());
    }

    @Test
    void newEventSerializerShouldThrowIllegalArgumentExceptionOnUnknownSerializationType() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> SerializerFactory.newEventSerializer(Void.class));
    }

}