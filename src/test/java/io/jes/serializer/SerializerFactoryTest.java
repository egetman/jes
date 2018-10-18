package io.jes.serializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SerializerFactoryTest {

    @Test
    void newBinarySerializerShouldReturnKryoImplAsDefault() {
        Assertions.assertEquals(KryoBinaryEventSerializer.class, SerializerFactory.newBinarySerializer().getClass());
    }

    @Test
    void newStringSerializerShouldThrowUnsupportedOperationException() {
        Assertions.assertThrows(UnsupportedOperationException.class, SerializerFactory::newStringSerializer);
    }

    @Test
    void newEventSerializerShouldThrowUnsupportedOperationExceptionWhenStringClassPassed() {
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> SerializerFactory.newEventSerializer(String.class));
    }

    @Test
    void newEventSerializerShouldReturnKryoImplWhenByteClassPassed() {
        Assertions.assertEquals(KryoBinaryEventSerializer.class,
                SerializerFactory.newEventSerializer(byte[].class).getClass());
    }

    @Test
    void newEventSerializerShouldThrowIllegalArgumentExceptionOnUnknownSerializationType() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> SerializerFactory.newEventSerializer(Void.class));
    }

}