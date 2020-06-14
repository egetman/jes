package store.jesframework.serializer.impl;

import javax.annotation.Nonnull;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import store.jesframework.Event;
import store.jesframework.serializer.api.Format;
import store.jesframework.serializer.api.SerializationOption;
import store.jesframework.serializer.api.Serializer;
import store.jesframework.serializer.api.Upcaster;

import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static store.jesframework.serializer.impl.SerializerFactory.newAggregateSerializer;
import static store.jesframework.serializer.impl.SerializerFactory.newEventSerializer;

@Execution(CONCURRENT)
class SerializerFactoryTest {

    @Test
    void defaultEventSerializerShouldBeJsonJackson() {
        final Serializer<Event, Object> serializer = newEventSerializer();
        Assertions.assertEquals(Format.JSON_JACKSON, serializer.format());
    }

    @Test
    void newEventSerializerShouldReturnNonNullSerializerWhenJsonFormatRequested() {
        final Serializer<Event, Object> serializer = newEventSerializer(Format.JSON_JACKSON);
        Assertions.assertEquals(Format.JSON_JACKSON, serializer.format());
    }

    @Test
    void newEventSerializerShouldReturnNonNullSerializerWhenXstreamRequested() {
        final Serializer<Event, Object> serializer = newEventSerializer(Format.XML_XSTREAM);
        Assertions.assertEquals(Format.XML_XSTREAM, serializer.format());
    }

    @Test
    void newEventSerializerShouldReturnNonNullSerializerWhenKryoRequested() {
        final Serializer<Event, Object> serializer = newEventSerializer(Format.BINARY_KRYO);
        Assertions.assertEquals(Format.BINARY_KRYO, serializer.format());
    }

    @Test
    void newAggregateSerializerShouldReturnKryoImplAsDefaultWhenBinaryFormatRequested() {
        Assertions.assertEquals(KryoSerializer.class, newAggregateSerializer(Format.BINARY_KRYO).getClass());
    }

    @Test
    void newAggregateSerializerShouldReturnJacksonImplAsDefaultWhenJsonFormatRequested() {
        Assertions.assertEquals(JacksonSerializer.class, newAggregateSerializer(Format.JSON_JACKSON).getClass());
    }

    @Test
    void newAggregateSerializerShouldReturnJacksonImplAsDefaultWhenXmlFormatRequested() {
        Assertions.assertEquals(XStreamSerializer.class, newAggregateSerializer(Format.XML_XSTREAM).getClass());
    }

    @Test
    void wrongUpcastersShouldNotFailInitialization() {
        final SerializationOption byteUpcaster = new Upcaster<byte[]>() {
            @Nonnull
            @Override
            public byte[] upcast(@Nonnull byte[] raw) {
                return new byte[0];
            }

            @Nonnull
            @Override
            public String eventTypeName() {
                return "";
            }
        };

        final SerializationOption stringUpcaster = new Upcaster<String>() {

            @Nonnull
            @Override
            public String upcast(@Nonnull String raw) {
                return "";
            }

            @Nonnull
            @Override
            public String eventTypeName() {
                return "";
            }
        };
        Assertions.assertDoesNotThrow(() -> Context.parse(byteUpcaster, stringUpcaster));
    }

}