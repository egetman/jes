package io.jes.serializer;

import java.util.Objects;
import javax.annotation.Nonnull;

public class SerializerFactory {

    private SerializerFactory() {}

    @Nonnull
    @SuppressWarnings("WeakerAccess")
    public static EventSerializer<byte[]> newBinarySerializer() {
        return new KryoBinaryEventSerializer();
    }

    @Nonnull
    @SuppressWarnings("WeakerAccess")
    public static EventSerializer<String> newStringSerializer() {
        throw new UnsupportedOperationException("String serializer not implemented yet");
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <T> EventSerializer<T> newEventSerializer(@Nonnull Class<T> serializationType) {
        Objects.requireNonNull(serializationType, "Serialization type must be provided");
        if (serializationType == byte[].class) {
            return (EventSerializer<T>) newBinarySerializer();
        } else if (serializationType == String.class) {
            return (EventSerializer<T>) newStringSerializer();
        }
        throw new IllegalArgumentException("Serialization for type " + serializationType + " not supported");
    }

}
