package io.jes.serializer;

import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Factory, that provides different serialization implementations based on target serialization type and
 * {@link SerializationOption}.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class SerializerFactory {

    private SerializerFactory() {
    }

    @Nonnull
    public static EventSerializer<byte[]> newBinarySerializer(SerializationOption... options) {
        return new KryoBinaryEventSerializer();
    }

    @Nonnull
    public static EventSerializer<String> newStringSerializer(SerializationOption... options) {
        throw new UnsupportedOperationException("String serializer not implemented yet");
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <T> EventSerializer<T> newEventSerializer(@Nonnull Class<T> serializationType,
                                                            SerializationOption... options) {
        Objects.requireNonNull(serializationType, "Serialization type must be provided");
        if (serializationType == byte[].class) {
            return (EventSerializer<T>) newBinarySerializer();
        } else if (serializationType == String.class) {
            return (EventSerializer<T>) newStringSerializer();
        }
        throw new IllegalArgumentException("Serialization for type " + serializationType + " not supported");
    }

}
