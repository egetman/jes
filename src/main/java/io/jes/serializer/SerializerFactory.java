package io.jes.serializer;

import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Factory, that provides different serialization implementations based on target serialization type and
 * {@link SerializationOption}.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class SerializerFactory {

    public static final String NULL_SERIALIZER_OPTIONS_ERROR = "Serialization options must not be null";

    private SerializerFactory() {}

    @Nonnull
    public static EventSerializer<byte[]> newBinarySerializer(@Nonnull SerializationOption... options) {
        Objects.requireNonNull(options, NULL_SERIALIZER_OPTIONS_ERROR);
        return new KryoEventSerializer();
    }

    @Nonnull
    public static EventSerializer<String> newStringSerializer(@Nonnull SerializationOption... options) {
        Objects.requireNonNull(options, NULL_SERIALIZER_OPTIONS_ERROR);
        return new GsonEventSerializer();
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <T> EventSerializer<T> newEventSerializer(@Nonnull Class<T> serializationType,
                                                            SerializationOption... options) {
        Objects.requireNonNull(options, NULL_SERIALIZER_OPTIONS_ERROR);
        Objects.requireNonNull(serializationType, "Serialization type must be provided");
        if (serializationType == byte[].class) {
            return (EventSerializer<T>) newBinarySerializer();
        } else if (serializationType == String.class) {
            return (EventSerializer<T>) newStringSerializer();
        }
        throw new IllegalArgumentException("Serialization for type " + serializationType + " not supported");
    }

}
