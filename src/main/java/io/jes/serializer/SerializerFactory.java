package io.jes.serializer;

import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Factory, that provides different serialization implementations based on target serialization type and
 * {@link SerializationOption}.
 */
public class SerializerFactory {

    private static final String NULL_SERIALIZER_OPTIONS_ERROR = "Serialization options must not be null";

    private SerializerFactory() {
    }

    @Nonnull
    static EventSerializer<byte[]> newEventBinarySerializer(@Nonnull SerializationOption... options) {
        Objects.requireNonNull(options, NULL_SERIALIZER_OPTIONS_ERROR);
        return new KryoEventSerializer();
    }

    @Nonnull
    static EventSerializer<String> newEventStringSerializer(@Nonnull SerializationOption... options) {
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
            return (EventSerializer<T>) newEventBinarySerializer(options);
        } else if (serializationType == String.class) {
            return (EventSerializer<T>) newEventStringSerializer(options);
        }
        throw new IllegalArgumentException("Serialization for type " + serializationType + " not supported");
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static <T> AggregateSerializer<T> newAggregateSerializer(@Nonnull Class<T> serializationType,
                                                                    SerializationOption... options) {
        Objects.requireNonNull(options, NULL_SERIALIZER_OPTIONS_ERROR);
        Objects.requireNonNull(serializationType, "Serialization type must be provided");
        if (serializationType == String.class) {
            return (AggregateSerializer<T>) new GsonAggregateSerializer();
        }
        throw new IllegalArgumentException("Serialization for type " + serializationType + " not supported");
    }

}
