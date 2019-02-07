package io.jes.serializer;

import java.util.Objects;
import javax.annotation.Nonnull;

import io.jes.Aggregate;
import io.jes.Event;

/**
 * Factory, that provides different serialization implementations based on target serialization type and
 * {@link SerializationOption}.
 */
public class SerializerFactory {


    private SerializerFactory() {
    }

    @Nonnull
    @SuppressWarnings({"unused", "unchecked"})
    static <E extends Serializer<Event, byte[]>> E newEventBinarySerializer(@Nonnull SerializationOption... options) {
        return (E) new KryoSerializer<Event>();
    }

    @Nonnull
    @SuppressWarnings({"unused", "unchecked"})
    static <E extends Serializer<Event, String>> E newEventStringSerializer(@Nonnull SerializationOption... options) {
        return (E) new JacksonSerializer<Event>();
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "squid:S1905"})
    public static <T> Serializer<Event, T> newEventSerializer(@Nonnull Class<T> serializationType,
                                                       SerializationOption... options) {
        Objects.requireNonNull(serializationType, "Serialization type must be provided");
        if (serializationType == byte[].class) {
            return (Serializer<Event, T>) newEventBinarySerializer(options);
        } else if (serializationType == String.class) {
            return (Serializer<Event, T>) newEventStringSerializer(options);
        }
        throw new IllegalArgumentException("Serialization for type " + serializationType + " not supported");
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "unused", "squid:S2293"})
    public static <T> Serializer<Aggregate, T> newAggregateSerializer(@Nonnull Class<T> serializationType,
                                                                     SerializationOption... options) {
        Objects.requireNonNull(serializationType, "Serialization type must be provided");
        if (serializationType == byte[].class) {
            return (Serializer<Aggregate, T>) new KryoSerializer<Aggregate>();
        } else if (serializationType == String.class) {
            return (Serializer<Aggregate, T>) new JacksonSerializer<Aggregate>();
        }
        throw new IllegalArgumentException("Serialization for type " + serializationType + " not supported");
    }

}
