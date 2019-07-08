package io.jes.serializer;

import java.util.Objects;
import javax.annotation.Nonnull;

import io.jes.Aggregate;
import io.jes.Event;
import lombok.extern.slf4j.Slf4j;

import static java.lang.String.format;
import static java.util.Arrays.stream;

/**
 * Factory, that provides different serialization implementations based on target serialization type and
 * {@link SerializationOption}.
 * Note: some serializers implementations may ignore inapplicable serialization options.
 */
@Slf4j
public class SerializerFactory {

    private SerializerFactory() {}

    @Nonnull
    @SuppressWarnings({"unused", "unchecked"})
    static <T, E extends Serializer<T, byte[]>> E newBinarySerializer(@Nonnull SerializationOption... options) {
        for (SerializationOption option : options) {
            log.warn("Binary serializer can't use option: {}", option);
        }
        return (E) new EventSerializerProxy<>(new KryoSerializer<>());
    }

    @Nonnull
    @SuppressWarnings({"unused", "unchecked"})
    static <T, E extends Serializer<T, String>> E newStringSerializer(@Nonnull SerializationOption... options) {
        for (SerializationOption option : options) {
            if (option instanceof SerializationOptions) {
                final SerializationOptions defaults = (SerializationOptions) option;
                if (defaults == SerializationOptions.USE_TYPE_ALIASES) {
                    log.debug("Resolved option: {}", option);
                    TypeRegistry registry = stream(options)
                            .filter(serializationOption -> serializationOption instanceof TypeRegistry)
                            .findFirst()
                            .map(TypeRegistry.class::cast)
                            .orElseThrow(() -> {
                                final String error = format("%s without provided %s", option, TypeRegistry.class);
                                return new IllegalArgumentException(error);
                            });
                    return (E) new EventSerializerProxy<>(new JacksonSerializer<>(registry.getAliases()));
                } else {
                    log.warn("String serializer can't use option: {}", option);
                }
            }
        }
        return (E) new EventSerializerProxy<>(new JacksonSerializer<>());
    }

    /**
     * Produces and return new {@link Serializer} for events.
     */
    @Nonnull
    @SuppressWarnings({"unchecked", "squid:S1905"})
    public static <T> Serializer<Event, T> newEventSerializer(@Nonnull Class<T> serializationType,
                                                              SerializationOption... options) {
        Objects.requireNonNull(serializationType, "Serialization type must be provided");
        if (serializationType == byte[].class) {
            Serializer<Event, byte[]> serializer = newBinarySerializer(options);
            return (Serializer<Event, T>) serializer;
        } else if (serializationType == String.class) {
            Serializer<Event, String> serializer = newStringSerializer(options);
            return (Serializer<Event, T>) serializer;
        }
        throw new IllegalArgumentException("Serialization for type " + serializationType + " not supported");
    }

    /**
     * Produces and return new {@link Serializer} for aggregates.
     */
    @Nonnull
    @SuppressWarnings({"unchecked", "unused", "squid:S2293"})
    public static <T> Serializer<Aggregate, T> newAggregateSerializer(@Nonnull Class<T> serializationType,
                                                                     SerializationOption... options) {
        Objects.requireNonNull(serializationType, "Serialization type must be provided");
        if (serializationType == byte[].class) {
            Serializer<Aggregate, byte[]> serializer = new KryoSerializer<>();
            return (Serializer<Aggregate, T>) serializer;
        } else if (serializationType == String.class) {
            Serializer<Aggregate, String> serializer = new JacksonSerializer<>();
            return (Serializer<Aggregate, T>) serializer;
        }
        throw new IllegalArgumentException("Serialization for type " + serializationType + " not supported");
    }

}
