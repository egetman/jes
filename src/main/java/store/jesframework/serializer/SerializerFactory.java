package store.jesframework.serializer;

import java.util.Objects;
import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;
import store.jesframework.Aggregate;
import store.jesframework.serializer.api.EventSerializer;
import store.jesframework.serializer.api.SerializationOption;
import store.jesframework.serializer.api.Serializer;
import store.jesframework.serializer.api.Upcaster;

/**
 * Factory, that provides different serialization implementations based on target serialization type and
 * {@link SerializationOption}.
 * Note: some serializers implementations may ignore inapplicable serialization options.
 */
@Slf4j
public class SerializerFactory {

    private SerializerFactory() {}

    @Nonnull
    @SuppressWarnings({"unused"})
    static EventSerializer<byte[]> newBinarySerializer(@Nonnull ParsedOptions<byte[]> options) {
        return new EventSerializerProxy<>(new KryoEventSerializer(), options.upcasterRegistry);
    }

    @Nonnull
    @SuppressWarnings({"unused"})
    static EventSerializer<String> newStringSerializer(@Nonnull ParsedOptions<String> options) {
        return new EventSerializerProxy<>(new JacksonEventSerializer(options.typeRegistry), options.upcasterRegistry);
    }

    /**
     * Produces and return new {@link Serializer} for events.
     *
     * @param serializationType is one of two ({@literal String}, {@literal byte[]}) serialization types.
     * @param options           are hooks for serizlization/deserialization process.
     * @param <T>               is serialization format.
     * @return configured {@link Serializer}.
     */
    @Nonnull
    @SuppressWarnings({"unchecked", "squid:S1905"})
    public static <T> EventSerializer<T> newEventSerializer(@Nonnull Class<T> serializationType,
                                                            SerializationOption... options) {
        Objects.requireNonNull(serializationType, "Serialization type must be provided");
        final ParsedOptions<T> parsedOptions = ParsedOptions.parse(options);

        if (serializationType == byte[].class) {
            final EventSerializer<byte[]> serializer = newBinarySerializer((ParsedOptions<byte[]>) parsedOptions);
            return (EventSerializer<T>) serializer;
        } else if (serializationType == String.class) {
            final EventSerializer<String> serializer = newStringSerializer((ParsedOptions<String>) parsedOptions);
            return (EventSerializer<T>) serializer;
        }
        throw new IllegalArgumentException("Serialization for type " + serializationType + " not supported");
    }

    /**
     * Produces and return new {@link Serializer} for aggregates.
     *
     * @param serializationType is one of two ({@literal String}, {@literal byte[]}) serialization types.
     * @param options           are hooks for serizlization/deserialization process.
     * @param <T>               is serialization format.
     * @return configured {@link Serializer}.
     */
    @Nonnull
    @SuppressWarnings({"unchecked", "unused", "squid:S2293"})
    public static <T> Serializer<Aggregate, T> newAggregateSerializer(@Nonnull Class<T> serializationType,
                                                                      SerializationOption... options) {
        Objects.requireNonNull(serializationType, "Serialization type must be provided");
        if (serializationType == byte[].class) {
            final Serializer<Aggregate, byte[]> serializer = new KryoSerializer<>();
            return (Serializer<Aggregate, T>) serializer;
        } else if (serializationType == String.class) {
            final Serializer<Aggregate, String> serializer = new JacksonSerializer<>();
            return (Serializer<Aggregate, T>) serializer;
        }
        throw new IllegalArgumentException("Serialization for type " + serializationType + " not supported");
    }

    static class ParsedOptions<T> {

        private TypeRegistry typeRegistry;
        private UpcasterRegistry<T> upcasterRegistry = new UpcasterRegistry<>();

        static <T> ParsedOptions<T> parse(SerializationOption... options) {
            final ParsedOptions<T> parsedOptions = new ParsedOptions<>();
            if (options == null || options.length == 0) {
                return parsedOptions;
            }

            for (SerializationOption option : options) {
                if (option instanceof TypeRegistry) {
                    parsedOptions.typeRegistry = (TypeRegistry) option;
                } else if (option instanceof Upcaster) {
                    try {
                        //noinspection unchecked
                        parsedOptions.upcasterRegistry.addUpcaster((Upcaster<T>) option);
                    } catch (ClassCastException e) {
                        log.warn("Failed to register upcaster {}: type mismatch", option, e);
                    }
                } else {
                    log.warn("Unsupported serialization option found: {}", option);
                }
            }
            log.debug("parsing of {} serialization option(s) complete", options.length);
            return parsedOptions;
        }
    }
}
