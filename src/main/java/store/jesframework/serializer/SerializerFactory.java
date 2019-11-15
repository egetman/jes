package store.jesframework.serializer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.extern.slf4j.Slf4j;
import store.jesframework.Aggregate;
import store.jesframework.Event;
import store.jesframework.serializer.api.Format;
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

    /**
     * Produces and return new {@link Serializer} for events.
     *
     * @param options           are hooks for serizlization/deserialization process.
     * @param <T>               is serialization format.
     * @return configured {@link Serializer}.
     */
    @Nonnull
    public static <T> Serializer<Event, T> newEventSerializer(@Nullable SerializationOption... options) {
        final ParsedOptions<T> parsedOptions = ParsedOptions.parse(options);
        final Serializer<Event, T> serializer = newSerializer(parsedOptions);
        return new EventSerializerProxy<>(serializer, parsedOptions.upcasterRegistry);
    }

    /**
     * Produces and return new {@link Serializer} for aggregates.
     *
     * @param options           are hooks for serizlization/deserialization process.
     * @param <T>               is serialization format.
     * @return configured {@link Serializer}.
     */
    @Nonnull
    public static <T> Serializer<Aggregate, T> newAggregateSerializer(@Nullable SerializationOption... options) {
        final ParsedOptions<T> parsedOptions = ParsedOptions.parse(options);
        return newSerializer(parsedOptions);
    }

    private static <S, T> Serializer<S, T> newSerializer(@Nonnull ParsedOptions parsedOptions) {
        switch (parsedOptions.format) {
            case JSON_JACKSON:
                //noinspection unchecked
                return (Serializer<S, T>) new JacksonSerializer<>(parsedOptions.typeRegistry);
            case BINARY_KRYO:
                //noinspection unchecked
                return (Serializer<S, T>) new KryoSerializer<>();
            case XML_XSTREAM:
                //noinspection unchecked
                return (Serializer<S, T>) new XStreamSerializer<>(parsedOptions.typeRegistry);
            default:
                throw new IllegalArgumentException("Unknown serializer format: " + parsedOptions.format);
        }
    }

    static class ParsedOptions<T> {

        private TypeRegistry typeRegistry;
        // safe defaults
        private Format format = Format.JSON_JACKSON;
        private UpcasterRegistry<T> upcasterRegistry = new UpcasterRegistry<>();

        @Nonnull
        static <T> ParsedOptions<T> parse(SerializationOption... options) {
            final ParsedOptions<T> parsedOptions = new ParsedOptions<>();
            if (options == null || options.length == 0) {
                return parsedOptions;
            }

            for (SerializationOption option : options) {
                if (option instanceof TypeRegistry) {
                    parsedOptions.typeRegistry = (TypeRegistry) option;
                } else if (option instanceof Format) {
                    parsedOptions.format = (Format) option;
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
