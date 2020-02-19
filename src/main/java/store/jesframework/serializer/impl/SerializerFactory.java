package store.jesframework.serializer.impl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.extern.slf4j.Slf4j;
import store.jesframework.Aggregate;
import store.jesframework.Event;
import store.jesframework.serializer.api.SerializationOption;
import store.jesframework.serializer.api.Serializer;

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
     * @param options           are hooks for serialization/deserialization process.
     * @param <T>               is serialization format.
     * @return configured {@link Serializer}.
     */
    @Nonnull
    public static <T> Serializer<Event, T> newEventSerializer(@Nullable SerializationOption... options) {
        final Context<T> context = Context.parse(options);
        final Serializer<Event, T> serializer = newSerializer(context);
        return new EventSerializerProxy<>(serializer, context);
    }

    /**
     * Produces and return new {@link Serializer} for aggregates.
     *
     * @param options           are hooks for serialization/deserialization process.
     * @param <T>               is serialization format.
     * @return configured {@link Serializer}.
     */
    @Nonnull
    public static <T> Serializer<Aggregate, T> newAggregateSerializer(@Nullable SerializationOption... options) {
        final Context<T> context = Context.parse(options);
        return newSerializer(context);
    }

    @SuppressWarnings("unchecked")
    private static <S, T> Serializer<S, T> newSerializer(@Nonnull Context<T> context) {
        switch (context.getFormat()) {
            case JSON_JACKSON:
                return (Serializer<S, T>) new JacksonSerializer<>(context);
            case BINARY_KRYO:
                return (Serializer<S, T>) new KryoSerializer<>();
            case XML_XSTREAM:
                return (Serializer<S, T>) new XStreamSerializer<>(context);
            default:
                throw new IllegalArgumentException("Unknown serializer format: " + context.getFormat());
        }
    }

}
