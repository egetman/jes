package store.jesframework.serializer;

import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.extern.slf4j.Slf4j;
import store.jesframework.Event;
import store.jesframework.common.UnknownTypeResolved;
import store.jesframework.serializer.api.Format;
import store.jesframework.serializer.api.Serializer;

/**
 * This Serializer handles the case event cannot be deserialized cause of missing type information.
 *
 * @param <T> type of raw event type.
 */
@Slf4j
class EventSerializerProxy<T> implements Serializer<Event, T> {

    private final Serializer<Event, T> actual;
    private final UpcasterRegistry<T> registry;

    EventSerializerProxy(@Nonnull Serializer<Event, T> actual, @Nonnull UpcasterRegistry<T> registry) {
        this.actual = Objects.requireNonNull(actual);
        this.registry = Objects.requireNonNull(registry, "Upcaster registry must not be null");
    }

    @Nonnull
    @Override
    public T serialize(@Nonnull Event toSerialize) {
        return actual.serialize(toSerialize);
    }

    @Nonnull
    @Override
    public Event deserialize(@Nonnull T toDeserialize) {
        try {
            final String typeName = fetchTypeName(toDeserialize);
            toDeserialize = registry.tryUpcast(toDeserialize, typeName);
            return actual.deserialize(toDeserialize);
        } catch (TypeNotPresentException e) {
            log.trace("Can't find type information for {}", e.typeName());
            return new UnknownTypeResolved(e.typeName(), toDeserialize);
        }
    }

    @Nullable
    @Override
    public String fetchTypeName(@Nonnull T raw) {
        return actual.fetchTypeName(raw);
    }

    @Nonnull
    @Override
    public Format format() {
        return actual.format();
    }
}
