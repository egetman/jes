package io.jes.serializer;

import java.util.Objects;
import javax.annotation.Nonnull;

import io.jes.Event;
import io.jes.common.UnknownTypeResolved;
import lombok.extern.slf4j.Slf4j;

/**
 * This Serializer handles the case event cannot be deserialized cause of missing type information.
 *
 * @param <T> type of raw event type.
 */
@Slf4j
class EventSerializerProxy<T> implements Serializer<Event, T> {

    private final Serializer<Event, T> actual;

    EventSerializerProxy(@Nonnull Serializer<Event, T> actual) {
        this.actual = Objects.requireNonNull(actual);
    }

    @Override
    public T serialize(Event toSerialize) {
        return actual.serialize(toSerialize);
    }

    @Override
    public Event deserialize(T toDeserialize) {
        try {
            return actual.deserialize(toDeserialize);
        } catch (TypeNotPresentException e) {
            log.trace("Can't find type information for {}", e.typeName());
            return new UnknownTypeResolved(e.typeName(), toDeserialize);
        }
    }
}
