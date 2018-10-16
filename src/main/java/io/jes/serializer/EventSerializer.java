package io.jes.serializer;

import io.jes.Event;

/**
 * Base interface for event serialization stuff.
 *
 * @param <T> type of target serialization format.
 */
public interface EventSerializer<T> {

    T serialize(Event event);

    Event deserialize(T event);
}
