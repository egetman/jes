package io.jes.serializer;

import io.jes.Event;

public interface EventSerializer<T> {

    T serialize(Event event);

    Event deserialize(T event);
}
