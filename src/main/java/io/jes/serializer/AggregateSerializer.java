package io.jes.serializer;

import io.jes.Aggregate;

/**
 * Base interface for aggregate serialization stuff.
 *
 * @param <T> type of target serialization format.
 */
public interface AggregateSerializer<T> {

    T serialize(Aggregate event);

    Aggregate deserialize(T event);

}
