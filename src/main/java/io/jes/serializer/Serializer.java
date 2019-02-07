package io.jes.serializer;

/**
 * Base interface for event serialization stuff.
 *
 * @param <S> type of source serialization format. (i.e. it's entity type to process)
 * @param <T> type of target serialization format. (i.e. it's raw data to process)
 */
public interface Serializer<S, T> {

    T serialize(S toSerialize);

    S deserialize(T toDeserialize);
}
