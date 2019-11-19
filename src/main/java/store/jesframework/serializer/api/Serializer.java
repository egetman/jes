package store.jesframework.serializer.api;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Base interface for event serialization stuff.
 *
 * @param <S> type of source serialization format. (i.e. its entity type to process)
 * @param <T> type of target serialization format. (i.e. its raw data to process)
 */
public interface Serializer<S, T> {

    /**
     * Serializes given type {@code S} to raw {@code T} form.
     *
     * @param toSerialize is a type to serialize.
     * @return raw 'serialized' form of supplied type.
     */
    @Nonnull
    T serialize(@Nonnull S toSerialize);

    /**
     * Deserializes specified 'raw' form {@code T} to a type {@code S}.
     *
     * @param toDeserialize is a 'raw' data to deserialize.
     * @return deserialized type.
     */
    @Nonnull
    S deserialize(@Nonnull T toDeserialize);

    /**
     * This method tries to retrieve event name from the specified 'raw' event data.
     *
     * @param raw is an event serialized form.
     * @return the name of given event or null, if can't fetch it.
     */
    @Nullable
    default String fetchTypeName(@Nonnull T raw) {
        return null;
    }

    /**
     * @return returns the supported by this serializer format.
     */
    @Nonnull
    Format format();

}
