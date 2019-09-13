package store.jesframework.serializer.api;

import javax.annotation.Nonnull;

/**
 * Note: upcasters are WIP.
 *
 * @param <T> type of raw event.
 */
public interface Upcaster<T> extends SerializationOption {

    /**
     * Perform the upcast operation on a raw type.
     *
     * @param raw is a serialized event form to upcast.
     * @return 'upcasted' serialized event.
     */
    @Nonnull
    T upcast(@Nonnull T raw);

    /**
     * Return event type name for upcast operations.
     *
     * @return event type name (or event alias) that should be upcasted.
     */
    @Nonnull
    String eventTypeName();
}
