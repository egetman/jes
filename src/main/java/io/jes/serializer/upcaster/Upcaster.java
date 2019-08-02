package io.jes.serializer.upcaster;

import javax.annotation.Nonnull;

/**
 * Note: upcasters are WIP.
 *
 * @param <T> type of raw event.
 */
public interface Upcaster<T> {

    @Nonnull
    T upcast(long offset, T raw);

    @Nonnull
    String eventTypeName();
}
