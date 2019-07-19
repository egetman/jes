package io.jes.offset;

import javax.annotation.Nonnull;

/**
 * Offset used by all {@literal Reactor} subclasses. It's main responsibility to provide actual and consistent
 * information about current {@link io.jes.JEventStore} offset state in clustered/distributed environment for
 * concrete client.
 */
public interface Offset {

    /**
     * @param key is an owner identifier, that manages this offset.
     * @return current offset value by specified {@code key}.
     */
    long value(@Nonnull String key);

    /**
     * Increment current offset value by specified {@code key}.
     *
     * @param key is an owner identifier, that manages this offset.
     */
    void increment(@Nonnull String key);

    /**
     * Reset offset value by specified {@code key}.
     *
     * @param key is an owner identifier, that manages this offset.
     */
    void reset(@Nonnull String key);

}
