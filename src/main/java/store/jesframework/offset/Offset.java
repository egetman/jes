package store.jesframework.offset;

import javax.annotation.Nonnull;

import store.jesframework.JEventStore;

/**
 * Offset used by all {@literal Reactor} subclasses. It's main responsibility to provide actual and consistent
 * information about current {@link JEventStore} offset state in clustered/distributed environment for
 * concrete client.
 */
public interface Offset {

    /**
     * Add given value to current offset by specified {@code key}.
     *
     * @param key is an owner identifier, that manages this offset.
     * @param value is a value to add.
     */
    default void add(@Nonnull String key, long value) {
        for (long i = 0; i < value; i++) {
            increment(key);
        }
    }

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
