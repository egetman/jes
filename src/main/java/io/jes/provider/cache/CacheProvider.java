package io.jes.provider.cache;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.jes.Event;

/**
 * A {@link CacheProvider} is a Map-like data structure that provides temporary storage of application data. It stores
 * and load events based on the event offset value.
 */
public interface CacheProvider {

    /**
     * Return cached event based on it's offset if present;
     *
     * @param offset value to retrieve event.
     * @return found by offset event, null otherwise.
     * @throws IllegalArgumentException when offset value is less than 0.
     */
    @Nullable
    Event get(long offset);

    /**
     * Put given event into the cache with a specified offset value.
     *
     * @param offset value to store event. It acts as a key in the cache.
     * @param event  is an event to store.
     * @throws NullPointerException     if the event is null.
     * @throws IllegalArgumentException when offset value is less than 0.
     */
    void put(long offset, @Nonnull Event event);

    /**
     * Clears the entire cache.
     */
    void invalidate();
}
