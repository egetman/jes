package io.jes.provider.cache;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.jes.Event;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CaffeineCacheProvider implements CacheProvider {

    private static final int DEFAULT_EXPIRE_POLICY = 15;

    private final Cache<Long, Event> cache;

    /**
     * Constructor for {@link CaffeineCacheProvider}.
     *
     * @throws IllegalArgumentException if {@literal cacheSize} is less than or equal 0.
     */
    public CaffeineCacheProvider(int cacheSize) {
        if (cacheSize <= 0) {
            throw new IllegalArgumentException("Cache size must be greater than 0: " + cacheSize);
        }
        cache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .expireAfterWrite(DEFAULT_EXPIRE_POLICY, TimeUnit.MINUTES)
                .build();
    }

    @Nullable
    @Override
    public Event get(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset value is less than 0: " + offset);
        }
        return cache.getIfPresent(offset);
    }

    @Override
    public void put(long offset, @Nonnull Event event) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset value is less than 0: " + offset);
        }
        cache.put(offset, Objects.requireNonNull(event, "Event must not be null"));
    }

    @Override
    public void invalidate() {
        cache.invalidateAll();
    }
}
