package io.jes.provider.cache;

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.codec.JsonJacksonCodec;

import io.jes.Event;

public class RedisCacheProvider implements CacheProvider, AutoCloseable {

    private static final int DEFAULT_CACHE_SIZE = 5000;
    private final ConcurrentMap<Long, Event> cache;
    private final RedissonClient redissonClient;

    /**
     * Constructor for {@link RedisCacheProvider}.
     *
     * @param redissonClient is a reddison client used to build cache provider instance.
     * @throws NullPointerException if {@literal redissonClient} is null.
     */
    @SuppressWarnings("WeakerAccess")
    public RedisCacheProvider(@Nonnull RedissonClient redissonClient) {
        this(redissonClient, new JsonJacksonCodec(), DEFAULT_CACHE_SIZE);
    }

    /**
     * Constructor for {@link RedisCacheProvider}.
     *
     * @param redissonClient is a reddison client used to build cache provider instance.
     * @param codec          is a codec to use during encoding/decoding data for network interaction.
     * @param cacheSize      is a cahe size to use.
     * @throws NullPointerException     if {@literal redissonClient} or {@literal codec} is null.
     * @throws IllegalArgumentException if cache size is 0 or below.
     */
    public RedisCacheProvider(@Nonnull RedissonClient redissonClient, @Nonnull Codec codec, int cacheSize) {
        if (cacheSize <= 0) {
            throw new IllegalArgumentException("Cache size must be greater than 0: " + cacheSize);
        }
        this.redissonClient = Objects.requireNonNull(redissonClient, "RedissonClient must not be null");

        final LocalCachedMapOptions<Long, Event> options = LocalCachedMapOptions.<Long, Event>defaults()
                .cacheSize(cacheSize)
                .evictionPolicy(LocalCachedMapOptions.EvictionPolicy.LRU)
                .reconnectionStrategy(LocalCachedMapOptions.ReconnectionStrategy.CLEAR)
                .syncStrategy(LocalCachedMapOptions.SyncStrategy.INVALIDATE);

        cache = redissonClient.getLocalCachedMap(getClass().getName(), Objects.requireNonNull(codec), options);
    }

    @Nullable
    @Override
    public Event get(long offset) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset value is less than 0: " + offset);
        }
        return cache.get(offset);
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
        cache.clear();
    }

    @Override
    public void close() {
        redissonClient.shutdown();
    }
}
