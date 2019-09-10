package store.jesframework.snapshot;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import javax.annotation.Nonnull;

import org.redisson.api.LocalCachedMapOptions;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.Codec;
import org.redisson.codec.JsonJacksonCodec;

import store.jesframework.Aggregate;

public class RedisSnapshotProvider implements SnapshotProvider, AutoCloseable {

    private static final int MAX_CACHE_SIZE = 5000;
    private final ConcurrentMap<UUID, Aggregate> redisCache;
    private final RedissonClient redissonClient;

    public RedisSnapshotProvider(@Nonnull RedissonClient redissonClient) {
        this(redissonClient, new JsonJacksonCodec(), MAX_CACHE_SIZE);
    }

    public RedisSnapshotProvider(@Nonnull RedissonClient redissonClient, @Nonnull Codec codec, int cacheSize) {
        this.redissonClient = Objects.requireNonNull(redissonClient, "Redisson client must not be null");
        final LocalCachedMapOptions<UUID, Aggregate> options = LocalCachedMapOptions.<UUID, Aggregate>defaults()
                .cacheSize(cacheSize)
                .evictionPolicy(LocalCachedMapOptions.EvictionPolicy.LRU)
                .reconnectionStrategy(LocalCachedMapOptions.ReconnectionStrategy.CLEAR)
                .syncStrategy(LocalCachedMapOptions.SyncStrategy.INVALIDATE);
        redisCache = redissonClient.getLocalCachedMap(getClass().getName(), codec, options);
    }

    @Nonnull
    @Override
    public <T extends Aggregate> T initialStateOf(@Nonnull UUID uuid, @Nonnull Class<T> type) {
        final Aggregate aggregate = redisCache.get(Objects.requireNonNull(uuid, "Aggregate uuid must not be null"));
        if (aggregate == null) {
            return SnapshotProvider.super.initialStateOf(uuid, type);
        }
        //noinspection unchecked
        return (T) aggregate;
    }

    @Nonnull
    @Override
    public <T extends Aggregate> T snapshot(@Nonnull T aggregate) {
        redisCache.put(aggregate.uuid(), aggregate);
        return aggregate;
    }

    @Override
    public void reset(@Nonnull UUID uuid) {
        Objects.requireNonNull(uuid, "Uuid mut not be null");
        redisCache.remove(uuid);
    }

    @Override
    public void close() {
        redissonClient.shutdown();
    }
}
