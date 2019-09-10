package store.jesframework.offset;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

import org.redisson.api.RLongAdder;
import org.redisson.api.RedissonClient;

public class RedisOffset implements Offset, AutoCloseable {

    private static final String MISSING_KEY = "Key must not be null";

    private final RedissonClient redissonClient;
    private final Map<String, RLongAdder> offsets = new ConcurrentHashMap<>();

    public RedisOffset(@Nonnull RedissonClient redissonClient) {
        this.redissonClient = Objects.requireNonNull(redissonClient);
    }

    @Override
    public long value(@Nonnull String key) {
        return getOffsetByKey(key).sum();
    }

    @Override
    public void increment(@Nonnull String key) {
        getOffsetByKey(key).increment();
    }

    @Override
    public void reset(@Nonnull String key) {
        getOffsetByKey(key).reset();
    }

    @Nonnull
    private RLongAdder getOffsetByKey(@Nonnull String key) {
        return offsets.computeIfAbsent(Objects.requireNonNull(key, MISSING_KEY), redissonClient::getLongAdder);
    }

    @Override
    public void close() {
        redissonClient.shutdown();
    }
}
