package store.jesframework.lock;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import javax.annotation.Nonnull;

import org.redisson.api.RedissonClient;

import lombok.extern.slf4j.Slf4j;

import static java.util.Objects.requireNonNull;

@Slf4j
public class RedisReentrantLock extends AbstractReadWriteLock implements AutoCloseable {

    private final RedissonClient redissonClient;
    private final Map<String, ReadWriteLock> locks = new ConcurrentHashMap<>();

    public RedisReentrantLock(@Nonnull RedissonClient redissonClient) {
        this.redissonClient = Objects.requireNonNull(redissonClient);
    }

    @Nonnull
    @Override
    protected ReadWriteLock getLockByKey(@Nonnull String key) {
        return locks.computeIfAbsent(requireNonNull(key, "Key must not be null"), redissonClient::getReadWriteLock);
    }

    @Override
    public void close() {
        redissonClient.shutdown();
    }
}
