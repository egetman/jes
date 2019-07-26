package io.jes.lock;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import javax.annotation.Nonnull;

import org.redisson.api.RedissonClient;

import lombok.extern.slf4j.Slf4j;

import static java.util.Objects.requireNonNull;

@Slf4j
public class RedissonReentrantLock extends AbstractReadWriteLock {

    private final RedissonClient redissonClient;
    private final Map<String, ReadWriteLock> locks = new ConcurrentHashMap<>();

    @SuppressWarnings("WeakerAccess")
    public RedissonReentrantLock(@Nonnull RedissonClient redissonClient) {
        this.redissonClient = Objects.requireNonNull(redissonClient);
    }

    @Nonnull
    @Override
    protected ReadWriteLock getLockByKey(@Nonnull String key) {
        return locks.computeIfAbsent(requireNonNull(key, "Key must not be null"), redissonClient::getReadWriteLock);
    }
}
