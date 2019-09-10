package store.jesframework.lock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import static java.util.Objects.requireNonNull;

@Slf4j
public class InMemoryReentrantLock extends AbstractReadWriteLock {

    private final Map<String, ReadWriteLock> locks = new ConcurrentHashMap<>();

    @Nonnull
    @Override
    protected ReadWriteLock getLockByKey(@Nonnull String key) {
        return locks.computeIfAbsent(requireNonNull(key, "Key must not be null"), str -> new ReentrantReadWriteLock());
    }
}
