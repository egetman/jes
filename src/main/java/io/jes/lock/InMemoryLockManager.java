package io.jes.lock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@Slf4j
public class InMemoryLockManager implements LockManager {

    private static final String LOCK_ACQUISITION = "%s lock acquired by %s";
    private static final String LOCK_RELEASE = "%s lock released by %s";

    private static final Map<String, ReadWriteLock> LOCKS = new ConcurrentHashMap<>();

    @Override
    public void doExclusive(@Nonnull String key, @Nonnull Runnable action) {
        final ReadWriteLock lock = getLockByKey(key);
        try {
            lock.writeLock().lock();
            log.trace(format(LOCK_ACQUISITION, "Exclusive (Protected write)", key));
            requireNonNull(action, "Action must not be null").run();
        } finally {
            lock.writeLock().unlock();
            log.trace(format(LOCK_RELEASE, "Exclusive (Protected write)", key));
        }
    }

    @Override
    public void doProtectedRead(@Nonnull String key, @Nonnull Runnable action) {
        final ReadWriteLock lock = getLockByKey(key);
        try {
            lock.readLock().lock();
            log.trace(format(LOCK_ACQUISITION, "Protected read", key));
            requireNonNull(action, "Action must not be null").run();
        } finally {
            lock.readLock().unlock();
            log.trace(format(LOCK_RELEASE, "Protected read", key));
        }
    }

    @Override
    public void doProtectedWrite(@Nonnull String key, @Nonnull Runnable action) {
        doExclusive(key, action);
    }

    @Nonnull
    private ReadWriteLock getLockByKey(@Nonnull String key) {
        return LOCKS.computeIfAbsent(requireNonNull(key, "Key must not be null"), str -> new ReentrantReadWriteLock());
    }
}
