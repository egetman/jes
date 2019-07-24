package io.jes.lock;

import java.util.concurrent.locks.ReadWriteLock;
import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

abstract class AbstractReadWriteLockManager implements LockManager {

    @Override
    public void doProtectedRead(@Nonnull String key, @Nonnull Runnable action) {
        final ReadWriteLock lock = getLockByKey(key);
        try {
            lock.readLock().lock();
            requireNonNull(action, "Action must not be null").run();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void doProtectedWrite(@Nonnull String key, @Nonnull Runnable action) {
        final ReadWriteLock lock = getLockByKey(key);
        try {
            lock.writeLock().lock();
            requireNonNull(action, "Action must not be null").run();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Nonnull
    protected abstract ReadWriteLock getLockByKey(@Nonnull String key);

}
