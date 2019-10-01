package store.jesframework.lock;

import java.util.concurrent.locks.ReadWriteLock;
import javax.annotation.Nonnull;

import static java.util.Objects.requireNonNull;

abstract class AbstractReadWriteLock implements Lock {

    @Override
    public void doExclusively(@Nonnull String key, @Nonnull Runnable action) {
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
