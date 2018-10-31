package io.jes.lock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import lombok.extern.slf4j.Slf4j;

import static java.lang.String.*;

@Slf4j
@SuppressWarnings("unused")
public final class StandaloneLockManager implements LockManager {

    private static final String LOCK_ACQUISITION = "Lock acquired by %s";
    private static final String LOCK_RELEASE = "Lock released by %s";

    private static final LockManager MANAGER = new StandaloneLockManager();
    private static final Map<String, ReadWriteLock> LOCKS = new ConcurrentHashMap<>();

    private StandaloneLockManager() {}

    public static LockManager getInstance() {
        return MANAGER;
    }

    @Override
    public void doExclusive(String lockName, Runnable action) {
        final ReadWriteLock lock = LOCKS.computeIfAbsent(lockName, ignored -> new ReentrantReadWriteLock());
        try {
            lock.writeLock().lock();
            log.trace(format(LOCK_ACQUISITION, lockName));
            action.run();
        } finally {
            lock.writeLock().unlock();
            log.trace(format(LOCK_RELEASE, lockName));
        }
    }

    @Override
    public void doProtectedRead(String lockName, Runnable action) {
        final ReadWriteLock lock = LOCKS.computeIfAbsent(lockName, ignored -> new ReentrantReadWriteLock());
        try {
            lock.readLock().lock();
            log.trace(format(LOCK_ACQUISITION, lockName));
            action.run();
        } finally {
            lock.readLock().unlock();
            log.trace(format(LOCK_RELEASE, lockName));
        }
    }

    @Override
    public void doProtectedWrite(String lockName, Runnable action) {
        doExclusive(lockName, action);
    }
}
