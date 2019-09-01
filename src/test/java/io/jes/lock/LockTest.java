package io.jes.lock;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static io.jes.internal.FancyStuff.newPostgresDataSource;
import static io.jes.internal.FancyStuff.newRedissonClient;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class LockTest {

    private static final Collection<Lock> REENTRANT_LOCK_MANAGERS = asList(
            new InMemoryReentrantLock(),
            new RedisReentrantLock(newRedissonClient())
    );

    private static final Collection<Lock> ALL_LOCKS = asList(
            new InMemoryReentrantLock(),
            new RedisReentrantLock(newRedissonClient()),
            new JdbcLock(newPostgresDataSource())
    );

    private static Collection<Lock> reentrantLocks() {
        return REENTRANT_LOCK_MANAGERS;
    }

    private static Collection<Lock> allLocks() {
        return ALL_LOCKS;
    }

    @ParameterizedTest
    @MethodSource("allLocks")
    @SuppressWarnings("ConstantConditions")
    void shouldThrowNpeOnNullArguments(@Nonnull Lock lock) {
        assertThrows(NullPointerException.class, () -> lock.doProtectedWrite(null, () -> {}));
        assertThrows(NullPointerException.class, () -> lock.doProtectedWrite("", null));
    }

    // note: only for reentrant impl's
    @SneakyThrows
    @ParameterizedTest
    @MethodSource("reentrantLocks")
    void protectedWriteShouldAllowReacquisitionBySameThread(@Nonnull Lock lock) {
        final String key = UUID.randomUUID().toString();
        final ExecutorService threadPool = Executors.newSingleThreadExecutor();
        final CountDownLatch latch = new CountDownLatch(2);

        threadPool.execute(() -> lock.doProtectedWrite(key, () -> {
            lock.doProtectedWrite(key, latch::countDown);
            latch.countDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                log.error("", e);
                Thread.currentThread().interrupt();
            }
        }));

        assertTrue(latch.await(2, TimeUnit.SECONDS));
        threadPool.shutdown();
    }

    @Test
    @SneakyThrows
    void protectedWriteShouldPreventAllConcurrentAccess() {
        final DataSource dataSource = newPostgresDataSource();
        final JdbcLock first = new JdbcLock(dataSource);
        final JdbcLock second = new JdbcLock(dataSource);

        @RequiredArgsConstructor
        class Accessor implements Runnable {

            private final CountDownLatch latch;

            @Override
            public void run() {
                latch.countDown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    log.error("", e);
                    Thread.currentThread().interrupt();
                }
            }
        }

        final String key = UUID.randomUUID().toString();
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final CountDownLatch latch = new CountDownLatch(2);

        threadPool.execute(() -> first.doProtectedWrite(key, new Accessor(latch)));
        threadPool.execute(() -> second.doProtectedWrite(key, new Accessor(latch)));

        assertFalse(latch.await(2, TimeUnit.SECONDS));
        threadPool.shutdown();
    }

    @AfterAll
    @SneakyThrows
    static void closeResources() {
        closeAll(ALL_LOCKS);
        closeAll(REENTRANT_LOCK_MANAGERS);
    }

    private static void closeAll(Collection<Lock> locks) {
        for (Lock lock : locks) {
            if (lock instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) lock).close();
                } catch (Exception ignored) {}
            }
        }
    }

}