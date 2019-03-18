package io.jes.lock;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static io.jes.internal.FancyStuff.newRedissonClient;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class LockManagerTest {

    private static final Collection<LockManager> LOCK_MANAGERS = asList(
            new InMemoryReentrantLockManager(),
            new RedissonReentrantLockManager(newRedissonClient())
    );

    private static Collection<LockManager> createLockManagers() {
        return LOCK_MANAGERS;
    }

    @ParameterizedTest
    @MethodSource("createLockManagers")
    @SuppressWarnings("ConstantConditions")
    void shouldThrowNpeOnNullArguments(@Nonnull LockManager lockManager) {
        assertThrows(NullPointerException.class, () -> lockManager.doProtectedRead(null, () -> {}));
        assertThrows(NullPointerException.class, () -> lockManager.doProtectedRead("", null));

        assertThrows(NullPointerException.class, () -> lockManager.doProtectedWrite(null, () -> {}));
        assertThrows(NullPointerException.class, () -> lockManager.doProtectedWrite("", null));
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("createLockManagers")
    void protectedReadShouldAllowMultipleReaders(@Nonnull LockManager lockManager) {
        final int readersCount = 5;
        final String key = UUID.randomUUID().toString();
        final ExecutorService threadPool = Executors.newCachedThreadPool();
        final CountDownLatch latch = new CountDownLatch(readersCount);

        for (int i = 0; i < readersCount; i++) {
            // all 3 readers concurrently executes protection read context
            threadPool.execute(() -> lockManager.doProtectedRead(key, () -> {
                latch.countDown();
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    log.error("", e);
                    Thread.currentThread().interrupt();
                }
            }));
        }

        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("createLockManagers")
    void protectedWriteShouldBlockReaders(@Nonnull LockManager lockManager) {
        final String key = UUID.randomUUID().toString();
        final ExecutorService threadPool = Executors.newCachedThreadPool();
        final CountDownLatch latch = new CountDownLatch(2);

        threadPool.execute(() -> lockManager.doProtectedWrite(key, () -> {
            threadPool.execute(() -> lockManager.doProtectedRead(key, latch::countDown));
            latch.countDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                log.error("", e);
                Thread.currentThread().interrupt();
            }
        }));

        assertFalse(latch.await(2, TimeUnit.SECONDS));
    }

    // note: only for reentrant impl's
    @SneakyThrows
    @ParameterizedTest
    @MethodSource("createLockManagers")
    void protectedWriteShouldAllowReacquisitionBySameThread(@Nonnull LockManager lockManager) {
        final String key = UUID.randomUUID().toString();
        final ExecutorService threadPool = Executors.newSingleThreadExecutor();
        final CountDownLatch latch = new CountDownLatch(2);

        threadPool.execute(() -> lockManager.doProtectedWrite(key, () -> {
            lockManager.doProtectedWrite(key, latch::countDown);
            latch.countDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                log.error("", e);
                Thread.currentThread().interrupt();
            }
        }));

        assertTrue(latch.await(2, TimeUnit.SECONDS));
    }

}