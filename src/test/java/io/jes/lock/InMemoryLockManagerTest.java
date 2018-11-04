package io.jes.lock;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class InMemoryLockManagerTest {

    @Test
    @SuppressWarnings("ConstantConditions")
    void shouldThrowNpeOnNullArguments() {
        final LockManager lockManager = new InMemoryLockManager();

        assertThrows(NullPointerException.class, () -> lockManager.doExclusive(null, () -> {}));
        assertThrows(NullPointerException.class, () -> lockManager.doExclusive("", null));

        assertThrows(NullPointerException.class, () -> lockManager.doProtectedRead(null, () -> {}));
        assertThrows(NullPointerException.class, () -> lockManager.doProtectedRead("", null));

        assertThrows(NullPointerException.class, () -> lockManager.doProtectedWrite(null, () -> {}));
        assertThrows(NullPointerException.class, () -> lockManager.doProtectedWrite("", null));
    }

    @Test
    @SneakyThrows
    void protectedReadShouldAllowMultipleReaders() {
        final int readersCount = 5;
        final String key = UUID.randomUUID().toString();
        final LockManager lockManager = new InMemoryLockManager();
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

    @Test
    @SneakyThrows
    void protectedWriteShouldBlockReaders() {
        final String key = UUID.randomUUID().toString();
        final LockManager lockManager = new InMemoryLockManager();
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

    @Test
    @SneakyThrows
    void exclusiveLockShouldBlockReaders() {
        final String key = UUID.randomUUID().toString();
        final LockManager lockManager = new InMemoryLockManager();
        final ExecutorService threadPool = Executors.newCachedThreadPool();
        final CountDownLatch latch = new CountDownLatch(2);

        threadPool.execute(() -> lockManager.doExclusive(key, () -> {
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

    @Test
    @SneakyThrows
    void exclusiveLockShouldAllowReacquisitionBySameThread() {
        final String key = UUID.randomUUID().toString();
        final LockManager lockManager = new InMemoryLockManager();
        final ExecutorService threadPool = Executors.newSingleThreadExecutor();
        final CountDownLatch latch = new CountDownLatch(2);

        threadPool.execute(() -> lockManager.doExclusive(key, () -> {
            lockManager.doExclusive(key, latch::countDown);
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

    @Test
    @SneakyThrows
    void protectedWriteShouldAllowReacquisitionBySameThread() {
        final String key = UUID.randomUUID().toString();
        final LockManager lockManager = new InMemoryLockManager();
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