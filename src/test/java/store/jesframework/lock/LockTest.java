package store.jesframework.lock;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
import store.jesframework.ex.BrokenStoreException;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static store.jesframework.internal.FancyStuff.newPostgresDataSource;
import static store.jesframework.internal.FancyStuff.newRedissonClient;

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

    @Test
    @SneakyThrows
    void exceptionsInJdbcLockShouldBeWrappedInBrokenStoreException() {
        final DataSource dataSource = mock(DataSource.class);
        final Connection connection = mock(Connection.class);
        when(dataSource.getConnection()).thenReturn(connection);

        // first verify creation
        assertThrows(BrokenStoreException.class, () -> new JdbcLock(dataSource));

        final DatabaseMetaData metadata = mock(DatabaseMetaData.class);
        when(connection.getMetaData()).thenReturn(metadata);
        when(metadata.getDatabaseProductName()).thenReturn("PostgreSQL");
        final PreparedStatement statement = mock(PreparedStatement.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);

        // ok, create new one, try all other methods
        final JdbcLock lock = new JdbcLock(dataSource);

        assertThrows(BrokenStoreException.class, () -> lock.doProtectedWrite("", () -> {
            throw new IllegalArgumentException("Foo");
        }));

        // SQLException just handled as warn
        when(statement.executeUpdate()).thenThrow(SQLException.class);
        assertDoesNotThrow(() -> lock.doProtectedWrite("", () -> {}));
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