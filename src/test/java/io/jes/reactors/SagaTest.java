package io.jes.reactors;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import io.jes.JEventStore;
import io.jes.bus.CommandBus;
import io.jes.bus.SyncCommandBus;
import io.jes.internal.Events.FancyEvent;
import io.jes.internal.FancyStuff;
import io.jes.lock.JdbcLock;
import io.jes.lock.Lock;
import io.jes.offset.JdbcOffset;
import io.jes.offset.Offset;
import io.jes.provider.JdbcStoreProvider;
import lombok.SneakyThrows;

import static io.jes.internal.Events.SampleEvent;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class SagaTest {

    @Test
    @SneakyThrows
    void cuncurrentSagasMustNotProcessDuplicates() {
        final DataSource dataSource = FancyStuff.newPostgresDataSource();
        final JEventStore store = new JEventStore(new JdbcStoreProvider<>(dataSource, String.class));
        final Lock lock = new JdbcLock(dataSource);
        final Offset offset = new JdbcOffset(dataSource);
        final CommandBus bus = new SyncCommandBus();

        final int iterationsCount = 30;
        final CountDownLatch isFailed = new CountDownLatch(1);
        final CountDownLatch counter = new CountDownLatch(iterationsCount * 2);
        // run 2 sagas
        try (TestSaga ignored1 = new TestSaga(store, offset, bus, lock, counter, isFailed);
             TestSaga ignored2 = new TestSaga(store, offset, bus, lock, counter, isFailed)) {

            // start writing events
            for (int i = 0; i < iterationsCount; i++) {
                // write 2 SampleEvent for each iteration
                store.write(new SampleEvent("name-" + i, randomUUID()));
                store.write(new SampleEvent("name-" + i, randomUUID()), new FancyEvent("foo", randomUUID()));
            }
            assertTrue(counter.await(5, TimeUnit.SECONDS));
            assertFalse(isFailed.await(500, TimeUnit.MILLISECONDS), "Illegal concurrent access detected");
        }
    }

    private static class TestSaga extends Saga {

        private final CountDownLatch counter;
        private final CountDownLatch isFailed;

        TestSaga(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull CommandBus bus, @Nonnull Lock lock,
                 @Nonnull CountDownLatch counter, @Nonnull CountDownLatch isFailed) {
            super(store, offset, bus, lock);
            this.counter = Objects.requireNonNull(counter);
            this.isFailed = isFailed;
        }

        @ReactsOn
        @SuppressWarnings("unused")
        void handle(@Nonnull SampleEvent event) {
            if (counter.getCount() == 0) {
                isFailed.countDown();
                fail("Handled events count reach 0. New event received: " + event);
            }
            counter.countDown();
        }

    }

}