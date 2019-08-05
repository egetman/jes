package io.jes.reactors;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import io.jes.Event;
import io.jes.JEventStore;
import io.jes.internal.Events.FancyEvent;
import io.jes.lock.JdbcLock;
import io.jes.lock.Lock;
import io.jes.offset.InMemoryOffset;
import io.jes.offset.JdbcOffset;
import io.jes.offset.Offset;
import io.jes.provider.JdbcStoreProvider;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static io.jes.internal.Events.SampleEvent;
import static io.jes.internal.FancyStuff.newPostgresDataSource;
import static java.lang.Runtime.getRuntime;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class SagaTest {

    @Test
    @SneakyThrows
    void cuncurrentSagasMustNotProcessDuplicates() {
        final DataSource dataSource = newPostgresDataSource();
        final JEventStore store = new JEventStore(new JdbcStoreProvider<>(dataSource, String.class));
        final Lock lock = new JdbcLock(dataSource);
        final Offset offset = new JdbcOffset(dataSource);

        final int iterationsCount = 30;
        final CountDownLatch isFailed = new CountDownLatch(1);
        final CountDownLatch counter = new CountDownLatch(iterationsCount * 2);
        // run 2 sagas
        try (TestLatchSaga ignored1 = new TestLatchSaga(store, offset, lock, counter, isFailed);
             TestLatchSaga ignored2 = new TestLatchSaga(store, offset, lock, counter, isFailed)) {

            // start writing events
            for (int i = 0; i < iterationsCount; i++) {
                // write 2 SampleEvent for each iteration
                store.write(new SampleEvent("name-" + i, randomUUID()));
                store.write(new SampleEvent("name-" + i, randomUUID()), new FancyEvent("foo", randomUUID()));
            }
            assertTrue(counter.await(5, SECONDS));
            assertFalse(isFailed.await(500, MILLISECONDS), "Illegal concurrent access detected");
        }
    }

    @Test
    @SneakyThrows
    void sagaShouldNotLoseAnyEventsOnConcurrentReadWrite() {
        final DataSource dataSource = newPostgresDataSource();
        final JdbcStoreProvider<String> provider = new JdbcStoreProvider<>(dataSource, String.class);
        final JEventStore store = new JEventStore(provider);
        final Lock lock = new JdbcLock(dataSource);
        final Offset offset = new InMemoryOffset();

        final int workersCount = getRuntime().availableProcessors();
        final ExecutorService executor = Executors.newFixedThreadPool(workersCount);

        final int streamSize = 100;
        final int submittedCount = streamSize * workersCount;

        final CountDownLatch tasksLatch = new CountDownLatch(workersCount);
        final CountDownLatch writesLatch = new CountDownLatch(submittedCount);
        final CountDownLatch readsLatch = new CountDownLatch(submittedCount);

        try (TestCounterSaga saga1 = new TestCounterSaga(store, offset, lock, writesLatch, readsLatch);
             TestCounterSaga saga2 = new TestCounterSaga(store, offset, lock, writesLatch, readsLatch)) {

            for (int i = 0; i < workersCount; i++) {
                executor.execute(() -> {
                    newParallelEventStream(streamSize).forEach(store::write);
                    tasksLatch.countDown();
                });
            }
            // all tasks are submitted, all streams are written
            assertTrue(tasksLatch.await(5, SECONDS), "All tasks was not submitted in specified amount of time");

            assertTrue(writesLatch.await(20, SECONDS), "Written events count differ from " + submittedCount);
            assertTrue(readsLatch.await(20, SECONDS), "Read events count differ from " + submittedCount);

            assertEquals(0, saga1.counter.intValue() + saga2.counter.intValue(), "Inner counters failed to reach 0");
            assertEquals(
                    submittedCount * 2, store.readFrom(0).count(),
                    "Total events count in store differ from " + submittedCount * 2);
        } finally {
            executor.shutdown();
            provider.close();
        }
    }

    @SuppressWarnings("SameParameterValue")
    private Stream<? extends Event> newParallelEventStream(int count) {
        return IntStream.range(0, count).mapToObj(val -> new FancyEvent("name " + val, randomUUID())).parallel();
    }

    @Slf4j
    private static class TestCounterSaga extends Saga {

        private final LongAdder counter = new LongAdder();
        private final CountDownLatch expectedReads;
        private final CountDownLatch expectedWrites;

        TestCounterSaga(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull Lock lock,
                        CountDownLatch expectedWrites, CountDownLatch expectedReads) {
            super(store, offset, lock);
            this.expectedReads = expectedReads;
            this.expectedWrites = expectedWrites;
        }

        @ReactsOn
        @SuppressWarnings("unused")
        void handle(@Nonnull FancyEvent event) {
            if (expectedWrites.getCount() == 0) {
                log.error("Writes count already 0");
            }
            counter.increment();
            expectedWrites.countDown();
            store.write(new SampleEvent(event.getName(), event.uuid()));
        }

        @ReactsOn
        @SuppressWarnings("unused")
        void handle(@Nonnull SampleEvent event) {
            if (expectedReads.getCount() == 0) {
                log.error("Reads count already 0");
            }
            counter.decrement();
            expectedReads.countDown();
        }

    }

    private static class TestLatchSaga extends Saga {

        private final CountDownLatch counter;
        private final CountDownLatch isFailed;

        TestLatchSaga(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull Lock lock, CountDownLatch counter,
                      CountDownLatch isFailed) {
            super(store, offset, lock);
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