package store.jesframework.reactors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import store.jesframework.Event;
import store.jesframework.JEventStore;
import store.jesframework.common.SagaFailure;
import store.jesframework.internal.Events;
import store.jesframework.lock.InMemoryReentrantLock;
import store.jesframework.lock.JdbcLock;
import store.jesframework.lock.Lock;
import store.jesframework.offset.InMemoryOffset;
import store.jesframework.offset.JdbcOffset;
import store.jesframework.offset.Offset;
import store.jesframework.provider.InMemoryStoreProvider;
import store.jesframework.provider.JdbcStoreProvider;

import static java.lang.Runtime.getRuntime;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static store.jesframework.internal.FancyStuff.newPostgresDataSource;

@Slf4j
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
        try (final Saga ignored1 = new TestLatchSaga(store, offset, lock, counter, isFailed);
             final Saga ignored2 = new TestLatchSaga(store, offset, lock, counter, isFailed)) {

            // start writing events
            for (int i = 0; i < iterationsCount; i++) {
                // write 2 SampleEvent for each iteration
                store.write(new Events.SampleEvent("name-" + i, randomUUID()));
                store.write(new Events.SampleEvent("name-" + i, randomUUID()), new Events.FancyEvent("foo", randomUUID()));
            }
            assertTrue(counter.await(5, SECONDS));
            assertFalse(isFailed.await(500, MILLISECONDS), "Illegal concurrent access detected");
        }
    }

    @SneakyThrows
    @RepeatedTest(5)
    void sagaShouldNotLoseAnyEventsOnConcurrentReadWrite() {
        final JdbcStoreProvider<String> provider = new JdbcStoreProvider<>(newPostgresDataSource(), String.class);

        final JEventStore store = new JEventStore(provider);
        final Lock lock = new InMemoryReentrantLock();
        final Offset offset = new InMemoryOffset();

        final int workersCount = getRuntime().availableProcessors();
        final ExecutorService executor = Executors.newFixedThreadPool(workersCount);

        final int startEventsWrites = 200;
        final int inFlightEventWrites = 200;
        final int submittedCount = startEventsWrites * workersCount;

        // first write {startEventsWrites} FancyEvents.
        newFancyEventStream(submittedCount).forEach(store::write);

        // we expecting to read all submitted Fancy events (on each FancyEvent read Saga writes one SampleEvent) and
        // all Sample events written by Sagas + inFlightEventWrites SampleEvents writes by hand
        final CountDownLatch fancyEventReads = new CountDownLatch(submittedCount);
        final CountDownLatch sampleEventReads = new CountDownLatch(submittedCount + inFlightEventWrites);

        try (final Saga ignored = new TestCounterSaga(store, offset, lock, fancyEventReads, sampleEventReads);
             final Saga ignored1 = new TestCounterSaga(store, offset, lock, fancyEventReads, sampleEventReads);
             final Saga ignored2 = new TestCounterSaga(store, offset, lock, fancyEventReads, sampleEventReads);
             final Saga ignored3 = new TestCounterSaga(store, offset, lock, fancyEventReads, sampleEventReads);
             final Saga ignored4 = new TestCounterSaga(store, offset, lock, fancyEventReads, sampleEventReads);
             final Saga ignored5 = new TestCounterSaga(store, offset, lock, fancyEventReads, sampleEventReads)) {

            newSampleEventStream(inFlightEventWrites).forEach(store::write);
            // all tasks are submitted, all streams are written here

            assertTrue(fancyEventReads.await(20, SECONDS), "Not all FancyEvents was read in 20 sec");
            assertTrue(sampleEventReads.await(20, SECONDS), "Not all SampleEvents was read in 20 sec");

            // each handling of FancyEvent by TestCounterSaga write one SampleEvent, so total count is x2
            final long expectedEventsCount = submittedCount * 2 + inFlightEventWrites;
            final long actualEventsCount;
            try (Stream<Event> stream = store.readFrom(0)) {
                actualEventsCount = stream.count();
            }

            assertEquals(expectedEventsCount, actualEventsCount, "Total events count doesn't match with expected");

            //noinspection CastCanBeRemovedNarrowingVariableType
            final Collection<String> duplicates = ((TestCounterSaga) ignored).listDuplicatedEvents();
            assertTrue(duplicates.isEmpty(), "Found duplicated events: " + duplicates);
        } finally {
            executor.shutdown();
            provider.close();
        }
    }

    @Test
    @SneakyThrows
    void sagaShouldWriteSagaFailureEventOnFailedEventHandling() {
        final JEventStore store = new JEventStore(new InMemoryStoreProvider());
        final InMemoryOffset offset = new InMemoryOffset();
        final InMemoryReentrantLock lock = new InMemoryReentrantLock();

        final CountDownLatch latch = new CountDownLatch(1);
        //noinspection unused
        final Saga saga = new Saga(store, offset, lock) {
            @ReactsOn
            void handle(Events.SampleEvent  event) {
                throw new IllegalStateException("Boom");
            }
            @ReactsOn
            void handle(SagaFailure event) {
                latch.countDown();
                log.debug("Received {}", event);
            }
        };

        final UUID uuid = randomUUID();
        store.write(new Events.SampleEvent("Sample", uuid));

        assertTrue(latch.await(2, SECONDS));

        final Collection<Event> actual = store.readBy(uuid);
        assertEquals(2, actual.size());

        final Iterator<Event> iterator = actual.iterator();
        assertEquals(Events.SampleEvent.class, iterator.next().getClass());
        assertEquals(SagaFailure.class, iterator.next().getClass());
    }

    @SuppressWarnings("SameParameterValue")
    private Stream<? extends Event> newFancyEventStream(int count) {
        return IntStream.range(0, count).mapToObj(val -> new Events.FancyEvent("# " + val, randomUUID()));
    }

    @SuppressWarnings("SameParameterValue")
    private Stream<? extends Event> newSampleEventStream(int count) {
        return IntStream.range(0, count).mapToObj(val -> new Events.SampleEvent("# " + val, randomUUID()));
    }

    @Slf4j
    private static class TestCounterSaga extends Saga {

        private final CountDownLatch expectedFancyEventReads;
        private final CountDownLatch expectedSampleEventReads;

        // this maps used across all sagas of type TestCounterSaga
        static final Map<String, LongAdder> FANCY_EVENT_READS = new ConcurrentHashMap<>();
        static final Map<String, LongAdder> SAMPLE_EVENT_READS = new ConcurrentHashMap<>();
        static final Map<String, LongAdder> SAMPLE_EVENT_WRITES = new ConcurrentHashMap<>();

        TestCounterSaga(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull Lock lock,
                        CountDownLatch expectedFancyEventReads, CountDownLatch expectedSampleEventReads) {
            super(store, offset, lock);
            this.expectedFancyEventReads = expectedFancyEventReads;
            this.expectedSampleEventReads = expectedSampleEventReads;

            FANCY_EVENT_READS.clear();
            SAMPLE_EVENT_READS.clear();
            SAMPLE_EVENT_WRITES.clear();
        }

        @Nonnull
        private String eventToString(@Nonnull Events.FancyEvent event) {
            final UUID uuid = event.uuid();
            if (uuid == null) {
                return event.getName();
            }
            return uuid.toString() + " " + event.getName();
        }

        @Nonnull
        private String eventToString(@Nonnull Events.SampleEvent event) {
            return event.uuid().toString() + " " + event.getName();
        }

        @ReactsOn
        @SuppressWarnings("unused")
        void handle(@Nonnull Events.FancyEvent event) {
            LongAdder adder = FANCY_EVENT_READS.putIfAbsent(eventToString(event), new LongAdder());
            if (adder != null) {
                adder.increment();
                if (adder.intValue() != 1) {
                    log.error("Offset error at Fancy Read {}", offset.value(getKey()));
                }
            }

            expectedFancyEventReads.countDown();

            final Events.SampleEvent sampleEvent = new Events.SampleEvent(event.getName(), event.uuid());
            store.write(sampleEvent);

            adder = SAMPLE_EVENT_WRITES.putIfAbsent(eventToString(sampleEvent), new LongAdder());
            if (adder != null) {
                adder.increment();
                if (adder.intValue() != 1) {
                    log.error("Offset error at Sample Write {}", offset.value(getKey()));
                }
            }
        }

        @ReactsOn
        @SuppressWarnings("unused")
        void handle(@Nonnull Events.SampleEvent event) {
            final LongAdder adder = SAMPLE_EVENT_READS.putIfAbsent(eventToString(event), new LongAdder());
            if (adder != null) {
                adder.increment();
                if (adder.intValue() != 1) {
                    log.error("Offset error at Sample Read {}", offset.value(getKey()));
                }
            }
            expectedSampleEventReads.countDown();
        }

        @Nonnull
        Collection<String> listDuplicatedEvents() {
            final ArrayList<String> duplicates = new ArrayList<>();
            duplicates.addAll(listDuplicatedEvents(FANCY_EVENT_READS));
            duplicates.addAll(listDuplicatedEvents(SAMPLE_EVENT_READS));
            duplicates.addAll(listDuplicatedEvents(SAMPLE_EVENT_WRITES));
            return duplicates;
        }

        @Nonnull
        private Collection<String> listDuplicatedEvents(@Nonnull Map<String, LongAdder> source) {
            return source.entrySet().stream()
                    .filter(entry -> entry.getValue().intValue() > 1)
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
        }
    }

    private static class TestLatchSaga extends Saga {

        private final CountDownLatch counter;
        private final CountDownLatch isFailed;

        TestLatchSaga(@Nonnull JEventStore store, @Nonnull Offset offset, @Nonnull Lock lock, CountDownLatch counter,
                      CountDownLatch isFailed) {
            super(store, offset, lock, new PollingTrigger());
            this.counter = Objects.requireNonNull(counter);
            this.isFailed = isFailed;
        }

        @ReactsOn
        @SuppressWarnings("unused")
        void handle(@Nonnull Events.SampleEvent event) {
            if (counter.getCount() == 0) {
                isFailed.countDown();
                fail("Handled events count reach 0. New event received: " + event);
            }
            counter.countDown();
        }
    }


}