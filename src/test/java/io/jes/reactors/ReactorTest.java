package io.jes.reactors;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.jes.JEventStore;
import io.jes.ex.BrokenReactorException;
import io.jes.internal.Events;
import io.jes.offset.InMemoryOffset;
import io.jes.offset.Offset;
import io.jes.provider.JdbcStoreProvider;
import lombok.SneakyThrows;

import static io.jes.internal.FancyStuff.newPostgresDataSource;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReactorTest {

    @Test
    @SuppressWarnings({"ConstantConditions", "unused"})
    void shouldHandleItsInvariants() {
        final JEventStore store = Mockito.mock(JEventStore.class);

        assertThrows(NullPointerException.class, () -> new Reactor(store, null) {});
        assertThrows(NullPointerException.class, () -> new Reactor(null, new InMemoryOffset()) {});

        BrokenReactorException exception = assertThrows(BrokenReactorException.class,
                () -> new Reactor(store, new InMemoryOffset()) {});

        assertEquals("Methods with @Handle annotation not found", exception.getMessage());

        exception = assertThrows(BrokenReactorException.class, () -> new Reactor(store, new InMemoryOffset()) {
            @Handler
            private int handle(Events.SampleEvent event) {
                return -1;
            }
        });

        assertEquals("Handler method should not have any return value", exception.getMessage());

        exception = assertThrows(BrokenReactorException.class, () -> new Reactor(store, new InMemoryOffset()) {
            @Handler
            private void handle(Events.SampleEvent sampleEvent, Events.FancyEvent fancyEvent) {}
        });

        assertEquals("Handler method should have only 1 parameter", exception.getMessage());

        exception = assertThrows(BrokenReactorException.class, () -> new Reactor(store, new InMemoryOffset()) {
            @Handler
            private void handle(Object object) {}
        });

        assertEquals("Handler method parameter must be an instance of the Event class. "
                + "Found type: " + Object.class, exception.getMessage());

        assertDoesNotThrow(() -> new Reactor(store, new InMemoryOffset()) {
            @Handler
            private void handle(Events.SampleEvent event) {}
        });
    }

    @Test
    @SneakyThrows
    void reactorShouldReactOnEventStoreChanges() {
        final CountDownLatch endLatch = new CountDownLatch(1);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch fancyLatch = new CountDownLatch(1);
        final CountDownLatch sampleLatch = new CountDownLatch(2);

        final String key = SampleReactor.class.getName();

        final Offset offset = new InMemoryOffset();
        final JdbcStoreProvider<byte[]> provider = new JdbcStoreProvider<>(newPostgresDataSource(), byte[].class);
        final JEventStore store = new JEventStore(provider);

        // after creation reactor start listening event store and handle written events
        // noinspection unused
        try (final Reactor reactor = new SampleReactor(store, offset, endLatch, startLatch, fancyLatch, sampleLatch)) {
            // verify reactor launch listening
            assertTrue(startLatch.await(1, TimeUnit.SECONDS));
            // verify no events written now
            assertEquals(1, fancyLatch.getCount());
            assertEquals(2, sampleLatch.getCount());
            assertEquals(0, offset.value(key));

            store.write(new Events.FancyEvent("FOO", UUID.randomUUID()));

            // verify reactor handle first store change
            assertTrue(fancyLatch.await(1, TimeUnit.SECONDS));
            assertEquals(0, fancyLatch.getCount());
            assertEquals(2, sampleLatch.getCount());

            store.write(new Events.SampleEvent("BAR", UUID.randomUUID()));
            store.write(new Events.SampleEvent("BAZ", UUID.randomUUID()));
            store.write(new Events.ProcessingTerminated());

            // verify reactor handle all other store changes
            assertTrue(sampleLatch.await(1, TimeUnit.SECONDS));
            assertEquals(0, fancyLatch.getCount());
            assertEquals(0, sampleLatch.getCount());

            // verify last event was handled and offset value reflects the total amount of processed events
            assertTrue(endLatch.await(1, TimeUnit.SECONDS));
            assertEquals(4, offset.value(key));
        }
        try {
            provider.close();
        } catch (Exception ignored) {}
    }

    @SuppressWarnings("unused")
    static class SampleReactor extends Reactor {

        @Nonnull
        private final CountDownLatch endLatch;
        @Nonnull
        private final CountDownLatch startLatch;
        @Nonnull
        private final CountDownLatch fancyLatch;
        @Nonnull
        private final CountDownLatch sampleLatch;

        private volatile boolean terminated;

        SampleReactor(@Nonnull JEventStore store, @Nonnull Offset offset,
                      @Nonnull CountDownLatch endLatch,
                      @Nonnull CountDownLatch startLatch,
                      @Nonnull CountDownLatch fancyLatch,
                      @Nonnull CountDownLatch sampleLatch) {

            super(store, offset);
            this.startLatch = startLatch;
            this.endLatch = endLatch;
            this.fancyLatch = fancyLatch;
            this.sampleLatch = sampleLatch;
        }


        @Handler
        private void handle(Events.ProcessingTerminated event) {
            this.terminated = true;
        }

        @Handler
        private void handle(Events.SampleEvent event) {
            sampleLatch.countDown();
        }

        @Handler
        private void handle(Events.FancyEvent event) {
            fancyLatch.countDown();
        }

        @Override
        void tailStore() {
            super.tailStore();
            startLatch.countDown();
            if (terminated) {
                endLatch.countDown();
            }
        }
    }
}