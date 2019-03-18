package io.jes.reactors;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.jes.Event;
import io.jes.JEventStore;
import io.jes.lock.InMemoryReentrantLockManager;
import io.jes.offset.InMemoryOffset;
import io.jes.offset.Offset;
import io.jes.provider.JdbcStoreProvider;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static io.jes.internal.Events.FancyEvent;
import static io.jes.internal.Events.ProcessingStarted;
import static io.jes.internal.Events.ProcessingTerminated;
import static io.jes.internal.Events.SampleEvent;
import static io.jes.internal.FancyStuff.newPostgresDataSource;
import static io.jes.reactors.ProjectorTest.SampleProjector.Projection;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProjectorTest {

    @Test
    @SuppressWarnings({"unused", "ConstantConditions"})
    void shouldHandleInvariants() {
        final JEventStore store = Mockito.mock(JEventStore.class);

        assertThrows(NullPointerException.class, () -> new Projector(store, new InMemoryOffset(), null) {
            @Handler
            private void foo(Event event) {}
        });
    }

    @Test
    @SneakyThrows
    void projectorShouldCorrectlyRebuildProjection() {
        final InMemoryOffset offset = new InMemoryOffset();
        final JdbcStoreProvider<String> provider = new JdbcStoreProvider<>(newPostgresDataSource(), String.class);
        final JEventStore store = new JEventStore(provider);

        try (final SampleProjector projector = new SampleProjector(store, offset)) {
            // verify projector start listen event store and projection is not created yet
            assertTrue(projector.isStarted());
            assertNull(projector.getProjection());

            final SampleEvent sampleEvent = new SampleEvent("FOO");
            final FancyEvent fancyEvent = new FancyEvent("BAR", UUID.randomUUID());

            store.write(new ProcessingStarted());
            store.write(sampleEvent);
            store.write(fancyEvent);
            store.write(new ProcessingTerminated());

            final Projection projection = projector.getProjection();

            assertNotNull(projection);
            assertEquals(4, projection.getTotalProcessed());
            assertEquals(sampleEvent.getName(), projection.getName());
            assertIterableEquals(
                    new HashSet<>(asList(sampleEvent.uuid(), fancyEvent.uuid())),
                    projection.getUniqueEventStreams()
            );

            projector.recreate();

            final Projection newProjection = projector.getProjection();

            assertEquals(projection, newProjection);
            // verify that it's another, 'recreated' projection, not the same obj as earlier
            assertNotSame(projection, newProjection);
        }

        try {
            provider.close();
        } catch (Exception ignored) {}
    }

    @Slf4j
    @SuppressWarnings("unused")
    static class SampleProjector extends Projector {

        private Projection projection;
        private CountDownLatch endStreamLatch = new CountDownLatch(1);
        private final CountDownLatch started = new CountDownLatch(1);

        SampleProjector(@Nonnull JEventStore store, @Nonnull Offset offset) {
            super(store, offset, new InMemoryReentrantLockManager());
        }

        @Handler
        private void handle(@Nonnull ProcessingStarted event) {
            projection = new Projection();
            projection.totalProcessed++;
        }

        @Handler
        private void handle(@Nonnull SampleEvent event) {
            projection.name = event.getName();
            projection.totalProcessed++;
            projection.uniqueEventStreams.add(event.uuid());
        }

        @Handler
        private void handle(@Nonnull FancyEvent event) {
            projection.totalProcessed++;
            projection.uniqueEventStreams.add(event.uuid());
        }

        @Handler
        private void handle(@Nonnull ProcessingTerminated event) {
            projection.totalProcessed++;
            endStreamLatch.countDown();
        }

        @SneakyThrows
        Projection getProjection() {
            endStreamLatch.await(1, TimeUnit.SECONDS);
            return projection;
        }

        @Override
        void tailStore() {
            super.tailStore();
            started.countDown();
        }

        @SneakyThrows
        boolean isStarted() {
            return started.await(1, TimeUnit.SECONDS);
        }

        @Override
        public void recreate() {
            super.recreate();
            endStreamLatch = new CountDownLatch(1);
        }

        // this is just in-memory projection. But it can be any kind of projection like separate sql-db, or elastic etc.
        @Data
        static class Projection {

            private String name;
            private long totalProcessed;
            private final Set<UUID> uniqueEventStreams = new HashSet<>();

        }
    }
}