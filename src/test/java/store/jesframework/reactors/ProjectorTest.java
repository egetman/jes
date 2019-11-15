package store.jesframework.reactors;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import store.jesframework.Event;
import store.jesframework.JEventStore;
import store.jesframework.common.ProjectionFailure;
import store.jesframework.lock.InMemoryReentrantLock;
import store.jesframework.offset.InMemoryOffset;
import store.jesframework.offset.Offset;
import store.jesframework.provider.InMemoryStoreProvider;
import store.jesframework.provider.JdbcStoreProvider;

import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static store.jesframework.internal.Events.FancyEvent;
import static store.jesframework.internal.Events.ProcessingStarted;
import static store.jesframework.internal.Events.ProcessingTerminated;
import static store.jesframework.internal.Events.SampleEvent;
import static store.jesframework.internal.FancyStuff.newPostgresDataSource;
import static store.jesframework.reactors.ProjectorTest.SampleProjector.Projection;

@Slf4j
class ProjectorTest {

    @Test
    @SuppressWarnings({"unused", "ConstantConditions"})
    void shouldHandleInvariants() {
        final JEventStore store = Mockito.mock(JEventStore.class);

        assertThrows(NullPointerException.class, () -> new Projector(store, new InMemoryOffset(), null) {
            @ReactsOn
            private void foo(Event event) {}

            @Override
            protected void cleanUp() {}
        });
    }

    @Test
    @SneakyThrows
    void projectorShouldCorrectlyRebuildProjection() {
        final InMemoryOffset offset = new InMemoryOffset();
        final JdbcStoreProvider<String> provider = new JdbcStoreProvider<>(newPostgresDataSource());
        final JEventStore store = new JEventStore(provider);

        try (final SampleProjector projector = new SampleProjector(store, offset)) {
            // verify projector start listen event store, and the projection was not created yet
            assertNull(projector.getProjection());

            final SampleEvent sampleEvent = new SampleEvent("FOO");
            final FancyEvent fancyEvent = new FancyEvent("BAR", UUID.randomUUID());

            store.write(new ProcessingStarted());
            store.write(sampleEvent);
            store.write(fancyEvent);
            store.write(new ProcessingTerminated());

            final Projection projection = projector.getProjection();

            assertNotNull(projection);
            Assertions.assertEquals(4, projection.getTotalProcessed());
            Assertions.assertEquals(sampleEvent.getName(), projection.getName());
            assertIterableEquals(
                    new HashSet<>(asList(sampleEvent.uuid(), fancyEvent.uuid())),
                    projection.getUniqueEventStreams()
            );

            projector.recreate();

            final Projection newProjection = projector.getProjection();

            Assertions.assertEquals(projection, newProjection);
            // verify that it's another, 'recreated' projection, not the same obj as earlier
            assertNotSame(projection, newProjection);
        }

        try {
            provider.close();
        } catch (Exception ignored) {}
    }

    @Test
    @SneakyThrows
    void projectorShouldWriteProjectionFailureEventOnFailedEventHandling() {
        final JEventStore store = new JEventStore(new InMemoryStoreProvider());
        final InMemoryOffset offset = new InMemoryOffset();
        final InMemoryReentrantLock lock = new InMemoryReentrantLock();

        final CountDownLatch latch = new CountDownLatch(1);
        //noinspection unused
        final Projector projector = new Projector(store, offset, lock) {

            @ReactsOn
            void handle(FancyEvent  event) {
                throw new IllegalStateException("Boom Bam");
            }
            @ReactsOn
            void handle(ProjectionFailure event) {
                latch.countDown();
                log.debug("Received {}", event);
            }

            // do nothing
            @Override
            protected void cleanUp() {}
        };

        final UUID uuid = randomUUID();
        store.write(new FancyEvent("Fancy", uuid));

        assertTrue(latch.await(2, SECONDS));

        final Collection<Event> actual = store.readBy(uuid);
        assertEquals(2, actual.size());

        final Iterator<Event> iterator = actual.iterator();
        assertEquals(FancyEvent.class, iterator.next().getClass());
        assertEquals(ProjectionFailure.class, iterator.next().getClass());
    }

    @SuppressWarnings("unused")
    static class SampleProjector extends Projector {

        private Projection projection;
        private CountDownLatch endStreamLatch = new CountDownLatch(1);

        SampleProjector(@Nonnull JEventStore store, @Nonnull Offset offset) {
            super(store, offset, new InMemoryReentrantLock());
        }

        @ReactsOn
        private void handle(@Nonnull ProcessingStarted event) {
            projection = new Projection();
            projection.totalProcessed++;
        }

        @ReactsOn
        private void handle(@Nonnull SampleEvent event) {
            projection.name = event.getName();
            projection.totalProcessed++;
            projection.uniqueEventStreams.add(event.uuid());
        }

        @ReactsOn
        private void handle(@Nonnull FancyEvent event) {
            projection.totalProcessed++;
            projection.uniqueEventStreams.add(event.uuid());
        }

        @ReactsOn
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
        protected void cleanUp() {
            projection = null;
            endStreamLatch = new CountDownLatch(1);
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