package io.jes.reactors;

import java.util.HashSet;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.jes.Event;
import io.jes.JEventStore;
import io.jes.JEventStoreImpl;
import io.jes.common.FancyEvent;
import io.jes.common.ProcessingStarted;
import io.jes.common.ProcessingTerminated;
import io.jes.common.SampleEvent;
import io.jes.offset.InMemoryOffset;
import io.jes.provider.JdbcStoreProvider;
import lombok.SneakyThrows;

import static io.jes.common.FancyStuff.newDataSource;
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
        final JEventStoreImpl store = new JEventStoreImpl(new JdbcStoreProvider<>(newDataSource(), String.class));
        try (final SampleProjector projector = new SampleProjector(store, offset)) {
            assertTrue(projector.isStarted());
            assertNull(projector.getProjection());

            final SampleEvent sampleEvent = new SampleEvent("FOO");
            final FancyEvent fancyEvent = new FancyEvent("BAR", UUID.randomUUID());

            store.write(new ProcessingStarted());
            store.write(sampleEvent);
            store.write(fancyEvent);
            store.write(new ProcessingTerminated());

            final SampleProjector.Projection projection = projector.getProjection();
            assertNotNull(projection);
            assertEquals(4, projection.getTotalProcessed());
            assertEquals(sampleEvent.getName(), projection.getName());
            assertIterableEquals(
                    new HashSet<>(asList(sampleEvent.uuid(), fancyEvent.uuid())),
                    projection.getUniqueEventStreams()
            );

            projector.recreate();

            final SampleProjector.Projection newProjection = projector.getProjection();
            assertNotNull(newProjection);
            assertEquals(4, newProjection.getTotalProcessed());
            assertEquals(sampleEvent.getName(), newProjection.getName());
            assertIterableEquals(
                    new HashSet<>(asList(sampleEvent.uuid(), fancyEvent.uuid())),
                    newProjection.getUniqueEventStreams()
            );

            // verify that it's another, 'recreated' projection, not the same obj as earlier
            assertNotSame(projection, newProjection);
        }
    }
}