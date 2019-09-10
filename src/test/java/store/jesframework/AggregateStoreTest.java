package store.jesframework;

import java.util.UUID;
import javax.persistence.EntityManagerFactory;

import org.junit.jupiter.api.Test;

import store.jesframework.internal.FancyAggregate;
import store.jesframework.provider.JpaStoreProvider;
import store.jesframework.snapshot.InMemorySnapshotProvider;
import store.jesframework.snapshot.NoopSnapshotProvider;

import static store.jesframework.internal.Events.FancyEvent;
import static store.jesframework.internal.Events.ProcessingStarted;
import static store.jesframework.internal.Events.ProcessingTerminated;
import static store.jesframework.internal.Events.SampleEvent;
import static store.jesframework.internal.FancyStuff.newEntityManagerFactory;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class AggregateStoreTest {

    @Test
    void aggregateStateShouldReactToEventStreamChange() {
        final EntityManagerFactory entityManager = newEntityManagerFactory(String.class);
        final JpaStoreProvider<String> storeProvider = new JpaStoreProvider<>(entityManager, String.class);

        final JEventStore eventStore = new JEventStore(storeProvider);
        final AggregateStore aggregateStore = new AggregateStore(eventStore, new InMemorySnapshotProvider());

        final UUID uuid = UUID.randomUUID();
        eventStore.write(new SampleEvent("FOO", uuid));

        FancyAggregate aggregate = aggregateStore.readBy(uuid, FancyAggregate.class);
        assertNotNull(aggregate);
        assertEquals(uuid, aggregate.uuid());
        assertEquals(1, aggregate.streamVersion());
        assertNull(aggregate.getFancyName());

        eventStore.write(new FancyEvent("BAR", uuid));
        aggregate = aggregateStore.readBy(uuid, FancyAggregate.class);
        assertNotNull(aggregate);
        assertEquals(uuid, aggregate.uuid());
        assertEquals(2, aggregate.streamVersion());
        assertEquals("BAR", aggregate.getFancyName());

        eventStore.write(new ProcessingTerminated(uuid));
        aggregate = aggregateStore.readBy(uuid, FancyAggregate.class);
        assertTrue(aggregate.isCancelled());

        // not handled event type should not broke anithing
        eventStore.write(new ProcessingStarted(uuid));
        assertDoesNotThrow(() -> aggregateStore.readBy(uuid, FancyAggregate.class));

        try {
            entityManager.close();
        } catch (Exception ignored) {}
    }

    @Test
    void ifSnapshotProviderNotSpecifiedDefaultImplementationUsed() {
        final JEventStore eventStore = mock(JEventStore.class);
        final AggregateStore aggregateStore = new AggregateStore(eventStore);

        assertEquals(NoopSnapshotProvider.class, aggregateStore.snapshotter.getClass());
    }

    @Test
    void shouldDelegateWritingEventsToUnderlyingStore() {
        final JEventStore eventStore = mock(JEventStore.class);
        final AggregateStore aggregateStore = new AggregateStore(eventStore);

        final SampleEvent sampleEvent = new SampleEvent("FOO", UUID.randomUUID());
        aggregateStore.write(sampleEvent);
        verify(eventStore, times(1)).write(sampleEvent);

        final Event[] array = {new SampleEvent("BAR", UUID.randomUUID()), new FancyEvent("Fancy", UUID.randomUUID())};
        aggregateStore.write(array);
        verify(eventStore, times(1)).write(array);
    }

}