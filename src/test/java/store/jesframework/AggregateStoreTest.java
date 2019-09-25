package store.jesframework;

import java.util.UUID;
import javax.persistence.EntityManagerFactory;

import org.junit.jupiter.api.Test;

import store.jesframework.internal.FancyAggregate;
import store.jesframework.provider.JpaStoreProvider;
import store.jesframework.snapshot.InMemorySnapshotProvider;
import store.jesframework.snapshot.SnapshotProvider;
import store.jesframework.snapshot.SnapshotStrategy;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static store.jesframework.internal.Events.FancyEvent;
import static store.jesframework.internal.Events.ProcessingStarted;
import static store.jesframework.internal.Events.ProcessingTerminated;
import static store.jesframework.internal.Events.SampleEvent;
import static store.jesframework.internal.FancyStuff.newEntityManagerFactory;

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

    @Test
    void snapshotMustBeDoneIfConditionsAreMet() {
        final JEventStore eventStore = mock(JEventStore.class);
        final SnapshotProvider snapshotProvider = mock(SnapshotProvider.class);
        final SnapshotStrategy snapshotStrategy = mock(SnapshotStrategy.class);

        final AggregateStore aggregateStore = new AggregateStore(eventStore, snapshotProvider, snapshotStrategy);

        final UUID uuid = UUID.randomUUID();
        final FancyAggregate expected = new FancyAggregate(uuid);
        when(snapshotProvider.snapshot(any())).thenReturn(expected);
        when(snapshotProvider.initialStateOf(any(), any())).thenReturn(expected);
        when(eventStore.readBy(uuid, 0)).thenReturn(singletonList(new SampleEvent("Sample", uuid)));
        when(snapshotStrategy.isSnapshotNecessary(any(), any())).thenReturn(true);

        final FancyAggregate read = aggregateStore.readBy(uuid, FancyAggregate.class);

        assertEquals(uuid, read.uuid());
        verify(snapshotProvider, times(1)).snapshot(read);
    }

    @Test
    void snapshotMustNotBeDoneIfConditionsAreNotMet() {
        final JEventStore eventStore = mock(JEventStore.class);
        final SnapshotProvider snapshotProvider = mock(SnapshotProvider.class);
        final SnapshotStrategy snapshotStrategy = mock(SnapshotStrategy.class);

        final AggregateStore store = new AggregateStore(eventStore, snapshotProvider, snapshotStrategy);

        final UUID uuid = UUID.randomUUID();
        final FancyAggregate expected = new FancyAggregate(uuid);
        when(snapshotProvider.snapshot(any())).thenReturn(expected);
        when(snapshotProvider.initialStateOf(any(), any())).thenReturn(expected);
        when(eventStore.readBy(uuid, 0)).thenReturn(singletonList(new FancyEvent("Sample", uuid)));
        when(snapshotStrategy.isSnapshotNecessary(any(), any())).thenReturn(false);

        final FancyAggregate read = store.readBy(uuid, FancyAggregate.class);

        assertEquals(uuid, read.uuid());
        verify(snapshotProvider, never()).snapshot(any());
    }

}