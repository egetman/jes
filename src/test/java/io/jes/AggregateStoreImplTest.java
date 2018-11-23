package io.jes;

import java.util.UUID;

import javax.persistence.EntityManager;

import org.junit.jupiter.api.Test;

import io.jes.internal.FancyAggregate;
import io.jes.provider.JpaStoreProvider;
import io.jes.snapshot.InMemorySnapshotProvider;

import static io.jes.internal.Events.FancyEvent;
import static io.jes.internal.Events.ProcessingStarted;
import static io.jes.internal.Events.ProcessingTerminated;
import static io.jes.internal.Events.SampleEvent;
import static io.jes.internal.FancyStuff.newEntityManager;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregateStoreImplTest {

    @Test
    void aggregateStateShouldReactToEventStreamChange() {
        final EntityManager entityManager = newEntityManager(String.class);
        final JpaStoreProvider<String> storeProvider = new JpaStoreProvider<>(entityManager, String.class);

        final JEventStore eventStore = new JEventStoreImpl(storeProvider);
        final AggregateStore aggregateStore = new AggregateStoreImpl(eventStore, new InMemorySnapshotProvider());

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
    }

}