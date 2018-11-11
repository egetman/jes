package io.jes.aggregate;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.jes.JEventStore;
import io.jes.JEventStoreImpl;
import io.jes.ex.AggregateCreationException;
import io.jes.internal.Events;
import io.jes.internal.FancyAggregate;
import io.jes.provider.JdbcStoreProvider;

import static io.jes.internal.FancyStuff.newDataSource;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class AggregateStoreImplTest {

    @Test
    void shouldThrowAggregateCreationExceptionIfFailedToInstantiateAggregate() {
        final JEventStore store = mock(JEventStore.class);
        final AggregateStoreImpl aggregateStore = new AggregateStoreImpl(store);

        class FooAggregate extends SimpleAggregate {
            @SuppressWarnings("unused")
            public FooAggregate(String string, int integer) {
                // do nothing, just to disable no-arg constructor
            }
        }
        assertThrows(AggregateCreationException.class, () -> aggregateStore.initialStateOf(FooAggregate.class));
    }

    @Test
    void aggregateStateShouldReactToEventStreamChange() {
        final JEventStoreImpl eventStore = new JEventStoreImpl(new JdbcStoreProvider<>(newDataSource(), byte[].class));
        final AggregateStore aggregateStore = new AggregateStoreImpl(eventStore);

        final UUID uuid = UUID.randomUUID();
        aggregateStore.write(new Events.SampleEvent("FOO", uuid));

        FancyAggregate aggregate = aggregateStore.readBy(uuid, FancyAggregate.class);
        assertNotNull(aggregate);
        assertEquals(uuid, aggregate.uuid());
        assertEquals(1, aggregate.getVersion());
        assertNull(aggregate.getFancyName());

        eventStore.write(new Events.FancyEvent("BAR", uuid));
        aggregate = aggregateStore.readBy(uuid, FancyAggregate.class);
        assertNotNull(aggregate);
        assertEquals(uuid, aggregate.uuid());
        assertEquals(2, aggregate.getVersion());
        assertEquals("BAR", aggregate.getFancyName());

        aggregateStore.write(new Events.ProcessingTerminated(uuid));
        aggregate = aggregateStore.readBy(uuid, FancyAggregate.class);
        assertTrue(aggregate.isCancelled());

        // not handled event type should not broke anithing
        aggregateStore.write(new Events.ProcessingStarted(uuid));
        assertDoesNotThrow(() -> aggregateStore.readBy(uuid, FancyAggregate.class));
    }

}