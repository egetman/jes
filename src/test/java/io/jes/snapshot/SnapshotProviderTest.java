package io.jes.snapshot;

import java.util.Collection;
import java.util.UUID;
import javax.annotation.Nonnull;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.jes.Aggregate;
import io.jes.AggregateStore;
import io.jes.JEventStore;
import io.jes.ex.AggregateCreationException;
import io.jes.ex.EmptyEventStreamException;
import io.jes.internal.Events;
import io.jes.internal.FancyAggregate;
import io.jes.provider.JdbcStoreProvider;

import static io.jes.internal.FancyStuff.newDataSource;
import static io.jes.internal.FancyStuff.newRedissonClient;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SnapshotProviderTest {

    private static Collection<SnapshotProvider> createSnapshotProviders() {
        return asList(
                new InMemorySnapshotProvider(),
                new JdbcSnapshotProvider<>(newDataSource(), String.class),
                new RedissonSnapshotProvider(newRedissonClient())
        );
    }

    @ParameterizedTest
    @MethodSource("createSnapshotProviders")
    void shouldThrowAggregateCreationExceptionIfFailedToInstantiateAggregate(@Nonnull SnapshotProvider provider) {
        class FooAggregate extends Aggregate {
            @SuppressWarnings("unused")
            public FooAggregate(String string, int integer) {
                // do nothing, just to disable no-arg constructor
            }
        }
        assertThrows(AggregateCreationException.class, () -> provider.initialStateOf(randomUUID(), FooAggregate.class));
    }

    @ParameterizedTest
    @MethodSource("createSnapshotProviders")
    void shouldReportNullSnapshotValues(@Nonnull SnapshotProvider provider) {
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> provider.snapshot(null));
    }

    @ParameterizedTest
    @MethodSource("createSnapshotProviders")
    void shouldReturnTheSameAggregateAsSnapshotByDefault(@Nonnull SnapshotProvider provider) {
        final Aggregate aggregate = new FancyAggregate() {{
                this.uuid = UUID.randomUUID();
        }};
        assertSame(aggregate, provider.snapshot(aggregate));
    }

    @ParameterizedTest
    @MethodSource("createSnapshotProviders")
    void sholdCreateSnapshotAndUpdateItReadThroughAggregateStore(@Nonnull SnapshotProvider provider) {
        final JEventStore eventStore = new JEventStore(new JdbcStoreProvider<>(newDataSource(), String.class));
        final AggregateStore aggregateStore = new AggregateStore(eventStore, provider);

        final UUID uuid = randomUUID();
        final Events.SampleEvent foo = new Events.SampleEvent("FOO", uuid);
        final Events.FancyEvent fancy = new Events.FancyEvent("Fancy", uuid);
        assertThrows(EmptyEventStreamException.class, () -> aggregateStore.readBy(uuid, FancyAggregate.class));

        eventStore.write(foo);
        eventStore.write(fancy);

        // should read populated aggregate
        final FancyAggregate fromStore = aggregateStore.readBy(uuid, FancyAggregate.class);
        assertEquals(foo.uuid(), fromStore.uuid());
        assertEquals(fancy.getName(), fromStore.getFancyName());

        // check state in cache
        final FancyAggregate fromSnapshot = provider.initialStateOf(uuid, FancyAggregate.class);
        assertEquals(fromStore, fromSnapshot);
    }
}