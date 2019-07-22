package io.jes.snapshot;

import java.util.Collection;
import java.util.UUID;
import javax.annotation.Nonnull;

import org.junit.jupiter.api.AfterAll;
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
import lombok.SneakyThrows;

import static io.jes.internal.FancyStuff.newPostgresDataSource;
import static io.jes.internal.FancyStuff.newRedissonClient;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SnapshotProviderTest {

    private static final Collection<SnapshotProvider> SNAPSHOT_PROVIDERS = asList(
            new InMemorySnapshotProvider(),
            new JdbcSnapshotProvider<>(newPostgresDataSource(), String.class),
            new RedissonSnapshotProvider(newRedissonClient())
    );

    private static Collection<SnapshotProvider> createSnapshotProviders() {
        return SNAPSHOT_PROVIDERS;
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
    void shouldSuccessfullyUpdateSnapshot(@Nonnull SnapshotProvider provider) {
        final FancyAggregate sample = new FancyAggregate(randomUUID());
        final FancyAggregate initialStateOf = provider.initialStateOf(sample.uuid(), FancyAggregate.class);
        assertNotNull(initialStateOf);
        provider.snapshot(sample);

        sample.setFancyName("Fancy");
        provider.snapshot(sample);

        sample.setCancelled(true);
        final FancyAggregate target = provider.snapshot(sample);

        assertEquals("Fancy", target.getFancyName());
        assertTrue(target.isCancelled());
        assertEquals(sample.uuid(), target.uuid());
    }

    @ParameterizedTest
    @MethodSource("createSnapshotProviders")
    void resetShouldNotThrowAnyExceptionsIfUuidProvided(@Nonnull SnapshotProvider provider) {
        final FancyAggregate sample = new FancyAggregate(randomUUID());
        provider.snapshot(sample);
        assertDoesNotThrow(() -> provider.reset(sample.uuid()));
    }

    @ParameterizedTest
    @MethodSource("createSnapshotProviders")
    void resetShouldThrowNullPointerExceptionIfUuidMissing(@Nonnull SnapshotProvider provider) {
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> provider.reset(null));
    }

    @ParameterizedTest
    @MethodSource("createSnapshotProviders")
    void initialStateShouldReturnNewInstanceIfSnapshotWasReseted(@Nonnull SnapshotProvider provider) {
        final FancyAggregate sample = new FancyAggregate(randomUUID());
        sample.setFancyName("Name");
        provider.snapshot(sample);
        final FancyAggregate restored = provider.initialStateOf(sample.uuid(), FancyAggregate.class);

        assertEquals(sample.getFancyName(), restored.getFancyName());
        assertDoesNotThrow(() -> provider.reset(sample.uuid()));
        final FancyAggregate target = provider.initialStateOf(sample.uuid(), FancyAggregate.class);
        assertNull(target.getFancyName());
    }

    @ParameterizedTest
    @MethodSource("createSnapshotProviders")
    void sholdCreateSnapshotAndUpdateItReadThroughAggregateStore(@Nonnull SnapshotProvider provider) {
        final JEventStore eventStore = new JEventStore(new JdbcStoreProvider<>(newPostgresDataSource(), String.class));
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

    @AfterAll
    @SneakyThrows
    static void closeResources() {
        for (SnapshotProvider snapshotProvider : SNAPSHOT_PROVIDERS) {
            if (snapshotProvider instanceof AutoCloseable) {
                ((AutoCloseable) snapshotProvider).close();
            }
        }
    }

}