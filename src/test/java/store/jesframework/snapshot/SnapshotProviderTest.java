package store.jesframework.snapshot;

import java.util.Collection;
import java.util.UUID;
import javax.annotation.Nonnull;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import store.jesframework.Aggregate;
import store.jesframework.AggregateStore;
import store.jesframework.JEventStore;
import store.jesframework.ex.AggregateCreationException;
import store.jesframework.internal.Events;
import store.jesframework.internal.FancyAggregate;
import store.jesframework.provider.JdbcStoreProvider;

import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static store.jesframework.internal.FancyStuff.newMySqlDataSource;
import static store.jesframework.internal.FancyStuff.newPostgresDataSource;
import static store.jesframework.internal.FancyStuff.newRedissonClient;
import static store.jesframework.serializer.api.Format.XML_XSTREAM;

class SnapshotProviderTest {

    private static final Collection<SnapshotProvider> SNAPSHOT_PROVIDERS = asList(
            new InMemorySnapshotProvider(),
            new JdbcSnapshotProvider<>(newPostgresDataSource()),
            new JdbcSnapshotProvider<>(newMySqlDataSource("es")),
            new RedisSnapshotProvider(newRedissonClient())
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

    @Test
    void resetFallbackShouldBeNoop() {
        //noinspection ConstantConditions
        assertThrows(NullPointerException.class, () -> new SnapshotProvider() {}.reset(null));
        assertDoesNotThrow(() -> new SnapshotProvider() {}.reset(randomUUID()));
    }

    @ParameterizedTest
    @MethodSource("createSnapshotProviders")
    void initialStateShouldReturnNewInstanceIfSnapshotWasReset(@Nonnull SnapshotProvider provider) {
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
    void shouldCreateSnapshotAndUpdateItReadThroughAggregateStore(@Nonnull SnapshotProvider provider) {
        final JEventStore eventStore = new JEventStore(new JdbcStoreProvider<>(newPostgresDataSource(), XML_XSTREAM));
        final AggregateStore aggregateStore = new AggregateStore(eventStore, provider);

        final UUID uuid = randomUUID();
        final Events.SampleEvent foo = new Events.SampleEvent("FOO", uuid);
        final Events.FancyEvent fancy = new Events.FancyEvent("Fancy", uuid);

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

    @ParameterizedTest
    @MethodSource("createSnapshotProviders")
    void initialStateShouldReturnNewInstanceFromAggregateCreatorIfRegistered(@Nonnull SnapshotProvider provider) {
        @RequiredArgsConstructor
        @SuppressWarnings("unused")
        class AggregateWithoutDefaultConstructor extends Aggregate {
            private final UUID uuid;
        }

        assertThrows(AggregateCreationException.class,
                () -> provider.initialStateOf(randomUUID(), AggregateWithoutDefaultConstructor.class));

        final DefaultSnapshotProvider defaultProvider = (DefaultSnapshotProvider) provider;
        defaultProvider.addAggregateCreator(
                AggregateWithoutDefaultConstructor.class,
                (uuid, aClass) -> new AggregateWithoutDefaultConstructor(uuid)
        );

        assertDoesNotThrow(() -> provider.initialStateOf(randomUUID(), AggregateWithoutDefaultConstructor.class));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    void defaultSnapshotProviderShouldNotPermitNullValues() {
        final DefaultSnapshotProvider provider = new DefaultSnapshotProvider();
        assertThrows(NullPointerException.class, () -> provider.addAggregateCreator(null, ((uuid, aClass) -> null)));
        assertThrows(NullPointerException.class, () -> provider.addAggregateCreator(Aggregate.class, null));
    }

    @Test
    void defaultSnapshotProviderShouldNotReturnNull() {
        final DefaultSnapshotProvider provider = new DefaultSnapshotProvider();
        provider.addAggregateCreator(Aggregate.class, (uuid, aClass) -> null);
        assertThrows(AggregateCreationException.class, () -> provider.initialStateOf(randomUUID(), Aggregate.class));
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