package io.jes.provider;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.jes.Event;
import io.jes.ex.VersionMismatchException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static io.jes.internal.Events.FancyEvent;
import static io.jes.internal.Events.SampleEvent;
import static io.jes.internal.FancyStuff.newEntityManagerFactory;
import static io.jes.internal.FancyStuff.newH2DataSource;
import static io.jes.internal.FancyStuff.newPostgresDataSource;
import static java.lang.Runtime.getRuntime;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
class StoreProviderTest {

    private static final Collection<StoreProvider> PROVIDERS = asList(
            new InMemoryStoreProvider(),
            new JdbcStoreProvider<>(newH2DataSource(), String.class),
            new JdbcStoreProvider<>(newH2DataSource(), byte[].class),
            new JdbcStoreProvider<>(newPostgresDataSource(), byte[].class),
            new JdbcStoreProvider<>(newPostgresDataSource(), String.class),
            new JpaStoreProvider<>(newEntityManagerFactory(byte[].class), byte[].class),
            new JpaStoreProvider<>(newEntityManagerFactory(String.class), String.class)
    );

    private static Collection<StoreProvider> createProviders() {
        return PROVIDERS;
    }

    @ParameterizedTest
    @MethodSource("createProviders")
    void shouldReadOwnWrites(@Nonnull StoreProvider provider) {

        final List<Event> expected = asList(new SampleEvent("FOO"), new SampleEvent("BAR"), new SampleEvent("BAZ"));
        expected.forEach(provider::write);

        final List<Event> actual;
        try (final Stream<Event> stream = provider.readFrom(0)) {
            actual = stream.collect(toList());
        }

        assertNotNull(actual);
        assertIterableEquals(expected, actual);
        actual.forEach(event -> log.info("{}", event));
    }

    @ParameterizedTest
    @MethodSource("createProviders")
    void shouldReadEventStreamByUuid(@Nonnull StoreProvider provider) {
        final UUID uuid = randomUUID();
        final List<Event> expected = asList(
                new SampleEvent("FOO", uuid),
                new SampleEvent("BAR", uuid),
                new SampleEvent("BAZ", uuid)
        );

        expected.forEach(provider::write);

        final Collection<Event> actual = provider.readBy(uuid);

        assertNotNull(actual);
        assertIterableEquals(expected, actual);
        actual.forEach(event -> log.info("Loaded event {}", event));
    }

    @ParameterizedTest
    @MethodSource("createProviders")
    void shouldReadBatchEventWrites(@Nonnull StoreProvider provider) {
        final UUID uuid = randomUUID();
        final List<Event> expected = asList(
                new SampleEvent("FOO", uuid),
                new SampleEvent("BAR", uuid),
                new SampleEvent("BAZ", uuid)
        );

        provider.write(expected.toArray(new Event[0]));

        final Collection<Event> actual = provider.readBy(uuid);

        assertNotNull(actual);
        assertIterableEquals(expected, actual);
        actual.forEach(event -> log.info("Loaded event {}", event));
    }

    @ParameterizedTest
    @MethodSource("createProviders")
    void shouldSuccessfullyWriteVersionedEventStream(@Nonnull StoreProvider provider) {
        final UUID uuid = randomUUID();
        final List<Event> expected = asList(
                new SampleEvent("FOO", uuid, 0),
                new SampleEvent("BAR", uuid, 1),
                new SampleEvent("BAZ", uuid, 2)
        );

        assertDoesNotThrow(() -> expected.forEach(provider::write));
    }

    @ParameterizedTest
    @MethodSource("createProviders")
    void shouldThrowVersionMismatchException(@Nonnull StoreProvider provider) {
        final UUID uuid = randomUUID();
        final List<Event> expected = asList(
                new SampleEvent("FOO", uuid, 0),
                new SampleEvent("BAR", uuid, 1),
                new SampleEvent("BAZ", uuid, 1)
        );

        assertThrows(VersionMismatchException.class, () -> expected.forEach(provider::write));
        assertDoesNotThrow(() -> provider.write(new SampleEvent("LAZ", uuid, 2)));
    }

    @ParameterizedTest
    @MethodSource("createProviders")
    void shouldDeleteFullStreamByUuid(@Nonnull StoreProvider provider) {
        final UUID uuid = randomUUID();
        final UUID anotherUuid = randomUUID();
        final List<Event> events = asList(
                new SampleEvent("FOO", uuid),
                new SampleEvent("BAR", uuid),
                new SampleEvent("BAZ", anotherUuid)
        );

        events.forEach(provider::write);
        log.debug("Written event stream {} with size 2 and event stream {} with size 1", uuid, anotherUuid);
        provider.deleteBy(uuid);

        final List<Event> remaining;
        try (final Stream<Event> stream = provider.readFrom(0)) {
            remaining = stream.collect(toList());
        }
        assertEquals(1, remaining.size(), "EventStore should contain only one event");
        assertEquals(new SampleEvent("BAZ", anotherUuid), remaining.iterator().next());
    }

    @ParameterizedTest
    @MethodSource("createProviders")
    void deletingNonExistingEventStreamByUuidShouldNotFail(@Nonnull StoreProvider provider) {
        final SampleEvent expected = new SampleEvent("FOO", randomUUID());

        provider.write(expected);
        provider.deleteBy(randomUUID());

        final Collection<Event> actual = provider.readBy(expected.uuid());
        assertEquals(1, actual.size());
        assertEquals(expected, actual.iterator().next());
    }

    @SneakyThrows
    @ParameterizedTest
    @MethodSource("createProviders")
    void providersShouldWriteAllEventsInConcurrentEnvironment(@Nonnull StoreProvider provider) {
        final int workersCount = getRuntime().availableProcessors();
        final int streamSize = 100;
        log.info("Prepare to verify dataset with size {}", workersCount * streamSize);
        final ExecutorService executor = Executors.newFixedThreadPool(workersCount);
        try {
            final CountDownLatch latch = new CountDownLatch(workersCount);
            for (int i = 0; i < workersCount; i++) {
                executor.execute(() -> {
                    newParallelEventStream(streamSize).forEach(provider::write);
                    latch.countDown();
                });
            }
            assertTrue(latch.await(5, TimeUnit.SECONDS), "Dataset wasn't written in 5 sec");

            try (Stream<Event> stream = provider.readFrom(0)) {
                assertEquals(workersCount * streamSize, stream.count(), "Written events count wrong");
            }
        } finally {
            executor.shutdown();
        }
    }

    @SuppressWarnings("SameParameterValue")
    private Stream<? extends Event> newParallelEventStream(int count) {
        return IntStream.range(0, count).mapToObj(val -> new FancyEvent("name " + val, randomUUID())).parallel();
    }

    @AfterEach
    void clearEventStore() {
        for (StoreProvider provider : PROVIDERS) {
            try (final Stream<Event> stream = provider.readFrom(0)) {
                final Set<UUID> uuids = stream.map(Event::uuid).filter(Objects::nonNull).collect(toSet());
                uuids.forEach(provider::deleteBy);
            }
        }
    }

    @AfterAll
    @SneakyThrows
    static void closeResources() {
        for (StoreProvider provider : PROVIDERS) {
            if (provider instanceof AutoCloseable) {
                try {
                    ((AutoCloseable) provider).close();
                } catch (Exception ignored) {}
            }
        }
    }
}
