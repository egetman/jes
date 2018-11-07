package io.jes.provider;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.jes.Event;
import io.jes.ex.VersionMismatchException;
import lombok.extern.slf4j.Slf4j;

import static io.jes.internal.Events.SampleEvent;
import static io.jes.internal.FancyStuff.newDataSource;
import static io.jes.internal.FancyStuff.newEntityManager;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

@Slf4j
class StoreProviderTest {

    private static Collection<StoreProvider> createProviders() {
        return asList(
                new JdbcStoreProvider<>(newDataSource(), byte[].class),
                new JdbcStoreProvider<>(newDataSource(), String.class),
                new JpaStoreProvider<>(newEntityManager(byte[].class), byte[].class),
                new JpaStoreProvider<>(newEntityManager(String.class), String.class)
        );
    }

    @ParameterizedTest
    @MethodSource("createProviders")
    void shouldReadOwnWrites(StoreProvider provider) {
        final List<Event> expected = asList(new SampleEvent("FOO"), new SampleEvent("BAR"), new SampleEvent("BAZ"));
        expected.forEach(provider::write);

        final List<Event> actual = provider.readFrom(0).collect(toList());

        Assertions.assertNotNull(actual);
        Assertions.assertIterableEquals(expected, actual);
        actual.forEach(event -> log.info("{}", event));
    }

    @ParameterizedTest
    @MethodSource("createProviders")
    void shouldReadEventStreamByUuid(StoreProvider provider) {
        final UUID uuid = UUID.randomUUID();
        final List<Event> expected = asList(
                new SampleEvent("FOO", uuid),
                new SampleEvent("BAR", uuid),
                new SampleEvent("BAZ", uuid)
        );

        expected.forEach(provider::write);

        final Collection<Event> actual = provider.readBy(uuid);

        Assertions.assertNotNull(actual);
        Assertions.assertIterableEquals(expected, actual);
        actual.forEach(event -> log.info("Loaded event {}", event));
    }

    @ParameterizedTest
    @MethodSource("createProviders")
    void shouldSuccessfullyWriteVersionedEventStream(StoreProvider provider) {
        final UUID uuid = UUID.randomUUID();
        final List<Event> expected = asList(
                new SampleEvent("FOO", uuid, 0),
                new SampleEvent("BAR", uuid, 1),
                new SampleEvent("BAZ", uuid, 2)
        );

        expected.forEach(provider::write);
    }

    @ParameterizedTest
    @MethodSource("createProviders")
    void shouldThrowVersionMismatchException(StoreProvider provider) {
        final UUID uuid = UUID.randomUUID();
        final List<Event> expected = asList(
                new SampleEvent("FOO", uuid, 0),
                new SampleEvent("BAR", uuid, 1),
                new SampleEvent("BAZ", uuid, 1)
        );

        Assertions.assertThrows(VersionMismatchException.class, () -> expected.forEach(provider::write));
    }

    @ParameterizedTest
    @MethodSource("createProviders")
    void shouldDeleteFullStreamByUuid(StoreProvider provider) {
        final UUID uuid = UUID.randomUUID();
        final UUID anotherUuid = UUID.randomUUID();
        final List<Event> events = asList(
                new SampleEvent("FOO", uuid),
                new SampleEvent("BAR", uuid),
                new SampleEvent("BAZ", anotherUuid)
        );

        events.forEach(provider::write);
        log.debug("Written event stream {} with size 2 and event stream {} with size 1", uuid, anotherUuid);
        provider.deleteBy(uuid);

        final List<Event> remaining = provider.readFrom(0).collect(toList());
        Assertions.assertEquals(1, remaining.size(), "EventStore should contain only one event");
        Assertions.assertEquals(new SampleEvent("BAZ", anotherUuid), remaining.iterator().next());
    }

    @ParameterizedTest
    @MethodSource("createProviders")
    void deletingNonExistingEventStreamByUuidShouldNotFail(StoreProvider provider) {
        final SampleEvent expected = new SampleEvent("FOO", UUID.randomUUID());

        provider.write(expected);
        provider.deleteBy(UUID.randomUUID());

        final Collection<Event> actual = provider.readBy(expected.uuid());
        Assertions.assertEquals(1, actual.size());
        Assertions.assertEquals(expected, actual.iterator().next());
    }

}
