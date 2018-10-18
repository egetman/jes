package io.jes.provider;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import javax.annotation.Nonnull;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import io.jes.Event;
import io.jes.common.SampleEvent;
import io.jes.ex.VersionMismatchException;
import lombok.extern.slf4j.Slf4j;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.TestInstance.Lifecycle;

@Slf4j
@TestInstance(Lifecycle.PER_CLASS)
abstract class StoreProviderTest {

    @Nonnull
    abstract StoreProvider getProvider();

    @AfterEach
    void clearEventStore() {
        final StoreProvider provider = getProvider();
        final Set<UUID> uuids = provider.readFrom(0).map(Event::uuid).filter(Objects::nonNull).collect(toSet());
        uuids.forEach(provider::deleteBy);
    }

    @Test
    void shouldReadOwnWrites() {
        final StoreProvider provider = getProvider();

        final List<Event> expected = asList(new SampleEvent("FOO"), new SampleEvent("BAR"), new SampleEvent("BAZ"));
        expected.forEach(provider::write);

        final List<Event> actual = provider.readFrom(0).collect(toList());

        Assertions.assertNotNull(actual);
        Assertions.assertIterableEquals(expected, actual);
        actual.forEach(event -> log.info("{}", event));
    }

    @Test
    void shouldReadEventStreamByUuid() {
        final StoreProvider provider = getProvider();

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

    @Test
    void shouldSuccessfullyWriteVersionedEventStream() {
        final StoreProvider provider = getProvider();

        final UUID uuid = UUID.randomUUID();
        final List<Event> expected = asList(
                new SampleEvent("FOO", uuid, 0),
                new SampleEvent("BAR", uuid, 1),
                new SampleEvent("BAZ", uuid, 2)
        );

        expected.forEach(provider::write);
    }

    @Test
    void shouldThrowVersionMismatchException() {
        final StoreProvider provider = getProvider();

        final UUID uuid = UUID.randomUUID();
        final List<Event> expected = asList(
                new SampleEvent("FOO", uuid, 0),
                new SampleEvent("BAR", uuid, 1),
                new SampleEvent("BAZ", uuid, 1)
        );

        Assertions.assertThrows(VersionMismatchException.class, () -> expected.forEach(provider::write));
    }

    @Test
    void shouldDeleteFullStreamByUuid() {
        final StoreProvider provider = getProvider();

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

    @Test
    void deletingNonExistingEventStreamByUuidShouldNotFail() {
        final StoreProvider provider = getProvider();
        final SampleEvent expected = new SampleEvent("FOO", UUID.randomUUID());

        provider.write(expected);
        provider.deleteBy(UUID.randomUUID());

        final Collection<Event> actual = provider.readBy(expected.uuid());
        Assertions.assertEquals(1, actual.size());
        Assertions.assertEquals(expected, actual.iterator().next());
    }
}
