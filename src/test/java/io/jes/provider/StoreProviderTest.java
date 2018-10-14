package io.jes.provider;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.jes.Event;
import io.jes.ex.VersionMismatchException;
import lombok.extern.slf4j.Slf4j;

import static java.util.Arrays.asList;

@Slf4j
abstract class StoreProviderTest {

    abstract StoreProvider createProvider();

    @Test
    void shouldReadOwnWrites() {
        StoreProvider provider = createProvider();

        final List<Event> expected = asList(new SampleEvent("FOO"), new SampleEvent("BAR"), new SampleEvent("BAZ"));
        expected.forEach(provider::write);

        final List<Event> actual = provider.readFrom(0).collect(Collectors.toList());

        Assertions.assertNotNull(actual);
        Assertions.assertIterableEquals(expected, actual);
        actual.forEach(event -> log.info("{}", event));
    }

    @Test
    void shouldReadEventsByStream() {
        StoreProvider provider = createProvider();

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
        StoreProvider provider = createProvider();

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
        StoreProvider provider = createProvider();

        final UUID uuid = UUID.randomUUID();
        final List<Event> expected = asList(
                new SampleEvent("FOO", uuid, 0),
                new SampleEvent("BAR", uuid, 1),
                new SampleEvent("BAZ", uuid, 1)
        );

        Assertions.assertThrows(VersionMismatchException.class, () -> expected.forEach(provider::write));
    }


}
