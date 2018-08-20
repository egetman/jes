package io.jes.provider;

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

        final String stream = UUID.randomUUID().toString();
        final List<Event> expected = asList(
                new SampleEvent("FOO", stream),
                new SampleEvent("BAR", stream),
                new SampleEvent("BAZ", stream)
        );

        expected.forEach(provider::write);

        final List<Event> actual = provider.readBy(stream).collect(Collectors.toList());

        Assertions.assertNotNull(actual);
        Assertions.assertIterableEquals(expected, actual);
        actual.forEach(event -> log.info("Loaded event {}", event));
    }

    @Test
    void shouldSuccessfullyWriteVersionedEventStream() {
        StoreProvider provider = createProvider();

        final String stream = UUID.randomUUID().toString();
        final List<Event> expected = asList(
                new SampleEvent("FOO", stream, 0),
                new SampleEvent("BAR", stream, 1),
                new SampleEvent("BAZ", stream, 2)
        );

        expected.forEach(provider::write);
    }

    @Test
    void shouldThrowVersionMismatchException() {
        StoreProvider provider = createProvider();

        final String stream = UUID.randomUUID().toString();
        final List<Event> expected = asList(
                new SampleEvent("FOO", stream, 0),
                new SampleEvent("BAR", stream, 1),
                new SampleEvent("BAZ", stream, 1)
        );

        Assertions.assertThrows(VersionMismatchException.class, () -> expected.forEach(provider::write));
    }


}
