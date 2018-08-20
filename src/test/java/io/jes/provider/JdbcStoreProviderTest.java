package io.jes.provider;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.jes.Event;
import io.jes.ex.VersionMismatchException;
import lombok.extern.slf4j.Slf4j;

import static io.jes.provider.InfrastructureFactory.newDataSource;
import static io.jes.provider.jdbc.DataSourceType.POSTGRESQL;
import static java.util.Arrays.asList;

// todo: version caching? to avoid every-write check
// todo: multithreaded write - to be or not to be?
// todo: store drain-to? - recreate event store
// todo: event-intersepter ?? handling?
// todo: make snapshotting
// todo: store structure validation on start
// todo: event idempotency on read (clustered environment)


@Slf4j
class JdbcStoreProviderTest {

    @Test
    void shouldReadOwnWrites() {
        final StoreProvider provider = new JdbcStoreProvider<>(newDataSource(), POSTGRESQL, byte[].class);

        final List<Event> expected = asList(new SampleEvent("FOO"), new SampleEvent("BAR"), new SampleEvent("BAZ"));
        expected.forEach(provider::write);

        final List<Event> actual = provider.readFrom(0).collect(Collectors.toList());

        Assertions.assertNotNull(actual);
        Assertions.assertIterableEquals(expected, actual);
        actual.forEach(event -> log.info("{}", event));
    }

    @Test
    void shouldReadEventsByStream() {
        final StoreProvider provider = new JdbcStoreProvider<>(newDataSource(), POSTGRESQL, byte[].class);

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
        actual.forEach(event -> log.info("{}", event));
    }

    @Test
    void shouldSuccessfullyWriteVersionedEventStream() {
        final StoreProvider provider = new JdbcStoreProvider<>(newDataSource(), POSTGRESQL, byte[].class);

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
        final StoreProvider provider = new JdbcStoreProvider<>(newDataSource(), POSTGRESQL, byte[].class);

        final String stream = UUID.randomUUID().toString();
        final List<Event> expected = asList(
                new SampleEvent("FOO", stream, 0),
                new SampleEvent("BAR", stream, 1),
                new SampleEvent("BAZ", stream, 1)
        );

        Assertions.assertThrows(VersionMismatchException.class, () -> expected.forEach(provider::write));
    }

}
