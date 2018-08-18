package io.jes;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

import io.jes.ex.VersionMismatchException;
import io.jes.provider.JdbcStoreProvider;
import io.jes.provider.StoreProvider;
import lombok.extern.slf4j.Slf4j;

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
        final StoreProvider provider = new JdbcStoreProvider<>(createDataSource(), POSTGRESQL, byte[].class);

        final List<Event> expected = asList(new SampleEvent("FOO"), new SampleEvent("BAR"), new SampleEvent("BAZ"));
        expected.forEach(provider::write);

        final List<Event> actual = provider.readFrom(0).collect(Collectors.toList());

        Assertions.assertNotNull(actual);
        Assertions.assertIterableEquals(expected, actual);
        actual.forEach(event -> log.info("{}", event));
    }

    @Test
    void shouldReadEventsByStream() {
        final StoreProvider provider = new JdbcStoreProvider<>(createDataSource(), POSTGRESQL, byte[].class);

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
        final StoreProvider provider = new JdbcStoreProvider<>(createDataSource(), POSTGRESQL, byte[].class);

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
        final StoreProvider provider = new JdbcStoreProvider<>(createDataSource(), POSTGRESQL, byte[].class);

        final String stream = UUID.randomUUID().toString();
        final List<Event> expected = asList(
                new SampleEvent("FOO", stream, 0),
                new SampleEvent("BAR", stream, 1),
                new SampleEvent("BAZ", stream, 1)
        );

        Assertions.assertThrows(VersionMismatchException.class, () -> expected.forEach(provider::write));
    }

    private DataSource createDataSource() {
        final String user = "csi";
        final String password = "csi";
        final PostgreSQLContainer container = new PostgreSQLContainer()
                .withDatabaseName("jes")
                .withUsername(user)
                .withPassword(password);
        container.start();

        final HikariConfig config = new HikariConfig();

        config.setUsername(user);
        config.setPassword(password);
        config.setMaximumPoolSize(50);
        config.setJdbcUrl(container.getJdbcUrl());
        config.setDriverClassName(container.getDriverClassName());
//        config.setJdbcUrl("jdbc:postgresql://192.168.14.202:5432/csi");
        final HikariDataSource dataSource = new HikariDataSource(config);
        log.debug("url: {}, driver: {}, user: {}, password: {}\n", config.getJdbcUrl(), user, password);

        return dataSource;
    }
}
