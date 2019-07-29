package io.jes.provider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.Spliterators.AbstractSpliterator;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

import io.jes.Event;
import io.jes.ex.BrokenStoreException;
import io.jes.ex.VersionMismatchException;
import io.jes.provider.jdbc.DDLFactory;
import io.jes.serializer.SerializationOption;
import io.jes.serializer.Serializer;
import io.jes.serializer.SerializerFactory;
import io.jes.snapshot.SnapshotReader;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static io.jes.util.JdbcUtils.createConnection;
import static io.jes.util.JdbcUtils.unwrapJdbcType;
import static io.jes.util.PropsReader.getPropety;
import static java.lang.Long.MAX_VALUE;
import static java.util.Objects.requireNonNull;
import static java.util.Spliterator.ORDERED;
import static java.util.stream.Collectors.toList;

/**
 * JDBC {@link StoreProvider} implementation.
 *
 * @param <T> type of event serialization.
 */
@Slf4j
public class JdbcStoreProvider<T> implements StoreProvider, SnapshotReader, AutoCloseable {

    private final DataSource dataSource;
    private final Serializer<Event, T> serializer;

    public JdbcStoreProvider(@Nonnull DataSource dataSource, @Nonnull Class<T> serializationType,
                             @Nonnull SerializationOption... options) {
        try {
            this.dataSource = requireNonNull(dataSource);
            this.serializer = SerializerFactory.newEventSerializer(serializationType, options);

            try (final Connection connection = createConnection(this.dataSource)) {
                final String ddl = DDLFactory.getEventStoreDDL(connection, serializationType);
                createEventStore(connection, ddl);
            }
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }

    @SneakyThrows
    private void createEventStore(@Nonnull Connection connection, @Nonnull String ddl) {
        try (PreparedStatement statement = connection.prepareStatement(ddl)) {
            final int code = statement.executeUpdate();
            if (code == 0) {
                log.info("JEventStore successfully created");
            }
        }
    }

    @Override
    public Stream<Event> readFrom(long offset) {
        return readBy(getPropety("jes.jdbc.statement.select-events"), offset);
    }

    @Override
    public Collection<Event> readBy(@Nonnull UUID uuid) {
        try (final Stream<Event> stream = readBy(getPropety("jes.jdbc.statement.select-events-by-uuid"), uuid)) {
            return stream.collect(toList());
        }
    }

    @Override
    public Collection<Event> readBy(@Nonnull UUID uuid, long skip) {
        final String statement = getPropety("jes.jdbc.statement.select-events-by-uuid-with-skip");
        try (final Stream<Event> stream = readBy(requireNonNull(statement), uuid, skip)) {
            return stream.collect(toList());
        }
    }

    private Stream<Event> readBy(@Nonnull String from, @Nonnull Object... values) {
        try {
            final Connection connection = createConnection(dataSource);
            final PreparedStatement statement = connection.prepareStatement(from);

            int index = 1;
            for (Object parameter : values) {
                statement.setObject(index++, parameter);
            }

            final ResultSet set = statement.executeQuery();

            return resultSetToStream(connection, statement, set);
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }

    private Stream<Event> resultSetToStream(Connection connection, Statement statement, ResultSet set) {
        return StreamSupport.stream(new AbstractSpliterator<Event>(MAX_VALUE, ORDERED) {

            @Override
            public boolean tryAdvance(Consumer<? super Event> action) {
                try {
                    if (!set.next()) {
                        return false;
                    }
                    T data = unwrapJdbcType(set.getObject("data"));
                    action.accept(serializer.deserialize(data));
                } catch (Exception e) {
                    throw new BrokenStoreException(e);
                }
                return true;
            }
        }, false).onClose(() -> closeQuietly(set, statement, connection));
    }

    @Override
    public void write(@Nonnull Event event) {
        writeTo(event, getPropety("jes.jdbc.statement.insert-events"));
    }

    private void writeTo(Event event, String where) {
        try (final Connection connection = createConnection(dataSource);
             final PreparedStatement statement = connection.prepareStatement(where)) {

            final UUID uuid = event.uuid();
            verifyStreamVersion(event, connection);

            final T data = serializer.serialize(event);

            statement.setObject(1, uuid);
            statement.setObject(2, data);

            statement.executeUpdate();
        } catch (BrokenStoreException | VersionMismatchException e) {
            throw e;
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }

    @SneakyThrows
    private void verifyStreamVersion(Event event, Connection connection) {
        final UUID uuid = event.uuid();
        final long expectedVersion = event.expectedStreamVersion();
        if (uuid != null && expectedVersion != -1) {
            final String select = getPropety("jes.jdbc.statement.select-events-version");
            try (PreparedStatement statement = connection.prepareStatement(select)) {
                statement.setObject(1, uuid);
                try (final ResultSet query = statement.executeQuery()) {
                    if (!query.next()) {
                        throw new BrokenStoreException("Can't read uuid [" + uuid + "] version");
                    }
                    final long actualVersion = query.getLong(1);
                    if (expectedVersion != actualVersion) {
                        log.error("Version mismatch detected for {}", event);
                        throw new VersionMismatchException(expectedVersion, actualVersion);
                    }
                }
            }
        }
    }

    @Override
    public void deleteBy(@Nonnull UUID uuid) {
        log.warn("Prepare to remove {} event stream", uuid);
        final String query = getPropety("jes.jdbc.statement.delete-events");
        try (Connection connection = createConnection(dataSource);
             PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setObject(1, uuid);
            final int affectedEvents = statement.executeUpdate();
            log.warn("{} events successfully removed", affectedEvents);
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }

    private void closeQuietly(AutoCloseable... resources) {
        for (AutoCloseable resource : resources) {
            if (resource != null) {
                try {
                    resource.close();
                } catch (Exception e) {
                    log.error("Exception during resource #close", e);
                }
            }
        }
    }

    @Override
    public void close() {
        if (dataSource instanceof AutoCloseable) {
            try {
                ((AutoCloseable) dataSource).close();
            } catch (Exception e) {
                log.error("Failed to close resource:", e);
            }
        }
    }
}
