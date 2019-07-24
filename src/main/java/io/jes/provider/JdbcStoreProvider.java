package io.jes.provider;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
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
import io.jes.provider.jdbc.StoreDDLProducer;
import io.jes.serializer.SerializationOption;
import io.jes.serializer.Serializer;
import io.jes.serializer.SerializerFactory;
import io.jes.snapshot.SnapshotReader;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

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

    private static final String DEFAULT_SCHEMA_NAME = "es";

    private final DataSource dataSource;
    private final StoreDDLProducer ddlProducer;
    private final Serializer<Event, T> serializer;

    public JdbcStoreProvider(@Nonnull DataSource dataSource, @Nonnull Class<T> serializationType,
                             @Nonnull SerializationOption... options) {
        try {
            this.dataSource = requireNonNull(dataSource);
            this.serializer = SerializerFactory.newEventSerializer(serializationType, options);

            try (final Connection connection = dataSource.getConnection()) {

                final String schema = connection.getSchema() != null ? connection.getSchema() : DEFAULT_SCHEMA_NAME;
                final DatabaseMetaData metaData = connection.getMetaData();
                final String databaseName = metaData.getDatabaseProductName();
                this.ddlProducer = DDLFactory.newDDLProducer(requireNonNull(databaseName), schema);
                createEventStore(connection, ddlProducer.createStore(serializationType));
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
        return readBy(ddlProducer.queryEvents(), offset);
    }

    @Override
    public Collection<Event> readBy(@Nonnull UUID uuid) {
        try (final Stream<Event> stream = readBy(ddlProducer.queryEventsByUuid(), uuid)) {
            return stream.collect(toList());
        }
    }

    @Override
    public Collection<Event> readBy(@Nonnull UUID uuid, long skip) {
        try (final Stream<Event> stream = readBy(requireNonNull(ddlProducer.queryEventsByUuidWithSkip()), uuid, skip)) {
            return stream.collect(toList());
        }
    }

    private Stream<Event> readBy(@Nonnull String from, @Nonnull Object... values) {
        try {
            final Connection connection = dataSource.getConnection();
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
                    T data = unwrapJdbcType(set.getObject(ddlProducer.contentName()));
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
        writeTo(event, ddlProducer.insertEvents());
    }

    private void writeTo(Event event, String where) {
        try (final Connection connection = dataSource.getConnection();
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
            try (PreparedStatement statement = connection.prepareStatement(ddlProducer.queryEventsStreamVersion())) {
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
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(ddlProducer.deleteEvents())) {
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

    @SneakyThrows
    @SuppressWarnings("unchecked")
    private T unwrapJdbcType(@Nonnull Object jdbcType) {
        if (jdbcType instanceof String || jdbcType instanceof byte[]) {
            return (T) jdbcType;
        } else if (jdbcType instanceof Clob) {
            return (T) ((Clob) jdbcType).getSubString(1, (int) ((Clob) jdbcType).length());
        } else if (jdbcType instanceof Blob) {
            return (T) ((Blob) jdbcType).getBytes(1, (int) ((Blob) jdbcType).length());
        }
        throw new IllegalArgumentException("Unsupported jdbc type: " + jdbcType.getClass());
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
