package io.jes.provider;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Objects;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

import io.jes.Event;
import io.jes.ex.BrokenStoreException;
import io.jes.ex.VersionMismatchException;
import io.jes.provider.jdbc.DataSourceSyntax;
import io.jes.provider.jdbc.SyntaxFactory;
import io.jes.serializer.EventSerializer;
import io.jes.serializer.SerializerFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static java.lang.Long.MAX_VALUE;
import static java.util.Objects.requireNonNull;
import static java.util.Spliterator.ORDERED;

@Slf4j
public class JdbcStoreProvider<T> implements StoreProvider {

    private final DataSource dataSource;
    private final DataSourceSyntax syntax;
    private final EventSerializer<T> serializer;

    @SuppressWarnings("WeakerAccess")
    public JdbcStoreProvider(@Nonnull DataSource dataSource, @Nonnull Class<T> serializationType) {
        try {
            this.dataSource = requireNonNull(dataSource);
            this.serializer = SerializerFactory.newEventSerializer(serializationType);

            try (final Connection connection = dataSource.getConnection()) {

                final String schema = Objects.requireNonNull(connection.getSchema(), "Schema must not be null");
                final DatabaseMetaData metaData = connection.getMetaData();
                final String databaseName = metaData.getDatabaseProductName();
                this.syntax = SyntaxFactory.newDataSourceSyntax(requireNonNull(databaseName), schema);
                createEventStore(connection, syntax.createStore(serializationType));
            }
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }

    @SneakyThrows
    private void createEventStore(@Nonnull Connection connection, @Nonnull String ddl) {
        try (PreparedStatement statement = connection.prepareStatement(ddl)) {
            statement.executeUpdate();
        }
    }

    @Override
    public Stream<Event> readFrom(long offset) {
        return readBy(offset, syntax.queryEvents());
    }

    @Override
    public Stream<Event> readBy(@Nonnull String stream) {
        return readBy(stream, syntax.queryEventsByStream());
    }

    @SuppressWarnings("squid:S2095")
    private Stream<Event> readBy(@Nonnull Object value, @Nonnull String from) {
        try {
            final Connection connection = dataSource.getConnection();
            final PreparedStatement statement = connection.prepareStatement(from);

            statement.setObject(1, value);
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
                    //noinspection unchecked
                    T data = (T) set.getObject(syntax.eventContentName());
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
        writeTo(event, syntax.insertEvents());
    }

    private void writeTo(Event event, String where) {
        try (final Connection connection = dataSource.getConnection();
             final PreparedStatement statement = connection.prepareStatement(where)) {

            final String stream = event.stream();
            verifyStreamVersion(event, connection);

            final T data = serializer.serialize(event);

            statement.setString(1, stream);
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
        final String stream = event.stream();
        final long expectedVersion = event.expectedStreamVersion();
        if (stream != null && expectedVersion != -1) {
            try (PreparedStatement versionStatement = connection.prepareStatement(syntax.queryEventsStreamVersion())) {
                versionStatement.setString(1, stream);
                try (final ResultSet query = versionStatement.executeQuery()) {
                    if (!query.next()) {
                        throw new BrokenStoreException("Can't read stream [" + stream + "] version");
                    }
                    final long actualVersion = query.getLong(1);
                    if (expectedVersion != actualVersion) {
                        throw new VersionMismatchException(expectedVersion, actualVersion);
                    }
                }
            }
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
}
