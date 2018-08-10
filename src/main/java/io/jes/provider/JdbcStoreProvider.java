package io.jes.provider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Spliterators.AbstractSpliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

import io.jes.Event;
import io.jes.ex.BrokenStoreException;
import io.jes.provider.jdbc.DataSourceSyntax;
import io.jes.provider.jdbc.DataSourceSyntaxFactory;
import io.jes.provider.jdbc.DataSourceType;
import io.jes.serializer.EventSerializer;
import io.jes.serializer.SerializerFactory;
import lombok.extern.slf4j.Slf4j;

import static java.lang.Long.MAX_VALUE;
import static java.util.Objects.requireNonNull;
import static java.util.Spliterator.ORDERED;

@Slf4j
public class JdbcStoreProvider<T> implements StoreProvider {

    private final DataSource dataSource;
    private final DataSourceSyntax syntax;
    private final EventSerializer<T> serializer;
    private final Consumer<AutoCloseable> closeQuietly = resource -> {
        try {
            resource.close();
        } catch (Exception e) {
            log.error("", e);
        }
    };

    @SuppressWarnings("WeakerAccess")
    public JdbcStoreProvider(@Nonnull DataSource dataSource,
                             @Nonnull DataSourceType type,
                             @Nonnull Class<T> serializationType) {
        try {
            this.dataSource = requireNonNull(dataSource);
            this.serializer = SerializerFactory.newEventSerializer(serializationType);
            this.syntax = DataSourceSyntaxFactory.newDataSourceSyntax(requireNonNull(type));

            createEventStore(dataSource.getConnection(), syntax.createStore(serializationType));
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void createEventStore(@Nonnull Connection connection, @Nonnull String ddl) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(ddl)) {
            connection.setAutoCommit(false);
            statement.executeUpdate();
            connection.commit();
        } catch (Exception e) {
            connection.rollback();
            throw e;
        }
    }

    @Override
    public Stream<Event> readFrom(long offset) {
        return readBy(offset, syntax.readEvents());
    }

    @Override
    public Stream<Event> readBy(@Nonnull String stream) {
        return readBy(stream, syntax.readEventsByStream());
    }

    private Stream<Event> readBy(@Nonnull Object value, @Nonnull String from) {
        try {
            final Connection connection = dataSource.getConnection();
            final PreparedStatement statement = connection.prepareStatement(from);

            statement.setObject(1, value);
            final ResultSet set = statement.executeQuery();

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
            }, false).onClose(() -> {
                closeQuietly.accept(set);
                closeQuietly.accept(statement);
                closeQuietly.accept(connection);
            });
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }

    @Override
    public void write(@Nonnull Event event) {
        writeTo(event, syntax.writeEvents());
    }

    private void writeTo(Event event, String where) {
        try (final Connection connection = dataSource.getConnection();
             final PreparedStatement statement = connection.prepareStatement(where)) {

            connection.setAutoCommit(false);
            final T data = serializer.serialize(event);

            try {
                statement.setString(1, event.stream());
                statement.setObject(2, data);

                statement.executeUpdate();
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            }
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }
}
