package io.jes.provider;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Spliterators.AbstractSpliterator;
import java.util.concurrent.atomic.AtomicLong;
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
public class JdbcStoreProvider implements StoreProvider {

    private final EventSerializer<byte[]> serializer = SerializerFactory.binarySerializer();

    private final DataSource dataSource;
    private final DataSourceSyntax syntax;
    private final SequenceGenerator sequenceGenerator;
    private final Consumer<AutoCloseable> closeQuietly = resource -> {
        try {
            resource.close();
        } catch (Exception e) {
            log.error("", e);
        }
    };

    @SuppressWarnings("WeakerAccess")
    public JdbcStoreProvider(@Nonnull DataSource dataSource, @Nonnull DataSourceType type) {
        this.dataSource = requireNonNull(dataSource);
        this.syntax = DataSourceSyntaxFactory.forType(requireNonNull(type));
        try {
            createEventStore(dataSource.getConnection(), syntax.createStore());
            this.sequenceGenerator = new SequenceGenerator(dataSource, syntax);
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private void createEventStore(@Nonnull Connection connection, @Nonnull String structure) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(structure)) {
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

                        final InputStream stream = set.getBinaryStream(syntax.eventContentName());
                        byte[] bytes = new byte[stream.available()];
                        //noinspection ResultOfMethodCallIgnored
                        stream.read(bytes, 0, stream.available());
                        final EventWrapper wrapper = (EventWrapper) serializer.deserialize(bytes);
                        action.accept(wrapper.unwrap());
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

    @SuppressWarnings("SameParameterValue")
    private void writeTo(Event event, String where) {
        try (final Connection connection = dataSource.getConnection();
             final PreparedStatement statement = connection.prepareStatement(where)) {

            connection.setAutoCommit(false);
            final EventWrapper wrapper = new EventWrapper(sequenceGenerator.nextSequenceNum(), event);
            final byte[] bytes = serializer.serialize(wrapper);

            try {
                statement.setLong(1, wrapper.id());
                statement.setString(2, wrapper.stream());
                statement.setBinaryStream(3, new ByteArrayInputStream(bytes));

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

    private static class SequenceGenerator {

        private static final AtomicLong GENERATOR = new AtomicLong();

        SequenceGenerator(DataSource dataSource, DataSourceSyntax syntax) throws Exception {
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement statement = connection.prepareStatement(syntax.nextSequenceNumber());
                 ResultSet result = statement.executeQuery()) {

                if (result.next()) {
                    GENERATOR.set(result.getLong(syntax.sequenceValueName()));
                }
            }
        }

        long nextSequenceNum() {
            return GENERATOR.incrementAndGet();
        }

    }
}
