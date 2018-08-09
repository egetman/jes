package io.jes.provider.jdbc;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Objects;
import java.util.Spliterators.AbstractSpliterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

import io.jes.Event;
import io.jes.serializer.EventSerializer;
import io.jes.serializer.KryoBinaryEventSerializer;
import io.jes.ex.BrokenStoreException;
import io.jes.ex.RewriteEventException;
import io.jes.provider.StoreProvider;
import lombok.extern.slf4j.Slf4j;

import static java.lang.Long.MAX_VALUE;
import static java.util.Spliterator.ORDERED;

@Slf4j
public class JdbcStoreProvider implements StoreProvider {

    @SuppressWarnings("SqlResolve")
    private static final String SEQUENCE_CALL = "SELECT max(id) FROM event_store";
    private static final String READ_EVENTS = "SELECT * FROM event_store WHERE id > ? ORDER BY id";
    private static final String READ_EVENTS_BY_LINK = "SELECT * FROM event_store WHERE stream = ? ORDER BY id";
    private static final String WRITE_EVENTS = "INSERT INTO event_store (id, stream, data) VALUES (?, ?, ?)";
    private static final String CREATE_EVENT_STORE = "CREATE TABLE IF NOT EXISTS event_store "
            + "(id BIGINT NOT NULL PRIMARY KEY, stream VARCHAR(256) NOT NULL, data BYTEA NOT NULL);";

    private final EventSerializer<byte[]> serializer = new KryoBinaryEventSerializer();

    private final DataSource dataSource;
    private final SequenceGenerator sequenceGenerator;
    private final Consumer<AutoCloseable> closeQuietly = resource -> {
        try {
            resource.close();
        } catch (Exception e) {
            log.error("", e);
        }
    };

    @SuppressWarnings("WeakerAccess")
    public JdbcStoreProvider(@Nonnull DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource);
        try {
            createEventStore(dataSource.getConnection(), CREATE_EVENT_STORE);
            this.sequenceGenerator = new SequenceGenerator(dataSource);
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
            throw new BrokenStoreException(e);
        }
    }

    @Override
    public Stream<Event> readFrom(long offset) {
        return readBy(offset, READ_EVENTS);
    }

    @Override
    public Stream<Event> readBy(@Nonnull String stream) {
        return readBy(stream, READ_EVENTS_BY_LINK);
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

                        final InputStream stream = set.getBinaryStream("data");
                        byte[] bytes = new byte[stream.available()];
                        //noinspection ResultOfMethodCallIgnored
                        stream.read(bytes, 0, stream.available());
                        action.accept(serializer.deserialize(bytes));
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
        if (event.getId() != 0) {
            throw new RewriteEventException(event);
        }
        writeTo(event, WRITE_EVENTS);
    }

    @SuppressWarnings("SameParameterValue")
    private void writeTo(Event event, String where) {
        try (final Connection connection = dataSource.getConnection();
             final PreparedStatement statement = connection.prepareStatement(where)) {

            connection.setAutoCommit(false);
            final long id = sequenceGenerator.nextSequenceNum();
            event.setId(id);
            final byte[] bytes = serializer.serialize(event);

            try (InputStream inputStream = new ByteArrayInputStream(bytes)) {

                statement.setLong(1, id);
                statement.setString(2, event.stream());
                statement.setBinaryStream(3, inputStream);

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

        SequenceGenerator(DataSource dataSource) throws Exception {
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement statement = connection.prepareStatement(SEQUENCE_CALL);
                 ResultSet result = statement.executeQuery()) {

                if (result.next()) {
                    GENERATOR.set(result.getLong("max"));
                }
            }
        }

        long nextSequenceNum() {
            return GENERATOR.incrementAndGet();
        }

    }
}
