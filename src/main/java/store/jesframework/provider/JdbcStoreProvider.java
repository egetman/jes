package store.jesframework.provider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import javax.sql.DataSource;

import store.jesframework.Event;
import store.jesframework.ex.BrokenStoreException;
import store.jesframework.ex.VersionMismatchException;
import store.jesframework.provider.jdbc.DDLFactory;
import store.jesframework.serializer.SerializationOption;
import store.jesframework.serializer.Serializer;
import store.jesframework.serializer.SerializerFactory;
import store.jesframework.serializer.upcaster.Upcaster;
import store.jesframework.snapshot.SnapshotReader;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static store.jesframework.util.JdbcUtils.createConnection;
import static store.jesframework.util.JdbcUtils.unwrapJdbcType;
import static store.jesframework.util.PropsReader.getPropety;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.util.Objects.requireNonNull;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Collectors.toList;

/**
 * JDBC {@link StoreProvider} implementation.
 *
 * @param <T> type of event serialization.
 */
@Slf4j
public class JdbcStoreProvider<T> implements StoreProvider, SnapshotReader, AutoCloseable {

    private static final int FETCH_SIZE = 100;

    private final DataSource dataSource;
    private final Serializer<Event, T> serializer;
    private final UpcasterRegistry<T> upcasterRegistry = new UpcasterRegistry<>();

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
        final String query = getPropety("jes.jdbc.statement.select-events");
        final SequentialResultSetIterator iterator = new SequentialResultSetIterator(query, offset);
        return StreamSupport.stream(spliteratorUnknownSize(iterator, ORDERED), false).onClose(iterator::close);
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

    @Nonnull
    private Stream<Event> readBy(@Nonnull String from, @Nonnull Object... values) {
        final Connection connection = createConnection(dataSource);
        try {
            connection.setReadOnly(true);
            connection.setAutoCommit(false);

            final PreparedStatement statement = connection.prepareStatement(from, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
            statement.setFetchSize(FETCH_SIZE);

            int index = 1;
            for (Object parameter : values) {
                statement.setObject(index++, parameter);
            }

            final ResultSet set = statement.executeQuery();

            return resultSetToStream(connection, statement, set);
        } catch (Exception e) {
            closeQuietly(connection);
            throw new BrokenStoreException(e);
        }
    }

    @Nonnull
    private Stream<Event> resultSetToStream(Connection connection, Statement statement, ResultSet set) {
        final ResultSetIterator iterator = new ResultSetIterator(connection, statement, set);
        return StreamSupport.stream(spliteratorUnknownSize(iterator, ORDERED), false).onClose(iterator::close);
    }

    @Override
    public void write(@Nonnull Event event) {
        final String query = getPropety("jes.jdbc.statement.insert-events");
        try (final Connection connection = createConnection(dataSource);
             final PreparedStatement statement = connection.prepareStatement(query)) {

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

    @Override
    public void write(@Nonnull Event... events) {
        try (final Connection connection = createConnection(dataSource)) {
            final boolean supportsBatches = connection.getMetaData().supportsBatchUpdates();
            // first check if we can use batch
            if (!supportsBatches) {
                log.warn("Current db doesn't support batch updates. Separate updates will be used");
                for (Event event : events) {
                    write(event);
                }
                return;
            }
            // ok, we can use it
            connection.setAutoCommit(false);
            final String query = getPropety("jes.jdbc.statement.insert-events");
            try (final PreparedStatement statement = connection.prepareStatement(query)) {
                for (Event event : events) {
                    final UUID uuid = event.uuid();
                    verifyStreamVersion(event, connection);
                    final T data = serializer.serialize(event);

                    statement.setObject(1, uuid);
                    statement.setObject(2, data);
                    statement.addBatch();
                }
                statement.executeBatch();
                connection.commit();
            }
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
            final String query = getPropety("jes.jdbc.statement.select-events-version");
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setObject(1, uuid);
                try (final ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        throw new BrokenStoreException("Can't read uuid [" + uuid + "] version");
                    }
                    final long actualVersion = resultSet.getLong(1);
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
        log.trace("Prepare to remove {} event stream", uuid);
        final String query = getPropety("jes.jdbc.statement.delete-events");
        try (Connection connection = createConnection(dataSource);
             PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setObject(1, uuid);
            final int affectedEvents = statement.executeUpdate();
            log.trace("{} events successfully removed", affectedEvents);
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }

    @Override
    public void close() {
        if (dataSource instanceof AutoCloseable) {
            closeQuietly((AutoCloseable) dataSource);
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

    @SuppressWarnings("unused")
    public void addUpcaster(@Nonnull Upcaster<T> upcaster) {
        upcasterRegistry.addUpcaster(upcaster);
    }

    /**
     * This iterator is NOT Thread safe.
     */
    private class ResultSetIterator implements Iterator<Event>, AutoCloseable {

        @Getter(AccessLevel.PACKAGE)
        long lastOffset;
        private FetchState fetchState = FetchState.UNKNOWN;

        private final ResultSet set;
        private final Statement statement;
        private final Connection connection;

        ResultSetIterator(@Nonnull Connection connection, @Nonnull Statement statement, @Nonnull ResultSet set) {
            this.set = Objects.requireNonNull(set, "ResultSet must not be null");
            this.statement = Objects.requireNonNull(statement, "Statement must not be null");
            this.connection = Objects.requireNonNull(connection, "Connection must not be null");
        }

        @Override
        @SneakyThrows
        public boolean hasNext() {
            if (fetchState == FetchState.UNKNOWN) {
                final boolean next = set.next();
                if (next) {
                    fetchState = FetchState.HAS_NEXT;
                } else {
                    fetchState = FetchState.EMPTY;
                }
            }
            return fetchState == FetchState.HAS_NEXT;
        }

        @Override
        public Event next() {
            if (fetchState == FetchState.EMPTY || (fetchState == FetchState.UNKNOWN && !hasNext())) {
                closeQuietly(set, statement, connection);
                throw new NoSuchElementException("No more events to read");
            }
            final Event event = readEvent();
            fetchState = FetchState.UNKNOWN;
            return event;
        }

        @Nonnull
        @SneakyThrows
        private Event readEvent() {
            // get values by index a bit more efficient
            lastOffset = set.getLong(1);
            T data = unwrapJdbcType(set.getObject(2));
            data = upcasterRegistry.tryUpcast(lastOffset, data);
            return serializer.deserialize(data);
        }

        @Override
        public void close() {
            closeQuietly(set, statement, connection);
        }
    }

    /**
     * This iterator is NOT Thread safe.
     */
    private class SequentialResultSetIterator implements Iterator<Event>, AutoCloseable {

        private static final int MAX_RETRIES = 5;
        private static final int SEQUENTIAL_FETCH_SIZE = 1000;

        private int retryCount;
        private long beforeLastOffset;

        private final String query;
        private ResultSetIterator delegate;

        SequentialResultSetIterator(@Nonnull String from, long offset) {
            query = Objects.requireNonNull(from);
            delegate = createIterator(from, offset);
        }

        private ResultSetIterator createIterator(@Nonnull String from, long offset) {
            final Connection connection = createConnection(dataSource);
            try {
                connection.setAutoCommit(false);
                connection.setReadOnly(true);

                PreparedStatement statement = connection.prepareStatement(from, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
                statement.setFetchSize(SEQUENTIAL_FETCH_SIZE);
                statement.setLong(1, offset);
                final ResultSet set = statement.executeQuery();

                return new ResultSetIterator(connection, statement, set);
            } catch (Exception e) {
                closeQuietly(connection);
                throw new BrokenStoreException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        private boolean isSequencial() {
            return beforeLastOffset == 0 || delegate.getLastOffset() - beforeLastOffset == 1;
        }

        @Override
        @SuppressWarnings("squid:CommentedOutCodeLine")
        public Event next() {
            final Event next = delegate.next();
            // ok, keep sequencially read stuff
            if (isSequencial()) {
                beforeLastOffset = delegate.getLastOffset();
                return next;
            }
            log.trace("last returned offset {}, offset before last {}", delegate.getLastOffset(), beforeLastOffset);
            /*
            We have some kind of read anomaly. It can be explained in context of PostgreSQL.
            First, create table:

            CREATE TABLE counters(counter INT);
            INSERT INTO counters(counter) VALUES(1);
            BEGIN TRANSACTION ISOLATION LEVEL serializable;
            SELECT SUM(counter) FROM counters; / this will return 1 /

            // insert sum into counters. wait until committing next transaction before executing the insert
            INSERT INTO counters(counter) VALUES(1);
            COMMIT;

            // this transaction should commit before doing the insert in the above transaction and after the
            // above transaction has calculated the sum
            BEGIN TRANSACTION ISOLATION LEVEL serializable;
            INSERT INTO counters(counter) VALUES(10);
            COMMIT;

            both transactions commit and the final table looks like: 1, 10, 1

            which is possible if the first transaction committed first, and then the second transaction committed
            which is different from the order the transactions committed in by the wall clock. so it is possible
            for another client to see the table as:

                [1] (the initial state)
                [1, 10] (after the second transaction committed)
                [1, 1, 10] (after the first transaction committed)

            which is a sequence of states which should not be possible. if you see [1], [1, 10] then you should see
            [1, 10, 11] as the last state. hence it violates external consistency.
            */
            log.trace("Closing current delegate and create new one with start offset {}", beforeLastOffset);
            close();
            delegate = createIterator(query, beforeLastOffset);

            if (retryCount++ > MAX_RETRIES) {
                /*
                If we have gap there possible 2 situations:
                1. some transactions are slow, and they just not commited yet
                2. some transaction rollback and increment sequence in db or stream deletion was performed. (in that
                case it's ok to skip it)
                case 1 should be avoided with MAX_RETRIES read retries.
                 */
                beforeLastOffset++;
            }
            return next();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    /**
     * Result set fetching status for {@literal ResultSetIterator}.
     */
    private enum FetchState {
        UNKNOWN, HAS_NEXT, EMPTY
    }

}