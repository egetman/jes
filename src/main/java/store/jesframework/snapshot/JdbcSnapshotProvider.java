package store.jesframework.snapshot;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import store.jesframework.Aggregate;
import store.jesframework.ex.BrokenStoreException;
import store.jesframework.provider.jdbc.DDLFactory;
import store.jesframework.serializer.SerializationOption;
import store.jesframework.serializer.Serializer;
import store.jesframework.serializer.SerializerFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static store.jesframework.util.JdbcUtils.createConnection;
import static store.jesframework.util.JdbcUtils.unwrapJdbcType;
import static store.jesframework.util.PropsReader.getPropety;
import static java.util.Objects.requireNonNull;

@Slf4j
public class JdbcSnapshotProvider<T> implements SnapshotProvider, AutoCloseable {

    private final DataSource dataSource;
    private final Serializer<Aggregate, T> serializer;

    public JdbcSnapshotProvider(@Nonnull DataSource dataSource, @Nonnull Class<T> serializationType,
                                @Nonnull SerializationOption... options) {
        try {
            this.dataSource = requireNonNull(dataSource);
            this.serializer = SerializerFactory.newAggregateSerializer(serializationType, options);

            try (final Connection connection = createConnection(this.dataSource)) {
                createSnapshotStore(connection, DDLFactory.getAggregateStoreDDL(connection));
            }
        } catch (Exception e) {
            throw new BrokenStoreException(e);
        }
    }

    @SneakyThrows
    private void createSnapshotStore(@Nonnull Connection connection, @Nonnull String ddl) {
        try (PreparedStatement statement = connection.prepareStatement(ddl)) {
            final int code = statement.executeUpdate();
            if (code == 0) {
                log.info("Snapshot store successfully created");
            }
        }
    }

    @Nonnull
    @Override
    @SneakyThrows
    public <A extends Aggregate> A initialStateOf(@Nonnull UUID uuid, @Nonnull Class<A> type) {
        final Aggregate aggregate = findAggregateByUuid(uuid);
        if (aggregate == null) {
            return SnapshotProvider.super.initialStateOf(uuid, type);
        }
        //noinspection unchecked
        return (A) aggregate;
    }

    @Nonnull
    @Override
    @SneakyThrows
    @SuppressWarnings("squid:S2077")
    public <A extends Aggregate> A snapshot(@Nonnull A aggregate) {
        // check if we already have a snapshot for that aggregate
        final boolean snapshotExists = existsAggregateByUuid(aggregate.uuid());
        final String sql = snapshotExists ? getPropety("jes.jdbc.statement.update-aggregate")
                : getPropety("jes.jdbc.statement.insert-aggregate");

        final Integer affectedCount = execute(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setObject(1, serializer.serialize(aggregate));
                statement.setObject(2, aggregate.uuid());
                return statement.executeUpdate();
            }
        });
        log.debug(" {} Aggregate snapshot successfully {}", affectedCount, snapshotExists ? "updated" : "created");
        return aggregate;
    }

    @Override
    @SneakyThrows
    public void reset(@Nonnull UUID uuid) {
        Objects.requireNonNull(uuid);
        execute(connection -> {
            final String query = getPropety("jes.jdbc.statement.delete-aggregate");
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setObject(1, uuid);
                final int deletedRows = statement.executeUpdate();
                log.debug("Deleted {} snapshots by uuid {}", deletedRows, uuid);
                return deletedRows;
            }
        });
    }

    @Nullable
    @SneakyThrows
    private Aggregate findAggregateByUuid(@Nonnull UUID uuid) {
        return execute(connection -> {
            final String query = getPropety("jes.jdbc.statement.select-aggregate");
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setObject(1, Objects.requireNonNull(uuid, "Aggregate uuid must not be null"));
                final ResultSet set = statement.executeQuery();
                if (!set.next()) {
                    return null;
                }
                return serializer.deserialize(unwrapJdbcType(set.getObject(1)));
            }
        });
    }

    @SneakyThrows
    private boolean existsAggregateByUuid(@Nonnull UUID uuid) {
        return execute(connection -> {
            final String query = getPropety("jes.jdbc.statement.exists-aggregate");
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setObject(1, Objects.requireNonNull(uuid, "Aggregate uuid must not be null"));
                final ResultSet set = statement.executeQuery();
                if (!set.next()) {
                    return false;
                }
                return set.getBoolean(1);
            }
        });
    }

    @SneakyThrows
    private <Y> Y execute(@Nonnull ThrowableFunction<Connection, Y> consumer) {
        try (Connection connection = createConnection(dataSource)) {
            return Objects.requireNonNull(consumer, "Consumer must not be null").apply(connection);
        } catch (Exception e) {
            throw new BrokenStoreException(e);
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

    /**
     * Represents a function that accepts one argument and produces a result.
     *
     * <p>This is a <a href="package-summary.html">functional interface</a>
     * whose functional method is {@link #apply(Object)}.
     *
     * @param <T> the type of the input to the function
     * @param <R> the type of the result of the function
     */
    private interface ThrowableFunction<T, R> {

        /**
         * Applies this function to the given argument.
         *
         * @param argument the function argument
         * @return the function result
         */
        @SuppressWarnings("squid:S00112")
        R apply(T argument) throws Throwable;

    }
}
