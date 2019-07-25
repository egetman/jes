package io.jes.snapshot;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.sql.DataSource;

import io.jes.Aggregate;
import io.jes.ex.BrokenStoreException;
import io.jes.provider.jdbc.DDLFactory;
import io.jes.provider.jdbc.SnapshotDDLProducer;
import io.jes.serializer.SerializationOption;
import io.jes.serializer.Serializer;
import io.jes.serializer.SerializerFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import static io.jes.util.JdbcUtils.unwrapJdbcType;
import static java.util.Objects.requireNonNull;

@Slf4j
public class JdbcSnapshotProvider<T> implements SnapshotProvider, AutoCloseable {

    private final DataSource dataSource;
    private final SnapshotDDLProducer ddlProducer;
    private final Serializer<Aggregate, T> serializer;

    public JdbcSnapshotProvider(@Nonnull DataSource dataSource, @Nonnull Class<T> serializationType,
                                @Nonnull SerializationOption... options) {
        try {
            this.dataSource = requireNonNull(dataSource);
            this.serializer = SerializerFactory.newAggregateSerializer(serializationType, options);

            try (final Connection connection = dataSource.getConnection()) {
                this.ddlProducer = DDLFactory.newSnapshotDDLProducer(connection);
                createSnapshotStore(connection, ddlProducer.createSnapshotStore(serializationType));
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
        final boolean snapshotExists = findAggregateByUuid(aggregate.uuid()) != null;
        final String sql = snapshotExists ? ddlProducer.updateAggregate() : ddlProducer.insertAggregate();
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
            try (PreparedStatement statement = connection.prepareStatement(ddlProducer.deleteAggregates())) {
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
            try (PreparedStatement statement = connection.prepareStatement(ddlProducer.queryAggregateByUuid())) {
                statement.setObject(1, Objects.requireNonNull(uuid, "Aggregate uuid must not be null"));
                final ResultSet set = statement.executeQuery();
                if (!set.next()) {
                    return null;
                }
                return serializer.deserialize(unwrapJdbcType(set.getObject(ddlProducer.contentName())));
            }
        });
    }

    @SneakyThrows
    private <Y> Y execute(@Nonnull ThrowableFunction<Connection, Y> consumer) {
        try (Connection connection = dataSource.getConnection()) {
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
