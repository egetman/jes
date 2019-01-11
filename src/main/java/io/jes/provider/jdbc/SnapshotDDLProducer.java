package io.jes.provider.jdbc;

import javax.annotation.Nonnull;

public interface SnapshotDDLProducer {

    /**
     * Return ddl statement for {@literal Snapshot Store} based on aggregate payload type.
     *
     * @param contentType type of serialized aggregate.
     * @return ddl of {@literal Snapshot Store}, never null.
     */
    @Nonnull
    String createSnapshotStore(Class<?> contentType);

    /**
     * @return SQL select statement for quering aggregates by uuid for specific database.
     */
    @Nonnull
    String queryAggregateByUuid();

    /**
     * @return SQL insert statement for specific database.
     */
    @Nonnull
    String insertAggregate();

    /**
     * @return SQL update statement for specific database.
     */
    @Nonnull
    String updateAggregate();

    /**
     * @return SQL delete statement for specific database. (Clear whole snapshot store)
     */
    @Nonnull
    String deleteAggregates();

    /**
     * @return aggregate payload (serialized data) column name, never null.
     */
    @Nonnull
    String contentName();

}
