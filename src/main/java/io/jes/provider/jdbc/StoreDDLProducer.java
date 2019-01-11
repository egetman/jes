package io.jes.provider.jdbc;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.jes.Event;

/**
 * {@link io.jes.provider.JdbcStoreProvider} component, that provides database-specific SQL statements.
 */
public interface StoreDDLProducer {

    /**
     * Return ddl statement for {@literal Event Store} based on event payload type.
     *
     * @param contentType type of serialized event.
     * @return ddl of {@literal Event Store}, never null.
     */
    @Nonnull
    String createStore(Class<?> contentType);

    /**
     * @return event payload (serialized data) column name, never null.
     */
    @Nonnull
    String contentName();

    /**
     * @return SQL insert statement for specific database.
     */
    @Nonnull
    String insertEvents();

    /**
     * @return SQL select statement for specific database.
     */
    @Nonnull
    String queryEvents();

    /**
     * @return SQL delete statement for specific database.
     */
    @Nonnull
    String deleteEvents();

    /**
     * @return SQL select statement for quering events by uuid for specific database.
     */
    @Nonnull
    String queryEventsByUuid();

    /**
     * Optional query for snapshot processing.
     * @return SQL select statement for quering events by uuid with skip first n for specific database.
     */
    @Nullable
    default String queryEventsByUuidWithSkip() {
        return null;
    }

    /**
     * Note: event stream: collection of events grouped by {@link Event#uuid()}.
     *
     * @return SQL select statement for quering event stream version for specific database.
     */
    @Nonnull
    String queryEventsStreamVersion();
}
