package io.jes.provider.jdbc;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

class PostgresDDL implements StoreDDLProducer, SnapshotDDLProducer {

    /**
     * Provided ddl statements (assumed schema name 'foo')
     *
     * <p>CREATE SCHEMA IF NOT EXISTS foo;</p>
     *
     * <p>CREATE TABLE IF NOT EXISTS foo.event_store (
     *      id BIGSERIAL PRIMARY KEY,
     *      uuid UUID,
     *      data (BYTEA | TEXT) NOT NULL
     * );</p>
     *
     * <p>CREATE INDEX CONCURRENTLY IF NOT EXISTS uuid_idx ON foo.event_store (uuid NULLS LAST);</p>
     *
     */
    private static final String READ_EVENTS = "SELECT * FROM %sevent_store WHERE id > ? ORDER BY id";
    private static final String READ_EVENTS_BY_STREAM = "SELECT * FROM %sevent_store WHERE uuid = ? ORDER BY id";
    private static final String READ_EVENTS_STREAM_VERSION = "SELECT count(*) FROM %sevent_store WHERE uuid = ?";
    private static final String READ_EVENTS_BY_STREAM_WITH_SKIP = "SELECT * FROM %sevent_store WHERE uuid = ? ORDER "
            + "BY id OFFSET ?";
    private static final String WRITE_EVENTS = "INSERT INTO %sevent_store (uuid, data) VALUES (?, ?)";
    private static final String DELETE_EVENTS = "DELETE FROM %sevent_store WHERE uuid = ?";

    private static final String DELETE_AGGREGATES = "DELETE FROM %ssnapshot_store WHERE uuid = ?";
    private static final String WRITE_AGGREGATE = "INSERT INTO %ssnapshot_store (data, uuid) VALUES (?, ?)";
    private static final String UPDATE_AGGREGATE = "UPDATE %ssnapshot_store SET data = ? WHERE uuid = ?";
    private static final String READ_AGGREGATE_BY_STREAM = "SELECT * FROM %ssnapshot_store WHERE uuid = ?";

    private static final String CONTENT_NAME = "data";
    private static final String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS %s;";

    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %sevent_store "
            + "(id BIGSERIAL PRIMARY KEY, uuid UUID, " + CONTENT_NAME + " %s NOT NULL);";
    private static final String CREATE_INDEX = "CREATE INDEX CONCURRENTLY IF NOT EXISTS uuid_idx "
            + "ON %sevent_store (uuid NULLS LAST);";

    private static final String CREATE_SNAPSHOT_TABLE = "CREATE TABLE IF NOT EXISTS %ssnapshot_store "
            + "(id BIGSERIAL PRIMARY KEY, uuid UUID NOT NULL, " + CONTENT_NAME + " TEXT NOT NULL);";
    private static final String CREATE_SNAPSHOT_INDEX = "CREATE INDEX CONCURRENTLY IF NOT EXISTS snapshot_uuid_idx "
            + "ON %ssnapshot_store (uuid NULLS LAST);";

    private final String schema;
    private String queryEvents;
    private String queryEventsByStream;
    private String queryEventsStreamVersion;
    private String queryEventsByStreamWithSkip;
    private String insertEvents;
    private String deleteEvents;

    private String insertAggregate;
    private String updateAggregate;
    private String deleteAggregates;
    private String queryAggregateByStream;

    PostgresDDL(@Nonnull String schema) {
        this.schema = schema;
    }

    @Nonnull
    @Override
    public String createStore(@Nonnull Class<?> contentType) {
        if (contentType != String.class && contentType != byte[].class) {
            throw new IllegalArgumentException("Illegal type of content column: " + contentType);
        }
        final String type = contentType == String.class ? "TEXT" : "BYTEA";

        final StringBuilder ddl = new StringBuilder();
        ddl.append(String.format(CREATE_SCHEMA, schema));
        ddl.append(String.format(CREATE_TABLE, formatSchema(), type));
        ddl.append(String.format(CREATE_INDEX, formatSchema()));
        return ddl.toString();
    }

    @Nonnull
    @Override
    public String contentName() {
        return CONTENT_NAME;
    }

    @Nonnull
    @Override
    public String insertEvents() {
        if (insertEvents == null) {
            insertEvents = String.format(WRITE_EVENTS, formatSchema());
        }
        return insertEvents;
    }

    @Nonnull
    @Override
    public String queryEvents() {
        if (queryEvents == null) {
            queryEvents = String.format(READ_EVENTS, formatSchema());
        }
        return queryEvents;
    }

    @Nonnull
    @Override
    public String deleteEvents() {
        if (deleteEvents == null) {
            deleteEvents = String.format(DELETE_EVENTS, formatSchema());
        }
        return deleteEvents;
    }

    @Nonnull
    @Override
    public String queryEventsByUuid() {
        if (queryEventsByStream == null) {
            queryEventsByStream = String.format(READ_EVENTS_BY_STREAM, formatSchema());
        }
        return queryEventsByStream;
    }

    @Nullable
    @Override
    public String queryEventsByUuidWithSkip() {
        if (queryEventsByStreamWithSkip == null) {
            queryEventsByStreamWithSkip = String.format(READ_EVENTS_BY_STREAM_WITH_SKIP, formatSchema());
        }
        return queryEventsByStreamWithSkip;
    }

    @Nonnull
    @Override
    public String queryEventsStreamVersion() {
        if (queryEventsStreamVersion == null) {
            queryEventsStreamVersion = String.format(READ_EVENTS_STREAM_VERSION, formatSchema());
        }
        return queryEventsStreamVersion;
    }

    @Nonnull
    @Override
    public String createSnapshotStore(Class<?> contentType) {
        if (contentType != String.class) {
            throw new IllegalArgumentException("Illegal type of content column: " + contentType);
        }

        final StringBuilder ddl = new StringBuilder();
        ddl.append(String.format(CREATE_SNAPSHOT_TABLE, formatSchema()));
        ddl.append(String.format(CREATE_SNAPSHOT_INDEX, formatSchema()));
        return ddl.toString();
    }

    @Nonnull
    @Override
    public String queryAggregateByUuid() {
        if (queryAggregateByStream == null) {
            queryAggregateByStream = String.format(READ_AGGREGATE_BY_STREAM, formatSchema());
        }
        return queryAggregateByStream;
    }

    @Nonnull
    @Override
    public String insertAggregate() {
        if (insertAggregate == null) {
            insertAggregate = String.format(WRITE_AGGREGATE, formatSchema());
        }
        return insertAggregate;
    }

    @Nonnull
    @Override
    public String updateAggregate() {
        if (updateAggregate == null) {
            updateAggregate = String.format(UPDATE_AGGREGATE, formatSchema());
        }
        return updateAggregate;
    }

    @Nonnull
    @Override
    public String deleteAggregates() {
        if (deleteAggregates == null) {
            deleteAggregates = String.format(DELETE_AGGREGATES, formatSchema());
        }
        return deleteAggregates;
    }

    @Nonnull
    private String formatSchema() {
        return schema + ".";
    }
}
