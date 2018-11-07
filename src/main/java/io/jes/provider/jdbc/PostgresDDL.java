package io.jes.provider.jdbc;

import javax.annotation.Nonnull;

class PostgresDDL implements DDLProducer {

    private static final String READ_EVENTS = "SELECT * FROM %sevent_store WHERE id > ? ORDER BY id";
    private static final String READ_EVENTS_BY_STREAM = "SELECT * FROM %sevent_store WHERE uuid = ? ORDER BY id";
    private static final String READ_EVENTS_STREAM_VERSION = "SELECT count(*) FROM %sevent_store WHERE uuid = ?";
    private static final String WRITE_EVENTS = "INSERT INTO %sevent_store (uuid, data) VALUES (?, ?)";
    private static final String DELETE_EVENTS = "DELETE FROM %sevent_store WHERE uuid = ?";

    private static final String EVENT_CONTENT_NAME = "data";
    private static final String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS %s;";
    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %sevent_store "
            + "(id BIGSERIAL PRIMARY KEY, uuid UUID, " + EVENT_CONTENT_NAME + " %s NOT NULL);";

    private final String schema;
    private String queryEvents;
    private String queryEventsByStream;
    private String queryEventsStreamVersion;
    private String insertEvents;
    private String deleteEvents;

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
        return ddl.toString();
    }

    @Nonnull
    @Override
    public String eventContentName() {
        return EVENT_CONTENT_NAME;
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

    @Nonnull
    @Override
    public String queryEventsStreamVersion() {
        if (queryEventsStreamVersion == null) {
            queryEventsStreamVersion = String.format(READ_EVENTS_STREAM_VERSION, formatSchema());
        }
        return queryEventsStreamVersion;
    }

    @Nonnull
    private String formatSchema() {
        return schema + ".";
    }
}
