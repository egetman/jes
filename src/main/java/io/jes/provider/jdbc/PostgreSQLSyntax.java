package io.jes.provider.jdbc;

import javax.annotation.Nonnull;

class PostgreSQLSyntax implements DataSourceSyntax {

    private static final String READ_EVENTS = "SELECT * FROM %sevent_store WHERE id > ? ORDER BY id";
    private static final String READ_EVENTS_BY_STREAM = "SELECT * FROM %sevent_store WHERE stream = ? ORDER BY id";
    private static final String READ_EVENTS_STREAM_VERSION = "SELECT count(*) FROM %sevent_store WHERE stream = ?";
    private static final String WRITE_EVENTS = "INSERT INTO %sevent_store (stream, data) VALUES (?, ?)";

    private static final String CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS %s;";
    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %sevent_store "
            + "(id BIGSERIAL PRIMARY KEY, stream VARCHAR(36), data %s NOT NULL);";
    private static final String EVENT_CONTENT_NAME = "data";

    private final String schema;
    private String readEvents;
    private String readEventsByStream;
    private String readEventsStreamVersion;
    private String writeEvents;

    PostgreSQLSyntax(String schema) {
        this.schema = schema;
    }

    @Nonnull
    @Override
    public String createStore(Class<?> contentType) {
        if (contentType != String.class && contentType != byte[].class) {
            throw new IllegalArgumentException("Illegal type of content column: " + contentType);
        }
        final String type = contentType == String.class ? "TEXT" : "BYTEA";

        final StringBuilder ddl = new StringBuilder();
        if (schema != null) {
            ddl.append(String.format(CREATE_SCHEMA, schema));
        }
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
    public String writeEvents() {
        if (writeEvents == null) {
            writeEvents = String.format(WRITE_EVENTS, formatSchema());
        }
        return writeEvents;
    }

    @Nonnull
    @Override
    public String readEvents() {
        if (readEvents == null) {
            readEvents = String.format(READ_EVENTS, formatSchema());
        }
        return readEvents;
    }

    @Nonnull
    @Override
    public String readEventsByStream() {
        if (readEventsByStream == null) {
            readEventsByStream = String.format(READ_EVENTS_BY_STREAM, formatSchema());
        }
        return readEventsByStream;

    }

    @Nonnull
    @Override
    public String eventsStreamVersion() {
        if (readEventsStreamVersion == null) {
            readEventsStreamVersion = String.format(READ_EVENTS_STREAM_VERSION, formatSchema());
        }
        return readEventsStreamVersion;
    }

    @Nonnull
    private String formatSchema() {
        return schema != null ? schema + "." : "";
    }
}
