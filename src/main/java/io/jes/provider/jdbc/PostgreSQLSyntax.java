package io.jes.provider.jdbc;

class PostgreSQLSyntax implements DataSourceSyntax {

    private static final String EVENT_CONTENT_NAME = "data";
    private static final String READ_EVENTS = "SELECT * FROM event_store WHERE id > ? ORDER BY id";
    private static final String READ_EVENTS_BY_STREAM = "SELECT * FROM event_store WHERE stream = ? ORDER BY id";
    private static final String READ_EVENTS_STREAM_VERSION = "SELECT count(*) FROM event_store WHERE stream = ?";
    private static final String WRITE_EVENTS = "INSERT INTO event_store (stream, data) VALUES (?, ?)";

    private static final String CREATE_EVENT_STORE = "CREATE TABLE IF NOT EXISTS event_store "
            + "(id BIGSERIAL PRIMARY KEY, stream VARCHAR(36), data %s NOT NULL);";

    @Override
    public String createStore(Class<?> contentType) {
        if (contentType != String.class && contentType != byte[].class) {
            throw new IllegalArgumentException("Illegal type of content column: " + contentType);
        }
        final String type = contentType == String.class ? "TEXT" : "BYTEA";
        return String.format(CREATE_EVENT_STORE, type);
    }

    @Override
    public String eventContentName() {
        return EVENT_CONTENT_NAME;
    }

    @Override
    public String writeEvents() {
        return WRITE_EVENTS;
    }

    @Override
    public String readEvents() {
        return READ_EVENTS;
    }

    @Override
    public String readEventsByStream() {
        return READ_EVENTS_BY_STREAM;
    }

    @Override
    public String eventsStreamVersion() {
        return READ_EVENTS_STREAM_VERSION;
    }
}
