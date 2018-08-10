package io.jes.provider.jdbc;

class PostgreSQLSyntax implements DataSourceSyntax {

    private static final String SEQUENCE_VALUE_NAME = "max";
    private static final String EVENT_CONTENT_NAME = "data";
    private static final String SEQUENCE_CALL = "SELECT max(id) FROM event_store";
    private static final String READ_EVENTS = "SELECT * FROM event_store WHERE id > ? ORDER BY id";
    private static final String READ_EVENTS_BY_STREAM = "SELECT * FROM event_store WHERE stream = ? ORDER BY id";
    private static final String WRITE_EVENTS = "INSERT INTO event_store (id, stream, data) VALUES (?, ?, ?)";
    private static final String CREATE_EVENT_STORE = "CREATE TABLE IF NOT EXISTS event_store "
            + "(id BIGINT NOT NULL PRIMARY KEY, stream VARCHAR(256), data BYTEA NOT NULL);";

    @Override
    public String createStore() {
        return CREATE_EVENT_STORE;
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
    public String eventContentName() {
        return EVENT_CONTENT_NAME;
    }

    @Override
    public String sequenceValueName() {
        return SEQUENCE_VALUE_NAME;
    }

    @Override
    public String nextSequenceNumber() {
        return SEQUENCE_CALL;
    }
}
