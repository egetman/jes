-- schemaName is alias. It will be replaced with actual types in runtime
CREATE SCHEMA IF NOT EXISTS schemaName;

CREATE TABLE IF NOT EXISTS schemaName.locks
(
    id        BIGSERIAL PRIMARY KEY,
    key       VARCHAR(256) UNIQUE     NOT NULL,
    locked_at TIMESTAMP DEFAULT NOW() NOT NULL
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS locks_key_idx ON schemaName.locks (key);