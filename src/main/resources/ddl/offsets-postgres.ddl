-- schemaName is alias. It will be replaced with actual types in runtime
CREATE SCHEMA IF NOT EXISTS schemaName;

CREATE TABLE IF NOT EXISTS schemaName.offsets
(
    id    BIGSERIAL PRIMARY KEY,
    key   VARCHAR(256) UNIQUE NOT NULL,
    value BIGINT DEFAULT 0    NOT NULL
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS offsets_key_idx ON schemaName.offsets (key);