-- schemaName & contentType are aliases. They will be replaced with actual types in runtime
CREATE SCHEMA IF NOT EXISTS schemaName;

-- noinspection SqlResolve

CREATE TABLE IF NOT EXISTS schemaName.event_store
(
    id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    uuid UUID,
    data contentType NOT NULL
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS uuid_idx ON schemaName.event_store USING HASH (uuid);