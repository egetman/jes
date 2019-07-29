-- schemaName is alias. It will be replaced with actual types in runtime
CREATE SCHEMA IF NOT EXISTS schemaName;

CREATE TABLE IF NOT EXISTS schemaName.snapshot_store
(
    id   BIGSERIAL PRIMARY KEY,
    uuid UUID NOT NULL,
    data TEXT NOT NULL
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS snapshot_uuid_idx ON schemaName.snapshot_store (uuid NULLS LAST);