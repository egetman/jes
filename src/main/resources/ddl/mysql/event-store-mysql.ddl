-- schemaName & contentType are aliases. They will be replaced with actual types at runtime.
CREATE SCHEMA IF NOT EXISTS schemaName;

-- noinspection SqlResolve

CREATE TABLE IF NOT EXISTS schemaName.event_store
(
    id   BIGINT NOT NULL AUTO_INCREMENT,
    uuid BINARY(80),
    data contentType NOT NULL,
    CONSTRAINT es_pk PRIMARY KEY (id)
) ENGINE = InnoDB;

CREATE INDEX uuid_idx ON schemaName.event_store (uuid);

-- todo: trim columns for better performance (for example uuid - binary(80) for mysql >.<)