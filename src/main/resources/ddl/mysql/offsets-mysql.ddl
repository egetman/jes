-- schemaName is alias. It will be replaced with actual types in runtime
CREATE SCHEMA IF NOT EXISTS schemaName;

CREATE TABLE IF NOT EXISTS schemaName.offsets
(
    id         BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    offset_key VARCHAR(255) NOT NULL UNIQUE,
    value      BIGINT       NOT NULL DEFAULT 0
);