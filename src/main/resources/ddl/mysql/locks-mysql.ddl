-- schemaName is alias. It will be replaced with actual types in runtime
CREATE SCHEMA IF NOT EXISTS schemaName;

CREATE TABLE IF NOT EXISTS schemaName.locks
(
    id        BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    lock_key  VARCHAR(255) NOT NULL UNIQUE,
    locked_at TIMESTAMP    NOT NULL DEFAULT NOW()
) ENGINE = InnoDB;
