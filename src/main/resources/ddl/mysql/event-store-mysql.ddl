-- schemaName & contentType are aliases. They will be replaced with actual types at runtime.
CREATE SCHEMA IF NOT EXISTS schemaName;

-- noinspection SqlResolve

CREATE TABLE IF NOT EXISTS schemaName.event_store
(
    id   BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    uuid BINARY(80),
    data contentType NOT NULL
) ENGINE = InnoDB;

SET @x := (SELECT COUNT(*)
           FROM information_schema.statistics
           WHERE table_name = 'event_store'
             AND index_name = 'uuid_idx'
             AND table_schema = DATABASE());
SET @sql := if(@x > 0, 'select ''idx exists''', 'ALTER TABLE event_store ADD INDEX uuid_idx (uuid);');
PREPARE stmt FROM @sql;
EXECUTE stmt;

-- todo: trim columns for better performance (for example uuid - binary(80) for mysql >.<)